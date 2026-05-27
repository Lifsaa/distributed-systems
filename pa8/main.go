package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

/*
Purpose:

	Picks server, client, or tester mode from the first command-line argument.
*/
func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: go run . server|client|tester [flags]")
		os.Exit(1)
	}
	switch os.Args[1] {
	case "server":
		runServer(os.Args[2:])
	case "client":
		runClientMode(os.Args[2:])
	case "tester":
		runTesterMode(os.Args[2:])
	default:
		log.Fatal("first argument must be server, client, or tester")
	}
}

/*
Purpose:

	Parses server flags, opens the replica database, and starts one Raft-enabled
	banking server.

Args:

	args: Command-line flags after the "server" subcommand.

Rationale:

	Each Docker service runs this same binary with a different id, port, database,
	debug log path, and peer list.
*/
func runServer(args []string) {
	fs := flag.NewFlagSet("server", flag.ExitOnError)
	id := fs.Int("id", 1, "replica id")
	addr := fs.String("addr", ":5001", "listen address")
	self := fs.String("self", "", "published address other replicas use for this server")
	dbPath := fs.String("db", "bank.db", "sqlite database path")
	logPath := fs.String("log", "", "debug output file")
	peers := fs.String("peers", "", "peer list like 2=replica2:5002,3=replica3:5003")
	leader := fs.Bool("leader", false, "start as leader for compatibility tests; PA08 docker leaves this false")
	leaderAddr := fs.String("leader-addr", "", "initial known leader hint")
	fs.Parse(normalizeBoolFlagPairs(args))
	db, err := openDB(*dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	cfg := ReplicaConfig{
		ID:         *id,
		IsLeader:   *leader,
		SelfAddr:   firstNonEmpty(*self, publishAddr(*addr)),
		Peers:      parsePeerMap(*peers),
		LeaderAddr: *leaderAddr,
		Term:       1,
		LogPath:    *logPath,
	}
	if err := serveRPC(*addr, newBankWithConfig(db, cfg)); err != nil {
		log.Fatal(err)
	}
}

/*
Purpose:

	Accepts "-leader true" as well as "-leader=true" for compatibility with older
	assignment-style command examples.
*/
func normalizeBoolFlagPairs(args []string) []string {
	out := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		if args[i] == "-leader" && i+1 < len(args) && (args[i+1] == "true" || args[i+1] == "false") {
			out = append(out, "-leader="+args[i+1])
			i++
			continue
		}
		out = append(out, args[i])
	}
	return out
}

/*
Purpose:

	Parses client flags and runs one banking command, demo story, or load test.
*/
func runClientMode(args []string) {
	fs := flag.NewFlagSet("client", flag.ExitOnError)
	addr := fs.String("addr", "localhost:5001", "server address")
	op := fs.String("op", "balance", "operation")
	username := fs.String("username", "", "username")
	account := fs.Int64("account", 0, "account id")
	to := fs.Int64("to", 0, "destination account id")
	amount := fs.Int64("amount", 0, "amount in cents")
	percent := fs.Float64("percent", 0, "percent for teller update")
	clients := fs.Int("clients", 100, "load test clients")
	requests := fs.Int("requests", 1000, "load test requests")
	fs.Parse(args)
	err := runClient(
		*addr,
		*op,
		*username,
		*account,
		*to,
		*amount,
		*percent,
		*clients,
		*requests,
	)
	if err != nil {
		log.Fatal(err)
	}
}

/*
Purpose:

	Parses tester flags and compares the Raft logs across all replicas.
*/
func runTesterMode(args []string) {
	fs := flag.NewFlagSet("tester", flag.ExitOnError)
	replicas := fs.String("replicas", defaultReplicaList(), "replica list like name=host:port")
	fs.Parse(args)
	if err := runTester(parseNamedAddrMap(*replicas)); err != nil {
		log.Fatal(err)
	}
}

/*
Purpose:

	Parses a replica peer list into numeric ids and RPC addresses.

Args:

	raw: Comma-separated values like "2=replica2:5002,3=replica3:5003".
*/
func parsePeerMap(raw string) map[int]string {
	peers := map[int]string{}
	for key, addr := range parseNamedAddrMap(raw) {
		id, err := strconv.Atoi(key)
		if err == nil && strings.TrimSpace(addr) != "" {
			peers[id] = addr
		}
	}
	return peers
}

/*
Purpose:

	Parses comma-separated name=address values used by peers and tester replicas.
*/
func parseNamedAddrMap(raw string) map[string]string {
	result := map[string]string{}
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		pieces := strings.SplitN(part, "=", 2)
		if len(pieces) != 2 {
			continue
		}
		name := strings.TrimSpace(pieces[0])
		addr := strings.TrimSpace(pieces[1])
		if name != "" && addr != "" {
			result[name] = addr
		}
	}
	return result
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func defaultReplicaList() string {
	return "replica1=replica1:5001,replica2=replica2:5002,replica3=replica3:5003,replica4=replica4:5004,replica5=replica5:5005"
}
