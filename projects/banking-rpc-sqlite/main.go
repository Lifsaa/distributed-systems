package main

import (
	"flag"
	"fmt"
	"log"
	"os"
)

// main picks server or client mode.
func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: go run . server|client [flags]")
		os.Exit(1)
	}
	switch os.Args[1] {
	case "server":
		runServer(os.Args[2:])
	case "client":
		runClientMode(os.Args[2:])
	default:
		log.Fatal("first argument must be server or client")
	}
}

// runServer parses flags and starts the bank server.
func runServer(args []string) {
	fs := flag.NewFlagSet("server", flag.ExitOnError)
	addr := fs.String("addr", ":5001", "listen address")
	dbPath := fs.String("db", "bank.db", "sqlite database path")
	fs.Parse(args)
	db, err := openDB(*dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	if err := serveRPC(*addr, newBank(db)); err != nil {
		log.Fatal(err)
	}
}

// runClientMode parses flags and runs a client operation.
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
