package main

import (
	"database/sql"
	"fmt"
	"io"
	"net"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	RoleFollower  = "follower"
	RoleCandidate = "candidate"
	RoleLeader    = "leader"

	heartbeatInterval = 200 * time.Millisecond
	electionMin       = 800 * time.Millisecond
	electionJitter    = 700 * time.Millisecond
)

type ReplicaConfig struct {
	ID         int
	IsLeader   bool
	Peers      map[int]string
	SelfAddr   string
	LeaderID   int
	LeaderAddr string
	Term       int64
	LogPath    string
}

type Bank struct {
	mu            sync.Mutex
	db            *sql.DB
	id            int
	selfAddr      string
	peers         map[int]string
	role          string
	leaderID      int
	leaderAddr    string
	term          int64
	votedFor      int
	commitIndex   int64
	appliedIndex  int64
	nextRequestID int64
	nextIndex     map[int]int64
	matchIndex    map[int]int64
	electionReset time.Time
	logger        io.Writer
	logFile       *os.File
	stopCh        chan struct{}
}

type Raft struct {
	bank *Bank
}

/*
Purpose:

	Creates a single-node leader for tests and local banking demos.

Args:

	db: SQLite handle that stores accounts, operation logs, and Raft state.

Returns:

	A Bank configured as replica 1 with no peers.
*/
func newBank(db *sql.DB) *Bank {
	return newBankWithConfig(db, ReplicaConfig{ID: 1, IsLeader: true, Term: 1})
}

/*
Purpose:

	Builds one PA8 replica from command-line configuration and durable state.

Args:

	db: SQLite handle for this replica.
	cfg: Replica id, peer addresses, startup term, leader hint, and log file path.

Returns:

	A Bank with Raft counters loaded from SQLite and peer indexes initialized.

Rationale:

	Raft term, vote, commit index, applied index, and next request id must survive
	server restarts so recovered replicas can rejoin safely.
*/
func newBankWithConfig(db *sql.DB, cfg ReplicaConfig) *Bank {
	if cfg.ID == 0 {
		cfg.ID = 1
	}
	if cfg.Peers == nil {
		cfg.Peers = map[int]string{}
	}
	term, _ := getStateInt(db, "current_term")
	if term == 0 {
		term = cfg.Term
	}
	if term == 0 {
		term = 1
	}
	votedFor, _ := getStateInt(db, "voted_for")
	commitIndex, _ := getStateInt(db, "commit_index")
	appliedIndex, _ := getStateInt(db, "applied_index")
	nextRequestID, _ := getStateInt(db, "next_request_id")
	if nextRequestID == 0 {
		nextRequestID = 1
	}
	role := RoleFollower
	leaderID := cfg.LeaderID
	leaderAddr := cfg.LeaderAddr
	if cfg.IsLeader {
		role = RoleLeader
		leaderID = cfg.ID
		leaderAddr = cfg.SelfAddr
	}
	b := &Bank{
		db:            db,
		id:            cfg.ID,
		selfAddr:      cfg.SelfAddr,
		peers:         cfg.Peers,
		role:          role,
		leaderID:      leaderID,
		leaderAddr:    leaderAddr,
		term:          term,
		votedFor:      int(votedFor),
		commitIndex:   commitIndex,
		appliedIndex:  appliedIndex,
		nextRequestID: nextRequestID,
		nextIndex:     map[int]int64{},
		matchIndex:    map[int]int64{},
		electionReset: time.Now(),
		logger:        os.Stdout,
		stopCh:        make(chan struct{}),
	}
	if cfg.LogPath != "" {
		if f, err := os.OpenFile(cfg.LogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); err == nil {
			b.logFile = f
			b.logger = io.MultiWriter(os.Stdout, f)
		}
	}
	last, _ := lastRaftEntry(db)
	for id := range cfg.Peers {
		b.nextIndex[id] = last.Index + 1
	}
	_ = setStateInt(db, "current_term", b.term)
	_ = setStateInt(db, "voted_for", int64(b.votedFor))
	_ = setStateInt(db, "next_request_id", b.nextRequestID)
	return b
}

/*
Purpose:

	Stops background loops and closes the optional debug log file.

Returns:

	Any error from closing the log file, or nil when there is no file to close.
*/
func (b *Bank) Close() error {
	closeOnce := func() {
		select {
		case <-b.stopCh:
		default:
			close(b.stopCh)
		}
	}
	closeOnce()
	if b.logFile != nil {
		return b.logFile.Close()
	}
	return nil
}

/*
Purpose:

	Registers banking and Raft RPC services, starts Raft background loops, and
	listens for replica/client traffic.

Args:

	addr: TCP address this process listens on, such as ":5001".
	bank: Replica state and banking database wrapper.

Returns:

	An error if RPC registration or TCP listen setup fails.
*/
func serveRPC(addr string, bank *Bank) error {
	bank.mu.Lock()
	bank.selfAddr = publishAddr(addr)
	if bank.role == RoleLeader {
		bank.leaderID = bank.id
		bank.leaderAddr = bank.selfAddr
	}
	bank.mu.Unlock()
	bank.startRaftLoops()

	server := rpc.NewServer()
	if err := server.RegisterName("Bank", bank); err != nil {
		return err
	}
	if err := server.RegisterName("Raft", &Raft{bank: bank}); err != nil {
		return err
	}
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	fmt.Printf("replica %d %s listening on %s\n", bank.id, bank.role, addr)
	server.Accept(l)
	return nil
}

/*
Purpose:

	Converts a listen address into an address that can be shown to clients.

Args:

	addr: Listen address from the server flag.

Returns:

	A host:port style address when the listen address only contains a port.
*/
func publishAddr(addr string) string {
	if strings.HasPrefix(addr, ":") {
		return "localhost" + addr
	}
	return addr
}

/*
Purpose:

	Starts the election timeout loop and heartbeat loop for this replica.

Rationale:

	Election and heartbeat work runs outside RPC handlers so the server can detect
	leader failure and maintain leadership without waiting for client traffic.
*/
func (b *Bank) startRaftLoops() {
	go b.electionLoop()
	go b.heartbeatLoop()
}
