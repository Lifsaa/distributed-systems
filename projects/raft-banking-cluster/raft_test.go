package main

import (
	"database/sql"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

/*
openTempBankDB creates a clean SQLite database for one test.
*/
func openTempBankDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := openDB(t.TempDir() + "/bank.db")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() {
		db.Close()
	})
	return db
}

/*
testPayload encodes a request payload for a log entry.
*/
func testPayload(t *testing.T, v any) string {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return string(data)
}

func TestLeaderCommitsClientOperationThroughLog(t *testing.T) {
	db := openTempBankDB(t)
	bank := newBankWithConfig(db, ReplicaConfig{
		ID:       1,
		IsLeader: true,
		Peers:    map[int]string{},
	})

	var opened AccountResponse
	if err := bank.OpenAccount(&OpenAccountRequest{
		Username:       "alice",
		InitialBalance: 10000,
	}, &opened); err != nil {
		t.Fatalf("open account rpc: %v", err)
	}
	if !opened.OK || opened.Account.ID == 0 {
		t.Fatalf("open account failed: %+v", opened)
	}

	var balance BalanceResponse
	if err := bank.CheckBalance(&AccountRequest{AccountID: opened.Account.ID}, &balance); err != nil {
		t.Fatalf("balance rpc: %v", err)
	}
	if !balance.OK || balance.Balance != 10000 {
		t.Fatalf("balance after commit = %+v", balance)
	}

	entries, err := listRaftLog(db)
	if err != nil {
		t.Fatalf("list raft log: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("raft log length = %d, want 2", len(entries))
	}
	if entries[0].Operation != OpOpenAccount || entries[1].Operation != OpCheckBalance {
		t.Fatalf("operations = %s, %s", entries[0].Operation, entries[1].Operation)
	}
	if entries[0].RequestID != 1 || entries[1].RequestID != 2 {
		t.Fatalf("request ids = %d, %d; want 1, 2", entries[0].RequestID, entries[1].RequestID)
	}
}

func TestFollowerAppendsAndAppliesCommittedEntries(t *testing.T) {
	db := openTempBankDB(t)
	bank := newBankWithConfig(db, ReplicaConfig{ID: 2, IsLeader: false})

	entry := LogEntry{
		Index:     1,
		Term:      1,
		RequestID: 9,
		Operation: OpOpenAccount,
		Payload: testPayload(t, OpenAccountRequest{
			Username:       "bob",
			InitialBalance: 2500,
		}),
	}
	var resp AppendEntriesResponse
	err := bank.AppendEntries(&AppendEntriesRequest{
		Term:         1,
		LeaderID:     1,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{entry},
		LeaderCommit: 1,
	}, &resp)
	if err != nil {
		t.Fatalf("append entries rpc: %v", err)
	}
	if !resp.Success || resp.MatchIndex != 1 {
		t.Fatalf("append response = %+v", resp)
	}

	entries, err := listRaftLog(db)
	if err != nil {
		t.Fatalf("list raft log: %v", err)
	}
	if len(entries) != 1 || entries[0].Operation != OpOpenAccount {
		t.Fatalf("entries = %+v", entries)
	}
	if entries[0].RequestID != 9 {
		t.Fatalf("request id = %d, want 9", entries[0].RequestID)
	}

	tx, err := begin(db)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback()
	account, err := getAccount(tx, 1)
	if err != nil {
		t.Fatalf("account was not applied: %v", err)
	}
	if account.Username != "bob" || account.Balance != 2500 {
		t.Fatalf("account = %+v", account)
	}
}

func TestCompareRaftLogsDetectsMismatch(t *testing.T) {
	good := []LogEntry{{Index: 1, Term: 1, RequestID: 1, Operation: OpDeposit, Payload: `{"AccountID":1,"Amount":5}`}}
	bad := []LogEntry{{Index: 1, Term: 1, RequestID: 1, Operation: OpDeposit, Payload: `{"AccountID":1,"Amount":6}`}}

	if err := compareRaftLogs(map[string][]LogEntry{
		"replica1": good,
		"replica2": good,
		"replica3": good,
	}); err != nil {
		t.Fatalf("matching logs failed: %v", err)
	}
	if err := compareRaftLogs(map[string][]LogEntry{
		"replica1": good,
		"replica2": bad,
	}); err == nil {
		t.Fatalf("mismatch was not detected")
	}
}

func TestRequestVoteGrantsOnceAndPersistsTerm(t *testing.T) {
	db := openTempBankDB(t)
	bank := newBankWithConfig(db, ReplicaConfig{ID: 1, Peers: map[int]string{2: "server2:5002", 3: "server3:5003"}})

	var first RequestVoteResponse
	if err := bank.RequestVote(&RequestVoteRequest{
		Term:         2,
		CandidateID:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}, &first); err != nil {
		t.Fatalf("first request vote: %v", err)
	}
	if !first.VoteGranted || first.Term != 2 {
		t.Fatalf("first vote = %+v, want granted term 2", first)
	}

	var second RequestVoteResponse
	if err := bank.RequestVote(&RequestVoteRequest{
		Term:         2,
		CandidateID:  3,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}, &second); err != nil {
		t.Fatalf("second request vote: %v", err)
	}
	if second.VoteGranted {
		t.Fatalf("second vote granted in same term: %+v", second)
	}

	term, _ := getStateInt(db, "current_term")
	votedFor, _ := getStateInt(db, "voted_for")
	if term != 2 || votedFor != 2 {
		t.Fatalf("persisted term/vote = %d/%d, want 2/2", term, votedFor)
	}
}

func TestRequestVoteRejectsCandidateWithStaleLog(t *testing.T) {
	db := openTempBankDB(t)
	if err := putRaftEntry(db, LogEntry{
		Index:     1,
		Term:      3,
		RequestID: 1,
		Operation: OpDeposit,
		Payload:   `{"AccountID":1,"Amount":5}`,
	}); err != nil {
		t.Fatalf("seed raft log: %v", err)
	}
	bank := newBankWithConfig(db, ReplicaConfig{ID: 1})

	var resp RequestVoteResponse
	if err := bank.RequestVote(&RequestVoteRequest{
		Term:         4,
		CandidateID:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}, &resp); err != nil {
		t.Fatalf("request vote: %v", err)
	}
	if resp.VoteGranted {
		t.Fatalf("stale candidate got vote: %+v", resp)
	}
}

func TestCandidateCannotWinElectionWithoutMajority(t *testing.T) {
	db := openTempBankDB(t)
	logPath := t.TempDir() + "/server1.log"
	bank := newBankWithConfig(db, ReplicaConfig{
		ID: 1,
		Peers: map[int]string{
			2: "127.0.0.1:1",
			3: "127.0.0.1:2",
			4: "127.0.0.1:3",
			5: "127.0.0.1:4",
		},
		LogPath: logPath,
	})
	t.Cleanup(func() {
		_ = bank.Close()
	})

	bank.startElection()

	bank.mu.Lock()
	role := bank.role
	bank.mu.Unlock()
	if role == RoleLeader {
		t.Fatalf("candidate became leader without majority")
	}
}

func TestLeaderStepsDownWhenAppendEntriesHasHigherTerm(t *testing.T) {
	db := openTempBankDB(t)
	bank := newBankWithConfig(db, ReplicaConfig{ID: 1, IsLeader: true, Term: 1})

	var resp AppendEntriesResponse
	if err := bank.AppendEntries(&AppendEntriesRequest{
		Term:       2,
		LeaderID:   2,
		LeaderAddr: "replica2:5002",
		RequestID:  0,
	}, &resp); err != nil {
		t.Fatalf("append entries: %v", err)
	}

	bank.mu.Lock()
	defer bank.mu.Unlock()
	if bank.role != RoleFollower || bank.term != 2 || bank.leaderID != 2 {
		t.Fatalf("state = role %s term %d leader %d, want follower term 2 leader 2", bank.role, bank.term, bank.leaderID)
	}
}

func TestFollowerRejectsClientRequestWithLeaderHint(t *testing.T) {
	db := openTempBankDB(t)
	bank := newBankWithConfig(db, ReplicaConfig{
		ID:         2,
		LeaderID:   1,
		LeaderAddr: "replica1:5001",
	})

	var resp BasicResponse
	if err := bank.Deposit(&MoneyRequest{AccountID: 1, Amount: 100}, &resp); err != nil {
		t.Fatalf("deposit: %v", err)
	}
	if resp.OK || !strings.Contains(resp.Err, "not leader") {
		t.Fatalf("follower response = %+v, want not leader", resp)
	}
	if resp.LeaderID != 1 || resp.LeaderAddr != "replica1:5001" {
		t.Fatalf("leader hint = %d/%q, want 1/replica1:5001", resp.LeaderID, resp.LeaderAddr)
	}
}

func TestAppendEntriesReplacesConflictingLogEntry(t *testing.T) {
	db := openTempBankDB(t)
	if err := putRaftEntry(db, LogEntry{
		Index:     1,
		Term:      1,
		RequestID: 1,
		Operation: OpDeposit,
		Payload:   `{"AccountID":1,"Amount":5}`,
	}); err != nil {
		t.Fatalf("seed log: %v", err)
	}
	bank := newBankWithConfig(db, ReplicaConfig{ID: 2})

	var resp AppendEntriesResponse
	if err := bank.AppendEntries(&AppendEntriesRequest{
		Term:         2,
		LeaderID:     1,
		LeaderAddr:   "replica1:5001",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []LogEntry{{
			Index:     1,
			Term:      2,
			RequestID: 7,
			Operation: OpWithdraw,
			Payload:   `{"AccountID":1,"Amount":2}`,
		}},
		LeaderCommit: 0,
	}, &resp); err != nil {
		t.Fatalf("append entries: %v", err)
	}
	if !resp.Success {
		t.Fatalf("append response = %+v, want success", resp)
	}
	entries, err := listRaftLog(db)
	if err != nil {
		t.Fatalf("list log: %v", err)
	}
	if len(entries) != 1 || entries[0].Term != 2 || entries[0].RequestID != 7 || entries[0].Operation != OpWithdraw {
		t.Fatalf("repaired entries = %+v", entries)
	}
}

func TestDebugLoggerWritesRequiredAssignmentLines(t *testing.T) {
	db := openTempBankDB(t)
	logPath := t.TempDir() + "/server1.log"
	bank := newBankWithConfig(db, ReplicaConfig{
		ID:      1,
		LogPath: logPath,
	})
	t.Cleanup(func() {
		_ = bank.Close()
	})

	bank.logCandidateVoteRequest(2)
	bank.logVoteDecision(2, 1, true)
	bank.logElectionResult(2, true)
	bank.logElectionTie()
	bank.logLeaderReceivesRequest(5)
	bank.logLeaderAddsRequest(5, 8)
	bank.logLeaderSendsRequest(5, 2)
	bank.logFollowerReceivesRequest(5)
	bank.logLeaderReceivesAck(2, 5)

	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	got := string(data)
	required := []string{
		"CANDIDATE SERVER 1 SENDING A VOTING REQUEST",
		"SERVER 2 VOTES VOTE FOR SERVER 1",
		"CANDIDATE SERVER 1 WINS THE ELECTION FOR TERM 2",
		"ELECTION TIE, TIMEOUT BEGINS",
		"LEADER SERVER 1 RECEIVES REQUEST 5 FROM CLIENT",
		"LEADER SERVER 1 ADDS REQUEST 5 TO LOG ENTRY 8",
		"LEADER SENDS REQUEST 5 TO SERVER 2",
		"FOLLOWER 1 RECEIVES REQUEST 5",
		"LEADER RECEIVES ACK FROM SERVER 2 FOR REQUEST 5",
	}
	for _, want := range required {
		if !strings.Contains(got, want) {
			t.Fatalf("debug log missing %q\nfull log:\n%s", want, got)
		}
	}
}
