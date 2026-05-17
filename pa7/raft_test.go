package main

import (
	"database/sql"
	"encoding/json"
	"testing"
)

// openTempBankDB creates a clean SQLite database for one test.
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

// testPayload encodes a request payload for a log entry.
func testPayload(t *testing.T, v any) string {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return string(data)
}

// TestLeaderCommitsClientOperationThroughLog checks the leader commit path.
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
}

// TestFollowerAppendsAndAppliesCommittedEntries checks follower append/apply.
func TestFollowerAppendsAndAppliesCommittedEntries(t *testing.T) {
	db := openTempBankDB(t)
	bank := newBankWithConfig(db, ReplicaConfig{ID: 2, IsLeader: false})

	entry := LogEntry{
		Index:     1,
		Term:      1,
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

// TestCompareRaftLogsDetectsMismatch checks the tester comparison logic.
func TestCompareRaftLogsDetectsMismatch(t *testing.T) {
	good := []LogEntry{{Index: 1, Term: 1, Operation: OpDeposit, Payload: `{"AccountID":1,"Amount":5}`}}
	bad := []LogEntry{{Index: 1, Term: 1, Operation: OpDeposit, Payload: `{"AccountID":1,"Amount":6}`}}

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
