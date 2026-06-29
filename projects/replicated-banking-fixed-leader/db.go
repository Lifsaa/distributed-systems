package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

// openDB opens sqlite and creates the tables we need.
func openDB(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`PRAGMA busy_timeout = 5000`); err != nil {
		db.Close()
		return nil, err
	}
	if err := createTables(db); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// createTables creates accounts and operation_log if missing.
func createTables(db *sql.DB) error {
	_, err := db.Exec(`
CREATE TABLE IF NOT EXISTS accounts (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	username TEXT NOT NULL,
	balance INTEGER NOT NULL,
	status TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS operation_log (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	created_at TEXT NOT NULL,
	actor_type TEXT NOT NULL,
	operation TEXT NOT NULL,
	details TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS raft_log (
	idx INTEGER PRIMARY KEY,
	term INTEGER NOT NULL,
	operation TEXT NOT NULL,
	payload TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS replica_state (
	key TEXT PRIMARY KEY,
	value INTEGER NOT NULL
);
`)
	return err
}

// begin starts one transaction for one RPC.
func begin(db *sql.DB) (*sql.Tx, error) {
	return db.BeginTx(context.Background(), nil)
}

// logOperation records the operation before changing or reading accounts.
func logOperation(tx *sql.Tx, actor, op, details string) error {
	_, err := tx.Exec(
		`INSERT INTO operation_log(created_at, actor_type, operation, details) VALUES (?, ?, ?, ?)`,
		time.Now().Format(time.RFC3339Nano), actor, op, details,
	)
	return err
}

// getAccount reads one account by id.
func getAccount(tx *sql.Tx, id int64) (Account, error) {
	var a Account
	err := tx.QueryRow(`SELECT id, username, balance, status FROM accounts WHERE id = ?`, id).
		Scan(&a.ID, &a.Username, &a.Balance, &a.Status)
	if errors.Is(err, sql.ErrNoRows) {
		return a, fmt.Errorf("account not found")
	}
	return a, err
}

// needActive rejects frozen and closed customer accounts.
func needActive(a Account) error {
	if a.Status == StatusClosed {
		return fmt.Errorf("account is closed")
	}
	if a.Status == StatusFrozen {
		return fmt.Errorf("account is frozen")
	}
	if a.Status != StatusActive {
		return fmt.Errorf("account is not active")
	}
	return nil
}

// needNotClosed rejects teller changes to closed accounts.
func needNotClosed(a Account) error {
	if a.Status == StatusClosed {
		return fmt.Errorf("account is closed")
	}
	return nil
}

// logCount returns how many operations were logged.
func logCount(db *sql.DB) (int, error) {
	var n int
	err := db.QueryRow(`SELECT COUNT(*) FROM operation_log`).Scan(&n)
	return n, err
}

// getStateInt reads one persisted replica counter.
func getStateInt(db *sql.DB, key string) (int64, error) {
	var value int64
	err := db.QueryRow(`SELECT value FROM replica_state WHERE key = ?`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return value, err
}

// setStateInt saves one persisted replica counter.
func setStateInt(db *sql.DB, key string, value int64) error {
	_, err := db.Exec(
		`INSERT INTO replica_state(key, value) VALUES(?, ?)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
		key,
		value,
	)
	return err
}

// appendRaftEntry inserts a new replicated log entry.
func appendRaftEntry(db *sql.DB, entry LogEntry) error {
	_, err := db.Exec(
		`INSERT INTO raft_log(idx, term, operation, payload) VALUES (?, ?, ?, ?)`,
		entry.Index,
		entry.Term,
		entry.Operation,
		entry.Payload,
	)
	return err
}

// putRaftEntry appends or replaces one replicated log entry.
func putRaftEntry(db *sql.DB, entry LogEntry) error {
	_, err := db.Exec(
		`INSERT INTO raft_log(idx, term, operation, payload) VALUES (?, ?, ?, ?)
		 ON CONFLICT(idx) DO UPDATE SET term = excluded.term, operation = excluded.operation, payload = excluded.payload`,
		entry.Index,
		entry.Term,
		entry.Operation,
		entry.Payload,
	)
	return err
}

// deleteRaftFrom removes conflicting entries at and after an index.
func deleteRaftFrom(db *sql.DB, index int64) error {
	_, err := db.Exec(`DELETE FROM raft_log WHERE idx >= ?`, index)
	return err
}

// getRaftEntry reads one log entry by index.
func getRaftEntry(db *sql.DB, index int64) (LogEntry, error) {
	var entry LogEntry
	err := db.QueryRow(
		`SELECT idx, term, operation, payload FROM raft_log WHERE idx = ?`,
		index,
	).Scan(&entry.Index, &entry.Term, &entry.Operation, &entry.Payload)
	if errors.Is(err, sql.ErrNoRows) {
		return entry, fmt.Errorf("log entry %d not found", index)
	}
	return entry, err
}

// lastRaftEntry returns the final log entry, or zero values for an empty log.
func lastRaftEntry(db *sql.DB) (LogEntry, error) {
	var entry LogEntry
	err := db.QueryRow(
		`SELECT idx, term, operation, payload FROM raft_log ORDER BY idx DESC LIMIT 1`,
	).Scan(&entry.Index, &entry.Term, &entry.Operation, &entry.Payload)
	if errors.Is(err, sql.ErrNoRows) {
		return LogEntry{}, nil
	}
	return entry, err
}

// listRaftLog returns the full replicated log in index order.
func listRaftLog(db *sql.DB) ([]LogEntry, error) {
	rows, err := db.Query(`SELECT idx, term, operation, payload FROM raft_log ORDER BY idx`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var entries []LogEntry
	for rows.Next() {
		var entry LogEntry
		if err := rows.Scan(&entry.Index, &entry.Term, &entry.Operation, &entry.Payload); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, rows.Err()
}
