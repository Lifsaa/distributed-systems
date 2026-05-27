package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

/*
Purpose:

	Opens one SQLite database and prepares all banking and Raft tables.

Args:

	path: Database file path for this replica.

Returns:

	Open database handle, or an error if SQLite setup fails.
*/
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

/*
Purpose:

	Creates the banking, operation log, Raft log, and durable replica state tables.

Args:

	db: SQLite handle for this replica.

Returns:

	Nil when schema setup succeeds.

Rationale:

	The ALTER statement keeps older PA7-style databases usable by adding the PA8
	request id column if it is missing.
*/
func createTables(db *sql.DB) error {
	if _, err := db.Exec(`
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
	request_id INTEGER NOT NULL DEFAULT 0,
	operation TEXT NOT NULL,
	payload TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS replica_state (
	key TEXT PRIMARY KEY,
	value INTEGER NOT NULL
);
`); err != nil {
		return err
	}
	_, _ = db.Exec(`ALTER TABLE raft_log ADD COLUMN request_id INTEGER NOT NULL DEFAULT 0`)
	return nil
}

/*
Purpose:

	Starts one SQLite transaction for a committed banking operation.
*/
func begin(db *sql.DB) (*sql.Tx, error) {
	return db.BeginTx(context.Background(), nil)
}

func logOperation(tx *sql.Tx, actor, op, details string) error {
	_, err := tx.Exec(
		`INSERT INTO operation_log(created_at, actor_type, operation, details) VALUES (?, ?, ?, ?)`,
		time.Now().Format(time.RFC3339Nano), actor, op, details,
	)
	return err
}

func getAccount(tx *sql.Tx, id int64) (Account, error) {
	var a Account
	err := tx.QueryRow(`SELECT id, username, balance, status FROM accounts WHERE id = ?`, id).
		Scan(&a.ID, &a.Username, &a.Balance, &a.Status)
	if errors.Is(err, sql.ErrNoRows) {
		return a, fmt.Errorf("account not found")
	}
	return a, err
}

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

func needNotClosed(a Account) error {
	if a.Status == StatusClosed {
		return fmt.Errorf("account is closed")
	}
	return nil
}

func logCount(db *sql.DB) (int, error) {
	var n int
	err := db.QueryRow(`SELECT COUNT(*) FROM operation_log`).Scan(&n)
	return n, err
}

/*
Purpose:

	Reads one durable Raft state value from replica_state.

Args:

	db: SQLite handle for this replica.
	key: State key such as "current_term", "voted_for", or "commit_index".

Returns:

	Stored integer value. Missing keys return zero with nil error.
*/
func getStateInt(db *sql.DB, key string) (int64, error) {
	var value int64
	err := db.QueryRow(`SELECT value FROM replica_state WHERE key = ?`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return value, err
}

/*
Purpose:

	Inserts or updates one durable Raft state value.

Args:

	db: SQLite handle for this replica.
	key: State key to write.
	value: Integer value to store.
*/
func setStateInt(db *sql.DB, key string, value int64) error {
	_, err := db.Exec(
		`INSERT INTO replica_state(key, value) VALUES(?, ?)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
		key,
		value,
	)
	return err
}

/*
Purpose:

	Inserts a newly created leader log entry.
*/
func appendRaftEntry(db *sql.DB, entry LogEntry) error {
	_, err := db.Exec(
		`INSERT INTO raft_log(idx, term, request_id, operation, payload) VALUES (?, ?, ?, ?, ?)`,
		entry.Index,
		entry.Term,
		entry.RequestID,
		entry.Operation,
		entry.Payload,
	)
	return err
}

/*
Purpose:

	Inserts or replaces a Raft log entry at a specific index.

Rationale:

	Followers use this during log repair when the leader overwrites conflicting
	uncommitted entries.
*/
func putRaftEntry(db *sql.DB, entry LogEntry) error {
	_, err := db.Exec(
		`INSERT INTO raft_log(idx, term, request_id, operation, payload) VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(idx) DO UPDATE SET term = excluded.term, request_id = excluded.request_id, operation = excluded.operation, payload = excluded.payload`,
		entry.Index,
		entry.Term,
		entry.RequestID,
		entry.Operation,
		entry.Payload,
	)
	return err
}

/*
Purpose:

	Deletes a conflicting log suffix starting at index.
*/
func deleteRaftFrom(db *sql.DB, index int64) error {
	_, err := db.Exec(`DELETE FROM raft_log WHERE idx >= ?`, index)
	return err
}

/*
Purpose:

	Reads one Raft log entry by index.
*/
func getRaftEntry(db *sql.DB, index int64) (LogEntry, error) {
	var entry LogEntry
	err := db.QueryRow(
		`SELECT idx, term, request_id, operation, payload FROM raft_log WHERE idx = ?`,
		index,
	).Scan(&entry.Index, &entry.Term, &entry.RequestID, &entry.Operation, &entry.Payload)
	if errors.Is(err, sql.ErrNoRows) {
		return entry, fmt.Errorf("log entry %d not found", index)
	}
	return entry, err
}

/*
Purpose:

	Reads the last local Raft log entry.

Returns:

	Zero-value LogEntry when the log is empty.
*/
func lastRaftEntry(db *sql.DB) (LogEntry, error) {
	var entry LogEntry
	err := db.QueryRow(
		`SELECT idx, term, request_id, operation, payload FROM raft_log ORDER BY idx DESC LIMIT 1`,
	).Scan(&entry.Index, &entry.Term, &entry.RequestID, &entry.Operation, &entry.Payload)
	if errors.Is(err, sql.ErrNoRows) {
		return LogEntry{}, nil
	}
	return entry, err
}

/*
Purpose:

	Reads the full replicated log in index order.

Returns:

	All durable log entries, or an error from SQLite.
*/
func listRaftLog(db *sql.DB) ([]LogEntry, error) {
	rows, err := db.Query(`SELECT idx, term, request_id, operation, payload FROM raft_log ORDER BY idx`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var entries []LogEntry
	for rows.Next() {
		var entry LogEntry
		if err := rows.Scan(&entry.Index, &entry.Term, &entry.RequestID, &entry.Operation, &entry.Payload); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, rows.Err()
}
