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
