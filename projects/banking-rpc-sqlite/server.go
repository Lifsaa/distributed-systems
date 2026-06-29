package main

import (
	"database/sql"
	"fmt"
	"math"
	"net"
	"net/rpc"
)

type Bank struct {
	db *sql.DB
}

// newBank keeps the database handle with the RPC server.
func newBank(db *sql.DB) *Bank {
	return &Bank{db: db}
}

// serveRPC starts the bank RPC server.
func serveRPC(addr string, bank *Bank) error {
	if err := rpc.Register(bank); err != nil {
		return err
	}
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	fmt.Printf("bank server listening on %s\n", addr)
	rpc.Accept(l)
	return nil
}

// CheckBalance lets a customer see an active account balance.
func (b *Bank) CheckBalance(req *AccountRequest, resp *BalanceResponse) error {
	tx, err := begin(b.db)
	if err != nil {
		resp.Err = err.Error()
		return nil
	}
	defer tx.Rollback()
	if err := logOperation(
		tx,
		"customer",
		"check_balance",
		fmt.Sprintf("account=%d", req.AccountID),
	); err != nil {
		resp.Err = err.Error()
		return nil
	}
	a, err := getAccount(tx, req.AccountID)
	if err != nil {
		resp.Err = err.Error()
		return nil
	}
	if err := needActive(a); err != nil {
		resp.Err = err.Error()
		return nil
	}
	if err := tx.Commit(); err != nil {
		resp.Err = err.Error()
		return nil
	}
	resp.OK = true
	resp.Balance = a.Balance
	resp.Status = a.Status
	return nil
}

// Deposit adds money to an active customer account.
func (b *Bank) Deposit(req *MoneyRequest, resp *BasicResponse) error {
	if req.Amount <= 0 {
		resp.Err = "amount must be positive"
		return nil
	}
	tx, err := begin(b.db)
	if err != nil {
		resp.Err = err.Error()
		return nil
	}
	defer tx.Rollback()
	if err := logOperation(
		tx,
		"customer",
		"deposit",
		fmt.Sprintf("account=%d amount=%d", req.AccountID, req.Amount),
	); err != nil {
		resp.Err = err.Error()
		return nil
	}
	a, err := getAccount(tx, req.AccountID)
	if err != nil {
		resp.Err = err.Error()
		return nil
	}
	if err := needActive(a); err != nil {
		resp.Err = err.Error()
		return nil
	}
	if _, err := tx.Exec(
		`UPDATE accounts SET balance = balance + ? WHERE id = ?`,
		req.Amount,
		req.AccountID,
	); err != nil {
		resp.Err = err.Error()
		return nil
	}
	if err := tx.Commit(); err != nil {
		resp.Err = err.Error()
		return nil
	}
	resp.OK = true
	return nil
}

// Withdraw removes money from an active account if funds exist.
func (b *Bank) Withdraw(req *MoneyRequest, resp *BasicResponse) error {
	if req.Amount <= 0 {
		resp.Err = "amount must be positive"
		return nil
	}
	tx, err := begin(b.db)
	if err != nil {
		resp.Err = err.Error()
		return nil
	}
	defer tx.Rollback()
	if err := logOperation(
		tx,
		"customer",
		"withdraw",
		fmt.Sprintf("account=%d amount=%d", req.AccountID, req.Amount),
	); err != nil {
		resp.Err = err.Error()
		return nil
	}
	a, err := getAccount(tx, req.AccountID)
	if err != nil {
		resp.Err = err.Error()
		return nil
	}
	if err := needActive(a); err != nil {
		resp.Err = err.Error()
		return nil
	}
	if a.Balance < req.Amount {
		resp.Err = "insufficient funds"
		return nil
	}
	if _, err := tx.Exec(
		`UPDATE accounts SET balance = balance - ? WHERE id = ?`,
		req.Amount,
		req.AccountID,
	); err != nil {
		resp.Err = err.Error()
		return nil
	}
	if err := tx.Commit(); err != nil {
		resp.Err = err.Error()
		return nil
	}
	resp.OK = true
	return nil
}

// Transfer moves money between two active accounts.
func (b *Bank) Transfer(req *TransferRequest, resp *BasicResponse) error {
	if req.Amount <= 0 {
		resp.Err = "amount must be positive"
		return nil
	}
	if req.FromAccountID == req.ToAccountID {
		resp.Err = "cannot transfer to same account"
		return nil
	}
	tx, err := begin(b.db)
	if err != nil {
		resp.Err = err.Error()
		return nil
	}
	defer tx.Rollback()
	if err := logOperation(
		tx,
		"customer",
		"transfer",
		fmt.Sprintf(
			"from=%d to=%d amount=%d",
			req.FromAccountID,
			req.ToAccountID,
			req.Amount,
		),
	); err != nil {
		resp.Err = err.Error()
		return nil
	}
	from, err := getAccount(tx, req.FromAccountID)
	if err != nil {
		resp.Err = err.Error()
		return nil
	}
	to, err := getAccount(tx, req.ToAccountID)
	if err != nil {
		resp.Err = err.Error()
		return nil
	}
	if err := needActive(from); err != nil {
		resp.Err = err.Error()
		return nil
	}
	if err := needActive(to); err != nil {
		resp.Err = err.Error()
		return nil
	}
	if from.Balance < req.Amount {
		resp.Err = "insufficient funds"
		return nil
	}
	if _, err := tx.Exec(
		`UPDATE accounts SET balance = balance - ? WHERE id = ?`,
		req.Amount,
		req.FromAccountID,
	); err != nil {
		resp.Err = err.Error()
		return nil
	}
	if _, err := tx.Exec(
		`UPDATE accounts SET balance = balance + ? WHERE id = ?`,
		req.Amount,
		req.ToAccountID,
	); err != nil {
		resp.Err = err.Error()
		return nil
	}
	if err := tx.Commit(); err != nil {
		resp.Err = err.Error()
		return nil
	}
	resp.OK = true
	return nil
}

// OpenAccount lets a teller create a new active account.
func (b *Bank) OpenAccount(req *OpenAccountRequest, resp *AccountResponse) error {
	if req.Username == "" {
		resp.Err = "username is required"
		return nil
	}
	if req.InitialBalance < 0 {
		resp.Err = "initial balance cannot be negative"
		return nil
	}
	tx, err := begin(b.db)
	if err != nil {
		resp.Err = err.Error()
		return nil
	}
	defer tx.Rollback()
	if err := logOperation(
		tx,
		"teller",
		"open_account",
		fmt.Sprintf("username=%s balance=%d", req.Username, req.InitialBalance),
	); err != nil {
		resp.Err = err.Error()
		return nil
	}
	res, err := tx.Exec(
		`INSERT INTO accounts(username, balance, status) VALUES (?, ?, ?)`,
		req.Username,
		req.InitialBalance,
		StatusActive,
	)
	if err != nil {
		resp.Err = err.Error()
		return nil
	}
	id, err := res.LastInsertId()
	if err != nil {
		resp.Err = err.Error()
		return nil
	}
	if err := tx.Commit(); err != nil {
		resp.Err = err.Error()
		return nil
	}
	resp.OK = true
	resp.Account = Account{
		ID:       id,
		Username: req.Username,
		Balance:  req.InitialBalance,
		Status:   StatusActive,
	}
	return nil
}

// CloseAccount marks an account closed.
func (b *Bank) CloseAccount(req *AccountRequest, resp *BasicResponse) error {
	b.setStatus(req.AccountID, StatusClosed, "close_account", resp)
	return nil
}

// FreezeAccount marks an account frozen.
func (b *Bank) FreezeAccount(req *AccountRequest, resp *BasicResponse) error {
	b.setStatus(req.AccountID, StatusFrozen, "freeze_account", resp)
	return nil
}

// UnfreezeAccount marks an account active again.
func (b *Bank) UnfreezeAccount(req *AccountRequest, resp *BasicResponse) error {
	b.setStatus(req.AccountID, StatusActive, "unfreeze_account", resp)
	return nil
}

// setStatus handles the teller status changes.
func (b *Bank) setStatus(id int64, status, op string, resp *BasicResponse) {
	tx, err := begin(b.db)
	if err != nil {
		resp.Err = err.Error()
		return
	}
	defer tx.Rollback()
	if err := logOperation(
		tx,
		"teller",
		op,
		fmt.Sprintf("account=%d status=%s", id, status),
	); err != nil {
		resp.Err = err.Error()
		return
	}
	a, err := getAccount(tx, id)
	if err != nil {
		resp.Err = err.Error()
		return
	}
	if a.Status == StatusClosed {
		resp.Err = "account is closed"
		return
	}
	if _, err := tx.Exec(`UPDATE accounts SET status = ? WHERE id = ?`, status, id); err != nil {
		resp.Err = err.Error()
		return
	}
	if err := tx.Commit(); err != nil {
		resp.Err = err.Error()
		return
	}
	resp.OK = true
}

// ApplyPercent applies a teller bonus or interest percentage.
func (b *Bank) ApplyPercent(req *PercentRequest, resp *BasicResponse) error {
	tx, err := begin(b.db)
	if err != nil {
		resp.Err = err.Error()
		return nil
	}
	defer tx.Rollback()
	if err := logOperation(
		tx,
		"teller",
		"apply_percent",
		fmt.Sprintf("account=%d percent=%.2f", req.AccountID, req.Percent),
	); err != nil {
		resp.Err = err.Error()
		return nil
	}
	a, err := getAccount(tx, req.AccountID)
	if err != nil {
		resp.Err = err.Error()
		return nil
	}
	if err := needNotClosed(a); err != nil {
		resp.Err = err.Error()
		return nil
	}
	change := int64(math.Round(float64(a.Balance) * req.Percent / 100.0))
	if _, err := tx.Exec(
		`UPDATE accounts SET balance = balance + ? WHERE id = ?`,
		change,
		req.AccountID,
	); err != nil {
		resp.Err = err.Error()
		return nil
	}
	if err := tx.Commit(); err != nil {
		resp.Err = err.Error()
		return nil
	}
	resp.OK = true
	return nil
}

// ChargeFee subtracts a positive fee from a non-closed account.
func (b *Bank) ChargeFee(req *MoneyRequest, resp *BasicResponse) error {
	if req.Amount <= 0 {
		resp.Err = "fee must be positive"
		return nil
	}
	tx, err := begin(b.db)
	if err != nil {
		resp.Err = err.Error()
		return nil
	}
	defer tx.Rollback()
	if err := logOperation(
		tx,
		"teller",
		"charge_fee",
		fmt.Sprintf("account=%d fee=%d", req.AccountID, req.Amount),
	); err != nil {
		resp.Err = err.Error()
		return nil
	}
	a, err := getAccount(tx, req.AccountID)
	if err != nil {
		resp.Err = err.Error()
		return nil
	}
	if err := needNotClosed(a); err != nil {
		resp.Err = err.Error()
		return nil
	}
	if a.Balance < req.Amount {
		resp.Err = "insufficient funds"
		return nil
	}
	if _, err := tx.Exec(
		`UPDATE accounts SET balance = balance - ? WHERE id = ?`,
		req.Amount,
		req.AccountID,
	); err != nil {
		resp.Err = err.Error()
		return nil
	}
	if err := tx.Commit(); err != nil {
		resp.Err = err.Error()
		return nil
	}
	resp.OK = true
	return nil
}
