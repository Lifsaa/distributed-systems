package main

const (
	StatusActive = "active"
	StatusFrozen = "frozen"
	StatusClosed = "closed"
)

type Account struct {
	ID       int64
	Username string
	Balance  int64
	Status   string
}

type BasicResponse struct {
	OK  bool
	Err string
}

type AccountResponse struct {
	OK      bool
	Err     string
	Account Account
}

type BalanceResponse struct {
	OK      bool
	Err     string
	Balance int64
	Status  string
}

type OpenAccountRequest struct {
	Username       string
	InitialBalance int64
}

type AccountRequest struct {
	AccountID int64
}

type MoneyRequest struct {
	AccountID int64
	Amount    int64
}

type TransferRequest struct {
	FromAccountID int64
	ToAccountID   int64
	Amount        int64
}

type PercentRequest struct {
	AccountID int64
	Percent   float64
}
