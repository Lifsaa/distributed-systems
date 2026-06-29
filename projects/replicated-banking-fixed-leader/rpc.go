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

const (
	OpCheckBalance  = "check_balance"
	OpDeposit       = "deposit"
	OpWithdraw      = "withdraw"
	OpTransfer      = "transfer"
	OpOpenAccount   = "open_account"
	OpCloseAccount  = "close_account"
	OpFreezeAccount = "freeze_account"
	OpUnfreeze      = "unfreeze_account"
	OpApplyPercent  = "apply_percent"
	OpChargeFee     = "charge_fee"
)

type LogEntry struct {
	Index     int64
	Term      int64
	Operation string
	Payload   string
}

type AppendEntriesRequest struct {
	Term         int64
	LeaderID     int
	PrevLogIndex int64
	PrevLogTerm  int64
	Entries      []LogEntry
	LeaderCommit int64
}

type AppendEntriesResponse struct {
	Term       int64
	Success    bool
	MatchIndex int64
	Err        string
}

type GetLogRequest struct{}

type GetLogResponse struct {
	OK      bool
	Err     string
	Entries []LogEntry
}

type GetStateRequest struct{}

type GetStateResponse struct {
	OK          bool
	Err         string
	ID          int
	Term        int64
	IsLeader    bool
	CommitIndex int64
	LastIndex   int64
}
