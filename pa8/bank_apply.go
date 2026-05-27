package main

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
)

func (b *Bank) CheckBalance(req *AccountRequest, resp *BalanceResponse) error {
	return b.replicateClientOperation(OpCheckBalance, req, resp)
}

func (b *Bank) Deposit(req *MoneyRequest, resp *BasicResponse) error {
	return b.replicateClientOperation(OpDeposit, req, resp)
}

func (b *Bank) Withdraw(req *MoneyRequest, resp *BasicResponse) error {
	return b.replicateClientOperation(OpWithdraw, req, resp)
}

func (b *Bank) Transfer(req *TransferRequest, resp *BasicResponse) error {
	return b.replicateClientOperation(OpTransfer, req, resp)
}

func (b *Bank) OpenAccount(req *OpenAccountRequest, resp *AccountResponse) error {
	return b.replicateClientOperation(OpOpenAccount, req, resp)
}

func (b *Bank) CloseAccount(req *AccountRequest, resp *BasicResponse) error {
	return b.replicateClientOperation(OpCloseAccount, req, resp)
}

func (b *Bank) FreezeAccount(req *AccountRequest, resp *BasicResponse) error {
	return b.replicateClientOperation(OpFreezeAccount, req, resp)
}

func (b *Bank) UnfreezeAccount(req *AccountRequest, resp *BasicResponse) error {
	return b.replicateClientOperation(OpUnfreeze, req, resp)
}

func (b *Bank) ApplyPercent(req *PercentRequest, resp *BasicResponse) error {
	return b.replicateClientOperation(OpApplyPercent, req, resp)
}

func (b *Bank) ChargeFee(req *MoneyRequest, resp *BasicResponse) error {
	return b.replicateClientOperation(OpChargeFee, req, resp)
}

/*
Purpose:

	Applies every committed but unapplied log entry through the target index.

Args:

	target: Highest committed log index that should be reflected in SQLite.
	responseEntry: Entry whose result should be written into the client response.
	response: Optional client response object for the current leader request.

Returns:

	Nil after all entries are applied, or the first decode/apply/persistence error.

Rationale:

	Raft separates committing an entry from applying it. This keeps the bank state
	aligned with commitIndex and makes follower catch-up deterministic.
*/
func (b *Bank) applyThroughLocked(target int64, responseEntry LogEntry, response any) error {
	for b.appliedIndex < target {
		nextIndex := b.appliedIndex + 1
		entry, err := getRaftEntry(b.db, nextIndex)
		if err != nil {
			return err
		}
		var out any
		if response != nil && responseEntry.Index == entry.Index {
			out = response
		}
		if err := b.applyEntry(entry, out); err != nil {
			return err
		}
		b.appliedIndex = nextIndex
		if err := setStateInt(b.db, "applied_index", b.appliedIndex); err != nil {
			return err
		}
	}
	return nil
}

/*
Purpose:

	Decodes one committed Raft log entry and dispatches it to the matching banking
	operation.

Args:

	entry: Committed log entry containing operation name and JSON payload.
	response: Optional response object for the client request that caused entry.

Returns:

	Nil when the operation was decoded and applied. Unknown operations or invalid
	payloads return an error.
*/
func (b *Bank) applyEntry(entry LogEntry, response any) error {
	switch entry.Operation {
	case OpCheckBalance:
		var req AccountRequest
		if err := decodePayload(entry.Payload, &req); err != nil {
			return err
		}
		resp := responseOr(response, &BalanceResponse{}).(*BalanceResponse)
		return b.applyCheckBalance(&req, resp)
	case OpDeposit:
		var req MoneyRequest
		if err := decodePayload(entry.Payload, &req); err != nil {
			return err
		}
		resp := responseOr(response, &BasicResponse{}).(*BasicResponse)
		return b.applyDeposit(&req, resp)
	case OpWithdraw:
		var req MoneyRequest
		if err := decodePayload(entry.Payload, &req); err != nil {
			return err
		}
		resp := responseOr(response, &BasicResponse{}).(*BasicResponse)
		return b.applyWithdraw(&req, resp)
	case OpTransfer:
		var req TransferRequest
		if err := decodePayload(entry.Payload, &req); err != nil {
			return err
		}
		resp := responseOr(response, &BasicResponse{}).(*BasicResponse)
		return b.applyTransfer(&req, resp)
	case OpOpenAccount:
		var req OpenAccountRequest
		if err := decodePayload(entry.Payload, &req); err != nil {
			return err
		}
		resp := responseOr(response, &AccountResponse{}).(*AccountResponse)
		return b.applyOpenAccount(&req, resp)
	case OpCloseAccount, OpFreezeAccount, OpUnfreeze:
		var req AccountRequest
		if err := decodePayload(entry.Payload, &req); err != nil {
			return err
		}
		resp := responseOr(response, &BasicResponse{}).(*BasicResponse)
		status := map[string]string{
			OpCloseAccount:  StatusClosed,
			OpFreezeAccount: StatusFrozen,
			OpUnfreeze:      StatusActive,
		}[entry.Operation]
		b.applySetStatus(req.AccountID, status, entry.Operation, resp)
		return nil
	case OpApplyPercent:
		var req PercentRequest
		if err := decodePayload(entry.Payload, &req); err != nil {
			return err
		}
		resp := responseOr(response, &BasicResponse{}).(*BasicResponse)
		return b.applyPercent(&req, resp)
	case OpChargeFee:
		var req MoneyRequest
		if err := decodePayload(entry.Payload, &req); err != nil {
			return err
		}
		resp := responseOr(response, &BasicResponse{}).(*BasicResponse)
		return b.applyFee(&req, resp)
	default:
		return fmt.Errorf("unknown operation %q", entry.Operation)
	}
}

/*
Purpose:

	Applies a balance read after it has been committed in the Raft log.

Rationale:

	PA8 logs reads as well as writes so every replica processes the same request
	order.
*/
func (b *Bank) applyCheckBalance(req *AccountRequest, resp *BalanceResponse) error {
	tx, err := begin(b.db)
	if err != nil {
		resp.Err = err.Error()
		return nil
	}
	defer tx.Rollback()
	if err := logOperation(tx, "customer", OpCheckBalance, fmt.Sprintf("account=%d", req.AccountID)); err != nil {
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

func (b *Bank) applyDeposit(req *MoneyRequest, resp *BasicResponse) error {
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
	if err := logOperation(tx, "customer", OpDeposit, fmt.Sprintf("account=%d amount=%d", req.AccountID, req.Amount)); err != nil {
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
	if _, err := tx.Exec(`UPDATE accounts SET balance = balance + ? WHERE id = ?`, req.Amount, req.AccountID); err != nil {
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

func (b *Bank) applyWithdraw(req *MoneyRequest, resp *BasicResponse) error {
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
	if err := logOperation(tx, "customer", OpWithdraw, fmt.Sprintf("account=%d amount=%d", req.AccountID, req.Amount)); err != nil {
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
	if _, err := tx.Exec(`UPDATE accounts SET balance = balance - ? WHERE id = ?`, req.Amount, req.AccountID); err != nil {
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

func (b *Bank) applyTransfer(req *TransferRequest, resp *BasicResponse) error {
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
	if err := logOperation(tx, "customer", OpTransfer, fmt.Sprintf("from=%d to=%d amount=%d", req.FromAccountID, req.ToAccountID, req.Amount)); err != nil {
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
	if _, err := tx.Exec(`UPDATE accounts SET balance = balance - ? WHERE id = ?`, req.Amount, req.FromAccountID); err != nil {
		resp.Err = err.Error()
		return nil
	}
	if _, err := tx.Exec(`UPDATE accounts SET balance = balance + ? WHERE id = ?`, req.Amount, req.ToAccountID); err != nil {
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

func (b *Bank) applyOpenAccount(req *OpenAccountRequest, resp *AccountResponse) error {
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
	if err := logOperation(tx, "teller", OpOpenAccount, fmt.Sprintf("username=%s balance=%d", req.Username, req.InitialBalance)); err != nil {
		resp.Err = err.Error()
		return nil
	}
	res, err := tx.Exec(`INSERT INTO accounts(username, balance, status) VALUES (?, ?, ?)`, req.Username, req.InitialBalance, StatusActive)
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
	resp.Account = Account{ID: id, Username: req.Username, Balance: req.InitialBalance, Status: StatusActive}
	return nil
}

/*
Purpose:

	Applies an account status change for close, freeze, or unfreeze operations.

Args:

	id: Account id being changed.
	status: New account status.
	op: Operation name recorded in the banking audit log.
	resp: Response returned to the client.
*/
func (b *Bank) applySetStatus(id int64, status, op string, resp *BasicResponse) {
	tx, err := begin(b.db)
	if err != nil {
		resp.Err = err.Error()
		return
	}
	defer tx.Rollback()
	if err := logOperation(tx, "teller", op, fmt.Sprintf("account=%d status=%s", id, status)); err != nil {
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

func (b *Bank) applyPercent(req *PercentRequest, resp *BasicResponse) error {
	tx, err := begin(b.db)
	if err != nil {
		resp.Err = err.Error()
		return nil
	}
	defer tx.Rollback()
	if err := logOperation(tx, "teller", OpApplyPercent, fmt.Sprintf("account=%d percent=%.2f", req.AccountID, req.Percent)); err != nil {
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
	if _, err := tx.Exec(`UPDATE accounts SET balance = balance + ? WHERE id = ?`, change, req.AccountID); err != nil {
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

func (b *Bank) applyFee(req *MoneyRequest, resp *BasicResponse) error {
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
	if err := logOperation(tx, "teller", OpChargeFee, fmt.Sprintf("account=%d fee=%d", req.AccountID, req.Amount)); err != nil {
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
	if _, err := tx.Exec(`UPDATE accounts SET balance = balance - ? WHERE id = ?`, req.Amount, req.AccountID); err != nil {
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

/*
Purpose:

	Decodes a JSON log payload into the concrete banking request type.
*/
func decodePayload(payload string, out any) error {
	return json.Unmarshal([]byte(payload), out)
}

/*
Purpose:

	Uses the caller's response object when present, or a throwaway response while
	a follower replays committed entries.
*/
func responseOr(response any, fallback any) any {
	if response == nil {
		return fallback
	}
	return response
}

/*
Purpose:

	Writes an error onto any banking response type and attaches a leader hint when
	one is known.

Rationale:

	Clients may contact followers, so the response needs enough information to
	retry against the current leader.
*/
func setResponseError(response any, message string, leaderID int, leaderAddr string) {
	switch resp := response.(type) {
	case *BasicResponse:
		resp.OK = false
		resp.Err = message
		resp.LeaderID = leaderID
		resp.LeaderAddr = leaderAddr
	case *AccountResponse:
		resp.OK = false
		resp.Err = message
		resp.LeaderID = leaderID
		resp.LeaderAddr = leaderAddr
	case *BalanceResponse:
		resp.OK = false
		resp.Err = message
		resp.LeaderID = leaderID
		resp.LeaderAddr = leaderAddr
	}
}

/*
Purpose:

	Builds the best leader address hint this follower currently knows.
*/
func (b *Bank) notLeaderMessageLocked() string {
	if strings.TrimSpace(b.leaderAddr) != "" {
		return b.leaderAddr
	}
	if b.leaderID > 0 {
		if addr := b.peers[b.leaderID]; addr != "" {
			return addr
		}
	}
	return "unknown"
}

/*
Purpose:

	Returns the number of acknowledgements required to commit an entry.

Rationale:

	With five PA8 replicas, majority is three. The formula also keeps local tests
	with fewer peers working.
*/
func (b *Bank) majority() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.majorityLocked()
}

func (b *Bank) majorityLocked() int {
	return (len(b.peers)+1)/2 + 1
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
