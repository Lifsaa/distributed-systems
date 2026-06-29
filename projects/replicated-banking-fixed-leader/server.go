package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/rpc"
	"strings"
	"sync"
)

type ReplicaConfig struct {
	ID         int
	IsLeader   bool
	Peers      map[int]string
	LeaderAddr string
	Term       int64
}

type Bank struct {
	mu           sync.Mutex
	db           *sql.DB
	id           int
	isLeader     bool
	peers        map[int]string
	leaderAddr   string
	term         int64
	commitIndex  int64
	appliedIndex int64
}

type Raft struct {
	bank *Bank
}

// newBank keeps the database handle with a single-node leader default.
func newBank(db *sql.DB) *Bank {
	return newBankWithConfig(db, ReplicaConfig{ID: 1, IsLeader: true, Term: 1})
}

// newBankWithConfig builds one fixed-role replica from command-line config.
func newBankWithConfig(db *sql.DB, cfg ReplicaConfig) *Bank {
	if cfg.Term == 0 {
		cfg.Term = 1
	}
	if cfg.Peers == nil {
		cfg.Peers = map[int]string{}
	}
	commitIndex, _ := getStateInt(db, "commit_index")
	appliedIndex, _ := getStateInt(db, "applied_index")
	return &Bank{
		db:           db,
		id:           cfg.ID,
		isLeader:     cfg.IsLeader,
		peers:        cfg.Peers,
		leaderAddr:   cfg.LeaderAddr,
		term:         cfg.Term,
		commitIndex:  commitIndex,
		appliedIndex: appliedIndex,
	}
}

// serveRPC starts the bank and raft RPC servers.
func serveRPC(addr string, bank *Bank) error {
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
	role := "follower"
	if bank.isLeader {
		role = "leader"
	}
	fmt.Printf("replica %d %s listening on %s\n", bank.id, role, addr)
	server.Accept(l)
	return nil
}

// CheckBalance logs and commits a balance request before answering.
func (b *Bank) CheckBalance(req *AccountRequest, resp *BalanceResponse) error {
	return b.replicateClientOperation(OpCheckBalance, req, resp)
}

// Deposit logs and commits a deposit before applying it.
func (b *Bank) Deposit(req *MoneyRequest, resp *BasicResponse) error {
	return b.replicateClientOperation(OpDeposit, req, resp)
}

// Withdraw logs and commits a withdrawal before applying it.
func (b *Bank) Withdraw(req *MoneyRequest, resp *BasicResponse) error {
	return b.replicateClientOperation(OpWithdraw, req, resp)
}

// Transfer logs and commits a transfer before applying it.
func (b *Bank) Transfer(req *TransferRequest, resp *BasicResponse) error {
	return b.replicateClientOperation(OpTransfer, req, resp)
}

// OpenAccount logs and commits a teller account creation.
func (b *Bank) OpenAccount(req *OpenAccountRequest, resp *AccountResponse) error {
	return b.replicateClientOperation(OpOpenAccount, req, resp)
}

// CloseAccount logs and commits a teller close operation.
func (b *Bank) CloseAccount(req *AccountRequest, resp *BasicResponse) error {
	return b.replicateClientOperation(OpCloseAccount, req, resp)
}

// FreezeAccount logs and commits a teller freeze operation.
func (b *Bank) FreezeAccount(req *AccountRequest, resp *BasicResponse) error {
	return b.replicateClientOperation(OpFreezeAccount, req, resp)
}

// UnfreezeAccount logs and commits a teller unfreeze operation.
func (b *Bank) UnfreezeAccount(req *AccountRequest, resp *BasicResponse) error {
	return b.replicateClientOperation(OpUnfreeze, req, resp)
}

// ApplyPercent logs and commits a teller percent operation.
func (b *Bank) ApplyPercent(req *PercentRequest, resp *BasicResponse) error {
	return b.replicateClientOperation(OpApplyPercent, req, resp)
}

// ChargeFee logs and commits a teller fee operation.
func (b *Bank) ChargeFee(req *MoneyRequest, resp *BasicResponse) error {
	return b.replicateClientOperation(OpChargeFee, req, resp)
}

// AppendEntries lets tests call the follower path without RPC.
func (b *Bank) AppendEntries(req *AppendEntriesRequest, resp *AppendEntriesResponse) error {
	return b.handleAppendEntries(req, resp)
}

// AppendEntries receives replicated log entries from the fixed leader.
func (r *Raft) AppendEntries(req *AppendEntriesRequest, resp *AppendEntriesResponse) error {
	return r.bank.handleAppendEntries(req, resp)
}

// GetLog returns this replica's durable raft log for the tester.
func (r *Raft) GetLog(_ *GetLogRequest, resp *GetLogResponse) error {
	entries, err := listRaftLog(r.bank.db)
	if err != nil {
		resp.Err = err.Error()
		return nil
	}
	resp.OK = true
	resp.Entries = entries
	return nil
}

// GetState returns simple replica state for debugging.
func (r *Raft) GetState(_ *GetStateRequest, resp *GetStateResponse) error {
	r.bank.mu.Lock()
	defer r.bank.mu.Unlock()
	last, err := lastRaftEntry(r.bank.db)
	if err != nil {
		resp.Err = err.Error()
		return nil
	}
	resp.OK = true
	resp.ID = r.bank.id
	resp.Term = r.bank.term
	resp.IsLeader = r.bank.isLeader
	resp.CommitIndex = r.bank.commitIndex
	resp.LastIndex = last.Index
	return nil
}

// replicateClientOperation runs one client request through the leader log.
func (b *Bank) replicateClientOperation(operation string, req any, resp any) error {
	if !b.isLeader {
		setResponseError(resp, b.notLeaderMessage())
		return nil
	}
	payload, err := json.Marshal(req)
	if err != nil {
		setResponseError(resp, err.Error())
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	prev, err := lastRaftEntry(b.db)
	if err != nil {
		setResponseError(resp, err.Error())
		return nil
	}
	entry := LogEntry{
		Index:     prev.Index + 1,
		Term:      b.term,
		Operation: operation,
		Payload:   string(payload),
	}
	if err := appendRaftEntry(b.db, entry); err != nil {
		setResponseError(resp, err.Error())
		return nil
	}
	acks := 1
	for _, addr := range b.peers {
		if b.sendAppendEntries(addr, prev, []LogEntry{entry}, b.commitIndex) {
			acks++
		}
	}
	if acks < b.majority() {
		setResponseError(resp, fmt.Sprintf("only %d replicas acknowledged entry %d", acks, entry.Index))
		return nil
	}
	b.commitIndex = entry.Index
	if err := setStateInt(b.db, "commit_index", b.commitIndex); err != nil {
		setResponseError(resp, err.Error())
		return nil
	}
	if err := b.applyThroughLocked(b.commitIndex, entry, resp); err != nil {
		setResponseError(resp, err.Error())
		return nil
	}
	for _, addr := range b.peers {
		b.sendAppendEntries(addr, entry, nil, b.commitIndex)
	}
	return nil
}

// sendAppendEntries sends one best-effort append entries RPC.
func (b *Bank) sendAppendEntries(addr string, prev LogEntry, entries []LogEntry, leaderCommit int64) bool {
	c, err := rpc.Dial("tcp", addr)
	if err != nil {
		return false
	}
	defer c.Close()
	req := &AppendEntriesRequest{
		Term:         b.term,
		LeaderID:     b.id,
		PrevLogIndex: prev.Index,
		PrevLogTerm:  prev.Term,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}
	var resp AppendEntriesResponse
	if err := c.Call("Raft.AppendEntries", req, &resp); err != nil {
		return false
	}
	return resp.Success
}

// handleAppendEntries appends leader entries and applies committed entries.
func (b *Bank) handleAppendEntries(req *AppendEntriesRequest, resp *AppendEntriesResponse) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	resp.Term = b.term
	if req.Term < b.term {
		resp.Err = "stale term"
		return nil
	}
	b.term = req.Term
	if req.PrevLogIndex > 0 {
		prev, err := getRaftEntry(b.db, req.PrevLogIndex)
		if err != nil || prev.Term != req.PrevLogTerm {
			resp.Err = "previous log entry does not match"
			return nil
		}
	}
	for _, entry := range req.Entries {
		if existing, err := getRaftEntry(b.db, entry.Index); err == nil {
			if existing.Term == entry.Term && existing.Operation == entry.Operation && existing.Payload == entry.Payload {
				continue
			}
			if err := deleteRaftFrom(b.db, entry.Index); err != nil {
				resp.Err = err.Error()
				return nil
			}
		}
		if err := putRaftEntry(b.db, entry); err != nil {
			resp.Err = err.Error()
			return nil
		}
	}
	last, err := lastRaftEntry(b.db)
	if err != nil {
		resp.Err = err.Error()
		return nil
	}
	if req.LeaderCommit > b.commitIndex {
		b.commitIndex = minInt64(req.LeaderCommit, last.Index)
		if err := setStateInt(b.db, "commit_index", b.commitIndex); err != nil {
			resp.Err = err.Error()
			return nil
		}
		if err := b.applyThroughLocked(b.commitIndex, LogEntry{}, nil); err != nil {
			resp.Err = err.Error()
			return nil
		}
	}
	resp.Success = true
	resp.MatchIndex = last.Index
	resp.Term = b.term
	return nil
}

// applyThroughLocked applies unapplied entries through the target index.
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

// applyEntry decodes and applies one committed banking operation.
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

// applyCheckBalance reads an active account inside the committed path.
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

// applyDeposit adds money to an active account.
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

// applyWithdraw removes money from an active account.
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

// applyTransfer moves money between two active accounts.
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

// applyOpenAccount creates a new active account.
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

// applySetStatus changes one account status for teller operations.
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

// applyPercent applies a teller percentage change.
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

// applyFee subtracts a teller fee from a non-closed account.
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

// decodePayload unmarshals one JSON request payload.
func decodePayload(payload string, out any) error {
	return json.Unmarshal([]byte(payload), out)
}

// responseOr returns the caller response or a local throwaway response.
func responseOr(response any, fallback any) any {
	if response == nil {
		return fallback
	}
	return response
}

// setResponseError writes an error onto any banking response type.
func setResponseError(response any, message string) {
	switch resp := response.(type) {
	case *BasicResponse:
		resp.OK = false
		resp.Err = message
	case *AccountResponse:
		resp.OK = false
		resp.Err = message
	case *BalanceResponse:
		resp.OK = false
		resp.Err = message
	}
}

// notLeaderMessage tells clients where the fixed leader lives.
func (b *Bank) notLeaderMessage() string {
	if strings.TrimSpace(b.leaderAddr) == "" {
		return "not leader; send requests to replica1:5001"
	}
	return "not leader; send requests to " + b.leaderAddr
}

// majority returns the ack count needed to commit.
func (b *Bank) majority() int {
	return (len(b.peers)+1)/2 + 1
}

// minInt64 keeps commit indexes from passing the local log.
func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
