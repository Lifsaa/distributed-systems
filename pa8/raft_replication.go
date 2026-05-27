package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

/*
Purpose:

	Sends periodic heartbeats while this replica is the leader.

Rationale:

	Heartbeats keep followers from starting elections and also give restarted
	followers a chance to catch up even when no new client requests arrive.
*/
func (b *Bank) heartbeatLoop() {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-b.stopCh:
			return
		case <-ticker.C:
			b.mu.Lock()
			isLeader := b.role == RoleLeader
			b.mu.Unlock()
			if isLeader {
				b.broadcastHeartbeat()
			}
		}
	}
}

func (b *Bank) AppendEntries(req *AppendEntriesRequest, resp *AppendEntriesResponse) error {
	return b.handleAppendEntries(req, resp)
}

func (b *Bank) RequestVote(req *RequestVoteRequest, resp *RequestVoteResponse) error {
	return b.handleRequestVote(req, resp)
}

func (r *Raft) AppendEntries(req *AppendEntriesRequest, resp *AppendEntriesResponse) error {
	return r.bank.handleAppendEntries(req, resp)
}

func (r *Raft) RequestVote(req *RequestVoteRequest, resp *RequestVoteResponse) error {
	return r.bank.handleRequestVote(req, resp)
}

/*
Purpose:

	Returns this replica's durable Raft log to the tester.

Args:

	resp: Filled with all log entries or an error message.

Rationale:

	The assignment verifies correctness by comparing every replica's final log.
*/
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

/*
Purpose:

	Returns current role, term, leader hint, and log indexes for inspection.

Args:

	resp: Filled with this replica's observable Raft state.
*/
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
	resp.Role = r.bank.role
	resp.IsLeader = r.bank.role == RoleLeader
	resp.LeaderID = r.bank.leaderID
	resp.LeaderAddr = r.bank.leaderAddr
	resp.CommitIndex = r.bank.commitIndex
	resp.LastIndex = last.Index
	return nil
}

/*
Purpose:

	Handles one client banking request by appending it to the leader log,
	replicating it to a majority, committing it, and applying it to SQLite.

Args:

	operation: Banking operation name stored in the Raft log.
	req: Banking request payload that is encoded into the log entry.
	resp: Banking response object returned to the client.

Returns:

	Nil for RPC transport success. Banking or Raft errors are written into resp.

Rationale:

	All banking operations, including reads, go through the replicated log so
	every server observes the same ordered history.
*/
func (b *Bank) replicateClientOperation(operation string, req any, resp any) error {
	payload, err := json.Marshal(req)
	if err != nil {
		setResponseError(resp, err.Error(), 0, "")
		return nil
	}

	b.mu.Lock()
	if b.role != RoleLeader {
		leaderID, leaderAddr := b.leaderID, b.leaderAddr
		if leaderAddr == "" {
			leaderAddr = b.notLeaderMessageLocked()
		}
		b.mu.Unlock()
		setResponseError(resp, "not leader; send requests to "+leaderAddr, leaderID, leaderAddr)
		return nil
	}
	requestID := b.nextRequestID
	b.nextRequestID++
	_ = setStateInt(b.db, "next_request_id", b.nextRequestID)
	b.logLeaderReceivesRequest(requestID)
	prev, err := lastRaftEntry(b.db)
	if err != nil {
		b.mu.Unlock()
		setResponseError(resp, err.Error(), 0, "")
		return nil
	}
	entry := LogEntry{
		Index:     prev.Index + 1,
		Term:      b.term,
		RequestID: requestID,
		Operation: operation,
		Payload:   string(payload),
	}
	if err := appendRaftEntry(b.db, entry); err != nil {
		b.mu.Unlock()
		setResponseError(resp, err.Error(), 0, "")
		return nil
	}
	b.matchIndex[b.id] = entry.Index
	b.logLeaderAddsRequest(requestID, entry.Index)
	peers := copyPeers(b.peers)
	b.mu.Unlock()

	acks := 1
	for peerID := range peers {
		if b.replicateEntryToPeer(peerID, entry) {
			acks++
		}
	}
	if acks < b.majority() {
		setResponseError(resp, fmt.Sprintf("only %d replicas acknowledged entry %d", acks, entry.Index), b.id, b.selfAddr)
		return nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.role != RoleLeader {
		setResponseError(resp, "leader changed before commit", b.leaderID, b.leaderAddr)
		return nil
	}
	b.commitIndex = entry.Index
	if err := setStateInt(b.db, "commit_index", b.commitIndex); err != nil {
		setResponseError(resp, err.Error(), b.id, b.selfAddr)
		return nil
	}
	if err := b.applyThroughLocked(b.commitIndex, entry, resp); err != nil {
		setResponseError(resp, err.Error(), b.id, b.selfAddr)
		return nil
	}
	go b.broadcastHeartbeat()
	return nil
}

/*
Purpose:

	Sends one target log entry, and any missing earlier entries, to a follower.

Args:

	peerID: Replica id of the follower being updated.
	entry: Latest entry the leader wants the follower to contain.

Returns:

	True when the follower acknowledges the append; false when it cannot be
	updated in this attempt.

Rationale:

	Backing up nextIndex on rejection is the simple Raft repair path for followers
	that missed entries or have conflicting entries.
*/
func (b *Bank) replicateEntryToPeer(peerID int, entry LogEntry) bool {
	for attempts := 0; attempts < int(entry.Index)+2; attempts++ {
		req, ok := b.buildAppendRequest(peerID, entry)
		if !ok {
			return false
		}
		b.logLeaderSendsRequest(entry.RequestID, peerID)
		var resp AppendEntriesResponse
		err := callRPC(b.peers[peerID], "Raft.AppendEntries", req, &resp)
		if err == nil && resp.Success {
			b.mu.Lock()
			b.nextIndex[peerID] = resp.MatchIndex + 1
			b.matchIndex[peerID] = resp.MatchIndex
			b.mu.Unlock()
			b.logLeaderReceivesAck(peerID, entry.RequestID)
			return true
		}
		if err == nil && resp.Term > req.Term {
			b.mu.Lock()
			b.becomeFollowerLocked(resp.Term, 0, "")
			b.mu.Unlock()
			return false
		}
		b.mu.Lock()
		if b.nextIndex[peerID] > 1 {
			b.nextIndex[peerID]--
		} else {
			b.mu.Unlock()
			return false
		}
		b.mu.Unlock()
	}
	return false
}

/*
Purpose:

	Builds an AppendEntries request for a follower from its next missing index.

Args:

	peerID: Replica id of the follower.
	target: Last log entry that should be included in this request.

Returns:

	The request and true when the leader can build a valid log slice. Returns
	false if this server is no longer leader or the local log is inconsistent.
*/
func (b *Bank) buildAppendRequest(peerID int, target LogEntry) (*AppendEntriesRequest, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.role != RoleLeader {
		return nil, false
	}
	next := b.nextIndex[peerID]
	if next == 0 || next > target.Index {
		next = target.Index
	}
	prevIndex := next - 1
	var prevTerm int64
	if prevIndex > 0 {
		prev, err := getRaftEntry(b.db, prevIndex)
		if err != nil {
			return nil, false
		}
		prevTerm = prev.Term
	}
	entries, err := listRaftLogRange(b.db, next, target.Index)
	if err != nil {
		return nil, false
	}
	return &AppendEntriesRequest{
		Term:         b.term,
		LeaderID:     b.id,
		LeaderAddr:   b.selfAddr,
		RequestID:    target.RequestID,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: b.commitIndex,
	}, true
}

/*
Purpose:

	Reads a contiguous slice of Raft log entries from SQLite.

Args:

	db: SQLite handle for the local replica.
	from: First log index to include.
	to: Last log index to include.

Returns:

	Entries in index order, or an error from SQLite.
*/
func listRaftLogRange(db *sql.DB, from, to int64) ([]LogEntry, error) {
	if from > to {
		return nil, nil
	}
	rows, err := db.Query(`SELECT idx, term, request_id, operation, payload FROM raft_log WHERE idx >= ? AND idx <= ? ORDER BY idx`, from, to)
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

/*
Purpose:

	Sends heartbeat or catch-up work to every follower.

Rationale:

	A heartbeat is empty when the follower is up to date. If a follower is behind,
	the same loop sends missing entries so restarted servers converge before the
	tester compares logs.
*/
func (b *Bank) broadcastHeartbeat() {
	b.mu.Lock()
	if b.role != RoleLeader {
		b.mu.Unlock()
		return
	}
	peers := copyPeers(b.peers)
	term := b.term
	commit := b.commitIndex
	self := b.selfAddr
	last, _ := lastRaftEntry(b.db)
	b.mu.Unlock()
	for id, addr := range peers {
		go func(peerID int, peerAddr string) {
			b.mu.Lock()
			next := b.nextIndex[peerID]
			b.mu.Unlock()
			if last.Index > 0 && (next == 0 || next <= last.Index) {
				b.replicateEntryToPeer(peerID, last)
				return
			}
			req := b.buildHeartbeatRequest(peerID, term, commit, self)
			var resp AppendEntriesResponse
			if callRPC(peerAddr, "Raft.AppendEntries", req, &resp) != nil {
				return
			}
			if resp.Term > term {
				b.mu.Lock()
				b.becomeFollowerLocked(resp.Term, 0, "")
				b.mu.Unlock()
				return
			}
			if !resp.Success {
				b.mu.Lock()
				if b.nextIndex[peerID] > 1 {
					b.nextIndex[peerID]--
				}
				b.mu.Unlock()
			}
		}(id, addr)
	}
}

/*
Purpose:

	Builds an empty AppendEntries heartbeat for one follower.

Args:

	peerID: Replica id of the follower.
	term: Leader term captured before launching heartbeat goroutines.
	commit: Leader commit index sent to the follower.
	self: Leader address used as the client redirection hint.
*/
func (b *Bank) buildHeartbeatRequest(peerID int, term, commit int64, self string) *AppendEntriesRequest {
	b.mu.Lock()
	defer b.mu.Unlock()
	next := b.nextIndex[peerID]
	if next == 0 {
		last, _ := lastRaftEntry(b.db)
		next = last.Index + 1
	}
	prevIndex := next - 1
	var prevTerm int64
	if prevIndex > 0 {
		if prev, err := getRaftEntry(b.db, prevIndex); err == nil {
			prevTerm = prev.Term
		}
	}
	return &AppendEntriesRequest{
		Term:         term,
		LeaderID:     b.id,
		LeaderAddr:   self,
		RequestID:    0,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      nil,
		LeaderCommit: commit,
	}
}

/*
Purpose:

	Handles AppendEntries from the leader, including heartbeats, log repair,
	commit advancement, and applying committed entries.

Args:

	req: Leader term, previous log pointer, entries, and leader commit index.
	resp: Filled with this replica's term, success flag, and last matched index.

Returns:

	Nil when the RPC handler completed. Log mismatch is reported with
	resp.Success set to false.

Rationale:

	This is the follower side of Raft replication. It rejects stale leaders,
	deletes conflicting local entries, appends leader entries, and applies only
	entries the leader says are committed.
*/
func (b *Bank) handleAppendEntries(req *AppendEntriesRequest, resp *AppendEntriesResponse) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	resp.Term = b.term
	if req.Term < b.term {
		resp.Err = "stale term"
		return nil
	}
	if req.Term > b.term || b.role != RoleFollower {
		b.becomeFollowerLocked(req.Term, req.LeaderID, req.LeaderAddr)
	}
	b.leaderID = req.LeaderID
	b.leaderAddr = req.LeaderAddr
	b.electionReset = time.Now()
	if req.RequestID != 0 {
		b.logFollowerReceivesRequest(req.RequestID)
	}
	if req.PrevLogIndex > 0 {
		prev, err := getRaftEntry(b.db, req.PrevLogIndex)
		if err != nil || prev.Term != req.PrevLogTerm {
			resp.Err = "previous log entry does not match"
			resp.Success = false
			return nil
		}
	}
	for _, entry := range req.Entries {
		if existing, err := getRaftEntry(b.db, entry.Index); err == nil {
			if existing.Term == entry.Term && existing.RequestID == entry.RequestID && existing.Operation == entry.Operation && existing.Payload == entry.Payload {
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
