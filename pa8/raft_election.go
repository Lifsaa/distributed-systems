package main

import (
	"math/rand"
	"net/rpc"
	"time"
)

/*
Purpose:

	Watches for missing leader heartbeats and starts a new election after a
	randomized timeout.

Rationale:

	Randomized timeouts reduce split votes because replicas usually become
	candidates at different times.
*/
func (b *Bank) electionLoop() {
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(b.id)))
	timeout := func() time.Duration {
		return electionMin + time.Duration(rng.Int63n(int64(electionJitter)))
	}
	deadline := timeout()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-b.stopCh:
			return
		case <-ticker.C:
			b.mu.Lock()
			ready := b.role != RoleLeader && time.Since(b.electionReset) >= deadline
			b.mu.Unlock()
			if ready {
				b.startElection()
				deadline = timeout()
			}
		}
	}
}

/*
Purpose:

	Runs one election round for this replica as a candidate.

Returns:

	Nothing directly. The Bank role changes to leader only when a majority votes
	for this candidate in the same term.

Rationale:

	The candidate copies peer addresses before dialing so network calls happen
	without holding the shared Raft mutex.
*/
func (b *Bank) startElection() {
	b.mu.Lock()
	b.role = RoleCandidate
	b.term++
	b.votedFor = b.id
	b.leaderID = 0
	b.leaderAddr = ""
	b.electionReset = time.Now()
	term := b.term
	peers := copyPeers(b.peers)
	last, err := lastRaftEntry(b.db)
	if err != nil {
		b.mu.Unlock()
		return
	}
	_ = setStateInt(b.db, "current_term", term)
	_ = setStateInt(b.db, "voted_for", int64(b.votedFor))
	b.logCandidateVoteRequest(term)
	b.mu.Unlock()

	votes := 1
	for peerID, addr := range peers {
		req := &RequestVoteRequest{
			Term:          term,
			CandidateID:   b.id,
			CandidateAddr: b.selfAddr,
			LastLogIndex:  last.Index,
			LastLogTerm:   last.Term,
		}
		var resp RequestVoteResponse
		if callRPC(addr, "Raft.RequestVote", req, &resp) != nil {
			continue
		}
		if resp.Term > term {
			b.mu.Lock()
			b.becomeFollowerLocked(resp.Term, 0, "")
			b.mu.Unlock()
			return
		}
		if resp.VoteGranted {
			votes++
			b.logVoteDecision(peerID, b.id, true)
		} else {
			b.logVoteDecision(peerID, b.id, false)
		}
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.role != RoleCandidate || b.term != term {
		return
	}
	if votes >= b.majorityLocked() {
		b.role = RoleLeader
		b.leaderID = b.id
		b.leaderAddr = b.selfAddr
		last, _ := lastRaftEntry(b.db)
		for id := range b.peers {
			b.nextIndex[id] = last.Index + 1
			b.matchIndex[id] = 0
		}
		b.logElectionResult(term, true)
		go b.broadcastHeartbeat()
		return
	}
	b.logElectionResult(term, false)
	if votes == b.majorityLocked()-1 {
		b.logElectionTie()
	}
	b.role = RoleFollower
}

/*
Purpose:

	Moves this replica back to follower state and records the known leader.

Args:

	term: Newer term observed from another replica.
	leaderID: Replica id of the known leader, or 0 if unknown.
	leaderAddr: Address clients should use for the leader, if known.

Rationale:

	Any newer term proves this server is stale, so it must stop acting as
	candidate or leader.
*/
func (b *Bank) becomeFollowerLocked(term int64, leaderID int, leaderAddr string) {
	if term > b.term {
		b.term = term
		b.votedFor = 0
		_ = setStateInt(b.db, "current_term", b.term)
		_ = setStateInt(b.db, "voted_for", 0)
	}
	b.role = RoleFollower
	b.leaderID = leaderID
	b.leaderAddr = leaderAddr
	b.electionReset = time.Now()
}

/*
Purpose:

	Copies the peer map so election code can release the lock before dialing peers.

Rationale:

	Network calls can block, so this avoids holding the mutex while contacting
	other replicas.
*/
func copyPeers(in map[int]string) map[int]string {
	out := make(map[int]string, len(in))
	for id, addr := range in {
		out[id] = addr
	}
	return out
}

/*
Purpose:

	Dials one replica, calls one RPC method, and closes the connection.

Args:

	addr: Peer address, such as "replica2:5002".
	method: RPC method name, such as "Raft.RequestVote".
	req: Request object passed to net/rpc.
	resp: Response object filled by net/rpc.

Returns:

	Nil when the RPC transport succeeds; otherwise the dial or call error.
*/
func callRPC(addr, method string, req any, resp any) error {
	c, err := rpc.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer c.Close()
	return c.Call(method, req, resp)
}

/*
Purpose:

	Handles a RequestVote RPC from a candidate. This is the core voting rule
	for leader election.

Args:

	req: Candidate term, candidate id, and the candidate's last log position.
	resp: Filled with this server's current term and whether the vote was granted.

Returns:

	Nil when the RPC handler completed. Raft-level rejection is reported with
	resp.VoteGranted set to false.

Rationale:

	A server only votes once per term and only votes for candidates whose logs
	are up to date, which prevents an old replica from becoming leader.
*/
func (b *Bank) handleRequestVote(req *RequestVoteRequest, resp *RequestVoteResponse) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	resp.Term = b.term
	if req.Term < b.term {
		b.logVoteDecision(b.id, req.CandidateID, false)
		return nil
	}
	if req.Term > b.term {
		b.becomeFollowerLocked(req.Term, 0, "")
	}
	last, err := lastRaftEntry(b.db)
	if err != nil {
		resp.Err = err.Error()
		return nil
	}
	upToDate := req.LastLogTerm > last.Term || (req.LastLogTerm == last.Term && req.LastLogIndex >= last.Index)
	canVote := b.votedFor == 0 || b.votedFor == req.CandidateID
	if canVote && upToDate {
		b.votedFor = req.CandidateID
		b.role = RoleFollower
		b.electionReset = time.Now()
		_ = setStateInt(b.db, "current_term", b.term)
		_ = setStateInt(b.db, "voted_for", int64(b.votedFor))
		resp.VoteGranted = true
		if req.CandidateAddr != "" {
			b.leaderAddr = req.CandidateAddr
		}
	} else {
		resp.VoteGranted = false
	}
	resp.Term = b.term
	b.logVoteDecision(b.id, req.CandidateID, resp.VoteGranted)
	return nil
}
