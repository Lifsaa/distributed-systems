package main

import "fmt"

/*
Purpose:

	Writes one required PA08 debug line to stdout and the optional server log file.

Rationale:

	The assignment asks for readable server debugging output while scenarios run.
*/
func (b *Bank) logLine(format string, args ...any) {
	line := fmt.Sprintf(format, args...)
	if b.logger != nil {
		fmt.Fprintln(b.logger, line)
	}
}

func (b *Bank) logCandidateVoteRequest(term int64) {
	b.logLine("CANDIDATE SERVER %d SENDING A VOTING REQUEST", b.id)
}

func (b *Bank) logVoteDecision(serverID, candidateID int, granted bool) {
	decision := "DENIES"
	if granted {
		decision = "VOTES"
	}
	b.logLine("SERVER %d %s VOTE FOR SERVER %d", serverID, decision, candidateID)
}

func (b *Bank) logElectionResult(term int64, won bool) {
	result := "LOSES"
	if won {
		result = "WINS"
	}
	b.logLine("CANDIDATE SERVER %d %s THE ELECTION FOR TERM %d", b.id, result, term)
}

func (b *Bank) logElectionTie() {
	b.logLine("ELECTION TIE, TIMEOUT BEGINS")
}

func (b *Bank) logLeaderReceivesRequest(requestID int64) {
	b.logLine("LEADER SERVER %d RECEIVES REQUEST %d FROM CLIENT", b.id, requestID)
}

func (b *Bank) logLeaderAddsRequest(requestID, index int64) {
	b.logLine("LEADER SERVER %d ADDS REQUEST %d TO LOG ENTRY %d", b.id, requestID, index)
}

func (b *Bank) logLeaderSendsRequest(requestID int64, serverID int) {
	b.logLine("LEADER SENDS REQUEST %d TO SERVER %d", requestID, serverID)
}

func (b *Bank) logFollowerReceivesRequest(requestID int64) {
	b.logLine("FOLLOWER %d RECEIVES REQUEST %d", b.id, requestID)
}

func (b *Bank) logLeaderReceivesAck(serverID int, requestID int64) {
	b.logLine("LEADER RECEIVES ACK FROM SERVER %d FOR REQUEST %d", serverID, requestID)
}
