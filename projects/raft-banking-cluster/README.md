# Raft-Style Banking Cluster

Class step: PA8 for CPE-469 Distributed Systems.

## What it is

This is the five-replica version of the banking service. Instead of one fixed leader, replicas run a Raft-style election. Followers redirect clients with a leader hint, and the leader commits banking operations after replication to a majority.

## How it works

```text
client --deposit/withdraw/transfer--> any replica
   ^                                      |
   +------ "not leader, try <addr>" ------+   (followers send a leader hint)

leader --AppendEntries + heartbeats--> 4 followers
   |      followers whose election timeout fires start a vote (RequestVote)
   |
   +-- commit once a majority (3 of 5) has the entry, then apply it to SQLite
   +-- walk back a lagging follower's log until it matches, then resend
```

## What I worked on

- Election timeouts and candidate/leader/follower roles.
- RequestVote RPCs with one vote per term.
- Rejecting candidates with stale logs.
- Heartbeats and AppendEntries replication.
- Repairing follower logs when they fall behind.
- Storing terms, votes, logs, and account state in SQLite.

## How to run

Start the cluster:

```bash
make up
```

After an election, run requests against the current leader:

```bash
make demo LEADER=localhost:5001
make test-logs
```

If the target is not the leader, the client prints the leader address. Re-run the command with that address.

Stop and clear volumes:

```bash
make down
```

## Checks

```bash
go test ./...
```

The tests cover commit/apply behavior, one-vote-per-term persistence, stale-log vote rejection, failed elections without a majority, stepping down on higher terms, and log mismatch detection.

## Files to look at

- `raft_election.go`: elections and voting.
- `raft_replication.go`: heartbeats, AppendEntries, and log repair.
- `bank_apply.go`: applying committed banking operations.
- `raft_test.go`: the main replication/election tests.
