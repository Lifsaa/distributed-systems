# Replicated Banking With Fixed Leader

Class step: PA7 for CPE-469 Distributed Systems.

## What it is

This extends the banking server to three replicas. Replica 1 is always the leader. Clients send requests to it, and the leader commits an operation after a majority of replicas acknowledge the log entry.

## What I worked on

- Adding a replicated log for banking operations.
- Sending AppendEntries-style RPCs from the leader to followers.
- Committing only after a majority ACK.
- Applying operations to SQLite only after commit.
- Comparing replica logs with a small tester.

## How to run

Start the replicas:

```bash
make up
```

Run a demo and compare logs:

```bash
make demo
make test-logs
```

Stop and clear volumes:

```bash
make down
```

## Checks

```bash
go test ./...
```

The tests cover leader commit, follower append/apply behavior, and detecting mismatched logs.

## Files to look at

- `server.go`: fixed-leader replication and majority commit path.
- `raft_test.go`: replication/log comparison tests.
- `tester.go`: log comparison client.
