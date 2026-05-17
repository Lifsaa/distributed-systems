# PA7 - Replicated Banking Server

This version extends the PA6 banking server with a small fixed-leader
replication layer. There are 3 replicas. Replica 1 is always the leader for
term 1, and there are no elections.

All client requests go to replica 1. The leader appends every request to
`raft_log`, sends `Raft.AppendEntries` to the followers, commits after a
majority ACK, applies the operation to SQLite, and then responds to the client.
Read-only balance requests are logged the same way as writes.

## Run with Docker

```bash
make up
```

In another terminal:

```bash
make demo
make test-logs
```

Or run the tester inside Docker:

```bash
docker compose --profile test run --rm tester
```

Stop and remove replica volumes:

```bash
make down
```

## Local commands

Run one leader manually:

```bash
go run . server -id 1 -addr :5001 -db bank1.db -leader true
```

Run clients against the fixed leader:

```bash
go run . client -addr localhost:5001 -op open -username alice -amount 10000
go run . client -addr localhost:5001 -op deposit -account 1 -amount 500
go run . client -addr localhost:5001 -op balance -account 1
go run . client -addr localhost:5001 -op demo
```

Compare logs across replicas:

```bash
go run . tester -replicas replica1=localhost:5001,replica2=localhost:5002,replica3=localhost:5003
```

## Files

- `server.go`: fixed-leader replication and committed banking apply path
- `db.go`: SQLite account tables, audit log, raft log, and replica counters
- `rpc.go`: banking and raft RPC request/response types
- `tester.go`: log comparison client for the Tester container
- `docker-compose.yml`: three replicas plus a tester service
