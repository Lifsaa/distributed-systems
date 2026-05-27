# PA8 - Replicated Banking Server II

PA8 extends the PA7 replicated banking server with a 5-server Raft-style leader election layer.

Any server can become leader. Followers start an election when heartbeats stop, candidates request votes only from replicas whose logs can safely elect them, and leaders replicate banking requests to a majority before committing them. Client requests sent to followers return a leader hint.

## Run with Docker

```bash
make up
```

Wait a few seconds for an election, then run client requests against the current leader:

```bash
make demo LEADER=localhost:5001
make load20 LEADER=localhost:5001
make test-logs
```

If the target is not the leader, the client output includes the leader address. Re-run the command with that address:

```bash
make demo LEADER=localhost:5003
```

Stop containers:

```bash
make down
```

Replica logs and databases are written into local `server-logs/replica*/` folders. To start a fresh scenario and clear those files:

```bash
make reset-scenario
make up
```

## Local Commands

Run all tests:

```bash
go test ./...
```

Run one server manually:

```bash
go run . server -id 1 -addr :5001 -self localhost:5001 -db bank1.db -log server1.log -peers 2=localhost:5002,3=localhost:5003,4=localhost:5004,5=localhost:5005
```

Run clients:

```bash
go run . client -addr localhost:5001 -op open -username alice -amount 10000
go run . client -addr localhost:5001 -op deposit -account 1 -amount 500
go run . client -addr localhost:5001 -op balance -account 1
go run . client -addr localhost:5001 -op demo
```

Compare logs across replicas:

```bash
go run . tester -replicas replica1=localhost:5001,replica2=localhost:5002,replica3=localhost:5003,replica4=localhost:5004,replica5=localhost:5005
```

## Scenario Recording

The exact screen-recording steps for Scenario 1 and Scenario 2 are in `SCENARIO.md`.

Use `docker compose stop replicaN` to fail a server and `docker compose start replicaN` to restart it. The final verification for both scenarios is:

```bash
make test-logs
```

Archive a completed scenario before tearing containers down:

```bash
make save-scenario NAME=scenario1
```

This saves per-replica logs/databases plus combined Docker output under `recordings/scenario1/`.

## Files

- `server.go`: replica configuration, state, and RPC startup
- `raft_election.go`: election timeout, vote requests, and role transitions
- `raft_replication.go`: heartbeats, AppendEntries, log repair, and commit flow
- `bank_apply.go`: committed banking operation apply path
- `debug_log.go`: required PA8 debug output lines
- `db.go`: SQLite account tables, audit log, raft log, and durable Raft counters
- `rpc.go`: banking and Raft RPC request/response types
- `tester.go`: raft-log comparison client for all 5 replicas
- `docker-compose.yml`: five replicas plus a tester service
