# PA6 and PA7 Demo Navigation Guide

This guide is for explaining the code during a demo. The short version:

- PA6 is a single RPC banking server backed by SQLite.
- PA7 keeps the same banking API, but routes every client request through a fixed-leader replicated log before applying it.
- In PA7, replica 1 is always the leader. Replicas 2 and 3 are followers. There are no elections.

## Demo Commands

Run PA7 replicas:

```powershell
cd C:\Users\Salif\Desktop\distributed-systems\pa7
docker compose down -v
docker compose up --build
```

In another terminal:

```powershell
cd C:\Users\Salif\Desktop\distributed-systems\pa7
go run . client -addr localhost:5001 -op demo
go run . tester -replicas replica1=localhost:5001,replica2=localhost:5002,replica3=localhost:5003
```

Expected ending:

```text
PASS logs match
```

## PA6 Big Picture

PA6 has one server and one SQLite database.

Client flow:

```text
main.go -> runClientMode -> runClient -> client helper -> net/rpc call
server.go Bank method -> SQLite transaction -> operation_log -> accounts table -> response
```

Server flow:

```text
main.go -> runServer -> openDB -> newBank -> serveRPC
```

Important point for demo: every banking operation in PA6 directly opens a database transaction, logs the operation in `operation_log`, changes or reads `accounts`, commits, and returns.

## PA6 Files And Functions

### `pa6/main.go`

- `main`: reads the first command-line argument. `server` starts the server, `client` runs a client command.
- `runServer`: parses `-addr` and `-db`, opens SQLite, creates a `Bank`, and starts RPC listening.
- `runClientMode`: parses client flags like `-op`, `-account`, `-amount`, then calls `runClient`.

### `pa6/rpc.go`

- `StatusActive`, `StatusFrozen`, `StatusClosed`: account status constants.
- `Account`: account record returned to clients.
- `BasicResponse`: generic response with `OK` and `Err`.
- `AccountResponse`: response for opening an account.
- `BalanceResponse`: response for checking balance.
- `OpenAccountRequest`: username plus initial balance.
- `AccountRequest`: one account id.
- `MoneyRequest`: account id plus amount.
- `TransferRequest`: source account, destination account, amount.
- `PercentRequest`: account id plus percentage.

### `pa6/db.go`

- `openDB`: opens SQLite, limits connection count to 1, sets busy timeout, creates tables.
- `createTables`: creates `accounts` and `operation_log`.
- `begin`: starts one SQL transaction.
- `logOperation`: inserts a readable audit record into `operation_log`.
- `getAccount`: loads one account by id or returns `account not found`.
- `needActive`: rejects closed, frozen, or unknown-status accounts for customer operations.
- `needNotClosed`: rejects closed accounts for teller operations.
- `logCount`: returns count of audit-log rows.

### `pa6/server.go`

- `Bank`: holds the SQLite database handle.
- `newBank`: creates the server object.
- `serveRPC`: registers `Bank` methods and accepts RPC connections.
- `CheckBalance`: logs a balance check, verifies the account is active, returns balance/status.
- `Deposit`: validates positive amount, logs, checks active account, adds money.
- `Withdraw`: validates positive amount, logs, checks active account and funds, subtracts money.
- `Transfer`: validates amount and different accounts, checks both accounts active, moves money.
- `OpenAccount`: validates username/balance, logs, creates an active account.
- `CloseAccount`: calls `setStatus` with `closed`.
- `FreezeAccount`: calls `setStatus` with `frozen`.
- `UnfreezeAccount`: calls `setStatus` with `active`.
- `setStatus`: shared teller status update logic.
- `ApplyPercent`: applies a percentage increase/decrease to a non-closed account.
- `ChargeFee`: subtracts a teller fee from a non-closed account if funds exist.

### `pa6/client.go`

- `dialBank`: opens an RPC connection.
- `callBank`: opens one RPC connection, calls one method, closes it.
- `runClient`: switches `-op` into the correct client helper.
- `clientOpen`: calls `Bank.OpenAccount`.
- `clientBalance`: calls `Bank.CheckBalance`.
- `clientMoney`: calls deposit, withdraw, or fee RPCs.
- `clientTransfer`: calls `Bank.Transfer`.
- `clientBasic`: calls close, freeze, or unfreeze RPCs.
- `clientPercent`: calls `Bank.ApplyPercent`.
- `mustCall`: demo helper that prints each RPC result.
- `runDemo`: opens Alice and Bob, runs deposit/withdraw/transfer/freeze/unfreeze/percent/fee/close/balance.
- `openLoadAccounts`: creates accounts for the load test.
- `runLoad`: starts many goroutines doing random deposits and withdrawals.

## PA7 Big Picture

PA7 keeps the same client-facing banking RPC methods, but adds replication.

Leader client flow:

```text
client -> Bank.Deposit on replica1
Bank.Deposit -> replicateClientOperation
replicateClientOperation -> append raft_log entry on leader
replicateClientOperation -> send Raft.AppendEntries to followers
followers -> handleAppendEntries -> write raft_log
leader gets majority ACK -> marks commit_index
leader -> applyThroughLocked -> applyEntry -> applyDeposit
leader responds to client
leader sends another AppendEntries with updated LeaderCommit
followers apply committed entries
```

Follower client flow:

```text
client -> Bank.Deposit on replica2
replica2 sees isLeader == false
response Err = "not leader; send requests to replica1:5001"
```

Tester flow:

```text
tester -> Raft.GetLog on replica1, replica2, replica3
compareRaftLogs checks same length and same entries
prints PASS logs match
```

Important point for demo: PA7 stores two kinds of logs.

- `raft_log`: the replicated log required by the assignment. This is what the tester compares.
- `operation_log`: the banking/audit log from PA6. This is written only when a committed entry is applied.

## PA7 Files And Functions

### `pa7/main.go`

- `main`: chooses `server`, `client`, or `tester` mode.
- `runServer`: parses replica config, opens DB, builds `ReplicaConfig`, starts RPC server.
- `normalizeBoolFlagPairs`: lets `-leader true` work as well as `-leader=true`.
- `runClientMode`: same client flag parsing as PA6.
- `runTesterMode`: parses replica addresses and runs log comparison.
- `parsePeerMap`: converts `2=host:port,3=host:port` into follower peer config.
- `parseNamedAddrMap`: generic parser for comma-separated `name=address` strings.

### `pa7/rpc.go`

PA7 keeps all PA6 banking request/response types.

New PA7 replication types:

- `OpCheckBalance` through `OpChargeFee`: string names stored in `raft_log`.
- `LogEntry`: one replicated log row: index, term, operation, JSON payload.
- `AppendEntriesRequest`: simplified Raft append entries request.
- `AppendEntriesResponse`: ACK/failure response from follower.
- `GetLogRequest` / `GetLogResponse`: tester RPC for reading a replica log.
- `GetStateRequest` / `GetStateResponse`: debugging RPC for term, role, commit index, last index.

### `pa7/db.go`

PA7 keeps all PA6 DB helpers and adds Raft storage.

Shared banking helpers:

- `openDB`, `createTables`, `begin`, `logOperation`, `getAccount`, `needActive`, `needNotClosed`, `logCount`: same purpose as PA6.

New replication helpers:

- `getStateInt`: reads `commit_index` or `applied_index` from `replica_state`.
- `setStateInt`: saves `commit_index` or `applied_index`.
- `appendRaftEntry`: inserts a new leader log entry.
- `putRaftEntry`: inserts or replaces a follower log entry.
- `deleteRaftFrom`: deletes conflicting follower entries at/after an index.
- `getRaftEntry`: reads one log entry by index.
- `lastRaftEntry`: reads the highest-index log entry.
- `listRaftLog`: returns the whole log for the tester.

### `pa7/server.go`: Types

- `ReplicaConfig`: startup config: id, leader/follower role, peers, leader address, term.
- `Bank`: main server object. Holds DB, replica id, leader flag, peers, term, commit index, applied index, and a mutex.
- `Raft`: small wrapper that exposes Raft RPC methods separately from banking RPC methods.

### `pa7/server.go`: Startup And Public RPCs

- `newBank`: creates a single-node leader default, mainly for tests/backward compatibility.
- `newBankWithConfig`: creates a configured leader or follower and loads saved commit/apply indexes.
- `serveRPC`: registers both `Bank` and `Raft` RPC services.

Banking RPC entrypoints:

- `CheckBalance`, `Deposit`, `Withdraw`, `Transfer`, `OpenAccount`, `CloseAccount`, `FreezeAccount`, `UnfreezeAccount`, `ApplyPercent`, `ChargeFee`: these no longer directly touch accounts. They call `replicateClientOperation`.

Raft RPC entrypoints:

- `Bank.AppendEntries`: test-friendly wrapper for append entries.
- `Raft.AppendEntries`: actual follower RPC called by leader.
- `Raft.GetLog`: used by tester to fetch `raft_log`.
- `Raft.GetState`: optional debugging state RPC.

### `pa7/server.go`: Replication Core

- `replicateClientOperation`: the leader path. It rejects follower client calls, JSON-encodes the request, appends to local `raft_log`, sends append entries to followers, waits for majority ACK, commits, applies, and responds.
- `sendAppendEntries`: opens RPC to one follower and calls `Raft.AppendEntries`.
- `handleAppendEntries`: follower path. It checks term and previous log match, appends/replaces entries, advances commit index, and applies committed entries.
- `applyThroughLocked`: applies every unapplied log entry up to a target commit index.
- `applyEntry`: decodes a committed `LogEntry` and dispatches to the correct banking apply function.

### `pa7/server.go`: Banking Apply Functions

These are the PA6 banking operations moved behind the commit step:

- `applyCheckBalance`: logs balance check, verifies active account, returns balance.
- `applyDeposit`: validates amount, checks active account, adds money.
- `applyWithdraw`: validates amount/funds, checks active account, subtracts money.
- `applyTransfer`: validates accounts/amount/funds, moves money.
- `applyOpenAccount`: creates a new active account.
- `applySetStatus`: shared close/freeze/unfreeze logic.
- `applyPercent`: applies a percentage change to a non-closed account.
- `applyFee`: subtracts a fee from a non-closed account.

Small helpers:

- `decodePayload`: JSON-decodes a log payload into the right request struct.
- `responseOr`: uses the real client response on the leader, or a throwaway response on followers.
- `setResponseError`: writes an error onto any banking response type.
- `notLeaderMessage`: builds the follower error message.
- `majority`: returns required ACK count. With 3 replicas, majority is 2.
- `minInt64`: prevents commit index from passing the local last log index.

### `pa7/client.go`

Same as PA6 client code. This is intentional: clients still call `Bank.OpenAccount`, `Bank.Deposit`, etc. The replication is hidden on the server side.

### `pa7/tester.go`

- `runTester`: fetches logs from each replica and prints `PASS logs match` if equal.
- `fetchReplicaLog`: calls `Raft.GetLog` on one replica.
- `compareRaftLogs`: checks every replica has the same number of entries and identical entry values.

### `pa7/*_test.go`

- `TestLeaderCommitsClientOperationThroughLog`: proves leader logs and applies client operations.
- `TestFollowerAppendsAndAppliesCommittedEntries`: proves a follower can append and apply committed entries.
- `TestCompareRaftLogsDetectsMismatch`: proves tester comparison catches mismatches.
- `TestNormalizeBoolFlagPairs`: proves `-leader true` is parsed correctly.

## What To Say During Demo

Use this simple explanation:

> PA6 is the base single-node bank. Each RPC directly opens a SQLite transaction, writes an audit row, updates or reads the account table, commits, and returns.

> PA7 keeps the same public banking RPCs, but changes what happens inside them. A client still calls `Bank.Deposit`, but on the leader that request is first serialized into a `LogEntry`, stored in `raft_log`, and sent to followers with `Raft.AppendEntries`.

> Once the leader has a majority, it advances `commit_index`, applies the committed log entry to the real bank tables, and then responds to the client. Followers apply entries when the leader sends a higher `LeaderCommit`.

> The tester container proves replication by calling `Raft.GetLog` on all three replicas and comparing every log entry.

## Demo Walkthrough In Code

For `go run . client -addr localhost:5001 -op demo`:

1. `main.go` calls `runClientMode`.
2. `runClientMode` parses `-op demo`.
3. `client.go` `runClient` calls `runDemo`.
4. `runDemo` calls `Bank.OpenAccount`, `Bank.Deposit`, etc.
5. On PA7 leader, each public `Bank.*` method calls `replicateClientOperation`.
6. `replicateClientOperation` appends a `LogEntry` and calls followers.
7. Followers receive `Raft.AppendEntries` and run `handleAppendEntries`.
8. Leader gets majority ACK and runs `applyThroughLocked`.
9. `applyThroughLocked` calls `applyEntry`.
10. `applyEntry` dispatches to the correct `apply*` function.
11. The `apply*` function does the actual SQLite banking work.
12. Tester later calls `Raft.GetLog` and compares logs.

## Most Likely Questions

### Why log balance reads too?

The assignment says to treat read-only requests the same as writes for simplicity. So `CheckBalance` also becomes a `raft_log` entry.

### Where is the majority check?

In `replicateClientOperation`. It starts `acks := 1` for the leader, sends to followers, then checks `acks < b.majority()`.

### Where is the leader election?

There is no election by assignment assumption. Replica 1 is started with `-leader true`.

### How do followers know what to apply?

The leader sends `LeaderCommit` in `AppendEntriesRequest`. Followers update `commitIndex` and run `applyThroughLocked`.

### What proves logs match?

`tester.go` calls `Raft.GetLog` on every replica and `compareRaftLogs` checks all entries match exactly.

### Why use SQLite for `raft_log`?

It keeps the log durable and easy to compare without managing separate text files. Each row is one log entry.

