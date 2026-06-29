# Banking RPC Service With SQLite

Class step: PA6 for CPE-469 Distributed Systems.

## What it is

This is a single-node banking server. Clients talk to it over RPC, account data lives in SQLite, and every operation is written to an operation log.

## What I worked on

- RPC commands for opening accounts, deposits, withdrawals, transfers, balances, freezes, and fees.
- SQLite tables for accounts and the operation log.
- A demo client and a load-test mode.
- Keeping money as integer cents instead of floats.

## How to run

Start the server:

```bash
make server
```

In another terminal:

```bash
make demo
```

Manual examples:

```bash
go run . client -op open -username alice -amount 10000
go run . client -op deposit -account 1 -amount 500
go run . client -op balance -account 1
```

`10000` means `$100.00`.

## Checks

```bash
go test ./...
```

There are no unit tests for this one, so this checks that it builds. `make demo` is the quick manual check.

## Files to look at

- `server.go`: banking RPC methods.
- `db.go`: SQLite schema and transaction helpers.
- `client.go`: manual client, demo flow, and load test.
