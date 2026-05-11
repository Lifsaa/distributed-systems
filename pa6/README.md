# PA6 - Single-Node Banking Server

This is a simple RPC banking server. It stores accounts in sqlite and keeps an
`operation_log` table for the sequence of operations.

## Run server

```bash
go run . server -addr :5001 -db bank.db
```

## Run clients

```bash
go run . client -op open -username alice -amount 10000
go run . client -op deposit -account 1 -amount 500
go run . client -op balance -account 1
go run . client -op demo
go run . client -op load -clients 100 -requests 1000
```

Or use make:

```bash
make server
make demo
make load
make log
```

Money is in cents, so `10000` means `$100.00`.

## Operations

- Customers: balance, deposit, withdraw, transfer
- Tellers: open, close, freeze, unfreeze, percent, fee

## Check the log

```bash
sqlite3 bank.db "SELECT * FROM operation_log LIMIT 20;"
```

## Files

- `main.go`: starts server or client
- `rpc.go`: RPC structs
- `db.go`: sqlite setup
- `server.go`: bank RPC methods
- `client.go`: manual client, demo, and load test
