# MapReduce Over RPC

Class step: PA2 for CPE-469 Distributed Systems.

## What it is

This is a local MapReduce word count. A coordinator splits the input, workers ask for tasks over RPC, and reducers write sorted word-count files.

## What I worked on

- Splitting input into map tasks.
- Having workers ask the coordinator for work instead of hardcoding assignments.
- Partitioning map output across reduce buckets.
- Keeping the RPC structs simple enough that the coordinator and workers can share them.

## How to run

```bash
make run
```

Or run it directly:

```bash
go run . -input=sample-input.txt -M=9 -R=3 -W=3
```

Generated `split-*`, `intermediate-*`, and `mr-out-*` files are ignored.

## Checks

```bash
go test ./...
```

There are no unit tests for this one, so this checks that the package still builds. `make run` is the quick behavior check.

## Files to look at

- `coordinator.go`: input splitting and task assignment.
- `worker.go`: map and reduce task execution.
- `rpc.go`: request and response types used between the coordinator and workers.
