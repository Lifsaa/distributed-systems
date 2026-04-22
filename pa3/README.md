# PA3 - MapReduce Virtual Cluster
CPE-469 Distributed Systems

## What it does
Same MapReduce word count as PA2, but runs across Docker containers on a bridge network.
The coordinator splits the input file, hands out map and reduce tasks to workers over RPC,
and detects crashed workers via timeout. Workers are separate containers — reduce workers
fetch intermediate files from map workers over RPC.

## How to run

Requires Docker Desktop running. Place your input file at `pa3/data.txt`, then:

```bash
make up
```

This builds the binary, rebuilds the Docker image, and starts 1 coordinator + 3 workers.
Workers wait for the coordinator to be healthy before starting.

## Output
After the run completes:
```bash
make output
```

Produces `mr-out-0`, `mr-out-1`, `mr-out-2` — sorted word counts split across 3 reduce buckets:
```
and 4654006
the 7319769
to  5309120
```

## Cleanup
```bash
make down    # stop and remove containers
make clean   # remove binary, splits, intermediates, outputs
```

## Files
- `coordinator.go` — splits input, manages task state, handles worker registration, detects timeouts
- `worker.go` — runs map/reduce tasks, serves intermediate files over RPC to reduce workers
- `rpc.go` — shared message types between coordinator and workers
- `main.go` — entry point, reads ROLE env var to start as coordinator or worker
- `Dockerfile` — builds the Go binary into an Alpine container image
- `docker-compose.yml` — coordinator + 3 worker services on `mrnet` bridge network
- `Makefile` — build, up, down, output, clean targets

## Design notes
- Workers register their hostname with the coordinator on startup — no hardcoded addresses
- Intermediate files written to tmpfs (`/tmp/mr/`) inside each worker container for fast I/O
- Coordinator binds `:5001` (all interfaces) so Docker healthcheck works on loopback
- Worker-to-worker RPC on port 5002 for reduce workers to pull intermediate files
</content>
