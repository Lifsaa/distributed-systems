# MapReduce Virtual Cluster

Class step: PA3 for CPE-469 Distributed Systems.

## What it is

This is the MapReduce project running across Docker containers. The coordinator and workers run as separate services on a bridge network, and reduce workers pull intermediate files from map workers over RPC.

## What I worked on

- Moving the coordinator/worker setup from one local process group into containers.
- Letting workers register with the coordinator by hostname.
- Serving intermediate map output over RPC.
- Timing out tasks so a crashed worker can be retried.

## How to run

```bash
make up
make output
make down
```

The Compose setup uses `sample-input.txt` by default. For a bigger local run, put a file at `data.txt`; that path is ignored so large inputs stay out of Git.

## Checks

```bash
go test ./...
```

There are no unit tests for this one, so this is a compile check. The Docker run above is the main demo path.

## Files to look at

- `docker-compose.yml`: coordinator and worker services on the same network.
- `coordinator.go`: task state, registration, and timeout retry logic.
- `worker.go`: map/reduce work plus RPC serving for intermediate files.
