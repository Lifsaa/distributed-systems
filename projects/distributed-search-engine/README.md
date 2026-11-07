# Distributed Search Engine

Class step: PA5 for CPE-469 Distributed Systems. This used to be in the `pa4` folder, but the project itself is the PA5 search engine.

## What it is

This project crawls pages, builds sharded inverted indexes, replicates shard files, loads indexes into workers, and serves keyword searches through a coordinator.

## How it works

```text
seed URLs
    |
    v
coordinator  (frontier, task state, shard map; heartbeats every worker)
    |
    |  1. crawl      hands out URL batches; workers fetch and parse pages
    |  2. reduce     workers build inverted-index shards
    |  3. replicate  each shard is copied until it has 2 live holders
    |  4. load       workers load their shards
    v
workers ....... a worker that misses 3 heartbeats is treated as dead;
                its crawl or reduce task is reassigned after a timeout
    ^
    |  5. search
client --keyword--> coordinator --routes to a live worker holding that shard
```

## What I worked on

- Splitting crawler work into batches.
- Building reduce shards for the inverted index.
- Replicating shard files so a worker does not become the only copy.
- Routing search queries to the worker that owns the right shard.
- Running concurrent client queries and printing latency numbers.

## How to run

Requires Docker Desktop.

```bash
make up URLS=100 WORKERS=2 BATCH=50
make down
```

After an index has been built, the search client can run with:

```bash
docker compose --profile search up client
```

The `Makefile` also has benchmark targets for changing URL count, worker count, and batch size. Logs, indexes, binaries, and generated charts are ignored.

## Checks

```bash
go test ./...
```

There are no unit tests for this one, so this checks that it still builds. A small Docker run is the practical check.

## Files to look at

- `coordinator.go`: frontier, task state, replication, and search routing.
- `worker.go`: crawl, reduce, replicate, load-index, and lookup work.
- `client.go`: concurrent search client and latency output.
- `crawler.go`: page fetch and parsing logic.
