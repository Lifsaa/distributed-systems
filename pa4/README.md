# PA5 - Distributed Search Engine
CPE-469 Distributed Systems

## What it does
Crawls URLs in batches, builds inverted-index shards, reduces/merges them, then serves
keyword search queries. Coordinator owns the frontier, assigns crawl/reduce/replicate/loadindex
tasks over RPC, and routes search queries to the correct worker shard. Clients query
concurrently and print latency stats.

## How to run

Requires Docker Desktop.

### Crawl + Index (scenarios 1-3 graphs + scenarios 4-8)
Edit `MAX_URLS` and `NUM_WORKERS` in `docker-compose.yml` as needed, then:

```bash
make up
# wait for job to finish (watch logs)
make down
```

Starts 1 coordinator + workers. Workers mount `./mr-output` at `/tmp/mr`.

### Search phase (scenarios 4-8)
After crawl finishes, start the client:

```bash
docker compose --profile search up client
```

### Override env vars

| Variable | Default | Description |
|---|---|---|
| `MAX_URLS` | 10000 | URLs to crawl |
| `NUM_REDUCE` | 3 | Reduce shards / workers |
| `BATCH_SIZE` | 100 | URLs per crawl task |
| `NUM_THREADS` | 100 | Client goroutines |
| `CRAWL_PARALLELISM` | 12 | Concurrent page fetches per worker |
| `CRAWL_TIMEOUT_SECONDS` | 8 | HTTP timeout per page |
| `CRAWL_MAX_BYTES` | 2097152 | Max bytes parsed per page |
| `CRAWL_ALLOWED_HOST` | en.wikipedia.org | Optional host filter for discovered links |

For repeated benchmark runs, build the Docker image once:

```bash
make image
make fs1a   # fast no-rebuild scenario targets are fs1a ... fs3c
```

## Graph scenarios (run each, fill in plot_graphs.py)

| Scenario | MAX_URLS | workers | BATCH_SIZE |
|---|---|---|---|
| 1a | 100 | 4 | 100 |
| 1b | 1000 | 4 | 100 |
| 1c | 5000 | 4 | 100 |
| 1d | 10000 | 4 | 100 |
| 1e | 20000 | 4 | 100 |
| 2a | 10000 | 2 | 100 |
| 2b | 10000 | 3 | 100 |
| 2c | 10000 | 4 | 100 |
| 3a | 10000 | 4 | 100 |
| 3b | 10000 | 4 | 200 |
| 3c | 10000 | 4 | 500 |

After running each, fill in the `time_y*` arrays in `plot_graphs.py`, then:

```bash
python plot_graphs.py   # generates pa5_graphs.png
```

## Output
`mr-output/mr-out-0.json` … `mr-out-{NUM_REDUCE-1}.json` — word → URL arrays.
`coord-logs/coordinator.log` — full coordinator log.

## Cleanup
```bash
make down
make clean
rm -rf mr-output coord-logs
```

## Files
- `coordinator.go` — frontier, tasks, heartbeats, replication, search routing
- `worker.go` — crawl/reduce/replicate, in-memory index, Lookup RPC
- `client.go` — concurrent search client, latency stats
- `crawler.go` — HTTP fetch, HTML parse, stop words
- `rpc.go` — all message types
- `main.go` — `ROLE=coordinator|worker|client`
- `plot_graphs.py` — generates the 3 required graphs
- `keywords.txt` — seed search keywords
- `docker-compose.yml`, `Dockerfile`, `Makefile`
