# PA4 - Distributed Search Engine
CPE-469 Distributed Systems

## What it does
Crawls URLs in batches, builds inverted-index shards, then reduces/merges them across Docker
containers. Coordinator owns the frontier, assigns crawl/reduce/replicate tasks over RPC,
pings workers with heartbeats, and keeps two copies of each reduce output.

## How to run

Requires Docker Desktop. Edit `seed-urls.txt` if needed, then:

```bash
make up
```

Builds the Linux binary, rebuilds the image, starts 1 coordinator + 5 workers. Workers mount
`./mr-output` at `/tmp/mr` so JSON lands on the host.

Env (set in `docker-compose.yml` or override): `SEED_FILE`, `MAX_URLS`, `NUM_REDUCE`.

## Output
After the job finishes, see `mr-output/mr-out-0.json` … `mr-out-{NUM_REDUCE-1}.json` (word → URL arrays) and `intermediate-*-*.json` shards.

## Cleanup
```bash
make down
make clean    # removes local pa4 binary + any mr-out/intermediate in cwd (not mr-output/)
rm -rf mr-output   # optional: clear host-mounted crawl output
```

## Files
- `coordinator.go` — frontier, tasks, heartbeats, replication
- `worker.go` — crawl/reduce/replicate, RPC server for fetch/heartbeat
- `crawler.go` — HTTP fetch, HTML parse, `cleanText` / stop words
- `rpc.go` — message types and constants
- `main.go` — `ROLE=coordinator` or `ROLE=worker`
- `Dockerfile`, `docker-compose.yml`, `Makefile`
- `stopword.txt`, `seed-urls.txt`
