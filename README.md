# Distributed Systems Class Projects

This repo collects my CPE-469 Distributed Systems projects. The projects start with crawling and MapReduce, then build toward containerized workers, distributed search, replicated storage, and Raft-style leader election.

Each folder has its own README with the original PA number, run commands, and the main files worth reading.

**Start here** if you only have a few minutes:

- [Distributed Search Engine](projects/distributed-search-engine): coordinator and workers crawl, build sharded inverted indexes, replicate shards, and route queries to live replicas.
- [Raft Banking Cluster](projects/raft-banking-cluster): five replicas with leader election, heartbeats, majority commit, and follower log repair.

## Projects

| Project | Class step | What it does | Systems pieces | Quick check |
|---|---:|---|---|---|
| [Web Crawler Indexer](projects/web-crawler-indexer) | PA1 | Crawls pages and writes an inverted index | crawling, indexing, file output | `cd projects/web-crawler-indexer && go test ./...` |
| [MapReduce RPC](projects/mapreduce-rpc) | PA2 | Runs local MapReduce workers through RPC | coordinator/worker RPC, map/reduce tasks | `cd projects/mapreduce-rpc && go test ./...` |
| [MapReduce Virtual Cluster](projects/mapreduce-virtual-cluster) | PA3 | Runs MapReduce across Docker containers | Docker networking, worker registration, timeouts | `cd projects/mapreduce-virtual-cluster && go test ./...` |
| [Distributed Search Engine](projects/distributed-search-engine) | PA5 | Crawls, shards indexes, and serves search queries | crawling, sharding, replication, search routing | `cd projects/distributed-search-engine && go test ./...` |
| [Banking RPC SQLite](projects/banking-rpc-sqlite) | PA6 | Single-node banking RPC server | SQLite, transactions, operation logs | `cd projects/banking-rpc-sqlite && go test ./...` |
| [Replicated Banking Fixed Leader](projects/replicated-banking-fixed-leader) | PA7 | Three banking replicas with one fixed leader | majority commit, replicated logs | `cd projects/replicated-banking-fixed-leader && go test ./...` |
| [Raft Banking Cluster](projects/raft-banking-cluster) | PA8 | Five banking replicas with leader election | Raft-style elections, heartbeats, log repair | `cd projects/raft-banking-cluster && go test ./...` |

## What This Covers

- RPC between coordinators, workers, clients, and replicas.
- MapReduce task splitting, reduce partitioning, and worker coordination.
- Docker Compose clusters for multi-process distributed runs.
- Sharded inverted indexes, search routing, and crawler batching.
- SQLite-backed state with operation logs and replicated log entries.
- Majority commit, leader/follower replication, leader election, heartbeats, and log repair.
