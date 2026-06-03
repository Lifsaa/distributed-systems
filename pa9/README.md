# PA09: Ray Monte Carlo Reliability Simulator

This project uses Ray to estimate the reliability of replicated distributed
services with Monte Carlo simulation. Each trial randomly fails nodes and,
for one scenario, entire availability zones. Ray distributes independent
simulation chunks across worker containers and the driver aggregates the
outage counts into final availability estimates.

## Why Ray Fits

Monte Carlo trials are independent, so the workload is embarrassingly
parallel. Ray Core is used as the distributed execution engine:

- `ray.init("ray://ray-head:10001")` connects the app to the cluster.
- `@ray.remote` marks a simulation chunk as distributed work.
- `.remote()` submits chunks to Ray workers.
- `ray.get()` gathers the results for aggregation.

## Scenarios

The demo evaluates three service designs:

1. `3 replicas need 2`: small majority-replica service.
2. `5 replicas need 3`: larger majority-replica service.
3. `9 replicas 3 zones need 5`: replicas spread across zones with random
   node and zone failures.

For each scenario the program prints outage probability, availability,
serial runtime, Ray runtime, and speedup.

## Run With Docker Compose

```bash
cd pa9
TRIALS=5000000 CHUNKS=16 docker compose up --build --abort-on-container-exit --exit-code-from app
```

Quick smoke test:

```bash
TRIALS=10000 CHUNKS=4 docker compose up --build --abort-on-container-exit --exit-code-from app
```

Optional larger run for a stronger speedup chart:

```bash
TRIALS=20000000 CHUNKS=32 docker compose up --build --abort-on-container-exit
```

Generated files are written to `outputs/`:

- `results.csv`
- `reliability_chart.png`
- `speedup_chart.png`

The Ray dashboard is exposed at `http://localhost:8265` while the cluster is
running.

## Run Locally Without Docker

```bash
cd pa9
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m src.main --local --trials 1000000 --chunks 8
```

## Test

```bash
cd pa9
pytest -q
```

## Presentation Outline

1. Application: estimate outage risk for replicated distributed services.
2. System: Ray cluster with a head node, worker nodes, and remote tasks.
3. Suitability: Monte Carlo chunks are independent and parallelizable.
4. Implementation: split trials, submit Ray tasks, aggregate outage counts.
5. Demo: compare serial runtime with Ray runtime and show charts.
