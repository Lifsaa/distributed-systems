# PA09b: Ray Distributed Mandelbrot Renderer

Renders the Mandelbrot set in parallel across a Ray cluster by slicing the image into horizontal bands, distributing each band as a Ray task, and stitching the results back together. Includes a deep-zoom animation where every frame is a distributed render.

## Run

```bash
cd pa9b
docker compose up --build --abort-on-container-exit --exit-code-from app
```

Outputs land in `outputs/`: `fractal.png`, `speedup_chart.png`, `results.csv`.
Ray dashboard at <http://localhost:8265>, Grafana at <http://localhost:3000>.

## Zoom animation

```bash
EXTRA_ARGS="--zoom --frames 72" \
  docker compose up --build --abort-on-container-exit --exit-code-from app
```

## Test

```bash
pip install -r requirements.txt
pytest -q
```

## Layout

```
src/
  cli.py         CLI + render pipelines
  fractal.py     Viewport + escape-time math
  ray_runner.py  Ray remote task + band assembly
  tiling.py      Row splitting
  benchmark.py   Serial-vs-Ray timing
  zoom.py        Zoom sequence + video writer
  coloring.py    Escape-times to RGB
  artifacts.py   Save image / chart / CSV
  main.py        Entry point
monitoring/      Prometheus + Grafana config
presentation.html  10-slide demo deck
```
