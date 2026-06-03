# PA09b: Ray Distributed Mandelbrot Renderer

This project uses **Ray** to render the Mandelbrot set in parallel across a
cluster. The image is sliced into horizontal bands, each band is rendered by an
independent Ray task on a worker, and the driver stitches the bands back into
the final picture. It also renders a **deep-zoom animation** where every frame
is a distributed render.

## The Four Questions

1. **What is the task?**
   Render high-resolution Mandelbrot images (and a deep-zoom video) by
   computing the escape-time of every pixel.

2. **What system, and how does it work?**
   Ray Core, a distributed execution engine. A *head* node schedules work,
   *worker* nodes run tasks. A function marked `@ray.remote` becomes a task;
   `.remote()` submits it and returns a future; `ray.get()` collects the
   results. Ray handles scheduling, data transfer and fault tolerance.

3. **Why is Ray suitable?**
   Every pixel is computed independently (`z = z**2 + c` iterated until it
   escapes), so the work is *embarrassingly parallel*. Splitting the image into
   bands gives many independent tasks with no shared state — an ideal fit.

4. **How did we use Ray?**
   - `ray.init("ray://ray-head:10001")` connects to the cluster.
   - `@ray.remote` marks a band render as distributed work.
   - `.remote()` submits one task per band.
   - `ray.get()` gathers the bands, which the driver stacks into the image.

## Project Layout

```
src/
  fractal.py     # Viewport + vectorized escape-time math (pure NumPy)
  coloring.py    # escape-times -> RGB image
  ray_runner.py  # @ray.remote band render, row splitting, assembly
  benchmark.py   # serial-vs-Ray timing and speedup
  zoom.py        # deep-zoom frame sequence + video writer
  plotting.py    # save image, speedup chart and CSV
  main.py        # CLI entry point
```

## Run With Docker Compose

Single image (serial vs Ray, saves `fractal.png` + speedup chart):

```bash
cd pa9b
docker compose up --build --abort-on-container-exit --exit-code-from app
```

Bigger image for a stronger speedup (measured ~3.6x on a 5-CPU local cluster —
more tiles balance the load, the larger render amortizes Ray's overhead):

```bash
WIDTH=2400 HEIGHT=1800 MAX_ITER=600 TILES=64 \
  docker compose up --build --abort-on-container-exit --exit-code-from app
```

Deep-zoom animation (`zoom.mp4`):

```bash
EXTRA_ARGS="--zoom --frames 150" \
  docker compose up --build --abort-on-container-exit --exit-code-from app
```

Outputs are written to `outputs/`:

- `fractal.png` — the rendered image
- `speedup_chart.png` — serial vs Ray runtime
- `results.csv` — the benchmark numbers
- `zoom.mp4` — the zoom animation (zoom mode)

The Ray dashboard is at <http://localhost:8265> while the cluster is running —
open it during the demo to watch tasks fan out across workers.

## Run Locally Without Docker

```bash
cd pa9b
python -m venv .venv
.venv\Scripts\activate          # Windows  (use: source .venv/bin/activate on macOS/Linux)
pip install -r requirements.txt

python -m src.main --local --width 1200 --height 900 --max-iter 300 --tiles 8
python -m src.main --local --zoom --frames 80
```

## Test

```bash
cd pa9b
pytest -q
```

## Demo Script (tomorrow)

1. Pre-render the zoom video ahead of time so it can't fail live.
2. On stage, run a single high-res render and read out the serial-vs-Ray
   speedup.
3. Open the Ray dashboard (`localhost:8265`) to show tasks distributed across
   workers.
4. Play the zoom video as the finale.
