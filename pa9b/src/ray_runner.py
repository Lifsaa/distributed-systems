"""Distribute a fractal render across a Ray cluster.

The image is sliced into horizontal bands; each band is rendered by an
independent Ray task and the driver stitches the bands back into one array.
Because the bands share no state, the workload is embarrassingly parallel —
exactly what Ray Core is built for:

- ``ray.init("ray://ray-head:10001")`` connects the app to the cluster.
- ``@ray.remote`` marks a band render as distributed work.
- ``.remote()`` submits each band to a Ray worker.
- ``ray.get()`` gathers the rendered bands for final assembly.
"""

from __future__ import annotations

import numpy as np
import ray

from src.fractal import Viewport, compute_band
from src.tiling import split_rows


@ray.remote
def render_band(
    viewport: Viewport,
    row_start: int,
    row_end: int,
    max_iter: int,
) -> np.ndarray:
    """Render a single horizontal band as a Ray task.

    Args:
        viewport: The region of the complex plane being rendered.
        row_start: First pixel row in the band (inclusive).
        row_end: Last pixel row in the band (exclusive).
        max_iter: Iteration cap before a pixel is treated as in-set.

    Returns:
        Escape-time array of shape ``(row_end - row_start, viewport.width)``.
    """
    return compute_band(viewport, row_start, row_end, max_iter)


def run_ray_render(viewport: Viewport, max_iter: int, tiles: int) -> np.ndarray:
    """Render ``viewport`` by fanning bands out to Ray workers.

    Submits one Ray task per band, gathers the results with ``ray.get``, and
    vertically stacks them into the full escape-time image.

    Args:
        viewport: The region of the complex plane to render.
        max_iter: Iteration cap for the Mandelbrot computation.
        tiles: Number of horizontal bands to distribute across workers.

    Returns:
        Escape-time array of shape ``(viewport.height, viewport.width)``.
    """
    bands = split_rows(viewport.height, tiles)
    futures = [
        render_band.remote(viewport, row_start, row_end, max_iter)
        for row_start, row_end in bands
    ]
    return np.vstack(ray.get(futures))
