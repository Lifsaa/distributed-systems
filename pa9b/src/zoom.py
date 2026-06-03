"""Build a deep-zoom animation by rendering a sequence of viewports with Ray.

Each frame zooms a little further toward a fixed target point. The zoom factor
between frames is constant, so the scale shrinks geometrically, which the eye
reads as a steady dive into the set. Every frame is rendered on the Ray cluster
and the frames are stitched into a video.
"""

from __future__ import annotations

from pathlib import Path
from typing import Callable, Sequence

import imageio.v2 as imageio
import numpy as np

from src.coloring import DEFAULT_COLORMAP, colorize
from src.fractal import Viewport
from src.ray_runner import run_ray_render


def zoom_sequence(
    target_real: float,
    target_imag: float,
    start_scale: float,
    end_scale: float,
    frames: int,
    width: int,
    height: int,
) -> list[Viewport]:
    """Return ``frames`` viewports zooming from ``start_scale`` to ``end_scale``.

    The scale is interpolated geometrically so each step multiplies the zoom by
    a constant factor, giving a smooth approach toward ``(target_real,
    target_imag)``.
    """
    if frames <= 0:
        raise ValueError("frames must be greater than zero")
    if start_scale <= 0 or end_scale <= 0:
        raise ValueError("scales must be greater than zero")

    scales = np.geomspace(start_scale, end_scale, frames)
    return [
        Viewport(
            center_real=target_real,
            center_imag=target_imag,
            scale=float(scale),
            width=width,
            height=height,
        )
        for scale in scales
    ]


def _iterations_for(viewport: Viewport, start_scale: float, base_iter: int) -> int:
    """Scale the iteration cap up with zoom depth so deep frames keep detail.

    Detail near the boundary needs more iterations to resolve as the view
    shrinks, so the cap grows with the base-10 log of the zoom depth.
    """
    depth = start_scale / viewport.scale
    return int(base_iter * (1.0 + np.log10(max(depth, 1.0))))


def render_zoom(
    viewports: Sequence[Viewport],
    base_iter: int,
    tiles: int,
    output_path: str | Path,
    fps: int = 20,
    colormap: str = DEFAULT_COLORMAP,
    on_frame: Callable[[int, int], None] | None = None,
) -> Path:
    """Render each viewport with Ray and write the frames to a video file.

    Args:
        viewports: Ordered viewports, e.g. from :func:`zoom_sequence`.
        base_iter: Iteration cap for the first frame; deeper frames scale up.
        tiles: Number of Ray bands per frame.
        output_path: Destination video path (``.mp4`` or ``.gif``).
        fps: Frames per second of the output video.
        colormap: Matplotlib colormap name for every frame.
        on_frame: Optional progress callback ``(completed, total)``.

    Returns:
        The path the video was written to.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    start_scale = viewports[0].scale
    total = len(viewports)
    with imageio.get_writer(output_path, fps=fps) as writer:
        for index, viewport in enumerate(viewports):
            iterations = _iterations_for(viewport, start_scale, base_iter)
            escape = run_ray_render(viewport, iterations, tiles)
            writer.append_data(colorize(escape, iterations, colormap))
            if on_frame is not None:
                on_frame(index + 1, total)

    return output_path
