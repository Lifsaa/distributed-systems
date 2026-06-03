"""Translate raw escape-times into a colored RGB image."""

from __future__ import annotations

import numpy as np
from matplotlib import colormaps
from matplotlib.colors import Normalize

DEFAULT_COLORMAP = "twilight_shifted"


def colorize(
    escape: np.ndarray,
    max_iter: int,
    colormap: str = DEFAULT_COLORMAP,
) -> np.ndarray:
    """Map escape-times to an 8-bit RGB image.

    Pixels that never escaped (``escape == max_iter``) are painted black to
    mark the interior of the set; every other pixel is colored by how quickly
    it escaped.

    Args:
        escape: Escape-time array of shape ``(height, width)``.
        max_iter: The iteration cap used during rendering.
        colormap: Any registered matplotlib colormap name.

    Returns:
        A ``uint8`` array of shape ``(height, width, 3)``.
    """
    in_set = escape >= max_iter
    normalized = Normalize(vmin=0, vmax=max_iter)(escape)
    rgba = colormaps[colormap](normalized)
    rgb = (rgba[:, :, :3] * 255).astype(np.uint8)
    rgb[in_set] = 0
    return rgb
