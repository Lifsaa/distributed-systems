"""Core Mandelbrot math.

A :class:`Viewport` describes a rectangular window onto the complex plane and
the pixel resolution it is sampled at. :func:`compute_band` evaluates the
escape-time of every pixel in a horizontal strip of that window using
vectorized NumPy.

The escape-time of a pixel is the iteration at which ``z = z**2 + c`` first
leaves the escape radius (``|z| > 2``), starting from ``z = 0``. Pixels that
never escape within ``max_iter`` iterations are treated as members of the
Mandelbrot set. The computation is fully deterministic: there is no randomness,
so the same viewport always yields the same image.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

ESCAPE_RADIUS = 2.0


@dataclass(frozen=True)
class Viewport:
    """A rectangular window onto the complex plane sampled at pixel resolution.

    Attributes:
        center_real: Real part of the window center.
        center_imag: Imaginary part of the window center.
        scale: Half-width of the window along the real axis. Smaller values
            zoom further in. The imaginary half-height is derived from the
            pixel aspect ratio so the image is never stretched.
        width: Image width in pixels.
        height: Image height in pixels.
    """

    center_real: float
    center_imag: float
    scale: float
    width: int
    height: int

    def __post_init__(self) -> None:
        if self.scale <= 0:
            raise ValueError("scale must be greater than zero")
        if self.width <= 0 or self.height <= 0:
            raise ValueError("width and height must be greater than zero")

    @property
    def scale_imag(self) -> float:
        """Half-height of the window, matched to the pixel aspect ratio."""
        return self.scale * (self.height / self.width)

    def real_axis(self) -> np.ndarray:
        """Return the real coordinate of each pixel column."""
        return np.linspace(
            self.center_real - self.scale,
            self.center_real + self.scale,
            self.width,
        )

    def imag_axis(self) -> np.ndarray:
        """Return the imaginary coordinate of each pixel row."""
        return np.linspace(
            self.center_imag - self.scale_imag,
            self.center_imag + self.scale_imag,
            self.height,
        )


def compute_band(
    viewport: Viewport,
    row_start: int,
    row_end: int,
    max_iter: int,
) -> np.ndarray:
    """Compute escape-times for the rows ``[row_start, row_end)``.

    Args:
        viewport: The region of the complex plane being rendered.
        row_start: First pixel row in the band (inclusive).
        row_end: Last pixel row in the band (exclusive).
        max_iter: Iteration cap before a pixel is treated as part of the set.

    Returns:
        An ``int32`` array of shape ``(row_end - row_start, viewport.width)``
        holding the escape iteration of each pixel. In-set pixels hold
        ``max_iter``.
    """
    if not 0 <= row_start <= row_end <= viewport.height:
        raise ValueError("band must satisfy 0 <= row_start <= row_end <= height")
    if max_iter <= 0:
        raise ValueError("max_iter must be greater than zero")

    real = viewport.real_axis()[np.newaxis, :]
    imag = viewport.imag_axis()[row_start:row_end, np.newaxis]
    c = real + 1j * imag

    z = np.zeros_like(c)
    escape = np.full(c.shape, max_iter, dtype=np.int32)
    active = np.ones(c.shape, dtype=bool)

    for iteration in range(max_iter):
        z[active] = z[active] * z[active] + c[active]
        escaped = active & (np.abs(z) > ESCAPE_RADIUS)
        escape[escaped] = iteration
        active &= ~escaped
        if not active.any():
            break

    return escape


def render(viewport: Viewport, max_iter: int) -> np.ndarray:
    """Render the whole image serially as a single band.

    This is the single-process baseline that the Ray renderer is compared
    against.
    """
    return compute_band(viewport, 0, viewport.height, max_iter)
