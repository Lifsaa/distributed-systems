"""Divide an image into horizontal bands for parallel rendering."""

from __future__ import annotations


def split_rows(height: int, tiles: int) -> list[tuple[int, int]]:
    """Split ``height`` rows into at most ``tiles`` contiguous bands.

    Rows are divided as evenly as possible, with the remainder spread one row
    at a time across the leading bands. The returned ``(row_start, row_end)``
    pairs exactly cover ``[0, height)`` with no gaps or overlap.

    Args:
        height: Total number of pixel rows in the image.
        tiles: Maximum number of bands to produce.

    Returns:
        List of ``(row_start, row_end)`` tuples, each defining a half-open
        row interval. Fewer than ``tiles`` bands are returned when
        ``tiles > height``.
    """
    if height <= 0:
        raise ValueError("height must be greater than zero")
    if tiles <= 0:
        raise ValueError("tiles must be greater than zero")

    base = height // tiles
    remainder = height % tiles
    bounds: list[tuple[int, int]] = []
    start = 0
    for index in range(tiles):
        size = base + (1 if index < remainder else 0)
        if size == 0:
            break
        bounds.append((start, start + size))
        start += size
    return bounds
