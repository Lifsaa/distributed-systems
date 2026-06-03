import pytest

from src.tiling import split_rows


def test_bands_cover_image_exactly():
    bounds = split_rows(height=30, tiles=4)

    assert bounds[0][0] == 0
    assert bounds[-1][1] == 30
    for (_, end), (next_start, _) in zip(bounds, bounds[1:]):
        assert end == next_start


def test_band_sizes_differ_by_at_most_one():
    sizes = [end - start for start, end in split_rows(height=30, tiles=7)]

    assert max(sizes) - min(sizes) <= 1
    assert sum(sizes) == 30


def test_fewer_bands_returned_when_tiles_exceed_height():
    bounds = split_rows(height=3, tiles=8)

    assert bounds == [(0, 1), (1, 2), (2, 3)]


def test_invalid_height_rejected():
    with pytest.raises(ValueError, match="height"):
        split_rows(height=0, tiles=4)


def test_invalid_tiles_rejected():
    with pytest.raises(ValueError, match="tiles"):
        split_rows(height=10, tiles=0)
