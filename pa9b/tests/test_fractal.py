import numpy as np
import pytest

from src.fractal import Viewport, compute_band, render


def test_origin_is_in_the_set():
    # c = 0 never escapes, so its escape-time saturates at max_iter.
    viewport = Viewport(center_real=0.0, center_imag=0.0, scale=0.001, width=1, height=1)

    escape = compute_band(viewport, 0, 1, max_iter=50)

    assert escape[0, 0] == 50


def test_point_far_outside_escapes_immediately():
    # c = 2 leaves the escape radius after a single iteration.
    viewport = Viewport(center_real=2.0, center_imag=0.0, scale=0.001, width=1, height=1)

    escape = compute_band(viewport, 0, 1, max_iter=50)

    assert escape[0, 0] < 50


def test_bands_concatenate_into_the_full_render():
    viewport = Viewport(center_real=-0.5, center_imag=0.0, scale=1.5, width=40, height=30)

    full = render(viewport, max_iter=60)
    top = compute_band(viewport, 0, 15, max_iter=60)
    bottom = compute_band(viewport, 15, 30, max_iter=60)

    assert np.array_equal(full, np.vstack([top, bottom]))


def test_invalid_scale_rejected():
    with pytest.raises(ValueError, match="scale"):
        Viewport(center_real=0.0, center_imag=0.0, scale=0.0, width=10, height=10)


def test_invalid_dimensions_rejected():
    with pytest.raises(ValueError, match="width and height"):
        Viewport(center_real=0.0, center_imag=0.0, scale=1.0, width=0, height=10)


def test_band_outside_image_rejected():
    viewport = Viewport(center_real=0.0, center_imag=0.0, scale=1.0, width=10, height=10)

    with pytest.raises(ValueError, match="band"):
        compute_band(viewport, 0, 11, max_iter=10)
