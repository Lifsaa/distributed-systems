import numpy as np
import pytest

ray = pytest.importorskip("ray")

from src.fractal import Viewport, render
from src.ray_runner import run_ray_render


def test_ray_render_matches_serial():
    ray.init(local_mode=True, ignore_reinit_error=True)
    try:
        viewport = Viewport(center_real=-0.5, center_imag=0.0, scale=1.5, width=40, height=30)

        expected = render(viewport, max_iter=60)
        actual = run_ray_render(viewport, max_iter=60, tiles=4)

        assert np.array_equal(expected, actual)
    finally:
        ray.shutdown()
