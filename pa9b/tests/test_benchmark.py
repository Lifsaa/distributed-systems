import numpy as np

from src.benchmark import RenderBenchmark, TimedRender, time_call
from src.fractal import Viewport


def _viewport() -> Viewport:
    return Viewport(center_real=-0.5, center_imag=0.0, scale=1.5, width=10, height=10)


def test_speedup_divides_serial_by_parallel():
    image = np.zeros((10, 10), dtype=np.int32)
    benchmark = RenderBenchmark(
        viewport=_viewport(),
        max_iter=50,
        serial=TimedRender(image=image, seconds=4.0),
        parallel=TimedRender(image=image, seconds=2.0),
    )

    assert benchmark.speedup == 2.0


def test_speedup_zero_when_parallel_is_instant():
    image = np.zeros((10, 10), dtype=np.int32)
    benchmark = RenderBenchmark(
        viewport=_viewport(),
        max_iter=50,
        serial=TimedRender(image=image, seconds=4.0),
        parallel=TimedRender(image=image, seconds=0.0),
    )

    assert benchmark.speedup == 0.0


def test_time_call_captures_result_and_duration():
    sentinel = np.arange(9).reshape(3, 3)

    timed = time_call(lambda: sentinel)

    assert np.array_equal(timed.image, sentinel)
    assert timed.seconds >= 0.0
