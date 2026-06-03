"""Time a render call and compare serial vs parallel performance."""

from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter
from typing import Callable

import numpy as np

from src.fractal import Viewport


@dataclass(frozen=True)
class TimedRender:
    """A rendered escape-time image paired with its wall-clock duration."""

    image: np.ndarray
    seconds: float


@dataclass(frozen=True)
class RenderBenchmark:
    """Serial-vs-parallel timings for the same viewport."""

    viewport: Viewport
    max_iter: int
    serial: TimedRender
    parallel: TimedRender

    @property
    def speedup(self) -> float:
        """Serial runtime divided by parallel runtime (0.0 if parallel was instant)."""
        if self.parallel.seconds == 0.0:
            return 0.0
        return self.serial.seconds / self.parallel.seconds


def time_call(func: Callable[[], np.ndarray]) -> TimedRender:
    """Run ``func`` and return its result image paired with elapsed seconds."""
    started = perf_counter()
    image = func()
    return TimedRender(image=image, seconds=perf_counter() - started)
