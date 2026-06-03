from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter
from typing import Callable

from src.simulation import ServiceScenario, SimulationResult, run_monte_carlo_trials


@dataclass(frozen=True)
class TimedResult:
    result: SimulationResult
    seconds: float


@dataclass(frozen=True)
class ScenarioBenchmark:
    scenario: ServiceScenario
    serial: TimedResult
    ray: TimedResult

    @property
    def speedup(self) -> float:
        if self.ray.seconds == 0.0:
            return 0.0
        return self.serial.seconds / self.ray.seconds


def split_trials(total_trials: int, chunks: int) -> list[int]:
    if total_trials < 0:
        raise ValueError("total_trials must be non-negative")
    if chunks <= 0:
        raise ValueError("chunks must be greater than zero")

    base = total_trials // chunks
    remainder = total_trials % chunks
    splits = [base + (1 if index < remainder else 0) for index in range(chunks)]
    return [split for split in splits if split > 0]


def time_call(func: Callable[[], SimulationResult]) -> TimedResult:
    started = perf_counter()
    result = func()
    return TimedResult(result=result, seconds=perf_counter() - started)


def run_serial_benchmark(
    scenario: ServiceScenario,
    total_trials: int,
    seed: int,
) -> TimedResult:
    return time_call(lambda: run_monte_carlo_trials(total_trials, scenario, seed=seed))
