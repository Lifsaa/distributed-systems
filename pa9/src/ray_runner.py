from __future__ import annotations

from typing import Iterable

try:
    import ray
except ImportError:
    ray = None

from src.simulation import (
    ServiceScenario,
    SimulationResult,
    aggregate_results,
    run_monte_carlo_trials,
)


def _simulate_chunk(
    num_trials: int,
    scenario: ServiceScenario,
    seed: int,
) -> SimulationResult:
    return run_monte_carlo_trials(num_trials, scenario, seed=seed)


simulate_chunk = ray.remote(_simulate_chunk) if ray is not None else None


def build_task_plan(chunk_sizes: Iterable[int], base_seed: int) -> list[tuple[int, int]]:
    return [
        (chunk_size, base_seed + index)
        for index, chunk_size in enumerate(chunk_sizes)
    ]


def run_ray_benchmark(
    scenario: ServiceScenario,
    chunk_sizes: list[int],
    base_seed: int,
) -> SimulationResult:
    if ray is None or simulate_chunk is None:
        raise RuntimeError("Ray is not installed. Install requirements.txt or run with Docker.")

    task_plan = build_task_plan(chunk_sizes, base_seed)
    futures = [
        simulate_chunk.remote(chunk_size, scenario, seed)
        for chunk_size, seed in task_plan
    ]
    return aggregate_results(ray.get(futures))
