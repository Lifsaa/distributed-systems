from __future__ import annotations

import argparse
from pathlib import Path

import ray

from src.benchmark import ScenarioBenchmark, split_trials, time_call, run_serial_benchmark
from src.plotting import save_reliability_chart, save_results_csv, save_speedup_chart
from src.ray_runner import run_ray_benchmark
from src.simulation import ServiceScenario


DEFAULT_SCENARIOS = [
    ServiceScenario(
        name="3 replicas need 2",
        replicas=3,
        min_alive=2,
        node_failure_probability=0.05,
    ),
    ServiceScenario(
        name="5 replicas need 3",
        replicas=5,
        min_alive=3,
        node_failure_probability=0.05,
    ),
    ServiceScenario(
        name="9 replicas 3 zones need 5",
        replicas=9,
        min_alive=5,
        node_failure_probability=0.05,
        zones=3,
        zone_failure_probability=0.01,
    ),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Estimate distributed service reliability with Ray and Monte Carlo simulation."
    )
    parser.add_argument("--trials", type=int, default=5_000_000)
    parser.add_argument("--chunks", type=int, default=16)
    parser.add_argument("--seed", type=int, default=469)
    parser.add_argument("--address", default="auto")
    parser.add_argument("--output-dir", default="outputs")
    parser.add_argument(
        "--local",
        action="store_true",
        help="Start a local Ray runtime instead of connecting to an existing cluster.",
    )
    return parser.parse_args()


def print_banner(args: argparse.Namespace) -> None:
    mode = "local Ray runtime" if args.local else f"Ray cluster at {args.address}"
    print("Ray Monte Carlo Reliability Simulator")
    print("=" * 42)
    print(f"Execution mode: {mode}")
    print(f"Trials per scenario: {args.trials:,}")
    print(f"Ray chunks per scenario: {args.chunks}")
    print()


def format_probability(value: float) -> str:
    return f"{value * 100:.4f}%"


def print_benchmark(benchmark: ScenarioBenchmark) -> None:
    ray_result = benchmark.ray.result
    print(f"Scenario: {benchmark.scenario.name}")
    print(f"  Outage probability: {format_probability(ray_result.outage_probability)}")
    print(f"  Availability:        {format_probability(ray_result.availability)}")
    print(f"  Serial runtime:      {benchmark.serial.seconds:.3f}s")
    print(f"  Ray runtime:         {benchmark.ray.seconds:.3f}s")
    print(f"  Speedup:             {benchmark.speedup:.2f}x")
    print()


def main() -> None:
    args = parse_args()
    print_banner(args)

    if args.local:
        ray.init(ignore_reinit_error=True)
    else:
        ray.init(args.address)

    try:
        output_dir = Path(args.output_dir)
        chunk_sizes = split_trials(args.trials, args.chunks)
        benchmarks = []

        for index, scenario in enumerate(DEFAULT_SCENARIOS):
            print(f"Running {scenario.name}...")
            serial = run_serial_benchmark(
                scenario=scenario,
                total_trials=args.trials,
                seed=args.seed + (index * 10_000),
            )
            ray_timed = time_call(
                lambda scenario=scenario, index=index: run_ray_benchmark(
                    scenario=scenario,
                    chunk_sizes=chunk_sizes,
                    base_seed=args.seed + (index * 10_000),
                )
            )
            benchmark = ScenarioBenchmark(
                scenario=scenario,
                serial=serial,
                ray=ray_timed,
            )
            benchmarks.append(benchmark)
            print_benchmark(benchmark)

        save_results_csv(benchmarks, output_dir / "results.csv")
        save_reliability_chart(benchmarks, output_dir / "reliability_chart.png")
        save_speedup_chart(benchmarks, output_dir / "speedup_chart.png")
        print(f"Saved outputs to {output_dir.resolve()}")
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
