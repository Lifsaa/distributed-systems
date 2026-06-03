from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt

from src.benchmark import ScenarioBenchmark


def save_results_csv(benchmarks: list[ScenarioBenchmark], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "scenario,total_trials,outages,outage_probability,availability,serial_seconds,ray_seconds,speedup"
    ]

    for benchmark in benchmarks:
        ray_result = benchmark.ray.result
        lines.append(
            ",".join(
                [
                    benchmark.scenario.name,
                    str(ray_result.trials),
                    str(ray_result.outages),
                    f"{ray_result.outage_probability:.8f}",
                    f"{ray_result.availability:.8f}",
                    f"{benchmark.serial.seconds:.4f}",
                    f"{benchmark.ray.seconds:.4f}",
                    f"{benchmark.speedup:.2f}",
                ]
            )
        )

    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def save_reliability_chart(
    benchmarks: list[ScenarioBenchmark],
    output_path: Path,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    names = [benchmark.scenario.name for benchmark in benchmarks]
    availability = [benchmark.ray.result.availability * 100 for benchmark in benchmarks]

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(names, availability, color=["#2f6f73", "#d98c3f", "#7c5fb3"])
    ax.set_ylabel("Estimated availability (%)")
    ax.set_title("Monte Carlo Reliability Estimates")
    ax.set_ylim(min(availability) - 1 if availability else 0, 100)
    ax.tick_params(axis="x", rotation=15)
    fig.tight_layout()
    fig.savefig(output_path)
    plt.close(fig)


def save_speedup_chart(
    benchmarks: list[ScenarioBenchmark],
    output_path: Path,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    names = [benchmark.scenario.name for benchmark in benchmarks]
    speedups = [benchmark.speedup for benchmark in benchmarks]

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(names, speedups, color=["#456990", "#49a078", "#d1495b"])
    ax.axhline(1.0, color="#444444", linestyle="--", linewidth=1)
    ax.set_ylabel("Speedup vs serial")
    ax.set_title("Ray Monte Carlo Speedup")
    ax.tick_params(axis="x", rotation=15)
    fig.tight_layout()
    fig.savefig(output_path)
    plt.close(fig)
