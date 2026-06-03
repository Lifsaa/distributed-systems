"""Save render artifacts: the fractal image, speedup chart, and CSV."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

from src.benchmark import RenderBenchmark


def _prepare(path: str | Path) -> Path:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def save_image(rgb: np.ndarray, output_path: str | Path) -> None:
    """Write an RGB image array to ``output_path`` as a PNG.

    Args:
        rgb: ``uint8`` array of shape ``(height, width, 3)``.
        output_path: Destination file path.
    """
    plt.imsave(_prepare(output_path), rgb)


def save_speedup_chart(benchmark: RenderBenchmark, output_path: str | Path) -> None:
    """Draw a serial-vs-Ray runtime bar chart and save it as a PNG.

    Args:
        benchmark: Completed benchmark holding both timed renders.
        output_path: Destination file path.
    """
    labels = ["Serial", "Ray"]
    seconds = [benchmark.serial.seconds, benchmark.parallel.seconds]

    fig, ax = plt.subplots(figsize=(6, 5))
    ax.bar(labels, seconds, color=["#d1495b", "#49a078"])
    ax.set_ylabel("Render time (seconds)")
    ax.set_title(f"Mandelbrot Render: {benchmark.speedup:.2f}x speedup with Ray")
    for index, value in enumerate(seconds):
        ax.text(index, value, f"{value:.2f}s", ha="center", va="bottom")
    fig.tight_layout()
    fig.savefig(_prepare(output_path))
    plt.close(fig)


def save_results_csv(benchmark: RenderBenchmark, output_path: str | Path) -> None:
    """Write benchmark numbers to a single-row CSV.

    Args:
        benchmark: Completed benchmark.
        output_path: Destination file path.
    """
    header = "width,height,max_iter,serial_seconds,ray_seconds,speedup"
    row = ",".join([
        str(benchmark.viewport.width),
        str(benchmark.viewport.height),
        str(benchmark.max_iter),
        f"{benchmark.serial.seconds:.4f}",
        f"{benchmark.parallel.seconds:.4f}",
        f"{benchmark.speedup:.2f}",
    ])
    _prepare(output_path).write_text(header + "\n" + row + "\n", encoding="utf-8")
