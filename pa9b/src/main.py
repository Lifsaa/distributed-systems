"""Command-line entry point for the Ray Mandelbrot renderer.

Default run: render one image serially and with Ray, print the speedup, and
save the fractal image, a speedup chart, and a CSV. With ``--zoom`` it renders
a deep-zoom animation, one Ray-distributed frame at a time, into a video file.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import ray

from src.artifacts import save_image, save_results_csv, save_speedup_chart
from src.benchmark import RenderBenchmark, time_call
from src.coloring import DEFAULT_COLORMAP, colorize
from src.fractal import Viewport, render
from src.ray_runner import run_ray_render
from src.zoom import render_zoom, zoom_sequence

# A point in "Seahorse Valley" that stays interesting under deep zoom.
ZOOM_TARGET_REAL = -0.743643887037151
ZOOM_TARGET_IMAG = 0.131825904205330


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Render the Mandelbrot set on a Ray cluster."
    )
    parser.add_argument("--width", type=int, default=1600)
    parser.add_argument("--height", type=int, default=1200)
    parser.add_argument("--max-iter", type=int, default=300)
    parser.add_argument("--tiles", type=int, default=16)
    parser.add_argument("--center-real", type=float, default=-0.5)
    parser.add_argument("--center-imag", type=float, default=0.0)
    parser.add_argument("--scale", type=float, default=1.5)
    parser.add_argument("--colormap", default=DEFAULT_COLORMAP)
    parser.add_argument("--address", default="auto")
    parser.add_argument("--output-dir", default="outputs")
    parser.add_argument(
        "--local",
        action="store_true",
        help="Start a local Ray runtime instead of connecting to a cluster.",
    )
    parser.add_argument(
        "--zoom",
        action="store_true",
        help="Render a deep-zoom animation instead of a single image.",
    )
    parser.add_argument("--frames", type=int, default=120)
    parser.add_argument("--end-scale", type=float, default=5e-4)
    parser.add_argument("--fps", type=int, default=20)
    return parser.parse_args()


def init_ray(args: argparse.Namespace) -> None:
    """Connect to a Ray cluster, or start a local runtime with ``--local``."""
    if args.local:
        ray.init(ignore_reinit_error=True)
    else:
        ray.init(args.address)


def run_single(args: argparse.Namespace) -> None:
    """Render one image serially and with Ray, then save the artifacts."""
    viewport = Viewport(
        center_real=args.center_real,
        center_imag=args.center_imag,
        scale=args.scale,
        width=args.width,
        height=args.height,
    )

    print("Ray Mandelbrot Renderer")
    print("=" * 42)
    print(f"Image: {args.width}x{args.height}, max_iter={args.max_iter}, tiles={args.tiles}")
    print()

    serial = time_call(lambda: render(viewport, args.max_iter))
    parallel = time_call(lambda: run_ray_render(viewport, args.max_iter, args.tiles))
    benchmark = RenderBenchmark(
        viewport=viewport,
        max_iter=args.max_iter,
        serial=serial,
        parallel=parallel,
    )

    output_dir = Path(args.output_dir)
    save_image(colorize(parallel.image, args.max_iter, args.colormap), output_dir / "fractal.png")
    save_speedup_chart(benchmark, output_dir / "speedup_chart.png")
    save_results_csv(benchmark, output_dir / "results.csv")

    print(f"  Serial render: {serial.seconds:.3f}s")
    print(f"  Ray render:    {parallel.seconds:.3f}s")
    print(f"  Speedup:       {benchmark.speedup:.2f}x")
    print()
    print(f"Saved outputs to {output_dir.resolve()}")


def run_zoom(args: argparse.Namespace) -> None:
    """Render a deep-zoom animation, one Ray-distributed frame at a time."""
    viewports = zoom_sequence(
        target_real=ZOOM_TARGET_REAL,
        target_imag=ZOOM_TARGET_IMAG,
        start_scale=args.scale,
        end_scale=args.end_scale,
        frames=args.frames,
        width=args.width,
        height=args.height,
    )

    output_dir = Path(args.output_dir)
    video_path = output_dir / "zoom.mp4"

    print("Ray Mandelbrot Renderer (zoom)")
    print("=" * 42)
    print(f"Rendering {args.frames} frames into {video_path.name}...")

    def report(completed: int, total: int) -> None:
        print(f"  frame {completed}/{total}", end="\r", flush=True)

    render_zoom(
        viewports,
        base_iter=args.max_iter,
        tiles=args.tiles,
        output_path=video_path,
        fps=args.fps,
        colormap=args.colormap,
        on_frame=report,
    )
    print()
    print(f"Saved zoom animation to {video_path.resolve()}")


def main() -> None:
    args = parse_args()
    init_ray(args)
    try:
        if args.zoom:
            run_zoom(args)
        else:
            run_single(args)
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
