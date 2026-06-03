import pytest

pytest.importorskip("imageio")

from src.zoom import zoom_sequence


def test_zoom_sequence_starts_and_ends_at_requested_scales():
    viewports = zoom_sequence(
        target_real=-0.743,
        target_imag=0.131,
        start_scale=1.5,
        end_scale=0.01,
        frames=10,
        width=80,
        height=60,
    )

    assert len(viewports) == 10
    assert viewports[0].scale == pytest.approx(1.5)
    assert viewports[-1].scale == pytest.approx(0.01)


def test_zoom_sequence_scale_decreases_monotonically():
    viewports = zoom_sequence(
        target_real=-0.743,
        target_imag=0.131,
        start_scale=1.5,
        end_scale=0.01,
        frames=10,
        width=80,
        height=60,
    )

    scales = [viewport.scale for viewport in viewports]
    assert all(later < earlier for earlier, later in zip(scales, scales[1:]))


def test_zoom_sequence_validates_inputs():
    with pytest.raises(ValueError, match="frames"):
        zoom_sequence(-0.743, 0.131, 1.5, 0.01, frames=0, width=80, height=60)
    with pytest.raises(ValueError, match="scales"):
        zoom_sequence(-0.743, 0.131, 0.0, 0.01, frames=10, width=80, height=60)
