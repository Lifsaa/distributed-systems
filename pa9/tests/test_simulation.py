import pytest

from src.simulation import (
    ServiceScenario,
    SimulationResult,
    aggregate_results,
    run_monte_carlo_trials,
)


def test_zero_failure_probability_has_no_outages():
    scenario = ServiceScenario(
        name="three replicas",
        replicas=3,
        min_alive=2,
        node_failure_probability=0.0,
    )

    result = run_monte_carlo_trials(100, scenario, seed=42)

    assert result == SimulationResult(trials=100, outages=0)


def test_full_failure_probability_outages_every_trial_when_service_needs_alive_nodes():
    scenario = ServiceScenario(
        name="three replicas",
        replicas=3,
        min_alive=1,
        node_failure_probability=1.0,
    )

    result = run_monte_carlo_trials(100, scenario, seed=42)

    assert result == SimulationResult(trials=100, outages=100)


def test_zone_failure_can_take_down_every_replica_in_failed_zone():
    scenario = ServiceScenario(
        name="three zones",
        replicas=6,
        min_alive=5,
        node_failure_probability=0.0,
        zones=3,
        zone_failure_probability=1.0,
    )

    result = run_monte_carlo_trials(50, scenario, seed=7)

    assert result == SimulationResult(trials=50, outages=50)


def test_invalid_min_alive_rejected():
    with pytest.raises(ValueError, match="min_alive"):
        ServiceScenario(
            name="invalid",
            replicas=3,
            min_alive=4,
            node_failure_probability=0.1,
        )


def test_zone_count_must_divide_replicas():
    with pytest.raises(ValueError, match="divide"):
        ServiceScenario(
            name="invalid zones",
            replicas=5,
            min_alive=3,
            node_failure_probability=0.1,
            zones=2,
            zone_failure_probability=0.01,
        )


def test_aggregate_results_combines_trials_and_outages():
    result = aggregate_results(
        [
            SimulationResult(trials=10, outages=1),
            SimulationResult(trials=15, outages=3),
        ]
    )

    assert result.trials == 25
    assert result.outages == 4
    assert result.outage_probability == pytest.approx(0.16)
    assert result.availability == pytest.approx(0.84)
