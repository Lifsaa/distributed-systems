from __future__ import annotations

from dataclasses import dataclass
import random


@dataclass(frozen=True)
class ServiceScenario:
    name: str
    replicas: int
    min_alive: int
    node_failure_probability: float
    zones: int = 1
    zone_failure_probability: float = 0.0

    def __post_init__(self) -> None:
        if self.replicas <= 0:
            raise ValueError("replicas must be greater than zero")
        if self.min_alive < 0 or self.min_alive > self.replicas:
            raise ValueError("min_alive must be between 0 and replicas")
        if not 0.0 <= self.node_failure_probability <= 1.0:
            raise ValueError("node_failure_probability must be between 0 and 1")
        if self.zones <= 0:
            raise ValueError("zones must be greater than zero")
        if self.replicas % self.zones != 0:
            raise ValueError("zones must divide replicas evenly")
        if not 0.0 <= self.zone_failure_probability <= 1.0:
            raise ValueError("zone_failure_probability must be between 0 and 1")


@dataclass(frozen=True)
class SimulationResult:
    trials: int
    outages: int

    @property
    def outage_probability(self) -> float:
        if self.trials == 0:
            return 0.0
        return self.outages / self.trials

    @property
    def availability(self) -> float:
        return 1.0 - self.outage_probability


def run_monte_carlo_trials(
    num_trials: int,
    scenario: ServiceScenario,
    seed: int | None = None,
) -> SimulationResult:
    if num_trials < 0:
        raise ValueError("num_trials must be non-negative")

    rng = random.Random(seed)
    outages = 0
    replicas_per_zone = scenario.replicas // scenario.zones

    for _ in range(num_trials):
        failed_zones = [
            rng.random() < scenario.zone_failure_probability
            for _ in range(scenario.zones)
        ]
        alive = 0

        for replica in range(scenario.replicas):
            zone_index = replica // replicas_per_zone
            if failed_zones[zone_index]:
                continue
            if rng.random() > scenario.node_failure_probability:
                alive += 1

        if alive < scenario.min_alive:
            outages += 1

    return SimulationResult(trials=num_trials, outages=outages)


def aggregate_results(results: list[SimulationResult]) -> SimulationResult:
    return SimulationResult(
        trials=sum(result.trials for result in results),
        outages=sum(result.outages for result in results),
    )
