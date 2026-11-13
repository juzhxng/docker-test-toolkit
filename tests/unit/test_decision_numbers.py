"""The numbers a refusal, wait or breach records: a closed set of named non-negative integers."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from docker_test_toolkit.application import lifecycle
from docker_test_toolkit.application.lifecycle import CapacitySnapshot
from docker_test_toolkit.domain import numbers, policy
from docker_test_toolkit.domain.models import PhaseBudget
from docker_test_toolkit.domain.policy import (
    CapacityDecision,
    CapacityState,
    DiskFigure,
    PhaseReservation,
    QueueEntryView,
)

GIB = 1024**3


def test_numbers_are_a_closed_set_of_non_negative_integers() -> None:
    assert numbers.valid_numbers({"external_bytes": GIB, "external_containers": 1})


@pytest.mark.parametrize(
    "value",
    [
        {"shoe_size": 1},  # not a decision operand
        {"holders": True},  # a bool is an int to Python, never a number here
        {"holders": "1"},
        {"holders": 1.5},
        {"holders": -1},
        {"holders": {"n": 1}},
        {},
        dict.fromkeys(sorted(numbers.NUMBER_KEYS)[: numbers.MAX_NUMBERS + 1], 1),
        [("holders", 1)],
        None,
    ],
)
def test_anything_else_is_refused(value: Any) -> None:
    assert not numbers.valid_numbers(value)


def test_numbers_render_bytes_in_gib_and_counts_as_integers() -> None:
    rendered = numbers.render_numbers(
        {"external_bytes": GIB, "external_containers": 1, "daemon_memory_bytes": 5 * GIB // 2}
    )

    assert rendered == "daemon_memory=2.50GiB external=1.00GiB external_containers=1"


def test_an_operand_that_is_not_a_non_negative_integer_is_left_out() -> None:
    values = {"headroom_bytes": -1, "host_memory_bytes": GIB, "host_swap_used_bytes": 0, "swapout_bytes": None}

    assert numbers.numbers_for("invalid-memory-observation", values) == {
        "host_memory_bytes": GIB,
        "host_swap_used_bytes": 0,
    }
    assert numbers.numbers_for("docker said no", {"holders": 1}) == {}


_MEMORY = {
    "guest_needed_bytes",
    "daemon_memory_bytes",
    "host_needed_bytes",
    "host_memory_bytes",
    "container_bytes",
    "host_process_bytes",
    "host_reserve_bytes",
}
_EXTERNAL = {*_MEMORY, "external_bytes", "external_containers"}
_CPU = {"daemon_cpus_needed", "daemon_cpus", "host_threads_needed", "host_cpus"}


def _snapshot(daemon: float = 64, cpus: int = 8, external: tuple[int | None, ...] = ()) -> CapacitySnapshot:
    return CapacitySnapshot(int(daemon * GIB), cpus, 64 * GIB, 8, "native", external, runtime="docker-engine")


def _sample(tick: int, *, headroom: int = 8 * GIB, swap: int = 0, swapout: int = 0, physical: int = 16 * GIB):
    return lifecycle.HostMemoryObservation("boot", tick, physical, headroom, swap, swapout, 4096, {})


_WINDOW = 30_000_000_000


def _view(run_id: str, ticket: int, phase: str) -> QueueEntryView:
    return QueueEntryView(run_id, ticket, phase, "project", False, "executing")


def _oom_record() -> dict[str, Any]:
    observation = {"memory_limit_bytes": GIB, "oom_killed": False, "oom_events": 2}
    managed = {"memory_bytes": GIB, "observation": observation}  # the limit sits beside the observation, as recorded
    return {"resources": {"containers": {"c" * 64: managed}}, "owned_resources": {"containers": {}}}


DECISIONS: dict[str, tuple[Callable[[], CapacityDecision], set[str]]] = {
    "external-capacity": (
        lambda: lifecycle.assess_capacity(PhaseBudget(2 * GIB, 0, 1, 1), _snapshot(3, external=(GIB,))),
        _EXTERNAL,
    ),
    "unknown-external-capacity": (
        lambda: lifecycle.assess_capacity(PhaseBudget(GIB, 0, 1, 1), _snapshot(external=(None, GIB))),
        {"unlimited_containers", "external_containers"},
    ),
    "unsupported-base-capacity": (
        lambda: lifecycle.assess_capacity(PhaseBudget(2 * GIB, 0, 1, 1), _snapshot(2.25)),
        _MEMORY,
    ),
    "insufficient-cpu": (lambda: lifecycle.assess_capacity(PhaseBudget(GIB, 0, 9, 1), _snapshot()), _CPU),
    "coholder-capacity-memory": (
        lambda: lifecycle.admission_decision(
            {"tests": PhaseBudget(2 * GIB, 0, 1, 1)}, _snapshot(4), [PhaseReservation(2 * GIB, 0, 1, 1)]
        ),
        (_EXTERNAL) | {"coholders", "coholder_container_bytes", "coholder_host_process_bytes"},
    ),
    "coholder-capacity-cpu": (
        lambda: lifecycle.admission_decision(
            {"tests": PhaseBudget(GIB, 0, 2, 1)}, _snapshot(cpus=3), [PhaseReservation(GIB, 0, 2, 1)]
        ),
        _CPU,
    ),
    "native-headroom": (
        lambda: lifecycle.assess_pressure(_sample(_WINDOW, headroom=GIB), _sample(0)),
        {"headroom_bytes", "headroom_floor_bytes", "host_memory_bytes"},
    ),
    "swapout-rate": (
        lambda: lifecycle.assess_pressure(_sample(_WINDOW, swapout=8 * GIB), _sample(0)),
        {"swapout_bytes_per_s", "swapout_limit_bytes_per_s", "window_ns"},
    ),
    "admission-swap-growth": (
        lambda: lifecycle.assess_pressure(
            _sample(_WINDOW, swap=4 * GIB),
            _sample(0),
            granted=True,
            admission_baseline=_sample(0),
            phase_baseline=_sample(0),
        ),
        {"swap_growth_bytes", "swap_growth_limit_bytes", "growth_allowance_bytes", "host_swap_used_bytes"},
    ),
    "invalid-memory-observation": (
        lambda: lifecycle.assess_pressure(_sample(_WINDOW, swap=-1), _sample(0)),
        {"headroom_bytes", "host_memory_bytes", "swapout_bytes"},  # the invalid swap figure is left out
    ),
    "telemetry-discontinuity": (
        lambda: lifecycle.assess_pressure(_sample(_WINDOW), _sample(0, physical=8 * GIB)),
        {"host_memory_bytes", "baseline_host_memory_bytes", "swapout_bytes", "baseline_swapout_bytes"},
    ),
    "disk-capacity-data-root": (
        lambda: policy.disk_fit(DiskFigure("data-root", 10 * GIB, 50 * GIB, 2 * GIB, "measured"), 0, 60 * GIB),
        {"data_root_free_bytes", "data_root_total_bytes", "data_root_reserve_bytes", "estimate_bytes"}
        | {"other_leases_bytes"},
    ),
    "disk-capacity-private-root": (
        lambda: policy.private_root_fit(GIB // 2),
        {"private_root_free_bytes", "private_root_reserve_bytes"},
    ),
    "queue-position": (
        lambda: CapacityDecision(
            CapacityState.WAITING,
            "queue-position",
            numbers=policy.holder_numbers([_view("a", 1, "granted"), _view("b", 2, "queued")], 1),
        ),
        {"holders", "limit"},
    ),
    "cpu-pressure": (
        lambda: CapacityDecision(CapacityState.WAITING, "cpu-pressure", numbers=policy.cpu_pressure_numbers(62.5)),
        {"cpu_pressure_permille", "cpu_pressure_limit_permille"},
    ),
    "container-oom": (
        lambda: CapacityDecision(CapacityState.REFUSED, "container-oom", numbers=numbers.oom_numbers(_oom_record())),
        {"container_limit_bytes", "oom_events"},
    ),
    "incomplete-pressure-window": (lambda: lifecycle.assess_pressure(_sample(1), _sample(0)), set()),
    "static-capacity": (lambda: lifecycle.assess_capacity(PhaseBudget(GIB, 0, 1, 1), _snapshot()), set()),
}


@pytest.mark.parametrize("name", sorted(DECISIONS))
def test_every_decision_carries_exactly_its_numbers(name: str) -> None:
    produce, keys = DECISIONS[name]

    decision = produce()

    assert set(decision.numbers) == keys
    assert not decision.numbers or numbers.valid_numbers(dict(decision.numbers))


def test_the_numbers_are_the_operands_that_decided() -> None:
    external = lifecycle.assess_capacity(PhaseBudget(2 * GIB, 0, 1, 1), _snapshot(3, external=(GIB,)))
    headroom = lifecycle.assess_pressure(_sample(_WINDOW, headroom=GIB), _sample(0))
    cpu = lifecycle.admission_decision(
        {"tests": PhaseBudget(GIB, 0, 2, 1)}, _snapshot(cpus=3), [PhaseReservation(GIB, 0, 2, 1)]
    )

    assert (external.reason, external.numbers["external_bytes"], external.numbers["external_containers"]) == (
        "external-capacity",
        GIB,
        1,
    )
    assert external.numbers["daemon_memory_bytes"] == 3 * GIB
    assert (headroom.numbers["headroom_bytes"], headroom.numbers["headroom_floor_bytes"]) == (GIB, 3 * GIB // 2)
    assert (cpu.reason, cpu.numbers["daemon_cpus_needed"], cpu.numbers["daemon_cpus"]) == ("coholder-capacity", 4, 3)
