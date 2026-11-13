"""The ``failure_evidence`` run-record key: shape validation and the first-wins writer."""

from __future__ import annotations

import copy
from typing import Any

import pytest

from docker_test_toolkit.application import lifecycle
from docker_test_toolkit.domain.errors import ResourceError
from docker_test_toolkit.domain.records import validate_failure_evidence
from docker_test_toolkit.domain.telemetry import TelemetryFault


def _evidence(
    category: str = "observation-late",
    *,
    reason: str | None = None,
    observer: str = "engine",
    state: str = "active",
    detail: str | None = None,
    waiting: Any = None,
    pressure: Any = None,
    numbers: Any = ...,
) -> dict[str, Any]:
    """Schema 1 without ``numbers``; schema 2 with them (``None`` or a numbers object)."""
    extra = {} if numbers is ... else {"numbers": numbers}
    return {
        "schema": 1 if numbers is ... else 2,
        **extra,
        "category": category,
        "reason": reason or category,
        "detail": detail,
        "observer": observer,
        "first_observed": {"origin": "boot-1", "tick_ns": 5, "state": state, "phase": "tests", "revision": 7},
        "waiting": waiting,
        "pressure": pressure,
    }


_WAITING = [{"reason": "native-headroom", "first_tick_ns": 10, "last_tick_ns": 40, "waited_ns": 30}]
_WAITING_WITH_NUMBERS = [{**_WAITING[0], "reason": "queue-position", "numbers": {"holders": 1}}]
_PRESSURE = {
    "host_headroom_bytes": 1,
    "host_swap_used_bytes": 2,
    "host_swap_total_bytes": 3,
    "swapout_bytes_per_s": 4,
    "daemon_memory_bytes": 5,
}

# One fixture per writer-table row.
WRITER_ROWS = {
    "breach-active": _evidence("breach", reason="admission-swap-growth", observer="host"),
    "breach-stopping-oom": _evidence("breach", reason="container-oom", observer="container", state="stopping"),
    "daemon-identity-changed": _evidence("daemon-identity-changed", observer="engine"),
    "observation-late": _evidence("observation-late"),
    "unreadable": _evidence("telemetry-unreadable", observer="host"),
    "unreadable-unclassified": _evidence("telemetry-unreadable", observer="process", detail="unclassified"),
    "admission-expired": _evidence(
        "admission-expired",
        reason="native-headroom",
        observer="admission",
        state="queued",
        waiting=_WAITING,
        pressure=_PRESSURE,
    ),
    "admission-expired-no-sample": _evidence(
        "admission-expired", reason="queue-position", observer="admission", state="admitting", waiting=_WAITING
    ),
    "breach-with-numbers": _evidence(
        "breach",
        reason="native-headroom",
        observer="host",
        numbers={"headroom_bytes": 1, "headroom_floor_bytes": 2, "host_memory_bytes": 3},
    ),
    "admission-refused": _evidence(
        "admission-refused",
        reason="unsupported-base-capacity",
        observer="admission",
        state="admitting",
        numbers={"container_bytes": 100, "daemon_memory_bytes": 2},
    ),
    "admission-expired-with-numbers": _evidence(
        "admission-expired",
        reason="queue-position",
        observer="admission",
        state="queued",
        waiting=_WAITING_WITH_NUMBERS,
        numbers=None,
    ),
}


@pytest.mark.parametrize("name", sorted(WRITER_ROWS))
def test_every_writer_row_is_valid(name: str) -> None:
    validate_failure_evidence(copy.deepcopy(WRITER_ROWS[name]))


def _mutate(path: tuple[str, ...], value: Any, base: str = "observation-late") -> dict[str, Any]:
    document = copy.deepcopy(WRITER_ROWS[base] if base in WRITER_ROWS else _evidence(base))
    cursor: Any = document
    for step in path[:-1]:
        cursor = cursor[step]
    cursor[path[-1]] = value
    return document


@pytest.mark.parametrize(
    "document",
    (
        _mutate(("category",), "admin"),
        _mutate(("observer",), "gpu"),
        _mutate(("first_observed", "state"), "complete"),
        _mutate(("first_observed", "state"), "prepared"),
        _mutate(("first_observed", "state"), []),  # unhashable JSON types are a refusal, not a TypeError
        _mutate(("first_observed", "state"), {}),
        _mutate(("category",), []),
        _mutate(("observer",), {}),
        _mutate(("detail",), "other"),
        _mutate(("schema",), 3),
        _mutate(("schema",), True),
        _mutate(("schema",), 2),  # schema 2 without its ``numbers`` key
        {**_evidence(), "numbers": None},  # schema 1 with a ``numbers`` key
        _mutate(("waiting",), _WAITING_WITH_NUMBERS, base="admission-expired"),  # schema 1, a waiting item with numbers
        _mutate(("numbers",), {"headroom_bytes": "1"}, base="breach-with-numbers"),
        _mutate(("numbers",), {"shoe_size": 1}, base="breach-with-numbers"),
        _mutate(("schema",), 1, base="admission-refused"),
        _mutate(("numbers",), None, base="admission-refused"),  # a refusal carries the figures that decided it
        _mutate(("waiting",), _WAITING, base="admission-refused"),
        _mutate(("numbers",), {"holders": 1}, base="admission-expired-with-numbers"),  # numbers sit on the items
        _mutate(("reason",), ""),
        _mutate(("first_observed", "tick_ns"), -1),
        _mutate(("first_observed", "tick_ns"), True),
        _mutate(("first_observed", "revision"), -1),
        _mutate(("first_observed", "origin"), ""),
        _mutate(("first_observed", "phase"), 3),
        _mutate(("waiting",), _WAITING),  # waiting on a non-admission category
        _mutate(("pressure",), _PRESSURE),  # pressure on a non-admission category
        _mutate(("waiting",), None, base="admission-expired"),  # admission evidence must carry its waiting list
        _mutate(("waiting",), [], base="admission-expired"),  # and the writer never emits an empty one
        _mutate(("waiting",), [{"reason": "x"}], base="admission-expired"),
        _mutate(
            ("waiting",),
            [{"reason": "x", "first_tick_ns": 9, "last_tick_ns": 3, "waited_ns": 1}],
            base="admission-expired",
        ),
        _mutate(("pressure",), {"host_headroom_bytes": 1}, base="admission-expired"),
        _mutate(("pressure",), {**_PRESSURE, "daemon_memory_bytes": -1}, base="admission-expired"),
        {**_evidence(), "reported": {"by": "leave"}},  # no ``reported`` object
        {key: value for key, value in _evidence().items() if key != "detail"},
        {"schema": 1, "category": "admin"},
        "observation-late",
        None,
        [],
    ),
)
def test_malformed_evidence_is_refused(document: Any) -> None:
    with pytest.raises(ResourceError, match="failure evidence is invalid"):
        validate_failure_evidence(document)


def test_first_wins_writer_sets_failure_and_evidence_together() -> None:
    current: dict[str, Any] = {"infrastructure_failure": None}
    evidence = _evidence("observation-late")

    assert lifecycle._publish_first_failure(current, "observation-late", evidence) is True

    assert current["infrastructure_failure"] == "observation-late"
    assert current["failure_evidence"] == evidence


@pytest.mark.parametrize(
    "current",
    (
        {"infrastructure_failure": "guardian-lost"},
        {"infrastructure_failure": None, "failure_evidence": _evidence("breach", reason="container-oom")},
    ),
    ids=("failure-already-set", "evidence-already-present"),
)
def test_first_wins_writer_writes_nothing_when_not_first(current: dict[str, Any]) -> None:
    before = copy.deepcopy(current)

    assert lifecycle._publish_first_failure(current, "observation-late", _evidence("observation-late")) is False

    assert current == before


def test_observation_evidence_reads_state_phase_and_revision_from_the_record() -> None:
    data = {"state": "stopping", "grant": {"phase": "tests"}, "revision": 11}

    evidence = lifecycle._observation_evidence(
        data,
        category="breach",
        reason="container-oom",
        observer="container",
        first_tick=("boot-9", 123),
    )

    assert evidence["first_observed"] == {
        "origin": "boot-9",
        "tick_ns": 123,
        "state": "stopping",
        "phase": "tests",
        "revision": 11,
    }
    assert (evidence["category"], evidence["reason"], evidence["observer"], evidence["detail"]) == (
        "breach",
        "container-oom",
        "container",
        None,
    )
    validate_failure_evidence(evidence)


def _oom_record(state: str, **overrides: Any) -> dict[str, Any]:
    return {"state": state, "grant": {"phase": "tests"}, "revision": 3, "infrastructure_failure": None, **overrides}


_KILLED = {"memory_bytes": 1024**3, "observation": {"memory_limit_bytes": 1024**3, "oom_killed": True, "oom_events": 2}}


@pytest.mark.parametrize(
    ("state", "resources", "schema", "numbers"),
    (
        ("active", {}, 1, None),
        ("stopping", {}, 1, None),
        (
            "active",
            {"resources": {"containers": {"c" * 64: _KILLED}}},
            2,
            {"container_limit_bytes": 1024**3, "oom_events": 2},
        ),
    ),
)
def test_container_oom_as_first_failure_carries_evidence_with_the_literal_state(
    state: str, resources: dict[str, Any], schema: int, numbers: dict[str, int] | None
) -> None:
    current = _oom_record(state, **resources)

    lifecycle._set_container_oom(current)

    assert (current["failure_evidence"]["schema"], current["failure_evidence"].get("numbers")) == (schema, numbers)
    assert current["infrastructure_failure"] == "container-oom"
    assert current["failure_evidence"]["first_observed"]["state"] == state
    assert (current["failure_evidence"]["category"], current["failure_evidence"]["observer"]) == (
        "breach",
        "container",
    )
    validate_failure_evidence(current["failure_evidence"])


def test_container_oom_after_an_earlier_failure_overwrites_without_evidence() -> None:
    current = _oom_record("stopping", infrastructure_failure="requester-lost")

    lifecycle._set_container_oom(current)

    assert current["infrastructure_failure"] == "container-oom"  # today's overwrite semantics stay
    assert "failure_evidence" not in current


def test_container_oom_outside_the_observed_states_writes_no_evidence() -> None:
    """Evidence in a state the loader refuses would make the record unloadable; such a record gets no evidence."""
    current = _oom_record("stopped-retained")

    lifecycle._set_container_oom(current)

    assert current["infrastructure_failure"] == "container-oom"
    assert "failure_evidence" not in current


def test_every_container_oom_assignment_goes_through_the_one_helper() -> None:
    import re
    from pathlib import Path

    source = Path(lifecycle.__file__).read_text()
    assert not re.findall(r'\["infrastructure_failure"\]\s*=\s*"container-oom"', source)  # nobody writes the literal
    assert re.findall(r'^\s*failure = "container-oom"', source, re.MULTILINE) == ['    failure = "container-oom"']
    assert 'current["infrastructure_failure"] = failure' in source  # and the one helper assigns it


def test_unclassified_fault_reason_is_its_bounded_error_text() -> None:
    unclassified = TelemetryFault("telemetry-unreadable", "engine", "unclassified")
    classified = TelemetryFault("telemetry-unreadable", "engine")

    assert lifecycle._fault_reason(unclassified, OSError("docker   said\nno")) == "docker said no"
    assert len(lifecycle._fault_reason(unclassified, RuntimeError("x" * 5000))) == lifecycle._FAULT_REASON_MAX
    assert lifecycle._fault_reason(unclassified, RuntimeError("")) == "telemetry-unreadable"
    assert lifecycle._fault_reason(classified, OSError("docker said no")) == "telemetry-unreadable"
