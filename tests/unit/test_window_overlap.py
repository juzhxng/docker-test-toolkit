"""Admission-window overlap: the cross-process host-sample script and the pure window-start selection."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from docker_test_toolkit import PhaseBudget
from docker_test_toolkit.application import lifecycle
from docker_test_toolkit.application.lifecycle import HostMemoryObservation, NativeClock, _HostScript
from docker_test_toolkit.application.recovery import atomic_json
from docker_test_toolkit.domain.errors import ResourceError
from docker_test_toolkit.factories import testing
from docker_test_toolkit.factories.testing import (
    bind_test_host_samples,
    bind_test_observation_reads,
    build_test_services,
)

GIB = 1024**3
S = 1_000_000_000


def _value(offset_s: float = 0, headroom: int = 40 * GIB, swap_used: int = 0, swapout: int = 0) -> dict[str, int]:
    return {
        "offset_ns": int(offset_s * S),
        "headroom_bytes": headroom,
        "swap_used_bytes": swap_used,
        "swapout_bytes": swapout,
    }


@pytest.mark.parametrize(
    "bind",
    (
        lambda directory: lifecycle.bind_test_host_samples(directory, [_value()]),
        lambda directory: lifecycle.bind_test_capacity(directory),
        lambda directory: lifecycle.bind_test_daemon(directory, "daemon"),
        lambda directory: lifecycle.bind_test_window(directory, 10),
        lambda directory: lifecycle.bind_test_poll_interval(directory, 50_000_000),
        lambda directory: lifecycle.withdraw_test_runs(directory, recorded=directory),
    ),
    ids=("host-samples", "capacity", "daemon", "window", "poll", "withdrawal"),
)
@pytest.mark.parametrize("absent_child", (False, True), ids=("account", "absent-child"))
def test_private_test_bindings_refuse_the_real_account(bind, absent_child: bool, real_account: Path) -> None:
    """The refusal holds before the account or the private directory exists, as on a fresh CI host."""
    absent = real_account / "absent-private-domain"

    with pytest.raises(ResourceError, match="real account domain"):
        bind(absent if absent_child else real_account)

    assert not absent.exists()


@pytest.mark.parametrize(
    "document",
    [
        {"schema": 1, "samples": [{**_value(), "extra": 1}]},
        {"schema": 1, "samples": [{key: value for key, value in _value().items() if key != "swapout_bytes"}]},
        {"schema": 1, "samples": [{**_value(), "headroom_bytes": True}]},
        {"schema": 1, "samples": [_value(offset_s=1)]},
        {"schema": 1, "samples": [_value(headroom=-1)]},
        {"schema": 1, "samples": [{"fail": True}]},
        {"schema": 1, "samples": [_value()] * 257},
        {"schema": 2, "samples": [_value()]},
        {"schema": 1, "samples": [_value(), {"fail": False}]},
        {"schema": 1, "samples": []},
        {"samples": [_value()]},
    ],
    ids=[
        "unknown-key",
        "missing-key",
        "bool-for-int",
        "positive-offset",
        "negative-bytes",
        "no-value-entry",
        "too-many",
        "schema-2",
        "fail-false",
        "empty",
        "no-schema",
    ],
)
def test_host_sample_shapes_are_refused(tmp_path: Path, document: dict[str, Any]) -> None:
    with pytest.raises(ResourceError, match="host samples are invalid"):
        lifecycle._validate_host_samples(document)
    directory = tmp_path / "admission"
    directory.mkdir(mode=0o700)
    atomic_json(directory / "test-host-samples.json", document)
    with pytest.raises(ResourceError, match="host samples are invalid"):
        _HostScript.load(directory)


def test_host_script_consumes_and_repeats_the_last_value(tmp_path: Path) -> None:
    services = build_test_services(tmp_path / "state")
    bind_test_host_samples(services, [_value(-40, headroom=40 * GIB), _value(0, headroom=41 * GIB)])
    script = _HostScript.load(services.root / "admission")
    assert script is not None
    clock = NativeClock()

    first = script.next(clock, 64 * GIB)
    second = script.next(clock, 64 * GIB)
    third = script.next(clock, 64 * GIB)

    now = clock.read()
    assert isinstance(first, HostMemoryObservation) and first.origin == clock.origin
    assert now - first.tick_ns >= 40 * S and first.headroom_bytes == 40 * GIB and first.physical_bytes == 64 * GIB
    assert now - second.tick_ns < 5 * S and second.headroom_bytes == 41 * GIB
    assert now - third.tick_ns < 5 * S and third.headroom_bytes == 41 * GIB and third.tick_ns >= second.tick_ns


def test_host_script_fail_entry_raises_telemetry_unreadable(tmp_path: Path) -> None:
    services = build_test_services(tmp_path / "state")
    bind_test_host_samples(services, [_value(-40, headroom=7 * GIB), {"fail": True}])
    script = _HostScript.load(services.root / "admission")
    assert script is not None
    clock = NativeClock()
    script.next(clock, 64 * GIB)

    with pytest.raises(ResourceError) as raised:
        script.next(clock, 64 * GIB)

    assert (raised.value.reason, raised.value.observer) == ("telemetry-unreadable", "host")
    assert script.next(clock, 64 * GIB).headroom_bytes == 7 * GIB  # after the end: the last value entry, offset 0


def test_host_script_is_absent_in_production(tmp_path: Path) -> None:
    assert _HostScript.load(None) is None
    empty = tmp_path / "admission"
    empty.mkdir(mode=0o700)
    assert _HostScript.load(empty) is None  # a private domain without a script keeps today's synthetic window
    services = build_test_services(tmp_path / "state")
    bind_test_host_samples(services, [_value()])
    stored = json.loads((services.root / "admission" / "test-host-samples.json").read_text())
    assert stored == {"schema": 1, "samples": [_value()]}


ORIGIN = "darwin-continuous:11111111-2222-3333-4444-555555555555"
T = 1_000 * S


def _sample(at_s: float, *, swapout: int = 0, headroom: int = 8 * GIB, origin: str = ORIGIN) -> HostMemoryObservation:
    return HostMemoryObservation(origin, T + int(at_s * S), 16 * GIB, headroom, 0, swapout, 4096, {})


def test_select_window_start_picks_the_newest_old_enough() -> None:
    queued = [_sample(-45), _sample(-40), _sample(-5)]
    current = _sample(0)

    start = lifecycle.select_window_start(queued, [_sample(-1)], current)

    assert start == queued[1]  # the newest at least 30 s old, not the oldest
    pair = [_sample(-40), _sample(-20)]
    assert lifecycle.select_window_start(pair, [_sample(-1)], current) == pair[0]  # the default is the 30 s window
    assert lifecycle.select_window_start(pair, [_sample(-1)], current, window_ns=10 * S) == pair[1]  # a bound window


def test_queued_candidates_must_be_pairwise_non_decreasing() -> None:
    mib = 1024**2
    queued = [_sample(-45, swapout=100 * mib), _sample(-25, swapout=20 * mib)]

    start = lifecycle.select_window_start(queued, [_sample(-1, swapout=150 * mib)], _sample(0, swapout=150 * mib))

    assert start is None  # the chain breaks at the decrease; q1 is beyond the break


def test_unsafe_current_sample_waits_on_a_safe_queued_window() -> None:
    start = lifecycle.select_window_start([_sample(-40)], [_sample(-1)], _sample(0, headroom=3 * GIB // 2 - 1))
    assert start is not None
    assert lifecycle.assess_pressure(_sample(0, headroom=3 * GIB // 2 - 1), start).reason == "native-headroom"
    swapping = _sample(0, swapout=128 * 1024**2 * 40 + 1)
    window = lifecycle.select_window_start([_sample(-40)], [_sample(-1)], swapping)
    assert lifecycle.assess_pressure(swapping, window).reason == "swapout-rate"


def test_sample_window_retention_and_bound() -> None:
    window = lifecycle._SampleWindow()
    for second in range(100):
        window.add(_sample(second))
    kept = window.samples()

    assert len(kept) <= 64
    newest = kept[-1].tick_ns
    old_enough = [sample for sample in kept if newest - sample.tick_ns >= 30 * S]
    assert old_enough and max(sample.tick_ns for sample in old_enough) == newest - 30 * S
    window.clear()
    assert window.samples() == ()


def test_post_reservation_discontinuity_still_refuses() -> None:
    first_post = _sample(-35, swapout=GIB)
    current = _sample(0, swapout=0)  # swap-out went backwards after reservation
    start = lifecycle.select_window_start([], [first_post], current)

    decision = lifecycle.assess_pressure(current, start, admission_baseline=first_post)

    assert decision.reason == "telemetry-discontinuity"


def test_assess_pressure_thresholds_are_unchanged() -> None:
    window = _sample(-30)
    assert lifecycle.assess_pressure(_sample(0, headroom=3 * GIB // 2), window).reason == "native-pressure"
    assert lifecycle.assess_pressure(_sample(0, headroom=3 * GIB // 2 - 1), window).reason == "native-headroom"
    limit = 128 * 1024**2 * 30
    assert lifecycle.assess_pressure(_sample(0, swapout=limit), window).reason == "native-pressure"
    assert lifecycle.assess_pressure(_sample(0, swapout=limit + 1), window).reason == "swapout-rate"
    almost = HostMemoryObservation(ORIGIN, T - 30 * S + 1, 16 * GIB, 8 * GIB, 0, 0, 4096, {})
    assert lifecycle.assess_pressure(_sample(0), almost).reason == "incomplete-pressure-window"


def test_production_queued_sample_reads_the_host_with_a_thirty_second_deadline() -> None:
    calls: list[object] = []
    clock = NativeClock()

    def reader(read_clock: NativeClock, deadline: object) -> HostMemoryObservation:
        calls.append(deadline)
        return HostMemoryObservation(read_clock.origin, read_clock.read(), 16 * GIB, 8 * GIB, 0, 0, 4096, {})

    with bind_test_observation_reads(host=reader):
        sample = lifecycle._queued_host_sample(None, None, clock)

    assert sample is not None and len(calls) == 1
    deadline = calls[0]
    assert isinstance(deadline, lifecycle.Deadline)
    remaining = deadline.remaining_ns(clock)
    assert 25 * S < remaining <= 30 * S

    def failing(read_clock: NativeClock, deadline: object) -> HostMemoryObservation:
        raise ResourceError("host unreadable", reason="telemetry-unreadable", observer="host")

    with bind_test_observation_reads(host=failing), pytest.raises(ResourceError):
        lifecycle._queued_host_sample(None, None, clock)  # the guardian's caller drops the sample and clears


def test_private_domain_without_a_script_takes_no_queued_sample(tmp_path: Path) -> None:
    assert lifecycle._queued_host_sample(tmp_path, None, NativeClock()) is None


def test_production_window_is_thirty_seconds() -> None:
    assert lifecycle._WINDOW_NS == 30_000_000_000
    assert lifecycle._test_window_ns(None) == 30 * S


def test_production_guardian_window_ignores_bindings(tmp_path: Path) -> None:
    services = build_test_services(tmp_path / "state")
    testing.bind_test_window(services, 10)  # a valid private binding exists ...

    assert lifecycle._test_window_ns(None) == 30 * S  # ... and a guardian without a test directory never reads it
    assert lifecycle._test_window_ns(services.root / "admission") == 10 * S


def test_window_reader_refuses_the_real_account(tmp_path: Path, real_account: Path) -> None:
    account = real_account
    linked = tmp_path / "linked-admission"
    linked.symlink_to(account, target_is_directory=True)  # a private-looking path that resolves into the account

    for directory in (account, account / "absent-private-domain", linked, account.parent):
        with pytest.raises(ResourceError, match="real account domain"):
            lifecycle._test_window_ns(directory)  # refused before the file's existence is even checked

    assert not (account / "absent-private-domain").exists()


def test_window_values_are_refused(tmp_path: Path) -> None:
    services = build_test_services(tmp_path / "state")
    admission = services.root / "admission"

    for seconds in (30, 0, -1, True, 10.0):
        with pytest.raises(ResourceError, match="window"):
            lifecycle.bind_test_window(admission, seconds)
    assert not (admission / "test-window.json").exists()
    for document in (
        {"window_ns": 30 * S},
        {"window_ns": S - 1},
        {"window_ns": True},
        {"window_ns": float(10 * S)},
        {"window_ns": 10 * S, "extra": 1},
        {},
        [10 * S],
    ):
        atomic_json(admission / "test-window.json", document)
        with pytest.raises(ResourceError):  # a non-object fails the private JSON reader before the shape check
            lifecycle._test_window_ns(admission)


MS = 1_000_000
POLL = {"schema": 1, "interval_ns": 50 * MS}


def test_poll_reader_ignores_the_real_account(tmp_path: Path) -> None:
    """A binding in, under, over or linked into the canonical account is ignored without being opened."""
    canonical = tmp_path / "canonical"
    canonical.mkdir(mode=0o700)
    child = canonical / "child"
    child.mkdir(mode=0o700)
    linked = tmp_path / "linked"
    linked.symlink_to(canonical, target_is_directory=True)
    target = tmp_path / "target.json"
    atomic_json(target, POLL)
    private = tmp_path / "private"
    private.mkdir(mode=0o700)
    atomic_json(private / "test-poll.json", POLL)
    assert lifecycle._poll_interval_ns(private, canonical=canonical) == 50 * MS  # control: a private binding is read

    for directory in (canonical, child, tmp_path, linked):  # equal, child, parent, symlinked
        for hostile in ("valid", "mode-0644", "symlink"):  # the last two raise if opened
            path = directory / "test-poll.json"
            path.unlink(missing_ok=True)
            if hostile == "symlink":
                path.symlink_to(target)
            else:
                atomic_json(path, POLL)
                if hostile == "mode-0644":
                    path.chmod(0o644)
            assert lifecycle._poll_interval_ns(directory, canonical=canonical) == S, (directory, hostile)


def test_poll_values_are_refused(tmp_path: Path) -> None:
    admission = tmp_path / "admission"
    admission.mkdir(mode=0o700)

    for interval in (0, 10 * MS - 1, S + 1, True, 50.0 * MS):
        with pytest.raises(ResourceError, match="poll"):
            lifecycle.bind_test_poll_interval(admission, interval)
    assert not (admission / "test-poll.json").exists()
    for document in (
        {"schema": 1, "interval_ns": 0},
        {"schema": 1, "interval_ns": S + 1},
        {"schema": 1, "interval_ns": True},
        {"schema": 2, "interval_ns": 50 * MS},
        {"interval_ns": 50 * MS},
        {**POLL, "extra": 1},
        [POLL],
    ):
        atomic_json(admission / "test-poll.json", document)
        with pytest.raises(ResourceError):
            lifecycle._poll_interval_ns(admission, canonical=None)


def test_production_poll_is_one_second(tmp_path: Path) -> None:
    assert lifecycle._PRODUCTION_POLL_NS == 1_000_000_000
    assert lifecycle._poll_interval_ns(None, canonical=None) == S
    services = build_test_services(tmp_path / "state")
    admission = services.root / "admission"
    testing.bind_test_poll_interval(services, S)  # one second is accepted
    assert lifecycle._poll_interval_ns(admission, canonical=None) == S
    (admission / "test-poll.json").unlink()  # no binding at all: one second
    assert lifecycle._poll_interval_ns(admission, canonical=None) == S


def test_validate_published_grant_refuses_managed_scope_without_installation(tmp_path: Path) -> None:
    """From the source tree there is no real account: a ``managed`` grant on a private account is contradictory."""
    from docker_test_toolkit import PhaseBudget, RunSpec
    from tests.integration.account_fixtures import source_artifact

    project = tmp_path / "project"
    project.mkdir()
    spec = RunSpec(
        project,
        "integration",
        "scope",
        (),
        (),
        "fixture",
        {"tests": PhaseBudget(0, 1, 1, 1)},
        "tests",
        source_artifact(),
    )
    handle = build_test_services(tmp_path / "state").open_run(spec).enter()
    try:
        record = json.loads(handle._context.record.read_text())
        lifecycle._validate_published_grant(record)  # control: the private grant is coherent
        record["grant"]["scope"] = "managed"

        with pytest.raises(ResourceError, match="account execution scope"):
            lifecycle._validate_published_grant(record)
    finally:
        handle.leave()


@pytest.mark.parametrize(
    ("uptime_s", "state", "reason"),
    (
        (25, lifecycle.CapacityState.WAITING, "incomplete-pressure-window"),
        (30, lifecycle.CapacityState.READY, "native-pressure"),
    ),
    ids=("younger-than-the-window", "as-old-as-the-window"),
)
def test_synthetic_window_on_a_young_machine_waits_instead_of_refusing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, uptime_s: int, state: lifecycle.CapacityState, reason: str
) -> None:
    """A clock counts from boot, so a window start 30 s back would be negative just after a virtual machine boots."""

    class YoungClock:
        origin = "boot:young"

        def read(self) -> int:
            return uptime_s * S

    monkeypatch.setattr(lifecycle, "NativeClock", YoungClock)

    inputs = lifecycle._test_capacity_inputs(tmp_path, {"topology": "native"}, {"tests": PhaseBudget(GIB, 1, 1, 1)})

    decision = lifecycle.assess_pressure(inputs.current, inputs.baseline)
    assert inputs.baseline.tick_ns == 0
    assert (decision.state, decision.reason) == (state, reason)
