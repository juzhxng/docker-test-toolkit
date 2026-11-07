"""Admission-window overlap through the real guardian child: scripted host reads cross the process boundary.

The private account binds a W = 10 s admission window; every sleep, gate, budget and other offset is
the one written for the production 30 s window, which the unit pins and the synthetic pair (T5) keep.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pytest

from docker_test_toolkit.application import sharing
from docker_test_toolkit.application.recovery import atomic_json
from docker_test_toolkit.factories.testing import bind_test_host_samples, bind_test_window
from tests.integration.account_fixtures import (
    EnterThread,
    prepare_for_child_guardian,
    reap,
    start_guardian_child,
    three_private_projects,
    wait_until,
)
from tests.integration.test_capacity_pressure import _spec as _pressure_spec

pytestmark = pytest.mark.integration

GIB = 1024**3
S = 1_000_000_000
H0, H1, H2, H3, H4 = 40 * GIB, 41 * GIB, 42 * GIB, 43 * GIB, 44 * GIB
W = 10 * S  # the private admission window this file binds


def _services(tmp_path: Path):
    """One private account with three projects whose guardians decide at the bound window W."""
    services, projects = three_private_projects(tmp_path)
    sharing.record_parallel_runs(services.root / "admission", 1)  # the window at one run at a time
    bind_test_window(services, W // S)
    return services, projects


def _spec(project: Path, purpose: str, container_gib: int, *, wait_seconds: int = 120):
    """A run whose queue budget fits queueing plus a full 30 s window after the reservation."""
    return _pressure_spec(project, purpose, container_gib, wait_seconds=wait_seconds)


def _value(offset_s: float, headroom: int, swapout: int = 0) -> dict[str, int]:
    return {"offset_ns": int(offset_s * S), "headroom_bytes": headroom, "swap_used_bytes": 0, "swapout_bytes": swapout}


def _record(handle) -> dict:
    return json.loads(handle._context.record.read_text())


def _window(record: dict) -> tuple[dict, dict]:
    return record["admission_observation"]["window_start"], record["hold_timeline"]


def _held(services, project: Path):
    """An unscripted capacity-one holder; its guardian loaded no script because none existed yet."""
    holder = services.open_run(_spec(project, "holder", 1))
    holder.enter()
    return holder


def _queued(handle) -> None:
    wait_until(
        lambda: _record(handle)["state"] == "queued" and _record(handle).get("guardian") is not None,
        timeout=30,
        message="the waiting run's guardian enrolled",
    )


def _print_margin(node: str, record: dict, delta_s: int) -> None:
    """Print one G9 line from recorded fields only: how far the newest queued sample was from reaching W.

    ``delta_s`` is the authored offset of the selected window start behind the newest scripted queued sample. The newest
    queued sample's tick trails by one more pass, which the formula omits: the margin is understated, conservatively.
    """
    start, timeline = _window(record)
    queued_age = timeline["reserved_ns"] - start["tick_ns"]
    margin = W - (queued_age - delta_s * S)
    print(f"G9 {node} queued_age_ns={queued_age} margin_ns={margin}")


def _reads_around_publication(log: Path) -> tuple[int, int]:
    """Count the capacity reads before the loop decided READY and the reads after it (the publication's re-read)."""
    stages = [json.loads(line)["stage"] for line in log.read_text().splitlines() if json.loads(line)["kind"] == "stage"]
    decided = stages.index("publish-pending")
    before = stages[:decided].count("admission-capacity-read")
    after = stages[decided:].count("admission-capacity-read")
    return before, after


def test_queued_thirty_seconds_grants_within_one_pass_of_reserving(tmp_path: Path) -> None:
    services, projects = _services(tmp_path)
    holder = _held(services, projects[0])
    bind_test_host_samples(services, [_value(-40, H0), _value(0, H1)])
    waiter = services.open_run(_spec(projects[1], "waiter", 1))
    entering = EnterThread(waiter)
    _queued(waiter)
    time.sleep(4)  # several queued passes: the -40 s sample is queued history
    holder.leave()
    entering.join(timeout=30)
    assert entering.error is None

    start, timeline = _window(_record(waiter))
    _print_margin("test_queued_thirty_seconds_grants_within_one_pass_of_reserving", _record(waiter), 40)

    assert start["headroom_bytes"] == H0  # a queued sample is the window start
    assert start["tick_ns"] < timeline["reserved_ns"]
    assert timeline["granted_ns"] - timeline["reserved_ns"] < 5 * S
    waiter.leave()


def test_queued_under_thirty_seconds_waits_the_remainder(tmp_path: Path) -> None:
    services, projects = _services(tmp_path)
    holder = _held(services, projects[0])
    bind_test_host_samples(services, [_value(0, H0), _value(0, H1)])
    waiter = services.open_run(_spec(projects[1], "waiter", 1))
    entering = EnterThread(waiter)
    _queued(waiter)
    time.sleep(3)
    holder.leave()
    entering.join(timeout=120)
    assert entering.error is None

    start, timeline = _window(_record(waiter))
    waited = timeline["granted_ns"] - timeline["reserved_ns"]
    queued_age = timeline["reserved_ns"] - start["tick_ns"]
    _print_margin("test_queued_under_thirty_seconds_waits_the_remainder", _record(waiter), 0)

    assert start["headroom_bytes"] in (H0, H1) and start["tick_ns"] < timeline["reserved_ns"]  # a queued sample
    assert queued_age < W
    assert W - queued_age - int(1.5 * S) <= waited < W
    waiter.leave()


def test_first_pass_reservation_keeps_no_queued_history(tmp_path: Path) -> None:
    services, projects = _services(tmp_path)
    bind_test_host_samples(services, [_value(-40, H0), _value(0, H1)])
    run = services.open_run(_spec(projects[0], "alone", 1))
    entering = EnterThread(run)
    entering.join(timeout=120)
    assert entering.error is None

    start, timeline = _window(_record(run))

    assert start["headroom_bytes"] == H1  # the reserving pass's -40 s sample is not history
    assert start["tick_ns"] >= timeline["reserved_ns"]
    assert W <= timeline["granted_ns"] - timeline["reserved_ns"] < W + 5 * S
    run.leave()


def test_unscripted_private_grant_stays_immediate(tmp_path: Path) -> None:
    services, projects = _services(tmp_path)
    run = services.open_run(_spec(projects[0], "unscripted", 1))
    run.enter()

    record = _record(run)
    start, timeline = _window(record)

    assert record["admission_observation"]["memory"]["tick_ns"] - start["tick_ns"] == 30 * S  # the synthetic pair
    assert timeline["granted_ns"] - timeline["reserved_ns"] < 5 * S
    run.leave()


def test_scripted_private_run_decides_at_the_bound_window(tmp_path: Path) -> None:
    """Queued history exactly W old grants within one pass: selection, the pass and publication all use W, not 30 s."""
    services, projects = _services(tmp_path)
    holder = _held(services, projects[0])
    bind_test_host_samples(services, [_value(-10, H0), _value(0, H1)])
    waiter = services.open_run(_spec(projects[1], "waiter", 1))
    entering = EnterThread(waiter)
    _queued(waiter)
    time.sleep(2)  # the -10 s sample is queued history, already W old when it was read
    holder.leave()
    entering.join(timeout=30)
    assert entering.error is None

    start, timeline = _window(_record(waiter))

    assert start["headroom_bytes"] == H0  # the W-old queued sample starts the window (30 s would reject it)
    assert W <= timeline["reserved_ns"] - start["tick_ns"] < 30 * S
    assert timeline["granted_ns"] - timeline["reserved_ns"] < 5 * S  # within one pass of reserving, not at 30 s
    waiter.leave()


def _gated_waiter(tmp_path: Path, services, project: Path, stages: dict):
    waiter = services.open_run(_spec(project, "waiter", 1))
    prepare_for_child_guardian(waiter)
    log = tmp_path / "guardian.log"
    guardian, _ = start_guardian_child(
        tmp_path, waiter, services.root / "admission", stages, name="guardian-waiter", observer_log=log
    )
    return waiter, guardian, log


def _gate(reached: Path, release: Path, occurrence: int) -> dict:
    steps = [{"do": "write-file", "path": str(reached)}, {"do": "wait-file", "path": str(release)}]
    return {"action": steps, "occurrence": occurrence}


def test_failed_read_on_the_reserving_pass_falls_back_to_w0(tmp_path: Path) -> None:
    services, projects = _services(tmp_path)
    holder = _held(services, projects[0])
    bind_test_host_samples(services, [_value(-40, H0), {"fail": True}, _value(0, H2)])
    reached, release = tmp_path / "reached", tmp_path / "release"
    waiter, guardian, _log = _gated_waiter(
        tmp_path, services, projects[1], {"admission-capacity-read": _gate(reached, release, 2)}
    )
    try:
        entering = EnterThread(waiter, existing_attempt=True)
        wait_until(reached.exists, timeout=30, message="the waiter reached its second queued read")
        holder.leave()
        release.write_text("")
        entering.join(timeout=120)
        assert entering.error is None
        start, timeline = _window(_record(waiter))
        assert start["headroom_bytes"] == H2  # the failed read cleared H0 although its pass reserved
        assert W <= timeline["granted_ns"] - timeline["reserved_ns"] < W + 5 * S
        waiter.leave()
    finally:
        reap(guardian)


def test_failed_queued_read_clears_the_buffer_and_never_revokes(tmp_path: Path) -> None:
    services, projects = _services(tmp_path)
    holder = _held(services, projects[0])
    bind_test_host_samples(services, [_value(-40, H0), {"fail": True}, _value(0, H2)])
    waiter = services.open_run(_spec(projects[1], "waiter", 1))
    entering = EnterThread(waiter)
    _queued(waiter)
    time.sleep(3)
    holder.leave()
    entering.join(timeout=120)
    assert entering.error is None

    record = _record(waiter)
    start, _timeline = _window(record)

    assert start["headroom_bytes"] != H0 and record["infrastructure_failure"] is None
    waiter.leave()


def test_discontinuous_queued_sample_is_discarded(tmp_path: Path) -> None:
    services, projects = _services(tmp_path)
    holder = _held(services, projects[0])
    bind_test_host_samples(services, [_value(-40, H0, swapout=10 * GIB), _value(0, H1, swapout=0)])
    waiter = services.open_run(_spec(projects[1], "waiter", 1))
    entering = EnterThread(waiter)
    _queued(waiter)
    time.sleep(3)
    holder.leave()
    entering.join(timeout=120)
    assert entering.error is None

    start, _timeline = _window(_record(waiter))

    assert start["headroom_bytes"] == H1  # the queued swap-out above the later samples breaks the chain (X11)
    waiter.leave()


def test_unsafe_current_sample_never_grants(tmp_path: Path) -> None:
    services, projects = _services(tmp_path)
    holder = _held(services, projects[0])
    bind_test_host_samples(services, [_value(-40, H0), _value(0, GIB)])
    waiter = services.open_run(_spec(projects[1], "waiter", 1, wait_seconds=15))
    entering = EnterThread(waiter)
    _queued(waiter)
    time.sleep(2)
    holder.leave()
    entering.join(timeout=120)
    wait_until(lambda: _record(waiter)["state"] == "complete", timeout=30, message="the waiter was revoked")

    record = _record(waiter)

    assert record.get("grant") is None and "hold_timeline" not in record
    assert "native-headroom" in {item["reason"] for item in record["failure_evidence"]["waiting"]}  # X10


def test_invalid_script_fails_the_guardian(tmp_path: Path) -> None:
    services, projects = _services(tmp_path)
    script = services.root / "admission" / "test-host-samples.json"
    atomic_json(script, {"schema": 1, "samples": []})
    os.chmod(script, 0o600)
    run = services.open_run(_spec(projects[0], "invalid", 1))
    entering = EnterThread(run)
    entering.join(timeout=120)
    wait_until(lambda: _record(run)["infrastructure_failure"] is not None, timeout=30, message="the guardian failed")

    assert _record(run)["infrastructure_failure"] == "guardian-error"


def test_loop_keeps_post_reservation_continuity(tmp_path: Path) -> None:
    services, projects = _services(tmp_path)
    holder = _held(services, projects[0])
    script = [
        _value(-40, H0, swapout=0),
        _value(0, H1, swapout=0),  # the reserving pass: not buffered
        _value(0, GIB, swapout=GIB),  # the first post-reservation sample: low headroom waits
        _value(0, H2, swapout=0),  # the next pass: the counter went backwards after the reservation, not after H0
    ]
    bind_test_host_samples(services, script)
    reached, release = tmp_path / "reached", tmp_path / "release"
    waiter, guardian, log = _gated_waiter(
        tmp_path, services, projects[1], {"admission-capacity-read": _gate(reached, release, 2)}
    )
    try:
        entering = EnterThread(waiter, existing_attempt=True)
        wait_until(reached.exists, timeout=30, message="the waiter reached its second queued read")
        holder.leave()
        release.write_text("")
        entering.join(timeout=120)
        wait_until(lambda: _record(waiter)["state"] == "complete", timeout=60, message="the waiter was revoked")
        record = _record(waiter)
        stages = [
            json.loads(line)["stage"] for line in log.read_text().splitlines() if json.loads(line)["kind"] == "stage"
        ]
        assert record.get("grant") is None and "hold_timeline" not in record
        assert record["infrastructure_failure"] == "telemetry-discontinuity"
        assert "publish-pending" not in stages  # the loop refused; publication never started
    finally:
        reap(guardian)
