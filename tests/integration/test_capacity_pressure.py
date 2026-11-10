"""Pressure accounting through real guardian processes on a private account.

``bind_test_capacity`` makes the private account's capacity snapshot tight and, with ``containers``, plants the
daemon's running containers; with the default snapshot nothing is ever short.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from docker_test_toolkit import PhaseBudget, RunSpec
from docker_test_toolkit.application import lifecycle, sharing
from docker_test_toolkit.application.recovery import Context, lock
from docker_test_toolkit.domain.records import canonical_json_bytes
from docker_test_toolkit.factories.testing import bind_test_capacity, install_failpoint
from tests.integration.account_fixtures import (
    EnterThread,
    current_identity,
    dead_identity,
    prepare_for_child_guardian,
    queue_phases,
    reap,
    retire_actor,
    source_artifact,
    start_guardian_child,
    three_private_projects,
    wait_until,
    write_raw_queue,
)

pytestmark = pytest.mark.integration

GIB = 1024**3


def _spec(project: Path, purpose: str, container_gib: int, *, wait_seconds: int = 30, capacity: int = 1) -> RunSpec:
    return RunSpec(
        project_root=project,
        entrypoint="integration",
        purpose=purpose,
        services=(),
        closure=(),
        profile="fixture",
        phases={"tests": PhaseBudget(container_gib * GIB, 1, 1, 1)},
        initial_phase="tests",
        artifact=source_artifact(),
        wait_seconds=wait_seconds,
        capacity=capacity,
    )


def _record(handle) -> dict:
    return json.loads(handle._context.record.read_text())


def _never_two_holders_while(account: Path, seconds: float, *, waiter: str) -> None:
    """Sample the queue for ``seconds``: the waiter stays queued, so it never reaches ``admitting``."""
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        assert queue_phases(account).get(waiter) in (None, "queued"), queue_phases(account)
        time.sleep(0.05)


def test_deadline_without_waiting_records_no_reason(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    account = services.root / "admission"
    run = services.open_run(_spec(projects[0], "no-wait", 1, wait_seconds=6))
    prepare_for_child_guardian(run)
    pause = {"slot-reserved": {"action": {"do": "sleep", "seconds": 8}}}
    guardian, _ = start_guardian_child(tmp_path, run, account, pause, name="guardian")
    try:
        entering = EnterThread(run, existing_attempt=True)
        entering.join(timeout=40)
        assert entering.error is not None
        guardian.wait(timeout=30)  # it wakes after its pause and revokes the run itself
    finally:
        reap(guardian)

    assert _record(run)["infrastructure_failure"] is None  # no preceding shortfall: today's behaviour
    assert queue_phases(account) == {}


def test_own_budget_failure_at_publish_revokes_as_today(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    bind_test_capacity(services, daemon_memory_bytes=12 * GIB)
    run = services.open_run(_spec(projects[0], "too-big", 100, wait_seconds=20))
    entering = EnterThread(run)
    entering.join(timeout=40)

    assert entering.error is not None
    assert _record(run)["infrastructure_failure"] == "unsupported-base-capacity"


def test_unreadable_holder_admits_nobody_without_reason(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    account = services.root / "admission"
    holder = services.open_run(_spec(projects[0], "unreadable-holder", 1)).enter()
    genuine = json.loads((account / "queue.json").read_bytes())["entries"][holder.run_id]["holder"]

    def set_holder(identity: dict) -> None:
        with lock(Context(account, account), "metadata"):
            queue = json.loads((account / "queue.json").read_bytes())
            queue["entries"][holder.run_id]["holder"] = identity
            write_raw_queue(account, canonical_json_bytes(queue))

    set_holder(current_identity())  # the record names another guardian: untrusted, so capacity one
    waiter = services.open_run(_spec(projects[1], "unreadable-waiter", 1, wait_seconds=6))
    waiting = EnterThread(waiter)
    try:
        _never_two_holders_while(account, 3.0, waiter=waiter.run_id)
        waiting.join(timeout=30)
        assert waiting.error is not None  # the budget ended while the holder stayed untrusted
    finally:
        set_holder(genuine)

    assert _record(waiter)["infrastructure_failure"] is None
    holder.leave()


def _planted_container(identifier: str, name: str, memory: int, *, project: str | None = None) -> dict:
    labels = {} if project is None else {"com.docker.compose.project": project}
    return {
        "Id": identifier * 64,
        "Name": "/" + name,
        "State": {"Running": True},
        "HostConfig": {"Memory": memory},
        "Config": {"Labels": labels},
    }


def _raise(kind: str, reason: str | None, observer: str | None, occurrence: int = 1) -> dict:
    """A guardian-child stage entry that throws one observation error on the guardian's own stack."""
    action = {"do": "raise", "type": kind, "reason": reason, "observer": observer, "message": f"injected {kind}"}
    return {"action": action, "occurrence": occurrence}


def _guarded(tmp_path: Path, services, project: Path, purpose: str, stages: dict, *, wait_seconds: int = 30):
    """Prepare a run and start a failpoint-instrumented guardian child for it; return the stage log too."""
    run = services.open_run(_spec(project, purpose, 1, wait_seconds=wait_seconds))
    prepare_for_child_guardian(run)
    log = tmp_path / f"{purpose}.log"
    guardian, _ = start_guardian_child(
        tmp_path, run, services.root / "admission", stages, name=f"guardian-{purpose}", observer_log=log
    )
    return run, guardian, log


def _stage_count(log: Path, stage: str) -> int:
    lines = [json.loads(line) for line in log.read_text().splitlines()]
    return sum(1 for line in lines if line["kind"] == "stage" and line["stage"] == stage)


def _external_capacity_account(services) -> None:
    """A 2 GiB daemon with one foreign 1 GiB container: a 1 GiB phase (1.5 GiB guest base) never fits beside it."""
    bind_test_capacity(services, daemon_memory_bytes=2 * GIB, containers=[_planted_container("e", "external", GIB)])


def test_admission_expiry_records_waiting_reasons_and_pressure(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    _external_capacity_account(services)
    run = services.open_run(_spec(projects[0], "expiry", 1, wait_seconds=3))
    deadline = _record(run)["queue_deadline"]
    entering = EnterThread(run)
    entering.join(timeout=40)
    wait_until(lambda: _record(run)["state"] == "complete", message="the guardian revoked the expired run")

    record = _record(run)
    evidence = record["failure_evidence"]
    assert entering.error is not None
    assert (record["admission"], record["infrastructure_failure"]) == ("revoked", None)  # the reason is unchanged
    assert record["queue_deadline"] == deadline  # no accounting extends the original budget
    assert (evidence["category"], evidence["observer"], evidence["detail"]) == ("admission-expired", "admission", None)
    assert evidence["first_observed"]["state"] in ("queued", "admitting")
    waiting = evidence["waiting"]
    assert [item["reason"] for item in waiting] == ["external-capacity"]
    assert evidence["reason"] == "external-capacity"
    span = max(item["last_tick_ns"] for item in waiting) - min(item["first_tick_ns"] for item in waiting)
    assert all(0 < item["waited_ns"] <= span for item in waiting)
    assert max(item["last_tick_ns"] for item in waiting) == deadline["expires_ns"]  # the final pass closes at expiry
    assert evidence["first_observed"]["tick_ns"] >= deadline["expires_ns"]  # captured at the write, like every kind
    assert evidence["pressure"] == {
        "host_headroom_bytes": 32 * GIB,
        "host_swap_used_bytes": 0,
        "host_swap_total_bytes": 0,
        "swapout_bytes_per_s": 0,
        "daemon_memory_bytes": 2 * GIB,
    }
    detail = lifecycle._admission_ended_detail(record)
    assert "external-capacity" in detail and "admission-expired" not in detail  # the reasons, not the category name


def test_admission_expiry_before_any_capacity_sample_has_no_pressure(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    sharing.record_parallel_runs(services.root / "admission", 1)  # one at a time: the waiter waits its queue position
    holder = services.open_run(_spec(projects[0], "holder", 1, capacity=1)).enter()
    waiter = services.open_run(_spec(projects[1], "waiter", 1, wait_seconds=3, capacity=1))
    entering = EnterThread(waiter)
    entering.join(timeout=40)
    wait_until(lambda: _record(waiter)["state"] == "complete", message="the guardian revoked the expired run")

    evidence = _record(waiter)["failure_evidence"]
    assert entering.error is not None
    assert evidence["category"] == "admission-expired"
    assert [item["reason"] for item in evidence["waiting"]] == ["queue-position"]
    assert evidence["pressure"] is None  # a capacity-one reservation pass reads no capacity: null, not zeros
    holder.leave()


def test_admission_expiry_lists_a_late_observation_beside_the_capacity_reason(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    _external_capacity_account(services)
    stages = {"admission-capacity-read": _raise("ControlError", "observation-deadline", "engine", occurrence=1)}
    run, guardian, _log = _guarded(tmp_path, services, projects[0], "late-and-short", stages, wait_seconds=4)
    try:
        entering = EnterThread(run, existing_attempt=True)
        entering.join(timeout=40)
        assert guardian.wait(timeout=30) == 0
    finally:
        reap(guardian)

    evidence = _record(run)["failure_evidence"]
    assert entering.error is not None
    assert {item["reason"] for item in evidence["waiting"]} == {"observation-late", "external-capacity"}
    assert evidence["reason"] == "observation-late"  # the smallest first tick: the late first pass came first


@pytest.mark.parametrize(
    ("kind", "reason", "observer"),
    [
        ("ControlError", "observation-deadline", "engine"),
        ("ResourceError", "observation-deadline", "host"),
        # a Docker client read that exited non-zero (a busy shared daemon): the next pass reads again
        ("InfrastructureFailure", "client-exit", None),
    ],
    ids=["late-engine", "late-host", "client-exit"],
)
def test_admission_classified_late_read_is_a_wait_not_a_crash(
    tmp_path: Path, kind: str, reason: str, observer: str | None
) -> None:
    """A late read of the admitting pass delays the grant only: the next pass reads again and grants."""
    services, projects = three_private_projects(tmp_path)
    stages = {"admission-capacity-read": _raise(kind, reason, observer)}
    run, guardian, log = _guarded(tmp_path, services, projects[0], "late-read", stages)
    try:
        entering = EnterThread(run, existing_attempt=True)
        entering.join(timeout=40)
        assert entering.error is None  # the later pass granted the run
        assert guardian.poll() is None  # and the guardian is still serving it
        assert _stage_count(log, "admission-capacity-read") == 2
        assert _stage_count(log, "publish-pending") == 1  # no grant came out of the late pass
        run.leave()
        assert guardian.wait(timeout=30) == 0
    finally:
        reap(guardian)


@pytest.mark.parametrize(
    ("kind", "reason", "observer", "category", "detail", "expected_observer", "evidence_reason"),
    [
        ("ResourceError", "telemetry-unreadable", "host", "telemetry-unreadable", None, "host", "telemetry-unreadable"),
        # an unclassified fault names its error, so the next occurrence says what failed
        ("OSError", None, None, "telemetry-unreadable", "unclassified", "engine", "[Errno 13] injected OSError"),
    ],
    ids=["classified", "unclassified-oserror"],
)
def test_admission_classified_unreadable_read_revokes_with_evidence(
    tmp_path: Path, kind: str, reason, observer, category: str, detail, expected_observer: str, evidence_reason: str
) -> None:
    services, projects = three_private_projects(tmp_path)
    stages = {"admission-capacity-read": _raise(kind, reason, observer)}
    run, guardian, _log = _guarded(tmp_path, services, projects[0], "unreadable", stages)
    try:
        entering = EnterThread(run, existing_attempt=True)
        entering.join(timeout=40)
        assert guardian.wait(timeout=30) == 0
    finally:
        reap(guardian)

    record = _record(run)
    evidence = record["failure_evidence"]
    assert (record["state"], record["admission"], record["infrastructure_failure"]) == (
        "complete",
        "revoked",
        category,
    )
    assert (evidence["category"], evidence["reason"], evidence["detail"]) == (category, evidence_reason, detail)
    assert (evidence["observer"], evidence["first_observed"]["state"]) == (expected_observer, "admitting")
    assert (evidence["waiting"], evidence["pressure"]) == (None, None)
    assert category in str(entering.error) and f"observer {expected_observer}" in str(entering.error)


def test_admission_persistent_client_exit_expires_naming_the_command_and_cause(tmp_path: Path) -> None:
    """A client read that keeps exiting non-zero never grants; the expiry evidence names what failed and why."""
    services, projects = three_private_projects(tmp_path)
    message = "Docker client `info` exited with status 1 (connect-reset)."
    action = {
        "do": "raise",
        "type": "InfrastructureFailure",
        "reason": "client-exit",
        "observer": None,
        "message": message,
    }
    run, guardian, log = _guarded(
        tmp_path,
        services,
        projects[0],
        "client-exit-persists",
        {"admission-capacity-read": {"action": action}},
        wait_seconds=3,
    )
    try:
        entering = EnterThread(run, existing_attempt=True)
        entering.join(timeout=40)
        assert guardian.wait(timeout=30) == 0
    finally:
        reap(guardian)

    record = _record(run)
    evidence = record["failure_evidence"]
    assert entering.error is not None
    assert (record["admission"], evidence["category"]) == ("revoked", "admission-expired")
    assert _stage_count(log, "publish-pending") == 0  # nothing was ever published on the missing read
    assert [item["reason"] for item in evidence["waiting"]] == [message]
    assert evidence["reason"] == message


def test_admission_client_exit_that_persists_revokes_naming_the_command_and_cause(tmp_path: Path) -> None:
    """A client read failing pass after pass is no transient: after the bound the run is revoked with what failed."""
    services, projects = three_private_projects(tmp_path)
    message = "Docker client `info` exited with status 1 (daemon-unreachable)."
    action = {
        "do": "raise",
        "type": "InfrastructureFailure",
        "reason": "client-exit",
        "observer": None,
        "message": message,
    }
    run, guardian, log = _guarded(
        tmp_path,
        services,
        projects[0],
        "client-exit-bound",
        {"admission-capacity-read": {"action": action}},
        wait_seconds=600,
    )
    try:
        entering = EnterThread(run, existing_attempt=True)
        entering.join(timeout=120)
        assert guardian.wait(timeout=60) == 0
    finally:
        reap(guardian)

    record = _record(run)
    evidence = record["failure_evidence"]
    assert entering.error is not None and message in str(entering.error)
    assert (record["admission"], record["infrastructure_failure"]) == ("revoked", "telemetry-unreadable")
    assert (evidence["category"], evidence["reason"], evidence["detail"]) == (
        "telemetry-unreadable",
        message,
        "unclassified",
    )
    assert _stage_count(log, "admission-capacity-read") == lifecycle._CLIENT_EXIT_PASSES_TO_FAIL
    assert _stage_count(log, "publish-pending") == 0


def test_admission_client_exit_streak_resets_on_a_clean_read(tmp_path: Path) -> None:
    """Failures separated by a clean read never add up: the run is granted, not revoked."""
    services, projects = three_private_projects(tmp_path)
    bound = lifecycle._CLIENT_EXIT_PASSES_TO_FAIL
    action = {"do": "raise", "type": "InfrastructureFailure", "reason": "client-exit", "observer": None, "message": "x"}
    # bound - 1 failing passes, one clean pass, then bound - 1 failing passes again: 2 * (bound - 1) in total
    failing = [*range(1, bound), *range(bound + 1, 2 * bound)]
    stages = {"admission-capacity-read": [{"action": action, "occurrence": n} for n in failing]}
    run, guardian, log = _guarded(tmp_path, services, projects[0], "client-exit-reset", stages, wait_seconds=600)
    try:
        entering = EnterThread(run, existing_attempt=True)
        entering.join(timeout=120)
        assert entering.error is None
        run.leave()
        assert guardian.wait(timeout=30) == 0
    finally:
        reap(guardian)
    assert _stage_count(log, "publish-pending") == 1


def test_admission_expiry_clamps_a_slow_final_pass_to_the_deadline(tmp_path: Path) -> None:
    """A pass that runs past the queue budget is accounted only up to the expiry tick, never beyond the budget."""
    services, projects = three_private_projects(tmp_path)
    _external_capacity_account(services)
    stages = {"admission-capacity-read": {"action": {"do": "sleep", "seconds": 5}, "occurrence": 2}}
    run, guardian, _log = _guarded(tmp_path, services, projects[0], "slow-final-pass", stages, wait_seconds=3)
    try:
        entering = EnterThread(run, existing_attempt=True)
        entering.join(timeout=40)
        assert guardian.wait(timeout=30) == 0
    finally:
        reap(guardian)

    record = _record(run)
    deadline, waiting = record["queue_deadline"], record["failure_evidence"]["waiting"]
    span = max(item["last_tick_ns"] for item in waiting) - min(item["first_tick_ns"] for item in waiting)
    assert max(item["last_tick_ns"] for item in waiting) == deadline["expires_ns"]
    assert span <= deadline["duration_ns"]
    assert all(item["waited_ns"] <= span for item in waiting)


def _held_at_evidence(tmp_path: Path, name: str) -> tuple[Path, Path, dict]:
    reached, release = tmp_path / f"{name}.reached", tmp_path / f"{name}.release"
    stages = {
        "admission-expiry-evidence-pending": {
            "action": [{"do": "write-file", "path": str(reached)}, {"do": "wait-file", "path": str(release)}]
        }
    }
    return reached, release, stages


def test_admission_expiry_evidence_is_kept_when_the_requester_left_before_the_write(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    _external_capacity_account(services)
    reached, release, stages = _held_at_evidence(tmp_path, "left")
    run, guardian, _log = _guarded(tmp_path, services, projects[0], "left-before-write", stages, wait_seconds=3)
    try:
        EnterThread(run, existing_attempt=True)
        wait_until(reached.exists, message="the guardian decided the expiry and has not written it")
        with lock(run._context.project, f"run-{run.run_id}"):
            retire_actor(run._context.project.state, current_identity(), dead_identity())
        release.write_text("")
        assert guardian.wait(timeout=30) == 0
    finally:
        reap(guardian)

    record = _record(run)
    assert (record["state"], record["admission"]) == ("complete", "revoked")
    assert record["failure_evidence"]["category"] == "admission-expired"  # the record always keeps an expiry's reason


def test_admission_classified_requester_left_writes_no_evidence(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    _external_capacity_account(services)
    run, guardian, log = _guarded(tmp_path, services, projects[0], "requester-left", {})
    try:
        EnterThread(run, existing_attempt=True)
        wait_until(lambda: log.exists() and _stage_count(log, "admission-capacity-read") >= 2, message="a waiting pass")
        with lock(run._context.project, f"run-{run.run_id}"):
            retire_actor(run._context.project.state, current_identity(), dead_identity())
        assert guardian.wait(timeout=30) == 0
    finally:
        reap(guardian)

    record = _record(run)
    assert (record["state"], record["admission"], record["infrastructure_failure"]) == ("complete", "revoked", None)
    assert "failure_evidence" not in record  # nobody was left to be told: not an expiry, not a classified fault


def test_admission_expiry_after_the_deadline_keeps_its_evidence_without_a_requester(tmp_path: Path) -> None:
    """Both ended the wait (the budget and the requester): the expiry decides, so its reason is written."""
    services, projects = three_private_projects(tmp_path)
    _external_capacity_account(services)
    stages = {"admission-capacity-read": {"action": {"do": "sleep", "seconds": 4}, "occurrence": 2}}
    run, guardian, log = _guarded(tmp_path, services, projects[0], "left-and-late", stages, wait_seconds=3)
    try:
        EnterThread(run, existing_attempt=True)
        wait_until(lambda: log.exists() and _stage_count(log, "admission-capacity-read") >= 2, message="a slow pass")
        with lock(run._context.project, f"run-{run.run_id}"):
            retire_actor(run._context.project.state, current_identity(), dead_identity())
        assert guardian.wait(timeout=30) == 0
    finally:
        reap(guardian)

    record = _record(run)
    assert (record["state"], record["admission"]) == ("complete", "revoked")
    assert record["failure_evidence"]["category"] == "admission-expired"


def test_admission_expiry_inside_publication_revokes_with_the_report_not_a_guardian_error(tmp_path: Path) -> None:
    """The budget can end between the readiness decision and the grant write: the run is revoked, not abandoned."""
    services, projects = three_private_projects(tmp_path)
    stages = {"publish-pending": {"action": {"do": "sleep", "seconds": 4}}}
    run, guardian, _log = _guarded(tmp_path, services, projects[0], "expires-in-publication", stages, wait_seconds=3)
    try:
        entering = EnterThread(run, existing_attempt=True)
        entering.join(timeout=40)
        assert guardian.wait(timeout=30) == 0  # the guardian revoked the run itself and ended cleanly
    finally:
        reap(guardian)

    record = _record(run)
    assert entering.error is not None
    assert (record["state"], record["admission"]) == ("complete", "revoked")
    assert record["infrastructure_failure"] != "guardian-error"
    assert "failure_evidence" not in record  # nothing was waited on: the run was ready when its budget ended


def test_requester_handoff_reports_diagnostics_not_yet_persisted(tmp_path: Path) -> None:
    """The requester's deadline can end before the guardian wrote the evidence: it says so; the record gets it later."""
    services, projects = three_private_projects(tmp_path)
    _external_capacity_account(services)
    reached, release, stages = _held_at_evidence(tmp_path, "pending")
    run, guardian, _log = _guarded(tmp_path, services, projects[0], "pending", stages, wait_seconds=3)
    try:
        entering = EnterThread(run, existing_attempt=True)
        wait_until(reached.exists, message="the guardian decided the expiry and has not written it")
        entering.join(timeout=40)
        late_ns = entering.ended_tick - _record(run)["queue_deadline"]["expires_ns"]
        assert "admission diagnostics not yet persisted by the guardian" in str(entering.error)
        assert 5_000_000_000 <= late_ns <= 8_000_000_000  # it waited its stated 5 s for the report, and no longer
        assert "failure_evidence" not in _record(run)
        release.write_text("")
        assert guardian.wait(timeout=30) == 0
    finally:
        reap(guardian)

    assert _record(run)["failure_evidence"]["category"] == "admission-expired"


def test_report_wait_ends_five_seconds_after_the_deadline_however_late_it_starts(tmp_path: Path) -> None:
    """A requester that reaches the wait late (a stall across its deadline) does not wait a fresh 5 s."""
    services, projects = three_private_projects(tmp_path)
    _external_capacity_account(services)
    reached, release, stages = _held_at_evidence(tmp_path, "late-entry")
    run, guardian, _log = _guarded(tmp_path, services, projects[0], "late-entry", stages, wait_seconds=3)
    try:
        EnterThread(run, existing_attempt=True)
        wait_until(reached.exists, message="the guardian decided the expiry and has not written it")
        clock = lifecycle.NativeClock()
        now = clock.read()
        long_past = lifecycle.Deadline(clock.origin, now - 20_000_000_000, 10_000_000_000, now - 10_000_000_000)

        started = clock.read()
        record = lifecycle._admission_report_record(run._context, long_past, clock)

        assert clock.read() - started < 2_000_000_000  # one final read, not another 5 s
        assert record["state"] != "complete"  # the held guardian has not written
    finally:
        release.write_text("")
        reap(guardian)


def test_expiry_message_names_the_waits(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    _external_capacity_account(services)
    run = services.open_run(_spec(projects[0], "expiry-message", 1, wait_seconds=3))
    entering = EnterThread(run)
    entering.join(timeout=40)

    assert "Original admission deadline expired before a grant (waited: external-capacity" in str(entering.error)


def test_expiry_message_waits_for_a_late_guardian_write(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    _external_capacity_account(services)
    stages = {"admission-expiry-evidence-pending": {"action": {"do": "sleep", "seconds": 1}}}
    run, guardian, _log = _guarded(tmp_path, services, projects[0], "late-writer", stages, wait_seconds=3)
    try:
        entering = EnterThread(run, existing_attempt=True)
        entering.join(timeout=40)
        assert guardian.wait(timeout=30) == 0
    finally:
        reap(guardian)

    assert "(waited: external-capacity" in str(entering.error)


def test_expiry_message_ends_at_once_when_the_guardian_died(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    _external_capacity_account(services)
    stages = {"admission-expiry-evidence-pending": {"action": {"do": "sigkill"}}}
    run, guardian, _log = _guarded(tmp_path, services, projects[0], "dead-writer", stages, wait_seconds=3)
    try:
        entering = EnterThread(run, existing_attempt=True)
        entering.join(timeout=40)
    finally:
        reap(guardian)

    late_ns = entering.ended_tick - _record(run)["queue_deadline"]["expires_ns"]
    assert "admission diagnostics not yet persisted by the guardian" in str(entering.error)
    assert late_ns < 4_000_000_000  # no waiting out the 5 s for a guardian that is gone


@pytest.mark.parametrize("stage", ["admission-report-read", "admission-report-final-read"])
def test_expiry_message_reports_a_write_that_lands_between_reads(tmp_path: Path, stage: str) -> None:
    """The write can land just before the guardian exits or just before the cutoff: the final read reports it."""
    services, projects = three_private_projects(tmp_path)
    _external_capacity_account(services)
    _reached, release, stages = _held_at_evidence(tmp_path, f"between-{stage}")
    run, guardian, _log = _guarded(tmp_path, services, projects[0], f"between-{stage}", stages, wait_seconds=3)
    released = []

    def land_the_write(name: str) -> None:
        if name == stage and not released:
            released.append(True)
            release.write_text("")
            assert guardian.wait(timeout=30) == 0  # the write is in and the guardian is gone

    install_failpoint(land_the_write)
    try:
        entering = EnterThread(run, existing_attempt=True)
        entering.join(timeout=60)
    finally:
        install_failpoint(None)
        release.write_text("")
        reap(guardian)

    assert released
    assert "(waited: external-capacity" in str(entering.error)


def test_expiry_wait_surfaces_an_unreadable_record(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    _external_capacity_account(services)
    _reached, release, stages = _held_at_evidence(tmp_path, "unreadable")
    run, guardian, _log = _guarded(tmp_path, services, projects[0], "unreadable", stages, wait_seconds=3)
    saved: list[bytes] = []

    def break_the_record(name: str) -> None:
        if name == "admission-report-read" and not saved:
            saved.append(run._context.record.read_bytes())
            lifecycle.atomic_json(run._context.record, {})

    install_failpoint(break_the_record)
    try:
        entering = EnterThread(run, existing_attempt=True)
        entering.join(timeout=40)
    finally:
        install_failpoint(None)
        if saved:
            run._context.record.write_bytes(saved[0])
        release.write_text("")
        reap(guardian)

    assert saved
    assert "admission diagnostics not yet persisted" not in str(entering.error)
    assert isinstance(entering.error, lifecycle.ResourceError)


def _host_reserve_spec(project: Path, purpose: str) -> RunSpec:
    """A heavy CI shape: a 5 GiB host process beside one 256 MiB service."""
    return RunSpec(
        project_root=project,
        entrypoint="integration",
        purpose=purpose,
        services=(),
        closure=(),
        profile="fixture",
        phases={"tests": PhaseBudget(256 * 1024**2, 5 * GIB, 1, 1)},
        initial_phase="tests",
        artifact=source_artifact(),
        wait_seconds=20,
        capacity=1,
    )


def test_default_host_reserve_refuses_a_small_ci_host(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    bind_test_capacity(services, host_memory_bytes=15 * GIB // 2)
    run = services.open_run(_host_reserve_spec(projects[0], "ci-default"))
    entering = EnterThread(run)
    entering.join(timeout=40)

    assert entering.error is not None
    assert _record(run)["infrastructure_failure"] == "unsupported-base-capacity"


def test_declared_host_reserve_admits_the_same_ci_host(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    bind_test_capacity(services, host_memory_bytes=15 * GIB // 2)
    sharing.record_host_reserve(services.root / "admission", GIB)
    run = services.open_run(_host_reserve_spec(projects[0], "ci-declared"))
    entering = EnterThread(run)
    entering.join(timeout=40)

    assert entering.error is None
    assert _record(run)["admission"] == "published"
    run.leave()


def _pristine_account(tmp_path: Path) -> tuple[Path, Path]:
    state = tmp_path / "registry"
    account = state / "test-admission"
    state.mkdir(mode=0o700)
    account.mkdir(mode=0o700)
    (account / "metadata.lock").touch(mode=0o600)
    return state, account


def test_invalid_recorded_reserve_fails_the_guardian_closed(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    reserve = services.root / "admission" / "host-reserve.json"
    reserve.write_text('{"schema": 1, "host_reserve_bytes": 1073741824}\n')
    reserve.chmod(0o644)  # a shared-mode declaration is not the operator's private record
    run = services.open_run(_host_reserve_spec(projects[0], "invalid-reserve"))
    entering = EnterThread(run)
    entering.join(timeout=40)

    assert entering.error is not None
    assert _record(run)["infrastructure_failure"] == "guardian-error"


def test_capacity_two_request_runs_as_capacity_one(tmp_path: Path) -> None:
    """A capacity value is accepted and ignored: two capacity-2 requests still hold the one slot in turn."""
    services, projects = three_private_projects(tmp_path)
    account = services.root / "admission"
    first = services.open_run(_spec(projects[0], "ignored-a", 1, capacity=2)).enter()
    second = services.open_run(_spec(projects[1], "ignored-b", 1, capacity=2))
    waiting = EnterThread(second)
    wait_until(lambda: second.status().state == "queued", message="the second run queued")

    _never_two_holders_while(account, 1.0, waiter=second.run_id)
    assert queue_phases(account) == {first.run_id: "granted", second.run_id: "queued"}
    first.leave()
    waiting.join()

    assert waiting.error is None and queue_phases(account) == {second.run_id: "granted"}
    second.leave()


# ---- machine settings read per decision under the metadata lock (shareable runs) ----------------------------


def _held_locks_at(log: Path, stage: str) -> list[set[str]]:
    """Return, for every firing of ``stage`` in one guardian's observer log, the lock names held at that moment."""
    held: list[str] = []
    found: list[set[str]] = []
    for line in (json.loads(item) for item in log.read_text().splitlines()):
        if line["kind"] == "lock" and line["event"] == "acquire":
            held.append(line["name"])
        elif line["kind"] == "lock" and line["event"] == "release" and line["name"] in held:
            held.remove(line["name"])
        elif line["kind"] == "stage" and line["stage"] == stage:
            found.append(set(held))
    return found


def test_reserve_changed_while_queued_applies_at_publication(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    bind_test_capacity(services, host_memory_bytes=15 * GIB // 2)
    account = services.root / "admission"
    sharing.record_host_reserve(account, GIB)  # fits as declared (test_declared_host_reserve_admits_the_same_ci_host)
    marker, go = tmp_path / "reserved", tmp_path / "go"
    stages = {
        "slot-reserved": {"action": [{"do": "write-file", "path": str(marker)}, {"do": "wait-file", "path": str(go)}]}
    }
    run = services.open_run(_host_reserve_spec(projects[0], "reserve-raised"))
    prepare_for_child_guardian(run)
    guardian, _ = start_guardian_child(tmp_path, run, account, stages, name="guardian-reserve")
    try:
        entering = EnterThread(run, existing_attempt=True)
        wait_until(marker.exists, message="the guardian reserved its slot")
        sharing.record_host_reserve(account, 6 * GIB)  # the operator raises the reserve before publication
        go.write_text("go")
        entering.join(timeout=40)
        guardian.wait(timeout=30)
    finally:
        reap(guardian)

    assert entering.error is not None
    assert _record(run)["admission"] != "published"


@pytest.mark.parametrize("site", ["publication", "reservation"], ids=["publication", "reservation"])
def test_settings_read_inside_metadata_section(tmp_path: Path, site: str) -> None:
    services, projects = three_private_projects(tmp_path)
    run, guardian, log = _guarded(tmp_path, services, projects[0], f"settings-{site}", {})
    try:
        entering = EnterThread(run, existing_attempt=True)
        entering.join(timeout=40)
        assert entering.error is None
        run.leave()
        guardian.wait(timeout=30)
    finally:
        reap(guardian)

    held = _held_locks_at(log, "admission-settings-read")
    assert len(held) >= 2, "the reservation and the publication must each read the admission settings"
    assert all("metadata" in locks for locks in held)
    lines = [json.loads(item) for item in log.read_text().splitlines()]
    reads = [i for i, line in enumerate(lines) if line.get("stage") == "admission-settings-read"]
    capacity = next(i for i, line in enumerate(lines) if line.get("kind") == "lock" and line.get("name") == "capacity")
    if site == "reservation":  # the reservation reads the settings before it takes the capacity lock
        assert reads[0] < capacity
    else:  # the publication reads them again after the capacity lock is held
        assert reads[-1] > capacity
