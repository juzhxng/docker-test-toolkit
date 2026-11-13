"""Shared admission on private accounts: N holders under the machine limit, SH capacity, the co-holder fit."""

from __future__ import annotations

import fcntl
import json
import os
import signal
import time
from pathlib import Path

import pytest

from docker_test_toolkit import PhaseBudget, RunSpec
from docker_test_toolkit.application import lifecycle, sharing
from docker_test_toolkit.application.recovery import atomic_json
from docker_test_toolkit.domain.policy import allowance_bytes
from docker_test_toolkit.domain.share_policy import SHARE_RULES_VERSION
from docker_test_toolkit.factories.testing import bind_test_capacity
from tests.integration.account_fixtures import (
    EnterThread,
    current_identity,
    dead_identity,
    hold_lock_until_released,
    pause_guardian,
    prepare_for_child_guardian,
    queue_phases,
    reap,
    reap_orphan,
    release_lock,
    replace_identity,
    resume_guardian,
    seed_dead_since,
    source_artifact,
    start_guardian_child,
    start_recover_child,
    start_requester_child,
    three_private_projects,
    wait_until,
)

pytestmark = pytest.mark.integration

GIB = 1024**3


def _spec(project: Path, purpose: str, *, gib: int = 1, cpus: int = 1, wait_seconds: int = 30, phases=None) -> RunSpec:
    return RunSpec(
        project_root=project,
        entrypoint="integration",
        purpose=purpose,
        services=(),
        closure=(),
        profile="fixture",
        phases=phases or {"tests": PhaseBudget(gib * GIB, 1, cpus, 1)},
        initial_phase="tests",
        artifact=source_artifact(),
        wait_seconds=wait_seconds,
    )


def _record(handle) -> dict:
    return json.loads(handle._context.record.read_text())


def _seed_shareable(handle, shareable: bool = True) -> None:
    """Write a verdict for the run's request, as an earlier exclusive run of the same request would have taught."""
    context = handle._context
    spec = handle._spec
    origin = sharing.repository_origin(spec.project_root)
    assert origin is not None, "test projects carry a Git origin"
    key = sharing.request_key(origin, spec.entrypoint, spec.profile, spec.closure)
    path = sharing.verdict_path(context.project.state, context.project.project_id)
    path.parent.mkdir(mode=0o700, exist_ok=True)
    verdict = {"shareable": shareable, "reason": None, "run_id": context.run_id, "origin": "seed", "tick_ns": 0}
    atomic_json(path, {"rules": SHARE_RULES_VERSION, "schema": 1, "verdicts": {key: verdict}})


def _open(services, project: Path, purpose: str, *, shareable: bool | None = True, **budget):
    handle = services.open_run(_spec(project, purpose, **budget))
    if shareable is not None:
        _seed_shareable(handle, shareable)
    return handle


def _holds_for(seconds: float, condition) -> None:
    """Sample a test-side observation for ``seconds``: it must hold at every sample (a bounded check, not a wait)."""
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        assert condition()
        time.sleep(0.05)


def _waited(handle, reason: str) -> bool:
    timeline = _record(handle).get("hold_timeline") or {}
    return any(item["reason"] == reason for item in timeline.get("waiting", []))


def _capacity_mode(account: Path) -> str:
    """Probe ``capacity.lock`` without waiting: free, held shared (SH) or held exclusively (EX)."""
    descriptor = os.open(account / "capacity.lock", os.O_RDWR)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            return "free"
        except BlockingIOError:
            pass
        try:
            fcntl.flock(descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            return "SH"
        except BlockingIOError:
            return "EX"
    finally:
        os.close(descriptor)


def test_two_shared_runs_overlap_with_sh_capacity(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    account = services.root / "admission"
    sharing.record_parallel_runs(account, 2)
    first = _open(services, projects[0], "shared-a").enter()

    second = _open(services, projects[1], "shared-b").enter()

    assert queue_phases(account) == {first.run_id: "granted", second.run_id: "granted"}
    assert _record(first)["sharing"] == _record(second)["sharing"] == {"mode": "shared"}
    assert _record(first)["hold_timeline"]["granted_ns"] < _record(second)["hold_timeline"]["granted_ns"]
    assert _capacity_mode(account) == "SH"
    first.leave()
    second.leave()
    assert _capacity_mode(account) == "free"


def test_exclusive_prediction_admits_exclusive_and_waits_shared_joiner(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    account = services.root / "admission"
    sharing.record_parallel_runs(account, 2)
    exclusive = _open(services, projects[0], "unknown-request", shareable=None).enter()  # no verdict: exclusive
    joiner = _open(services, projects[1], "shared-joiner")

    waiting = EnterThread(joiner)
    wait_until(lambda: joiner.status().state == "queued", message="the joiner queued")
    assert "sharing" not in _record(exclusive)
    assert _capacity_mode(account) == "EX"
    assert queue_phases(account)[joiner.run_id] == "queued"
    exclusive.leave()
    waiting.join(30)

    assert waiting.error is None and _waited(joiner, "exclusive-holder")
    joiner.leave()


def test_only_the_head_waits_with_the_heads_reason(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    account = services.root / "admission"
    sharing.record_parallel_runs(account, 2)
    exclusive = _open(services, projects[0], "holder", shareable=None).enter()
    head, behind = _open(services, projects[1], "head"), _open(services, projects[2], "behind")
    waiting = [EnterThread(head)]
    wait_until(lambda: head.status().state == "queued", message="the head queued")
    waiting.append(EnterThread(behind))
    wait_until(lambda: behind.status().state == "queued", message="the second joiner queued")
    _holds_for(1.5, lambda: queue_phases(account)[behind.run_id] == "queued")  # several passes behind the head
    exclusive.leave()
    for thread in waiting:
        thread.join(30)
        assert thread.error is None

    reasons = {
        name: {item["reason"] for item in _record(handle)["hold_timeline"]["waiting"]}
        for name, handle in (("head", head), ("behind", behind))
    }
    assert "exclusive-holder" in reasons["head"]
    assert reasons["behind"] <= {"queue-position"} and reasons["behind"]
    head.leave()
    behind.leave()


def test_negative_verdict_admits_exclusive(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    account = services.root / "admission"
    sharing.record_parallel_runs(account, 2)

    run = _open(services, projects[0], "negative", shareable=False).enter()

    assert "sharing" not in _record(run) and _capacity_mode(account) == "EX"
    run.leave()


def test_limit_one_admits_exclusive_despite_positive_verdict(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    account = services.root / "admission"

    sharing.record_parallel_runs(account, 1)
    run = _open(services, projects[0], "limit-one").enter()

    assert "sharing" not in _record(run) and _capacity_mode(account) == "EX"
    run.leave()


def test_reserve_takes_capacity_before_writes_under_release_recovery(tmp_path: Path) -> None:
    """A recovery holding ``capacity.lock`` exclusively with no queue entry: the head writes nothing until it ends."""
    services, projects = three_private_projects(tmp_path)
    account = services.root / "admission"
    sharing.record_parallel_runs(account, 2)
    (account / "capacity.lock").touch(mode=0o600)
    recovery = hold_lock_until_released(account / "capacity.lock")
    head = _open(services, projects[0], "behind-recovery")
    try:
        waiting = EnterThread(head)
        wait_until(lambda: head.run_id in queue_phases(account), message="the head enrolled")
        # two seconds of passes: the head never becomes a holder while the lock is held
        _holds_for(2.0, lambda: queue_phases(account)[head.run_id] == "queued" and _record(head)["state"] == "queued")
    finally:
        release_lock(recovery)
    waiting.join(30)

    assert waiting.error is None and _waited(head, "capacity-lock-busy")
    head.leave()


def test_cpu_only_coholder_shortfall_waits(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    account = services.root / "admission"
    sharing.record_parallel_runs(account, 2)
    bind_test_capacity(services, daemon_cpus=3, host_cpus=8)
    first = _open(services, projects[0], "cpu-a", cpus=2).enter()
    second = _open(services, projects[1], "cpu-b", cpus=2)

    waiting = EnterThread(second)
    wait_until(lambda: second.status().state == "queued", message="the joiner queued")
    assert queue_phases(account)[second.run_id] == "queued"
    # The reason is recorded only at the grant, so hold the first run for at least one full pass of the joiner.
    _holds_for(1.5, lambda: queue_phases(account)[second.run_id] == "queued")
    first.leave()
    waiting.join(30)

    assert waiting.error is None and _waited(second, "coholder-capacity")
    assert _record(second)["infrastructure_failure"] is None
    second.leave()


def test_a_joiner_waits_while_host_cpu_pressure_is_high(tmp_path: Path) -> None:
    # Memory fit alone admitted a second stack onto a CPU-saturated CI host; joins now also wait on the
    # host's CPU pressure, while a run alone is never held back by it.
    services, projects = three_private_projects(tmp_path)
    account = services.root / "admission"
    sharing.record_parallel_runs(account, 2)
    bind_test_capacity(services, cpu_pressure=55.0)
    first = _open(services, projects[0], "busy-a").enter()
    second = _open(services, projects[1], "busy-b")

    waiting = EnterThread(second)
    wait_until(lambda: second.status().state in ("queued", "admitting"), message="the joiner waited")
    # Memory fits; only the CPU holds it, at the reservation and again at the grant (either keeps it from granted).
    _holds_for(2.0, lambda: queue_phases(account)[second.run_id] != "granted")
    first.leave()
    waiting.join(30)

    assert waiting.error is None and _waited(second, "cpu-pressure")
    assert _record(second)["infrastructure_failure"] is None
    second.leave()


def test_a_joiner_enters_beside_a_holder_while_host_cpu_pressure_is_low(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    account = services.root / "admission"
    sharing.record_parallel_runs(account, 2)
    bind_test_capacity(services, cpu_pressure=49.0)
    first = _open(services, projects[0], "calm-a").enter()
    second = _open(services, projects[1], "calm-b").enter()

    assert {queue_phases(account)[run.run_id] for run in (first, second)} == {"granted"}
    assert not _waited(second, "cpu-pressure")
    second.leave()
    first.leave()


def _origin(root: Path, url: str) -> None:
    (root / ".git").mkdir(exist_ok=True)
    (root / ".git" / "config").write_text(f'[remote "origin"]\n\turl = {url}\n')


def test_a_repository_scoped_machine_shares_only_within_a_repository(tmp_path: Path) -> None:
    # On a 4-core CI host two repositories' rounds together saturated the CPU and timed Neo4j out; with
    # --share-scope repository a run joins only holders of its own repository (checkouts in any folder).
    services, projects = three_private_projects(tmp_path)
    account = services.root / "admission"
    sharing.record_parallel_runs(account, 3)
    sharing.record_share_scope(account, "repository")
    # One's leave gives one a turn that holds two's head for the grace; this test is not about it.
    bind_test_capacity(services, turn_grace_ns=1_000_000_000)
    _origin(projects[0], "https://github.com/example/one.git")
    _origin(projects[1], "git@github.com:example/one")  # another checkout of the same repository
    _origin(projects[2], "https://github.com/example/two.git")
    first = _open(services, projects[0], "repo-one-a").enter()
    same = _open(services, projects[1], "repo-one-b").enter()  # joins: same repository
    other = _open(services, projects[2], "repo-two")

    waiting = EnterThread(other)
    wait_until(lambda: other.status().state in ("queued", "admitting"), message="the other repository waited")
    _holds_for(2.0, lambda: queue_phases(account)[other.run_id] != "granted")
    assert {queue_phases(account)[run.run_id] for run in (first, same)} == {"granted"}
    same.leave()
    first.leave()
    waiting.join(30)

    assert waiting.error is None and _waited(other, "other-repository")
    other.leave()


def test_a_repository_overtakes_a_head_of_another_repository_while_it_holds(tmp_path: Path) -> None:
    # CI host: the head waited for another repository's holders, whose own next run waited behind it, so nothing
    # overlapped. A run of the holders' repository may now pass heads of other repositories.
    services, projects = three_private_projects(tmp_path)
    account = services.root / "admission"
    sharing.record_parallel_runs(account, 3)
    sharing.record_share_scope(account, "repository")
    # One's leave gives one a turn that holds two's head for the grace; this test is not about it.
    bind_test_capacity(services, turn_grace_ns=1_000_000_000)
    _origin(projects[0], "https://github.com/example/one.git")
    _origin(projects[1], "https://github.com/example/one.git")
    _origin(projects[2], "https://github.com/example/two.git")
    first = _open(services, projects[0], "one-a").enter()
    other = _open(services, projects[2], "two")
    waiting = EnterThread(other)
    wait_until(lambda: other.status().state == "queued", message="the other repository queued first")
    same = _open(services, projects[1], "one-b").enter()  # later ticket, same repository: passes the head

    assert queue_phases(account)[other.run_id] == "queued"
    same.leave()
    first.leave()
    waiting.join(30)
    assert waiting.error is None and _waited(other, "other-repository")
    other.leave()


def test_no_run_overtakes_a_head_that_waited_past_the_window(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    account = services.root / "admission"
    sharing.record_parallel_runs(account, 3)
    sharing.record_share_scope(account, "repository")
    bind_test_capacity(services, overtake_window_ns=1)  # every head has waited past the window
    _origin(projects[0], "https://github.com/example/one.git")
    _origin(projects[1], "https://github.com/example/one.git")
    _origin(projects[2], "https://github.com/example/two.git")
    first = _open(services, projects[0], "one-a").enter()
    other = _open(services, projects[2], "two")
    head = EnterThread(other)
    wait_until(lambda: other.status().state == "queued", message="the other repository queued first")
    same = _open(services, projects[1], "one-b")
    behind = EnterThread(same)
    wait_until(lambda: same.status().state == "queued", message="the same repository queued behind")
    _holds_for(2.0, lambda: queue_phases(account)[same.run_id] == "queued")  # FIFO again: the holders drain

    first.leave()
    head.join(30)
    other.leave()
    behind.join(30)
    assert head.error is None and behind.error is None
    same.leave()


def test_joiner_own_cpus_exceed_daemon_refused_insufficient_cpu(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    account = services.root / "admission"
    sharing.record_parallel_runs(account, 2)
    bind_test_capacity(services, daemon_cpus=2, host_cpus=8)
    first = _open(services, projects[0], "small", cpus=1).enter()
    too_big = _open(services, projects[1], "too-big", cpus=3)

    refused = EnterThread(too_big)
    refused.join(30)

    assert refused.error is not None
    assert _record(too_big)["infrastructure_failure"] == "insufficient-cpu"
    assert queue_phases(account) == {first.run_id: "granted"}
    first.leave()


def test_auto_limit_admits_three_fitting_runs_and_waits_fourth_on_fit(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    fourth_project = tmp_path / "project-d"
    fourth_project.mkdir()
    _origin(fourth_project, "https://example.invalid/project-d.git")
    account = services.root / "admission"
    sharing.record_parallel_runs(account, "auto")
    bind_test_capacity(services, daemon_cpus=3, host_cpus=8)
    holders = [_open(services, project, f"auto-{index}").enter() for index, project in enumerate(projects)]
    fourth = _open(services, fourth_project, "auto-fourth")

    waiting = EnterThread(fourth)
    wait_until(lambda: fourth.status().state == "queued", message="the fourth queued")
    assert sorted(queue_phases(account).values()) == ["granted", "granted", "granted", "queued"]
    holders[0].leave()
    waiting.join(30)

    assert waiting.error is None
    assert _waited(fourth, "coholder-capacity") and not _waited(fourth, "parallel-limit")
    for handle in [*holders[1:], fourth]:
        handle.leave()


@pytest.mark.parametrize(
    ("start", "lowered", "reason"), [(2, 1, "queue-position"), (3, 2, "parallel-limit")], ids=["to-one", "to-two"]
)
def test_limit_lowered_with_two_granted_holders_waits_without_preemption(
    tmp_path: Path, start: int, lowered: int, reason: str
) -> None:
    services, projects = three_private_projects(tmp_path)
    account = services.root / "admission"
    sharing.record_parallel_runs(account, start)
    first = _open(services, projects[0], "lowered-a").enter()
    second = _open(services, projects[1], "lowered-b").enter()
    sharing.record_parallel_runs(account, lowered)
    third = _open(services, projects[2], "lowered-c")

    waiting = EnterThread(third)
    wait_until(lambda: third.status().state == "queued", message="the third queued")
    first.leave()
    if lowered == 1:  # rule 0: it waits until no holder is left
        wait_until(lambda: first.run_id not in queue_phases(account), message="the first left")
        _holds_for(1.0, lambda: queue_phases(account)[third.run_id] == "queued")
        second.leave()
    waiting.join(30)

    assert waiting.error is None and _waited(third, reason)
    assert _record(first)["infrastructure_failure"] is None and _record(second)["infrastructure_failure"] is None
    if lowered != 1:
        second.leave()
    third.leave()


def _paused_at_slot_reserved(tmp_path: Path, services, handle, name: str):
    """Start a failpoint guardian that writes ``<name>-reserved`` after reserving and waits for ``<name>-release``."""
    prepare_for_child_guardian(handle)
    reserved, release = tmp_path / f"{name}-reserved", tmp_path / f"{name}-release"
    stages = {
        "slot-reserved": {
            "action": [{"do": "write-file", "path": str(reserved)}, {"do": "wait-file", "path": str(release)}]
        }
    }
    guardian, _ = start_guardian_child(tmp_path, handle, services.root / "admission", stages, name=f"guardian-{name}")
    return guardian, reserved, release


def test_publication_waits_for_earlier_paused_admitting_entry(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    account = services.root / "admission"
    sharing.record_parallel_runs(account, 2)
    first = _open(services, projects[0], "paused-a")
    guardian, reserved, release = _paused_at_slot_reserved(tmp_path, services, first, "a")
    try:
        entering_first = EnterThread(first, existing_attempt=True)
        wait_until(reserved.exists, message="the first reserved")
        second = _open(services, projects[1], "behind-a")
        entering_second = EnterThread(second)
        wait_until(lambda: queue_phases(account).get(second.run_id) == "admitting", message="the second reserved")
        # two seconds of publication passes: the second never publishes before the first
        _holds_for(2.0, lambda: queue_phases(account)[second.run_id] == "admitting")
        release.write_text("go")
        entering_first.join(30)
        entering_second.join(30)
        assert entering_first.error is None and entering_second.error is None
        assert _record(first)["hold_timeline"]["granted_ns"] < _record(second)["hold_timeline"]["granted_ns"]
        assert _waited(second, "queue-position")
        first.leave()
        second.leave()
        guardian.wait(timeout=30)
    finally:
        reap(guardian)


def test_limit_lowered_with_two_admitting_entries_publishes_earliest_only(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    account = services.root / "admission"
    sharing.record_parallel_runs(account, 2)
    # A queue deadline that outlasts the pauses and the 30 s window on a loaded CI host (30 s did not).
    first = _open(services, projects[0], "admit-a", wait_seconds=180)
    second = _open(services, projects[1], "admit-b", wait_seconds=180)
    guardian_a, reserved_a, release_a = _paused_at_slot_reserved(tmp_path, services, first, "a")
    guardian_b, reserved_b, release_b = _paused_at_slot_reserved(tmp_path, services, second, "b")
    try:
        entering_first = EnterThread(first, existing_attempt=True)
        wait_until(reserved_a.exists, message="the first reserved")
        entering_second = EnterThread(second, existing_attempt=True)
        wait_until(reserved_b.exists, message="the second reserved")
        sharing.record_parallel_runs(account, 1)
        release_a.write_text("go")
        release_b.write_text("go")
        entering_first.join(180)
        wait_until(lambda: queue_phases(account).get(first.run_id) == "granted", message="the first granted")
        _holds_for(1.0, lambda: queue_phases(account)[second.run_id] == "admitting")
        first.leave()
        entering_second.join(180)
        assert entering_first.error is None and entering_second.error is None
        assert _waited(second, "queue-position")
        second.leave()
        guardian_a.wait(timeout=30)
        guardian_b.wait(timeout=30)
    finally:
        reap(guardian_a)
        reap(guardian_b)


def test_crash_between_queue_and_record_write_blocks_joins(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    account = services.root / "admission"
    sharing.record_parallel_runs(account, 2)
    crashed = _open(services, projects[0], "crashed")
    prepare_for_child_guardian(crashed)
    stages = {"reserve-queue-written": {"action": {"do": "sigkill"}}}
    guardian, _ = start_guardian_child(tmp_path, crashed, account, stages, name="guardian-crashed")
    try:
        guardian.wait(timeout=30)
        assert queue_phases(account)[crashed.run_id] == "admitting" and _record(crashed)["state"] == "queued"
        joiner = _open(services, projects[1], "after-crash", wait_seconds=4)
        waiting = EnterThread(joiner)
        waiting.join(30)
    finally:
        reap(guardian)

    assert waiting.error is not None  # its budget ended: the holder's record carries no shared mode
    # one snapshot: the joiner's own guardian process removes its entry at any time after the budget ends
    assert queue_phases(account).get(joiner.run_id) in (None, "queued")


def test_holder_appearing_after_sample_waits_without_writes(tmp_path: Path) -> None:
    """A joiner whose pass sampled while no holder existed never decides its fit on that sample."""
    services, projects = three_private_projects(tmp_path)
    account = services.root / "admission"
    sharing.record_parallel_runs(account, 2)
    first, second = _open(services, projects[0], "sample-a"), _open(services, projects[1], "sample-b")
    paused_a, release_a = tmp_path / "a-sampled", tmp_path / "a-release"
    paused_b, release_b = tmp_path / "b-sampled", tmp_path / "b-release"
    prepare_for_child_guardian(first)
    stages_a = {
        "admission-sample-taken": {
            "action": [{"do": "write-file", "path": str(paused_a)}, {"do": "wait-file", "path": str(release_a)}],
            "occurrence": 1,
        }
    }
    guardian_a, _ = start_guardian_child(tmp_path, first, account, stages_a, name="guardian-sample-a")
    log = tmp_path / "sample-b.log"
    guardian_b = None
    try:
        entering_first = EnterThread(first, existing_attempt=True)
        wait_until(paused_a.exists, message="the first paused after its sample")
        prepare_for_child_guardian(second)
        capture = {"queue": str(account / "queue.json")}
        stages_b = {
            "admission-sample-taken": {
                "action": [
                    {"do": "write-file", "path": str(paused_b), "capture": capture},
                    {"do": "wait-file", "path": str(release_b)},
                ],
                "occurrence": 1,
            }
        }
        guardian_b, _ = start_guardian_child(
            tmp_path, second, account, stages_b, name="guardian-sample-b", observer_log=log
        )
        entering_second = EnterThread(second, existing_attempt=True)
        wait_until(paused_b.exists, message="the second paused after its sample")
        release_a.write_text("go")
        wait_until(lambda: _record(first)["state"] in ("admitting", "active"), message="the first reserved")
        before = (queue_phases(account)[second.run_id], _record(second)["state"])
        release_b.write_text("go")
        entering_first.join(30)
        entering_second.join(30)

        sampled = json.loads(bytes.fromhex(json.loads(paused_b.read_text())["queue"]))
        assert all(entry["phase"] == "queued" for entry in sampled["entries"].values())  # holder-free inputs
        assert before == ("queued", "queued")
        assert entering_first.error is None and entering_second.error is None
        assert _waited(second, "coholder-capacity")  # the first decision after the holder appeared
        lines = [json.loads(line) for line in log.read_text().splitlines()]
        released = next(index for index, line in enumerate(lines) if line.get("stage") == "admission-sample-taken")
        next_sample = next(
            index
            for index, line in enumerate(lines)
            if index > released and line.get("stage") == "admission-sample-taken"
        )
        assert not any(  # no capacity acquisition in the pass that sampled holder-free
            line.get("kind") == "lock" and line.get("name") == "capacity" for line in lines[released:next_sample]
        )
        first.leave()
        second.leave()
        guardian_a.wait(timeout=30)
        guardian_b.wait(timeout=30)
    finally:
        reap(guardian_a)
        if guardian_b is not None:
            reap(guardian_b)


def _seed_from_record(context) -> None:
    """Seed a positive verdict for a run prepared elsewhere (a requester child), from its own record's request."""
    data = json.loads(context.record.read_text())
    origin = sharing.repository_origin(data["project_root"])
    assert origin is not None, "test projects carry a Git origin"
    key = sharing.request_key(origin, data["entrypoint"], data["profile"], data["closure"])
    path = sharing.verdict_path(context.project.state, context.project.project_id)
    path.parent.mkdir(mode=0o700, exist_ok=True)
    verdict = {"shareable": True, "reason": None, "run_id": context.run_id, "origin": "seed", "tick_ns": 0}
    atomic_json(path, {"rules": SHARE_RULES_VERSION, "schema": 1, "verdicts": {key: verdict}})


def _kill_shared_guardian(context) -> None:
    """SIGKILL a shared holder's guardian and wait until it is gone; its SH capacity lock never blocks a SH fence."""
    data = json.loads(context.record.read_text())
    identity = lifecycle._identity_from_json(data["guardian"])
    os.kill(identity.pid, signal.SIGKILL)
    reap_orphan(identity.pid)
    wait_until(lambda: lifecycle.observe_process(identity) != "executing", message="the guardian ended")


def test_dead_shared_holder_recovered_in_sh_beside_live_holder(tmp_path: Path) -> None:
    """A dead shared holder is recovered under a shared fence: the live co-holder keeps running untouched."""
    services, projects = three_private_projects(tmp_path)
    account = services.root / "admission"
    sharing.record_parallel_runs(account, 2)
    live = _open(services, projects[0], "live-holder").enter()
    dying = start_requester_child(tmp_path, services.root / "registry", account, projects[1], {}, gated=True)
    try:
        dead = dying.context()
        _seed_from_record(dead)
        dying.open_gate()
        wait_until(dying.entered_file.exists, timeout=30, message="the second holder entered")
        assert queue_phases(account)[dead.run_id] == "granted"
        assert json.loads(dead.record.read_text())["sharing"] == {"mode": "shared"}
        dying.process.kill()
        dying.process.wait(timeout=30)
        _kill_shared_guardian(dead)
    finally:
        reap(dying.process)
    seed_dead_since(account, dead.run_id, age_s=31)
    live_before = live._context.record.read_bytes()
    waiter = _open(services, projects[2], "after-dead-holder")

    waiting = EnterThread(waiter)
    waiting.join(
        60
    )  # the queued guardian spawns the takeover; it recovers the dead holder under SH beside the live one

    assert waiting.error is None
    record = json.loads(dead.record.read_text())
    assert (record["state"], record["recovery_outcome"]) == ("complete", "complete")
    assert live._context.record.read_bytes() == live_before
    assert queue_phases(account) == {live.run_id: "granted", waiter.run_id: "granted"}
    live.leave()
    waiter.leave()


def test_predicted_shareable_run_whose_phase_maximum_never_fits_is_admitted_exclusive(tmp_path: Path) -> None:
    """E(self) is the field-wise maximum of a shared run's phases; when that can never fit the machine although each
    phase does, the run is admitted exclusive (the per-phase rule) instead of waiting as a shared holder until its
    deadline."""
    services, projects = three_private_projects(tmp_path)
    account = services.root / "admission"
    sharing.record_parallel_runs(account, 2)
    # With the 4.5 GiB host reserve each phase fits 14 GiB of host memory (9 and 12.25 GiB); their maximum does not.
    bind_test_capacity(services, daemon_memory_bytes=6 * GIB, host_memory_bytes=14 * GIB, daemon_cpus=4, host_cpus=8)
    phases = {"build": PhaseBudget(4 * GIB, 1, 2, 1), "tests": PhaseBudget(GIB, 6 * GIB, 1, 1)}
    run = services.open_run(_spec(projects[0], "phases-shared", phases=phases))
    _seed_shareable(run)
    run.enter()

    assert _record(run)["admission"] == "published"
    assert "sharing" not in _record(run) and _capacity_mode(account) == "EX"
    assert not _waited(run, "coholder-capacity")
    run.leave()


def test_shared_observation_reads_no_limit(tmp_path: Path) -> None:
    """An invalid limit stops new admissions but never the co-holder read of an incumbent's observation."""
    services, projects = three_private_projects(tmp_path)
    account = services.root / "admission"
    sharing.record_parallel_runs(account, 2)
    first = _open(services, projects[0], "observe-a").enter()
    second = _open(services, projects[1], "observe-b").enter()
    (account / "parallel-runs.json").write_text('{"parallel_runs": 0, "schema": 1}')
    (account / "parallel-runs.json").chmod(0o600)
    try:
        read = lifecycle._read_coholders(first._context, account, lifecycle.Deadline.after(lifecycle.NativeClock(), 10))
        assert [holder.run_id for holder in read.holders] == [second.run_id] and not read.unknown
    finally:
        sharing.record_parallel_runs(account, 2)
    first.leave()
    second.leave()


def test_older_version_queued_entry_blocks_join(tmp_path: Path) -> None:
    """M1: a shared head waits ``older-version-entry`` beside shared holders while a previous-release entry queues."""
    services, projects = three_private_projects(tmp_path)
    fourth = tmp_path / "project-d"
    fourth.mkdir()
    account = services.root / "admission"
    sharing.record_parallel_runs(account, 3)
    holder = _open(services, projects[0], "m1-holder").enter()
    head = _open(services, projects[1], "m1-head")
    prepare_for_child_guardian(head)
    paused, release = tmp_path / "head-sampled", tmp_path / "head-release"
    stages = {
        "admission-sample-taken": {
            "action": [{"do": "write-file", "path": str(paused)}, {"do": "wait-file", "path": str(release)}],
            "occurrence": 1,
        }
    }
    guardian, _ = start_guardian_child(tmp_path, head, account, stages, name="guardian-m1-head")
    older = services.open_run(
        RunSpec(
            project_root=fourth,
            entrypoint="integration",
            purpose="previous-release",
            services=(),
            closure=(),
            profile="fixture",
            phases={"tests": PhaseBudget(GIB, 1, 1, 1)},
            initial_phase="tests",
            artifact=source_artifact(),
            wait_seconds=60,
        )
    )
    try:
        entering_head = EnterThread(head, existing_attempt=True)
        wait_until(paused.exists, message="the head paused before its reservation")
        waiting_older = EnterThread(older)
        wait_until(lambda: older.run_id in queue_phases(account), message="the older entry queued behind the head")
        planted = json.loads(older._context.record.read_text())  # the record a previous release's guardian wrote
        planted["artifact"]["version"] = "0.5.3"
        atomic_json(older._context.record, planted)
        release.write_text("go")
        _holds_for(2.0, lambda: queue_phases(account)[head.run_id] == "queued")
        older.cancel() if older.status().state == "prepared" else None
        holder.leave()  # the holder leaves: the head no longer joins beside anyone and is admitted alone
        entering_head.join(30)
        assert entering_head.error is None and _waited(head, "older-version-entry")
        head.leave()
        waiting_older.join(60)
        if waiting_older.error is None:
            older.leave()
        guardian.wait(timeout=30)
    finally:
        reap(guardian)


def test_unreadable_coholder_record_reads_as_unknown(tmp_path: Path) -> None:
    """The co-holder read marks an unreadable granted record unknown, which the observation makes sticky."""
    services, projects = three_private_projects(tmp_path)
    account = services.root / "admission"
    sharing.record_parallel_runs(account, 2)
    first = _open(services, projects[0], "unknown-a").enter()
    second = _open(services, projects[1], "unknown-b").enter()
    # Not private (0644): rejected unread, also for root (CI jobs run as root). Its own guardian would reject it too,
    # so it is paused while the record is damaged.
    guardian = pause_guardian(second._context)
    second._context.record.chmod(0o644)
    try:
        read = lifecycle._read_coholders(first._context, account, lifecycle.Deadline.after(lifecycle.NativeClock(), 10))
    finally:
        second._context.record.chmod(0o600)
        resume_guardian(guardian)

    assert read.unknown is True and read.holders == ()
    first.leave()
    second.leave()


def _dead_shared_holder(tmp_path: Path, services, account: Path, project: Path):
    """Enter a shared run through a requester child, then kill the child and its guardian: a dead shared holder."""
    dying = start_requester_child(tmp_path, services.root / "registry", account, project, {}, gated=True)
    try:
        dead = dying.context()
        _seed_from_record(dead)
        dying.open_gate()
        wait_until(dying.entered_file.exists, timeout=30, message="the shared holder entered")
        dying.process.kill()
        dying.process.wait(timeout=30)
        _kill_shared_guardian(dead)
    finally:
        reap(dying.process)
    seed_dead_since(account, dead.run_id, age_s=31)
    return dead


def test_dead_shared_recoverer_death_retakes_sh_fence(tmp_path: Path) -> None:
    """A recoverer killed after its first record write leaves the run to the next attempt, which fences SH again."""
    services, projects = three_private_projects(tmp_path)
    account = services.root / "admission"
    sharing.record_parallel_runs(account, 2)
    live = _open(services, projects[0], "live-beside-recovery").enter()
    dead = _dead_shared_holder(tmp_path, services, account, projects[1])
    live_before = live._context.record.read_bytes()
    dead_before = json.loads(dead.record.read_text())
    log = tmp_path / "first-recoverer.log"
    first, _ = start_recover_child(
        tmp_path,
        dead,
        account,
        {"empty-grant-record-written": {"action": {"do": "sigkill"}}},
        name="first",
        observer_log=log,
    )
    try:
        first.wait(timeout=60)
    finally:
        reap(first)
    assert first.returncode == -signal.SIGKILL  # died at the failpoint, not before it
    interrupted = json.loads(dead.record.read_text())
    assert interrupted["revision"] > dead_before["revision"]  # the first recovery record write is durable
    assert (interrupted["state"], interrupted["recovery_outcome"]) != ("complete", "complete")
    second, result = start_recover_child(
        tmp_path, dead, account, {}, name="second", observer_log=tmp_path / "second.log"
    )
    try:
        second.wait(timeout=120)
    finally:
        reap(second)

    record = json.loads(dead.record.read_text())
    assert (record["state"], record["recovery_outcome"]) == ("complete", "complete"), result.read_text()
    fences = [
        json.loads(line)
        for line in (tmp_path / "second.log").read_text().splitlines()
        if '"capacity"' in line and '"acquire"' in line
    ]
    assert fences and all(line["shared"] is True for line in fences)  # the retry fenced SH beside the live holder
    assert live._context.record.read_bytes() == live_before
    assert queue_phases(account) == {live.run_id: "granted"}
    live.leave()


# ---- grants log repair by recovery ---------------------------------------------------------


def _granted_beside_then_dead_at_publication(tmp_path: Path, services, account: Path, projects: list[Path]):
    """A live shared holder A; B granted beside it whose guardian dies at ``grant-published``, after its grant write
    and before its grants-log append; B's requester (this process) is then recorded as dead in B's record only."""
    holder = _open(services, projects[0], "live-holder").enter()
    publisher = _open(services, projects[1], "dies-at-publication")
    prepare_for_child_guardian(publisher)
    stages = {"grant-published": {"action": {"do": "sigkill"}}}
    guardian, _ = start_guardian_child(tmp_path, publisher, account, stages, name="guardian-publisher")
    entering = EnterThread(publisher, existing_attempt=True)
    try:
        guardian.wait(timeout=60)
        entering.join(60)
    finally:
        reap(guardian)
    assert guardian.returncode == -signal.SIGKILL
    assert queue_phases(account)[publisher.run_id] == "granted"
    assert all(grant.run_id != publisher.run_id for grant in sharing.read_grants(account))  # the crash point
    record = publisher._context.record
    atomic_json(record, replace_identity(json.loads(record.read_text()), current_identity(), dead_identity()))
    seed_dead_since(account, publisher.run_id, age_s=31)
    return holder, publisher._context


def _recover(tmp_path: Path, dead, account: Path, stages: dict, name: str) -> int:
    child, _result = start_recover_child(
        tmp_path, dead, account, stages, name=name, observer_log=tmp_path / f"{name}.log"
    )
    try:
        child.wait(timeout=120)
    finally:
        reap(child)
    return child.returncode


def _allowance_of(holder, account: Path) -> int:
    memory = json.loads(holder.record.read_text())["admission_observation"]["memory"]
    return allowance_bytes(
        sharing.read_grants(account),
        run_id=holder.run_id,
        origin=memory["origin"],
        admission_baseline_ns=memory["tick_ns"],
    )


def test_grant_repair_by_recovery(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    account = services.root / "admission"
    sharing.record_parallel_runs(account, 2)
    holder, dead = _granted_beside_then_dead_at_publication(tmp_path, services, account, projects)
    try:
        assert _recover(tmp_path, dead, account, {}, "recoverer") == 0

        grants = [grant for grant in sharing.read_grants(account) if grant.run_id == dead.run_id]
        assert [(grant.generation, grant.budget_bytes) for grant in grants] == [(1, GIB + 1)]  # container + host budget
        assert dead.run_id not in queue_phases(account)  # the entry went only after the log was repaired
        assert _allowance_of(holder._context, account) == GIB + 1  # A, admitted before B, now counts B's budget
    finally:
        holder.leave()


def test_recovery_crash_after_grant_append_counts_grant_once(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    account = services.root / "admission"
    sharing.record_parallel_runs(account, 2)
    holder, dead = _granted_beside_then_dead_at_publication(tmp_path, services, account, projects)
    try:
        crash = {"recovery-grant-appended": {"action": {"do": "sigkill"}}}
        assert _recover(tmp_path, dead, account, crash, "first") == -signal.SIGKILL
        assert dead.run_id in queue_phases(account)  # died after the upsert, before the queue removal
        assert _recover(tmp_path, dead, account, {}, "second") == 0

        assert [grant.run_id for grant in sharing.read_grants(account)].count(dead.run_id) == 1
        assert _allowance_of(holder._context, account) == GIB + 1  # counted once
    finally:
        holder.leave()


def test_publication_beside_a_coholder_logs_its_grant_and_a_lone_one_does_not(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    account = services.root / "admission"
    sharing.record_parallel_runs(account, 2)
    alone = _open(services, projects[0], "granted-alone").enter()
    assert sharing.read_grants(account) == []  # no co-holder at its publication: never logged
    beside = _open(services, projects[1], "granted-beside", gib=2, cpus=2).enter()  # no builder reserve on top
    # The guardian appends the grant after the grant write that ends enter(), in the same locked section.
    wait_until(lambda: sharing.read_grants(account) != [], message="the publication's grant logged")

    grants = sharing.read_grants(account)

    record = _record(beside)
    assert [(grant.run_id, grant.generation, grant.granted_ns) for grant in grants] == [
        (beside.run_id, 1, record["hold_timeline"]["granted_ns"])
    ]
    assert _allowance_of(alone._context, account) == 2 * GIB + 1
    beside.leave()
    alone.leave()
    # the lone run's own leave repairs nothing: no holder could count a grant made alone
    assert [grant.run_id for grant in sharing.read_grants(account)] == [beside.run_id]


def test_leave_repairs_a_grant_its_publication_failed_to_log(tmp_path: Path) -> None:
    """A publication whose log append failed (the grant itself is written) is repaired by the run's own leave,
    before its entry is removed, so co-holders' allowances keep its budget after it has gone."""
    services, projects = three_private_projects(tmp_path)
    account = services.root / "admission"
    sharing.record_parallel_runs(account, 2)
    alone = _open(services, projects[0], "stays").enter()
    beside = _open(services, projects[1], "leaves").enter()
    # The guardian appends the grant after the grant write that ends enter(); wait for it, then undo it.
    wait_until(lambda: sharing.read_grants(account) != [], message="the publication's grant logged")
    (account / sharing.GRANTS_FILE).unlink()  # as if the publication's append had failed

    beside.leave()

    assert [grant.run_id for grant in sharing.read_grants(account)] == [beside.run_id]
    assert _allowance_of(alone._context, account) == GIB + 1
    alone.leave()
