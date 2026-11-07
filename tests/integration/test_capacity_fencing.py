"""Capacity fencing: recovery and release take the capacity lock before they touch Docker."""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest

from docker_test_toolkit import ArtifactIdentity, PhaseBudget, RunHandle, RunSpec
from docker_test_toolkit.application import lifecycle
from docker_test_toolkit.application.recovery import Context, atomic_json, lock
from docker_test_toolkit.domain.errors import ResourceBusy
from docker_test_toolkit.factories.cli import bind_docker_adapter, build_docker_adapter
from docker_test_toolkit.factories.testing import (
    _TestServices,
    build_test_services,
    install_failpoint,
    install_lock_observer,
)
from tests.integration.account_fixtures import (
    EnterThread,
    Interrupted,
    _hold_lock,
    child_result,
    current_identity,
    dead_identity,
    kill_guardian,
    queue_entries,
    queue_phases,
    reap,
    retire_actor,
    source_artifact,
    start_requester_child,
    wait_guardians_exited,
    wait_until,
)
from tests.integration.test_takeover import _crash_at, _empty_grant, _recover, _resource_grant
from tests.unit.test_docker_receipt_service import IMAGE_ID, FakeDockerPort

pytestmark = pytest.mark.integration

REFERENCE = "example/cache@sha256:" + "2" * 64


@pytest.fixture
def fake_docker() -> Iterator[FakeDockerPort]:
    """A Docker fake that lifecycle's release and recovery paths use, restoring the production composition after."""
    fake = FakeDockerPort()
    fake.images = {IMAGE_ID: {"Id": IMAGE_ID, "Created": "shared-created", "Config": {"Labels": {}}, "RepoDigests": []}}
    lifecycle.configure_docker_factories(lambda: fake, lambda _identity: fake)
    yield fake
    lifecycle.configure_docker_factories(build_docker_adapter, bind_docker_adapter)


def _retained_spec(project: Path, artifact: ArtifactIdentity | None = None) -> RunSpec:
    return RunSpec(
        project_root=project,
        entrypoint="integration",
        purpose="fencing",
        services=(),
        closure=(),
        profile="fixture",
        phases={"tests": PhaseBudget(2 * 1024**3, 1, 2, 1)},
        initial_phase="tests",
        artifact=artifact or source_artifact(),
        retain=True,
        wait_seconds=60,
    )


def _capacity_spec(project: Path, purpose: str) -> RunSpec:
    return RunSpec(
        project_root=project,
        entrypoint="integration",
        purpose=purpose,
        services=(),
        closure=(),
        profile="fixture",
        phases={"tests": PhaseBudget(0, 1, 1, 1)},
        initial_phase="tests",
        artifact=source_artifact(),
        wait_seconds=30,
    )


def _exit_at(stage: str) -> Callable[[str], None]:
    def callback(name: str) -> None:
        if name == stage:
            raise SystemExit(f"fixture exit at {stage}")

    return callback


def _helper_record(registry: Path, a_context: lifecycle.RunContext) -> dict:
    """Return the release helper's record, named by the target's ``release_authority``."""
    authority = json.loads(a_context.record.read_text())["release_authority"]
    return json.loads(Path(authority["record"]).read_text())


@dataclass
class _ReleaseWorld:
    """A retained capacity-one run A whose releaser died after it wrote ``stopping`` (its helper left, terminal)."""

    services: _TestServices
    registry: Path
    account: Path
    a: RunHandle
    context: lifecycle.RunContext
    fake: FakeDockerPort


def _interrupted_release(tmp_path: Path, fake: FakeDockerPort) -> _ReleaseWorld:
    services = build_test_services(tmp_path / "state")
    registry, account = services.root / "registry", services.root / "admission"
    project_a = tmp_path / "project-a"
    project_a.mkdir()
    a = services.open_run(_retained_spec(project_a), docker_factory=lambda: fake).enter()
    descriptor = json.loads(((a.docker_context, a.private_root)[1] / "docker-authority.json").read_text())
    lifecycle.dispatch_docker(a._context, descriptor, fake, ("pull", REFERENCE))
    assert a.retain().state == "stopped-retained"
    install_failpoint(_exit_at("release-stopping-written"))
    try:
        with pytest.raises(SystemExit):
            a.release()
    finally:
        install_failpoint(None)
    wait_guardians_exited(registry)  # the release helper's guardian de-indexes it after its leave returns
    helper = _helper_record(registry, a._context)
    assert (helper["state"], helper["cleanup_outcome"]) == ("complete", "complete")
    assert helper["test_outcome"] in {"failed", "cancelled"}
    assert helper["run_id"] not in json.loads(Context(project_a.resolve(), registry).record.read_text())["runs"]
    assert lifecycle._is_release_shape(json.loads(a._context.record.read_text()))
    retire_actor(registry, current_identity(), dead_identity(), account=account)
    return _ReleaseWorld(services, registry, account, a, a._context, fake)


def test_release_recovery_never_removes_while_a_foreign_run_holds_capacity(
    tmp_path: Path, fake_docker: FakeDockerPort
) -> None:
    fake = fake_docker
    world = _interrupted_release(tmp_path, fake)
    registry, account, a_context = world.registry, world.account, world.context
    project_b = tmp_path / "project-b"
    project_b.mkdir()
    # B: a live capacity-one run of another project; its guardian holds the capacity lock and it bound the image.
    b = start_requester_child(tmp_path, registry, account, project_b, {}, name="foreign", leave=True)
    try:
        wait_until(b.entered_file.exists, timeout=30, message="B entered")
        b_context = b.context()
        with lock(b_context.project, f"run-{b_context.run_id}"):
            record = json.loads(b_context.record.read_text())
            record["image_bindings"][REFERENCE] = {
                "created": "shared-created",
                "identifier": IMAGE_ID,
                "ownership": "external",
            }
            record["revision"] += 1
            atomic_json(b_context.record, record)
        before = (a_context.record.read_bytes(), b_context.record.read_bytes(), (account / "queue.json").read_bytes())
        commands = len(fake.commands)

        refusal: ResourceBusy | None = None
        try:
            lifecycle.recover_run(a_context, _test_directory=account)
        except ResourceBusy as error:
            refusal = error

        removals = [argv for argv in fake.commands[commands:] if argv[:3] == ("image", "rm", "--force")]
        assert removals == [], f"recovery removed {removals} while a foreign run held the capacity lock"
        assert refusal is not None and refusal.reason == "capacity-lock-held"
        after = (a_context.record.read_bytes(), b_context.record.read_bytes(), (account / "queue.json").read_bytes())
        assert after == before
    finally:
        b.release()
        try:
            child_result(b.process, b.result, cap=60)
        finally:
            reap(b.process)
    wait_guardians_exited(registry)  # B's guardian closes its capacity descriptor after B's leave returns
    # Once the foreign run left, the retry completes A exactly once.
    status = lifecycle.recover_run(a_context, _test_directory=account)
    assert (status.state, status.cleanup_outcome) == ("complete", "complete")
    assert [argv for argv in fake.commands[commands:] if argv[:3] == ("image", "rm", "--force")] == [
        ("image", "rm", "--force", REFERENCE)
    ]


# ---- the fence precedes every write, never waits, and uses the dead run's mode ---------------------------------------


def _watched(call: Callable[[], object]) -> list[tuple]:
    """Run ``call`` and return the lock-observer events and failpoint stages in the order they happened."""
    log: list[tuple] = []
    install_lock_observer(lambda *event: log.append(event))
    install_failpoint(lambda stage: log.append(("stage", stage)))
    try:
        call()
    finally:
        install_failpoint(None)
        install_lock_observer(None)
    return log


def _capacity_acquires(log: list[tuple]) -> list[tuple]:
    return [event for event in log if len(event) == 5 and event[0] == "acquire" and event[1] == "capacity"]


@pytest.fixture(params=["holder", "release", "empty"])
def _recovery_world(request: pytest.FixtureRequest, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """One recovery of each kind that holds the capacity fence: (context, account, recover-callable)."""
    if request.param == "holder":
        _handle, context, account = _resource_grant(tmp_path, monkeypatch)
    elif request.param == "empty":
        context, account = _empty_grant(tmp_path)
    else:
        fake = request.getfixturevalue("fake_docker")
        world = _interrupted_release(tmp_path, fake)
        context, account = world.context, world.account
    return context, account, lambda: _recover(context, account)


def test_capacity_fence_precedes_every_write(_recovery_world) -> None:
    context, account, recover = _recovery_world
    initial = (context.record.read_bytes(), (account / "queue.json").read_bytes())
    at_fence: list[tuple[bytes, bytes]] = []

    def watch(stage: str) -> None:
        if stage == "recovery-capacity-acquired":
            at_fence.append((context.record.read_bytes(), (account / "queue.json").read_bytes()))

    install_failpoint(watch)
    try:
        recover()
    finally:
        install_failpoint(None)

    assert at_fence == [initial]  # the lock was held before the recovery changed the record or the queue


def test_capacity_fence_never_waits(_recovery_world) -> None:
    _context, _account, recover = _recovery_world

    log = _watched(recover)

    acquires = _capacity_acquires(log)
    assert acquires and all(event[3] is False and event[4] is False for event in acquires)  # never wait, no deadline


def test_empty_grant_fence_is_exclusive_and_refuses_contested(tmp_path: Path) -> None:
    context, account = _empty_grant(tmp_path)
    holder = _hold_lock(account / "capacity.lock")
    try:
        before = (context.record.read_bytes(), (account / "queue.json").read_bytes())
        with pytest.raises(ResourceBusy) as busy:
            _recover(context, account)
        assert busy.value.reason == "capacity-lock-held"
        assert (context.record.read_bytes(), (account / "queue.json").read_bytes()) == before
    finally:
        holder.kill()
        holder.wait(timeout=10)

    acquires = _capacity_acquires(_watched(lambda: _recover(context, account)))

    assert [event[2] for event in acquires] == [False]  # the fence is always exclusive


def _dead_empty_holder(services: _TestServices, project: Path, purpose: str) -> lifecycle.RunContext:
    """A granted run with no resources whose guardian was killed and whose requester withdrew and died."""
    handle = services.open_run(_capacity_spec(project, purpose)).enter()
    kill_guardian(handle._context)

    def requester_dies(stage: str) -> None:
        if stage == "leave-withdrawal-written":
            raise Interrupted

    install_failpoint(requester_dies)
    try:
        with pytest.raises(Interrupted):
            handle.leave()
    finally:
        install_failpoint(None)
    return handle._context


def test_terminal_retirement_takes_no_capacity_lock(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A record that is already final only needs its queue entry removed: no Docker work, so no fence."""
    _handle, context, account = _resource_grant(tmp_path, monkeypatch)
    me = current_identity()
    _crash_at("queue-tempfile-written", lambda: _recover(context, account))  # the removal save; the record is final
    assert set(queue_entries(account)) == {context.run_id}
    retire_actor(context.project.state, me, dead_identity(), account=account)

    log = _watched(lambda: _recover(context, account))

    assert queue_entries(account) == {} and _capacity_acquires(log) == []


def test_reconcile_preparations_waits_on_capacity_lock_held_then_contention(
    tmp_path: Path, fake_docker: FakeDockerPort
) -> None:
    """A project's interrupted release waits (no lock held) for a neighbour to leave, then ends in ``lock-deadline``."""
    world = _interrupted_release(tmp_path, fake_docker)
    project_b = tmp_path / "project-b"
    project_b.mkdir()
    b = start_requester_child(tmp_path, world.registry, world.account, project_b, {}, name="foreign", leave=True)
    try:
        wait_until(b.entered_file.exists, timeout=30, message="B entered")
        waits: list[str] = []
        install_failpoint(lambda stage: waits.append(stage) if stage == "reconcile-preparations-wait" else None)
        started = time.monotonic()
        try:
            with pytest.raises(ResourceBusy) as busy:
                lifecycle.reconcile_preparations(
                    world.context.project,
                    _test_directory=world.account,
                    _deadline_ns=time.monotonic_ns() + 1_000_000_000,
                )
        finally:
            install_failpoint(None)
        assert busy.value.reason == "lock-deadline" and len(waits) >= 1 and time.monotonic() - started >= 0.9
        assert json.loads(world.context.record.read_text())["state"] == "stopping"  # nothing was removed meanwhile
    finally:
        b.release()
        try:
            child_result(b.process, b.result, cap=60)
        finally:
            reap(b.process)

    lifecycle.reconcile_preparations(world.context.project, _test_directory=world.account)

    assert json.loads(world.context.record.read_text())["state"] == "complete"


# ---- interleavings of recoverers that need the capacity lock ------------------------------------------


class _Leader:
    """Run ``call`` in a thread that pauses at ``stage`` (holding whatever locks it has) until released."""

    def __init__(self, call: Callable[[], object], stage: str) -> None:
        self.reached, self.go = threading.Event(), threading.Event()
        self.error: BaseException | None = None
        self.stage = stage
        self.call = call
        self._thread = threading.Thread(target=self._run)

    def _pause(self, name: str) -> None:
        if name == self.stage:
            self.reached.set()
            assert self.go.wait(60), "the leader was never released"

    def _run(self) -> None:
        try:
            self.call()
        except BaseException as error:  # reported to the test by ``finish``
            self.error = error

    def start(self) -> None:
        install_failpoint(self._pause)
        self._thread.start()
        assert self.reached.wait(60), f"the leader never reached {self.stage}"

    def finish(self) -> None:
        self.go.set()
        self._thread.join(60)
        install_failpoint(None)
        assert self.error is None, self.error


def _refused_without_waiting(call: Callable[[], object]) -> ResourceBusy:
    """Run ``call`` expecting a prompt ``capacity-lock-held`` refusal: it returns at once and waits for nothing."""
    log: list[tuple] = []
    install_lock_observer(lambda *event: log.append(event))
    started = time.monotonic()
    try:
        with pytest.raises(ResourceBusy) as busy:
            call()
    finally:
        install_lock_observer(None)
    assert busy.value.reason == "capacity-lock-held" and time.monotonic() - started < 5
    waits = [event for event in log if event[:2] == ("acquire", "metadata") and event[3]]
    assert all(event[4] for event in waits)  # any metadata wait was bounded: the refusal came from the capacity lock
    return busy.value


def _two_recoverer_world(tmp_path: Path, fake: FakeDockerPort):
    """One account holding a dead holder H and an interrupted release of a retained run A."""
    world = _interrupted_release(tmp_path, fake)
    project_h = tmp_path / "project-h"
    project_h.mkdir()
    dead = _dead_empty_holder(world.services, project_h, "dead-holder")
    return world, dead


def _removals(fake: FakeDockerPort, since: int) -> list[tuple[str, ...]]:
    return [argv for argv in fake.commands[since:] if argv[:3] == ("image", "rm", "--force")]


def test_interleaving_takeover_then_release_recovery(tmp_path: Path, fake_docker: FakeDockerPort) -> None:
    """T holds the lock (a dead holder's recovery); RR needs it too and refuses."""
    world, dead = _two_recoverer_world(tmp_path, fake_docker)
    a_bytes = world.context.record.read_bytes()
    commands = len(fake_docker.commands)
    leader = _Leader(lambda: _recover(dead, world.account), "empty-grant-record-written")
    leader.start()
    try:
        _refused_without_waiting(lambda: _recover(world.context, world.account))
        assert world.context.record.read_bytes() == a_bytes and _removals(fake_docker, commands) == []
    finally:
        leader.finish()
    assert json.loads(dead.record.read_text())["state"] == "complete"
    assert _recover(world.context, world.account).state == "complete"
    assert _removals(fake_docker, commands) == [("image", "rm", "--force", REFERENCE)]


def test_interleaving_release_recovery_then_takeover(tmp_path: Path, fake_docker: FakeDockerPort) -> None:
    """RR holds the lock; the takeover of a dead holder needs it too and refuses."""
    world, dead = _two_recoverer_world(tmp_path, fake_docker)
    h_bytes, queue_bytes = dead.record.read_bytes(), (world.account / "queue.json").read_bytes()
    leader = _Leader(lambda: _recover(world.context, world.account), "release-recovery-record-written")
    leader.start()
    try:
        _refused_without_waiting(lambda: _recover(dead, world.account))
        assert (dead.record.read_bytes(), (world.account / "queue.json").read_bytes()) == (h_bytes, queue_bytes)
    finally:
        leader.finish()
    assert json.loads(world.context.record.read_text())["state"] == "complete"
    assert _recover(dead, world.account).state == "complete"


# ---- the two-grant crash matrix and a real SIGKILL beside a co-holder ------------------------------------------------


# ---- admission against recoveries and a release-recovery crash ------------------------------------------------


def test_interleaving_release_recovery_blocks_an_admission_until_it_ends(
    tmp_path: Path, fake_docker: FakeDockerPort
) -> None:
    """G' meets RR's exclusive fence: it stays queued, writes nothing, then is admitted."""
    world = _interrupted_release(tmp_path, fake_docker)
    project_g = tmp_path / "project-g"
    project_g.mkdir()
    admitted = world.services.open_run(_capacity_spec(project_g, "g-prime"))
    leader = _Leader(lambda: _recover(world.context, world.account), "release-recovery-record-written")
    leader.start()
    waiting = EnterThread(admitted)
    try:
        wait_until(lambda: admitted.status().state == "queued", message="G' queued")
        time.sleep(1.5)  # several admission attempts, each refused by the lock before it touches the queue
        assert queue_phases(world.account) == {admitted.run_id: "queued"}
    finally:
        leader.finish()
    waiting.join()

    assert waiting.error is None and queue_phases(world.account) == {admitted.run_id: "granted"}
    assert json.loads(world.context.record.read_text())["state"] == "complete"
    admitted.leave()


def test_crash_of_release_recovery_beside_a_dead_holder_resumes_once(
    tmp_path: Path, fake_docker: FakeDockerPort
) -> None:
    world = _interrupted_release(tmp_path, fake_docker)
    project_h = tmp_path / "project-h"
    project_h.mkdir()
    dead = _dead_empty_holder(world.services, project_h, "dead-holder")
    dead_record, commands = dead.record.read_bytes(), len(fake_docker.commands)

    _crash_at("release-recovery-record-written", lambda: _recover(world.context, world.account))

    crashed = json.loads(world.context.record.read_text())
    assert crashed["recovery_outcome"] == "pending" and crashed["recovery_actor"] is not None
    assert _removals(fake_docker, commands) == [] and dead.record.read_bytes() == dead_record
    retire_actor(world.registry, current_identity(), dead_identity(), account=world.account)  # the recoverer is gone
    dead_record = dead.record.read_bytes()  # retiring rewrote this process's identity everywhere, the dead holder too

    status = _recover(world.context, world.account)

    assert status.state == "complete" and _removals(fake_docker, commands) == [("image", "rm", "--force", REFERENCE)]
    assert dead.record.read_bytes() == dead_record  # the dead holder is recovered by its own takeover only
    assert _recover(dead, world.account).state == "complete"
