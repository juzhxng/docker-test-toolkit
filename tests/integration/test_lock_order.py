"""Design 4.3, mechanical enforcement: one call per row of the lock table, observed through the lock observer.

Each row's call runs against a private account with the ``tests/guard/lock_order`` checker installed. A row passes when
no blocking acquisition inverts the rank order P < R < G < M, at least one lock was observed, a row that touches the
queue observed a ``metadata`` acquisition, and every row that bounds waited only with a deadline. The guard
suite keeps the checker's own unit tests; the rows need real accounts, Docker fakes and processes, so they live here.
"""

from __future__ import annotations

import json
import signal
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import pytest

from docker_test_toolkit.application import build_cache, lifecycle
from docker_test_toolkit.application.api import _open_run
from docker_test_toolkit.application.recovery import (
    Context,
    bind_daemon,
    claim_volumes,
    cleanup_project,
    lock,
    record_images,
    register,
)
from docker_test_toolkit.config.runtime import PROJECT_LABEL
from docker_test_toolkit.domain.errors import ResourceBusy, ResourceError
from docker_test_toolkit.factories.testing import build_test_services, install_lock_observer
from tests.guard.lock_order import LockOrderChecker
from tests.integration.account_fixtures import (
    EnterThread,
    ScriptedDocker,
    current_identity,
    dead_identity,
    kill_guardian,
    prepare_for_child_guardian,
    queue_entries,
    reap,
    retire_actor,
    source_artifact,
    start_guardian_child,
    wait_until,
    write_claim_intent,
)
from tests.integration.share_fixtures import _dispatch, _leave, _World, shared_world
from tests.integration.test_docker_receipts import _interrupt_retained_release, _retained_owned_container
from tests.integration.test_project_retirement import _activated, _evidence, _retire, _vanished_project
from tests.integration.test_takeover import (
    ATTEMPT,
    _crashed_terminal_resource_grant,
    _dead_granted_holder,
    _dead_queued_entry,
    _empty_grant,
    _empty_spec,
    _resource_grant,
    _two_projects,
    _worker_arguments,
)
from tests.unit.test_build_cache_service import Run as BuildRun
from tests.unit.test_docker_receipt_service import write_cache_layout

pytestmark = pytest.mark.integration


@dataclass(frozen=True)
class Row:
    """One row of: the observed call, whether it touches the queue and whether every wait is bounded."""

    call: Callable[[], object]
    queue: bool
    bounded: bool


def _soon() -> int:
    return time.monotonic_ns() + 30_000_000_000


def _register(tmp_path: Path, _monkeypatch: pytest.MonkeyPatch) -> Row:
    context = Context(tmp_path / "project", tmp_path / "state")
    return Row(
        lambda: register(
            context,
            session_id="11111111-1111-4111-8111-111111111111",
            temporary_parent=tmp_path,
            deadline_ns=_soon(),
        ),
        queue=False,
        bounded=True,
    )


def _record_images(tmp_path: Path, _monkeypatch: pytest.MonkeyPatch) -> Row:
    context = Context(tmp_path / "project", tmp_path / "state")
    return Row(lambda: record_images(context, [("sha256:" + "a" * 64, "ref:tag")]), queue=False, bounded=False)


def _bind_daemon(tmp_path: Path, _monkeypatch: pytest.MonkeyPatch) -> Row:
    context = Context(tmp_path / "project", tmp_path / "state")
    return Row(lambda: bind_daemon(context, ScriptedDocker()), queue=False, bounded=False)


def _claim_volumes(tmp_path: Path, _monkeypatch: pytest.MonkeyPatch) -> Row:
    project = tmp_path / "project"
    project.mkdir()
    context = Context(project.resolve(), tmp_path / "state")
    docker = ScriptedDocker(volume=[{"Name": "claimed", "CreatedAt": "2026-01-01T00:00:00Z", "Labels": {}}])
    return Row(lambda: claim_volumes(context, docker, ["claimed"]), queue=False, bounded=False)


def _prepare_run(tmp_path: Path, _monkeypatch: pytest.MonkeyPatch) -> Row:
    services = build_test_services(tmp_path / "state")
    project = tmp_path / "project"
    project.mkdir()
    request = lifecycle.RunRequest(project, "integration", "lock-order", (), (), "fixture", wait_seconds=60)
    return Row(
        lambda: lifecycle.prepare_run(
            request,
            _test_state=services.root / "registry",
            _test_directory=services.root / "admission",
            deadline_ns=_soon(),
        ),
        queue=False,
        bounded=True,
    )


def _open_run_row(tmp_path: Path, _monkeypatch: pytest.MonkeyPatch) -> Row:
    """The whole opener: preparation and the metadata declaration that follows it share one deadline."""
    services = build_test_services(tmp_path / "state")
    project = tmp_path / "project"
    project.mkdir()
    spec = _empty_spec(project, "open")
    return Row(
        lambda: _open_run(
            spec,
            test_state=services.root / "registry",
            test_directory=services.root / "admission",
            prepare_deadline_ns=_soon(),
        ),
        queue=False,
        bounded=True,
    )


def _reconcile_preparations(tmp_path: Path, _monkeypatch: pytest.MonkeyPatch) -> Row:
    services = build_test_services(tmp_path / "state")
    project = tmp_path / "project"
    project.mkdir()
    handle = services.open_run(_empty_spec(project, "indexed"))
    return Row(
        lambda: lifecycle.reconcile_preparations(
            handle._context.project, _test_directory=services.root / "admission", _deadline_ns=_soon()
        ),
        queue=False,
        bounded=True,
    )


def _enter_run(tmp_path: Path, _monkeypatch: pytest.MonkeyPatch) -> Row:
    """The requester's own loop: its guardian is dead, so it revokes its own run."""
    services = build_test_services(tmp_path / "state")
    account = services.root / "admission"
    project = tmp_path / "project"
    project.mkdir()
    handle = services.open_run(_empty_spec(project, "enter", wait_seconds=30))
    prepare_for_child_guardian(handle)
    child, _result = start_guardian_child(
        tmp_path, handle, account, {"queue-saved": {"action": "sigkill", "occurrence": 1}}
    )
    try:
        assert child.wait(timeout=30) == -signal.SIGKILL
    finally:
        reap(child)

    def call() -> None:
        with pytest.raises(ResourceError, match="Admission ended before grant"):
            lifecycle.enter_run(handle._context, _test_directory=account)

    return Row(call, queue=True, bounded=True)


def _revoke_ungranted(tmp_path: Path, _monkeypatch: pytest.MonkeyPatch) -> Row:
    services = build_test_services(tmp_path / "state")
    project = tmp_path / "project"
    project.mkdir()
    queued = _dead_queued_entry(tmp_path, services, project, "revoke")
    return Row(
        lambda: lifecycle.revoke_ungranted(queued, _test_directory=services.root / "admission", _deadline_ns=_soon()),
        queue=True,
        bounded=True,
    )


def _recover_generation_zero(tmp_path: Path, _monkeypatch: pytest.MonkeyPatch) -> Row:
    services = build_test_services(tmp_path / "state")
    project = tmp_path / "project"
    project.mkdir()
    handle = services.open_run(_empty_spec(project, "generation-zero"))
    retire_actor(handle._context.project.state, current_identity(), dead_identity())
    return Row(  # a never-enrolled run has no account entry: says generation 0 never takes metadata
        lambda: lifecycle.recover_run(handle._context, _test_directory=services.root / "admission"),
        queue=False,
        bounded=True,
    )


def _recover_holder(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Row:
    _handle, context, account = _resource_grant(tmp_path, monkeypatch)
    return Row(lambda: lifecycle.recover_run(context, _test_directory=account), queue=True, bounded=True)


def _recover_release(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Row:
    handle, context, _container, account = _retained_owned_container(tmp_path, monkeypatch)
    _interrupt_retained_release(handle, context, monkeypatch)
    retire_actor(context.project.state, current_identity(), dead_identity())
    return Row(lambda: lifecycle.recover_run(context, _test_directory=account), queue=True, bounded=True)


def _recover_empty(tmp_path: Path, _monkeypatch: pytest.MonkeyPatch) -> Row:
    context, account = _empty_grant(tmp_path)
    return Row(lambda: lifecycle.recover_run(context, _test_directory=account), queue=True, bounded=True)


def _retire_completed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Row:
    context, account = _crashed_terminal_resource_grant(tmp_path, monkeypatch)
    return Row(lambda: lifecycle.recover_run(context, _test_directory=account), queue=True, bounded=True)


def _worker(tmp_path: Path, _monkeypatch: pytest.MonkeyPatch) -> Row:
    services = build_test_services(tmp_path / "state")
    account = services.root / "admission"
    first, _second = _two_projects(tmp_path)
    dead = _dead_granted_holder(services, first)
    write_claim_intent(account, dead.run_id, ATTEMPT, expires_in_s=600)
    return Row(lambda: lifecycle.main(_worker_arguments(dead, ATTEMPT, account)), queue=True, bounded=True)


def _guardian_detection(tmp_path: Path, _monkeypatch: pytest.MonkeyPatch) -> Row:
    services = build_test_services(tmp_path / "state")
    account = services.root / "admission"
    first, _second = _two_projects(tmp_path)
    services.open_run(_empty_spec(first, "alive-holder")).enter()
    return Row(lambda: lifecycle.reconcile_account(_test_directory=account), queue=True, bounded=False)


def _cleanup(tmp_path: Path, _monkeypatch: pytest.MonkeyPatch) -> Row:
    project = tmp_path / "project"
    project.mkdir()
    context = Context(project.resolve(), tmp_path / "state")
    return Row(lambda: cleanup_project(context, ScriptedDocker(), execute=True), queue=False, bounded=False)


def _retirement_lock(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Row:
    service = _activated(tmp_path, monkeypatch)
    _vanished_project(service, tmp_path / "vanished")
    evidence = _evidence(service)
    return Row(lambda: _retire(service, {"evidence": evidence}, execute=True), queue=True, bounded=False)


def _cache_planning(tmp_path: Path, _monkeypatch: pytest.MonkeyPatch) -> Row:
    """A loaded build under real ``flock`` anchors: the shared cache anchor, then the exclusive one to publish."""
    run = BuildRun(tmp_path)
    run.fake.cache_layers = {"a": b"A"}
    return Row(run.build, queue=False, bounded=False)


def _cache_publication(tmp_path: Path, _monkeypatch: pytest.MonkeyPatch) -> Row:
    project_cache = tmp_path / "cache" / ("b" * 64)
    platform_dir = project_cache / "linux-arm64"
    for level in (tmp_path / "cache", project_cache, platform_dir):
        build_cache.require_safe_directory(level)
    staging = build_cache.create_staging(platform_dir, str(uuid.uuid4()), str(uuid.uuid4()))
    write_cache_layout(staging, {"a": b"A"})
    plan = {"key": "c" * 64, "staging": str(staging)}
    return Row(
        lambda: build_cache.publish(
            platform_dir, project_cache, "linux-arm64", plan, run_id=str(uuid.uuid4()), operation_id=str(uuid.uuid4())
        ),
        queue=False,
        bounded=True,
    )


ROWS: dict[str, Callable[[Path, pytest.MonkeyPatch], Row]] = {
    "register": _register,
    "record_images": _record_images,
    "bind_daemon": _bind_daemon,
    "claim_volumes": _claim_volumes,
    "prepare_run": _prepare_run,
    "open_run": _open_run_row,
    "reconcile_preparations": _reconcile_preparations,
    "enter_run": _enter_run,
    "guardian_admission_detection": _guardian_detection,
    "revoke_ungranted": _revoke_ungranted,
    "recover_run_generation_zero": _recover_generation_zero,
    "recover_resource_grant_holder": _recover_holder,
    "recover_resource_grant_release": _recover_release,
    "recover_empty_grant": _recover_empty,
    "retire_completed_resource_grant": _retire_completed,
    "worker_recover": _worker,
    "recovery_cleanup": _cleanup,
    "retirement_lock_project": _retirement_lock,
    "cache_planning": _cache_planning,
    "cache_publication": _cache_publication,
}


@pytest.mark.parametrize("row", list(ROWS))
def test_every_lock_table_row_respects_rank_order(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, row: str) -> None:
    prepared = ROWS[row](tmp_path, monkeypatch)  # fixture setup is not part of the observed call
    checker = LockOrderChecker()
    install_lock_observer(checker)
    try:
        prepared.call()
    finally:
        install_lock_observer(None)

    assert checker.violations == []
    assert [event for event in checker.events if event[0] == "acquire"], "the row acquired no lock at all"
    if prepared.queue:
        assert checker.acquired("metadata"), "a queue-touching row never acquired metadata"
    if prepared.bounded:
        unbounded = [event for event in checker.events if event[0] == "acquire" and event[3] and not event[4]]
        assert unbounded == []  #: every blocking wait on this path ends with a deadline


def test_guardian_admission_passes_respect_rank_order_and_bound_every_wait(tmp_path: Path) -> None:
    """Enroll, reserve and publish run in the guardian process: its own observer log is the evidence."""
    services = build_test_services(tmp_path / "state")
    account = services.root / "admission"
    project = tmp_path / "project"
    project.mkdir()
    handle = services.open_run(_empty_spec(project, "guardian-passes"))
    prepare_for_child_guardian(handle)
    log = tmp_path / "guardian.log"
    child, _result = start_guardian_child(
        tmp_path, handle, account, {"queue-saved": {"action": "sigkill", "occurrence": 3}}, observer_log=log
    )  # enrollment saves first, the reservation second, the grant third: killed right after the grant is published
    waiter = EnterThread(handle, existing_attempt=True)
    try:
        assert child.wait(timeout=60) == -signal.SIGKILL
    finally:
        reap(child)
        waiter.join(60)

    checker = LockOrderChecker()
    for line in log.read_text().splitlines():
        entry = json.loads(line)
        if entry.get("kind") == "lock":
            checker(entry["event"], entry["name"], entry["shared"], entry["wait"], entry["bounded"])
    assert checker.violations == []
    assert checker.acquired("metadata")
    assert [event for event in checker.events if event[0] == "acquire" and event[3] and not event[4]] == []


def test_live_foreign_guardian_and_holder_recovery_never_deadlock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A live guardian (its project lock held for its lifetime) and a holder recovery of a dead run finish or refuse."""
    services = build_test_services(tmp_path / "state")
    account = services.root / "admission"
    first, second = _two_projects(tmp_path)
    dead = _dead_granted_holder(services, first)
    live = services.open_run(_empty_spec(second, "live-guardian", wait_seconds=60))
    waiter = EnterThread(live)  # queues behind the dead holder: its guardian holds its project lock throughout
    wait_until(lambda: live._context.run_id in queue_entries(account), message="the live waiter is queued")
    outcomes: list[str] = []

    def recover() -> None:
        try:
            lifecycle.recover_run(dead, _test_directory=account)
            outcomes.append("recovered")
        except ResourceError as error:
            outcomes.append(f"refused:{error.reason}")

    thread = threading.Thread(target=recover)
    thread.start()
    thread.join(20)
    assert not thread.is_alive(), "holder recovery hung beside a live foreign guardian"
    waiter.join(60)
    assert outcomes and waiter.error is None
    live.leave()
    with lock(Context(account, account), "metadata", deadline_ns=time.monotonic_ns() + 5_000_000_000):
        pass  # no lock was left behind


def test_metadata_declaration_stops_waiting_at_its_deadline_against_a_real_lock_holder(tmp_path: Path) -> None:
    """The run lock of a preparation another process holds ends the declaration's wait at the opener's deadline."""
    services = build_test_services(tmp_path / "state")
    project = tmp_path / "project"
    project.mkdir()
    context = lifecycle.prepare_run(
        lifecycle.RunRequest(project, "integration", "lock-order", (), (), "fixture", wait_seconds=60),
        _test_state=services.root / "registry",
        _test_directory=services.root / "admission",
        deadline_ns=_soon(),
    )
    held, release = threading.Event(), threading.Event()

    def hold() -> None:
        with lock(context.project, f"run-{context.run_id}"):
            held.set()
            release.wait(30)

    holder = threading.Thread(target=hold)
    holder.start()
    assert held.wait(10)
    try:
        started = time.monotonic()
        with pytest.raises(ResourceBusy) as raised:
            deadline_ns = time.monotonic_ns() + 300_000_000
            lifecycle.declare_run_metadata(context, source_artifact(), (), deadline_ns=deadline_ns)
        elapsed = time.monotonic() - started
    finally:
        release.set()
        holder.join(10)

    assert raised.value.reason == "lock-deadline"
    assert 0.3 <= elapsed < 5.0  # waited to the deadline, not to the holder's 30 seconds
    assert lifecycle.load_run_record(context)["artifact"] is None  # the refused declaration changed nothing
    lifecycle.declare_run_metadata(context, source_artifact(), (), deadline_ns=_soon())
    assert lifecycle.load_run_record(context)["artifact"]["sha256"] == source_artifact().sha256


# ---- image lock K rows: observed on a shared private account whose Docker is one in-process fake ------

_PULLED = "example/cache@sha256:" + "2" * 64


def _observe(call: Callable[[], object]) -> LockOrderChecker:
    checker = LockOrderChecker()
    install_lock_observer(checker)
    try:
        call()
    finally:
        install_lock_observer(None)
    assert checker.violations == []
    assert checker.acquired("images"), "the row never took the image lock K"
    return checker


def _shared_owner(world: _World):
    owner = world.open(shared=True, declare_image=False)
    _dispatch(world, owner, "pull", _PULLED)
    return owner


def test_row_leave_k_with_capacity_and_shared_project(tmp_path: Path) -> None:
    """The guardian's leave takes K holding only its capacity descriptor and the shared project lock."""
    with shared_world(tmp_path) as world:
        owner = _shared_owner(world)
        project = owner._context.project

        def leave_image_step() -> None:
            with lock(project, project.project_id, shared=True):
                lifecycle._cleanup_owned_docker_resources(owner._context)

        _observe(leave_image_step)
        _leave(world, owner)


def test_row_release_run_k_with_nothing_held(tmp_path: Path) -> None:
    with shared_world(tmp_path) as world:
        owner = _shared_owner(world)
        _observe(lambda: lifecycle._cleanup_owned_docker_resources(owner._context, time.monotonic_ns() + 10**10))
        _leave(world, owner)


def test_row_recovery_releases_run_lock_before_k(tmp_path: Path) -> None:
    """Recovery of a dead shared holder releases its project and run locks before the receipts and resources phases
    wait for K."""
    with shared_world(tmp_path) as world:
        owner = _shared_owner(world)
        kill_guardian(owner._context)
        retire_actor(owner._context.project.state, current_identity(), dead_identity())
        account = world.services.root / "admission"

        checker = _observe(lambda: lifecycle.recover_run(owner._context, _test_directory=account))

        assert checker.acquired("metadata")
        assert json.loads(owner._context.record.read_text())["recovery_outcome"] == "complete"


def test_row_operator_cleanup_k_nonblocking(tmp_path: Path) -> None:
    image_id = "sha256:" + "e" * 64
    project = tmp_path / "project"
    project.mkdir()
    context = Context(project.resolve(), tmp_path / "registry")
    lifecycle.atomic_json(
        context.record,
        {"version": 2, "project_root": str(context.root), "sessions": {}, "images": {image_id: ["a:tag"]}, "runs": {}},
    )
    docker = ScriptedDocker(
        image=[{"Id": image_id, "RepoTags": ["a:tag"], "Labels": {PROJECT_LABEL: context.project_id}}]
    )
    account = tmp_path / "account"
    account.mkdir(mode=0o700)

    checker = _observe(lambda: cleanup_project(context, docker, execute=True, image_account=account))

    waits = [event for event in checker.events if event[0] == "acquire" and event[1] == "images"]
    assert waits and not any(event[3] for event in waits)  # K is never waited for
