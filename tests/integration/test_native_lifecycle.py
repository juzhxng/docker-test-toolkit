from __future__ import annotations

import functools
import hashlib
import json
import os
import signal
import subprocess
import sys
import threading
import time
from contextlib import ExitStack
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace

import pytest

from docker_test_toolkit import ArtifactIdentity, PhaseBudget, RunSpec
from docker_test_toolkit.application import admission, lifecycle, sharing
from docker_test_toolkit.application.recovery import (
    Context,
    atomic_json,
    claim_build_records,
    claim_volumes,
    cleanup_project,
    cleanup_session,
    lock,
    record_images,
    register,
)
from docker_test_toolkit.config.installation import HOME_ROOT
from docker_test_toolkit.config.runtime import PROJECT_LABEL, SESSION_LABEL
from docker_test_toolkit.domain.errors import (
    IncompatibleState,
    IncompleteCleanup,
    ResourceBusy,
    ResourceError,
    ToolkitError,
)
from docker_test_toolkit.domain.models import DockerResult, ProbeRun
from docker_test_toolkit.domain.records import canonical_json_bytes
from docker_test_toolkit.factories.api import open_run
from docker_test_toolkit.factories.testing import _TestServices, build_test_services, install_failpoint
from tests.installation_fixtures import build_archive, temporary_installation
from tests.integration.account_fixtures import (
    OPEN_RUN_CHILD,
    EnterThread,
    Interrupted,
    ScriptedDocker,
    _hold_lock,
    child_result,
    dead_identity,
    hold_lock_until_released,
    kill_guardian,
    pause_guardian,
    plant_run_record,
    prepare_for_child_guardian,
    prewrite_withdrawal,
    private_account,
    queue_entries,
    queue_entry,
    queue_phases,
    reap,
    release_lock,
    resume_guardian,
    run_context_of,
    run_lock_path,
    run_spec,
    seed_terminal_run_record,
    source_artifact,
    start_guardian_child,
    start_prepare_child,
    start_recover_child,
    three_private_projects,
    try_lock,
    wait_guardians_exited,
    wait_until,
    write_v2_queue,
)
from tests.unit.test_failure_evidence import WRITER_ROWS

pytestmark = pytest.mark.integration


def test_deadline_converts_with_its_suspend_aware_clock() -> None:
    deadline = lifecycle.Deadline.after(lifecycle.NativeClock(), 5)

    assert 0 < deadline.remaining_seconds() <= 5


def test_deadline_rejects_a_different_native_clock_origin() -> None:
    deadline = lifecycle.Deadline("foreign-clock", 0, 1, 1)

    with pytest.raises(ResourceError, match="another clock origin"):
        deadline.remaining_seconds()


def test_the_source_tree_has_no_real_account(tmp_path: Path) -> None:
    """Outside an installation there is no real account, whatever HOME holds (a planted installation included)."""
    planted = tmp_path / "home" / HOME_ROOT
    (planted / "0.5.0").mkdir(parents=True)
    (planted / "current").symlink_to("0.5.0")
    script = (
        "from docker_test_toolkit.application.state import account_directory\n"
        "from docker_test_toolkit.domain.errors import ResourceError\n"
        "try:\n"
        "    print(account_directory())\n"
        "except ResourceError as error:\n"
        "    print(error.reason)\n"
    )
    result = subprocess.run(
        [sys.executable, "-P", "-c", script],
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, "HOME": str(tmp_path / "home")},
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "toolkit-not-installed"


def test_project_registry_and_admission_share_the_installation_state(real_account: Path) -> None:
    assert lifecycle.account_directory() == real_account
    assert lifecycle.registry_directory() == real_account.parent


def test_account_comes_from_the_running_installation_only(tmp_path: Path) -> None:
    """The real account and registry depend on the running archive's installation only, not cwd or environment."""
    root = temporary_installation(tmp_path, [build_archive(tmp_path / "a")])
    archive = (root / "current").resolve() / "docker-test-toolkit.pyz"  # as the loader does: the resolved version
    script = (
        f"import sys; sys.path.insert(0, {str(archive)!r})\n"
        "from docker_test_toolkit.application.recovery import context\n"
        "from docker_test_toolkit.application.state import account_directory\n"
        "print(account_directory()); print(context().state)\n"
    )
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    variants = [
        {"cwd": tmp_path, "HOME": str(tmp_path), "TMPDIR": str(tmp_path), "PWD": str(tmp_path)},
        {"cwd": elsewhere, "HOME": "/nonexistent-home", "TMPDIR": str(elsewhere), "PWD": "/"},
        {"cwd": elsewhere, "HOME": "", "TMPDIR": "", "PWD": ""},
    ]
    for variant in variants:
        environment = {key: value for key, value in variant.items() if key != "cwd"}
        environment |= {"PATH": os.environ["PATH"], "DOCKER_TEST_TOOLKIT_PROJECT_ROOT": str(elsewhere)}
        result = subprocess.run(
            [sys.executable, "-I", "-c", script],
            cwd=variant["cwd"],
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr
        assert result.stdout.splitlines() == [str(root / "state" / "test-admission"), str(root / "state")]


def test_module_entrypoint_runs_outside_the_source_tree(tmp_path: Path) -> None:
    result = subprocess.run(
        [sys.executable, "-m", "docker_test_toolkit.application.lifecycle", "--help"],
        cwd=tmp_path,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr


def test_duplicate_json_fields_are_rejected() -> None:
    from docker_test_toolkit.application.recovery import unique_object

    with pytest.raises(ValueError, match="Duplicate JSON field"):
        unique_object([("run_id", "first"), ("run_id", "second")])


def test_account_queue_marker_preserves_the_legacy_wire_identity(tmp_path: Path) -> None:
    account = tmp_path / "account"
    account.mkdir(mode=0o700)
    queue = lifecycle.QueueState(1, {})
    with (account / "metadata.lock").open("w+") as anchor:
        lifecycle._save_queue(account, queue, anchor)
        anchor.seek(0)
        assert anchor.read() == admission.STATE_MARKER
        assert lifecycle._load_queue(account, anchor) == queue


def test_account_queue_marker_refuses_foreign_bytes_without_mutation(tmp_path: Path) -> None:
    account = tmp_path / "account"
    account.mkdir(mode=0o700)
    marker = "docker-test-toolkit-queue:1\n"
    queue = lifecycle.QueueState(1, {})
    with (account / "metadata.lock").open("w+") as anchor:
        anchor.write(marker)
        anchor.flush()
        with pytest.raises(ResourceError, match="unsupported protocol"):
            lifecycle._load_queue(account, anchor)
        with pytest.raises(ResourceError, match="unsupported protocol"):
            lifecycle._save_queue(account, queue, anchor)
        anchor.seek(0)
        assert anchor.read() == marker
    assert not (account / "queue.json").exists()


def _identity() -> ArtifactIdentity:
    return ArtifactIdentity(
        "0.1.0",
        "a" * 64,
        {
            "activation": 1,
            "batch": 1,
            "cli": 1,
            "distribution": 1,
            "queue": 1,
            "receipt": 1,
            "registry": 2,
            "run": 1,
        },
    )


def _spec(root: Path, purpose: str, wait_seconds: int = 20) -> RunSpec:
    return RunSpec(
        project_root=root,
        entrypoint="integration",
        purpose=purpose,
        services=("postgres",),
        closure=("postgres",),
        profile="fixture",
        phases={"tests": PhaseBudget(0, 1, 1, 1)},
        initial_phase="tests",
        artifact=_identity(),
        wait_seconds=wait_seconds,
    )


def test_private_native_run_enters_and_leaves_with_acknowledged_cleanup(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    handle = build_test_services(tmp_path / "state").open_run(_spec(project, "native-lifecycle"))

    handle.enter()
    assert handle.status().state == "active"
    status = handle.leave()

    assert status.state == "complete"
    assert status.cleanup_outcome == "complete"


def test_private_native_admission_is_fifo_and_exclusive(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    services = build_test_services(tmp_path / "state")
    first = services.open_run(_spec(project, "fifo-first"))
    second = services.open_run(_spec(project, "fifo-second"))
    first.enter()
    entered = threading.Event()
    failure: list[BaseException] = []

    def enter_second() -> None:
        try:
            second.enter()
            entered.set()
        except Exception as error:
            failure.append(error)

    thread = threading.Thread(target=enter_second)
    thread.start()
    deadline = time.monotonic() + 10
    while second.status().state == "prepared" and time.monotonic() < deadline:
        time.sleep(0.05)
    assert second.status().state in {"queued", "admitting"}
    assert not entered.is_set()

    first.leave()
    thread.join(timeout=10)
    assert not failure
    assert entered.is_set()
    second.leave()


def _grant_accounting() -> dict:
    """The guardian's admission account and reservation tick that a direct ``_publish_grant`` call supplies."""
    waits = lifecycle._AdmissionWaits()
    waits.begin(lifecycle.NativeClock().read())
    return {"waits": waits, "reserved_ns": lifecycle.NativeClock().read(), "window_ns": lifecycle._test_window_ns(None)}


class _NoContainers:
    """The publication's Docker reads: an empty container inventory (the snapshot itself is stubbed) and, on a Mac VM
    topology, a quiet VM probe."""

    def objects(self, kind: str, *, deadline: object = None) -> list[dict]:
        assert kind == "container"
        return []

    def call(self, *argv: str, deadline: object = None) -> str:
        assert argv == ("info", "--format", "{{.DockerRootDir}}")
        return "/var/lib/docker"

    def run_probe(self, argv: tuple[str, ...], *, deadline: object, container_name: str) -> ProbeRun:
        return ProbeRun("some avg10=0.00 avg60=0.00 avg300=0.00 total=0\n::dtt::\n", 0, False)


def _publication_setup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fresh_headroom: int,
    external_limits: tuple[int, ...],
    refreshed_daemon: str,
) -> SimpleNamespace:
    """A guardian-like process and a queued run ready to reserve, with the pressure and capacity reads bound."""
    from docker_test_toolkit.application import api

    project = tmp_path / "project"
    project.mkdir()
    temporary_parent = tmp_path / "tmp"
    temporary_parent.mkdir()
    registry = tmp_path / "registry"
    registry.mkdir(mode=0o700)
    account = registry / "test-admission"
    account.mkdir(mode=0o700)
    monkeypatch.setenv("TMPDIR", str(temporary_parent))
    monkeypatch.setattr(lifecycle, "account_directory", lambda: account)
    monkeypatch.setattr(lifecycle, "canonical_account", lambda: account)
    monkeypatch.setattr(lifecycle, "_gate_refusal", lambda *_arguments, **_keywords: None)  # gate: test_upgrade_gate
    handle = api._open_run(_spec(project, "fresh-pressure"), test_state=registry, docker_factory=lambda: object())
    budget = lifecycle.PhaseBudget(0, 1, 1, 1)
    lifecycle.configure_run(
        handle._context,
        services=("postgres",),
        closure=("postgres",),
        phases={"tests": budget},
        initial_phase="tests",
        topology="desktop",
    )
    attempt = lifecycle.start_guardian_attempt(handle._context)
    record = lifecycle.load_run_record(handle._context)
    requester = lifecycle._identity_from_json(record["requester"])
    guardian_pid = requester.pid + 1
    guardian_token = (
        None
        if requester.audit_token is None
        else [*requester.audit_token[:5], guardian_pid, *requester.audit_token[6:]]
    )
    guardian = lifecycle.ProcessIdentity(
        requester.origin,
        guardian_pid,
        requester.birth,
        guardian_token,
    )
    monkeypatch.setattr(lifecycle, "current_process", lambda _clock: guardian)
    monkeypatch.setattr(lifecycle, "observe_process", lambda _identity: "executing")
    lifecycle.enroll_guardian(handle._context, attempt)

    clock = lifecycle.NativeClock()
    now = clock.read()
    gib = 1024**3
    baseline = lifecycle.HostMemoryObservation(clock.origin, now - 40_000_000_000, 16 * gib, 8 * gib, 0, 0, 4096, {})
    stale = lifecycle.HostMemoryObservation(clock.origin, now - 10_000_000_000, 16 * gib, 8 * gib, 0, 0, 4096, {})
    fresh = lifecycle.HostMemoryObservation(clock.origin, now, 16 * gib, fresh_headroom, 0, 0, 4096, {})
    assert lifecycle.assess_pressure(stale, baseline).state is lifecycle.CapacityState.READY
    monkeypatch.setattr(lifecycle, "observe_host_memory", lambda _clock, deadline=None: fresh)
    snapshot = lifecycle.CapacitySnapshot(16 * gib, 8, 16 * gib, 8, "desktop", (), runtime="docker-desktop")
    assert lifecycle.assess_capacity(budget, snapshot).state is lifecycle.CapacityState.READY
    refreshed_snapshot = lifecycle.CapacitySnapshot(
        16 * gib, 8, 16 * gib, 8, "desktop", external_limits, runtime="docker-desktop"
    )
    monkeypatch.setattr(lifecycle, "_docker", _NoContainers)
    monkeypatch.setattr(
        lifecycle,
        "observe_capacity",
        lambda _docker, topology, inventory=None, deadline=None: (
            (refreshed_daemon, refreshed_snapshot) if topology == "desktop" and inventory == [] else None
        ),
    )

    return SimpleNamespace(
        handle=handle, attempt=attempt, snapshot=snapshot, stale=stale, baseline=baseline, fresh=fresh, account=account
    )


@pytest.mark.parametrize(
    ("fresh_headroom", "external_limits", "refreshed_daemon", "failure"),
    [
        (8 * 1024**3, (), "fixture-daemon", None),
        (1024**3, (), "fixture-daemon", "fresh complete native pressure"),
        (8 * 1024**3, (16 * 1024**3,), "fixture-daemon", "phase passed capacity admission"),
        (8 * 1024**3, (), "other-daemon", "Docker daemon changed"),
    ],
)
def test_grant_uses_post_reconciliation_capacity_and_pressure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fresh_headroom: int,
    external_limits: tuple[int, ...],
    refreshed_daemon: str,
    failure: str | None,
) -> None:
    setup = _publication_setup(tmp_path, monkeypatch, fresh_headroom, external_limits, refreshed_daemon)
    handle, attempt, snapshot, stale, baseline, fresh = (
        setup.handle,
        setup.attempt,
        setup.snapshot,
        setup.stale,
        setup.baseline,
        setup.fresh,
    )

    with ExitStack() as capacity_stack:
        outcome = lifecycle.reserve_capacity(handle._context, attempt, capacity_stack)
        assert outcome.waiting is None and outcome.descriptor is not None
        capacity = outcome.descriptor
        if failure is None:
            status = lifecycle._publish_grant(
                handle._context,
                attempt,
                capacity,
                snapshot,
                stale,
                baseline,
                daemon_id="fixture-daemon",
                **_grant_accounting(),
            )
            assert status.state == "active"
            assert lifecycle.load_run_record(handle._context)["admission_observation"]["memory"] == vars(fresh)
        else:
            with pytest.raises(ResourceError, match=failure):
                lifecycle._publish_grant(
                    handle._context,
                    attempt,
                    capacity,
                    snapshot,
                    stale,
                    baseline,
                    daemon_id="fixture-daemon",
                    **_grant_accounting(),
                )
            refused = lifecycle.load_run_record(handle._context)
            assert refused["state"] == "admitting"
            assert refused["admission"] == "never-granted"


def test_grant_is_refused_when_the_sample_ages_past_its_limit_while_waiting_for_the_metadata_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A fresh sample proves nothing after a long lock wait: the commit boundary re-checks its age."""
    setup = _publication_setup(tmp_path, monkeypatch, 8 * 1024**3, (), "fixture-daemon")
    holder = threading.Thread(target=_hold_metadata_lock, args=(setup.account, 6.0))

    with ExitStack() as capacity_stack:
        outcome = lifecycle.reserve_capacity(setup.handle._context, setup.attempt, capacity_stack)
        assert outcome.descriptor is not None
        holder.start()
        time.sleep(0.3)  # the holder owns the metadata lock: the publication reads its sample, then waits for it
        try:
            with pytest.raises(lifecycle._AdmissionFault) as refused:
                lifecycle._publish_grant(
                    setup.handle._context,
                    setup.attempt,
                    outcome.descriptor,
                    setup.snapshot,
                    setup.stale,
                    setup.baseline,
                    daemon_id="fixture-daemon",
                    **_grant_accounting(),
                )
        finally:
            holder.join()

    assert refused.value.fault.category == "observation-late"
    record = lifecycle.load_run_record(setup.handle._context)
    assert (record["state"], record["admission"]) == ("admitting", "never-granted")  # nothing was published


def _hold_metadata_lock(account: Path, seconds: float) -> None:
    with lock(Context(account, account), "metadata"):
        time.sleep(seconds)


def test_run_record_rejects_unknown_or_missing_authority_fields(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    state = tmp_path / "state"
    handle = build_test_services(state).open_run(_spec(project, "unknown-field"))
    record = state / "registry/runs" / f"{handle.run_id}.json"
    payload = json.loads(record.read_text())
    mutations = (
        {**payload, "unexpected_authority": True},
        {key: value for key, value in payload.items() if key != "purpose"},
    )
    for mutation in mutations:
        record.write_text(json.dumps(mutation))
        with pytest.raises(ResourceError, match="field"):
            handle.status()


def test_project_cleanup_refuses_false_clean_while_a_run_is_indexed(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    state = tmp_path / "state"
    handle = build_test_services(state).open_run(_spec(project, "indexed-run"))
    context = Context(project.resolve(), state / "registry")

    with pytest.raises(ResourceError, match="Indexed test runs"):
        cleanup_project(context, object())

    assert handle.status().state == "prepared"


def test_session_cleanup_ignores_other_sessions_and_retires_only_its_registration(tmp_path: Path) -> None:
    class EmptyDocker:
        engine = "docker"  # the port's engine member

        @staticmethod
        def call(*arguments: str) -> str:
            if arguments == ("info", "--format", "{{.ID}}"):
                return "fixture-daemon\n"
            if arguments == ("buildx", "ls", "--format", "{{.Name}}"):
                return ""
            raise AssertionError(f"unexpected Docker command: {arguments}")

        @staticmethod
        def objects(_kind: str) -> list[dict]:
            return []

    project = tmp_path / "project"
    project.mkdir()
    state = tmp_path / "state"
    services = build_test_services(state)
    selected = services.open_run(_spec(project, "selected-session"))
    other = services.open_run(_spec(project, "other-session"))
    context = Context(project.resolve(), state / "registry")
    selected.cancel()
    # The cancel removed the session folder; an older installed toolkit left it behind, which cleanup still removes.
    selected.private_root.parent.parent.mkdir(mode=0o700)

    planned = cleanup_session(context, EmptyDocker(), selected.session_id)
    executed = cleanup_session(context, EmptyDocker(), selected.session_id, execute=True)
    record = json.loads(context.record.read_text())

    assert planned["clean"] is False
    assert planned["session_id"] == selected.session_id
    assert planned["inventory"]["temporary_roots"] == [
        str(Path(os.environ.get("TMPDIR", "/tmp")).resolve() / "docker-test-toolkit" / selected.session_id)
    ]
    assert executed["clean"] is True
    assert selected.session_id not in record["sessions"]
    assert other.session_id in record["sessions"]
    assert other.run_id in record["runs"]


def test_session_cleanup_removes_only_exact_session_labeled_resources(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    context = Context(project.resolve(), tmp_path / "registry")
    selected = "11111111-1111-4111-8111-111111111111"
    other = "22222222-2222-4222-8222-222222222222"
    register(context, session_id=selected, temporary_parent=tmp_path)
    register(context, session_id=other, temporary_parent=tmp_path)

    def resource(identity: str, session_id: str, **extra) -> dict:
        return {
            "Id": identity,
            "Labels": {PROJECT_LABEL: context.project_id, SESSION_LABEL: session_id},
            **extra,
        }

    class SessionDocker:
        engine = "docker"  # the port's engine member

        def __init__(self) -> None:
            self.resources = {
                "container": [
                    resource("a" * 64, selected, Name="/selected", Mounts=[], Image="sha256:" + "a" * 64),
                    resource("b" * 64, other, Name="/other", Mounts=[], Image="sha256:" + "b" * 64),
                ],
                "network": [
                    resource("c" * 64, selected, Containers={}),
                    resource("d" * 64, other, Containers={}),
                ],
                "volume": [
                    {**resource("unused", selected), "Name": "selected-volume", "CreatedAt": "selected-created"},
                    {**resource("unused", other), "Name": "other-volume", "CreatedAt": "other-created"},
                ],
                "image": [
                    resource("sha256:" + "e" * 64, selected, RepoTags=["selected:image"]),
                    resource("sha256:" + "f" * 64, other, RepoTags=["other:image"]),
                ],
            }
            self.commands = []

        def call(self, *arguments: str) -> str:
            if arguments == ("info", "--format", "{{.ID}}"):
                return "fixture-daemon\n"
            if arguments == ("buildx", "ls", "--format", "{{.Name}}"):
                return ""
            self.commands.append(arguments)
            kind, target = arguments[0], arguments[-1]
            resource_kind = {"container": "container", "network": "network", "volume": "volume", "image": "image"}[kind]
            if resource_kind == "image":
                self.resources[resource_kind] = [
                    item for item in self.resources[resource_kind] if target not in item["RepoTags"]
                ]
            else:
                key = "Name" if resource_kind == "volume" else "Id"
                self.resources[resource_kind] = [item for item in self.resources[resource_kind] if item[key] != target]
            return target

        def execute(
            self, argv: tuple[str, ...], *, timeout_seconds: float, stream_output: bool = False, hold_fd=None
        ) -> DockerResult:
            del timeout_seconds, stream_output, hold_fd
            self.call(*argv)
            return DockerResult(0, "", "")

        def objects(self, kind: str) -> list[dict]:
            return list(self.resources[kind])

    docker = SessionDocker()
    planned = cleanup_session(context, docker, selected)
    executed = cleanup_session(context, docker, selected, execute=True)

    assert planned["inventory"]["containers"] == ["a" * 64]
    assert planned["inventory"]["networks"] == ["c" * 64]
    assert planned["inventory"]["volumes"] == ["selected-volume"]
    assert set(planned["inventory"]["images"]) == {"sha256:" + "e" * 64}
    assert executed["clean"] is True
    assert {item["Id"] for item in docker.resources["container"]} == {"b" * 64}
    assert {item["Id"] for item in docker.resources["network"]} == {"d" * 64}
    assert {item["Name"] for item in docker.resources["volume"]} == {"other-volume"}
    assert {item["Id"] for item in docker.resources["image"]} == {"sha256:" + "f" * 64}


def test_session_cleanup_retains_an_image_claimed_by_another_project_session(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    context = Context(project.resolve(), tmp_path / "registry")
    selected = "11111111-1111-4111-8111-111111111111"
    other = "22222222-2222-4222-8222-222222222222"
    image_id = "sha256:" + "e" * 64
    register(context, session_id=selected, temporary_parent=tmp_path)
    register(context, session_id=other, temporary_parent=tmp_path)
    for session_id in (selected, other):
        root = tmp_path / "docker-test-toolkit" / session_id
        root.mkdir(parents=True)
        (root / "docker-image-ids").write_text(f"{image_id}\n")

    class SharedImageDocker:
        engine = "docker"  # the port's engine member

        @staticmethod
        def call(*arguments: str) -> str:
            if arguments == ("info", "--format", "{{.ID}}"):
                return "fixture-daemon\n"
            if arguments == ("buildx", "ls", "--format", "{{.Name}}"):
                return ""
            raise AssertionError(f"unexpected Docker command: {arguments}")

        @staticmethod
        def objects(kind: str) -> list[dict]:
            if kind != "image":
                return []
            return [
                {
                    "Id": image_id,
                    "Labels": {PROJECT_LABEL: context.project_id, SESSION_LABEL: selected},
                    "RepoTags": ["shared:image"],
                }
            ]

    planned = cleanup_session(context, SharedImageDocker(), selected)

    assert planned["inventory"]["images"] == {}
    assert planned["inventory"]["retained_images"] == {image_id: "Referenced by another project or foreign container"}


def _legacy_container_with_anonymous_volume(session: str) -> ScriptedDocker:
    """A session-labelled legacy container whose anonymous volume is provable only through that container."""
    return ScriptedDocker(
        container=[
            {
                "Id": "a" * 64,
                "Name": "/legacy",
                "Image": "sha256:" + "c" * 64,
                "Labels": {SESSION_LABEL: session},
                "Mounts": [{"Type": "volume", "Name": "anonymous-volume"}],
            }
        ],
        volume=[
            {
                "Name": "anonymous-volume",
                "CreatedAt": "2026-01-01T00:00:00Z",
                "Labels": {"com.docker.volume.anonymous": ""},
            }
        ],
    )


def test_cleanup_does_not_hold_registry_across_docker_work(tmp_path: Path) -> None:
    state = tmp_path / "state"
    project, other_project = tmp_path / "a", tmp_path / "b"
    project.mkdir()
    other_project.mkdir()
    context = Context(project.resolve(), state / "registry")
    docker = ScriptedDocker(
        container=[
            {
                "Id": "a" * 64,
                "Name": "/owned",
                "Image": "sha256:" + "c" * 64,
                "Labels": {PROJECT_LABEL: context.project_id},
            }
        ]
    )
    paused, release, other_opened = threading.Event(), threading.Event(), threading.Event()
    results: dict[str, object] = {}

    def gate(stage: str) -> None:
        if stage == "cleanup-removals-started":
            paused.set()
            assert release.wait(timeout=60)

    def clean_project() -> None:
        results["cleanup"] = cleanup_project(context, docker, execute=True)

    def open_other_project() -> None:
        build_test_services(state).open_run(_spec(other_project, "other-project"))
        other_opened.set()

    install_failpoint(gate)
    cleanup = threading.Thread(target=clean_project)
    other = threading.Thread(target=open_other_project)
    try:
        cleanup.start()
        assert paused.wait(timeout=30)  # the pre-removal record is published and no Docker removal has started
        other.start()
        # Another project's open_run needs the account-wide registry lock; it must not wait for A's Docker work.
        assert other_opened.wait(timeout=30)
        assert docker.removals() == []
    finally:
        release.set()
        cleanup.join(timeout=60)
        other.join(timeout=60)
        install_failpoint(None)

    cleaned = results["cleanup"]
    assert isinstance(cleaned, dict) and cleaned["clean"] is True
    assert docker.removals() == [("container", "rm", "-f", "a" * 64)]


@pytest.mark.parametrize("stage", ["cleanup-pending-saved", "cleanup-removed-one"])
def test_cleanup_crash_after_proving_container_keeps_pending_identities(tmp_path: Path, stage: str) -> None:
    project = tmp_path / "project"
    project.mkdir()
    context = Context(project.resolve(), tmp_path / "registry")
    session = "11111111-1111-4111-8111-111111111111"
    register(context, session_id=session, temporary_parent=tmp_path)
    docker = _legacy_container_with_anonymous_volume(session)

    install_failpoint(lambda name: _raise_interrupted() if name == stage else None)
    try:
        with pytest.raises(Interrupted):
            cleanup_project(context, docker, execute=True)
    finally:
        install_failpoint(None)

    pending = json.loads(context.record.read_text())["pending"]
    assert pending["containers"] == ["a" * 64]
    assert pending["volumes"] == {"anonymous-volume": "2026-01-01T00:00:00Z"}
    assert bool(docker.objects("container")) is (stage == "cleanup-pending-saved")  # removed-one: the proof is gone
    # With the proving container gone, only the saved identity still proves the anonymous volume.
    planned = cleanup_project(context, docker)
    assert planned["inventory"]["volumes"] == ["anonymous-volume"]
    assert planned["clean"] is False


def test_cleanup_rechecks_cross_project_image_claims(tmp_path: Path) -> None:
    image_id = "sha256:" + "e" * 64
    project, other_project = tmp_path / "a", tmp_path / "b"
    project.mkdir()
    other_project.mkdir()
    context = Context(project.resolve(), tmp_path / "registry")
    other = Context(other_project.resolve(), tmp_path / "registry")
    atomic_json(
        context.record,
        {
            "version": 2,
            "project_root": str(context.root),
            "sessions": {},
            "images": {image_id: ["a:tag"]},
            "runs": {},
        },
    )
    docker = ScriptedDocker(
        image=[{"Id": image_id, "RepoTags": ["a:tag"], "Labels": {PROJECT_LABEL: context.project_id}}]
    )

    def claim_after_the_plan(stage: str) -> None:
        if stage == "cleanup-removals-started":
            record_images(other, [(image_id, "b:tag")])

    assert cleanup_project(context, docker)["inventory"]["images"] == {image_id: ["a:tag"]}  # unclaimed at plan time
    install_failpoint(claim_after_the_plan)
    try:
        result = cleanup_project(context, docker, execute=True)
    finally:
        install_failpoint(None)

    assert docker.removals() == []  # the claim recorded between plan and removal keeps the image
    assert [item["Id"] for item in docker.objects("image")] == [image_id]
    assert result["errors"] == [] and result["clean"] is False
    assert result["inventory"]["retained_images"] == {image_id: "Referenced by another project or foreign container"}


def test_cleanup_refuses_a_later_save_over_a_record_changed_during_docker_work(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    context = Context(project.resolve(), tmp_path / "registry")
    session = "11111111-1111-4111-8111-111111111111"
    register(context, session_id=session, temporary_parent=tmp_path)
    docker = _legacy_container_with_anonymous_volume(session)

    def change_the_record(stage: str) -> None:
        if stage == "cleanup-removals-started":
            register(context, session_id="22222222-2222-4222-8222-222222222222", temporary_parent=tmp_path)

    install_failpoint(change_the_record)
    try:
        with pytest.raises(IncompatibleState) as raised:
            cleanup_project(context, docker, execute=True)
    finally:
        install_failpoint(None)

    assert raised.value.reason == "record-changed"
    record = json.loads(context.record.read_text())
    assert "22222222-2222-4222-8222-222222222222" in record["sessions"]  # the concurrent write was not overwritten
    assert record["pending"]["volumes"] == {"anonymous-volume": "2026-01-01T00:00:00Z"}


def test_cleanup_reports_history_unverified_never_clean(tmp_path: Path) -> None:
    project, run_id, record = seed_terminal_run_record(tmp_path)
    context = Context(project, tmp_path / "registry")
    verify = functools.partial(lifecycle.run_record_loads, context.state)
    other_project = tmp_path / "other"
    other_project.mkdir()
    foreign = {"project_id": Context(other_project.resolve(), context.state).project_id}
    plant_run_record(context, "33333333-3333-4333-8333-333333333333", foreign)  # attributable to another project
    (context.state / "runs" / "44444444-4444-4444-8444-444444444444.json").write_bytes(b"{not json")  # no attribution
    plant_run_record(context, run_id, record)

    verified = cleanup_project(context, ScriptedDocker(), verify_run=verify)
    assert verified["history_unverified"] == [] and verified["clean"] is True  # a loader-valid terminal record
    unjudged = cleanup_project(context, ScriptedDocker())  # no verifier: nothing is verified, so never clean
    assert unjudged["history_unverified"] != [] and unjudged["clean"] is False
    unjudged_executed = cleanup_project(context, ScriptedDocker(), execute=True)
    assert unjudged_executed["clean"] is False

    broken = plant_run_record(context, run_id, {**record, "cleanup_outcome": "bogus"})  # intact identity, bad body
    planned = cleanup_project(context, ScriptedDocker(), verify_run=verify)
    executed = cleanup_project(context, ScriptedDocker(), execute=True, verify_run=verify)

    assert planned["history_unverified"] == [broken] and planned["clean"] is False
    assert executed["history_unverified"] == [broken] and executed["clean"] is False


@pytest.mark.parametrize(
    "defect",
    [
        {"run_id": "33333333-3333-4333-8333-333333333333"},
        {"project_root": "/somewhere/else"},
        {"state": "bogus"},
        {"cleanup_outcome": None},
        {"unexpected": 1},
        {"private_root": None},
    ],
    ids=[
        "run-id-differs-from-file",
        "root-differs-from-project",
        "state-unknown",
        "field-missing",
        "field-unknown",
        "root-missing",
    ],
)
def test_cleanup_lists_each_defect_of_attributable_history(tmp_path: Path, defect: dict) -> None:
    project, run_id, record = seed_terminal_run_record(tmp_path)
    context = Context(project, tmp_path / "registry")
    verify = functools.partial(lifecycle.run_record_loads, context.state)
    mutated = {key: value for key, value in {**record, **defect}.items() if value is not None}

    cell = plant_run_record(context, run_id, mutated)

    assert cleanup_project(context, ScriptedDocker(), verify_run=verify)["history_unverified"] == [cell]


def test_hostile_nesting_in_a_project_record_is_a_resource_error_not_a_crash(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    context = Context(project.resolve(), tmp_path / "registry")
    context.record.parent.mkdir(parents=True, mode=0o700)
    context.record.write_bytes(b"[" * 200_000 + b"]" * 200_000)  # nested far beyond the parser's recursion limit
    context.record.chmod(0o600)

    with pytest.raises(ResourceError, match="Cannot read ownership record"):
        cleanup_project(context, ScriptedDocker())


def test_session_cleanup_rejects_a_non_object_indexed_run_record(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    context = Context(project.resolve(), tmp_path / "registry")
    session_id = "11111111-1111-4111-8111-111111111111"
    run_id = "22222222-2222-4222-8222-222222222222"
    register(context, session_id=session_id, temporary_parent=tmp_path)
    run_record = context.state / "runs" / f"{run_id}.json"
    run_record.parent.mkdir()
    run_record.write_text("[]")
    project_record = json.loads(context.record.read_text())
    project_record["runs"][run_id] = str(run_record)
    atomic_json(context.record, project_record)

    with pytest.raises(ResourceError, match="Indexed run identity"):
        cleanup_session(context, object(), session_id)


def test_session_cleanup_rejects_a_symlinked_indexed_run_record(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    context = Context(project.resolve(), tmp_path / "registry")
    session_id = "11111111-1111-4111-8111-111111111111"
    run_id = "22222222-2222-4222-8222-222222222222"
    register(context, session_id=session_id, temporary_parent=tmp_path)
    target = tmp_path / "forged-run.json"
    target.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "project_id": context.project_id,
                "project_root": str(context.root),
                "session_id": "33333333-3333-4333-8333-333333333333",
            }
        )
    )
    run_record = context.state / "runs" / f"{run_id}.json"
    run_record.parent.mkdir()
    run_record.symlink_to(target)
    project_record = json.loads(context.record.read_text())
    project_record["runs"][run_id] = str(run_record)
    atomic_json(context.record, project_record)

    with pytest.raises(ResourceError, match="Indexed run record"):
        cleanup_session(context, object(), session_id)


def test_session_cleanup_rejects_a_symlinked_image_manifest(tmp_path: Path) -> None:
    class EmptyDocker:
        engine = "docker"  # the port's engine member

        @staticmethod
        def call(*arguments: str) -> str:
            if arguments == ("info", "--format", "{{.ID}}"):
                return "fixture-daemon\n"
            if arguments == ("buildx", "ls", "--format", "{{.Name}}"):
                return ""
            raise AssertionError(f"unexpected Docker command: {arguments}")

        @staticmethod
        def objects(_kind: str) -> list[dict]:
            return []

    project = tmp_path / "project"
    project.mkdir()
    context = Context(project.resolve(), tmp_path / "registry")
    session_id = "11111111-1111-4111-8111-111111111111"
    register(context, session_id=session_id, temporary_parent=tmp_path)
    target = tmp_path / "forged-image-manifest"
    target.write_text("sha256:" + "e" * 64 + "\n")
    root = tmp_path / "docker-test-toolkit" / session_id
    root.mkdir(parents=True)
    (root / "docker-image-ids").symlink_to(target)

    with pytest.raises(ResourceError, match="image manifest"):
        cleanup_session(context, EmptyDocker(), session_id)


def test_session_cleanup_rejects_a_tampered_builder_identity_before_docker(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    context = Context(project.resolve(), tmp_path / "registry")
    session_id = "11111111-1111-4111-8111-111111111111"
    register(context, session_id=session_id, temporary_parent=tmp_path)
    project_record = json.loads(context.record.read_text())
    project_record["sessions"][session_id]["builder"] = "foreign-builder"
    atomic_json(context.record, project_record)

    with pytest.raises(ResourceError, match="Invalid session ownership"):
        cleanup_session(context, object(), session_id)


def test_project_claims_reject_overlong_docker_names_before_inspection(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    context = Context(project.resolve(), tmp_path / "registry")
    overlong = "x" * 129

    with pytest.raises(ResourceError, match="exact volume names"):
        claim_volumes(context, object(), [overlong])
    with pytest.raises(ResourceError, match="builder name"):
        claim_build_records(context, object(), "build_cache", [overlong, "a" * 20])


def test_project_cleanup_treats_claims_on_an_absent_builder_as_absent(tmp_path: Path) -> None:
    class MissingBuilderDocker:
        engine = "docker"  # the port's engine member

        @staticmethod
        def call(*arguments: str) -> str:
            if arguments == ("info", "--format", "{{.ID}}"):
                return "fixture-daemon\n"
            if arguments == ("buildx", "ls", "--format", "{{.Name}}"):
                return ""
            raise AssertionError(f"unexpected Docker command: {arguments}")

        @staticmethod
        def objects(_kind: str) -> list[dict]:
            return []

    project = tmp_path / "project"
    project.mkdir()
    context = Context(project.resolve(), tmp_path / "registry")
    atomic_json(
        context.record,
        {
            "version": 2,
            "project_root": str(project.resolve()),
            "sessions": {},
            "images": {},
            "runs": {},
            "build_cache": {"missing-builder": {"cache-record": "created"}},
        },
    )

    result = cleanup_project(context, MissingBuilderDocker(), execute=False)

    assert result["clean"] is True
    assert result["inventory"]["build_cache"] == {}


def test_project_cleanup_upgrades_a_valid_version_one_record(tmp_path: Path) -> None:
    class EmptyDocker:
        engine = "docker"  # the port's engine member

        @staticmethod
        def call(*arguments: str) -> str:
            if arguments == ("info", "--format", "{{.ID}}"):
                return "fixture-daemon\n"
            if arguments == ("buildx", "ls", "--format", "{{.Name}}"):
                return ""
            raise AssertionError(f"unexpected Docker command: {arguments}")

        @staticmethod
        def objects(_kind: str) -> list[dict]:
            return []

    project = tmp_path / "project"
    project.mkdir()
    context = Context(project.resolve(), tmp_path / "registry")
    atomic_json(
        context.record,
        {"version": 1, "project_root": str(project.resolve()), "sessions": {}, "images": {}},
    )

    result = cleanup_project(context, EmptyDocker(), execute=True)

    assert result["clean"] is True


def test_project_cleanup_ignores_unmanaged_compose_resources_from_the_same_root(tmp_path: Path) -> None:
    class UnmanagedComposeDocker:
        engine = "docker"  # the port's engine member

        def __init__(self, root: Path) -> None:
            compose = {
                "com.docker.compose.project": "unmanaged",
                "com.docker.compose.project.working_dir": str(root),
            }
            self.resources = {
                "container": [
                    {
                        "Id": "a" * 64,
                        "Name": "/unmanaged-service-1",
                        "Labels": compose,
                        "Mounts": [{"Type": "volume", "Name": "unmanaged-data"}],
                        "Image": "sha256:" + "d" * 64,
                    }
                ],
                "network": [
                    {
                        "Id": "b" * 64,
                        "Name": "unmanaged_default",
                        "Labels": compose,
                        "Containers": {},
                    }
                ],
                "volume": [
                    {
                        "Name": "unmanaged-data",
                        "Labels": compose,
                        "CreatedAt": "2026-09-18T00:00:00Z",
                    }
                ],
                "image": [],
            }
            self.commands: list[tuple[str, ...]] = []

        def call(self, *arguments: str) -> str:
            if arguments == ("info", "--format", "{{.ID}}"):
                return "fixture-daemon\n"
            if arguments == ("buildx", "ls", "--format", "{{.Name}}"):
                return ""
            self.commands.append(arguments)
            kind, target = arguments[0], arguments[-1]
            resource_kind = {"container": "container", "network": "network", "volume": "volume"}[kind]
            identity = "Name" if resource_kind == "volume" else "Id"
            self.resources[resource_kind] = [item for item in self.resources[resource_kind] if item[identity] != target]
            return target

        def objects(self, kind: str) -> list[dict]:
            return list(self.resources[kind])

    project = tmp_path / "project"
    project.mkdir()
    context = Context(project.resolve(), tmp_path / "registry")
    atomic_json(
        context.record,
        {"version": 2, "project_root": str(project.resolve()), "sessions": {}, "images": {}, "runs": {}},
    )
    docker = UnmanagedComposeDocker(project.resolve())

    result = cleanup_project(context, docker, execute=True)

    assert result["clean"] is True
    assert docker.commands == []
    assert [item["Id"] for item in docker.resources["container"]] == ["a" * 64]
    assert [item["Id"] for item in docker.resources["network"]] == ["b" * 64]
    assert [item["Name"] for item in docker.resources["volume"]] == ["unmanaged-data"]


def test_project_cleanup_does_not_infer_compose_sibling_ownership_from_labels(tmp_path: Path) -> None:
    class CollidingComposeDocker:
        engine = "docker"  # the port's engine member

        def __init__(self, root: Path, project_id: str) -> None:
            compose = {
                "com.docker.compose.project": "colliding",
                "com.docker.compose.project.working_dir": str(root),
            }
            self.resources = {
                "container": [
                    {
                        "Id": "a" * 64,
                        "Name": "/owned-service-1",
                        "Labels": {**compose, PROJECT_LABEL: project_id},
                        "Mounts": [],
                        "Image": "sha256:" + "d" * 64,
                    }
                ],
                "network": [
                    {
                        "Id": "b" * 64,
                        "Name": "colliding_default",
                        "Labels": compose,
                        "Containers": {},
                    }
                ],
                "volume": [
                    {
                        "Name": "colliding-data",
                        "Labels": compose,
                        "CreatedAt": "2026-09-18T00:00:00Z",
                    }
                ],
                "image": [],
            }
            self.commands: list[tuple[str, ...]] = []

        def call(self, *arguments: str) -> str:
            if arguments == ("info", "--format", "{{.ID}}"):
                return "fixture-daemon\n"
            if arguments == ("buildx", "ls", "--format", "{{.Name}}"):
                return ""
            self.commands.append(arguments)
            kind, target = arguments[0], arguments[-1]
            resource_kind = {"container": "container", "network": "network", "volume": "volume"}[kind]
            identity = "Name" if resource_kind == "volume" else "Id"
            self.resources[resource_kind] = [item for item in self.resources[resource_kind] if item[identity] != target]
            return target

        def objects(self, kind: str) -> list[dict]:
            return list(self.resources[kind])

    project = tmp_path / "project"
    project.mkdir()
    context = Context(project.resolve(), tmp_path / "registry")
    atomic_json(
        context.record,
        {"version": 2, "project_root": str(project.resolve()), "sessions": {}, "images": {}, "runs": {}},
    )
    docker = CollidingComposeDocker(project.resolve(), context.project_id)

    result = cleanup_project(context, docker, execute=True)

    assert result["clean"] is True
    assert docker.commands == [("container", "rm", "-f", "a" * 64)]
    assert docker.resources["container"] == []
    assert [item["Id"] for item in docker.resources["network"]] == ["b" * 64]
    assert [item["Name"] for item in docker.resources["volume"]] == ["colliding-data"]


def test_project_cleanup_removes_worker_left_by_buildx(tmp_path: Path) -> None:
    class PartialBuilderRemovalDocker:
        engine = "docker"  # the port's engine member

        def __init__(self, builder: str, worker_id: str) -> None:
            self.builder = builder
            self.worker_id = worker_id
            self.worker = {
                "Id": worker_id,
                "Name": f"/buildx_buildkit_{builder}0",
                "Labels": {},
                "Mounts": [],
                "Image": "sha256:" + "b" * 64,
            }
            self.commands: list[tuple[str, ...]] = []

        def call(self, *arguments: str) -> str:
            self.commands.append(arguments)
            if arguments == ("info", "--format", "{{.ID}}"):
                return "fixture-daemon\n"
            if arguments == ("buildx", "ls", "--format", "{{.Name}}"):
                return f"{self.builder}\n" if self.builder else ""
            if arguments == ("buildx", "rm", self.builder):
                self.builder = ""
                return ""
            if arguments == ("container", "rm", "-f", self.worker_id):
                self.worker = None
                return self.worker_id
            raise AssertionError(f"unexpected Docker command: {arguments}")

        def objects(self, kind: str) -> list[dict]:
            return [self.worker] if kind == "container" and self.worker is not None else []

    project = tmp_path / "project"
    project.mkdir()
    context = Context(project.resolve(), tmp_path / "registry")
    session_id = "11111111-1111-4111-8111-111111111111"
    register(context, session_id=session_id, temporary_parent=tmp_path)
    builder = json.loads(context.record.read_text())["sessions"][session_id]["builder"]
    docker = PartialBuilderRemovalDocker(builder, "a" * 64)

    result = cleanup_project(context, docker, execute=True)

    assert result["clean"] is True
    assert ("buildx", "rm", builder) in docker.commands
    assert ("container", "rm", "-f", "a" * 64) in docker.commands


def test_project_cleanup_is_not_clean_while_another_project_claims_its_image(tmp_path: Path) -> None:
    image_id = "sha256:" + "a" * 64

    class SharedImageDocker:
        engine = "docker"  # the port's engine member

        @staticmethod
        def call(*arguments: str) -> str:
            if arguments == ("info", "--format", "{{.ID}}"):
                return "fixture-daemon\n"
            if arguments == ("buildx", "ls", "--format", "{{.Name}}"):
                return ""
            raise AssertionError(f"unexpected Docker command: {arguments}")

        @staticmethod
        def objects(kind: str) -> list[dict]:
            if kind == "image":
                return [{"Config": {"Labels": {}}, "Id": image_id, "RepoTags": ["shared:one"]}]
            return []

    state = tmp_path / "registry"
    project = tmp_path / "project"
    other_project = tmp_path / "other"
    project.mkdir()
    other_project.mkdir()
    context = Context(project.resolve(), state)
    other = Context(other_project.resolve(), state)
    for owner in (context, other):
        atomic_json(
            owner.record,
            {
                "version": 2,
                "project_root": str(owner.root),
                "sessions": {},
                "images": {image_id: ["shared:one"]},
                "runs": {},
            },
        )

    result = cleanup_project(context, SharedImageDocker(), execute=True)

    assert result["clean"] is False
    assert list(result["inventory"]["retained_images"]) == [image_id]  # still claimed by the other project


def test_lock_deadline_times_out_against_a_real_holder(tmp_path: Path) -> None:
    state = tmp_path / "state"
    ctx = Context(tmp_path / "project", state)
    holder = _hold_lock(state / "run-11111111-1111-4111-8111-111111111111.lock")
    try:
        started = time.monotonic()
        with (
            pytest.raises(ResourceBusy) as raised,
            lock(ctx, "run-11111111-1111-4111-8111-111111111111", deadline_ns=time.monotonic_ns() + 200_000_000),
        ):
            pass
        assert raised.value.reason == "lock-deadline"
        elapsed = time.monotonic() - started
        # The lower bound stays exact: the helper must wait at least until the deadline. The upper bound is generous to
        # tolerate scheduler noise; it only has to prove that the wait is bounded, not that it is fast.
        assert 0.2 <= elapsed < 3.0
    finally:
        holder.kill()
        holder.wait(timeout=10)


def test_lock_without_deadline_waits_for_release(tmp_path: Path) -> None:
    ctx = Context(tmp_path / "project", tmp_path / "state")
    held = threading.Event()

    def hold() -> None:
        with lock(ctx, "registry"):
            held.set()
            time.sleep(0.3)

    thread = threading.Thread(target=hold)
    thread.start()
    try:
        assert held.wait(timeout=10)
        started = time.monotonic()
        with lock(ctx, "registry"):
            waited = time.monotonic() - started
        # Blocking acquisition: the waiter returns only after the holder's 0.3 s hold ends.
        assert waited >= 0.25
    finally:
        thread.join(timeout=10)


def test_run_record_protocol_maps_are_a_closed_set(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    handle = build_test_services(tmp_path / "state").open_run(_spec(project, "protocol-set"))
    context = handle._context
    record = json.loads(context.record.read_text())
    version_1 = {"registry": 2, "run": 1, "queue": 1, "receipt": 1}
    for accepted in (version_1, {**version_1, "queue": 2}):
        record["protocol"] = accepted
        atomic_json(context.record, record)
        assert lifecycle.load_run_record(context)["protocol"] == accepted

    record["protocol"] = {**version_1, "queue": 3}
    atomic_json(context.record, record)
    with pytest.raises(ResourceError, match=r"Unsupported lifecycle protocol\."):
        lifecycle.load_run_record(context)


def _queued(handle) -> bool:
    return handle.status().state in {"queued", "admitting"}


def test_three_projects_strict_fifo(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    account = services.root / "admission"
    first = services.open_run(_spec(projects[0], "fifo-a"))
    first.enter()
    second = services.open_run(_spec(projects[1], "fifo-b"))
    third = services.open_run(_spec(projects[2], "fifo-c"))
    second_waiter = EnterThread(second)
    wait_until(lambda: second.status().state == "queued", message="B queued")
    third_waiter = EnterThread(third)
    wait_until(lambda: third.status().state == "queued", message="C queued")
    assert queue_phases(account) == {first.run_id: "granted", second.run_id: "queued", third.run_id: "queued"}

    first.leave()
    second_waiter.join()
    assert second_waiter.error is None
    assert second.status().state == "active"
    assert third.status().state == "queued"
    assert queue_phases(account) == {second.run_id: "granted", third.run_id: "queued"}

    second.leave()
    third_waiter.join()
    assert third_waiter.error is None
    assert third.status().state == "active"
    assert queue_phases(account) == {third.run_id: "granted"}
    third.leave()
    assert queue_phases(account) == {}


def test_corrupt_queued_head_is_skipped(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    first = services.open_run(_spec(projects[0], "holder"))
    first.enter()
    corrupt = services.open_run(_spec(projects[1], "corrupt-head"))
    healthy = services.open_run(_spec(projects[2], "healthy"))
    corrupt_waiter = EnterThread(corrupt)
    wait_until(lambda: corrupt.status().state == "queued", message="corrupt head queued")
    healthy_waiter = EnterThread(healthy)
    wait_until(lambda: healthy.status().state == "queued", message="healthy waiter queued")
    record = services.root / "registry" / "runs" / f"{corrupt.run_id}.json"
    original = record.read_bytes()
    record.write_bytes(b"{")

    first.leave()
    healthy_waiter.join()
    assert healthy_waiter.error is None
    assert healthy.status().state == "active"  # granted past the blocked head
    assert record.read_bytes() == b"{"  # the pass never repaired or rewrote the foreign record
    with pytest.raises(ToolkitError) as raised:
        services.open_run(_spec(projects[1], "second-try"))
    assert raised.value.reason == "blocked-entry"
    assert corrupt.run_id in str(raised.value)
    healthy.leave()
    record.write_bytes(original)  # let the corrupt waiter end cleanly
    corrupt_waiter.join(timeout=30)


def test_clean_project_queues_behind_foreign_cleanup_pending_run(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    foreign = services.open_run(_spec(projects[0], "cleanup-pending"))
    record = services.root / "registry" / "runs" / f"{foreign.run_id}.json"
    data = json.loads(record.read_text())
    data.update(state="complete", admission="revoked", test_outcome="cancelled", cleanup_outcome="pending")
    lifecycle_context = foreign._context
    atomic_json(record, data)
    assert lifecycle.run_status(lifecycle_context).cleanup_outcome == "pending"

    clean = services.open_run(_spec(projects[1], "clean-project"))
    clean.enter()

    assert clean.status().state == "active"  # a foreign cleanup-pending run is never a reason to refuse or wait
    clean.leave()


def test_live_foreign_holder_never_raises_account_wide_contention(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    account = services.root / "admission"
    holder = services.open_run(_spec(projects[0], "live-holder"))
    holder.enter()
    waiter = services.open_run(_spec(projects[1], "waiter"))
    waiting = EnterThread(waiter)
    wait_until(lambda: waiter.status().state == "queued", message="waiter queued")
    time.sleep(1.5)  # several admission passes while the foreign holder is live

    assert waiter.status().state == "queued"  # never granted beside a live holder
    assert waiting.error is None
    assert queue_phases(account) == {holder.run_id: "granted", waiter.run_id: "queued"}
    holder.leave()
    waiting.join()
    assert waiting.error is None
    waiter.leave()


def test_corrupt_holder_keeps_the_slot(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    account = services.root / "admission"
    holder = services.open_run(_spec(projects[0], "corrupt-holder"))
    holder.enter()
    waiter = services.open_run(_spec(projects[1], "behind-corrupt-holder", wait_seconds=6))
    waiting = EnterThread(waiter)
    wait_until(lambda: waiter.status().state == "queued", message="waiter queued")
    record = services.root / "registry" / "runs" / f"{holder.run_id}.json"
    original = record.read_bytes()
    record.write_bytes(b"{")
    time.sleep(2.0)  # several admission passes read the corrupt holder record

    assert waiting.error is None  # the corrupt holder blocks only its own project, never an account-wide error
    assert waiter.status().state == "queued"
    assert queue_phases(account) == {holder.run_id: "granted", waiter.run_id: "queued"}  # it keeps the slot
    waiting.join(timeout=30)
    # The waiter ends only by its own queue deadline: it was never granted beside the unreadable holder.
    assert isinstance(waiting.error, ResourceError)
    assert "deadline expired" in str(waiting.error)
    record.write_bytes(original)  # manual repair


def test_unindexed_forged_active_record_is_ignored(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    runs = services.root / "registry" / "runs"
    victim = services.open_run(_spec(projects[0], "victim"))
    forged_id = "ffffffff-ffff-4fff-8fff-ffffffffffff"
    forged = json.loads((runs / f"{victim.run_id}.json").read_text())
    forged.update(run_id=forged_id, state="active", ticket=1, admission="published")
    atomic_json(runs / f"{forged_id}.json", forged)
    forged_bytes = (runs / f"{forged_id}.json").read_bytes()

    victim.enter()  # the forged record has no queue entry and no index entry: it never blocks or redirects anything

    assert victim.status().state == "active"
    assert (runs / f"{forged_id}.json").read_bytes() == forged_bytes
    victim.leave()


def test_same_project_second_submission_waits(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    account = services.root / "admission"
    first = services.open_run(_spec(projects[0], "same-project-first"))
    first.enter()
    second = services.open_run(_spec(projects[0], "same-project-second"))
    waiting = EnterThread(second)
    wait_until(lambda: second.status().state == "queued", message="second submission queued")
    time.sleep(1.0)

    assert second.status().state == "queued"  # never granted beside the live holder of the same project
    assert queue_phases(account) == {first.run_id: "granted", second.run_id: "queued"}
    first.leave()
    waiting.join()
    assert waiting.error is None
    second.leave()


def _record(services, handle) -> dict:
    return json.loads((services.root / "registry" / "runs" / f"{handle.run_id}.json").read_text())


def _prepared_run_with_guardian_child(tmp_path: Path, stages: dict):
    """Prepare one run, start its guardian as a failpoint child and begin the requester's wait."""
    project = tmp_path / "project"
    project.mkdir()
    services = build_test_services(tmp_path / "state")
    account = services.root / "admission"
    handle = services.open_run(_spec(project, "crash-prefix"))
    prepare_for_child_guardian(handle)
    child, _result = start_guardian_child(tmp_path, handle, account, stages)
    return services, account, handle, child


def test_enroll_crash_after_record_write_is_resumable(tmp_path: Path) -> None:
    services, account, handle, child = _prepared_run_with_guardian_child(
        tmp_path, {"enroll-record-written": {"action": "sigkill"}}
    )
    try:
        assert child.wait(timeout=30) == -signal.SIGKILL
        # The prefix: the guardian and account are bound, no queue entry exists and the ticket is unassigned.
        record = _record(services, handle)
        assert record["guardian"] is not None and record["account_directory"] == str(account)
        assert (record["state"], record["ticket"]) == ("prepared", None)
        assert queue_phases(account) == {}
        entering = EnterThread(handle, existing_attempt=True)
        entering.join()
    finally:
        reap(child)
    assert isinstance(entering.error, ResourceError)
    assert "Admission ended before grant" in str(entering.error)
    final = _record(services, handle)
    assert (final["state"], final["admission"]) == ("complete", "revoked")  # the dead guardian's run was revoked
    assert queue_phases(account) == {}


def test_enroll_crash_after_queue_write_keeps_ticket(tmp_path: Path) -> None:
    services, account, handle, child = _prepared_run_with_guardian_child(
        tmp_path, {"queue-saved": {"action": "sigkill", "occurrence": 1}}
    )
    try:
        assert child.wait(timeout=30) == -signal.SIGKILL
        # The prefix: the entry holds its ticket while the record has not yet moved to queued.
        assert queue_phases(account) == {handle.run_id: "queued"}
        record = _record(services, handle)
        assert (record["state"], record["ticket"]) == ("prepared", None)
        entering = EnterThread(handle, existing_attempt=True)
        entering.join()
    finally:
        reap(child)
    assert isinstance(entering.error, ResourceError)
    assert queue_phases(account) == {}  # the revocation removed the abandoned entry; nothing leaks
    assert _record(services, handle)["admission"] == "revoked"


def test_reserve_crash_after_queue_write(tmp_path: Path) -> None:
    services, account, handle, child = _prepared_run_with_guardian_child(
        tmp_path, {"queue-saved": {"action": "sigkill", "occurrence": 2}}
    )
    try:
        assert child.wait(timeout=30) == -signal.SIGKILL
        # The prefix: the entry holds the slot (admitting) while the record is still queued.
        assert queue_phases(account) == {handle.run_id: "admitting"}
        assert _record(services, handle)["state"] == "queued"
        entering = EnterThread(handle, existing_attempt=True)
        entering.join()
    finally:
        reap(child)
    assert isinstance(entering.error, ResourceError)
    assert queue_phases(account) == {}  # the dead guardian's slot was released by revoking its run
    other = tmp_path / "other"
    other.mkdir()
    second = services.open_run(_spec(other, "after-reserve-crash"))
    second.enter()
    assert second.status().state == "active"  # the slot is not leaked
    second.leave()


def test_revoke_crash_after_record_write_before_queue_removal(tmp_path: Path) -> None:
    services, account, handle, child = _prepared_run_with_guardian_child(
        tmp_path, {"queue-saved": {"action": "sigkill", "occurrence": 2}}
    )
    try:
        assert child.wait(timeout=30) == -signal.SIGKILL
    finally:
        reap(child)
    assert queue_phases(account) == {handle.run_id: "admitting"}  # the dead guardian holds the slot

    def interrupt(stage: str) -> None:
        if stage == "queue-tempfile-written":  # the revocation's queue removal, after its terminal record write
            raise Interrupted

    install_failpoint(interrupt)
    try:
        with pytest.raises(Interrupted):
            lifecycle.revoke_ungranted(handle._context, _test_directory=account)
    finally:
        install_failpoint(None)
    record = _record(services, handle)
    assert (record["state"], record["admission"]) == ("complete", "revoked")  # the terminal record is durable
    assert queue_phases(account) == {handle.run_id: "admitting"}  # the entry still holds the slot

    lifecycle.revoke_ungranted(handle._context, _test_directory=account)  # the resumed call finishes the removal

    assert queue_phases(account) == {}
    assert _record(services, handle) == record  # the retry only removed the entry
    other = tmp_path / "other"
    other.mkdir()
    second = services.open_run(_spec(other, "after-revoke-crash"))
    second.enter()
    try:
        assert second.status().state == "active"
    finally:
        second.leave()


def test_grant_crash_between_record_and_queue(tmp_path: Path) -> None:
    services, account, handle, child = _prepared_run_with_guardian_child(
        tmp_path, {"queue-tempfile-written": {"action": "sigkill", "occurrence": 3}}
    )
    try:
        assert child.wait(timeout=30) == -signal.SIGKILL
        # entered after the kill: entering between the grant's record write and the kill would rightly succeed
        entering = EnterThread(handle, existing_attempt=True)
        entering.join()
    finally:
        reap(child)
    record = _record(services, handle)
    assert (record["state"], record["admission"]) == ("active", "published")  # the grant is durable
    assert queue_phases(account) == {handle.run_id: "admitting"}  # the entry was not yet upgraded and still loads
    assert isinstance(entering.error, ResourceError)
    assert "lost its guardian" in str(entering.error)


def _acknowledged_but_unretired(tmp_path: Path):
    """Leave a run whose guardian is killed after the record write and before the queue removal of its leave ack."""
    services, account, handle, child = _prepared_run_with_guardian_child(
        tmp_path, {"queue-tempfile-written": {"action": "sigkill", "occurrence": 4}}
    )
    entering = EnterThread(handle, existing_attempt=True)
    entering.join()
    assert entering.error is None
    try:
        handle.leave()
    except ToolkitError:
        pass  # the guardian died while acknowledging; the durable prefix below is what matters
    assert child.wait(timeout=30) == -signal.SIGKILL
    reap(child)
    return services, account, handle


def test_ack_crash_before_queue_removal(tmp_path: Path) -> None:
    services, account, handle = _acknowledged_but_unretired(tmp_path)
    record = _record(services, handle)
    assert record["state"] == "complete" and record["leave_ack"] is not None  # the terminal record is durable
    assert queue_phases(account) == {handle.run_id: "granted"}  # the slot entry is still there

    lifecycle.recover_run(handle._context, _test_directory=account)

    assert queue_phases(account) == {}
    assert handle.run_id not in json.loads(handle._context.project.record.read_text())["runs"]
    assert _record(services, handle) == record  # retirement removes references only; the record is untouched


class _StopAtRetirement(Exception):
    """Raised by the failpoint callback to simulate a crash between the two retirement saves."""


def test_retire_completed_saves_queue_removal_before_index_deletion(tmp_path: Path) -> None:
    services, account, handle = _acknowledged_but_unretired(tmp_path)
    before = _record(services, handle)

    def crash(stage: str) -> None:
        if stage == "retire-queue-removed":
            raise _StopAtRetirement

    install_failpoint(crash)
    try:
        with pytest.raises(_StopAtRetirement):
            lifecycle.recover_run(handle._context, _test_directory=account)
    finally:
        install_failpoint(None)
    # The exact prefix: the queue entry is already gone while the project index still names the terminal run.
    assert queue_phases(account) == {}
    assert handle.run_id in json.loads(handle._context.project.record.read_text())["runs"]

    lifecycle.recover_run(handle._context, _test_directory=account)  # the resume finishes the index deletion
    assert handle.run_id not in json.loads(handle._context.project.record.read_text())["runs"]
    assert _record(services, handle) == before


def _ack_crash_with_lease(tmp_path: Path, *, retain: bool = False):
    """``_acknowledged_but_unretired`` written out: keep the requester's lease and the error its leave raised."""
    project = tmp_path / "project"
    project.mkdir()
    services = build_test_services(tmp_path / "state")
    account = services.root / "admission"
    handle = services.open_run(_capacity_spec(project, "crash-prefix", retain=retain))
    prepare_for_child_guardian(handle)
    child, _result = start_guardian_child(
        tmp_path, handle, account, {"queue-tempfile-written": {"action": "sigkill", "occurrence": 4}}
    )
    entering = EnterThread(handle, existing_attempt=True)
    entering.join()
    assert entering.error is None
    lease = handle._lease
    error: ToolkitError | None = None
    try:
        handle.retain() if retain else handle.leave()
    except ToolkitError as raised:
        error = raised  # the guardian died while acknowledging; the requester's verdict is what the cell checks
    assert child.wait(timeout=30) == -signal.SIGKILL
    reap(child)
    assert lease is not None and handle._lease is None  # the handle cleared its lease on the raised leave
    return services, account, handle, lease, error


def _indexed(handle) -> bool:
    return handle.run_id in json.loads(handle._context.project.record.read_text())["runs"]


def test_ack_crash_before_queue_removal_refuses_retirement_incomplete(tmp_path: Path) -> None:
    services, account, handle, lease, error = _ack_crash_with_lease(tmp_path)

    assert isinstance(error, ResourceError) and error.reason == "retirement-incomplete"
    record = _record(services, handle)
    assert record["state"] == "complete" and record["leave_ack"] is not None  # the terminal record is durable
    assert queue_phases(account) == {handle.run_id: "granted"} and _indexed(handle)
    before = handle._context.record.read_bytes()
    with pytest.raises(ResourceError) as again:
        lifecycle.leave_run(lease, "passed")  # function level: the handle's lease is gone
    assert again.value.reason == "retirement-incomplete"
    assert handle._context.record.read_bytes() == before  # nothing beyond the withdrawal was written

    lifecycle.recover_run(handle._context, _test_directory=account)

    assert queue_phases(account) == {} and not _indexed(handle)
    assert lifecycle.leave_run(lease, "passed").state == "complete"
    assert handle.status().state == "complete"


def test_retained_ack_crash_before_queue_removal_refuses_retirement_incomplete(tmp_path: Path) -> None:
    services, account, handle, _lease, error = _ack_crash_with_lease(tmp_path, retain=True)

    # The index is permitted for a retained run; the queue entry alone is the present reference.
    assert isinstance(error, ResourceError) and error.reason == "retirement-incomplete"
    record = _record(services, handle)
    assert record["state"] == "stopped-retained" and record["leave_ack"] is not None
    assert queue_phases(account) == {handle.run_id: "granted"} and _indexed(handle)


def test_index_only_prefix_refuses_retirement_incomplete(tmp_path: Path) -> None:
    _services, account, handle, lease, _error = _ack_crash_with_lease(tmp_path)

    def crash(stage: str) -> None:
        if stage == "retire-queue-removed":
            raise _StopAtRetirement

    install_failpoint(crash)
    try:
        with pytest.raises(_StopAtRetirement):
            lifecycle.recover_run(handle._context, _test_directory=account)
    finally:
        install_failpoint(None)
    assert queue_phases(account) == {} and _indexed(handle)  # the index alone is left

    with pytest.raises(ResourceError) as refused:
        lifecycle.leave_run(lease, "passed")
    assert refused.value.reason == "retirement-incomplete"

    lifecycle.recover_run(handle._context, _test_directory=account)
    assert not _indexed(handle)
    assert lifecycle.leave_run(lease, "passed").state == "complete"


class _LeaveThread:
    """Run ``leave_run`` in a thread; keep its status or error and how long it took."""

    def __init__(self, handle, outcome: str) -> None:
        self.status: lifecycle.RunStatus | None = None
        self.error: BaseException | None = None
        self.elapsed: float | None = None
        self._lease = handle._lease
        self._outcome = outcome
        self._started = time.monotonic()
        self._thread = threading.Thread(target=self._run)
        self._thread.start()

    def _run(self) -> None:
        try:
            self.status = lifecycle.leave_run(self._lease, self._outcome)
        except BaseException as error:  # the test inspects it
            self.error = error
        self.elapsed = time.monotonic() - self._started

    def join(self, timeout: float) -> bool:
        """Join within ``timeout``; return whether the leave finished."""
        self._thread.join(timeout)
        return not self._thread.is_alive()


def _blocked_tail(tmp_path: Path, purpose: str, held: str):
    """An entered run whose guardian is paused, whose 3 s withdrawal is written, and whose ``held`` lock is held.

    ``held`` is ``"registry"`` (the project registry lock) or ``"metadata"`` (the account metadata lock). The 3 s
    budget is test input written as a record ( production writes at least 30 s).
    """
    project = tmp_path / "project"
    project.mkdir()
    services = build_test_services(tmp_path / "state")
    account = services.root / "admission"
    handle = services.open_run(_spec(project, purpose)).enter()
    context = handle._context
    pid = pause_guardian(context)
    withdrawal = prewrite_withdrawal(context, "passed", 3)
    path = context.project.state / "registry.lock" if held == "registry" else account / "metadata.lock"
    return services, account, handle, pid, withdrawal["revision"], hold_lock_until_released(path)


def _acknowledged(context: lifecycle.RunContext, revision: int) -> bool:
    return (json.loads(context.record.read_text()).get("leave_ack") or {}).get("request_revision") == revision


def test_leave_is_bounded_while_the_tail_is_blocked(tmp_path: Path) -> None:
    services, account, handle, pid, revision, holder = _blocked_tail(tmp_path, "bounded-tail", "registry")
    context = handle._context
    fired: list[str] = []
    acknowledged: list[bytes] = []

    def sequence(stage: str) -> None:
        if stage == "leave-poll-read" and "leave-poll-read" not in fired:
            resume_guardian(pid)  # it acknowledges, removes its entry and blocks on the held registry lock
            wait_until(lambda: _acknowledged(context, revision), timeout=30, message="the guardian acknowledged")
            acknowledged.append(context.record.read_bytes())
        fired.append(stage)

    install_failpoint(sequence)
    leaving = None
    try:
        leaving = _LeaveThread(handle, "passed")
        assert leaving.join(20), "the leave did not end at its deadline"
    finally:
        install_failpoint(None)
        release_lock(holder)
        resume_guardian(pid)
        if leaving is not None:
            leaving.join(60)
    assert isinstance(leaving.error, ResourceBusy) and leaving.error.reason == "lock-deadline"
    assert leaving.elapsed is not None and leaving.elapsed < 10
    assert "leave-deadline-branch" not in fired  # the blocked lock was the barrier, not _leave_incomplete's
    assert context.record.read_bytes() == acknowledged[0]  # terminal with leave_ack; the requester wrote nothing
    wait_guardians_exited(services.root / "registry")
    assert queue_phases(account) == {} and not _indexed(handle)


def _lock_deadline_before_the_acknowledgment(tmp_path: Path):
    """The guardian blocks on the held metadata lock holding the run lock; the requester's bounded re-read expires."""
    services, account, handle, pid, _revision, holder = _blocked_tail(tmp_path, "pending-record", "metadata")
    context = handle._context
    fired: list[str] = []

    def sequence(stage: str) -> None:
        if stage == "leave-poll-read" and "leave-poll-read" not in fired:
            resume_guardian(pid)  # it takes project+run and blocks on the held metadata lock
        elif stage == "leave-deadline-branch":
            wait_until(lambda: not try_lock(run_lock_path(context)), timeout=30, message="the guardian holds run")
        fired.append(stage)

    install_failpoint(sequence)
    leaving = None
    try:
        leaving = _LeaveThread(handle, "passed")
        finished = leaving.join(20)
    finally:
        install_failpoint(None)
        if leaving is None or not leaving.join(0):
            release_lock(holder)
            resume_guardian(pid)
            if leaving is not None:
                leaving.join(60)
    assert finished, "the leave did not end at its deadline"
    assert isinstance(leaving.error, ResourceBusy) and leaving.error.reason == "lock-deadline"
    assert leaving.elapsed is not None and leaving.elapsed < 10
    assert "leave-deadline-branch" in fired
    record = _record(services, handle)
    assert (record["state"], record["cleanup_outcome"], record.get("leave_ack")) == ("stopping", "pending", None)
    return services, account, handle, holder


def test_lock_deadline_before_the_acknowledgment_leaves_a_pending_record(tmp_path: Path) -> None:
    services, account, handle, holder = _lock_deadline_before_the_acknowledgment(tmp_path)

    release_lock(holder)
    wait_guardians_exited(services.root / "registry")

    assert _record(services, handle)["leave_ack"] is not None  # the live guardian acknowledged on its own
    assert queue_phases(account) == {} and not _indexed(handle)


def test_lock_deadline_pending_record_is_finished_by_recovery(tmp_path: Path) -> None:
    _services, account, handle, holder = _lock_deadline_before_the_acknowledgment(tmp_path)

    kill_guardian(handle._context)  # blocked on the metadata lock: the kernel drops its locks
    release_lock(holder)  # only now: a release before the kill would let the guardian finish by itself
    status = lifecycle.recover_run(handle._context, _test_directory=account)

    assert status.state == "complete" and status.cleanup_outcome == "incomplete"
    assert queue_phases(account) == {} and not _indexed(handle)


def test_retained_run_whose_guardian_dies_after_the_queue_save_leaves_cleanly(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    services = build_test_services(tmp_path / "state")
    account = services.root / "admission"
    handle = services.open_run(_capacity_spec(project, "retained-queue-saved", retain=True))
    prepare_for_child_guardian(handle)
    child, _result = start_guardian_child(
        tmp_path, handle, account, {"queue-saved": {"action": "sigkill", "occurrence": 4}}
    )
    entering = EnterThread(handle, existing_attempt=True)
    entering.join()
    assert entering.error is None
    try:
        status = handle.retain()  # the guardian dies after its leave's queue save; the index is expected present
    finally:
        assert child.wait(timeout=30) == -signal.SIGKILL
        reap(child)

    assert status.state == "stopped-retained"
    assert queue_phases(account) == {} and _indexed(handle)


def test_concurrent_recovery_never_yields_a_status_with_a_reference_present(tmp_path: Path) -> None:
    outcomes = []
    for attempt in range(10):
        root = tmp_path / f"attempt-{attempt}"
        root.mkdir()
        _services, account, handle, lease, _error = _ack_crash_with_lease(root)
        context = handle._context
        stages: dict = {}
        if attempt % 2:  # the recoverer is first: it waits at its registry wait until the requester has started
            stages = {
                "recovery-registry-wait": {
                    "action": [
                        {"do": "write-file", "path": str(root / "ready")},
                        {"do": "wait-file", "path": str(root / "go")},
                    ]
                }
            }
        child, result = start_recover_child(root, context, account, stages)
        if attempt % 2:
            wait_until((root / "ready").exists, timeout=30, message="the recoverer reached its registry wait")
            (root / "go").write_text("")
        try:
            status = lifecycle.leave_run(lease, "passed")
        except ResourceError as error:
            assert error.reason == "retirement-incomplete", error
            outcomes.append("refused")
        else:
            assert status.state == "complete"
            assert handle.run_id not in queue_phases(account) and not _indexed(handle)  # never a reference present
            outcomes.append("status")
        child_result(child, result, cap=60)
        if _indexed(handle) or handle.run_id in queue_phases(account):
            lifecycle.recover_run(context, _test_directory=account)
        assert queue_phases(account) == {} and not _indexed(handle)
    assert len(outcomes) == 10


def test_resource_free_leave_revision_is_the_acknowledgment(tmp_path: Path) -> None:
    """A faster poll adds reads, never writes: the acknowledgment is the record's last revision."""
    project = tmp_path / "project"
    project.mkdir()
    services = build_test_services(tmp_path / "state")
    handle = services.open_run(_spec(project, "revision-observation")).enter()
    context = handle._context

    handle.leave()
    record = json.loads(context.record.read_text())
    after_leave = context.record.read_bytes()
    wait_guardians_exited(services.root / "registry")

    assert record["withdrawal"]["revision"] + 1 == record["leave_ack"]["revision"] == record["revision"]
    assert context.record.read_bytes() == after_leave  # the guardian wrote nothing after the leave returned


def test_reconcile_account_never_reads_foreign_records(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    account = services.root / "admission"
    holder = services.open_run(_spec(projects[0], "foreign-holder"))
    holder.enter()
    record = services.root / "registry" / "runs" / f"{holder.run_id}.json"
    original = json.loads(record.read_bytes())
    pid = pause_guardian(holder._context)  # the holder's own guardian must not read the hostile window
    try:
        record.write_bytes(b"{")  # a hostile foreign record

        lifecycle.reconcile_account(_test_directory=account)  # neither raises nor recovers a stranger's run

        assert record.read_bytes() == b"{"  # nothing read it for repair and nothing rewrote it
        assert queue_phases(account) == {holder.run_id: "granted"}
    finally:
        atomic_json(record, original)  # one atomic, loader-valid restore before the guardian resumes
        resume_guardian(pid)
    holder.leave()  # the holder's guardian survived the window


@pytest.mark.parametrize("hold", ["released-in-time", "held-past-the-queue-deadline"])
def test_metadata_holder_bounds_admission_pass(tmp_path: Path, hold: str) -> None:
    services = build_test_services(tmp_path / "state")
    account = services.root / "admission"
    first, second = tmp_path / "project-a", tmp_path / "project-b"
    first.mkdir()
    second.mkdir()
    holder_run = services.open_run(_spec(first, "slot-holder")).enter()  # a live holder: B queues behind it
    expiring = hold == "held-past-the-queue-deadline"
    opened = time.monotonic()
    waiting = services.open_run(_spec(second, "queued-waiter", wait_seconds=2 if expiring else 60))
    waiter = EnterThread(waiting)
    wait_until(lambda: waiting._context.run_id in queue_entries(account), message="the waiter is queued")
    metadata = _hold_lock(account / "metadata.lock")  # a real process owns metadata during the guardian's passes
    try:
        if expiring:
            waiter.join(30)  # the requester's own queue deadline ends its wait
            elapsed = time.monotonic() - opened
            assert isinstance(waiter.error, ResourceError) and 2.0 <= elapsed < 10.0
        else:
            time.sleep(2.5)  # several one-second passes wait on the held lock instead of failing
            assert waiter._thread.is_alive() and waiter.error is None
    finally:
        reap(metadata)
    if expiring:
        wait_until(
            lambda: _record(services, waiting)["state"] == "complete",
            timeout=30,
            message="the expired run is revoked once metadata is free, as before",
        )
        assert waiting._context.run_id not in queue_entries(account)
        holder_run.leave()
        return
    holder_run.leave()  # the slot frees: the waiter's pass proceeds after the release and it is granted
    waiter.join(60)
    assert waiter.error is None
    assert queue_phases(account)[waiting._context.run_id] == "granted"
    waiting.leave()


def test_registry_holder_bounds_open_run(tmp_path: Path) -> None:
    artifact = source_artifact()
    service = private_account(tmp_path)
    project = tmp_path / "project"
    project.mkdir()
    spec = run_spec(project, artifact, "registry-held")
    holder = _hold_lock(service.state_directory / "registry.lock")  # a real process owns the account registry
    try:
        started = time.monotonic()
        with pytest.raises(ResourceBusy) as raised:
            open_run(spec, _test_state=service.state_directory, _prepare_budget_seconds=2)
        elapsed = time.monotonic() - started
    finally:
        reap(holder)

    assert raised.value.category == "contention" and raised.value.reason == "lock-deadline"
    assert 2.0 <= elapsed < 10.0  # the preparation budget, not the queue wait, bounds the registry-side waits
    assert list((service.state_directory / "runs").glob("*.json")) == []  # nothing was prepared: no record to recover
    open_run(spec, _test_state=service.state_directory)  # with the registry free it opens


def test_two_open_run_processes_within_one_second(tmp_path: Path) -> None:
    service = private_account(tmp_path)
    control = tmp_path / "control"
    control.mkdir()
    names = ("first", "second")
    projects = {name: tmp_path / f"project-{name}" for name in names}
    for project in projects.values():
        project.mkdir()

    def exists(name: str, step: str) -> bool:
        return (control / f"{name}.{step}").exists()

    def signal_child(name: str, step: str) -> None:
        (control / f"{name}.{step}").write_text("")

    def tickets() -> dict[str, tuple[int, str]]:
        entries = queue_entries(service.account_directory).values()
        by_project = {Path(entry.project_root).name: (entry.ticket, entry.phase) for entry in entries}
        return {name: by_project[f"project-{name}"] for name in names if f"project-{name}" in by_project}

    children = []
    try:
        children.extend(
            subprocess.Popen(
                [
                    sys.executable,
                    "-P",
                    str(OPEN_RUN_CHILD),
                    str(service.state_directory),
                    str(projects[name]),
                    str(control),
                    name,
                ]
            )
            for name in names
        )
        for name in names:
            wait_until(lambda name=name: exists(name, "ready"), message=f"{name} imported")
        # Both processes call open_run back to back; the BUG-03 window is the overlap of their metadata phases.
        signal_child("first", "go")
        signal_child("second", "go")
        for name in names:
            wait_until(
                lambda name=name: exists(name, "opened") or exists(name, "result.json"), message=f"{name} opened"
            )
        calls = {name: int((control / f"{name}.call").read_text()) for name in names}
        assert abs(calls["second"] - calls["first"]) <= 1_000_000_000  # the two open_run calls start within one second
        # Admission order is then explicit: the first run enters and takes its ticket before the second one enters.
        signal_child("first", "enter-go")
        wait_until(lambda: "first" in tickets(), message="first ticket taken")
        wait_until(lambda: exists("first", "entered"), message="first run granted")
        signal_child("second", "enter-go")
        wait_until(lambda: len(tickets()) == 2, message="second ticket taken")
        observed = tickets()
        assert observed["second"][0] == observed["first"][0] + 1
        assert observed["first"][1] == "granted"
        assert observed["second"][1] == "queued"
        signal_child("first", "release")
        wait_until(lambda: exists("second", "entered"), message="second run granted after the first left")
        signal_child("second", "release")
        for child in children:
            assert child.wait(timeout=30) == 0
    finally:
        for child in children:
            reap(child)

    results = {name: json.loads((control / f"{name}.result.json").read_text()) for name in names}
    assert {name: (result["error_type"], result["final_state"]) for name, result in results.items()} == {
        "first": (None, "complete"),
        "second": (None, "complete"),
    }


_ATTEMPT = "11111111-1111-4111-8111-111111111111"
_OTHER_ATTEMPT = "22222222-2222-4222-8222-222222222222"


def test_initialized_account_without_queue_file_refuses_reconciliation_required(tmp_path: Path) -> None:
    account = tmp_path / "account"
    account.mkdir(mode=0o700)
    anchor_path = account / "metadata.lock"
    anchor_path.write_text(admission.STATE_MARKER)
    anchor_path.chmod(0o600)

    with (
        anchor_path.open("r+") as anchor,
        pytest.raises(ResourceError, match="Initialized account queue history is missing"),
    ):
        admission.load_queue(account, anchor)
    assert not (account / "queue.json").exists()


@pytest.mark.parametrize("field", ["none", "project_root", "registry"])
def test_guardian_refuses_an_own_queue_entry_that_names_another_project_or_registry(tmp_path: Path, field: str) -> None:
    project = tmp_path / "project"
    project.mkdir()
    services = build_test_services(tmp_path / "state")
    account = services.root / "admission"
    handle = services.open_run(_spec(project, "own-entry-identity"))
    prepare_for_child_guardian(handle)
    context = handle._context
    # This test process plays the queued guardian (enroll_guardian refuses the requester's own pid): its record and
    # its queue entry are written in the shape enroll_guardian leaves.
    record = _record(services, handle)
    attempt = record["guardian_attempt"]
    record.update(
        state="queued",
        ticket=1,
        account_directory=str(account),
        guardian=asdict(lifecycle.current_process(lifecycle.NativeClock())),
        pending_operation=None,
    )
    atomic_json(context.record, record)
    entry = {
        "ticket": 1,
        "attempt": attempt,
        "record": str(context.record),
        "registry": str(context.project.state),
        "project_root": str(context.project.root),
        "project_id": hashlib.sha256(str(context.project.root).encode()).hexdigest(),
        "phase": "queued",
        "holder": None,
        "dead_since": None,
        "takeover": None,
        "last_completed_phase": None,
    }
    if field == "project_root":
        entry["project_root"] = "/elsewhere/project"
        entry["project_id"] = hashlib.sha256(b"/elsewhere/project").hexdigest()
    elif field == "registry":  # a consistent relocation: registry and record move together, so the codec accepts it
        entry["registry"] = "/elsewhere/registry"
        entry["record"] = f"/elsewhere/registry/runs/{handle.run_id}.json"
    queue = write_v2_queue(account, [(handle.run_id, entry)], next_ticket=2)
    (account / "metadata.lock").write_text(
        admission.STATE_MARKER
    )  # the initialization marker of an account with a queue
    (account / "metadata.lock").chmod(0o600)
    record_before = _record(services, handle)
    stack = ExitStack()

    if field == "none":  # the control: the same harness reserves the slot when the entry names this run's project
        with stack:
            outcome = lifecycle.reserve_capacity(context, attempt, stack, _test_directory=account)
            assert outcome.descriptor is not None and outcome.waiting is None
        assert queue_phases(account) == {handle.run_id: "admitting"}
        return
    with pytest.raises(ResourceError, match="account ticket identity"):
        lifecycle.reserve_capacity(context, attempt, stack, _test_directory=account)

    assert (account / "queue.json").read_bytes() == queue  # nothing reserved
    assert _record(services, handle) == record_before


def _reserve(tmp_path: Path, *, holder: bool = False, lock_capacity: bool = False):
    """Queue one guardian-run (this process plays the guardian) and ask for its slot; return outcome and queue bytes."""
    _services, handle, context, _record_value, account, fields = _queued_entry_fixture(tmp_path)
    entries = [(handle.run_id, {key: value for key, value in fields.items() if key != "run_id"})]
    if holder:
        run_id, entry = queue_entry(2, 2, "granted")
        entries.append((run_id, {**entry, "holder": dead_identity()}))
    write_v2_queue(account, entries, next_ticket=3)
    sharing.record_parallel_runs(account, 1)  # one at a time: a granted holder means queue position
    (account / "metadata.lock").write_text(admission.STATE_MARKER)
    (account / "metadata.lock").chmod(0o600)
    queue_before = (account / "queue.json").read_bytes()
    blocker = _hold_lock(account / "capacity.lock") if lock_capacity else None
    try:
        with ExitStack() as stack:
            outcome = lifecycle.reserve_capacity(context, fields["attempt"], stack, _test_directory=account)
            return outcome, account, queue_before
    finally:
        if blocker is not None:
            blocker.kill()
            blocker.wait()


def test_reserve_capacity_reports_the_reservation(tmp_path: Path) -> None:
    outcome, account, _before = _reserve(tmp_path)

    assert outcome.waiting is None and outcome.descriptor is not None
    assert list(queue_phases(account).values()) == ["admitting"]


def test_reserve_capacity_reports_queue_position(tmp_path: Path) -> None:
    outcome, account, before = _reserve(tmp_path, holder=True)

    assert outcome == lifecycle.ReserveOutcome(None, "queue-position")
    assert (account / "queue.json").read_bytes() == before  # a waiting run writes nothing


def test_reserve_capacity_reports_capacity_lock_busy(tmp_path: Path) -> None:
    outcome, account, before = _reserve(tmp_path, lock_capacity=True)

    assert outcome == lifecycle.ReserveOutcome(None, "capacity-lock-busy")
    assert (account / "queue.json").read_bytes() == before


def _queued_entry_fixture(tmp_path: Path):
    """Make one prepared run look like a queued guardian's (this process plays the guardian); return its pieces."""
    project = tmp_path / "project"
    project.mkdir()
    services = build_test_services(tmp_path / "state")
    account = services.root / "admission"
    handle = services.open_run(_capacity_spec(project, "entry-identity"))
    prepare_for_child_guardian(handle)
    context = handle._context
    record = _record(services, handle)
    attempt = record["guardian_attempt"]
    record.update(  # the record of a queued guardian, as enroll_guardian leaves it
        state="queued",
        ticket=1,
        account_directory=str(account),
        guardian=asdict(lifecycle.current_process(lifecycle.NativeClock())),
        pending_operation=None,
    )
    atomic_json(context.record, record)
    fields = {
        "run_id": handle.run_id,
        "ticket": 1,
        "attempt": attempt,
        "record": str(context.record),
        "registry": str(context.project.state),
        "project_root": str(context.project.root),
        "project_id": hashlib.sha256(str(context.project.root).encode()).hexdigest(),
        "phase": "queued",
        "holder": None,
        "dead_since": None,
        "takeover": None,
        "last_completed_phase": None,
    }
    return services, handle, context, record, account, fields


@pytest.mark.parametrize(
    "mutation", ["none", "attempt", "ticket", "record", "project_root", "registry", "record_account_directory"]
)
def test_queue_entry_is_blocked_exactly_when_its_record_contradicts_its_identity(tmp_path: Path, mutation: str) -> None:
    _services, handle, context, record, account, fields = _queued_entry_fixture(tmp_path)
    if mutation == "record_account_directory":
        atomic_json(context.record, {**record, "account_directory": "/elsewhere/account"})
    fields.update(
        {
            "attempt": {"attempt": "99999999-9999-4999-8999-999999999999"},
            "ticket": {"ticket": 2},
            "record": {"record": str(context.record) + ".other"},
            "project_root": {"project_root": str(tmp_path / "another-project")},
            "registry": {"registry": str(tmp_path / "another-state")},
        }.get(mutation, {})
    )

    facts = lifecycle._entry_facts(handle.run_id, admission.QueueEntry(**fields), account)

    assert facts.blocked is (mutation != "none")
    assert facts.guardian_state == (None if facts.blocked else "executing")  # an unblocked entry reports liveness


def test_holder_entry_whose_record_names_another_guardian_is_untrusted(tmp_path: Path) -> None:
    """A holder entry contradicting its record's guardian is blocked (fail closed)."""
    _services, handle, _context, record, account, fields = _queued_entry_fixture(tmp_path)
    holder = {**fields, "phase": "granted", "holder": record["guardian"]}

    trusted = lifecycle._entry_facts(handle.run_id, admission.QueueEntry(**holder), account)
    other = {"pid": 2_147_483_000, "origin": "test", "birth": [1], "audit_token": None}
    contradicted = lifecycle._entry_facts(handle.run_id, admission.QueueEntry(**{**holder, "holder": other}), account)

    assert (trusted.blocked, contradicted.blocked) == (False, True)


def test_entry_facts_read_the_record_on_every_call(tmp_path: Path) -> None:
    """The facts are never cached: an unreadable record blocks the entry for that call only."""
    _services, handle, context, record, account, fields = _queued_entry_fixture(tmp_path)
    entry = admission.QueueEntry(**fields)

    assert not lifecycle._entry_facts(handle.run_id, entry, account).blocked
    context.record.write_text("{")
    unreadable = lifecycle._entry_facts(handle.run_id, entry, account)
    atomic_json(context.record, record)

    assert unreadable.blocked
    assert not lifecycle._entry_facts(handle.run_id, entry, account).blocked


def _raise_interrupted() -> None:
    raise Interrupted


@pytest.mark.parametrize("field", ["none", "project_root", "registry", "phase"])
def test_publish_grant_refuses_a_slot_entry_that_names_another_project_or_registry(tmp_path: Path, field: str) -> None:
    reserved, resume = tmp_path / "reserved.ready", tmp_path / "resume"
    stages = {
        "queue-saved": {
            # occurrence 2 is the reservation save: the guardian pauses holding the slot, before it publishes anything
            "action": [{"do": "write-file", "path": str(reserved)}, {"do": "wait-file", "path": str(resume)}],
            "occurrence": 2,
        }
    }
    services, account, handle, child = _prepared_run_with_guardian_child(tmp_path, stages)
    try:
        wait_until(reserved.exists, message="guardian reserved the slot")
        # The guardian holds the metadata lock while paused, so the entry is rewritten directly (no lock, no codec).
        queue_path = account / "queue.json"
        queue = json.loads(queue_path.read_text())
        entry = queue["entries"][handle.run_id]
        if field == "project_root":
            entry["project_root"] = "/elsewhere/project"
            entry["project_id"] = hashlib.sha256(b"/elsewhere/project").hexdigest()
        elif field == "registry":
            entry["registry"] = "/elsewhere/registry"
            entry["record"] = f"/elsewhere/registry/runs/{handle.run_id}.json"
        elif field == "phase":
            entry["phase"] = "granted"  # the record is still admitting: publication must see the contradiction
        queue_path.write_bytes(canonical_json_bytes(queue))
        resume.write_text("")
        if field == "none":  # the control: an untouched entry is published
            wait_until(lambda: _record(services, handle)["admission"] == "published", message="grant published")
        else:
            assert child.wait(timeout=30) != 0  # the guardian ends on the contradiction
            record = _record(services, handle)
            assert record["admission"] != "published" and record["generation"] == 0  # no grant was published
    finally:
        reap(child)


# Writer-set audit: every writer of a terminal run record must leave a record the loader accepts, and a
# record that is no longer project-indexed must be quiescent or a generation-0 complete record with cleanup complete.
# Writers that need a fake Docker or the operator locator are proved by the named tests in their own modules (the
# last test below checks that those tests exist).
_AUDIT_COVERED_ELSEWHERE = {
    "_restore_stopped_retained": (
        "tests.integration.test_docker_receipts",
        "test_cancelling_a_resume_preparation_restores_the_stopped_retained_record",
    ),
    "_recover_resource_grant": (
        "tests.integration.test_docker_receipts",
        "test_withdrawn_dead_owner_recovery_records_complete_cleanup",
    ),
    "_recover_resource_grant (retained)": (
        "tests.integration.test_docker_receipts",
        "test_withdrawn_retained_dead_owner_recovery_keeps_stored_cleanup",
    ),
    "release_run (completed retry)": (
        "tests.integration.test_docker_receipts",
        "test_completed_release_recovery_retires_its_project_index_after_a_retry",
    ),
}


def _audit_project(tmp_path: Path, name: str) -> Path:
    project = tmp_path / name
    project.mkdir()
    return project


def _audit_cancel_run(tmp_path: Path, services, monkeypatch: pytest.MonkeyPatch) -> tuple[lifecycle.RunContext, bool]:
    handle = services.open_run(_spec(_audit_project(tmp_path, "cancel"), "audit-cancel"))
    handle.cancel()
    return handle._context, False


def _audit_empty_leave_ack(
    tmp_path: Path, services, monkeypatch: pytest.MonkeyPatch
) -> tuple[lifecycle.RunContext, bool]:
    handle = services.open_run(_spec(_audit_project(tmp_path, "leave"), "audit-leave")).enter()
    handle.leave()
    return handle._context, False


def _audit_generation_zero_recover(
    tmp_path: Path, services, monkeypatch: pytest.MonkeyPatch
) -> tuple[lifecycle.RunContext, bool]:
    handle = services.open_run(_spec(_audit_project(tmp_path, "gen0"), "audit-gen0"))
    record = json.loads(handle._context.record.read_text())
    atomic_json(handle._context.record, {**record, "requester": dead_identity()})
    lifecycle.recover_run(handle._context, _test_directory=services.root / "admission")
    return handle._context, False


def _audit_revoke_with_live_requester(
    tmp_path: Path, services, monkeypatch: pytest.MonkeyPatch
) -> tuple[lifecycle.RunContext, bool]:
    handle = services.open_run(_spec(_audit_project(tmp_path, "revoke-live"), "audit-revoke-live"))
    prepare_for_child_guardian(handle)
    child, _result = start_guardian_child(
        tmp_path, handle, services.root / "admission", {"queue-saved": {"action": "sigkill", "occurrence": 2}}
    )
    try:
        assert child.wait(timeout=30) == -signal.SIGKILL
    finally:
        reap(child)
    lifecycle.revoke_ungranted(handle._context, _test_directory=services.root / "admission")
    return handle._context, True  # the requester lives: the revoked record stays project-indexed


def _audit_revoke_with_dead_requester(
    tmp_path: Path, services, monkeypatch: pytest.MonkeyPatch
) -> tuple[lifecycle.RunContext, bool]:
    handle = services.open_run(_spec(_audit_project(tmp_path, "revoke-dead"), "audit-revoke-dead"))
    prepare_for_child_guardian(handle)
    record = json.loads(handle._context.record.read_text())
    bootstrap = {**record["bootstrap_deadline"], "duration_ns": 1}
    bootstrap["expires_ns"] = bootstrap["created_ns"] + 1  # the guardian start expired: takeover is no longer refused
    atomic_json(handle._context.record, {**record, "requester": dead_identity(), "bootstrap_deadline": bootstrap})
    lifecycle.revoke_ungranted(handle._context, _test_directory=services.root / "admission")
    return handle._context, False


def _audit_recover_empty_grant(
    tmp_path: Path, services, monkeypatch: pytest.MonkeyPatch
) -> tuple[lifecycle.RunContext, bool]:
    handle = services.open_run(_spec(_audit_project(tmp_path, "empty"), "audit-empty")).enter()
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
    lifecycle.recover_run(handle._context, _test_directory=services.root / "admission")
    return handle._context, False


def _audit_leave_incomplete(
    tmp_path: Path, services, monkeypatch: pytest.MonkeyPatch
) -> tuple[lifecycle.RunContext, bool]:
    handle = services.open_run(_spec(_audit_project(tmp_path, "incomplete"), "audit-incomplete")).enter()
    kill_guardian(handle._context)
    with pytest.raises(IncompleteCleanup):
        handle.leave()  # the guardian is gone: cleanup_outcome becomes incomplete, the run stays stopping
    return handle._context, True


_AUDIT_DRIVEN = {
    "cancel_run": _audit_cancel_run,
    "_acknowledge_empty_leave": _audit_empty_leave_ack,
    "recover_run generation 0": _audit_generation_zero_recover,
    "revoke_ungranted live requester": _audit_revoke_with_live_requester,
    "revoke_ungranted dead requester": _audit_revoke_with_dead_requester,
    "_recover_empty_grant": _audit_recover_empty_grant,
    "_leave_incomplete": _audit_leave_incomplete,
}


@pytest.mark.parametrize("writer", list(_AUDIT_DRIVEN))
def test_writer_set_audit_records_all_load(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, writer: str) -> None:
    services = build_test_services(tmp_path / "state")
    context, indexed = _AUDIT_DRIVEN[writer](tmp_path, services, monkeypatch)
    wait_guardians_exited(context.record.parent.parent)  # a guardian still finishing de-indexes the run a moment later

    record = lifecycle.load_run_record(context)  # the loader accepts what the writer left
    index = json.loads(context.project.record.read_text())["runs"]
    assert (context.run_id in index) is indexed
    if not indexed:
        assert record["state"] == "complete"
        assert lifecycle.completed_run_is_quiescent(record) or (
            record["generation"] == 0 and record["cleanup_outcome"] == "complete"
        )


def test_writer_set_audit_names_a_real_test_for_every_writer_it_does_not_drive() -> None:
    import importlib

    for writer, (module, name) in _AUDIT_COVERED_ELSEWHERE.items():
        assert callable(getattr(importlib.import_module(module), name, None)), f"{writer}: {module}.{name} is missing"


def _prefix_account(tmp_path: Path):
    """A private account that owns its queue file, and one project root for creation-prefix tests."""
    service = private_account(tmp_path)
    write_v2_queue(service.account_directory, [])
    project = tmp_path / "prefix-project"
    project.mkdir()
    return service, project


def _project_runs(context: lifecycle.RunContext) -> dict:
    return json.loads(context.project.record.read_text())["runs"]


def _dead_requester_prefix(tmp_path: Path, service, project: Path):
    """Kill the requester right after prepare_run wrote its record: an unindexed creation prefix with a dead owner."""
    child, _result = start_prepare_child(
        tmp_path,
        service,
        project,
        {"prepare-record-written": {"action": "sigkill"}},
    )
    assert child.wait(timeout=30) == -signal.SIGKILL
    return run_context_of(service, project), child


def _bytes_of(context: lifecycle.RunContext) -> dict[str, bytes]:
    return {"record": context.record.read_bytes(), "index": context.project.record.read_bytes()}


def _recover(context: lifecycle.RunContext, service):
    return lifecycle.recover_run(context, _test_directory=service.account_directory)


def _assert_finalized_prefix(context: lifecycle.RunContext) -> None:
    final = json.loads(context.record.read_text())
    assert (final["state"], final["admission"], final["test_outcome"], final["cleanup_outcome"]) == (
        "complete",
        "revoked",
        "cancelled",
        "complete",
    )
    assert context.run_id not in _project_runs(context)  # no reference left
    assert not context.private_root.exists() and not context.private_root.parent.parent.exists()


def test_creation_prefix_is_repaired_and_finalized(tmp_path: Path) -> None:
    service, project = _prefix_account(tmp_path)
    context, _child = _dead_requester_prefix(tmp_path, service, project)
    record = json.loads(context.record.read_text())
    assert (record["state"], record["generation"], record["revision"]) == ("prepared", 0, 0)
    assert context.run_id not in _project_runs(context)  # the prefix: a prepared record that no index names

    _recover(context, service)

    _assert_finalized_prefix(context)


@pytest.mark.parametrize(
    "stage", ["prefix-index-restored", "revoke-record-written", "private-root-removed", "revoke-cleanup-written"]
)
def test_creation_prefix_resumes_after_each_repair_write(tmp_path: Path, stage: str) -> None:
    service, project = _prefix_account(tmp_path)
    context, _child = _dead_requester_prefix(tmp_path, service, project)

    def interrupt(name: str) -> None:
        if name == stage:
            raise Interrupted

    install_failpoint(interrupt)
    try:
        with pytest.raises(Interrupted):
            _recover(context, service)
    finally:
        install_failpoint(None)
    record = json.loads(context.record.read_text())
    assert (context.run_id in _project_runs(context)) is True  # the index entry was restored before anything else
    expected_state = "prepared" if stage == "prefix-index-restored" else "complete"
    assert record["state"] == expected_state
    assert (record["cleanup_outcome"] == "complete") is (stage == "revoke-cleanup-written")

    _recover(context, service)  # a rerun of the exact recovery finishes from whatever prefix is durable

    _assert_finalized_prefix(context)


def test_creation_prefix_with_live_requester_is_untouched(tmp_path: Path) -> None:
    service, project = _prefix_account(tmp_path)

    def interrupt(name: str) -> None:
        if name == "prepare-record-written":
            raise Interrupted

    install_failpoint(interrupt)
    try:
        with pytest.raises(Interrupted):
            lifecycle.prepare_run(
                lifecycle.RunRequest(project, "integration", "live-prefix", (), (), "fixture", wait_seconds=60),
                _test_state=service.state_directory,
                _test_directory=service.account_directory,
            )
    finally:
        install_failpoint(None)
    context = run_context_of(service, project)
    before = _bytes_of(context)

    with pytest.raises(ResourceBusy) as raised:
        _recover(context, service)

    assert raised.value.reason == "actor-live"
    assert _bytes_of(context) == before


@pytest.mark.parametrize(("observed", "reason"), [("zombie", "actor-live"), ("unobservable", "actor-unverifiable")])
def test_creation_prefix_with_unprovable_requester_is_untouched(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, observed: str, reason: str
) -> None:
    service, project = _prefix_account(tmp_path)

    def interrupt(name: str) -> None:
        if name == "prepare-record-written":
            raise Interrupted

    install_failpoint(interrupt)
    try:
        with pytest.raises(Interrupted):
            lifecycle.prepare_run(
                lifecycle.RunRequest(project, "integration", "unprovable-prefix", (), (), "fixture", wait_seconds=60),
                _test_state=service.state_directory,
                _test_directory=service.account_directory,
            )
    finally:
        install_failpoint(None)
    context = run_context_of(service, project)
    before = _bytes_of(context)
    original = lifecycle.observe_process

    def unprovable(identity: lifecycle.ProcessIdentity) -> str:
        # The OS seam: macOS cannot observe a zombie (the kernel reports it as gone), so the unprovable states a
        # requester can be in are stood in for at the observation boundary, as the receipts tests do for resume.
        if identity.pid != os.getpid():
            return original(identity)
        if observed == "unobservable":
            raise ResourceError("Process identity is ambiguous.")
        return observed

    monkeypatch.setattr(lifecycle, "observe_process", unprovable)

    with pytest.raises(ToolkitError) as raised:
        _recover(context, service)

    assert raised.value.reason == reason  # a zombie is not proven dead; an unreadable observation is not death
    assert _bytes_of(context) == before


def test_creation_prefix_with_unregistered_root_refuses(tmp_path: Path) -> None:
    service, project = _prefix_account(tmp_path)
    context, _child = _dead_requester_prefix(tmp_path, service, project)
    foreign_root = tmp_path / "foreign-private-root"
    foreign_root.mkdir(mode=0o700)
    record = json.loads(context.record.read_text())
    atomic_json(context.record, {**record, "private_root": str(foreign_root)})
    context = lifecycle.RunContext(context.project, context.run_id, context.session_id, context.record, foreign_root)
    before = _bytes_of(context)

    with pytest.raises(ResourceError) as raised:
        _recover(context, service)

    assert raised.value.reason == "record-changed"
    assert _bytes_of(context) == before
    assert foreign_root.exists()  # the root was never removed


def test_second_recover_after_completed_prefix_is_zero_write(tmp_path: Path) -> None:
    service, project = _prefix_account(tmp_path)
    context, _child = _dead_requester_prefix(tmp_path, service, project)
    _recover(context, service)
    before = _bytes_of(context)

    status = _recover(context, service)

    assert (status.state, status.cleanup_outcome) == ("complete", "complete")
    assert _bytes_of(context) == before
    assert context.run_id not in _project_runs(context)


def test_two_concurrent_recoverers_of_one_creation_prefix(tmp_path: Path) -> None:
    service, project = _prefix_account(tmp_path)
    context, _child = _dead_requester_prefix(tmp_path, service, project)
    second_read, first_done = threading.Event(), threading.Event()
    results: dict[str, object] = {}
    restored_by: list[str] = []

    def run(name: str) -> None:
        try:
            results[name] = _recover(context, service)
        except ToolkitError as error:
            results[name] = error
        if name == "first":
            first_done.set()

    def gate(stage: str) -> None:
        if stage == "prefix-index-restored":
            restored_by.append(threading.current_thread().name)
        # the second recoverer pauses after its unlocked prefix read, before it takes the registry lock
        if stage == "prefix-read" and threading.current_thread().name == "second":
            second_read.set()
            assert first_done.wait(timeout=30)

    install_failpoint(gate)
    try:
        second = threading.Thread(target=run, args=("second",), name="second")
        second.start()
        assert second_read.wait(timeout=30)
        first = threading.Thread(target=run, args=("first",), name="first")
        first.start()
        first.join(timeout=60)
        second.join(timeout=60)
    finally:
        install_failpoint(None)

    finished = [name for name, value in results.items() if not isinstance(value, ToolkitError)]
    assert "first" in finished  # the unobstructed recoverer finalized the run
    assert all(isinstance(value, ToolkitError) or value.cleanup_outcome == "complete" for value in results.values())
    assert restored_by == ["first"]  # the late recoverer's re-check under the registry lock wrote no index entry
    _assert_finalized_prefix(context)  # and no entry exists for the finished record


def test_revoke_tail_second_caller_neither_rewrites_nor_raises(tmp_path: Path) -> None:
    service, project = _prefix_account(tmp_path)
    child, _result = start_prepare_child(tmp_path, service, project, {})
    assert child.wait(timeout=30) == 0  # the requester prepared its run and ended
    context = run_context_of(service, project)

    first = lifecycle.revoke_ungranted(context, _test_directory=service.account_directory)
    assert first.cleanup_outcome == "complete"
    before = _bytes_of(context)

    second = lifecycle.revoke_ungranted(context, _test_directory=service.account_directory)

    assert second.cleanup_outcome == "complete"
    assert _bytes_of(context) == before  # no rewrite


def test_unindexed_run_that_is_not_an_exact_prefix_refuses_without_writes(tmp_path: Path) -> None:
    service, project = _prefix_account(tmp_path)
    context, _child = _dead_requester_prefix(tmp_path, service, project)
    record = json.loads(context.record.read_text())
    atomic_json(context.record, {**record, "revision": 1})  # progressed past the exact prefix: no longer repairable
    before = _bytes_of(context)

    with pytest.raises(ResourceError) as raised:
        _recover(context, service)

    assert raised.value.reason == "record-changed"
    assert _bytes_of(context) == before


# ---- capacity: accepted and ignored ------------------------------------------------------------------------------


def _capacity_spec(root: Path, purpose: str, *, capacity: int = 1, retain: bool = False) -> RunSpec:
    return RunSpec(
        project_root=root,
        entrypoint="integration",
        purpose=purpose,
        services=("postgres",),
        closure=("postgres",),
        profile="fixture",
        phases={"tests": PhaseBudget(0, 1, 1, 1)},
        initial_phase="tests",
        artifact=_identity(),
        wait_seconds=20,
        retain=retain,
        capacity=capacity,
    )


def _capacity_project(tmp_path: Path, name: str = "project") -> Path:
    project = tmp_path / name
    project.mkdir()
    return project


def _record_path(services: _TestServices, run_id: str) -> Path:
    return services.root / "registry" / "runs" / f"{run_id}.json"


def _record_context(services: _TestServices, project: Path, run_id: str) -> lifecycle.RunContext:
    path = _record_path(services, run_id)
    record = json.loads(path.read_text())
    return lifecycle.RunContext(
        Context(project.resolve(), services.root / "registry"),
        run_id,
        record["session_id"],
        path,
        Path(record["private_root"]),
    )


def test_capacity_one_record_has_todays_field_set(tmp_path: Path) -> None:
    services = build_test_services(tmp_path / "state")
    handle = services.open_run(_spec(_capacity_project(tmp_path), "cap-one"))
    handle.enter()
    handle.leave()

    record = json.loads(_record_path(services, handle.run_id).read_text())

    assert set(record) == set(lifecycle._RUN_FIELDS) | {"hold_timeline"}  # no capacity key; the grant adds the timeline


def _prepared_record(tmp_path: Path):
    services = build_test_services(tmp_path / "state")
    project = _capacity_project(tmp_path)
    handle = services.open_run(_spec(project, "evidence"))
    path = _record_path(services, handle.run_id)
    return handle, path, _record_context(services, project, handle.run_id)


@pytest.mark.parametrize("name", ["admission-expired"])
def test_run_record_loads_and_retains_every_failure_evidence_shape(tmp_path: Path, name: str) -> None:
    handle, path, context = _prepared_record(tmp_path)
    record = json.loads(path.read_text())
    record["failure_evidence"] = WRITER_ROWS[name]
    atomic_json(path, record)  # a valid shape loads whatever infrastructure_failure holds, including None

    loaded = lifecycle.load_run_record(context)

    assert loaded["failure_evidence"] == WRITER_ROWS[name]
    assert loaded["infrastructure_failure"] is None
    handle.cancel()


@pytest.mark.parametrize(
    "evidence",
    (
        {**WRITER_ROWS["observation-late"], "waiting": [], "pressure": None},
        None,
    ),
    ids=("waiting-on-wrong-category", "null"),
)
def test_run_record_refuses_malformed_failure_evidence(tmp_path: Path, evidence: object) -> None:
    handle, path, context = _prepared_record(tmp_path)
    record = json.loads(path.read_text())
    record["failure_evidence"] = evidence
    atomic_json(path, record)

    with pytest.raises(ResourceError, match="failure evidence is invalid"):
        lifecycle.load_run_record(context)
    record.pop("failure_evidence")
    atomic_json(path, record)
    handle.cancel()


def test_run_record_without_failure_evidence_is_never_injected_with_the_key(tmp_path: Path) -> None:
    handle, path, context = _prepared_record(tmp_path)

    assert "failure_evidence" not in lifecycle.load_run_record(context)
    assert "failure_evidence" not in json.loads(path.read_text())
    handle.cancel()


def test_retained_reopen_ignores_the_capacity(tmp_path: Path) -> None:
    services = build_test_services(tmp_path / "state")
    project = _capacity_project(tmp_path)
    spec = _capacity_spec(project, "retained-cap", retain=True, capacity=2)
    handle = services.open_run(spec).enter()
    assert handle.retain().state == "stopped-retained"
    locator = tmp_path / "locator.json"
    handle.write_retained_locator(locator)

    reopened = services.open_retained_run(_capacity_spec(project, "retained-cap", retain=True, capacity=1), locator)

    assert reopened.run_id == handle.run_id
    reopened.release()


def test_bridge_accepts_and_ignores_a_capacity(tmp_path: Path) -> None:
    from functools import partial

    from docker_test_toolkit.application.operator import LeaseOperator

    artifact = source_artifact()
    service = private_account(tmp_path)
    project = tmp_path / "bridge-project"
    project.mkdir()
    payload = {
        "project_root": str(project),
        "entrypoint": "integration",
        "purpose": "bridge-cap2",
        "services": [],
        "closure": [],
        "profile": "fixture",
        "phases": {"tests": {"container_bytes": 0, "host_process_bytes": 1, "daemon_cpus": 1, "host_threads": 1}},
        "initial_phase": "tests",
        "artifact": {"version": artifact.version, "sha256": artifact.sha256, "protocols": dict(artifact.protocols)},
        "external_resources": [],
        "retain": False,
        "wait_seconds": 60,
        "capacity": 2,
    }
    bridge = LeaseOperator(partial(open_run, _test_state=service.state_directory), lambda: pytest.fail("no Docker"))

    prepared = bridge.execute("run.prepare", payload)
    (record_file,) = service.state_directory.rglob(f"runs/{prepared['run_id']}.json")
    record = json.loads(record_file.read_text())
    bridge.execute("run.enter", {"docker": False})
    phases = {entry.phase for entry in queue_entries(service.account_directory).values()}
    bridge.execute("run.leave", {"outcome": "passed"})

    assert "capacity" not in record and phases == {"granted"}


# ---- the two lifecycle readers of the queue go through the codec -----------------------------------


def _real_entry(context: lifecycle.RunContext, ticket: int, phase: str, **fields: object) -> tuple[str, dict]:
    """Return a queue entry for a prepared run, valid under the codec (its record path names the run)."""
    registry = str(context.project.state)
    root = str(context.project.root)
    holder = {"pid": ticket, "origin": "test", "birth": [ticket], "audit_token": None} if phase != "queued" else None
    entry = {
        "ticket": ticket,
        "attempt": f"{ticket:08d}-1111-4111-8111-111111111111",
        "record": f"{registry}/runs/{context.run_id}.json",
        "registry": registry,
        "project_root": root,
        "project_id": context.project.project_id,
        "phase": phase,
        "holder": holder,
        "dead_since": None,
        "takeover": None,
        "last_completed_phase": None,
        **fields,
    }
    return context.run_id, entry


def _prepared(tmp_path: Path) -> tuple[_TestServices, lifecycle.RunContext]:
    services = build_test_services(tmp_path / "state")
    context = lifecycle.prepare_run(
        lifecycle.RunRequest(
            project_root=_capacity_project(tmp_path),
            entrypoint="integration",
            purpose="reader",
            services=(),
            closure=(),
            profile="fixture",
        ),
        _test_state=services.root / "registry",
        _test_directory=services.root / "admission",
    )
    return services, context


def test_recovery_phase_reader_follows_the_codec(tmp_path: Path) -> None:
    services, context = _prepared(tmp_path)
    account = services.root / "admission"
    own = _real_entry(context, 1, "queued", last_completed_phase="claim")
    write_v2_queue(account, [own, queue_entry(2, 2, "granted")])  # one holder: valid

    assert lifecycle._last_recovery_phase(context, account) == "claim"

    write_v2_queue(account, [own, queue_entry(2, 2, "granted"), queue_entry(3, 3, "admitting")])  # two: still valid
    assert lifecycle._last_recovery_phase(context, account) == "claim"

    write_v2_queue(  # a duplicate ticket: the codec refuses, so the stored phase contributes nothing
        account, [own, queue_entry(2, 2, "granted"), queue_entry(3, 2, "admitting")]
    )
    assert lifecycle._last_recovery_phase(context, account) == "none"


def test_own_attempt_reader_follows_the_codec(tmp_path: Path) -> None:
    services, context = _prepared(tmp_path)
    account = services.root / "admission"
    attempt = "66666666-6666-4666-8666-666666666666"
    takeover = {
        "attempt": attempt,
        "deadline": asdict(lifecycle.Deadline.after(lifecycle.NativeClock(), 60)),
        "claimant": None,
    }
    own = _real_entry(context, 1, "queued", takeover=takeover)
    write_v2_queue(account, [own, queue_entry(2, 2, "granted")])

    lifecycle._require_own_attempt(context, attempt, account)  # an unbound, unexpired claim of this worker's attempt

    write_v2_queue(account, [own, queue_entry(2, 2, "granted"), queue_entry(3, 3, "admitting")])  # two: still valid
    lifecycle._require_own_attempt(context, attempt, account)

    write_v2_queue(account, [own, queue_entry(2, 2, "granted"), queue_entry(3, 2, "admitting")])  # duplicate ticket
    with pytest.raises(ResourceError) as refused:
        lifecycle._require_own_attempt(context, attempt, account)
    assert refused.value.reason == "claim-held"
