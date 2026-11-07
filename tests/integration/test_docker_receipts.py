from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
from dataclasses import asdict
from pathlib import Path

import pytest

from docker_test_toolkit import ArtifactIdentity, ExternalDockerResource, PhaseBudget, RunSpec
from docker_test_toolkit.adapters.outbound.docker import RELAY_STREAM_BYTES, DockerCliAdapter
from docker_test_toolkit.application import lifecycle
from docker_test_toolkit.application.api import RunHandle
from docker_test_toolkit.application.recovery import Context, atomic_json, load, lock
from docker_test_toolkit.application.supervision import private_process_environment
from docker_test_toolkit.domain.errors import IncompleteCleanup, InfrastructureFailure, ResourceBusy, ResourceError
from docker_test_toolkit.domain.records import validate_docker_state
from docker_test_toolkit.factories.testing import build_test_services, install_failpoint
from tests.integration.account_fixtures import (
    Interrupted,
    authority_bytes,
    dead_identity,
    distinct_process_identity,
    kill_guardian,
    queue_phases,
    reap,
    retire_actor,
    start_live_actor,
    wait_guardians_exited,
    wait_until,
)
from tests.integration.test_native_lifecycle import _acknowledged_but_unretired
from tests.unit.test_failure_evidence import WRITER_ROWS

pytestmark = pytest.mark.integration
IMAGE_ID = "sha256:" + "f" * 64


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


def _fake_docker(path: Path) -> Path:
    docker = path / "docker"
    docker.write_text(
        f"""#!/bin/sh
set -eu
case "${{DOCKER_CONTEXT:-}}|$*" in
  "|context show") printf '%s\\n' fixture ;;
  "fixture|version --format {{{{.Client.Version}}}}") printf '%s\\n' 28.4.0 ;;
  "fixture|version --format {{{{.Client.Version}}}} {{{{json .Server.Components}}}}") printf '%s\\n' '28.4.0 null' ;;
  "fixture|info --format {{{{.ID}}}}") printf '%s\\n' 11111111-1111-1111-1111-111111111111 ;;
  "fixture|image inspect "*) printf '%s\\n' '[{{"Id":"{IMAGE_ID}"}}]' ;;
  *) exit 71 ;;
esac
"""
    )
    docker.chmod(0o700)
    return docker


def _stateful_fake_docker(path: Path) -> Path:
    state = path / "container.json"
    network_state = path / "network.json"
    mode_file = path / "mode"
    records = path.parent / "state/registry"
    docker = path / "docker"
    docker.write_text(
        f"""#!{sys.executable}
import json
import fcntl
import pathlib
import os
import sys
import time

state = pathlib.Path({str(state)!r})
network_state = pathlib.Path({str(network_state)!r})
mode_file = pathlib.Path({str(mode_file)!r})
records = pathlib.Path({str(records)!r})
args = sys.argv[1:]
mode = mode_file.read_text() if mode_file.exists() else ""
if args == ["context", "show"]:
    print("fixture")
elif os.environ.get("DOCKER_CONTEXT") == "fixture" and args == ["version", "--format", "{{{{.Client.Version}}}}"]:
    print("28.4.0")
elif os.environ.get("DOCKER_CONTEXT") == "fixture" and args == [
    "version", "--format", "{{{{.Client.Version}}}} {{{{json .Server.Components}}}}"
]:
    print("28.4.0 null")
elif os.environ.get("DOCKER_CONTEXT") == "fixture" and args == ["info", "--format", "{{{{.ID}}}}"]:
    print("11111111-1111-1111-1111-111111111111")
else:
    assert os.environ.get("DOCKER_CONTEXT") == "fixture"
    command = args
    if command[:2] == ["image", "inspect"]:
        print(json.dumps([{{"Id": {IMAGE_ID!r}}}]))
    elif command[0] == "run":
        record_path, = records.joinpath("runs").glob("*.json")
        record = json.loads(record_path.read_text())
        receipts = list(record["docker_receipts"].values())
        assert receipts and receipts[-1]["state"] == "pending"
        lock_path = records / ("run-" + record["run_id"] + ".lock")
        with lock_path.open("r+") as lock:
            # The guardian holds the run lock briefly every second, so probe for a bounded time instead of once.
            # A wrapper that held the lock across this child still fails here: it waits on the child.
            deadline = time.monotonic() + 2
            while True:
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        raise
                    time.sleep(0.01)
            fcntl.flock(lock, fcntl.LOCK_UN)
        if mode == "child-fail":
            raise SystemExit(42)
        cid = "a" * 64
        cidfile = pathlib.Path(command[command.index("--cidfile") + 1])
        name = command[command.index("--name") + 1]
        labels = {{}}
        for index, value in enumerate(command):
            if value == "--label":
                key, label_value = command[index + 1].split("=", 1)
                labels[key] = label_value
        if mode != "missing-cid":
            cidfile.write_text(cid + "\\n")
        if mode not in {{"fast-rm", "fast-rm-fail"}}:
            state.write_text(json.dumps({{
                "Id": cid,
                "Name": "/" + name,
                "Created": "2026-09-16T00:00:00Z",
            "Image": {IMAGE_ID!r},
                "Config": {{"Labels": labels}},
                "State": {{"Running": False}},
                "HostConfig": {{
                    "Memory": 268435456, "NanoCpus": 500000000, "Privileged": False, "NetworkMode": "none",
                    "PidMode": "", "IpcMode": "private", "UTSMode": "", "UsernsMode": "", "CgroupnsMode": "private",
                    "CapAdd": [], "Devices": [], "DeviceRequests": [], "CgroupParent": "", "PortBindings": {{}},
                    "SecurityOpt": [],
                }},
                "Mounts": [],
                "NetworkSettings": {{"Networks": {{"none": {{"NetworkID": "0" * 64}}}}}},
            }}))
        else:
            cidfile.unlink()
            if mode == "fast-rm-fail":
                raise SystemExit(42)
    elif command[:2] == ["container", "inspect"]:
        if not state.exists():
            print("[]")
            print(f"Error response from daemon: No such container: {{command[-1]}}", file=sys.stderr)
            raise SystemExit(1)
        print(json.dumps([json.loads(state.read_text())]))
    elif command[:2] == ["container", "rm"]:
        if mode == "block-rm":
            # A removal that never finishes: say so, then wait until the recoverer that started it is gone.
            pathlib.Path({str(path / "in-removal")!r}).write_text(command[-1])
            parent = os.getppid()
            while os.getppid() == parent:
                time.sleep(0.05)
            raise SystemExit(1)
        if state.exists():
            state.unlink()
        print(command[-1])
    elif command[:2] == ["network", "create"]:
        name = command[-1]
        labels = {{}}
        for index, value in enumerate(command):
            if value == "--label":
                key, label_value = command[index + 1].split("=", 1)
                labels[key] = label_value
        if mode == "stderr-invalid":
            sys.stderr.buffer.write(b"\\xffboom")
            raise SystemExit(1)
        if mode == "stderr-huge":
            sys.stderr.buffer.write(b"x" * (1024 * 1024 + 1))
            raise SystemExit(1)
        network_state.write_text(json.dumps({{
            "Id": "b" * 64, "Name": name, "Created": "2026-09-16T00:00:00Z", "Labels": labels,
        }}))
        if mode == "both-huge":
            sys.stdout.buffer.write(b"\\xff" * (2 * 1024 * 1024))
            sys.stderr.buffer.write(b"\\xff" * (2 * 1024 * 1024))
        else:
            print("b" * 64)
    elif command[:2] == ["network", "inspect"]:
        if mode == "inspect-invalid":
            sys.stdout.buffer.write(b"\\xff")
            raise SystemExit(0)
        if not network_state.exists():
            print("[]")
            print(f"Error response from daemon: network {{command[-1]}} not found", file=sys.stderr)
            raise SystemExit(1)
        print(json.dumps([json.loads(network_state.read_text())]))
    elif command[:2] == ["network", "rm"]:
        if network_state.exists():
            network_state.unlink()
        print(command[-1])
    else:
        raise SystemExit(71)
"""
    )
    docker.chmod(0o700)
    return docker


def _configure_fake_docker(path: Path):
    def _environment() -> dict[str, str]:
        environment = private_process_environment(
            private_additions={"DOCKER_CONTEXT": "fixture"},
        )
        environment["PATH"] = f"{path}{os.pathsep}{environment['PATH']}"
        return environment

    def _build():
        return DockerCliAdapter.discover(environment=_environment())

    def _bind(identity):
        return DockerCliAdapter.from_identity(identity, environment=_environment())

    lifecycle.configure_docker_factories(_build, _bind)
    return _build


def _spec(project: Path, *, retain: bool = False) -> RunSpec:
    return RunSpec(
        project_root=project,
        entrypoint="integration",
        purpose="docker-receipts",
        services=("fixture",),
        closure=("fixture",),
        profile="fixture",
        phases={"tests": PhaseBudget(0, 1, 1, 1)},
        initial_phase="tests",
        artifact=_identity(),
        external_resources=(ExternalDockerResource("image", IMAGE_ID, "fixture", "use_image"),),
        wait_seconds=20,
        retain=retain,
    )


def _retained_owned_container(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[object, lifecycle.RunContext, Path, Path]:
    project = tmp_path / "project"
    project.mkdir()
    binary_directory = tmp_path / "bin"
    binary_directory.mkdir()
    _stateful_fake_docker(binary_directory)
    monkeypatch.setenv("PATH", f"{binary_directory}{os.pathsep}{os.environ['PATH']}")
    docker_factory = _configure_fake_docker(binary_directory)
    handle = (
        build_test_services(tmp_path / "state")
        .open_run(_spec(project, retain=True), docker_factory=docker_factory)
        .enter()
    )
    docker_context = handle.docker_context
    created = subprocess.run(
        [docker_context.executable, "run", IMAGE_ID, "true"],
        env=dict(docker_context.environment),
        capture_output=True,
        text=True,
        check=False,
    )
    assert created.returncode == 0, created.stderr
    assert handle.retain().state == "stopped-retained"
    context = lifecycle.RunContext(
        Context(project.resolve(), tmp_path / "state/registry"),
        handle.run_id,
        handle.session_id,
        tmp_path / "state/registry/runs" / f"{handle.run_id}.json",
        handle.private_root,
    )
    return handle, context, binary_directory / "container.json", tmp_path / "state/admission"


def _interrupt_retained_release(
    handle: RunHandle,
    context: lifecycle.RunContext,
    monkeypatch: pytest.MonkeyPatch,
    *,
    cleanup_first: bool = False,
) -> dict:
    cleanup = lifecycle._cleanup_owned_docker_resources

    def fail_release(target: lifecycle.RunContext, deadline_ns: int | None = None) -> None:
        if cleanup_first:
            cleanup(target, deadline_ns)
        raise InfrastructureFailure("fixture release failure")

    with monkeypatch.context() as patcher:
        patcher.setattr(lifecycle, "_cleanup_owned_docker_resources", fail_release)
        with pytest.raises(InfrastructureFailure, match="fixture release failure"):
            handle.release()
    # The release run's guardian de-indexes it and exits after its leave returns; recovery reads both.
    wait_guardians_exited(context.project.state)
    return json.loads(context.record.read_text())


def test_entered_handle_materializes_private_exact_docker_context(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    binary_directory = tmp_path / "bin"
    binary_directory.mkdir()
    real = _fake_docker(binary_directory)
    del monkeypatch
    environment = private_process_environment()

    def docker_factory():
        return DockerCliAdapter.discover(real, environment=environment)

    handle = (
        build_test_services(tmp_path / "state")
        .open_run(_spec(project), docker_factory=docker_factory, admit_docker=True)
        .enter()
    )

    context = handle.docker_context
    descriptor = json.loads((handle.private_root / "docker-authority.json").read_text())
    record = json.loads((tmp_path / "state/registry/runs" / f"{handle.run_id}.json").read_text())

    assert context.executable == handle.private_root / "docker"
    assert context.environment["PATH"].split(os.pathsep)[0] == str(handle.private_root)
    assert set(context.environment) == {"HOME", "LANG", "LC_ALL", "PATH", "TEMP", "TMP", "TMPDIR"}
    assert descriptor["authority"]["client_path"] == str(real.resolve())
    assert descriptor["generation"] == handle.generation
    assert record["docker_authority"]["descriptor_path"] == str(handle.private_root / "docker-authority.json")
    assert record["docker_receipts"] == {}
    assert not any(record["owned_resources"].values())

    handle.leave()


def test_private_shim_journals_before_promoting_and_removing_exact_container(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    binary_directory = tmp_path / "bin"
    binary_directory.mkdir()
    _stateful_fake_docker(binary_directory)
    monkeypatch.setenv("PATH", f"{binary_directory}{os.pathsep}{os.environ['PATH']}")
    handle = (
        build_test_services(tmp_path / "state")
        .open_run(_spec(project), docker_factory=_configure_fake_docker(binary_directory))
        .enter()
    )
    context = handle.docker_context

    created = subprocess.run(
        [context.executable, "run", IMAGE_ID, "true"],
        env=dict(context.environment),
        capture_output=True,
        text=True,
        check=False,
    )
    record_path = tmp_path / "state/registry/runs" / f"{handle.run_id}.json"
    record = json.loads(record_path.read_text())
    container_id = "a" * 64

    assert created.returncode == 0, created.stderr
    assert list(record["docker_receipts"].values())[-1]["state"] == "promoted"
    assert record["owned_resources"]["containers"][container_id]["state"] == "stopped"

    removed = subprocess.run(
        [context.executable, "container", "rm", container_id],
        env=dict(context.environment),
        capture_output=True,
        text=True,
        check=False,
    )
    record = json.loads(record_path.read_text())

    assert removed.returncode == 0, removed.stderr
    assert record["owned_resources"]["containers"][container_id]["state"] == "removed"
    assert list(record["docker_receipts"].values())[-1]["state"] == "removed"
    handle.leave()


def _entered_shim(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    project = tmp_path / "project"
    project.mkdir()
    binary_directory = tmp_path / "bin"
    binary_directory.mkdir()
    _stateful_fake_docker(binary_directory)
    monkeypatch.setenv("PATH", f"{binary_directory}{os.pathsep}{os.environ['PATH']}")
    handle = (
        build_test_services(tmp_path / "state")
        .open_run(_spec(project), docker_factory=_configure_fake_docker(binary_directory))
        .enter()
    )
    return handle, tmp_path / "state/registry/runs" / f"{handle.run_id}.json", binary_directory / "mode"


def _shim(handle, *argv: str) -> subprocess.CompletedProcess[bytes]:
    context = handle.docker_context
    return subprocess.run([context.executable, *argv], env=dict(context.environment), capture_output=True, check=False)


def _last_receipt(record_path: Path) -> dict:
    return list(json.loads(record_path.read_text())["docker_receipts"].values())[-1]


def test_shim_relays_a_failing_childs_invalid_bytes_and_finishes_the_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    handle, record_path, mode_file = _entered_shim(tmp_path, monkeypatch)
    mode_file.write_text("stderr-invalid")

    created = _shim(handle, "network", "create")

    receipt = _last_receipt(record_path)
    assert created.returncode == 1
    assert created.stderr.decode().endswith("\n\ufffdboom")  # after the interpreter's own warning lines
    assert (receipt["state"], receipt["result"]) == ("failed", {"category": "child_failure", "exit_code": 1})
    handle.leave()


def test_shim_relays_an_over_limit_failing_child_with_a_marker(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    handle, record_path, mode_file = _entered_shim(tmp_path, monkeypatch)
    mode_file.write_text("stderr-huge")

    created = _shim(handle, "network", "create")

    receipt = _last_receipt(record_path)
    assert created.returncode == 1
    assert (receipt["state"], receipt["result"]) == ("failed", {"category": "child_failure", "exit_code": 1})
    assert "[docker-test-toolkit: stderr cut from 1048577 bytes" in created.stderr.decode()
    relayed = created.stderr[created.stderr.index(b"xxxx") :]  # the child's own bytes, after interpreter warnings
    assert len(relayed) <= RELAY_STREAM_BYTES + 128
    handle.leave()


def test_shim_relays_an_over_limit_passing_child_and_promotes_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    handle, record_path, mode_file = _entered_shim(tmp_path, monkeypatch)
    mode_file.write_text("both-huge")

    created = _shim(handle, "network", "create")

    record = json.loads(record_path.read_text())
    receipt = list(record["docker_receipts"].values())[-1]
    assert created.returncode == 0, created.stderr[-300:]
    assert (receipt["state"], receipt["result"]) == ("promoted", {"category": "complete", "exit_code": 0})
    assert record["owned_resources"]["networks"]["b" * 64]["state"] == "present"
    assert b"stdout cut from" in created.stdout and b"stderr cut from" in created.stderr
    assert len(created.stdout) + len(created.stderr) < 1024 * 1024
    mode_file.write_text("")
    handle.leave()


def test_shim_observation_output_stays_strict(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    handle, _record_path, mode_file = _entered_shim(tmp_path, monkeypatch)
    assert _shim(handle, "network", "create").returncode == 0
    mode_file.write_text("inspect-invalid")

    observed = _shim(handle, "network", "inspect", "b" * 64)

    assert (observed.returncode, observed.stdout) == (5, b"")
    assert b"not valid UTF-8" in observed.stderr
    mode_file.write_text("")
    handle.leave()


@pytest.mark.parametrize("non_terminal_state", ["pending", "unresolved"])
def test_only_terminal_docker_receipts_may_precede_the_current_generation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, non_terminal_state: str
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    binary_directory = tmp_path / "bin"
    binary_directory.mkdir()
    _stateful_fake_docker(binary_directory)
    monkeypatch.setenv("PATH", f"{binary_directory}{os.pathsep}{os.environ['PATH']}")
    handle = (
        build_test_services(tmp_path / "state")
        .open_run(_spec(project), docker_factory=_configure_fake_docker(binary_directory))
        .enter()
    )
    context = handle.docker_context

    created = subprocess.run(
        [context.executable, "run", IMAGE_ID, "true"],
        env=dict(context.environment),
        capture_output=True,
        text=True,
        check=False,
    )
    assert created.returncode == 0, created.stderr
    record = json.loads((tmp_path / "state/registry/runs" / f"{handle.run_id}.json").read_text())
    receipt = next(iter(record["docker_receipts"].values()))
    record["generation"] = 2
    record["grant"]["generation"] = 2
    record["docker_authority"]["generation"] = 2

    validate_docker_state(record)
    assert receipt["state"] == "promoted"
    receipt["state"] = non_terminal_state
    with pytest.raises(ResourceError, match="receipt is incomplete or contradictory"):
        validate_docker_state(record)

    handle.leave()


def test_leave_journals_and_verifies_exact_owned_cleanup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = tmp_path / "project"
    project.mkdir()
    binary_directory = tmp_path / "bin"
    binary_directory.mkdir()
    _stateful_fake_docker(binary_directory)
    monkeypatch.setenv("PATH", f"{binary_directory}{os.pathsep}{os.environ['PATH']}")
    handle = (
        build_test_services(tmp_path / "state")
        .open_run(_spec(project), docker_factory=_configure_fake_docker(binary_directory))
        .enter()
    )
    context = handle.docker_context
    created = subprocess.run(
        [context.executable, "run", IMAGE_ID, "true"],
        env=dict(context.environment),
        capture_output=True,
        text=True,
        check=False,
    )
    assert created.returncode == 0, created.stderr

    status = handle.leave()
    record = json.loads((tmp_path / "state/registry/runs" / f"{handle.run_id}.json").read_text())

    assert status.cleanup_outcome == "complete"
    assert record["owned_resources"]["containers"]["a" * 64]["state"] == "removed"
    assert list(record["docker_receipts"].values())[-1]["kind"] == "container-rm"
    assert list(record["docker_receipts"].values())[-1]["state"] == "removed"
    assert not (binary_directory / "container.json").exists()


@pytest.mark.parametrize(
    ("with_resource", "retain", "corruption"),
    [
        (False, False, None),
        (True, False, None),
        (True, False, "account"),
        (True, False, "reservation"),
        (True, False, "project"),
        (True, False, "project-retired"),
        (False, False, "project-retired"),
        (True, True, None),
        (True, True, "project"),
        (True, True, "project-retired"),
    ],
)
def test_reconciliation_retires_references_after_terminal_record_persisted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    with_resource: bool,
    retain: bool,
    corruption: str | None,
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    binary_directory = tmp_path / "bin"
    binary_directory.mkdir()
    _stateful_fake_docker(binary_directory)
    monkeypatch.setenv("PATH", f"{binary_directory}{os.pathsep}{os.environ['PATH']}")
    state = tmp_path / "state"
    handle = (
        build_test_services(state)
        .open_run(_spec(project, retain=retain), docker_factory=_configure_fake_docker(binary_directory))
        .enter()
    )
    docker_context = handle.docker_context
    if with_resource:
        created = subprocess.run(
            [docker_context.executable, "run", IMAGE_ID, "true"],
            env=dict(docker_context.environment),
            capture_output=True,
            text=True,
            check=False,
        )
        assert created.returncode == 0, created.stderr
    account = state / "admission"
    project_context = Context(project.resolve(), state / "registry")
    queued = json.loads((account / "queue.json").read_text())
    indexed = load(project_context)
    context = lifecycle.RunContext(
        project_context,
        handle.run_id,
        handle.session_id,
        state / "registry/runs" / f"{handle.run_id}.json",
        handle.private_root,
    )

    status = handle.leave()
    assert status.cleanup_outcome == "complete"
    assert status.state == ("stopped-retained" if retain else "complete")
    wait_guardians_exited(state / "registry")  # recovery refuses while the original guardian still executes
    docker_state = (binary_directory / "container.json").read_bytes() if retain else None
    run_bytes = context.record.read_bytes()
    if corruption == "account":
        # A terminal record whose slot entry was reset to a plain queued entry is a partially retired reference.
        queued["entries"][handle.run_id].update(phase="queued", holder=None)
    elif corruption == "reservation":
        # The queue entry already left (queue saved first) while the project index still names the terminal run:
        # the resumable prefix of the record, queue, index retirement order.
        queued["entries"].pop(handle.run_id)
    elif corruption == "project":
        indexed["runs"][handle.run_id] = str(tmp_path / "substituted.json")
    elif corruption == "project-retired":
        indexed["runs"].pop(handle.run_id)
    atomic_json(account / "queue.json", queued)
    atomic_json(project_context.record, indexed)

    unrecoverable_project_retirement = corruption == "project-retired" and retain
    if corruption in {"account", "project"} or unrecoverable_project_retirement:
        messages = {
            "account": "only partially retired",
            "project": "project reference changed identity",
            "project-retired": "project reference changed identity",
        }
        message = messages[corruption]
        with pytest.raises(ResourceError, match=message):
            lifecycle.recover_run(context, _test_directory=account)
        assert json.loads((account / "queue.json").read_text())["entries"] == queued["entries"]
        assert load(project_context)["runs"] == indexed["runs"]
        assert context.record.read_bytes() == run_bytes
        assert ((binary_directory / "container.json").read_bytes() if retain else None) == docker_state
        return

    lifecycle.recover_run(context, _test_directory=account)

    assert json.loads((account / "queue.json").read_text())["entries"] == {}
    assert load(project_context)["runs"] == (indexed["runs"] if retain else {})
    assert context.record.read_bytes() == run_bytes
    assert ((binary_directory / "container.json").read_bytes() if retain else None) == docker_state

    if retain:
        lifecycle.recover_run(context, _test_directory=account)
        assert load(project_context)["runs"] == indexed["runs"]
        assert context.record.read_bytes() == run_bytes
        assert (binary_directory / "container.json").read_bytes() == docker_state
    else:
        lifecycle.recover_run(context, _test_directory=account)
        atomic_json(project_context.record, indexed)
        lifecycle.recover_run(context, _test_directory=account)
        assert load(project_context)["runs"] == {}
        atomic_json(account / "queue.json", queued)
        lifecycle.recover_run(context, _test_directory=account)
        assert json.loads((account / "queue.json").read_text())["entries"] == {}


@pytest.mark.parametrize("guardian", ["live", "zombie"])
def test_recovery_refuses_a_live_or_zombie_guardian_as_actor_live(tmp_path: Path, guardian: str) -> None:
    """Recovery never retires the references of a run whose original guardian may still run."""
    _services, account, handle = _acknowledged_but_unretired(tmp_path)
    context = handle._context
    helper, identity = start_live_actor(f"{guardian}-guardian")
    try:
        project = context.project
        with lock(project, project.project_id, shared=True), lock(project, f"run-{context.run_id}"):
            record = json.loads(context.record.read_text())
            record["guardian"] = identity  # the acknowledged-but-unretired record now names a helper as its guardian
            record["grant"]["guardian"] = identity
            atomic_json(context.record, record)
        if guardian == "zombie":
            helper.kill()  # exited, never reaped: a zombie still holds its pid
            observed = lifecycle._identity_from_json(identity)
            wait_until(lambda: lifecycle.observe_process(observed) != "executing", message="the helper exited")
            if sys.platform == "darwin":
                # macOS libproc returns no BSD info (ESRCH) for a zombie, so an exited unreaped guardian observes as
                # absent there and recovery may finish; the zombie clause is proven on Linux, where /proc shows "Z".
                assert lifecycle.observe_process(observed) == "absent"
                lifecycle.recover_run(context, _test_directory=account)
                assert queue_phases(account) == {}
                return
            assert lifecycle.observe_process(observed) == "zombie"
        before = authority_bytes(context, account)

        with pytest.raises(ResourceBusy) as refused:
            lifecycle.recover_run(context, _test_directory=account)

        assert refused.value.reason == "actor-live"
        assert authority_bytes(context, account) == before  # nothing written
    finally:
        reap(helper)
    lifecycle.recover_run(context, _test_directory=account)
    assert queue_phases(account) == {}
    assert handle.run_id not in json.loads(context.project.record.read_text())["runs"]


def test_release_requires_docker_composition_before_mutating_retained_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    handle, context, _container, _account = _retained_owned_container(tmp_path, monkeypatch)
    before = context.record.read_bytes()
    monkeypatch.setattr(lifecycle, "_DOCKER_FACTORY", None)

    with pytest.raises(InfrastructureFailure, match="composition is unavailable"):
        handle.release()

    assert context.record.read_bytes() == before


@pytest.mark.parametrize(("legacy", "cleanup_first"), ((False, False), (True, True)))
def test_recovery_finishes_an_exact_interrupted_retained_release(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    legacy: bool,
    cleanup_first: bool,
) -> None:
    handle, context, container, account = _retained_owned_container(tmp_path, monkeypatch)
    stuck = _interrupt_retained_release(handle, context, monkeypatch, cleanup_first=cleanup_first)
    if legacy:
        stuck.pop("release_authority")
        atomic_json(context.record, stuck)
    assert stuck["state"] == "stopping"
    assert stuck["recovery_outcome"] == "pending"
    assert json.loads((account / "queue.json").read_text())["entries"] == {}
    queue_bytes = (account / "queue.json").read_bytes()

    recovered = lifecycle.recover_run(context, _test_directory=account)

    assert recovered.state == "complete"
    assert recovered.cleanup_outcome == "complete"
    assert not container.exists()
    assert (account / "queue.json").read_bytes() == queue_bytes
    assert context.run_id not in load(context.project)["runs"]


def test_release_recovery_does_not_require_a_globally_empty_queue(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    handle, context, container, account = _retained_owned_container(tmp_path, monkeypatch)
    _interrupt_retained_release(handle, context, monkeypatch)
    queue = json.loads((account / "queue.json").read_text())
    foreign_run = "88888888-8888-4888-8888-888888888888"
    foreign_root = tmp_path / "foreign-project"
    queue["entries"][foreign_run] = {
        "attempt": "77777777-7777-4777-8777-777777777777",
        "dead_since": None,
        "holder": None,
        "last_completed_phase": None,
        "phase": "queued",
        "project_id": hashlib.sha256(str(foreign_root).encode()).hexdigest(),
        "project_root": str(foreign_root),
        "record": f"/foreign/registry/runs/{foreign_run}.json",
        "registry": "/foreign/registry",
        "takeover": None,
        "ticket": queue["next_ticket"],
    }
    queue["next_ticket"] += 1
    atomic_json(account / "queue.json", queue)
    queue_bytes = (account / "queue.json").read_bytes()

    recovered = lifecycle.recover_run(context, _test_directory=account)

    assert recovered.state == "complete"  # another project's waiting entry no longer blocks this release recovery
    assert not container.exists()
    assert (account / "queue.json").read_bytes() == queue_bytes  # and recovery leaves the foreign entry untouched


def test_recovery_rejects_a_substituted_retained_release_authority(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    handle, context, container, account = _retained_owned_container(tmp_path, monkeypatch)
    stuck = _interrupt_retained_release(handle, context, monkeypatch)
    substituted = "99999999-9999-4999-8999-999999999999"
    stuck["release_authority"].update(
        run_id=substituted,
        record=str(context.project.state / "runs" / f"{substituted}.json"),
    )
    atomic_json(context.record, stuck)
    before = context.record.read_bytes()

    with pytest.raises(ResourceError, match="missing or unreadable"):
        lifecycle.recover_run(context, _test_directory=account)

    assert context.record.read_bytes() == before
    assert container.exists()


def test_recovery_rejects_non_exact_terminal_release_evidence_without_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    handle, context, container, account = _retained_owned_container(tmp_path, monkeypatch)
    stuck = _interrupt_retained_release(handle, context, monkeypatch)
    release_path = Path(stuck["release_authority"]["record"])
    original = json.loads(release_path.read_text())
    before = context.record.read_bytes()
    owned_resources = json.loads(json.dumps(original["owned_resources"]))
    owned_resources["containers"]["b" * 64] = {
        "evidence": {},
        "identifier": "b" * 64,
        "kind": "container",
        "name": "release-fixture",
        "receipt_id": "77777777-7777-4777-8777-777777777777",
        "state": "removed",
    }

    for field, value in (
        ("project_id", "0" * 64),
        ("session_id", "77777777-7777-4777-8777-777777777777"),
        ("account_directory", str(account / "foreign")),
        ("source_identity", "c" * 64),
        ("topology", "native" if original["topology"] != "native" else "desktop"),
        ("purpose", "other-release"),
        ("test_outcome", "passed"),
        ("cleanup_outcome", "incomplete"),
        ("owned_resources", owned_resources),
    ):
        atomic_json(release_path, {**original, field: value})
        with pytest.raises(ResourceError):
            lifecycle.recover_run(context, _test_directory=account)
        assert context.record.read_bytes() == before, field
        assert container.exists(), field
    atomic_json(release_path, original)

    release_root = Path(original["private_root"])
    release_root.mkdir(mode=0o700, parents=True)
    with pytest.raises(ResourceError, match="absent or ambiguous"):
        lifecycle.recover_run(context, _test_directory=account)
    assert context.record.read_bytes() == before
    shutil.rmtree(release_root)

    registry = load(context.project)
    registry["runs"][original["run_id"]] = str(release_path)
    atomic_json(context.project.record, registry)
    with pytest.raises(ResourceError, match="not terminal in its project index"):
        lifecycle.recover_run(context, _test_directory=account)
    assert context.record.read_bytes() == before
    assert container.exists()


def test_legacy_release_recovery_rejects_duplicate_matching_authority(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    handle, context, container, account = _retained_owned_container(tmp_path, monkeypatch)
    stuck = _interrupt_retained_release(handle, context, monkeypatch)
    release_path = Path(stuck.pop("release_authority")["record"])
    release = json.loads(release_path.read_text())
    duplicate_id = "88888888-8888-4888-8888-888888888888"
    release.update(
        run_id=duplicate_id,
        private_root=str(Path(release["private_root"]).with_name(duplicate_id)),
    )
    atomic_json(release_path.with_name(f"{duplicate_id}.json"), release)
    atomic_json(context.record, stuck)
    before = context.record.read_bytes()

    with pytest.raises(ResourceError, match="absent or ambiguous"):
        lifecycle.recover_run(context, _test_directory=account)

    assert context.record.read_bytes() == before
    assert container.exists()


@pytest.mark.parametrize("legacy", (False, True))
def test_interrupted_release_recovery_can_retry_after_its_own_cleanup_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, legacy: bool
) -> None:
    handle, context, container, account = _retained_owned_container(tmp_path, monkeypatch)
    stuck = _interrupt_retained_release(handle, context, monkeypatch)
    if legacy:
        stuck.pop("release_authority")
        atomic_json(context.record, stuck)
    first_actor = distinct_process_identity(10_000)
    second_actor = distinct_process_identity(20_000)
    # The interrupted release was this process; recovery by another actor needs the releaser gone.
    me = asdict(lifecycle.current_process(lifecycle.NativeClock()))
    retire_actor(context.project.state, me, dead_identity())

    def fail_recovery(_context: lifecycle.RunContext, _deadline_ns: int | None = None) -> None:
        raise InfrastructureFailure("fixture recovery failure")

    with monkeypatch.context() as patcher:
        patcher.setattr(lifecycle, "current_process", lambda _clock: first_actor)
        patcher.setattr(lifecycle, "_cleanup_owned_docker_resources", fail_recovery)
        with pytest.raises(InfrastructureFailure, match="fixture recovery failure"):
            lifecycle.recover_run(context, _test_directory=account)

    interrupted = json.loads(context.record.read_text())
    assert interrupted["recovery_actor"] == asdict(first_actor)

    with monkeypatch.context() as patcher:
        patcher.setattr(lifecycle, "current_process", lambda _clock: second_actor)
        recovered = lifecycle.recover_run(context, _test_directory=account)

    assert recovered.state == "complete"
    assert not container.exists()
    assert json.loads(context.record.read_text())["release_authority"] is None


def test_completed_release_recovery_retires_its_project_index_after_a_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    handle, context, _container, account = _retained_owned_container(tmp_path, monkeypatch)
    _interrupt_retained_release(handle, context, monkeypatch)
    save = lifecycle.save

    def fail_project_retirement(target: Context, record: dict) -> None:
        if target == context.project and context.run_id not in record["runs"]:
            raise InfrastructureFailure("fixture project retirement failure")
        save(target, record)

    with monkeypatch.context() as patcher:
        patcher.setattr(lifecycle, "save", fail_project_retirement)
        with pytest.raises(InfrastructureFailure, match="fixture project retirement failure"):
            lifecycle.recover_run(context, _test_directory=account)

    assert json.loads(context.record.read_text())["state"] == "complete"
    assert context.run_id in load(context.project)["runs"]

    assert lifecycle.recover_run(context, _test_directory=account).state == "complete"
    assert context.run_id not in load(context.project)["runs"]
    assert lifecycle.recover_run(context, _test_directory=account).state == "complete"


def test_interrupted_release_recovery_retries_after_private_root_deletion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    handle, context, container, account = _retained_owned_container(tmp_path, monkeypatch)
    _interrupt_retained_release(handle, context, monkeypatch)
    write = lifecycle.atomic_json

    def fail_terminal_write(path: Path, record: dict) -> None:
        if path == context.record and record["state"] == "complete":
            raise InfrastructureFailure("fixture terminal write failure")
        write(path, record)

    with monkeypatch.context() as patcher:
        patcher.setattr(lifecycle, "atomic_json", fail_terminal_write)
        with pytest.raises(InfrastructureFailure, match="fixture terminal write failure"):
            lifecycle.recover_run(context, _test_directory=account)

    assert not container.exists()
    assert not context.private_root.exists() and not context.private_root.parent.parent.exists()
    assert json.loads(context.record.read_text())["state"] == "stopping"
    assert lifecycle.recover_run(context, _test_directory=account).state == "complete"


def test_interrupted_release_recovery_rejects_a_reappearing_account_grant(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    handle, context, container, account = _retained_owned_container(tmp_path, monkeypatch)
    stuck = _interrupt_retained_release(handle, context, monkeypatch)
    queue = json.loads((account / "queue.json").read_text())
    queue["entries"][context.run_id] = {
        "attempt": stuck["guardian_attempt"],
        "dead_since": None,
        "holder": stuck["guardian"],
        "last_completed_phase": None,
        "phase": "admitting",
        "project_id": context.project.project_id,
        "project_root": str(context.project.root),
        "record": str(context.record),
        "registry": str(context.project.state),
        "takeover": None,
        "ticket": stuck["ticket"],
    }
    queue["next_ticket"] = max(queue["next_ticket"], stuck["ticket"] + 1)
    atomic_json(account / "queue.json", queue)
    before = context.record.read_bytes()

    with pytest.raises(ResourceError, match="Release authority changed"):
        lifecycle.recover_run(context, _test_directory=account)

    assert context.record.read_bytes() == before
    assert container.exists()


def test_prior_resume_snapshot_missing_only_release_authority_remains_readable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _handle, context, _container, _account = _retained_owned_container(tmp_path, monkeypatch)
    retained = json.loads(context.record.read_text())
    owned_container = next(iter(retained["owned_resources"]["containers"].values()))
    owned_container["evidence"].update(service="fixture", completed_service=False)
    atomic_json(context.record, retained)
    requester = distinct_process_identity(30_000)

    with monkeypatch.context() as patcher:
        patcher.setattr(lifecycle, "observe_process", lambda _identity: "exited")
        patcher.setattr(lifecycle, "current_process", lambda _clock: requester)
        patcher.setattr(lifecycle, "source_identity", lambda: retained["source_identity"])
        assert lifecycle.prepare_resume(context, _test_directory=tmp_path / "state/admission").state == "prepared"

    prior = json.loads(context.record.read_text())
    prior.pop("release_authority")
    prior["resume_origin"].pop("release_authority")
    atomic_json(context.record, prior)

    assert lifecycle.load_run_record(context)["state"] == "prepared"


def test_admission_resume_revoke_restores_the_record_without_failure_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A resumed admission that ends (expired or a classified fault) returns the retained record, key removed."""
    _handle, context, _container, account = _retained_owned_container(tmp_path, monkeypatch)
    retained = json.loads(context.record.read_text())
    assert "failure_evidence" not in retained  # an old-shape retained record loads and is resumable as before
    owned_container = next(iter(retained["owned_resources"]["containers"].values()))
    owned_container["evidence"].update(service="fixture", completed_service=False)
    memory = retained["admission_observation"]["memory"]
    retained["hold_timeline"] = {  # the retained generation's own timeline
        "schema": 1,
        "origin": memory["origin"],
        "accounting_from_ns": memory["tick_ns"],
        "reserved_ns": memory["tick_ns"],
        "granted_ns": memory["tick_ns"],
        "waiting": [],
        "release": "leave",
        "release_origin": memory["origin"],
        "released_ns": memory["tick_ns"],
    }
    atomic_json(context.record, retained)
    lifecycle.load_run_record(context)  # the retained record with its timeline loads
    requester = distinct_process_identity(30_000)

    with monkeypatch.context() as patcher:
        patcher.setattr(lifecycle, "observe_process", lambda _identity: "exited")
        patcher.setattr(lifecycle, "current_process", lambda _clock: requester)
        patcher.setattr(lifecycle, "source_identity", lambda: retained["source_identity"])
        assert lifecycle.prepare_resume(context, _test_directory=tmp_path / "state/admission").state == "prepared"
        written = json.loads(context.record.read_text())
        assert "hold_timeline" not in written  # the resumed generation writes its own at its own grant
        written["failure_evidence"] = WRITER_ROWS["unreadable"]  # as a revoke attempt of this generation
        atomic_json(context.record, written)

        restored = lifecycle.revoke_ungranted(
            context,
            _test_directory=account,
            _failure_reason="telemetry-unreadable",
            _admission_evidence=lambda _data: WRITER_ROWS["unreadable"],
        )

        record = lifecycle.load_run_record(context)
        assert restored.state == "stopped-retained"
        assert "failure_evidence" not in record and record["infrastructure_failure"] is None
        assert "hold_timeline" not in record  # the rollback never restores the dropped timeline
        assert {key: record[key] for key in retained if key not in ("revision", "hold_timeline")} == {
            key: value for key, value in retained.items() if key not in ("revision", "hold_timeline")
        }
        assert (
            lifecycle.prepare_resume(context, _test_directory=tmp_path / "state/admission").state == "prepared"
        )  # and the run is resumable again


def test_stopped_retained_run_carrying_failure_evidence_cannot_resume(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _handle, context, _container, _account = _retained_owned_container(tmp_path, monkeypatch)
    retained = json.loads(context.record.read_text())
    retained["failure_evidence"] = WRITER_ROWS["admission-expired"]
    atomic_json(context.record, retained)
    requester = distinct_process_identity(30_000)
    before = context.record.read_bytes()

    with monkeypatch.context() as patcher:
        patcher.setattr(lifecycle, "observe_process", lambda _identity: "exited")
        patcher.setattr(lifecycle, "current_process", lambda _clock: requester)
        patcher.setattr(lifecycle, "source_identity", lambda: retained["source_identity"])
        with pytest.raises(ResourceError, match="Only one healthy exact stopped-retained run may resume"):
            lifecycle.prepare_resume(context, _test_directory=tmp_path / "state/admission")

    assert context.record.read_bytes() == before


def test_matching_foreign_container_without_cidfile_never_becomes_owned(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    binary_directory = tmp_path / "bin"
    binary_directory.mkdir()
    _stateful_fake_docker(binary_directory)
    monkeypatch.setenv("PATH", f"{binary_directory}{os.pathsep}{os.environ['PATH']}")
    (binary_directory / "mode").write_text("missing-cid")
    handle = (
        build_test_services(tmp_path / "state")
        .open_run(_spec(project), docker_factory=_configure_fake_docker(binary_directory))
        .enter()
    )
    context = handle.docker_context

    created = subprocess.run(
        [context.executable, "run", "--rm", IMAGE_ID, "true"],
        env=dict(context.environment),
        capture_output=True,
        text=True,
        check=False,
    )
    record_path = tmp_path / "state/registry/runs" / f"{handle.run_id}.json"
    record = json.loads(record_path.read_text())

    assert created.returncode == 7
    assert list(record["docker_receipts"].values())[-1]["state"] == "unresolved"
    assert record["owned_resources"]["containers"] == {}
    assert (binary_directory / "container.json").exists()
    with pytest.raises(ResourceError, match=r"cleanup|guardian"):
        handle.leave("failed")


@pytest.mark.parametrize(
    ("mode", "exit_code", "receipt_state"),
    [("child-fail", 42, "failed"), ("fast-rm", 0, "removed"), ("fast-rm-fail", 42, "failed")],
)
def test_container_receipt_closes_only_on_decisive_terminal_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    exit_code: int,
    receipt_state: str,
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    binary_directory = tmp_path / "bin"
    binary_directory.mkdir()
    _stateful_fake_docker(binary_directory)
    monkeypatch.setenv("PATH", f"{binary_directory}{os.pathsep}{os.environ['PATH']}")
    (binary_directory / "mode").write_text(mode)
    handle = (
        build_test_services(tmp_path / "state")
        .open_run(_spec(project), docker_factory=_configure_fake_docker(binary_directory))
        .enter()
    )
    context = handle.docker_context

    result = subprocess.run(
        [context.executable, "run", "--rm", IMAGE_ID, "true"],
        env=dict(context.environment),
        capture_output=True,
        text=True,
        check=False,
    )
    record = json.loads((tmp_path / "state/registry/runs" / f"{handle.run_id}.json").read_text())

    assert result.returncode == exit_code
    assert list(record["docker_receipts"].values())[-1]["state"] == receipt_state
    assert record["owned_resources"]["containers"] == {}
    handle.leave("failed" if exit_code else "passed")


def test_changed_or_moved_private_shim_refuses_before_receipt(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = tmp_path / "project"
    project.mkdir()
    binary_directory = tmp_path / "bin"
    binary_directory.mkdir()
    _stateful_fake_docker(binary_directory)
    monkeypatch.setenv("PATH", f"{binary_directory}{os.pathsep}{os.environ['PATH']}")
    handle = (
        build_test_services(tmp_path / "state")
        .open_run(_spec(project), docker_factory=_configure_fake_docker(binary_directory))
        .enter()
    )
    context = handle.docker_context
    original = context.executable.read_bytes()
    context.executable.write_bytes(original + b"\n")

    changed = subprocess.run(
        [context.executable, "info"],
        env=dict(context.environment),
        capture_output=True,
        text=True,
        check=False,
    )
    context.executable.write_bytes(original)
    context.executable.chmod(0o700)
    moved = tmp_path / "moved"
    moved.mkdir()
    shutil.copy2(context.executable, moved / "docker")
    shutil.copy2(handle.private_root / "docker-test-toolkit-dispatch", moved / "docker-test-toolkit-dispatch")
    moved_result = subprocess.run(
        [moved / "docker", "info"],
        env=dict(context.environment),
        capture_output=True,
        text=True,
        check=False,
    )
    record = json.loads((tmp_path / "state/registry/runs" / f"{handle.run_id}.json").read_text())

    assert changed.returncode == 3
    assert moved_result.returncode == 3
    assert record["docker_receipts"] == {}
    handle.leave()


def _entered_resource_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, retain: bool):
    """Enter a run, let it create one exact owned container, and return its handle, context and account directory."""
    project = tmp_path / "project"
    project.mkdir()
    binary_directory = tmp_path / "bin"
    binary_directory.mkdir()
    _stateful_fake_docker(binary_directory)
    monkeypatch.setenv("PATH", f"{binary_directory}{os.pathsep}{os.environ['PATH']}")
    handle = (
        build_test_services(tmp_path / "state")
        .open_run(_spec(project, retain=retain), docker_factory=_configure_fake_docker(binary_directory))
        .enter()
    )
    docker_context = handle.docker_context
    created = subprocess.run(
        [
            docker_context.executable,
            "run",
            IMAGE_ID,
            "true",
        ],
        env=dict(docker_context.environment),
        capture_output=True,
        text=True,
        check=False,
    )
    assert created.returncode == 0, created.stderr
    context = lifecycle.RunContext(
        Context(project.resolve(), tmp_path / "state/registry"),
        handle.run_id,
        handle.session_id,
        tmp_path / "state/registry/runs" / f"{handle.run_id}.json",
        handle.private_root,
    )
    return handle, context, tmp_path / "state/admission"


def _withdraw_then_lose_the_requester(handle: RunHandle, context: lifecycle.RunContext) -> dict:
    """Kill the guardian, then let the requester write its withdrawal and die before any acknowledgment."""
    kill_guardian(context)

    def requester_dies(stage: str) -> None:
        if stage == "leave-withdrawal-written":
            raise Interrupted

    install_failpoint(requester_dies)
    try:
        with pytest.raises(Interrupted):
            handle.leave()
    finally:
        install_failpoint(None)
    return json.loads(context.record.read_text())


def test_withdrawn_dead_owner_recovery_records_complete_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    handle, context, account = _entered_resource_run(tmp_path, monkeypatch, retain=False)
    before = _withdraw_then_lose_the_requester(handle, context)
    assert (before["state"], before["cleanup_outcome"], before["leave_ack"]) == ("stopping", "pending", None)

    lifecycle.recover_run(context, _test_directory=account)

    status = lifecycle.run_status(context)  # the recovered record must load
    record = json.loads(context.record.read_text())
    assert (status.state, status.cleanup_outcome) == ("complete", "complete")
    assert record["owned_resources"]["containers"]["a" * 64]["state"] == "removed"
    assert record["recovery_actor"] is not None and record["leave_ack"] is None  # toolkit recovery, not the requester
    assert lifecycle.completed_run_is_quiescent(record)


def test_withdrawn_retained_dead_owner_recovery_keeps_stored_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    handle, context, account = _entered_resource_run(tmp_path, monkeypatch, retain=True)
    before = _withdraw_then_lose_the_requester(handle, context)

    lifecycle.recover_run(context, _test_directory=account)

    record = json.loads(context.record.read_text())
    assert lifecycle.run_status(context).state == "stopped-retained"
    assert record["cleanup_outcome"] == before["cleanup_outcome"] == "pending"  # a retained run keeps its stored value
    changed = {key for key in record if record[key] != before[key]}
    assert changed <= {
        "state",
        "admission",
        "revision",
        "recovery_actor",
        "recovery_outcome",
        "quiescent",
        "release_authority",
        "resources",
        "owned_resources",
        "docker_receipts",
        "hold_timeline",  # the slot release is recorded as recovery
    }
    assert record["hold_timeline"]["release"] == "recovery"


def test_guardian_lost_recovery_preserves_incomplete_cleanup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    handle, context, account = _entered_resource_run(tmp_path, monkeypatch, retain=False)
    kill_guardian(context)
    with pytest.raises(IncompleteCleanup):
        handle.leave()  # the requester records the lost guardian: cleanup_outcome incomplete
    assert json.loads(context.record.read_text())["cleanup_outcome"] == "incomplete"

    lifecycle.recover_run(context, _test_directory=account)

    status = lifecycle.run_status(context)
    assert (status.state, status.cleanup_outcome) == ("complete", "incomplete")  # a real failure stays recorded
    assert json.loads(context.record.read_text())["infrastructure_failure"] == "guardian-lost"


def test_cancelling_a_resume_preparation_restores_the_stopped_retained_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _handle, context, _container, _account = _retained_owned_container(tmp_path, monkeypatch)
    retained = json.loads(context.record.read_text())
    owned_container = next(iter(retained["owned_resources"]["containers"].values()))
    owned_container["evidence"].update(service="fixture", completed_service=False)
    atomic_json(context.record, retained)
    requester = distinct_process_identity(30_000)

    with monkeypatch.context() as patcher:  # a second terminal: the same OS-level seams as the resume test above
        patcher.setattr(lifecycle, "observe_process", lambda _identity: "exited")
        patcher.setattr(lifecycle, "current_process", lambda _clock: requester)
        patcher.setattr(lifecycle, "source_identity", lambda: retained["source_identity"])
        assert lifecycle.prepare_resume(context, _test_directory=tmp_path / "state/admission").state == "prepared"
        restored = lifecycle.cancel_run(context)

    record = lifecycle.load_run_record(context)  # the loader accepts the restored record
    assert restored.state == "stopped-retained"
    assert record["retained_grant"] is None and record["resume_origin"] is None
    assert "hold_timeline" in retained and "hold_timeline" not in record  # dropped at resume, never restored
    assert {key: record[key] for key in retained if key not in ("revision", "hold_timeline")} == {
        key: value for key, value in retained.items() if key not in ("revision", "hold_timeline")
    }  # every other field is back to the retained snapshot
    assert json.loads(context.project.record.read_text())["runs"] == {context.run_id: str(context.record)}


def test_resume_rebinds_a_run_another_version_retained(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A retained run carries over within a state format: the resumer's version takes it over."""
    _handle, context, _container, _account = _retained_owned_container(tmp_path, monkeypatch)
    retained = json.loads(context.record.read_text())
    owned_container = next(iter(retained["owned_resources"]["containers"].values()))
    owned_container["evidence"].update(service="fixture", completed_service=False)
    older = {"version": "0.0.9", "sha256": "a" * 64, "protocols": {"run": 1}}
    retained.update(source_identity=older["sha256"], artifact=older)  # as an older installed version recorded it
    atomic_json(context.record, retained)
    requester = distinct_process_identity(30_000)

    with monkeypatch.context() as patcher:
        patcher.setattr(lifecycle, "observe_process", lambda _identity: "exited")
        patcher.setattr(lifecycle, "current_process", lambda _clock: requester)
        assert lifecycle.prepare_resume(context, _test_directory=tmp_path / "state/admission").state == "prepared"
        rebound = json.loads(context.record.read_text())
        assert rebound["source_identity"] == lifecycle.source_identity()
        assert rebound["artifact"] == lifecycle.toolkit_identity_record()
        assert (rebound["resume_origin"]["source_identity"], rebound["resume_origin"]["artifact"]) == (
            older["sha256"],
            older,
        )
        lifecycle.cancel_run(context)  # an ungranted resume rolls back to exactly what was retained

    restored = json.loads(context.record.read_text())
    assert (restored["state"], restored["source_identity"], restored["artifact"]) == (
        "stopped-retained",
        older["sha256"],
        older,
    )


def test_resume_without_retained_containers_is_refused_before_any_write(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    services = build_test_services(tmp_path / "state")
    phases = {"tests": PhaseBudget(0, 1, 1, 1)}
    spec = RunSpec(project, "integration", "empty-retained", (), (), "fixture", phases, "tests", retain=True)
    handle = services.open_run(spec).enter()
    assert handle.retain().state == "stopped-retained"
    before = handle._context.record.read_bytes()

    with pytest.raises(ResourceError, match="Only one healthy exact stopped-retained run may resume"):
        lifecycle.prepare_resume(handle._context, _test_directory=tmp_path / "state/admission")

    assert handle._context.record.read_bytes() == before
    assert lifecycle.load_run_record(handle._context)["state"] == "stopped-retained"
