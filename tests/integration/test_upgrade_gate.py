"""The version gate on a bound test installation: only ``current`` admits new test runs.

Each private composition here is bound to a temporary installation (``bind_test_installation``): ``root/9.9.1`` is the
version this process "loaded", and the test switches ``root/current`` to ``9.9.2`` under the account's metadata lock,
as the installer does. One test per gate point and state.
"""

from __future__ import annotations

import json
import os
import threading
from io import BytesIO, StringIO
from pathlib import Path

import pytest

from docker_test_toolkit import PhaseBudget, RunSpec
from docker_test_toolkit.adapters.inbound.bridge import serve_lease
from docker_test_toolkit.application.recovery import Context, lock
from docker_test_toolkit.domain.errors import IncompatibleState, InvalidInput
from docker_test_toolkit.factories.testing import bind_test_installation, build_test_services
from tests.integration.account_fixtures import (
    EnterThread,
    prepare_for_child_guardian,
    queue_phases,
    reap,
    source_artifact,
    start_guardian_child,
    wait_until,
)

pytestmark = pytest.mark.integration


class _World:
    """One private composition bound to a two-version temporary installation, and its projects."""

    def __init__(self, tmp_path: Path) -> None:
        self.tmp = tmp_path
        self.services = build_test_services(tmp_path / "state")
        self.account = self.services.root / "admission"
        self.registry = self.services.root / "registry"
        self.root = tmp_path / "installation"
        for version in ("9.9.1", "9.9.2"):
            (self.root / version).mkdir(parents=True)
        (self.root / "current").symlink_to("9.9.1")
        bind_test_installation(self.services, root=self.root, loaded=self.root / "9.9.1")

    def project(self, name: str) -> Path:
        project = self.tmp / name
        project.mkdir()
        return project

    def spec(self, project: Path, purpose: str = "gated", *, retain: bool = False) -> RunSpec:
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
            retain=retain,
        )

    def switch(self) -> None:
        """Switch ``current`` to 9.9.2 under the account's exclusive metadata lock, as the installer's step 4 does."""
        with lock(Context(self.account, self.account), "metadata"):
            link = self.root / ".current.tmp"
            link.symlink_to("9.9.2")
            os.replace(link, self.root / "current")

    def runs(self) -> list[str]:
        directory = self.registry / "runs"
        return sorted(path.name for path in directory.iterdir()) if directory.exists() else []

    def indexed(self, handle) -> bool:
        return handle.run_id in json.loads(handle._context.project.record.read_text())["runs"]


def _record(handle) -> dict:
    return json.loads(handle._context.record.read_text())


def _finalized(world: _World, handle) -> None:
    """The run ended as an upgraded preparation and left nothing: record complete, no private root, no index entry."""
    record = _record(handle)
    assert (record["state"], record["admission"], record["infrastructure_failure"]) == (
        "complete",
        "revoked",
        "toolkit-upgraded",
    )
    assert record["cleanup_outcome"] == "complete" and record["grant"] is None
    assert not handle.private_root.exists() and not handle.private_root.parent.parent.exists()
    assert not world.indexed(handle)
    assert handle.run_id not in queue_phases(world.account)


def _upgraded(error: BaseException | None) -> bool:
    return isinstance(error, IncompatibleState) and error.reason == "toolkit-upgraded"


def test_old_code_prepare_writes_no_record(tmp_path: Path) -> None:
    world = _World(tmp_path)
    project = world.project("project")
    world.switch()

    with pytest.raises(IncompatibleState) as refused:
        world.services.open_run(world.spec(project))

    assert refused.value.reason == "toolkit-upgraded"
    assert "restart this test process" in str(refused.value)
    assert world.runs() == []


def test_prepared_unentered_run_is_cancelled_and_cleaned_on_enter(tmp_path: Path) -> None:
    world = _World(tmp_path)
    handle = world.services.open_run(world.spec(world.project("project")))
    world.switch()

    with pytest.raises(IncompatibleState) as refused:
        handle.enter()

    assert refused.value.reason == "toolkit-upgraded"
    record = _record(handle)
    assert (record["state"], record["test_outcome"], record["cleanup_outcome"]) == ("complete", "cancelled", "complete")
    assert record["guardian_attempt"] is None  # no guardian was started
    assert not handle.private_root.exists() and not world.indexed(handle)
    assert not handle.private_root.parent.parent.exists()


def test_queued_in_process_run_is_revoked_and_finalized(tmp_path: Path) -> None:
    world = _World(tmp_path)
    holder = world.services.open_run(world.spec(world.project("holder"))).enter()
    waiter = world.services.open_run(world.spec(world.project("waiter")))
    waiting = EnterThread(waiter)
    wait_until(lambda: queue_phases(world.account).get(waiter.run_id) == "queued", message="the waiter queued")

    world.switch()  # the waiter is not the head of anything it can take: the gate ends it at its next pass anyway
    waiting.join(30)

    assert _upgraded(waiting.error), waiting.error
    _finalized(world, waiter)
    assert queue_phases(world.account) == {holder.run_id: "granted"}  # the running old-version test is untouched
    holder.leave()


def test_queued_run_through_a_never_cancelling_bridge_is_finalized(tmp_path: Path) -> None:
    world = _World(tmp_path)
    holder = world.services.open_run(world.spec(world.project("holder"))).enter()
    project = world.project("bridged")
    payload = {
        "project_root": str(project),
        "entrypoint": "integration",
        "purpose": "bridged",
        "services": [],
        "closure": [],
        "profile": "fixture",
        "phases": {"tests": {"container_bytes": 0, "host_process_bytes": 1, "daemon_cpus": 1, "host_threads": 1}},
        "initial_phase": "tests",
        "artifact": {
            "version": source_artifact().version,
            "sha256": source_artifact().sha256,
            "protocols": dict(source_artifact().protocols),
        },
        "external_resources": [],
        "retain": False,
        "wait_seconds": 30,
    }
    source = BytesIO(  # only prepare and enter: the client never cancels, like a bridge whose close ignores queued runs
        b"".join(
            json.dumps(request).encode() + b"\n"
            for request in (
                {"command": "run.prepare", "input": payload},
                {"command": "run.enter", "input": {"docker": False}},
            )
        )
    )
    target = StringIO()
    bridge = threading.Thread(
        target=serve_lease, args=(source, target), kwargs={"open_run_factory": world.services.open_run}
    )
    bridge.start()
    wait_until(lambda: "queued" in queue_phases(world.account).values(), message="the bridged run queued")
    (run_id,) = [run for run, phase in queue_phases(world.account).items() if phase == "queued"]

    world.switch()
    bridge.join(30)

    responses = [json.loads(line) for line in target.getvalue().splitlines()]
    assert responses[1]["command"] == "run.enter" and responses[1]["ok"] is False
    assert (responses[1]["error"]["category"], responses[1]["error"]["context"]["reason"]) == (
        "incompatible_state",
        "toolkit-upgraded",
    )
    record = json.loads((world.registry / "runs" / f"{run_id}.json").read_text())
    assert (record["state"], record["infrastructure_failure"], record["cleanup_outcome"]) == (
        "complete",
        "toolkit-upgraded",
        "complete",
    )
    assert not Path(record["private_root"]).exists()
    holder.leave()


def _paused_guardian(world: _World, stage: str, extra: dict | None = None):
    """Prepare a run whose guardian child pauses at ``stage``; return the handle, the child, the gates and the log."""
    handle = world.services.open_run(world.spec(world.project("paused")))
    prepare_for_child_guardian(handle)
    reached, release = world.tmp / f"{stage}.reached", world.tmp / f"{stage}.release"
    log = world.tmp / "guardian.log"
    stages = {
        stage: {"action": [{"do": "write-file", "path": str(reached)}, {"do": "wait-file", "path": str(release)}]}
    }
    stages |= extra or {}
    guardian, _result = start_guardian_child(world.tmp, handle, world.account, stages, observer_log=log)
    return handle, guardian, reached, release, log


def _stages(log: Path) -> list[str]:
    lines = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
    return [line["stage"] for line in lines if line["kind"] == "stage"]


@pytest.mark.parametrize("stage", ["publish-pending", "slot-reserved"])
def test_publication_refuses_inside_its_section_after_switch(tmp_path: Path, stage: str) -> None:
    """Reserved before the switch, paused outside every lock: publication itself refuses, after leaving its sections."""
    world = _World(tmp_path)
    handle, guardian, reached, release, log = _paused_guardian(world, stage)
    try:
        entering = EnterThread(handle, existing_attempt=True)
        wait_until(reached.exists, message=f"the guardian reached {stage}")
        world.switch()
        release.write_text("")
        entering.join(30)
        assert guardian.wait(timeout=30) == 0
    finally:
        reap(guardian)

    assert _upgraded(entering.error), entering.error
    assert "publish-gate-checked" in _stages(log)
    _finalized(
        world, handle
    )  # refused as toolkit-upgraded, not by a lock wait that ran out (a revoke inside the section)


def test_publication_section_excludes_a_concurrent_switch(tmp_path: Path) -> None:
    """A switch started while publication holds its section completes only after publication left it."""
    world = _World(tmp_path)
    handle, guardian, reached, release, _log = _paused_guardian(world, "publish-gate-checked")
    switched = threading.Event()

    def switch() -> None:
        world.switch()
        switched.set()

    try:
        entering = EnterThread(handle, existing_attempt=True)
        wait_until(reached.exists, message="the guardian passed the gate inside the publication section")
        switcher = threading.Thread(target=switch)
        switcher.start()
        blocked = not switched.wait(1.0)  # bounded: the switch waits for the publication's metadata section
        release.write_text("")
        switcher.join(30)
        entering.join(30)

        assert blocked and switched.is_set()
        assert entering.error is None  # published before the switch: a running old-version test
        assert _record(handle)["state"] == "active"
        handle.leave()  # the running old-version test ends normally after the switch
        assert guardian.wait(timeout=30) == 0
    finally:
        release.write_text("")
        reap(guardian)


def test_old_code_resume_is_refused_and_stays_stopped_retained(tmp_path: Path) -> None:
    world = _World(tmp_path)
    project = world.project("retained")
    spec = world.spec(project, "retained", retain=True)
    handle = world.services.open_run(spec).enter()
    assert handle.retain().state == "stopped-retained"
    locator = tmp_path / "locator.json"
    handle.write_retained_locator(locator)
    before = _record(handle)["source_identity"]
    world.switch()

    with pytest.raises(IncompatibleState) as refused:
        world.services.open_retained_run(spec, locator).resume()

    assert refused.value.reason == "toolkit-upgraded"
    record = _record(handle)
    assert (record["state"], record["source_identity"]) == ("stopped-retained", before)


def test_old_version_releases_its_retained_run_after_switch(tmp_path: Path) -> None:
    world = _World(tmp_path)
    project = world.project("retained")
    spec = world.spec(project, "retained", retain=True)
    handle = world.services.open_run(spec).enter()
    assert handle.retain().state == "stopped-retained"
    locator = tmp_path / "locator.json"
    handle.write_retained_locator(locator)
    world.switch()

    released = world.services.open_retained_run(spec, locator).release()

    assert (released.state, released.cleanup_outcome) == ("complete", "complete")


def test_reserved_purpose_is_refused_by_open_run_and_run_prepare(tmp_path: Path) -> None:
    world = _World(tmp_path)
    project = world.project("reserved")

    with pytest.raises(InvalidInput):
        world.services.open_run(world.spec(project, "retained-release"))

    request = {
        "command": "run.prepare",
        "input": {
            "project_root": str(project),
            "entrypoint": "integration",
            "purpose": "retained-release",
            "services": [],
            "closure": [],
            "profile": "fixture",
            "phases": {"tests": {"container_bytes": 0, "host_process_bytes": 1, "daemon_cpus": 1, "host_threads": 1}},
            "initial_phase": "tests",
            "artifact": {
                "version": source_artifact().version,
                "sha256": source_artifact().sha256,
                "protocols": dict(source_artifact().protocols),
            },
            "external_resources": [],
            "retain": False,
            "wait_seconds": 30,
        },
    }
    target = StringIO()
    serve_lease(BytesIO(json.dumps(request).encode() + b"\n"), target, open_run_factory=world.services.open_run)

    (response,) = [json.loads(line) for line in target.getvalue().splitlines()]
    assert response["ok"] is False and response["error"]["category"] == "invalid_input"
    assert world.runs() == []
