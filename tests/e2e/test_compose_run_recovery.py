"""A run killed or disturbed during ``compose run`` recovers on a real daemon.

Stopping a container directly with ``docker stop`` stands in for what the toolkit cannot see: an engine stop, a
restart policy or an operator. Each test touches only its own compose project.
"""

from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import time
import uuid
from functools import partial
from pathlib import Path

import pytest
from tests.e2e.conftest import redis_id, remove_run_leftovers
from tests.integration.account_fixtures import abandon_run

from docker_test_toolkit import ArtifactIdentity, ExternalDockerResource, PhaseBudget, RunSpec
from docker_test_toolkit.application import lifecycle, recovery
from docker_test_toolkit.domain.errors import ResourceError
from docker_test_toolkit.factories.cli import bind_docker_adapter, build_docker_adapter
from docker_test_toolkit.factories.testing import build_test_services

pytestmark = pytest.mark.e2e

_DOCKER = shutil.which("docker") or "docker"


def _spec(project: Path) -> RunSpec:
    return RunSpec(
        project_root=project,
        entrypoint="e2e",
        purpose="compose-run-recovery",
        services=("fixture",),
        closure=("fixture",),
        profile="fixture",
        phases={"tests": PhaseBudget(512 * 1024**2, 128 * 1024**2, 1, 1)},
        initial_phase="tests",
        artifact=ArtifactIdentity(
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
        ),
        external_resources=(ExternalDockerResource("image", redis_id(), "redis", "use_image"),),
        wait_seconds=60,
    )


def _entered(tmp_path: Path, *, volume: bool = False):
    """An entered run whose one service exposes a port, after ``compose up``; the Compose file is ``compose.yml``."""
    project = tmp_path / "project"
    project.mkdir()
    # ``expose`` without ``ports``: the running container reports {"6379/tcp": null}, the exited one {}
    compose = f"services:\n  fixture:\n    image: {redis_id()}\n    expose:\n      - '6379'\n"
    if volume:
        compose += "    volumes:\n      - data:/data\nvolumes:\n  data: {}\n"
    (project / "compose.yml").write_text(compose)
    services = build_test_services(tmp_path / "state")
    lifecycle.configure_docker_factories(build_docker_adapter, bind_docker_adapter)
    handle = services.open_run(_spec(project), admit_docker=True).enter()
    up = _shim(handle.docker_context, project, "compose", "up", "--detach")
    _out, err = up.communicate(timeout=120)
    assert up.returncode == 0, err
    return services, handle, project


def _shim(docker, project: Path, *argv: str, **popen) -> subprocess.Popen[str]:
    return subprocess.Popen(
        [str(docker.executable), *argv],
        cwd=project,
        env=dict(docker.environment),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        **popen,
    )


def _service_containers(compose_project: str) -> list[str]:
    listed = subprocess.run(
        [_DOCKER, "ps", "-aq", "--no-trunc", "--filter", f"label=com.docker.compose.project={compose_project}"],
        check=True,
        capture_output=True,
        text=True,
    )
    return listed.stdout.split()


def _stop_directly(compose_project: str) -> None:
    subprocess.run([_DOCKER, "stop", "-t", "1", *_service_containers(compose_project)], check=True, capture_output=True)


def _project_dry_run(context) -> dict:
    """``cleanup project`` without ``--execute``, against the run's own project and registry."""
    return recovery.cleanup_project(
        context.project,
        build_docker_adapter(),
        execute=False,
        verify_run=partial(lifecycle.run_record_loads, context.project.state),
        locate_run=partial(lifecycle.indexed_run_item, context.project),
    )


def _wait_for_pending_compose_run(record_path: Path, compose_project: str, deadline_s: float = 60) -> None:
    deadline = time.monotonic() + deadline_s
    while time.monotonic() < deadline:
        record = json.loads(record_path.read_text())
        if (
            any(
                item["kind"] == "compose-run" and item["state"] == "pending"
                for item in record["docker_receipts"].values()
            )
            and len(_service_containers(compose_project)) > 1  # the one-off exists beside the service
        ):
            return
        time.sleep(0.05)
    raise AssertionError("no pending compose-run receipt with its one-off container appeared")


def test_killed_compose_run_recovers_after_its_containers_stopped(tmp_path: Path, redis_image: None) -> None:
    services, handle, project = _entered(tmp_path)
    compose_project = handle.docker_context.compose_project
    long_run = _shim(
        handle.docker_context,
        project,
        "compose",
        "run",
        "--rm",
        "--no-deps",
        "--entrypoint",
        "sh",
        "fixture",
        "-c",
        "sleep 300",
        start_new_session=True,
    )
    try:
        _wait_for_pending_compose_run(handle._context.record, compose_project)
        os.killpg(long_run.pid, signal.SIGKILL)  # the client dies mid-run
        long_run.wait(timeout=10)
        abandon_run(handle._context, services.root / "admission")
        _stop_directly(compose_project)  # stands in for the engine stop: every container exits

        status = lifecycle.recover_run(handle._context, _test_directory=services.root / "admission")

        record = json.loads(handle._context.record.read_text())
        assert status.state == "complete"
        assert lifecycle.quiescence_blockers(record) == ()
        assert all(
            item["state"] == "removed" for items in record["owned_resources"].values() for item in items.values()
        )
        assert _service_containers(compose_project) == []
        assert _project_dry_run(handle._context)["clean"] is True  # A-AC2: project cleanup's final dry run is clean
    finally:
        if long_run.poll() is None:
            os.killpg(long_run.pid, signal.SIGKILL)
            long_run.wait(timeout=10)
        assert remove_run_leftovers(handle._context.project.state) == []


def test_compose_run_tolerates_a_sibling_stopped_meanwhile(tmp_path: Path, redis_image: None) -> None:
    _services, handle, project = _entered(tmp_path)
    compose_project = handle.docker_context.compose_project
    try:
        _stop_directly(compose_project)  # the service exits before the next compose run finishes

        one_off = _shim(
            handle.docker_context,
            project,
            "compose",
            "run",
            "--rm",
            "--no-deps",
            "--entrypoint",
            "true",
            "fixture",
            "true",
        )
        _out, err = one_off.communicate(timeout=120)
        assert one_off.returncode == 0, err
        assert handle.leave("passed").cleanup_outcome == "complete"
        assert _service_containers(compose_project) == []
    finally:
        assert remove_run_leftovers(handle._context.project.state) == []


def test_renamed_container_is_released_by_disowning_it_and_its_dependencies(tmp_path: Path, redis_image: None) -> None:
    """A running service container renamed by someone else is no longer provably the run's: disown releases the run.

    Its network and volume stay in use by it, so each removal is refused naming the object; disowning them too ends
    the run, and every object stays on the daemon untouched.
    """
    services, handle, _project = _entered(tmp_path, volume=True)
    compose_project = handle.docker_context.compose_project
    [service] = _service_containers(compose_project)
    foreign = f"dtt-e2e-foreign-{uuid.uuid4().hex[:8]}"
    record = json.loads(handle._context.record.read_text())
    network = next(iter(record["owned_resources"]["networks"]))
    volume = next(iter(record["owned_resources"]["volumes"]))
    try:
        abandon_run(handle._context, services.root / "admission")
        subprocess.run([_DOCKER, "rename", service, foreign], check=True, capture_output=True)

        def recover(*disown: tuple[str, str]):
            return lifecycle.recover_run(handle._context, disown=disown, _test_directory=services.root / "admission")

        with pytest.raises(ResourceError) as refused:
            recover()
        assert refused.value.reason == "owned-identity-conflict"
        assert (refused.value.details["conflict"]["identifier"], refused.value.details["conflict"]["keys"]) == (
            service,
            ["name"],
        )
        request = [("container", service)]
        named = []
        for _attempt in range(3):
            try:
                status = recover(*request)
                break
            except ResourceError as error:
                resource = error.details["resource"]
                named.append(resource["kind"])
                request.append((resource["kind"], resource["identifier"]))
        assert status.state == "complete"
        assert sorted(named) == ["network", "volume"]
        assert sorted(request) == sorted([("container", service), ("network", network), ("volume", volume)])

        final = json.loads(handle._context.record.read_text())
        assert all(
            lifecycle._owned_bucket(final, kind)[identifier]["evidence"]["disowned"] for kind, identifier in request
        )
        assert lifecycle.quiescence_blockers(final) == ()
        inspected = subprocess.run(
            [_DOCKER, "inspect", "--format", "{{.Name}} {{.State.Running}}", service],
            check=True,
            capture_output=True,
            text=True,
        )
        assert inspected.stdout.split() == [f"/{foreign}", "true"]
        assert subprocess.run([_DOCKER, "network", "inspect", network], capture_output=True).returncode == 0
        assert subprocess.run([_DOCKER, "volume", "inspect", volume], capture_output=True).returncode == 0
    finally:
        subprocess.run([_DOCKER, "rm", "-f", "-v", service], check=False, capture_output=True)
        subprocess.run([_DOCKER, "network", "rm", network], check=False, capture_output=True)
        subprocess.run([_DOCKER, "volume", "rm", volume], check=False, capture_output=True)
        assert remove_run_leftovers(handle._context.project.state) == []
