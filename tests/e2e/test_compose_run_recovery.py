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
from pathlib import Path

import pytest
from tests.e2e.conftest import redis_id
from tests.integration.account_fixtures import current_identity, dead_identity, kill_guardian, retire_actor

from docker_test_toolkit import ArtifactIdentity, ExternalDockerResource, PhaseBudget, RunSpec
from docker_test_toolkit.application import lifecycle
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


def _project(tmp_path: Path) -> tuple[Path, Path]:
    project = tmp_path / "project"
    project.mkdir()
    compose = project / "compose.yml"
    # ``expose`` without ``ports``: the running container reports {"6379/tcp": null}, the exited one {}
    compose.write_text(f"services:\n  fixture:\n    image: {redis_id()}\n    expose:\n      - '6379'\n")
    return project, compose


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
    for container_id in _service_containers(compose_project):
        subprocess.run([_DOCKER, "stop", "-t", "1", container_id], check=True, capture_output=True)


def _remove_leftovers(compose_project: str) -> None:
    """Remove only this test's compose project objects, if a failed assertion left any."""
    for container_id in _service_containers(compose_project):
        subprocess.run([_DOCKER, "rm", "-f", "-v", container_id], check=False, capture_output=True)
    for kind in ("network", "volume"):
        listed = subprocess.run(
            [_DOCKER, kind, "ls", "-q", "--filter", f"label=com.docker.compose.project={compose_project}"],
            check=False,
            capture_output=True,
            text=True,
        )
        for name in listed.stdout.split():
            subprocess.run([_DOCKER, kind, "rm", name], check=False, capture_output=True)


def _pending_compose_run(record_path: Path, deadline_s: float = 60) -> dict:
    deadline = time.monotonic() + deadline_s
    while time.monotonic() < deadline:
        record = json.loads(record_path.read_text())
        pending = next(
            (
                item
                for item in record["docker_receipts"].values()
                if item["kind"] == "compose-run" and item["state"] == "pending"
            ),
            None,
        )
        if pending is not None and _service_containers(pending["plan"]["project"]):
            return pending
        time.sleep(0.05)
    raise AssertionError("no pending compose-run receipt appeared")


def test_killed_compose_run_recovers_after_its_containers_stopped(tmp_path: Path, redis_image: None) -> None:
    project, compose = _project(tmp_path)
    services = build_test_services(tmp_path / "state")
    lifecycle.configure_docker_factories(build_docker_adapter, bind_docker_adapter)
    handle = services.open_run(_spec(project), admit_docker=True).enter()
    docker = handle.docker_context
    up = _shim(docker, project, "compose", "--file", str(compose), "up", "--detach")
    assert up.wait(timeout=120) == 0, up.stderr.read() if up.stderr else ""
    long_run = _shim(
        docker,
        project,
        "compose",
        "--file",
        str(compose),
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
    compose_project = docker.compose_project
    try:
        _pending_compose_run(handle._context.record)
        os.killpg(long_run.pid, signal.SIGKILL)  # the client dies mid-run
        long_run.wait(timeout=10)
        kill_guardian(handle._context)
        retire_actor(
            handle._context.project.state, current_identity(), dead_identity(), account=services.root / "admission"
        )
        _stop_directly(compose_project)  # stands in for the engine stop: every container exits

        status = lifecycle.recover_run(handle._context, _test_directory=services.root / "admission")

        record = json.loads(handle._context.record.read_text())
        assert status.state == "complete"
        assert lifecycle.quiescence_blockers(record) == ()
        assert all(
            item["state"] == "removed" for items in record["owned_resources"].values() for item in items.values()
        )
        assert _service_containers(compose_project) == []
    finally:
        if long_run.poll() is None:
            os.killpg(long_run.pid, signal.SIGKILL)
            long_run.wait(timeout=10)
        _remove_leftovers(compose_project)


def test_compose_run_tolerates_a_sibling_stopped_meanwhile(tmp_path: Path, redis_image: None) -> None:
    project, compose = _project(tmp_path)
    services = build_test_services(tmp_path / "state")
    lifecycle.configure_docker_factories(build_docker_adapter, bind_docker_adapter)
    handle = services.open_run(_spec(project), admit_docker=True).enter()
    docker = handle.docker_context
    compose_project = docker.compose_project
    try:
        up = _shim(docker, project, "compose", "--file", str(compose), "up", "--detach")
        assert up.wait(timeout=120) == 0
        _stop_directly(compose_project)  # the service exits before the next compose run finishes

        one_off = _shim(
            docker,
            project,
            "compose",
            "--file",
            str(compose),
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
        _remove_leftovers(compose_project)
