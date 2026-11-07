from __future__ import annotations

import json
import os
import signal
import subprocess
import time
from pathlib import Path

import pytest
from tests.e2e.conftest import redis_id

from docker_test_toolkit import ArtifactIdentity, ExternalDockerResource, PhaseBudget, RunSpec
from docker_test_toolkit.application import build_cache
from docker_test_toolkit.factories.testing import build_test_services

pytestmark = pytest.mark.e2e


def _run(context, cwd: Path, *argv: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(context.executable), *argv],
        check=False,
        cwd=cwd,
        env=dict(context.environment),
        text=True,
        capture_output=True,
        timeout=1200,
    )


def test_exact_resource_cleanup_uses_one_real_daemon(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    (project / "Dockerfile").write_text('FROM scratch\nLABEL purpose="docker-test-toolkit-e2e"\n')
    spec = RunSpec(
        project_root=project,
        entrypoint="e2e",
        purpose="docker-lifecycle",
        services=("fixture",),
        closure=("fixture",),
        profile="fixture",
        phases={"tests": PhaseBudget(2 * 1024**3, 128 * 1024**2, 2, 1)},
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
        wait_seconds=60,
    )
    handle = build_test_services(tmp_path / "state").open_run(spec, admit_docker=True).enter()
    docker = handle.docker_context

    try:
        assert _run(docker, project, "buildx", "create").returncode == 0
        image = _run(
            docker,
            project,
            "buildx",
            "build",
            "--tag",
            f"dtt-e2e:{handle.run_id}",
            "--tag",
            f"dtt-e2e-copy:{handle.run_id}",
            ".",
        )
        assert image.returncode == 0, image.stderr
        assert _run(docker, project, "network", "create").returncode == 0
        assert _run(docker, project, "volume", "create").returncode == 0
    finally:
        status = handle.leave("passed")

    assert status.cleanup_outcome == "complete"
    final = json.loads(handle._context.record.read_text())
    assert all(
        item["state"] == "removed" for resources in final["owned_resources"].values() for item in resources.values()
    )
    assert all(item["state"] not in {"pending", "unresolved"} for item in final["docker_receipts"].values())
    build = next(item for item in final["docker_receipts"].values() if item["kind"] == "build")
    assert final["hold_timeline"]["release"] == "leave"  # every new grant records its timeline
    assert set(build["plan"]["cache"]) >= {"finished_ns"}  # a dispatcher-finished build records its finish tick
    assert build["plan"]["cache"]["outcome"] == "miss"  # the first build of a project exports and publishes
    platform_dir = build_cache.project_cache(
        handle._context.project.state, final["project_id"]
    ) / build_cache.platform_name(build["plan"]["daemon_platform"])
    assert {path.name for path in platform_dir.iterdir()} <= {"generations", "staging"}
    staging = platform_dir / "staging"
    assert not staging.exists() or not any(staging.iterdir())  # the build's own staging was published or collected
    assert (platform_dir.parent / f"{platform_dir.name}.lock").is_file()
    platform = next(iter(final["owned_resources"]["images"].values()))["evidence"]["platform"]
    assert platform["emulated"] == "native"  # a build without --platform runs on the daemon's own platform
    assert platform["image"]["architecture"] == build["plan"]["daemon_platform"]["architecture"]


def test_borrowed_python_consumer_is_the_only_process_allowed_to_use_its_shim(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    ready = project / "ready"
    proceed = project / "proceed"
    probe = project / "borrow.py"
    probe.write_text(
        "import json, os, pathlib, subprocess, sys, time\n"
        "import docker_test_toolkit as toolkit\n"
        "context = toolkit.current_consumer_run()\n"
        "assert context is not None\n"
        "pathlib.Path(sys.argv[1]).write_text(str(context.docker_context.executable))\n"
        "while not pathlib.Path(sys.argv[2]).exists(): time.sleep(0.02)\n"
        "pid = os.fork()\n"
        "if pid == 0:\n"
        "    try:\n"
        "        toolkit.current_consumer_run()\n"
        "    except toolkit.ToolkitError:\n"
        "        accessor_refused = True\n"
        "    else:\n"
        "        accessor_refused = False\n"
        "    attempted = subprocess.run([str(context.docker_context.executable), 'version', '--format', "
        "'{{.Client.Version}}'], env=dict(context.docker_context.environment), capture_output=True)\n"
        "    os._exit(0 if accessor_refused and attempted.returncode != 0 else 91)\n"
        "_, status = os.waitpid(pid, 0)\n"
        "happy = subprocess.run([str(context.docker_context.executable), 'version', '--format', "
        "'{{.Client.Version}}'], env=dict(context.docker_context.environment), capture_output=True, text=True)\n"
        "print(json.dumps({'fork': os.waitstatus_to_exitcode(status), 'happy': happy.returncode, "
        "'version': happy.stdout.strip(), 'error': happy.stderr}))\n"
    )
    spec = RunSpec(
        project_root=project,
        entrypoint="e2e",
        purpose="borrowed-consumer",
        services=("fixture",),
        closure=("fixture",),
        profile="fixture",
        phases={"tests": PhaseBudget(0, 128 * 1024**2, 1, 1)},
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
        wait_seconds=60,
    )
    handle = build_test_services(tmp_path / "state").open_run(spec, admit_docker=True).enter()
    child = None
    consumer_id = None
    try:
        child, consumer_id = handle.launch_python_consumer(
            "script",
            str(probe),
            (str(ready), str(proceed)),
            ("fixture",),
            cwd=project,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            borrow_docker=True,
        )
        deadline = time.monotonic() + 20
        while not ready.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        assert ready.exists()
        wrong_process = subprocess.run(
            [ready.read_text(), "version", "--format", "{{.Client.Version}}"],
            cwd=project,
            capture_output=True,
            text=True,
            check=False,
        )
        assert wrong_process.returncode != 0
        assert "registered Python consumer" in wrong_process.stderr
        proceed.touch()
        stdout, stderr = child.communicate(timeout=30)
        assert child.returncode == 0, stderr
        payload = json.loads(stdout)
        assert payload["fork"] == 0
        assert payload["happy"] == 0
        assert (
            payload["version"]
            == subprocess.run(
                ["docker", "version", "--format", "{{.Client.Version}}"],
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
        )
        assert "registered Python consumer" not in payload["error"]
        handle.complete_consumer(consumer_id)
        consumer_id = None
    finally:
        if child is not None and child.poll() is None and consumer_id is not None:
            handle.stop_consumer(consumer_id)
        handle.leave("passed")


def test_long_blocking_run_and_outer_cancellation_reconcile_exactly(tmp_path: Path, redis_image: None) -> None:
    project = tmp_path / "project"
    project.mkdir()
    compose = project / "compose.yml"
    compose.write_text(f"services:\n  fixture:\n    image: {redis_id()}\n")
    spec = RunSpec(
        project_root=project,
        entrypoint="e2e",
        purpose="blocking-run",
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
    handle = build_test_services(tmp_path / "state").open_run(spec, admit_docker=True).enter()
    docker = handle.docker_context
    controls = (
        "--rm",
        "--read-only",
        "--network=none",
        "--cpus=1",
        "--memory=134217728",
        "--memory-swap=134217728",
        "--pids-limit=64",
        "--log-driver=none",
    )
    process: subprocess.Popen[str] | None = None
    container_id = ""
    try:
        completed = _run(
            docker,
            project,
            "run",
            *controls,
            redis_id(),
            "sh",
            "-c",
            "sleep 31; printf long-run-complete; printf long-run-diagnostic >&2; exit 1",
        )
        assert completed.returncode == 1
        assert completed.stdout == "long-run-complete"
        assert completed.stderr.rstrip().endswith("long-run-diagnostic")
        composed = _run(
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
            "sleep 31; printf long-compose-complete; printf long-compose-diagnostic >&2; exit 1",
        )
        assert composed.returncode == 1
        assert composed.stdout == "long-compose-complete"
        assert composed.stderr.rstrip().endswith("long-compose-diagnostic")
        process = subprocess.Popen(
            [
                str(docker.executable),
                "run",
                *controls,
                redis_id(),
                "sh",
                "-c",
                "sleep 300",
            ],
            cwd=project,
            env=dict(docker.environment),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        deadline = time.monotonic() + 20
        pending = None
        while time.monotonic() < deadline:
            record = json.loads(handle._context.record.read_text())
            pending = next(
                (item for item in record["docker_receipts"].values() if item["state"] == "pending"),
                None,
            )
            if pending is not None and Path(pending["plan"]["cidfile"]).exists():
                container_id = Path(pending["plan"]["cidfile"]).read_text().strip()
                if container_id:
                    break
            time.sleep(0.05)
        assert pending is not None and container_id
        os.killpg(process.pid, signal.SIGKILL)
        assert process.wait(timeout=5) != 0
    finally:
        if process is not None and process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=5)
        status = handle.leave("failed")

    assert status.cleanup_outcome == "complete"
    final = json.loads(handle._context.record.read_text())
    assert all(item["state"] not in {"pending", "unresolved"} for item in final["docker_receipts"].values())
    assert all(
        item["state"] == "removed" for resources in final["owned_resources"].values() for item in resources.values()
    )
    inspected = subprocess.run(
        ["docker", "container", "inspect", container_id],
        check=False,
        capture_output=True,
        text=True,
        env=dict(docker.environment),
    )
    assert inspected.returncode != 0
