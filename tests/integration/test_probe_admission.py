"""The daemon probe against the real Docker daemon."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from docker_test_toolkit.application import probe
from docker_test_toolkit.application.lifecycle import Deadline, NativeClock
from docker_test_toolkit.factories.cli import build_docker_adapter

pytestmark = pytest.mark.integration


def _probe_containers() -> list[str]:
    listed = subprocess.run(
        ["docker", "ps", "-a", "-q", "--filter", f"label={probe.PROBE_LABEL}"],
        capture_output=True,
        text=True,
        check=True,
    )
    return listed.stdout.split()


def test_the_pinned_probe_image_is_present_after_ensure_and_is_the_pinned_index(tmp_path: Path) -> None:
    docker, clock = build_docker_adapter(), NativeClock()
    daemon_id = docker.observe_daemon().daemon_id

    outcome = probe.ensure_probe_image(
        docker, SimpleNamespace(state=tmp_path / "state"), tmp_path, daemon_id=daemon_id, clock=clock
    )

    assert outcome in ("present", "pulled")
    digests = subprocess.run(
        ["docker", "image", "inspect", probe.PROBE_IMAGE, "--format", "{{json .RepoDigests}}"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert probe.PROBE_IMAGE.split("@")[1] in " ".join(json.loads(digests.stdout))


def test_the_probe_reads_the_real_daemon_and_leaves_nothing(tmp_path: Path) -> None:
    docker, clock = build_docker_adapter(), NativeClock()
    daemon_id = docker.observe_daemon().daemon_id
    probe.ensure_probe_image(
        docker, SimpleNamespace(state=tmp_path / "state"), tmp_path, daemon_id=daemon_id, clock=clock
    )
    info = json.loads(
        subprocess.run(["docker", "info", "--format", "{{json .}}"], capture_output=True, text=True, check=True).stdout
    )
    before = set(_probe_containers())

    result = probe.DaemonProbe(tmp_path).read(
        docker, daemon_id=daemon_id, data_root=info["DockerRootDir"], deadline=Deadline.after(clock, 30), clock=clock
    )

    figure = result.data_root
    assert figure is not None and figure.state == "measured", result
    assert figure.total_bytes is not None and figure.free_bytes is not None
    assert figure.total_bytes > figure.free_bytes > 0
    if sys.platform == "darwin":  # the VM's kernel publishes PSI (Docker Desktop, Colima, Podman)
        assert isinstance(result.cpu_pressure, float)
    assert list((tmp_path / probe.PROBES_DIRECTORY).iterdir()) == []
    assert set(_probe_containers()) <= before  # `--rm`: nothing of this probe remains
