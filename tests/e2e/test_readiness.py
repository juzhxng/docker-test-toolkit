from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from docker_test_toolkit.config.installation import installation_of

pytestmark = pytest.mark.e2e
ROOT = Path(__file__).parents[2]


def _docker_inventory() -> dict[str, tuple[str, ...]]:
    docker = "docker"
    commands = {
        "containers": (docker, "container", "ls", "--all", "--quiet", "--no-trunc"),
        "networks": (docker, "network", "ls", "--quiet", "--no-trunc"),
        "volumes": (docker, "volume", "ls", "--quiet"),
    }
    return {
        name: tuple(sorted(subprocess.run(command, check=True, capture_output=True, text=True).stdout.splitlines()))
        for name, command in commands.items()
    }


def _account_inventory(root: Path) -> tuple[tuple[str, int, int], ...]:
    if not root.exists():
        return ()
    return tuple(
        (str(path.relative_to(root)), path.lstat().st_size, path.lstat().st_mtime_ns)
        for path in sorted(root.rglob("*"))
    )


def test_installed_readiness_observes_real_docker_without_state_change(session_installation: Path) -> None:
    installation = installation_of(session_installation)
    assert installation is not None
    before_docker = _docker_inventory()
    before_account = _account_inventory(installation.account_directory)

    result = subprocess.run(
        [sys.executable, "-I", str(session_installation), "readiness", "inspect", "--json"],
        cwd=ROOT,
        env={
            "HOME": os.environ.get("HOME", ""),
            "PATH": os.environ.get("PATH", ""),
            "TMPDIR": os.environ.get("TMPDIR", "/tmp"),
        },
        check=False,
        capture_output=True,
        text=True,
        timeout=40,
    )

    assert result.returncode == 0, result.stderr or result.stdout
    response = json.loads(result.stdout)
    assert response["command"] == "readiness.inspect"
    assert response["cleanup"] is None
    assert response["result"]["protocol"] == "docker-test-toolkit.readiness/4"
    assert response["result"]["ready"] is True
    checks = response["result"]["checks"]
    assert [check["check"] for check in checks] == [
        "docker-client",
        "daemon",
        "compose",
        "buildx",
        "clock",
        "capacity",
    ]
    assert [check["code"] for check in checks] == [
        "docker-client-ready",
        "daemon-ready",
        "compose-ready",
        "buildx-ready",
        "clock-ready",
        "capacity-ready",
    ]
    assert [set(check["facts"]) for check in checks] == [
        {
            "client_device",
            "client_inode",
            "client_mtime_ns",
            "client_path",
            "client_sha256",
            "client_size",
            "client_version",
        },
        {
            "api_version",
            "architecture",
            "cgroup_version",
            "daemon_id",
            "endpoint",
            "min_api_version",
            "name",
            "operating_system",
            "os_type",
            "runtime",
            "server_version",
        },
        {"version"},
        {"version"},
        {"origin", "tick_ns"},
        {
            "daemon_cpus",
            "daemon_memory_bytes",
            "host_cpus",
            "host_headroom_bytes",
            "host_memory_bytes",
            "host_page_size",
            "host_swap_total_bytes",
            "host_swap_used_bytes",
            "host_reserve_bytes",
            "parallel_runs",
            "cpu_pressure_percent",
            "cpu_pressure_source",
            "probe_image",
            "probe_reading_age_s",
            *(
                f"disk_{place}_{suffix}"
                for place in ("data_root", "vm_disk_host", "private_root")
                for suffix in ("free_bytes", "total_bytes", "reserve_bytes", "state")
            ),
            "disk_reserve_declared_bytes",
            "retention_last_tick_ns",
            "retention_last_removed_bytes",
        },
    ]
    assert checks[1]["facts"]["architecture"] in {"amd64", "arm64"}
    assert checks[-1]["facts"]["host_swap_total_bytes"] >= 0
    assert checks[-1]["facts"]["parallel_runs"] == "auto"  # nothing declared on the test installation: the default
    assert _docker_inventory() == before_docker
    assert _account_inventory(installation.account_directory) == before_account
