from __future__ import annotations

import json
import platform
import subprocess
import sys
from pathlib import Path

import pytest

from docker_test_toolkit.application.lifecycle import NativeClock

pytestmark = pytest.mark.e2e


def _command(*arguments: str) -> str:
    result = subprocess.run(arguments, check=False, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip()
    return result.stdout.strip()


def test_supported_host_floors_are_observed_before_mutation() -> None:
    assert sys.version_info >= (3, 12)
    system = platform.system()
    assert system in {"Darwin", "Linux"}
    if system == "Linux":
        assert Path("/sys/fs/cgroup/cgroup.controllers").is_file()

    api = _command("docker", "version", "--format", "{{json .Server.APIVersion}}")
    compose = _command("docker", "compose", "version", "--short")
    buildx = _command("docker", "buildx", "version")
    assert isinstance(json.loads(api), str)
    assert compose[0].isdigit()
    assert "buildx" in buildx.casefold()
    clock = NativeClock()
    assert clock.read() > 0
    assert clock.origin.startswith("darwin-continuous:" if system == "Darwin" else "linux-boottime:")
