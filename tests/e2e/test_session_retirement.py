"""Prove the session sweep end to end: an abandoned preparation is recovered, cleaned and retired."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import time
from collections.abc import Callable
from pathlib import Path

import pytest

from docker_test_toolkit.application.state import archive_directory
from docker_test_toolkit.config.installation import installation_of

pytestmark = pytest.mark.e2e
ROOT = Path(__file__).parents[2]
LOADER = ROOT / "docs" / "loader"

WORKER = r"""
import json, os, pathlib, sys

project, loader_directory, installation_root, control = map(pathlib.Path, sys.argv[1:5])
sys.path.insert(0, str(loader_directory))
import docker_test_toolkit_loader as loader

toolkit, _archive = loader.load_toolkit((installation_root,))
identity = toolkit.toolkit_identity()
handle = toolkit.open_run(
    toolkit.RunSpec(
        project_root=project,
        entrypoint="e2e",
        purpose="abandoned-preparation",
        services=(),
        closure=(),
        profile="session-retirement",
        phases={"tests": toolkit.PhaseBudget(0, 1, 1, 1)},
        initial_phase="tests",
        artifact=identity,
    )
)
control.write_text(json.dumps({"run_id": handle.run_id}))
os._exit(0)  # a declared, prepared, never-enrolled run whose requester is now dead
"""


def test_end_sweep_recovers_cleans_and_retires_an_abandoned_preparation(
    tmp_path: Path, account_sweep: Callable[..., list[str]], session_installation: Path
) -> None:
    base = (tmp_path / "sweep-base").resolve()
    project = base / "abandoned"
    project.mkdir(parents=True)
    control = tmp_path / "control.json"
    installation = installation_of(session_installation)
    assert installation is not None
    base_python = str(Path(getattr(sys, "_base_executable", sys.executable)).resolve())
    subprocess.run(
        [base_python, "-I", "-c", WORKER, str(project), str(LOADER), str(installation.root), str(control)],
        check=True,
        timeout=180,
    )
    run_id = json.loads(control.read_text())["run_id"]
    registry = installation.state_directory
    project_id = hashlib.sha256(str(project).encode()).hexdigest()
    assert (registry / "projects" / f"{project_id}.json").exists()
    assert (registry / "runs" / f"{run_id}.json").exists()

    leftovers = account_sweep(base, "end", time.monotonic() + 120)

    assert leftovers == []
    assert not project.exists()
    assert not (registry / "projects" / f"{project_id}.json").exists()
    assert not (registry / "runs" / f"{run_id}.json").exists()
    # A fixed basetemp reuses the project path (so the project id and archive directory): older runs may be there.
    with os.scandir(archive_directory(registry) / project_id / "runs") as listing:  # a listing error must surface
        archived = sorted(Path(entry.path) for entry in listing if entry.name.startswith(f"{run_id}-"))
    assert len(archived) == 1 and archived[0].name.endswith(".json")
    digest = archived[0].name.removesuffix(".json").rsplit("-", 1)[-1]
    assert hashlib.sha256(archived[0].read_bytes()).hexdigest() == digest
