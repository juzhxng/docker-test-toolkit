from __future__ import annotations

from pathlib import Path
from typing import cast

import pytest

from docker_test_toolkit import ArtifactIdentity, PhaseBudget, RunSpec
from docker_test_toolkit.application.operator import run_spec_from_payload
from docker_test_toolkit.domain.errors import InvalidInput, ToolkitError

_PROTOCOLS = {
    "activation": 1,
    "batch": 1,
    "cli": 1,
    "distribution": 1,
    "queue": 2,
    "receipt": 1,
    "registry": 2,
    "run": 1,
}


def _identity() -> ArtifactIdentity:
    return ArtifactIdentity(version="0.1.0", sha256="a" * 64, protocols=dict(_PROTOCOLS))


def _spec(root: Path, *, capacity: int = 1, retain: bool = False) -> RunSpec:
    return RunSpec(
        project_root=root,
        entrypoint="unit",
        purpose="capacity-model",
        services=(),
        closure=(),
        profile="fixture",
        phases={"tests": PhaseBudget(0, 1, 1, 1)},
        initial_phase="tests",
        artifact=_identity(),
        retain=retain,
        capacity=capacity,
    )


def _payload(root: Path, **extra: object) -> dict:
    return {
        "project_root": str(root),
        "entrypoint": "unit",
        "purpose": "capacity-model",
        "services": [],
        "closure": [],
        "profile": "fixture",
        "phases": {"tests": {"container_bytes": 0, "host_process_bytes": 1, "daemon_cpus": 1, "host_threads": 1}},
        "initial_phase": "tests",
        "artifact": {"version": "0.1.0", "sha256": "a" * 64, "protocols": dict(_PROTOCOLS)},
        "external_resources": [],
        "retain": False,
        "wait_seconds": 20,
        **extra,
    }


def test_run_spec_capacity_is_any_integer_and_ignored(tmp_path: Path) -> None:
    for value in (1, 2, 7, 0, -1):
        assert _spec(tmp_path, capacity=value).capacity == value
    for invalid in (True, False, 2.0, "2", None):
        with pytest.raises(ToolkitError, match="capacity"):
            _spec(tmp_path, capacity=cast(int, invalid))


def test_bridge_payload_capacity_is_accepted_and_ignored(tmp_path: Path) -> None:
    assert run_spec_from_payload(_payload(tmp_path)).capacity == 1
    for value in (1, 2, 7, "2", None):
        assert run_spec_from_payload(_payload(tmp_path, capacity=value)).capacity == 1
    assert run_spec_from_payload(_payload(tmp_path, unknown=1)) == run_spec_from_payload(_payload(tmp_path))
    with pytest.raises(InvalidInput):
        run_spec_from_payload({key: value for key, value in _payload(tmp_path).items() if key != "retain"})
