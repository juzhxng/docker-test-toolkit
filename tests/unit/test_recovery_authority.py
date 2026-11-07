"""Recovery authority over an interrupted retained release: the matched release record's guardian decides.

``_verify_interrupted_release`` scans the run directory for the release helper's record when the target names no
release authority. The records are built through the private test composition and direct record writes; no Docker
port is involved. The stale candidate sorts after the matched one, so a check that reads the loop variable instead of
the matched record reads the stale guardian.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from docker_test_toolkit import PhaseBudget, RunSpec
from docker_test_toolkit.application import lifecycle
from docker_test_toolkit.application.recovery import atomic_json
from docker_test_toolkit.domain.errors import ResourceBusy
from docker_test_toolkit.factories.testing import build_test_services
from tests.integration.account_fixtures import (
    dead_identity,
    reap,
    source_artifact,
    start_live_actor,
    wait_guardians_exited,
)


def _left_run(services, project: Path, purpose: str):
    spec = RunSpec(
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
    )
    handle = services.open_run(spec).enter()
    handle.leave("cancelled")
    return handle


def _release_shaped(context: lifecycle.RunContext, guardian: dict, purpose: str = "retained-release") -> dict:
    """Rewrite a completed run's record into the shape a release helper leaves behind; return the new record."""
    record = json.loads(context.record.read_text())
    record.update(
        entrypoint="release",
        purpose=purpose,
        profile="retained-release",
        services=[],
        closure=[],
        mode="managed",
        retain=False,
        artifact=None,
        test_outcome="cancelled",
        guardian=guardian,
    )
    record["grant"].update(guardian=guardian, requirements_identity=lifecycle._requirements_identity(record))
    atomic_json(context.record, record)
    return record


def _interrupted_release(tmp_path: Path, *, matched_guardian: dict, stale_guardian: dict):
    """Return (target context, target data): one matched release record and one stale candidate sorted after it."""
    services = build_test_services(tmp_path / "state")
    project = tmp_path / "project"
    project.mkdir()
    first, second, target = (_left_run(services, project, name) for name in ("release-a", "release-b", "target"))
    wait_guardians_exited(services.root / "registry")
    matched, stale = sorted((first._context, second._context), key=lambda context: str(context.record))
    release = _release_shaped(matched, matched_guardian)
    _release_shaped(stale, stale_guardian, purpose="stale-release")  # requester matches; the shape does not
    data = json.loads(target._context.record.read_text())
    data.update(
        release_authority=None,
        recovery_actor=release["requester"],
        session_id=release["session_id"],
        source_identity=release["source_identity"],
    )
    return target._context, data, matched


def test_interrupted_release_refuses_a_live_guardian_of_the_matched_record(tmp_path: Path) -> None:
    helper, live = start_live_actor("matched-guardian")
    try:
        context, data, _matched = _interrupted_release(tmp_path, matched_guardian=live, stale_guardian=dead_identity())

        with pytest.raises(ResourceBusy) as refused:
            lifecycle._verify_interrupted_release(context, data)
    finally:
        reap(helper)

    assert refused.value.reason == "actor-live"


def test_interrupted_release_ignores_a_stale_candidate_guardian(tmp_path: Path) -> None:
    helper, live = start_live_actor("stale-guardian")
    try:
        context, data, matched = _interrupted_release(tmp_path, matched_guardian=dead_identity(), stale_guardian=live)

        authority = lifecycle._verify_interrupted_release(context, data)
    finally:
        reap(helper)

    assert authority["run_id"] == matched.run_id and authority["record"] == str(matched.record)
