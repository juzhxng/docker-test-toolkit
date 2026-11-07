"""The leave contract after the acknowledgment: the reported reason, the verification's locks and the one deadline.

Design 2.1: after the barrier the requester verifies retirement while holding the project registry lock and,
nested inside it, the account metadata lock; every lock taken after the durable withdrawal ends at one deadline, the
recorded remaining budget plus a fixed grace; a reference still present is reported as ``retirement-incomplete``.
"""

from __future__ import annotations

import time
from dataclasses import asdict
from pathlib import Path

from docker_test_toolkit import PhaseBudget, RunSpec
from docker_test_toolkit.application import lifecycle
from docker_test_toolkit.application.protocol import error_envelope
from docker_test_toolkit.domain.errors import ResourceError
from docker_test_toolkit.factories.testing import build_test_services, install_lock_observer
from tests.integration.account_fixtures import source_artifact

S = 1_000_000_000
TOLERANCE = 50_000_000


def test_retirement_incomplete_is_reported_by_the_envelope() -> None:
    envelope, exit_code = error_envelope(
        "run.leave", ResourceError("Leave acknowledged, but retirement is incomplete.", reason="retirement-incomplete")
    )

    assert envelope["error"]["context"]["reason"] == "retirement-incomplete"
    assert envelope["error"]["category"] == "incompatible_state"
    assert exit_code == 3


def test_verification_holds_the_registry_lock_across_the_metadata_read(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    spec = RunSpec(
        project_root=project,
        entrypoint="integration",
        purpose="leave-lock-order",
        services=(),
        closure=(),
        profile="fixture",
        phases={"tests": PhaseBudget(0, 1, 1, 1)},
        initial_phase="tests",
        artifact=source_artifact(),
        wait_seconds=30,
    )
    handle = build_test_services(tmp_path / "state").open_run(spec).enter()
    run_lock = f"run-{handle.run_id}"
    events: list[tuple[str, str, bool, bool, bool]] = []
    install_lock_observer(lambda *event: events.append(event))
    try:
        handle.leave()
    finally:
        install_lock_observer(None)

    barrier_end = max(index for index, event in enumerate(events) if event[:2] == ("release", run_lock))
    after = [event for event in events[barrier_end + 1 :] if event[1] in ("registry", "metadata")]
    assert [event[:2] for event in after] == [
        ("acquire", "registry"),
        ("acquire", "metadata"),
        ("release", "metadata"),
        ("release", "registry"),
    ]
    assert all(event[4] is True for event in after if event[0] == "acquire")  # both waits are bounded


def test_leave_deadline_adds_the_grace_to_the_recorded_remaining() -> None:
    clock = lifecycle.NativeClock()
    withdrawal = {"deadline": asdict(lifecycle.Deadline.after(clock, 3))}

    now = time.monotonic_ns()
    deadline_ns = lifecycle._leave_deadline_ns(withdrawal)
    assert now + 8 * S - TOLERANCE <= deadline_ns <= now + 8 * S + TOLERANCE

    # A valid origin of another boot: the remaining budget is unknowable and counts as zero.
    foreign = dict(withdrawal["deadline"], origin="darwin-continuous:00000000-0000-4000-8000-000000000000")
    assert foreign["origin"] != clock.origin
    now = time.monotonic_ns()
    deadline_ns = lifecycle._leave_deadline_ns({"deadline": foreign})
    assert now + 5 * S - TOLERANCE <= deadline_ns <= now + 5 * S + TOLERANCE
