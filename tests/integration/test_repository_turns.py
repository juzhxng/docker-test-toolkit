"""Repository turn grace through real guardian processes on a private account.

The private account runs at repository share scope above limit 1, with the turn grace bound short; each of the three
private projects is a checkout of its own repository.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from docker_test_toolkit import PhaseBudget, RunSpec
from docker_test_toolkit.application import sharing
from docker_test_toolkit.factories.testing import bind_test_capacity
from tests.integration.account_fixtures import EnterThread, queue_phases, source_artifact, three_private_projects

pytestmark = pytest.mark.integration

GRACE_NS = 3_000_000_000


def _spec(project: Path, purpose: str) -> RunSpec:
    return RunSpec(
        project_root=project,
        entrypoint="integration",
        purpose=purpose,
        services=(),
        closure=(),
        profile="fixture",
        phases={"tests": PhaseBudget(1024**3, 1, 1, 1)},
        initial_phase="tests",
        artifact=source_artifact(),
        wait_seconds=60,
    )


def _account(tmp_path: Path, *, scope: str = "repository", limit: int = 3):
    services, projects = three_private_projects(tmp_path)
    account = services.root / "admission"
    sharing.record_share_scope(account, scope)
    sharing.record_parallel_runs(account, limit)
    bind_test_capacity(services, turn_grace_ns=GRACE_NS)
    return services, projects, account


def _queued(services, project: Path, purpose: str, account: Path):
    handle = services.open_run(_spec(project, purpose))
    entering = EnterThread(handle)
    deadline = time.monotonic() + 15
    while queue_phases(account).get(handle.run_id) is None:
        assert time.monotonic() < deadline, f"{purpose} never entered the queue"
        time.sleep(0.05)
    return handle, entering


def _granted(account: Path, run_id: str, timeout: float = 30) -> None:
    deadline = time.monotonic() + timeout
    while queue_phases(account).get(run_id) != "granted":
        assert time.monotonic() < deadline, f"{run_id} was never granted: {queue_phases(account)}"
        time.sleep(0.05)


def _stays_queued(account: Path, run_id: str, seconds: float) -> None:
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        assert queue_phases(account).get(run_id) == "queued", queue_phases(account)
        time.sleep(0.05)


def _waited(handle, reason: str) -> bool:
    waiting = json.loads(handle._context.record.read_text())["hold_timeline"]["waiting"]
    return reason in {item["reason"] for item in waiting}


def test_the_fifo_head_of_another_repository_waits_for_a_valid_turn(tmp_path: Path) -> None:
    services, (one, two, _), account = _account(tmp_path)
    bind_test_capacity(services, turn_grace_ns=600_000_000_000)  # long enough for any process start
    first = services.open_run(_spec(one, "one-a")).enter()
    other, other_entering = _queued(services, two, "two", account)  # the FIFO head once the holder leaves

    first.leave()
    assert (account / sharing.TURNS_FILE).exists()  # written with the leaver's queue removal
    _stays_queued(account, other.run_id, 1.0)  # the turn holds the head for one's next batch
    second, second_entering = _queued(services, one, "one-b", account)
    second_entering.join(timeout=30)

    assert second_entering.error is None and queue_phases(account)[other.run_id] == "queued"
    second.leave()
    bind_test_capacity(services, turn_grace_ns=GRACE_NS)  # now let the refreshed turn run out
    other_entering.join(timeout=30)  # it expires with no batch of one queued: two goes
    assert other_entering.error is None and _waited(other, "repository-turn")
    other.leave()


def test_a_turn_expires_and_the_other_head_goes(tmp_path: Path) -> None:
    services, (one, two, _), account = _account(tmp_path)
    first = services.open_run(_spec(one, "one-a")).enter()
    other, entering = _queued(services, two, "two", account)

    left = time.monotonic()
    first.leave()
    _granted(account, other.run_id)

    assert time.monotonic() - left >= GRACE_NS / 1e9 - 0.5  # it waited for the grace, then went (±1 pass)
    entering.join(timeout=10)
    assert entering.error is None
    other.leave()


def test_turns_are_ignored_once_the_oldest_waited_the_window(tmp_path: Path) -> None:
    services, (one, two, _), account = _account(tmp_path)
    bind_test_capacity(services, turn_grace_ns=60_000_000_000, overtake_window_ns=1_000_000_000)
    first = services.open_run(_spec(one, "one-a")).enter()
    other, entering = _queued(services, two, "two", account)
    time.sleep(1.5)  # the head has now waited past the bound window

    first.leave()
    entering.join(timeout=15)  # far inside the 60 s turn: only the window lets it go

    assert entering.error is None and not _waited(other, "repository-turn")
    other.leave()


@pytest.mark.parametrize(("scope", "limit"), [("machine", 3), ("repository", 1)])
def test_no_turn_is_written_under_machine_scope_or_limit_one(tmp_path: Path, scope: str, limit: int) -> None:
    services, (one, _two, _), account = _account(tmp_path, scope=scope, limit=limit)

    services.open_run(_spec(one, "one-a")).enter().leave()

    assert not (account / sharing.TURNS_FILE).exists()


def test_a_turn_of_another_daemon_is_ignored(tmp_path: Path) -> None:
    from docker_test_toolkit.application.recovery import atomic_json

    services, (one, two, _), account = _account(tmp_path)
    bind_test_capacity(services, turn_grace_ns=60_000_000_000)
    first = services.open_run(_spec(one, "one-a")).enter()
    other, entering = _queued(services, two, "two", account)
    first.leave()
    turns = json.loads((account / sharing.TURNS_FILE).read_text())
    for turn in turns["turns"].values():
        turn["daemon_id"] = "another-daemon"  # as if the daemon changed since the leave
    atomic_json(account / sharing.TURNS_FILE, turns)

    entering.join(timeout=15)  # far inside the 60 s grace: only the daemon mismatch lets it go

    assert entering.error is None
    other.leave()
