"""A leaving run's repository turn: written with the grace that decides it; never fails a leave."""

from __future__ import annotations

from pathlib import Path

import pytest

from docker_test_toolkit.application import lifecycle
from docker_test_toolkit.application.sharing import AdmissionSettings

DATA = {"grant": {"daemon_id": "daemon-1"}, "project_root": "/srv/project-a", "run_id": "run-1"}


@pytest.fixture
def repository_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(lifecycle, "read_admission_settings", lambda _directory: AdmissionSettings(3, 1, "repository"))
    monkeypatch.setattr(lifecycle, "repository_identity", lambda _root: "git.example/team/a")


@pytest.mark.usefixtures("repository_scope")
def test_the_turn_is_written_with_the_grace_that_decides_it(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    written: dict[str, object] = {}
    monkeypatch.setattr(lifecycle, "write_turns", lambda _directory, _turns, **options: written.update(options))
    monkeypatch.setattr(lifecycle, "_test_capacity_record", lambda _directory: {"turn_grace_ns": 600_000_000_000})

    lifecycle._record_turn(tmp_path, DATA, tmp_path)  # a private account binds its own grace
    bound = written["grace_ns"]
    lifecycle._record_turn(tmp_path, DATA, None)  # a real account uses the default

    assert (bound, written["grace_ns"]) == (600_000_000_000, lifecycle.TURN_GRACE_NS)


@pytest.mark.usefixtures("repository_scope")
def test_a_failed_turn_write_never_fails_the_leave(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def full_disk(*_arguments: object, **_options: object) -> None:
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(lifecycle, "write_turns", full_disk)

    lifecycle._record_turn(tmp_path, DATA, None)  # the leave goes on; the next batch simply has no turn
