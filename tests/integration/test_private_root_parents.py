"""Across processes: a run being prepared beside a finishing one."""

from __future__ import annotations

import stat
import threading
from pathlib import Path

import pytest

from docker_test_toolkit.application import lifecycle
from tests.integration.account_fixtures import (
    private_account,
    run_context_of,
    start_prepare_child,
    wait_until,
    write_v2_queue,
)

pytestmark = pytest.mark.integration


def _mode(path: Path) -> int:
    return stat.S_IMODE(path.lstat().st_mode)


def test_a_sibling_preparing_while_its_session_is_pruned_recreates_its_folders(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Isolated temporary parent, inherited by the child: the top folder's survival is then decisive.
    monkeypatch.setenv("TMPDIR", str(tmp_path / "temporary"))
    top = (tmp_path / "temporary").resolve() / "docker-test-toolkit"
    service = private_account(tmp_path)
    write_v2_queue(service.account_directory, [])
    project = tmp_path / "project"
    project.mkdir()
    first = lifecycle.prepare_run(
        lifecycle.RunRequest(project, "integration", "first", (), (), "fixture"),
        _test_state=service.state_directory,
        _test_directory=service.account_directory,
    )
    session_folder = first.private_root.parent.parent
    paused, release = tmp_path / "sibling.paused", tmp_path / "sibling.release"
    # The sibling stops after writing its record, holding the registry lock, before it creates its own root.
    child, _result = start_prepare_child(
        tmp_path,
        service,
        project,
        {
            "prepare-record-written": {
                "action": [{"do": "write-file", "path": str(paused)}, {"do": "wait-file", "path": str(release)}]
            }
        },
        name="sibling",
        parent_session=first.session_id,
    )
    cancel = threading.Thread(target=lifecycle.cancel_run, args=(first,))
    try:
        wait_until(paused.exists, timeout=30, message="the sibling paused before creating its root")
        cancel.start()  # removal and prune need no registry lock; the index step then waits for the sibling
        wait_until(lambda: not session_folder.exists(), timeout=30, message="the first run pruned its session")
        release.write_text("")
        assert child.wait(timeout=30) == 0
        cancel.join(timeout=30)
        assert not cancel.is_alive()
    finally:
        release.write_text("")
        if child.poll() is None:
            child.kill()
            child.wait(timeout=30)
        if cancel.is_alive():
            cancel.join(timeout=30)

    sibling = run_context_of(service, project, {first.run_id})
    assert sibling.private_root.parent.parent == session_folder
    for folder in (sibling.private_root, sibling.private_root.parent, session_folder):
        assert folder.is_dir() and _mode(folder) == 0o700
    assert not first.private_root.exists()

    # recovery of the sibling, whose requester has exited, removes its root and both parents.
    lifecycle.recover_run(sibling, _test_directory=service.account_directory)
    assert not session_folder.exists()
    assert top.is_dir() and not any(top.iterdir())
