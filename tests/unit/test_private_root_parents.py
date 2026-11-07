"""A finished run takes its empty project and session folders with it."""

from __future__ import annotations

import dataclasses
from pathlib import Path

import pytest

from docker_test_toolkit.application import lifecycle
from docker_test_toolkit.factories.testing import build_test_services
from tests.integration.account_fixtures import wait_guardians_exited
from tests.unit.test_testing_factory import _enter, _spec


def _isolate(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Give this test its own temporary parent: under the shared one the top folder is never empty, so an
    implementation that also removed it would pass by accident."""
    monkeypatch.setenv("TMPDIR", str(tmp_path / "temporary"))
    return (tmp_path / "temporary").resolve() / "docker-test-toolkit"


def _assert_only_top_left(root: Path, top: Path) -> None:
    session_folder = root.parent.parent
    assert session_folder.parent == top
    assert not session_folder.exists()
    assert top.is_dir() and not any(top.iterdir())


def test_leave_removes_the_run_root_and_its_empty_parents_but_keeps_the_shared_folder(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    top = _isolate(monkeypatch, tmp_path)
    services, handle = _enter(tmp_path)
    root = handle.private_root
    assert root.is_dir()

    handle.leave()
    wait_guardians_exited(services.root / "registry")

    _assert_only_top_left(root, top)


def test_cancel_of_an_unentered_run_removes_its_empty_parents(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    top = _isolate(monkeypatch, tmp_path)
    project = tmp_path / "project"
    project.mkdir()
    handle = build_test_services(tmp_path / "state").open_run(_spec(project))
    root = handle.private_root
    assert root.is_dir()

    handle.cancel()

    _assert_only_top_left(root, top)


def test_a_sibling_run_in_the_same_session_keeps_its_folders_until_it_finishes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    top = _isolate(monkeypatch, tmp_path)
    project = tmp_path / "project"
    project.mkdir()
    services = build_test_services(tmp_path / "state")
    first = services.open_run(_spec(project))
    sibling = lifecycle.prepare_run(
        lifecycle.RunRequest(
            project, "integration", "private-proof", (), (), "fixture", parent_session=first.session_id
        ),
        _test_state=services.root / "registry",
        _test_directory=services.root / "admission",
    )
    assert sibling.private_root.parent == first.private_root.parent

    first.cancel()

    assert not first.private_root.exists()
    assert sibling.private_root.is_dir()

    lifecycle.cancel_run(sibling)

    _assert_only_top_left(sibling.private_root, top)


@pytest.mark.parametrize(
    "layout",
    [
        pytest.param(("elsewhere", "{session}", "{project}"), id="top-folder-not-the-toolkits"),
        pytest.param(("docker-test-toolkit", "{project}", "{session}"), id="session-and-project-swapped"),
    ],
)
def test_a_root_without_the_toolkit_shape_keeps_its_ancestors(tmp_path: Path, layout: tuple[str, str, str]) -> None:
    project = tmp_path / "project"
    project.mkdir()
    services = build_test_services(tmp_path / "state")
    handle = services.open_run(_spec(project))
    context = handle._context
    names = {"session": context.session_id, "project": context.project.project_id}
    top, first, second = (part.format(**names) for part in layout)
    root = tmp_path / "shape" / top / first / second / context.run_id
    root.mkdir(mode=0o700, parents=True)
    for folder in (root, root.parent, root.parent.parent, root.parent.parent.parent):
        folder.chmod(0o700)

    lifecycle._remove_private_root(dataclasses.replace(context, private_root=root))

    assert not root.exists()
    assert root.parent.is_dir() and root.parent.parent.is_dir()
    handle.cancel()


def test_toolkit_test_runs_keep_their_run_folders_inside_pytests_temporary_area(
    tmp_path: Path, tmp_path_factory: pytest.TempPathFactory
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    handle = build_test_services(tmp_path / "state").open_run(_spec(project))

    assert handle.private_root.is_relative_to(tmp_path_factory.getbasetemp().resolve())
    handle.cancel()
