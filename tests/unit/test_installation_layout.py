"""Installation layout rules: the installation is derived from an archive path alone."""

from __future__ import annotations

from pathlib import Path

import pytest

from docker_test_toolkit.config.installation import (
    ARCHIVE_NAME,
    HOME_ROOT,
    LINUX_ROOT,
    Installation,
    current_target,
    imported_through_current,
    installation_of,
    platform_root,
)
from docker_test_toolkit.domain.errors import ResourceError


def _installed(root: Path, version: str = "0.5.0") -> Path:
    folder = root / version
    folder.mkdir(parents=True)
    archive = folder / ARCHIVE_NAME
    archive.write_bytes(b"archive")
    (root / "current").symlink_to(version)
    return archive


def test_installation_is_derived_from_the_version_archive(tmp_path: Path) -> None:
    root = tmp_path / "root"
    archive = _installed(root)

    installation = installation_of(archive)

    assert installation == Installation(root=root, version_folder=root / "0.5.0", archive=archive)
    assert installation.state_directory == root / "state"
    assert installation.account_directory == root / "state" / "test-admission"
    assert installation.launcher == root / "bin" / "docker-test-toolkit"
    assert current_target(root) == (root / "0.5.0").resolve()


def test_archive_outside_an_installation_has_none(tmp_path: Path) -> None:
    loose = tmp_path / "dist" / ARCHIVE_NAME
    loose.parent.mkdir()
    loose.write_bytes(b"archive")
    renamed = _installed(tmp_path / "root").with_name("other.pyz")

    assert installation_of(loose) is None
    assert installation_of(renamed) is None
    assert current_target(tmp_path / "dist") is None


def test_archive_imported_through_current_is_detected(tmp_path: Path) -> None:
    root = tmp_path / "root"
    _installed(root)

    assert imported_through_current(root / "current" / ARCHIVE_NAME)
    assert not imported_through_current(root / "0.5.0" / ARCHIVE_NAME)


def test_symlinked_parent_without_current_is_not_flagged(tmp_path: Path) -> None:
    real = tmp_path / "real"
    (real / "0.5.0").mkdir(parents=True)
    (tmp_path / "linked").symlink_to(real)  # like macOS /var -> /private/var: a symlink, but not named "current"

    assert not imported_through_current(tmp_path / "linked" / "0.5.0" / ARCHIVE_NAME)
    assert not imported_through_current(Path("/var/folders") / ARCHIVE_NAME)


def test_a_directory_named_current_is_not_a_symlink(tmp_path: Path) -> None:
    (tmp_path / "current").mkdir()

    assert not imported_through_current(tmp_path / "current" / ARCHIVE_NAME)


@pytest.mark.parametrize(
    ("platform", "expected"),
    [("darwin", "home"), ("linux", "linux")],
)
def test_platform_root_per_platform(tmp_path: Path, platform: str, expected: str) -> None:
    root = platform_root(tmp_path, platform)

    assert root == (tmp_path / HOME_ROOT if expected == "home" else LINUX_ROOT)


def test_platform_root_refuses_other_platforms(tmp_path: Path) -> None:
    with pytest.raises(ResourceError):
        platform_root(tmp_path, "win32")


def test_canonical_account_never_raises_on_an_unreadable_installation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from docker_test_toolkit.application import state

    archive = _installed(tmp_path / "root")
    monkeypatch.setattr(state, "RUNNING_ARCHIVE", archive)  # as if this code ran from that archive
    assert state.canonical_account() == tmp_path / "root" / "state" / "test-admission"
    # The installation cannot be read: an over-long name fails the lookup with an OSError for every user, root included
    # (a permission denial would not stop root, which is the user of the CI job containers).
    monkeypatch.setattr(state, "RUNNING_ARCHIVE", tmp_path / ("x" * 300) / "0.5.0" / ARCHIVE_NAME)

    assert state.canonical_account() is None
    with pytest.raises(ResourceError) as refused:
        state.account_directory()
    assert refused.value.reason == "toolkit-not-installed"
