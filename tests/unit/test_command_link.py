"""``install`` links the command where the platform's users find it, and never fails for lack of that folder."""

from __future__ import annotations

from pathlib import Path

from docker_test_toolkit.application.installation import command_link_path


def test_macos_links_into_the_users_own_folder(tmp_path: Path) -> None:
    assert command_link_path(tmp_path, "darwin") == tmp_path / ".local/bin/docker-test-toolkit"


def test_linux_links_into_a_writable_system_folder(tmp_path: Path) -> None:
    assert command_link_path(Path("/home/x"), "linux", system_bin=tmp_path) == tmp_path / "docker-test-toolkit"


def test_linux_without_a_writable_system_folder_links_nothing(tmp_path: Path) -> None:
    assert command_link_path(Path("/home/x"), "linux", system_bin=tmp_path / "absent") is None
