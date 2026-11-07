"""Installation layout: versioned immutable folders, a ``current`` link and the state folder.

Everything here is derived from paths alone, with ``lstat`` and ``readlink``; nothing is read or written.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from docker_test_toolkit.domain.errors import ResourceError

ARCHIVE_NAME = "docker-test-toolkit.pyz"
HOME_ROOT = Path(".local/share/docker-test-toolkit")
LINUX_ROOT = Path("/opt/docker-test-toolkit")


@dataclass(frozen=True)
class Installation:
    """One installation root and the version folder whose archive is running."""

    root: Path
    version_folder: Path
    archive: Path

    @property
    def state_directory(self) -> Path:
        """The account state folder shared by every version of this installation."""
        return self.root / "state"

    @property
    def account_directory(self) -> Path:
        """The admission account inside the state folder."""
        return self.state_directory / "test-admission"

    @property
    def launcher(self) -> Path:
        """The installation's command entry point."""
        return self.root / "bin" / "docker-test-toolkit"


def platform_root(home: Path, platform: str) -> Path:
    """Return the installation root of a platform: the home root on macOS, ``/opt`` on Linux.

    Raises:
        ResourceError: The platform has no installation root.
    """
    if platform == "darwin":
        return home / HOME_ROOT
    if platform.startswith("linux"):
        return LINUX_ROOT
    raise ResourceError(f"The toolkit is not installable on platform {platform!r}.")


def installation_of(archive: Path) -> Installation | None:
    """Return the installation an archive belongs to, or None for an archive outside an installation."""
    root = archive.parent.parent
    if archive.name != ARCHIVE_NAME or not (root / "current").is_symlink():
        return None
    return Installation(root=root, version_folder=archive.parent, archive=archive)


def current_target(root: Path) -> Path | None:
    """Return the resolved version folder ``root/current`` points at, or None when there is no such link."""
    link = root / "current"
    return Path(os.path.realpath(link)) if link.is_symlink() else None


def imported_through_current(import_path: Path) -> bool:
    """Return whether some component of the path is a symlink named ``current`` (the path follows the switch)."""
    path = Path(os.path.abspath(import_path))
    return any(prefix.name == "current" and prefix.is_symlink() for prefix in (path, *path.parents))
