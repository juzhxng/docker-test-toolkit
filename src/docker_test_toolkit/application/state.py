"""Native account-state location and identity boundaries."""

import os
from pathlib import Path

from docker_test_toolkit.config.installation import imported_through_current, installation_of
from docker_test_toolkit.domain.errors import IncompatibleState, ResourceError


def containing_zipapp(module_file: str) -> Path | None:
    """Return the ``.pyz`` archive a module file was imported from (its rightmost zipapp boundary), or None."""
    location = str(Path(module_file).absolute())
    marker = ".pyz" + os.sep
    if marker not in location:
        return None
    candidate = Path(location.rsplit(marker, 1)[0] + ".pyz")
    return candidate if candidate.is_file() else None


RUNNING_ARCHIVE = containing_zipapp(__file__)


def canonical_account() -> Path | None:
    """Return the real account of the running installation, or None when this code runs outside one. Never raises."""
    try:
        installation = installation_of(RUNNING_ARCHIVE) if RUNNING_ARCHIVE is not None else None
    except OSError:  # an unreadable installation is no installation here; account_directory refuses with its reason
        return None
    return installation.account_directory if installation is not None else None


def account_directory() -> Path:
    """Return the real account of the running installation (``<root>/state/test-admission``).

    Raises:
        ResourceError: ``toolkit-not-installed`` when this code runs outside an installation.
        IncompatibleState: ``toolkit-load-unresolved`` when it was imported through ``current`` (invariant I2).
    """
    account = canonical_account()
    if account is None:
        raise ResourceError(
            "The toolkit is not installed: the real account belongs to an installation; "
            "install the toolkit, or use a private test composition.",
            reason="toolkit-not-installed",
        )
    if RUNNING_ARCHIVE is not None and imported_through_current(RUNNING_ARCHIVE):
        raise IncompatibleState(
            "The toolkit was loaded through the installation's current link, which an install may switch; "
            "load the resolved version folder instead (the loader and the launcher do).",
            reason="toolkit-load-unresolved",
        )
    return account


def overlaps_account(directory: Path, account: Path) -> bool:
    """Return whether a directory is, lies in or contains the account (both resolved non-strictly, links followed)."""
    candidate, real = directory.resolve(), account.resolve()
    return candidate == real or real in candidate.parents or candidate in real.parents


def registry_directory() -> Path:
    """Return the real project registry: the installation's state folder, which holds the account.

    Raises:
        ResourceError: ``toolkit-not-installed`` when this code runs outside an installation.
    """
    return account_directory().parent


def make_private_directories(path: Path) -> None:
    """Create ``path`` and every missing parent with mode 0700.

    ``Path.mkdir(parents=True)`` applies its mode only to the last component, so on a fresh account (a CI runner)
    the parents followed the umask and failed the later privacy checks. Creating each component with 0700 is enough:
    a umask only clears bits, so no umask adds group or other access. Existing directories, including ones another
    process creates concurrently, are left unchanged for the caller's own check, never repaired. This is
    ``Path.mkdir(parents=True, exist_ok=True)``'s own algorithm with the mode applied to every created level, so a
    non-directory in the way raises the same native error.
    """
    try:
        path.mkdir(mode=0o700, exist_ok=True)
    except FileNotFoundError:
        if path.parent == path:
            raise
        make_private_directories(path.parent)
        path.mkdir(mode=0o700, exist_ok=True)


def archive_directory(registry: Path) -> Path:
    """Return the retired-record archive that sits beside one registry root."""
    return registry.parent / f"{registry.name}-archive"
