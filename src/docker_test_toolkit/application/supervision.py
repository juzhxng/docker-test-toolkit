"""Lifecycle-owned process environment and executable-selection policy."""

from __future__ import annotations

import atexit
import grp
import os
import pwd
import shutil
import stat
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Mapping

from docker_test_toolkit.domain.errors import ResourceError

_BASE_ENVIRONMENT = {"HOME", "LANG", "LC_ALL", "PATH", "TMPDIR", "TMP", "TEMP"}
_RESERVED_ENVIRONMENT = {
    *_BASE_ENVIRONMENT,
    "BUILDX_BUILDER",
    "COMPOSE_PROJECT_NAME",
    "DOCKER_CERT_PATH",
    "DOCKER_CONFIG",
    "DOCKER_CONTEXT",
    "DOCKER_HOST",
    "DOCKER_TLS_VERIFY",
}


@dataclass(frozen=True)
class SafeSearchRoot:
    """Describe one fixed executable search directory and containment policy."""

    search: Path
    containment: Path
    trust: Literal["private", "local-admin", "system"]


def safe_search_roots(platform: str, home: Path) -> tuple[SafeSearchRoot, ...]:
    """Return the exact ordered search/containment roots for one supported host."""
    private = SafeSearchRoot(home / ".local/bin", home / ".local", "private")
    if platform == "darwin":
        return (
            private,
            SafeSearchRoot(Path("/opt/homebrew/bin"), Path("/opt/homebrew"), "local-admin"),
            SafeSearchRoot(Path("/usr/local/bin"), Path("/usr/local"), "local-admin"),
            SafeSearchRoot(Path("/usr/bin"), Path("/usr"), "system"),
            SafeSearchRoot(Path("/bin"), Path("/bin"), "system"),
            SafeSearchRoot(Path("/usr/sbin"), Path("/usr"), "system"),
            SafeSearchRoot(Path("/sbin"), Path("/sbin"), "system"),
        )
    if platform == "linux":
        return (
            private,
            SafeSearchRoot(Path("/usr/local/bin"), Path("/usr/local"), "local-admin"),
            SafeSearchRoot(Path("/usr/local/sbin"), Path("/usr/local"), "local-admin"),
            SafeSearchRoot(Path("/usr/bin"), Path("/usr"), "system"),
            SafeSearchRoot(Path("/usr/sbin"), Path("/usr"), "system"),
            SafeSearchRoot(Path("/bin"), Path("/bin"), "system"),
            SafeSearchRoot(Path("/sbin"), Path("/sbin"), "system"),
        )
    raise ResourceError("Supervisor platform is unsupported.")


def account_home() -> Path:
    """Return the canonical passwd-database home for the running account."""
    try:
        home = Path(pwd.getpwuid(os.getuid()).pw_dir).resolve(strict=True)
    except (KeyError, OSError, RuntimeError) as error:
        raise ResourceError("Real account home is unavailable.") from error
    if not home.is_dir():
        raise ResourceError("Real account home is unavailable.")
    return home


def fixed_environment(
    home: Path,
    temporary: Path,
    path: tuple[Path, ...],
    *,
    additions: Mapping[str, str] | None = None,
    private_additions: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Build one empty-derived child environment with exact allowed additions."""
    try:
        observed_temporary = temporary.stat(follow_symlinks=False)
    except OSError as error:
        raise ResourceError("Supervisor private temporary directory is unavailable.") from error
    if (
        not home.is_absolute()
        or not temporary.is_absolute()
        or not stat.S_ISDIR(observed_temporary.st_mode)
        or observed_temporary.st_uid != os.getuid()
        or stat.S_IMODE(observed_temporary.st_mode) != 0o700
        or any(not item.is_absolute() for item in path)
        or len(set(path)) != len(path)
    ):
        raise ResourceError("Supervisor fixed environment inputs are unsafe.")
    extra = dict(additions or {})
    private = dict(private_additions or {})
    if (
        any(
            not isinstance(name, str)
            or not name
            or "=" in name
            or "\0" in name
            or name in _RESERVED_ENVIRONMENT
            or name.startswith("DOCKER_TEST_TOOLKIT_")
            or not isinstance(value, str)
            or "\0" in value
            for name, value in extra.items()
        )
        or any(
            not isinstance(name, str)
            or not name
            or "=" in name
            or "\0" in name
            or not isinstance(value, str)
            or "\0" in value
            for name, value in private.items()
        )
        or set(extra) & set(private)
    ):
        raise ResourceError("Supervisor environment additions are invalid or reserved.")
    return {
        **extra,
        **private,
        "HOME": str(home),
        "LANG": "C",
        "LC_ALL": "C",
        "PATH": os.pathsep.join(map(str, path)),
        "TEMP": str(temporary),
        "TMP": str(temporary),
        "TMPDIR": str(temporary),
    }


def _inside(path: Path, root: Path) -> bool:
    return path == root or root in path.parents


def _admin_groups(platform: str) -> frozenset[int]:
    names = ("admin",) if platform == "darwin" else ("sudo", "wheel")
    values = set()
    for name in names:
        try:
            values.add(grp.getgrnam(name).gr_gid)
        except KeyError:
            continue
    return frozenset(values)


def _validate_directory(path: Path, trust: str, *, platform: str, uid: int) -> None:
    try:
        observed = path.stat(follow_symlinks=False)
    except OSError as error:
        raise ResourceError("Safe-path directory is unavailable.") from error
    mode = observed.st_mode
    if not stat.S_ISDIR(mode) or mode & stat.S_IWOTH:
        raise ResourceError("Safe-path directory type or mode is unsafe.")
    if trust == "private":
        if observed.st_uid != uid or mode & stat.S_IWGRP:
            raise ResourceError("Private safe-path directory ownership or mode is unsafe.")
        return
    if trust == "system":
        if observed.st_uid != 0 or mode & stat.S_IWGRP:
            raise ResourceError("System safe-path directory ownership or mode is unsafe.")
        return
    if observed.st_uid not in {0, uid} or (mode & stat.S_IWGRP and observed.st_gid not in _admin_groups(platform)):
        raise ResourceError("Local-admin safe-path directory ownership or mode is unsafe.")


def _resolved_search_roots(
    platform: str,
    home: Path,
) -> tuple[tuple[Path, Path, Literal["private", "local-admin", "system"]], ...]:
    resolved = []
    identities = set()
    for spec in safe_search_roots(platform, home):
        try:
            search = spec.search.resolve(strict=True)
            containment = spec.containment.resolve(strict=True)
        except FileNotFoundError:
            continue
        except (OSError, RuntimeError) as error:
            raise ResourceError("Safe-path directory cannot be resolved.") from error
        if spec.trust == "system" and spec.search in {Path("/bin"), Path("/sbin")} and _inside(search, Path("/usr")):
            containment = Path("/usr")
        if not _inside(search, containment):
            raise ResourceError("Safe-path directory escaped its containment root.")
        first = containment if spec.trust == "private" else Path("/")
        chain = []
        current = search
        while True:
            chain.append(current)
            if current == first:
                break
            if current == current.parent:
                raise ResourceError("Safe-path containment chain is incomplete.")
            current = current.parent
        for component in reversed(chain):
            _validate_directory(component, spec.trust, platform=platform, uid=os.getuid())
        observed = search.stat()
        identity = (observed.st_dev, observed.st_ino)
        if identity in identities:
            continue
        identities.add(identity)
        resolved.append((search, containment, spec.trust))
    return tuple(resolved)


def safe_path_directories(platform: str, home: Path) -> tuple[Path, ...]:
    """Return existing validated and device/inode-deduplicated search directories."""
    return tuple(item[0] for item in _resolved_search_roots(platform, home))


def account_environment(
    temporary: Path,
    *,
    additions: Mapping[str, str] | None = None,
    private_additions: Mapping[str, str] | None = None,
    platform: str | None = None,
) -> dict[str, str]:
    """Build the exact fixed environment for one private toolkit child."""
    home = account_home()
    selected_platform = sys.platform if platform is None else platform
    return fixed_environment(
        home,
        temporary,
        safe_path_directories(selected_platform, home),
        additions=additions,
        private_additions=private_additions,
    )


def _supervisor_temporary(parent: Path | None = None) -> Path:
    temporary = Path(tempfile.mkdtemp(prefix="docker-test-toolkit-supervisor-", dir=parent))
    temporary.chmod(0o700)
    return temporary


def private_process_environment(
    *,
    additions: Mapping[str, str] | None = None,
    private_additions: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Create one process-lifetime private temp and return its fixed environment."""
    try:
        temporary = _supervisor_temporary()
    except OSError as error:
        raise ResourceError("Supervisor private temporary directory could not be created.") from error
    atexit.register(shutil.rmtree, temporary, ignore_errors=True)
    return account_environment(temporary, additions=additions, private_additions=private_additions)
