"""Install one archive into a toolkit installation: version folder, launcher, format check, switch."""

from __future__ import annotations

import ast
import json
import os
import pwd
import stat
import subprocess
import sys
import tempfile
import zipfile
from collections.abc import Callable
from dataclasses import asdict
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from docker_test_toolkit.application import admission, retention, sharing
from docker_test_toolkit.application.lifecycle import Deadline, NativeClock, retention_image_interest
from docker_test_toolkit.application.probe import ensure_on_current_daemon
from docker_test_toolkit.application.recovery import Context, failpoint, lock
from docker_test_toolkit.config.installation import ARCHIVE_NAME, Installation, current_target, platform_root
from docker_test_toolkit.domain.errors import InfrastructureFailure, InvalidInput, ResourceError

if TYPE_CHECKING:
    from docker_test_toolkit.application.ports.docker import DockerPort

_MODELS_MEMBER = "docker_test_toolkit/domain/models.py"
_LAUNCHER = """#!/bin/sh
# Written by `docker-test-toolkit install`; replaced on every install.
set -eu
python={python}
root={root}
[ -x "$python" ] || {{
    echo "docker-test-toolkit: recorded Python $python is missing; reinstall the toolkit" >&2
    exit 5
}}
version=$(CDPATH= cd -- "$root/current" && pwd -P)
exec "$python" -I "$version/docker-test-toolkit.pyz" "$@"
"""


def _shell_quote(path: Path) -> str:
    return "'" + str(path).replace("'", "'\\''") + "'"


def launcher_text(python: Path, root: Path) -> bytes:
    """Return the launcher script: the recorded interpreter and root are embedded; it resolves ``current`` once."""
    return _LAUNCHER.format(python=_shell_quote(python), root=_shell_quote(root)).encode()


def archive_version(archive: Path) -> str:
    """Return ``TOOLKIT_VERSION`` of an archive, read from its ``domain/models.py`` without importing it.

    Raises:
        InvalidInput: The file is not a toolkit archive.
    """
    try:
        with zipfile.ZipFile(archive) as members:
            tree = ast.parse(members.read(_MODELS_MEMBER))
    except (OSError, KeyError, SyntaxError, zipfile.BadZipFile) as error:
        raise InvalidInput(f"{archive} is not a toolkit archive.") from error
    for node in tree.body:
        if (
            isinstance(node, ast.Assign)
            and [getattr(target, "id", None) for target in node.targets] == ["TOOLKIT_VERSION"]
            and isinstance(node.value, ast.Constant)
            and isinstance(node.value.value, str)
        ):
            version = node.value.value
            if version and "/" not in version and not version.startswith("."):
                return version
    raise InvalidInput(f"{archive} carries no valid TOOLKIT_VERSION.")


def _require_interpreter(python: Path) -> None:
    """Refuse an interpreter that is missing, older than 3.12, or inside a virtual environment."""
    probe = "import sys; print(sys.version_info >= (3, 12), sys.prefix != sys.base_prefix)"
    try:
        result = subprocess.run([str(python), "-I", "-c", probe], capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.TimeoutExpired) as error:
        raise InvalidInput(f"The interpreter {python} cannot run.") from error
    answer = result.stdout.split()
    if result.returncode != 0 or len(answer) != 2:
        raise InvalidInput(f"The interpreter {python} cannot run.")
    recent, virtual = answer
    if recent != "True":
        raise InvalidInput(f"The interpreter {python} is older than Python 3.12; pass --python with a newer one.")
    if virtual == "True":
        raise InvalidInput(
            f"The interpreter {python} belongs to a virtual environment; pass --python with a base interpreter."
        )


def _require_root(root: Path, *, test_root: bool) -> None:
    """Refuse a root other than the platform's (unless for tests) and one that others may write or own."""
    if not test_root:
        home = Path(pwd.getpwuid(os.getuid()).pw_dir)
        if root != platform_root(home, sys.platform):
            raise ResourceError(f"{root} is not this platform's installation root.")
    root.mkdir(mode=0o755, parents=True, exist_ok=True)
    observed = root.stat()
    if not stat.S_ISDIR(observed.st_mode) or observed.st_mode & 0o022 or observed.st_uid not in (os.getuid(), 0):
        raise ResourceError(f"The installation root {root} is writable by others or owned by another user.")


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _place_version(archive: Path, root: Path, version: str) -> None:
    """Place the archive as ``root/<version>`` (folder 0555, file 0444), reusing identical bytes only."""
    content = archive.read_bytes()
    folder = root / version
    if folder.exists():
        if not (folder / ARCHIVE_NAME).is_file():  # not a folder an install wrote: never repaired or removed here
            raise ResourceError(f"{folder} holds no toolkit archive; remove it, then install again.")
        if (folder / ARCHIVE_NAME).read_bytes() != content:
            raise ResourceError(f"Version {version} is installed with different bytes; version folders are immutable.")
    else:
        staging = Path(tempfile.mkdtemp(prefix=f".{version}.tmp-", dir=root))
        target = staging / ARCHIVE_NAME
        with target.open("wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        target.chmod(0o444)
        _fsync_directory(staging)
        staging.rename(folder)  # a directory is renamed while still writable: macOS refuses to move a 0555 one
        _fsync_directory(root)
    folder.chmod(0o555)  # also completes a folder an earlier install placed but crashed before closing


def command_link_path(home: Path, platform: str, *, system_bin: Path = Path("/usr/local/bin")) -> Path | None:
    """Return where ``install`` links the command: the user's folder on macOS, the system's on Linux if writable."""
    if platform == "darwin":
        return home / ".local/bin/docker-test-toolkit"
    return system_bin / "docker-test-toolkit" if os.access(system_bin, os.W_OK) else None


def _write_launcher(root: Path, python: Path, command_link: Path | None) -> Path:
    """Write ``root/bin/docker-test-toolkit`` through a temporary file and refresh the platform command link."""
    directory = root / "bin"
    directory.mkdir(mode=0o755, exist_ok=True)
    launcher = directory / "docker-test-toolkit"
    descriptor, temporary = tempfile.mkstemp(prefix=".docker-test-toolkit.tmp-", dir=directory)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(launcher_text(python, root))
        stream.flush()
        os.fsync(stream.fileno())
        os.fchmod(stream.fileno(), 0o755)  # the launcher is a command every user of the installation runs
    os.replace(temporary, launcher)
    if command_link is not None:
        command_link.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
        staged = command_link.with_name(f".{command_link.name}.tmp-{os.getpid()}")
        staged.unlink(missing_ok=True)
        staged.symlink_to(launcher)
        os.replace(staged, command_link)
        return command_link
    return launcher


def _holder_versions(account: Path, anchor) -> list[str]:
    """Return the toolkit versions of the runs that hold the slot: they explain why new runs wait."""
    versions = []
    for holder in admission.holders(admission.load_queue(account, anchor)):
        try:
            record = json.loads(Path(holder.record).read_text())
            versions.append(str(record["artifact"]["version"]))
        except (OSError, ValueError, KeyError, TypeError):
            versions.append("unknown")
    return versions


def install(
    archive: Path,
    *,
    root: Path,
    python: Path,
    host_reserve_bytes: int | None = None,
    disk_reserve_bytes: int | None = None,
    parallel_runs: int | Literal["auto"] | None = None,
    share_scope: str | None = None,
    command_link: Path | None = None,
    test_root: bool = False,
    docker_factory: Callable[[], DockerPort] | None = None,
) -> dict[str, object]:
    """Install ``archive`` into ``root`` and make it current; idempotent and safe to rerun after a crash.

    Under ``root/install.lock``: refuse unsuitable interpreters and roots, place the immutable version folder, write
    the launcher, then, under the account's metadata lock, check the state format, switch ``current`` and record the
    machine settings (host and disk reserves, then the parallel-runs limit) that admission reads under the same lock.
    A crash before the switch leaves the previous version current; a rerun after any crash writes the settings again.
    An omitted setting keeps the account's existing value. With ``docker_factory``, the daemon probe's image is then
    made present outside every lock; its outcome is reported, never an install failure.

    Raises:
        InvalidInput: An unsuitable interpreter, a non-archive, a host or disk reserve below the 1 GiB floor, or a
            parallel-runs limit that is neither an integer of at least 1 nor ``"auto"``.
        ResourceError: An unsuitable root, or a version folder that holds different bytes.
        IncompatibleState: ``state-format-newer`` when the account was written by a newer state format.
    """
    root = Path(os.path.abspath(root))
    python = Path(os.path.abspath(python))
    version = archive_version(archive)
    if host_reserve_bytes is not None and (
        type(host_reserve_bytes) is not int or host_reserve_bytes < sharing.HOST_RESERVE_FLOOR
    ):
        raise InvalidInput("A host reserve must be at least 1 GiB.")
    if disk_reserve_bytes is not None and (
        type(disk_reserve_bytes) is not int or disk_reserve_bytes < sharing.DISK_RESERVE_FLOOR
    ):
        raise InvalidInput("A disk reserve must be at least 1 GiB.")
    if parallel_runs is not None and not sharing.valid_parallel_runs(parallel_runs):
        raise InvalidInput("The parallel-runs limit must be an integer of at least 1 or auto.")
    if share_scope is not None and share_scope not in sharing.SHARE_SCOPES:
        raise InvalidInput("The share scope must be machine or repository.")
    _require_interpreter(python)
    _require_root(root, test_root=test_root)
    installation = Installation(root, root / version, root / version / ARCHIVE_NAME)
    with lock(Context(root, root), "install"):
        _place_version(archive, root, version)
        command = _write_launcher(root, python, command_link)
        previous = current_target(root)
        account = installation.account_directory
        with lock(Context(account, account), "metadata") as anchor:
            holders = _holder_versions(account, anchor)  # reading the queue admits the state format first
            failpoint("install-before-switch")
            if previous != Path(os.path.realpath(root / version)):  # the whole path: a same-named folder elsewhere
                link = root / f".current.tmp-{os.getpid()}"
                link.unlink(missing_ok=True)
                link.symlink_to(version)
                os.replace(link, root / "current")
                _fsync_directory(root)
            failpoint("install-switched")
            if host_reserve_bytes is not None:
                sharing.record_host_reserve(account, host_reserve_bytes)
            failpoint("install-reserve-written")
            if disk_reserve_bytes is not None:
                sharing.record_disk_reserve(account, disk_reserve_bytes)
            if parallel_runs is not None:
                sharing.record_parallel_runs(account, parallel_runs)
            limit = sharing.read_parallel_runs(account)
            if share_scope is not None:
                sharing.record_share_scope(account, share_scope)
            scope = sharing.read_share_scope(account)
    try:
        docker = docker_factory() if docker_factory is not None else None
    except (InfrastructureFailure, ResourceError):
        docker = None  # an unreachable daemon leaves only the file kinds to the retention pass
    probe_image = "not-checked" if docker_factory is None else _ensure_probe_image(docker, account)
    pass_report = _retention_at_install(docker, account, installation.state_directory)
    return {
        "previous": None if previous is None else previous.name,
        "version": version,
        "root": str(root),
        "command": str(command),
        "holder_versions": [held for held in holders if held != version],
        "parallel_runs": "auto" if limit is None else limit,
        "share_scope": scope,
        "probe_image": probe_image,
        "retention": pass_report,
    }


def _retention_at_install(docker: DockerPort | None, account: Path, registry: Path) -> dict[str, object] | None:
    """Run one forced retention pass after the settings; its report, or ``None`` on failure.

    Outside every install lock; without a daemon (``None``) only the file kinds are retained.
    """
    try:
        report = retention.run_pass(
            account=account,
            registry=registry,
            trigger="install",
            docker=docker,
            clock=NativeClock(),
            force=True,
            image_interest=retention_image_interest(account, registry),
            deadline=Deadline.after(NativeClock(), retention.RETENTION_BUDGET_S),
        )
    except (OSError, ValueError, InfrastructureFailure, ResourceError):
        return None
    return None if report is None else asdict(report)


def _ensure_probe_image(docker: DockerPort | None, account: Path) -> str:
    """Return the probe image outcome on the current daemon, or ``daemon-unavailable`` when it cannot be reached."""
    return "daemon-unavailable" if docker is None else ensure_on_current_daemon(docker, account, NativeClock())
