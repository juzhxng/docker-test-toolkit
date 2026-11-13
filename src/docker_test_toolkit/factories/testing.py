"""Non-exported composition for isolated infrastructure tests."""

from __future__ import annotations

import json
import os
import pwd
import stat
import subprocess
import sys
from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from docker_test_toolkit.application import lifecycle, recovery
from docker_test_toolkit.application.api import RunHandle, _open_retained_run, _open_run
from docker_test_toolkit.application.lifecycle import (
    Deadline,
    EngineReply,
    HostMemoryObservation,
    NativeClock,
    ProcessIdentity,
)
from docker_test_toolkit.application.ports.docker import DockerPort
from docker_test_toolkit.application.state import canonical_account, overlaps_account
from docker_test_toolkit.domain.errors import ToolkitError
from docker_test_toolkit.domain.models import DaemonIdentity, RunSpec


@dataclass(frozen=True)
class _TestServices:
    root: Path

    def open_run(
        self,
        spec: RunSpec,
        *,
        docker_factory: Callable[[], DockerPort] | None = None,
        admit_docker: bool = False,
    ) -> RunHandle:
        """Prepare through the same facade with isolated registry and admission roots."""
        if docker_factory is None:
            from docker_test_toolkit.factories.cli import build_docker_adapter

            docker_factory = build_docker_adapter
        handle = _open_run(
            spec,
            test_state=self.root / "registry",
            test_directory=self.root / "admission",
            docker_factory=docker_factory,
        )
        if admit_docker:
            adapter = docker_factory()
            lifecycle.bind_test_daemon(self.root / "admission", adapter.observe_daemon().daemon_id)
        return handle

    def open_retained_run(
        self,
        spec: RunSpec,
        locator: Path,
        *,
        docker_factory: Callable[[], DockerPort] | None = None,
    ) -> RunHandle:
        """Open an exact stopped retained test run without using the production account."""
        bind_factory: Callable[[DaemonIdentity], DockerPort]
        if docker_factory is None:
            from docker_test_toolkit.factories.cli import bind_docker_adapter, build_docker_adapter

            docker_factory = build_docker_adapter
            bind_factory = bind_docker_adapter
        else:

            def _bind_test_adapter(_identity: DaemonIdentity) -> DockerPort:
                return docker_factory()

            bind_factory = _bind_test_adapter

        lifecycle.configure_docker_factories(docker_factory, bind_factory)
        return _open_retained_run(
            spec,
            locator,
            test_directory=self.root / "admission",
            docker_factory=docker_factory,
        )


def install_failpoint(callback: Callable[[str], None] | None) -> None:
    """Install (or clear with ``None``) the failpoint callback in this process; test composition only."""
    recovery.configure_failpoint(callback)


def install_lock_observer(observer: Callable[[str, str, bool, bool, bool], None] | None) -> None:
    """Install (or clear with ``None``) the lock observer in this process; test composition only."""
    recovery.configure_lock_observer(observer)


_DEFAULT_TEST_POLL_NS = 50_000_000
# Every private root this process composed, mapped to its registry's resolved path at build; kept for the whole session
# so the teardown and the session census can find a test's runs.
_BUILT_ROOTS: dict[Path, Path] = {}


def build_test_services(state_root: Path) -> _TestServices:
    """Construct the sole test-only composition at a caller-owned private root.

    The private admission domain gets a 50 ms leave poll unless a binding already exists.
    """
    root = Path(state_root).resolve()
    canonical = canonical_account()
    if canonical is not None and overlaps_account(root, canonical):
        raise ToolkitError("Private test composition cannot use the real account domain.")
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    root.chmod(0o700)
    observed = root.stat()
    if not stat.S_ISDIR(observed.st_mode) or observed.st_uid != os.getuid() or observed.st_mode & 0o077:
        raise ToolkitError("Private test state root must be account-owned mode 0700.")
    for child in (root / "registry", root / "admission"):
        child.mkdir(mode=0o700, exist_ok=True)
        observed = child.stat(follow_symlinks=False)
        if not stat.S_ISDIR(observed.st_mode) or observed.st_uid != os.getuid() or observed.st_mode & 0o077:
            raise ToolkitError("Private test state directories must be account-owned mode 0700.")
    if not (root / "admission" / "test-poll.json").exists():  # never overwrite an explicit binding of a shared root
        lifecycle.bind_test_poll_interval(root / "admission", _DEFAULT_TEST_POLL_NS)
    _BUILT_ROOTS.setdefault(root, (root / "registry").resolve(strict=True))
    return _TestServices(root)


def end_test_runs(within: Path) -> list[str]:
    """Withdraw the runs left active under every private root built inside ``within``; return their ids.

    The teardown of one test: each root this process composed at or below ``within`` goes through
    ``lifecycle.withdraw_test_runs`` with the registry path recorded at build, so a re-pointed registry is refused.
    Roots built elsewhere (module- or session-scoped) are untouched. Test composition only.
    """
    base = Path(within).resolve()
    withdrawn: list[str] = []
    for root, registry in sorted(_BUILT_ROOTS.items()):
        if root == base or base in root.parents:
            withdrawn += lifecycle.withdraw_test_runs(root / "registry", recorded=registry)
    return withdrawn


_GUARDIAN_COMMAND = "_guardian"


def _guardian_processes() -> list[tuple[int, int, list[str] | str]]:
    """Return ``(pid, parent pid, argv)`` for every process; argv is a list on Linux and one command line on macOS.

    Linux reads ``/proc/<pid>/stat`` (the parent pid follows the command's closing parenthesis) and the NUL-separated
    ``/proc/<pid>/cmdline``. macOS has no ``/proc``: ``ps -axww -o pid=,ppid=,command=`` prints every process with its
    full command line (``ww``: never truncated), the same scan the integration fixtures use.
    """
    processes: list[tuple[int, int, list[str] | str]] = []
    if sys.platform.startswith("linux"):
        for entry in Path("/proc").iterdir():
            if not entry.name.isdigit():
                continue
            try:
                stat_line = (entry / "stat").read_text()
                argv = (entry / "cmdline").read_bytes().split(b"\0")
            except OSError:
                continue  # the process ended during the scan
            parent = int(stat_line[stat_line.rindex(")") + 2 :].split()[1])
            processes.append((int(entry.name), parent, [part.decode(errors="replace") for part in argv if part]))
        return processes
    listing = subprocess.run(
        ["ps", "-axww", "-o", "pid=,ppid=,command="], capture_output=True, text=True, check=True
    ).stdout
    for line in listing.splitlines():
        fields = line.split(None, 2)
        if len(fields) == 3 and fields[0].isdigit() and fields[1].isdigit():
            processes.append((int(fields[0]), int(fields[1]), fields[2]))
    return processes


def _guardian_record(argv: list[str] | str) -> Path | None:
    """Return the run record a toolkit guardian's ``--context`` payload names, or None for any other process."""
    if isinstance(argv, list):
        if _GUARDIAN_COMMAND not in argv or "--context" not in argv[argv.index(_GUARDIAN_COMMAND) :]:
            return None
        position = argv.index("--context", argv.index(_GUARDIAN_COMMAND))
        payload = argv[position + 1] if position + 1 < len(argv) else ""
    else:
        marker = f" {_GUARDIAN_COMMAND} --context "
        if marker not in argv:
            return None
        payload = argv[argv.index(marker) + len(marker) :]
    try:
        context, _end = json.JSONDecoder().raw_decode(payload)
    except ValueError:
        return None
    record = context.get("record") if isinstance(context, dict) else None
    return Path(record).resolve() if isinstance(record, str) and Path(record).is_absolute() else None


def surviving_test_guardians(*, parent: int, roots: Iterable[Path], excluded: Iterable[Path] = ()) -> list[str]:
    """Name every live toolkit guardian that is a child of ``parent`` or whose run record lies under one of ``roots``.

    The session census: parentage finds the guardians the test process started, the ``--context``
    record finds orphans re-parented elsewhere; a guardian whose record lies under one of ``excluded`` is left out.
    Both sides of every path comparison are resolved. Each returned line names the pid, parent pid and record. Test
    composition only; it reads the process table and signals nothing.
    """
    chosen = [Path(root).resolve() for root in roots]
    left_out = [Path(root).resolve() for root in excluded]
    survivors = []
    for pid, parent_pid, argv in _guardian_processes():
        record = _guardian_record(argv)
        if record is None or any(root == record or root in record.parents for root in left_out):
            continue
        if parent_pid == parent or any(root in record.parents for root in chosen):
            survivors.append(f"toolkit guardian pid {pid} (parent {parent_pid}) record {record}")
    return survivors


def bind_test_capacity(
    services: _TestServices,
    *,
    daemon_memory_bytes: int | None = None,
    host_memory_bytes: int | None = None,
    daemon_cpus: int | None = None,
    host_cpus: int | None = None,
    containers: Sequence[dict] = (),
    cpu_pressure: float | None = None,
    overtake_window_ns: int | None = None,
    disk: dict[str, dict[str, int]] | None = None,
    previous_image_sizes: list[int] | None = None,
    turn_grace_ns: int | None = None,
    runtime: str | None = None,
    machine: dict[str, object] | None = None,
) -> None:
    """Replace the private account's synthetic capacity snapshot and daemon inventory; test composition only."""
    lifecycle.bind_test_capacity(
        services.root / "admission",
        daemon_memory_bytes=daemon_memory_bytes,
        host_memory_bytes=host_memory_bytes,
        daemon_cpus=daemon_cpus,
        host_cpus=host_cpus,
        containers=containers,
        cpu_pressure=cpu_pressure,
        overtake_window_ns=overtake_window_ns,
        disk=disk,
        previous_image_sizes=previous_image_sizes,
        turn_grace_ns=turn_grace_ns,
        runtime=runtime,
        machine=machine,
    )


def bind_test_host_samples(services: _TestServices, samples: Sequence[dict]) -> None:
    """Script the private account's host-memory reads for the admission window; test composition only."""
    lifecycle.bind_test_host_samples(services.root / "admission", samples)


def bind_test_poll_interval(services: _TestServices, interval_ns: int) -> None:
    """Set the private account's leave poll (10 ms to 1 s); test composition only."""
    lifecycle.bind_test_poll_interval(services.root / "admission", interval_ns)


def bind_test_installation(services: _TestServices, *, root: Path, loaded: Path) -> None:
    """Bind the private account to a temporary installation so the version gate evaluates it; test composition only."""
    lifecycle.bind_test_installation(services.root / "admission", root=root, loaded=loaded)


def bind_test_window(services: _TestServices, seconds: int) -> None:
    """Shorten the private account's admission window to ``seconds`` (1 <= seconds < 30); test composition only."""
    lifecycle.bind_test_window(services.root / "admission", seconds)


def _local_endpoint(_docker: object, _deadline: object) -> str:
    """The local daemon socket a synthetic engine stands behind: Docker Desktop's on macOS, the system one on Linux."""
    if sys.platform == "darwin":
        return f"unix://{Path(pwd.getpwuid(os.getuid()).pw_dir)}/.docker/run/docker.sock"
    return "unix:///var/run/docker.sock"


@contextmanager
def bind_test_observation_reads(
    *,
    engine: Callable[[object, str, Deadline | None], EngineReply] | None = None,
    rss: Callable[[ProcessIdentity, Deadline], int] | None = None,
    host: Callable[[NativeClock, Deadline | None], HostMemoryObservation] | None = None,
    endpoint: Callable[[object, Deadline | None], str] | None = None,
) -> Iterator[None]:
    """Bind the Engine, RSS and host reads of one observation pass for a block, then restore the defaults.

    A reader left ``None`` keeps its production read; with a synthetic ``engine`` the endpoint defaults to this
    platform's local daemon socket. Nothing a previous block bound is inherited, and the defaults
    are restored on exit, also when the block raises. The binding is process-wide, like ``install_failpoint``.
    Test composition only; production never calls it.
    """
    if endpoint is None and engine is not None:
        endpoint = _local_endpoint
    lifecycle.configure_observation_reads(engine=engine, rss=rss, host=host, endpoint=endpoint)
    try:
        yield
    finally:
        lifecycle.configure_observation_reads()
