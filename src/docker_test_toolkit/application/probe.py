"""The daemon probe: one short-lived container reads the Docker host's CPU pressure and data-root free space.

Design 2.3.

On a Mac the containers run in a Linux VM whose pressure and disk the host cannot see; the probe reads both inside the
daemon's own kernel. Every guardian charges the probe's memory limit as external memory, so no special case exists in
capacity reads. The probe never pulls: its image is obtained by ``ensure_probe_image`` before a run queues and at
install, and the toolkit never deletes it (a public image cannot be proven toolkit-exclusive).
"""

from __future__ import annotations

import hashlib
import os
import re
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from docker_test_toolkit.application.recovery import Context, atomic_json, image_lock
from docker_test_toolkit.application.recovery import read_private_json as _read_private_json
from docker_test_toolkit.domain.errors import InfrastructureFailure, ResourceBusy, ResourceError
from docker_test_toolkit.domain.models import ExactInspection
from docker_test_toolkit.domain.policy import (
    PROBE_SENTINEL,
    DiskFigure,
    parse_cpu_pressure,
    parse_df,
    split_probe_output,
)

if TYPE_CHECKING:
    from docker_test_toolkit.application.lifecycle import NativeClock
    from docker_test_toolkit.application.ports.docker import DockerPort

# The alpine 3.22 manifest list, pinned so every platform resolves its own image of one known content.
PROBE_IMAGE = "alpine@sha256:2e0b81d60597f449baa56dc25d355367869d2dca4cd57e866f1e2b52af3eb55f"
PROBE_LABEL = "com.docker-test-toolkit.probe"
PROBE_MEMORY = "32m"
PROBE_CACHE_NS = 10_000_000_000  # NOTE: fixed; avg60 is a one-minute average, so 10 s loses nothing
PROBE_RUN_BOUND_S = 10  # NOTE: fixed bound of one probe run inside an admission pass
PROBE_PULL_BOUND_S = 120  # NOTE: fixed bound of one probe image pull, outside every admission pass
PROBE_PULL_BACKOFF_NS = 600_000_000_000  # NOTE: a failed pull is retried after 10 min, per daemon
PROBES_DIRECTORY = "probes"
PROBE_IMAGE_FILE = "probe-image.json"
PROBE_READING_FILE = "probe-reading.json"  # the last reading per daemon, for readiness (which runs no container)
_SAFE_DATA_ROOT = re.compile(r"/[A-Za-z0-9._/-]*")


@dataclass(frozen=True)
class ProbeResult:
    """One probe reading; each figure is valid or ``None`` on its own."""

    cpu_pressure: float | None
    data_root: DiskFigure | None
    read_ns: int
    clock_origin: str
    daemon_id: str


PROBE_NAME = re.compile(r"dtt-probe-[0-9a-f]{12}-[0-9a-f]{12}\Z")  # the grammar of ``probe_name``


def probe_name(account: Path) -> str:
    """Return a fresh probe container name, unique per run and recognisable per account."""
    return f"dtt-probe-{_account_tag(account)}-{uuid.uuid4().hex[:12]}"


def _account_tag(account: Path) -> str:
    return hashlib.sha256(str(account.resolve()).encode()).hexdigest()[:12]


def probe_argv(image: str, data_root: str | None, name: str, account: Path) -> tuple[str, ...]:
    """Return the probe's ``docker`` argv: read-only, network-less, memory-limited, never pulling.

    ``data_root`` is bind-mounted read-only for ``df`` only when it is a plain absolute path; otherwise only the
    pressure is read and the disk figure stays unknown.
    """
    mount: tuple[str, ...] = ()
    target = "/"
    if data_root is not None:
        mount = ("--mount", f"type=bind,src={data_root},dst=/dtt-data,ro")
        target = "/dtt-data"
    script = f"cat /proc/pressure/cpu; echo {PROBE_SENTINEL}; df -P -k {target}"
    return (
        "run",
        "--rm",
        "--pull=never",
        "--name",
        name,
        "--network",
        "none",
        "--read-only",
        "--memory",
        PROBE_MEMORY,
        "--cpus",
        "1",
        "--security-opt",
        "no-new-privileges",
        "--label",
        f"{PROBE_LABEL}={_account_tag(account)}",
        *mount,
        image,
        "sh",
        "-c",
        script,
    )


def usable_data_root(value: object) -> str | None:
    """Return the daemon's data root when it is a plain absolute path a bind mount can name, else ``None``."""
    if isinstance(value, str) and _SAFE_DATA_ROOT.fullmatch(value) and ".." not in value.split("/"):
        return value
    return None


class DaemonProbe:
    """Read the daemon host's figures through one bounded probe run, cached for ``PROBE_CACHE_NS``.

    One instance per account lives as long as its guardian process, so the cache spans admission passes; the Docker
    port is passed per read because each pass composes its own.
    """

    def __init__(self, account: Path) -> None:
        self._account = account
        self._last: ProbeResult | None = None

    def cached(self, daemon_id: str, clock: NativeClock) -> ProbeResult | None:
        """Return this daemon's reading when it is younger than ``PROBE_CACHE_NS``, else ``None``.

        Every guardian of the account shares the readings through ``probe-reading.json``: one probe per daemon and
        interval serves all of them, not one per waiting run.
        """
        now = clock.read()
        last = self._last
        if (
            last is not None
            and last.daemon_id == daemon_id
            and last.clock_origin == clock.origin
            and 0 <= now - last.read_ns < PROBE_CACHE_NS
        ):
            return last
        shared = latest_reading(self._account, daemon_id, clock.origin)
        if shared is None or not 0 <= now - shared.read_ns < PROBE_CACHE_NS:
            return None
        return shared

    def read(
        self, docker: DockerPort, *, daemon_id: str, data_root: object, deadline: object, clock: NativeClock
    ) -> ProbeResult:
        """Return the cached reading of this daemon when it is younger than ``PROBE_CACHE_NS``, else probe once.

        The container's name and daemon are recorded in ``<account>/probes/<name>`` before it starts and the record
        is removed once it is gone, so retention can remove a probe a killed process left. A failed or
        late probe yields a reading with no figures; it never raises for the probe's own failure.
        """
        cached = self.cached(daemon_id, clock)
        if cached is not None:
            return cached
        now = clock.read()
        root = usable_data_root(data_root)
        name = probe_name(self._account)
        record = self._account / PROBES_DIRECTORY / name
        empty = ProbeResult(None, None, now, clock.origin, daemon_id)
        try:
            record.parent.mkdir(mode=0o700, exist_ok=True)
            descriptor = os.open(record, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            with os.fdopen(descriptor, "w") as stream:
                stream.write(daemon_id)
        except OSError:
            return empty  # no record, no container: a probe the toolkit could not account for is never started
        try:
            run = docker.run_probe(
                probe_argv(PROBE_IMAGE, root, name, self._account), deadline=deadline, container_name=name
            )
        except (InfrastructureFailure, ResourceError):
            record.unlink(missing_ok=True)  # run_probe raises only before the container starts
            self._last = empty
            return empty
        if run.timed_out:
            self._last = empty
            return empty  # the record stays until retention has seen the container gone
        record.unlink(missing_ok=True)  # `--rm` removed the container with its exit
        psi_text, df_text = split_probe_output(run.stdout)
        sizes = parse_df(df_text) if root is not None else None
        figure = (
            DiskFigure("data-root", sizes[1], sizes[0], None, "measured")
            if sizes is not None
            else DiskFigure("data-root", None, None, None, "unknown")
        )
        result = ProbeResult(parse_cpu_pressure(psi_text), figure, now, clock.origin, daemon_id)
        self._last = result
        _write_reading(self._account, result)
        return result


def read_probe_images(account: Path) -> dict[str, dict]:
    """Return ``probe-image.json``'s per-daemon entries; an absent or unreadable file is empty (it is only a cache)."""
    return _read_daemons(account / PROBE_IMAGE_FILE)


def _read_daemons(path: Path) -> dict[str, dict]:
    """Return a per-daemon cache file's entries; an absent or unreadable file is empty."""
    try:
        document = _read_private_json(path)
    except (OSError, ResourceError):
        return {}
    daemons = document.get("daemons") if document.get("schema") == 1 else None
    return (
        {key: value for key, value in daemons.items() if isinstance(value, dict)} if isinstance(daemons, dict) else {}
    )


def _write_daemons(path: Path, entries: dict[str, dict]) -> None:
    """Replace a per-daemon cache file; a lost write only costs one more inspect, pull or probe reading."""
    try:
        atomic_json(path, {"schema": 1, "daemons": entries})
    except (OSError, ResourceError):
        pass


def ensure_on_current_daemon(docker: DockerPort, account: Path, clock: NativeClock) -> str:
    """Ensure the probe image on the daemon ``docker`` reaches now, under the account's image lock; return the outcome,
    or ``daemon-unavailable`` when the daemon's identity cannot be read."""
    try:
        daemon_id = docker.observe_daemon().daemon_id
    except (InfrastructureFailure, ResourceError):
        return "daemon-unavailable"
    return ensure_probe_image(docker, Context(account, account), account, daemon_id=daemon_id, clock=clock)


def ensure_probe_image(
    docker: DockerPort,
    lock_context: object,
    account: Path,
    *,
    daemon_id: str,
    clock: NativeClock,
    pull_bound_s: float = PROBE_PULL_BOUND_S,
) -> str:
    """Make the probe image present on this daemon; return ``present``, ``pulled``, ``backoff``, ``busy`` or ``failed``.

    Called before a run queues and at install, never inside an admission pass. The daemon is inspected first;
    only an absent image is pulled, under the image lock K and bounded by ``PROBE_PULL_BOUND_S``. A failed pull is
    stamped per daemon and not retried for ``PROBE_PULL_BACKOFF_NS``, so an offline host does not pay the bound on
    every run; another daemon's stamp never suppresses this daemon's pull.
    """
    entries = read_probe_images(account)
    entry = entries.get(daemon_id, {})
    now = clock.read()
    try:
        if docker.inspect_exact(ExactInspection("image", PROBE_IMAGE)).exists:
            if entry.get("present") is not True:
                entries[daemon_id] = {"present": True, "pulled_by_toolkit": False, "failed": None}
                _write_daemons(account / PROBE_IMAGE_FILE, entries)
            return "present"
    except InfrastructureFailure:
        return "failed"
    failed = entry.get("failed")
    if (
        isinstance(failed, dict)
        and failed.get("clock_origin") == clock.origin
        and isinstance(failed.get("tick"), int)
        and 0 <= now - failed["tick"] < PROBE_PULL_BACKOFF_NS
    ):
        return "backoff"
    try:
        with image_lock(lock_context, deadline_ns=time.monotonic_ns() + int(pull_bound_s * 1_000_000_000)):
            result = docker.execute(("pull", PROBE_IMAGE), timeout_seconds=pull_bound_s)
    except ResourceBusy:
        return "busy"  # another image operation holds K: not a pull failure, so no backoff
    except (InfrastructureFailure, ResourceError):
        result = None
    if result is None or result.exit_code:
        entries[daemon_id] = {
            "present": False,
            "pulled_by_toolkit": False,
            "failed": {"clock_origin": clock.origin, "tick": now},
        }
        _write_daemons(account / PROBE_IMAGE_FILE, entries)
        return "failed"
    entries[daemon_id] = {"present": True, "pulled_by_toolkit": True, "failed": None}
    _write_daemons(account / PROBE_IMAGE_FILE, entries)
    return "pulled"


def _write_reading(account: Path, result: ProbeResult) -> None:
    """Keep the daemon's latest reading for readiness; a lost write only makes readiness report an older one."""
    figure = result.data_root
    entry = {
        "cpu_pressure": result.cpu_pressure,
        "data_root_free_bytes": None if figure is None else figure.free_bytes,
        "data_root_total_bytes": None if figure is None else figure.total_bytes,
        "clock_origin": result.clock_origin,
        "tick": result.read_ns,
    }
    readings = read_probe_readings(account)
    readings[result.daemon_id] = entry
    _write_daemons(account / PROBE_READING_FILE, readings)


def latest_reading(account: Path, daemon_id: str, clock_origin: str) -> ProbeResult | None:
    """Return the account's latest probe reading of ``daemon_id`` on this clock origin, whatever its age, or ``None``
    when there is none, it is of another boot or it is malformed (``probe-reading.json``, written by every probe)."""
    entry = read_probe_readings(account).get(daemon_id, {})
    tick, free, total = entry.get("tick"), entry.get("data_root_free_bytes"), entry.get("data_root_total_bytes")
    pressure = entry.get("cpu_pressure")
    if (
        entry.get("clock_origin") != clock_origin
        or type(tick) is not int
        or not (pressure is None or type(pressure) in (int, float))
    ):
        return None
    figure = (
        DiskFigure("data-root", free, total, None, "measured")
        if type(free) is int and type(total) is int
        else DiskFigure("data-root", None, None, None, "unknown")
    )
    return ProbeResult(None if pressure is None else float(pressure), figure, tick, clock_origin, daemon_id)


def read_probe_readings(account: Path) -> dict[str, dict]:
    """Return ``probe-reading.json``'s per-daemon readings; an absent or unreadable file is empty."""
    return _read_daemons(account / PROBE_READING_FILE)
