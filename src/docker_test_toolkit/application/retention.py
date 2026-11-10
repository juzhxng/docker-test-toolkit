"""Retention: keep toolkit-owned state within its caps without an operator command.

One bounded, throttled pass removes only what the toolkit's records name, never under an account metadata or run lock:
build-cache generations (the 0700 store is their record), archived run records, empty recorded session folders, the
state volumes of promoted builders whose creation identity still matches, and probe containers recorded on this daemon.
Everything it may not prove is reported, never deleted. Pending image deletions keep their own drain at every leave.
"""

from __future__ import annotations

import os
import re
import stat
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import TYPE_CHECKING

from docker_test_toolkit.application import build_cache, probe
from docker_test_toolkit.application.build_cache import real_directory
from docker_test_toolkit.application.recovery import (
    DOCKER_ID,
    Context,
    archived_run_files,
    atomic_json,
    buildkit_state_volume,
    image_lock,
    indexed_session_ids,
    lock,
    private_directory_entries,
    project_names,
    run_files,
    verified_archived_run,
)
from docker_test_toolkit.application.recovery import read_private_json as _read_private_json
from docker_test_toolkit.application.sharing import drain_pending, pending_absent
from docker_test_toolkit.application.state import archive_directory
from docker_test_toolkit.config.runtime import PRIVATE_ROOT_NAME
from docker_test_toolkit.domain.errors import InfrastructureFailure, ResourceBusy, ResourceError
from docker_test_toolkit.domain.models import ExactInspection
from docker_test_toolkit.domain.policy import (
    ARCHIVE_AGE_NS,
    PROBE_FILE_AGE_NS,
    SESSION_FOLDER_AGE_NS,
    RetentionCandidate,
    archive_cap_bytes,
    cache_cap_bytes,
    eviction_plan,
)

if TYPE_CHECKING:
    from docker_test_toolkit.application.lifecycle import Deadline, NativeClock
    from docker_test_toolkit.application.ports.docker import DockerPort

RETENTION_INTERVAL_NS = 600_000_000_000  # NOTE: one pass per 10 min unless the last one deferred work
RETENTION_BUDGET_S = 60  # NOTE: fixed time bound of one pass
RETENTION_MAX_DELETIONS = 200  # NOTE: fixed bound of one pass
STAMP_FILE = "retention-stamp.json"
REPORT_FILE = "retention-report.json"
_TERMINAL = "complete"


@dataclass
class KindReport:
    """What one kind removed, skipped (with the reason) and left for the next pass."""

    removed_bytes: int = 0
    removed_count: int = 0
    skipped: list[dict] = field(default_factory=list)
    deferred: bool = False

    def skip(self, identifier: str, reason: str, size: int = 0) -> None:
        """Report one item left in place, with the reason and the bytes it holds."""
        self.skipped.append({"id": identifier, "reason": reason, "bytes": size})


@dataclass
class RetentionReport:
    """One pass's outcome per kind, and the leftovers no record names (reported, never removed)."""

    trigger: str
    clock_origin: str
    tick: int
    kinds: dict[str, dict]
    legacy: dict[str, int]


class _Budget:
    """The pass's shared bound: wall time and deletions."""

    def __init__(self, seconds: float, deletions: int, deadline: Deadline | None = None) -> None:
        self._end = time.monotonic() + seconds
        self._left = deletions
        self.deadline = deadline  # the pass's absolute deadline for every Docker read that takes one

    def allows(self) -> bool:
        """Whether another deletion fits the pass's time and count."""
        return self._left > 0 and time.monotonic() < self._end

    def exhausted(self, report: KindReport) -> bool:
        """Whether the pass is out of time or deletions; marks ``report`` deferred when it is."""
        if self.allows():
            return False
        report.deferred = True
        return True

    def spend(self) -> None:
        """Count one deletion against the pass."""
        self._left -= 1

    def timeout_s(self) -> int:
        """Return a Docker command's bound: what is left of the pass, at least 1 s and at most the usual 30 s."""
        return max(1, min(30, int(self._end - time.monotonic())))


def run_pass(
    *,
    account: Path,
    registry: Path,
    trigger: str,
    docker: DockerPort | None,
    clock: NativeClock,
    budget_s: float = RETENTION_BUDGET_S,
    max_deletions: int = RETENTION_MAX_DELETIONS,
    force: bool = False,
    wall_ns: int | None = None,
    fs_total: Callable[[Path], int] | None = None,
    image_interest: Callable[[], Callable[[str], bool]] | None = None,
    deadline: Deadline | None = None,
) -> RetentionReport | None:
    """Run one bounded pass; ``None`` when throttled (a pass of this boot younger than the interval deferred nothing).

    Holds no account metadata or run lock: each kind takes only its own locks (cache anchor, registry, shared project
    lock, image lock K),
    non-blocking where another holder may be at work. ``docker`` is ``None`` where no daemon is reachable; the Docker
    kinds are then skipped. ``image_interest`` is called under K and returns whether a live or retained run uses an
    image; without it pending images are left to the leave drain. ``deadline`` (the pass's ``Deadline``) bounds every
    Docker read; a removal is bounded by what is left of the budget, plus its own identity check. The stamp is written
    at the end, so an interrupted
    pass runs again at the next trigger.
    """
    now = clock.read()
    if not force and throttled(account, clock):
        return None
    wall = time.time_ns() if wall_ns is None else wall_ns
    total_of = _filesystem_total if fs_total is None else fs_total
    if deadline is None:  # every pass is bounded, whoever calls it
        from docker_test_toolkit.application.lifecycle import Deadline

        deadline = Deadline.after(clock, budget_s)
    budget = _Budget(budget_s, max_deletions, deadline)
    kinds = {
        "build_cache": _retain_build_cache(registry, budget, total_of),
        "archives": _retain_archives(registry, budget, wall, total_of),
        "session_roots": _retain_session_roots(registry, budget, wall),
    }
    if docker is not None:
        kinds["builder_volumes"] = _docker_kind(lambda: _retain_builder_volumes(account, registry, docker, budget))
        kinds["probe_containers"] = _docker_kind(lambda: _retain_probe_containers(account, docker, budget, wall))
        kinds["pending_images"] = _docker_kind(lambda: _retain_pending_images(account, docker, budget, image_interest))
    legacy = {
        "session_folders_unrecorded": _count(kinds["session_roots"], {"unrecorded"}),
        "builder_volumes_unrecorded": _count(kinds.get("builder_volumes"), {"unrecorded", "unpromoted-builder"}),
    }
    deferred = any(report.deferred for report in kinds.values())
    report = RetentionReport(trigger, clock.origin, now, {name: asdict(value) for name, value in kinds.items()}, legacy)
    try:
        atomic_json(account / REPORT_FILE, {"schema": 1, **asdict(report)})
        atomic_json(
            account / STAMP_FILE, {"schema": 1, "clock_origin": clock.origin, "tick": now, "deferred": deferred}
        )
    except (OSError, ResourceError):
        pass  # only the next pass's timing and readiness depend on them
    return report


def _docker_kind(retain: Callable[[], KindReport]) -> KindReport:
    """Run one Docker kind; a daemon that fails or changes mid-pass defers that kind only, so the pass still reports
    the kinds it finished and the next trigger retries (nothing is deleted on a failed read)."""
    try:
        return retain()
    except (InfrastructureFailure, ResourceError):
        report = KindReport(deferred=True)
        report.skip("*", "docker-unavailable")
        return report


def _count(report: KindReport | None, reasons: set[str]) -> int:
    return 0 if report is None else sum(1 for item in report.skipped if item["reason"] in reasons)


def read_report(account: Path) -> dict | None:
    """Return the last pass's report, or ``None`` when no pass ran or it is unreadable."""
    try:
        document = _read_private_json(account / REPORT_FILE)
    except (OSError, ResourceError):
        return None
    return document if document.get("schema") == 1 else None


def throttled(account: Path, clock: NativeClock) -> bool:
    """Whether a pass of this clock origin younger than the interval deferred nothing."""
    origin, now = clock.origin, clock.read()
    try:
        stamp = _read_private_json(account / STAMP_FILE)
    except (OSError, ResourceError):
        return False
    return (
        stamp.get("clock_origin") == origin
        and isinstance(stamp.get("tick"), int)
        and 0 <= now - stamp["tick"] < RETENTION_INTERVAL_NS
        and stamp.get("deferred") is False
    )


def _filesystem_total(path: Path) -> int:
    observed = os.statvfs(path)
    return observed.f_blocks * observed.f_frsize


def _retain_build_cache(registry: Path, budget: _Budget, total_of: Callable[[Path], int]) -> KindReport:
    """Evict whole generations oldest first across every project while the store exceeds its cap.

    A generation whose platform anchor is busy (a live import may read it) is protected and the next oldest goes
    instead; when protected generations keep the store over its cap, the pass is deferred so the next trigger retries.
    """
    report = KindReport()
    root = build_cache.cache_root(registry)
    if not real_directory(root):
        return report  # absent, or a symlink: never entered
    candidates: list[RetentionCandidate] = []
    places: dict[str, tuple[Path, str, Path]] = {}
    for project in private_directory_entries(root):
        if not DOCKER_ID.fullmatch(project.name) or not real_directory(project):
            continue
        for platform in build_cache.platform_names(project):  # only platform-shaped names are cache data
            platform_dir = project / platform
            if not real_directory(platform_dir):
                continue
            for generation in build_cache.list_generations(platform_dir):
                if not generation.valid:
                    continue  # invalid entries are the store's own reconcile's to remove, under its anchor
                path = platform_dir / "generations" / generation.key / generation.name
                candidates.append(RetentionCandidate(str(path), generation.bytes, generation.wall_ns, False))
                places[str(path)] = (project, platform_dir.name, path)
    cap = cache_cap_bytes(total_of(root))
    busy: set[str] = set()
    while True:
        evict = eviction_plan([replace(item, protected=item.identifier in busy) for item in candidates], cap)
        if not evict:
            break
        if budget.exhausted(report):
            break
        identifier = evict[0]
        project, platform, path = places[identifier]
        size = next(item.size_bytes for item in candidates if item.identifier == identifier)
        try:
            with build_cache.platform_anchor(project, platform, shared=False, nonblocking=True):
                build_cache.remove_entry(path)
        except ResourceBusy:
            busy.add(identifier)
            report.skip(identifier, "busy", size)
            continue
        budget.spend()
        candidates = [item for item in candidates if item.identifier != identifier]
        report.removed_bytes += size
        report.removed_count += 1
    excess = sum(item.size_bytes for item in candidates) - cap
    if busy and excess > 0:
        report.skip("*", "over-cap-protected", excess)
        report.deferred = True  # a live import holds generations the cap needs gone: the next trigger retries
    return report


def _archive_age_wall(path: Path) -> int:
    observed = path.stat()
    return max(
        observed.st_mtime_ns, observed.st_ctime_ns
    )  # retirement sets mtime after the rename; ctime covers a crash between


def _retain_archives(registry: Path, budget: _Budget, wall: int, total_of: Callable[[Path], int]) -> KindReport:
    """Delete archived run records older than 90 days, then the oldest until the archive store is under its cap.

    Only a record whose bytes hash to the digest in its name and that names its run is ever deleted; other files with
    an archive-shaped name are reported ``unverified`` and kept.
    """
    report = KindReport()
    archive = archive_directory(registry)
    if not real_directory(archive):
        return report
    try:
        with lock(Context(registry, registry), "registry", nonblocking=True):
            files = []
            for project in private_directory_entries(archive):
                for path, match in archived_run_files(project):
                    if verified_archived_run(path, match) is None:
                        report.skip(str(path), "unverified")
                        continue
                    files.append((path, path.lstat().st_size, _archive_age_wall(path)))
            expired = [item for item in files if wall - item[2] > ARCHIVE_AGE_NS]
            kept = [item for item in files if item not in expired]
            evict = eviction_plan(
                [RetentionCandidate(str(path), size, age, False) for path, size, age in kept],
                archive_cap_bytes(total_of(archive)),
            )
            chosen = [*expired, *(item for item in kept if str(item[0]) in evict)]
            _remove_archives(archive, chosen, budget, report)
    except ResourceBusy:
        report.deferred = True  # a retirement holds the registry: the next trigger retries
    return report


def _remove_archives(archive: Path, chosen: list[tuple[Path, int, int]], budget: _Budget, report: KindReport) -> None:
    """Delete the chosen archived records and the project archive folders (``<project id>/runs``) left empty; nothing
    else in the archive is removed. The caller holds the registry lock."""
    for path, size, _age in chosen:
        if budget.exhausted(report):
            return
        path.unlink(missing_ok=True)
        budget.spend()
        report.removed_bytes += size
        report.removed_count += 1
    for project in private_directory_entries(archive):
        if not DOCKER_ID.fullmatch(project.name):
            continue  # not a project archive: no record names it, never removed
        runs = project / "runs"
        if real_directory(runs) and not any(runs.iterdir()):
            if budget.exhausted(report):
                return
            runs.rmdir()
            budget.spend()
        if real_directory(project) and not any(project.iterdir()):
            if budget.exhausted(report):
                return
            project.rmdir()
            budget.spend()


def _indexed_sessions(record: dict, registry: Path) -> set[str] | None:
    """Return the sessions of a project's indexed runs (recovery's validated read); ``None`` when one cannot be read."""
    try:
        return indexed_session_ids(Context(Path(record["project_root"]), registry), record)
    except (KeyError, TypeError, ValueError, ResourceError):
        return None


def _retain_session_roots(registry: Path, budget: _Budget, wall: int) -> KindReport:
    """Remove a project's empty folder of a session no project indexes a run in, then the session folder once empty.

    Each project removes only its own child (the folder named by its project ID), holding its shared project lock and
    the registry lock, both without waiting: registration indexes a run and creates its folders under the registry
    lock, so no run can appear between the index read and a removal. Anything else in a session folder is left. A
    record that cannot be read stops the kind: what is idle cannot be proven then.
    """
    report = KindReport()
    recorded: dict[Path, set[str]] = {}
    try:
        projects = [registry / "projects" / f"{name}.json" for name in project_names(registry)]
    except OSError:
        report.deferred = True  # the project index cannot be listed: what is idle cannot be proven
        return report
    for record_path in projects:
        project_id = record_path.name.removesuffix(".json")
        try:
            with (
                lock(Context(registry, registry), project_id, shared=True, nonblocking=True),
                lock(Context(registry, registry), "registry", nonblocking=True),
            ):
                indexed = _indexed_everywhere(projects, registry)
                record = _read_private_json(record_path)
                if indexed is None:
                    report.deferred = True
                    return report
                for session_id, session in (record.get("sessions") or {}).items():
                    previous = session.get("previous_temporary_parents", []) if isinstance(session, dict) else None
                    if not isinstance(previous, list):
                        continue  # not a session entry this release wrote: never entered
                    parents = [session.get("temporary_parent"), *previous]
                    bases = {Path(parent) / PRIVATE_ROOT_NAME for parent in parents if _absolute(parent)}
                    for base in bases:
                        recorded.setdefault(base, set()).add(session_id)
                        if session_id not in indexed and real_directory(base):  # a symlinked base is never entered
                            _remove_session_folder(base / session_id, project_id, budget, wall, report)
        except ResourceBusy:
            report.deferred = True  # a registration, cleanup or retirement is at work: the next trigger retries
        except (OSError, ResourceError):
            report.deferred = True
            return report
    for base, sessions in recorded.items():
        if real_directory(base):
            for folder in sorted(base.iterdir()):
                if folder.name not in sessions:
                    report.skip(str(folder), "unrecorded")
    return report


def _indexed_everywhere(projects: list[Path], registry: Path) -> set[str] | None:
    """Return the sessions of every project's indexed runs (a session may serve several projects), or ``None``."""
    indexed: set[str] = set()
    for record_path in projects:
        try:
            sessions = _indexed_sessions(_read_private_json(record_path), registry)
        except (OSError, ResourceError):
            return None
        if sessions is None:
            return None
        indexed |= sessions
    return indexed


def _absolute(parent: object) -> bool:
    return isinstance(parent, str) and parent.startswith("/")


def _remove_session_folder(folder: Path, project_id: str, budget: _Budget, wall: int, report: KindReport) -> None:
    """Remove the project's own empty folder in the session, then the session folder once empty and older than a day."""
    if not real_directory(folder):
        return
    own = folder / project_id
    if real_directory(own) and not any(own.iterdir()):
        if budget.exhausted(report):
            return
        own.rmdir()
        budget.spend()
    if any(folder.iterdir()):
        if (folder / project_id).exists():
            report.skip(str(folder), "has-content")
        return  # only another project's folder or a foreign one is left: never this project's to remove
    if wall - folder.stat().st_mtime_ns > SESSION_FOLDER_AGE_NS:
        if budget.exhausted(report):
            return
        folder.rmdir()
        budget.spend()
        report.removed_count += 1


_LEGACY_VOLUME = re.compile(r"buildx_buildkit_dtt-[0-9a-f]{12}-[0-9a-f]{12}0_state\Z")


def _retain_builder_volumes(account: Path, registry: Path, docker: DockerPort, budget: _Budget) -> KindReport:
    """Remove the state volume of each promoted builder of a terminal run, only while it is the recorded incarnation.

    A builder whose run ended before promotion is reported ``unpromoted``; a toolkit-shaped volume no record names is
    reported ``unrecorded``; the pass counts both under ``legacy``. Neither is ever deleted: a name cannot prove whose
    volume it is.
    """
    report = KindReport()
    if budget.exhausted(report):
        return report
    daemon_id = docker.observe_daemon(budget.deadline).daemon_id
    known: set[str] = set()
    promoted: list[tuple[str, str, object]] = []
    unpromoted: set[str] = set()
    disowned: set[str] = set()  # a disowned builder is no longer the run's: its volume is never deleted for it
    for _name, _run_id, _bytes, raw in run_files(registry):
        if not isinstance(raw, dict):
            continue
        builders = (raw.get("owned_resources") or {}).get("builders") or {}
        planned = [
            receipt["plan"]["state_volume"]
            for receipt in (raw.get("docker_receipts") or {}).values()
            if isinstance(receipt, dict)
            and isinstance(receipt.get("plan"), dict)
            and isinstance(receipt["plan"].get("state_volume"), str)
        ]
        volumes = {buildkit_state_volume(name) for name in builders}
        known |= set(planned) | volumes
        if raw.get("state") != _TERMINAL or (raw.get("grant") or {}).get("daemon_id") != daemon_id:
            continue  # a live run's builder, or one recorded on another daemon: never touched from this one
        evidenced = set()
        for builder_name, builder in builders.items():
            if isinstance(builder, dict) and (builder.get("evidence") or {}).get("disowned") is True:
                disowned.add(buildkit_state_volume(builder_name))
                continue
            volume = (builder.get("evidence") or {}).get("state_volume") if isinstance(builder, dict) else None
            if isinstance(volume, dict) and volume.get("name") == buildkit_state_volume(builder_name):
                promoted.append((builder_name, volume["name"], volume.get("created_at")))
                evidenced.add(volume["name"])
        unpromoted |= set(planned) | volumes
        unpromoted -= evidenced
    unpromoted -= disowned
    listed = docker.execute(("volume", "ls", "-q"), timeout_seconds=budget.timeout_s())
    present = set(listed.stdout.split()) if listed.exit_code == 0 else set()
    for name in sorted(disowned & present):
        report.skip(name, "disowned")
    for name in sorted(unpromoted & present):
        report.skip(name, "unpromoted-builder")
    unrecorded = sorted(name for name in present if _LEGACY_VOLUME.fullmatch(name) and name not in known)
    for name in unrecorded:
        report.skip(name, "unrecorded")
    if not promoted:
        return report
    mounted = {
        mount.get("Name")
        for item in docker.objects("container", deadline=budget.deadline)
        for mount in (item.get("Mounts") or [])
        if isinstance(mount, dict) and mount.get("Type") == "volume"
    }
    builders_listed = docker.execute(("buildx", "ls", "--format", "{{.Name}}"), timeout_seconds=budget.timeout_s())
    live_builders = set(builders_listed.stdout.split()) if builders_listed.exit_code == 0 else None
    for builder_name, name, created in promoted:
        if name not in present:
            continue
        if name in mounted or live_builders is None or builder_name in live_builders:
            report.skip(name, "in-use")
            continue
        if budget.exhausted(report):
            return report
        try:
            with image_lock(Context(account, account), nonblocking=True) as hold_fd:
                # the identity, again under K
                observed = docker.inspect_exact(ExactInspection("volume", name), deadline=budget.deadline)
                if observed.attributes.get("CreatedAt") != created:
                    report.skip(name, "identity-changed")
                    continue
                removed = docker.execute(("volume", "rm", name), timeout_seconds=budget.timeout_s(), hold_fd=hold_fd)
        except ResourceBusy:
            report.skip(name, "busy")
            continue
        if removed.exit_code != 0:
            report.skip(name, "remove-failed")
            continue
        budget.spend()
        report.removed_count += 1
    return report


def _probe_record(path: Path) -> str | None:
    """Return the daemon ID a probe record holds; ``None`` unless it is a private regular file of a probe's name."""
    if not probe.PROBE_NAME.fullmatch(path.name):
        return None
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError:
        return None
    with os.fdopen(descriptor, "rb") as stream:
        observed = os.fstat(stream.fileno())
        if not stat.S_ISREG(observed.st_mode) or observed.st_uid != os.getuid() or observed.st_size > 4096:
            return None
        return stream.read().decode(errors="replace").strip()


def _retain_probe_containers(account: Path, docker: DockerPort, budget: _Budget, wall: int) -> KindReport:
    """Remove probe containers a killed process left, by their records, on the daemon each record names."""
    report = KindReport()
    directory = account / probe.PROBES_DIRECTORY
    if budget.exhausted(report):
        return report
    daemon_id = docker.observe_daemon(budget.deadline).daemon_id
    for record in private_directory_entries(directory):
        recorded_daemon = _probe_record(record)
        if recorded_daemon is None:
            report.skip(record.name, "unreadable")
            continue
        try:
            if wall - record.lstat().st_mtime_ns < PROBE_FILE_AGE_NS:
                continue  # a probe of a live pass
        except FileNotFoundError:
            continue  # its probe ended and removed the record since it was read
        if recorded_daemon != daemon_id:
            report.skip(record.name, "other-daemon")
            continue
        if budget.exhausted(report):
            break
        try:
            with image_lock(Context(account, account), nonblocking=True) as hold_fd:
                if docker.inspect_exact(ExactInspection("container", record.name), deadline=budget.deadline).exists:
                    docker.execute(
                        ("container", "rm", "-f", record.name), timeout_seconds=budget.timeout_s(), hold_fd=hold_fd
                    )
                after = docker.inspect_exact(ExactInspection("container", record.name), deadline=budget.deadline)
                gone = not after.exists
        except ResourceBusy:
            report.skip(record.name, "busy")
            continue
        except InfrastructureFailure:
            report.skip(record.name, "docker-unavailable")
            continue
        if not gone:
            report.skip(record.name, "remove-failed")
            continue
        record.unlink(missing_ok=True)
        budget.spend()
        report.removed_count += 1
    return report


def _retain_pending_images(
    account: Path, docker: DockerPort, budget: _Budget, image_interest: Callable[[], Callable[[str], bool]] | None
) -> KindReport:
    """Under K, drain handed-off images nobody uses; an image re-tagged since its hand-off is dropped, never deleted."""
    report = KindReport()
    if image_interest is None or pending_absent(account):
        return report
    if budget.exhausted(report):
        return report
    daemon_id = docker.observe_daemon(budget.deadline).daemon_id
    try:
        with image_lock(Context(account, account), nonblocking=True) as hold_fd:
            outcomes = drain_pending(
                docker,
                account,
                daemon_id,
                image_interest(),  # read now, under K; a busy metadata lock defers the kind
                hold_fd,
                drop_retagged=True,
                allows=budget.allows,
                spend=budget.spend,
                timeout_s=budget.timeout_s,
                deadline=budget.deadline,
            )
    except ResourceBusy:
        report.deferred = True  # a run's image step holds K, or the metadata lock is busy: the next trigger retries
        return report
    report.removed_count = len(outcomes.removed)
    report.deferred = bool(outcomes.deferred)
    for entry in outcomes.retagged:
        report.skip(entry.image_id, "re-tagged")
    return report
