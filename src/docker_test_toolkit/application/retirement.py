"""Retire throwaway projects and archive real projects' terminal history into the sibling archive by rename."""

from __future__ import annotations

import hashlib
import json
import os
import stat
import time
from collections.abc import Callable, Mapping
from contextlib import ExitStack
from dataclasses import dataclass, field
from pathlib import Path

from docker_test_toolkit.application import admission, build_cache, lifecycle
from docker_test_toolkit.application.recovery import DOCKER_ID as _SHA256
from docker_test_toolkit.application.recovery import (
    RUN_FILE,
    UUID,
    Context,
    archived_run_files,
    lock,
    private_file_bytes,
    project_id_for,
    project_names,
    read_record,
    run_attribution,
    temporary_roots,
    verified_archived_run,
)
from docker_test_toolkit.application.sharing import remove_verdicts
from docker_test_toolkit.application.state import archive_directory
from docker_test_toolkit.domain.errors import (
    IncompatibleState,
    InfrastructureFailure,
    InvalidInput,
    ResourceBusy,
    ResourceError,
)
from docker_test_toolkit.domain.policy import known_fields
from docker_test_toolkit.domain.records import unique_object

_INCOMPATIBLE = "incompatible_state"
_INFRASTRUCTURE = "infrastructure_failure"
_CONTENTION = "contention"
# The closed reason-code table of the command contract; its order is the order reasons are reported in.
_CODES: dict[str, str] = {
    "root-present": _INCOMPATIBLE,
    "root-unobservable": _INFRASTRUCTURE,
    "record-invalid": _INCOMPATIBLE,
    "indexed-runs": _INCOMPATIBLE,
    "images-present": _INCOMPATIBLE,
    "pending-present": _INCOMPATIBLE,
    "build-records-present": _INCOMPATIBLE,
    "run-invalid": _INCOMPATIBLE,
    "run-unattributable": _INCOMPATIBLE,
    "run-nonterminal": _INCOMPATIBLE,
    "run-retained": _INCOMPATIBLE,
    "run-cleanup-pending": _INCOMPATIBLE,
    "run-recovery-incomplete": _INCOMPATIBLE,
    "run-not-quiescent": _INCOMPATIBLE,
    "run-operation-pending": _INCOMPATIBLE,
    "run-receipts-open": _INCOMPATIBLE,
    "run-consumers-active": _INCOMPATIBLE,
    "run-resources-present": _INCOMPATIBLE,
    "run-owned-docker-present": _INCOMPATIBLE,
    "run-endpoints-active": _INCOMPATIBLE,
    "private-root-present": _INCOMPATIBLE,
    "session-root-present": _INCOMPATIBLE,
    "actor-live": _CONTENTION,
    "actor-unverifiable": _INCOMPATIBLE,
    "queue-reference": _INCOMPATIBLE,
    "queue-unreadable": _INCOMPATIBLE,
    "lock-held": _CONTENTION,
    "drift": _INCOMPATIBLE,
    "account-changed": _INCOMPATIBLE,
    "archive-conflict": _INCOMPATIBLE,
    "reincarnated": _INCOMPATIBLE,
    "rename-failed": _INFRASTRUCTURE,
}
_RECOVER_CODES = frozenset(
    {"run-nonterminal", "run-cleanup-pending", "run-recovery-incomplete", "run-not-quiescent", "run-operation-pending"}
)
_MAX_PROJECTS = 1024
_EVIDENCE_FIELDS = frozenset({"project_id", "project_root", "project_record_sha256", "runs"})


@dataclass(frozen=True)
class RetirementGate:
    """Carry the account anchor bytes one retirement command was admitted under, and the actor observer."""

    marker: bytes
    observe_actor: Callable[[Mapping[str, object]], str]


def _observe_actor(identity: Mapping[str, object]) -> str:
    """Return ``executing``, ``exited`` or ``unknown`` for one recorded process identity."""
    try:
        origin = identity.get("origin")
        pid = identity.get("pid")
        birth = identity.get("birth")
        audit_token = identity.get("audit_token")
        if (
            not isinstance(origin, str)
            or type(pid) is not int
            or not isinstance(birth, list)
            or any(type(value) is not int for value in birth)
            or (
                audit_token is not None
                and (not isinstance(audit_token, list) or any(type(value) is not int for value in audit_token))
            )
        ):
            return "unknown"
        state = lifecycle.observe_process(lifecycle.ProcessIdentity(origin, pid, birth, audit_token))
        return "executing" if state == "executing" else "exited"
    except (OSError, ResourceError):
        return "unknown"


def _retirement_gate(account: Path, observe_actor: Callable[[Mapping[str, object]], str]) -> RetirementGate:
    """Read the account anchor once; an absent anchor is a fresh account that never ran (empty marker)."""
    anchor = account / "metadata.lock"
    marker = b"" if not os.path.lexists(anchor) else private_file_bytes(anchor)
    return RetirementGate(marker, observe_actor)


@dataclass
class _Scope:
    """Carry what one command invocation resolved once: roots, the admitted gate and the run-name index."""

    registry: Path
    account: Path
    archive: Path
    gate: RetirementGate
    index: dict[str, str | None]
    session_owners: dict[str, set[str]] | None = None


@dataclass
class _Derivation:
    """Hold every clause result for one project; reasons are the codes of the clauses that failed."""

    project_id: str
    project_root: str | None = None
    record_sha256: str | None = None
    reasons: set[str] = field(default_factory=set)
    runs: list[dict[str, object]] = field(default_factory=list)
    run_sizes: dict[str, int] = field(default_factory=dict)
    locators: list[dict[str, object]] = field(default_factory=list)
    archived_runs: list[str] = field(default_factory=list)
    shared_session_ids: list[str] = field(default_factory=list)
    root_absent: bool = False
    session_roots: list[Path] = field(default_factory=list)

    def ordered_reasons(self) -> list[str]:
        """Return the failed clauses in the closed table order, without duplicates."""
        return [code for code in _CODES if code in self.reasons]

    def category(self) -> str:
        """Return the most severe category among the reasons."""
        return _category(self.reasons)


def _category(codes: set[str] | list[str]) -> str:
    """Return the most severe category: infrastructure, then incompatible, then contention."""
    categories = {_CODES[code] for code in codes}
    for category in (_INFRASTRUCTURE, _INCOMPATIBLE):
        if category in categories:
            return category
    return _CONTENTION


def _evidence_item(item: object, invalid: InvalidInput) -> dict:
    """Validate one replayed evidence object; unknown fields are dropped at every level."""
    item = known_fields(item, _EVIDENCE_FIELDS, invalid=invalid)
    project_id, root, record_sha, runs = (
        item[key] for key in ("project_id", "project_root", "project_record_sha256", "runs")
    )
    if (
        not isinstance(project_id, str)
        or not _SHA256.fullmatch(project_id)
        or not isinstance(root, str)
        or not Path(root).is_absolute()
        or project_id_for(root) != project_id
        or not isinstance(record_sha, str)
        or not _SHA256.fullmatch(record_sha)
        or not isinstance(runs, list)
    ):
        raise invalid
    previous = ""
    runs = [known_fields(run, {"run_id", "sha256"}, invalid=invalid) for run in runs]
    for run in runs:
        run_id, sha256 = run["run_id"], run["sha256"]
        if (
            not isinstance(run_id, str)
            or not UUID.fullmatch(run_id)
            or run_id <= previous
            or not isinstance(sha256, str)
            or not _SHA256.fullmatch(sha256)
        ):
            raise invalid
        previous = run_id
    return {**item, "runs": runs}


def _parse_request(payload: object, *, execute: bool) -> tuple[str, list]:
    """Validate the request shape before any filesystem access; unknown fields are ignored.

    Returns ("bulk", []) without ``project_ids``, ("ids", [...]) for a dry-run selection and ("evidence", [...]) for
    execute.
    """
    invalid = InvalidInput("Retirement input is invalid.")
    if not isinstance(payload, dict):
        raise invalid
    if execute:
        items = known_fields(payload, {"evidence"}, invalid=invalid)["evidence"]
        if not isinstance(items, list) or not 1 <= len(items) <= _MAX_PROJECTS:
            raise invalid
        parsed = [_evidence_item(item, invalid) for item in items]
        if len({item["project_id"] for item in parsed}) != len(parsed):
            raise invalid
        return "evidence", parsed
    if "evidence" in payload:  # execute evidence without --execute: never silently widened to a bulk listing
        raise invalid
    if "project_ids" not in payload:
        return "bulk", []
    ids = payload["project_ids"]
    if (
        not isinstance(ids, list)
        or not 1 <= len(ids) <= _MAX_PROJECTS
        or any(not isinstance(item, str) or not _SHA256.fullmatch(item) for item in ids)
        or len(set(ids)) != len(ids)
    ):
        raise invalid
    return "ids", list(ids)


def _require_archive_root(registry: Path, archive: Path) -> None:
    """Validate the archive root without creating it; every failure is an infrastructure failure."""
    invalid = InfrastructureFailure("Retired-record archive root is invalid.")
    try:
        device = os.lstat(registry).st_dev
        for name in ("runs", "projects"):
            try:
                if os.lstat(registry / name).st_dev != device:
                    raise invalid
            except FileNotFoundError:
                continue
        if os.path.lexists(archive):
            observed = os.lstat(archive)
            private = not observed.st_mode & 0o077
        else:
            observed = os.lstat(archive.parent)
            private = True
        valid = (
            stat.S_ISDIR(observed.st_mode) and observed.st_uid == os.getuid() and observed.st_dev == device and private
        )
    except OSError as error:
        raise invalid from error
    if not valid:
        raise invalid


def _read_owner(path: Path) -> tuple[bool, str | None]:
    """Return (present, owner) for one run file; a file that vanished before it was opened is another command's."""
    try:
        raw = json.loads(private_file_bytes(path), object_pairs_hook=unique_object)
    except ResourceError as error:
        if isinstance(error.__cause__, FileNotFoundError):
            return False, None
        return True, None
    except (UnicodeDecodeError, ValueError, RecursionError):
        return True, None
    return True, run_attribution(raw)


def _run_index(registry: Path) -> dict[str, str | None]:
    """Map each canonical run name to its project once per command; unattributable entries map to None."""
    index: dict[str, str | None] = {}
    try:
        listing = sorted(os.scandir(registry / "runs"), key=lambda entry: entry.name)
    except FileNotFoundError:
        return index
    for entry in listing:
        match = RUN_FILE.fullmatch(entry.name)
        if match is None:
            continue
        present, owner = _read_owner(Path(entry.path))
        if present:
            index[match["run_id"]] = owner
    return index


def _absence(path: Path, present: str) -> str | None:
    """Return None only for ENOENT/ENOTDIR on the exact recorded string; never resolve."""
    try:
        os.lstat(path)
    except (FileNotFoundError, NotADirectoryError):
        return None
    except OSError:
        return "root-unobservable"
    return present


def _walk_session_root(path: Path, reasons: set[str]) -> None:
    """Walk one session root by lstat only: directories are acceptable, anything else is authority."""
    try:
        observed = os.lstat(path)
    except (FileNotFoundError, NotADirectoryError):
        return
    except OSError:
        reasons.add("root-unobservable")
        return
    if not stat.S_ISDIR(observed.st_mode):
        reasons.add("session-root-present")
        return
    try:
        with os.scandir(path) as entries:
            children = [Path(entry.path) for entry in entries]
    except (FileNotFoundError, NotADirectoryError):
        return
    except OSError:
        reasons.add("root-unobservable")
        return
    for child in children:
        _walk_session_root(child, reasons)


def _archived_runs(scope: _Scope, project_id: str, sessions: set[str]) -> list[str]:
    """List run ids already archived for this project that belong to its current sessions (reporting only)."""
    found: list[str] = []
    for path, match in archived_run_files(scope.archive / project_id):
        record = verified_archived_run(path, match)
        session_id = record.get("session_id") if record is not None else None
        if isinstance(session_id, str) and session_id in sessions:
            found.append(match["run_id"])
    return sorted(set(found))


def _actors(raw: dict) -> list[object]:
    """Return the identities that must have exited: requester, guardian and every consumer identity."""
    actors: list[object] = [raw.get("requester"), raw.get("guardian")]
    for group in (raw.get("consumers", {}), raw.get("container_consumers", {})):
        if isinstance(group, dict):
            actors.extend(item.get("identity") for item in group.values() if isinstance(item, dict))
    return actors


def _actor_reasons(scope: _Scope, raw: dict, reasons: set[str]) -> None:
    """Report an actor of the run that is still executing or whose state cannot be verified."""
    for actor in _actors(raw):
        if actor is None:
            continue
        observed = scope.gate.observe_actor(actor) if isinstance(actor, dict) else "unknown"
        if observed == "executing":
            reasons.add("actor-live")
        elif observed != "exited":
            reasons.add("actor-unverifiable")


def _derive_run(scope: _Scope, derivation: _Derivation, run_id: str, indexed: bool) -> None:
    """Evaluate every run clause for one run attributed to the project and record locators and evidence."""
    path = scope.registry / "runs" / f"{run_id}.json"
    reasons = derivation.reasons
    try:
        data = private_file_bytes(path)
        raw = json.loads(data, object_pairs_hook=unique_object)
    except (ResourceError, UnicodeDecodeError, ValueError, RecursionError):
        reasons.add("run-invalid")
        return
    if not isinstance(raw, dict):
        reasons.add("run-invalid")
        return
    sha256 = hashlib.sha256(data).hexdigest()
    derivation.runs.append({"run_id": run_id, "sha256": sha256})
    derivation.run_sizes[run_id] = len(data)

    context = lifecycle.recorded_run_context(scope.registry, run_id, raw)
    loaded = None
    if context is not None:
        try:
            loaded = lifecycle.load_run_record(context)
        except ResourceError:
            loaded = None
    if loaded is None:
        reasons.add("run-invalid")
        if raw.get("state") == "stopped-retained":
            reasons.add("run-retained")
        elif raw.get("state") != "complete":
            reasons.add("run-nonterminal")
        blockers: tuple[str, ...] = ()
    else:
        blockers = lifecycle.quiescence_blockers(loaded)
        reasons.update(blockers)
    private_root = raw.get("private_root")
    if isinstance(private_root, str) and Path(private_root).is_absolute():
        reason = _absence(Path(private_root), "private-root-present")
        if reason is not None:
            reasons.add(reason)
    _actor_reasons(scope, raw, reasons)
    if loaded is None or context is None:
        return
    action = None
    if loaded["state"] == "stopped-retained":
        action = "run.release"
    elif indexed or _RECOVER_CODES & set(blockers):
        action = "run.recover"
    if action is not None:
        derivation.locators.append({"action": action, "context": lifecycle.run_locator(context)})


def _queue_reasons(scope: _Scope, run_ids: set[str], reasons: set[str]) -> None:
    """Report a queue entry or reservation that still names one of the project's runs."""
    path = scope.account / "queue.json"
    if not os.path.lexists(path):
        return
    try:
        queue = json.loads(private_file_bytes(path), object_pairs_hook=unique_object)
        named = set(admission.to_logical(queue).entries)
    except (ResourceError, IncompatibleState, UnicodeDecodeError, ValueError, RecursionError):
        reasons.add("queue-unreadable")
        return
    if named & run_ids:
        reasons.add("queue-reference")


def _session_owners(scope: _Scope) -> dict[str, set[str]]:
    """Map each recorded session id to the projects that record it; read once per command (informational only)."""
    if scope.session_owners is None:
        owners: dict[str, set[str]] = {}
        for project_id in project_names(scope.registry):
            try:
                record = read_record(scope.registry / "projects" / f"{project_id}.json")
            except (ResourceError, OSError):
                continue
            for session_id in record["sessions"]:
                owners.setdefault(session_id, set()).add(project_id)
        scope.session_owners = owners
    return scope.session_owners


def _record_reasons(record: dict) -> set[str]:
    """Return the clauses the project record itself fails: indexed runs, images, pending work, build records."""
    reasons: set[str] = set()
    if record.get("runs"):
        reasons.add("indexed-runs")
    if record["images"]:
        reasons.add("images-present")
    pending = record.get("pending", {})
    if isinstance(pending, dict) and any(pending.values()):
        reasons.add("pending-present")
    if record.get("build_history") or record.get("build_cache"):
        reasons.add("build-records-present")
    return reasons


def _derive(scope: _Scope, project_id: str) -> _Derivation:
    """Evaluate every eligibility clause for one project without short-circuiting.

    Pure reads: no lock is taken and nothing is written. The result is advisory until execute re-derives it
    under the project's locks.
    """
    derivation = _Derivation(project_id)
    reasons = derivation.reasons
    if None in scope.index.values():
        reasons.add("run-unattributable")
    path = scope.registry / "projects" / f"{project_id}.json"
    try:
        encoded = private_file_bytes(path)
        record = read_record(path, encoded=encoded)
    except (ResourceError, OSError):
        reasons.add("record-invalid")
        return derivation
    derivation.record_sha256 = hashlib.sha256(encoded).hexdigest()
    root = record["project_root"]
    if Path(root).is_absolute() and project_id_for(root) == project_id:
        derivation.project_root = root
        reason = _absence(Path(root), "root-present")
        if reason is None:
            derivation.root_absent = True
        else:
            reasons.add(reason)
    else:
        reasons.add("record-invalid")
    indexed = set(record.get("runs", {}))
    reasons |= _record_reasons(record)
    run_ids = sorted(name for name, owner in scope.index.items() if owner == project_id)
    for run_id in run_ids:
        _derive_run(scope, derivation, run_id, run_id in indexed)
    _queue_reasons(scope, set(run_ids) | indexed, reasons)
    try:
        derivation.session_roots = temporary_roots(record)
        for session_root in derivation.session_roots:
            _walk_session_root(session_root, reasons)
    except ResourceError:
        reasons.add("record-invalid")
    sessions = set(record["sessions"])
    derivation.archived_runs = _archived_runs(scope, project_id, sessions)
    owners = _session_owners(scope)
    derivation.shared_session_ids = sorted(session for session in sessions if owners.get(session, set()) - {project_id})
    return derivation


def _bulk_selection(scope: _Scope) -> list[str]:
    """Select every project whose root is absent, plus present-root projects with archived runs (blocked only)."""
    selected = []
    for project_id in project_names(scope.registry):
        path = scope.registry / "projects" / f"{project_id}.json"
        try:
            record = read_record(path, encoded=private_file_bytes(path))
        except (ResourceError, OSError):
            selected.append(project_id)  # an unreadable record is reported, never silently skipped
            continue
        root = record["project_root"]
        absent_or_unusable = not Path(root).is_absolute() or _absence(Path(root), "root-present") != "root-present"
        if absent_or_unusable or _archived_runs(scope, project_id, set(record["sessions"])):
            selected.append(project_id)
    return selected


def _evidence(derivation: _Derivation) -> dict[str, object]:
    """Return the closed, replayable evidence object of an eligible project."""
    return {
        "project_id": derivation.project_id,
        "project_root": derivation.project_root,
        "project_record_sha256": derivation.record_sha256,
        "runs": sorted(derivation.runs, key=lambda item: str(item["run_id"])),
    }


def _select_projects(scope: _Scope, selection: tuple[str, list]) -> tuple[list[str], list[str]]:
    """Return (selected, not_found) project ids: the bulk selection, or the requested ids that have a record."""
    kind, requested = selection
    if kind == "bulk":
        return _bulk_selection(scope), []
    selected: list[str] = []
    not_found: list[str] = []
    for project_id in sorted(requested):
        found = os.path.lexists(scope.registry / "projects" / f"{project_id}.json")
        (selected if found else not_found).append(project_id)
    return selected, not_found


def _cache_sizes(scope: _Scope, project_id: str) -> dict[str, int] | str | None:
    """Return one project's build-cache bytes per platform without creating anything, or ``None`` without a cache.

    A cache that cannot be read is ``"error"``: the cache is never a retire failure (design 3.10).
    """
    try:
        inventory = build_cache.cache_inventory(build_cache.cache_root(scope.registry), project_id)
    except (OSError, ResourceError):
        return "error"
    return None if inventory is None else {name: entry["bytes"] for name, entry in inventory["platforms"].items()}


def _remove_cache(scope: _Scope, project_id: str) -> dict[str, int | str] | str | None:
    """Remove an archived project's build cache with no lock held; a failure is ``"error"``, never raised."""
    try:
        return build_cache.remove_project_cache(build_cache.cache_root(scope.registry), project_id)
    except (OSError, ResourceError):
        return "error"


def _dry_run(scope: _Scope, selection: tuple[str, list]) -> dict[str, object]:
    """List the selected projects as eligible (with evidence) or blocked (with reasons and locators)."""
    selected, not_found = _select_projects(scope, selection)
    eligible, blocked = [], []
    for project_id in selected:
        derivation = _derive(scope, project_id)
        if not derivation.reasons and derivation.root_absent:
            eligible.append(
                {
                    "evidence": _evidence(derivation),
                    "archived_runs": derivation.archived_runs,
                    "shared_session_ids": derivation.shared_session_ids,
                    "build_cache": _cache_sizes(scope, project_id),
                }
            )
        else:
            blocked.append(
                {
                    "project_id": project_id,
                    "project_root": derivation.project_root,
                    "category": derivation.category(),
                    "reasons": derivation.ordered_reasons(),
                    "archived_runs": derivation.archived_runs,
                    "locators": derivation.locators,
                    "build_cache": _cache_sizes(scope, project_id),
                }
            )
    return {
        "mode": "dry-run",
        "registry": str(scope.registry),
        "archive": str(scope.archive),
        "eligible": eligible,
        "blocked": blocked,
        "not_found": not_found,
        "summary": {
            "eligible": len(eligible),
            "blocked": len(blocked),
        },
    }


def _fsync_directory(path: Path) -> None:
    """Make a rename in `path` durable."""
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _ensure_private_directory(path: Path) -> None:
    """Create one archive directory with mode 0700 (fsyncing its parent), or require the existing one to be private."""
    try:
        path.mkdir(mode=0o700)
        _fsync_directory(path.parent)
    except FileExistsError:
        pass
    observed = os.lstat(path)
    if not stat.S_ISDIR(observed.st_mode) or observed.st_uid != os.getuid() or observed.st_mode & 0o077:
        raise InfrastructureFailure("Archive subdirectory is invalid.")


def _place(rename: Callable[[Path, Path], None], source: Path, destination: Path) -> bool:
    """Put one record at its archive name by rename; never copy and never overwrite.

    Returns False on a conflicting destination. An identical destination (an earlier attempt whose rename took
    effect) is not a conflict: the source is removed instead. Durability is a separate step, so the caller can
    report the move as progress before any directory sync can fail.
    """
    if os.path.lexists(destination):
        expected = source.read_bytes()
        try:
            identical = private_file_bytes(destination) == expected
        except ResourceError:
            identical = False
        if not identical:
            return False
        source.unlink()
        return True
    rename(source, destination)
    try:  # a rename keeps the record's own mtime; retention ages an archive from when it was archived (design 2.7)
        os.utime(destination, None)
    except OSError:
        pass  # retention also reads ctime, which the rename itself updated
    return True


def _sync(source: Path, destination: Path) -> None:
    """Make a move durable: fsync the directory it left and the directory it entered."""
    _fsync_directory(source.parent)
    _fsync_directory(destination.parent)


def _outcome(project_id: str, reasons: set[str] | list[str], moved: list[str], project_file: str | None = None) -> dict:
    """Build one per-project execute result: archived when no clause failed, refused otherwise."""
    codes = [code for code in _CODES if code in set(reasons)]
    return {
        "project_id": project_id,
        "outcome": "refused" if codes else "archived",
        "category": _category(codes) if codes else None,
        "reasons": codes,
        "archived_runs": list(moved),
        "archived_project": project_file,
    }


def _refresh_index(scope: _Scope, project_id: str) -> bool:
    """Bring the run-name index up to date under the project lock; return False when an indexed run vanished."""
    present = set()
    try:
        with os.scandir(scope.registry / "runs") as listing:
            for entry in listing:
                match = RUN_FILE.fullmatch(entry.name)
                if match is not None:
                    present.add(match["run_id"])
    except FileNotFoundError:
        pass  # a registry that never prepared a run has no runs/ at all
    if any(owner == project_id and name not in present for name, owner in scope.index.items()):
        return False
    for name in [name for name in scope.index if name not in present]:
        del scope.index[name]
    for name in sorted(present - set(scope.index)):
        known, owner = _read_owner(scope.registry / "runs" / f"{name}.json")
        if known:
            scope.index[name] = owner
    return True


def _lock_project(held: ExitStack, scope: _Scope, context: Context, project_id: str) -> str | None:
    """Take the project, its runs' and the registry locks without blocking; return a refusal code or None."""
    try:
        held.enter_context(lock(context, project_id, nonblocking=True))
    except ResourceBusy:
        return "lock-held"
    if not os.path.lexists(context.record) or not _refresh_index(scope, project_id):
        return "drift"
    try:
        for run_id in sorted(name for name, owner in scope.index.items() if owner == project_id):
            held.enter_context(lock(context, f"run-{run_id}", nonblocking=True))
        held.enter_context(lock(Context(scope.registry, scope.registry), "registry", nonblocking=True))
    except ResourceBusy:
        return "lock-held"
    return None


def _hold_metadata(held: ExitStack, scope: _Scope) -> str | None:
    """Take `metadata` exclusively (bounded non-blocking retries) and confirm the account is the one admitted.

    Returns a refusal code when the lock stays busy or the marker or qualification journal changed, else None.
    """
    deadline = time.monotonic() + 5
    while True:
        try:
            anchor = held.enter_context(lock(Context(scope.account, scope.account), "metadata", nonblocking=True))
            break
        except ResourceBusy:
            if time.monotonic() >= deadline:
                return "lock-held"
            time.sleep(0.05)
    if os.pread(anchor.fileno(), 4096, 0) != scope.gate.marker:
        return "account-changed"
    return None


def _final_reasons(scope: _Scope, derivation: _Derivation) -> set[str]:
    """Re-check, after the runs moved, that nothing of the project reappeared before its record moves.

    Only names that are new since the index are read, so this never rescans `runs/`. Every run of the project
    left the index when it moved, so any remaining owner id of the project is a run that appeared meanwhile.
    """
    reasons: set[str] = set()
    _refresh_index(scope, "")
    owners = set(scope.index.values())
    if derivation.project_id in owners:
        reasons.add("reincarnated")
    if None in owners:
        reasons.add("run-unattributable")
    if derivation.project_root is not None:
        reappeared = _absence(Path(derivation.project_root), "reincarnated")
        if reappeared is not None:
            reasons.add(reappeared)
    for session_root in derivation.session_roots:
        _walk_session_root(session_root, reasons)
    return reasons


def _execute_project(
    scope: _Scope,
    evidence: dict,
    rename: Callable[[Path, Path], None],
    failpoint: Callable[[str, str], None],
) -> tuple[dict, bool]:
    """Archive one reviewed project under its locks; return its result and whether the command must stop."""
    project_id = evidence["project_id"]
    context = Context(Path(evidence["project_root"]), scope.registry)
    moved: list[str] = []
    with ExitStack() as held:
        refusal = _lock_project(held, scope, context, project_id)
        if refusal:
            return _outcome(project_id, {refusal}, moved), False
        derivation = _derive(scope, project_id)
        if derivation.reasons:
            return _outcome(project_id, derivation.reasons, moved), False
        submitted = [(run["run_id"], run["sha256"]) for run in evidence["runs"]]
        derived = [
            (str(run["run_id"]), str(run["sha256"])) for run in sorted(derivation.runs, key=lambda r: str(r["run_id"]))
        ]
        if derivation.record_sha256 != evidence["project_record_sha256"] or derived != submitted:
            return _outcome(project_id, {"drift"}, moved), False
        refusal = _hold_metadata(held, scope)
        if refusal:
            return _outcome(project_id, {refusal}, moved), False
        archive_runs = scope.archive / project_id / "runs"
        project_file = None
        try:
            for directory in (scope.archive, scope.archive / project_id, archive_runs):
                _ensure_private_directory(directory)
            for run_id, sha256 in derived:
                source = scope.registry / "runs" / f"{run_id}.json"
                destination = archive_runs / f"{run_id}-{sha256}.json"
                if not _place(rename, source, destination):
                    return _outcome(project_id, {"archive-conflict"}, moved), False
                moved.append(run_id)
                scope.index.pop(run_id, None)
                _sync(source, destination)
                failpoint("after-run-rename", run_id)
            failpoint("before-final-rederivation", project_id)
            late = _final_reasons(scope, derivation)
            if late:
                return _outcome(project_id, late, moved), False
            failpoint("before-project-rename", project_id)
            remove_verdicts(scope.registry, project_id)  # under the held registry lock; a prediction, never authority
            destination = scope.archive / project_id / f"project-{derivation.record_sha256}.json"
            if not _place(rename, context.record, destination):
                return _outcome(project_id, {"archive-conflict"}, moved), False
            project_file = destination.name
            _sync(context.record, destination)
            failpoint("after-project-rename", project_id)
        except (OSError, InfrastructureFailure):
            return _outcome(project_id, {"rename-failed"}, moved, project_file), True
    return _outcome(project_id, [], moved, project_file), False


def _unreviewed_vanished_roots(scope: _Scope, reviewed: set[str]) -> list[str]:
    """List projects whose recorded root is absent but which were not named in the evidence (reported only)."""
    found = []
    for project_id in project_names(scope.registry):
        if project_id in reviewed:
            continue
        path = scope.registry / "projects" / f"{project_id}.json"
        try:
            root = read_record(path, encoded=private_file_bytes(path))["project_root"]
        except (ResourceError, OSError):
            continue
        if Path(root).is_absolute() and _absence(Path(root), "root-present") is None:
            found.append(project_id)
    return found


def _execute(
    scope: _Scope,
    evidence: list[dict],
    rename: Callable[[Path, Path], None],
    failpoint: Callable[[str, str], None] | None,
) -> dict[str, object]:
    """Archive each reviewed project in evidence order and stop after the first rename failure."""
    hook = failpoint or (lambda _stage, _identity: None)
    unreviewed = _unreviewed_vanished_roots(scope, {item["project_id"] for item in evidence})
    results: list[dict] = []
    stopped = False
    for item in evidence:
        if stopped:
            results.append(
                {
                    "project_id": item["project_id"],
                    "outcome": "not-attempted",
                    "category": None,
                    "reasons": [],
                    "archived_runs": [],
                    "archived_project": None,
                    "build_cache": None,
                }
            )
            continue
        try:
            result, stopped = _execute_project(scope, item, rename, hook)
        except (OSError, ResourceError):
            if not any(entry["outcome"] == "archived" or entry["archived_runs"] for entry in results):
                raise  # nothing has been mutated yet: an ordinary pre-mutation failure
            # Mutation has begun: keep the earlier results, stop here and report this project as failed.
            result, stopped = _outcome(item["project_id"], {"rename-failed"}, []), True
        # The cache goes only after the records are archived, with no lock held; a busy platform is no failure.
        archived = result["outcome"] == "archived"
        result["build_cache"] = _remove_cache(scope, item["project_id"]) if archived else None
        results.append(result)
    return {
        "mode": "execute",
        "registry": str(scope.registry),
        "archive": str(scope.archive),
        "results": results,
        "unreviewed_vanished_roots": unreviewed,
        "stopped": stopped,
    }


def retire_projects(
    registry: Path,
    account: Path,
    payload: object,
    *,
    execute: bool,
    rename: Callable[[Path, Path], None] = os.replace,
    failpoint: Callable[[str, str], None] | None = None,
    _observe_actor: Callable[[Mapping[str, object]], str] = _observe_actor,
) -> dict[str, object]:
    """List (dry-run) or archive (execute) throwaway projects whose recorded root is gone.

    Validates the closed request, reads the account anchor once (the gate the execute re-checks), validates
    the archive root without creating it and indexes `runs/` once. A dry-run derives every eligibility clause
    for each selected project from read-only evidence and mutates nothing; it is advisory. Execute takes only
    the evidence a dry-run emitted and, per project in that order, takes the project, run and registry locks,
    re-derives everything, then moves each run record and finally the project record into the archive by one
    atomic rename each (never a copy, never an overwrite), re-checking after the runs moved that nothing of the
    project reappeared. Every refusal after the gate is a per-project result, never an exception; a rename
    failure stops the command after the current project.

    Idempotent and resumable: the archive is the record of progress. Running the dry-run and execute again
    completes a project whose earlier attempt stopped part-way, and a finished project is simply absent.

    Args:
        registry: The registry (state) directory.
        account: The account directory whose anchor the command is admitted under.
        payload: `{}` or `{"project_ids": [...]}` for a dry-run, `{"evidence": [...]}` for execute.
        execute: Whether to archive instead of list.
        rename: The single rename seam, `os.replace` in production.
        failpoint: Optional test hook called as `failpoint(stage, identity)` at the durable transitions; it may
            raise to simulate a crash or mutate the world to simulate interference.
        _observe_actor: Test seam replacing the native process observation of recorded actors.

    Returns:
        The dry-run result (`eligible`, `blocked`, `not_found`, `summary`) or the execute result (`results`,
        `unreviewed_vanished_roots`, `stopped`).

    Raises:
        InvalidInput: If the request is not one of the closed shapes.
        IncompatibleState: If the account anchor is not private and account-owned.
        InfrastructureFailure: If the archive root is invalid.
        OSError: If a registry directory listing itself is unreadable.
    """
    selection = _parse_request(payload, execute=execute)
    registry = Path(registry).resolve()
    account = Path(account).resolve()
    gate = _retirement_gate(account, _observe_actor)
    archive = archive_directory(registry)
    _require_archive_root(registry, archive)
    scope = _Scope(registry, account, archive, gate, _run_index(registry))
    if execute:
        return _execute(scope, selection[1], rename, failpoint)
    return _dry_run(scope, selection)
