"""Account settings and shared-run state files: pure codecs; every caller owns the lock it needs."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from docker_test_toolkit.application.recovery import atomic_json, failpoint
from docker_test_toolkit.application.recovery import read_private_json as _read_private_json
from docker_test_toolkit.domain.errors import ResourceError
from docker_test_toolkit.domain.models import ExactInspection
from docker_test_toolkit.domain.policy import TURN_GRACE_NS, Grant, Turn, turn_current
from docker_test_toolkit.domain.share_policy import SHARE_RULES_VERSION

if TYPE_CHECKING:
    from docker_test_toolkit.application.ports.docker import DockerPort

HOST_RESERVE_DEFAULT = 9 * 1024**3 // 2  # a developer machine's own work: IDE, browser, other projects
HOST_RESERVE_FLOOR = 1024**3
HOST_RESERVE_FILE = "host-reserve.json"
_HOST_RESERVE_FIELDS = frozenset({"schema", "host_reserve_bytes"})
PARALLEL_RUNS_FILE = "parallel-runs.json"
_PARALLEL_RUNS_FIELDS = frozenset({"schema", "parallel_runs"})
SHARE_SCOPE_FILE = "share-scope.json"
_SHARE_SCOPE_FIELDS = frozenset({"schema", "share_scope"})
SHARE_SCOPES = ("machine", "repository")


@dataclass(frozen=True)
class AdmissionSettings:
    """The machine settings one admission decision reads: the run-count limit (None = ``auto``), the host reserve and
    the share scope (``machine``: any runs share; ``repository``: only runs of one repository share)."""

    limit: int | None
    host_reserve_bytes: int
    share_scope: str = "machine"


def _absent(path: Path) -> bool:
    return not path.exists() and not path.is_symlink()


def _header_ok(document: dict, fields: frozenset[str]) -> bool:
    """Return whether a closed account document has exactly ``fields`` and schema 1 (an int, never ``true``)."""
    return set(document) == fields and type(document["schema"]) is int and document["schema"] == 1


def read_host_reserve(account_directory: Path) -> int:
    """Return the account's declared host reserve, or the developer-machine default when none was declared.

    Raises:
        ResourceError: If the declaration is not exactly ``{"schema": 1, "host_reserve_bytes": n}`` with ``n`` at least
            the floor; an unreadable declaration fails closed.
    """
    path = account_directory / HOST_RESERVE_FILE
    if _absent(path):
        return HOST_RESERVE_DEFAULT
    document = _read_private_json(path)
    value = document.get("host_reserve_bytes")
    if not _header_ok(document, _HOST_RESERVE_FIELDS) or type(value) is not int or value < HOST_RESERVE_FLOOR:
        raise ResourceError("The account's host reserve declaration is invalid.")
    return value


def record_host_reserve(account_directory: Path, value: int) -> None:
    """Record the operator's host reserve for one account, replacing any earlier value (the installer's setting).

    Raises:
        ResourceError: If ``value`` is below the 1 GiB floor.
    """
    if type(value) is not int or value < HOST_RESERVE_FLOOR:
        raise ResourceError("A host reserve must be at least 1 GiB.")
    atomic_json(account_directory / HOST_RESERVE_FILE, {"host_reserve_bytes": value, "schema": 1})


DISK_RESERVE_FILE = "disk-reserve.json"
_DISK_RESERVE_FIELDS = frozenset({"schema", "disk_reserve_bytes"})
DISK_RESERVE_FLOOR = 1024**3
LEASES_FILE = "disk-leases.json"


def read_disk_reserve(account_directory: Path) -> int | None:
    """Return the declared disk reserve, or ``None`` when undeclared (each place then derives its own).

    Raises:
        ResourceError: If the declaration is not exactly ``{"schema": 1, "disk_reserve_bytes": n}`` with ``n`` at least
            the floor; an unreadable declaration fails closed.
    """
    path = account_directory / DISK_RESERVE_FILE
    if _absent(path):
        return None
    document = _read_private_json(path)
    value = document.get("disk_reserve_bytes")
    if not _header_ok(document, _DISK_RESERVE_FIELDS) or type(value) is not int or value < DISK_RESERVE_FLOOR:
        raise ResourceError("The account's disk reserve declaration is invalid.")
    return value


def declared_disk_reserve(account_directory: Path) -> int | None:
    """Return the declared disk reserve for an admission decision: an unreadable or invalid declaration counts as
    undeclared, so each place derives its own and no run is refused for a broken settings file."""
    try:
        return read_disk_reserve(account_directory)
    except (OSError, ResourceError):
        return None


def record_disk_reserve(account_directory: Path, value: int) -> None:
    """Record the operator's disk reserve for one account, replacing any earlier value (the installer's setting).

    Raises:
        ResourceError: If ``value`` is below the 1 GiB floor.
    """
    if type(value) is not int or value < DISK_RESERVE_FLOOR:
        raise ResourceError("A disk reserve must be at least 1 GiB.")
    atomic_json(account_directory / DISK_RESERVE_FILE, {"disk_reserve_bytes": value, "schema": 1})


@dataclass(frozen=True)
class Lease:
    """One holder's disk estimate, written at its reservation: bytes, their source and when."""

    bytes: int
    source: str
    clock_origin: str
    tick: int


def read_leases(account_directory: Path) -> dict[str, Lease]:
    """Return the account's disk leases by run ID; an absent or unreadable file reads empty.

    Reading empty is the conservative failure: every holder without a lease counts the disk floor.
    The caller holds the metadata lock M.
    """
    entries = _read_entries(account_directory / LEASES_FILE, "leases", ("bytes", "tick"), ("source", "clock_origin"))
    return {run_id: Lease(**entry) for run_id, entry in entries.items()}


def _read_entries(path: Path, key: str, ints: tuple[str, ...], strings: tuple[str, ...]) -> dict[str, dict]:
    """Return a closed account file's entries under ``key``, each exactly ``ints`` and ``strings`` typed as named;
    an absent or unreadable file, a wrong header or any malformed entry reads empty (each caller's conservative
    failure)."""
    try:
        document = {} if _absent(path) else _read_private_json(path)
    except (OSError, ResourceError):
        return {}
    entries = document.get(key)
    if not _header_ok(document, frozenset({"schema", key})) or not isinstance(entries, dict):
        return {}
    for entry in entries.values():
        if (
            not isinstance(entry, dict)
            or set(entry) != {*ints, *strings}
            or any(type(entry[name]) is not int for name in ints)
            or any(not isinstance(entry[name], str) for name in strings)
        ):
            return {}
    return entries


def write_leases(account_directory: Path, leases: Mapping[str, Lease], *, holders: set[str], clock_origin: str) -> None:
    """Replace the lease file with the leases of current holders of this clock origin; drop everything else.

    Dropping on every write keeps the file bounded by the account's holders. The caller holds the metadata lock M.
    """
    kept = {
        run_id: asdict(lease)
        for run_id, lease in leases.items()
        if run_id in holders and lease.clock_origin == clock_origin
    }
    _replace_or_remove(account_directory / LEASES_FILE, "leases", kept)


def _replace_or_remove(path: Path, key: str, kept: Mapping[str, object] | Sequence[object]) -> None:
    """Write ``{key: kept, schema: 1}`` to ``path``, or remove the file when nothing is kept."""
    if kept:
        atomic_json(path, {key: kept, "schema": 1})
    else:
        path.unlink(missing_ok=True)


TURNS_FILE = "turn.json"


def read_turns(account_directory: Path) -> dict[str, Turn]:
    """Return the repositories' turns; an absent or unreadable file reads empty: today's order.

    The caller holds the metadata lock M.
    """
    entries = _read_entries(
        account_directory / TURNS_FILE, "turns", ("released_ns",), ("run_id", "daemon_id", "clock_origin")
    )
    return {repository: Turn(**entry) for repository, entry in entries.items()}


def write_turns(
    account_directory: Path,
    turns: Mapping[str, Turn],
    *,
    now_ns: int,
    clock_origin: str,
    grace_ns: int = TURN_GRACE_NS,
) -> None:
    """Replace the turn file with the turns still within their grace on this clock origin; drop everything else.

    The caller holds the metadata lock M. Without a turn left the file is removed.
    """
    kept = {
        repository: asdict(turn)
        for repository, turn in turns.items()
        if turn_current(turn, now_ns=now_ns, clock_origin=clock_origin, grace_ns=grace_ns)
    }
    _replace_or_remove(account_directory / TURNS_FILE, "turns", kept)


def remove_turn(account_directory: Path, repository: str, run_id: str) -> None:
    """Remove a repository's turn only when ``run_id`` earned it (recovery of that run); the caller holds M."""
    turns = read_turns(account_directory)
    turn = turns.get(repository)
    if turn is None or turn.run_id != run_id:
        return
    del turns[repository]
    _replace_or_remove(account_directory / TURNS_FILE, "turns", {key: asdict(value) for key, value in turns.items()})


def valid_parallel_runs(value: object) -> bool:
    """Return whether ``value`` is a storable limit: an integer of at least 1, or ``"auto"`` (no count cap)."""
    return value == "auto" if isinstance(value, str) else type(value) is int and value >= 1


def read_parallel_runs(account_directory: Path) -> int | None:
    """Return the machine's run-count limit: None for ``auto``, which is also the default when none was declared.

    Raises:
        ResourceError: If the declaration is not exactly ``{"schema": 1, "parallel_runs": n | "auto"}``; an invalid
            declaration fails closed rather than falling back to a limit.
    """
    path = account_directory / PARALLEL_RUNS_FILE
    if _absent(path):
        return None
    document = _read_private_json(path)
    value = document.get("parallel_runs")
    if not _header_ok(document, _PARALLEL_RUNS_FIELDS) or not valid_parallel_runs(value):
        raise ResourceError("The account's parallel-runs declaration is invalid.")
    return None if value == "auto" else value


def record_parallel_runs(account_directory: Path, value: int | Literal["auto"]) -> None:
    """Record the machine's run-count limit, replacing any earlier value (the installer's setting).

    Raises:
        ResourceError: If ``value`` is neither an integer of at least 1 nor ``"auto"``.
    """
    if not valid_parallel_runs(value):
        raise ResourceError("The parallel-runs limit must be an integer of at least 1 or auto.")
    atomic_json(account_directory / PARALLEL_RUNS_FILE, {"parallel_runs": value, "schema": 1})


def read_share_scope(account_directory: Path) -> str:
    """Return the machine's share scope: ``machine`` (the default when none was declared) or ``repository``.

    Raises:
        ResourceError: If the declaration is not exactly ``{"schema": 1, "share_scope": "machine" | "repository"}``.
    """
    path = account_directory / SHARE_SCOPE_FILE
    if _absent(path):
        return "machine"
    document = _read_private_json(path)
    value = document.get("share_scope")
    if not _header_ok(document, _SHARE_SCOPE_FIELDS) or value not in SHARE_SCOPES:
        raise ResourceError("The account's share-scope declaration is invalid.")
    return str(value)


def record_share_scope(account_directory: Path, value: str) -> None:
    """Record the machine's share scope, replacing any earlier value (the installer's setting).

    Raises:
        ResourceError: If ``value`` is neither ``machine`` nor ``repository``.
    """
    if value not in SHARE_SCOPES:
        raise ResourceError("The share scope must be machine or repository.")
    atomic_json(account_directory / SHARE_SCOPE_FILE, {"share_scope": value, "schema": 1})


def repository_identity(project_root: str) -> str:
    """Return a project's repository identity: its Git ``origin`` address, normalized, else its resolved path.

    Every checkout of one repository (two CI runners' work folders, any folder name) has the same address; another
    repository has another. A checkout without Git or without ``origin`` is only itself, so it never shares by mistake.
    """
    return repository_origin(project_root) or str(Path(project_root).resolve())


def repository_origin(project_root: str) -> str | None:
    """Return a checkout's Git ``origin`` address, normalized, or ``None`` when it has no readable ``origin``.

    Share verdicts are keyed by it: a checkout without an origin has no repository identity worth teaching another
    checkout, so it neither predicts nor writes verdicts.
    """
    root = Path(project_root).resolve()
    git = root / ".git"
    try:
        if git.is_file():  # a worktree or submodule: ``gitdir: <path>``, whose config lives in the common directory
            pointed = Path(git.read_text().split("gitdir:", 1)[1].strip())
            git = (root / pointed).resolve()
            common = git / "commondir"
            if common.is_file():
                git = (git / common.read_text().strip()).resolve()
        section = None
        for line in (git / "config").read_text().splitlines():
            stripped = line.strip()
            if stripped.startswith("["):
                section = "".join(stripped.split())
            elif section == '[remote"origin"]':
                key, separator, value = stripped.partition("=")
                if separator and key.strip() == "url":
                    return _normalized_remote(value.strip().strip('"'), root)
    except (OSError, IndexError, UnicodeDecodeError):
        pass
    return None


def _normalized_remote(url: str, root: Path) -> str:
    """Return a Git remote as ``host[:port]/path`` (no scheme, user, ``.git`` or trailing slash), or a local remote's
    resolved path.

    A port stays (another port can be another server); the scp form ``host:path`` has none. A local remote (a path or
    ``file://``) is resolved against the checkout, so relative origins of unrelated trees never collide.
    """
    if url.startswith("file://"):
        return str((root / url[len("file://") :]).resolve())
    if "://" in url:
        address = url.split("://", 1)[1]
        authority, _, path = address.partition("/")
        host = authority.rsplit("@", 1)[-1].lower()
    elif ":" in url.split("/", 1)[0]:  # scp-like ``user@host:path``
        authority, _, path = url.partition(":")
        host = authority.rsplit("@", 1)[-1].lower()
    else:  # a local path
        return str((root / url).resolve())
    path = path.rstrip("/")
    path = path[: -len(".git")] if path.endswith(".git") else path
    return f"{host}/{path}"


def read_admission_settings(account_directory: Path) -> AdmissionSettings:
    """Read the limit, the host reserve and the share scope for one admission decision; the caller holds the account
    metadata lock.

    Raises:
        ResourceError: If a declaration is invalid.
    """
    settings = AdmissionSettings(
        read_parallel_runs(account_directory), read_host_reserve(account_directory), read_share_scope(account_directory)
    )
    failpoint("admission-settings-read")
    return settings


VERDICTS_DIRECTORY = "share-verdicts"
_VERDICT_FILE_FIELDS = frozenset({"schema", "rules", "verdicts"})
_VERDICT_FIELDS = frozenset({"shareable", "reason", "run_id", "origin", "tick_ns"})
# NOTE: fixed bound; ceiling: a project with more distinct selections gets exclusive runs for the evicted ones;
# upgrade path: an LRU keyed by last use.
VERDICTS_PER_PROJECT = 256


def request_key(repository: str, entrypoint: str, profile: str, closure: Sequence[str]) -> str:
    """Return the verdict-store key of one run request: what the request names, never its rendered Compose files.

    ``repository`` is the checkout's ``repository_origin``: two checkouts of one repository, such as two CI runners'
    work folders, share keys and can read each other's verdicts (``read_verdict``), while another repository's request
    with the same generic profile does not, even when both checkouts have the same folder name.
    """
    canonical = json.dumps([repository, entrypoint, profile, sorted(closure)], separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def verdict_path(registry: Path, project_id: str) -> Path:
    """Return the project's verdict store inside its registry.

    Raises:
        ResourceError: The project identity would leave the store directory (every reader, writer and remover).
    """
    path = registry / VERDICTS_DIRECTORY / f"{project_id}.json"
    if path.parent.parent != registry or path.name != f"{project_id}.json" or project_id.startswith("."):
        raise ResourceError("Verdict store project identity is invalid.")
    return path


def _stored_verdicts(path: Path) -> dict[str, dict] | None:
    """Return the valid verdicts of a store written under the running rules, or None when it predicts nothing."""
    if _absent(path):
        return None
    try:
        document = _read_private_json(path)
    except ResourceError:
        return None
    verdicts = document.get("verdicts")
    if (
        not _header_ok(document, _VERDICT_FILE_FIELDS)
        or document["rules"] != SHARE_RULES_VERSION
        or not isinstance(verdicts, dict)
    ):
        return None
    if not all(_valid_verdict(entry) for entry in verdicts.values()):
        return None  # one invalid entry makes the file invalid: it predicts nothing and the next writer replaces it
    return verdicts


def _valid_verdict(entry: object) -> bool:
    return (
        isinstance(entry, dict)
        and set(entry) == _VERDICT_FIELDS
        and type(entry["shareable"]) is bool
        and (entry["reason"] is None or isinstance(entry["reason"], str))
        and isinstance(entry["run_id"], str)
        and isinstance(entry["origin"], str)
        and type(entry["tick_ns"]) is int
    )


def read_verdict(registry: Path, project_id: str, key: str) -> bool | None:
    """Return the recorded shareability of one request key: True, False, or None when nothing usable is recorded.

    Every store of the registry with the same request (this checkout's and the repository's other checkouts') counts: a
    negative anywhere is False, so a clean run in one checkout never teaches shared what another checkout saw break a
    rule (a negative is never replaced, ``write_verdict``); else any positive is True. An absent,
    unreadable or invalid store, or one written under another rules version, predicts nothing (None), which admits the
    run exclusive. Read without a lock: a verdict is a prediction, never authority; a shared run still checks every
    mutation, so a wrong positive costs one refused command.
    """
    try:
        path = verdict_path(registry, project_id)
    except ResourceError:
        return None  # an identity outside the store predicts nothing
    recorded = [
        verdicts[key]["shareable"]
        for store in sorted({path, *path.parent.glob("*.json")})
        if (verdicts := _stored_verdicts(store) or {}) and key in verdicts
    ]
    return all(recorded) if recorded else None


def write_verdict(
    registry: Path,
    project_id: str,
    key: str,
    *,
    shareable: bool,
    reason: str | None,
    run_id: str,
    origin: str,
    tick_ns: int,
) -> None:
    """Record one request's verdict; the caller holds the project's ``registry`` lock.

    A store that is absent, invalid or written under other rules starts fresh. A positive verdict never replaces a
    negative one: a request's render can vary between its runs (an environment-chosen bind source), so a clean run
    proves nothing about the variant that broke a rule, and a positive over it would admit that variant shared and
    refuse it. A negative stays until the share rules change or the key is evicted. Beyond
    ``VERDICTS_PER_PROJECT`` keys, positives are evicted before negatives, another clock origin's entries before
    this one's, then the oldest tick. Raises OSError or ResourceError when it cannot write.
    """
    path = verdict_path(registry, project_id)
    verdicts = _stored_verdicts(path) or {}
    existing = verdicts.get(key)
    if shareable and existing is not None and existing["shareable"] is False:
        return
    verdicts[key] = {"shareable": shareable, "reason": reason, "run_id": run_id, "origin": origin, "tick_ns": tick_ns}
    # A negative is the safety memory, so positives go first; ticks restart low after a reboot, so an earlier boot's
    # entries go before this boot's; the key just written never goes.
    candidates = sorted(
        (name for name in verdicts if name != key),
        key=lambda name: (
            verdicts[name]["shareable"] is False,
            verdicts[name]["origin"] == origin,
            verdicts[name]["tick_ns"],
        ),
    )
    for oldest in candidates[: max(0, len(verdicts) - VERDICTS_PER_PROJECT)]:
        del verdicts[oldest]
    path.parent.mkdir(mode=0o700, exist_ok=True)
    atomic_json(path, {"schema": 1, "rules": SHARE_RULES_VERSION, "verdicts": verdicts})


def remove_verdicts(registry: Path, project_id: str) -> None:
    """Remove a project's verdict store; the caller holds the project's ``registry`` lock (retirement)."""
    verdict_path(registry, project_id).unlink(missing_ok=True)


PENDING_FILE = "pending-image-deletions.json"
_PENDING_FILE_FIELDS = frozenset({"schema", "entries"})
_PENDING_FIELDS = ("image_id", "created", "references", "owner_run", "owner_project", "daemon_id")


@dataclass(frozen=True)
class PendingImage:
    """One owned image handed off because another run still used it when its owner left."""

    image_id: str
    created: str
    references: tuple[str, ...]
    owner_run: str
    owner_project: str
    daemon_id: str  # the daemon the owner's grant named: no other daemon drains it, whatever its content ID


def pending_absent(account_directory: Path) -> bool:
    """Return whether the account has no pending image deletion list (the common case: no K is needed for it)."""
    return _absent(account_directory / PENDING_FILE)


def read_pending(account_directory: Path) -> list[PendingImage]:
    """Return the account's pending image deletions; the caller holds the image lock K.

    An absent file is empty. Raises ResourceError for an unreadable or invalid file: no hand-off and no drain then.
    """
    path = account_directory / PENDING_FILE
    if _absent(path):
        return []
    document = _read_private_json(path)
    entries = document.get("entries")
    if not _header_ok(document, _PENDING_FILE_FIELDS) or not isinstance(entries, list):
        raise ResourceError("Pending image deletion list is invalid.")
    pending = []
    for entry in entries:
        if (
            not isinstance(entry, dict)
            or set(entry) != set(_PENDING_FIELDS)
            or not all(
                isinstance(entry[key], str)
                for key in ("image_id", "created", "owner_run", "owner_project", "daemon_id")
            )
            or not isinstance(entry["references"], list)
            or not all(isinstance(reference, str) for reference in entry["references"])
        ):
            raise ResourceError("Pending image deletion entry is invalid.")
        pending.append(PendingImage(**{**entry, "references": tuple(entry["references"])}))
    return pending


def write_pending(account_directory: Path, entries: Sequence[PendingImage]) -> None:
    """Replace the account's pending image deletions atomically; the caller holds the image lock K.

    An empty list removes the file, so a machine back at limit 1 takes K for nothing.
    """
    _replace_or_remove(account_directory / PENDING_FILE, "entries", [asdict(entry) for entry in entries])


@dataclass(frozen=True)
class DrainOutcome:
    """What one drain did: the entries it removed, those it dropped as re-tagged and those left for want of budget."""

    removed: tuple[PendingImage, ...]
    retagged: tuple[PendingImage, ...]
    deferred: tuple[PendingImage, ...]


def drain_pending(
    adapter: DockerPort,
    account_directory: Path,
    daemon_id: str,
    in_use: Callable[[str], bool],
    hold_fd: int,
    *,
    drop_retagged: bool = False,
    allows: Callable[[], bool] = lambda: True,
    spend: Callable[[], None] = lambda: None,
    timeout_s: Callable[[], int] = lambda: 30,
    deadline: object | None = None,
) -> DrainOutcome:
    """Under K, delete each pending image of ``daemon_id`` nobody uses after its identity check; return the outcome.

    An entry stays in the list when it is in use, another daemon's, still present after the removal, or reached
    without budget (``allows``, ``spend`` and ``timeout_s`` bound a retention pass; those are ``deferred``). With
    ``drop_retagged`` an image that carries a tag the entry does not record is dropped from the list and never deleted
    (retention). An entry that is absent, or another image under the recorded ID, leaves the list without
    a deletion. The caller holds K and has checked that ``adapter`` reaches ``daemon_id``.
    """
    removed: list[PendingImage] = []
    retagged: list[PendingImage] = []
    deferred: list[PendingImage] = []
    kept: list[PendingImage] = []
    pending = read_pending(account_directory)
    for entry in pending:
        if not allows():
            deferred.append(entry)
            kept.append(entry)
            continue
        if entry.daemon_id != daemon_id or in_use(entry.image_id):
            kept.append(entry)
            continue
        observed = adapter.inspect_exact(ExactInspection("image", entry.image_id), deadline=deadline)
        same = observed.exists and observed.attributes.get("Id") == entry.image_id
        if not same or (entry.created and observed.attributes.get("Created") != entry.created):
            continue  # absent, or a different image under the recorded ID: nothing deleted, the entry leaves the list
        tags = observed.attributes.get("RepoTags") or []
        if drop_retagged and any(tag not in entry.references for tag in tags):
            retagged.append(entry)
            continue
        # Each reference is proven to name this image before it is removed: a tag moved to another image since the
        # hand-off is left alone, and with no reference left the image goes by its ID.
        references = tuple(
            reference
            for reference in entry.references
            if adapter.inspect_exact(ExactInspection("image", reference), deadline=deadline).attributes.get("Id")
            == entry.image_id
        )
        adapter.execute(
            ("image", "rm", *(references or (entry.image_id,))), timeout_seconds=timeout_s(), hold_fd=hold_fd
        )
        # Docker may delete the bytes and still fail on a stale alias: only the image's absence decides
        if adapter.inspect_exact(ExactInspection("image", entry.image_id), deadline=deadline).exists:
            kept.append(entry)  # still present: retried by the next image step
            continue
        failpoint("pending-image-removed")
        spend()
        removed.append(entry)
    if kept != pending:
        write_pending(account_directory, kept)
    return DrainOutcome(tuple(removed), tuple(retagged), tuple(deferred))


GRANTS_FILE = "shared-grants.json"
_GRANTS_FILE_FIELDS = frozenset({"schema", "grants"})
_GRANT_FIELDS = ("run_id", "generation", "origin", "granted_ns", "budget_bytes")


def _valid_grant(entry: object) -> bool:
    return (
        isinstance(entry, dict)
        and set(entry) == set(_GRANT_FIELDS)
        and all(isinstance(entry[key], str) and entry[key] for key in ("run_id", "origin"))
        and all(type(entry[key]) is int and entry[key] >= 0 for key in ("generation", "granted_ns", "budget_bytes"))
        and entry["generation"] >= 1
    )


def read_grants(account_directory: Path) -> list[Grant]:
    """Return the account's grants log; the caller holds the metadata lock M.

    An absent file is empty. Raises ResourceError for an unreadable or invalid file: one invalid entry invalidates the
    whole log, and the observer then treats the run as having shared with no shared-only breach.
    """
    path = account_directory / GRANTS_FILE
    if _absent(path):
        return []
    document = _read_private_json(path)
    grants = document.get("grants")
    if not _header_ok(document, _GRANTS_FILE_FIELDS) or not isinstance(grants, list):
        raise ResourceError("Shared grants log is invalid.")
    if not all(_valid_grant(entry) for entry in grants):
        raise ResourceError("Shared grants log entry is invalid.")
    return [Grant(**entry) for entry in grants]


def append_grant(account_directory: Path, grant: Grant, *, holder_baselines: Mapping[str, tuple[str, int]]) -> None:
    """Upsert one grant keyed by ``(run_id, generation, origin)`` and prune; the caller holds the metadata lock M.

    ``holder_baselines`` maps every current holder entry (the publisher included) to the origin and tick of its
    admission baseline. Pruned: grants of another clock origin than ``grant``'s, and grants of runs that hold no entry
    and were granted before the earliest holder baseline of that origin.

    Raises:
        ResourceError: The existing log is unreadable or invalid; nothing is written.
    """
    baselines = [tick for origin, tick in holder_baselines.values() if origin == grant.origin]
    earliest = min(baselines, default=grant.granted_ns)
    kept = [
        existing
        for existing in read_grants(account_directory)
        if existing.origin == grant.origin
        and (existing.run_id in holder_baselines or existing.granted_ns >= earliest)
        and (existing.run_id, existing.generation) != (grant.run_id, grant.generation)
    ]
    atomic_json(
        account_directory / GRANTS_FILE,
        {"schema": 1, "grants": [asdict(entry) for entry in [*kept, grant]]},
    )
