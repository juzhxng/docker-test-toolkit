"""Typed phase-admission validation and the account queue codec."""

import fcntl
import os
import re
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TextIO

from docker_test_toolkit.application.recovery import (
    UUID,
    atomic_bytes,
    failpoint,
    project_id_for,
    read_private_json,
)
from docker_test_toolkit.domain.errors import IncompatibleState, ResourceError, ToolkitError
from docker_test_toolkit.domain.models import PhaseBudget
from docker_test_toolkit.domain.policy import HOLDER_PHASES, NAME, STORED_RECOVERY_PHASES
from docker_test_toolkit.domain.records import canonical_json_bytes


def phase_budgets(data: dict, initial: str | None) -> dict[str, PhaseBudget]:
    """Validate and materialize one closed mapping of finite phase commitments."""
    if not isinstance(data, dict):
        raise ResourceError("Phase budgets must be a named mapping.")
    if (not data and initial is not None) or (data and (not isinstance(initial, str) or initial not in data)):
        raise ResourceError("Initial phase is absent from declared budgets.")
    result = {}
    for name, fields in data.items():
        if (
            not isinstance(name, str)
            or not NAME.fullmatch(name)
            or not isinstance(fields, dict)
            or set(fields) != set(PhaseBudget.__dataclass_fields__)
        ):
            raise ResourceError("Invalid phase name or incomplete budget.")
        try:
            result[name] = PhaseBudget(**fields)
        except ToolkitError as error:
            raise ResourceError(str(error)) from error
    return result


STATE_FORMAT = 1
STATE_MARKER = f"docker-test-toolkit-state:{STATE_FORMAT}\n"
_STATE_MARKER_FORMAT = re.compile(r"docker-test-toolkit-state:([1-9][0-9]{0,8})\n")


def _admit_marker(marker: str) -> str:
    """Admit an anchor's marker text: empty (a fresh account) or this state format.

    Raises:
        IncompatibleState: ``state-format-newer`` for the marker of a newer state format.
        ResourceError: For a marker of any other protocol.
    """
    if marker in ("", STATE_MARKER):
        return marker
    matched = _STATE_MARKER_FORMAT.fullmatch(marker)
    if matched is not None and int(matched.group(1)) > STATE_FORMAT:
        raise IncompatibleState(
            f"The account state has format {matched.group(1)}, newer than this toolkit's {STATE_FORMAT}; "
            "use the installed toolkit that wrote it.",
            reason="state-format-newer",
        )
    raise ResourceError("Account metadata anchor has an unsupported protocol.")


def _admit_anchor(anchor: TextIO) -> str:
    """Read the anchor marker and admit it (``_admit_marker``); returns the marker text."""
    anchor.seek(0)
    return _admit_marker(anchor.read(128))


def check_account_format(account: Path) -> None:
    """Refuse an account whose state format this toolkit does not read; an absent anchor (a fresh account) passes.

    The anchor is read under a shared ``flock`` on a descriptor of its own, closed before returning: ``flock``
    belongs to the open file description, so a lock kept here would block a later metadata section of this process.

    Raises:
        IncompatibleState: ``state-format-newer`` for a newer state format.
        ResourceError: For an unreadable anchor or a marker of another protocol.
    """
    try:
        descriptor = os.open(account / "metadata.lock", os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    except FileNotFoundError:
        return
    except OSError as error:
        raise ResourceError("Account metadata anchor is unreadable.") from error
    try:
        fcntl.flock(descriptor, fcntl.LOCK_SH)
        marker = os.read(descriptor, 128).decode("utf-8", errors="replace")
    except OSError as error:
        raise ResourceError("Account metadata anchor is unreadable.") from error
    finally:
        os.close(descriptor)
    _admit_marker(marker)


def _require_no_lost_history(marker: str) -> None:
    """Refuse a missing queue file when the account's marker says a queue once existed."""
    if marker:
        raise ResourceError("Initialized account queue history is missing; reconciliation required.")


_QUEUE_KEYS = frozenset({"version", "next_ticket", "entries"})
_DEADLINE_KEYS = frozenset({"origin", "created_ns", "duration_ns", "expires_ns"})
_ENTRY_KEYS = frozenset(
    {"ticket", "attempt", "record", "registry", "project_root"}
    | {"project_id", "phase", "holder", "dead_since", "takeover", "last_completed_phase"}
)
_PHASES = ("queued", *HOLDER_PHASES)
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


@dataclass
class QueueEntry:
    """One run's place in the account queue (queue protocol 2)."""

    run_id: str
    ticket: int
    attempt: str
    record: str
    registry: str
    project_root: str
    project_id: str
    phase: str
    holder: dict | None
    dead_since: dict | None
    takeover: dict | None
    last_completed_phase: str | None


@dataclass
class QueueState:
    """The account queue (queue protocol 2): the next ticket and every entry by run id."""

    next_ticket: int
    entries: dict[str, QueueEntry]


def _invalid(message: str) -> IncompatibleState:
    return IncompatibleState(message, reason="queue-invalid")


def _uuid(value: object) -> bool:
    return isinstance(value, str) and UUID.fullmatch(value) is not None


def _absolute(value: object) -> bool:
    return isinstance(value, str) and Path(value).is_absolute()


def _next_ticket(data: dict) -> int:
    if type(data["next_ticket"]) is not int or data["next_ticket"] < 1:
        raise _invalid("Invalid account queue metadata.")
    return data["next_ticket"]


def _validated_holder(value: object, validate_identity: Callable[[dict], object] | None) -> dict:
    if not isinstance(value, dict):
        raise _invalid("Invalid queue holder identity.")
    if validate_identity is not None:
        try:
            validate_identity(value)
        except ResourceError as error:
            raise _invalid("Invalid queue holder identity.") from error
    return value


def _check_entry(run_id: object, raw: object, keys: frozenset[str], next_ticket: int, tickets: set[int]) -> dict:
    """Validate the fields every queue entry carries and record its ticket as seen."""
    if not _uuid(run_id) or not isinstance(raw, dict) or set(raw) != keys:
        raise _invalid("Invalid account run reference.")
    ticket = raw["ticket"]
    if type(ticket) is not int or not 0 < ticket < next_ticket or ticket in tickets:
        raise _invalid("Invalid or duplicate account ticket.")
    tickets.add(ticket)
    if not _uuid(raw["attempt"]) or not all(_absolute(raw[key]) for key in ("record", "registry", "project_root")):
        raise _invalid("Invalid account reference identity/location.")
    return raw


def _check_dead_since(dead: object) -> None:
    if dead is not None and (
        not isinstance(dead, dict)
        or set(dead) != {"origin", "tick_ns"}
        or not isinstance(dead["origin"], str)
        or type(dead["tick_ns"]) is not int
    ):
        raise _invalid("Invalid queue death observation.")


def _is_deadline(value: object) -> bool:
    """Return whether ``value`` has the exact shape of a serialized native ``Deadline`` (lifecycle owns its meaning)."""
    return (
        isinstance(value, dict)
        and set(value) == _DEADLINE_KEYS
        and isinstance(value["origin"], str)
        and all(type(value[key]) is int for key in _DEADLINE_KEYS - {"origin"})
    )


def _check_takeover(takeover: object, validate_identity: Callable[[dict], object] | None) -> None:
    if takeover is None:
        return
    if (
        not isinstance(takeover, dict)
        or set(takeover) != {"attempt", "deadline", "claimant"}
        or not _uuid(takeover["attempt"])
        or not _is_deadline(takeover["deadline"])
    ):
        raise _invalid("Invalid queue takeover claim.")
    if takeover["claimant"] is not None:
        _validated_holder(takeover["claimant"], validate_identity)


def _check_v2_entry(run_id: str, raw: dict, validate_identity: Callable[[dict], object] | None) -> None:
    """Validate the version 2 fields of one entry: project binding, phase/holder agreement and recovery state."""
    if (
        not isinstance(raw["project_id"], str)
        or not _SHA256.fullmatch(raw["project_id"])
        or raw["project_id"] != project_id_for(raw["project_root"])
        or raw["record"] != f"{raw['registry']}/runs/{run_id}.json"
    ):
        raise _invalid("Queue entry identity contradicts its project.")
    if raw["phase"] not in _PHASES or (raw["holder"] is not None) != (raw["phase"] in HOLDER_PHASES):
        raise _invalid("Queue entry phase contradicts its holder.")
    if raw["holder"] is not None:
        _validated_holder(raw["holder"], validate_identity)
    _check_dead_since(raw["dead_since"])
    _check_takeover(raw["takeover"], validate_identity)
    if raw["last_completed_phase"] is not None and raw["last_completed_phase"] not in STORED_RECOVERY_PHASES:
        raise _invalid("Invalid queue recovery phase.")


def _queue_state(data: dict, validate_identity: Callable[[dict], object] | None) -> QueueState:
    next_ticket = _next_ticket(data)
    if not isinstance(data["entries"], dict):
        raise _invalid("Invalid account queue metadata.")
    tickets: set[int] = set()
    entries: dict[str, QueueEntry] = {}
    for run_id, raw in data["entries"].items():
        _check_entry(run_id, raw, _ENTRY_KEYS, next_ticket, tickets)
        _check_v2_entry(run_id, raw, validate_identity)
        entries[run_id] = QueueEntry(run_id=run_id, **{key: raw[key] for key in sorted(_ENTRY_KEYS)})
    return QueueState(next_ticket, entries)


def to_logical(data: object, *, validate_identity: Callable[[dict], object] | None = None) -> QueueState:
    """Validate decoded ``queue.json`` content (queue protocol 2) and return it as the queue model."""
    if (
        not isinstance(data, dict)
        or set(data) != _QUEUE_KEYS
        or type(data["version"]) is not int
        or data["version"] != 2
    ):
        raise _invalid("Invalid account queue metadata.")
    return _queue_state(data, validate_identity)


def _queue_data(state: QueueState) -> dict:
    return {
        "version": 2,
        "next_ticket": state.next_ticket,
        "entries": {
            run_id: {key: getattr(entry, key) for key in sorted(_ENTRY_KEYS)} for run_id, entry in state.entries.items()
        },
    }


def load_queue(
    directory: Path,
    anchor: TextIO,
    *,
    validate_identity: Callable[[dict], object] | None = None,
) -> QueueState:
    """Read the account queue under the caller-held metadata anchor.

    ``validate_identity`` checks guardian process identities (lifecycle owns that vocabulary and admission never imports
    it). A missing queue file is an empty account only when no initialization marker says history existed. Reading
    never rewrites the file.
    """
    marker = _admit_anchor(anchor)
    path = directory / "queue.json"
    if not path.exists() and not path.is_symlink():
        _require_no_lost_history(marker)
        return QueueState(1, {})
    if not marker:
        raise ResourceError("Account queue lacks its durable initialization marker.")
    return to_logical(read_private_json(path), validate_identity=validate_identity)


def save_queue(
    directory: Path,
    state: QueueState,
    anchor: TextIO,
) -> None:
    """Replace the account queue under the caller-held metadata anchor.

    The permanent locked inode survives queue-file loss, so the initialization marker is written before the first
    replacement and a crash cannot manufacture an empty account. A crash leaves either the old bytes or the complete
    new bytes.
    """
    if _admit_anchor(anchor) == "":
        anchor.seek(0)
        anchor.write(STATE_MARKER)
        anchor.flush()
        os.fsync(anchor.fileno())
    atomic_bytes(
        directory / "queue.json", canonical_json_bytes(_queue_data(state)), before_replace="queue-tempfile-written"
    )
    failpoint("queue-saved")


_REFERENCE_KEYS = ("attempt", "ticket", "record", "registry", "project_root")


def reference(entry: QueueEntry) -> dict:
    """Return the five identity fields that bind a queue entry to its original run (the v1 entry shape)."""
    return {key: getattr(entry, key) for key in _REFERENCE_KEYS}


def holders(state: QueueState) -> list[QueueEntry]:
    """Return every slot holder (admitting or granted), lowest ticket first."""
    return sorted((entry for entry in state.entries.values() if entry.phase in HOLDER_PHASES), key=lambda e: e.ticket)


def enroll(state: QueueState, run_id: str, ref: dict) -> int:
    """Add one run as ``queued`` with the next ticket, or return its existing ticket for the same reference.

    ``ref`` carries ``attempt``, ``record``, ``registry`` and ``project_root``. Tickets continue from
    ``state.next_ticket``, so they stay monotonic across the version 1 to 2 conversion. A retry by the same guardian
    keeps its ticket; a different reference for the same run is an ownership change and is refused.
    """
    prior = state.entries.get(run_id)
    if prior is not None:
        if {key: getattr(prior, key) for key in ref} != ref:
            raise ResourceError("Account run reference has changed ownership.")
        return prior.ticket
    ticket = state.next_ticket
    state.next_ticket += 1
    state.entries[run_id] = QueueEntry(
        run_id=run_id,
        ticket=ticket,
        attempt=ref["attempt"],
        record=ref["record"],
        registry=ref["registry"],
        project_root=ref["project_root"],
        project_id=project_id_for(ref["project_root"]),
        phase="queued",
        holder=None,
        dead_since=None,
        takeover=None,
        last_completed_phase=None,
    )
    return ticket


def reserve(state: QueueState, run_id: str, guardian: dict, *, limit: int | None = 1) -> None:
    """Move a queued entry to ``admitting`` with the guardian as one of the account's slot holders.

    The decision who may reserve beside whom belongs to the caller (``policy.reservation_verdict``); this codec keeps
    one backstop: it refuses with ``ResourceError`` while the holders already reach ``limit`` (``None`` = no count
    cap). Nothing changes when it refuses.
    """
    entry = state.entries.get(run_id)
    if entry is None or entry.phase != "queued":
        raise ResourceError("Queued run lost its account ticket identity.")
    if limit is not None and len(holders(state)) >= limit:
        raise ResourceError("Account capacity is full.")
    entry.phase = "admitting"
    entry.holder = guardian


def grant(state: QueueState, run_id: str, guardian: dict) -> None:
    """Mark the reserving guardian's entry ``granted`` (the run record already carries the published grant)."""
    entry = state.entries.get(run_id)
    if entry is None or entry.holder != guardian or entry.phase not in HOLDER_PHASES:
        raise ResourceError("Grant publisher is not the reserved holder.")
    entry.phase = "granted"


def remove(state: QueueState, run_id: str) -> QueueEntry | None:
    """Remove one run's entry (releasing the slot when it was the holder) and return it, or None when absent."""
    return state.entries.pop(run_id, None)
