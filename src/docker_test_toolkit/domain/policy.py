"""Pure validation and closed compatibility policies."""

import hashlib
import ipaddress
import json
import math
import re
import urllib.parse
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any

from docker_test_toolkit.domain.errors import ToolkitError

if TYPE_CHECKING:  # models imports this module, so its types are only named for the checker
    from docker_test_toolkit.domain.models import MachineFacts, PhaseBudget

BUILD_VALUE_LETTERS = frozenset("fot")  # the Buildx short options that take a value (-f, -o, -t)
NAME = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}\Z")
DEAD_GRACE_NS = (
    30_000_000_000  # NOTE: fixed 30 s dead grace; ceiling: one constant per artifact; upgrade path: profile setting
)
UNBOUND_CLAIM_NS = 30_000_000_000
TAKEOVER_ACCOUNT_CAP = (
    2  # NOTE: account worker cap 2; ceiling: two concurrent takeovers; upgrade path: configurable cap
)
RECOVERY_PHASES = ("authorize", "claim", "operation", "receipts", "consumers", "resources", "private-root", "publish")
STORED_RECOVERY_PHASES = RECOVERY_PHASES[1:]  # ``authorize`` leaves no durable trace, so a queue entry never stores it
HOLDER_PHASES = ("admitting", "granted")  # the queue phases in which an entry holds one of the account's slots
BUILDER_MEMORY_BYTES = 2 * 1024**3  # the calibrated per-run Buildx worker envelope (hard limits)
BUILDER_DAEMON_CPUS = 2
VERSION = re.compile(r"([0-9]+)\.([0-9]+)\.([0-9]+)([-+][a-zA-Z0-9.-]+)?\Z")  # release, then a suffix
_DEAD_STATES = ("absent", "replaced")
_CREATION_PREFIX_VALUES: tuple[tuple[str, object], ...] = (
    ("state", "prepared"),
    ("admission", "never-granted"),
    ("generation", 0),
    ("revision", 0),
    ("guardian_attempt", None),
    ("ticket", None),
    ("account_directory", None),
    ("artifact", None),
    ("pending_operation", None),
    ("bootstrap_deadline", None),
)


def known_fields(
    value: object, required: Iterable[str], optional: Iterable[str] = (), *, invalid: ToolkitError
) -> dict[str, Any]:
    """Return a request object's known fields; fields a newer peer adds are ignored (design E3).

    Raises:
        ToolkitError: ``invalid``, when ``value`` is no object or lacks a required field.
    """
    required = frozenset(required)
    if not isinstance(value, dict) or not required <= value.keys():
        raise invalid
    known = required | frozenset(optional)
    return {key: item for key, item in value.items() if key in known}


class AdmissionMode(StrEnum):
    """How a run holds the account (design 2.4): beside other shared holders, or alone. Records store the value."""

    SHARED = "shared"
    EXCLUSIVE = "exclusive"


def require_name(value: str, label: str) -> str:
    """Return one bounded public name or raise the stable toolkit error."""
    if not isinstance(value, str) or not NAME.fullmatch(value):
        raise ToolkitError(f"Invalid {label}; expected a bounded public name.")
    return value


@dataclass(frozen=True)
class QueueEntryView:
    """Carry exactly what the FIFO decision reads about one queue entry."""

    run_id: str
    ticket: int
    phase: str  # "queued" | "admitting" | "granted"
    project_id: str
    blocked: bool
    guardian_state: str | None  # observe_process vocabulary (absent, replaced, zombie, executing), None if unknown
    mode: str | None = None  # an AdmissionMode value from the entry's record; None when the record is unreadable
    version_ok: bool = True  # whether the entry's record names a version that reads multi-holder queues (M1)


@dataclass(frozen=True)
class ReservationVerdict:
    """The run that may reserve next, or None with the waiting reason the caller records."""

    run_id: str | None
    reason: str | None


def _live(entry: QueueEntryView) -> bool:
    return not entry.blocked and entry.guardian_state not in _DEAD_STATES


def live_queued(entries: Sequence[QueueEntryView]) -> list[QueueEntryView]:
    """Return the queued entries that are neither blocked nor proven dead, in ticket order (the FIFO line)."""
    return sorted((entry for entry in entries if entry.phase == "queued" and _live(entry)), key=lambda e: e.ticket)


def next_grantable(entries: Sequence[QueueEntryView]) -> str | None:
    """Return the run id that may take a slot next, or None.

    The account has one slot: while any entry holds it (a dead holder too, until its takeover removes the entry),
    nothing is grantable. Otherwise the head is the lowest ticket among queued entries, skipping blocked entries and
    entries whose guardian is proven dead (absent or replaced): neither can take the slot, and neither may stall the
    queue. An unprovable state (zombie, unknown) is not death, so that entry keeps its place.
    """
    return None if any(entry.phase in HOLDER_PHASES for entry in entries) else fifo_head(entries)


def fifo_head(entries: Sequence[QueueEntryView]) -> str | None:
    """Return the lowest-ticket queued entry that is neither blocked nor proven dead: the only run that may reserve."""
    queued = sorted((entry for entry in entries if entry.phase == "queued" and _live(entry)), key=lambda e: e.ticket)
    return queued[0].run_id if queued else None


def reservation_verdict(entries: Sequence[QueueEntryView], *, head_mode: str, limit: int | None) -> ReservationVerdict:
    """Return which run may reserve next under the machine limit (design 2.4, rules 0-5), without the fit.

    Rule 0: at limit 1 today's ``next_grantable`` decides, with today's reason ``queue-position``. Above it, only the
    FIFO head (lowest live, unblocked queued ticket) may reserve: alone when no entry holds; beside admitting and
    granted holders only when the head (``head_mode``) and every holder are shared, holders stay below ``limit``
    (``None`` = no cap) and every queue entry passes the version gate (M1). The caller compares the returned run with
    itself and then checks the fit; the reasons are positional waiting reasons, never error reasons.
    """
    if limit == 1:
        chosen = next_grantable(entries)
        return ReservationVerdict(chosen, None if chosen is not None else "queue-position")
    head = fifo_head(entries)
    if head is None:
        return ReservationVerdict(None, "queue-position")
    holding = [entry for entry in entries if entry.phase in HOLDER_PHASES]
    if not holding:
        return ReservationVerdict(head, None)
    if head_mode != AdmissionMode.SHARED or any(entry.mode != AdmissionMode.SHARED for entry in holding):
        return ReservationVerdict(None, "exclusive-holder")
    if limit is not None and len(holding) >= limit:
        return ReservationVerdict(None, "parallel-limit")
    if not all(entry.version_ok for entry in entries):
        return ReservationVerdict(None, "older-version-entry")
    return ReservationVerdict(head, None)


def publication_verdict(
    entries: Sequence[QueueEntryView], *, publisher: str, publisher_mode: str, limit: int | None
) -> str | None:
    """Return None when ``publisher`` may publish its grant, else the waiting reason (design 2.4, R3-2).

    Checks in order: an earlier live, unblocked admitting entry publishes first (grant order; admitting entries never
    count toward the limit); with no granted co-holder the publisher publishes alone; at limit 1 it waits
    (``queue-position``, only after a lowering); an exclusive publisher or a co-holder that is not readably shared
    waits (``exclusive-holder``); co-holders at the limit wait (``parallel-limit``); a queue entry failing the version
    gate waits (``older-version-entry``). The order matches ``reservation_verdict``. The fit is checked afterwards by
    the caller.
    """
    own = next((entry for entry in entries if entry.run_id == publisher), None)
    if own is None:
        raise ValueError("The publisher has no queue entry.")
    if any(
        entry.phase == "admitting" and entry.ticket < own.ticket and _live(entry)
        for entry in entries
        if entry.run_id != publisher
    ):
        return "queue-position"
    coholders = [entry for entry in entries if entry.phase == "granted" and entry.run_id != publisher]
    if not coholders:
        return None
    if limit == 1:
        return "queue-position"
    if publisher_mode != AdmissionMode.SHARED or any(entry.mode != AdmissionMode.SHARED for entry in coholders):
        return "exclusive-holder"
    if limit is not None and len(coholders) >= limit:
        return "parallel-limit"
    if not all(entry.version_ok for entry in entries):
        return "older-version-entry"
    return None


def version_at_least(version: object, minimum: str) -> bool:
    """Return whether ``version`` is a release not older than ``minimum`` (numeric triple; a suffix sorts first).

    An unparsable or missing version counts as older.
    """

    def key(value: object) -> tuple[tuple[int, int, int], int] | None:
        """Sort key: the numeric triple, then 0 for a pre-release suffix and 1 for a release; None if unparsable."""
        found = VERSION.fullmatch(value) if isinstance(value, str) else None
        if found is None:
            return None
        triple = (int(found.group(1)), int(found.group(2)), int(found.group(3)))
        return triple, 0 if found.group(4) else 1

    observed, required = key(version), key(minimum)
    return observed is not None and required is not None and observed >= required


def admission_gate(
    *, test_composition: bool, release_helper: bool, loaded: Path | None, current: Path | None
) -> str | None:
    """Return the version gate's refusal reason for a new test run, or None to admit it (design 3.2, invariant I4).

    The release helper is cleanup, not a new test run, and is admitted first. A test composition without a bound
    installation (``loaded is None``) admits, as private compositions skip the real account. Otherwise code that does
    not run from an installation with a ``current`` link refuses with ``toolkit-not-installed``, and code whose loaded
    version folder is not the resolved ``current`` refuses with ``toolkit-upgraded``. Both paths are resolved by the
    caller.
    """
    if release_helper or (test_composition and loaded is None):
        return None
    if loaded is None or current is None:
        return "toolkit-not-installed"
    return None if loaded == current else "toolkit-upgraded"


def actors_dead(states: Iterable[str | None]) -> bool:
    """Return whether every observed actor state proves death (absent or replaced); anything unprovable is not death."""
    return all(state in _DEAD_STATES for state in states)


def claim_eligible(
    takeover: Mapping[str, Any] | None,
    *,
    caller_attempt: str | None,
    claimant_state: str | None,
    now_tick_ns: int,
    origin: str,
) -> bool:
    """Return whether a recoverer may proceed against a queue entry's takeover claim (one predicate for all paths).

    A worker (``caller_attempt`` given) proceeds only on its own spawn attempt: unbound and not yet expired, or bound
    to a claimant proven dead. A cleared claim, another attempt (even an expired one) and a live or unknown claimant
    refuse. Every other recoverer (operator, detection) proceeds on no claim, on a claimant proven dead, or on an
    unbound claim past its deadline or from another clock origin (a reboot). A zombie or an unknown observation is
    never death.
    """
    if takeover is None:
        return caller_attempt is None
    if caller_attempt is not None and takeover["attempt"] != caller_attempt:
        return False
    if takeover["claimant"] is not None:
        return claimant_state in _DEAD_STATES
    deadline = takeover["deadline"]
    unexpired = deadline["origin"] == origin and now_tick_ns <= deadline["expires_ns"]
    return unexpired if caller_attempt is not None else not unexpired


def spawn_eligible(
    entry: Mapping[str, Any],
    *,
    claimant_state: str | None,
    now_tick_ns: int,
    origin: str,
    live_or_unbound_claims: int,
    spawned_this_pass: bool,
) -> bool:
    """Return whether a dead queue entry may get a takeover worker spawned for it in this admission pass.

    Requires: no other spawn this pass; fewer than the account cap of live or unbound-unexpired claims; the entry seen
    dead for at least the grace on the same clock origin (a changed origin restarts the grace); and its own claim free
    (null, unbound and expired, or bound to a proven-dead claimant). An unblocked entry is the caller's precondition.
    """
    if spawned_this_pass or live_or_unbound_claims >= TAKEOVER_ACCOUNT_CAP:
        return False
    dead_since = entry["dead_since"]
    if dead_since is None or dead_since["origin"] != origin or now_tick_ns - dead_since["tick_ns"] < DEAD_GRACE_NS:
        return False
    return claim_eligible(
        entry["takeover"], caller_attempt=None, claimant_state=claimant_state, now_tick_ns=now_tick_ns, origin=origin
    )


def is_creation_prefix(record: Mapping[str, object]) -> bool:
    """Return whether a run record is exactly the state written by ``prepare_run`` before its index entry.

    A pure field predicate: no process liveness, no filesystem and no index are consulted here.
    """
    missing = object()
    return all(
        (value := record.get(field, missing)) is not missing and type(value) is type(expected) and value == expected
        for field, expected in _CREATION_PREFIX_VALUES
    )


@dataclass(frozen=True)
class PhaseReservation:
    """The finite capacity one run keeps for itself: the field-wise maximum of its phase budgets (``R(r)``)."""

    container_bytes: int
    host_process_bytes: int
    daemon_cpus: int
    host_threads: int


def reservation(budgets: Iterable["PhaseBudget"]) -> PhaseReservation:
    """Return the field-wise maximum over a run's phase budgets: what it may need at its largest in every dimension."""
    items = list(budgets)
    return PhaseReservation(
        max(item.container_bytes for item in items),
        max(item.host_process_bytes for item in items),
        max(item.daemon_cpus for item in items),
        max(item.host_threads for item in items),
    )


def admits_builder(phases: Mapping[str, "PhaseBudget"]) -> bool:
    """Return whether some declared phase admits the Buildx worker envelope (2 GiB and 2 daemon CPUs)."""
    return any(
        budget.container_bytes >= BUILDER_MEMORY_BYTES and budget.daemon_cpus >= BUILDER_DAEMON_CPUS
        for budget in phases.values()
    )


def effective_reservation(phases: Mapping[str, "PhaseBudget"], *, builder_removed: bool = False) -> PhaseReservation:
    """Return ``E(r)``: the run's reservation plus the builder's memory while it may still run a builder (design N2b).

    A run that removed its builder holds no builder reserve: a shared run may not create another (share rule R16), so
    the 2 GiB would stand empty beside its co-holders (two such runs did not fit together on a 24 GB CI host).
    """
    reserved = reservation(phases.values())
    if builder_removed or not admits_builder(phases):
        return reserved
    return replace(reserved, container_bytes=reserved.container_bytes + BUILDER_MEMORY_BYTES)


def combined(items: Iterable[PhaseReservation]) -> PhaseReservation:
    """Return the field-wise sum of reservations (``E(S)``); the sum of none is zero."""
    listed = list(items)
    return PhaseReservation(
        sum(item.container_bytes for item in listed),
        sum(item.host_process_bytes for item in listed),
        sum(item.daemon_cpus for item in listed),
        sum(item.host_threads for item in listed),
    )


@dataclass(frozen=True)
class HolderFacts:
    """What one holder entry contributes to the account's accounting, derived from its queue entry and record.

    ``compose_project`` is the run-scoped Compose project; ``builder_worker`` the BuildKit worker container name when
    the run admits a builder (its reserve is part of ``reservation``), else None.
    """

    run_id: str
    reservation: PhaseReservation
    compose_project: str
    builder_worker: str | None


def memory_limit(container: Mapping[str, Any]) -> int | None:
    """Return an inspected container's hard memory limit, or None when it has none (``HostConfig.Memory``)."""
    host = container.get("HostConfig")
    limit = host.get("Memory") if isinstance(host, Mapping) else None
    return limit if type(limit) is int and limit > 0 else None


def attributed_ids(containers: Sequence[Mapping[str, Any]], holder: HolderFacts, *, run_label: str) -> frozenset[str]:
    """Return the running containers that count against a holder's reservation instead of as foreign (design 2.4).

    A container is the holder's when it carries the holder's Compose project label or its ``run_label`` value, or is
    its BuildKit worker. Each needs a hard memory limit. The worker is attributed within the builder reserve; the
    others, in ID order, within the reservation minus that reserve when the run admits a builder (R3-9). A container
    beyond the bound, or without a limit, stays external. Attribution is accounting only, never authority.
    """
    builder_bound = BUILDER_MEMORY_BYTES if holder.builder_worker is not None else 0
    bound, total, attributed = holder.reservation.container_bytes - builder_bound, 0, set()
    for container in sorted(containers, key=lambda item: str(item.get("Id"))):
        state, config = container.get("State"), container.get("Config")
        labels = config.get("Labels") if isinstance(config, Mapping) else None
        name = container.get("Name")
        limit = memory_limit(container)
        if not isinstance(state, Mapping) or state.get("Running") is not True or limit is None:
            continue
        if holder.builder_worker is not None and isinstance(name, str) and name.lstrip("/") == holder.builder_worker:
            if limit <= builder_bound:
                attributed.add(str(container["Id"]))
            continue
        owned = isinstance(labels, Mapping) and (
            labels.get("com.docker.compose.project") == holder.compose_project or labels.get(run_label) == holder.run_id
        )
        if owned and total + limit <= bound:
            total += limit
            attributed.add(str(container["Id"]))
    return frozenset(attributed)


# NOTE: one fixed threshold on the kernel's minute average; an operator setting is the upgrade if machines differ.
CPU_PRESSURE_JOIN_LIMIT = 50.0  # percent of time some task waited for a CPU (Linux PSI ``some avg60``)
_CPU_PRESSURE = re.compile(r"some avg10=[0-9.]+ avg60=([0-9]+\.[0-9]+) ")


def parse_cpu_pressure(text: str) -> float | None:
    """Return the ``some avg60`` figure of ``/proc/pressure/cpu``, or None when the text is not that file's format."""
    match = _CPU_PRESSURE.match(text)
    if match is None:
        return None
    value = float(match.group(1))
    return value if 0 <= value <= 100 else None


PROBE_SENTINEL = "::dtt::"


def split_probe_output(text: str) -> tuple[str, str]:
    """Split the VM probe's output at its sentinel line into ``(psi_text, df_text)``; without one, ``("", "")``.

    The two halves are parsed independently: a kernel without PSI must not cost the disk figure, nor the reverse.
    """
    lines = text.splitlines(keepends=True)
    for index, line in enumerate(lines):
        if line.rstrip("\n") == PROBE_SENTINEL:
            return "".join(lines[:index]), "".join(lines[index + 1 :])
    return "", ""


def parse_df(text: str) -> tuple[int, int] | None:
    """Return ``(total_bytes, available_bytes)`` of the one filesystem in ``df -P -k`` output, or None otherwise.

    POSIX df may wrap a long device name onto its own line, so the figures are read from the data rows joined.
    """
    lines = text.strip().splitlines()
    if len(lines) < 2 or not lines[0].startswith("Filesystem"):
        return None
    fields = " ".join(lines[1:]).split()
    if len(fields) != 6:
        return None
    try:
        total, _used, available = (int(field) for field in fields[1:4])
    except ValueError:
        return None
    if total <= 0 or not 0 <= available <= total:
        return None
    return total * 1024, available * 1024


def cpu_pressure_blocks_join(avg60: float | None) -> bool:
    """Whether a run must wait before joining holders: CPU pressure at the limit (no figures never block).

    Memory admission does not see CPU: on a small CI host the jobs' own steps can keep every core busy, and a further
    stack then stalls its services past their timeouts. Only joins wait; a run alone is never held back by it.
    """
    return avg60 is not None and avg60 >= CPU_PRESSURE_JOIN_LIMIT


def net_external_limits(
    external_limits: Sequence[int | None], containers: Sequence[Mapping[str, Any]], attributed: frozenset[str]
) -> tuple[int | None, ...]:
    """Remove one occurrence of each attributed container's memory limit from the external commitments."""
    remaining = list(external_limits)
    for container in containers:
        limit = memory_limit(container)
        if container.get("Id") in attributed and limit in remaining:
            remaining.remove(limit)
    return tuple(remaining)


def short_options(argv: tuple[str, ...], value_letters: frozenset[str]) -> list[tuple[str, str | None]]:
    """Return the short options the Docker CLI reads from argv as ``(letter, attached value)``.

    Mirrors the flag parser: ``-ovalue``, ``-o=value`` and clusters such as ``-qovalue`` all
    yield the option letters. A letter in ``value_letters``, or any letter followed by ``=``,
    ends its cluster and takes the rest of the token as its value (``None`` when the value is
    the next argument). Other letters are read as booleans, so a wrongly assumed boolean can
    only over-refuse and never hide an option. Because every letter of a cluster is read, an
    attached value after a boolean letter that contains a refused letter is refused too
    (``up -fcompose.yml`` contains ``p``); that is intended, not a parsing bug.
    """
    found: list[tuple[str, str | None]] = []
    for token in argv:
        if len(token) < 2 or token[0] != "-" or token[1] == "-":
            continue
        for position, letter in enumerate(token[1:], start=1):
            rest = token[position + 1 :]
            if letter not in value_letters and not rest.startswith("="):
                found.append((letter, None))
                continue
            found.append((letter, rest[1:] if rest.startswith("=") else rest or None))
            break
    return found


_TERMINAL_RUN_STATES = frozenset({"complete", "failed"})


@dataclass(frozen=True)
class InterestView:
    """What the image interest decision reads from one run record (design 2.7).

    ``state`` and ``images`` are None for an unreadable record, which counts as using every image and never ended.
    """

    run_id: str
    state: str | None
    recovery_actor_set: bool
    containers_removed: bool
    images: frozenset[str] | None


def interest_ended(view: InterestView) -> bool:
    """Return whether a run no longer uses its images: terminal, or no new mutations and no container left.

    "No new mutations" is state ``stopping`` or a recovery actor set; a ``stopped-retained`` run keeps its interest
    until its release.
    """
    if view.state is None:
        return False
    if view.state in _TERMINAL_RUN_STATES:
        return True
    return (view.state == "stopping" or view.recovery_actor_set) and view.containers_removed


def image_in_use(image_id: str, views: Sequence[InterestView], *, actor_run: str) -> bool:
    """Return whether a run other than ``actor_run`` still uses ``image_id`` (an unreadable record uses every image)."""
    return any(
        view.run_id != actor_run and not interest_ended(view) and (view.images is None or image_id in view.images)
        for view in views
    )


@dataclass(frozen=True)
class Grant:
    """One entry of the account's grants log: a run granted while another held (design 2.5).

    ``budget_bytes`` is the run's effective container plus host-process reservation.
    """

    run_id: str
    generation: int
    origin: str
    granted_ns: int
    budget_bytes: int


def counted_grant(grant: Grant, *, run_id: str, origin: str, admission_baseline_ns: int) -> bool:
    """Whether one grant joins a run's allowance (``allowance_bytes``, ``has_shared``): another run's, granted after
    the run's admission baseline on the same clock."""
    return grant.origin == origin and grant.run_id != run_id and grant.granted_ns > admission_baseline_ns


def allowance_bytes(grants: Sequence[Grant], *, run_id: str, origin: str, admission_baseline_ns: int) -> int:
    """Return the budgets of other runs granted after this run's admission baseline (same clock origin).

    Keyed to the admission baseline of the current generation, never the phase baseline, so a phase transition never
    removes a member; a member stays counted after its run leaves.
    """
    return sum(
        grant.budget_bytes
        for grant in grants
        if counted_grant(grant, run_id=run_id, origin=origin, admission_baseline_ns=admission_baseline_ns)
    )


def has_shared(
    *,
    coholder_seen: bool,
    grants: Sequence[Grant],
    run_id: str,
    generation: int,
    origin: str,
    admission_baseline_ns: int,
) -> bool:
    """Return whether a run has shared in its current generation (design 2.5).

    True for a granted co-holder seen in this observation, the run's own current grant in the log (it was granted
    beside a co-holder), or another run's grant after its admission baseline. An admitting peer that never published
    leaves no grant and is never a co-holder, so it never counts.
    """
    return (
        coholder_seen
        or any(grant.run_id == run_id and grant.generation == generation and grant.origin == origin for grant in grants)
        or any(
            counted_grant(grant, run_id=run_id, origin=origin, admission_baseline_ns=admission_baseline_ns)
            for grant in grants
        )
    )


_GIB = 1024**3
_MIB = 1024**2


def cache_cap_bytes(fs_total: int) -> int:
    """Return the build-cache store's total cap: 1/64 of its filesystem, clamped to 4..32 GiB (design DD7).

    NOTE: fixed fraction and clamps; ceiling: a store larger than 32 GiB is never kept; upgrade path: an install
    setting beside the disk reserve.
    """
    return min(max(fs_total // 64, 4 * _GIB), 32 * _GIB)


def archive_cap_bytes(fs_total: int) -> int:
    """Return the run-archive store's total cap: 1/1024 of its filesystem, clamped to 64 MiB..1 GiB (design DD7).

    NOTE: fixed fraction and clamps; archives are small records, so the clamp, not the disk, decides in practice.
    """
    return min(max(fs_total // 1024, 64 * _MIB), _GIB)


ARCHIVE_AGE_NS = 90 * 86_400 * 1_000_000_000  # NOTE: fixed age of an archived run record
SESSION_FOLDER_AGE_NS = 86_400 * 1_000_000_000  # NOTE: an empty recorded session folder older than a day
PROBE_FILE_AGE_NS = 60 * 1_000_000_000  # NOTE: a probe record older than a minute outlived its probe

# (guest reserve, host side) per runtime; a runtime without an entry is not qualified and is refused (design 2.5)
RUNTIME_OVERHEADS: dict[str, tuple[int, int]] = {
    "docker-desktop": (512 * _MIB, 256 * _MIB),
    # Colima 0.10.3, vz, 8 GiB VM (2026-11-04 window): in-guest use beside the runs 349-368 MiB; its own host
    # processes 140 MiB (the VZ process's resident size cannot isolate its overhead: macOS compresses guest pages)
    "colima": (512 * _MIB, 256 * _MIB),
    # Podman 6.1.4, applehv (2026-11-04 W0): in-guest use 440-485 MiB idle (485 on a 2048 MiB machine), 447 MiB beside
    # a held 1.5 GiB allocation; vfkit + gvproxy 34-53 MiB
    "podman": (640 * _MIB, 256 * _MIB),
    "docker-engine": (512 * _MIB, 0),
}
REMOTE_OVERHEADS = (512 * _MIB, 0)  # a remote daemon: the guest reserve only, the host side is not this machine's
_COLIMA_NAME = re.compile(r"colima(?:-([a-z0-9_-]+))?\Z")
UNSUPPORTED_RUNTIME_TEXT = (
    "Unsupported container runtime for this client; supported on macOS: Docker Desktop, Colima, Podman."
)
PODMAN_ENGINE_COMPONENT = "Podman Engine"
PODMAN_HELPER_ENDPOINT = "unix:///var/run/docker.sock"
# NOTE: below every measured guest share of configured memory (0.938-0.965, W0); upgrade: a per-size table
PODMAN_GUEST_SHARE = 0.93
PODMAN_MEMORY_STEP_MIB = 256  # NOTE: the granularity the size hint rounds `podman machine set --memory` to
PODMAN_HELPER_TEXT = (
    "Podman reached through /var/run/docker.sock (podman-mac-helper) is not supported; point a Docker context at the"
    " machine's API socket (podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}'): docker context"
    " create podman --docker host=unix://<that path> && docker context use podman."
)
PODMAN_BINARY_TEXT = (
    "Podman engine selected but no podman binary is on PATH; install Podman (brew install podman) so the toolkit can"
    " read the machine's identity."
)


@dataclass(frozen=True)
class RuntimeRefusal:
    """Why a local daemon is not a supported runtime for this client."""

    message: str
    reason: str = "unsupported-runtime"


def parse_version_components(output: str) -> tuple[str, str]:
    """Return ``(client version, engine)`` from ``version --format '{{.Client.Version}} {{json .Server.Components}}'``.

    The first word is the client version; the rest is JSON: ``null``, ``[]`` or a list of objects with a string
    ``Name``. A component named ``Podman Engine`` anywhere selects ``podman``, anything else ``docker`` (design PD1).

    Raises:
        ValueError: For any other shape (a deterministic client fault, never a late read).
    """
    version, _, rest = output.strip().partition(" ")
    if not version or version.startswith(("[", "{", "n")) or not rest:
        raise ValueError("the client version output has no version and component list")
    components = json.loads(rest)
    if components is None:
        return version, "docker"
    if not isinstance(components, list) or any(
        not isinstance(item, dict) or not isinstance(item.get("Name"), str) for item in components
    ):
        raise ValueError("the server components are not a list of named objects")
    podman = any(item["Name"] == PODMAN_ENGINE_COMPONENT for item in components)
    return version, "podman" if podman else "docker"


def podman_identity(machine: "MachineFacts") -> str:
    """Return the NAME-shaped identity of a Podman machine: a hash of its name, ``Created`` and ``Rootful`` (PD4)."""
    canonical = json.dumps(
        {"created": machine.created, "name": machine.name, "rootful": machine.rootful},
        sort_keys=True,
        separators=(",", ":"),
    )
    return "podman-" + hashlib.sha256(canonical.encode()).hexdigest()[:40]


def match_machine(endpoint: str, machines: Sequence["MachineFacts"]) -> "MachineFacts | RuntimeRefusal":
    """Return the one running machine whose API socket is the endpoint, compared as a string (PD5).

    The Docker socket of ``podman-mac-helper`` is refused before any match; no or several matches name the running
    machine's socket only when exactly one machine runs.
    """
    if endpoint == PODMAN_HELPER_ENDPOINT:
        return RuntimeRefusal(PODMAN_HELPER_TEXT, "podman-helper-unsupported")
    matches = [
        machine for machine in machines if endpoint.startswith("unix://") and endpoint[7:] == machine.socket_path
    ]
    if len(matches) == 1:
        return matches[0]
    hint = f"point the context at unix://{machines[0].socket_path}" if len(machines) == 1 else "start one machine"
    return RuntimeRefusal(
        f"The selected Docker endpoint {endpoint} is not the API socket of a running podman machine; {hint}.",
        "podman-machine-not-found",
    )


def loopback_endpoint(endpoint: str) -> bool:
    """Whether a Docker endpoint is a TCP socket on this machine's loopback (``127.0.0.1``, ``localhost``, ``::1``)."""
    if not endpoint.startswith("tcp://"):
        return False
    try:
        parsed = urllib.parse.urlsplit(endpoint)
        host, _port = parsed.hostname, parsed.port  # reading the port validates it
    except ValueError:
        return False  # a malformed URL names no machine: never this one
    if host == "localhost":
        return True
    try:
        return host is not None and ipaddress.ip_address(host).is_loopback  # 127.0.0.0/8 and ::1, never a DNS name
    except ValueError:
        return False


def detect_runtime(
    info: Mapping[str, object], client_platform: str, endpoint: str, home: Path
) -> str | RuntimeRefusal | None:
    """Name the runtime behind a local Docker daemon from its ``/info``, the client platform and the client endpoint.

    ``None`` is an endpoint that is not this machine's (another host's ``tcp://``, ``ssh://``, ``npipe://``): a remote
    daemon, accounted as today's remote from any client. A ``tcp://`` endpoint on the loopback is this machine's daemon
    (a CI runner reaching its own host's daemon) and is classified like its local socket; on a Mac only a daemon this
    rule recognises there (Docker Desktop) is, and any other stays remote (a tunnel to another host). A Mac VM daemon is
    only recognised from a ``darwin`` client, and Colima only through its own profile's socket, so no Mac VM is ever
    accounted as native Linux. A Podman machine is never classified here: its binding decides (``lifecycle.runtime_of``;
    the adapter refuses a Podman engine it cannot bind, design 2.2).
    """
    local_socket = endpoint.startswith("unix://")
    if not local_socket and not loopback_endpoint(endpoint):
        return None
    operating_system = info.get("OperatingSystem")
    desktop = isinstance(operating_system, str) and "docker desktop" in operating_system.lower()
    name = info.get("Name")
    colima = _COLIMA_NAME.fullmatch(name) if isinstance(name, str) else None
    if client_platform == "darwin":
        if desktop:
            return "docker-desktop"
        if colima is not None:
            socket = home / ".colima" / (colima.group(1) or "default") / "docker.sock"
            if endpoint == f"unix://{socket}":
                return "colima"
        if not local_socket:
            return None  # a loopback port to a daemon not recognised here: a tunnel to another host stays remote
    elif client_platform == "linux":
        if desktop and not local_socket:
            return None  # a loopback port to a Desktop daemon (WSL2's exposed one) stays remote, as before
        if desktop:
            return RuntimeRefusal(
                "Docker Desktop daemon seen from a Linux client; use Docker Engine in the WSL2 distribution."
            )
        if colima is None:
            return "docker-engine"
    return RuntimeRefusal(UNSUPPORTED_RUNTIME_TEXT)


def runtime_topology(runtime: str | None) -> str:
    """Return the accounting topology of a detected runtime: a Mac VM is ``desktop``, Linux is ``native``."""
    if runtime is None:
        return "remote"
    return "native" if runtime == "docker-engine" else "desktop"


def vm_disk_host_directory(runtime: str | None, name: str | None, home: Path) -> Path | None:
    """Return the host folder of a Colima VM's disk, ``~/.colima/_lima/<instance>`` (the instance is the daemon's
    ``Name``); ``None`` for any other runtime (Docker Desktop's folder comes from its settings, read by the caller).
    """
    if runtime != "colima" or name is None or _COLIMA_NAME.fullmatch(name) is None:
        return None
    return home / ".colima" / "_lima" / name


# NOTE: one measurement (8 GiB configured: 96.7 % reported as MemTotal, c6 window); upgrade: a per-size table
COLIMA_GUEST_SHARE = 0.96


def small_vm_memory_gib(container_bytes: int, guest_reserve: int) -> int:
    """Return the smallest whole GiB of Colima VM memory whose usable share admits ``container_bytes`` and the guest
    reserve (design 2.5): the VM's kernel keeps part of the configured memory, so ``MemTotal`` is below it."""
    return math.ceil((container_bytes + guest_reserve) / (COLIMA_GUEST_SHARE * _GIB))


def engine_stats(memory: Mapping[str, Any], hard_limit: object, engine: str) -> Mapping[str, Any]:
    """Return one-shot memory stats in Docker's shape (design PD8): Docker's unchanged. Podman's compat stats carry no
    counter map (no OOM event; OOM is decided by ``State.OOMKilled``) and report the VM's memory as the limit of a
    container whose hard limit exceeds it (W0 M6), so a limit in ``(0, hard_limit]`` reads as the hard limit."""
    if engine != "podman":
        return memory
    limit = memory.get("limit")
    bounded = type(limit) is int and type(hard_limit) is int and 0 < limit <= hard_limit
    counters = memory.get("stats")
    return {**memory, "stats": {} if counters is None else counters, "limit": hard_limit if bounded else limit}


def podman_machine_size(container_bytes: int, guest_reserve: int, cpus: int, daemon_cpus: int) -> tuple[int, int]:
    """Return ``(MiB, CPUs)`` of a Podman machine that admits the run (design 2.4).

    The memory is the smallest 256 MiB step whose conservative guest share holds the container budget and the guest
    reserve; the CPUs are the larger of the machine's and the phase's.
    """
    need_mib = -(-(container_bytes + guest_reserve) // _MIB)
    steps = -(-need_mib // (PODMAN_GUEST_SHARE * PODMAN_MEMORY_STEP_MIB))
    return int(steps) * PODMAN_MEMORY_STEP_MIB, max(cpus, daemon_cpus)


def podman_size_hint(memory: int, cpu_count: int, memory_mib: int, name: str) -> str:
    """Name the resize of the machine ``name`` to ``podman_machine_size``'s figures; never less memory than it has."""
    return (
        f"resize the Podman machine: podman machine stop {name} && podman machine set --memory"
        f" {max(memory, memory_mib)} --cpus {cpu_count} {name} && podman machine start {name}"
    )


def small_vm_hint(container_bytes: int, guest_reserve: int) -> str:
    """Name the Colima VM size that admits a run whose guest side does not fit the VM (design 2.5)."""
    size = small_vm_memory_gib(container_bytes, guest_reserve)
    return f"raise the VM memory: colima stop && colima start --memory {size}"


class CapacityState(StrEnum):
    """Classify admission as ready, retryable waiting, or refused."""

    READY = "ready"
    WAITING = "waiting"
    REFUSED = "refused"


@dataclass
class CapacityDecision:
    """Explain one admission decision and its computed reservations; ``detail`` names the figures of a disk decision."""

    state: CapacityState
    reason: str
    guest_bytes: int | None = None
    host_bytes: int | None = None
    detail: str | None = None


DISK_FLOOR_BYTES = 4 * _GIB  # NOTE: the per-run estimate without a previous run's image sizes (design DD8)
PRIVATE_ROOT_FLOOR_BYTES = _GIB  # NOTE: a run's private root needs this much free; it holds no images


@dataclass(frozen=True)
class DiskFigure:
    """One place's disk figures; ``state`` is ``measured`` or ``unknown`` (then every figure is ``None``)."""

    place: str
    free_bytes: int | None
    total_bytes: int | None
    reserve_bytes: int | None
    state: str


def derive_reserve(fs_total: int, declared: int | None) -> int:
    """Return a place's reserve: the declared one, else 1/32 clamped to 2..8 GiB; never more than half the place.

    NOTE: fixed fraction and clamps (design DD7); upgrade path: the declared `install --disk-reserve-bytes`.
    """
    derived = min(max(fs_total // 32, 2 * _GIB), 8 * _GIB) if declared is None else declared
    return min(derived, fs_total // 2)


def disk_estimate(previous_sizes: Sequence[int] | None) -> tuple[int, str]:
    """Return a run's disk estimate and its source: the previous run's image sizes plus a quarter, else the floor."""
    if not previous_sizes:
        return DISK_FLOOR_BYTES, "floor"
    total = sum(previous_sizes)
    return total + total // 4, "derived"


def disk_fit(figure: DiskFigure, other_leases: int, estimate: int) -> CapacityDecision:
    """Decide one leased place: refused when it can never fit, waiting while holders' leases leave too little room.

    An unmeasured place admits: a measurement gap never blocks a run (design 2.6).
    """
    free, total, reserve = figure.free_bytes, figure.total_bytes, figure.reserve_bytes
    if figure.state != "measured" or free is None or total is None or reserve is None:
        return CapacityDecision(CapacityState.READY, "disk-unknown")
    detail = f"{figure.place}: free {free}, total {total}, reserve {reserve}, estimate {estimate} bytes"
    if estimate > total - reserve:
        return CapacityDecision(CapacityState.REFUSED, "disk-capacity", detail=detail)
    if free - reserve - other_leases < estimate:
        return CapacityDecision(CapacityState.WAITING, "disk-capacity", detail=f"{detail}, leased {other_leases}")
    return CapacityDecision(CapacityState.READY, "disk-fits", detail=detail)


def private_root_fit(free: int) -> CapacityDecision:
    """Wait while the run's private root has less than its floor free (design DD5)."""
    if free < PRIVATE_ROOT_FLOOR_BYTES:
        detail = f"private-root: free {free}, floor {PRIVATE_ROOT_FLOOR_BYTES} bytes"
        return CapacityDecision(CapacityState.WAITING, "disk-capacity", detail=detail)
    return CapacityDecision(CapacityState.READY, "disk-fits")


OVERTAKE_WINDOW_NS = 30 * 60 * 1_000_000_000  # NOTE: fixed bound; an operator setting is the upgrade if needed
TURN_GRACE_NS = 120_000_000_000  # NOTE: fixed; an operator setting is the upgrade


@dataclass(frozen=True)
class Turn:
    """A repository's turn: the run that left, on which daemon and clock, and when (design 2.4, DD1)."""

    run_id: str
    daemon_id: str
    clock_origin: str
    released_ns: int


@dataclass(frozen=True)
class TurnCandidate:
    """One live queued run as the turn decision sees it."""

    run_id: str
    ticket: int
    repository: str


@dataclass(frozen=True)
class TurnDecision:
    """The run a valid turn chooses (``None``: none queued yet) and whether every other contender waits."""

    chosen: str | None
    wait: bool


def turn_current(turn: Turn, *, now_ns: int, clock_origin: str, grace_ns: int) -> bool:
    """Whether a turn is still within its grace on this clock (design 2.4): the one rule that prunes and decides."""
    return turn.clock_origin == clock_origin and 0 <= now_ns - turn.released_ns < grace_ns


def turn_decision(
    candidates: Sequence[TurnCandidate],
    turns: Mapping[str, Turn],
    *,
    now_ns: int,
    clock_origin: str,
    daemon_id: str,
    oldest_waited_ns: int,
    holders_present: bool,
    share_scope: str,
    limit: int | None,
    window_ns: int = OVERTAKE_WINDOW_NS,
    grace_ns: int = TURN_GRACE_NS,
) -> TurnDecision:
    """Decide the reservation order by repository turns, only with no holder under repository scope above limit 1.

    A turn is valid on its own daemon and clock origin, within ``TURN_GRACE_NS`` of its release (never from the
    future), and only while the oldest queued run has waited less than the overtake window. The earliest queued run,
    by ticket, of a repository with a valid turn is chosen; with valid turns and none of their runs queued, every
    contender waits for the next batch until the turns expire.
    """
    if holders_present or share_scope != "repository" or limit == 1 or oldest_waited_ns >= window_ns:
        return TurnDecision(None, False)
    valid = {
        repository
        for repository, turn in turns.items()
        if turn.daemon_id == daemon_id
        and turn_current(turn, now_ns=now_ns, clock_origin=clock_origin, grace_ns=grace_ns)
    }
    if not valid:
        return TurnDecision(None, False)
    ours = min((c for c in candidates if c.repository in valid), key=lambda c: c.ticket, default=None)
    return TurnDecision(None if ours is None else ours.run_id, True)


@dataclass(frozen=True)
class RetentionCandidate:
    """One evictable item of a capped kind: its size, its age (wall clock) and whether it may be removed at all."""

    identifier: str
    size_bytes: int
    wall_ns: int
    protected: bool


def eviction_plan(candidates: Sequence[RetentionCandidate], cap: int) -> tuple[str, ...]:
    """Return what to evict: the oldest unprotected items first, only while the kind's total exceeds ``cap`` (design
    2.7).

    No item has priority, an orphaned project's cache included: it goes only when it is the oldest, so a checkout
    whose folder is briefly absent keeps its warm cache while the store is under its cap or newer items can go first.
    """
    total = sum(item.size_bytes for item in candidates)
    evict: list[str] = []
    for item in sorted((item for item in candidates if not item.protected), key=lambda item: item.wall_ns):
        if total <= cap:
            break
        evict.append(item.identifier)
        total -= item.size_bytes
    return tuple(evict)
