"""Pure build-cache policy: switch, bounds, export-slot key, generation names, outcomes (design 3.5 to 3.9)."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import PurePosixPath

from docker_test_toolkit.domain.policy import BUILD_VALUE_LETTERS, short_options

SWITCH_ENV = "DOCKER_TEST_TOOLKIT_BUILD_CACHE"
N_MAX = 16
MARGIN_BYTES = 2 * 1024**3
SHARED_LOCK_SECONDS = 5
EXCLUSIVE_LOCK_SECONDS = 10
# ponytail: probe-derived bounds (design 3.8, 5); ceiling: one value for every project and machine; upgrade path:
# an account setting when projects need different values.
C_RET_BYTES = 4 * 1024**3
CACHE_BLOB_COUNT_MAX = 4096
CACHE_METADATA_MAX = 16 * 1024**2
# The probe saw BuildKit import a tampered blob without checking its digest, so the import verifies every blob.
DIGEST_VERIFICATION = True
# The index.json manifest entry annotation the probe observed (evidence/shorten-slot-hold/probe/summary.md).
CACHE_INDEX_ANNOTATION = ("org.opencontainers.image.ref.name", "latest")
SKIPPED_REASONS = (
    "disabled", "disabled-invalid-value", "lock-busy", "cache-root-unsafe", "disk-space", "error", "not-promoted",
)  # fmt: skip
NOT_ELIGIBLE_REASONS = (
    "platform-unknown", "foreign-platform", "build-secret", "private-root-context", "stdin-input",
)  # fmt: skip
NOT_PUBLISHED_REASONS = ("lock-busy", "removed", "invalid-export", "over-cap", "not-native", "error")
_UUID = r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}"
_GENERATION = re.compile(rf"(?P<wall>[0-9]{{1,20}})-(?P<run>{_UUID})-(?P<operation>{_UUID})")
_OWNED_EXACT = frozenset(
    {"docker", "docker-test-toolkit-dispatch", "docker-authority.json", "guardian.stdout.log", "guardian.stderr.log",
     "buildkitd.toml", "exports"}
)  # fmt: skip
_OWNED_SHAPES = re.compile(
    "|".join(
        (
            rf"cid-{_UUID}",
            rf"build-{_UUID}\.json",
            r"generation-(?:[2-9]|[1-9][0-9]+)",
            rf"python-{_UUID}-docker",
            rf"python-{_UUID}\.(?:request|ready|grant)\.json",
            r"docker-test-toolkit-[a-z0-9_]{8}",
            r"docker-test-toolkit-[A-Za-z0-9]{6}",
            r"docker-test-toolkit-recover-[a-z0-9_]{8}",
            r"docker-test-toolkit-supervisor-[a-z0-9_]{8}",
        )
    )
)
_ATOMIC_TEMPORARY = re.compile(r"\.(?P<target>.+)\.[a-z0-9_]{8}")
_CACHED_TOPOLOGIES = frozenset({"desktop", "native"})
_KEY_DROPPED_FLAGS = frozenset({"--load", "--quiet", "-q"})
_KEY_DROPPED_VALUE_OPTIONS = frozenset({"--tag", "-t", "--label", "--progress"})


def switch_verdict(value: str | None) -> str | None:
    """Return ``None`` when the per-run switch leaves the cache on, else the ``skipped:…`` outcome (design 3.5)."""
    if value is None or value == "on":
        return None
    return "skipped:disabled" if value == "off" else "skipped:disabled-invalid-value"


def build_key(caller_argv: Sequence[str], *, private_root: str, project_root: str) -> str:
    """Return the export-slot key of one build: SHA-256 of the normalized caller argv (design 3.6, DD1).

    Run-scoped tags, labels and output options never change the key; the private and project roots are replaced by
    placeholders so a run-specific private root keeps the key stable.
    """
    tokens: list[str] = []
    skip = False
    for token in caller_argv:
        if skip:
            skip = False
            continue
        if token in _KEY_DROPPED_FLAGS:
            continue
        if token in _KEY_DROPPED_VALUE_OPTIONS:
            skip = True
            continue
        if any(token.startswith(f"{option}=") for option in _KEY_DROPPED_VALUE_OPTIONS) or (
            token.startswith("-t") and not token.startswith("--")
        ):
            continue
        tokens.append(token.replace(private_root, "${private}").replace(project_root, "${project}"))
    # ponytail: short-option clusters such as -qtTAG are kept verbatim; ceiling: such a spelling only loses key
    # stability (hits), never correctness; upgrade path: read clusters with the shared short-option reader.
    return hashlib.sha256(json.dumps(tokens, separators=(",", ":")).encode()).hexdigest()


def generation_name(wall_ns: int, run_id: str, operation_id: str) -> str:
    """Return the directory name of one cache generation (design 3.7)."""
    return f"{wall_ns}-{run_id}-{operation_id}"


def parse_generation_name(name: str) -> tuple[int, str, str]:
    """Return ``(wall_ns, run_id, operation_id)`` of a generation name; raise ``ValueError`` for any other shape."""
    match = _GENERATION.fullmatch(name)
    if match is None:
        raise ValueError("Cache generation name is invalid.")
    return int(match["wall"]), match["run"], match["operation"]


@dataclass(frozen=True)
class CheckedPaths:
    """The local paths a build may read: Dockerfiles, named local contexts and candidate positional contexts."""

    dockerfiles: tuple[str, ...]
    named_contexts: tuple[str, ...]
    positional: tuple[str, ...]
    stdin: bool


@dataclass(frozen=True)
class BuildFacts:
    """The facts the eligibility verdict reads; the application resolves the private-root check (design 3.5)."""

    switch: str | None  # the raw DOCKER_TEST_TOOLKIT_BUILD_CACHE value
    daemon_platform: Mapping[str, str] | None  # the receipt's plan["daemon_platform"]
    topology: str  # the run record's "topology"
    requested_raw: str | None  # plan["platform"]["requested_raw"]
    requested_emulated: str  # plan["platform"]["requested_emulated"]
    secret_or_ssh: bool
    private_root_touched: bool
    stdin: bool


def _owned_name(name: str) -> bool:
    return name in _OWNED_EXACT or _OWNED_SHAPES.fullmatch(name) is not None


def is_toolkit_owned(name: str) -> bool:
    """Return whether a first path component below the private root is a toolkit-owned name (design 3.5, I8).

    An atomic-write temporary (``.<target>.<8 characters>``) is owned when its target is; never recursively.
    """
    if _owned_name(name):
        return True
    temporary = _ATOMIC_TEMPORARY.fullmatch(name)
    return temporary is not None and _owned_name(temporary["target"])


def _named_context_path(value: str) -> str | None:
    """Return the local path of one ``name=source`` named context, or ``None`` for a remote or image source."""
    name, separator, source = value.partition("=")
    if not separator or not name or not source:
        raise ValueError("Build context option is malformed.")
    if source.startswith("oci-layout://"):
        return source.removeprefix("oci-layout://").split("@sha256:", 1)[0]
    if "://" in source or source.startswith("git@") or source.endswith(".git"):
        return None
    return source


def checked_paths(arguments: Sequence[str]) -> CheckedPaths:
    """Return the paths of one build's caller arguments; raise ``ValueError`` when an option lacks its value.

    Every bare token not consumed by a Dockerfile or named-context option is a candidate positional context, so
    over-reading can only make a build ineligible (cold), never cache one that touches the private root.
    """
    dockerfiles: list[str] = []
    named: list[str] = []
    positional: list[str] = []
    position = 0

    def take(option: str) -> str:
        """Consume and return the next argument as ``option``'s value; raise ``ValueError`` when none is left."""
        nonlocal position
        if position >= len(arguments):
            raise ValueError(f"Build option {option} lacks its value.")
        position += 1
        return arguments[position - 1]

    while position < len(arguments):
        argument = arguments[position]
        position += 1
        option, separator, attached = argument.partition("=")
        if option in ("--file", "--build-context"):
            value = attached if separator else take(option)
            if option == "--file":
                dockerfiles.append(value)
            elif (local := _named_context_path(value)) is not None:
                named.append(local)
            continue
        if argument == "-" or not argument.startswith("-"):
            positional.append(argument)
            continue
        if argument.startswith("--"):
            continue
        for letter, attached_value in short_options((argument,), BUILD_VALUE_LETTERS):
            if letter == "f":
                dockerfiles.append(take("-f") if attached_value is None else attached_value)
    stdin = "-" in positional or "-" in dockerfiles
    return CheckedPaths(tuple(dockerfiles), tuple(named), tuple(positional), stdin)


def private_root_verdict(resolved: Sequence[str], private_root: str) -> bool:
    """Return whether a resolved path is the private root, an ancestor of it, or under a toolkit-owned child of it."""
    root = PurePosixPath(private_root)
    for raw in resolved:
        path = PurePosixPath(raw)
        if path == root or path in root.parents:
            return True
        if root in path.parents and is_toolkit_owned(path.relative_to(root).parts[0]):
            return True
    return False


def eligibility(facts: BuildFacts) -> str | None:
    """Return ``None`` for an eligible build, else the first failing ``not-eligible:`` or switch verdict (design 3.5).

    The switch is read before every build fact, so a disabled run reports only ``skipped:disabled``.
    """
    switched = switch_verdict(facts.switch)
    if switched is not None:
        return switched
    if facts.daemon_platform is None or facts.topology not in _CACHED_TOPOLOGIES:
        return "not-eligible:platform-unknown"
    if facts.requested_raw is not None and facts.requested_emulated != "native":
        return "not-eligible:foreign-platform"
    if facts.secret_or_ssh:
        return "not-eligible:build-secret"
    if facts.private_root_touched:
        return "not-eligible:private-root-context"
    if facts.stdin:
        return "not-eligible:stdin-input"
    return None


@dataclass(frozen=True)
class GenerationFacts:
    """One published generation as the store reads it; ``valid`` is structural validity only (design 3.8, T6)."""

    key: str
    name: str
    wall_ns: int
    run_id: str
    bytes: int
    valid: bool


@dataclass(frozen=True)
class ReconcilePlan:
    """What one reconcile removes, in its order: staging, invalid, superseded, then evicted oldest first (C1)."""

    staging: tuple[str, ...]
    invalid: tuple[tuple[str, str], ...]
    superseded: tuple[tuple[str, str], ...]
    evicted: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class PublicationPlan:
    """What one publication supersedes (its key's current generation) and marks to fit the caps."""

    superseded: tuple[tuple[str, str], ...]
    marked: tuple[tuple[str, str], ...]


def generation_age(generation: GenerationFacts) -> tuple[int, str]:
    """Order generations oldest first by ``(wall_ns, name)``; a backwards clock only reorders, never invalidates."""
    return generation.wall_ns, generation.name


def _ids(generations: Sequence[GenerationFacts]) -> tuple[tuple[str, str], ...]:
    return tuple((generation.key, generation.name) for generation in generations)


def _within_caps(generations: Sequence[GenerationFacts], extra_count: int = 0, extra_bytes: int = 0) -> bool:
    count = len(generations) + extra_count
    return count <= N_MAX and sum(generation.bytes for generation in generations) + extra_bytes <= C_RET_BYTES


def reconcile_plan(generations: Sequence[GenerationFacts], *, staging: Sequence[str]) -> ReconcilePlan:
    """Plan one reconcile: keep the newest valid generation per key, then evict the oldest kept until within caps.

    Postconditions after the plan is applied: at most ``N_MAX`` generations, at most ``C_RET_BYTES`` bytes, one
    generation per key, all structurally valid (design 3.8, I9).
    """
    invalid = [generation for generation in generations if not generation.valid]
    newest: dict[str, GenerationFacts] = {}
    for generation in sorted((item for item in generations if item.valid), key=generation_age):
        newest[generation.key] = generation
    superseded = [item for item in generations if item.valid and newest[item.key] is not item]
    kept = sorted(newest.values(), key=generation_age)
    evicted: list[GenerationFacts] = []
    while kept and not _within_caps(kept):
        evicted.append(kept.pop(0))
    return ReconcilePlan(tuple(staging), _ids(invalid), _ids(sorted(superseded, key=generation_age)), _ids(evicted))


def publication_plan(
    kept: Sequence[GenerationFacts], *, key: str, run_id: str, new_bytes: int
) -> PublicationPlan | None:
    """Plan one publication into a reconciled store, or return ``None`` when it is over-cap (design 3.8, X23).

    The key's current generation is superseded; the oldest generations of other runs are marked until the new one
    fits. A generation of the publishing run is never evicted to make room.
    """
    if new_bytes > C_RET_BYTES:
        return None
    superseded = [generation for generation in kept if generation.key == key]
    remaining = sorted((generation for generation in kept if generation.key != key), key=generation_age)
    marked: list[GenerationFacts] = []
    candidates = [generation for generation in remaining if generation.run_id != run_id]
    while not _within_caps(remaining, 1, new_bytes):
        if not candidates:
            return None
        victim = candidates.pop(0)
        remaining.remove(victim)
        marked.append(victim)
    return PublicationPlan(_ids(superseded), _ids(marked))


def layer_outcome(new_layers: Sequence[str], imported_layers: Sequence[str]) -> str:
    """Return ``"hit"`` when something was imported and every new layer is among the imported ones, else ``"miss"``."""
    if imported_layers and set(new_layers) <= set(imported_layers):
        return "hit"
    return "miss"
