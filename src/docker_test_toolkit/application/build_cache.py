"""Project build-cache store: location, permanent anchors, structural validation, listing and staging (design 3.7, 3.8).

The cache lives beside the account registry, one subtree per project and daemon platform:
``<registry>-build-cache/<project_id>/<os>-<architecture>/{generations/<key>/<generation>, staging/<run>-<operation>}``.
Each platform has a permanent lock anchor ``<project_id>/<platform>.lock`` beside its data subtree; nothing ever
renames or unlinks the anchor or the project directory. Every reader and writer fails open to a cold build.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import stat
import time
from collections.abc import Callable, Mapping, Sequence
from contextlib import ExitStack
from dataclasses import replace
from pathlib import Path
from typing import Any

from docker_test_toolkit.application import image_archive
from docker_test_toolkit.application.recovery import (
    Context,
    failpoint,
    lock,
    private_file_bytes,
    project_id_for,
    read_record,
)
from docker_test_toolkit.application.state import make_private_directories
from docker_test_toolkit.domain.build_cache import (
    C_RET_BYTES,
    CACHE_BLOB_COUNT_MAX,
    CACHE_INDEX_ANNOTATION,
    CACHE_METADATA_MAX,
    DIGEST_VERIFICATION,
    EXCLUSIVE_LOCK_SECONDS,
    MARGIN_BYTES,
    N_MAX,
    SHARED_LOCK_SECONDS,
    SWITCH_ENV,
    BuildFacts,
    GenerationFacts,
    build_key,
    checked_paths,
    eligibility,
    generation_age,
    generation_name,
    layer_outcome,
    parse_generation_name,
    private_root_verdict,
    publication_plan,
    reconcile_plan,
)
from docker_test_toolkit.domain.errors import InvalidInput, ResourceBusy, ResourceError
from docker_test_toolkit.domain.records import unique_object

_SHA256_HEX = re.compile(r"[0-9a-f]{64}")
_PLATFORM = re.compile(r"[a-z0-9_]+-[a-z0-9_]+")  # an ``<os>-<architecture>`` platform and its anchor name
_DIGEST = re.compile(r"sha256:(?P<hex>[0-9a-f]{64})")
_CACHE_CONFIG = "application/vnd.buildkit.cacheconfig.v0"
_INVALID = "Cache generation is structurally invalid."


def cache_root(registry: Path) -> Path:
    """Return the account's build-cache root beside its registry."""
    return registry.parent / f"{registry.name}-build-cache"


def project_cache(registry: Path, project_id: str) -> Path:
    """Return one project's cache directory."""
    return cache_root(registry) / project_id


def platform_name(daemon_platform: Mapping[str, str]) -> str:
    """Return the platform directory and anchor name of a receipt's ``daemon_platform``."""
    return f"{daemon_platform['os']}-{daemon_platform['architecture']}"


def platform_anchor(
    project_cache: Path,
    platform: str,
    *,
    shared: bool,
    nonblocking: bool = False,
    deadline_ns: int | None = None,
):
    """Hold the permanent ``<platform>.lock`` anchor of one project's platform cache (C15).

    Shared for a build that imports or stages; exclusive for reconcile, publication and removal. Raises
    ``ResourceBusy`` when ``nonblocking`` or the deadline refuses the wait.
    """
    context = Context(project_cache, project_cache)
    return lock(context, platform, shared=shared, nonblocking=nonblocking, deadline_ns=deadline_ns)


def require_safe_directory(path: Path) -> None:
    """Create ``path`` privately when missing and refuse (``ValueError``) one that is not private; never repair.

    Existing ancestors are left unchanged, so callers check every level of the cache tree from the top.
    """
    if "," in str(path) or '"' in str(path):
        raise ValueError("Cache path contains a character the cache flags cannot carry.")
    make_private_directories(path)
    observed = os.lstat(path)
    if not stat.S_ISDIR(observed.st_mode) or observed.st_uid != os.getuid() or observed.st_mode & 0o077:
        raise ValueError("Cache directory is not a private account-owned directory.")


def _read_metadata(path: Path) -> object:
    """Parse one metadata file of at most ``CACHE_METADATA_MAX`` bytes, refusing a symlink and duplicate keys."""
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "rb") as stream:
        observed = os.fstat(stream.fileno())
        if not stat.S_ISREG(observed.st_mode) or observed.st_size > CACHE_METADATA_MAX:
            raise ValueError(_INVALID)
        return json.loads(stream.read().decode(), object_pairs_hook=unique_object)


def _descriptor_blob(layout: Path, descriptor: object) -> Path:
    """Return the blob a descriptor names after checking it exists as a regular file of exactly the declared size."""
    if not isinstance(descriptor, dict):
        raise ValueError(_INVALID)
    digest = _DIGEST.fullmatch(descriptor.get("digest", "")) if isinstance(descriptor.get("digest"), str) else None
    size = descriptor.get("size")
    if digest is None or type(size) is not int:
        raise ValueError(_INVALID)
    blob = layout / "blobs" / "sha256" / digest["hex"]
    observed = os.lstat(blob)
    if not stat.S_ISREG(observed.st_mode) or observed.st_size != size:
        raise ValueError(_INVALID)
    return blob


def _check_entries(layout: Path, *, staging: bool) -> None:
    """Refuse any entry outside the closed cache layout, any non-regular file, and too many blobs."""
    with os.scandir(layout) as entries:
        listed = list(entries)
    for entry in listed:
        kind = entry.stat(follow_symlinks=False).st_mode
        if entry.name in ("oci-layout", "index.json", "index.json.lock", "cache.lock") and stat.S_ISREG(kind):
            continue
        if entry.name == "blobs" and stat.S_ISDIR(kind):
            continue
        if staging and entry.name == "ingest" and stat.S_ISDIR(kind) and not os.listdir(entry.path):
            continue
        raise ValueError(_INVALID)
    if os.listdir(layout / "blobs") != ["sha256"] or not stat.S_ISDIR(os.lstat(layout / "blobs" / "sha256").st_mode):
        raise ValueError(_INVALID)
    with os.scandir(layout / "blobs" / "sha256") as entries:
        blobs = list(entries)
    if len(blobs) > CACHE_BLOB_COUNT_MAX:
        raise ValueError(_INVALID)
    for entry in blobs:
        if not _SHA256_HEX.fullmatch(entry.name):
            raise ValueError(_INVALID)
        if not stat.S_ISREG(entry.stat(follow_symlinks=False).st_mode):
            raise ValueError(_INVALID)
    if not stat.S_ISREG(os.lstat(layout / "oci-layout").st_mode):
        raise ValueError(_INVALID)


def _layers(layout: Path, manifest: object) -> tuple[str, ...]:
    """Return the layer digests of an image-manifest or index form cache manifest after checking every blob."""
    if not isinstance(manifest, dict):
        raise ValueError(_INVALID)
    if "config" in manifest and "layers" in manifest:
        config, layers = manifest["config"], manifest["layers"]
    elif isinstance(manifest.get("manifests"), list):
        entries = manifest["manifests"]
        configs = [item for item in entries if isinstance(item, dict) and item.get("mediaType") == _CACHE_CONFIG]
        if len(configs) != 1:
            raise ValueError(_INVALID)
        config = configs[0]
        layers = [item for item in entries if item is not config]
    else:
        raise ValueError(_INVALID)
    if not isinstance(config, dict) or config.get("mediaType") != _CACHE_CONFIG or not isinstance(layers, list):
        raise ValueError(_INVALID)
    if _descriptor_blob(layout, config).stat().st_size > CACHE_METADATA_MAX:
        raise ValueError(_INVALID)
    return tuple(f"sha256:{_descriptor_blob(layout, layer).name}" for layer in layers)


def validate_generation(path: Path, *, staging: bool = False) -> tuple[str, ...]:
    """Check one cache export's structure without reading layer content and return its layer digests (T6).

    Raises ``ValueError("Cache generation is structurally invalid.")`` for any deviation, including an unreadable or
    vanished entry.
    """
    try:
        _check_entries(path, staging=staging)
        index = _read_metadata(path / "index.json")
        manifests = index.get("manifests") if isinstance(index, dict) else None
        if not isinstance(manifests, list) or len(manifests) != 1 or not isinstance(manifests[0], dict):
            raise ValueError(_INVALID)
        annotations = manifests[0].get("annotations")
        key, value = CACHE_INDEX_ANNOTATION
        if not isinstance(annotations, dict) or annotations.get(key) != value:
            raise ValueError(_INVALID)
        return _layers(path, _read_metadata(_descriptor_blob(path, manifests[0])))
    except (OSError, UnicodeDecodeError, RecursionError, ValueError) as error:
        raise ValueError(_INVALID) from error


def _tree_bytes(path: Path) -> int:
    total = 0
    for directory, _, files in os.walk(path):
        for name in files:
            try:
                total += os.lstat(os.path.join(directory, name)).st_size
            except OSError:
                continue
    return total


def list_generations(platform_dir: Path) -> list[GenerationFacts]:
    """List every entry under ``generations/`` as facts; foreign keys, names and shapes are listed invalid."""
    root = platform_dir / "generations"
    if not root.is_dir() or root.is_symlink():
        return []
    listed: list[GenerationFacts] = []
    with os.scandir(root) as entries:
        keys = sorted(entries, key=lambda entry: entry.name)
    for key_entry in keys:
        if not key_entry.is_dir(follow_symlinks=False):
            listed.append(GenerationFacts(key_entry.name, "", 0, "", 0, False))
            continue
        with os.scandir(key_entry.path) as entries:
            names = sorted(entries, key=lambda item: item.name)
        for entry in names:
            path = Path(entry.path)
            try:
                wall_ns, run_id, _ = parse_generation_name(entry.name)
            except ValueError:
                wall_ns, run_id = 0, ""
            valid = (
                bool(run_id)
                and _SHA256_HEX.fullmatch(key_entry.name) is not None
                and entry.is_dir(follow_symlinks=False)
            )
            if valid:
                try:
                    validate_generation(path)
                except ValueError:
                    valid = False
            size = _tree_bytes(path) if entry.is_dir(follow_symlinks=False) else 0
            listed.append(GenerationFacts(key_entry.name, entry.name, wall_ns, run_id, size, valid))
    return listed


def verify_generation_digests(path: Path) -> bool:
    """Return whether every blob's SHA-256 equals its file name; called only at import, under the shared lock (DF13)."""
    try:
        with os.scandir(path / "blobs" / "sha256") as entries:
            blobs = [entry.path for entry in entries]
        for blob in blobs:
            digest = hashlib.sha256()
            with open(blob, "rb") as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(chunk)
            if digest.hexdigest() != os.path.basename(blob):
                return False
    except OSError:
        return False
    return True


def importable_generations(platform_dir: Path) -> list[Path]:
    """Return per key the newest valid generation, at most ``N_MAX`` of them, newest first (Review Focus 3).

    With ``DIGEST_VERIFICATION`` a generation whose blob bytes no longer match their digests is skipped: the build
    runs colder, and the key's next publication supersedes it.
    """
    newest: dict[str, GenerationFacts] = {}
    for generation in list_generations(platform_dir):
        current = newest.get(generation.key)
        if generation.valid and (current is None or generation_age(generation) > generation_age(current)):
            newest[generation.key] = generation
    chosen = sorted(newest.values(), key=generation_age, reverse=True)[:N_MAX]
    paths = [platform_dir / "generations" / item.key / item.name for item in chosen]
    return [path for path in paths if not DIGEST_VERIFICATION or verify_generation_digests(path)]


def create_staging(platform_dir: Path, run_id: str, operation_id: str) -> Path:
    """Create this build's private staging directory; an existing one raises ``FileExistsError``."""
    staging_root = platform_dir / "staging"
    require_safe_directory(staging_root)
    staging = staging_root / f"{run_id}-{operation_id}"
    os.mkdir(staging, 0o700)
    return staging


def export_space_ok(platform_dir: Path, available_bytes: Callable[[str], int] | None = None) -> bool:
    """Return whether the cache filesystem can hold a full retention plus margin (the export precheck)."""
    try:
        image_archive.require_free_space(str(platform_dir), C_RET_BYTES + MARGIN_BYTES, available_bytes=available_bytes)
    except ResourceError:
        return False
    return True


def remove_entry(path: Path) -> None:
    """Delete one whole generation, staging or stray entry; a symlink or file is unlinked, never followed."""
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
    else:
        path.unlink(missing_ok=True)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def reconcile(platform_dir: Path, *, keep_staging: str | None = None) -> None:
    """Restore the store's postconditions; the caller holds the platform anchor exclusively (design 3.8, C1).

    Removes, in order and one whole directory at a time: every staging directory except ``keep_staging``, every
    invalid generation, every superseded generation, then the oldest generations until both caps hold; then empty key
    directories. Reads no layer content (T6), so a crash between deletions leaves only whole generations.
    """
    staging_root = platform_dir / "staging"
    real_staging = staging_root.is_dir() and not staging_root.is_symlink()  # never follow out of the cache (3.10)
    staging = sorted(path.name for path in staging_root.iterdir()) if real_staging else []
    plan = reconcile_plan(list_generations(platform_dir), staging=[name for name in staging if name != keep_staging])
    generations = platform_dir / "generations"
    for name in plan.staging:
        remove_entry(staging_root / name)
    for key, name in plan.invalid:
        remove_entry(generations / key / name)
    for key, name in plan.superseded:
        failpoint("cache-reconcile-superseding")
        remove_entry(generations / key / name)
    for key, name in plan.evicted:
        remove_entry(generations / key / name)
    if generations.is_dir() and not generations.is_symlink():
        for key_directory in generations.iterdir():
            if key_directory.is_dir() and not key_directory.is_symlink() and not any(key_directory.iterdir()):
                key_directory.rmdir()
    failpoint("cache-reconciled")


def _not_published(reason: str) -> dict[str, object]:
    return {"published": f"not-published:{reason}", "bytes": None}


def _discard(staging: Path) -> None:
    """Best-effort removal of this build's own staging; a concurrent reconcile may already have removed it."""
    shutil.rmtree(staging, ignore_errors=True)


def publish(
    platform_dir: Path,
    project_cache: Path,
    platform: str,
    plan: Mapping[str, object],
    *,
    run_id: str,
    operation_id: str,
) -> dict[str, object]:
    """Publish this build's staging as the newest generation of its key, or record why not (design 3.8).

    Takes the anchor exclusively with a 10 s deadline, reconciles, validates the staging, plans evictions, renames
    the staging into ``generations/<key>/`` (the commit point) and then deletes superseded and marked generations.
    Returns ``{"published": <generation name or "not-published:<reason>">, "bytes": <int or None>}``. A reader sees
    either the previous generations or the new one, never a partial one (I4).
    """
    staging, key = Path(str(plan["staging"])), str(plan["key"])
    deadline_ns = time.monotonic_ns() + EXCLUSIVE_LOCK_SECONDS * 1_000_000_000
    with ExitStack() as held:
        try:
            held.enter_context(platform_anchor(project_cache, platform, shared=False, deadline_ns=deadline_ns))
        except ResourceBusy:
            _discard(staging)
            return _not_published("lock-busy")
        reconcile(platform_dir, keep_staging=staging.name)
        if not staging.is_dir():
            return _not_published("removed")
        ingest = staging / "ingest"
        if ingest.is_dir() and not ingest.is_symlink() and not any(ingest.iterdir()):
            ingest.rmdir()
        try:
            validate_generation(staging)
        except ValueError:
            _discard(staging)
            return _not_published("invalid-export")
        size = _tree_bytes(staging)
        publication = publication_plan(list_generations(platform_dir), key=key, run_id=run_id, new_bytes=size)
        if publication is None:
            _discard(staging)
            return _not_published("over-cap")
        generations = platform_dir / "generations"
        require_safe_directory(generations)
        require_safe_directory(generations / key)
        name = generation_name(time.time_ns(), run_id, operation_id)
        os.rename(staging, generations / key / name)  # the commit point: from here on the generation is published
        failpoint("cache-renamed")
        try:
            _fsync_directory(generations / key)
            for old_key, old_name in (*publication.superseded, *publication.marked):
                failpoint("cache-publication-deleting")
                remove_entry(generations / old_key / old_name)
        except OSError:
            pass  # superseded or marked generations left behind are the next exclusive reconcile's work
        return {"published": name, "bytes": size}


def remove_platform(project_cache: Path, platform: str) -> int | str:
    """Delete one platform's cache data in place without waiting, keeping its anchor (design 3.8, C2, DF1).

    Returns the removed bytes, or ``"busy"`` when a build or another exclusive operation holds the anchor (nothing is
    removed then). Never renames to a tombstone: an interrupted removal leaves a partial subtree that the next
    exclusive operation's reconcile completes.
    """
    platform_dir = project_cache / platform
    with ExitStack() as held:
        try:
            held.enter_context(platform_anchor(project_cache, platform, shared=False, nonblocking=True))
        except ResourceBusy:
            return "busy"
        if not platform_dir.is_dir() or platform_dir.is_symlink():  # never follow out of the cache (3.10)
            return 0
        reconcile(platform_dir)
        removed = _tree_bytes(platform_dir)
        generations = platform_dir / "generations"
        whole = [generations / item.key / item.name for item in list_generations(platform_dir)]
        for child in (*whole, *sorted(platform_dir.iterdir())):
            failpoint("cache-removing")
            remove_entry(child)
        platform_dir.rmdir()
        return removed


def switch_private_addition(environ: Mapping[str, str] = os.environ) -> dict[str, str]:
    """Return the per-run cache switch as a private environment addition when this process carries it (DD3)."""
    return {SWITCH_ENV: environ[SWITCH_ENV]} if SWITCH_ENV in environ else {}


def skipped(reason: str) -> dict[str, object]:
    """Return the planning object of an eligible build that runs without the cache (``skipped:<reason>``)."""
    return {
        "eligible": True,
        "key": None,
        "imports": [],
        "staging": None,
        "export": False,
        "outcome": f"skipped:{reason}",
    }


def _skipped_or_ineligible(verdict: str) -> dict[str, object]:
    if verdict.startswith("not-eligible:"):
        return {"eligible": False, "outcome": verdict}
    return skipped(verdict.removeprefix("skipped:"))


def _build_facts(data: Mapping, command, caller_argv: Sequence[str], environment: Mapping[str, str]) -> BuildFacts:
    """Read the eligibility facts of one loaded build; local paths resolve against the dispatcher's directory."""
    paths = checked_paths(caller_argv)
    directory = os.getcwd()
    resolved = [
        os.path.realpath(os.path.join(directory, value))
        for value in (*paths.named_contexts, *paths.positional)
        if value != "-"
    ]
    dockerfiles = paths.dockerfiles or tuple(  # both default spellings of each candidate context (council C-1)
        os.path.join(value, name) for value in paths.positional if value != "-" for name in ("Dockerfile", "dockerfile")
    )
    resolved += [
        os.path.dirname(os.path.realpath(os.path.join(directory, value))) for value in dockerfiles if value != "-"
    ]
    requested = command.plan.get("platform") or {}
    return BuildFacts(
        switch=environment.get(SWITCH_ENV),
        daemon_platform=command.plan.get("daemon_platform"),
        topology=str(data.get("topology")),
        requested_raw=requested.get("requested_raw"),
        requested_emulated=str(requested.get("requested_emulated")),
        secret_or_ssh=any(
            token == option or token.startswith(f"{option}=")
            for token in caller_argv
            for option in ("--secret", "--ssh")
        ),
        private_root_touched=private_root_verdict(resolved, os.path.realpath(data["private_root"])),
        stdin=paths.stdin,
    )


def _cache_flags(imports: Sequence[Path], staging: Path | None) -> tuple[str, ...]:
    flags: list[str] = []
    for generation in imports:
        flags += ["--cache-from", f"type=local,src={generation}"]
    if staging is not None:
        flags += ["--cache-to", f"type=local,dest={staging},mode=max,ignore-error=true"]
    return tuple(flags)


def plan_build_cache(
    registry: Path,
    data: Mapping,
    command,
    caller_argv: Sequence[str],
    *,
    operation_id: str,
    environment: Mapping[str, str],
    available_bytes: Callable[[str], int] | None = None,
):
    """Return the build command with the toolkit's cache flags, the held shared anchor and the planning object.

    The returned ``ExitStack`` owns the shared lock only when planning completed: the lock lives in a local stack that
    ``pop_all()`` hands over on success, so any failure after acquisition releases it before the exception leaves
    (design 3.7, T5). Every other outcome returns an empty stack and the unchanged command.
    """
    verdict = eligibility(_build_facts(data, command, caller_argv, environment))
    if verdict is not None:
        return command, ExitStack(), _skipped_or_ineligible(verdict)
    platform = platform_name(command.plan["daemon_platform"])
    cache = project_cache(registry, data["project_id"])
    platform_dir = cache / platform
    try:
        for level in (cache_root(registry), cache, platform_dir):  # top first: an unsafe root is never passed (P2)
            require_safe_directory(level)
    except (ValueError, FileExistsError, NotADirectoryError):
        return command, ExitStack(), skipped("cache-root-unsafe")
    deadline_ns = time.monotonic_ns() + SHARED_LOCK_SECONDS * 1_000_000_000
    with ExitStack() as local:
        try:
            local.enter_context(platform_anchor(cache, platform, shared=True, deadline_ns=deadline_ns))
        except ResourceBusy:
            return command, ExitStack(), skipped("lock-busy")
        local.callback(failpoint, "cache-lock-release")
        failpoint("cache-planned-lock")
        imports = importable_generations(platform_dir)
        export = export_space_ok(platform_dir, available_bytes)
        staging = create_staging(platform_dir, data["run_id"], operation_id) if export else None
        failpoint("cache-planned-staging")
        split = len(command.argv) - len(caller_argv)
        argv = (*command.argv[:split], *_cache_flags(imports, staging), *command.argv[split:])
        failpoint("cache-planned-argv")
        plan = {
            "eligible": True,
            "key": build_key(caller_argv, private_root=data["private_root"], project_root=data["project_root"]),
            "imports": [path.name for path in imports],
            "staging": None if staging is None else str(staging),
            "export": staging is not None,
            "outcome": None if staging is not None else "skipped:disk-space",
        }
        return replace(command, argv=argv), local.pop_all(), plan


def _imported_layers(platform_dir: Path, names: Sequence[str]) -> set[str]:
    """Return the layer digests of the imported generations that still exist and validate (read without a lock)."""
    layers: set[str] = set()
    for name in names:
        for generation in (platform_dir / "generations").glob(f"*/{name}"):
            try:
                layers.update(validate_generation(generation))
            except ValueError:
                continue
    return layers


def _finished(plan: Mapping[str, Any], outcome: str, published: str | None, size: object = None) -> dict:
    return {
        "eligible": True,
        "key": plan["key"],
        "imports": list(plan["imports"]),
        "export": plan["export"],
        "outcome": outcome,
        "bytes": size,
        "published": published,
    }


def complete_build_cache(
    plan: Mapping[str, Any], *, promoted: bool, native: bool, run_id: str, operation_id: str
) -> dict[str, object]:
    """Return the closed cache outcome of a finished loaded build, publishing its export when allowed (design 3.8).

    Publication requires a promoted build, an export and only native images; otherwise this build's staging is
    removed. The finish write adds ``finished_ns``.
    """
    if not plan["eligible"]:
        return dict(plan)
    if plan["outcome"] is not None:
        return _finished(plan, str(plan["outcome"]), None)
    staging = Path(str(plan["staging"]))
    if not promoted:
        return failure_outcome(plan)
    platform_dir = staging.parent.parent
    try:
        new_layers = validate_generation(staging, staging=True)
    except ValueError:
        _discard(staging)
        return _finished(plan, "miss", "not-published:invalid-export")
    imported = sorted(_imported_layers(platform_dir, plan["imports"]))
    outcome = layer_outcome(new_layers, imported)
    if not native:
        _discard(staging)
        return _finished(plan, outcome, "not-published:not-native")
    failpoint("cache-publishing")
    published = publish(
        platform_dir, platform_dir.parent, platform_dir.name, plan, run_id=run_id, operation_id=operation_id
    )
    return _finished(plan, outcome, str(published["published"]), published["bytes"])


def failure_outcome(plan: Mapping[str, Any]) -> dict[str, object]:
    """Return the cache outcome of a failed or unpromoted build; a planning verdict is kept unchanged."""
    if not plan["eligible"]:
        return dict(plan)
    if plan["outcome"] is not None:
        return _finished(plan, str(plan["outcome"]), None)
    _discard(Path(str(plan["staging"])))
    return _finished(plan, "skipped:not-promoted", None)


def error_outcome(plan: Mapping[str, Any]) -> dict[str, object]:
    """Return the cache outcome after a fail-open catch: ``skipped:error``; an export is removed, never published."""
    if not plan["eligible"]:
        return dict(plan)
    if plan.get("staging") is not None:
        _discard(Path(str(plan["staging"])))
    published = "not-published:error" if plan["export"] else None
    return _finished(plan, "skipped:error", published)


def real_directory(path: Path) -> bool:
    """Return whether ``path`` is a directory by ``lstat``: a symlink is never followed out of the cache (3.10)."""
    return path.is_dir() and not path.is_symlink()


def platform_names(project_dir: Path) -> list[str]:
    """Return every platform of one project cache: its anchors and its data subtrees (lstat only)."""
    names: set[str] = set()
    with os.scandir(project_dir) as entries:
        for entry in entries:
            name = entry.name.removesuffix(".lock")
            if not _PLATFORM.fullmatch(name):
                continue  # only platform-shaped names are cache data (council C-2)
            anchor = entry.name.endswith(".lock") and entry.is_file(follow_symlinks=False)
            if anchor or (not entry.name.endswith(".lock") and entry.is_dir(follow_symlinks=False)):
                names.add(name)
    return sorted(names)


def _busy(project_dir: Path, platform: str) -> bool:
    """Probe an existing anchor without waiting and release it at once; a missing anchor is never created (C16)."""
    if not (project_dir / f"{platform}.lock").is_file():
        return False
    try:
        with platform_anchor(project_dir, platform, shared=False, nonblocking=True):
            return False
    except ResourceBusy:
        return True


def cache_inventory(cache: Path, project_id: str) -> dict[str, Any] | None:
    """Describe one project's cache per platform without creating anything, or ``None`` when it has none."""
    project_dir = cache / project_id
    if not real_directory(cache) or not real_directory(project_dir):
        return None
    platforms = {}
    for platform in platform_names(project_dir):
        platform_dir = project_dir / platform
        present = platform_dir.is_dir() and not platform_dir.is_symlink()
        staging = platform_dir / "staging"
        platforms[platform] = {
            "bytes": _tree_bytes(platform_dir) if present else 0,
            "generations": len(list_generations(platform_dir)) if present else 0,
            "staging": len(os.listdir(staging)) if present and real_directory(staging) else 0,
            "busy": _busy(project_dir, platform),
        }
    return {"project_id": project_id, "platforms": platforms}


def remove_project_cache(cache: Path, project_id: str) -> dict[str, int | str] | None:
    """Remove every platform of one project's cache in place; a busy platform is reported, never waited for."""
    project_dir = cache / project_id
    if not real_directory(cache) or not real_directory(project_dir):
        return None
    return {platform: remove_platform(project_dir, platform) for platform in platform_names(project_dir)}


def orphans(registry: Path) -> list[str]:
    """Return the cached projects whose registry record is missing or whose recorded root no longer exists."""
    root = cache_root(registry)
    if not real_directory(root):
        return []
    found = []
    for project_id in sorted(os.listdir(root)):
        if not _SHA256_HEX.fullmatch(project_id) or not real_directory(root / project_id):
            continue
        record = registry / "projects" / f"{project_id}.json"
        if not os.path.lexists(record):
            found.append(project_id)
            continue
        try:
            project_root = read_record(record, encoded=private_file_bytes(record))["project_root"]
        except (ResourceError, OSError):
            continue  # an unreadable record is never treated as an orphan
        if not os.path.lexists(project_root):
            found.append(project_id)
    return found


def remove_caches(registry: Path, payload: object, *, execute: bool) -> dict[str, object]:
    """List (dry run) or remove one project's or every orphan's build cache (design 3.10).

    The input selects ``{"project_root": <path>}`` or ``{"orphans": "all"}``; other fields are ignored. Orphans are
    re-derived at execute time, so a project whose root reappeared after the dry run is kept. A dry run creates nothing.

    Raises:
        InvalidInput: For any other input or an unresolvable project root.
    """
    request: dict[str, object] = payload if isinstance(payload, dict) else {}
    selectors = {"orphans", "project_root"} & request.keys()  # fields a newer peer adds are ignored
    root = request.get("project_root")
    if selectors == {"orphans"} and request["orphans"] == "all":
        project_ids, orphan_mode = orphans(registry), True
    elif selectors == {"project_root"} and isinstance(root, str):
        try:
            project_ids, orphan_mode = [project_id_for(Path(root).resolve(strict=True))], False
        except (OSError, RuntimeError, ValueError) as error:
            raise InvalidInput("Cache removal input is invalid.") from error
    else:
        raise InvalidInput("Cache removal input is invalid.")
    cache = cache_root(registry)
    if not execute:
        projects = [item for item in (cache_inventory(cache, project_id) for project_id in project_ids) if item]
        if orphan_mode:
            projects = [item for item in projects if any(entry["bytes"] for entry in item["platforms"].values())]
        return {"mode": "dry-run", "cache_root": str(cache), "projects": projects}
    results: list[dict[str, Any]] = []
    for project_id in project_ids:
        removed = remove_project_cache(cache, project_id)
        if removed is not None:
            results.append({"project_id": project_id, "platforms": removed})
    busy = any(value == "busy" for item in results for value in item["platforms"].values())
    return {"mode": "execute", "cache_root": str(cache), "results": results, "busy": busy}
