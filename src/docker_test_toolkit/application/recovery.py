"""Durable project ownership and bounded Docker cleanup."""

import fcntl
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import tempfile
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from docker_test_toolkit.application.ports.docker import DockerPort
from docker_test_toolkit.application.state import archive_directory, make_private_directories, registry_directory
from docker_test_toolkit.config.runtime import (
    BUILDER_PREFIX,
    PRIVATE_ROOT_NAME,
    PROJECT_ID_ENV,
    PROJECT_LABEL,
    PROJECT_LABEL_ENV,
    PROJECT_ROOT_ENV,
    SESSION_ID_ENV,
    SESSION_LABEL,
    TEST_BUILDER_PREFIX,
)
from docker_test_toolkit.domain.errors import IncompatibleState, ResourceBusy, ResourceError
from docker_test_toolkit.domain.policy import NAME
from docker_test_toolkit.domain.records import record_cell_id, unique_object

SESSION = SESSION_LABEL
PROJECT = PROJECT_LABEL
UUID = re.compile(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}\Z")
DOCKER_ID = re.compile(r"[0-9a-f]{64}\Z")
IMAGE = re.compile(r"sha256:[0-9a-f]{64}\Z")
RUN_FILE = re.compile(r"(?P<run_id>[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12})\.json\Z")


def project_id_for(root: str | Path) -> str:
    """Return the stable project identity: the SHA-256 of the canonical project root string."""
    return hashlib.sha256(str(root).encode()).hexdigest()


@dataclass
class Context:
    """Bind one canonical project root to its durable registry root."""

    root: Path
    state: Path

    @property
    def project_id(self):
        """Return the stable digest of the canonical project root."""
        return project_id_for(self.root)

    @property
    def record(self):
        """Return the project's exact registry-record path."""
        return self.state / "projects" / f"{self.project_id}.json"

    @property
    def label(self):
        """Return the exact project ownership label assignment."""
        return f"{PROJECT}={self.project_id}"

    def environment(self):
        """Return bounded child context without secrets or ambient authority."""
        return {
            PROJECT_ID_ENV: self.project_id,
            PROJECT_LABEL_ENV: self.label,
            PROJECT_ROOT_ENV: str(self.root),
        }


def _builder_names(project_id, session_id):
    suffix = f"{project_id[:12]}-{session_id.replace('-', '')}"
    return {
        "builder": f"{BUILDER_PREFIX}-{suffix}",
        "test_builder": f"{TEST_BUILDER_PREFIX}-{suffix}",
    }


def context():
    """Resolve the operator project's canonical root and registry context."""
    root = os.environ.get(PROJECT_ROOT_ENV)
    if not root:
        result = subprocess.run(["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True)
        root = result.stdout.strip() if result.returncode == 0 else os.getcwd()
    return Context(Path(root).resolve(), registry_directory())


_LOCK_OBSERVER: Callable[[str, str, bool, bool, bool], None] | None = None
_FAILPOINT: Callable[[str], None] | None = None


def configure_lock_observer(observer: Callable[[str, str, bool, bool, bool], None] | None) -> None:
    """Install or clear the test-only lock observer; production composition never installs one."""
    global _LOCK_OBSERVER
    _LOCK_OBSERVER = observer


def notify_lock_observer(event: str, name: str, shared: bool, wait: bool, bounded: bool) -> None:
    """Report one lock event to the installed observer, if any.

    ``wait`` is true when the acquisition can wait at all (false only for a single non-blocking attempt) and
    ``bounded`` is true when that wait is deadline-polled. This is the only caller of the installed observer.
    """
    if _LOCK_OBSERVER is not None:
        _LOCK_OBSERVER(event, name, shared, wait, bounded)


def configure_failpoint(callback: Callable[[str], None] | None) -> None:
    """Install or clear the test-only failpoint callback; production composition never installs one."""
    global _FAILPOINT
    _FAILPOINT = callback


def engine_images(docker: DockerPort, deadline: object | None = None) -> set[str]:
    """The images the engine's containers use, whoever created them: another toolkit account on the same engine is
    invisible to this account's records. Callers remove their own containers before their images."""
    return {str(item.get("Image")) for item in docker.objects("container", deadline=deadline)}


def failpoint(stage: str) -> None:
    """Call the installed failpoint callback for ``stage``; a no-op unless a test installed one."""
    if _FAILPOINT is not None:
        _FAILPOINT(stage)


def flock_until(stream, operation: int, deadline_ns: int) -> None:
    """Poll a non-blocking flock until it succeeds or the absolute monotonic deadline passes."""
    while True:
        try:
            fcntl.flock(stream, operation | fcntl.LOCK_NB)
            return
        except BlockingIOError:
            remaining = deadline_ns - time.monotonic_ns()
            if remaining <= 0:
                raise
            time.sleep(min(0.1, remaining / 1_000_000_000))


@contextmanager
def lock(ctx, name, *, shared=False, nonblocking=False, deadline_ns=None):
    """Hold one permanent private lock anchor for the requested scope.

    ``deadline_ns`` is an absolute ``time.monotonic_ns()`` expiry: the wait is polled and raises ``ResourceBusy`` with
    reason ``lock-deadline`` at expiry. ``None`` keeps the blocking (or ``nonblocking``) behavior exactly.
    """
    make_private_directories(ctx.state)
    if not isinstance(name, str) or not re.fullmatch(r"[a-zA-Z0-9_-]+", name):
        raise ResourceError("Invalid lock anchor name.")
    try:
        descriptor = os.open(ctx.state / f"{name}.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    except OSError as error:
        raise ResourceError("Cannot open permanent lock anchor.") from error
    with os.fdopen(descriptor, "r+") as stream:
        observed = os.fstat(stream.fileno())
        if not stat.S_ISREG(observed.st_mode) or observed.st_uid != os.getuid() or observed.st_mode & 0o077:
            raise ResourceError("Lock anchor is not private and account-owned.")
        operation = fcntl.LOCK_SH if shared else fcntl.LOCK_EX
        try:
            if deadline_ns is not None:
                flock_until(stream, operation, deadline_ns)
            else:
                fcntl.flock(stream, operation | fcntl.LOCK_NB if nonblocking else operation)
        except BlockingIOError as error:
            text = (
                "Project has an active wrapped command"
                if name == ctx.project_id
                else "Resource lock has an active owner"
            )
            raise ResourceBusy(
                f"{text}; operation refused.",
                reason="lock-deadline" if deadline_ns is not None else "unspecified",
            ) from error
        bounded = deadline_ns is not None
        wait = bounded or not nonblocking  # a deadline polls (waits) even when nonblocking was also requested
        notify_lock_observer("acquire", name, shared, wait, bounded)
        try:
            yield stream
        finally:
            notify_lock_observer("release", name, shared, wait, bounded)
            fcntl.flock(stream, fcntl.LOCK_UN)


IMAGE_LOCK = "images"


@contextmanager
def image_lock(ctx, *, deadline_ns=None, nonblocking=False):
    """Hold the account image lock K and yield its descriptor for Docker children to inherit.

    Released only by closing this descriptor, never by ``LOCK_UN``: flock keeps K held while any inheriting Docker
    child still runs, so K outlives a holder that unwinds or dies first. A busy nonblocking attempt raises
    ``ResourceBusy`` reason ``image-lock-held``; an expired ``deadline_ns`` raises reason ``lock-deadline``.
    """
    make_private_directories(ctx.state)
    try:
        descriptor = os.open(
            ctx.state / f"{IMAGE_LOCK}.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600
        )
    except OSError as error:
        raise ResourceError("Cannot open the image lock anchor.") from error
    try:
        observed = os.fstat(descriptor)
        if not stat.S_ISREG(observed.st_mode) or observed.st_uid != os.getuid() or observed.st_mode & 0o077:
            raise ResourceError("Image lock anchor is not private and account-owned.")
        try:
            if deadline_ns is not None:
                flock_until(descriptor, fcntl.LOCK_EX, deadline_ns)
            else:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB if nonblocking else fcntl.LOCK_EX)
        except BlockingIOError as error:
            raise ResourceBusy(
                "The image lock has an active owner; operation refused.",
                reason="lock-deadline" if deadline_ns is not None else "image-lock-held",
            ) from error
        bounded = deadline_ns is not None
        wait = bounded or not nonblocking
        notify_lock_observer("acquire", IMAGE_LOCK, False, wait, bounded)
        try:
            yield descriptor
        finally:
            notify_lock_observer("release", IMAGE_LOCK, False, wait, bounded)
    finally:
        os.close(descriptor)  # never LOCK_UN: an inheriting Docker child keeps K until it exits


def read_private_json(path: Path) -> dict:
    """Read one private account-owned JSON object, refusing symlinks, shared modes and duplicate fields."""
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, "r") as stream:
            observed = os.fstat(stream.fileno())
            if not stat.S_ISREG(observed.st_mode) or observed.st_uid != os.getuid() or observed.st_mode & 0o077:
                raise ResourceError("Lifecycle record is not a private account-owned regular file.")
            data = json.load(stream, object_pairs_hook=unique_object)
    except (OSError, RecursionError, ValueError) as error:
        raise ResourceError("Run record is missing or unreadable.") from error
    if not isinstance(data, dict):
        raise ResourceError("Lifecycle record must be an object.")
    return data


def private_file_bytes(path: Path) -> bytes:
    """Read one account-owned, private, regular file without following a final symlink."""
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError as error:
        raise IncompatibleState(f"Activation file is missing or unreadable: {path.name}.") from error
    with os.fdopen(descriptor, "rb") as stream:
        observed = os.fstat(stream.fileno())
        if not stat.S_ISREG(observed.st_mode) or observed.st_uid != os.getuid() or observed.st_mode & 0o077:
            raise IncompatibleState(f"Activation file is not private and account-owned: {path.name}.")
        return stream.read()


ARCHIVED_RUN = re.compile(r"(?P<run_id>[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12})-(?P<sha256>[0-9a-f]{64})\.json\Z")


def buildkit_worker(builder: str) -> str:
    """Return the BuildKit worker container Buildx's ``docker-container`` driver names for a builder."""
    return f"buildx_buildkit_{builder}0"


def buildkit_state_volume(builder: str) -> str:
    """Return the state volume of a builder's BuildKit worker (its ``/var/lib/buildkit``)."""
    return f"{buildkit_worker(builder)}_state"


def project_names(registry: Path) -> list[str]:
    """List the canonical project record ids; an unreadable directory propagates (fail closed)."""
    try:
        names = os.listdir(registry / "projects")
    except FileNotFoundError:
        return []
    return sorted(
        name.removesuffix(".json")
        for name in names
        if DOCKER_ID.fullmatch(name.removesuffix(".json")) and name.endswith(".json")
    )


def private_directory_entries(directory: Path) -> list[Path]:
    """List one account-owned private real directory; anything else contributes nothing."""
    try:
        observed = directory.lstat()
        if not stat.S_ISDIR(observed.st_mode) or observed.st_uid != os.getuid() or observed.st_mode & 0o077:
            return []
        return sorted(directory.iterdir())
    except OSError:
        return []


def archived_run_files(project: Path) -> Iterator[tuple[Path, re.Match[str]]]:
    """Yield each archived run file of one archive project directory with its name match.

    Every level is lstat-, uid- and mode-checked (`A/<pid>`, and only its child named `runs`); names only
    select candidates, so callers must still verify the bytes.
    """
    for runs in private_directory_entries(project):
        if runs.name != "runs":
            continue
        for path in private_directory_entries(runs):
            match = ARCHIVED_RUN.fullmatch(path.name)
            if match is not None:
                yield path, match


def verified_archived_run(path: Path, match: re.Match[str]) -> dict | None:
    """Return an archived run record only if its own bytes hash to the digest in its name and it names its run id.

    Hash first: bytes that do not match their name are never parsed, so a damaged candidate is unusable and never
    masks a later verified copy. A file can always be named by its own hash, so a verified file may still be hostile
    (nested past the parser's limit, wrong types): it is parsed defensively and confers no authority by itself; callers
    must still compare the result with a pinned cell or a project's sessions.
    """
    try:
        data = private_file_bytes(path)
        record = (
            json.loads(data, object_pairs_hook=unique_object)
            if hashlib.sha256(data).hexdigest() == match["sha256"]
            else None
        )
    except (ResourceError, OSError, UnicodeDecodeError, ValueError, RecursionError):
        return None  # RecursionError: a file named by its own hash can still be nested past the parser's limit
    return record if isinstance(record, dict) and record.get("run_id") == match["run_id"] else None


def run_attribution(raw: object) -> str | None:
    """Return the project a parsed run record belongs to, or None when it carries no usable identity."""
    if not isinstance(raw, dict):
        return None
    if isinstance(raw.get("project_id"), str):
        return str(raw["project_id"]) if DOCKER_ID.fullmatch(raw["project_id"]) else None
    if isinstance(raw.get("project_root"), str):
        return project_id_for(raw["project_root"])
    return None


def read_record(path, *, encoded=None):
    """Read and validate one closed project record or exact pre-read bytes."""
    try:
        data = json.loads(path.read_text() if encoded is None else encoded, object_pairs_hook=unique_object)
    except (OSError, RecursionError, ValueError) as error:
        raise ResourceError(f"Cannot read ownership record {path}: {error}") from error
    if (
        not isinstance(data, dict)
        or type(data.get("version")) is not int
        or data["version"] not in (1, 2)
        or not isinstance(data.get("project_root"), str)
        or not isinstance(data.get("sessions"), dict)
        or not isinstance(data.get("images"), dict)
    ):
        raise ResourceError(f"Invalid ownership record: {path}")
    if data["version"] == 2:
        runs = data.get("runs")
        if not isinstance(runs, dict) or any(
            not UUID.fullmatch(run_id) or not isinstance(location, str) or not Path(location).is_absolute()
            for run_id, location in runs.items()
        ):
            raise ResourceError(f"Invalid run index: {path}")
    project_id = project_id_for(data["project_root"])
    for session_id, session in data["sessions"].items():
        if not UUID.fullmatch(session_id) or not isinstance(session, dict):
            raise ResourceError(f"Invalid session ownership: {path}")
        parent = session.get("temporary_parent")
        if not isinstance(parent, str) or not Path(parent).is_absolute():
            raise ResourceError(f"Invalid temporary parent: {path}")
        expected_builders = _builder_names(project_id, session_id)
        for field, expected in expected_builders.items():
            if field not in session:
                continue
            if session[field] != expected:
                raise ResourceError(f"Invalid session ownership: {path}")
    for image_id, refs in data["images"].items():
        if not IMAGE.fullmatch(image_id) or not isinstance(refs, list) or not all(isinstance(ref, str) for ref in refs):
            raise ResourceError(f"Invalid image ownership: {path}")
    pending = data.get("pending", {})
    if (
        not isinstance(pending, dict)
        or set(pending) - {"containers", "networks", "volumes", "images"}
        or not isinstance(pending.get("volumes", {}), dict)
        or any(
            not isinstance(name, str) or not NAME.fullmatch(name) or not isinstance(created_at, str) or not created_at
            for name, created_at in pending.get("volumes", {}).items()
        )
    ):
        raise ResourceError(f"Invalid pending cleanup record: {path}")
    for kind, identity in (("containers", DOCKER_ID), ("networks", DOCKER_ID), ("images", IMAGE)):
        values = pending.get(kind, [])
        if (
            not isinstance(values, list)
            or not all(isinstance(value, str) and identity.fullmatch(value) for value in values)
            or len(set(values)) != len(values)
        ):
            raise ResourceError(f"Invalid pending cleanup identities: {path}")
    return data


def load(ctx):
    """Load a project record or return its closed empty version."""
    if not ctx.record.exists():
        return {"version": 2, "project_root": str(ctx.root), "sessions": {}, "images": {}, "runs": {}}
    record = read_record(ctx.record)
    if record["project_root"] != str(ctx.root):
        raise ResourceError("Project identity does not match its ownership record.")
    if record["version"] == 1:
        record["version"] = 2
        record["runs"] = {}
    return record


def atomic_bytes(path, data, *, before_replace=None):
    """Durably replace a private file with exact bytes; callers serialize read-modify-write.

    ``before_replace`` names a failpoint stage called after the temporary file is written and synced and before it
    replaces ``path``, so a crash there leaves the old bytes in place.
    """
    make_private_directories(path.parent)
    descriptor, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        if before_replace is not None:
            failpoint(before_replace)
        temporary.replace(path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


def _record_bytes(record) -> bytes:
    return (json.dumps(record, indent=2, allow_nan=False) + "\n").encode()


def atomic_json(path, record):
    """Durably replace a private record; callers serialize read-modify-write."""
    atomic_bytes(path, _record_bytes(record))


def save(ctx, record):
    """Validate and atomically persist one project ownership record."""
    atomic_json(ctx.record, record)


def register(ctx, *, session_id=None, temporary_parent=None, deadline_ns=None):
    """Register one exact session and its private temporary-parent provenance.

    ``deadline_ns`` (absolute ``time.monotonic_ns()``) bounds the registry wait; expiry is ``lock-deadline``.
    """
    session_id = os.environ.get(SESSION_ID_ENV, "") if session_id is None else session_id
    if not UUID.fullmatch(session_id):
        raise ResourceError("Toolkit session identity must be a lowercase hexadecimal UUID.")
    with lock(ctx, "registry", deadline_ns=deadline_ns):
        record = load(ctx)
        parent = str(Path(temporary_parent or os.environ.get("TMPDIR", "/tmp")).resolve())
        prior = record["sessions"].get(session_id)
        if prior and prior["temporary_parent"] != parent:
            # Retain every root if the same session resumes after a reboot.
            parents = prior.setdefault("previous_temporary_parents", [])
            if prior["temporary_parent"] not in parents:
                parents.append(prior["temporary_parent"])
        elif not prior:
            prior = {}
        prior["temporary_parent"] = parent
        prior.update(_builder_names(ctx.project_id, session_id))
        record["sessions"][session_id] = prior
        save(ctx, record)


def record_images(ctx, images):
    """Record immutable image ownership after binding the current daemon."""
    with lock(ctx, "registry"):
        record = load(ctx)
        for image_id, reference in images:
            if not IMAGE.fullmatch(image_id):
                raise ResourceError("Expected a full immutable sha256 image ID.")
            references = record["images"].setdefault(image_id, [])
            if reference and reference not in references:
                references.append(reference)
        save(ctx, record)


def claim_volumes(ctx, docker, names):
    """Persist user-declared ownership of exact existing volume incarnations."""
    if not names or any(not isinstance(name, str) or not NAME.fullmatch(name) for name in names):
        raise ResourceError("claim-volume requires exact volume names.")
    bind_daemon(ctx, docker)
    with lock(ctx, ctx.project_id, nonblocking=True), lock(ctx, "registry"):
        volumes = {item["Name"]: item for item in docker.objects("volume")}
        selected = {}
        for name in names:
            item = volumes.get(name)
            if not item or not item.get("CreatedAt"):
                raise ResourceError(f"Volume is missing or has no creation identity: {name}")
            if labels(item).get(PROJECT) not in (None, ctx.project_id):
                raise ResourceError(f"Volume belongs to another project: {name}")
            selected[name] = item["CreatedAt"]
        record = load(ctx)
        record.setdefault("pending", {}).setdefault("volumes", {}).update(selected)
        save(ctx, record)


def build_records(docker, kind, builder):
    """Read exact native history or cache identities, never relative offsets."""
    command = ("history", "ls") if kind == "build_history" else ("du",)
    output = docker.call("buildx", *command, "--builder", builder, "--format", "json")
    try:
        rows = [json.loads(line) for line in output.splitlines() if line.strip()]
        return {(row["ref"].rsplit("/", 1)[-1] if kind == "build_history" else row["ID"]): row for row in rows}
    except (ValueError, KeyError, TypeError, AttributeError, RecursionError) as error:
        raise ResourceError(f"Invalid {kind} inventory for {builder}.") from error


def claim_build_records(ctx, docker, kind, arguments):
    """Record externally verified exact build history/cache ownership."""
    if (
        len(arguments) < 2
        or not NAME.fullmatch(arguments[0])
        or any(not re.fullmatch(r"[a-z0-9]{20,64}", value) for value in arguments[1:])
    ):
        raise ResourceError("Expected one exact builder name and full build/cache IDs.")
    builder, *identifiers = arguments
    bind_daemon(ctx, docker)
    with lock(ctx, ctx.project_id, nonblocking=True), lock(ctx, "registry"):
        available = build_records(docker, kind, builder)
        if any(identifier not in available for identifier in identifiers):
            raise ResourceError("A supplied build/cache ID does not exist on the selected builder.")
        record = load(ctx)
        claimed = record.setdefault(kind, {}).setdefault(builder, {})
        for identifier in identifiers:
            item = available[identifier]
            claimed[identifier] = item["ref"] if kind == "build_history" else item["CreatedAt"]
        save(ctx, record)


def daemon_identity(docker, deadline=None) -> str:
    """Return the daemon's identity for a comparison with a record.

    On Docker this is exactly today's read, ``info --format {{.ID}}``; on Podman the compat ``ID`` changes on every
    call, so it is the bound machine's identity after a fresh verification. Never a cached value.
    """
    if docker.engine == "podman":
        return docker.observe_daemon(deadline).daemon_id
    if deadline is None:
        return docker.call("info", "--format", "{{.ID}}").strip()
    return docker.call("info", "--format", "{{.ID}}", deadline=deadline).strip()


def bind_daemon(ctx, docker):
    """Pin durable ownership to one daemon; switching daemons needs a separate registry."""
    daemon_id = daemon_identity(docker)
    if not daemon_id:
        raise ResourceError("Docker did not return its daemon identity.")
    with lock(ctx, "registry"):
        record = load(ctx)
        if record.get("daemon_id") not in (None, daemon_id):
            raise ResourceError("Docker daemon differs from this project's ownership record; cleanup refused.")
        record["daemon_id"] = daemon_id
        save(ctx, record)


def labels(resource):
    """Return normalized Docker labels from a resource inspection."""
    return resource.get("Labels") or (resource.get("Config") or {}).get("Labels") or {}


def temporary_roots(record, selected_session=None):
    """Return deduplicated private roots recorded for the selected sessions."""
    roots = []
    for session_id, data in record["sessions"].items():
        if selected_session is not None and session_id != selected_session:
            continue
        parents = [data["temporary_parent"], *data.get("previous_temporary_parents", [])]
        for parent in parents:
            if not isinstance(parent, str) or not Path(parent).is_absolute():
                raise ResourceError("Invalid persisted temporary parent.")
            roots.append(Path(parent) / PRIVATE_ROOT_NAME / session_id)
    return list(dict.fromkeys(roots))


def _manifest_images(root):
    """Read one regular account-owned image manifest without following links."""
    if root.is_symlink():
        return set()
    try:
        root_descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        return set()
    except OSError as error:
        raise ResourceError(f"Invalid image manifest root: {root}") from error
    try:
        try:
            descriptor = os.open(
                "docker-image-ids",
                os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                dir_fd=root_descriptor,
            )
        except FileNotFoundError:
            return set()
        except OSError as error:
            raise ResourceError(f"Invalid image manifest: {root / 'docker-image-ids'}") from error
        try:
            with os.fdopen(descriptor, encoding="utf-8") as stream:
                observed = os.fstat(stream.fileno())
                if not stat.S_ISREG(observed.st_mode) or observed.st_uid != os.getuid():
                    raise ResourceError(f"Invalid image manifest: {root / 'docker-image-ids'}")
                image_ids = set(stream.read().splitlines())
        except (OSError, UnicodeDecodeError) as error:
            raise ResourceError(f"Invalid image manifest: {root / 'docker-image-ids'}") from error
    finally:
        os.close(root_descriptor)
    if any(not IMAGE.fullmatch(image_id) for image_id in image_ids):
        raise ResourceError(f"Invalid image manifest: {root / 'docker-image-ids'}")
    return image_ids


def _foreign_claims(ctx, daemon_id):
    """Return the image ids and session ids that other projects on this daemon currently claim."""
    images, sessions = set(), set()
    for path in (ctx.state / "projects").glob("*.json"):
        if path != ctx.record:
            other = read_record(path)
            if other.get("daemon_id") in (None, daemon_id):
                images.update(other["images"])
                images.update(other.get("pending", {}).get("images", []))
                sessions.update(other["sessions"])
    return images, sessions


def inventory(ctx, record, docker, *, selected_session=None):
    """Build a complete ownership inventory without mutating Docker or records."""
    # Finish every read before deletion; a failed inventory never becomes an empty one.
    daemon_id = daemon_identity(docker)
    if not daemon_id or record.get("daemon_id") not in (None, daemon_id):
        raise ResourceError("Docker daemon identity is missing or differs from the ownership record.")
    resources = {kind: docker.objects(kind) for kind in ("container", "network", "volume", "image")}
    builders = set(docker.call("buildx", "ls", "--format", "{{.Name}}").split())
    sessions = set(record["sessions"])
    if selected_session is not None:
        sessions &= {selected_session}
    other_images, other_sessions = _foreign_claims(ctx, daemon_id)
    exclusive_sessions = sessions - other_sessions
    session_builders = {
        session[field]
        for identity, session in record["sessions"].items()
        if selected_session is None or identity == selected_session
        for field in ("builder", "test_builder")
        if isinstance(session.get(field), str)
        and (selected_session is None or session[field] == _builder_names(ctx.project_id, identity)[field])
    }
    candidate_builders = session_builders & builders
    pending = record.get("pending", {}) if selected_session is None else {}

    def owned(item):
        """Return whether labels bind the item to this exact project."""
        metadata = labels(item)
        if selected_session is not None:
            return metadata.get(SESSION) == selected_session and (
                metadata.get(PROJECT) == ctx.project_id
                or (metadata.get(PROJECT) is None and selected_session in exclusive_sessions)
            )
        if PROJECT in metadata:
            return metadata[PROJECT] == ctx.project_id
        return metadata.get(SESSION) in exclusive_sessions

    def compatible(item):
        """Return whether an unlabeled legacy item is safe for exact reconciliation."""
        return labels(item).get(PROJECT) in (None, ctx.project_id)

    containers = resources["container"]
    owned_builders = {
        name
        for name in candidate_builders
        if any(item.get("Name", "").lstrip("/") == buildkit_worker(name) and compatible(item) for item in containers)
    }
    selected = {
        item["Id"]
        for item in containers
        if compatible(item)
        and (
            owned(item)
            or item["Id"] in pending.get("containers", [])
            or item.get("Name", "").lstrip("/") in {buildkit_worker(name) for name in owned_builders}
        )
    }
    foreign = [item for item in containers if item["Id"] not in selected]
    builder_containers = {
        item["Id"]
        for item in containers
        if item.get("Name", "").lstrip("/") in {buildkit_worker(name) for name in owned_builders}
    }
    foreign_volumes = {
        mount["Name"] for item in foreign for mount in item.get("Mounts", []) if mount.get("Type") == "volume"
    }
    attached = {
        mount["Name"]
        for item in containers
        if item["Id"] in selected
        for mount in item.get("Mounts", [])
        if mount.get("Type") == "volume"
    }
    claimed_volumes = {
        item["Name"]
        for item in resources["volume"]
        if compatible(item)
        and (
            owned(item)
            or (item["Name"] in attached and "com.docker.volume.anonymous" in labels(item))
            or (
                item["Name"] in pending.get("volumes", {}) and item.get("CreatedAt") == pending["volumes"][item["Name"]]
            )
        )
    }
    volumes = claimed_volumes - foreign_volumes
    claimed_networks = {
        item["Id"]
        for item in resources["network"]
        if compatible(item) and (owned(item) or item["Id"] in pending.get("networks", []))
    }
    networks = {
        item["Id"]
        for item in resources["network"]
        if item["Id"] in claimed_networks and not (set(item.get("Containers") or {}) - selected)
    }
    image_ids = set(record["images"]) if selected_session is None else set()
    if selected_session is not None:
        other_images.update(record["images"])
    image_roots = temporary_roots(record)
    for root in image_roots:
        selected_root = selected_session is None or root.name == selected_session
        if selected_root and root.name not in exclusive_sessions:
            continue
        (image_ids if selected_root else other_images).update(_manifest_images(root))
    foreign_images = {item["Image"] for item in foreign}
    images = {}
    retained = {}
    for item in resources["image"]:
        image_id = item["Id"]
        if not (owned(item) or image_id in image_ids or image_id in pending.get("images", [])):
            continue
        if (
            image_id in other_images
            or image_id in foreign_images
            or labels(item).get(PROJECT) not in (None, ctx.project_id)
        ):
            retained[image_id] = "Referenced by another project or foreign container"
        else:
            images[image_id] = (
                [ref for ref in (item.get("RepoTags") or []) if ref != "<none>:<none>"]
                or item.get("RepoDigests")
                or [image_id]
            )
    build_inventory: dict[str, dict[str, list[str]]] = {
        "build_history": {},
        "build_cache": {},
        "retained_build_records": {},
    }
    for kind in ("build_history", "build_cache"):
        for builder, claims in record.get(kind, {}).items():
            if selected_session is not None and builder not in session_builders:
                continue
            if builder not in builders:
                continue
            available = build_records(docker, kind, builder)
            for identifier in claims:
                item = available.get(identifier)
                if item is None:
                    continue
                same = (item["ref"] if kind == "build_history" else item["CreatedAt"]) == claims[identifier]
                removable = (
                    item.get("status") in {"Completed", "Error", "Canceled"}
                    if kind == "build_history"
                    else item.get("Reclaimable") is True and item.get("Shared") is False
                )
                selected_kind = kind if same and removable else "retained_build_records"
                build_inventory[selected_kind].setdefault(builder, []).append(identifier)
    return {
        **build_inventory,
        "containers": sorted(selected),
        "networks": sorted(networks),
        "volumes": sorted(volumes),
        "images": images,
        "builders": sorted(owned_builders),
        "builder_containers": sorted(builder_containers),
        "temporary_roots": [
            str(root)
            for root in temporary_roots(record, selected_session=selected_session)
            if root.name in exclusive_sessions and (root.exists() or root.is_symlink())
        ],
        "retained_images": retained,
        "retained_volumes": sorted(claimed_volumes & foreign_volumes),
        "retained_networks": sorted(claimed_networks - networks),
        "retained_builders": sorted(candidate_builders - owned_builders),
        "retained_temporary_roots": [
            str(root)
            for root in temporary_roots(record, selected_session=selected_session)
            if root.name in other_sessions and (root.exists() or root.is_symlink())
        ],
        "volume_identities": {
            item["Name"]: item.get("CreatedAt") for item in resources["volume"] if item["Name"] in claimed_volumes
        },
    }


def indexed_session_ids(ctx, record):
    """Return validated session identities for every indexed run."""
    sessions: set[str] = set()
    if not record["runs"]:
        return sessions
    try:
        directory = os.open(ctx.state / "runs", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError as error:
        raise ResourceError("Indexed run record directory is unavailable or invalid.") from error
    try:
        for run_id, location in record["runs"].items():
            path = Path(location)
            if path != ctx.state / "runs" / f"{run_id}.json":
                raise ResourceError("Indexed run path is outside the project registry.")
            try:
                descriptor = os.open(f"{run_id}.json", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
                with os.fdopen(descriptor, encoding="utf-8") as stream:
                    observed = os.fstat(stream.fileno())
                    if not stat.S_ISREG(observed.st_mode) or observed.st_uid != os.getuid():
                        raise ResourceError("Indexed run record is unavailable or invalid.")
                    data = json.load(stream, object_pairs_hook=unique_object)
            except ResourceError:
                raise
            except (OSError, RecursionError, UnicodeDecodeError, ValueError) as error:
                raise ResourceError("Indexed run record is unavailable or invalid.") from error
            if not isinstance(data, dict):
                raise ResourceError("Indexed run identity is invalid.")
            session_id = data.get("session_id")
            if (
                data.get("run_id") != run_id
                or data.get("project_id") != ctx.project_id
                or data.get("project_root") != str(ctx.root)
                or not isinstance(session_id, str)
                or not UUID.fullmatch(session_id)
            ):
                raise ResourceError("Indexed run identity is invalid.")
            sessions.add(session_id)
    finally:
        os.close(directory)
    return sessions


def _digest_of(path) -> str | None:
    """Return the SHA-256 of one file's bytes, or None when it cannot be read."""
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return None


def _require_unchanged(ctx, expected_digest) -> None:
    """Refuse when the project record is no longer the exact bytes this cleanup last saw or wrote."""
    if _digest_of(ctx.record) != expected_digest:
        raise IncompatibleState("Project record changed during cleanup; re-run the dry-run.", reason="record-changed")


def _save_if_unchanged(ctx, record, expected_digest) -> str:
    """Write ``record`` over the exact bytes this cleanup last saw and return the digest of the new bytes.

    Takes the registry lock for the one write. Raises ``IncompatibleState`` (``record-changed``) when another writer
    replaced the record while the cleanup was working without that lock.
    """
    with lock(ctx, "registry"):
        _require_unchanged(ctx, expected_digest)
        data = _record_bytes(record)
        atomic_bytes(ctx.record, data)
        return hashlib.sha256(data).hexdigest()


def _unverified_history(ctx, verify_run) -> list[str]:
    """Return cell ids of this project's attributable run records that do not verify.

    A record is attributable when it parses to an object naming this project. ``verify_run(run_id, raw)`` is the
    application's verdict (``lifecycle.run_record_loads``); without one nothing verifies, so a caller that cannot judge
    history never reports a clean project. Records that cannot be read or parsed carry no attribution and are not
    listed here.
    """
    unverified = []
    for name, run_id, data, raw in run_files(ctx.state):
        if raw is None:
            continue
        if run_attribution(raw) == ctx.project_id and not (verify_run and verify_run(run_id, raw)):
            unverified.append(record_cell_id("run", f"runs/{name}", hashlib.sha256(data).hexdigest()))
    return unverified


def run_files(state: Path) -> Iterator[tuple[str, str, bytes, dict | None]]:
    """Yield ``(file name, run id, bytes, parsed record)`` for each run-record file in ``state/runs``, sorted by name.

    The parsed record is None when the file cannot be read or parsed (its bytes are then empty); a missing directory
    yields nothing.
    """
    try:
        listing = sorted(os.scandir(state / "runs"), key=lambda entry: entry.name)
    except FileNotFoundError:
        return
    for entry in listing:
        match = RUN_FILE.fullmatch(entry.name)
        if match is None:
            continue
        try:
            data = private_file_bytes(Path(entry.path))
            raw = json.loads(data, object_pairs_hook=unique_object)
        except (ResourceError, UnicodeDecodeError, ValueError, RecursionError):
            yield entry.name, match["run_id"], b"", None
            continue
        yield entry.name, match["run_id"], data, raw


def _private_directory_state(path: Path) -> str:
    """Classify one archive directory by `lstat` alone: "absent" (ENOENT), "private" or "unverifiable".

    An observation error is never absence: any other `OSError` (EACCES, ELOOP, ENOTDIR on an ancestor) and any entry
    that is not an account-owned directory without group or other mode bits is "unverifiable".
    """
    try:
        observed = os.lstat(path)
    except FileNotFoundError:
        return "absent"
    except OSError:
        return "unverifiable"
    private = stat.S_ISDIR(observed.st_mode) and observed.st_uid == os.getuid() and not observed.st_mode & 0o077
    return "private" if private else "unverifiable"


def _unverified_archive(ctx) -> list[str]:
    """List this project's present archive entries that cannot be verified, as typed strings.

    The permissive pin-checking walker ignores a non-private directory and a non-canonical name, so cleanup checks
    them itself and reports what it cannot verify instead of treating it as absent. A directory that is not observable
    or not private lists `archive-subtree:<pid>` (the project directory) or `archive-subtree:<pid>/runs`; a
    non-canonical entry lists `archive-entry:<pid>/runs/<name>`; a canonically named file that does not verify by its
    own bytes or is attributed to another project lists its cell id. A missing archive or project directory is clean.
    Deletion of an archive file is not detected here: only the pin-checking paths know the expected set. Other
    projects' directories are never examined.
    """
    project_id = ctx.project_id
    unverifiable = f"archive-subtree:{project_id}"
    runs = archive_directory(ctx.state) / project_id / "runs"
    levels = ((runs.parent.parent, unverifiable), (runs.parent, unverifiable), (runs, f"{unverifiable}/runs"))
    for directory, label in levels:
        state = _private_directory_state(directory)
        if state == "absent":
            return []
        if state == "unverifiable":
            return [label]
    try:
        with os.scandir(runs) as listing:
            names = sorted(entry.name for entry in listing)
    except OSError:
        return [levels[-1][1]]
    unverified: list[str] = []
    for name in names:
        match = ARCHIVED_RUN.fullmatch(name)
        if match is None:
            unverified.append(f"archive-entry:{project_id}/runs/{name}")
            continue
        record = verified_archived_run(runs / name, match)
        if record is None or run_attribution(record) != project_id:
            unverified.append(record_cell_id("run", f"runs/{match['run_id']}.json", match["sha256"]))
    return sorted(unverified)


def _cleanup(
    ctx,
    docker,
    execute=False,
    *,
    selected_session=None,
    verify_run=None,
    locate_run=None,
    image_hold=None,
    image_in_use=None,
):
    """Plan or execute exact project or session cleanup.

    The project lock is held throughout. The registry lock is held only for planning (reading the record and every
    other project's claims) and the pre-removal publication, then re-taken for each image removal and each later
    record write, so another project's registry operations never wait on this cleanup's Docker work. Every re-taking
    requires the project record to still be the bytes this cleanup last wrote.

    Args:
        ctx: Canonical project and registry context.
        docker: Bound Docker adapter used for inspection and cleanup.
        execute: Whether to perform the planned cleanup.
        verify_run: Verdict on one attributable unindexed run record, ``(run_id, raw) -> bool``; project cleanup only.
        locate_run: Describes one indexed run for the refusal, ``(run_id, location) -> dict``.
        image_hold: The image lock K's descriptor an executing cleanup holds; each image removal's child inherits it.
        image_in_use: Whether a live or retained run still uses an image, decided under K; such images are kept.

    Raises:
        ResourceError: If ownership evidence is invalid, changed, or unresolved.
        IncompatibleState: ``record-changed`` when another writer replaced the project record during Docker work.
    """
    with lock(ctx, ctx.project_id, nonblocking=True):
        with lock(ctx, "registry"):
            record = load(ctx)
            if selected_session is not None and selected_session not in record["sessions"]:
                raise ResourceError("Session is not registered to this project.")
            indexed_sessions = indexed_session_ids(ctx, record) if selected_session is not None else set()
            if record["runs"] and (selected_session is None or selected_session in indexed_sessions):
                items = [locate_run(*entry) for entry in record["runs"].items()] if locate_run else []
                raise ResourceError(
                    "Indexed test runs require lifecycle reconciliation before project cleanup.",
                    details={
                        "runs": [
                            item
                            for item in items
                            if selected_session is None
                            or item["context"] is None
                            or item["context"]["session_id"] == selected_session
                        ]
                    },
                )
            before = inventory(ctx, record, docker, selected_session=selected_session)
            history = (
                {"history_unverified": [*_unverified_history(ctx, verify_run), *_unverified_archive(ctx)]}
                if selected_session is None
                else {}
            )
            identity = {
                "project_root": str(ctx.root),
                "project_id": ctx.project_id,
                **({"session_id": selected_session} if selected_session is not None else {}),
            }
            if not execute:
                return {
                    **identity,
                    "inventory": before,
                    "clean": not any(before.values()) and not history.get("history_unverified"),
                    "executed": False,
                    **history,
                }
            if any(not isinstance(value, str) or not value for value in before["volume_identities"].values()):
                raise ResourceError("A selected volume has no immutable creation identity; cleanup refused.")
            daemon_id = daemon_identity(docker)
            current_digest: str | None
            if selected_session is None:
                record["daemon_id"] = daemon_id
                record["pending"] = {
                    "containers": before["containers"],
                    "networks": before["networks"] + before["retained_networks"],
                    "volumes": before["volume_identities"],
                    "images": list(before["images"]) + list(before["retained_images"]),
                }
                for image_id in record["pending"]["images"]:
                    record["images"].setdefault(image_id, [])
                # Persist legacy discoveries before their proving containers can disappear.
                data = _record_bytes(record)
                atomic_bytes(ctx.record, data)
                current_digest = hashlib.sha256(data).hexdigest()
                failpoint("cleanup-pending-saved")
            else:
                current_digest = _digest_of(ctx.record)
        failpoint("cleanup-removals-started")
        errors = []

        def remove(*command: str) -> None:
            """Run one exact removal, recording its failure; a crash after a success is a failpoint."""
            try:
                docker.call(*command)
            except ResourceError as error:
                errors.append(str(error))
                return
            failpoint("cleanup-removed-one")

        for command, targets in [
            (
                ("container", "rm", "-f"),
                sorted(set(before["containers"]) - set(before["builder_containers"])),
            ),
            (("buildx", "rm"), before["builders"]),
        ]:
            for target in targets:
                remove(*command, target)
        builder_containers = set(before["builder_containers"])
        remaining_workers = sorted(
            item["Id"] for item in docker.objects("container") if item.get("Id") in builder_containers
        )
        for command, targets in [
            (("container", "rm", "-f"), remaining_workers),
            (("network", "rm"), before["networks"]),
            (("volume", "rm"), before["volumes"]),
        ]:
            for target in targets:
                remove(*command, target)
        for image_id, references in before["images"].items():
            if image_in_use is not None and image_in_use(image_id):
                continue  # a live run still uses it: decided under K, before the registry lock
            for reference in references:
                # Another project may claim this image after the plan: re-read the claims under the registry lock
                # for each removal and keep the image as soon as one exists.
                with lock(ctx, "registry"):
                    _require_unchanged(ctx, current_digest)
                    if image_id in _foreign_claims(ctx, daemon_id)[0]:
                        break
                    try:
                        # with K held, the Docker child inherits it, so K outlives this process until the child exits
                        removal = docker.execute(("image", "rm", reference), timeout_seconds=30, hold_fd=image_hold)
                        if removal.exit_code:  # child output never enters an error (the adapter's rule)
                            raise ResourceError(f"Docker image removal of {reference} exited {removal.exit_code}.")
                    except ResourceError as error:
                        # Docker may remove a digest alias with its last ordinary
                        # tag. Suppress only after proving those exact bytes absent.
                        if any(item["Id"] == image_id for item in docker.objects("image")):
                            errors.append(str(error))
        for kind in ("build_history", "build_cache"):
            for builder, identifiers in before[kind].items():
                for identifier in identifiers:
                    command = (
                        ("history", "rm", "--builder", builder, identifier)
                        if kind == "build_history"
                        else (
                            "prune",
                            "--builder",
                            builder,
                            "--filter",
                            f"id=^{identifier}$",
                            "--filter",
                            "inuse!=true",
                            "--filter",
                            "shared!=true",
                            "--force",
                        )
                    )
                    try:
                        docker.call("buildx", *command)
                    except ResourceError as error:
                        errors.append(str(error))
        after = inventory(ctx, record, docker, selected_session=selected_session)
        docker_remaining = any(
            after[key]
            for key in (
                "containers",
                "networks",
                "volumes",
                "images",
                "builders",
                "build_history",
                "build_cache",
            )
        )
        if not errors and not docker_remaining:
            for root_text in before["temporary_roots"]:
                root = Path(root_text)
                try:
                    if root.is_symlink():
                        root.unlink()
                    else:
                        shutil.rmtree(root)
                except OSError as error:
                    errors.append(f"Cannot remove recorded temporary root {root}: {error}")
            after = inventory(ctx, record, docker, selected_session=selected_session)
        if selected_session is None and not errors and not docker_remaining:
            # Finishing a project releases its use of shared image bytes. The
            # remaining project's record protects them until its own cleanup.
            for image_id in after["retained_images"]:
                record["images"].pop(image_id, None)
            record["pending"]["images"] = [
                image_id for image_id in record["pending"]["images"] if image_id not in after["retained_images"]
            ]
            current_digest = _save_if_unchanged(ctx, record, current_digest)
        clean = not errors and not any(after.values()) and not history.get("history_unverified")
        if clean and selected_session is not None:
            record["sessions"].pop(selected_session)
            current_digest = _save_if_unchanged(ctx, record, current_digest)
        removed: dict[str, object] = {
            key: sorted(set(before[key]) - set(after[key]))
            for key in ("containers", "networks", "volumes", "images", "builders", "temporary_roots")
        }
        for kind in ("build_history", "build_cache"):
            removed[kind] = {
                builder: sorted(set(identifiers) - set(after[kind].get(builder, [])))
                for builder, identifiers in before[kind].items()
            }
        return {
            **identity,
            "inventory": after,
            "removed": removed,
            "clean": clean,
            "executed": True,
            "errors": errors,
            **history,
        }


def cleanup_project(
    ctx, docker, execute=False, *, verify_run=None, locate_run=None, image_account=None, image_in_use=None
):
    """Plan or execute project cleanup.

    ``verify_run`` judges the project's unindexed run history (see ``_unverified_history``); unverified history keeps
    the result unclean. ``image_account`` (the account directory) makes an execution take the image lock K without
    waiting first: a busy K refuses at once with ``image-lock-held`` before anything is removed, and every image
    removal's Docker child inherits K. ``image_in_use(image_id)``, decided under K, keeps an image another
    run still uses.
    """
    with _image_lock_without_waiting(image_account, execute) as hold_fd:
        return _cleanup(
            ctx,
            docker,
            execute,
            verify_run=verify_run,
            locate_run=locate_run,
            image_hold=hold_fd,
            image_in_use=image_in_use,
        )


def cleanup_session(ctx, docker, session_id, execute=False, *, locate_run=None, image_account=None, image_in_use=None):
    """Plan or execute cleanup for one exact registered session (``image_account``, ``image_in_use``: see
    ``cleanup_project``)."""
    if not isinstance(session_id, str) or not UUID.fullmatch(session_id):
        raise ResourceError("Session cleanup identity is invalid.")
    with _image_lock_without_waiting(image_account, execute) as hold_fd:
        return _cleanup(
            ctx,
            docker,
            execute,
            selected_session=session_id,
            locate_run=locate_run,
            image_hold=hold_fd,
            image_in_use=image_in_use,
        )


@contextmanager
def _image_lock_without_waiting(account, execute):
    """Hold K for an executing operator cleanup, never waiting for it, and yield its descriptor; a plan or an unnamed
    account takes nothing and yields None."""
    if not execute or account is None:
        yield None
        return
    with image_lock(Context(account, account), nonblocking=True) as hold_fd:
        yield hold_fd
