"""Account-wide Docker test lifecycle with durable ownership."""

import argparse
import atexit
import copy
import ctypes
import errno
import fcntl
import functools
import hashlib
import http.client
import importlib.resources
import ipaddress
import json
import math
import os
import pwd
import re
import runpy
import selectors
import shlex
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
import uuid
from collections.abc import Iterator, Mapping, Sequence
from contextlib import ExitStack, contextmanager, nullcontext
from dataclasses import asdict, dataclass, field, replace
from itertools import pairwise
from pathlib import Path, PurePosixPath
from typing import BinaryIO, Callable, ParamSpec, TextIO, TypeVar, cast

from docker_test_toolkit.application import admission as account_queue
from docker_test_toolkit.application import build_cache, image_archive, retention
from docker_test_toolkit.application.admission import QueueEntry, QueueState
from docker_test_toolkit.application.admission import load_queue as _admission_load_queue
from docker_test_toolkit.application.admission import phase_budgets as _phase_budgets
from docker_test_toolkit.application.admission import save_queue as _admission_save_queue
from docker_test_toolkit.application.docker_commands import (
    BUILDER_DAEMON_CPUS,
    BUILDER_MEMORY_BYTES,
    BUILDKIT_CONFIG_TEXT,
    ClassifiedDockerCommand,
    ComposeShareFacts,
    classify_command,
    compose_share_facts,
    compose_trailing_globals,
    container_share_facts,
    requested_platform_values,
)
from docker_test_toolkit.application.ports.docker import DockerPort
from docker_test_toolkit.application.probe import PROBE_RUN_BOUND_S, DaemonProbe, ProbeResult, ensure_on_current_daemon
from docker_test_toolkit.application.recovery import (
    IMAGE,
    PROJECT,
    SESSION,
    UUID,
    Context,
    ResourceBusy,
    ResourceError,
    atomic_json,
    bind_daemon,
    buildkit_state_volume,
    buildkit_worker,
    failpoint,
    image_lock,
    load,
    lock,
    register,
    run_files,
    save,
    temporary_roots,
    unique_object,
)
from docker_test_toolkit.application.recovery import (
    daemon_identity as _daemon_identity,
)
from docker_test_toolkit.application.recovery import read_private_json as _read_private_json
from docker_test_toolkit.application.sharing import (
    HOST_RESERVE_DEFAULT,
    AdmissionSettings,
    Lease,
    PendingImage,
    append_grant,
    declared_disk_reserve,
    drain_pending,
    pending_absent,
    read_admission_settings,
    read_grants,
    read_host_reserve,
    read_leases,
    read_parallel_runs,
    read_pending,
    read_turns,
    read_verdict,
    remove_turn,
    repository_identity,
    repository_origin,
    request_key,
    write_leases,
    write_pending,
    write_turns,
    write_verdict,
)
from docker_test_toolkit.application.state import (
    account_directory,
    canonical_account,
    containing_zipapp,
    make_private_directories,
    overlaps_account,
    registry_directory,
)
from docker_test_toolkit.application.supervision import account_environment, private_process_environment
from docker_test_toolkit.config.installation import current_target, installation_of
from docker_test_toolkit.config.runtime import (
    COMPOSE_PREFIX,
    PRIVATE_ROOT_NAME,
    RUN_LABEL,
)
from docker_test_toolkit.domain.errors import (
    ArchiveRejected,
    IncompatibleState,
    IncompleteCleanup,
    InfrastructureFailure,
    InvalidInput,
    ToolkitError,
)
from docker_test_toolkit.domain.models import (
    BUILDER_RELEASE_MIN_VERSION,
    SHARED_QUEUE_MIN_VERSION,
    SUPPORTED_PROTOCOLS,
    TOOLKIT_VERSION,
    ArtifactIdentity,
    DaemonIdentity,
    DockerObservation,
    DockerResult,
    Endpoint,
    ExactInspection,
    ExternalDockerResource,
    MachineFacts,
    PhaseBudget,
    PodmanBinding,
)
from docker_test_toolkit.domain.platform import Platform, classify_emulation, daemon_platform, parse_requested_platform
from docker_test_toolkit.domain.policy import (
    DISK_FLOOR_BYTES,
    HOLDER_PHASES,
    NAME,
    OVERTAKE_WINDOW_NS,
    RECOVERY_PHASES,
    REMOTE_OVERHEADS,
    RUNTIME_OVERHEADS,
    TURN_GRACE_NS,
    UNBOUND_CLAIM_NS,
    AdmissionMode,
    CapacityDecision,
    CapacityState,
    DiskFigure,
    Grant,
    HolderFacts,
    InterestView,
    PhaseReservation,
    QueueEntryView,
    RuntimeRefusal,
    Turn,
    TurnCandidate,
    TurnDecision,
    actors_dead,
    admission_gate,
    admits_builder,
    allowance_bytes,
    attributed_ids,
    claim_eligible,
    combined,
    counted_grant,
    cpu_pressure_blocks_join,
    derive_reserve,
    detect_runtime,
    disk_estimate,
    disk_fit,
    effective_reservation,
    engine_stats,
    fifo_head,
    has_shared,
    image_in_use,
    is_creation_prefix,
    known_fields,
    live_queued,
    memory_limit,
    net_external_limits,
    parse_cpu_pressure,
    podman_machine_size,
    podman_size_hint,
    private_root_fit,
    publication_verdict,
    reservation_verdict,
    runtime_topology,
    small_vm_hint,
    spawn_eligible,
    turn_decision,
    version_at_least,
    vm_disk_host_directory,
)
from docker_test_toolkit.domain.records import validate_docker_state, validate_failure_evidence, validate_hold_timeline
from docker_test_toolkit.domain.share_policy import (
    DAEMON_SOCKETS,
    BindFact,
    ShareScope,
    ShareVerdict,
    builder_verdict,
    checked_mutation,
    compose_verdict,
    container_verdict,
    exec_verdict,
    network_connect_verdict,
    own_budget_verdict,
    started_service_limits,
)
from docker_test_toolkit.domain.telemetry import TelemetryFault, classify_fault

PROTOCOL = {key: SUPPORTED_PROTOCOLS[key] for key in ("registry", "run", "queue", "receipt")}
# Run records written before queue version 2 carry queue 1 and stay readable; no other map is accepted.
_RECORD_PROTOCOLS = (PROTOCOL, {**PROTOCOL, "queue": 1})
RUN = RUN_LABEL
_DOCKER_FACTORY: Callable[[], DockerPort] | None = None
_BOUND_DOCKER_FACTORY: Callable[[DaemonIdentity], DockerPort] | None = None
_BLOCKING_DOCKER_SECONDS = 14_400
UNKNOWN_EXTERNAL_CAPACITY = "unknown-external-capacity"
_RUN_FIELDS = frozenset(
    {
        "account_directory",
        "admission",
        "admission_observation",
        "bootstrap_deadline",
        "cleanup_outcome",
        "closure",
        "consumers",
        "artifact",
        "docker_authority",
        "docker_receipts",
        "endpoints",
        "entrypoint",
        "generation",
        "grant",
        "guardian",
        "guardian_attempt",
        "infrastructure_failure",
        "initial_phase",
        "last_operation",
        "leave_ack",
        "mode",
        "external_resources",
        "image_bindings",
        "owned_resources",
        "pending_operation",
        "phase_history",
        "phases",
        "private_root",
        "profile",
        "project_id",
        "project_root",
        "protocol",
        "purpose",
        "queue_deadline",
        "quiescent",
        "recovery_actor",
        "release_authority",
        "recovery_outcome",
        "requester",
        "resources",
        "resume_origin",
        "retain",
        "retained_grant",
        "revision",
        "run_id",
        "services",
        "session_id",
        "source_identity",
        "state",
        "synthetic_session",
        "test_outcome",
        "ticket",
        "topology",
        "version",
        "withdrawal",
    }
)


def configure_docker_factories(
    discover: Callable[[], DockerPort],
    bind: Callable[[DaemonIdentity], DockerPort],
) -> None:
    """Supply Docker composition at the process boundary without importing adapters."""
    global _DOCKER_FACTORY, _BOUND_DOCKER_FACTORY
    _DOCKER_FACTORY = discover
    _BOUND_DOCKER_FACTORY = bind


def _docker() -> DockerPort:
    if _DOCKER_FACTORY is None:
        raise InfrastructureFailure("Docker adapter composition is unavailable.")
    return _DOCKER_FACTORY()


def _darwin_library():
    return ctypes.CDLL("/usr/lib/libSystem.B.dylib", use_errno=True)


def _boot_uuid(lib) -> str:
    lib.sysctlbyname.argtypes = [
        ctypes.c_char_p,
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_size_t),
        ctypes.c_void_p,
        ctypes.c_size_t,
    ]
    lib.sysctlbyname.restype = ctypes.c_int
    buffer = ctypes.create_string_buffer(128)
    size = ctypes.c_size_t(len(buffer))
    if lib.sysctlbyname(b"kern.bootsessionuuid", buffer, ctypes.byref(size), None, 0) != 0:
        raise ResourceError("Cannot read native boot identity.")
    try:
        return str(uuid.UUID(buffer.value.decode("ascii")))
    except (ValueError, UnicodeError) as error:
        raise ResourceError("Invalid native boot identity.") from error


class _Timebase(ctypes.Structure):
    _fields_ = [("numer", ctypes.c_uint32), ("denom", ctypes.c_uint32)]


class NativeClock:
    """Elapsed time includes system sleep; an origin change never renews a budget."""

    def __init__(self):
        self.previous = None
        if sys.platform == "darwin":
            self.lib = _darwin_library()
            self.lib.mach_continuous_time.argtypes = []
            self.lib.mach_continuous_time.restype = ctypes.c_uint64
            self.lib.mach_timebase_info.argtypes = [ctypes.POINTER(_Timebase)]
            self.lib.mach_timebase_info.restype = ctypes.c_int
            self.timebase = _Timebase()
            if self.lib.mach_timebase_info(ctypes.byref(self.timebase)) or not (
                self.timebase.numer and self.timebase.denom
            ):
                raise ResourceError("Native continuous clock conversion is unavailable.")
        elif sys.platform != "linux" or not hasattr(time, "CLOCK_BOOTTIME"):
            raise ResourceError("Native suspend-aware clock is unsupported.")
        self.origin = self._origin()

    def _origin(self) -> str:
        if sys.platform == "darwin":
            return "darwin-continuous:" + _boot_uuid(self.lib)
        try:
            boot = str(uuid.UUID(Path("/proc/sys/kernel/random/boot_id").read_text().strip()))
            namespace = os.readlink("/proc/self/ns/time")
        except (OSError, ValueError) as error:
            raise ResourceError("Cannot resolve boot/time namespace.") from error
        return f"linux-boottime:{boot}:{namespace}"

    def read(self) -> int:
        """Read nanoseconds, rejecting origin changes and backwards observations."""
        if self._origin() != self.origin:
            raise ResourceError("Native clock origin changed; reconciliation required.")
        if sys.platform == "darwin":
            value = self.lib.mach_continuous_time() * self.timebase.numer // self.timebase.denom
        else:
            value = time.clock_gettime_ns(time.CLOCK_BOOTTIME)
        if value < 0 or (self.previous is not None and value < self.previous):
            raise ResourceError("Native clock moved backwards.")
        self.previous = value
        return value


@dataclass
class Deadline:
    """Bind an absolute suspend-aware expiry to one native clock origin."""

    origin: str
    created_ns: int
    duration_ns: int
    expires_ns: int

    @classmethod
    def after(cls, clock: NativeClock, seconds: float):
        """Create one finite budget; checking it never moves its expiry."""
        if type(seconds) not in (int, float) or not math.isfinite(seconds) or seconds <= 0:
            raise ResourceError("Deadline must be finite and positive.")
        duration = int(seconds * 1_000_000_000)
        if duration <= 0:
            raise ResourceError("Deadline is below native clock precision.")
        now = clock.read()
        return cls(clock.origin, now, duration, now + duration)

    def remaining_ns(self, clock: NativeClock) -> int:
        """Return remaining original budget; another clock origin is an error."""
        if self.origin != clock.origin:
            raise ResourceError("Deadline belongs to another clock origin.")
        return max(0, self.expires_ns - clock.read())

    def remaining_seconds(self) -> float:
        """Return the remaining budget using the deadline's native clock."""
        return self.remaining_ns(NativeClock()) / 1_000_000_000


class ControlError(ResourceError):
    """A bounded command failed; partial output is evidence, never success."""

    def __init__(
        self,
        message: str,
        result: subprocess.CompletedProcess,
        *,
        reason: str = "unspecified",
        observer: str | None = None,
    ):
        super().__init__(message, reason=reason, observer=observer)
        self.result = result


class ControlObsolete(ResourceError):
    """A completed container mutation no longer describes current state."""


_Params = ParamSpec("_Params")
_Result = TypeVar("_Result")


def _attributed(observer: str) -> Callable[[Callable[_Params, _Result]], Callable[_Params, _Result]]:
    """Attribute an escaping toolkit error to ``observer`` unless an inner boundary already named its source."""

    def decorate(function: Callable[_Params, _Result]) -> Callable[_Params, _Result]:
        """Wrap ``function`` so a toolkit error it raises carries ``observer`` when no inner boundary named one."""

        @functools.wraps(function)
        def attributed(*args: _Params.args, **kwargs: _Params.kwargs) -> _Result:
            """Call the wrapped function and attribute any escaping toolkit error."""
            try:
                return function(*args, **kwargs)
            except ToolkitError as error:
                if error.observer is None:
                    error.observer = observer
                raise

        return attributed

    return decorate


def _finish_control_child(child: subprocess.Popen) -> bool:
    """Stop/reap only this exclusively owned, unreaped direct child, then close pipes."""
    primary = sys.exception()
    try:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=1)
        return True
    except (OSError, subprocess.TimeoutExpired):
        if primary is not None:
            primary.add_note("Control child termination remains unverified.")
        return False
    finally:
        for stream in (child.stdin, child.stdout, child.stderr):
            if stream is not None:
                stream.close()


def run_control(
    argv: list[str],
    *,
    deadline: Deadline,
    stdin: bytes = b"",
    env: dict[str, str] | None = None,
    cwd: Path | None = None,
    output_limit: int = 8 * 1024 * 1024,
) -> subprocess.CompletedProcess:
    """Run one control child under its original native deadline and bounded pipes.

    This function exclusively owns Popen and its wait status until return. On
    failure it stops only that still-unreaped direct child; inherited pipes do not
    authorize signalling descendants. ControlError retains bounded partial output.
    No command arguments, environment or captured diagnostic text enter its message.
    """
    clock = NativeClock()
    if not isinstance(stdin, bytes) or type(output_limit) is not int or output_limit <= 0:
        raise ResourceError("Invalid control input/output bound.")
    if not deadline.remaining_ns(clock):
        raise ResourceError("Control deadline expired before spawn.", reason="observation-deadline")
    try:
        child = subprocess.Popen(
            argv,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=private_process_environment() if env is None else env,
            cwd=cwd,
            close_fds=True,
        )
    except OSError as error:
        raise ResourceError("Control executable could not start.") from error
    child_stdin = child.stdin
    child_stdout = child.stdout
    child_stderr = child.stderr
    if child_stdin is None or child_stdout is None or child_stderr is None:
        _finish_control_child(child)
        raise ResourceError("Control child pipes are unavailable.")
    output = {"stdout": bytearray(), "stderr": bytearray()}
    sent = 0
    failure = None
    failure_reason = "unspecified"
    with selectors.DefaultSelector() as selector:
        try:
            for stream, name in ((child_stdout, "stdout"), (child_stderr, "stderr")):
                os.set_blocking(stream.fileno(), False)
                selector.register(stream, selectors.EVENT_READ, name)
            if stdin:
                os.set_blocking(child_stdin.fileno(), False)
                selector.register(child_stdin, selectors.EVENT_WRITE, "stdin")
            else:
                child_stdin.close()
            while selector.get_map() or child.poll() is None:
                remaining = deadline.remaining_ns(clock)
                if not remaining:
                    failure = "Control deadline expired; command or inherited pipes did not finish."
                    failure_reason = "observation-deadline"
                    break
                for key, _ in selector.select(min(1, remaining / 1_000_000_000)):
                    stream = cast(BinaryIO, key.fileobj)
                    if key.data == "stdin":
                        try:
                            sent += os.write(stream.fileno(), stdin[sent : sent + 65536])
                        except BrokenPipeError:
                            failure = "Control input closed before the complete request was sent."
                        if failure or sent == len(stdin):
                            selector.unregister(stream)
                            stream.close()
                    else:
                        available = output_limit - sum(map(len, output.values()))
                        block = os.read(stream.fileno(), min(65536, available + 1))
                        output[key.data].extend(block[:available])
                        if len(block) > available:
                            failure = "Control output exceeded its bound."
                        if not block:
                            selector.unregister(stream)
                            stream.close()
                    if failure:
                        break
                if failure:
                    break
        finally:
            # No other code receives this Popen or reaps it. An unreaped original
            # child cannot have its PID reused between this poll and Popen.kill.
            if not _finish_control_child(child):
                failure = "Control child termination remains unverified."
    result = subprocess.CompletedProcess(argv, child.returncode, bytes(output["stdout"]), bytes(output["stderr"]))
    if failure is None and not deadline.remaining_ns(clock):
        failure = "Control deadline expired before verified completion."
        failure_reason = "observation-deadline"
    if failure:
        raise ControlError(failure, result, reason=failure_reason)
    return result


@dataclass
class EngineReply:
    """Carry one bounded Docker Engine response and transport status."""

    body: dict
    stderr: bytes
    exit_status: int


class _EnginePipe:
    """Bounded HTTPResponse file interface over the selected Docker CLI's pipes."""

    def __init__(self, child, deadline, clock, selector):
        self.child = child
        self.deadline = deadline
        self.clock = clock
        self.selector = selector
        self.buffer = bytearray()
        self.raw = bytearray()
        self.stderr = bytearray()
        self.eof = False
        self.line_bytes = 0
        for stream, name in ((child.stdout, "stdout"), (child.stderr, "stderr")):
            os.set_blocking(stream.fileno(), False)
            selector.register(stream, selectors.EVENT_READ, name)

    def makefile(self, mode):
        """Expose this bounded reader to the standard-library HTTP parser."""
        return self

    def close(self):
        """Close only the HTTP view; the transport owner still drains the child."""
        # HTTPResponse ends its file view; the owner still drains/reaps the CLI.
        return

    def receive(self):
        """Read one bounded selector batch before the native deadline."""
        remaining = self.deadline.remaining_ns(self.clock)
        if not remaining:
            raise ResourceError("Engine response/control deadline expired.", reason="observation-deadline")
        for key, _ in self.selector.select(min(1, remaining / 1_000_000_000)):
            block = os.read(key.fileobj.fileno(), 65536)
            if not block:
                self.selector.unregister(key.fileobj)
                if key.data == "stdout":
                    self.eof = True
            elif key.data == "stdout":
                self.raw.extend(block)
                if len(self.raw) > 2 * 1024 * 1024:
                    raise ResourceError("Engine wire response exceeded its bound.")
                self.buffer.extend(block)
            else:
                remaining_stderr = 65536 - len(self.stderr)
                self.stderr.extend(block[:remaining_stderr])
                if len(block) > remaining_stderr:
                    raise ResourceError("Engine diagnostic output exceeded its bound.")

    def read(self, count):
        """Read at most the caller's bounded body count."""
        if count < 0 or count > 1024 * 1024 + 1:
            raise ResourceError("Engine body read exceeded its bound.")
        while len(self.buffer) < count and not self.eof:
            self.receive()
        data = bytes(self.buffer[:count])
        del self.buffer[:count]
        return data

    def readline(self, limit=65537):
        """Read one bounded HTTP framing line."""
        while b"\n" not in self.buffer and len(self.buffer) < limit and not self.eof:
            self.receive()
        newline = self.buffer.find(b"\n")
        count = min(limit, newline + 1 if newline >= 0 else len(self.buffer))
        self.line_bytes += count
        # Includes chunk framing/trailers in this conservative aggregate line bound.
        if self.line_bytes > 65536:
            raise ResourceError("Engine response headers/framing exceeded 64 KiB.")
        data = bytes(self.buffer[:count])
        del self.buffer[:count]
        return data


def engine_get(docker, path: str, *, deadline: Deadline | None = None) -> EngineReply:
    """Read one fixed Engine endpoint through Docker's own context/auth transport.

    Require a complete HTTP200 JSON object under the original native deadline,
    64KiB aggregate line and1MiB decoded body bounds. Keep stdin open through parsing,
    then drain/reap the direct CLI. Return bounded diagnostics with its exit status.
    Any malformed/truncated/ambiguous response raises; unknown metrics never become0.
    """
    if not isinstance(path, str) or not re.fullmatch(
        r"/(?:v1\.\d{1,3}/)?(?:version|info|containers/[0-9a-f]{64}/(?:json|stats\?stream=false&one-shot=true))",
        path,
    ):
        raise ResourceError("Unsupported read-only Engine endpoint.")
    clock = NativeClock()
    deadline = deadline or Deadline.after(clock, 30)
    if not deadline.remaining_ns(clock):
        raise ResourceError("Engine control deadline expired before spawn.", reason="observation-deadline")
    argv = [*docker.prefix, "system", "dial-stdio"]
    try:
        child = subprocess.Popen(
            argv,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=docker.env,
            close_fds=True,
        )
    except OSError as error:
        raise ResourceError("Engine transport executable could not start.") from error
    child_stdin = child.stdin
    if child_stdin is None or child.stdout is None or child.stderr is None:
        _finish_control_child(child)
        raise ResourceError("Engine transport pipes are unavailable.")
    stream = None
    with selectors.DefaultSelector() as selector:
        try:
            stream = _EnginePipe(child, deadline, clock, selector)
            child_stdin.write(f"GET {path} HTTP/1.1\r\nHost: docker\r\nConnection: close\r\n\r\n".encode("ascii"))
            child_stdin.flush()
            response = http.client.HTTPResponse(cast(socket.socket, stream))
            response.begin()
            lengths = response.headers.get_all("Content-Length", [])
            encodings = response.headers.get_all("Transfer-Encoding", [])
            if (
                response.status != 200
                or len(lengths) > 1
                or (encodings and (len(encodings) != 1 or encodings[0].lower() != "chunked" or lengths))
                or (not response.chunked and (response.length is None or response.length > 1024 * 1024))
            ):
                raise ResourceError("Engine response status/framing is unsupported or ambiguous.")
            payload = response.read(1024 * 1024 + 1)
            if len(payload) > 1024 * 1024 or not response.isclosed():
                raise ResourceError("Engine body is oversized or incomplete.")
            body = json.loads(payload, object_pairs_hook=unique_object)
            if not isinstance(body, dict):
                raise ResourceError("Engine JSON response must be an object.")
            child_stdin.close()
            while selector.get_map() or child.poll() is None:
                stream.receive()
            if not deadline.remaining_ns(clock):
                raise ResourceError(
                    "Engine deadline expired before verified completion.", reason="observation-deadline"
                )
            if stream.buffer or child.returncode:
                raise ResourceError("Engine transport returned extra data or an unsuccessful exit status.")
            return EngineReply(body, bytes(stream.stderr), child.returncode)
        except (OSError, ValueError, http.client.HTTPException, ResourceError) as error:
            result = subprocess.CompletedProcess(
                argv,
                child.returncode,
                bytes(stream.raw) if stream else b"",
                bytes(stream.stderr) if stream else b"",
            )
            message = str(error) if isinstance(error, ResourceError) else "Engine response is incomplete or invalid."
            cause = error if isinstance(error, ResourceError) else None
            raise ControlError(
                message,
                result,
                reason="unspecified" if cause is None else cause.reason,  # unclassified, as the design requires
                observer=None if cause is None else cause.observer,
            ) from error
        finally:
            primary = sys.exception()
            if not _finish_control_child(child) and primary is None:
                raise ResourceError("Engine transport child termination remains unverified.")
            if isinstance(primary, ControlError):
                primary.result.returncode = child.returncode


@dataclass
class ContainerObservation:
    """Carry exact inspected identity and limits for one container."""

    daemon_id: str
    container_id: str
    started_at: str
    tick_ns: int
    memory_usage_bytes: int
    memory_limit_bytes: int
    oom_events: int
    oom_killed: bool
    restart_count: int


@_attributed("container")
def observe_container(
    docker,
    daemon_id: str,
    container_id: str,
    *,
    previous: ContainerObservation | None = None,
    deadline: Deadline | None = None,
) -> ContainerObservation:
    """Read one running exact container generation and preserve positive OOM evidence."""
    if (
        not isinstance(daemon_id, str)
        or not NAME.fullmatch(daemon_id)
        or not isinstance(container_id, str)
        or not re.fullmatch(r"[0-9a-f]{64}", container_id)
        or (previous is not None and not isinstance(previous, ContainerObservation))
    ):
        raise ResourceError("Invalid daemon/container observation identity.")
    clock = NativeClock()
    deadline = deadline or Deadline.after(clock, 30)
    observed_id, info = _identified_info(docker, deadline, expected=daemon_id)
    if observed_id != daemon_id:
        raise ResourceError(
            "Container observation daemon identity changed.", reason="daemon-identity-changed", observer="engine"
        )
    if info.get("OSType") != "linux" or str(info.get("CgroupVersion")) != "2":
        raise ResourceError("Container observation OS/cgroup identity changed.", reason="telemetry-unreadable")
    inspected = _engine_read(docker, f"/containers/{container_id}/json", deadline).body
    stats = _engine_read(docker, f"/containers/{container_id}/stats?stream=false&one-shot=true", deadline).body
    try:
        state = inspected["State"]
        hard_limit = inspected["HostConfig"]["Memory"]
        started_at = state["StartedAt"]
        running = state["Running"]
        oom_killed = state["OOMKilled"]
        restart_count = inspected["RestartCount"]
        memory = engine_stats(stats["memory_stats"], hard_limit, docker.engine)
        usage = memory["usage"]
        limit = memory["limit"]
        counters = memory["stats"]
    except (KeyError, TypeError, AttributeError) as error:
        raise ResourceError("Container observation is incomplete.", reason="telemetry-unreadable") from error
    oom_events = counters.get("failcnt", 0) if isinstance(counters, dict) else None
    if (
        inspected.get("Id") != container_id
        # Docker 28 on native Linux omits the one-shot stats body's ``id``; the inspected ``Id`` binds the identity, and
        # an ``id`` that is present must still name this container.
        or ("id" in stats and stats["id"] != container_id)
        or not isinstance(started_at, str)
        or not started_at
        or type(running) is not bool
        or not running
        or type(oom_killed) is not bool
        or type(restart_count) is not int
        or restart_count < 0
        or type(hard_limit) is not int
        or hard_limit <= 0
        or type(usage) is not int
        or usage < 0
        or type(limit) is not int
        or limit != hard_limit
        or type(oom_events) is not int
        or oom_events < 0
    ):
        raise ResourceError(
            "Container observation is contradictory or lacks an effective hard limit.", reason="telemetry-unreadable"
        )
    observed = ContainerObservation(
        daemon_id,
        container_id,
        started_at,
        clock.read(),
        usage,
        limit,
        oom_events,
        oom_killed or bool(previous and previous.oom_killed),
        restart_count,
    )
    if previous is not None and (
        previous.daemon_id != daemon_id
        or previous.container_id != container_id
        or previous.started_at != started_at
        or previous.restart_count != restart_count
        or previous.tick_ns > observed.tick_ns
        or previous.oom_events > observed.oom_events
    ):
        raise ResourceError("Container generation or OOM counter changed without a journaled transition.")
    return observed


class _BsdInfo(ctypes.Structure):
    # Full SDK proc_bsdinfo, including fields before the two native birth values.
    _fields_ = (
        [
            (name, ctypes.c_uint32)
            for name in (
                "flags",
                "status",
                "xstatus",
                "pid",
                "ppid",
                "uid",
                "gid",
                "ruid",
                "rgid",
                "svuid",
                "svgid",
                "reserved",
            )
        ]
        + [("comm", ctypes.c_char * 16), ("name", ctypes.c_char * 32)]
        + [(name, ctypes.c_uint32) for name in ("nfiles", "pgid", "jobc", "tdev", "tpgid")]
        + [("nice", ctypes.c_int32), ("start_sec", ctypes.c_uint64), ("start_usec", ctypes.c_uint64)]
    )


@dataclass
class ProcessIdentity:
    """Bind a PID to its boot/time origin and native birth evidence."""

    origin: str
    pid: int
    birth: list[int]
    audit_token: list[int] | None


def _process_birth(pid: int) -> tuple[list[int], bool]:
    if sys.platform == "darwin":
        lib = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
        lib.proc_pidinfo.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_uint64, ctypes.c_void_p, ctypes.c_int]
        lib.proc_pidinfo.restype = ctypes.c_int
        info = _BsdInfo()
        ctypes.set_errno(0)
        size = lib.proc_pidinfo(pid, 3, 0, ctypes.byref(info), ctypes.sizeof(info))
        if size == 0 and ctypes.get_errno() == errno.ESRCH:
            raise ProcessLookupError(errno.ESRCH, "No executing target")
        if size != ctypes.sizeof(info) or info.pid != pid or info.start_usec >= 1_000_000:
            raise ResourceError("Native process identity is unreadable or unsupported.")
        return [info.start_sec, info.start_usec], info.status != 5
    try:
        raw = Path(f"/proc/{pid}/stat").read_text()
    except FileNotFoundError as error:
        raise ProcessLookupError(errno.ESRCH, "No executing target") from error
    suffix = raw.rfind(") ")
    values = raw[suffix + 2 :].split()
    if suffix < 0 or len(values) < 20 or not raw.startswith(f"{pid} ("):
        raise ResourceError("Malformed native process identity.")
    try:
        return [int(values[19])], values[0] != "Z"
    except ValueError as error:
        raise ResourceError("Malformed native process birth.") from error


def current_process(clock: NativeClock) -> ProcessIdentity:
    """Capture this final interpreter's native identity before granting any work."""
    pid = os.getpid()
    birth, executing = _process_birth(pid)
    if not executing:
        raise ResourceError("Requester is not executing.")
    token = None
    if sys.platform == "darwin":
        lib = _darwin_library()
        lib.task_info.argtypes = [
            ctypes.c_uint,
            ctypes.c_int,
            ctypes.POINTER(ctypes.c_uint),
            ctypes.POINTER(ctypes.c_uint),
        ]
        lib.task_info.restype = ctypes.c_int
        words = (ctypes.c_uint * 8)()
        count = ctypes.c_uint(8)
        task = ctypes.c_uint.in_dll(lib, "mach_task_self_").value
        if lib.task_info(task, 15, words, ctypes.byref(count)) or count.value != 8 or words[5] != pid:
            raise ResourceError("Native audit identity is unavailable.")
        token = list(words)
    return ProcessIdentity(clock.origin, pid, birth, token)


def observe_process(identity: ProcessIdentity) -> str:
    """Observe birth-bound liveness; unreadable identity raises rather than implying death.

    This observation grants no signalling or cleanup authority. A later process
    exit is possible; signalling must use the native retained-reference primitive.
    """
    _identity_from_json(asdict(identity))
    if NativeClock().origin != identity.origin:
        return "replaced"
    try:
        birth, executing = _process_birth(identity.pid)
    except ProcessLookupError:
        return "absent"
    except OSError as error:
        raise ResourceError("Process identity is ambiguous.") from error
    if birth != identity.birth:
        return "replaced"
    return "executing" if executing else "zombie"


def signal_process(identity: ProcessIdentity, signum: int) -> bool:
    """Signal a caller-authorized identity through an atomic native process reference.

    Return false only when native observation proves the registered target no
    longer executes. A stale audit token or inaccessible primitive raises
    ResourceError; it never falls back to PID/group signalling or proves cleanup.
    """
    if signum not in (signal.SIGTERM, signal.SIGKILL, signal.SIGINT, signal.SIGSTOP, signal.SIGCONT):
        raise ResourceError("Unsupported lifecycle signal.")
    _identity_from_json(asdict(identity))
    if sys.platform == "darwin":
        if observe_process(identity) != "executing":
            return False
        if identity.audit_token is None or len(identity.audit_token) != 8:
            raise ResourceError("Native audit identity is unavailable.")
        lib = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
        lib.proc_signal_with_audittoken.argtypes = [ctypes.POINTER(ctypes.c_uint), ctypes.c_int]
        lib.proc_signal_with_audittoken.restype = ctypes.c_int
        result = lib.proc_signal_with_audittoken((ctypes.c_uint * 8)(*identity.audit_token), signum)
        if result:
            raise ResourceError(f"Native audit-token signal refused (errno {result}).")
        return True
    if sys.platform != "linux" or not hasattr(os, "pidfd_open") or not hasattr(signal, "pidfd_send_signal"):
        raise ResourceError("Native birth-bound signalling is unsupported.")
    try:
        descriptor = os.pidfd_open(identity.pid)
    except ProcessLookupError:
        return False
    except OSError as error:
        raise ResourceError("Cannot open native process reference.") from error
    try:
        if observe_process(identity) != "executing":
            return False
        signal.pidfd_send_signal(descriptor, signum)
        return True
    except OSError as error:
        raise ResourceError("Native process signal refused.") from error
    finally:
        os.close(descriptor)


@dataclass
class _ObservationReads:
    """Test-composition overrides of the four reads one observation pass makes (``None`` keeps the production read)."""

    engine: Callable[[object, str, Deadline | None], "EngineReply"] | None = None
    rss: Callable[[ProcessIdentity, Deadline], int] | None = None
    host: Callable[[NativeClock, Deadline | None], "HostMemoryObservation"] | None = None
    jobs: Callable[[list[dict], set[str]], "tuple[JobMemoryLimit, ...] | None"] | None = None
    endpoint: Callable[[object, Deadline | None], str] | None = None


_READS = _ObservationReads()


def configure_observation_reads(
    *,
    engine: Callable[[object, str, Deadline | None], "EngineReply"] | None = None,
    rss: Callable[[ProcessIdentity, Deadline], int] | None = None,
    host: Callable[[NativeClock, Deadline | None], "HostMemoryObservation"] | None = None,
    jobs: Callable[[list[dict], set[str]], "tuple[JobMemoryLimit, ...] | None"] | None = None,
    endpoint: Callable[[object, Deadline | None], str] | None = None,
) -> None:
    """Replace the observation reads (``None`` restores the production read); test composition only."""
    _READS.engine, _READS.rss, _READS.host, _READS.jobs = engine, rss, host, jobs
    _READS.endpoint = endpoint


def _engine_read(docker, path: str, deadline: Deadline | None) -> "EngineReply":
    reader = _READS.engine
    return engine_get(docker, path, deadline=deadline) if reader is None else reader(docker, path, deadline)


def _endpoint_read(docker, deadline: Deadline | None) -> str:
    reader = _READS.endpoint
    return docker.endpoint(deadline) if reader is None else reader(docker, deadline)


def _rss_read(identity: ProcessIdentity, deadline: Deadline) -> int:
    reader = _READS.rss
    return observe_process_rss(identity, deadline=deadline) if reader is None else reader(identity, deadline)


def _host_read(clock: NativeClock, deadline: Deadline | None) -> "HostMemoryObservation":
    reader = _READS.host
    if reader is not None:
        return reader(clock, deadline)
    return observe_host_memory(clock) if deadline is None else observe_host_memory(clock, deadline=deadline)


def _job_read(
    containers: list[dict], owned_ids: set[str], is_stopped: Callable[[str], bool] | None = None
) -> "tuple[JobMemoryLimit, ...] | None":
    reader = _READS.jobs  # a test-composition override only (factories.testing)
    return _linux_job_limits(containers, owned_ids, is_stopped) if reader is None else reader(containers, owned_ids)


# The single-letter task states proc_pid_status prints (fs/proc/array.c task_state_array).
_LINUX_TASK_STATES = frozenset("RSDTtXZPIKW")


def _linux_status_rss(status: str) -> int:
    """Return the resident bytes one ``/proc/<pid>/status`` document charges to its run.

    The charge is ``RssAnon + RssShmem + VmLck``. File-backed resident pages (``RssFile``: mapped models and
    libraries) are reclaimable page cache, which the host read already counts as available, so charging them would
    count them twice; locked pages are unevictable whatever backs them, so ``VmLck`` is charged too (locked anonymous
    pages then count twice, an over-charge only for a process that locks memory). Kernels before 4.5 print no ``Rss*``
    rows; their tasks are charged ``VmRSS``. A task that has begun exiting releases its address space before it
    becomes a zombie, so its status keeps the ``State`` row but has no ``Vm*`` rows; it holds no resident memory. A
    status without exactly one recognizable ``State`` row, or with memory rows that are missing, duplicated or
    malformed, refuses.
    """
    rows = status.splitlines()
    states = [row.split() for row in rows if row.startswith("State:")]
    if len(states) != 1 or len(states[0]) < 2 or states[0][1] not in _LINUX_TASK_STATES:
        raise ResourceError("Process RSS has an unsupported Linux format.")
    if not any(row.startswith(("Vm", "Rss")) for row in rows):
        return 0
    if not any(row.startswith("Vm") for row in rows):
        raise ResourceError("Process RSS has an unsupported Linux format.")
    if not any(row.startswith(("RssAnon:", "RssShmem:")) for row in rows):
        return _status_kibibytes(rows, "VmRSS") * 1024
    return sum(_status_kibibytes(rows, name) for name in ("RssAnon", "RssShmem", "VmLck")) * 1024


def _status_kibibytes(rows: list[str], name: str) -> int:
    values = [row.split() for row in rows if row.startswith(f"{name}:")]
    if len(values) != 1 or len(values[0]) != 3 or not values[0][1].isdigit() or values[0][2] != "kB":
        raise ResourceError("Process RSS has an unsupported Linux format.")
    return int(values[0][1])


def observe_process_rss(identity: ProcessIdentity, *, deadline: Deadline | None = None) -> int:
    """Read one birth-bound process RSS without treating its PID as authority."""
    if observe_process(identity) != "executing":
        raise ResourceError("Process RSS target is not executing.")
    if sys.platform == "linux":
        try:
            status = Path(f"/proc/{identity.pid}/status").read_text()
        except OSError as error:
            raise ResourceError("Process RSS is unavailable.") from error
        rss = _linux_status_rss(status)
    elif sys.platform == "darwin":
        result = run_control(
            ["/bin/ps", "-o", "rss=", "-p", str(identity.pid)],
            deadline=deadline or Deadline.after(NativeClock(), 5),
        )
        value = result.stdout.strip()
        if result.returncode or not value.isdigit():
            raise ResourceError("Process RSS has an unsupported Darwin format.")
        rss = int(value) * 1024
    else:
        raise ResourceError("Process RSS observation is unsupported.")
    if observe_process(identity) != "executing":
        raise ResourceError("Process identity changed during RSS observation.")
    return rss


@_attributed("process")
def _active_process_rss(identity: ProcessIdentity, deadline: Deadline) -> int:
    """Return zero when an active process exits during its RSS observation."""
    if observe_process(identity) != "executing":
        return 0
    try:
        return _rss_read(identity, deadline)
    except ResourceError:
        if observe_process(identity) != "executing":
            return 0
        raise


def _private_directory(path: Path):
    make_private_directories(path)
    observed = path.lstat()
    if not stat.S_ISDIR(observed.st_mode) or observed.st_uid != os.getuid() or observed.st_mode & 0o077:
        raise ResourceError("Lifecycle directory is not private and account-owned.")


def bundle_manifest(directory: Path) -> dict:
    """Describe every Python source file under one verified package root."""
    directory = directory.resolve(strict=True)
    hashes = {}
    for path in sorted(directory.rglob("*.py")):
        if path.is_symlink() or not path.is_file():
            raise ResourceError("Runtime package contains a non-regular source file.")
        hashes[path.relative_to(directory).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
    if not hashes:
        raise ResourceError("Runtime package has no Python source files.")
    revision = hashlib.sha256(
        json.dumps(
            {"files": hashes, "protocols": dict(SUPPORTED_PROTOCOLS)}, sort_keys=True, separators=(",", ":")
        ).encode()
    ).hexdigest()
    return {
        "format": 1,
        "protocol": PROTOCOL,
        "source": {
            "location": "docker_test_toolkit",
            "revision": revision,
        },
        "files": hashes,
    }


def _archive_identity(path: Path) -> tuple[os.stat_result, str]:
    """Return the archive's stat and the SHA-256 of its bytes, read through one descriptor."""
    try:
        with path.open("rb") as stream:
            observed = os.fstat(stream.fileno())
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
    except OSError as error:
        raise ResourceError("Runtime artifact is unreadable.") from error
    return observed, digest


_PACKAGE_ROOT = Path(__file__).resolve().parents[1]
_ARCHIVE_PATH = containing_zipapp(__file__)
_LOADED_MANIFEST: dict | None
if _ARCHIVE_PATH is None:
    _LOADED_MANIFEST = bundle_manifest(_PACKAGE_ROOT)
    _LOADED_ARCHIVE: tuple[int, int, str] | None = None
else:
    _loaded_stat, _loaded_digest = _archive_identity(_ARCHIVE_PATH)
    _LOADED_MANIFEST = None
    _LOADED_ARCHIVE = (_loaded_stat.st_dev, _loaded_stat.st_ino, _loaded_digest)


def source_identity() -> str:
    """Pin loaded runtime bytes; changed source refuses new work, not recovery."""
    if _ARCHIVE_PATH is not None and _LOADED_ARCHIVE is not None:
        observed, digest = _archive_identity(_ARCHIVE_PATH)
        if (observed.st_dev, observed.st_ino, digest) != _LOADED_ARCHIVE:
            raise ResourceError("Runtime artifact changed after loading; use the exact installed version.")
        return digest
    current = bundle_manifest(_PACKAGE_ROOT)
    if current != _LOADED_MANIFEST:
        raise ResourceError("Runtime source changed after loading; use a fresh compatible helper for recovery.")
    return current["source"]["revision"]


def toolkit_identity_record() -> dict:
    """Return the running toolkit's identity as a run record carries it: version, archive SHA-256 and protocols."""
    return {"version": TOOLKIT_VERSION, "sha256": source_identity(), "protocols": dict(SUPPORTED_PROTOCOLS)}


def _runtime_import_root() -> Path:
    return _ARCHIVE_PATH if _ARCHIVE_PATH is not None else Path(__file__).parents[2]


def _toolkit_process_command(*arguments: str) -> list[str]:
    """Return the argv that runs this toolkit's own archive, or its source tree."""
    if _ARCHIVE_PATH is not None:
        return [sys.executable, "-I", str(_ARCHIVE_PATH), *arguments]
    return [sys.executable, "-P", "-m", "docker_test_toolkit.factories.cli", *arguments]


def _toolkit_child_environment(temporary: Path, additions: dict[str, str] | None = None) -> dict[str, str]:
    """Build one child's fixed environment."""
    private = build_cache.switch_private_addition()  # the per-run cache switch reaches guardians and consumers (DF12)
    return account_environment(temporary, additions=additions, private_additions=private)


@dataclass
class RunRequest:
    """Carry validated preparation metadata before account enrollment."""

    project_root: Path
    entrypoint: str
    purpose: str
    services: tuple[str, ...]
    closure: tuple[str, ...]
    profile: str
    parent_session: str | None = None
    mode: str = "managed"
    wait_seconds: float = 14_400
    retain: bool = False


@dataclass
class RunContext:
    """Bind one run to its project, registry record, session, and private root."""

    project: Context
    run_id: str
    session_id: str
    record: Path
    private_root: Path


def _remove_private_root(context: RunContext) -> None:
    """Remove a run's private root, then its project and session folders while they are empty.

    The parents go with ``rmdir`` only, project before session, stopping at the first failure, and only when the path
    has the shape ``…/docker-test-toolkit/<session>/<project>/<run>`` of this context; the shared top folder stays. A
    sibling run that exists or is being created in the same session keeps its folders: the creator re-creates any
    level removed before its own ``mkdir`` succeeds. Pruning runs even when the root was already gone, so a rerun of
    recovery after a crash finishes it.
    """
    root = context.private_root
    if root.exists() or root.is_symlink():
        _private_directory(root)
        shutil.rmtree(root)
    project_folder = root.parent
    session_folder = project_folder.parent
    shape = (root.name, project_folder.name, session_folder.name, session_folder.parent.name)
    if shape != (context.run_id, context.project.project_id, context.session_id, PRIVATE_ROOT_NAME):
        return
    # ponytail: safe for one remover per session (session ids are unique per run; the release helper's pair is
    # sequential). Two runs finishing concurrently in one session could fail a sibling's preparation; the upgrade path
    # is a bounded retry in make_private_directories, as git retries SCLD_VANISHED.
    try:
        project_folder.rmdir()
        session_folder.rmdir()
    except OSError:
        pass  # best effort: the authoritative cleanup above already succeeded; a non-empty or vanished level stops it


@dataclass
class RunLease:
    """Carry non-serializable authority for one exact granted generation."""

    context: RunContext
    generation: int
    grant_revision: int
    requester: ProcessIdentity


@dataclass
class RunStatus:
    """Expose bounded lifecycle status without serializing authority."""

    run_id: str
    state: str
    admission: str
    generation: int
    revision: int
    ticket: int | None
    test_outcome: str | None
    cleanup_outcome: str | None


@dataclass(frozen=True)
class _CurrentPythonConsumer:
    """Retain one validated child grant without carrying parent lifecycle authority."""

    context: RunContext
    launch_id: str
    identity: ProcessIdentity
    services: tuple[str, ...]


_CURRENT_PYTHON_CONSUMER: _CurrentPythonConsumer | None = None


def _validate_request(request: RunRequest):
    for value in (request.entrypoint, request.purpose, request.profile):
        if not isinstance(value, str) or not NAME.fullmatch(value):
            raise ResourceError("Invalid public request name; secrets belong in private runtime material.")
    for names in (request.services, request.closure):
        if (
            not isinstance(names, (tuple, list))
            or any(not isinstance(name, str) or not NAME.fullmatch(name) for name in names)
            or len(set(names)) != len(names)
        ):
            raise ResourceError("Services must be unique explicit names.")
    if not set(request.services) <= set(request.closure):
        raise ResourceError("Required services are absent from the declared closure.")
    if request.mode not in ("managed", "external") or type(request.retain) is not bool:
        raise ResourceError("Invalid lifecycle mode or retention request.")
    if request.parent_session is not None and (
        not isinstance(request.parent_session, str) or not UUID.fullmatch(request.parent_session)
    ):
        raise ResourceError("Invalid parent session identity.")


def prepare_run(
    request: RunRequest,
    *,
    _test_state: Path | None = None,
    _test_directory: Path | None = None,
    deadline_ns: int | None = None,
    _release_helper: bool = False,
) -> RunContext:
    """Record a new ungranted run without contacting Docker or entering its queue.

    Each call creates a new run. Invalid identity/deadline/private storage raises
    ResourceError before infrastructure mutation. Original owner fields never renew. ``deadline_ns`` (an absolute
    ``time.monotonic_ns()``) bounds every registry-side wait: registration, the own-project repair pass and the
    project and registry acquisitions that publish the record; expiry is ``lock-deadline``.
    """
    _validate_request(request)
    if (request.purpose == RELEASE_PURPOSE) != _release_helper:
        raise InvalidInput(f"The purpose {RELEASE_PURPOSE!r} is reserved for the release of a retained run.")
    refusal = _gate_refusal(_test_directory, release_helper=_release_helper)  # gate point 1: before any record
    if refusal is not None:
        raise _gate_error(refusal, _test_directory)
    source = source_identity()
    if _test_state is None:
        account_directory()  # the real account belongs to an installation: refuse early outside one
    clock = NativeClock()
    deadline = Deadline.after(clock, request.wait_seconds)
    requester = current_process(clock)
    root = Path(request.project_root).resolve(strict=True)
    if not root.is_dir():
        raise ResourceError("Project root is not a directory.")
    supplied_session = request.parent_session
    if supplied_session and not UUID.fullmatch(supplied_session):
        raise ResourceError("Invalid supplied session identity.")
    session_id = supplied_session or str(uuid.uuid4())
    state = registry_directory() if _test_state is None else _test_state.resolve(strict=True)
    project = Context(root, state)
    _private_directory(state)
    temporary_parent = temporary_folder()
    register(project, session_id=session_id, temporary_parent=temporary_parent, deadline_ns=deadline_ns)
    reconcile_preparations(project, _test_directory=_test_directory, _deadline_ns=deadline_ns)
    run_id = str(uuid.uuid4())
    record_path = state / "runs" / f"{run_id}.json"
    private = temporary_parent / PRIVATE_ROOT_NAME / session_id / project.project_id / run_id
    context = RunContext(project, run_id, session_id, record_path, private)
    data = {
        "version": 1,
        "run_id": run_id,
        "project_id": project.project_id,
        "project_root": str(root),
        "session_id": session_id,
        "synthetic_session": not bool(supplied_session),
        "entrypoint": request.entrypoint,
        "purpose": request.purpose,
        "services": list(request.services),
        "closure": list(request.closure),
        "profile": request.profile,
        "topology": None,
        "phases": {},
        "initial_phase": None,
        "mode": request.mode,
        "queue_deadline": asdict(deadline),
        "retain": request.retain,
        "source_identity": source,
        "protocol": PROTOCOL,
        "state": "prepared",
        "generation": 0,
        "revision": 0,
        "admission": "never-granted",
        "grant": None,
        "retained_grant": None,
        "resume_origin": None,
        "phase_history": [],
        "withdrawal": None,
        "leave_ack": None,
        "recovery_actor": None,
        "release_authority": None,
        "recovery_outcome": None,
        "quiescent": False,
        "admission_observation": None,
        "infrastructure_failure": None,
        "ticket": None,
        "requester": asdict(requester),
        "guardian_attempt": None,
        "guardian": None,
        "bootstrap_deadline": None,
        "account_directory": None,
        "consumers": {},
        "artifact": None,
        "external_resources": [],
        "image_bindings": {},
        "docker_authority": None,
        "docker_receipts": {},
        "owned_resources": {"builders": {}, "containers": {}, "images": {}, "networks": {}, "volumes": {}},
        "resources": {},
        "endpoints": {},
        "pending_operation": None,
        "last_operation": None,
        "test_outcome": None,
        "cleanup_outcome": None,
        "private_root": str(private),
    }
    with (
        lock(project, project.project_id, shared=True, deadline_ns=deadline_ns),
        lock(project, "registry", deadline_ns=deadline_ns),
    ):
        registry = load(project)
        _private_directory(record_path.parent)
        atomic_json(record_path, data)
        failpoint("prepare-record-written")
        registry["runs"][run_id] = str(record_path)
        save(project, registry)
        _private_directory(private)
    return context


def declare_run_metadata(
    context: RunContext,
    artifact: ArtifactIdentity,
    external_resources: tuple[ExternalDockerResource, ...],
    *,
    deadline_ns: int | None = None,
) -> None:
    """Bind public artifact and external-resource declarations before admission.

    ``deadline_ns`` (absolute ``time.monotonic_ns()``) ends the project and run lock waits; the opener passes its
    preparation deadline so the registry-side waits of ``open_run`` stay inside one budget.
    """
    if (
        not isinstance(artifact, ArtifactIdentity)
        or not isinstance(external_resources, tuple)
        or any(not isinstance(item, ExternalDockerResource) for item in external_resources)
    ):
        raise ResourceError("Run metadata declaration is invalid.")
    with _locked_run(context, deadline_ns):
        data = _load_run(context)
        if (
            data["state"] != "prepared"
            or data["guardian_attempt"] is not None
            or data["artifact"] is not None
            or data["external_resources"]
        ):
            raise ResourceError("Run metadata cannot change after declaration or startup.")
        data["artifact"] = {
            "version": artifact.version,
            "sha256": artifact.sha256,
            "protocols": dict(artifact.protocols),
        }
        data["external_resources"] = [asdict(item) for item in external_resources]
        data["revision"] += 1
        atomic_json(context.record, data)


def _write_private_executable(path: Path, content: bytes) -> str:
    """Atomically publish one account-private executable and return its digest."""
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.chmod(0o700)
        temporary.replace(path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)
    return hashlib.sha256(content).hexdigest()


def _docker_authority_paths(context: RunContext, generation: int) -> tuple[Path, Path, Path]:
    if type(generation) is not int or generation < 1:
        raise ResourceError("Docker authority generation is invalid.")
    directory = context.private_root if generation == 1 else context.private_root / f"generation-{generation}"
    return directory / "docker", directory / "docker-test-toolkit-dispatch", directory / "docker-authority.json"


def _consumer_docker_path(context: RunContext, launch_id: str) -> Path:
    """Return the fixed private Docker wrapper path for one Python launch."""
    if not isinstance(launch_id, str) or not UUID.fullmatch(launch_id):
        raise ResourceError("Python consumer launch identity is invalid.")
    return context.private_root / f"python-{launch_id}-docker" / "docker"


def _consumer_launch_paths(private_root: Path, launch_id: str) -> tuple[Path, Path, Path]:
    """Return the request, ready and grant files of one Python consumer launch."""
    return (
        private_root / f"python-{launch_id}.request.json",
        private_root / f"python-{launch_id}.ready.json",
        private_root / f"python-{launch_id}.grant.json",
    )


def _guardian_log_paths(private_root: Path) -> tuple[Path, Path]:
    return private_root / "guardian.stdout.log", private_root / "guardian.stderr.log"


def _consumer_docker_wrapper(context: RunContext, data: dict, launch_id: str) -> bytes:
    """Build one consumer wrapper over the already attested root dispatcher."""
    authority = data.get("docker_authority")
    if authority is None:
        raise ResourceError("Docker authority must be materialized before borrowing.")
    grant_path = _consumer_launch_paths(context.private_root, launch_id)[2]
    interpreter = Path(sys.executable).resolve(strict=True)
    child_environment = ""
    if _ARCHIVE_PATH is None:
        child_environment = f"PYTHONPATH={shlex.quote(str(_runtime_import_root()))}\nexport PYTHONPATH\n"
    command = _toolkit_process_command(
        "_docker_dispatch",
        "--descriptor",
        str(authority["descriptor_path"]),
        "--dispatcher",
        str(authority["dispatcher_path"]),
        "--shim",
        str(authority["shim_path"]),
        "--consumer-grant",
        str(grant_path),
    )
    command[0] = str(interpreter)
    return (
        "#!/bin/sh\nset -eu\n"
        "[ ! -L \"$0\" ] || { printf '%s\\n' "
        "'Docker toolkit consumer shim cannot execute through a symbolic link.' >&2; exit 3; }\n"
        f'{child_environment}exec {shlex.join(command)} -- "$@"\n'
    ).encode()


def _publish_consumer_docker_wrapper(context: RunContext, data: dict, launch_id: str) -> Path:
    path = _consumer_docker_path(context, launch_id)
    _private_directory(path.parent)
    _write_private_executable(path, _consumer_docker_wrapper(context, data, launch_id))
    return path


def _require_consumer_docker_wrapper(context: RunContext, data: dict, launch_id: str) -> Path:
    path = _consumer_docker_path(context, launch_id)
    expected = hashlib.sha256(_consumer_docker_wrapper(context, data, launch_id)).hexdigest()
    if _private_file_sha256(path, executable=True) != expected:
        raise ResourceError("Python consumer Docker wrapper changed after grant.")
    return path


def _publish_docker_authority(
    context: RunContext,
    data: dict,
    identity: DaemonIdentity,
    generation: int,
) -> dict[str, object]:
    """Write one generation-specific dispatcher before its run record is published."""
    try:
        shim_bytes = importlib.resources.files("docker_test_toolkit.adapters.outbound").joinpath("docker").read_bytes()
        interpreter = Path(sys.executable).resolve(strict=True)
    except OSError as error:
        raise ResourceError("Private Docker dispatcher source is unavailable.") from error
    shim_path, dispatcher_path, descriptor_path = _docker_authority_paths(context, generation)
    _private_directory(shim_path.parent)
    child_environment = ""
    if _ARCHIVE_PATH is None:
        child_environment = f"PYTHONPATH={shlex.quote(str(_runtime_import_root()))}\nexport PYTHONPATH\n"
    command = _toolkit_process_command("_docker_dispatch", "--descriptor", str(descriptor_path))
    command[0] = str(interpreter)
    dispatcher = (
        "#!/bin/sh\nset -eu\n"
        'dispatcher_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)\n'
        'dispatcher_path="$dispatcher_dir/$(basename -- "$0")"\n'
        f"{child_environment}exec {shlex.join(command)} "
        '--dispatcher "$dispatcher_path" --shim "$dispatcher_dir/docker" -- "$@"\n'
    ).encode()
    shim_sha256 = _write_private_executable(shim_path, shim_bytes)
    dispatcher_sha256 = _write_private_executable(dispatcher_path, dispatcher)
    compose_project = compose_project_name(context.project.project_id, context.run_id)
    builder = builder_name(context.project.project_id, context.run_id)
    authority = {
        **asdict(identity),
        "client_path": str(identity.client_path),
        "generation": generation,
        "compose_project": compose_project,
        "builder": builder,
        "shim_path": str(shim_path),
        "shim_sha256": shim_sha256,
        "dispatcher_path": str(dispatcher_path),
        "dispatcher_sha256": dispatcher_sha256,
        "descriptor_path": str(descriptor_path),
    }
    descriptor_record = {
        "protocol": "docker-test-toolkit.dispatch/1",
        "context": _context_payload(context),
        "run_id": context.run_id,
        "generation": generation,
        "source_identity": source_identity(),
        "artifact": data["artifact"],
        "authority": authority,
    }
    atomic_json(descriptor_path, descriptor_record)
    descriptor_sha256 = hashlib.sha256(descriptor_path.read_bytes()).hexdigest()
    authority["descriptor_sha256"] = descriptor_sha256
    return authority


def _authority_identity(authority: dict) -> DaemonIdentity:
    return DaemonIdentity(
        daemon_id=authority["daemon_id"],
        client_path=Path(authority["client_path"]),
        client_sha256=authority["client_sha256"],
        client_version=authority["client_version"],
        client_device=authority["client_device"],
        client_inode=authority["client_inode"],
        client_size=authority["client_size"],
        client_mtime_ns=authority["client_mtime_ns"],
        client_context=authority["client_context"],
    )


def _require_docker_authority_files(context: RunContext, authority: dict) -> None:
    """Require the exact private files bound by one authority."""
    expected = _docker_authority_paths(context, authority["generation"])
    recorded = tuple(Path(authority[key]) for key in ("shim_path", "dispatcher_path", "descriptor_path"))
    if (
        recorded != expected
        or _private_file_sha256(recorded[0], executable=True) != authority["shim_sha256"]
        or _private_file_sha256(recorded[1], executable=True) != authority["dispatcher_sha256"]
        or _private_file_sha256(recorded[2]) != authority["descriptor_sha256"]
    ):
        raise ResourceError("Docker execution authority files changed before publication.")


def materialize_docker_authority(lease: RunLease, docker) -> dict[str, object]:
    """Bind one immutable private shim context to the entered run and daemon."""
    data = _require_active_lease(lease, managed=False)
    identity = docker.observe_daemon()
    if identity.daemon_id != data["grant"]["daemon_id"]:
        raise ResourceError("Docker daemon differs from the admitted run authority.")
    for item in data["external_resources"]:
        declaration = ExternalDockerResource(**item)
        observation = docker.inspect_exact(ExactInspection(declaration.kind, declaration.identifier))
        if not observation.exists or observation.identifier != declaration.identifier:
            raise ResourceError("Declared external Docker resource is absent or changed.")
    authority = _publish_docker_authority(lease.context, data, identity, lease.generation)
    context = lease.context
    shim_path = Path(cast(str, authority["shim_path"]))
    project = context.project
    with lock(project, project.project_id, shared=True), lock(project, f"run-{context.run_id}"):
        current = _require_active_lease(lease, managed=False)
        if current["docker_authority"] is not None:
            if current["docker_authority"] != authority:
                raise ResourceError("Docker execution authority is already bound differently.")
        elif current["docker_receipts"] or any(current["owned_resources"].values()):
            raise ResourceError("Docker authority cannot bind after mutation evidence exists.")
        else:
            current["docker_authority"] = authority
            current["revision"] += 1
            atomic_json(context.record, current)
    return {
        "executable": shim_path,
        "path": f"{shim_path.parent}{os.pathsep}{os.environ.get('PATH', '')}",
        "compose_project": authority["compose_project"],
        "builder": authority["builder"],
    }


def _private_file_sha256(path: Path, *, executable: bool = False) -> str:
    try:
        observed = path.stat(follow_symlinks=False)
        content = path.read_bytes()
    except OSError as error:
        raise ResourceError("Private Docker authority file is unreadable.") from error
    if (
        not stat.S_ISREG(observed.st_mode)
        or observed.st_uid != os.getuid()
        or observed.st_mode & 0o077
        or (executable and not observed.st_mode & stat.S_IXUSR)
    ):
        raise ResourceError("Private Docker authority file has unsafe ownership or mode.")
    return hashlib.sha256(content).hexdigest()


def load_docker_dispatch(
    descriptor_path: Path,
    dispatcher_path: Path,
    shim_path: Path,
) -> tuple[RunContext, dict, DaemonIdentity]:
    """Validate an adjacent private dispatcher descriptor against the live run record."""
    path = descriptor_path.resolve(strict=True)
    descriptor = _read_private_json(path)
    if not isinstance(descriptor, dict) or set(descriptor) != {
        "artifact",
        "authority",
        "context",
        "generation",
        "protocol",
        "run_id",
        "source_identity",
    }:
        raise ResourceError("Docker dispatcher descriptor is incomplete.")
    if descriptor["protocol"] != "docker-test-toolkit.dispatch/1" or descriptor["source_identity"] != source_identity():
        raise ResourceError("Docker dispatcher protocol or runtime identity changed.")
    context = _context_from_payload(descriptor["context"])
    try:
        executed_dispatcher = dispatcher_path.resolve(strict=True)
        executed_shim = shim_path.resolve(strict=True)
    except OSError as error:
        raise ResourceError("Docker dispatcher or shim execution path is unavailable.") from error
    expected_descriptor = _docker_authority_paths(context, descriptor["generation"])[2]
    if path != expected_descriptor or descriptor["run_id"] != context.run_id:
        raise ResourceError("Docker dispatcher descriptor moved outside its exact run root.")
    data = _load_run(context)
    authority = data["docker_authority"]
    if (
        authority is None
        or descriptor["generation"] != data["generation"]
        or descriptor["generation"] != authority["generation"]
        or descriptor["artifact"] != data["artifact"]
        or descriptor["authority"] != {key: value for key, value in authority.items() if key != "descriptor_sha256"}
        or executed_dispatcher != Path(authority["dispatcher_path"])
        or executed_shim != Path(authority["shim_path"])
        or _private_file_sha256(path) != authority["descriptor_sha256"]
        or _private_file_sha256(Path(authority["shim_path"]), executable=True) != authority["shim_sha256"]
        or _private_file_sha256(Path(authority["dispatcher_path"]), executable=True) != authority["dispatcher_sha256"]
        or data["state"] != "active"
        or data["admission"] != "published"
        or data["grant"]["daemon_id"] != authority["daemon_id"]
    ):
        raise ResourceError("Docker dispatcher authority is stale, changed, or inactive.")
    try:
        identity = DaemonIdentity(
            daemon_id=authority["daemon_id"],
            client_path=Path(authority["client_path"]),
            client_sha256=authority["client_sha256"],
            client_version=authority["client_version"],
            client_device=authority["client_device"],
            client_inode=authority["client_inode"],
            client_size=authority["client_size"],
            client_mtime_ns=authority["client_mtime_ns"],
            client_context=authority["client_context"],
        )
    except (KeyError, TypeError) as error:
        raise ResourceError("Docker client identity is incomplete.") from error
    return context, descriptor, identity


def _owned_image_reference(data: dict, reference: str) -> bool:
    return any(
        isinstance(references, list) and reference in references
        for image in data["owned_resources"]["images"].values()
        for references in (image["evidence"].get("repo_tags"), image["evidence"].get("repo_digests"))
    )


def _authorize_observation(command: ClassifiedDockerCommand, data: dict) -> None:
    argv = command.argv
    logs = argv[:2] == ("container", "logs")
    if argv[:1] == ("inspect",):
        kind, start = None, 1
    elif argv[:2] in {
        ("container", "inspect"),
        ("image", "inspect"),
        ("network", "inspect"),
        ("volume", "inspect"),
    }:
        kind, start = argv[0], 2
    elif argv[:2] in {("container", "logs"), ("container", "stats"), ("container", "wait")}:
        kind, start = "container", 2
    elif argv[:2] == ("buildx", "inspect"):
        kind, start = "builder", 2
    else:
        return
    targets = []
    index = start
    while index < len(argv):
        value = argv[index]
        if logs and value in {"--follow", "--timestamps", "-f", "-t"}:
            index += 1
            continue
        if value == "--type" or value.startswith("--type="):
            if kind is not None:
                raise InvalidInput("Docker inspection type is valid only for general inspect.")
            if value == "--type":
                if index + 1 >= len(argv):
                    raise InvalidInput("Docker inspection type lacks its value.")
                kind = argv[index + 1]
                index += 2
            else:
                kind = value.split("=", 1)[1]
                index += 1
            if kind not in {"container", "image", "network", "volume"}:
                raise InvalidInput("Docker inspection type is outside the bounded toolkit surface.")
            continue
        if value in {"--condition", "--format", "--scope", "--since", "--tail", "--until", "-f", "-n"}:
            index += 2
            continue
        if value.startswith(
            ("--condition=", "--format=", "--scope=", "--since=", "--tail=", "--until=", "-f=", "-n=")
        ) or value in {
            "--bootstrap",
            "--details",
            "--follow",
            "--no-stream",
            "--no-trunc",
            "--size",
            "-s",
            "--verbose",
            "-v",
        }:
            index += 1
            continue
        if value.startswith("-"):
            raise InvalidInput("Docker inspection option is outside the bounded toolkit surface.")
        targets.append(value)
        index += 1
    if len(targets) != 1:
        raise InvalidInput("Docker inspection accepts exactly one target.")
    target = targets[0]
    kinds = (kind,) if kind is not None else ("container", "image", "network", "volume")
    matches = [
        candidate
        for candidate in kinds
        if target in data["owned_resources"].get(f"{candidate}s", {})
        or any(item["kind"] == candidate and item["identifier"] == target for item in data["external_resources"])
        or (
            candidate == "image"
            and (
                any(
                    target in {reference, binding["identifier"]}
                    for reference, binding in data["image_bindings"].items()
                )
                or _owned_image_reference(data, target)
            )
        )
    ]
    if len(matches) != 1:
        raise InvalidInput("Exact Docker inspection target is neither owned nor declared external.")


def _append_docker_receipt(
    context: RunContext,
    descriptor: dict,
    operation_id: str,
    command: ClassifiedDockerCommand,
    deadline: Deadline,
    deadline_ns: int | None = None,
    image_lock_wait: tuple[str, int, int] | None = None,
) -> dict:
    with _locked_run(context, deadline_ns):
        data = _load_run(context)
        authority = data["docker_authority"]
        if (
            data["state"] not in {"active", "stopping"}
            or data["generation"] != descriptor["generation"]
            or authority is None
            or any(item["state"] in {"pending", "unresolved"} for item in data["docker_receipts"].values())
        ):
            raise ResourceBusy("Another Docker mutation is unresolved or run authority is inactive.")
        if command.kind == "builder-create" or command.kind in _BUILD_KINDS:
            _require_builder_phase(data)
        if command.kind in _ARCHIVE_KINDS:
            _admit_archive_append(data, command)
        revision = data["revision"] + 1
        receipt = {
            "id": operation_id,
            "kind": command.kind,
            "generation": data["generation"],
            "revision": revision,
            "descriptor_sha256": authority["descriptor_sha256"],
            "deadline": asdict(deadline),
            "plan": command.plan,
            "state": "pending",
            "result": None,
            "resources": [],
        }
        data["docker_receipts"][operation_id] = receipt
        data["revision"] = revision
        if image_lock_wait is not None:
            _record_image_lock_wait(data, image_lock_wait)
        atomic_json(context.record, data)
        return receipt


def _record_image_lock_wait(data: dict, wait: tuple[str, int, int]) -> bool:
    """Merge one image-lock wait into ``hold_timeline.waiting`` (reason ``image-lock``), never into expiry or failure
    evidence (design N9); return whether the record changed. A wait from another clock origin than the timeline's is
    dropped."""
    origin, first, last = wait
    timeline = data.get("hold_timeline")
    if not isinstance(timeline, dict) or timeline.get("origin") != origin or last <= first:
        return False
    for item in timeline["waiting"]:
        if item["reason"] == IMAGE_LOCK_WAIT:
            item.update(last_tick_ns=max(item["last_tick_ns"], last), waited_ns=item["waited_ns"] + last - first)
            return True
    timeline["waiting"].append(
        {"reason": IMAGE_LOCK_WAIT, "first_tick_ns": first, "last_tick_ns": last, "waited_ns": last - first}
    )
    return True


def _record_grants_unreadable(data: dict, tick: tuple[str, int]) -> None:
    """Record once in ``hold_timeline.waiting`` that the grants log was unreadable (reason
    ``shared-grants-unreadable``, design 2.5, R3-8). A tick from another clock origin than the timeline's is dropped."""
    origin, now = tick
    timeline = data.get("hold_timeline")
    if not isinstance(timeline, dict) or timeline.get("origin") != origin:
        return
    if not any(item["reason"] == SHARED_GRANTS_UNREADABLE for item in timeline["waiting"]):
        timeline["waiting"].append(
            {"reason": SHARED_GRANTS_UNREADABLE, "first_tick_ns": now, "last_tick_ns": now, "waited_ns": 0}
        )


def _owned_resource(
    kind: str,
    identifier: str,
    name: str,
    receipt_id: str,
    state: str,
    evidence: dict[str, object],
) -> dict[str, object]:
    return {
        "kind": kind,
        "identifier": identifier,
        "name": name,
        "receipt_id": receipt_id,
        "state": state,
        "evidence": evidence,
    }


def _container_promotion(
    adapter, receipt: dict, result: DockerResult, labels: dict[str, str]
) -> tuple[str, list[dict]]:
    plan = receipt["plan"]
    cidfile = Path(plan["cidfile"])
    try:
        container_id = cidfile.read_text().strip()
    except FileNotFoundError:
        if plan["remove"]:
            named = adapter.inspect_exact(ExactInspection("container", plan["name"]))
            if not named.exists:
                return ("failed" if result.exit_code else "removed"), []
        return ("failed" if result.exit_code else "unresolved"), []
    except OSError as error:
        raise ResourceError("Private container identity receipt is unreadable.") from error
    if not re.fullmatch(r"[0-9a-f]{64}", container_id):
        return "unresolved", []
    observation = adapter.inspect_exact(ExactInspection("container", container_id))
    if not observation.exists:
        return ("removed" if plan["remove"] else "unresolved"), []
    item = observation.attributes
    observed_labels = item.get("Config", {}).get("Labels") if isinstance(item.get("Config"), dict) else None
    name = item.get("Name")
    state = item.get("State")
    if (
        item.get("Id") != container_id
        or not isinstance(name, str)
        or name.lstrip("/") != plan["name"]
        or not isinstance(observed_labels, dict)
        or any(observed_labels.get(key) != value for key, value in labels.items())
        or not isinstance(item.get("Created"), str)
        or item.get("Image") != plan["image_id"]
        or not isinstance(state, dict)
        or type(state.get("Running")) is not bool
    ):
        return "unresolved", []
    resource = _owned_resource(
        "container",
        container_id,
        plan["name"],
        receipt["id"],
        "running" if state["Running"] else "stopped",
        {"created": item["Created"], "image_id": item["Image"], "labels": labels},
    )
    return "promoted", [resource]


def _exact_control_promotion(adapter, receipt: dict, result: DockerResult) -> tuple[str, list[dict]]:
    container_id = receipt["plan"]["container_id"]
    observation = adapter.inspect_exact(ExactInspection("container", container_id))
    action = receipt["kind"].removeprefix("container-")
    if action == "rm":
        return ("removed" if not observation.exists else "unresolved"), []
    if not observation.exists:
        return ("failed" if result.exit_code else "unresolved"), []
    state = observation.attributes.get("State")
    expected = action == "start"
    if not isinstance(state, dict) or state.get("Running") is not expected:
        return ("failed" if result.exit_code else "unresolved"), []
    return "promoted", []


def _exact_resource_removal_promotion(adapter, receipt: dict) -> tuple[str, list[dict]]:
    kind = receipt["kind"].removesuffix("-rm")
    target = receipt["plan"][f"{kind}_id"]
    if kind == "builder":
        return ("removed" if _builder_observation(adapter, target) is None else "unresolved"), []
    if kind == "image":
        references = receipt["plan"].get("references")
        if references is not None:
            if (
                not isinstance(references, list)
                or not references
                or any(not isinstance(reference, str) or not reference for reference in references)
            ):
                return "unresolved", []
            exists = any(adapter.inspect_exact(ExactInspection("image", reference)).exists for reference in references)
            return ("unresolved" if exists else "removed"), []
        target = receipt["plan"].get("reference", target)
    observation = adapter.inspect_exact(ExactInspection(kind, target))
    return ("removed" if not observation.exists else "unresolved"), []


def _direct_resource_promotion(
    adapter, receipt: dict, result: DockerResult, labels: dict[str, str]
) -> tuple[str, list[dict]]:
    kind = receipt["kind"].removesuffix("-create")
    name = receipt["plan"]["name"]
    observation = adapter.inspect_exact(ExactInspection(kind, name))
    if not observation.exists:
        return ("failed" if result.exit_code else "unresolved"), []
    item = observation.attributes
    observed_labels = item.get("Labels")
    identifier = item.get("Id") if kind == "network" else item.get("Name")
    created_key = "Created" if kind == "network" else "CreatedAt"
    if (
        not isinstance(identifier, str)
        or not identifier
        or item.get("Name") != name
        or not isinstance(observed_labels, dict)
        or any(observed_labels.get(key) != value for key, value in labels.items())
        or not isinstance(item.get(created_key), str)
        or not item[created_key]
    ):
        return "unresolved", []
    evidence: dict[str, object] = {"created": item[created_key], "labels": labels}
    if kind == "volume":
        if not isinstance(item.get("Driver"), str) or not item["Driver"]:
            return "unresolved", []
        evidence["driver"] = item["Driver"]
    return "promoted", [_owned_resource(kind, identifier, name, receipt["id"], "present", evidence)]


def _compose_promotion(adapter, receipt: dict, result: DockerResult, data: dict) -> tuple[str, list[dict]]:
    plan = receipt["plan"]
    project = plan["project"]
    if not isinstance(project, str):
        raise InvalidInput("Compose project identity is invalid.")
    observed = adapter.execute(("compose", "--project-name", project, "ps", "-aq"), timeout_seconds=30)
    if observed.exit_code:
        return "unresolved", []
    identifiers = tuple(dict.fromkeys(observed.stdout.split()))
    resources = []
    attached_volumes: set[str] = set()
    compose_run = receipt["kind"] == "compose-run"
    known_containers = data["owned_resources"]["containers"]
    one_off_seen = False
    for container_id in identifiers:
        if not re.fullmatch(r"[0-9a-f]{64}", container_id):
            return "unresolved", []
        inspection = adapter.inspect_exact(ExactInspection("container", container_id))
        item = inspection.attributes
        config = item.get("Config")
        observed_labels = config.get("Labels") if isinstance(config, dict) else None
        name = item.get("Name")
        state = item.get("State")
        mounts = item.get("Mounts")
        network_settings = item.get("NetworkSettings")
        ports = network_settings.get("Ports") if isinstance(network_settings, dict) else None
        service = observed_labels.get("com.docker.compose.service") if isinstance(observed_labels, dict) else None
        expected_image = plan.get("image_ids", {}).get(service) if isinstance(plan.get("image_ids", {}), dict) else None
        if compose_run and container_id not in known_containers:
            exact_one_off = (
                not one_off_seen
                and isinstance(name, str)
                and name.lstrip("/") == plan["name"]
                and isinstance(observed_labels, dict)
                and observed_labels.get("com.docker.compose.oneoff") == "True"
                and service == plan["service"]
            )
            if not exact_one_off:
                return "unresolved", []
            one_off_seen = True
        if (
            not inspection.exists
            or item.get("Id") != container_id
            or not isinstance(name, str)
            or not isinstance(observed_labels, dict)
            or observed_labels.get("com.docker.compose.project") != project
            or service not in plan["services"]
            or not isinstance(item.get("Created"), str)
            or not isinstance(item.get("Image"), str)
            or (expected_image is not None and item.get("Image") != expected_image)
            or not isinstance(state, dict)
            or type(state.get("Running")) is not bool
            or not isinstance(state.get("StartedAt"), str)
            or not state["StartedAt"]
            or type(state.get("OOMKilled")) is not bool
            or type(item.get("RestartCount")) is not int
            or item["RestartCount"] < 0
            or not isinstance(mounts, list)
            or not isinstance(ports, dict)
        ):
            return "unresolved", []
        bindings: dict[str, list[dict[str, str | int]]] = {}
        for target, published in ports.items():
            if not isinstance(target, str) or not re.fullmatch(r"[1-9][0-9]{0,4}/(?:tcp|udp)", target):
                return "unresolved", []
            if published is None:
                bindings[target] = []
                continue
            if not isinstance(published, list):
                return "unresolved", []
            converted = []
            for binding in published:
                if (
                    not isinstance(binding, dict)
                    or not isinstance(binding.get("HostIp"), str)
                    or not isinstance(binding.get("HostPort"), str)
                    or not binding["HostPort"].isdigit()
                    or not 0 < int(binding["HostPort"]) <= 65535
                ):
                    return "unresolved", []
                converted.append({"host_ip": binding["HostIp"], "host_port": int(binding["HostPort"])})
            bindings[target] = sorted(converted, key=lambda value: (value["host_ip"], value["host_port"]))
        for mount in mounts:
            if not isinstance(mount, dict):
                return "unresolved", []
            if mount.get("Type") == "volume":
                volume_name = mount.get("Name")
                if not isinstance(volume_name, str) or not NAME.fullmatch(volume_name):
                    return "unresolved", []
                attached_volumes.add(volume_name)
        resources.append(
            _owned_resource(
                "container",
                container_id,
                name.lstrip("/"),
                receipt["id"],
                "running" if state["Running"] else "stopped",
                {
                    "compose_project": project,
                    "config_sha256": plan["config_sha256"],
                    "created": item["Created"],
                    "image_id": item["Image"],
                    "service": service,
                    "bindings": bindings,
                    "completed_service": service in set(plan.get("completed_services", ())),
                    "control": {
                        "started_at": state["StartedAt"],
                        "restart_count": item["RestartCount"],
                        "oom_killed": state["OOMKilled"],
                        "oom_events": 0,
                    },
                },
            )
        )
    for name in plan["networks"]:
        inspection = adapter.inspect_exact(ExactInspection("network", name))
        if not inspection.exists:
            continue
        item = inspection.attributes
        native_labels = item.get("Labels")
        network_id = item.get("Id")
        if (
            not isinstance(network_id, str)
            or not re.fullmatch(r"[0-9a-f]{64}", network_id)
            or item.get("Name") != name
            or not isinstance(item.get("Created"), str)
            or not isinstance(native_labels, dict)
            or native_labels.get("com.docker.compose.project") != project
        ):
            return "unresolved", []
        resources.append(
            _owned_resource(
                "network",
                network_id,
                name,
                receipt["id"],
                "present",
                {
                    "compose_project": project,
                    "config_sha256": plan["config_sha256"],
                    "created": item["Created"],
                },
            )
        )
    planned_volumes = set(plan["volumes"])
    for name in sorted(planned_volumes | attached_volumes):
        inspection = adapter.inspect_exact(ExactInspection("volume", name))
        if not inspection.exists:
            continue
        item = inspection.attributes
        native_labels = item.get("Labels")
        if (
            item.get("Name") != name
            or not isinstance(item.get("CreatedAt"), str)
            or not isinstance(item.get("Driver"), str)
            or not isinstance(native_labels, dict)
            or (
                native_labels.get("com.docker.compose.project") != project
                and (name in planned_volumes or not _anonymous_volume(name, native_labels))
            )
        ):
            return "unresolved", []
        resources.append(
            _owned_resource(
                "volume",
                name,
                name,
                receipt["id"],
                "present",
                {
                    "compose_project": project,
                    "config_sha256": plan["config_sha256"],
                    "created": item["CreatedAt"],
                    "driver": item["Driver"],
                },
            )
        )
    live_identities = {(resource["kind"], resource["identifier"]) for resource in resources}
    for owned in data["owned_resources"].values():
        for previous in owned.values():
            identity = (previous["kind"], previous["identifier"])
            if previous["evidence"].get("compose_project") != project or identity in live_identities:
                continue
            if adapter.inspect_exact(ExactInspection(*identity)).exists:
                if previous["kind"] == "volume" and _validate_owned_identity(adapter, previous):
                    resources.append({**previous, "receipt_id": receipt["id"]})
                    continue
                return "unresolved", []
            resources.append({**previous, "receipt_id": receipt["id"], "state": "removed"})
    if compose_run and not one_off_seen:
        return ("promoted" if result.exit_code == 0 else "failed"), resources
    if not any(resource["state"] != "removed" for resource in resources):
        if plan["subcommand"] in {"down", "rm"} and result.exit_code == 0:
            return "removed", resources
        return ("failed" if result.exit_code else "unresolved"), resources
    return "promoted", resources


def _builder_observation(adapter, name: str) -> dict | None:
    observed = adapter.execute(("buildx", "ls", "--format", "{{json .}}"), timeout_seconds=30)
    if observed.exit_code:
        raise InfrastructureFailure("Buildx builder inventory could not be inspected.")
    matches = []
    for line in observed.stdout.splitlines():
        try:
            item = json.loads(line, object_pairs_hook=unique_object)
        except ValueError as error:
            raise InfrastructureFailure("Buildx builder inventory is invalid JSON.") from error
        if isinstance(item, dict) and item.get("Name") == name and item not in matches:
            matches.append(item)
    if not matches:
        return None
    if len(matches) != 1:
        raise InfrastructureFailure("Buildx builder inventory contains contradictory exact identities.")
    return matches[0]


def _builder_endpoint_identity(item: dict) -> str | None:
    nodes = item.get("Nodes")
    if not isinstance(nodes, list) or not nodes:
        return None
    endpoints = []
    for node in nodes:
        if (
            not isinstance(node, dict)
            or not isinstance(node.get("Name"), str)
            or not isinstance(node.get("Endpoint"), str)
        ):
            return None
        endpoints.append({"name": node["Name"], "endpoint": node["Endpoint"]})
    return hashlib.sha256(
        json.dumps(sorted(endpoints, key=lambda value: value["name"]), sort_keys=True).encode()
    ).hexdigest()


def _builder_promotion(adapter, receipt: dict, result: DockerResult) -> tuple[str, list[dict]]:
    name = receipt["plan"]["name"]
    item = _builder_observation(adapter, name)
    if item is None:
        return ("failed" if result.exit_code else "unresolved"), []
    endpoint_identity = _builder_endpoint_identity(item)
    if item.get("Driver") != receipt["plan"]["driver"] or endpoint_identity is None:
        return "unresolved", []
    buildkit_name = receipt["plan"].get("buildkit_name")
    buildkit = None
    cleanup_buildkit = None
    if item["Driver"] == "docker-container":
        if not isinstance(buildkit_name, str):
            return "unresolved", []
        observation = adapter.inspect_exact(ExactInspection("container", buildkit_name))
        if observation.exists:
            cleanup_buildkit = _buildkit_identity_resource(observation.attributes, buildkit_name, name, receipt["id"])
            buildkit = _buildkit_resource(observation.attributes, buildkit_name, name, receipt["id"])
            if cleanup_buildkit is None:
                return "unresolved", []
    evidence = {
        "driver": item["Driver"],
        "endpoint_sha256": endpoint_identity,
        "buildkit_container_id": cleanup_buildkit["identifier"] if cleanup_buildkit else None,
        "limits_verified": buildkit is not None,
        "max_parallelism": receipt["plan"]["max_parallelism"],
        "config_sha256": receipt["plan"]["config_sha256"],
    }
    if buildkit is not None:
        evidence.update(
            memory_bytes=receipt["plan"]["memory_bytes"],
            daemon_cpus=receipt["plan"]["daemon_cpus"],
        )
        volume = _state_volume_identity(adapter, receipt["plan"].get("state_volume"), observation.attributes)
        if volume is not None:
            evidence["state_volume"] = volume
    resources = [_owned_resource("builder", name, name, receipt["id"], "present", evidence)]
    if cleanup_buildkit is not None:
        resources.append(buildkit or cleanup_buildkit)
    return "promoted", resources


def _state_volume_identity(adapter, state_volume: object, worker: dict) -> dict | None:
    """Return the worker's state volume as ``{name, created_at}`` when the worker mounts it, for retention to remove
    exactly that incarnation later (design 2.7); ``None`` when it cannot be proven (retention then only reports it)."""
    if not isinstance(state_volume, str):
        return None
    mounts = worker.get("Mounts")
    if not isinstance(mounts, list) or not any(
        isinstance(mount, dict) and mount.get("Type") == "volume" and mount.get("Name") == state_volume
        for mount in mounts
    ):
        return None
    observation = adapter.inspect_exact(ExactInspection("volume", state_volume))
    created = observation.attributes.get("CreatedAt") if observation.exists else None
    return {"name": state_volume, "created_at": created} if isinstance(created, str) and created else None


def _image_size(item: dict) -> int | None:
    """Return an inspected image's size for the derived disk estimate (design DD8), or ``None`` when it is not one."""
    size = item.get("Size")
    return size if type(size) is int and size >= 0 else None


def _buildkit_identity_resource(candidate: dict, name: str, builder: str, receipt_id: str) -> dict | None:
    state = candidate.get("State")
    if (
        not isinstance(candidate.get("Id"), str)
        or not re.fullmatch(r"[0-9a-f]{64}", candidate["Id"])
        or str(candidate.get("Name", "")).lstrip("/") != name
        or not isinstance(candidate.get("Created"), str)
        or not isinstance(candidate.get("Image"), str)
        or not isinstance(candidate.get("Config"), dict)
        or not isinstance(candidate["Config"].get("Labels"), dict)
        or not isinstance(state, dict)
        or type(state.get("Running")) is not bool
    ):
        return None
    return _owned_resource(
        "container",
        candidate["Id"],
        name,
        receipt_id,
        "running" if state["Running"] else "stopped",
        {
            "created": candidate["Created"],
            "image_id": candidate["Image"],
            "builder": builder,
        },
    )


def _buildkit_resource(candidate: dict, name: str, builder: str, receipt_id: str) -> dict | None:
    resource = _buildkit_identity_resource(candidate, name, builder, receipt_id)
    host = candidate.get("HostConfig")
    if (
        resource is None
        or not isinstance(host, dict)
        or host.get("Memory") != BUILDER_MEMORY_BYTES
        or host.get("MemorySwap") != BUILDER_MEMORY_BYTES
        or host.get("CpuPeriod") != 100000
        or host.get("CpuQuota") != 200000
        or not isinstance(host.get("RestartPolicy"), dict)
        or host["RestartPolicy"].get("Name") != "no"
    ):
        return None
    resource["evidence"].update(memory_bytes=BUILDER_MEMORY_BYTES, daemon_cpus=BUILDER_DAEMON_CPUS)
    return resource


def _write_buildkit_config(command: ClassifiedDockerCommand) -> None:
    path = Path(cast(str, command.plan["config_path"]))
    encoded = BUILDKIT_CONFIG_TEXT.encode()
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    except FileExistsError:
        try:
            existing = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        except OSError as error:
            raise InvalidInput("BuildKit configuration identity changed before builder creation.") from error
        with os.fdopen(existing, "rb") as stream:
            details = os.fstat(stream.fileno())
            payload = stream.read(len(encoded) + 1)
        if (
            not stat.S_ISREG(details.st_mode)
            or details.st_uid != os.getuid()
            or stat.S_IMODE(details.st_mode) != 0o600
            or payload != encoded
        ):
            raise InvalidInput("BuildKit configuration identity changed before builder creation.") from None
        return
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())


def _require_builder_phase(data: dict) -> None:
    budget = _phase_budgets(data["phases"], data["initial_phase"])[data["grant"]["phase"]]
    if budget.container_bytes < BUILDER_MEMORY_BYTES or budget.daemon_cpus < BUILDER_DAEMON_CPUS:
        raise InvalidInput("Current phase does not admit the calibrated test-builder envelope.")


def _prepare_builder_receipt(adapter, command: ClassifiedDockerCommand) -> ClassifiedDockerCommand:
    name = command.plan["name"]
    if not isinstance(name, str):
        raise InvalidInput("Buildx builder identity is invalid.")
    if _builder_observation(adapter, name) is not None:
        raise InvalidInput("Buildx builder identity collides with an existing builder.")
    buildkit_name = buildkit_worker(name)
    if adapter.inspect_exact(ExactInspection("container", buildkit_name)).exists:
        raise InvalidInput("Buildx worker identity collides with an existing container.")
    state_volume = buildkit_state_volume(name)  # Buildx's own name for the worker's state volume
    if adapter.inspect_exact(ExactInspection("volume", state_volume)).exists:
        raise InvalidInput("Buildx state volume identity collides with an existing volume.")  # never adopt one
    return ClassifiedDockerCommand(
        command.kind, command.argv, {**command.plan, "buildkit_name": buildkit_name, "state_volume": state_volume}
    )


def _named_compose_resources(config: dict, section: str, project: str) -> list[str]:
    values = config.get(section, {})
    if not isinstance(values, dict):
        raise InvalidInput("Rendered Compose resource declarations are invalid.")
    names = []
    for key, value in values.items():
        if not isinstance(key, str) or not NAME.fullmatch(key) or not isinstance(value, dict):
            raise InvalidInput("Rendered Compose resource identity is invalid.")
        name = value.get("name", f"{project}_{key}")
        if not isinstance(name, str) or not NAME.fullmatch(name):
            raise InvalidInput("Rendered Compose resource name is invalid.")
        names.append(name)
    return sorted(set(names))


def _completed_compose_services(config: dict) -> tuple[str, ...]:
    """Infer one-shot services from native Compose dependency conditions."""
    services = config.get("services")
    if not isinstance(services, dict):
        raise InvalidInput("Rendered Compose services are invalid.")
    completed: set[str] = set()
    for service in services.values():
        if not isinstance(service, dict):
            raise InvalidInput("Rendered Compose service is invalid.")
        dependencies = service.get("depends_on", {})
        if not isinstance(dependencies, dict):
            raise InvalidInput("Rendered Compose dependencies are invalid.")
        for dependency, condition in dependencies.items():
            if not isinstance(dependency, str) or dependency not in services or not isinstance(condition, dict):
                raise InvalidInput("Rendered Compose dependency is invalid.")
            if condition.get("condition") == "service_completed_successfully":
                completed.add(dependency)
    return tuple(sorted(completed))


_COMPOSE_CONSTRUCT_REFUSED = "compose-construct-refused"


def builder_name(project_id: str, run_id: str) -> str:
    """Return the run-scoped Buildx builder name (its BuildKit worker container is ``buildx_buildkit_<name>0``)."""
    return f"dtt-{project_id[:12]}-{run_id.replace('-', '')[:12]}"


def compose_project_name(project_id: str, run_id: str) -> str:
    """Return the run-scoped Compose project name every Compose command and label of that run carries."""
    return f"{COMPOSE_PREFIX}-{project_id[:12]}-{run_id.replace('-', '')[:12]}"


def _require_compose_files_unchanged(files: object) -> None:
    """Refuse execution when any recorded Compose file no longer hashes to its validated content."""
    if not isinstance(files, list):
        raise InvalidInput("Compose file record is invalid.", reason=_COMPOSE_CONSTRUCT_REFUSED)
    for item in files:
        try:
            digest = hashlib.sha256(Path(item["path"]).read_bytes()).hexdigest()
        except OSError as error:
            raise InvalidInput("A validated Compose file is unreadable.", reason=_COMPOSE_CONSTRUCT_REFUSED) from error
        if digest != item["sha256"]:
            raise InvalidInput("A Compose file changed after validation.", reason=_COMPOSE_CONSTRUCT_REFUSED)


def _prepare_compose_receipt(
    adapter, command: ClassifiedDockerCommand, data: dict
) -> tuple[ClassifiedDockerCommand, dict]:
    plan = command.plan
    index = plan["subcommand_index"]
    if type(index) is not int or not 3 <= index < len(command.argv):
        raise InvalidInput("Compose subcommand position is invalid.")
    # Compose also reads persistent options after the subcommand (``up --file=x``): the render sees them too, so the
    # rules judge the document that executes.
    hoisted, _understood = compose_trailing_globals(command.argv)
    rendered = adapter.execute((*command.argv[:index], *hoisted, "config", "--format", "json"), timeout_seconds=30)
    if rendered.exit_code:
        raise InvalidInput("Compose configuration could not be rendered before mutation.")
    try:
        config = json.loads(rendered.stdout, object_pairs_hook=unique_object)
    except ValueError as error:
        raise InvalidInput("Rendered Compose configuration is invalid JSON.") from error
    services = config.get("services") if isinstance(config, dict) else None
    if (
        not isinstance(services, dict)
        or not services
        or any(not isinstance(name, str) or not NAME.fullmatch(name) for name in services)
    ):
        raise InvalidInput("Rendered Compose services are absent or invalid.")
    image_ids = {}
    observed_platforms: dict[str, tuple[Platform | None, Platform | None]] = {}
    permitted_images = (
        {
            item["identifier"]
            for item in data["external_resources"]
            if item["kind"] == "image" and item["relationship"] == "use_image"
        }
        | set(data["owned_resources"]["images"])
        | {item["identifier"] for item in data["image_bindings"].values()}
    )
    selected_service = plan.get("service") if plan["subcommand"] == "run" else None
    if selected_service is not None and selected_service not in services:
        raise InvalidInput("Compose run service is absent from the rendered configuration.")
    for service_name, service in services.items():
        if not isinstance(service, dict):
            raise InvalidInput("Rendered Compose service is invalid.")
        mounts = service.get("volumes", [])
        if not isinstance(mounts, list) or any(
            not isinstance(mount, dict) or (mount.get("type") == "volume" and not isinstance(mount.get("source"), str))
            for mount in mounts
        ):
            raise InvalidInput("Anonymous or invalid Compose volumes cannot be journaled exactly.")
        if plan["subcommand"] in {"create", "up"} or service_name == selected_service:
            image_reference = service.get("image")
            if not isinstance(image_reference, str) or not image_reference:
                raise InvalidInput("Compose service lacks one declared image.")
            image = adapter.inspect_exact(ExactInspection("image", image_reference))
            image_id = image.attributes.get("Id")
            if not image.exists or not isinstance(image_id, str) or image_id not in permitted_images:
                raise InvalidInput("Compose service requires one exact owned or declared image.")
            image_ids[service_name] = image_id
            observed_platforms[service_name] = (
                parse_requested_platform(service["platform"]) if isinstance(service.get("platform"), str) else None,
                Platform.from_fields(
                    image.attributes.get("Os"), image.attributes.get("Architecture"), image.attributes.get("Variant")
                ),
            )
    project = plan["project"]
    if not isinstance(project, str):
        raise InvalidInput("Compose project identity is invalid.")
    networks = _named_compose_resources(config, "networks", project)
    if not networks:
        networks = [f"{project}_default"]
    volumes = _named_compose_resources(config, "volumes", project)
    enriched = {
        **plan,
        "config_sha256": hashlib.sha256(rendered.stdout.encode()).hexdigest(),
        "services": sorted(services),
        "networks": networks,
        "volumes": volumes,
    }
    completed_services = _completed_compose_services(config)
    if completed_services:
        enriched["completed_services"] = list(completed_services)
    if image_ids:
        enriched["image_ids"] = image_ids
        enriched.update(_compose_platform_facts(adapter, observed_platforms, data))
    prior_hashes = {
        resource["evidence"].get("config_sha256")
        for items in data["owned_resources"].values()
        for resource in items.values()
        if resource["evidence"].get("compose_project") == project
    }
    if prior_hashes and prior_hashes != {enriched["config_sha256"]}:
        raise InvalidInput("Compose configuration changed after exact resources were promoted.")
    if plan["subcommand"] == "run":
        name = plan.get("name")
        if not isinstance(name, str) or adapter.inspect_exact(ExactInspection("container", name)).exists:
            raise InvalidInput("Compose run reconciliation name is invalid or already exists.")
    current = adapter.execute(("compose", "--project-name", project, "ps", "-aq"), timeout_seconds=30)
    if current.exit_code:
        raise InfrastructureFailure("Compose project collision inspection failed.")
    known_containers = data["owned_resources"]["containers"]
    identifiers = current.stdout.split()
    if any(not re.fullmatch(r"[0-9a-f]{64}", item) or item not in known_containers for item in identifiers):
        raise InvalidInput("Compose project identity collides with an unowned container.")
    for kind, names in (("network", networks), ("volume", volumes)):
        owned = data["owned_resources"][f"{kind}s"]
        for name in names:
            observation = adapter.inspect_exact(ExactInspection(kind, name))
            if observation.exists:
                identifier = observation.attributes.get("Id") if kind == "network" else name
                resource = owned.get(identifier) if isinstance(identifier, str) else None
                if (
                    resource is None
                    or resource["state"] == "removed"
                    or not _validate_owned_identity(adapter, resource)
                ):
                    raise InvalidInput("Compose resource name collides with an unowned object.")
    return ClassifiedDockerCommand(command.kind, command.argv, enriched, command.mutating, command.blocking), config


def _private_root_trees(registry: Path) -> tuple[str, ...]:
    """Return ``<T>/docker-test-toolkit`` for every temporary parent T the registry's sessions recorded (design 2.3).

    An unreadable project record raises: a tree it would name could otherwise pass as foreign-free.
    """
    trees: set[str] = set()
    for path in sorted((registry / "projects").glob("*.json")):
        sessions = _read_private_json(path).get("sessions")
        for session in sessions.values() if isinstance(sessions, dict) else ():
            if not isinstance(session, dict):
                continue
            for parent in (session.get("temporary_parent"), *(session.get("previous_temporary_parents") or ())):
                if isinstance(parent, str):
                    trees.add(os.path.realpath(Path(parent) / PRIVATE_ROOT_NAME))
    return tuple(sorted(trees))


def _socket_paths() -> tuple[str, ...]:
    """Return the daemon sockets a bind may not expose: the well-known ones, ``DOCKER_HOST`` and every context's.

    Context endpoints are read from the client's context store files (no Docker call); a superset only over-refuses.
    """
    paths = set(DAEMON_SOCKETS)
    hosts = [os.environ.get("DOCKER_HOST", "")]
    config = Path(os.environ.get("DOCKER_CONFIG") or Path.home() / ".docker")
    for meta in config.glob("contexts/meta/*/meta.json"):
        try:
            hosts.append(json.loads(meta.read_text())["Endpoints"]["docker"]["Host"])
        except (OSError, ValueError, KeyError, TypeError):
            continue
    paths.update(host.removeprefix("unix://") for host in hosts if isinstance(host, str) and host.startswith("unix://"))
    return tuple(sorted(paths | {os.path.realpath(path) for path in paths}))


def _share_scope(context: RunContext, data: dict) -> ShareScope:
    return ShareScope(
        compose_project=data["docker_authority"]["compose_project"],
        private_root=os.path.realpath(data["private_root"]),
        checkout=os.path.realpath(data["project_root"]),
        private_root_trees=_private_root_trees(context.project.state),
        socket_paths=_socket_paths(),
    )


def _bind_fact(source: object, read_only: bool) -> BindFact:
    """Resolve one bind source with ``realpath``; a missing or unreadable source is not a socket."""
    resolved = os.path.realpath(str(source))
    try:
        is_socket = stat.S_ISSOCK(os.stat(resolved).st_mode)
    except OSError:
        is_socket = False
    return BindFact(resolved, read_only, is_socket)


_UNBOUNDED_BYTES = 1 << 62  # a container without a hard limit: R12 refuses beside it


def _hard_limit_bytes(container: dict) -> int:
    """Return an inspected container's hard memory limit, or unbounded when it has none (R12)."""
    return memory_limit(container) or _UNBOUNDED_BYTES


def _own_running_bytes(adapter, data: dict, *, counted_elsewhere: Callable[[dict], bool]) -> int:
    """Sum the hard memory limits of the run's own running containers (R12).

    A container is the run's when it carries its Compose project or run label; the BuildKit worker carries neither
    (``buildx create`` sets no label), so it is never counted. One without a limit counts as unbounded, so R12
    refuses beside it.
    """
    authority = data["docker_authority"]
    total = 0
    for item in adapter.objects("container"):
        labels = (item.get("Config") or {}).get("Labels") or {}
        own = (
            labels.get("com.docker.compose.project") == authority["compose_project"]
            or labels.get(RUN) == data["run_id"]
        )
        running = (item.get("State") or {}).get("Running") is True
        if not own or not running or counted_elsewhere(item):
            continue
        total += _hard_limit_bytes(item)
    return total


def _phase_container_bytes(data: dict) -> int:
    return _phase_budgets(data["phases"], data["initial_phase"])[data["grant"]["phase"]].container_bytes


def _may_overtake(
    run_id: str,
    data: dict,
    mode: str,
    settings: AdmissionSettings,
    state: QueueState,
    views: Sequence[QueueEntryView],
    directory: Path,
    test_directory: Path | None = None,
) -> bool:
    """Whether a queued run may reserve before the FIFO head under the ``repository`` share scope (design consultation
    2026-11-02): runs of the holders' repository join them while other repositories wait for the whole repository.

    Only the earliest waiting run of the holders' repository qualifies, every run ahead of it is of another repository,
    every holder is of its repository, shared and readable, the machine limit has room, every entry reads multi-holder
    queues, and the oldest waiting run has waited less than ``OVERTAKE_WINDOW_NS``; at that bound no run overtakes and
    the holders drain. Capacity, CPU and the image lock are checked afterwards as for any joiner.
    """
    if settings.share_scope != "repository" or mode != AdmissionMode.SHARED:
        return False
    if not all(view.version_ok for view in views):
        return False
    holding = [view for view in views if view.phase in HOLDER_PHASES]
    if not holding or any(view.mode != AdmissionMode.SHARED for view in holding):
        return False
    if settings.limit is not None and len(holding) >= settings.limit:
        return False
    own = repository_identity(data["project_root"])
    line = live_queued(views)
    ticket = next((view.ticket for view in line if view.run_id == run_id), None)
    if ticket is None or not line:
        return False
    for view in [*holding, *(item for item in line if item.ticket < ticket)]:
        record = _entry_record(view.run_id, state.entries[view.run_id], directory)
        if record is None:
            return False
        same = repository_identity(record["project_root"]) == own
        if same != (view.phase in HOLDER_PHASES):  # holders are of the repository, every run ahead is of another
            return False
    oldest = _entry_record(line[0].run_id, state.entries[line[0].run_id], directory)
    if oldest is None:
        return False
    waited, window = _overtake_wait(oldest["queue_deadline"], test_directory)
    return waited < window


def _overtake_wait(queued: dict, test_directory: Path | None) -> tuple[int, int]:
    """Return how long the oldest queued run has waited and the overtake window (``OVERTAKE_WINDOW_NS``, or a private
    account's bound one in tests); a deadline of another clock origin counts as the whole window."""
    clock = NativeClock()
    bound = _bound(test_directory)
    window = bound.get("overtake_window_ns", OVERTAKE_WINDOW_NS)
    return (clock.read() - queued["created_ns"] if queued["origin"] == clock.origin else window), window


def _turn_outcome(
    settings: AdmissionSettings,
    state: QueueState,
    views: Sequence[QueueEntryView],
    directory: Path,
    daemon_id: str | None,
    test_directory: Path | None,
) -> TurnDecision | None:
    """Decide the repository turn under M (design 2.4); ``None`` when no turn applies or a record cannot be read.

    The candidates are the live queued runs with their repositories read through their records, as ``_may_overtake``
    reads them; an unreadable one leaves today's order in place.
    """
    if (
        daemon_id is None
        or settings.share_scope != "repository"
        or settings.limit == 1
        or any(view.phase in HOLDER_PHASES for view in views)  # holders present: no turn applies, read no record
    ):
        return None
    turns = read_turns(directory)
    line = live_queued(views)
    if not turns or not line:
        return None
    candidates, records = [], []
    for view in line:
        record = _entry_record(view.run_id, state.entries[view.run_id], directory)
        if record is None:
            return None
        records.append(record)
        candidates.append(TurnCandidate(view.run_id, view.ticket, repository_identity(record["project_root"])))
    clock = NativeClock()
    now = clock.read()
    waited, window = _overtake_wait(records[0]["queue_deadline"], test_directory)
    bound = _bound(test_directory)  # never a real account's
    grace = bound.get("turn_grace_ns", TURN_GRACE_NS)
    return turn_decision(
        candidates,
        turns,
        now_ns=now,
        clock_origin=clock.origin,
        daemon_id=daemon_id,
        oldest_waited_ns=waited,
        holders_present=False,
        share_scope=settings.share_scope,
        limit=settings.limit,
        window_ns=window,
        grace_ns=grace,
    )


def _record_turn(directory: Path, data: dict, test_directory: Path | None) -> None:
    """Write the leaving run's repository turn, in the section that removes its queue entry (design 2.4, DD1).

    Only under repository scope above limit 1; under other settings the turn file is removed. The write prunes other
    turns with the grace that decides them (a private account's bound one in tests). The leave never fails for a turn:
    unreadable settings or a failed write leave the next batch without one.
    """
    clock = NativeClock()
    try:
        settings = read_admission_settings(directory)
        if settings.share_scope != "repository" or settings.limit == 1:
            write_turns(directory, {}, now_ns=clock.read(), clock_origin=clock.origin)  # removes the file
            return
        daemon_id = (data.get("grant") or {}).get("daemon_id")
        if not isinstance(daemon_id, str):
            return
        bound = _bound(test_directory)
        now = clock.read()
        turns = read_turns(directory)
        turns[repository_identity(data["project_root"])] = Turn(data["run_id"], daemon_id, clock.origin, now)
        write_turns(
            directory,
            turns,
            now_ns=now,
            clock_origin=clock.origin,
            grace_ns=bound.get("turn_grace_ns", TURN_GRACE_NS),
        )
    except (OSError, ResourceError):
        return


def _drop_lease(directory: Path, state: QueueState) -> None:
    """Drop the leases of runs that no longer hold, in the section that removed a queue entry (design DD4). A failed
    write only leaves a stale lease the next reservation's write drops; a holder without one counts the floor."""
    holders = {run_id for run_id, entry in state.entries.items() if entry.phase in HOLDER_PHASES}
    try:
        write_leases(directory, read_leases(directory), holders=holders, clock_origin=NativeClock().origin)
    except (OSError, ResourceError):
        return


def _drop_turn(directory: Path, data: dict) -> None:
    """Remove the recovered run's own turn; a turn another run of its repository earned stays (design 2.4)."""
    remove_turn(directory, repository_identity(data["project_root"]), data["run_id"])


def _other_repository(settings: AdmissionSettings, data: dict, holder: dict) -> bool:
    """Whether the machine shares only within a repository and ``holder`` is of another repository than ``data``."""
    if settings.share_scope != "repository":
        return False
    return repository_identity(holder["project_root"]) != repository_identity(data["project_root"])


def _builder_removed(data: dict) -> bool:
    """Whether the run may no longer run a builder, so it holds no builder reserve (share rule R16 refuses a new one).

    Builders belong to the run's first phase: once it left that phase with no live builder, none is left to reserve
    for. A holder of an older version than ``BUILDER_RELEASE_MIN_VERSION`` does not enforce R16 and keeps its reserve.
    """
    builders = data["owned_resources"]["builders"]
    live = any(item["state"] != "removed" for item in builders.values())
    grant = data.get("grant") or {}
    left_first_phase = grant.get("phase", data["initial_phase"]) != data["initial_phase"]
    artifact = data.get("artifact")
    enforces_r16 = version_at_least(
        artifact.get("version") if isinstance(artifact, dict) else None, BUILDER_RELEASE_MIN_VERSION
    )
    return enforces_r16 and left_first_phase and not live


def _evaluates_share_rules(command: ClassifiedDockerCommand) -> bool:
    """Return whether a classified command is a row of the enforcement table (design 2.3): never teardown or I/O."""
    subcommand = command.plan.get("subcommand")
    return (
        checked_mutation(command.kind, subcommand)
        or command.kind in {"network-connect", "builder-create"}
        or (command.kind == "compose-io" and subcommand == "exec")
    )


def _share_verdict(
    context: RunContext, data: dict, adapter, command: ClassifiedDockerCommand, argv: tuple[str, ...], config: dict
) -> ShareVerdict:
    """Return the first share rule one creating or starting command breaks (design 2.3 enforcement table)."""
    kind, budget = command.kind, _phase_container_bytes(data)
    if kind == "builder-create":
        return builder_verdict(removed=_builder_removed(data))
    if kind == "network-connect":
        return network_connect_verdict()
    if kind == "compose-io":
        return exec_verdict(privileged=compose_share_facts(argv).privileged)
    scope = _share_scope(context, data)
    if kind == "container-create":
        direct = container_share_facts(argv)
        network_id = command.plan.get("network_id")
        foreign = isinstance(network_id, str) and network_id not in data["owned_resources"]["networks"]
        verdict = container_verdict(
            memory_bytes=direct.memory_bytes,
            binds=[_bind_fact(source, read_only) for source, read_only in direct.binds],
            external_network=str(network_id) if foreign else None,
            scope=scope,
        )
        if not verdict.shareable or direct.memory_bytes is None:
            return verdict
        running = _own_running_bytes(adapter, data, counted_elsewhere=lambda _item: False)
        return own_budget_verdict(running_bytes=running, added_bytes=direct.memory_bytes, phase_container_bytes=budget)
    if kind == "container-start":
        target = str(command.plan["container_id"])
        started = _hard_limit_bytes(adapter.inspect_exact(ExactInspection("container", target)).attributes)
        running = _own_running_bytes(adapter, data, counted_elsewhere=lambda item: item.get("Id") == target)
        return own_budget_verdict(running_bytes=running, added_bytes=started, phase_container_bytes=budget)
    return _compose_share_verdict(data, adapter, command, argv, config, scope, budget)


def _compose_share_verdict(
    data: dict,
    adapter,
    command: ClassifiedDockerCommand,
    argv: tuple[str, ...],
    config: dict,
    scope: ShareScope,
    budget: int,
) -> ShareVerdict:
    """The Compose rows of the enforcement table: rules on the whole render, then R12 for what the command starts."""
    facts = compose_share_facts(argv)
    subcommand = str(command.plan["subcommand"])
    binds = {  # resolution and socket facts per source; each mount's own read-only mode is applied by the rule
        str(mount["source"]): _bind_fact(mount["source"], read_only=False)
        for service in config["services"].values()
        for mount in service.get("volumes", [])
        if isinstance(mount, dict) and mount.get("type") == "bind"
    }
    verdict = compose_verdict(
        config,
        scope,
        binds,
        scale=facts.scale,
        watch=facts.watch,
        services=facts.services,
        bound_input=_bindable(facts),
    )
    if not verdict.shareable:
        return verdict
    added = started_service_limits(
        config,
        subcommand=subcommand,
        services=facts.services,
        no_deps=facts.no_deps,  # the classifier requires --no-deps on every compose run
    )
    project = scope.compose_project

    def selected(item: dict) -> bool:
        """A running service container that ``up``/``create``/``start``/``restart`` keeps or replaces counts once, at
        its rendered limit; a ``run`` one-off starts beside it, and one-offs are never replaced, so both count."""
        labels = (item.get("Config") or {}).get("Labels") or {}
        return (
            subcommand != "run"
            and labels.get("com.docker.compose.oneoff") != "True"
            and labels.get("com.docker.compose.project") == project
            and labels.get("com.docker.compose.service") in added
        )

    if subcommand in ("start", "restart"):
        # start/restart reuse existing containers with their own limits, whatever the render now says: the stopped
        # service containers they start add their inspected limits; running ones are already counted
        stopped = _stopped_service_limits(adapter, project, set(added))
        running = _own_running_bytes(adapter, data, counted_elsewhere=lambda _item: False)
        return own_budget_verdict(running_bytes=running, added_bytes=stopped, phase_container_bytes=budget)
    running = _own_running_bytes(adapter, data, counted_elsewhere=selected)
    return own_budget_verdict(running_bytes=running, added_bytes=sum(added.values()), phase_container_bytes=budget)


def _stopped_service_limits(adapter, project: str, services: set[str]) -> int:
    """Sum the inspected memory limits of the stopped, non-one-off containers of ``services`` in ``project``."""
    total = 0
    for item in adapter.objects("container"):
        labels = (item.get("Config") or {}).get("Labels") or {}
        if (
            labels.get("com.docker.compose.project") != project
            or labels.get("com.docker.compose.service") not in services
            or labels.get("com.docker.compose.oneoff") == "True"
            or (item.get("State") or {}).get("Running") is True
        ):
            continue
        total += _hard_limit_bytes(item)
    return total


def _verdict_key(data: dict) -> str | None:
    """Return the run request's verdict-store key, or ``None`` when its checkout has no Git origin (design 2.2): such a
    run neither predicts nor teaches a verdict."""
    origin = repository_origin(data["project_root"])
    if origin is None:
        return None
    return request_key(origin, data["entrypoint"], data["profile"], data["closure"])


def _record_negative_verdict(context: RunContext, data: dict, verdict: ShareVerdict) -> bool:
    """Persist the violated rule as the request's negative verdict under the registry lock; return whether it was.

    A write failure is reported to the caller, never raised: an exclusive run proceeds, a shared run still refuses. A
    checkout without an origin records nothing (it is never shared, so nothing refuses on it).
    """
    key = _verdict_key(data)
    if key is None:
        return False
    project = context.project
    clock = NativeClock()
    try:
        with lock(project, "registry"):
            write_verdict(
                project.state,
                project.project_id,
                key,
                shareable=False,
                reason=f"{verdict.rule}: {verdict.reason}",
                run_id=data["run_id"],
                origin=clock.origin,
                tick_ns=clock.read(),
            )
    except (OSError, ResourceError):
        return False
    return True


def _refuse_or_record(context: RunContext, data: dict, verdict: ShareVerdict) -> None:
    """Record a violation; then refuse it when the run is shared (design D2: the verdict is durable first)."""
    if verdict.shareable:
        return
    recorded = _record_negative_verdict(context, data, verdict)
    if not recorded:
        _mark_share_unknown(data)
    failpoint("share-verdict-written")
    if _record_mode(data) == AdmissionMode.SHARED:
        note = "" if recorded else " The verdict was not recorded."
        raise InvalidInput(f"Shared run breaks share rule {verdict.rule}: {verdict.reason}.{note}", reason="share-rule")


SHARE_UNKNOWN_MARKER = "share-unknown"
_UNEVALUABLE = (
    ResourceError,
    InfrastructureFailure,
    InvalidInput,
    OSError,
    LookupError,
    TypeError,
    ValueError,
    AttributeError,
)


def _mark_share_unknown(data: dict) -> None:
    """Leave a marker in the run's private root: a violation went unrecorded or the rules could not be evaluated, so
    the leave acknowledgement teaches no positive verdict (best effort: a failed marker write is the only residual)."""
    try:
        (Path(data["private_root"]) / SHARE_UNKNOWN_MARKER).touch(mode=0o600)
    except OSError:
        pass


def _bindable(facts: ComposeShareFacts) -> bool:
    """Whether a Compose command's inputs can be bound (R15): every persistent option understood, explicit files, no
    stdin, and no relative input under ``--project-directory`` (which file Compose then reads is not this process's cwd
    to decide)."""
    relative = any(not os.path.isabs(path) for path in (*facts.files, *facts.env_files))
    return (
        facts.understood
        and bool(facts.files)
        and not facts.stdin_file
        and not (facts.project_directory is not None and relative)
    )


def _compose_input_files(argv: tuple[str, ...]) -> list[dict] | None:
    """Hash a shared run's Compose ``--file`` and ``--env-file`` inputs before the render (C8).

    Each path is kept as the command names it, joined to this process's cwd when relative but neither normalized nor
    resolved (``link/../x`` stays a lookup through the link), so the checks after the render and before execution read
    what Compose reads, a retargeted symlink included.
    """
    facts = compose_share_facts(argv)
    if not _bindable(facts):
        return None  # the rules refuse it as R15 and record the verdict
    described = []
    try:
        for supplied in (*facts.files, *facts.env_files):
            path = Path(supplied if os.path.isabs(supplied) else os.path.join(os.getcwd(), supplied))  # not normalized
            if not path.is_file():
                raise InvalidInput("A Compose input is not a regular file.", reason="share-rule")
            described.append({"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    except OSError as error:
        raise InvalidInput("A Compose input file is unreadable.", reason="share-rule") from error
    return described


def _binds_compose_inputs(data: dict, command: ClassifiedDockerCommand) -> bool:
    """Whether a command's Compose inputs are bound: a shared run's creating or starting Compose command."""
    return (
        _record_mode(data) == AdmissionMode.SHARED
        and command.kind in {"compose", "compose-run"}
        and checked_mutation(command.kind, command.plan.get("subcommand"))
    )


def _enforce_share_rules(
    context: RunContext,
    data: dict,
    adapter,
    command: ClassifiedDockerCommand,
    argv: tuple[str, ...],
    config: dict | None,
    bound_inputs: list[dict] | None,
) -> ClassifiedDockerCommand:
    """Evaluate the share rules on one creating or starting command before its receipt (design 2.3, N6).

    Every run records a violation as a negative verdict; only a shared run refuses (``share-rule``), and a shared run
    whose rules cannot be evaluated refuses too. An exclusive run never fails here. ``bound_inputs`` (a shared Compose
    command's inputs, hashed before its render) enter the plan and are checked again just before execution.
    """
    if not _evaluates_share_rules(command):
        return command
    shared = _record_mode(data) == AdmissionMode.SHARED
    try:
        verdict = _share_verdict(context, data, adapter, command, argv, config or {})
    except _UNEVALUABLE as error:
        # Exclusive runs never fail because of a share rule (design D2): an unevaluable rule (unreadable registry or
        # engine answer, malformed render) marks the run unknown, so it teaches nothing. A shared run refuses instead.
        if shared:
            raise InvalidInput("Shared run's share rules could not be evaluated.", reason="share-rule") from error
        _mark_share_unknown(data)
        return command
    _refuse_or_record(context, data, verdict)
    if bound_inputs is None:
        return command
    return replace(command, plan={**command.plan, "files": bound_inputs})


def _receipt_daemon_platform(adapter) -> Platform | None:
    """Read the daemon's platform once for a receipt (design 4.1); ``None`` when it cannot be known.

    Platform recording never refuses a mutation: a late, failed or unmapped read is only an unknown platform. The
    read goes through the observation-read seam, bounded by its own short deadline.
    """
    failpoint("receipt-daemon-platform-read")
    try:
        body = _engine_read(adapter, "/info", Deadline.after(NativeClock(), 5)).body
    except (ResourceError, OSError):
        return None
    if not isinstance(body, dict):
        return None
    return daemon_platform(body.get("OSType"), body.get("Architecture"))


def _local_daemon(data: dict) -> bool:
    """Whether the run's daemon is local, the only case where an emulation verdict can be given."""
    return data["topology"] in ("desktop", "native")


def _platform_dict(platform: Platform | None) -> dict[str, str] | None:
    return None if platform is None else platform.as_dict()


def _record_request_platform(adapter, command: ClassifiedDockerCommand, data: dict) -> None:
    """Add the daemon's platform and the requested build platform to the receipt plan; never refuses the build.

    A ``--platform`` without a value is left to Docker to refuse, so recording reads it as no request.
    """
    daemon = _receipt_daemon_platform(adapter)
    try:
        values = requested_platform_values(command.argv)
    except InvalidInput:
        values = []
    raw = ",".join(values) if values else None
    requested = parse_requested_platform(raw) if raw is not None and len(values) == 1 else None
    command.plan["daemon_platform"] = _platform_dict(daemon)
    command.plan["platform"] = {
        "requested": _platform_dict(requested),
        "requested_raw": raw,
        "requested_emulated": classify_emulation(daemon, requested, _local_daemon(data)),
    }


def _compose_platform_facts(
    adapter, observed: Mapping[str, tuple[Platform | None, Platform | None]], data: dict
) -> dict[str, object]:
    """Return the receipt plan's daemon platform and, per inspected service, its requested and image platform.

    The image platform wins over the request because it is what executes; recording never refuses the mutation.
    """
    daemon = _receipt_daemon_platform(adapter)
    local = _local_daemon(data)
    return {
        "daemon_platform": _platform_dict(daemon),
        "platforms": {
            service: {
                "requested": _platform_dict(requested),
                "image": _platform_dict(image),
                "emulated": classify_emulation(daemon, image or requested, local),
            }
            for service, (requested, image) in observed.items()
        },
    }


def _prepare_build_receipt(adapter, command: ClassifiedDockerCommand, data: dict) -> None:
    tags = command.plan["tags"]
    if not isinstance(tags, list) or not tags:
        raise InvalidInput("Build receipt lacks exact output tags.")
    owned = data["owned_resources"]["images"]
    for tag in tags:
        observation = adapter.inspect_exact(ExactInspection("image", tag))
        if observation.exists:
            image_id = observation.attributes.get("Id")
            if not isinstance(image_id, str) or image_id not in owned:
                raise InvalidInput("Build output tag collides with an unowned image.")
    _reserve_build_metadata(command, data)
    _record_request_platform(adapter, command, data)


def _reserve_build_metadata(command: ClassifiedDockerCommand, data: dict) -> None:
    """Create the private, empty Buildx metadata file the receipt names, refusing any pre-existing entry."""
    metadata_value = command.plan["metadata"]
    if not isinstance(metadata_value, str):
        raise InvalidInput("Build metadata receipt path is invalid.")
    metadata = Path(metadata_value)
    if metadata.parent != Path(data["private_root"]) or metadata.exists() or metadata.is_symlink():
        raise InvalidInput("Build metadata receipt path is not fresh and private.")
    try:
        descriptor = os.open(metadata, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        os.close(descriptor)
    except OSError as error:
        raise ResourceError("Build metadata receipt could not be reserved.") from error


def _read_build_metadata(path: str) -> tuple[dict, bytes] | None:
    """Read Buildx's metadata file as a bounded private regular file.

    Returns ``None`` when the file is absent, not a regular account-owned file, empty, oversized or not JSON. Any other
    ``OSError`` (an I/O failure, a symlink) propagates so a caller can tell an operational failure from a missing file.
    """
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        return None
    try:
        observed = os.fstat(descriptor)
        if not stat.S_ISREG(observed.st_mode) or observed.st_uid != os.getuid():
            return None
        os.fchmod(descriptor, 0o600)
        observed = os.fstat(descriptor)
        with open(descriptor, "rb", closefd=False) as stream:
            metadata_bytes = stream.read(image_archive.METADATA_MEMBER_MAX + 1)
    finally:
        os.close(descriptor)
    if (
        observed.st_mode & 0o777 != 0o600
        or not metadata_bytes
        or len(metadata_bytes) > image_archive.METADATA_MEMBER_MAX
    ):
        return None
    try:
        record = json.loads(metadata_bytes, object_pairs_hook=unique_object)
    except (UnicodeDecodeError, ValueError):
        return None
    return (record if isinstance(record, dict) else {}), metadata_bytes


def _observe_buildkit_worker(adapter, builder: dict, receipt_id: str) -> list[dict] | None:
    """Return the BuildKit worker resource a build promotes (none for other drivers), or ``None`` when ambiguous."""
    if builder["evidence"].get("driver") != "docker-container":
        return []
    name = buildkit_worker(builder["name"])
    observation = adapter.inspect_exact(ExactInspection("container", name))
    worker = _buildkit_resource(observation.attributes, name, builder["name"], receipt_id)
    return [worker] if observation.exists and worker is not None else None


def _present_built_image(data: dict, image_id: object) -> dict | None:
    """Return the run's present owned image of ``image_id`` that an earlier build recorded, else ``None``."""
    image = data["owned_resources"]["images"].get(image_id) if isinstance(image_id, str) else None
    evidence = image["evidence"] if image is not None else {}
    if image is None or image["state"] != "present" or not {"metadata_sha256", "repo_tags", "tags"} <= evidence.keys():
        return None
    return image


def _images_loaded_by_failed_build(adapter, receipt: dict, labels: dict[str, str], data: dict) -> list[dict]:
    """Return the images a failed build still loaded under its tags, as owned resources, so exact cleanup removes them.

    A Buildx client killed after or during ``--load`` exits non-zero although the daemon holds the image. Only an image
    carrying exactly this run's labels (they include the run ID) is claimed; a foreign or already owned one is not.
    """
    claimed: dict[str, dict] = {}
    for tag in receipt["plan"]["tags"]:
        item = adapter.inspect_exact(ExactInspection("image", tag)).attributes
        config = item.get("Config")
        image_labels = config.get("Labels") if isinstance(config, dict) else None
        image_id, repo_tags, repo_digests = item.get("Id"), item.get("RepoTags") or [], item.get("RepoDigests") or []
        if (
            not isinstance(image_id, str)
            or not IMAGE.fullmatch(image_id)
            or not isinstance(image_labels, dict)
            or any(image_labels.get(key) != value for key, value in labels.items())
            or image_id in data["owned_resources"]["images"]
            or not isinstance(repo_tags, list)
            or not isinstance(repo_digests, list)
            or any(not isinstance(value, str) for value in (*repo_tags, *repo_digests))
        ):
            continue
        platform = Platform.from_fields(item.get("Os"), item.get("Architecture"), item.get("Variant"))
        claimed[image_id] = _owned_resource(
            "image",
            image_id,
            image_id,
            receipt["id"],
            "present",
            {
                "created": item.get("Created"),
                "labels": labels,
                "platform": _image_platform_facts(receipt, platform, data),
                "repo_digests": sorted(repo_digests),
                "repo_tags": sorted(repo_tags),
                "tags": receipt["plan"]["tags"],
            },
        )
    return list(claimed.values())


def _build_promotion(
    adapter, receipt: dict, result: DockerResult, labels: dict[str, str], data: dict
) -> tuple[str, list[dict]]:
    if result.exit_code:
        return "failed", _images_loaded_by_failed_build(adapter, receipt, labels, data)
    try:
        loaded = _read_build_metadata(receipt["plan"]["metadata"])
    except OSError:
        return "unresolved", []
    if loaded is None:
        return "unresolved", []
    metadata_record, metadata_bytes = loaded
    metadata_digest = metadata_record.get("containerimage.digest")
    # On Podman the image ID is the config digest and its repository digests carry a digest of their own (measured
    # 2026-11-04): the build binds through the config digest, the repository digests through their repositories.
    podman = adapter.engine == "podman"
    config_digest = metadata_record.get("containerimage.config.digest") if podman else None
    tag_identities = [_build_tag_identity(tag) for tag in receipt["plan"]["tags"]]
    if (
        not isinstance(metadata_digest, str)
        or not IMAGE.fullmatch(metadata_digest)
        or None in tag_identities
        or (podman and (not isinstance(config_digest, str) or not IMAGE.fullmatch(config_digest)))
    ):
        return "unresolved", []
    expected_tags = {identity[0] for identity in tag_identities if identity is not None}
    expected_repo_digests = {f"{identity[1]}@{metadata_digest}" for identity in tag_identities if identity is not None}
    images = []
    image_platform: Platform | None = None
    for tag in receipt["plan"]["tags"]:
        inspection = adapter.inspect_exact(ExactInspection("image", tag))
        item = inspection.attributes
        config = item.get("Config")
        image_labels = config.get("Labels") if isinstance(config, dict) else None
        image_id = item.get("Id")
        created = item.get("Created")
        repo_tags = item.get("RepoTags", [])
        repo_digests = item.get("RepoDigests")
        if repo_digests is None:
            repo_digests = []
        # An identical rebuild in this run resolves to an image the run already owns, which keeps its earlier tags.
        prior = _present_built_image(data, image_id)
        prior_tags = set(prior["evidence"]["repo_tags"]) if prior is not None else set()
        prior_digests = set(prior["evidence"]["repo_digests"]) if prior is not None else set()
        if (
            not inspection.exists
            or not isinstance(image_id, str)
            or not IMAGE.fullmatch(image_id)
            or not isinstance(image_labels, dict)
            or any(image_labels.get(key) != value for key, value in labels.items())
            or (created is not None and (not isinstance(created, str) or not created))
            or not isinstance(repo_tags, list)
            or any(not isinstance(value, str) for value in repo_tags)
            or not isinstance(repo_digests, list)
            or any(not isinstance(value, str) for value in repo_digests)
            or not expected_tags <= set(repo_tags) <= expected_tags | prior_tags
            or not (
                {digest.rpartition("@")[0] for digest in repo_digests}
                <= {digest.rpartition("@")[0] for digest in expected_repo_digests | prior_digests}
                if podman
                else set(repo_digests).issubset(expected_repo_digests | prior_digests)
            )
            or (podman and image_id != config_digest)
        ):
            return "unresolved", []
        images.append((image_id, created, tuple(sorted(repo_tags)), tuple(sorted(repo_digests)), _image_size(item)))
        image_platform = image_platform or Platform.from_fields(
            item.get("Os"), item.get("Architecture"), item.get("Variant")
        )
    if any(value != images[0] for value in images[1:]):
        return "unresolved", []
    image_id, created, repo_tags, repo_digests, size_bytes = images[0]
    prior = _present_built_image(data, image_id)
    resources = [
        _owned_resource(
            "image",
            image_id,
            image_id,
            receipt["id"],
            "present",
            {
                "created": created,
                "labels": labels,
                "metadata_sha256": hashlib.sha256(metadata_bytes).hexdigest(),
                "platform": _image_platform_facts(receipt, image_platform, data),
                "repo_digests": list(repo_digests),
                "repo_tags": list(repo_tags),
                "tags": list(dict.fromkeys([*(prior["evidence"]["tags"] if prior else []), *receipt["plan"]["tags"]])),
                "size_bytes": size_bytes,
            },
        )
    ]
    workers = _observe_buildkit_worker(
        adapter, data["owned_resources"]["builders"][receipt["plan"]["builder"]], receipt["id"]
    )
    if workers is None:
        return "unresolved", []
    return "promoted", [*resources, *workers]


def _platform_from_dict(record: dict | None) -> Platform | None:
    """Rebuild a recorded platform (the closed ``as_dict`` form); ``None`` for no record."""
    if record is None:
        return None
    return Platform.from_fields(record.get("os"), record.get("architecture"), record.get("variant"))


def _image_platform_facts(receipt: dict, image: Platform | None, data: dict) -> dict:
    """Return the promoted image's platform and whether it runs natively on the daemon (design 4.1).

    The image platform wins over the request because it is what executes; a malformed or absent image platform falls
    back to the request, and an unknown daemon makes the answer ``unknown``.
    """
    plan = receipt["plan"]
    daemon = _platform_from_dict(plan.get("daemon_platform"))
    requested = _platform_from_dict((plan.get("platform") or {}).get("requested"))
    local = _local_daemon(data)
    return {"image": _platform_dict(image), "emulated": classify_emulation(daemon, image or requested, local)}


def _build_tag_identity(reference: str) -> tuple[str, str] | None:
    """Return Docker's local tag spelling and its repository."""
    if "@" in reference:
        return None
    separator = reference.rfind(":")
    if separator <= reference.rfind("/"):
        reference += ":latest"
        separator = reference.rfind(":")
    return reference, reference[:separator]


def _repository_digest(reference: str) -> str:
    """Return Docker's canonical tag-free repository digest spelling."""
    repository, separator, digest = reference.rpartition("@")
    if not separator:
        return reference
    tag = repository.rfind(":")
    if tag > repository.rfind("/"):
        repository = repository[:tag]
    return f"{repository}@{digest}"


def _image_pull_promotion(
    adapter, receipt: dict, result: DockerResult, *, by_presence: bool = False
) -> tuple[str, list[dict]]:
    """Promote a pull's image from its exact reference.

    ``by_presence`` (a shared run, decided under K): a failed or unknown client exit still promotes the image when it
    is present, because under K it was absent at dispatch and no other run could pull it since, so it is this pull's
    own output, owned and later deleted instead of leaked (design 2.8 re-reconcile, C11).
    """
    reference = receipt["plan"]["reference"]
    repository_digest = _repository_digest(reference)
    if result.exit_code and not by_presence:
        return "failed", []
    observation = adapter.inspect_exact(ExactInspection("image", reference))
    item = observation.attributes
    image_id = item.get("Id")
    created = item.get("Created")
    repo_digests = item.get("RepoDigests")
    if (
        not observation.exists
        or not isinstance(image_id, str)
        or not IMAGE.fullmatch(image_id)
        or (created is not None and (not isinstance(created, str) or not created))
        or not isinstance(repo_digests, list)
        or repository_digest not in repo_digests
        or any(not isinstance(value, str) for value in repo_digests)
    ):
        return ("failed" if result.exit_code else "unresolved"), []
    return "promoted", [
        _owned_resource(
            "image",
            image_id,
            reference,
            receipt["id"],
            "present",
            {"created": created, "repository_digest": repository_digest, "size_bytes": _image_size(item)},
        )
    ]


_BUILD_KINDS = frozenset({"build"})
_ARCHIVE_KINDS = frozenset({"image-save"})
# A pending build or export is a finite external workload, so host pressure stays sampled while it runs.
_OBSERVABLE_RECEIPT_KINDS = _BUILD_KINDS | {"image-save"}
_PROMOTABLE_RECEIPT_KINDS = frozenset(
    {
        "container-create",
        "container-rm",
        "container-start",
        "container-stop",
        "image-rm",
        "network-rm",
        "volume-rm",
        "builder-rm",
        "network-create",
        "volume-create",
        "compose",
        "compose-run",
        "builder-create",
        "build",
        "image-save",
        "image-pull",
        "network-connect",
    }
)
_RECONCILABLE_OPERATION_KINDS = frozenset(
    {"compose-up", "compose-observe", "compose-mutate", "container-control", "cleanup", "retain-stop"}
)


def _promote_docker_result(adapter, receipt: dict, result: DockerResult, data: dict) -> tuple[str, list[dict]]:
    if receipt["kind"] not in _PROMOTABLE_RECEIPT_KINDS:
        raise ResourceError("Docker receipt kind cannot be promoted.", reason="unsupported-receipt-kind")
    labels = {PROJECT: data["project_id"], SESSION: data["session_id"], RUN: data["run_id"]}
    if receipt["kind"] == "container-create":
        return _container_promotion(adapter, receipt, result, labels)
    if receipt["kind"] in {"container-rm", "container-start", "container-stop"}:
        return _exact_control_promotion(adapter, receipt, result)
    if receipt["kind"] in {"image-rm", "network-rm", "volume-rm"}:
        return _exact_resource_removal_promotion(adapter, receipt)
    if receipt["kind"] == "builder-rm":
        state, resources = _exact_resource_removal_promotion(adapter, receipt)
        if state != "removed":
            return state, resources
        builder = data["owned_resources"]["builders"][receipt["plan"]["builder_id"]]
        container_id = builder["evidence"].get("buildkit_container_id")
        if isinstance(container_id, str):
            worker = data["owned_resources"]["containers"][container_id]
            observed = adapter.inspect_exact(ExactInspection("container", container_id))
            container_state = observed.attributes.get("State") if observed.exists else None
            if not observed.exists:
                resources.append({**worker, "state": "removed"})
            elif isinstance(container_state, dict) and container_state.get("Running") is False:
                # Native Linux: ``buildx rm`` stops the worker and the daemon removes it later. A stopped worker is no
                # longer observed as running; exact cleanup removes it if it still exists at leave.
                resources.append({**worker, "state": "stopped"})
        return state, resources
    if receipt["kind"] in {"network-create", "volume-create"}:
        return _direct_resource_promotion(adapter, receipt, result, labels)
    if receipt["kind"] in {"compose", "compose-run"}:
        return _compose_promotion(adapter, receipt, result, data)
    if receipt["kind"] == "builder-create":
        return _builder_promotion(adapter, receipt, result)
    if receipt["kind"] == "build":
        if data["owned_resources"]["builders"].get(receipt["plan"]["builder"]) is None:
            return "unresolved", []
        return _build_promotion(adapter, receipt, result, labels, data)
    if receipt["kind"] == "image-pull":
        return _image_pull_promotion(adapter, receipt, result, by_presence=_record_mode(data) == AdmissionMode.SHARED)
    if receipt["kind"] == "network-connect":
        return ("promoted" if result.exit_code == 0 else "failed"), []
    raise ResourceError("Docker receipt kind cannot be promoted.")


def _finish_docker_receipt(
    context: RunContext,
    receipt: dict,
    state: str,
    result: DockerResult,
    resources: list[dict],
    deadline_ns: int | None = None,
    *,
    archive: dict | None = None,
    cache: dict | None = None,
    precheck: Callable[[], None] | None = None,
) -> None:
    """Publish one receipt result under the run lock; the receipt must still equal the pending one.

    ``archive`` is the validated ``plan["archive"]`` record of an export receipt, written in the same locked write.
    ``cache`` is a build's cache outcome; the finish tick is added to it here (design 3.2, 3.9).
    ``precheck`` runs under the lock after the pending check and may refuse the finish.
    """
    with _locked_run(context, deadline_ns):
        data = _load_run(context)
        _finish_receipt_locked(
            context, data, receipt, state, result, resources, archive=archive, cache=cache, precheck=precheck
        )


def _finish_receipt_locked(
    context: RunContext,
    data: dict,
    receipt: dict,
    state: str,
    result: DockerResult,
    resources: list[dict],
    *,
    archive: dict | None = None,
    cache: dict | None = None,
    precheck: Callable[[], None] | None = None,
) -> None:
    """Apply one receipt result to a freshly loaded record while the caller holds the run lock."""
    current = data["docker_receipts"].get(receipt["id"])
    if current != receipt or current["state"] not in {"pending", "unresolved"}:
        raise _record_changed()
    if precheck is not None:
        precheck()
    if archive is not None:
        image_archive.validate_archive_record(
            archive,
            final_path=image_archive.archive_paths(data["private_root"], receipt["id"]).final,
            image_id=receipt["plan"].get("image_id"),
        )
        current["plan"]["archive"] = archive
    if cache is not None:  # after the pending check: the in-memory pending receipt is never mutated (DD8)
        current["plan"]["cache"] = {**cache, "finished_ns": NativeClock().read()}
    for resource in resources:
        plural = "builders" if resource["kind"] == "builder" else f"{resource['kind']}s"
        builder_name = resource["evidence"].get("builder")
        if resource["kind"] == "container" and isinstance(builder_name, str):
            builder = data["owned_resources"]["builders"].get(builder_name)
            if builder is None or builder["evidence"].get("buildkit_container_id") not in {
                None,
                resource["identifier"],
            }:
                raise ResourceError("BuildKit worker promotion contradicts its owned builder.")
            builder["evidence"]["buildkit_container_id"] = resource["identifier"]
        existing = data["owned_resources"][plural].get(resource["identifier"])
        if existing is not None:
            replaced = (
                existing["state"] == "removed"
                and resource["kind"] in {"builder", "volume", "image"}
                and existing["kind"] == resource["kind"]
                and existing["name"] == resource["name"]
            )
            same_identity = existing["kind"] == resource["kind"] and existing["name"] == resource["name"]
            # A rebuild of identical content adds its tags to the image an earlier build of this run promoted.
            image_rebuild = (
                receipt["kind"] == "build"
                and same_identity
                and resource["kind"] == "image"
                and existing["state"] == "present"
                and all(
                    existing["evidence"].get(key) == resource["evidence"].get(key)
                    for key in ("created", "labels", "platform")
                )
                and set(existing["evidence"].get("repo_tags", [])) <= set(resource["evidence"]["repo_tags"])
            )
            compose_container_update = (
                receipt["kind"] == "compose"
                and same_identity
                and resource["kind"] == "container"
                and all(
                    existing["evidence"].get(key) == resource["evidence"].get(key)
                    for key in (
                        "compose_project",
                        "config_sha256",
                        "created",
                        "image_id",
                        "service",
                        "completed_service",
                    )
                )
            )
            if not replaced and not (
                same_identity
                and (existing["evidence"] == resource["evidence"] or compose_container_update or image_rebuild)
            ):
                raise ResourceError("Docker promotion collides with another exact owned identity.")
            if not replaced:
                resource = {**resource, "receipt_id": existing["receipt_id"]}
        data["owned_resources"][plural][resource["identifier"]] = resource
        if receipt["kind"] == "image-pull" and resource["kind"] == "image":
            data["image_bindings"][receipt["plan"]["reference"]] = {
                "created": resource["evidence"]["created"],
                "identifier": resource["identifier"],
                "ownership": "owned",
            }
    if receipt["kind"] == "container-rm" and state == "removed":
        container_id = receipt["plan"]["container_id"]
        data["owned_resources"]["containers"][container_id]["state"] = "removed"
        for history in data["endpoints"].values():
            for endpoint in history:
                if endpoint["active"] and endpoint["container_id"] == container_id:
                    endpoint.update(active=False, retired_revision=data["revision"] + 1)
    elif receipt["kind"] in {"builder-rm", "image-rm", "network-rm", "volume-rm"} and state == "removed":
        kind = receipt["kind"].removesuffix("-rm")
        plural = "builders" if kind == "builder" else f"{kind}s"
        data["owned_resources"][plural][receipt["plan"][f"{kind}_id"]]["state"] = "removed"
    elif receipt["kind"] in {"container-start", "container-stop"} and state == "promoted":
        container_id = receipt["plan"]["container_id"]
        data["owned_resources"]["containers"][container_id]["state"] = (
            "running" if receipt["kind"] == "container-start" else "stopped"
        )
        if receipt["kind"] == "container-stop":
            for history in data["endpoints"].values():
                for endpoint in history:
                    if endpoint["active"] and endpoint["container_id"] == container_id:
                        endpoint.update(active=False, retired_revision=data["revision"] + 1)
    elif receipt["kind"] == "compose" and state == "removed":
        for items in data["owned_resources"].values():
            for resource in items.values():
                if resource["evidence"].get("compose_project") == receipt["plan"]["project"]:
                    resource["state"] = "removed"
    if receipt["kind"] == "compose" and state in {"promoted", "removed"}:
        _retire_endpoints(data["endpoints"], data["revision"] + 1)
    current["state"] = state
    current["result"] = {
        "category": "complete" if result.exit_code == 0 else "child_failure",
        "exit_code": result.exit_code,
    }
    current["resources"] = [item["identifier"] for item in resources]
    data["revision"] += 1
    failpoint("finish-before-replace")
    atomic_json(context.record, data)
    failpoint("finish-record-replaced")


def _bind_existing_image(
    context: RunContext,
    descriptor: dict,
    adapter,
    command: ClassifiedDockerCommand,
    image_lock_wait: tuple[str, int, int] | None = None,
) -> DockerResult | None:
    """Bind an already-cached digest-pinned image as external without claiming it.

    A shared run's wait for the image lock is recorded in the same write (no receipt is appended on this path).
    """
    reference = command.plan["reference"]
    if not isinstance(reference, str):
        raise InvalidInput("Image acquisition reference is invalid.")
    repository_digest = _repository_digest(reference)
    project = context.project
    with lock(project, project.project_id, shared=True), lock(project, f"run-{context.run_id}"):
        data = _load_run(context)
        authority = data["docker_authority"]
        if (
            data["state"] != "active"
            or authority is None
            or descriptor["generation"] != data["generation"]
            or authority["descriptor_sha256"]
            != hashlib.sha256(Path(authority["descriptor_path"]).read_bytes()).hexdigest()
        ):
            raise ResourceError("Docker dispatcher authority changed before image acquisition.")
        observation = adapter.inspect_exact(ExactInspection("image", reference))
        if not observation.exists:
            return None
        item = observation.attributes
        image_id = item.get("Id")
        created = item.get("Created")
        repo_digests = item.get("RepoDigests")
        if (
            not isinstance(image_id, str)
            or not IMAGE.fullmatch(image_id)
            or (created is not None and (not isinstance(created, str) or not created))
            or not isinstance(repo_digests, list)
            or repository_digest not in repo_digests
            or any(not isinstance(value, str) for value in repo_digests)
        ):
            raise InvalidInput("Cached image does not match the requested repository digest.")
        binding = data["image_bindings"].get(reference)
        expected = {"created": created, "identifier": image_id, "ownership": "external"}
        if binding is not None and (
            binding["identifier"] != image_id
            or binding["ownership"] != "external"
            or (binding["created"] is not None and created is not None and binding["created"] != created)
        ):
            raise ResourceError("Cached image binding changed within the active run.")
        waited = image_lock_wait is not None and _record_image_lock_wait(data, image_lock_wait)
        if binding is None or waited:
            if binding is None:
                data["image_bindings"][reference] = expected
            data["revision"] += 1
            atomic_json(context.record, data)
        return DockerResult(0, "", "")


def _dispatch_seconds(command: ClassifiedDockerCommand) -> int:
    """Return one dispatched mutation's deadline; ``compose up|start --wait-timeout`` extends the 30 s budget."""
    if command.blocking:
        return _BLOCKING_DOCKER_SECONDS
    if command.kind in {"build", "image-pull"}:
        return 1200
    wait_seconds = command.plan.get("wait_seconds", 0)
    return 30 + (wait_seconds if isinstance(wait_seconds, int) else 0)


_CACHE_FAILURES = (OSError, ValueError, ResourceError, InvalidInput, RecursionError)


def _cache_fail_open(function: Callable[..., dict], plan: dict, *arguments: object, **keywords: object) -> dict:
    """Run one cache outcome step; a cache failure becomes ``skipped:error`` and never fails the build (I5)."""
    try:
        return function(plan, *arguments, **keywords)
    except _CACHE_FAILURES:
        return build_cache.error_outcome(plan)  # only indexes the plan and removes staging best effort


def _plan_cache_fail_open(
    context: RunContext,
    data: dict,
    command: ClassifiedDockerCommand,
    caller_argv: tuple[str, ...],
    operation_id: str,
    test_seams: "_ArchiveSeams",
) -> tuple[ClassifiedDockerCommand, ExitStack, dict]:
    """Plan one loaded build's cache; any cache failure runs the build cold with ``skipped:error`` (I5, X28)."""
    try:
        return build_cache.plan_build_cache(
            context.project.state,
            data,
            command,
            caller_argv,
            operation_id=operation_id,
            environment=os.environ,
            available_bytes=test_seams.available_bytes,
        )
    except _CACHE_FAILURES:
        return command, ExitStack(), build_cache.skipped("error")


def _close_cache_fail_open(held: ExitStack) -> bool:
    """Release the build's shared cache anchor; return ``False`` when the release reported a cache failure."""
    try:
        held.close()
    except _CACHE_FAILURES:
        return False
    return True


def _failed_build_cache(plan: dict | None) -> dict | None:
    return None if plan is None else _cache_fail_open(build_cache.failure_outcome, plan)


def _complete_cache_fail_open(
    plan: dict, state: str, resources: list[dict], released: bool, run_id: str, operation_id: str
) -> dict:
    """Complete one finished loaded build's cache: publish a promoted native export, else remove it (design 3.8)."""
    if not released:
        return _cache_fail_open(build_cache.error_outcome, plan)
    images = [resource for resource in resources if resource["kind"] == "image"]
    native = all(image["evidence"].get("platform", {}).get("emulated") == "native" for image in images)
    return _cache_fail_open(
        build_cache.complete_build_cache,
        plan,
        promoted=state == "promoted",
        native=native,
        run_id=run_id,
        operation_id=operation_id,
    )


def dispatch_docker(
    context: RunContext,
    descriptor: dict,
    adapter,
    argv: tuple[str, ...],
    *,
    _available_bytes: Callable[[str], int] | None = None,
    _remove_tree: Callable[[int, str, int], None] | None = None,
    _receipt_timeout_seconds: float | None = None,
) -> DockerResult:
    """Classify, journal, execute, and exactly promote one Docker-fronted command.

    The keyword-only underscore parameters are test seams: the free-space probe (archive exports and the build-cache
    export precheck), the staging-directory removal and the receipt deadline. Production callers omit them.
    """
    test_seams = _ArchiveSeams(_available_bytes, _remove_tree, _receipt_timeout_seconds)
    data = _load_run(context)
    authority = data["docker_authority"]
    if (
        authority is None
        or authority["descriptor_sha256"] != hashlib.sha256(Path(authority["descriptor_path"]).read_bytes()).hexdigest()
    ):
        raise ResourceError("Docker dispatcher descriptor is no longer authoritative.")
    operation_id = str(uuid.uuid4())
    command = classify_command(
        argv,
        operation_id=operation_id,
        run_id=data["run_id"],
        compose_project=authority["compose_project"],
        builder=authority["builder"],
        labels={PROJECT: data["project_id"], SESSION: data["session_id"], RUN: data["run_id"]},
        private_root=data["private_root"],
        owned=data["owned_resources"],
        external=tuple(ExternalDockerResource(**item) for item in data["external_resources"]),
    )
    if command.kind == "builder-create" or command.kind in _BUILD_KINDS:
        _require_builder_phase(data)
    if command.kind in _ARCHIVE_KINDS:
        return _dispatch_archive(context, descriptor, adapter, command, data, operation_id, test_seams)
    if command.kind not in _IMAGE_LOCK_SECONDS or _record_mode(data) != AdmissionMode.SHARED:
        return _dispatch_classified(context, descriptor, adapter, argv, data, command, operation_id, test_seams)
    # A shared run's image operation holds the account image lock K from before its preparation until its receipt is
    # finished, and its Docker child inherits K (design 2.6); exclusive runs never take K for their own operations.
    clock = NativeClock()
    first = clock.read()
    account = Path(data["account_directory"])
    with image_lock(
        Context(account, account), deadline_ns=time.monotonic_ns() + _IMAGE_LOCK_SECONDS[command.kind] * 1_000_000_000
    ) as hold_fd:
        wait = (clock.origin, first, clock.read())
        return _dispatch_classified(
            context, descriptor, adapter, argv, data, command, operation_id, test_seams, hold_fd, wait
        )


# K waits within each image operation's own bound (design 2.6 table); expiry is ResourceBusy lock-deadline.
_IMAGE_LOCK_SECONDS = {"build": 1200, "image-pull": 1200, "builder-create": 30, "image-rm": 30}
IMAGE_LOCK_WAIT = "image-lock"
SHARED_GRANTS_UNREADABLE = "shared-grants-unreadable"


def _dispatch_classified(
    context: RunContext,
    descriptor: dict,
    adapter,
    argv: tuple[str, ...],
    data: dict,
    command: ClassifiedDockerCommand,
    operation_id: str,
    test_seams: "_ArchiveSeams",
    hold_fd: int | None = None,
    image_lock_wait: tuple[str, int, int] | None = None,
) -> DockerResult:
    """Prepare, journal, execute and promote one classified non-archive command (``dispatch_docker``'s body).

    ``hold_fd`` is the image lock a shared run's image operation holds; every Docker child launched here inherits it.
    """
    held, cache_plan = ExitStack(), None  # the build's shared cache anchor and planning object (design 3.7)
    config, bound_inputs = None, None
    if command.kind in {"compose", "compose-io", "compose-run"}:
        if _binds_compose_inputs(data, command):
            try:
                bound_inputs = _compose_input_files(argv)  # hashed before the render, verified after it (C8)
            except InvalidInput as error:  # a missing or unreadable input is an R15 violation: verdict first
                _refuse_or_record(context, data, ShareVerdict("R15", str(error).rstrip(".")))
                raise
        command, config = _prepare_compose_receipt(adapter, command, data)
        if bound_inputs is not None:
            _require_compose_files_unchanged(bound_inputs)  # the render read exactly the hashed inputs
    elif command.kind == "builder-create":
        command = _prepare_builder_receipt(adapter, command)
        _write_buildkit_config(command)
    elif command.kind == "build":
        _prepare_build_receipt(adapter, command, data)
        command, held, cache_plan = _plan_cache_fail_open(context, data, command, argv[2:], operation_id, test_seams)
    elif command.kind == "image-pull":
        existing = _bind_existing_image(context, descriptor, adapter, command, image_lock_wait)
        if existing is not None:
            return existing
    elif command.kind == "image-rm":
        removed_id = str(command.plan["image_id"])
        resource = data["owned_resources"]["images"][removed_id]
        if hold_fd is not None and _hand_off_if_in_use(
            context, data, removed_id, resource, None, image_lock_wait=image_lock_wait
        ):
            return DockerResult(0, "", "")  # a shared owner's rm of an image another run uses hands it off (2.7)
        argv, plan = _image_removal(resource)
        command = ClassifiedDockerCommand(command.kind, argv, {**command.plan, **plan})
    command = _enforce_share_rules(context, data, adapter, command, argv, config, bound_inputs)
    failpoint("share-rules-evaluated")
    if not command.mutating:
        _authorize_observation(command, data)
        return adapter.execute(
            command.argv,
            timeout_seconds=_BLOCKING_DOCKER_SECONDS
            if command.blocking
            else 1200
            if command.kind == "compose-io"
            else 30,
            stream_output=command.kind == "compose-io",
        )
    seconds = _dispatch_seconds(command)
    deadline = Deadline.after(NativeClock(), seconds)
    appended = False
    try:
        receipt = _append_docker_receipt(
            context, descriptor, operation_id, command, deadline, image_lock_wait=image_lock_wait
        )
        appended = True
    finally:
        if not appended:
            _close_cache_fail_open(held)
            _failed_build_cache(cache_plan)
    timeout = max(0.001, deadline.remaining_ns(NativeClock()) / 1_000_000_000)
    child_started = False
    released = True
    result: DockerResult | None = None
    unresolved_cause = "its result could not be reconciled"
    try:
        if _record_mode(data) == AdmissionMode.SHARED and _evaluates_share_rules(command):
            # Judged again under this run's own pending receipt, which no other dispatcher of the run can pass: a
            # sibling that started containers since the first check counts now (R12 is not stale).
            try:
                recheck = _share_verdict(context, data, adapter, command, argv, config or {})
            except _UNEVALUABLE as error:
                raise InvalidInput("Shared run's share rules could not be evaluated.", reason="share-rule") from error
            _refuse_or_record(context, data, recheck)
        if "files" in command.plan:
            failpoint("compose-files-hashed")
            _require_compose_files_unchanged(command.plan["files"])
        if command.kind == "network-connect":
            network_id = command.plan["network_id"]
            if not isinstance(network_id, str):
                raise InvalidInput("Declared external network identity is invalid.")
            network = adapter.inspect_exact(ExactInspection("network", network_id))
            container = data["owned_resources"]["containers"][command.plan["container_id"]]
            if not network.exists or network.attributes.get("Id") != network_id:
                raise InvalidInput("Declared external network identity changed before mutation.")
            if not _validate_owned_identity(adapter, container):
                raise InvalidInput("Owned container disappeared before network attachment.")
            child_started = True
            result = adapter.execute(command.argv, timeout_seconds=timeout, hold_fd=hold_fd)
        elif command.kind == "container-create":
            image_id = command.plan["image_id"]
            if not isinstance(image_id, str):
                raise InvalidInput("Container image identity is invalid.")
            network_id = command.plan.get("network_id")
            if network_id is not None:
                if not isinstance(network_id, str):
                    raise InvalidInput("Container network identity is invalid.")
                network = adapter.inspect_exact(ExactInspection("network", network_id))
                if not network.exists or network.attributes.get("Id") != network_id:
                    raise InvalidInput("Declared external network identity changed before mutation.")
            owned_image = data["owned_resources"]["images"].get(image_id)
            if owned_image is not None:
                if not _validate_owned_identity(adapter, owned_image):
                    raise InvalidInput("Owned image disappeared before container creation.")
            else:
                image = adapter.inspect_exact(ExactInspection("image", image_id))
                if not image.exists or image.attributes.get("Id") != image_id:
                    raise InvalidInput("Declared external image identity changed before mutation.")
            child_started = True
            result = adapter.execute(
                command.argv, timeout_seconds=timeout, stream_output=command.blocking, hold_fd=hold_fd
            )
        elif command.kind == "build":
            try:  # the shared cache anchor is held until the build child returns (lever AC5)
                builder_resource = data["owned_resources"]["builders"].get(command.plan["builder"])
                if builder_resource is None or not _validate_owned_identity(adapter, builder_resource):
                    raise InvalidInput("Build requires the exact live owned builder.")
                child_started = True
                result = adapter.execute(command.argv, timeout_seconds=timeout, hold_fd=hold_fd)
            finally:
                released = _close_cache_fail_open(held)
        elif command.kind.endswith("-rm"):
            resource_kind = command.kind.removesuffix("-rm")
            plural = "builders" if resource_kind == "builder" else f"{resource_kind}s"
            target = command.plan[f"{resource_kind}_id"]
            resource = data["owned_resources"][plural][target]
            exists = _validate_owned_identity(adapter, resource, allow_absent_buildkit=resource_kind == "builder")
            if exists:
                child_started = True
                result = adapter.execute(command.argv, timeout_seconds=timeout, hold_fd=hold_fd)
            else:
                result = DockerResult(0, "", "")
        else:
            child_started = True
            result = adapter.execute(
                command.argv, timeout_seconds=timeout, stream_output=command.blocking, hold_fd=hold_fd
            )
        current = _load_run(context)
        state, resources = _promote_docker_result(adapter, receipt, result, current)
    except InvalidInput as error:
        if child_started:
            _finish_docker_receipt(context, receipt, "unresolved", DockerResult(5, "", ""), [])
            raise IncompleteCleanup("Docker mutation result could not be reconciled exactly.") from error
        _finish_docker_receipt(
            context, receipt, "failed", DockerResult(2, "", ""), [], cache=_failed_build_cache(cache_plan)
        )
        raise
    except (InfrastructureFailure, OSError, ResourceError) as error:
        if not child_started:
            _finish_docker_receipt(
                context, receipt, "failed", DockerResult(5, "", ""), [], cache=_failed_build_cache(cache_plan)
            )
            raise
        step = "the command did not finish" if result is None else "reconciling its result failed"
        # Toolkit errors carry fixed texts; an OS error is named by its type, since its text may hold paths.
        unresolved_cause = f"{step}: {type(error).__name__ if isinstance(error, OSError) else error}"
        result = DockerResult(5, "", "")
        state, resources = "unresolved", []
    invalid_builder = (
        command.kind == "builder-create"
        and state == "promoted"
        and any(
            resource["kind"] == "builder" and resource["evidence"].get("limits_verified") is not True
            for resource in resources
        )
    )
    # A loaded build finished by its dispatcher records its cache outcome and when it finished; an unresolved one is
    # finished later by reconciliation, which records no cache facts (design 3.2, 3.9).
    cache = None
    if cache_plan is not None and state != "unresolved":
        cache = _complete_cache_fail_open(cache_plan, state, resources, released, data["run_id"], operation_id)
    _finish_docker_receipt(context, receipt, state, result, resources, cache=cache)
    if state == "unresolved":
        raise IncompleteCleanup(
            f"Docker mutation may have created or changed an unbound resource ({command.kind}: {unresolved_cause})."
        )
    if invalid_builder:
        current = _load_run(context)
        _record_active_failure(context, "builder-envelope", current["revision"])
        raise InfrastructureFailure("Docker builder effective limits do not match the calibrated envelope.")
    return result


def _bound_docker(data: dict):
    authority = data["docker_authority"]
    if authority is None:
        raise ResourceError("Docker execution authority is not bound.")
    if _BOUND_DOCKER_FACTORY is None:
        raise InfrastructureFailure("Bound Docker adapter composition is unavailable.")
    return _BOUND_DOCKER_FACTORY(_authority_identity(authority))


def _docker_receipts_terminal(data: dict) -> bool:
    return all(receipt["state"] not in {"pending", "unresolved"} for receipt in data["docker_receipts"].values())


def _owned_removal_command(kind: str, identity: str, reference: str | None = None) -> tuple[str, ...]:
    if kind == "builder":
        return "buildx", "rm", identity
    if kind == "container":
        return "container", "rm", "-f", "-v", identity
    if kind == "image" and reference is not None:
        return "image", "rm", "--force", reference
    return kind, "rm", identity


def _image_removal(resource: dict) -> tuple[tuple[str, ...], dict[str, object]]:
    """Address a pulled alias or every recorded built-image reference exactly."""
    identity = resource["identifier"]
    evidence = resource["evidence"]
    repository_digest = evidence.get("repository_digest")
    if repository_digest is not None:
        if not isinstance(repository_digest, str):
            raise ResourceError("Owned image removal evidence is invalid.")
        return _owned_removal_command("image", identity, repository_digest), {"reference": repository_digest}
    repo_tags = evidence.get("repo_tags")
    repo_digests = evidence.get("repo_digests")
    if repo_tags is None and repo_digests is None:
        repo_tags = evidence.get("tags")
        repo_digests = []
    if (
        not isinstance(repo_tags, list)
        or any(not isinstance(reference, str) or not reference for reference in repo_tags)
        or not isinstance(repo_digests, list)
        or any(not isinstance(reference, str) or not reference for reference in repo_digests)
    ):
        raise ResourceError("Owned image removal evidence is invalid.")
    references = tuple(dict.fromkeys((*repo_tags, *repo_digests)))
    if not references:
        raise ResourceError("Owned image removal lacks exact reference evidence.")
    return ("image", "rm", *references), {"references": list(references)}


def _owned_docker_removed(data: dict) -> bool:
    return all(
        item["state"] == "removed" for resources in data["owned_resources"].values() for item in resources.values()
    )


def _owned_docker_quiescent(data: dict) -> bool:
    return _docker_receipts_terminal(data) and all(
        item["state"] != "running" for resources in data["owned_resources"].values() for item in resources.values()
    )


def _reconcile_docker_receipt(context: RunContext, deadline_ns: int | None = None) -> None:
    """Resolve the sole interrupted shim receipt only through its exact key."""
    data = _load_run(context)
    pending = [item for item in data["docker_receipts"].values() if item["state"] in {"pending", "unresolved"}]
    if not pending:
        if _archive_cleanup_deferred(data):
            _reconcile_archive_receipt(context, data, deadline_ns)
        return
    if len(pending) != 1:
        raise ResourceError("Docker receipt journal has multiple unresolved operations.")
    receipt = pending[0]
    if receipt["kind"] in _ARCHIVE_KINDS:
        _reconcile_archive_receipt(context, data, deadline_ns)
        return
    if receipt["kind"] not in _IMAGE_LOCK_SECONDS or _record_mode(data) != AdmissionMode.SHARED:
        _reconcile_receipt(context, data, receipt, deadline_ns)
        return
    # A shared run's image receipt is reconciled under K, taken first: acquiring it proves the operation's Docker
    # child (which inherited K) has exited, so nothing it does can land after the decision (design 2.6, C1).
    account = Path(data["account_directory"])
    with image_lock(Context(account, account), deadline_ns=deadline_ns) as hold_fd:
        _reconcile_receipt(context, _load_run(context), receipt, deadline_ns, hold_fd)


def _reconcile_receipt(
    context: RunContext, data: dict, receipt: dict, deadline_ns: int | None, hold_fd: int | None = None
) -> None:
    """Promote or finish one interrupted non-archive receipt through its exact key (``_reconcile_docker_receipt``)."""
    adapter = _bound_docker(data)
    recorded = receipt["result"]
    result = DockerResult(recorded["exit_code"], "", "") if recorded is not None else DockerResult(5, "", "")
    state, resources = _promote_docker_result(adapter, receipt, result, data)
    if hold_fd is not None and state == "promoted" and receipt["kind"] == "image-pull":
        # A reconciled receipt was interrupted, so K was not held continuously: another run may have pulled the same
        # image since.
        # Presence proves this run's provenance only while no other run owns, binds or handed off the image.
        image_id = resources[0]["identifier"]
        claimed = image_in_use(image_id, _interest_views(context, data, deadline_ns), actor_run=data["run_id"])
        pending = {entry.image_id for entry in read_pending(Path(data["account_directory"]))}
        if claimed or image_id in pending:
            state, resources = "failed", []
    removal_kinds = {"builder-rm", "container-rm", "image-rm", "network-rm", "volume-rm"}
    if state == "unresolved" and receipt["kind"] in removal_kinds:
        kind = receipt["kind"].removesuffix("-rm")
        identity = cast(str, receipt["plan"][f"{kind}_id"])
        plural = "builders" if kind == "builder" else f"{kind}s"
        resource = data["owned_resources"][plural][identity]
        exists = _validate_owned_identity(adapter, resource, allow_absent_buildkit=kind == "builder")
        if (
            kind == "image"
            and hold_fd is not None
            and exists
            and _hand_off_if_in_use(context, data, identity, resource, deadline_ns)
        ):
            # Another run bound the image after the owner journaled its removal: hand it off, never delete (2.7).
            _finish_docker_receipt(context, receipt, "removed", DockerResult(0, "", ""), [], deadline_ns)
            return
        if kind == "image":
            argv, expected = _image_removal(resource)
            for key in ("reference", "references"):
                if (key in receipt["plan"]) != (key in expected) or receipt["plan"].get(key) != expected.get(key):
                    raise ResourceError("Docker image removal receipt changed exact authority.")
        else:
            argv = _owned_removal_command(kind, identity)
        result = adapter.execute(argv, timeout_seconds=30, hold_fd=hold_fd) if exists else DockerResult(0, "", "")
        state, resources = _promote_docker_result(adapter, receipt, result, data)
    _finish_docker_receipt(context, receipt, state, result, resources, deadline_ns)
    if state == "unresolved":
        raise ResourceError("Docker mutation remains unresolved after exact reconciliation.")


# --- Archive export: ``image-save`` (Story B) -------------------------------------------------------------------------
#
# The normative protocol is docs/superpowers/specs/2026-10-07-owned-image-export-and-buildx-archive-design.md. In short:
# the dispatcher exports into a fresh private staging directory, verifies the archive through a held descriptor,
# links it under the run lock without replacing anything, and records success last. A reconciler never promotes; it
# fails the receipt and deletes only what it can prove belongs to the operation.


@dataclass
class _ArchiveSeams:
    """Carry the keyword-only test seams of ``dispatch_docker`` to the export path."""

    available_bytes: Callable[[str], int] | None = None
    remove_tree: Callable[[int, str, int], None] | None = None
    receipt_timeout_seconds: float | None = None


@dataclass
class _ArchiveExport:
    """Hold one export's open descriptors and how far its publication got."""

    paths: image_archive.ArchivePaths
    operation_name: str
    final_name: str
    exports_fd: int | None = None
    operation_fd: int | None = None
    verified_fd: int | None = None
    started: bool = False
    exit_code: int | None = None
    linked: bool = False
    verified_stamp: tuple[int, int, int] | None = None
    linked_stamp: tuple[int, int, int] | None = None
    settled_stamp: tuple[int, int, int] | None = None
    facts: image_archive.ArchiveFacts | None = None
    resources: list[dict] = field(default_factory=list)

    def close(self) -> None:
        """Close every descriptor this export still holds."""
        for name in ("verified_fd", "operation_fd", "exports_fd"):
            descriptor = getattr(self, name)
            if descriptor is not None:
                setattr(self, name, None)
                os.close(descriptor)


def _held(descriptor: int | None) -> int:
    """Return a descriptor the export must already hold."""
    if descriptor is None:
        raise ResourceError("The export descriptor is not open.")
    return descriptor


def _record_changed() -> ResourceError:
    return ResourceError("Docker receipt changed before result publication.", reason="record-changed")


def _admit_archive_append(data: dict, command: ClassifiedDockerCommand) -> None:
    """Admit an export receipt under the append lock: active run, once per image, and the ``exports`` directory."""
    if data["state"] != "active":
        raise ResourceBusy("Archive export requires an active run.")
    _refuse_repeat_export(data, cast(str, command.plan["image_id"]))
    try:
        root = image_archive.open_private_directory(data["private_root"])
        try:
            try:
                os.mkdir("exports", 0o700, dir_fd=root)
            except FileExistsError:
                pass
            os.fsync(root)
            os.close(image_archive.open_private_directory("exports", directory=root))
        finally:
            os.close(root)
    except OSError as error:
        raise ResourceError("The export directory could not be prepared.") from error


def _refuse_repeat_export(data: dict, image_id: str) -> None:
    """Refuse a second export of an image whose first export was published, quoting the recorded facts."""
    for item in data["docker_receipts"].values():
        if item["kind"] == "image-save" and item["state"] == "promoted" and item["plan"].get("image_id") == image_id:
            archive = item["plan"].get("archive")
            final = image_archive.archive_paths(data["private_root"], item["id"]).final
            image_archive.validate_archive_record(archive, final_path=final, image_id=image_id)
            if archive["state"] != "published":
                raise ResourceError(
                    "The promoted export lacks a published archive record.", reason="archive-record-invalid"
                )
            raise InvalidInput(
                f"Image {image_id} was already exported to {archive['path']} "
                f"(sha256 {archive['sha256']}, {archive['bytes']} bytes).",
                reason="archive-exists",
            )


def _prepare_image_export(
    adapter, command: ClassifiedDockerCommand, data: dict, seams: _ArchiveSeams
) -> tuple[ClassifiedDockerCommand, image_archive.ImageBinding]:
    """Check everything an image export needs before any file, directory or receipt exists."""
    image_id = cast(str, command.plan["image_id"])
    if data["state"] != "active":
        raise ResourceBusy("Archive export requires an active run.")
    _refuse_repeat_export(data, image_id)
    failpoint("archive-after-early-check")
    if not _validate_owned_identity(adapter, data["owned_resources"]["images"][image_id]):
        raise InvalidInput("Owned image disappeared before export.")
    item = adapter.inspect_exact(ExactInspection("image", image_id)).attributes
    if adapter.engine == "podman":  # Podman reports no descriptor; its archive binds the config digest (design 2.7)
        if item.get("Id") != image_id:
            raise ResourceError("The image is inconsistent with its ID.", reason="unsupported-image-store")
    else:
        descriptor = item.get("Descriptor")
        if not isinstance(descriptor, dict):
            raise ResourceError(
                "The Docker daemon does not report image descriptors; only the containerd image store is supported.",
                reason="unsupported-image-store",
            )
        if item.get("Id") != image_id or descriptor.get("digest") != image_id:
            raise ResourceError("The image descriptor is inconsistent with its ID.", reason="unsupported-image-store")
    try:
        platform = image_archive.normalized_platform(item.get("Os"), item.get("Architecture"), item.get("Variant"))
    except ArchiveRejected as error:
        raise ResourceError("The image platform is malformed.", reason="unsupported-image-store") from error
    size = item.get("Size")
    if type(size) is not int or size < 0 or size > image_archive.ARCHIVE_MAX_BYTES:
        raise ResourceError("The image is larger than the per-archive cap.", reason="export-storage")
    image_archive.require_free_space(
        data["private_root"], image_archive.ARCHIVE_MAX_BYTES, available_bytes=seams.available_bytes
    )
    plan = {**command.plan, "platform": platform.as_dict(), "inspected_size": size}
    return (
        ClassifiedDockerCommand(command.kind, command.argv, plan),
        image_archive.ImageBinding(image_id, platform),
    )


def _dispatch_archive(
    context: RunContext,
    descriptor: dict,
    adapter,
    command: ClassifiedDockerCommand,
    data: dict,
    operation_id: str,
    seams: _ArchiveSeams,
) -> DockerResult:
    """Export, verify and publish one archive, or fail its receipt with a closed failure class.

    The exception fence maps every failure of the staging, execution and verification steps to a failed receipt.
    Publication (V7 and V8) runs outside the fence so its own failures are never re-fenced.
    """
    command, binding = _prepare_image_export(adapter, command, data, seams)
    deadline = Deadline.after(
        NativeClock(), 1200 if seams.receipt_timeout_seconds is None else seams.receipt_timeout_seconds
    )
    receipt = _append_docker_receipt(context, descriptor, operation_id, command, deadline)
    paths = image_archive.archive_paths(data["private_root"], operation_id)
    export = _ArchiveExport(paths, operation_id, os.path.basename(paths.final))
    try:
        _guarded(
            lambda: _stage_and_verify(context, adapter, command, receipt, deadline, export, binding),
            lambda error: _fail_archive(context, receipt, export, deadline, seams, error),
        )
        return _publish_archive(context, receipt, command, export, deadline, seams, data)
    finally:
        export.close()


def _stage_and_verify(
    context: RunContext,
    adapter,
    command: ClassifiedDockerCommand,
    receipt: dict,
    deadline: Deadline,
    export: _ArchiveExport,
    binding: image_archive.ImageBinding,
) -> None:
    """Run steps 4 and 5 and V1 to V6; every failure here is fenced by the caller."""
    failpoint("archive-after-append")
    export.exports_fd = image_archive.open_private_directory(export.paths.exports)
    os.mkdir(export.operation_name, 0o700, dir_fd=export.exports_fd)
    export.operation_fd = image_archive.open_private_directory(export.operation_name, directory=export.exports_fd)
    timeout = max(0.001, deadline.remaining_ns(NativeClock()) / 1_000_000_000)
    export.started = True
    result = adapter.execute(command.argv, timeout_seconds=timeout)
    export.exit_code = result.exit_code
    if result.exit_code != 0:
        raise ArchiveRejected("child-failed", f"the Docker client exited with {result.exit_code}")
    with os.scandir(export.operation_fd) as entries:
        names = [entry.name for entry in entries]
    if not names:
        raise ArchiveRejected("absent", "the Docker client exited 0 without writing the archive")
    if names != ["archive.tar"]:
        raise ArchiveRejected("unexpected-entry", "the staging directory holds more than the archive")
    verified = os.open("archive.tar", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=export.operation_fd)
    export.verified_fd = verified
    export.verified_stamp = image_archive.accept_output(verified)
    facts = image_archive.verify_archive(verified, binding)
    failpoint("archive-after-walk")
    if image_archive.stamp(verified) != export.verified_stamp:
        raise _changed_after_check(context, receipt, "the archive changed while it was verified")
    if facts.bytes != export.verified_stamp[0]:
        raise ArchiveRejected("verification-failed", "the verified size differs from the file size")
    export.facts = facts
    failpoint("archive-after-verify")
    os.fsync(verified)
    if deadline.remaining_ns(NativeClock()) <= 0:
        raise ArchiveRejected("deadline", "the receipt deadline expired before publication")
    _link_if_pending(context, receipt, export)
    failpoint("archive-after-link")


def _changed_after_check(context: RunContext, receipt: dict, detail: str) -> ToolkitError:
    """Name a stamp mismatch: a reconciler's own unlinks look like a rewrite, but only while the receipt is pending."""
    with _locked_run(context):
        current = _load_run(context)["docker_receipts"].get(receipt["id"])
    return _record_changed() if current != receipt else ArchiveRejected("publish-substituted", detail)


def _link_if_pending(context: RunContext, receipt: dict, export: _ArchiveExport) -> None:
    """V6: link the verified file to its final name under the run lock, only while the receipt is pending."""
    exports, operation, verified = _held(export.exports_fd), _held(export.operation_fd), _held(export.verified_fd)
    with _locked_run(context):
        data = _load_run(context)
        if data["docker_receipts"].get(receipt["id"]) != receipt:
            raise _record_changed()
        if image_archive.stamp(verified) != export.verified_stamp:
            raise ArchiveRejected("publish-substituted", "the archive changed before publication")
        try:
            os.link("archive.tar", export.final_name, src_dir_fd=operation, dst_dir_fd=exports, follow_symlinks=False)
        except FileExistsError as error:
            raise ArchiveRejected("publish-collision", "the final name already exists") from error
        export.linked = True
        observed = image_archive.lstat_at(export.final_name, exports)
        if observed is None or image_archive.identity(observed) != image_archive.identity(os.fstat(verified)):
            raise ArchiveRejected("publish-substituted", "the final name is not the verified file")
        export.linked_stamp = image_archive.stamp(verified)
        if export.verified_stamp is None or export.linked_stamp[:2] != export.verified_stamp[:2]:
            raise ArchiveRejected("publish-substituted", "the archive changed during publication")
        os.fsync(exports)


def _guarded[T](step: Callable[[], T], recover: Callable[[Exception], ToolkitError]) -> T:
    """Run one step of an export; on any failure run ``recover`` and re-raise the primary error it returns.

    This is the exception fence of the export protocol. The broad catch is a cleanup boundary, not error handling:
    ``recover`` rolls back what the step created and records the failure, and the original error always propagates
    (``BaseException`` such as an interrupt is not caught and leaves the receipt pending for reconciliation).
    """
    try:
        return step()
    except Exception as error:
        primary = recover(error)
        if primary is error:
            raise
        raise primary from error


def _failure_class(error: Exception, export: _ArchiveExport, deadline: Deadline) -> str:
    """Map a fenced failure to its closed class: a domain class, a deadline, not-started, or io-error."""
    if isinstance(error, ArchiveRejected):
        return error.failure_class
    if export.started and isinstance(error, InfrastructureFailure) and deadline.remaining_ns(NativeClock()) <= 0:
        return "deadline"
    return "io-error" if export.started else "not-started"


def _finish_archive_best_effort(
    context: RunContext, receipt: dict, state: str, exit_code: int, record: dict
) -> IncompleteCleanup | None:
    """Record a non-success outcome; return a cleanup error when the receipt could not be made terminal.

    A finish refused with ``record-changed`` means a reconciler already finished the receipt, which is the expected
    loss and returns ``None``. Any other failure leaves the receipt pending for reconciliation; the caller reports it
    next to the primary error, which is never replaced.
    """
    try:
        _finish_docker_receipt(
            context,
            receipt,
            state,
            DockerResult(exit_code, "", ""),
            [],
            archive=record,
        )
    except ResourceError as error:
        if error.reason == "record-changed":
            return None
        return IncompleteCleanup("The failure could not be recorded; the receipt stays pending for reconciliation.")
    except OSError:
        return IncompleteCleanup("The failure could not be recorded; the receipt stays pending for reconciliation.")
    return None


def _fail_archive(
    context: RunContext,
    receipt: dict,
    export: _ArchiveExport,
    deadline: Deadline,
    seams: _ArchiveSeams,
    error: Exception,
) -> ToolkitError:
    """Roll back, dispose and fail the receipt; return the primary error, carrying a cleanup error if needed."""
    failure = _failure_class(error, export, deadline)
    primary = error if isinstance(error, ToolkitError) else ArchiveRejected(failure, type(error).__name__)
    problem: OSError | None = None
    if export.exports_fd is not None:
        if export.linked:
            problem = image_archive.rollback_final(
                export.exports_fd, export.final_name, export.verified_fd, created=True
            )
        staging = image_archive.dispose_staging(
            export.exports_fd, export.operation_name, export.operation_fd, remove_tree=seams.remove_tree
        )
        problem = problem or staging
    unrecorded: IncompleteCleanup | None = None
    if not (isinstance(error, ResourceError) and error.reason == "record-changed"):
        record = {
            "schema": 1,
            "state": "failed",
            "cleanup": "incomplete" if problem is not None else "complete",
            "failure": failure,
        }
        exit_code = export.exit_code if export.exit_code is not None else 5
        unrecorded = _finish_archive_best_effort(context, receipt, "failed", exit_code, record)
    if problem is not None and unrecorded is not None:
        primary.cleanup = IncompleteCleanup(
            "Export staging could not be removed completely and the failure could not be recorded; "
            "the receipt stays pending for reconciliation."
        )
    elif problem is not None:
        primary.cleanup = IncompleteCleanup("Export staging could not be removed completely.")
    elif unrecorded is not None:
        primary.cleanup = unrecorded
    return primary


def _publish_archive(
    context: RunContext,
    receipt: dict,
    command: ClassifiedDockerCommand,
    export: _ArchiveExport,
    deadline: Deadline,
    seams: _ArchiveSeams,
    data: dict,
) -> DockerResult:
    """V7 and V8: settle the staging directory, then record the published archive as the last step."""
    exports = _held(export.exports_fd)
    if export.facts is None or export.linked_stamp is None:
        raise ResourceError("The export reached publication without verified facts.")

    recover = functools.partial(_fail_archive, context, receipt, export, deadline, seams)
    _guarded(lambda: _require_link_unchanged(context, receipt, export), recover)
    staging = image_archive.dispose_staging(
        exports, export.operation_name, export.operation_fd, remove_tree=seams.remove_tree
    )
    if staging is not None:
        raise _staging_not_removed(context, receipt, export, staging)
    _guarded(lambda: _settle_published(export), recover)
    record = {
        "schema": 1,
        "state": "published",
        "cleanup": "complete",
        "path": export.paths.final,
        **export.facts.as_record(),
    }
    _guarded(
        lambda: _finish_docker_receipt(
            context,
            receipt,
            "promoted",
            DockerResult(0, "", ""),
            export.resources,
            archive=record,
            precheck=lambda: _require_published_namespace(export),
        ),
        lambda error: _resolve_publication(context, receipt, export, record, error, deadline, seams),
    )
    return DockerResult(0, _published_line(receipt, command, record, data) + "\n", "")


def _require_link_unchanged(context: RunContext, receipt: dict, export: _ArchiveExport) -> None:
    """V7(a): the published inode must still carry the stamp taken right after the link."""
    if image_archive.stamp(_held(export.verified_fd)) != export.linked_stamp:
        raise _changed_after_check(context, receipt, "the archive changed after publication")


def _settle_published(export: _ArchiveExport) -> None:
    """V7(c) and (d): make the removal of the staging directory durable and check the inode once more."""
    os.fsync(_held(export.exports_fd))
    export.settled_stamp = image_archive.stamp(_held(export.verified_fd))
    if export.linked_stamp is None or export.settled_stamp[:2] != export.linked_stamp[:2]:
        raise ArchiveRejected("publish-substituted", "the archive changed while its staging was removed")
    failpoint("archive-before-finish")


def _require_published_namespace(export: _ArchiveExport) -> None:
    """V8 anchors: ``exports`` is the directory we hold, ``final`` is the verified inode, and nothing was rewritten."""
    exports, verified = _held(export.exports_fd), _held(export.verified_fd)
    try:
        namespace = os.stat(export.paths.exports, follow_symlinks=False)
    except OSError as error:
        raise ArchiveRejected("publish-substituted", "the export directory is gone") from error
    if image_archive.identity(namespace) != image_archive.identity(os.fstat(exports)):
        raise ArchiveRejected("publish-substituted", "the export directory was replaced")
    observed = image_archive.lstat_at(export.final_name, exports)
    if observed is None or image_archive.identity(observed) != image_archive.identity(os.fstat(verified)):
        raise ArchiveRejected("publish-substituted", "the published name is not the verified file")
    if image_archive.stamp(verified) != export.settled_stamp:
        raise ArchiveRejected("publish-substituted", "the archive changed before it was recorded")


def _staging_not_removed(context: RunContext, receipt: dict, export: _ArchiveExport, cause: OSError) -> ToolkitError:
    """V7(b): the staging directory could not be removed, so the archive is not published (exit 7)."""
    image_archive.rollback_final(_held(export.exports_fd), export.final_name, export.verified_fd, created=True)
    record = {"schema": 1, "state": "failed", "cleanup": "incomplete", "failure": "cleanup-incomplete"}
    error = IncompleteCleanup("The export staging directory could not be removed; the archive was not published.")
    error.__cause__ = cause
    error.cleanup = _finish_archive_best_effort(context, receipt, "failed", 0, record)
    return error


def _resolve_publication(
    context: RunContext,
    receipt: dict,
    export: _ArchiveExport,
    record: dict,
    error: Exception,
    deadline: Deadline,
    seams: _ArchiveSeams,
) -> ToolkitError:
    """Decide the outcome of a V8 that raised, after re-reading the record under the lock (design 3.3)."""
    with _locked_run(context):
        current = _load_run(context)["docker_receipts"].get(receipt["id"])
    if current is not None and current["state"] == "promoted" and current["plan"].get("archive") == record:
        return InfrastructureFailure(
            f"The archive was recorded as published at {record['path']} (sha256 {record['sha256']}) "
            "but the commit could not be confirmed.",
            reason="archive-commit-unconfirmed",
        )
    if current == receipt:
        return _fail_archive(context, receipt, export, deadline, seams, error)
    return _fail_archive(context, receipt, export, deadline, seams, _record_changed())


def _published_line(receipt: dict, command: ClassifiedDockerCommand, record: dict, data: dict) -> str:
    """Return the canonical one-line JSON document the shim prints for a published archive."""
    line = {
        "schema": 1,
        "kind": command.kind,
        "operation_id": receipt["id"],
        "project_id": data["project_id"],
        "session_id": data["session_id"],
        "run_id": data["run_id"],
        "path": record["path"],
        "bytes": record["bytes"],
        "sha256": record["sha256"],
        "layout": record["layout"],
        "format": "docker-image-save",
        "root_digest": record["root_digest"],
        "manifest_digest": record["manifest_digest"],
        "config_digest": record["config_digest"],
        "platform": record["platform"],
        "image_id": command.plan["image_id"],
    }
    return json.dumps(line, sort_keys=True, separators=(",", ":"))


def _archive_cleanup_deferred(data: dict) -> bool:
    """Report whether a failed export receipt still owes the deletion of what its operation left behind."""
    return any(
        item["kind"] in _ARCHIVE_KINDS
        and item["state"] == "failed"
        and isinstance(item["plan"].get("archive"), dict)
        and item["plan"]["archive"].get("cleanup") == "deferred"
        for item in data["docker_receipts"].values()
    )


def _reconcile_archive_receipt(context: RunContext, data: dict, deadline_ns: int | None) -> None:
    """Fail the interrupted export receipt from disk facts alone, then complete every deferred deletion."""
    for receipt in data["docker_receipts"].values():
        if receipt["kind"] in _ARCHIVE_KINDS and receipt["state"] in {"pending", "unresolved"}:
            _classify_and_fail(context, receipt["id"], deadline_ns)
    for receipt in _load_run(context)["docker_receipts"].values():
        if receipt["kind"] in _ARCHIVE_KINDS:
            _settle_archive_cleanup(context, receipt["id"], deadline_ns)


def _open_exports_for_recovery(exports: str) -> int | None:
    """Open ``exports`` without following a symlink or blocking; ``None`` when it does not exist."""
    try:
        descriptor = os.open(exports, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        return None
    if os.fstat(descriptor).st_uid != os.getuid():
        os.close(descriptor)
        raise OSError("The export directory is not account-owned.")
    return descriptor


def _classify_export_on_disk(paths: image_archive.ArchivePaths, receipt_id: str) -> tuple[str, tuple[int, int] | None]:
    """RC1: classify what the interrupted export left behind, opening only directories and never ``final``."""
    try:
        exports = _open_exports_for_recovery(paths.exports)
        if exports is None:
            return "recovered-absent", None
        try:
            return image_archive.classify_interrupted(exports, receipt_id, os.path.basename(paths.final))
        finally:
            os.close(exports)
    except OSError:
        return "recovered-partial", None


def _classify_and_fail(context: RunContext, receipt_id: str, deadline_ns: int | None) -> None:
    """RC1 and RC2 in one run-lock hold: classify the interrupted export and record it ``failed``, never promoting."""
    failpoint("archive-before-classify")
    with _locked_run(context, deadline_ns):
        data = _load_run(context)
        current = data["docker_receipts"].get(receipt_id)
        if current is None or current["state"] not in {"pending", "unresolved"}:
            return
        paths = image_archive.archive_paths(data["private_root"], receipt_id)
        plan = current["plan"]
        if (plan.get("directory"), plan.get("output"), plan.get("final")) != (
            paths.directory,
            paths.output,
            paths.final,
        ):
            # The recorded paths are ignored, but the derived ones are still observed so RC3 can delete by identity.
            _, final_identity = _classify_export_on_disk(paths, receipt_id)
            failure = "archive-record-invalid"
        else:
            failure, final_identity = _classify_export_on_disk(paths, receipt_id)
        record = {
            "schema": 1,
            "state": "failed",
            "cleanup": "deferred",
            "failure": failure,
            "final_identity": None if final_identity is None else {"dev": final_identity[0], "ino": final_identity[1]},
        }
        _finish_receipt_locked(context, data, current, "failed", DockerResult(5, "", ""), [], archive=record)
    failpoint("archive-after-record-failed")


def _settle_archive_cleanup(context: RunContext, receipt_id: str, deadline_ns: int | None) -> None:
    """RC3: delete the operation's leftovers by identity, then record whether the deletion completed."""
    data = _load_run(context)
    current = data["docker_receipts"].get(receipt_id)
    archive = current["plan"].get("archive") if current is not None else None
    if current is None or current["state"] != "failed" or not isinstance(archive, dict):
        return
    if archive.get("cleanup") != "deferred":
        return
    paths = image_archive.archive_paths(data["private_root"], receipt_id)
    recorded = archive.get("final_identity")
    identity = None if recorded is None else (recorded["dev"], recorded["ino"])
    problem: OSError | None = None
    try:
        exports = _open_exports_for_recovery(paths.exports)
    except OSError as error:
        exports, problem = None, error
    if exports is not None:
        try:
            staging = image_archive.dispose_staging(exports, receipt_id, None)
            published = image_archive.remove_final_by_identity(exports, os.path.basename(paths.final), identity)
            problem = staging or published
        finally:
            os.close(exports)
    failpoint("archive-after-disposal")
    with _locked_run(context, deadline_ns):
        data = _load_run(context)
        current = data["docker_receipts"].get(receipt_id)
        archive = current["plan"].get("archive") if current is not None else None
        if current is None or current["state"] != "failed" or not isinstance(archive, dict):
            return
        if archive.get("cleanup") != "deferred":
            return
        archive["cleanup"] = "incomplete" if problem is not None else "complete"
        data["revision"] += 1
        atomic_json(context.record, data)


def _validate_owned_identity(
    adapter, resource: dict, *, allow_absent_buildkit: bool = False, deadline: Deadline | None = None
) -> bool:
    kind = resource["kind"]
    evidence = resource["evidence"]
    if kind == "builder":
        item = _builder_observation(adapter, resource["identifier"])
        if item is None:
            return False
        endpoint_identity = _builder_endpoint_identity(item)
        container_id = evidence.get("buildkit_container_id")
        if (
            item.get("Name") != resource["name"]
            or item.get("Driver") != evidence.get("driver")
            or endpoint_identity != evidence.get("endpoint_sha256")
        ):
            raise ResourceError("Owned builder identity changed before exact mutation.")
        if isinstance(container_id, str):
            container = adapter.inspect_exact(ExactInspection("container", container_id), deadline=deadline)
            if not container.exists:
                if allow_absent_buildkit:
                    return True
                raise ResourceError("Owned BuildKit worker disappeared before exact mutation.")
            candidate = container.attributes
            if candidate.get("Id") != container_id or str(candidate.get("Name", "")).lstrip("/") != buildkit_worker(
                resource["name"]
            ):
                raise ResourceError("Owned BuildKit worker identity changed before exact mutation.")
        return True
    observation = adapter.inspect_exact(ExactInspection(kind, resource["identifier"]), deadline=deadline)
    if not observation.exists:
        return False
    item = observation.attributes
    if kind == "container":
        config = item.get("Config")
        labels = config.get("Labels") if isinstance(config, dict) else None
        expected_labels = evidence.get("labels")
        if (
            item.get("Id") != resource["identifier"]
            or str(item.get("Name", "")).lstrip("/") != resource["name"]
            or item.get("Created") != evidence.get("created")
            or item.get("Image") != evidence.get("image_id")
            or not isinstance(labels, dict)
            or (
                isinstance(expected_labels, dict)
                and any(labels.get(key) != value for key, value in expected_labels.items())
            )
            or (
                isinstance(evidence.get("compose_project"), str)
                and labels.get("com.docker.compose.project") != evidence["compose_project"]
            )
        ):
            raise ResourceError("Owned container identity changed before exact mutation.")
    elif kind == "network":
        labels = item.get("Labels")
        if (
            item.get("Id") != resource["identifier"]
            or item.get("Name") != resource["name"]
            or item.get("Created") != evidence.get("created")
            or not isinstance(labels, dict)
            or (
                isinstance(evidence.get("labels"), dict)
                and any(labels.get(key) != value for key, value in evidence["labels"].items())
            )
            or (
                isinstance(evidence.get("compose_project"), str)
                and labels.get("com.docker.compose.project") != evidence["compose_project"]
            )
        ):
            raise ResourceError("Owned network identity changed before exact mutation.")
    elif kind == "volume":
        labels = item.get("Labels")
        if (
            item.get("Name") != resource["name"]
            or item.get("CreatedAt") != evidence.get("created")
            or item.get("Driver") != evidence.get("driver")
            or not isinstance(labels, dict)
            or (
                isinstance(evidence.get("labels"), dict)
                and any(labels.get(key) != value for key, value in evidence["labels"].items())
            )
            or (
                isinstance(evidence.get("compose_project"), str)
                and labels.get("com.docker.compose.project") != evidence["compose_project"]
                and not _anonymous_volume(resource["name"], labels)
            )
        ):
            raise ResourceError("Owned volume identity changed before exact mutation.")
    elif kind == "image":
        config = item.get("Config")
        labels = config.get("Labels") if isinstance(config, dict) else None
        expected_labels = evidence.get("labels")
        repository_digest = evidence.get("repository_digest")
        digest_changed = repository_digest is not None and (
            not isinstance(repository_digest, str)
            or not isinstance(item.get("RepoDigests"), list)
            or _repository_digest(repository_digest) not in item["RepoDigests"]
        )
        labels_changed = repository_digest is None and (
            not isinstance(labels, dict)
            or not isinstance(expected_labels, dict)
            or any(labels.get(key) != value for key, value in expected_labels.items())
        )
        repo_digests = item.get("RepoDigests")
        if repo_digests is None:
            repo_digests = []
        removal_plan = _image_removal(resource)[1] if repository_digest is None else {}
        recorded_references = removal_plan.get("references")
        repo_tags = item.get("RepoTags")
        if repo_tags is None:
            repo_tags = []
        references_changed = repository_digest is None and (
            not isinstance(repo_tags, list)
            or any(not isinstance(value, str) for value in repo_tags)
            or not isinstance(repo_digests, list)
            or any(not isinstance(value, str) for value in repo_digests)
            or not isinstance(recorded_references, list)
            or not {*repo_tags, *repo_digests}.issubset(recorded_references)
        )
        if not references_changed and isinstance(recorded_references, list):
            references_changed = any(
                observation.exists and observation.attributes.get("Id") != resource["identifier"]
                for observation in (
                    adapter.inspect_exact(ExactInspection("image", reference)) for reference in recorded_references
                )
            )
        if item.get("Id") != resource["identifier"] or digest_changed or labels_changed or references_changed:
            raise ResourceError("Owned image identity changed before exact mutation.")
    else:
        raise ResourceError("Owned Docker resource kind is unsupported.")
    return True


def _run_owned_mutation(
    context: RunContext,
    argv: tuple[str, ...],
    kind: str,
    plan: dict[str, object],
    deadline_ns: int | None = None,
    hold_fd: int | None = None,
) -> None:
    data = _load_run(context)
    authority = data["docker_authority"]
    if authority is None:
        raise ResourceError("Exact Docker cleanup lacks bound authority.")
    command = ClassifiedDockerCommand(kind, argv, plan)
    deadline = Deadline.after(NativeClock(), 30)
    receipt = _append_docker_receipt(
        context, {"generation": data["generation"]}, str(uuid.uuid4()), command, deadline, deadline_ns
    )
    adapter = _bound_docker(data)
    timeout = max(0.001, deadline.remaining_ns(NativeClock()) / 1_000_000_000)
    try:
        resource_kind = kind.removesuffix("-rm").removeprefix("container-") if kind.endswith("-rm") else "container"
        plural = "builders" if resource_kind == "builder" else f"{resource_kind}s"
        target_key = f"{resource_kind}_id" if kind.endswith("-rm") else "container_id"
        resource = data["owned_resources"][plural][plan[target_key]]
        exists = _validate_owned_identity(adapter, resource, allow_absent_buildkit=resource_kind == "builder")
        if not exists and kind.endswith("-rm"):
            result = DockerResult(0, "", "")
        else:
            if not exists:
                raise ResourceError("Owned Docker target is absent before exact control.")
            result = adapter.execute(argv, timeout_seconds=timeout, hold_fd=hold_fd)
        current = _load_run(context)
        state, resources = _promote_docker_result(adapter, receipt, result, current)
    except (InfrastructureFailure, OSError, ResourceError):
        result = DockerResult(5, "", "")
        state, resources = "unresolved", []
    _finish_docker_receipt(context, receipt, state, result, resources, deadline_ns)
    if state == "unresolved":
        raise ResourceError("Exact Docker cleanup result is unresolved.")


def _stop_owned_docker_resources(context: RunContext, deadline_ns: int | None = None) -> None:
    data = _load_run(context)
    for container_id, resource in data["owned_resources"]["containers"].items():
        if resource["state"] == "running":
            _run_owned_mutation(
                context,
                ("container", "stop", "--time", "15", container_id),
                "container-stop",
                {"container_id": container_id},
                deadline_ns,
            )


def _cleanup_owned_docker_resources(context: RunContext, deadline_ns: int | None = None) -> None:
    """Remove only exact promoted resources, journaling each mutation first.

    Images last: a shared run, or any run while handed-off images are pending, runs the image step under the image lock
    K (design 2.7); an exclusive run with nothing pending removes its images exactly as today.
    """
    operations = (
        ("builders", "builder"),
        ("containers", "container"),
        ("networks", "network"),
        ("volumes", "volume"),
    )
    _remove_owned(context, operations, deadline_ns)
    failpoint("owned-containers-removed")
    data = _load_run(context)
    account = Path(data["account_directory"])
    if not _image_step_locked(data):
        _remove_owned(context, (("images", "image"),), deadline_ns)
        return
    with image_lock(Context(account, account), deadline_ns=deadline_ns) as hold_fd:
        _image_step(context, deadline_ns, hold_fd)


_RELEASE_SECONDS = 180


def _owned_cleanup_needed(data: dict) -> bool:
    """Whether a leave, release or recovery runs the owned cleanup: something owned remains, or handed-off images are
    pending, which the last user drains even when it owns nothing (design 2.7)."""
    return not _owned_docker_removed(data) or not pending_absent(Path(data["account_directory"]))


def _image_step_locked(data: dict) -> bool:
    """Whether the image step runs under K: a shared record, or handed-off images pending; an exclusive run with
    nothing pending removes its images exactly as before shared runs (design 2.7)."""
    return _record_mode(data) == AdmissionMode.SHARED or not pending_absent(Path(data["account_directory"]))


def _remove_owned(
    context: RunContext,
    operations: tuple[tuple[str, str], ...],
    deadline_ns: int | None,
    hold_fd: int | None = None,
) -> None:
    """Remove every owned, not yet removed resource of the given kinds through its own journaled receipt."""
    for plural, kind in operations:
        current = _load_run(context)
        for identity, resource in current["owned_resources"][plural].items():
            if resource["state"] == "removed":
                continue
            plan = {f"{kind}_id": identity}
            if kind == "image":
                argv, image_plan = _image_removal(resource)
                plan.update(image_plan)
            else:
                argv = _owned_removal_command(kind, identity)
            _run_owned_mutation(context, argv, f"{kind}-rm", plan, deadline_ns, hold_fd)


def _interest_views(context: RunContext, data: dict, deadline_ns: int | None) -> list[InterestView]:
    """Read who may still use images for the run of ``context`` (see ``_account_interest_views``)."""
    return _account_interest_views(Path(data["account_directory"]), context.project.state, deadline_ns)


def operator_image_interest(account: Path, registry: Path) -> Callable[[str], bool]:
    """Return the in-use decision operator cleanup applies to each image; the caller evaluates it under K.

    It applies at every limit: an image a live holder or an indexed retained run uses is kept (design 2.7). At limit 1
    this keeps more than before shared runs existed, never less.
    """
    return lambda image_id: image_in_use(image_id, _account_interest_views(account, registry, None), actor_run="")


def _account_interest_views(account: Path, registry: Path, deadline_ns: int | None) -> list[InterestView]:
    """Read who may still use images (design 2.7): every holder entry's record and every indexed stopped-retained run.

    Queued entries never protect an image. An unreadable record, or a project index whose runs cannot be enumerated,
    counts as using every image.
    """
    with lock(Context(account, account), "metadata", deadline_ns=deadline_ns) as anchor:
        state = _load_queue(account, anchor)
        holders = [(run_id, entry) for run_id, entry in state.entries.items() if entry.phase in HOLDER_PHASES]
    paths = {run_id: Path(entry.record) for run_id, entry in holders}
    views = []
    for project_record in sorted((registry / "projects").glob("*.json")):
        try:
            runs = _read_private_json(project_record).get("runs")
        except ResourceError:
            runs = None
        if not isinstance(runs, dict):
            # Its stopped-retained runs cannot be enumerated: they may use any image (fail closed).
            views.append(InterestView(f"index:{project_record.name}", None, False, False, None))
            continue
        for run_id, record in runs.items():
            if isinstance(record, str):
                paths.setdefault(run_id, Path(record))
            else:  # a run the index cannot locate may use any image (fail closed)
                views.append(InterestView(str(run_id), None, False, False, None))
    for run_id, path in sorted(paths.items()):
        try:
            record = _read_private_json(path)
        except ResourceError:
            views.append(InterestView(run_id, None, False, False, None))
            continue
        if run_id not in dict(holders) and record.get("state") != "stopped-retained":
            continue
        views.append(_interest_view(run_id, record))
    return views


def _interest_view(run_id: str, record: dict) -> InterestView:
    """Translate one readable run record into what the interest decision reads; a malformed record uses everything."""
    try:
        containers = [
            *record["resources"].get("containers", {}).values(),
            *record["owned_resources"]["containers"].values(),
        ]
        images = {
            identity for identity, item in record["owned_resources"]["images"].items() if item["state"] != "removed"
        }
        images |= {item["identifier"] for item in record["image_bindings"].values()}
        images |= {item["identifier"] for item in record["external_resources"] if item["kind"] == "image"}
        return InterestView(
            run_id,
            record["state"],
            record.get("recovery_actor") is not None,
            all(item["state"] == "removed" for item in containers),
            frozenset(images),
        )
    except (KeyError, TypeError, AttributeError):
        return InterestView(run_id, None, False, False, None)


def _image_step(context: RunContext, deadline_ns: int | None, hold_fd: int) -> None:
    """Decide and delete owned images under K, then drain handed-off images nobody uses any more (design 2.7).

    A shared run hands an image still in use to the pending list instead of deleting it; an exclusive run deletes as
    today. An unreadable pending list hands nothing off and drains nothing: the cleanup stays incomplete.
    """
    data = _load_run(context)
    account = Path(data["account_directory"])
    try:
        read_pending(account)  # an unreadable list hands nothing off and drains nothing (R3-8)
    except ResourceError as error:
        raise IncompleteCleanup("The pending image deletion list is unreadable; images are kept.") from error
    if _record_mode(data) != AdmissionMode.SHARED:
        _remove_owned(context, (("images", "image"),), deadline_ns, hold_fd)
    else:
        for identity, resource in data["owned_resources"]["images"].items():
            if resource["state"] == "removed" or _hand_off_if_in_use(context, data, identity, resource, deadline_ns):
                continue
            argv, plan = _image_removal(resource)
            _run_owned_mutation(context, argv, "image-rm", {"image_id": identity, **plan}, deadline_ns, hold_fd)
    _drain_pending(context, account, deadline_ns, hold_fd)


def _hand_off_if_in_use(
    context: RunContext,
    data: dict,
    identity: str,
    resource: dict,
    deadline_ns: int | None,
    *,
    image_lock_wait: tuple[str, int, int] | None = None,
) -> bool:
    """Under K, hand one owned image another live run uses to the pending list; return whether it was handed off.

    The entry is appended once (a re-run after a crash finds it and only marks), then the resource is marked removed
    with ``pending_deletion`` evidence; a dispatch's K wait is merged into that write (N9). Raises IncompleteCleanup
    when the pending list is unreadable.
    """
    if not image_in_use(identity, _interest_views(context, data, deadline_ns), actor_run=data["run_id"]):
        return False
    account = Path(data["account_directory"])
    try:
        pending = read_pending(account)
    except ResourceError as error:
        raise IncompleteCleanup("The pending image deletion list is unreadable; images are kept.") from error
    daemon_id = data["grant"]["daemon_id"]
    if not any(entry.image_id == identity and entry.daemon_id == daemon_id for entry in pending):
        argv, _plan = _image_removal(resource)
        references = tuple(str(reference) for reference in argv[2:] if reference != "--force")
        created = resource["evidence"].get("created")
        owner = (data["run_id"], data["project_id"], daemon_id)  # only a granted run hands off
        pending.append(PendingImage(identity, created if isinstance(created, str) else "", references, *owner))
        write_pending(account, pending)
        failpoint("pending-appended")
    _mark_handed_off(context, identity, image_lock_wait)
    return True


def _mark_handed_off(context: RunContext, identity: str, image_lock_wait: tuple[str, int, int] | None) -> None:
    """Record an owned image as handed off: ``removed`` with ``pending_deletion`` evidence, in one journaled write."""
    with _locked_run(context, None):
        data = _load_run(context)
        resource = data["owned_resources"]["images"][identity]
        if resource["state"] == "removed":
            return
        resource["state"] = "removed"
        resource["evidence"] = {**resource["evidence"], "pending_deletion": True}
        if image_lock_wait is not None:
            _record_image_lock_wait(data, image_lock_wait)
        data["revision"] += 1
        atomic_json(context.record, data)


def _drain_pending(context: RunContext, account: Path, deadline_ns: int | None, hold_fd: int) -> None:
    """Under K, delete each pending image no live run uses, after its identity check; drop absent or changed ones."""
    pending = read_pending(account)
    if not pending:
        return
    data = _load_run(context)
    views = _interest_views(context, data, deadline_ns)
    # A run that never ran a Docker command (it only declared use_image) still drains, on the discovered daemon, but
    # only entries its own grant's daemon recorded, and only when the discovered daemon is that daemon.
    daemon_id = (data.get("grant") or {}).get("daemon_id")

    def in_use(image_id: str) -> bool:
        """Whether a live run other than this one uses the image."""
        return image_in_use(image_id, views, actor_run=data["run_id"])

    if not any(entry.daemon_id == daemon_id and not in_use(entry.image_id) for entry in pending):
        return  # nothing to delete: no Docker call and nothing to rewrite
    adapter = _bound_docker(data) if data["docker_authority"] is not None else _docker()
    if adapter.observe_daemon().daemon_id != daemon_id:
        return  # another daemon is reachable now: nothing is deleted and every entry stays
    drain_pending(adapter, account, daemon_id, in_use, hold_fd)


def cancel_run(context: RunContext) -> RunStatus:
    """Cancel the original requester's unentered preparation without admission."""
    project = context.project
    with lock(project, project.project_id, shared=True), lock(project, f"run-{context.run_id}"):
        data = _load_run(context)
        if data["state"] == "complete" and data["generation"] == 0 and data["cleanup_outcome"] == "complete":
            return run_status(context)
        actor = asdict(current_process(NativeClock()))
        if (
            data.get("retained_grant") is not None
            and data["requester"] == actor
            and data["state"] == "prepared"
            and data["guardian_attempt"] is None
        ):
            _restore_stopped_retained(data)
            atomic_json(context.record, data)
            return run_status(context)
        if (
            data["requester"] != actor
            or data["source_identity"] != source_identity()
            or data["state"] != "prepared"
            or data["guardian_attempt"] is not None
            or any(data.get(key) for key in ("resources", "endpoints", "consumers", "pending_operation"))
        ):
            raise ResourceError("Only the original requester may cancel an unentered empty preparation.")
        data.update(
            state="complete",
            admission="revoked",
            test_outcome="cancelled",
            cleanup_outcome="pending",
        )
        data["revision"] += 1
        atomic_json(context.record, data)
        _remove_private_root(context)
        data.update(cleanup_outcome="complete", recovery_outcome="complete", quiescent=True)
        data["revision"] += 1
        atomic_json(context.record, data)
        with lock(project, "registry"):
            registry = load(project)
            if registry["runs"].get(context.run_id) != str(context.record):
                raise ResourceError("Preparation lost its project recovery reference.")
            del registry["runs"][context.run_id]
            save(project, registry)
        return run_status(context)


def _finalize_upgraded_preparation(context: RunContext) -> None:
    """Finish a never-granted run its guardian revoked at the version gate, as ``cancel_run``'s tail does.

    ``revoke_ungranted`` leaves the private root and the registry index to a live requester; this requester removes
    them, so nothing remains even when the project never cancels (design 3.2). Idempotent.
    """
    project = context.project
    with lock(project, project.project_id, shared=True), lock(project, f"run-{context.run_id}"):
        data = _load_run(context)
        if data["state"] != "complete" or data["admission"] == "published" or data["grant"] is not None:
            raise ResourceError("Only a never-granted revoked run is finalized after an upgrade.")
        _remove_private_root(context)
        if data["cleanup_outcome"] != "complete":
            data.update(cleanup_outcome="complete", recovery_outcome="complete", quiescent=True)
            data["revision"] += 1
            atomic_json(context.record, data)
        with lock(project, "registry"):
            registry = load(project)
            if registry["runs"].get(context.run_id) == str(context.record):
                del registry["runs"][context.run_id]
                save(project, registry)


def prepare_resume(
    context: RunContext, *, wait_seconds: float = 14_400, _test_directory: Path | None = None
) -> RunStatus:
    """Bind a new requester to one exact stopped-retained run before re-admission."""
    if not isinstance(wait_seconds, (int, float)) or isinstance(wait_seconds, bool) or not 0 < wait_seconds <= 14_400:
        raise ResourceError("Resume wait must be finite and at most four hours.")
    refusal = _gate_refusal(_test_directory, release_helper=False)  # gate point 5: before rebinding
    if refusal is not None:
        raise _gate_error(refusal, _test_directory)
    project = context.project
    with lock(project, project.project_id, shared=True), lock(project, f"run-{context.run_id}"):
        data = _load_run(context)
        if (
            data["state"] != "stopped-retained"
            or not data["retain"]
            or data["infrastructure_failure"] is not None
            or "failure_evidence" in data
            or not _consumers_quiescent(data["consumers"])
            or not _endpoints_quiescent(data["endpoints"])
            or not _resources_quiescent(data["resources"])
            or not _owned_docker_quiescent(data)
            or not any(_retained_container_targets(data))  # nothing to restart: the resumed record would be refused
        ):
            raise ResourceError("Only one healthy exact stopped-retained run may resume.")
        clock = NativeClock()
        requester = asdict(current_process(clock))
        if requester == data["requester"] or observe_process(_identity_from_json(data["requester"])) == "executing":
            raise ResourceBusy("Original requester must end before a new-terminal resume.")
        if data["guardian"] is not None and observe_process(_identity_from_json(data["guardian"])) == "executing":
            raise ResourceBusy("Original guardian must end before a new-terminal resume.")
        plan = data["resources"].get("compose")
        if plan is not None:
            files = tuple(Path(item["path"]) for item in plan["files"])
            if _existing_compose_plan(context, files, tuple(plan["services"]), plan) != plan:
                raise ResourceError("Retained Compose inputs changed before resume.")
        origin = {key: json.loads(json.dumps(data.get(key))) for key in _RESUME_ORIGIN_FIELDS}
        own = toolkit_identity_record()
        data.update(
            source_identity=own["sha256"],  # rebinding: the resumer's version now owns the run (design 3.3)
            artifact=own,
            state="prepared",
            admission="never-granted",
            requester=requester,
            queue_deadline=asdict(Deadline.after(clock, wait_seconds)),
            guardian_attempt=None,
            guardian=None,
            bootstrap_deadline=None,
            account_directory=None,
            ticket=None,
            grant=None,
            retained_grant=origin["grant"],
            resume_origin=origin,
            phase_history=[],
            withdrawal=None,
            leave_ack=None,
            recovery_actor=None,
            release_authority=None,
            test_outcome=None,
            cleanup_outcome=None,
            recovery_outcome=None,
            quiescent=False,
            endpoints={},
            consumers={},
            pending_operation=None,
            last_operation=None,
            admission_observation=None,
            infrastructure_failure=None,
        )
        data.pop("hold_timeline", None)  # the resumed generation writes its own timeline at its own grant
        data["revision"] += 1
        atomic_json(context.record, data)
        return run_status(context)


def _restore_stopped_retained(data: dict) -> None:
    """Rollback an ungranted resume without changing retained Docker data."""
    origin = data.get("resume_origin")
    if not isinstance(origin, dict) or set(origin) != set(_RESUME_ORIGIN_FIELDS):
        raise ResourceError("Retained resume rollback lacks its original authority.")
    revision = data["revision"] + 1
    data.update(origin)
    data.update(
        state="stopped-retained",
        admission="revoked",
        revision=revision,
        retained_grant=None,
        resume_origin=None,
        pending_operation=None,
    )
    data.pop("failure_evidence", None)  # a resumed generation's evidence is not the retained run's (design 4.3, rule 5)
    data.pop("hold_timeline", None)  # dropped at resume preparation; never restored by the rollback (design 3.2)


def _valid_clock_origin(origin) -> bool:
    if not isinstance(origin, str):
        return False
    kind, separator, remainder = origin.partition(":")
    if not separator:
        return False
    if kind == "darwin-continuous":
        return bool(UUID.fullmatch(remainder))
    if kind == "linux-boottime":
        boot, separator, namespace = remainder.partition(":")
        return bool(separator and UUID.fullmatch(boot) and re.fullmatch(r"time:\[[1-9][0-9]*\]", namespace))
    return False


def _identity_from_json(data: dict) -> ProcessIdentity:
    if not isinstance(data, dict) or set(data) != set(ProcessIdentity.__dataclass_fields__):
        raise ResourceError("Incomplete process identity.")
    origin = data["origin"]
    birth = data["birth"]
    token = data["audit_token"]
    if not _valid_clock_origin(origin):
        raise ResourceError("Invalid process clock origin.")
    if (
        type(data["pid"]) is not int
        or data["pid"] <= 0
        or not isinstance(birth, list)
        or not all(type(value) is int and value >= 0 for value in birth)
    ):
        raise ResourceError("Invalid process birth identity.")
    if origin.startswith("darwin-continuous:"):
        if (
            len(birth) != 2
            or birth[1] >= 1_000_000
            or not isinstance(token, list)
            or len(token) != 8
            or not all(type(value) is int and 0 <= value < 2**32 for value in token)
            or token[5] != data["pid"]
        ):
            raise ResourceError("Invalid native audit identity.")
    elif len(birth) != 1 or token is not None:
        raise ResourceError("Invalid Linux process identity.")
    return ProcessIdentity(**data)


def _deadline_from_json(data: dict) -> Deadline:
    if not isinstance(data, dict) or set(data) != set(Deadline.__dataclass_fields__):
        raise ResourceError("Incomplete deadline.")
    if (
        not _valid_clock_origin(data["origin"])
        or not all(type(data[key]) is int and data[key] >= 0 for key in ("created_ns", "duration_ns", "expires_ns"))
        or data["duration_ns"] == 0
        or data["expires_ns"] != data["created_ns"] + data["duration_ns"]
    ):
        raise ResourceError("Invalid finite deadline.")
    return Deadline(**data)


def _validate_compose_plan(plan: dict) -> None:
    if not isinstance(plan, dict) or set(plan) not in (
        {"project", "files", "services", "labels"},
        {"project", "files", "services", "completed_services", "labels"},
    ):
        raise ResourceError("Compose operation plan is incomplete.")
    if not isinstance(plan["project"], str) or not NAME.fullmatch(plan["project"]):
        raise ResourceError("Compose project identity is invalid.")
    if (
        not isinstance(plan["files"], list)
        or not plan["files"]
        or any(
            not isinstance(item, dict)
            or set(item) != {"path", "sha256"}
            or not isinstance(item["path"], str)
            or not Path(item["path"]).is_absolute()
            or not isinstance(item["sha256"], str)
            or not re.fullmatch(r"[0-9a-f]{64}", item["sha256"])
            for item in plan["files"]
        )
        or not isinstance(plan["services"], list)
        or not plan["services"]
        or len(set(plan["services"])) != len(plan["services"])
        or any(not isinstance(item, str) or not NAME.fullmatch(item) for item in plan["services"])
        or not isinstance(plan.get("completed_services", []), list)
        or len(set(plan.get("completed_services", []))) != len(plan.get("completed_services", []))
        or not set(plan.get("completed_services", [])) <= set(plan["services"])
        or not isinstance(plan["labels"], dict)
        or any(not isinstance(key, str) or not isinstance(value, str) for key, value in plan["labels"].items())
    ):
        raise ResourceError("Compose files, services or ownership labels are invalid.")


def _validate_container_control_plan(plan: dict) -> None:
    if (
        not isinstance(plan, dict)
        or set(plan) != {"action", "targets", "expected", "request_sha256"}
        or plan["action"] not in ("start", "stop")
        or not isinstance(plan["targets"], list)
        or not plan["targets"]
        or len(set(plan["targets"])) != len(plan["targets"])
        or any(not isinstance(target, str) or not re.fullmatch(r"[0-9a-f]{64}", target) for target in plan["targets"])
        or not isinstance(plan["expected"], dict)
        or set(plan["expected"]) != set(plan["targets"])
        or not isinstance(plan["request_sha256"], str)
        or not re.fullmatch(r"[0-9a-f]{64}", plan["request_sha256"])
        or any(
            not isinstance(item, dict)
            or set(item) != {"started_at", "restart_count"}
            or not isinstance(item["started_at"], str)
            or not item["started_at"]
            or type(item["restart_count"]) is not int
            or item["restart_count"] < 0
            for item in plan["expected"].values()
        )
    ):
        raise ResourceError("Container control operation plan is invalid.")


def _anonymous_volume(name: str, labels: dict) -> bool:
    return bool(re.fullmatch(r"[0-9a-f]{64}", name)) and labels == {"com.docker.volume.anonymous": ""}


def _volume_labels_match(name: str, labels: dict, expected: dict) -> bool:
    """Accept run labels or Docker's unnameable image-declared anonymous volume."""
    return all(labels.get(key) == value for key, value in expected.items()) or _anonymous_volume(name, labels)


def _validate_operation(data: dict) -> None:
    pending = data.get("pending_operation")
    last = data.get("last_operation")
    if isinstance(pending, dict) and pending.get("kind") == "guardian-start":
        if (
            set(pending) != {"kind", "attempt"}
            or pending["attempt"] != data.get("guardian_attempt")
            or last is not None
        ):
            raise ResourceError("Guardian startup journal is contradictory.")
        return
    if pending is not None:
        if not isinstance(pending, dict) or set(pending) != {
            "id",
            "kind",
            "generation",
            "revision",
            "actor",
            "deadline",
            "plan",
        }:
            raise ResourceError("Docker operation journal is incomplete.")
        if (
            not isinstance(pending["id"], str)
            or not UUID.fullmatch(pending["id"])
            or pending["kind"]
            not in ("compose-up", "compose-observe", "compose-mutate", "container-control", "cleanup", "retain-stop")
            or type(pending["generation"]) is not int
            or pending["generation"] != data["generation"]
            or type(pending["revision"]) is not int
            or not 0 < pending["revision"] <= data["revision"]
            or pending["actor"]
            not in (
                tuple(actor for actor in (data["requester"], data["guardian"]) if actor is not None)
                if pending["kind"] == "container-control"
                else (data["requester"],)
                if pending["kind"] in ("compose-up", "compose-observe", "compose-mutate")
                else tuple(actor for actor in (data["guardian"], data.get("recovery_actor")) if actor is not None)
            )
            or _deadline_from_json(pending["deadline"]).origin != pending["actor"]["origin"]
        ):
            raise ResourceError("Docker operation authority is contradictory.")
        (
            _validate_container_control_plan(pending["plan"])
            if pending["kind"] == "container-control"
            else _validate_compose_plan(pending["plan"])
        )
    if last is not None:
        expected_fields = {
            "id",
            "kind",
            "generation",
            "revision",
            "actor",
            "deadline",
            "plan",
            "outcome",
            "result_revision",
        }
        if isinstance(last, dict) and last.get("kind") == "container-control" and last.get("outcome") == "complete":
            expected_fields.add("generations")
        if not isinstance(last, dict) or set(last) != expected_fields:
            raise ResourceError("Completed Docker operation journal is incomplete.")
        journal = {key: last[key] for key in last if key not in ("outcome", "result_revision", "generations")}
        shadow = {**data, "pending_operation": journal, "last_operation": None}
        if last["kind"] in ("cleanup", "retain-stop"):
            shadow["recovery_actor"] = last["actor"]
        _validate_operation(shadow)
        if (
            last["outcome"] not in ("complete", "failed")
            or type(last["result_revision"]) is not int
            or not (last["revision"] < last["result_revision"] <= data["revision"])
        ):
            raise ResourceError("Completed Docker operation result is contradictory.")
        if "generations" in last and (
            not isinstance(last["generations"], dict) or set(last["generations"]) != set(last["plan"]["targets"])
        ):
            raise ResourceError("Completed container control result is contradictory.")


def _validate_resources(data: dict) -> None:
    resources = data.get("resources")
    if resources == {}:
        return
    if not isinstance(resources, dict) or set(resources) != {"compose", "containers", "networks", "volumes"}:
        raise ResourceError("Managed resource evidence is incomplete.")
    _validate_compose_plan(resources["compose"])
    authority = data.get("grant") or data.get("retained_grant")
    if not isinstance(authority, dict):
        raise ResourceError("Managed resources lack admitted daemon authority.")
    expected_labels = {PROJECT: data["project_id"], SESSION: data["session_id"], RUN: data["run_id"]}
    containers = resources["containers"]
    networks = resources["networks"]
    volumes = resources["volumes"]
    if not all(isinstance(items, dict) for items in (containers, networks, volumes)) or not any(
        (containers, networks, volumes)
    ):
        raise ResourceError("Managed Compose resources are incomplete.")
    for container_id, item in containers.items():
        if (
            not re.fullmatch(r"[0-9a-f]{64}", container_id)
            or not isinstance(item, dict)
            or set(item)
            != {
                "service",
                "created",
                "image_id",
                "memory_bytes",
                "nano_cpus",
                "restart_policy",
                "labels",
                "network_ids",
                "volume_names",
                "bindings",
                "state",
                "observation",
            }
            or not isinstance(item["service"], str)
            or item["service"] not in resources["compose"]["services"]
            or not isinstance(item["created"], str)
            or not item["created"]
            or not isinstance(item["image_id"], str)
            or not IMAGE.fullmatch(item["image_id"])
            or type(item["memory_bytes"]) is not int
            or item["memory_bytes"] <= 0
            or type(item["nano_cpus"]) is not int
            or item["nano_cpus"] <= 0
            or item["restart_policy"] != "no"
            or not isinstance(item["labels"], dict)
            or any(item["labels"].get(key) != value for key, value in expected_labels.items())
            or not isinstance(item["network_ids"], list)
            or len(set(item["network_ids"])) != len(item["network_ids"])
            or any(
                not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value) for value in item["network_ids"]
            )
            or not isinstance(item["volume_names"], list)
            or len(set(item["volume_names"])) != len(item["volume_names"])
            or any(not isinstance(value, str) or not NAME.fullmatch(value) for value in item["volume_names"])
            or not isinstance(item["bindings"], dict)
            or any(
                not isinstance(target, str)
                or not re.fullmatch(r"[1-9][0-9]{0,4}/(?:tcp|udp)", target)
                or not isinstance(bindings, list)
                or any(
                    not isinstance(binding, dict)
                    or set(binding) != {"host_ip", "host_port"}
                    or not isinstance(binding["host_ip"], str)
                    or type(binding["host_port"]) is not int
                    or not 0 < binding["host_port"] <= 65535
                    for binding in bindings
                )
                for target, bindings in item["bindings"].items()
            )
            or item["state"] not in ("running", "stopped", "removed")
            or not isinstance(item["observation"], dict)
        ):
            raise ResourceError("Managed container identity is contradictory.")
        try:
            observation = ContainerObservation(**item["observation"])
        except TypeError as error:
            raise ResourceError("Managed container observation is incomplete.") from error
        if (
            set(item["observation"]) != set(ContainerObservation.__dataclass_fields__)
            or observation.container_id != container_id
            or observation.daemon_id != authority.get("daemon_id")
            or not isinstance(observation.started_at, str)
            or not observation.started_at
            or type(observation.tick_ns) is not int
            or observation.tick_ns < 0
            or type(observation.memory_usage_bytes) is not int
            or observation.memory_usage_bytes < 0
            or type(observation.memory_limit_bytes) is not int
            or observation.memory_limit_bytes != item["memory_bytes"]
            or type(observation.oom_events) is not int
            or observation.oom_events < 0
            or type(observation.oom_killed) is not bool
            or type(observation.restart_count) is not int
            or observation.restart_count < 0
        ):
            raise ResourceError("Managed container observation belongs to another authority.")
    for network_id, item in networks.items():
        if (
            not re.fullmatch(r"[0-9a-f]{64}", network_id)
            or not isinstance(item, dict)
            or set(item) != {"name", "created", "labels", "state"}
            or not isinstance(item["name"], str)
            or not NAME.fullmatch(item["name"])
            or not isinstance(item["created"], str)
            or not item["created"]
            or not isinstance(item["labels"], dict)
            or any(item["labels"].get(key) != value for key, value in expected_labels.items())
            or item["state"] not in ("present", "removed")
        ):
            raise ResourceError("Managed network identity is contradictory.")
    for name, item in volumes.items():
        if (
            not NAME.fullmatch(name)
            or not isinstance(item, dict)
            or set(item) != {"created_at", "driver", "labels", "state"}
            or not isinstance(item["created_at"], str)
            or not item["created_at"]
            or not isinstance(item["driver"], str)
            or not item["driver"]
            or not isinstance(item["labels"], dict)
            or not _volume_labels_match(name, item["labels"], expected_labels)
            or item["state"] not in ("present", "removed")
        ):
            raise ResourceError("Managed volume identity is contradictory.")
    if any(network_id not in networks for item in containers.values() for network_id in item["network_ids"]) or any(
        name not in volumes for item in containers.values() for name in item["volume_names"]
    ):
        raise ResourceError("Container attachment evidence references an unrecorded resource.")


def _resources_quiescent(resources: dict) -> bool:
    return resources == {} or (
        (
            all(item["state"] == "removed" for item in resources["containers"].values())
            and all(item["state"] == "removed" for item in resources["networks"].values())
            and all(item["state"] == "removed" for item in resources["volumes"].values())
        )
        or (
            bool(resources["containers"])
            and all(item["state"] == "stopped" for item in resources["containers"].values())
        )
    )


def _resources_removed(resources: dict) -> bool:
    return resources == {} or (
        all(item["state"] == "removed" for item in resources["containers"].values())
        and all(item["state"] == "removed" for item in resources["networks"].values())
        and all(item["state"] == "removed" for item in resources["volumes"].values())
    )


def _validate_endpoints(data: dict) -> None:
    endpoints = data.get("endpoints")
    if not isinstance(endpoints, dict):
        raise ResourceError("Endpoint registry is invalid.")
    for service, history in endpoints.items():
        if not isinstance(service, str) or not NAME.fullmatch(service) or not isinstance(history, list) or not history:
            raise ResourceError("Endpoint service/history is invalid.")
        active_namespaces: set[str | None] = set()
        previous_revision = 0
        for item in history:
            if not isinstance(item, dict) or set(item) != {
                *Endpoint.__dataclass_fields__,
                "active",
                "retired_revision",
            }:
                raise ResourceError("Endpoint evidence is incomplete.")
            try:
                endpoint = Endpoint(**{key: item[key] for key in Endpoint.__dataclass_fields__})
            except TypeError as error:
                raise ResourceError("Endpoint evidence has invalid fields.") from error
            if (
                endpoint.service != service
                or not NAME.fullmatch(endpoint.protocol)
                or not isinstance(endpoint.host, str)
                or not endpoint.host
                or type(endpoint.port) is not int
                or not 0 < endpoint.port <= 65535
                or (item["active"] and _endpoint_container(data, endpoint.container_id) is None)
                or type(endpoint.container_port) is not int
                or not 0 < endpoint.container_port <= 65535
                or endpoint.audience not in ("host", "container")
                or (
                    endpoint.namespace is not None
                    and (not isinstance(endpoint.namespace, str) or not NAME.fullmatch(endpoint.namespace))
                )
                or endpoint.generation != data["generation"]
                or type(endpoint.revision) is not int
                or not previous_revision < endpoint.revision <= data["revision"]
                or type(item["active"]) is not bool
                or (
                    item["retired_revision"] is not None
                    and (
                        type(item["retired_revision"]) is not int
                        or not endpoint.revision < item["retired_revision"] <= data["revision"]
                    )
                )
                or item["active"] == (item["retired_revision"] is not None)
            ):
                raise ResourceError("Endpoint identity/revision is contradictory.")
            if endpoint.audience == "host":
                try:
                    if not ipaddress.ip_address(endpoint.host).is_loopback:
                        raise ResourceError("Managed host endpoint is not loopback-bound.")
                except ValueError as error:
                    raise ResourceError("Managed host endpoint address is invalid.") from error
            elif not NAME.fullmatch(endpoint.host):
                raise ResourceError("Managed container endpoint alias is invalid.")
            if item["active"]:
                if endpoint.namespace in active_namespaces:
                    raise ResourceError("Endpoint namespace has multiple active revisions.")
                active_namespaces.add(endpoint.namespace)
            previous_revision = endpoint.revision


def _endpoints_quiescent(endpoints: dict) -> bool:
    return all(not item["active"] for history in endpoints.values() for item in history)


def _retire_endpoints(endpoints: dict, revision: int) -> None:
    for history in endpoints.values():
        for endpoint in history:
            if endpoint["active"]:
                endpoint.update(active=False, retired_revision=revision)


def _host_observation(data: dict) -> "HostMemoryObservation":
    if not isinstance(data, dict) or set(data) != set(HostMemoryObservation.__dataclass_fields__):
        raise ResourceError("Host memory observation is incomplete.")
    try:
        observation = HostMemoryObservation(**data)
    except TypeError as error:
        raise ResourceError("Host memory observation is invalid.") from error
    if (
        not isinstance(observation.origin, str)
        or not observation.origin
        or any(
            type(value) is not int or value < 0
            for value in (
                observation.tick_ns,
                observation.physical_bytes,
                observation.headroom_bytes,
                observation.swap_used_bytes,
                observation.swapout_bytes,
                observation.page_size,
            )
        )
        or observation.physical_bytes <= 0
        or observation.headroom_bytes > observation.physical_bytes
        or observation.page_size <= 0
        or not isinstance(observation.raw, dict)
        or any(not isinstance(key, str) or type(value) is not int for key, value in observation.raw.items())
    ):
        raise ResourceError("Host memory observation is contradictory.")
    return observation


def _validate_consumers(data: dict) -> None:
    consumers = data.get("consumers")
    if not isinstance(consumers, dict):
        raise ResourceError("Consumer registry is invalid.")
    for consumer_id, item in consumers.items():
        fields = {
            "id",
            "generation",
            "revision",
            "services",
            "identity",
            "stop_on_root_loss",
            "state",
            "finished_revision",
        }
        if (
            not UUID.fullmatch(consumer_id)
            or not isinstance(item, dict)
            or set(item) not in (fields, fields | {"docker_borrow"})
            or item["id"] != consumer_id
            or item["generation"] != data["generation"]
            or type(item["revision"]) is not int
            or not 0 < item["revision"] <= data["revision"]
            or not isinstance(item["services"], list)
            or not item["services"]
            or len(set(item["services"])) != len(item["services"])
            or any(service not in data["closure"] for service in item["services"])
            or type(item["stop_on_root_loss"]) is not bool
            or not item["stop_on_root_loss"]
            or type(item.get("docker_borrow", False)) is not bool
            or (item.get("docker_borrow", False) and item["services"] != data["closure"])
            or item["state"] not in ("active", "finished")
        ):
            raise ResourceError("Consumer identity or authority is contradictory.")
        _identity_from_json(item["identity"])
        if item["identity"] in (data["requester"], data["guardian"]):
            raise ResourceError("Consumer cannot impersonate the requester or guardian.")
        finished = item["finished_revision"]
        if (item["state"] == "active") != (finished is None) or (
            finished is not None and (type(finished) is not int or not item["revision"] < finished <= data["revision"])
        ):
            raise ResourceError("Consumer terminal evidence is contradictory.")


def _consumers_quiescent(consumers: dict) -> bool:
    return all(item["state"] == "finished" for item in consumers.values())


def quiescence_blockers(data: dict) -> tuple[str, ...]:
    """Return the ordered code of every quiescence clause a validated run record fails.

    An empty tuple means the run is complete and exactly quiescent. Every clause is evaluated, so one
    record reports all of its blockers. Callers pass a record that already passed `_load_run`.
    """
    clauses = (
        ("run-retained" if data["state"] == "stopped-retained" else "run-nonterminal", data["state"] == "complete"),
        ("run-cleanup-pending", data["cleanup_outcome"] in {"complete", "incomplete"}),
        ("run-recovery-incomplete", data.get("recovery_outcome") == "complete"),
        ("run-not-quiescent", data.get("quiescent") is True),
        ("run-operation-pending", data["pending_operation"] is None),
        ("run-receipts-open", _docker_receipts_terminal(data)),
        ("run-consumers-active", _consumers_quiescent(data["consumers"])),
        ("run-resources-present", _resources_removed(data["resources"])),
        ("run-owned-docker-present", _owned_docker_removed(data)),
        ("run-endpoints-active", _endpoints_quiescent(data["endpoints"])),
    )
    return tuple(code for code, holds in clauses if not holds)


def completed_run_is_quiescent(data: dict) -> bool:
    """Return whether a validated complete run has exact terminal evidence."""
    return not quiescence_blockers(data)


def finalization_pending_shape(data: dict) -> bool:
    """Return whether a run record is exactly the legacy finalization-pending shape.

    The old resource-grant recovery writer finished a withdrawn run whose requester died before acknowledging with
    ``cleanup_outcome`` still ``pending``. Every other quiescence clause holds in that shape: the run is complete,
    revoked, non-retained, recovered by a third actor (``recovery_actor``) without any ``leave_ack``, nothing is
    pending, open or present, and the private root is gone. Anything else, including a retained or acknowledged run,
    is not it.
    """
    root = Path(data["private_root"])
    return (
        data["admission"] == "revoked"
        and data["generation"] >= 1
        and data["retain"] is False
        and data["cleanup_outcome"] == "pending"
        and data["withdrawal"] is not None
        and data["recovery_actor"] is not None
        and data["leave_ack"] is None
        and quiescence_blockers(data) == ("run-cleanup-pending",)
        and not (root.exists() or root.is_symlink())
    )


def _is_release_shape(data: dict) -> bool:
    """Return whether a record is an interrupted retained release: the target's resources are removed, not stopped."""
    return (
        data["state"] == "stopping"
        and data["admission"] == "revoked"
        and data["retain"]
        and data["recovery_actor"] is not None
        and data["recovery_outcome"] == "pending"
        and data["cleanup_outcome"] in ("pending", "incomplete")
        and data["quiescent"] is False
    )


def derive_recovery_phase(data: dict) -> str | None:
    """Return the last recovery phase (design 3.6) a run record proves complete, or None before any recovery write.

    ``authorize`` leaves no durable trace, so it is never derived. A retained run keeps its private root and its
    resources only need to be quiescent, so its resources phase is judged by quiescence and the root is not examined.
    The phase is reported to operators and compared with a worker's stored progress; it is not authority.
    """
    if data.get("recovery_actor") is None:
        return None
    if data.get("recovery_outcome") == "complete":
        return "publish"
    if data["pending_operation"] is not None:
        return "claim"
    if not _docker_receipts_terminal(data):
        return "operation"
    if not _consumers_quiescent(data["consumers"]):
        return "receipts"
    retained = data["retain"] and not _is_release_shape(data)
    resources_done = (
        _resources_quiescent(data["resources"]) and _owned_docker_quiescent(data)
        if retained
        else _resources_removed(data["resources"]) and _owned_docker_removed(data)
    )
    if not resources_done:
        return "consumers"
    root = Path(data["private_root"])
    if not retained and (root.exists() or root.is_symlink()):
        return "resources"
    return "private-root"


def _load_run(context: RunContext) -> dict:
    data = _read_private_json(context.record)
    try:
        return _validated_run(context, data)
    except (TypeError, AttributeError, KeyError) as error:
        # Valid JSON of the wrong shape: the validators hash, match and index fields before every one is type-checked.
        # An unreadable field means an invalid record, never a crash of the reader (design D-ISO).
        raise ResourceError("Run record has a field of the wrong type.") from error


def _validated_run(context: RunContext, data: dict) -> dict:
    fields = set(data)
    if "failure_evidence" in data:
        validate_failure_evidence(data["failure_evidence"])
        fields.discard("failure_evidence")  # validated and retained; never injected when absent
    if "hold_timeline" in data:
        validate_hold_timeline(data["hold_timeline"])
        fields.discard("hold_timeline")  # validated and retained; never injected when absent (one-way boundary)
    if "sharing" in data:
        if data["sharing"] != {"mode": AdmissionMode.SHARED}:
            raise ResourceError("Run record carries an invalid sharing key.")
        fields.discard("sharing")  # only shared generations carry it; never injected when absent (S10)
    if fields == _RUN_FIELDS - {"release_authority"}:
        data["release_authority"] = None
    elif fields != _RUN_FIELDS:
        raise ResourceError("Run record has unknown or missing fields.")
    origin = data.get("resume_origin")
    if isinstance(origin, dict) and set(origin) == set(_RESUME_ORIGIN_FIELDS) - {"release_authority"}:
        origin["release_authority"] = None
    if type(data.get("version")) is not int or any(
        data.get(key) != value
        for key, value in {
            "version": 1,
            "run_id": context.run_id,
            "project_id": context.project.project_id,
            "project_root": str(context.project.root),
            "session_id": context.session_id,
            "private_root": str(context.private_root),
        }.items()
    ):
        raise ResourceError("Run identity does not match its original context.")
    try:
        request = RunRequest(
            project_root=context.project.root,
            parent_session=context.session_id,
            **{key: data[key] for key in ("entrypoint", "purpose", "services", "closure", "profile", "mode", "retain")},
        )
    except KeyError as error:
        raise ResourceError("Run request metadata is incomplete.") from error
    _validate_request(request)
    _phase_budgets(data.get("phases", {}), data.get("initial_phase"))
    if data.get("topology") not in (None, "desktop", "native", "remote"):
        raise ResourceError("Invalid declared daemon topology.")
    if data.get("phases") and data["topology"] is None:
        raise ResourceError("Configured phase budgets require a declared daemon topology.")
    if type(data.get("synthetic_session")) is not bool:
        raise ResourceError("Run session provenance is incomplete.")
    for key in ("generation", "revision"):
        if type(data.get(key)) is not int or data[key] < 0:
            raise ResourceError("Invalid generation/revision.")
    if data.get("protocol") not in _RECORD_PROTOCOLS or any(
        type(value) is not int for value in data["protocol"].values()
    ):
        raise ResourceError("Unsupported lifecycle protocol.")
    if not isinstance(data.get("source_identity"), str) or not re.fullmatch(r"[0-9a-f]{64}", data["source_identity"]):
        raise ResourceError("Invalid runtime source identity.")
    requester = _identity_from_json(cast(dict, data.get("requester")))
    if data.get("recovery_actor") is not None:
        _identity_from_json(data["recovery_actor"])
        if data["state"] not in ("stopping", "complete", "stopped-retained"):
            raise ResourceError("Recovery actor exists outside recovery state.")
    release_authority = data.get("release_authority")
    if release_authority is not None:
        if not isinstance(release_authority, dict):
            raise ResourceError("Retained release authority is invalid.")
        run_id = release_authority.get("run_id")
        expected_record = context.project.state / "runs" / f"{run_id}.json"
        if (
            set(release_authority) != {"generation", "record", "requester", "revision", "run_id"}
            or type(release_authority["generation"]) is not int
            or release_authority["generation"] != data["generation"]
            or type(release_authority["revision"]) is not int
            or not 0 < release_authority["revision"] <= data["revision"]
            or not isinstance(run_id, str)
            or not UUID.fullmatch(run_id)
            or release_authority["record"] != str(expected_record)
            or data["state"] != "stopping"
            or (
                release_authority["revision"] == data["revision"]
                and release_authority["requester"] != data.get("recovery_actor")
            )
        ):
            raise ResourceError("Retained release authority is invalid.")
        _identity_from_json(release_authority["requester"])
    deadline = _deadline_from_json(cast(dict, data.get("queue_deadline")))
    if deadline.origin != requester.origin:
        raise ResourceError("Requester and deadline origins differ.")
    _validate_consumers(data)
    validate_docker_state(data)
    _validate_resources(data)
    _validate_endpoints(data)
    _validate_operation(data)
    prepared = (data.get("state"), data.get("admission"), data.get("test_outcome"), data.get("cleanup_outcome"))
    resuming = data.get("retained_grant") is not None
    if resuming:
        _validate_resume_preparation(data)
    elif data["generation"] == 0 and (
        data.get("grant") is not None
        or prepared
        not in (
            ("prepared", "never-granted", None, None),
            ("queued", "never-granted", None, None),
            ("admitting", "never-granted", None, None),
            ("complete", "revoked", "cancelled", "pending"),
            ("complete", "revoked", "cancelled", "complete"),
        )
    ):
        raise ResourceError("Unsupported or contradictory admission history.")
    if data["generation"] != 0 and not resuming:
        _validate_published_grant(data)
    elif "hold_timeline" in data:  # the key exists only beside a published grant (design 3.2, I11)
        raise ResourceError("Run record hold timeline is invalid.")
    attempt = data.get("guardian_attempt")
    if attempt is None:
        if any(
            data.get(key) is not None
            for key in ("ticket", "guardian", "pending_operation", "bootstrap_deadline", "account_directory")
        ) or data["state"] in ("queued", "admitting"):
            raise ResourceError("Unbound preparation has contradictory enrollment data.")
    else:
        if not isinstance(attempt, str) or not UUID.fullmatch(attempt):
            raise ResourceError("Invalid guardian attempt identity.")
        bootstrap = _deadline_from_json(cast(dict, data.get("bootstrap_deadline")))
        if bootstrap.origin != requester.origin:
            raise ResourceError("Guardian bootstrap origin differs from requester.")
        if data.get("guardian") is not None:
            _identity_from_json(data["guardian"])
        if data["state"] == "prepared":
            if (
                data.get("pending_operation") != {"kind": "guardian-start", "attempt": attempt}
                or data["ticket"] is not None
            ):
                raise ResourceError("Invalid pending guardian startup.")
        elif data["state"] in ("queued", "admitting", "active", "stopping"):
            if (
                type(data.get("ticket")) is not int
                or data["ticket"] <= 0
                or data.get("guardian") is None
                or not isinstance(data.get("account_directory"), str)
                or not Path(data["account_directory"]).is_absolute()
                or (data["state"] in ("queued", "admitting") and data.get("pending_operation") is not None)
            ):
                raise ResourceError("Invalid guardian enrollment.")
        else:
            if data["admission"] != "revoked" or data.get("pending_operation") is not None:
                raise ResourceError("Guardian attempt needs explicit terminal reconciliation.")
            if data["ticket"] is not None and (type(data["ticket"]) is not int or data["ticket"] <= 0):
                raise ResourceError("Invalid revoked ticket identity.")
    return data


def _requirements_identity(data: dict) -> str:
    """Bind the frozen request/profile; phase progress must not rewrite these inputs."""
    fields = (
        "entrypoint",
        "purpose",
        "services",
        "closure",
        "profile",
        "topology",
        "phases",
        "initial_phase",
        "mode",
        "retain",
        "external_resources",
    )  # the toolkit's own identity is not a requirement: a retained run resumes under the version that is current
    return hashlib.sha256(json.dumps({key: data[key] for key in fields}, sort_keys=True).encode()).hexdigest()


_RESUME_ORIGIN_FIELDS = (
    "source_identity",
    "artifact",
    "requester",
    "queue_deadline",
    "guardian_attempt",
    "guardian",
    "bootstrap_deadline",
    "account_directory",
    "ticket",
    "grant",
    "phase_history",
    "withdrawal",
    "leave_ack",
    "recovery_actor",
    "release_authority",
    "test_outcome",
    "cleanup_outcome",
    "recovery_outcome",
    "quiescent",
    "endpoints",
    "consumers",
    "last_operation",
    "admission_observation",
    "infrastructure_failure",
)


def _retained_container_targets(data: dict) -> tuple[tuple[str, ...], tuple[str, ...]]:
    completed_services = set(data["resources"].get("compose", {}).get("completed_services", ()))
    managed = tuple(
        container_id
        for container_id, item in data["resources"].get("containers", {}).items()
        if item["state"] == "stopped" and item["service"] not in completed_services
    )
    owned = tuple(
        container_id
        for container_id, item in data["owned_resources"]["containers"].items()
        if item["state"] == "stopped"
        and "service" in item["evidence"]
        and not item["evidence"].get("completed_service", False)
    )
    return managed, owned


def _validate_resume_preparation(data: dict) -> None:
    """Validate a queued new requester without weakening retained ownership."""
    origin = data.get("resume_origin")
    retained_grant = data.get("retained_grant")
    managed_targets, owned_targets = _retained_container_targets(data)
    if (
        not isinstance(origin, dict)
        or set(origin) != set(_RESUME_ORIGIN_FIELDS)
        or retained_grant != origin.get("grant")
        or not isinstance(retained_grant, dict)
        or retained_grant.get("generation") != data["generation"]
        or retained_grant.get("daemon_id") is None
        or retained_grant.get("requirements_identity") != _requirements_identity(data)
        or data["state"] not in ("prepared", "queued", "admitting")
        or data["admission"] != "never-granted"
        or not data["retain"]
        or data.get("grant") is not None
        or data.get("withdrawal") is not None
        or data.get("leave_ack") is not None
        or data.get("recovery_actor") is not None
        or data.get("release_authority") is not None
        or data.get("test_outcome") is not None
        or data.get("cleanup_outcome") is not None
        or data.get("recovery_outcome") is not None
        or data.get("quiescent") is not False
        or data.get("endpoints") != {}
        or data.get("consumers") != {}
        or data.get("last_operation") is not None
        or data.get("admission_observation") is not None
        or data.get("infrastructure_failure") is not None
        or not (managed_targets or owned_targets)
        or not _resources_quiescent(data["resources"])
        or not _owned_docker_quiescent(data)
    ):
        raise ResourceError("Retained resume preparation is contradictory.")


def _validate_published_grant(data: dict) -> None:
    grant = data.get("grant")
    if (
        data["generation"] < 1
        or (data["admission"], data["state"])
        not in (
            ("published", "active"),
            ("published", "stopping"),
            ("revoked", "stopping"),
            ("revoked", "complete"),
            ("revoked", "stopped-retained"),
        )
        or not isinstance(grant, dict)
        or set(grant)
        != {
            "generation",
            "revision",
            "attempt",
            "ticket",
            "requester",
            "guardian",
            "daemon_id",
            "scope",
            "phase",
            "requirements_identity",
        }
    ):
        raise ResourceError("Unsupported or incomplete published grant history.")
    expected = {
        "generation": data["generation"],
        "attempt": data["guardian_attempt"],
        "ticket": data["ticket"],
        "requester": data["requester"],
        "guardian": data["guardian"],
    }
    if (
        any(grant[key] != value for key, value in expected.items())
        or type(grant["generation"]) is not int
        or type(grant["ticket"]) is not int
        or type(grant["revision"]) is not int
        or not 0 < grant["revision"] <= data["revision"]
        or not isinstance(grant["daemon_id"], str)
        or not NAME.fullmatch(grant["daemon_id"])
        or grant["scope"] not in ("managed", "host-proof")
        or not data["phases"]
        or grant["phase"] not in data["phases"]
        or grant["requirements_identity"] != _requirements_identity(data)
    ):
        raise ResourceError("Published grant differs from original actor/phase authority.")
    history = data.get("phase_history")
    admission = data.get("admission_observation")
    if (
        not isinstance(history, list)
        or not history
        or not isinstance(history[-1], dict)
        or history[-1].get("phase") != grant["phase"]
        or any(
            not isinstance(item, dict)
            or set(item) != {"phase", "revision", "memory"}
            or item["phase"] not in data["phases"]
            or type(item["revision"]) is not int
            or not grant["revision"] <= item["revision"] <= data["revision"]
            or not isinstance(item["memory"], dict)
            for item in history
        )
        or any(left["revision"] >= right["revision"] for left, right in pairwise(history))
    ):
        raise ResourceError("Published phase history is incomplete or contradictory.")
    if (
        not isinstance(admission, dict)
        or set(admission) != {"capacity", "memory", "window_start", "samples"}
        or not isinstance(admission["capacity"], dict)
        or not isinstance(admission["samples"], list)
        or not admission["samples"]
    ):
        raise ResourceError("Admission observation history is incomplete.")
    memory = _host_observation(admission["memory"])
    timeline = data.get("hold_timeline")
    if timeline is not None and (
        not _valid_clock_origin(timeline["origin"])
        or (timeline["release_origin"] is not None and not _valid_clock_origin(timeline["release_origin"]))
        or timeline["origin"] != memory.origin
        or timeline["granted_ns"] < memory.tick_ns
    ):
        raise ResourceError("Run record hold timeline is invalid.")
    window_start = _host_observation(admission["window_start"])
    samples = [_host_observation(item) for item in admission["samples"]]
    for item in history:
        _host_observation(item["memory"])
    if (
        window_start.origin != memory.origin
        or window_start.tick_ns > memory.tick_ns
        or memory.tick_ns > samples[0].tick_ns
        or any(left.tick_ns >= right.tick_ns for left, right in pairwise(samples))
    ):
        raise ResourceError("Admission memory chronology is contradictory.")
    canonical = canonical_account()
    if (grant["scope"] == "managed") != (canonical is not None and data["account_directory"] == str(canonical)):
        raise ResourceError("Published grant has contradictory account execution scope.")
    withdrawal = data.get("withdrawal")
    if data["state"] == "active":
        if withdrawal is not None or data["test_outcome"] is not None or data["cleanup_outcome"] is not None:
            raise ResourceError("Active execution has contradictory withdrawal/outcome evidence.")
    elif withdrawal is not None:
        if (
            not isinstance(withdrawal, dict)
            or set(withdrawal) != {"generation", "revision", "requester", "outcome", "deadline"}
            or type(withdrawal["generation"]) is not int
            or withdrawal["generation"] != data["generation"]
            or type(withdrawal["revision"]) is not int
            or not grant["revision"] < withdrawal["revision"] <= data["revision"]
            or withdrawal["requester"] != data["requester"]
            or withdrawal["outcome"] not in ("passed", "failed", "cancelled")
            or data["test_outcome"] != withdrawal["outcome"]
            or data["cleanup_outcome"] not in ("pending", "incomplete", "complete")
            or _deadline_from_json(withdrawal["deadline"]).origin != data["requester"]["origin"]
        ):
            raise ResourceError("Withdrawal does not match its original grant/requester.")
    elif data["state"] != "complete" or data["test_outcome"] != "cancelled":
        raise ResourceError("Stopping execution lacks its root's withdrawal.")
    if data["state"] in ("complete", "stopped-retained"):
        if data["state"] == "complete":
            if not completed_run_is_quiescent(data) and not finalization_pending_shape(data):
                raise ResourceError("Completed grant lacks verified quiescence.")
        else:
            if (
                data.get("recovery_outcome") != "complete"
                or data.get("quiescent") is not True
                or not data["retain"]
                or any(item["state"] != "stopped" for item in data["resources"].get("containers", {}).values())
                or not _owned_docker_quiescent(data)
            ):
                raise ResourceError("Retained grant lacks exact stopped resources.")


def run_status(context: RunContext) -> RunStatus:
    """Read an exact run; malformed or substituted identity raises ResourceError."""
    return _status_of(_load_run(context))


def _status_of(data: dict) -> RunStatus:
    """Build the public status from a loaded run record; a record without the status fields raises ResourceError."""
    try:
        return RunStatus(**{key: data[key] for key in RunStatus.__dataclass_fields__})
    except (KeyError, TypeError) as error:
        raise ResourceError("Run status is incomplete.") from error


def _leave_acknowledged(data: dict, request_revision: int) -> bool:
    acknowledgment = data.get("leave_ack")
    if acknowledgment is None:
        return False
    if (
        not isinstance(acknowledgment, dict)
        or set(acknowledgment) != {"generation", "request_revision", "revision"}
        or any(type(value) is not int for value in acknowledgment.values())
        or acknowledgment["generation"] != data["generation"]
        or acknowledgment["request_revision"] != request_revision
        or not request_revision < acknowledgment["revision"] <= data["revision"]
        or data["cleanup_outcome"] != "complete"
        or data["state"] not in ("complete", "stopped-retained")
        or data["admission"] != "revoked"
    ):
        raise ResourceError("Leave acknowledgment does not prove this request's verified quiescence.")
    return True


def _leave_incomplete(context: RunContext, revision: int, reason: str, deadline_ns: int | None) -> bool:
    """Re-read the withdrawn run under its locks: report a valid acknowledgment, else record incomplete cleanup.

    Return True when the locked re-read finds this request's valid acknowledgment (nothing is written; the caller
    finishes through ``_finish_acknowledged_leave``). A malformed acknowledgment propagates its ``ResourceError`` and
    writes nothing. An absent one records ``cleanup_outcome = incomplete`` once and raises ``IncompleteCleanup``.
    """
    project = context.project
    with (
        lock(project, project.project_id, shared=True, deadline_ns=deadline_ns),
        lock(project, f"run-{context.run_id}", deadline_ns=deadline_ns),
    ):
        data = _load_run(context)
        if _leave_acknowledged(data, revision):
            return True
        if data["withdrawal"] is None or data["withdrawal"]["revision"] != revision:
            raise ResourceError("Withdrawal identity changed before failure could be recorded.")
        if data["cleanup_outcome"] != "incomplete":
            data.update(cleanup_outcome="incomplete", infrastructure_failure=reason)
            data["revision"] += 1
            atomic_json(context.record, data)
    raise IncompleteCleanup(f"Leave cleanup incomplete: {reason}; original recovery is required.")


_RETIREMENT_GRACE_NS = 5_000_000_000
"""Time a leave allows past its recorded budget for the guardian's retirement section (in memory, never recorded)."""


def _leave_deadline_ns(withdrawal: dict) -> int:
    """Return the one absolute ``time.monotonic_ns()`` deadline of every lock a leave takes after its withdrawal.

    It is the withdrawal's remaining recorded budget plus ``_RETIREMENT_GRACE_NS``; a budget of another clock origin
    (a reboot) has no remaining time. Computed once; the recorded deadline never renews.
    """
    try:
        remaining = _deadline_from_json(withdrawal["deadline"]).remaining_ns(NativeClock())
    except ResourceError:
        remaining = 0
    return time.monotonic_ns() + remaining + _RETIREMENT_GRACE_NS


def _requester_poll_ns(data: dict) -> int:
    """Return the requester's acknowledgment poll: 1 s for a managed (canonical) grant without opening any binding.

    A private domain may shorten it (``_poll_interval_ns``); if the canonical account cannot be resolved the poll
    stays 1 s, the slow and safe direction, so production ``leave_run`` gains no new raise.
    """
    if data["grant"]["scope"] == "managed":
        return _PRODUCTION_POLL_NS
    return _poll_interval_ns(Path(data["account_directory"]), canonical=canonical_account())


def _finish_acknowledged_leave(context: RunContext, revision: int, deadline_ns: int | None) -> RunStatus:
    """Return the status of an acknowledged leave once its retirement is verified (I-LR1, I-LR2).

    The only path that returns a status after an acknowledgment. The barrier: the project and run locks, taken once,
    end the guardian's retirement section; under them the acknowledgment is required (a malformed one raises
    ``ResourceError``). Then, with the registry lock and the account metadata lock nested inside it held together
    (recovery's order), the queue entry must be absent and the index entry absent unless the run is retained; a
    present reference means the guardian died inside its section and raises ``ResourceError`` with reason
    ``retirement-incomplete`` (``run recover`` finishes it). A recorded ``infrastructure_failure`` then raises
    ``InfrastructureFailure`` instead of a status. Every lock waits at most until ``deadline_ns``. Writes nothing.
    """
    with _locked_run(context, deadline_ns):
        data = _load_run(context)
        if not _leave_acknowledged(data, revision):
            raise ResourceError("Leave acknowledgment disappeared from the withdrawn run.")
    project = context.project
    directory = Path(data["account_directory"])
    with lock(project, "registry", deadline_ns=deadline_ns):
        indexed = load(project)["runs"].get(context.run_id)
        with lock(Context(directory, directory), "metadata", deadline_ns=deadline_ns) as anchor:
            entry = _load_queue(directory, anchor).entries.get(context.run_id)
    retained = data["state"] == "stopped-retained"
    if entry is not None or (not retained and indexed is not None):
        raise ResourceError(
            "Leave acknowledged, but the guardian died before retiring the run's references; run recover finishes it.",
            reason="retirement-incomplete",
        )
    status = _status_of(data)
    if data["infrastructure_failure"] is not None:
        raise InfrastructureFailure(
            f"Managed infrastructure failed: {data['infrastructure_failure']}{_evidence_summary(data)}."
        )
    return status


def _queue_budget_ns(data: dict) -> int:
    """Return a run's original queue budget as an absolute ``time.monotonic_ns()`` lock deadline.

    The budget lives on the native clock and never renews; an origin change (a reboot) or an expired budget is an
    already-passed deadline, so the next blocking admission wait gives up at once.
    """
    try:
        remaining = _deadline_from_json(data["queue_deadline"]).remaining_ns(NativeClock())
    except ResourceError:
        remaining = 0
    return time.monotonic_ns() + remaining


@contextmanager
def _locked_run(context: RunContext, deadline_ns: int | None = None) -> Iterator[None]:
    """Hold the shared project lock, then the run lock (ranks P then R), each wait ending at ``deadline_ns``."""
    project = context.project
    with (
        lock(project, project.project_id, shared=True, deadline_ns=deadline_ns),
        lock(project, f"run-{context.run_id}", deadline_ns=deadline_ns),
    ):
        yield


def _leave_budget_seconds(data: dict) -> int:
    """Fix one finite cleanup budget from the exact registered resource set."""
    resources = data["resources"]
    containers = sum(item["state"] != "removed" for item in resources.get("containers", {}).values())
    networks = sum(item["state"] != "removed" for item in resources.get("networks", {}).values())
    volumes = sum(item["state"] != "removed" for item in resources.get("volumes", {}).values())
    direct = sum(item["state"] != "removed" for items in data["owned_resources"].values() for item in items.values())
    consumers = any(item["state"] == "active" for item in data["consumers"].values())
    return 30 + (10 if consumers else 0) + 20 * (containers + direct) + 10 * (networks + volumes)


def leave_run(lease: RunLease, outcome: str) -> RunStatus:
    """Withdraw this execution root and require its original guardian's acknowledgment and retirement.

    Orchestration: the withdrawal (state ``stopping``, the caller's outcome, the cleanup deadline) is written once under
    the project and run locks, whose waits stay unbounded; then the record is polled without locks until the guardian
    acknowledges. Every lock taken after the durable withdrawal waits at most until one deadline, the recorded
    remaining budget plus ``_RETIREMENT_GRACE_NS``. Post-condition of a returned status: the acknowledgment is durable,
    the guardian's retirement section has ended (the barrier), and with the registry and account metadata locks held
    together the queue entry is absent and the index entry is absent unless the run is retained. The guardian process
    may still be exiting; a live guardian finishes on its own, a dead one is finished by ``run recover``.

    Side effects and idempotency: the requester writes only its withdrawal. A retry by the same lease keeps the
    original withdrawal and deadline (a different outcome is refused); there is no public retry contract, and the
    public seams clear the lease on every raised leave.

    Raises:
        InfrastructureFailure: the acknowledged record carries ``infrastructure_failure`` (on every path that observes
            the acknowledgment).
        IncompleteCleanup: no acknowledgment while the guardian is lost, unobservable or past the budget; the record
            is ``incomplete``.
        ResourceError: ``retirement-incomplete`` when a queue or index reference is still present after the barrier
            (the record is terminal, nothing beyond the withdrawal was written); a malformed acknowledgment under the
            locks (nothing written); a changed lease or withdrawal identity.
        ResourceBusy: ``lock-deadline`` when a lock after the withdrawal is not free in time; the record is
            ``stopping``/``pending`` when that was before the acknowledgment, terminal when after it.
    """
    if (
        not isinstance(lease, RunLease)
        or type(lease.generation) is not int
        or lease.generation <= 0
        or type(lease.grant_revision) is not int
        or not isinstance(lease.requester, ProcessIdentity)
        or outcome not in ("passed", "failed", "cancelled")
    ):
        raise ResourceError("Invalid lease identity or closed test outcome.")
    context = lease.context
    project = context.project
    clock = NativeClock()
    actor = asdict(current_process(clock))
    with lock(project, project.project_id, shared=True), lock(project, f"run-{context.run_id}"):
        data = _load_run(context)
        if (
            data["generation"] != lease.generation
            or data["grant"]["revision"] != lease.grant_revision
            or actor != asdict(lease.requester)
            or actor != data["requester"]
        ):
            raise ResourceError("Withdrawal caller differs from its granted execution root.")
        withdrawal = data.get("withdrawal")
        if withdrawal is None:
            withdrawal = _apply_withdrawal(data, requester=actor, outcome=outcome, clock=clock)
            atomic_json(context.record, data)
            failpoint("leave-withdrawal-written")
        elif withdrawal["outcome"] != outcome:
            raise ResourceError("Withdrawal retry cannot rewrite its original test outcome.")
    revision = withdrawal["revision"]
    deadline = _deadline_from_json(withdrawal["deadline"])
    deadline_ns = _leave_deadline_ns(withdrawal)
    interval_ns = _requester_poll_ns(data)
    while True:
        data = _load_run(context)
        failpoint("leave-poll-read")
        try:
            acknowledged = _leave_acknowledged(data, revision)
        except ResourceError:
            acknowledged = _leave_incomplete(context, revision, "contradictory-acknowledgment", deadline_ns)
        if acknowledged:
            return _finish_acknowledged_leave(context, revision, deadline_ns)
        if data["cleanup_outcome"] == "incomplete":
            raise IncompleteCleanup("Leave cleanup remains incomplete; later recovery cannot certify this command.")
        try:
            guardian_state = observe_process(_identity_from_json(data["guardian"]))
            remaining = deadline.remaining_ns(clock)
        except ResourceError:
            if _leave_incomplete(context, revision, "leave-observation-unavailable", deadline_ns):
                return _finish_acknowledged_leave(context, revision, deadline_ns)
            raise  # unreachable: _leave_incomplete returns True or raises
        if guardian_state != "executing":
            if _leave_incomplete(context, revision, "guardian-lost", deadline_ns):
                return _finish_acknowledged_leave(context, revision, deadline_ns)
        if not remaining:
            failpoint("leave-deadline-branch")
            if _leave_incomplete(context, revision, "acknowledgment-deadline", deadline_ns):
                return _finish_acknowledged_leave(context, revision, deadline_ns)
        time.sleep(min(interval_ns, remaining) / 1_000_000_000)


def _record_hold_release(data: dict, release: str) -> bool:
    """Write the release into ``hold_timeline`` once; return whether the record changed (design 3.2).

    A record without the key (finalization, 0.3.12 and earlier, a rolled-back resume) is released as before and never
    gains the key. The first release wins: a retry never overwrites an earlier one. The release carries the current
    clock origin, so a release recorded on a later boot stays valid.
    """
    timeline = data.get("hold_timeline")
    if timeline is None or timeline["release"] is not None:
        return False
    clock = NativeClock()
    timeline.update(release=release, release_origin=clock.origin, released_ns=clock.read())
    return True


def _record_positive_verdict(context: RunContext, data: dict) -> None:
    """Teach the request's positive verdict at leave when the run performed a checked mutation (design 2.3).

    The caller holds the project's ``registry`` lock. A run without a creating or starting receipt teaches nothing.
    A negative verdict of the request stays (``write_verdict``): some run of it broke a rule. No-raise contract: a
    verdict is a prediction, so a failed write never fails the leave (the request stays unknown or keeps its earlier
    verdict, which admits exclusive runs).
    """
    timeline = data.get("hold_timeline")
    if not isinstance(timeline, dict) or not any(
        checked_mutation(receipt["kind"], receipt["plan"].get("subcommand"))
        for receipt in data["docker_receipts"].values()
    ):
        return
    key = _verdict_key(data)
    if key is None:
        return
    project = context.project
    clock = NativeClock()
    try:
        write_verdict(
            project.state,
            project.project_id,
            key,
            shareable=True,
            reason=None,
            run_id=data["run_id"],
            origin=clock.origin,
            tick_ns=clock.read(),
        )
    except (OSError, ResourceError):
        return


def _acknowledge_empty_leave(
    context: RunContext,
    attempt: str,
    capacity_stack: ExitStack,
    capacity: TextIO,
    *,
    _test_directory: Path | None = None,
) -> RunStatus:
    """Finish a resource-free withdrawn grant as its original live guardian.

    This narrow path proves the leave protocol before Docker resources are added.
    It deletes unused private material before publishing quiescence, then closes
    the sole capacity descriptor while account metadata is locked.
    """
    directory = account_directory() if _test_directory is None else _test_directory.resolve()
    project = context.project
    with lock(project, project.project_id, shared=True), lock(project, f"run-{context.run_id}"):
        data = _load_run(context)
        guardian = asdict(current_process(NativeClock()))
        if (
            data["state"] != "stopping"
            or data["admission"] != "published"
            or data["guardian_attempt"] != attempt
            or data["guardian"] != guardian
            or data["account_directory"] != str(directory)
            or data["withdrawal"] is None
            or data["pending_operation"] is not None
            or not _docker_receipts_terminal(data)
            or not _consumers_quiescent(data["consumers"])
            or not _endpoints_quiescent(data["endpoints"])
            or not (
                _resources_quiescent(data["resources"]) if data["retain"] else _resources_removed(data["resources"])
            )
            or not (_owned_docker_quiescent(data) if data["retain"] else _owned_docker_removed(data))
        ):
            raise ResourceError("Guardian cannot acknowledge this withdrawal as quiescent.")
        request_revision = data["withdrawal"]["revision"]
        share_unknown = (Path(data["private_root"]) / SHARE_UNKNOWN_MARKER).exists()  # read before the root goes
        if capacity.closed:
            raise ResourceError("Guardian lost its capacity descriptor before acknowledgment.")
        held = os.fstat(capacity.fileno())
        named = (directory / "capacity.lock").stat(follow_symlinks=False)
        if (held.st_dev, held.st_ino) != (named.st_dev, named.st_ino):
            raise ResourceError("Guardian capacity descriptor changed before acknowledgment.")
    retained = data["retain"]
    if not retained:
        _remove_private_root(context)
    account = Context(directory, directory)
    with lock(project, project.project_id, shared=True), lock(project, f"run-{context.run_id}"):
        data = _load_run(context)
        if (
            data["state"] != "stopping"
            or data["withdrawal"]["revision"] != request_revision
            or data["guardian"] != asdict(current_process(NativeClock()))
        ):
            raise ResourceError("Withdrawal changed before guardian acknowledgment.")
        with lock(account, "metadata") as anchor:
            state = _load_queue(directory, anchor)
            expected = _queue_reference(context, attempt, data["ticket"])
            entry = state.entries.get(context.run_id)
            if entry is None or account_queue.reference(entry) != expected:
                raise ResourceError("Account authority changed before guardian acknowledgment.")
            if entry.phase not in HOLDER_PHASES:
                raise ResourceError("Capacity reservation changed before guardian acknowledgment.")
            data.update(
                state="stopped-retained" if retained else "complete",
                admission="revoked",
                cleanup_outcome="complete",
                recovery_outcome="complete",
                quiescent=True,
            )
            _repair_grant_log(directory, state, entry, data)  # a publication append that failed is repaired first
            # The result is not needed: this write commits either way, in the write that precedes the removal (DF8).
            _record_hold_release(data, "leave")
            data["revision"] += 1
            data["leave_ack"] = {
                "generation": data["generation"],
                "request_revision": request_revision,
                "revision": data["revision"],
            }
            atomic_json(context.record, data)
            account_queue.remove(state, context.run_id)
            _save_queue(directory, state, anchor)
            _drop_lease(directory, state)
            # the turn and "this run no longer holds" become true together
            _record_turn(directory, data, _test_directory)
            capacity_stack.close()
        with lock(project, "registry"):
            registry = load(project)
            if registry["runs"].get(context.run_id) != str(context.record):
                raise ResourceError("Project authority changed before guardian acknowledgment.")
            if not share_unknown:
                _record_positive_verdict(context, data)
            if not retained:
                del registry["runs"][context.run_id]
                save(project, registry)
        return run_status(context)


def configure_run(
    context: RunContext,
    *,
    services: tuple[str, ...],
    closure: tuple[str, ...],
    phases: "dict[str, PhaseBudget]",
    initial_phase: str,
    topology: str,
) -> None:
    """Record selected requirements after collection, before guardian startup.

    Only the original requester may configure its never-enrolled preparation.
    Preserve the run, private root, original deadline and profile identity. Invalid
    selection, phase budgets or an already-started attempt raises without mutation.
    """
    if topology not in ("desktop", "native", "remote"):
        raise ResourceError("Selection must declare desktop, native or remote Docker topology.")
    if not isinstance(phases, dict) or any(not isinstance(value, PhaseBudget) for value in phases.values()):
        raise ResourceError("Selected phases require explicit typed budgets.")
    serialized = {name: asdict(value) for name, value in phases.items()}
    if not _phase_budgets(serialized, initial_phase):
        raise ResourceError("Selected infrastructure needs at least one declared phase.")
    project = context.project
    with lock(project, project.project_id, shared=True), lock(project, f"run-{context.run_id}"):
        data = _load_run(context)
        clock = NativeClock()
        if data["requester"] != asdict(current_process(clock)):
            raise ResourceError("Only the original requester can configure selection.")
        if data["state"] != "prepared" or data["guardian_attempt"] is not None:
            raise ResourceError("Requirements cannot change after guardian startup.")
        if data["source_identity"] != source_identity() or not _deadline_from_json(data["queue_deadline"]).remaining_ns(
            clock
        ):
            raise ResourceError("Runtime changed or original preparation deadline expired.")
        request = RunRequest(
            project.root,
            data["entrypoint"],
            data["purpose"],
            services,
            closure,
            data["profile"],
            parent_session=context.session_id,
            mode=data["mode"],
            retain=data["retain"],
        )
        _validate_request(request)
        data.update(
            services=list(services),
            closure=list(closure),
            phases=serialized,
            initial_phase=initial_phase,
            topology=topology,
        )
        data["revision"] += 1
        atomic_json(context.record, data)


def _context_payload(context: RunContext) -> str:
    return json.dumps(
        {
            "project_root": str(context.project.root),
            "project_state": str(context.project.state),
            "run_id": context.run_id,
            "session_id": context.session_id,
            "record": str(context.record),
            "private_root": str(context.private_root),
        },
        separators=(",", ":"),
    )


LOCATOR_FIELDS = frozenset({"project_root", "project_state", "run_id", "session_id", "record", "private_root"})


def locator_fields(value: object, invalid: ToolkitError) -> dict[str, str]:
    """Return a run locator's six string fields; fields a newer peer adds are ignored (design E3).

    Raises:
        ToolkitError: ``invalid``, when a field is missing or not a string.
    """
    locator = known_fields(value, LOCATOR_FIELDS, invalid=invalid)
    if any(not isinstance(field, str) for field in locator.values()):
        raise invalid
    return locator


def _context_from_payload(payload: str, *, validate_record: bool = True) -> RunContext:
    try:
        data = json.loads(payload, object_pairs_hook=unique_object)
    except (RecursionError, TypeError, ValueError) as error:
        raise ResourceError("Guardian context is not valid JSON.") from error
    if not isinstance(data, dict) or set(data) != LOCATOR_FIELDS or any(not isinstance(data[key], str) for key in data):
        raise ResourceError("Guardian context is incomplete.")
    project = Context(Path(data["project_root"]), Path(data["project_state"]))
    context = RunContext(
        project,
        data["run_id"],
        data["session_id"],
        Path(data["record"]),
        Path(data["private_root"]),
    )
    if validate_record:
        _load_run(context)
    return context


def load_operator_context(payload: str) -> RunContext:
    """Validate an exact persisted context supplied to an operator command."""
    return _context_from_payload(payload)


def load_run_record(context: RunContext) -> dict:
    """Return a validated run record for an application-level observation."""
    return _load_run(context)


def recorded_run_context(registry: Path, run_id: str, raw: dict) -> RunContext | None:
    """Rebuild a run's operator context from its raw record, or None when the record names no usable one."""
    if not all(isinstance(raw.get(key), str) for key in ("project_root", "session_id", "private_root")):
        return None
    if not Path(raw["project_root"]).is_absolute():
        return None
    return RunContext(
        Context(Path(raw["project_root"]), registry),
        run_id,
        raw["session_id"],
        registry / "runs" / f"{run_id}.json",
        Path(raw["private_root"]),
    )


def run_record_loads(registry: Path, run_id: str, raw: dict) -> bool:
    """Return whether a run record loads under the identity it records for itself."""
    context = recorded_run_context(registry, run_id, raw)
    if context is None:
        return False
    try:
        _load_run(context)
    except ResourceError:
        return False
    return True


def _open_guardian_log(path: Path) -> TextIO:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW, 0o600)
    stream = os.fdopen(descriptor, "a")
    observed = os.fstat(stream.fileno())
    if not stat.S_ISREG(observed.st_mode) or observed.st_uid != os.getuid() or observed.st_mode & 0o077:
        stream.close()
        raise ResourceError("Guardian log is not a private account-owned regular file.")
    return stream


def _launch_guardian(context: RunContext, attempt: str, test_directory: Path | None) -> None:
    stdout_path, stderr_path = _guardian_log_paths(context.private_root)
    stdout = _open_guardian_log(stdout_path)
    stderr = _open_guardian_log(stderr_path)
    command = _toolkit_process_command(
        "_guardian",
        "--context",
        _context_payload(context),
    )
    if test_directory is not None:
        command.extend(("--test-directory", str(test_directory.resolve())))
    try:
        child = subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=stdout,
            stderr=stderr,
            close_fds=True,
            start_new_session=True,
            env=_toolkit_child_environment(context.private_root),
        )
    except OSError as error:
        raise ResourceError("Guardian process could not start.") from error
    finally:
        stdout.close()
        stderr.close()
    # Popen retains the original wait authority while this root lives. If the root
    # dies, the detached guardian is reparented and continues protecting admission.
    threading.Thread(target=child.wait, name=f"guardian-reaper-{context.run_id}", daemon=True).start()


def enter_run(context: RunContext, *, _test_directory: Path | None = None) -> RunLease:
    """Start one guardian attempt and wait for its exact durable execution grant.

    The caller remains the original execution root. A retry by that same root waits
    on the existing attempt; it never creates another ticket or renews the deadline.
    The explicit test directory is a native host-proof seam and cannot grant Docker.
    """
    clock = NativeClock()
    actor = current_process(clock)
    data = _load_run(context)
    if data["requester"] != asdict(actor) or data["source_identity"] != source_identity():
        raise ResourceError("Only the original requester and runtime may enter this run.")
    if data["mode"] != "managed" or not data["phases"] or data["topology"] is None:
        raise ResourceError("Managed entry requires configured selected phase budgets.")
    if data["state"] == "prepared" and data["guardian_attempt"] is None:
        refusal = _gate_refusal(_test_directory, release_helper=data["purpose"] == RELEASE_PURPOSE)  # gate point 2
        if refusal is not None:
            cancel_run(context)  # a fresh preparation is cancelled and cleaned; a resume is rolled back
            raise _gate_error(refusal, _test_directory)
        if _test_directory is None and data["purpose"] != RELEASE_PURPOSE:
            _ensure_probe_image(data["topology"])
        attempt = start_guardian_attempt(context)
        _launch_guardian(context, attempt, _test_directory)
    elif data["state"] in ("prepared", "queued", "admitting"):
        attempt = data["guardian_attempt"]
    elif data["state"] == "active":
        grant = data["grant"]
        if observe_process(_identity_from_json(data["guardian"])) != "executing":
            raise ResourceError("Published grant lost its guardian before entry completed.")
        return RunLease(context, data["generation"], grant["revision"], actor)
    else:
        raise ResourceError("Run cannot enter from its terminal or stopping state.")
    deadline = _deadline_from_json(data["queue_deadline"])
    while deadline.remaining_ns(clock):
        data = _load_run(context)
        if data["state"] == "active":
            if data["guardian_attempt"] != attempt or data["source_identity"] != source_identity():
                raise ResourceError("Published grant differs from the original startup attempt.")
            if observe_process(_identity_from_json(data["guardian"])) != "executing":
                raise ResourceError("Published grant lost its guardian before entry completed.")
            return RunLease(context, data["generation"], data["grant"]["revision"], actor)
        upgraded = data["infrastructure_failure"] in ("toolkit-upgraded", "toolkit-not-installed")
        if data["state"] == "complete" and upgraded:
            _finalize_upgraded_preparation(context)
            raise _gate_error(data["infrastructure_failure"], _test_directory)
        if data["state"] == "complete":
            raise ResourceError(
                "Admission ended before grant"
                + (f": {data['infrastructure_failure']}" if data["infrastructure_failure"] else "")
                + _admission_ended_detail(data)
                + _disk_refusal_detail(context, data, _test_directory)
                + _small_vm_detail(data, _test_directory)
                + "."
            )
        guardian = data.get("guardian")
        if guardian is not None and observe_process(_identity_from_json(guardian)) != "executing":
            try:
                revoke_ungranted(context, _test_directory=_test_directory, _deadline_ns=_queue_budget_ns(data))
            except ResourceError as error:
                if not (isinstance(error, ResourceBusy) or error.reason == "claim-held"):
                    raise
                # a live worker holds this run's claim and will revoke the same run: wait, do not fail
                time.sleep(min(100_000_000, deadline.remaining_ns(clock)) / 1_000_000_000)
            continue
        bootstrap = data.get("bootstrap_deadline")
        if guardian is None and bootstrap is not None and not _deadline_from_json(bootstrap).remaining_ns(clock):
            revoke_ungranted(context, _test_directory=_test_directory)
            continue
        time.sleep(min(100_000_000, deadline.remaining_ns(clock)) / 1_000_000_000)
    data = _load_run(
        context
    )  # the one re-read after the deadline: the guardian may have written its evidence meanwhile
    if data["generation"]:
        raise ResourceError("Admission response missed a published grant; original recovery is required.")
    if data.get("guardian") == asdict(actor):
        raise ResourceError("Requester cannot act as its own guardian.")
    detail = _admission_ended_detail(data)
    if not detail and data["state"] != "complete":
        detail = " (admission diagnostics not yet persisted by the guardian)"
    raise ResourceError(f"Original admission deadline expired before a grant{detail}.")


def retention_after_release(
    context: RunContext, test_directory: Path | None, trigger: str
) -> retention.RetentionReport | None:
    """Run one retention pass when this release left no run holding the account (design 2.7, triggers 1 and 2).

    Called only where the caller holds no lock (the guardian after its leave, the operator's ``run recover``). The
    queue is read under the metadata lock without waiting for it, which is released before the pass; a busy lock skips
    the pass (the next trigger runs it). A private test composition passes no Docker port, so only file kinds run.
    No-raise contract: retention is housekeeping, so its failure never fails the leave or recovery it follows. Returns
    the pass's report, or ``None`` when no pass ran.
    """
    try:
        directory = account_directory() if test_directory is None else test_directory.resolve()
        with lock(Context(directory, directory), "metadata", nonblocking=True) as anchor:
            if any(entry.phase in HOLDER_PHASES for entry in _load_queue(directory, anchor).entries.values()):
                return None  # another run holds: the last one to leave runs the pass
        clock = NativeClock()
        if retention.throttled(directory, clock):
            return None  # checked before composing the Docker port, which a throttled pass never uses
        docker = _docker() if test_directory is None else None
        registry = context.project.state
        return retention.run_pass(
            account=directory,
            registry=registry,
            trigger=trigger,
            docker=docker,
            clock=clock,
            image_interest=retention_image_interest(directory, registry),
            deadline=Deadline.after(clock, retention.RETENTION_BUDGET_S),
        )
    except (OSError, ValueError, ToolkitError):
        return None  # the no-raise contract above: housekeeping never fails a leave or a recovery


def retention_image_interest(account: Path, registry: Path) -> Callable[[], Callable[[str], bool]]:
    """Return the in-use check retention reads under K: a live holder or an indexed retained run uses the image.

    It never waits for the metadata lock: a busy one raises ``ResourceBusy`` at once and the pass defers the kind.
    """

    def under_image_lock() -> Callable[[str], bool]:
        """Read the interest views now, under the caller's K, and return the decision over them."""
        views = _account_interest_views(account, registry, time.monotonic_ns())  # an expired deadline: no wait
        return lambda image_id: image_in_use(image_id, views, actor_run="")

    return under_image_lock


def _ensure_probe_image(topology: str) -> None:
    """Make the daemon probe's image present before a fresh run queues, holding no account lock (design DD9).

    Admission never pulls: a pass without the image reads no figures (joins on a Mac VM then wait on CPU pressure).
    Every outcome is accepted here; a failed pull is stamped per daemon and retried after its backoff.
    """
    try:
        docker = _docker()
        if topology == "remote" and not loopback_endpoint(_endpoint_read(docker, Deadline.after(NativeClock(), 30))):
            return  # another machine's daemon is never probed
        # the guardian records the account only when it starts; the requester resolves it here
        ensure_on_current_daemon(docker, account_directory(), NativeClock())
    except (InfrastructureFailure, ResourceError):
        pass  # the guardian's own Docker reads decide whether the daemon is usable; this is only the image


def _disk_refusal_detail(context: RunContext, data: dict, test_directory: Path | None) -> str:
    """Name the place a never-fitting run was refused on, read again by the requester (no record carries figures).

    A refusal does not depend on other runs' leases, so the requester's own reading reproduces it.
    """
    if data["infrastructure_failure"] != "disk-capacity":
        return ""
    try:
        account = Path(data["account_directory"])
        decided = _disk_decision(_disk_inputs(context, data, test_directory), 0, declared_disk_reserve(account))
    except (OSError, ResourceError, InfrastructureFailure):
        return ""
    return f" ({decided.detail})" if decided.state == CapacityState.REFUSED and decided.detail else ""


def _small_vm_detail(data: dict, test_directory: Path | None) -> str:
    """Name the VM size that admits a run refused for its base capacity or its CPUs, read again by the requester.

    The runtime is not recorded (design 2.5), so the requester detects it again; any failure to do so names nothing.
    Colima names a size for a base-capacity refusal. Podman names a resize only when one changes something (design
    2.4): a machine that already meets the computed size was refused on the host side, which resizing cannot fix.
    """
    failure = data["infrastructure_failure"]
    if failure not in ("unsupported-base-capacity", "insufficient-cpu"):
        return ""
    try:
        if test_directory is not None:
            bound = _test_capacity_record(test_directory)
            runtime, machine = _bound_runtime(bound, data["topology"]), bound.get("machine")
        else:
            docker = _docker()
            facts = _runtime_facts(docker, docker.observe_daemon().daemon_id, Deadline.after(NativeClock(), 30))
            runtime, found = facts.runtime, facts.machine
            machine = (
                None if found is None else {"name": found.name, "cpus": found.cpus, "memory_mib": found.memory_mib}
            )
    except (OSError, ResourceError, InfrastructureFailure):
        return ""
    if runtime not in RUNTIME_OVERHEADS:
        return ""
    budgets = _phase_budgets(data["phases"], data["initial_phase"]).values()
    container = max(budget.container_bytes for budget in budgets)
    reserve = RUNTIME_OVERHEADS[runtime][0]
    if runtime == "colima" and failure == "unsupported-base-capacity":
        return f" ({small_vm_hint(container, reserve)})"
    if runtime != "podman" or not isinstance(machine, dict):
        return ""
    cpus, memory_mib = machine["cpus"], machine["memory_mib"]
    daemon_cpus = max(budget.daemon_cpus for budget in budgets)
    needed_mib, needed_cpus = podman_machine_size(container, reserve, cpus, daemon_cpus)
    if needed_mib <= memory_mib and needed_cpus <= cpus:
        return ""  # the machine already has that size: the refusal was host-side
    return f" ({podman_size_hint(needed_mib, needed_cpus, memory_mib, machine['name'])})"


def _admission_ended_detail(data: dict) -> str:
    """Describe, for the requester, what the guardian persisted about why an admission ended; empty without evidence."""
    evidence = data.get("failure_evidence")
    if evidence is None:
        return ""
    if evidence["category"] == "admission-expired":
        longest = sorted(evidence["waiting"], key=lambda item: item["waited_ns"], reverse=True)[:3]
        return " (waited: " + ", ".join(f"{item['reason']} {item['waited_ns'] / 1e9:.1f} s" for item in longest) + ")"
    named = "" if evidence["reason"] == evidence["category"] else f": {evidence['reason']}"  # what failed, when known
    return f" ({evidence['category']}, observer {evidence['observer']}{named})"


def _apply_withdrawal(data: dict, *, requester: dict, outcome: str, clock: NativeClock) -> dict:
    """Write the requester's withdrawal into ``data`` and return it; the caller persists the record.

    The one writer of a withdrawal (``leave_run``, ``_withdraw_lost_requester``, ``withdraw_test_runs``): state
    ``stopping``, the withdrawal of the record's generation at ``revision + 1`` with the cleanup deadline that the exact
    registered resource set allows, the test outcome and a pending cleanup. No I/O; callers hold the project and run
    locks and keep their own identity checks, write and additions.
    """
    withdrawal = {
        "generation": data["generation"],
        "revision": data["revision"] + 1,
        "requester": requester,
        "outcome": outcome,
        "deadline": asdict(Deadline.after(clock, _leave_budget_seconds(data))),
    }
    data.update(state="stopping", withdrawal=withdrawal, test_outcome=outcome, cleanup_outcome="pending")
    data["revision"] = withdrawal["revision"]
    return withdrawal


def _withdraw_lost_requester(context: RunContext) -> None:
    project = context.project
    with lock(project, project.project_id, shared=True), lock(project, f"run-{context.run_id}"):
        data = _load_run(context)
        clock = NativeClock()
        if data["state"] != "active" or data["guardian"] != asdict(current_process(clock)):
            return
        if observe_process(_identity_from_json(data["requester"])) == "executing":
            return
        _apply_withdrawal(data, requester=data["requester"], outcome="cancelled", clock=clock)
        data["infrastructure_failure"] = "requester-lost"
        atomic_json(context.record, data)


def _record_guardian_failure(context: RunContext, reason: str) -> None:
    if not isinstance(reason, str) or not NAME.fullmatch(reason):
        reason = "guardian-error"
    project = context.project
    with lock(project, project.project_id, shared=True), lock(project, f"run-{context.run_id}"):
        data = _load_run(context)
        if data["generation"] == 0 and data["admission"] == "never-granted" and data["infrastructure_failure"] is None:
            data["infrastructure_failure"] = reason
            data["revision"] += 1
            atomic_json(context.record, data)


def _evidence_summary(data: dict) -> str:
    """Describe the first observation behind a recorded failure for the leave message; nothing is written."""
    evidence = data.get("failure_evidence")
    if evidence is None:
        return ""
    observed = evidence["first_observed"]
    clock = NativeClock()
    if observed["origin"] == clock.origin:
        when = f"first observed {max(0, clock.read() - observed['tick_ns']) // 1_000_000_000} s before this report"
    else:
        when = "first observed (interval unavailable (clock origin changed))"
    return (
        f" (first failure {evidence['category']}: {evidence['reason']}, observer {evidence['observer']}, {when},"
        f" state {observed['state']}, phase {observed['phase']})"
    )


_FAULT_REASON_MAX = 240  # bounded: the evidence names the failure, it does not store command output


def _fault_reason(fault: TelemetryFault, error: BaseException) -> str:
    """Return the evidence ``reason`` for one classified fault (design 4.3).

    An unclassified fault has no category of its own, so its evidence carries the error's own text, whitespace collapsed
    and bounded, which names what actually failed (for example the failed Docker command). A client exit keeps its
    message too (its command and closed cause, never Docker's output). Every other fault keeps its category as the
    reason.
    """
    message = " ".join(str(error).split())[:_FAULT_REASON_MAX]
    names_itself = fault.detail == "unclassified" or getattr(error, "client_exit", False)
    return message if names_itself and message else fault.category


def _observation_evidence(
    data: dict,
    *,
    category: str,
    reason: str,
    observer: str,
    first_tick: tuple[str, int],
    detail: str | None = None,
) -> dict:
    """Build the observation-kind ``failure_evidence`` value from the record being written (design 4.3)."""
    grant = data.get("grant")
    origin, tick_ns = first_tick
    return {
        "schema": 1,
        "category": category,
        "reason": reason,
        "detail": detail,
        "observer": observer,
        "first_observed": {
            "origin": origin,
            "tick_ns": tick_ns,
            "state": data["state"],
            "phase": None if grant is None else grant.get("phase"),
            "revision": data["revision"],
        },
        "waiting": None,
        "pressure": None,
    }


def _publish_first_failure(current: dict, reason: str, evidence: dict) -> bool:
    """Set ``infrastructure_failure`` and ``failure_evidence`` together, only for the first failure of the run.

    This is the one first-wins writer for observation-kind evidence. It writes nothing when a failure is already
    recorded or evidence exists, and reports whether it wrote. The caller owns the record lock, the revision bump and
    the atomic write.
    """
    if current.get("infrastructure_failure") is not None or "failure_evidence" in current:
        return False
    current["infrastructure_failure"] = reason
    current["failure_evidence"] = evidence
    return True


@dataclass
class _LateAttempts:
    """Guardian-memory count of consecutive late observation attempts since the last complete pass (design 4.2).

    The state is lost when the guardian restarts, like the admission accounting; recovery records ``guardian-lost``.
    """

    count: int = 0
    first_tick: tuple[str, int] | None = None


_LATE_ATTEMPTS_TO_FAIL = 3
# Admission passes whose Docker client read exited non-zero, with no completed read between them, before the run is
# revoked with that client
# exit's own message. A busy daemon's exit clears within a few 1 s passes; one that persists (a runner without a usable
# Docker context, 2026-11-01) would otherwise wait out the whole queue budget without saying why.
_CLIENT_EXIT_PASSES_TO_FAIL = 30


def _set_container_oom(current: dict) -> None:
    """Record a container OOM on the run; the first failure of the run also carries its evidence (design 4.3).

    An earlier failure keeps its own evidence and today's overwrite semantics stay. Evidence is attached only in the
    states the loader accepts, so a record in any other state is never made unloadable by it.
    """
    failure = "container-oom"  # a record value, not an exception reason
    if current["state"] in ("active", "stopping"):
        clock = NativeClock()
        evidence = _observation_evidence(
            current,
            category="breach",
            reason=failure,
            observer="container",
            first_tick=(clock.origin, clock.read()),
        )
        if _publish_first_failure(current, failure, evidence):
            return
    current["infrastructure_failure"] = failure


def _record_active_failure(
    context: RunContext,
    reason: str,
    expected_revision: int | None,
    evidence: Callable[[dict], dict] | None = None,
) -> None:
    """Publish a failure observed against the still-current run snapshot (any snapshot when the revision is None).

    ``evidence`` builds the ``failure_evidence`` value from the record being written, so the evidence and the failure
    reach the record in the same atomic write; writers without evidence pass none.
    """
    project = context.project
    with lock(project, project.project_id, shared=True), lock(project, f"run-{context.run_id}"):
        data = _load_run(context)
        if (
            data["state"] == "active"
            and (expected_revision is None or data["revision"] == expected_revision)
            and data["infrastructure_failure"] is None
        ):
            if evidence is None:
                data["infrastructure_failure"] = reason
            elif not _publish_first_failure(data, reason, evidence(data)):
                return
            data["revision"] += 1
            atomic_json(context.record, data)


def _only_observable_receipts_pending(open_receipts: list[dict]) -> bool:
    """Report whether every open receipt is a pending build or export, so host pressure stays sampled."""
    return bool(open_receipts) and all(
        receipt["kind"] in _OBSERVABLE_RECEIPT_KINDS and receipt["state"] == "pending" for receipt in open_receipts
    )


def _observe_active_run(context: RunContext, late: _LateAttempts | None = None, *, _budget_seconds: float = 30) -> None:
    """Persist one guardian observation and make capacity/OOM violations sticky.

    A failed read is classified, never swallowed: unreadable and daemon-change faults are sticky at once,
    and the third consecutive late pass is ``observation-late``. A breach measured before the fault is published first.
    """
    late = late if late is not None else _LateAttempts()
    project = context.project
    with lock(project, project.project_id, shared=True), lock(project, f"run-{context.run_id}"):
        data = _load_run(context)
        if data["state"] != "active" or data["guardian"] != asdict(current_process(NativeClock())):
            return
        if data["grant"]["scope"] != "managed":
            return
        compose_up_pending = data["pending_operation"] is not None and data["pending_operation"]["kind"] == "compose-up"
        open_receipts = [
            receipt for receipt in data["docker_receipts"].values() if receipt["state"] in {"pending", "unresolved"}
        ]
        build_in_flight = _only_observable_receipts_pending(open_receipts)
        if compose_up_pending or (open_receipts and not build_in_flight):
            # Exact container IDs do not exist until mutation result publication;
            # the journaled operation deadline owns this bounded observation gap.
            # A bounded BuildKit worker is observable as finite external capacity
            # while its build receipt is pending, so host pressure stays sampled.
            return
        revision = data["revision"]
        managed_ids = {
            container_id
            for container_id, item in data["resources"].get("containers", {}).items()
            if item["state"] == "running"
        }
        owned_ids = {
            container_id
            for container_id, item in data["owned_resources"]["containers"].items()
            if item["state"] == "running"
        }
        owned_targets = tuple(
            sorted(
                container_id
                for container_id in owned_ids
                if data["owned_resources"]["containers"][container_id]["evidence"].get("control") is not None
            )
        )
        capacity_ids = managed_ids | owned_ids
        mutation_pending = data["pending_operation"] is not None and data["pending_operation"]["kind"] in (
            "compose-mutate",
            "container-control",
        )
    clock = NativeClock()
    deadline = Deadline.after(clock, _budget_seconds)
    fault: tuple[OSError | ResourceError, str] | None = None
    step = "engine"
    capacity_decision = pressure = current = None
    terms = SharedPressure(False, 0, False)
    observations: dict[str, ContainerObservation] = {}
    owned_observations = None
    oom = False
    reserve = read_host_reserve(Path(data["account_directory"]))  # the account's setting, read each pass (D8)
    try:
        docker = _docker()
        mutation_plan = data["resources"].get("compose") if mutation_pending else None
        mutation_limits = None
        if mutation_plan is not None:
            mutation_limits = {
                service: sum(
                    resource["memory_bytes"]
                    for resource in data["resources"]["containers"].values()
                    if resource["service"] == service and resource["state"] != "removed"
                )
                for service in mutation_plan["services"]
            }
        step = "engine"
        shared = _record_mode(data) == AdmissionMode.SHARED
        # The holders read before the host samples excuse them too: one that leaves before the co-holder read below had
        # its memory in the samples.
        account = Path(data["account_directory"])
        before = _gather_coholders(lambda: _read_coholders(context, account, deadline)) if shared else None
        inventory = docker.objects("container", deadline=deadline) if shared else None
        daemon_id, capacity = observe_capacity(
            docker,
            data["topology"],
            capacity_ids,
            mutation_plan=mutation_plan,
            mutation_limits=mutation_limits,
            inventory=inventory,
            deadline=deadline,
        )
        if daemon_id != data["grant"]["daemon_id"]:
            raise ResourceError(
                "Docker daemon changed during active observation.", reason="daemon-identity-changed", observer="engine"
            )
        step = "host"
        current = _host_read(clock, deadline)
        # A shared run reads its co-holders after the inventory, so every inventoried co-holder container is explained,
        # and after the host sample, so a run admitted before the sample is in the membership the sample is judged with
        # (one admitted after it can only add an excuse).
        step = "engine"
        coholders = _gather_coholders(lambda: _read_coholders(context, account, deadline)) if shared else None
        step = "process"
        serialized_identities = [data["requester"]] + [
            item["identity"] for item in data["consumers"].values() if item["state"] == "active"
        ]
        host_rss = 0
        for serialized in serialized_identities:
            identity = _identity_from_json(serialized)
            failpoint("observation-process-rss")
            host_rss += _active_process_rss(identity, deadline)
        step = "engine"
        admission = _host_observation(data["admission_observation"]["memory"])
        phase = _host_observation(data["phase_history"][-1]["memory"])
        samples = [_host_observation(item) for item in data["admission_observation"]["samples"]]
        windows = [
            sample
            for sample in [_host_observation(data["admission_observation"]["window_start"]), *samples]
            if current.tick_ns - sample.tick_ns >= _WINDOW_NS
        ]
        window = max(windows, key=lambda sample: sample.tick_ns)
        budget = _phase_budgets(data["phases"], data["initial_phase"])[data["grant"]["phase"]]
        if coholders is None or inventory is None:
            capacity_decision = assess_capacity(budget, replace(capacity, host_reserve_bytes=reserve))
        else:
            capacity_decision = observation_decision(
                budget,
                _coholder_snapshot(capacity, inventory, coholders.holders, coholders.host_reserve_bytes),
                [holder.reservation for holder in coholders.holders],
                coholders.unknown,
            )
            terms = _shared_pressure_terms(
                coholders.grants,
                coholder_seen=bool(
                    coholders.holders or coholders.unknown or (before and (before.held or before.unknown))
                ),
                record=data,
                held=(*(before.held if before else ()), *coholders.held),
            )
        pressure = assess_pressure(
            current,
            window,
            granted=True,
            admission_baseline=admission,
            phase_baseline=phase,
            host_rss_bytes=host_rss,
            host_envelope_bytes=budget.host_process_bytes,
            growth_allowance_bytes=terms.allowance_bytes,
            shared_gate=terms.shared,
        )
    except (OSError, ResourceError) as error:
        fault = (error, step)
    if fault is None and not mutation_pending:
        try:
            for container_id in sorted(managed_ids):
                previous = ContainerObservation(**data["resources"]["containers"][container_id]["observation"])
                observation = observe_container(docker, daemon_id, container_id, previous=previous, deadline=deadline)
                observations[container_id] = observation
                oom = oom or observation.oom_killed or observation.oom_events > 0
            if owned_targets:
                # One target at a time: an OOM measured on an earlier target survives a later target's failed read.
                owned_observations = _owned_control_resources(data)
                for target in owned_targets:
                    measured = _observe_owned_control(data, docker, (target,), deadline)
                    owned_observations["containers"][target] = measured["containers"][target]
                    oom = oom or _owned_control_has_oom(owned_observations, (target,))
        except (OSError, ResourceError) as error:
            fault = (error, "container")
    # The live failure tuple decides first: a measured OOM, then a capacity breach, then a pressure breach.
    breach: tuple[str, str] | None = None
    if oom:
        breach = ("container-oom", "container")
    elif capacity_decision is not None and capacity_decision.state != CapacityState.READY:
        breach = (capacity_decision.reason, "engine")
    elif pressure is not None and pressure.state != CapacityState.READY:
        breach = (pressure.reason, "process" if pressure.reason == "host-process-envelope" else "host")
    tick = (clock.origin, clock.read())
    if breach is not None and fault is not None:
        reason, observer = breach
        _record_active_failure(
            context,
            reason,
            revision,
            lambda record: _observation_evidence(
                record, category="breach", reason=reason, observer=observer, first_tick=tick
            ),
        )
        return
    if fault is not None:
        raised, step_observer = fault
        classified = _classify_error(raised, step_observer)
        first_tick = tick
        if classified.category == "observation-late":
            late.count += 1
            if late.first_tick is None:
                late.first_tick = tick
            if late.count < _LATE_ATTEMPTS_TO_FAIL:
                return
            first_tick = late.first_tick
        _record_active_failure(
            context,
            classified.category,
            revision,
            lambda record: _observation_evidence(
                record,
                category=classified.category,
                reason=_fault_reason(classified, raised),
                observer=classified.observer,
                first_tick=first_tick,
                detail=classified.detail,
            ),
        )
        return
    late.count, late.first_tick = 0, None  # a pass that read everything it should is a complete sample
    with lock(project, project.project_id, shared=True), lock(project, f"run-{context.run_id}"):
        current_data = _load_run(context)
        if current_data["state"] != "active" or current_data["revision"] != revision:
            return
        for container_id, observation in observations.items():
            current_data["resources"]["containers"][container_id]["observation"] = asdict(observation)
        if owned_observations is not None:
            _publish_owned_control(current_data, owned_observations, owned_targets)
        current_data["admission_observation"]["samples"] = [
            *current_data["admission_observation"]["samples"][-7:],
            asdict(cast(HostMemoryObservation, current)),
        ]
        if terms.unreadable:
            _record_grants_unreadable(current_data, tick)
        if breach is not None:
            reason, observer = breach
            _publish_first_failure(
                current_data,
                reason,
                _observation_evidence(
                    current_data, category="breach", reason=reason, observer=observer, first_tick=tick
                ),
            )
        current_data["revision"] += 1
        atomic_json(context.record, current_data)


_TEST_CAPACITY_KEYS = frozenset(
    {
        "daemon_memory_bytes",
        "host_memory_bytes",
        "daemon_cpus",
        "host_cpus",
        "containers",
        "cpu_pressure",
        "overtake_window_ns",
        "disk",
        "previous_image_sizes",
        "turn_grace_ns",
        "runtime",
        "machine",
    }
)


def _refuse_real_account(directory: Path, binding: str) -> None:
    """Refuse a private test binding whose non-strictly resolved path is, lies in or contains the real account domain.

    Non-strict, so the refusal holds on a fresh host where neither the account nor the private directory exists yet.
    """
    account = canonical_account()
    if account is not None and overlaps_account(directory, account):
        raise ResourceError(f"Private test {binding} binding cannot use the real account domain.")


def _private_test_root(directory: Path, binding: str) -> Path:
    """Refuse the real account domain for a private test binding, then return the existing private directory."""
    _refuse_real_account(directory, binding)
    return directory.resolve(strict=True)


def bind_test_capacity(
    directory: Path,
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
    """Replace a private account's synthetic capacity snapshot and daemon inventory (non-exported test composition).

    Unset sizes keep the synthetic defaults. ``containers`` are the daemon's inspected containers; running ones are
    external commitments. ``cpu_pressure`` is the host's CPU pressure figure (none when unset). ``disk`` measures the
    named places (``data-root``, ``vm-disk-host``: ``free_bytes`` and ``total_bytes``; ``private-root``:
    ``free_bytes``); unnamed places are unknown. ``previous_image_sizes`` stands for the previous run's image sizes.
    ``runtime`` names the detected runtime (unset: the one the declared topology stands for); ``machine`` the bound
    Podman machine's ``name``, ``cpus`` and ``memory_mib``.
    """
    root = _private_test_root(directory, "capacity")
    sizes = {
        "daemon_memory_bytes": daemon_memory_bytes,
        "host_memory_bytes": host_memory_bytes,
        "daemon_cpus": daemon_cpus,
        "host_cpus": host_cpus,
    }
    if any(value is not None and (type(value) is not int or value <= 0) for value in sizes.values()):
        raise ResourceError("Private test capacity sizes must be positive integers.")
    if cpu_pressure is not None and (type(cpu_pressure) is not float or not 0 <= cpu_pressure <= 100):
        raise ResourceError("Private test CPU pressure must be a percentage.")
    _private_directory(root)
    atomic_json(
        root / "test-capacity.json",
        {
            **{key: value for key, value in sizes.items() if value is not None},
            "containers": list(containers),
            **({"cpu_pressure": cpu_pressure} if cpu_pressure is not None else {}),
            **({"overtake_window_ns": overtake_window_ns} if overtake_window_ns is not None else {}),
            **({"disk": disk} if disk is not None else {}),
            **({"previous_image_sizes": previous_image_sizes} if previous_image_sizes is not None else {}),
            **({"turn_grace_ns": turn_grace_ns} if turn_grace_ns is not None else {}),
            **({"runtime": runtime} if runtime is not None else {}),
            **({"machine": machine} if machine is not None else {}),
        },
    )


_HOST_SAMPLE_FIELDS = frozenset({"offset_ns", "headroom_bytes", "swap_used_bytes", "swapout_bytes"})
_HOST_SAMPLES_MAX = 256
_HOST_SAMPLES_FILE = "test-host-samples.json"


def _validate_host_samples(document: object) -> list[dict]:
    """Return the entries of a private test host-sample script (design 3.11, T8); refuse any other shape.

    Shape and types only: decreasing offsets or swap-out values are allowed, so tests can provoke the W0 fallback.

    Raises:
        ResourceError: If the document is not exactly the closed script shape.
    """
    invalid = ResourceError("Private test host samples are invalid.")
    if (
        not isinstance(document, dict)
        or set(document) != {"schema", "samples"}
        or type(document["schema"]) is not int
        or document["schema"] != 1
        or not isinstance(document["samples"], list)
        or not 1 <= len(document["samples"]) <= _HOST_SAMPLES_MAX
    ):
        raise invalid
    samples = document["samples"]
    for item in samples:
        if isinstance(item, dict) and set(item) == {"fail"} and item["fail"] is True:
            continue
        if (
            not isinstance(item, dict)
            or set(item) != _HOST_SAMPLE_FIELDS
            or any(type(value) is not int for value in item.values())
            or item["offset_ns"] > 0
            or any(item[key] < 0 for key in ("headroom_bytes", "swap_used_bytes", "swapout_bytes"))
        ):
            raise invalid
    if all("fail" in item for item in samples):
        raise invalid
    return samples


def bind_test_host_samples(directory: Path, samples: Sequence[dict]) -> None:
    """Script a private account's host-memory reads for the admission window (non-exported test composition).

    The guardian child reads the script once, so the samples cross the process boundary that an in-process reader
    binding cannot (design 3.11, T8). Production never looks for the file.
    """
    root = _private_test_root(directory, "host-sample")
    document = {"schema": 1, "samples": list(samples)}
    _validate_host_samples(document)
    _private_directory(root)
    atomic_json(root / _HOST_SAMPLES_FILE, document)


_TEST_WINDOW_FILE = "test-window.json"


def bind_test_window(directory: Path, seconds: int) -> None:
    """Bind a shorter admission window for one private test domain (non-exported test composition).

    The guardian of a private run reads it once at start and uses it for selection, its passes and publication
    (design 2.2, I-D1). Only a shorter integral window, ``1 <= seconds < 30``, is accepted; the real account domain is
    refused before anything is written. Production never looks for the file.
    """
    root = _private_test_root(directory, "window")
    if type(seconds) is not int or not 1 <= seconds < _WINDOW_NS // 1_000_000_000:
        raise ResourceError("Private test window must be whole seconds, at least 1 and below 30.")
    _private_directory(root)
    atomic_json(root / _TEST_WINDOW_FILE, {"window_ns": seconds * 1_000_000_000})


_TEST_INSTALLATION_FILE = "test-installation.json"
RELEASE_PURPOSE = "retained-release"


def bind_test_installation(directory: Path, *, root: Path, loaded: Path) -> None:
    """Bind a private test domain to a temporary installation: ``root`` holds ``current``, ``loaded`` is "this" version.

    Non-exported test composition: with a binding, the version gate evaluates the private domain like a real
    installation, so the cross-version tests can switch ``root/current`` under a running process. Production never
    looks for the file; the real account domain is refused before anything is written.
    """
    account = _private_test_root(directory, "installation")
    _private_directory(account)
    atomic_json(account / _TEST_INSTALLATION_FILE, {"loaded": str(loaded), "root": str(root), "schema": 1})


def _gate_inputs(test_directory: Path | None) -> tuple[bool, Path | None, Path | None]:
    """Return ``(test composition, loaded version folder, resolved current)`` for the version gate (design 3.2)."""
    if test_directory is None:
        installation = installation_of(_ARCHIVE_PATH) if _ARCHIVE_PATH is not None else None
        if installation is None:
            return False, None, None
        return False, Path(os.path.realpath(installation.version_folder)), current_target(installation.root)
    path = _private_test_root(test_directory, "installation") / _TEST_INSTALLATION_FILE
    if not path.exists():
        return True, None, None
    document = _read_private_json(path)
    if not isinstance(document, dict) or set(document) != {"loaded", "root", "schema"} or document["schema"] != 1:
        raise ResourceError("Private test installation binding is invalid.")
    return True, Path(os.path.realpath(document["loaded"])), current_target(Path(document["root"]))


def _gate_refusal(test_directory: Path | None, *, release_helper: bool) -> str | None:
    """Return the version gate's refusal reason for this process, or None to admit (invariant I4)."""
    test_composition, loaded, current = _gate_inputs(test_directory)
    return admission_gate(
        test_composition=test_composition, release_helper=release_helper, loaded=loaded, current=current
    )


def _gate_error(reason: str, test_directory: Path | None) -> IncompatibleState:
    """Build the refusal a requester sees: the restart message for an upgrade, the install hint otherwise."""
    if reason == "toolkit-upgraded":
        _composition, loaded, current = _gate_inputs(test_directory)
        own = loaded.name if loaded is not None else "this version"
        replacement = current.name if current is not None else "another version"
        return IncompatibleState(
            f"Docker Test Toolkit {own} was replaced by {replacement} on this machine; "
            "restart this test process to use it.",
            reason=reason,
        )
    return IncompatibleState("The toolkit is not installed on this machine; install it first.", reason=reason)


def withdraw_test_runs(registry: Path, *, recorded: Path) -> list[str]:
    """Withdraw the runs a private test registry still holds active, as each run's requester would; return their ids.

    Non-exported test composition: the teardown of a test that never left its runs (TK-121, ruling W). Nothing is
    signalled; each live guardian then cleans up under its own authority, acknowledges and exits. Refusals come before
    any read or write of a record: the real account domain (also for an absent path), a ``registry`` that is a symlink
    or no longer resolves to ``recorded`` (the resolved path recorded when its root was built), and a ``runs``
    directory or run record that resolves outside the registry. A vanished registry or ``runs`` directory is nothing
    to withdraw. Each record is decided only after it is re-loaded under the project (shared) and run locks; one that
    cannot be read, parsed or loaded is skipped, and its guardian is left to the session census. Idempotent: a
    withdrawn run is skipped. Production never calls it.

    Raises:
        ResourceError: For any of the refusals above.
    """
    _refuse_real_account(registry, "withdrawal")
    if registry.is_symlink():
        raise ResourceError("Private test registry is a symlink, no longer its recorded path.")
    if not registry.exists():
        return []
    root = registry.resolve(strict=True)
    if root != recorded:  # resolved when the root was built: resolving it again would follow a re-point
        raise ResourceError("Private test registry resolves elsewhere, no longer its recorded path.")
    runs = registry / "runs"
    if root not in runs.resolve().parents:
        raise ResourceError("Private test run records lie outside their private registry.")
    if not runs.is_dir():
        return []
    records = sorted(runs.glob("*.json"))
    if any(root not in record.resolve().parents for record in records):
        raise ResourceError("Private test run records lie outside their private registry.")
    withdrawn = []
    for record in records:
        try:
            raw = json.loads(record.read_text())
        except (OSError, ValueError):
            continue  # vanished or unreadable: left to the census
        context = recorded_run_context(root, record.stem, raw) if isinstance(raw, dict) else None
        if context is None:
            continue
        project = context.project
        try:
            with lock(project, project.project_id, shared=True), lock(project, f"run-{context.run_id}"):
                data = _load_run(context)
                if data["state"] != "active" or data.get("withdrawal") is not None:
                    continue
                _apply_withdrawal(data, requester=data["requester"], outcome="cancelled", clock=NativeClock())
                atomic_json(context.record, data)
        except (OSError, ResourceError):
            continue  # a record the loader refuses: left to the census
        withdrawn.append(context.run_id)
    return withdrawn


def _test_window_ns(test_directory: Path | None) -> int:
    """Return the admission window of one guardian: ``_WINDOW_NS`` in production, with no I/O.

    A private domain's binding shortens it. A directory equal to, under or over the real account is refused before
    any read; an absent file keeps 30 s; a present file must be exactly ``{"window_ns": N}`` with
    ``10**9 <= N < _WINDOW_NS``, else the guardian fails loudly, as for an invalid host script.

    Raises:
        ResourceError: For the real account domain or an invalid binding.
    """
    if test_directory is None:
        return _WINDOW_NS
    path = _private_test_root(test_directory, "window") / _TEST_WINDOW_FILE
    if not path.exists():
        return _WINDOW_NS
    document = _read_private_json(path)
    if (
        not isinstance(document, dict)
        or set(document) != {"window_ns"}
        or type(document["window_ns"]) is not int
        or not 1_000_000_000 <= document["window_ns"] < _WINDOW_NS
    ):
        raise ResourceError("Private test window binding is invalid.")
    return document["window_ns"]


class _HostScript:
    """One guardian's consumption of a private test host-sample script: one entry per scripted read."""

    def __init__(self, samples: list[dict]) -> None:
        self._samples = samples
        self._cursor = 0
        self._last_value = next(item for item in reversed(samples) if "fail" not in item)

    @classmethod
    def load(cls, test_directory: Path | None) -> "_HostScript | None":
        """Return the script of a private domain, or None in production and in a private domain without one."""
        if test_directory is None:
            return None
        path = test_directory / _HOST_SAMPLES_FILE
        if not path.exists():
            return None
        return cls(_validate_host_samples(_read_private_json(path)))

    def next(self, clock: NativeClock, physical_bytes: int) -> "HostMemoryObservation":
        """Return the next scripted sample; after the last entry, the last value entry at offset 0.

        Raises:
            ResourceError: For a fail entry, classified ``telemetry-unreadable`` from the host observer.
        """
        if self._cursor < len(self._samples):
            entry = self._samples[self._cursor]
            self._cursor += 1
        else:
            entry = {**self._last_value, "offset_ns": 0}
        if "fail" in entry:
            raise ResourceError("Private test host sample failed.", reason="telemetry-unreadable", observer="host")
        return HostMemoryObservation(
            clock.origin,
            clock.read() + entry["offset_ns"],
            physical_bytes,
            entry["headroom_bytes"],
            entry["swap_used_bytes"],
            entry["swapout_bytes"],
            4096,
            {},
        )


@dataclass
class CapacityInputs:
    """One capacity observation: the daemon identity, the snapshot with every running container external, the host
    samples, and the container inventory behind the snapshot."""

    daemon_id: str
    snapshot: "CapacitySnapshot"
    current: "HostMemoryObservation"
    baseline: "HostMemoryObservation"
    containers: list[dict]
    cpu_pressure: float | None = None  # Linux PSI ``some avg60``; None where the host publishes no figures


def _bound(test_directory: Path | None) -> dict:
    """A private account's bound test values (``bind_test_capacity``), or none for a real account."""
    return {} if test_directory is None else _test_capacity_record(test_directory)


def _test_capacity_record(test_directory: Path) -> dict:
    """Return the private account's ``bind_test_capacity`` record, or an empty one when it was never called."""
    capacity_file = test_directory / "test-capacity.json"
    if not capacity_file.exists():
        return {}
    bound = _read_private_json(capacity_file)
    if not set(bound) <= _TEST_CAPACITY_KEYS or not isinstance(bound.get("containers", []), list):
        raise ResourceError("Private test capacity record is invalid.")
    return bound


def _test_capacity_inputs(
    test_directory: Path,
    data: dict,
    budgets: Mapping[str, PhaseBudget],
    *,
    host_script: "_HostScript | None" = None,
    baseline: "HostMemoryObservation | None" = None,
) -> CapacityInputs:
    """Build the private account's synthetic observation, overridden by ``bind_test_capacity`` when it was called.

    Without a host-sample script the window is the synthetic complete pair (``now - 30 s``, ``now``). With one, the
    current sample is the script's next entry and the start is ``baseline`` or that sample, as in production (3.11).
    """
    gib = 1024**3
    clock = NativeClock()
    now = clock.read()
    bound = _test_capacity_record(test_directory)
    largest_container = max(budget.container_bytes for budget in budgets.values())
    daemon_memory = bound.get("daemon_memory_bytes", max(64 * gib, largest_container + gib))
    host_memory = bound.get("host_memory_bytes", 64 * gib)
    containers = list(bound.get("containers", []))
    external = tuple(item["HostConfig"]["Memory"] or None for item in containers if item["State"]["Running"] is True)
    snapshot = CapacitySnapshot(
        daemon_memory,
        bound.get("daemon_cpus", 64),
        host_memory,
        bound.get("host_cpus", 64),
        data["topology"],
        external,
        runtime=_bound_runtime(bound, data["topology"]),
    )
    if host_script is None:
        current = HostMemoryObservation(clock.origin, now, host_memory, host_memory // 2, 0, 0, 4096, {})
        start = HostMemoryObservation(clock.origin, now - _WINDOW_NS, host_memory, host_memory // 2, 0, 0, 4096, {})
    else:
        current = host_script.next(clock, host_memory)
        start = baseline or current
    return CapacityInputs(
        _test_daemon_id(test_directory), snapshot, current, start, containers, bound.get("cpu_pressure")
    )


def _test_daemon_id(test_directory: Path) -> str:
    """Return the private account's bound daemon identity, or the fixed test identity when none is bound."""
    daemon_file = test_directory / "docker-daemon.json"
    if not daemon_file.exists():
        return "11111111-1111-1111-1111-111111111111"
    daemon_record = _read_private_json(daemon_file)
    if set(daemon_record) != {"daemon_id"} or not isinstance(daemon_record["daemon_id"], str):
        raise ResourceError("Private test daemon identity is invalid.")
    return daemon_record["daemon_id"]


_FAULT_REASONS = {
    "observation-late": "observation-deadline",
    "telemetry-unreadable": "telemetry-unreadable",
    "daemon-identity-changed": "daemon-identity-changed",
}


class _AdmissionFault(ResourceError):
    """An observation read of an admission pass failed; ``fault`` is its classification (design 4.2 and 4.4).

    It is a ``ResourceError`` carrying the cause's reason and observer, so a caller that does not classify admission
    faults keeps today's behaviour. The guardian's admission loops turn it into a wait or a classified revoke.
    """

    def __init__(self, message: str, fault: TelemetryFault, *, client_exit: bool = False):
        super().__init__(message, reason=_FAULT_REASONS[fault.category], observer=fault.observer)
        self.fault = fault
        self.client_exit = client_exit  # a failed client read: its message names the command and the closed cause


def _classify_error(error: OSError | ResourceError, fallback_observer: str) -> TelemetryFault:
    """Classify a failed observation read: by the reason it carries, else unclassified (fail closed).

    The observer is the one the raise site named, else the step being performed.
    """
    if isinstance(error, ResourceError):
        return classify_fault(error.reason, observer=error.observer or fallback_observer)
    return classify_fault("unspecified", observer=fallback_observer)


def _admission_fault(error: Exception, *, observer: str = "engine") -> _AdmissionFault:
    """Classify a failure of an admission-time read as an ``_AdmissionFault`` (an existing one passes through)."""
    if isinstance(error, _AdmissionFault):
        return error
    fault = _classify_error(cast(OSError | ResourceError, error), observer)
    client_exit = getattr(error, "reason", None) == "client-exit"
    return _AdmissionFault(str(error) or "An admission observation failed.", fault, client_exit=client_exit)


def _admission_inputs(
    data: dict,
    test_directory: Path | None,
    baseline: "HostMemoryObservation | None" = None,
    *,
    host_script: "_HostScript | None" = None,
) -> CapacityInputs:
    """Read the capacity inputs for an admission pass; any failure of the read itself is a classified fault.

    The account's host reserve is applied afterwards, outside the fault classification: an invalid declaration is the
    operator's setting, not telemetry, and fails the guardian closed.
    """
    try:
        inputs = _capacity_inputs(data, test_directory, baseline, host_script=host_script)
    except (ResourceError, OSError) as error:
        raise _admission_fault(error) from error
    reserve = read_host_reserve(Path(data["account_directory"]))  # the account's setting, read per decision (D8)
    return replace(inputs, snapshot=replace(inputs.snapshot, host_reserve_bytes=reserve))


def _capacity_inputs(
    data: dict,
    test_directory: Path | None,
    baseline: "HostMemoryObservation | None" = None,
    *,
    host_script: "_HostScript | None" = None,
) -> CapacityInputs:
    """Observe the daemon and host once: the inventory is read first, the snapshot is derived from that same read.

    Every read shares one 30 second deadline, so an admission pass cannot take longer than one observation attempt.
    """
    failpoint("admission-capacity-read")
    budgets = _phase_budgets(data["phases"], data["initial_phase"])
    if test_directory is not None:
        return _test_capacity_inputs(test_directory, data, budgets, host_script=host_script, baseline=baseline)
    docker = _docker()
    deadline = Deadline.after(NativeClock(), 30)
    containers = docker.objects("container", deadline=deadline)  # kept: a shared fit attributes co-holder containers
    daemon_id, snapshot = observe_capacity(docker, data["topology"], inventory=containers, deadline=deadline)
    current = _host_read(NativeClock(), deadline)
    pressure = _join_pressure(data, docker, daemon_id, deadline, NativeClock())
    return CapacityInputs(daemon_id, snapshot, current, baseline or current, containers, pressure)


def temporary_folder() -> Path:
    """Return the resolved temporary folder a run's private root lives in (``$TMPDIR``, else ``/tmp``)."""
    return Path(os.environ.get("TMPDIR", "/tmp")).resolve()


def host_cpu_pressure(path: Path = Path("/proc/pressure/cpu")) -> float | None:
    """Read the host's system-wide CPU pressure (Linux PSI ``some avg60``, never namespaced to a cgroup).

    None where the host publishes no PSI file (macOS, kernels without PSI): memory admission alone decides there. A file
    that exists but cannot be read or parsed reads as full pressure, so it holds joins back but never a run alone.
    """
    try:
        text = path.read_text()
    except FileNotFoundError:
        return None
    except OSError:
        return 100.0
    value = parse_cpu_pressure(text)
    return 100.0 if value is None else value


_DAEMON_PROBES: dict[str, DaemonProbe] = {}  # one per account in a guardian process, so its cache spans passes


def _join_pressure(
    data: dict,
    docker: DockerPort,
    daemon_id: str,
    deadline: object,
    clock: NativeClock,
    *,
    host_psi: Path = Path("/proc/pressure/cpu"),
) -> float | None:
    """Return the CPU pressure a join decides on: the host's PSI, or on a Mac VM topology the VM's (design 2.3).

    A Linux host keeps its own figure (or none, on a kernel without PSI). Where the host publishes none and the daemon
    runs in a VM, the containers compete for the VM's CPUs, so the daemon probe reads the VM's PSI; a probe without a
    figure counts as full pressure, which holds joins and never a run alone. Called outside every account lock.
    """
    host = host_cpu_pressure(host_psi)
    if host is not None or data["topology"] != "desktop":
        return host
    reading = _probe_reading(data["account_directory"], docker, daemon_id, deadline, clock)
    return 100.0 if reading.cpu_pressure is None else reading.cpu_pressure


def _probe_reading(account: str, docker, daemon_id: str, deadline: object, clock: NativeClock) -> ProbeResult:
    """Return the daemon probe's reading, cached per account; without a data root only the disk figure is unknown.

    A probe ends by ``min(now + PROBE_RUN_BOUND_S, deadline - 2 s)`` (design 2.3), whatever its caller's deadline.
    """
    if isinstance(deadline, Deadline):
        deadline.remaining_ns(clock)  # a deadline of another clock origin is an error, as everywhere
    reader = _DAEMON_PROBES.setdefault(account, DaemonProbe(Path(account)))
    reading = reader.cached(daemon_id, clock)
    if reading is None:
        now = clock.read()
        end = now + int(PROBE_RUN_BOUND_S * 1e9)
        if isinstance(deadline, Deadline):
            end = min(end, deadline.expires_ns - 2_000_000_000)  # absolute ends: the caller's is never extended
        if end <= now:
            return ProbeResult(None, None, now, clock.origin, daemon_id)  # no time for a probe: no figures
        deadline = Deadline(clock.origin, now, end - now, end)
        try:
            data_root: object = docker.call("info", "--format", "{{.DockerRootDir}}", deadline=deadline).strip()
        except (InfrastructureFailure, ResourceError):
            data_root = None
        reading = reader.read(docker, daemon_id=daemon_id, data_root=data_root, deadline=deadline, clock=clock)
    return reading


@dataclass(frozen=True)
class DiskInputs:
    """One pass's disk figures, read outside every account lock (design 2.6): the leased places, the private root's
    free bytes (``None`` when unmeasured) and the run's estimate with its source."""

    figures: tuple[DiskFigure, ...]
    private_root_free: int | None
    estimate: int
    source: str
    daemon_id: str | None = None  # the daemon this pass observed; a turn of another daemon is not valid
    read_ns: int | None = None  # when the reading began, on ``clock_origin``: a grant decides on figures <= 10 s old
    clock_origin: str | None = None


DISK_FIGURES_MAX_AGE_NS = 10_000_000_000  # design 3.1 (7): a decision uses disk figures at most 10 s old


def disk_figures_fresh(disk: DiskInputs, clock: NativeClock) -> bool:
    """Whether a reading is young enough to decide on (design 3.1 (7)); one of unknown time or origin is not."""
    return (
        disk.read_ns is not None
        and disk.clock_origin == clock.origin
        and 0 <= clock.read() - disk.read_ns <= DISK_FIGURES_MAX_AGE_NS
    )


_VM_DISK_HOST = Path("Library/Containers/com.docker.docker/Data/vms/0/data")  # Docker Desktop's default disk folder


def vm_disk_folder() -> Path:
    """Return Docker Desktop's default VM disk folder under the passwd home.

    Desktop's settings, which name a moved disk image, are not read: they live in its Group Container, and reading
    another app's container makes macOS 15+ ask the user for access. A moved image is measured ``unknown`` once its
    default folder is gone (``measure_folder``); while an emptied default folder remains, that folder's volume is read.
    """
    return passwd_home() / _VM_DISK_HOST


def vm_disk_place(runtime: str | None, name: str | None, image_dir: Path | None = None) -> Path | None:
    """Return the host folder holding the daemon's VM disk: Docker Desktop's default folder, Colima's by instance, a
    Podman machine's image folder from its binding (``image_dir``)."""
    if runtime == "docker-desktop":
        return vm_disk_folder()
    if runtime == "podman":
        return image_dir
    return vm_disk_host_directory(runtime, name, passwd_home())


_DAEMON_ENDPOINTS: dict[str, str] = {}  # daemon ID -> the client's endpoint to it, per process


def _daemon_endpoint(docker, daemon_id: str, deadline: Deadline | None) -> str:
    """Return the client's endpoint to the verified daemon, read once per daemon and process like its runtime: a
    capacity observation runs every second while a run waits, and one daemon is reached through one endpoint."""
    if daemon_id not in _DAEMON_ENDPOINTS:
        _DAEMON_ENDPOINTS[daemon_id] = _endpoint_read(docker, deadline)
    return _DAEMON_ENDPOINTS[daemon_id]


@dataclass(frozen=True)
class RuntimeFacts:
    """What the bound daemon's runtime decides beyond admission (design section 12): its runtime (``None`` when
    remote or refused), its VM disk's host folder and, on Podman, the machine whose size a refusal may name."""

    runtime: str | None
    disk_place: Path | None
    machine: MachineFacts | None


_DAEMON_RUNTIMES: dict[str, RuntimeFacts] = {}  # daemon ID -> its runtime facts, per process


def runtime_of(binding: PodmanBinding | None, info: Mapping[str, object], endpoint: str) -> str | RuntimeRefusal | None:
    """The one runtime rule (design 2.3, section 12): a Podman-bound adapter is ``podman`` (its compat ``/info`` never
    decides); any other daemon is classified by ``detect_runtime``."""
    if binding is not None:
        return "podman"
    return detect_runtime(info, sys.platform, endpoint, passwd_home())


def _runtime_facts(docker, daemon_id: str, deadline: Deadline) -> RuntimeFacts:
    """Return the bound daemon's runtime facts, decided once per daemon and process (design 2.5, section 12)."""
    binding = docker.binding
    if binding is not None:
        return RuntimeFacts("podman", vm_disk_place("podman", binding.machine.name, binding.image_dir), binding.machine)
    if daemon_id not in _DAEMON_RUNTIMES:
        info = _engine_read(docker, "/info", deadline).body
        detected = runtime_of(None, info, _daemon_endpoint(docker, daemon_id, deadline))
        runtime = None if isinstance(detected, RuntimeRefusal) else detected
        name = info.get("Name")
        place = vm_disk_place(runtime, name if isinstance(name, str) else None)
        _DAEMON_RUNTIMES[daemon_id] = RuntimeFacts(runtime, place, None)
    return _DAEMON_RUNTIMES[daemon_id]


def _disk_inputs(context: RunContext, data: dict, test_directory: Path | None) -> DiskInputs:
    """Read the disk figures a reservation decides on; a place that cannot be measured is ``unknown`` (it admits).

    The data root is the daemon probe's ``df`` (cached 10 s per account), absent on a remote daemon; on a Mac VM
    runtime the VM disk's host folder (Docker Desktop's, Colima's instance, a Podman machine's image folder) is measured
    client-side; the private root's free bytes come from its parent folder. Every figure carries the undeclared reserve
    here: the declared one is applied under the metadata lock, with the declaration read there (``_disk_gate``).
    """
    declared = None
    clock = NativeClock()
    started = clock.read()
    if test_directory is not None:
        bound = _test_capacity_record(test_directory)
        places = bound.get("disk") or {}
        sizes = bound.get("previous_image_sizes", _previous_image_sizes(context, data))
        figures = tuple(
            measured_figure(place, places[place]["free_bytes"], places[place]["total_bytes"], declared)
            for place in ("data-root", "vm-disk-host")
            if place in places
        )
        private = (places.get("private-root") or {}).get("free_bytes")
        estimate = disk_estimate(sizes)
        return DiskInputs(figures, private, *estimate, _test_daemon_id(test_directory), started, clock.origin)
    try:
        docker = _docker()
        daemon_id: str | None = docker.observe_daemon().daemon_id
    except (InfrastructureFailure, ResourceError):
        docker, daemon_id = None, None  # no daemon identity: no turn is valid, today's order applies
    measured = [_data_root_figure(data, docker, daemon_id)]
    if data["topology"] == "desktop":
        folder = None
        if docker is not None and daemon_id is not None:
            try:
                folder = _runtime_facts(docker, daemon_id, Deadline.after(NativeClock(), 30)).disk_place
            except (InfrastructureFailure, ResourceError):
                folder = None  # the runtime cannot be read: the place is unknown
        measured.append(measure_folder("vm-disk-host", folder, declared))
    private = measure_folder("private-root", existing_ancestor(Path(data["private_root"]).parent), None)
    estimate = disk_estimate(_previous_image_sizes(context, data))
    return DiskInputs(tuple(measured), private.free_bytes, *estimate, daemon_id, started, clock.origin)


def measured_figure(place: str, free: int, total: int, declared: int | None) -> DiskFigure:
    """Return a measured place's figure with the reserve derived from its total and the declaration (design 2.6)."""
    return DiskFigure(place, free, total, derive_reserve(total, declared), "measured")


def measure_folder(place: str, path: Path | None, declared: int | None) -> DiskFigure:
    """Measure a client-side folder with ``statvfs``; a missing one is ``unknown`` (a moved disk image, design 2.6)."""
    if path is None:
        return DiskFigure(place, None, None, None, "unknown")
    try:
        observed = os.statvfs(path)
    except OSError:
        return DiskFigure(place, None, None, None, "unknown")
    total = observed.f_blocks * observed.f_frsize
    return measured_figure(place, observed.f_bavail * observed.f_frsize, total, declared)


def existing_ancestor(path: Path) -> Path:
    """Return ``path`` or its nearest existing parent: a run's folders may not exist yet where they will be made."""
    while not path.exists() and path.parent != path:
        path = path.parent
    return path


def loopback_endpoint(endpoint: str) -> bool:
    """Whether a Docker endpoint is a TCP socket on this machine's loopback (``127.0.0.1``, ``localhost``, ``::1``)."""
    if not endpoint.startswith("tcp://"):
        return False
    try:
        host = urllib.parse.urlsplit(endpoint).hostname
    except ValueError:
        return False  # a malformed URL names no machine: never this one
    if host == "localhost":
        return True
    try:
        return host is not None and ipaddress.ip_address(host).is_loopback  # 127.0.0.0/8 and ::1, never a DNS name
    except ValueError:
        return False


def _data_root_figure(data: dict, docker, daemon_id: str | None) -> DiskFigure:
    """Return the daemon's data-root figure from the probe; unknown without a verified daemon, on another machine's
    daemon, or when the probe has none. A remote endpoint on the loopback (a CI runner reaching its own host's daemon
    over ``tcp://127.0.0.1``) is this machine's daemon, and its data root is probed."""
    unknown = DiskFigure("data-root", None, None, None, "unknown")
    if docker is None or daemon_id is None:
        return unknown
    try:
        if data["topology"] == "remote" and not loopback_endpoint(
            _daemon_endpoint(docker, daemon_id, Deadline.after(NativeClock(), 30))
        ):
            return unknown
        reading = _probe_reading(
            data["account_directory"], docker, daemon_id, Deadline.after(NativeClock(), 30), NativeClock()
        )
    except (InfrastructureFailure, ResourceError):
        return unknown
    figure = reading.data_root
    if figure is None or figure.state != "measured" or figure.free_bytes is None or figure.total_bytes is None:
        return unknown
    return measured_figure("data-root", figure.free_bytes, figure.total_bytes, None)


_IMAGE_SIZES: dict[str, tuple[int, list[int] | None]] = {}  # run ID -> (runs folder mtime, estimate), per process


def _previous_image_sizes(context: RunContext, data: dict) -> list[int] | None:
    """Return the owned image sizes of the newest complete run of the same request (design DD8), or ``None``.

    A waiting guardian asks on every pass: the answer is kept until the runs folder changes (every record write
    renames a file into it), so an idle wait reads no run record again.
    """
    runs = context.project.state / "runs"
    try:
        stamp = runs.stat().st_mtime_ns
    except OSError:
        stamp = -1
    cached = _IMAGE_SIZES.get(data["run_id"])
    if cached is not None and cached[0] == stamp >= 0:
        return cached[1]
    sizes = _scan_image_sizes(context, data)
    _IMAGE_SIZES[data["run_id"]] = (stamp, sizes)
    return sizes


def _scan_image_sizes(context: RunContext, data: dict) -> list[int] | None:
    key = _verdict_key(data)
    if key is None:
        return None
    request = (data["entrypoint"], data["profile"], data["closure"])
    newest: tuple[int, list[int]] | None = None
    for name, _run_id, _bytes, raw in run_files(context.project.state):
        if not isinstance(raw, dict) or raw.get("state") != "complete" or raw.get("run_id") == data["run_id"]:
            continue
        if (raw.get("entrypoint"), raw.get("profile"), raw.get("closure")) != request:
            continue  # another request: its key differs, so its checkout's origin need not be read
        try:
            if _verdict_key(raw) != key:
                continue
            modified = (context.project.state / "runs" / name).stat().st_mtime_ns
        except (KeyError, TypeError, ValueError, OSError):
            continue
        sizes = [
            item["evidence"]["size_bytes"]
            for item in ((raw.get("owned_resources") or {}).get("images") or {}).values()
            if isinstance(item, dict) and type((item.get("evidence") or {}).get("size_bytes")) is int
        ]
        if sizes and (newest is None or modified > newest[0]):
            newest = (modified, sizes)
    return None if newest is None else newest[1]


def _never_fits(disk: DiskInputs, data: dict) -> str | None:
    """Return the refusal of a run that cannot fit even beside no other lease (it depends on no other run, design
    2.6), or ``None``."""
    decided = _disk_decision(disk, 0, declared_disk_reserve(Path(data["account_directory"])))
    return decided.reason if decided.state == CapacityState.REFUSED else None


def _disk_decision(disk: DiskInputs, other_leases: int, declared: int | None) -> CapacityDecision:
    """Decide every leased place with its reserve and the private root: a refusal anywhere wins over a wait anywhere."""
    decisions = [
        disk_fit(
            replace(figure, reserve_bytes=derive_reserve(figure.total_bytes, declared))
            if figure.state == "measured" and figure.total_bytes is not None
            else figure,
            other_leases,
            disk.estimate,
        )
        for figure in disk.figures
    ]
    if disk.private_root_free is not None:
        decisions.append(private_root_fit(disk.private_root_free))
    for state in (CapacityState.REFUSED, CapacityState.WAITING):
        found = next((decision for decision in decisions if decision.state == state), None)
        if found is not None:
            return found
    return CapacityDecision(CapacityState.READY, "disk-fits")


def _disk_gate(
    disk: DiskInputs, directory: Path, state: QueueState, run_id: str, clock_origin: str
) -> tuple[CapacityDecision, dict[str, Lease]]:
    """Decide this pass's disk figures under M against every other holder's lease of this boot (design 2.6).

    A holder without a lease of this clock origin counts the floor. The declared reserve is read here, under M, with
    the decision it serves; an invalid declaration is treated as undeclared (each place derives its own). Returns the
    decision and the valid leases, for the caller's lease write.
    """
    leases = {key: lease for key, lease in read_leases(directory).items() if lease.clock_origin == clock_origin}
    other = sum(
        leases[holder].bytes if holder in leases else DISK_FLOOR_BYTES
        for holder, entry in state.entries.items()
        if entry.phase in HOLDER_PHASES and holder != run_id
    )
    return _disk_decision(disk, other, declared_disk_reserve(directory)), leases


def _queued_host_sample(
    test_directory: Path | None, host_script: "_HostScript | None", clock: NativeClock
) -> "HostMemoryObservation | None":
    """Take a capacity-one run's host sample on one queued pass, or None in a private domain without a script.

    Production reads the host with the same 30 s deadline as an admission read; a private domain with a script takes
    its next entry (design 3.11). The read is a capacity read, so the failpoint stage is the admission read's.
    """
    if test_directory is None:
        failpoint("admission-capacity-read")
        return _host_read(clock, Deadline.after(clock, 30))
    if host_script is None:
        return None
    failpoint("admission-capacity-read")
    return host_script.next(clock, _test_capacity_record(test_directory).get("host_memory_bytes", 64 * 1024**3))


def bind_test_daemon(directory: Path, daemon_id: str) -> None:
    """Bind one real daemon only inside the non-exported private test composition."""
    root = _private_test_root(directory, "daemon")
    if not isinstance(daemon_id, str) or not NAME.fullmatch(daemon_id):
        raise ResourceError("Private test daemon identity is invalid.")
    _private_directory(root)
    atomic_json(root / "docker-daemon.json", {"daemon_id": daemon_id})


_PRODUCTION_POLL_NS = 1_000_000_000
_TEST_POLL_FILE = "test-poll.json"


def bind_test_poll_interval(directory: Path, interval_ns: int) -> None:
    """Bind a faster leave poll for one private test domain (non-exported test composition).

    The guardian's active loop and the requester's acknowledgment poll of a private run use it (design 2.3, I-D2); the
    admission passes keep their 1 s. ``10 ms <= interval_ns <= 1 s``; the real account domain is refused before
    anything is written.
    """
    root = _private_test_root(directory, "poll")
    if type(interval_ns) is not int or not 10_000_000 <= interval_ns <= _PRODUCTION_POLL_NS:
        raise ResourceError("Private test poll interval must be whole nanoseconds from 10 ms to 1 s.")
    _private_directory(root)
    atomic_json(root / _TEST_POLL_FILE, {"schema": 1, "interval_ns": interval_ns})


def _poll_interval_ns(directory: Path | None, *, canonical: Path | None) -> int:
    """Return the leave poll interval of one domain: 1 s unless a private binding shortens it.

    ``None``, a directory equal to, under or over the resolved ``canonical`` account (paths resolved before comparing)
    and an absent file give 1 s without opening anything; a present private file must be exactly
    ``{"schema": 1, "interval_ns": N}`` with ``10 ms <= N <= 1 s``.

    Raises:
        ResourceError: For an invalid private binding.
    """
    if directory is None or (canonical is not None and overlaps_account(directory, canonical)):
        return _PRODUCTION_POLL_NS
    path = directory.resolve() / _TEST_POLL_FILE
    if not path.exists():
        return _PRODUCTION_POLL_NS
    document = _read_private_json(path)
    if (
        not isinstance(document, dict)
        or set(document) != {"schema", "interval_ns"}
        or type(document["schema"]) is not int
        or document["schema"] != 1
        or type(document["interval_ns"]) is not int
        or not 10_000_000 <= document["interval_ns"] <= _PRODUCTION_POLL_NS
    ):
        raise ResourceError("Private test poll binding is invalid.")
    return document["interval_ns"]


def _queue_wait_ended(data: dict) -> bool:
    """Whether a queued run's requester left or its original queue budget ended: its guardian must revoke the run."""
    return observe_process(_identity_from_json(data["requester"])) != "executing" or not _deadline_from_json(
        data["queue_deadline"]
    ).remaining_ns(NativeClock())


def _pressure_fields(inputs: CapacityInputs) -> dict[str, int]:
    """Return the host and daemon figures behind one admission sample, as the expiry evidence records them."""
    current, start = inputs.current, inputs.baseline
    elapsed = current.tick_ns - start.tick_ns
    swapped = max(0, current.swapout_bytes - start.swapout_bytes)
    total = current.swap_total_bytes
    return {
        "host_headroom_bytes": current.headroom_bytes,
        "host_swap_used_bytes": current.swap_used_bytes,
        "host_swap_total_bytes": total or 0,
        "swapout_bytes_per_s": swapped * 1_000_000_000 // elapsed if elapsed > 0 else 0,
        "daemon_memory_bytes": inputs.snapshot.daemon_memory_bytes,
    }


@dataclass
class _WaitSpan:
    """One waiting reason's first and last tick and its accumulated, overlapping duration (the evidence keys)."""

    first_tick_ns: int
    last_tick_ns: int
    waited_ns: int = 0


def _waiting_items(spans: Mapping[str, _WaitSpan]) -> list[dict]:
    """Return waiting spans in the shared evidence shape, ordered by first tick, then reason."""
    ordered = sorted(spans.items(), key=lambda item: (item[1].first_tick_ns, item[0]))
    return [
        {
            "reason": reason,
            "first_tick_ns": span.first_tick_ns,
            "last_tick_ns": span.last_tick_ns,
            "waited_ns": span.waited_ns,
        }
        for reason, span in ordered
    ]


def _attribute_pass(spans: dict[str, _WaitSpan], start: int, reasons: set[str], tick: int) -> None:
    """Attribute one pass from ``start`` to ``tick`` to every reason it held.

    A pass that named no reason continues the wait of the last pass that did (the queue budget cut it short).
    """
    for reason in reasons:
        span = spans.setdefault(reason, _WaitSpan(start, start))
        span.last_tick_ns = tick
        span.waited_ns += tick - start


class _AdmissionWaits:
    """Guardian-memory account of why an admission kept waiting, over both admission loops (design 4.4).

    Each pass names the reasons that held during it; a pass lasts from its start to the start of the next (or to the
    expiry), so the durations of different reasons overlap and are not additive. The account is lost if the guardian
    dies; recovery records ``guardian-lost``. It is independent of the active run's late-attempt counter.
    """

    def __init__(self) -> None:
        self.first_tick_ns: int | None = None  # the first pass's start: waiting is complete from here (design 3.2)
        self._spans: dict[str, _WaitSpan] = {}
        self._pass: tuple[int, set[str]] | None = None
        self._last_reasons: set[str] = set()
        self._pressure: dict[str, int] | None = None
        self._client_exits = 0  # consecutive passes that failed on a Docker client exit

    def client_exit(self) -> int:
        """Count one more consecutive pass that failed on a Docker client exit and return the streak."""
        self._client_exits += 1
        return self._client_exits

    def read_completed(self) -> None:
        """End a client-exit streak: this pass's Docker reads completed."""
        self._client_exits = 0

    def begin(self, tick: int) -> None:
        """Start a pass at ``tick``; the pass before it ends at the same instant."""
        if self.first_tick_ns is None:
            self.first_tick_ns = tick
        self.close(tick)
        self._pass = (tick, set())

    def note(self, reason: str) -> None:
        """Name a reason that held during the current pass."""
        if self._pass is not None:
            self._pass[1].add(reason)

    def close(self, tick: int) -> None:
        """End the current pass at ``tick`` and attribute its interval to every reason it named."""
        if self._pass is None:
            return
        start, reasons = self._pass
        self._pass = None
        if reasons:
            self._last_reasons = reasons
        _attribute_pass(self._spans, start, reasons or self._last_reasons, tick)

    def waiting_at(self, tick: int) -> list[dict]:
        """Return the waiting spans as if the current pass closed at ``tick``, without changing the account."""
        spans = copy.deepcopy(self._spans)
        if self._pass is not None:
            start, reasons = self._pass
            _attribute_pass(spans, start, reasons or self._last_reasons, tick)
        return _waiting_items(spans)

    def sample(self, inputs: CapacityInputs) -> None:
        """Keep the most recent capacity and host sample."""
        self._pressure = _pressure_fields(inputs)

    def evidence(self, data: dict) -> dict | None:
        """Build the ``admission-expired`` evidence from the record being revoked; ``None`` if nothing was waited on."""
        if not self._spans:
            return None
        waiting = _waiting_items(self._spans)
        first_reason = waiting[0]["reason"]
        clock = NativeClock()
        evidence = _observation_evidence(
            data,
            category="admission-expired",
            reason=first_reason,
            observer="admission",
            first_tick=(clock.origin, clock.read()),  # captured at the write, like every kind but observation-late
        )
        evidence.update(waiting=waiting, pressure=self._pressure)
        return evidence


def _revoke_ended_wait(
    context: RunContext,
    capacity: ExitStack,
    test_directory: Path | None,
    waits: _AdmissionWaits,
    data: dict,
    *,
    requester_left: bool,
    expired: bool,
    failure_reason: str | None = None,
) -> RunStatus:
    """Revoke a run whose admission wait ended; a queue-deadline expiry (with its requester still there) records why.

    The last pass is accounted only up to the queue deadline, so a pass that ran past it never exceeds the budget. The
    requester's presence is decided again under the revoke's locks, where the evidence is built.
    """
    waits.close(min(NativeClock().read(), data["queue_deadline"]["expires_ns"]))
    expiry = expired and not requester_left
    if expiry:
        failpoint("admission-expiry-evidence-pending")

    def evidence(record: dict) -> dict | None:
        """Build the expiry evidence unless the requester has left since the loop head decided."""
        if observe_process(_identity_from_json(record["requester"])) != "executing":
            return None
        return waits.evidence(record)

    return revoke_ungranted(
        context,
        capacity,
        _test_directory=test_directory,
        _failure_reason=failure_reason,
        _admission_evidence=evidence if expiry else None,
    )


def _settle_admission_fault(
    context: RunContext,
    capacity: ExitStack,
    test_directory: Path | None,
    waits: _AdmissionWaits,
    fault: TelemetryFault,
    reason: str,
    *,
    client_exit: bool = False,
) -> RunStatus | None:
    """Turn a classified admission fault into a revoke (sticky categories) or a named wait (late).

    ``reason`` is the evidence reason (``_fault_reason``). A client exit is late until ``_CLIENT_EXIT_PASSES_TO_FAIL``
    client exits occurred without a completed read between them (another fault neither counts nor ends the streak); then
    it is unreadable telemetry whose reason is its own message.
    Returns the revoked status, or ``None`` when the run keeps waiting and the next pass decides from fresh inputs.
    """
    if client_exit and waits.client_exit() >= _CLIENT_EXIT_PASSES_TO_FAIL:
        fault = TelemetryFault("telemetry-unreadable", fault.observer, "unclassified")
    if fault.category == "observation-late":
        waits.note(reason)  # the category, or a client exit's own message naming its command and cause
        return None
    clock = NativeClock()
    first = (clock.origin, clock.read())

    def evidence(data: dict) -> dict:
        """Build the fault's evidence from the record as it is at the revoke write."""
        return _observation_evidence(
            data,
            category=fault.category,
            reason=reason,
            observer=fault.observer,
            first_tick=first,
            detail=fault.detail,
        )

    waits.close(first[1])
    return revoke_ungranted(
        context,
        capacity,
        _test_directory=test_directory,
        _failure_reason=fault.category,
        _admission_evidence=evidence,
    )


def _guardian_loop(context: RunContext, attempt: str, test_directory: Path | None) -> RunStatus:
    # the guardian's own lifetime project lock is an admission wait too: it ends with the run's queue budget
    start_deadline_ns = _queue_budget_ns(_load_run(context))
    with (
        lock(context.project, context.project.project_id, shared=True, deadline_ns=start_deadline_ns),
        ExitStack() as capacity_stack,
    ):
        enroll_guardian(context, attempt, _test_directory=test_directory)
        # Outside the per-pass handling: an unreadable or invalid test script fails the guardian loudly (design 3.11).
        host_script = _HostScript.load(test_directory)
        window_ns = _test_window_ns(test_directory)  # one window for selection, passes and publication (I-D1)
        # The active loop's poll only; the admission passes keep their 1 s (I-D2).
        active_poll_ns = (
            _PRODUCTION_POLL_NS
            if test_directory is None
            else _poll_interval_ns(test_directory, canonical=canonical_account())
        )
        capacity = None
        baseline = None
        waits = _AdmissionWaits()
        # Guardian memory only, never written to the record: every guardian process (a resumed generation included)
        # starts with an empty buffer, which is W0 (design 3.11, AC6). Keep it that way.
        queued = _SampleWindow()
        while capacity is None:
            data = _load_run(context)
            deadline = _deadline_from_json(data["queue_deadline"])
            clock = NativeClock()
            requester_left = observe_process(_identity_from_json(data["requester"])) != "executing"
            expired = not deadline.remaining_ns(clock)
            if requester_left or expired:
                return _revoke_ended_wait(
                    context, capacity_stack, test_directory, waits, data, requester_left=requester_left, expired=expired
                )
            waits.begin(clock.read())
            pass_sample: HostMemoryObservation | None = None
            try:
                # A joiner's fit is decided on a sample taken after it saw holders (design 2.4, Q5): never on one taken
                # while no holder existed. A run predicted shareable also samples, to learn whether E(self) can fit the
                # machine at all. At limit 1 only the limit itself is read (lock-free; decided again under M).
                sharing_possible = read_parallel_runs(Path(data["account_directory"])) != 1
                holders_seen = sharing_possible and _holders_present(data)
                sampled = (
                    _admission_inputs(data, test_directory, None, host_script=host_script)
                    if holders_seen or (sharing_possible and _predicted_shareable(context, data))
                    else None
                )
                pass_inputs = sampled if holders_seen else None
                if sampled is not None:
                    waits.read_completed()
                try:
                    pass_sample = _queued_host_sample(test_directory, host_script, clock)
                except (ResourceError, OSError):
                    pass_sample = None  # a failed queued read is dropped; the run keeps waiting (design 3.11)
                failpoint("admission-sample-taken")
                disk = (  # outside every account lock (design 2.6); freeing disk never waits for disk
                    None if data["purpose"] == RELEASE_PURPOSE else _disk_inputs(context, data, test_directory)
                )
                never = None if disk is None else _never_fits(disk, data)
                if never is not None:
                    # refused before any Docker mutation of this pass, another run's takeover included (C-AC2)
                    return revoke_ungranted(
                        context, capacity_stack, _test_directory=test_directory, _failure_reason=never
                    )
                # detection is part of every admission pass, whether or not this pass can reserve (design 3.10)
                reconcile_account(context, _test_directory=test_directory)
                reserved = reserve_capacity(
                    context,
                    attempt,
                    capacity_stack,
                    inputs=pass_inputs,
                    machine=None if sampled is None else sampled.snapshot,
                    disk=disk,
                    _test_directory=test_directory,
                )
                if reserved.waiting == "disk-capacity":
                    retention_after_release(context, test_directory, "disk-wait")  # it runs only with no holder
                if reserved.refusal is not None:  # decided inside the section; revoked only after leaving it
                    return revoke_ungranted(
                        context, capacity_stack, _test_directory=test_directory, _failure_reason=reserved.refusal
                    )
                capacity = reserved.descriptor
                if capacity is not None:
                    reserved_ns = NativeClock().read()  # the reservation tick, read before the window (design 3.2)
                if reserved.waiting is not None:
                    waits.note(reserved.waiting)
                if pass_sample is None:
                    queued.clear()  # a pass that yields no sample clears the buffer, whether or not it reserved
                elif capacity is None:
                    queued.add(pass_sample)  # only a pass that did not reserve contributes queued history (P3)
            except _AdmissionFault as error:
                queued.clear()  # the faulted pass yields no sample; today's classification follows unchanged (P7)
                settled = _settle_admission_fault(
                    context,
                    capacity_stack,
                    test_directory,
                    waits,
                    error.fault,
                    _fault_reason(error.fault, error),
                    client_exit=error.client_exit,
                )
                if settled is not None:
                    return settled
            except ResourceBusy as error:
                queued.clear()
                if error.reason != "lock-deadline":
                    raise
                waits.note("lock-wait")
                continue  # the pass's lock wait ended with the queue budget: the loop head revokes the expired run
            except ResourceError:
                queued.clear()
                if not _queue_wait_ended(_load_run(context)):
                    raise
                continue  # the budget ended or the requester left while the pass ran: the loop head revokes the run
            if capacity is None:
                time.sleep(min(1_000_000_000, deadline.remaining_ns(clock)) / 1_000_000_000)
        failpoint("slot-reserved")  # outside every lock: the entry is admitting, the grant is not yet published
        post = _SampleWindow()
        while True:
            data = _load_run(context)
            clock = NativeClock()
            deadline = _deadline_from_json(data["queue_deadline"])
            requester_left = observe_process(_identity_from_json(data["requester"])) != "executing"
            expired = not deadline.remaining_ns(clock)
            if requester_left or expired:
                return _revoke_ended_wait(
                    context,
                    capacity_stack,
                    test_directory,
                    waits,
                    data,
                    requester_left=requester_left,
                    expired=expired,
                )
            waits.begin(clock.read())
            try:
                budgets = _phase_budgets(data["phases"], data["initial_phase"])
                inputs = _admission_inputs(data, test_directory, baseline, host_script=host_script)
                waits.sample(inputs)
                waits.read_completed()
                baseline = inputs.baseline  # the first post-reservation sample (a fresh synthetic start, unscripted)
                snapshot, current, daemon_id = inputs.snapshot, inputs.current, inputs.daemon_id
                post.add(current)
                # W1 (design 3.11): queued samples count towards the window. A private domain without a script keeps
                # today's synthetic complete pair, and the pressure rule itself is unchanged.
                unscripted = test_directory is not None and host_script is None
                window_start = (
                    baseline
                    if unscripted
                    else select_window_start(queued.samples(), post.samples(), current, window_ns=window_ns)
                )
                decisions = [assess_capacity(budget, snapshot) for budget in budgets.values()]
                if any(decision.state == CapacityState.REFUSED for decision in decisions):
                    reason = next(decision.reason for decision in decisions if decision.state == CapacityState.REFUSED)
                    return revoke_ungranted(
                        context,
                        capacity_stack,
                        _test_directory=test_directory,
                        _failure_reason=reason,
                    )
                # The first post-reservation sample stays a continuity baseline: a counter that goes backwards after the
                # reservation is still refused, whichever sample starts the window (design 3.11).
                continuity = None if unscripted else baseline
                pressure = assess_pressure(current, window_start, admission_baseline=continuity, window_ns=window_ns)
                if pressure.state == CapacityState.REFUSED:
                    return revoke_ungranted(
                        context,
                        capacity_stack,
                        _test_directory=test_directory,
                        _failure_reason=pressure.reason,
                    )
                for decision in decisions:
                    if decision.state == CapacityState.WAITING:
                        waits.note(decision.reason)
                if pressure.state == CapacityState.WAITING:
                    waits.note(pressure.reason)
                if (
                    all(decision.state == CapacityState.READY for decision in decisions)
                    and pressure.state == CapacityState.READY
                    and window_start is not None  # READY already implies a complete window; this states it
                ):
                    if test_directory is None:
                        try:
                            docker = _docker()  # a read like the pass's own: a failed client read waits, never crashes
                        except InfrastructureFailure as error:
                            if error.reason != "client-exit":
                                raise  # any other composition failure keeps today's handling
                            raise _admission_fault(error) from error
                        bind_daemon(context.project, docker)
                    failpoint("publish-pending")  # outside every lock: the loop decided READY, publication has not yet
                    granting = _load_run(context)
                    fresh_disk = (
                        None
                        if granting["purpose"] == RELEASE_PURPOSE  # freeing disk never waits for disk
                        else _disk_inputs(context, granting, test_directory)  # fresh figures, outside every lock
                    )
                    published = _publish_grant(
                        context,
                        attempt,
                        capacity,
                        inputs.snapshot,
                        current,
                        window_start,
                        daemon_id=daemon_id,
                        containers=inputs.containers,
                        waits=waits,
                        reserved_ns=reserved_ns,
                        window_ns=window_ns,
                        _test_directory=test_directory,
                        _host_script=host_script,
                        _admission_baseline=continuity,
                        disk=fresh_disk,
                    )
                    if isinstance(published, _Refused):  # gate point 4 refused inside its section; revoke outside it
                        return revoke_ungranted(
                            context, capacity_stack, _test_directory=test_directory, _failure_reason=published.reason
                        )
                    if isinstance(published, _Waiting):  # grant order, the limit or co-holders: retry next pass
                        waits.note(published.reason)
                    else:
                        break
            except _AdmissionFault as error:
                settled = _settle_admission_fault(
                    context,
                    capacity_stack,
                    test_directory,
                    waits,
                    error.fault,
                    _fault_reason(error.fault, error),
                    client_exit=error.client_exit,
                )
                if settled is not None:
                    return settled
            except ResourceBusy as error:
                if error.reason != "lock-deadline":
                    raise
                waits.note("lock-wait")  # a lock wait ended with the queue budget: the loop head revokes
                continue
            except ResourceError:
                if not _queue_wait_ended(_load_run(context)):
                    raise  # a contradiction of the run's authority keeps today's handling
                continue  # the budget ended or the requester left while the pass ran: the loop head revokes the run
            time.sleep(min(1_000_000_000, deadline.remaining_ns(clock)) / 1_000_000_000)
        next_observation = NativeClock().read() + 5_000_000_000
        active_late = _LateAttempts()
        while True:
            data = _load_run(context)
            if data["state"] == "active":
                _withdraw_lost_requester(context)
                if test_directory is None and NativeClock().read() >= next_observation:
                    _observe_active_run(context, active_late)
                    next_observation = NativeClock().read() + 5_000_000_000
                time.sleep(active_poll_ns / 1_000_000_000)
            elif data["state"] == "stopping":
                # Waits for K end with the leave's own budget (design 2.6); the exclusive leave without pending images
                # takes no K and keeps today's unbounded steps.
                image_deadline = (
                    _leave_deadline_ns(data["withdrawal"])
                    if _image_step_locked(data) and data.get("withdrawal") is not None
                    else None
                )
                if not _docker_receipts_terminal(data) or _archive_cleanup_deferred(data):
                    _reconcile_docker_receipt(context, image_deadline)
                    data = _load_run(context)
                if data["pending_operation"] is not None and data["pending_operation"]["kind"] in (
                    "compose-up",
                    "compose-observe",
                    "compose-mutate",
                    "container-control",
                ):
                    (
                        _reconcile_pending_container_control(context)
                        if data["pending_operation"]["kind"] == "container-control"
                        else _reconcile_pending_compose(context)
                    )
                    data = _load_run(context)
                if not _consumers_quiescent(data["consumers"]):
                    _drain_consumers(context)
                if data["retain"]:
                    if not _resources_quiescent(data["resources"]):
                        _stop_compose_resources(context)
                    if not _owned_docker_quiescent(data):
                        _stop_owned_docker_resources(context)
                elif not _resources_removed(data["resources"]):
                    _cleanup_compose_resources(context)
                if not data["retain"] and _owned_cleanup_needed(_load_run(context)):
                    _cleanup_owned_docker_resources(context, image_deadline)
                status = _acknowledge_empty_leave(
                    context,
                    attempt,
                    capacity_stack,
                    capacity,
                    _test_directory=test_directory,
                )
                retention_after_release(context, test_directory, "leave")  # after every lock section (design 2.7)
                return status
            else:
                return run_status(context)


@contextmanager
def managed_run(context: RunContext, *, _test_directory: Path | None = None):
    """Yield one lease and require acknowledged cleanup before successful return."""
    lease = enter_run(context, _test_directory=_test_directory)
    try:
        yield lease
    except BaseException as error:
        outcome = "cancelled" if isinstance(error, (KeyboardInterrupt, SystemExit)) else "failed"
        try:
            leave_run(lease, outcome)
        except ResourceError as cleanup_error:
            # Cleanup cannot replace the primary test failure.
            error.add_note(str(cleanup_error))
        raise
    else:
        leave_run(lease, "passed")


def _wait_resumed_containers(lease: RunLease, targets: tuple[str, ...], *, wait_seconds: float = 120) -> None:
    """Require exact restarted containers to reach their declared health state."""
    data = _require_active_lease(lease, managed=False)
    clock = NativeClock()
    deadline = Deadline.after(clock, wait_seconds)
    docker = _docker()
    if _daemon_identity(docker, deadline) != data["grant"]["daemon_id"]:
        raise ResourceError("Docker daemon changed before retained readiness.")
    while deadline.remaining_ns(clock):
        states = {
            target: docker.call(
                "container",
                "inspect",
                "--format",
                "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}",
                target,
                deadline=deadline,
            ).strip()
            for target in targets
        }
        if all(state in ("healthy", "running") for state in states.values()):
            return
        if any(state in ("dead", "exited", "unhealthy") for state in states.values()):
            raise ResourceError("Retained container failed readiness after exact restart.")
        time.sleep(min(1_000_000_000, deadline.remaining_ns(clock)) / 1_000_000_000)
    raise ResourceError("Retained container readiness deadline expired.")


def resume_run(
    context: RunContext,
    *,
    wait_seconds: float = 14_400,
    _test_directory: Path | None = None,
) -> RunLease:
    """Re-admit and restart one exact stopped-retained run as a new generation."""
    prepare_resume(context, wait_seconds=wait_seconds, _test_directory=_test_directory)
    lease = enter_run(context, _test_directory=_test_directory)
    try:
        restart_retained_resources(lease)
        return lease
    except BaseException as error:
        try:
            leave_run(lease, "failed")
        except ResourceError as cleanup_error:
            error.add_note(str(cleanup_error))
        raise


def _require_retained_bindings_available(data: dict, targets: tuple[str, ...]) -> None:
    """Refuse a retained restart when another host process owns a recorded port."""
    for target in targets:
        managed = data["resources"].get("containers", {}).get(target)
        owned = data["owned_resources"]["containers"].get(target)
        bindings = managed["bindings"] if managed is not None else owned["evidence"].get("bindings", {})
        for name, published in bindings.items():
            protocol = name.rpartition("/")[2]
            socket_type = socket.SOCK_STREAM if protocol == "tcp" else socket.SOCK_DGRAM
            for binding in published:
                address = ipaddress.ip_address(binding["host_ip"])
                family = socket.AF_INET6 if address.version == 6 else socket.AF_INET
                try:
                    with socket.socket(family, socket_type) as probe:
                        probe.bind((binding["host_ip"], binding["host_port"]))
                except OSError as error:
                    raise ResourceError("A retained host port is already owned by another process.") from error


def restart_retained_resources(lease: RunLease) -> None:
    """Restart the exact stopped resources after a resumed generation is admitted."""
    data = _require_active_lease(lease, managed=False)
    managed_targets, owned_targets = _retained_container_targets(data)
    targets = (*managed_targets, *owned_targets)
    if not targets:
        raise ResourceError("Retained resume has no stopped containers.")
    _require_retained_bindings_available(data, targets)
    if _record_mode(data) == AdmissionMode.SHARED:
        _require_restart_fits(lease.context, data, targets)
    if managed_targets:
        expected = _control_generations(data["resources"], managed_targets)
        control_compose_containers(lease, str(uuid.uuid4()), "start", managed_targets, expected)
    for target in owned_targets:
        _run_owned_mutation(lease.context, ("container", "start", target), "container-start", {"container_id": target})
    _wait_resumed_containers(lease, targets)


def _require_restart_fits(context: RunContext, data: dict, targets: tuple[str, ...]) -> None:
    """R12 for a shared retained restart: the restarted targets plus the run's other running containers."""
    adapter = _bound_docker(data)
    wanted = set(targets)
    found = {item.get("Id"): item for item in adapter.objects("container") if item.get("Id") in wanted}
    added = sum(_hard_limit_bytes(found[target]) if target in found else _UNBOUNDED_BYTES for target in targets)
    running = _own_running_bytes(adapter, data, counted_elsewhere=lambda item: item.get("Id") in wanted)
    verdict = own_budget_verdict(
        running_bytes=running, added_bytes=added, phase_container_bytes=_phase_container_bytes(data)
    )
    _refuse_or_record(context, data, verdict)


def _require_active_lease(lease: RunLease, *, managed: bool = True, allow_guardian: bool = False) -> dict:
    if not isinstance(lease, RunLease):
        raise ResourceError("Docker operation requires a run lease.")
    data = _load_run(lease.context)
    actor = asdict(current_process(NativeClock()))
    if (
        data["state"] != "active"
        or data["admission"] != "published"
        or data["generation"] != lease.generation
        or data["grant"]["revision"] != lease.grant_revision
        or actor not in ([data["requester"], data["guardian"]] if allow_guardian else [data["requester"]])
        or asdict(lease.requester) != actor
        or data["source_identity"] != source_identity()
        or data["infrastructure_failure"] is not None
        or (managed and data["grant"]["scope"] != "managed")
    ):
        if data["infrastructure_failure"] is not None:
            cause = data["infrastructure_failure"]
            evidence = data.get("failure_evidence")
            if evidence is None:
                named = f"({cause})"
            elif evidence["reason"] == cause:
                named = f"({cause}, observer {evidence['observer']})"
            else:
                named = f"({cause}; first observed {evidence['reason']}, observer {evidence['observer']})"
            raise ResourceError(f"Docker operation refused: managed infrastructure already failed {named}.")
        raise ResourceError("Docker operation lease differs from current execution authority.")
    return data


def _register_consumer(
    lease: RunLease,
    identity: ProcessIdentity,
    services: tuple[str, ...],
    consumer_id: str,
    *,
    borrow_docker: bool = False,
) -> str:
    if (
        not isinstance(identity, ProcessIdentity)
        or not isinstance(services, tuple)
        or not services
        or len(set(services)) != len(services)
        or any(not isinstance(service, str) or not NAME.fullmatch(service) for service in services)
        or not isinstance(consumer_id, str)
        or not UUID.fullmatch(consumer_id)
        or type(borrow_docker) is not bool
    ):
        raise ResourceError("Consumer registration request is invalid.")
    context = lease.context
    project = context.project
    with lock(project, project.project_id, shared=True), lock(project, f"run-{context.run_id}"):
        data = _require_active_lease(lease, managed=False)
        if not set(services) <= set(data["closure"]):
            raise ResourceError("Consumer requests services outside the selected closure.")
        if borrow_docker and list(services) != data["closure"]:
            raise ResourceError("Docker borrowing requires the entire declared closure.")
        if borrow_docker and data["docker_authority"] is None:
            raise ResourceError("Docker authority must be materialized before borrowing.")
        if asdict(identity) in (data["requester"], data["guardian"]):
            raise ResourceError("Consumer must be a distinct final process.")
        if any(item["identity"] == asdict(identity) for item in data["consumers"].values()):
            raise ResourceError("Consumer process identity is already registered.")
        if observe_process(identity) != "executing":
            raise ResourceError("Consumer is not an executing birth-bound process.")
        if consumer_id in data["consumers"]:
            raise ResourceError("Consumer launch identity is already registered.")
        revision = data["revision"] + 1
        consumer = {
            "id": consumer_id,
            "generation": data["generation"],
            "revision": revision,
            "services": list(services),
            "identity": asdict(identity),
            "stop_on_root_loss": True,
            "state": "active",
            "finished_revision": None,
        }
        if borrow_docker:
            consumer["docker_borrow"] = True
        data["consumers"][consumer_id] = consumer
        data["revision"] = revision
        atomic_json(context.record, data)
        return consumer_id


def register_consumer(lease: RunLease, identity: ProcessIdentity, services: tuple[str, ...]) -> str:
    """Register one already-ready disposable child before it may use selected services."""
    return _register_consumer(lease, identity, services, str(uuid.uuid4()))


def finish_consumer(lease: RunLease, consumer_id: str) -> RunStatus:
    """Tombstone one registered child after native observation proves it stopped."""
    if not isinstance(consumer_id, str) or not UUID.fullmatch(consumer_id):
        raise ResourceError("Consumer identity is invalid.")
    context = lease.context
    project = context.project
    with lock(project, project.project_id, shared=True), lock(project, f"run-{context.run_id}"):
        data = _require_active_lease(lease, managed=False)
        consumer = data["consumers"].get(consumer_id)
        if consumer is None:
            raise ResourceError("Consumer is not registered in this run.")
        if consumer["state"] == "finished":
            return run_status(context)
        if observe_process(_identity_from_json(consumer["identity"])) == "executing":
            raise ResourceBusy("Consumer still executes.")
        data["revision"] += 1
        consumer.update(state="finished", finished_revision=data["revision"])
        atomic_json(context.record, data)
        return run_status(context)


def consumer_identity(lease: RunLease, consumer_id: str) -> ProcessIdentity:
    """Return one active consumer's birth-bound identity under its current grant."""
    if not isinstance(consumer_id, str) or not UUID.fullmatch(consumer_id):
        raise ResourceError("Consumer identity is invalid.")
    data = _require_active_lease(lease, managed=False)
    consumer = data["consumers"].get(consumer_id)
    if consumer is None or consumer["state"] != "active":
        raise ResourceError("Consumer is not active in this run.")
    return _identity_from_json(consumer["identity"])


def stop_consumer(lease: RunLease, consumer_id: str, *, wait_seconds: float = 5) -> RunStatus:
    """Stop one exact registered disposable child and record its terminal observation."""
    if (
        not isinstance(consumer_id, str)
        or not UUID.fullmatch(consumer_id)
        or not isinstance(wait_seconds, (int, float))
        or isinstance(wait_seconds, bool)
        or not 0 < wait_seconds <= 30
    ):
        raise ResourceError("Consumer stop request is invalid.")
    data = _require_active_lease(lease, managed=False)
    consumer = data["consumers"].get(consumer_id)
    if consumer is None or consumer["state"] != "active":
        raise ResourceError("Consumer stop target is not active in this grant.")
    identity = _identity_from_json(consumer["identity"])
    deadline = Deadline.after(NativeClock(), wait_seconds)
    signal_process(identity, signal.SIGTERM)
    while deadline.remaining_ns(NativeClock()) and observe_process(identity) == "executing":
        time.sleep(0.05)
    if observe_process(identity) == "executing":
        signal_process(identity, signal.SIGKILL)
        kill_deadline = Deadline.after(NativeClock(), 5)
        while kill_deadline.remaining_ns(NativeClock()) and observe_process(identity) == "executing":
            time.sleep(0.05)
    if observe_process(identity) == "executing":
        raise ResourceError("Registered consumer remained live after its stop deadline.")
    return finish_consumer(lease, consumer_id)


def transition_phase(lease: RunLease, phase: str) -> RunStatus:
    """Move to one declared phase after all child consumers from the prior phase finish."""
    if not isinstance(phase, str) or not NAME.fullmatch(phase):
        raise ResourceError("Phase transition target is invalid.")
    sample = observe_host_memory(NativeClock())
    context = lease.context
    project = context.project
    with lock(project, project.project_id, shared=True), lock(project, f"run-{context.run_id}"):
        data = _require_active_lease(lease, managed=False)
        if phase not in data["phases"]:
            raise ResourceError("Phase was not declared before admission.")
        if data["grant"]["phase"] == phase:
            return run_status(context)
        if not _consumers_quiescent(data["consumers"]):
            raise ResourceBusy("Active consumers must finish before a phase transition.")
        budget = _phase_budgets(data["phases"], data["initial_phase"])[phase]
        running_bytes = sum(
            item["memory_bytes"]
            for item in data["resources"].get("containers", {}).values()
            if item["state"] == "running"
        )
        if running_bytes > budget.container_bytes:
            raise ResourceError("Running container limits exceed the target phase commitment.")
        if _record_mode(data) == AdmissionMode.SHARED and data["docker_authority"] is not None:
            # R12 (design 2.3): every own running container counts, Compose and dispatcher-created, not the worker
            own = _own_running_bytes(_bound_docker(data), data, counted_elsewhere=lambda _item: False)
            _refuse_or_record(
                context,
                data,
                own_budget_verdict(running_bytes=own, added_bytes=0, phase_container_bytes=budget.container_bytes),
            )
        data["revision"] += 1
        data["grant"]["phase"] = phase
        data["phase_history"].append({"phase": phase, "revision": data["revision"], "memory": asdict(sample)})
        atomic_json(context.record, data)
        return run_status(context)


def launch_python_consumer(
    lease: RunLease,
    mode: str,
    target: str,
    args: tuple[str, ...],
    services: tuple[str, ...],
    *,
    cwd: Path,
    env: dict[str, str] | None = None,
    stdout: int | TextIO | None = None,
    stderr: int | TextIO | None = None,
    borrow_docker: bool = False,
) -> tuple[subprocess.Popen, str]:
    """Launch a fixed Python module, script or code body behind a durable ready/grant barrier."""
    if (
        mode not in ("module", "script", "code")
        or not isinstance(target, str)
        or not target
        or not isinstance(args, tuple)
        or any(not isinstance(item, str) or "\0" in item for item in args)
        or not isinstance(cwd, Path)
        or not isinstance(env, (dict, type(None)))
        or type(borrow_docker) is not bool
        or (
            stdout is not None
            and (
                stdout not in (subprocess.PIPE, subprocess.DEVNULL)
                if isinstance(stdout, int)
                else not hasattr(stdout, "fileno")
            )
        )
        or (
            stderr is not None
            and (
                stderr not in (subprocess.PIPE, subprocess.DEVNULL, subprocess.STDOUT)
                if isinstance(stderr, int)
                else not hasattr(stderr, "fileno")
            )
        )
        or any(
            not isinstance(key, str)
            or not key
            or "=" in key
            or "\0" in key
            or not isinstance(value, str)
            or "\0" in value
            for key, value in (env or {}).items()
        )
    ):
        raise ResourceError("Python consumer command is invalid.")
    data = _require_active_lease(lease, managed=False)
    if borrow_docker and list(services) != data["closure"]:
        raise ResourceError("Docker borrowing requires the entire declared closure.")
    if borrow_docker and data["docker_authority"] is None:
        raise ResourceError("Docker authority must be materialized before borrowing.")
    working_directory = cwd.resolve(strict=True)
    if not working_directory.is_dir() or (
        working_directory != lease.context.project.root and lease.context.project.root not in working_directory.parents
    ):
        raise ResourceError("Python consumer working directory is outside its project.")
    if mode == "module" and not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.]*", target):
        raise ResourceError("Python module target is invalid.")
    if mode == "script":
        script = Path(target).resolve(strict=True)
        if not script.is_file() or lease.context.project.root not in (script, *script.parents):
            raise ResourceError("Python script target is outside its project.")
        target = str(script)
    launch_id = str(uuid.uuid4())
    request, ready, grant = _consumer_launch_paths(lease.context.private_root, launch_id)
    deadline = Deadline.after(NativeClock(), 30)
    atomic_json(
        request,
        {
            "launch_id": launch_id,
            "context": _context_payload(lease.context),
            "mode": mode,
            "target": target,
            "args": list(args),
            "services": list(services),
            "source_identity": data["source_identity"],
            "deadline": asdict(deadline),
            "borrow_docker": borrow_docker,
        },
    )
    child = subprocess.Popen(
        _toolkit_process_command("_python_child", "--request", str(request)),
        cwd=working_directory,
        env=_toolkit_child_environment(lease.context.private_root, env),
        close_fds=True,
        stdout=stdout,
        stderr=stderr,
    )
    identity = None
    consumer_id = None
    try:
        while deadline.remaining_ns(NativeClock()):
            if ready.exists():
                payload = _read_private_json(ready)
                if set(payload) != {"launch_id", "identity"} or payload["launch_id"] != launch_id:
                    raise ResourceError("Python consumer ready identity is contradictory.")
                identity = _identity_from_json(payload["identity"])
                break
            if child.poll() is not None:
                raise ResourceError("Python consumer exited before registration.")
            time.sleep(0.05)
        if identity is None:
            raise ResourceError("Python consumer ready deadline expired.")
        consumer_id = _register_consumer(lease, identity, services, launch_id, borrow_docker=borrow_docker)
        if data["grant"]["scope"] == "managed" and not borrow_docker:
            for service in services:
                require_endpoint(lease, service, identity)
        if borrow_docker:
            data = _load_run(lease.context)
            _publish_consumer_docker_wrapper(lease.context, data, launch_id)
        atomic_json(
            grant,
            {
                "launch_id": launch_id,
                "consumer_id": consumer_id,
                "generation": lease.generation,
                "identity": asdict(identity),
                "source_identity": data["source_identity"],
                "borrow_docker": borrow_docker,
            },
        )
        return child, consumer_id
    except BaseException:
        if identity is not None:
            signal_process(identity, signal.SIGTERM)
        _finish_control_child(child)
        if consumer_id is not None:
            finish_consumer(lease, consumer_id)
        raise


def _run_python_child(request_path: Path) -> int:
    """Wait for exact registration, then execute one fixed target without another OS exec."""
    global _CURRENT_PYTHON_CONSUMER
    request = _read_private_json(request_path)
    if set(request) != {
        "launch_id",
        "context",
        "mode",
        "target",
        "args",
        "services",
        "source_identity",
        "deadline",
        "borrow_docker",
    }:
        raise ResourceError("Python child request is incomplete.")
    launch_id = request["launch_id"]
    if (
        not isinstance(launch_id, str)
        or not UUID.fullmatch(launch_id)
        or request["source_identity"] != source_identity()
        or request["mode"] not in ("module", "script", "code")
        or not isinstance(request["target"], str)
        or not request["target"]
        or not isinstance(request["args"], list)
        or any(not isinstance(item, str) or "\0" in item for item in request["args"])
        or not isinstance(request["services"], list)
        or not request["services"]
        or any(not isinstance(item, str) or not NAME.fullmatch(item) for item in request["services"])
        or type(request["borrow_docker"]) is not bool
    ):
        raise ResourceError("Python child request has incompatible launch/runtime identity.")
    context = _context_from_payload(request["context"])
    expected_request, ready, grant_path = _consumer_launch_paths(context.private_root, launch_id)
    if request_path != expected_request:
        raise ResourceError("Python child request is outside its exact run root.")
    deadline = _deadline_from_json(request["deadline"])
    identity = current_process(NativeClock())
    atomic_json(ready, {"launch_id": launch_id, "identity": asdict(identity)})
    while deadline.remaining_ns(NativeClock()):
        if grant_path.exists():
            grant = _read_private_json(grant_path)
            data = _load_run(context)
            consumer = data["consumers"].get(launch_id)
            if (
                set(grant) != {"launch_id", "consumer_id", "generation", "identity", "source_identity", "borrow_docker"}
                or grant["launch_id"] != launch_id
                or grant["consumer_id"] != launch_id
                or grant["generation"] != data["generation"]
                or grant["identity"] != asdict(identity)
                or grant["source_identity"] != source_identity()
                or grant["borrow_docker"] is not request["borrow_docker"]
                or consumer is None
                or consumer["state"] != "active"
                or consumer["identity"] != asdict(identity)
                or consumer["services"] != request["services"]
                or consumer.get("docker_borrow", False) is not request["borrow_docker"]
                or (request["borrow_docker"] and consumer["services"] != data["closure"])
                or observe_process(_identity_from_json(data["requester"])) != "executing"
            ):
                raise ResourceError("Python child grant is absent, stale or contradictory.")
            if request["borrow_docker"]:
                _require_consumer_docker_wrapper(context, data, launch_id)
                _CURRENT_PYTHON_CONSUMER = _CurrentPythonConsumer(
                    context,
                    launch_id,
                    identity,
                    tuple(request["services"]),
                )
            sys.argv = [request["target"], *request["args"]]
            if request["mode"] == "module":
                sys.path[0] = os.getcwd()
                runpy.run_module(request["target"], run_name="__main__", alter_sys=True)
            elif request["mode"] == "script":
                sys.path[0] = str(Path(request["target"]).parent)
                runpy.run_path(request["target"], run_name="__main__")
            elif request["mode"] == "code":
                sys.path[0] = os.getcwd()
                exec(compile(request["target"], "<managed-consumer>", "exec"), {"__name__": "__main__"})
            else:
                raise ResourceError("Python child target mode is invalid.")
            return 0
        time.sleep(0.05)
    raise ResourceError("Python child grant deadline expired.")


def _require_borrowed_consumer(
    context: RunContext,
    launch_id: str,
    identity: ProcessIdentity,
) -> dict:
    """Revalidate one active full-closure borrower without issuing lifecycle authority."""
    data = _load_run(context)
    consumer = data["consumers"].get(launch_id)
    if (
        data["state"] != "active"
        or data["admission"] != "published"
        or data["source_identity"] != source_identity()
        or consumer is None
        or consumer["state"] != "active"
        or consumer["generation"] != data["generation"]
        or consumer["identity"] != asdict(identity)
        or consumer["services"] != data["closure"]
        or consumer.get("docker_borrow", False) is not True
        or data["docker_authority"] is None
        or observe_process(identity) != "executing"
        or observe_process(_identity_from_json(data["requester"])) != "executing"
    ):
        raise ResourceError("Python consumer Docker authority is stale or inactive.")
    _require_consumer_docker_wrapper(context, data, launch_id)
    return data


def current_python_consumer() -> tuple[RunContext, str, ProcessIdentity, tuple[str, ...], dict, Path] | None:
    """Return the validated process-local borrowing grant, when this process owns one."""
    current = _CURRENT_PYTHON_CONSUMER
    if current is None:
        return None
    if asdict(current_process(NativeClock())) != asdict(current.identity):
        raise ResourceError("Python consumer context belongs to another process.")
    data = _require_borrowed_consumer(current.context, current.launch_id, current.identity)
    wrapper = _consumer_docker_path(current.context, current.launch_id)
    return current.context, current.launch_id, current.identity, current.services, data, wrapper


def current_python_consumer_status(
    context: RunContext,
    launch_id: str,
    identity: ProcessIdentity,
) -> RunStatus:
    """Observe status only while the calling process still owns the borrowed grant."""
    current = current_python_consumer()
    if current is None or current[:3] != (context, launch_id, identity):
        raise ResourceError("Python consumer context is unavailable.")
    return run_status(context)


def validate_consumer_docker_dispatch(context: RunContext, grant_path: Path) -> None:
    """Bind a Docker dispatcher invocation to its exact registered direct parent."""
    grant = _read_private_json(grant_path)
    if set(grant) != {
        "launch_id",
        "consumer_id",
        "generation",
        "identity",
        "source_identity",
        "borrow_docker",
    }:
        raise ResourceError("Python consumer Docker grant is incomplete.")
    launch_id = grant["launch_id"]
    if not isinstance(launch_id, str) or not UUID.fullmatch(launch_id):
        raise ResourceError("Python consumer Docker grant identity is invalid.")
    expected = _consumer_launch_paths(context.private_root, launch_id)[2]
    if grant_path != expected or grant["consumer_id"] != launch_id or grant["borrow_docker"] is not True:
        raise ResourceError("Python consumer Docker grant moved or changed.")
    identity = _identity_from_json(grant["identity"])
    data = _require_borrowed_consumer(context, launch_id, identity)
    if (
        grant["generation"] != data["generation"]
        or grant["source_identity"] != data["source_identity"]
        or os.getppid() != identity.pid
    ):
        raise ResourceError("Docker dispatcher caller is not the registered Python consumer.")


def _drain_consumers(context: RunContext, deadline_ns: int | None = None) -> None:
    """Stop exact disposable children after the original root withdraws or disappears."""
    with _locked_run(context, deadline_ns):
        data = _load_run(context)
        actor = asdict(current_process(NativeClock()))
        if data["state"] != "stopping" or actor not in (data["guardian"], data.get("recovery_actor")):
            raise ResourceError("Only the current lifecycle actor can drain consumers.")
        deadline = _deadline_from_json(data["withdrawal"]["deadline"])
        active = {
            consumer_id: _identity_from_json(item["identity"])
            for consumer_id, item in data["consumers"].items()
            if item["state"] == "active"
        }
    # A consumer that already exited needs no signal and no wait, even when the withdrawal deadline has long expired
    # (a recovery hours after the leave): observe every one once before the deadline-bounded waits.
    active = {
        consumer_id: identity for consumer_id, identity in active.items() if observe_process(identity) == "executing"
    }
    for identity in active.values():
        signal_process(identity, signal.SIGTERM)
    clock = NativeClock()
    if not deadline.remaining_ns(clock):  # a recovery after the leave's deadline gets a fresh bounded window
        deadline = Deadline.after(clock, 10)
    term_expires = min(deadline.expires_ns, clock.read() + 5_000_000_000)
    while active and clock.read() < term_expires:
        stopped = {consumer_id for consumer_id, identity in active.items() if observe_process(identity) != "executing"}
        for consumer_id in stopped:
            active.pop(consumer_id)
        if active:
            time.sleep(min(100_000_000, deadline.remaining_ns(clock)) / 1_000_000_000)
    if active:
        for identity in active.values():
            signal_process(identity, signal.SIGKILL)
        while active and deadline.remaining_ns(clock):
            stopped = {
                consumer_id for consumer_id, identity in active.items() if observe_process(identity) != "executing"
            }
            for consumer_id in stopped:
                active.pop(consumer_id)
            if active:
                time.sleep(min(100_000_000, deadline.remaining_ns(clock)) / 1_000_000_000)
    if active:
        raise ResourceError("Registered consumers remain live at the cleanup deadline.")
    if not data["consumers"]:
        return
    failpoint("recovery-drain-second-section")
    with _locked_run(context, deadline_ns):
        current = _load_run(context)
        if current["state"] != "stopping" or current["guardian"] != data["guardian"]:
            raise ResourceError("Consumer authority changed during exact drain.")
        current["revision"] += 1
        for item in current["consumers"].values():
            if item["state"] == "active":
                if observe_process(_identity_from_json(item["identity"])) == "executing":
                    raise ResourceError("Consumer restarted or remained live during drain.")
                item.update(state="finished", finished_revision=current["revision"])
        atomic_json(context.record, current)


def _compose_plan(
    context: RunContext,
    files: tuple[Path, ...],
    services: tuple[str, ...],
    completed_services: tuple[str, ...] = (),
) -> dict:
    if (
        not isinstance(files, (tuple, list))
        or not files
        or not isinstance(services, (tuple, list))
        or not services
        or len(set(services)) != len(services)
        or any(not isinstance(service, str) or not NAME.fullmatch(service) for service in services)
        or not isinstance(completed_services, (tuple, list))
        or len(set(completed_services)) != len(completed_services)
        or not set(completed_services) <= set(services)
    ):
        raise ResourceError("Compose operation requires explicit files and unique services.")
    roots = (context.project.root, context.private_root)
    described = []
    for supplied in files:
        if not isinstance(supplied, Path):
            raise ResourceError("Compose files must be pathlib paths.")
        path = supplied.resolve(strict=True)
        if path.is_symlink() or not path.is_file() or not any(path.is_relative_to(root) for root in roots):
            raise ResourceError("Compose file is outside the owned project/private roots.")
        described.append({"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    if len({item["path"] for item in described}) != len(described):
        raise ResourceError("Compose file list contains duplicate identities.")
    plan = {
        "project": compose_project_name(context.project.project_id, context.run_id),
        "files": described,
        "services": list(services),
        "labels": {
            PROJECT: context.project.project_id,
            SESSION: context.session_id,
            RUN: context.run_id,
        },
    }
    if completed_services:
        plan["completed_services"] = list(completed_services)
    _validate_compose_plan(plan)
    return plan


def _existing_compose_plan(
    context: RunContext,
    files: tuple[Path, ...],
    services: tuple[str, ...],
    current: dict,
) -> dict:
    """Rebuild a recorded plan while preserving its completed one-shot services."""
    return _compose_plan(context, files, services, tuple(current.get("completed_services", ())))


def _collect_compose_resources(
    docker: DockerPort,
    data: dict,
    plan: dict,
    deadline: Deadline,
    *,
    partial: bool = False,
) -> dict:
    """Inspect the project's containers, networks and volumes into the run's recorded resource shape."""
    expected = plan["labels"]
    completed_services = set(plan.get("completed_services", ()))
    containers = {}
    for item in docker.objects("container", deadline=deadline):
        labels = item.get("Config", {}).get("Labels") or {}
        if labels.get("com.docker.compose.project") != plan["project"]:
            continue
        try:
            container_id = item["Id"]
            service = labels["com.docker.compose.service"]
            created = item["Created"]
            image_id = item["Image"]
            memory = item["HostConfig"]["Memory"]
            nano_cpus = item["HostConfig"]["NanoCpus"]
            restart = item["HostConfig"]["RestartPolicy"]["Name"]
            running = item["State"]["Running"]
            network_ids = sorted(
                network["NetworkID"]
                for network in item["NetworkSettings"]["Networks"].values()
                if network["NetworkID"] or not partial
            )
            volume_names = sorted(mount["Name"] for mount in item["Mounts"] if mount.get("Type") == "volume")
            ports = item["NetworkSettings"]["Ports"] or {}
        except (KeyError, TypeError) as error:
            raise ResourceError("Compose container inspection is incomplete.") from error
        if (
            not re.fullmatch(r"[0-9a-f]{64}", container_id)
            or service not in plan["services"]
            or any(labels.get(key) != value for key, value in expected.items())
            or not isinstance(created, str)
            or not created
            or not isinstance(image_id, str)
            or not IMAGE.fullmatch(image_id)
            or type(memory) is not int
            or memory <= 0
            or type(nano_cpus) is not int
            or nano_cpus <= 0
            or restart != "no"
            or type(running) is not bool
            or (not partial and running == (service in completed_services))
            or any(not re.fullmatch(r"[0-9a-f]{64}", network_id) for network_id in network_ids)
            or any(not NAME.fullmatch(name) for name in volume_names)
            or not isinstance(ports, dict)
        ):
            raise ResourceError("Compose container lacks exact ownership, limits or running state.")
        bindings: dict[str, list[dict[str, str | int]]] = {}
        for target, published in ports.items():
            if not isinstance(target, str) or not re.fullmatch(r"[1-9][0-9]{0,4}/(?:tcp|udp)", target):
                raise ResourceError("Compose port target is invalid.")
            if published is None:
                bindings[target] = []
                continue
            if not isinstance(published, list):
                raise ResourceError("Compose published-port evidence is invalid.")
            bindings[target] = []
            for binding in published:
                if (
                    not isinstance(binding, dict)
                    or not isinstance(binding.get("HostIp"), str)
                    or not isinstance(binding.get("HostPort"), str)
                    or not binding["HostPort"].isdigit()
                    or not 0 < int(binding["HostPort"]) <= 65535
                ):
                    raise ResourceError("Compose published-port identity is incomplete.")
                bindings[target].append({"host_ip": binding["HostIp"], "host_port": int(binding["HostPort"])})
            bindings[target].sort(key=lambda value: (value["host_ip"], value["host_port"]))
        if running:
            observation = observe_container(docker, data["grant"]["daemon_id"], container_id, deadline=deadline)
        else:
            state = item["State"]
            if (
                not isinstance(state.get("StartedAt"), str)
                or not state["StartedAt"]
                or type(state.get("OOMKilled")) is not bool
                or type(item.get("RestartCount")) is not int
                or item["RestartCount"] < 0
                or (
                    service in completed_services
                    and (state.get("ExitCode") != 0 or state["OOMKilled"] or item["RestartCount"] != 0)
                )
            ):
                raise ResourceError("Stopped Compose container state is incomplete.")
            observation = ContainerObservation(
                data["grant"]["daemon_id"],
                container_id,
                state["StartedAt"],
                NativeClock().read(),
                0,
                memory,
                0,
                state["OOMKilled"],
                item["RestartCount"],
            )
        containers[container_id] = {
            "service": service,
            "created": created,
            "image_id": image_id,
            "memory_bytes": memory,
            "nano_cpus": nano_cpus,
            "restart_policy": restart,
            "labels": labels,
            "network_ids": network_ids,
            "volume_names": volume_names,
            "bindings": bindings,
            "state": "running" if running else "stopped",
            "observation": asdict(observation),
        }
    created_services = [item["service"] for item in containers.values()]
    if len(created_services) != len(set(created_services)) or (
        not partial and (set(created_services) != set(plan["services"]) or len(containers) != len(plan["services"]))
    ):
        raise ResourceError("Compose did not create exactly one owned container per selected service.")
    required_networks = {value for item in containers.values() for value in item["network_ids"]}
    networks = {}
    for item in docker.objects("network", deadline=deadline):
        labels = item.get("Labels") or {}
        owned_partial = partial and labels.get("com.docker.compose.project") == plan["project"]
        if item.get("Id") not in required_networks and not owned_partial:
            continue
        if (
            labels.get("com.docker.compose.project") != plan["project"]
            or any(labels.get(key) != value for key, value in expected.items())
            or not isinstance(item.get("Name"), str)
            or not NAME.fullmatch(item["Name"])
            or not isinstance(item.get("Created"), str)
            or not item["Created"]
        ):
            raise ResourceError("Compose network lacks exact ownership evidence.")
        networks[item["Id"]] = {
            "name": item["Name"],
            "created": item["Created"],
            "labels": labels,
            "state": "present",
        }
    required_volumes = {value for item in containers.values() for value in item["volume_names"]}
    volumes = {}
    for item in docker.objects("volume", deadline=deadline):
        labels = item.get("Labels") or {}
        owned_partial = partial and labels.get("com.docker.compose.project") == plan["project"]
        if item.get("Name") not in required_volumes and not owned_partial:
            continue
        if (
            (
                not _anonymous_volume(item["Name"], labels)
                and labels.get("com.docker.compose.project") != plan["project"]
            )
            or not _volume_labels_match(item["Name"], labels, expected)
            or not isinstance(item.get("CreatedAt"), str)
            or not item["CreatedAt"]
            or not isinstance(item.get("Driver"), str)
            or not item["Driver"]
        ):
            raise ResourceError("Compose volume lacks exact ownership evidence.")
        volumes[item["Name"]] = {
            "created_at": item["CreatedAt"],
            "driver": item["Driver"],
            "labels": labels,
            "state": "present",
        }
    if not required_networks <= set(networks) or not required_volumes <= set(volumes):
        raise ResourceError("Compose attachment inventory is incomplete.")
    if partial and not any((containers, networks, volumes)):
        raise ResourceError("Compose partial-resource inventory is empty.")
    return {"compose": plan, "containers": containers, "networks": networks, "volumes": volumes}


def _retain_removed_resource_history(previous: dict, current: dict) -> dict:
    """Keep exact retired identities needed by endpoint and cleanup history."""
    if not previous:
        return current
    if not current:
        current = {"compose": previous["compose"], "containers": {}, "networks": {}, "volumes": {}}
    for kind in ("containers", "networks", "volumes"):
        for identity, resource in previous[kind].items():
            if identity not in current[kind]:
                current[kind][identity] = {**resource, "state": "removed"}
    return current


def _container_control_plan(
    resources: dict,
    action: str,
    targets: tuple[str, ...],
    expected: dict[str, dict],
    request_sha256: str,
) -> dict:
    """Bind one stop/start request to exact recorded container generations."""
    if (
        action not in ("start", "stop")
        or not isinstance(targets, tuple)
        or not targets
        or len(set(targets)) != len(targets)
        or any(not isinstance(target, str) or not re.fullmatch(r"[0-9a-f]{64}", target) for target in targets)
        or not isinstance(expected, dict)
        or set(expected) != set(targets)
        or not isinstance(request_sha256, str)
        or not re.fullmatch(r"[0-9a-f]{64}", request_sha256)
    ):
        raise ResourceError("Container control request is invalid.")
    containers = resources.get("containers") if isinstance(resources, dict) else None
    if not isinstance(containers, dict):
        raise ResourceError("Container control lacks managed resources.")
    allowed_states = {"running", "stopped"}
    for target in targets:
        resource = containers.get(target)
        generation = expected[target]
        if (
            not isinstance(resource, dict)
            or resource.get("state") not in allowed_states
            or not isinstance(generation, dict)
            or set(generation) != {"started_at", "restart_count"}
            or not isinstance(generation["started_at"], str)
            or not generation["started_at"]
            or type(generation["restart_count"]) is not int
            or generation["restart_count"] < 0
            or resource.get("observation", {}).get("started_at") != generation["started_at"]
            or resource.get("observation", {}).get("restart_count") != generation["restart_count"]
        ):
            raise ResourceError("Container control target generation is stale or contradictory.")
        observation = resource["observation"]
        if observation.get("oom_killed") is not False or observation.get("oom_events") != 0:
            raise ResourceError("Container control refused sticky OOM evidence.")
    return {
        "action": action,
        "targets": list(targets),
        "expected": json.loads(json.dumps(expected)),
        "request_sha256": request_sha256,
    }


def _merge_container_observation_history(previous: dict, current: dict) -> dict:
    """Keep sticky OOM evidence while refreshing exact container state."""
    for container_id, resource in current["containers"].items():
        old = previous["containers"].get(container_id)
        if old is None:
            continue
        observation = resource["observation"]
        prior = old["observation"]
        if observation["started_at"] == prior["started_at"]:
            observation["oom_events"] = max(observation["oom_events"], prior["oom_events"])
            observation["oom_killed"] = observation["oom_killed"] or prior["oom_killed"]
    return current


def _stable_control_resources(resources: dict) -> dict:
    """Exclude only periodic container telemetry from control authority."""
    stable = json.loads(json.dumps(resources))
    for resource in stable["containers"].values():
        observation = resource["observation"]
        observation.pop("tick_ns")
        observation.pop("memory_usage_bytes")
    return stable


def _control_generations(resources: dict, targets: tuple[str, ...]) -> dict[str, dict]:
    return {
        target: {
            "started_at": resources["containers"][target]["observation"]["started_at"],
            "restart_count": resources["containers"][target]["observation"]["restart_count"],
        }
        for target in targets
    }


def _owned_control_resources(data: dict) -> dict:
    """Project shim-promoted containers into the existing control contract."""
    containers = {}
    for container_id, resource in data["owned_resources"]["containers"].items():
        control = resource["evidence"].get("control")
        if resource["state"] not in {"running", "stopped"} or control is None:
            continue
        if (
            not isinstance(control, dict)
            or set(control) != {"started_at", "restart_count", "oom_killed", "oom_events"}
            or not isinstance(control["started_at"], str)
            or not control["started_at"]
            or type(control["restart_count"]) is not int
            or control["restart_count"] < 0
            or type(control["oom_killed"]) is not bool
            or type(control["oom_events"]) is not int
            or control["oom_events"] < 0
        ):
            raise ResourceError("Owned container control evidence is incomplete.")
        containers[container_id] = {
            "state": resource["state"],
            "observation": dict(control),
        }
    return {"containers": containers}


def _control_resources(data: dict) -> dict:
    return data["resources"] or _owned_control_resources(data)


def _owned_container_state(docker, target: str, deadline: Deadline) -> tuple[dict, int]:
    """Return one exact inspection's validated ``State`` and ``RestartCount`` of an owned container."""
    item = docker.inspect_exact(ExactInspection("container", target), deadline=deadline).attributes
    state = item.get("State")
    if (
        not isinstance(state, dict)
        or type(state.get("Running")) is not bool
        or not isinstance(state.get("StartedAt"), str)
        or not state["StartedAt"]
        or type(state.get("OOMKilled")) is not bool
        or type(item.get("RestartCount")) is not int
        or item["RestartCount"] < 0
    ):
        raise ResourceError("Owned container control observation is incomplete.")
    return state, item["RestartCount"]


def _observe_owned_control(data: dict, docker, targets: tuple[str, ...], deadline: Deadline) -> dict:
    """Refresh exact shim-owned target state without enumerating the daemon."""
    resources = _owned_control_resources(data)
    for target in targets:
        resource = data["owned_resources"]["containers"].get(target)
        if (
            resource is None
            or target not in resources["containers"]
            or not _validate_owned_identity(docker, resource, deadline=deadline)
        ):
            raise ResourceError("Owned container control target is absent or changed.")
        state, restart_count = _owned_container_state(docker, target, deadline)
        raw = None
        if state["Running"]:
            inspected = (state["StartedAt"], restart_count)
            try:
                raw = observe_container(docker, data["grant"]["daemon_id"], target, deadline=deadline)
            except ResourceError as error:
                # An owned container may exit on its own between the exact inspection and the Engine reads, which
                # then see it stopped or read the stats of its dying cgroup. Only a re-inspection proving it stopped
                # in the inspected generation turns the unreadable sample into a stop; anything else stays a failure.
                if error.reason != "telemetry-unreadable":
                    raise
                state, restart_count = _owned_container_state(docker, target, deadline)
                if state["Running"] or (state["StartedAt"], restart_count) != inspected:
                    raise
        prior = resources["containers"][target]["observation"]
        same_generation = state["StartedAt"] == prior["started_at"] and restart_count == prior["restart_count"]
        if raw is not None:
            if raw.started_at != state["StartedAt"] or raw.restart_count != restart_count:
                raise ResourceError("Owned container generation changed during observation.")
            oom_events = max(raw.oom_events, prior["oom_events"] if same_generation else 0)
            oom_killed = raw.oom_killed or (same_generation and prior["oom_killed"])
        else:
            oom_events = prior["oom_events"] if same_generation else 0
            oom_killed = state["OOMKilled"] or (same_generation and prior["oom_killed"])
        resources["containers"][target] = {
            "state": "running" if raw is not None else "stopped",
            "observation": {
                "started_at": state["StartedAt"],
                "restart_count": restart_count,
                "oom_killed": oom_killed,
                "oom_events": oom_events,
            },
        }
    return resources


def _require_owned_control_unchanged(
    data: dict,
    docker,
    targets: tuple[str, ...],
    deadline: Deadline,
) -> dict:
    recorded = _owned_control_resources(data)
    observed = _observe_owned_control(data, docker, targets, deadline)
    for target in targets:
        if observed["containers"][target]["state"] != recorded["containers"][target]["state"] or _control_generations(
            observed, (target,)
        ) != _control_generations(recorded, (target,)):
            raise ResourceError("Owned container generation changed outside journaled control.")
    return observed


def _publish_owned_control(data: dict, resources: dict, targets: tuple[str, ...]) -> None:
    for target in targets:
        current = resources["containers"][target]
        observation = current["observation"]
        data["owned_resources"]["containers"][target]["state"] = current["state"]
        data["owned_resources"]["containers"][target]["evidence"]["control"] = {
            "started_at": observation["started_at"],
            "restart_count": observation["restart_count"],
            "oom_killed": observation["oom_killed"],
            "oom_events": observation["oom_events"],
        }


def _owned_control_has_oom(resources: dict, targets: tuple[str, ...]) -> bool:
    return any(
        resources["containers"][target]["observation"]["oom_killed"]
        or resources["containers"][target]["observation"]["oom_events"]
        for target in targets
    )


def _publish_owned_oom(lease: RunLease, data: dict, resources: dict, targets: tuple[str, ...]) -> None:
    context = lease.context
    with lock(context.project, context.project.project_id, shared=True), lock(context.project, f"run-{context.run_id}"):
        current = _require_active_lease(lease, managed=False)
        if current["pending_operation"] is not None or current["owned_resources"] != data["owned_resources"]:
            raise ResourceBusy("Container authority changed before OOM evidence publication.")
        _publish_owned_control(current, resources, targets)
        _set_container_oom(current)
        current["revision"] += 1
        atomic_json(context.record, current)


def _validate_control_generation(
    resources: dict,
    caller_id: str,
    caller_generation: dict[str, object],
    allowed_targets: tuple[str, ...],
) -> None:
    """Validate one exact owned caller generation and its bounded target set."""
    containers = resources.get("containers") if isinstance(resources, dict) else None
    if (
        not isinstance(caller_id, str)
        or not re.fullmatch(r"[0-9a-f]{64}", caller_id)
        or not isinstance(caller_generation, dict)
        or set(caller_generation) != {"started_at", "restart_count"}
        or not isinstance(allowed_targets, tuple)
        or len(set(allowed_targets)) != len(allowed_targets)
        or any(not isinstance(target, str) or not re.fullmatch(r"[0-9a-f]{64}", target) for target in allowed_targets)
        or caller_id in allowed_targets
        or not isinstance(containers, dict)
    ):
        raise ResourceError("Container control registration is invalid.")
    caller = containers.get(caller_id)
    if (
        not isinstance(caller, dict)
        or caller.get("state") != "running"
        or caller.get("observation", {}).get("started_at") != caller_generation["started_at"]
        or caller.get("observation", {}).get("restart_count") != caller_generation["restart_count"]
        or any(
            target not in containers or containers[target].get("state") not in {"running", "stopped"}
            for target in allowed_targets
        )
    ):
        raise ResourceError("Container control caller or target generation is stale.")


def register_container_control(
    lease: RunLease,
    caller_id: str,
    caller_generation: dict[str, object],
    allowed_targets: tuple[str, ...],
) -> dict[str, dict]:
    """Validate bridge-supplied product evidence and return exact owned generations."""
    data = _require_active_lease(lease, managed=False)
    resources = _control_resources(data)
    _validate_control_generation(resources, caller_id, caller_generation, allowed_targets)
    if not data["resources"]:
        targets = tuple(resources["containers"])
        resources = _require_owned_control_unchanged(
            data,
            _bound_docker(data),
            targets,
            Deadline.after(NativeClock(), 30),
        )
        if _owned_control_has_oom(resources, targets):
            _publish_owned_oom(lease, data, resources, targets)
            raise ResourceError("Container control refused sticky OOM evidence.")
        _validate_control_generation(resources, caller_id, caller_generation, allowed_targets)
    return _control_generations(resources, tuple(resources["containers"]))


def validate_container_control(
    lease: RunLease,
    caller_id: str,
    caller_generation: dict[str, object],
    allowed_targets: tuple[str, ...],
) -> None:
    """Revalidate exact caller ownership after intent is durable and before dispatch."""
    data = _require_active_lease(lease, managed=False)
    resources = _control_resources(data)
    _validate_control_generation(resources, caller_id, caller_generation, allowed_targets)
    if not data["resources"]:
        targets = (caller_id, *allowed_targets)
        resources = _require_owned_control_unchanged(
            data,
            _bound_docker(data),
            targets,
            Deadline.after(NativeClock(), 30),
        )
        if _owned_control_has_oom(resources, targets):
            raise ResourceError("Container control refused sticky OOM evidence.")
        _validate_control_generation(resources, caller_id, caller_generation, allowed_targets)


def _completed_container_control_replay(
    data: dict,
    operation_id: str,
    plan: dict,
    resources: dict,
) -> dict[str, dict] | None:
    last = data["last_operation"]
    if not isinstance(last, dict) or last.get("id") != operation_id:
        return None
    if last.get("kind") != "container-control" or last.get("plan") != plan:
        raise ResourceError("Container control replay changed its original request.")
    if last.get("outcome") != "complete":
        raise ResourceError("Container control request already failed.")
    generations = last.get("generations")
    if not isinstance(generations, dict) or set(generations) != set(plan["targets"]):
        raise ResourceError("Container control replay lacks its durable result.")
    if generations != _control_generations(resources, tuple(plan["targets"])):
        raise ControlObsolete("Container control replay is obsolete after a target generation change.")
    return generations


def _cancel_container_control(
    lease: RunLease,
    operation: dict,
    error: ResourceError,
    *,
    allow_guardian: bool,
) -> None:
    context = lease.context
    with lock(context.project, context.project.project_id, shared=True), lock(context.project, f"run-{context.run_id}"):
        current = _require_active_lease(lease, managed=False, allow_guardian=allow_guardian)
        if current["pending_operation"] != operation:
            raise ResourceError("Container control journal changed before cancellation.") from error
        current["revision"] += 1
        current["last_operation"] = {
            **operation,
            "outcome": "failed",
            "result_revision": current["revision"],
        }
        current["pending_operation"] = None
        atomic_json(context.record, current)


def _control_owned_containers(
    lease: RunLease,
    data: dict,
    operation_id: str,
    plan: dict,
    targets: tuple[str, ...],
    expected: dict[str, dict],
    deadline: Deadline,
    *,
    allow_guardian: bool,
    pre_dispatch: Callable[[], None] | None,
) -> dict[str, dict]:
    """Execute durable control against exact shim-promoted container identities."""
    context = lease.context
    project = context.project
    docker = _bound_docker(data)
    if docker.observe_daemon().daemon_id != data["grant"]["daemon_id"]:
        raise ResourceError("Docker daemon changed before container control.")
    observed = _require_owned_control_unchanged(data, docker, targets, deadline)
    if _owned_control_has_oom(observed, targets):
        _publish_owned_oom(lease, data, observed, targets)
        raise ResourceError("Container control refused sticky OOM evidence.")
    _container_control_plan(observed, plan["action"], targets, expected, plan["request_sha256"])
    with lock(project, project.project_id, shared=True), lock(project, f"run-{context.run_id}"):
        current = _require_active_lease(lease, managed=False, allow_guardian=allow_guardian)
        if current["pending_operation"] is not None or current["owned_resources"] != data["owned_resources"]:
            raise ResourceBusy("Container authority changed before control acceptance.")
        operation = {
            "id": operation_id,
            "kind": "container-control",
            "generation": current["generation"],
            "revision": current["revision"] + 1,
            "actor": asdict(current_process(NativeClock())),
            "deadline": asdict(deadline),
            "plan": plan,
        }
        current["pending_operation"] = operation
        current["revision"] = operation["revision"]
        atomic_json(context.record, current)
    if pre_dispatch is not None:
        try:
            pre_dispatch()
        except ResourceError as error:
            _cancel_container_control(lease, operation, error, allow_guardian=allow_guardian)
            raise
    expected_state = "stopped" if plan["action"] == "stop" else "running"
    mutation_targets = tuple(target for target in targets if observed["containers"][target]["state"] != expected_state)
    if mutation_targets:
        arguments = ["container", plan["action"]]
        if plan["action"] == "stop":
            arguments.extend(("--time", "15"))
        timeout = max(0.001, deadline.remaining_ns(NativeClock()) / 1_000_000_000)
        result = docker.execute((*arguments, *mutation_targets), timeout_seconds=timeout)
        if result.exit_code:
            raise InfrastructureFailure(f"Docker control failed with exit status {result.exit_code}.")
    result = _observe_owned_control(data, docker, targets, deadline)
    for target in targets:
        resource = result["containers"][target]
        observation = resource["observation"]
        if resource["state"] != expected_state:
            raise ResourceError("Container control result lacks the requested terminal state.")
        if observation["oom_killed"]:
            raise ResourceError("Container control observed sticky OOM evidence.")
        if plan["action"] == "stop" and (
            observation["started_at"] != expected[target]["started_at"]
            or observation["restart_count"] != expected[target]["restart_count"]
        ):
            raise ResourceError("Stopped container generation changed during control.")
        if (
            plan["action"] == "start"
            and target in mutation_targets
            and observation["started_at"] == expected[target]["started_at"]
        ):
            raise ResourceError("Started container did not publish a new generation.")
    with lock(project, project.project_id, shared=True), lock(project, f"run-{context.run_id}"):
        current = _require_active_lease(lease, managed=False, allow_guardian=allow_guardian)
        if current["pending_operation"] != operation:
            raise ResourceError("Container control journal changed before result publication.")
        _publish_owned_control(current, result, targets)
        current["revision"] += 1
        generations = _control_generations(result, targets)
        current["last_operation"] = {
            **operation,
            "outcome": "complete",
            "result_revision": current["revision"],
            "generations": generations,
        }
        current["pending_operation"] = None
        atomic_json(context.record, current)
    return generations


def control_compose_containers(
    lease: RunLease,
    operation_id: str,
    action: str,
    targets: tuple[str, ...],
    expected: dict[str, dict],
    *,
    request_sha256: str | None = None,
    deadline: Deadline | None = None,
    allow_guardian: bool = False,
    pre_dispatch: Callable[[], None] | None = None,
) -> dict[str, dict]:
    """Journal and execute one exact stop/start operation on recorded containers."""
    data = _require_active_lease(lease, managed=False, allow_guardian=allow_guardian)
    if not isinstance(operation_id, str) or not UUID.fullmatch(operation_id):
        raise ResourceError("Container control operation identity is invalid.")
    request_sha256 = (
        request_sha256
        or hashlib.sha256(
            json.dumps(
                {"action": action, "targets": targets, "expected": expected},
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
    )
    plan = {
        "action": action,
        "targets": list(targets) if isinstance(targets, tuple) else targets,
        "expected": json.loads(json.dumps(expected)) if isinstance(expected, dict) else expected,
        "request_sha256": request_sha256,
    }
    _validate_container_control_plan(plan)
    resources = _control_resources(data)
    replay = _completed_container_control_replay(data, operation_id, plan, resources)
    if replay is not None:
        return replay
    if data["pending_operation"] is not None:
        raise ResourceBusy("Another Docker mutation is already pending.")
    plan = _container_control_plan(resources, action, targets, expected, request_sha256)
    context = lease.context
    project = context.project
    clock = NativeClock()
    deadline = deadline or Deadline.after(clock, 30)
    if not data["resources"]:
        return _control_owned_containers(
            lease,
            data,
            operation_id,
            plan,
            targets,
            expected,
            deadline,
            allow_guardian=allow_guardian,
            pre_dispatch=pre_dispatch,
        )
    docker = _docker()
    if _daemon_identity(docker, deadline) != data["grant"]["daemon_id"]:
        raise ResourceError("Docker daemon changed before container control.")
    observed = _merge_container_observation_history(
        data["resources"],
        _collect_compose_resources(docker, data, data["resources"]["compose"], deadline, partial=True),
    )
    if any(
        observed["containers"][target]["observation"]["oom_killed"]
        or observed["containers"][target]["observation"]["oom_events"]
        for target in targets
    ):
        with lock(project, project.project_id, shared=True), lock(project, f"run-{context.run_id}"):
            current = _require_active_lease(lease, managed=False, allow_guardian=allow_guardian)
            if current["pending_operation"] is not None or _stable_control_resources(
                current["resources"]
            ) != _stable_control_resources(data["resources"]):
                raise ResourceBusy("Container authority changed before OOM evidence publication.")
            current["resources"] = observed
            _set_container_oom(current)
            current["revision"] += 1
            atomic_json(context.record, current)
        raise ResourceError("Container control refused sticky OOM evidence.")
    plan = _container_control_plan(observed, action, targets, expected, request_sha256)
    with lock(project, project.project_id, shared=True), lock(project, f"run-{context.run_id}"):
        current = _require_active_lease(lease, managed=False, allow_guardian=allow_guardian)
        if current["pending_operation"] is not None or _stable_control_resources(
            current["resources"]
        ) != _stable_control_resources(data["resources"]):
            raise ResourceBusy("Container authority changed before control acceptance.")
        operation = {
            "id": operation_id,
            "kind": "container-control",
            "generation": current["generation"],
            "revision": current["revision"] + 1,
            "actor": asdict(current_process(NativeClock())),
            "deadline": asdict(deadline),
            "plan": plan,
        }
        current["resources"] = observed
        current["pending_operation"] = operation
        current["revision"] = operation["revision"]
        atomic_json(context.record, current)
    if pre_dispatch is not None:
        try:
            pre_dispatch()
        except ResourceError as error:
            _cancel_container_control(lease, operation, error, allow_guardian=allow_guardian)
            raise
    expected_state = "stopped" if action == "stop" else "running"
    mutation_targets = tuple(target for target in targets if observed["containers"][target]["state"] != expected_state)
    if mutation_targets:
        arguments = ["container", action]
        if action == "stop":
            arguments.extend(("--time", "15"))
        docker.call(*arguments, *mutation_targets, deadline=deadline)
    result = _merge_container_observation_history(
        observed,
        _collect_compose_resources(docker, data, data["resources"]["compose"], deadline, partial=True),
    )
    for target in targets:
        resource = result["containers"].get(target)
        if resource is None or resource["state"] != expected_state:
            raise ResourceError("Container control result lacks the requested terminal state.")
        observation = resource["observation"]
        if observation["oom_killed"] or observation["oom_events"]:
            raise ResourceError("Container control observed sticky OOM evidence.")
        if action == "stop" and (
            observation["started_at"] != expected[target]["started_at"]
            or observation["restart_count"] != expected[target]["restart_count"]
        ):
            raise ResourceError("Stopped container generation changed during control.")
        if (
            action == "start"
            and target in mutation_targets
            and observation["started_at"] == expected[target]["started_at"]
        ):
            raise ResourceError("Started container did not publish a new generation.")
    with lock(project, project.project_id, shared=True), lock(project, f"run-{context.run_id}"):
        current = _require_active_lease(lease, managed=False, allow_guardian=allow_guardian)
        if current["pending_operation"] != operation:
            raise ResourceError("Container control journal changed before result publication.")
        current["resources"] = result
        current["revision"] += 1
        generations = _control_generations(current["resources"], targets)
        current["last_operation"] = {
            **operation,
            "outcome": "complete",
            "result_revision": current["revision"],
            "generations": generations,
        }
        current["pending_operation"] = None
        atomic_json(context.record, current)
        return generations


def publish_endpoint(
    lease: RunLease,
    service: str,
    protocol: str,
    container_id: str,
    container_port: int,
    host: str,
    port: int,
    *,
    audience: str = "host",
    namespace: str | None = None,
) -> Endpoint:
    """Publish a ready endpoint only when it maps to the current exact resource."""
    if (
        not isinstance(service, str)
        or not NAME.fullmatch(service)
        or not isinstance(protocol, str)
        or not NAME.fullmatch(protocol)
        or not isinstance(container_id, str)
        or not re.fullmatch(r"[0-9a-f]{64}", container_id)
        or type(container_port) is not int
        or not 0 < container_port <= 65535
        or not isinstance(host, str)
        or not host
        or type(port) is not int
        or not 0 < port <= 65535
        or audience not in ("host", "container")
        or (namespace is not None and (not isinstance(namespace, str) or not NAME.fullmatch(namespace)))
    ):
        raise ResourceError("Endpoint publication request is invalid.")
    context = lease.context
    project = context.project
    with lock(project, project.project_id, shared=True), lock(project, f"run-{context.run_id}"):
        data = _require_active_lease(lease, managed=False)
        resource = _endpoint_container(data, container_id)
        if resource is None or resource["state"] != "running" or resource["service"] != service:
            raise ResourceError("Endpoint target is not the current selected running service.")
        if audience == "host":
            try:
                address = ipaddress.ip_address(host)
            except ValueError as error:
                raise ResourceError("Managed host endpoint address is invalid.") from error
            key = f"{container_port}/tcp"
            if not address.is_loopback or {"host_ip": host, "host_port": port} not in resource["bindings"].get(key, []):
                raise ResourceError("Host endpoint is not an exact loopback binding of its target.")
        elif host != service or port != container_port:
            raise ResourceError("Container endpoint must use its selected service alias and target port.")
        history = data["endpoints"].setdefault(service, [])
        revision = data["revision"] + 1
        for prior in history:
            if prior["active"] and prior["namespace"] == namespace:
                prior.update(active=False, retired_revision=revision)
        endpoint = Endpoint(
            service,
            protocol,
            host,
            port,
            container_id,
            container_port,
            audience,
            namespace,
            data["generation"],
            revision,
        )
        history.append({**asdict(endpoint), "active": True, "retired_revision": None})
        data["revision"] = revision
        atomic_json(context.record, data)
        return endpoint


def publish_host_endpoint(
    lease: RunLease, service: str, protocol: str, container_port: int, *, namespace: str | None = None
) -> Endpoint:
    """Publish the unique dynamic loopback binding for one selected service."""
    data = _require_active_lease(lease, managed=False)
    containers = dict(data["resources"].get("containers", {}))
    for container_id, resource in data["owned_resources"]["containers"].items():
        evidence = resource["evidence"]
        if "service" in evidence:
            containers.setdefault(
                container_id,
                {
                    "state": resource["state"],
                    "service": evidence["service"],
                    "bindings": evidence.get("bindings", {}),
                },
            )
    matches = [
        (container_id, resource)
        for container_id, resource in containers.items()
        if resource["state"] == "running" and resource["service"] == service
    ]
    if len(matches) != 1:
        raise ResourceError("Selected service has no unique running container.")
    container_id, resource = matches[0]
    bindings = resource["bindings"].get(f"{container_port}/tcp", ())
    if len(bindings) != 1:
        raise ResourceError("Selected service has no unique dynamic loopback binding.")
    binding = bindings[0]
    return publish_endpoint(
        lease,
        service,
        protocol,
        container_id,
        container_port,
        binding["host_ip"],
        binding["host_port"],
        namespace=namespace,
    )


def require_endpoint(
    lease: RunLease, service: str, consumer: ProcessIdentity, *, namespace: str | None = None
) -> Endpoint:
    """Return the current endpoint to the exact granted root; stale revisions stay retired."""
    if (
        not isinstance(service, str)
        or not NAME.fullmatch(service)
        or not isinstance(consumer, ProcessIdentity)
        or (namespace is not None and (not isinstance(namespace, str) or not NAME.fullmatch(namespace)))
    ):
        raise ResourceError("Endpoint request identity is invalid.")
    data = _require_active_lease(lease, managed=False)
    serialized = asdict(consumer)
    authorized = serialized == data["requester"] or any(
        item["state"] == "active" and item["identity"] == serialized and service in item["services"]
        for item in data["consumers"].values()
    )
    if not authorized or observe_process(consumer) != "executing":
        raise ResourceError("Endpoint consumer is not active in the current grant.")
    active = [item for item in data["endpoints"].get(service, []) if item["active"] and item["namespace"] == namespace]
    if len(active) != 1:
        raise ResourceError("Selected service has no unique current endpoint.")
    item = active[0]
    resource = _endpoint_container(data, item["container_id"])
    if resource is None or resource["state"] != "running":
        raise ResourceError("Current endpoint target is no longer an active resource.")
    return Endpoint(**{key: item[key] for key in Endpoint.__dataclass_fields__})


def _endpoint_container(data: dict, container_id: str) -> dict | None:
    """Return the endpoint-relevant view of a managed or shim-promoted container."""
    managed = data["resources"].get("containers", {}).get(container_id)
    if managed is not None:
        return managed
    owned = data["owned_resources"]["containers"].get(container_id)
    if owned is None or "service" not in owned["evidence"]:
        return None
    return {
        "state": owned["state"],
        "service": owned["evidence"]["service"],
        "bindings": owned["evidence"].get("bindings", {}),
    }


def _reconcile_pending_compose(context: RunContext, deadline_ns: int | None = None) -> None:
    """Resolve a lost Compose result from its exact plan and unique ownership labels."""
    with _locked_run(context, deadline_ns):
        data = _load_run(context)
        operation = data["pending_operation"]
        if operation is None:
            return
        if operation["kind"] not in ("compose-up", "compose-observe", "compose-mutate") or data["state"] != "stopping":
            raise ResourceError("Only a withdrawn lost Compose result can be reconciled here.")
        actor = asdict(current_process(NativeClock()))
        if actor not in (data["guardian"], data.get("recovery_actor")):
            raise ResourceError("Only the current lifecycle actor can reconcile this Compose result.")
    deadline = (
        _deadline_from_json(data["withdrawal"]["deadline"])
        if actor == data["guardian"]
        else Deadline.after(NativeClock(), 30)
    )
    docker = _docker()
    if _daemon_identity(docker, deadline) != data["grant"]["daemon_id"]:
        raise ResourceError("Docker daemon changed during Compose result reconciliation.")
    expected = operation["plan"]["labels"]

    def owned(item: dict) -> bool:
        """Return whether an inspection belongs to this exact run generation."""
        labels = item.get("Labels") or item.get("Config", {}).get("Labels") or {}
        return all(labels.get(key) == value for key, value in expected.items())

    candidates = {
        "containers": [item for item in docker.objects("container", deadline=deadline) if owned(item)],
        "networks": [item for item in docker.objects("network", deadline=deadline) if owned(item)],
        "volumes": [item for item in docker.objects("volume", deadline=deadline) if owned(item)],
    }
    if not any(candidates.values()):
        with _locked_run(context, deadline_ns):
            current = _load_run(context)
            if current["pending_operation"] != operation:
                raise ResourceError("Compose journal changed during absence reconciliation.")
            current["revision"] += 1
            current["last_operation"] = {
                **operation,
                "outcome": "failed",
                "result_revision": current["revision"],
            }
            current["pending_operation"] = None
            current["resources"] = (
                _retain_removed_resource_history(current["resources"], {})
                if operation["kind"] == "compose-mutate"
                else {}
            )
            _retire_endpoints(current["endpoints"], current["revision"])
            current["infrastructure_failure"] = (
                "compose-mutation-incomplete" if operation["kind"] == "compose-mutate" else "compose-up-failed"
            )
            atomic_json(context.record, current)
        return
    resources = _collect_compose_resources(docker, data, operation["plan"], deadline, partial=True)
    completed_services = set(operation["plan"].get("completed_services", ()))
    complete = {item["service"] for item in resources["containers"].values()} == set(
        operation["plan"]["services"]
    ) and all(
        item["state"] == ("stopped" if item["service"] in completed_services else "running")
        for item in resources["containers"].values()
    )
    if operation["kind"] == "compose-mutate":
        resources = _retain_removed_resource_history(data["resources"], resources)
    with _locked_run(context, deadline_ns):
        current = _load_run(context)
        if current["pending_operation"] != operation or (
            operation["kind"] != "compose-mutate" and current["resources"]
        ):
            raise ResourceError("Compose authority changed during result reconciliation.")
        current["resources"] = resources
        current["revision"] += 1
        current["last_operation"] = {
            **operation,
            "outcome": "complete" if complete else "failed",
            "result_revision": current["revision"],
        }
        current["pending_operation"] = None
        if not complete:
            current["infrastructure_failure"] = (
                "compose-mutation-incomplete" if operation["kind"] == "compose-mutate" else "compose-up-failed"
            )
        atomic_json(context.record, current)


def _reconcile_pending_container_control(context: RunContext, deadline_ns: int | None = None) -> None:
    """Record the observed result of an interrupted exact stop/start without compensating."""
    with _locked_run(context, deadline_ns):
        data = _load_run(context)
        operation = data["pending_operation"]
        actor = asdict(current_process(NativeClock()))
        if (
            operation is None
            or operation["kind"] != "container-control"
            or data["state"] not in ("active", "stopping")
            or actor
            not in (
                (data["guardian"],) if data["state"] == "active" else (data["guardian"], data.get("recovery_actor"))
            )
        ):
            raise ResourceError("Only the lifecycle actor can reconcile interrupted container control.")
    deadline = Deadline.after(NativeClock(), 30)
    if data["resources"]:
        docker = _docker()
        if _daemon_identity(docker, deadline) != data["grant"]["daemon_id"]:
            raise ResourceError("Docker daemon changed before container control reconciliation.")
        observed = _merge_container_observation_history(
            data["resources"],
            _collect_compose_resources(docker, data, data["resources"]["compose"], deadline, partial=True),
        )
    else:
        docker = _bound_docker(data)
        if docker.observe_daemon().daemon_id != data["grant"]["daemon_id"]:
            raise ResourceError("Docker daemon changed before container control reconciliation.")
        observed = _observe_owned_control(data, docker, tuple(operation["plan"]["targets"]), deadline)
    oom = any(
        item["observation"]["oom_killed"] or item["observation"]["oom_events"]
        for item in observed["containers"].values()
    )
    with _locked_run(context, deadline_ns):
        current = _load_run(context)
        if current["pending_operation"] != operation:
            raise ResourceError("Container control journal changed during reconciliation.")
        if current["resources"]:
            current["resources"] = observed
        else:
            _publish_owned_control(current, observed, tuple(operation["plan"]["targets"]))
        current["revision"] += 1
        current["last_operation"] = {
            **operation,
            "outcome": "failed",
            "result_revision": current["revision"],
        }
        current["pending_operation"] = None
        if current["infrastructure_failure"] is None:
            if oom:
                _set_container_oom(current)
            else:
                current["infrastructure_failure"] = "container-control-incomplete"
        atomic_json(context.record, current)


def _stop_compose_resources(context: RunContext, deadline_ns: int | None = None) -> None:
    """Stop exact retained containers and retire endpoints before releasing capacity."""
    with _locked_run(context, deadline_ns):
        data = _load_run(context)
        actor = asdict(current_process(NativeClock()))
        resuming = data["pending_operation"] is not None
        if (
            data["state"] != "stopping"
            or not data["retain"]
            or actor not in (data["guardian"], data.get("recovery_actor"))
            or (resuming and data["pending_operation"]["kind"] != "retain-stop")
        ):
            raise ResourceError("Current lifecycle actor cannot stop retained resources.")
        if _resources_quiescent(data["resources"]):
            return
        if resuming:
            operation = data["pending_operation"]
        else:
            operation = {
                "id": str(uuid.uuid4()),
                "kind": "retain-stop",
                "generation": data["generation"],
                "revision": data["revision"] + 1,
                "actor": actor,
                "deadline": data["withdrawal"]["deadline"],
                "plan": data["resources"]["compose"],
            }
            data["pending_operation"] = operation
            data["revision"] = operation["revision"]
            atomic_json(context.record, data)
    deadline = _deadline_from_json(operation["deadline"])
    docker = _docker()
    if _daemon_identity(docker, deadline) != data["grant"]["daemon_id"]:
        raise ResourceError("Docker daemon changed before retained stop.")
    original_resources = data["resources"]
    stopped = json.loads(json.dumps(original_resources))
    current = {item["Id"]: item for item in docker.objects("container", deadline=deadline)}
    for container_id, recorded in stopped["containers"].items():
        item = current.get(container_id)
        labels = (item or {}).get("Config", {}).get("Labels") or {}
        if (
            item is None
            or item.get("Created") != recorded["created"]
            or item.get("Image") != recorded["image_id"]
            or any(labels.get(key) != value for key, value in stopped["compose"]["labels"].items())
            or item.get("State", {}).get("Running") not in (True, False)
        ):
            raise ResourceError("Retained container identity changed before stop.")
        if item["State"]["Running"]:
            observation = observe_container(
                docker,
                data["grant"]["daemon_id"],
                container_id,
                previous=ContainerObservation(**recorded["observation"]),
                deadline=deadline,
            )
            recorded["observation"] = asdict(observation)
            docker.call("container", "stop", "--time", "15", container_id, deadline=deadline)
    after = {item["Id"]: item for item in docker.objects("container", deadline=deadline)}
    oom = False
    for container_id, recorded in stopped["containers"].items():
        item = after.get(container_id)
        state = (item or {}).get("State", {})
        labels = (item or {}).get("Config", {}).get("Labels") or {}
        if (
            item is None
            or item.get("Created") != recorded["created"]
            or item.get("Image") != recorded["image_id"]
            or any(labels.get(key) != value for key, value in stopped["compose"]["labels"].items())
            or state.get("Running") is not False
            or type(state.get("OOMKilled")) is not bool
            or item.get("RestartCount") != recorded["observation"]["restart_count"]
        ):
            raise ResourceError("Retained container stop result is incomplete or contradictory.")
        recorded["observation"]["oom_killed"] = recorded["observation"]["oom_killed"] or state["OOMKilled"]
        oom = oom or recorded["observation"]["oom_killed"] or recorded["observation"]["oom_events"] > 0
        recorded["state"] = "stopped"
    with _locked_run(context, deadline_ns):
        current_data = _load_run(context)
        if current_data["pending_operation"] != operation or current_data["resources"] != original_resources:
            raise ResourceError("Retained resource authority changed during stop.")
        current_data["resources"] = stopped
        current_data["revision"] += 1
        _retire_endpoints(current_data["endpoints"], current_data["revision"])
        current_data["last_operation"] = {
            **operation,
            "outcome": "complete",
            "result_revision": current_data["revision"],
        }
        current_data["pending_operation"] = None
        if oom:
            _set_container_oom(current_data)
        atomic_json(context.record, current_data)


def _cleanup_compose_resources(context: RunContext, deadline_ns: int | None = None) -> None:
    existing = _load_run(context)["pending_operation"]
    if existing is not None and existing["kind"] in (
        "compose-up",
        "compose-observe",
        "compose-mutate",
        "container-control",
    ):
        (
            _reconcile_pending_container_control(context, deadline_ns)
            if existing["kind"] == "container-control"
            else _reconcile_pending_compose(context, deadline_ns)
        )
    with _locked_run(context, deadline_ns):
        data = _load_run(context)
        actor = asdict(current_process(NativeClock()))
        resuming = data["pending_operation"] is not None
        if (
            data["state"] != "stopping"
            or actor not in (data["guardian"], data.get("recovery_actor"))
            or (resuming and data["pending_operation"]["kind"] != "cleanup")
        ):
            raise ResourceError("Current lifecycle actor cannot begin exact resource cleanup.")
        if _resources_removed(data["resources"]):
            return
        if resuming:
            operation = data["pending_operation"]
        else:
            operation = {
                "id": str(uuid.uuid4()),
                "kind": "cleanup",
                "generation": data["generation"],
                "revision": data["revision"] + 1,
                "actor": actor,
                "deadline": (
                    data["withdrawal"]["deadline"]
                    if actor == data["guardian"]
                    else asdict(Deadline.after(NativeClock(), 30))
                ),
                "plan": data["resources"]["compose"],
            }
            data["pending_operation"] = operation
            data["revision"] = operation["revision"]
            atomic_json(context.record, data)
    docker = _docker()
    deadline = (
        _deadline_from_json(operation["deadline"]) if actor == operation["actor"] else Deadline.after(NativeClock(), 30)
    )
    if _daemon_identity(docker, deadline) != data["grant"]["daemon_id"]:
        raise ResourceError("Docker daemon changed before exact cleanup.")
    original_resources = data["resources"]
    resources = json.loads(json.dumps(original_resources))
    current_containers = {item["Id"]: item for item in docker.objects("container", deadline=deadline)}
    oom = False
    for container_id, recorded in resources["containers"].items():
        if recorded["state"] == "removed":
            continue
        item = current_containers.get(container_id)
        if item is None and resuming:
            recorded["state"] = "removed"
            continue
        labels = (item or {}).get("Config", {}).get("Labels") or {}
        if (
            item is None
            or item.get("Created") != recorded["created"]
            or item.get("Image") != recorded["image_id"]
            or any(labels.get(key) != value for key, value in resources["compose"]["labels"].items())
        ):
            raise ResourceError("Container identity changed before exact cleanup.")
        if recorded["state"] == "running":
            observation = observe_container(
                docker,
                data["grant"]["daemon_id"],
                container_id,
                previous=ContainerObservation(**recorded["observation"]),
                deadline=deadline,
            )
            recorded["observation"] = asdict(observation)
        else:
            state = item.get("State", {})
            if (
                state.get("Running") is not False
                or type(state.get("OOMKilled")) is not bool
                or item.get("RestartCount") != recorded["observation"]["restart_count"]
            ):
                raise ResourceError("Retained container state changed before exact cleanup.")
            recorded["observation"]["oom_killed"] = recorded["observation"]["oom_killed"] or state["OOMKilled"]
        oom = oom or recorded["observation"]["oom_killed"] or recorded["observation"]["oom_events"] > 0
        docker.call("container", "rm", "-f", "-v", container_id, deadline=deadline)
        recorded["state"] = "removed"
    current_networks = {item["Id"]: item for item in docker.objects("network", deadline=deadline)}
    for network_id, recorded in resources["networks"].items():
        if recorded["state"] == "removed":
            continue
        item = current_networks.get(network_id)
        if item is None and resuming:
            recorded["state"] = "removed"
            continue
        if (
            item is None
            or item.get("Name") != recorded["name"]
            or item.get("Created") != recorded["created"]
            or (item.get("Labels") or {}) != recorded["labels"]
        ):
            raise ResourceError("Network identity changed before exact cleanup.")
        docker.call("network", "rm", network_id, deadline=deadline)
        recorded["state"] = "removed"
    current_volumes = {item["Name"]: item for item in docker.objects("volume", deadline=deadline)}
    for name, recorded in resources["volumes"].items():
        if recorded["state"] == "removed":
            continue
        item = current_volumes.get(name)
        if item is None and (resuming or recorded["labels"] == {"com.docker.volume.anonymous": ""}):
            recorded["state"] = "removed"
            continue
        if (
            item is None
            or item.get("CreatedAt") != recorded["created_at"]
            or item.get("Driver") != recorded["driver"]
            or (item.get("Labels") or {}) != recorded["labels"]
        ):
            raise ResourceError("Volume identity changed before exact cleanup.")
        docker.call("volume", "rm", name, deadline=deadline)
        recorded["state"] = "removed"
    after_containers = {item["Id"] for item in docker.objects("container", deadline=deadline)}
    after_networks = {item["Id"] for item in docker.objects("network", deadline=deadline)}
    after_volumes = {item["Name"] for item in docker.objects("volume", deadline=deadline)}
    if (
        any(container_id in after_containers for container_id in resources["containers"])
        or any(network_id in after_networks for network_id in resources["networks"])
        or any(name in after_volumes for name in resources["volumes"])
    ):
        raise ResourceError("Exact Docker resources remain after cleanup.")
    with _locked_run(context, deadline_ns):
        current = _load_run(context)
        if current["pending_operation"] != operation or current["resources"] != original_resources:
            raise ResourceError("Resource authority changed during exact cleanup.")
        current["resources"] = resources
        current["revision"] += 1
        _retire_endpoints(current["endpoints"], current["revision"])
        current["last_operation"] = {
            **operation,
            "outcome": "complete",
            "result_revision": current["revision"],
        }
        current["pending_operation"] = None
        if oom:
            _set_container_oom(current)
        atomic_json(context.record, current)


def _require_ungranted(data: dict) -> None:
    if data.get("retained_grant") is not None:
        if (
            data["admission"] != "never-granted"
            or data["state"] not in ("prepared", "queued", "admitting")
            or (data["pending_operation"] is not None and data["pending_operation"].get("kind") != "guardian-start")
        ):
            raise ResourceError("Retained resume has contradictory ungranted authority.")
        return
    if (
        data["admission"] not in ("never-granted", "revoked")
        or data["generation"] != 0
        or any(data.get(key) for key in ("resources", "endpoints", "consumers", "last_operation"))
        or data.get("docker_receipts")
        or any(data.get("owned_resources", {}).values())
        or (data["pending_operation"] is not None and data["pending_operation"].get("kind") != "guardian-start")
    ):
        raise ResourceError("Execution authority requires original admitted-run recovery.")


def _verify_interrupted_release(context: RunContext, data: dict, deadline_ns: int | None = None) -> dict:
    """Return one exact terminal release authority for its retained target; the registry wait ends at the deadline."""
    authority = data.get("release_authority")
    release_actor = authority["requester"] if authority is not None else data.get("recovery_actor")
    if release_actor is None:
        raise ResourceError("Interrupted retained release lacks its exact authority.")
    paths = (
        (Path(authority["record"]),) if authority is not None else tuple(sorted(context.record.parent.glob("*.json")))
    )
    matches: list[RunContext] = []
    matched: list[dict] = []  # each match's loaded record, beside ``matches``
    for path in paths:
        if path == context.record:
            continue
        raw = _read_private_json(path)
        if raw.get("requester") != release_actor:
            continue
        identity_fields = ("project_root", "run_id", "session_id", "private_root")
        if any(not isinstance(raw.get(field), str) for field in identity_fields):
            raise ResourceError("Interrupted retained release record is invalid.")
        candidate = RunContext(
            Context(Path(raw["project_root"]), context.project.state),
            raw["run_id"],
            raw["session_id"],
            path,
            Path(raw["private_root"]),
        )
        release_data = _load_run(candidate)
        if (
            release_data["project_id"] != data["project_id"]
            or release_data["project_root"] != data["project_root"]
            or release_data["session_id"] != data["session_id"]
            or release_data["account_directory"] != data["account_directory"]
            or release_data["source_identity"] != data["source_identity"]
            or release_data["topology"] != data["topology"]
            or release_data["entrypoint"] != "release"
            or release_data["purpose"] != "retained-release"
            or release_data["profile"] != "retained-release"
            or release_data["services"]
            or release_data["closure"]
            or release_data["mode"] != "managed"
            or release_data["retain"]
            or release_data["artifact"] is not None
            or release_data["generation"] != 1
            or release_data["test_outcome"] not in ("failed", "cancelled")
            or release_data["cleanup_outcome"] != "complete"
            or release_data.get("release_authority") is not None
            or not completed_run_is_quiescent(release_data)
            or candidate.private_root.exists()
            or candidate.private_root.is_symlink()
        ):
            continue
        matches.append(candidate)
        matched.append(release_data)
    if len(matches) != 1:
        raise ResourceError("Interrupted retained release authority is absent or ambiguous.")
    release_context = matches[0]
    if authority is not None and (
        release_context.run_id != authority["run_id"] or str(release_context.record) != authority["record"]
    ):
        raise ResourceError("Interrupted retained release authority changed identity.")
    # The matched helper's own guardian, never the loop's last candidate: a release still in its guardian's tail is
    # live (I-LR3).
    _require_actor_gone(matched[0]["guardian"], "guardian of the interrupted release")
    with lock(context.project, "registry", deadline_ns=deadline_ns):
        if load(context.project)["runs"].get(release_context.run_id) is not None:
            raise ResourceError("Interrupted retained release is not terminal in its project index.")
    return authority or {
        "generation": data["generation"],
        "record": str(release_context.record),
        "requester": release_actor,
        "revision": data["revision"],
        "run_id": release_context.run_id,
    }


def release_run(
    context: RunContext,
    *,
    _test_state: Path | None = None,
    _test_directory: Path | None = None,
) -> RunStatus:
    """Queue once, then remove an exact stopped-retained run under fresh capacity exclusion."""
    retained = _load_run(context)
    if retained["state"] != "stopped-retained" or not retained["retain"]:
        raise ResourceError("Release requires an exact stopped-retained run.")
    release = prepare_run(
        RunRequest(
            context.project.root,
            "release",
            RELEASE_PURPOSE,
            (),
            (),
            RELEASE_PURPOSE,
            parent_session=context.session_id,
            wait_seconds=180,
        ),
        _test_state=_test_state,
        _test_directory=_test_directory,
        _release_helper=True,
    )
    configure_run(
        release,
        services=(),
        closure=(),
        phases={"release": PhaseBudget(0, 128 * 1024**2, 1, 1)},
        initial_phase="release",
        topology=retained["topology"],
    )
    result = None
    with managed_run(release, _test_directory=_test_directory):
        _docker()
        project = context.project
        actor = asdict(current_process(NativeClock()))
        with lock(project, project.project_id, shared=True), lock(project, f"run-{context.run_id}"):
            data = _load_run(context)
            if (
                data["state"] != "stopped-retained"
                or not data["retain"]
                or not _consumers_quiescent(data["consumers"])
                or not _endpoints_quiescent(data["endpoints"])
                or not _resources_quiescent(data["resources"])
            ):
                raise ResourceError("Retained run changed before explicit release.")
            data.update(
                state="stopping",
                recovery_actor=actor,
                release_authority={
                    "generation": data["generation"],
                    "record": str(release.record),
                    "requester": actor,
                    "revision": data["revision"] + 1,
                    "run_id": release.run_id,
                },
                recovery_outcome="pending",
                cleanup_outcome="pending",
                quiescent=False,
            )
            data["revision"] += 1
            atomic_json(context.record, data)
            failpoint("release-stopping-written")
        _cleanup_compose_resources(context)
        if _owned_cleanup_needed(_load_run(context)):
            # The release budget bounds the image step's K wait (the requester holds no lock here); the exclusive path
            # with nothing pending is today's.
            bounded = _image_step_locked(data)
            _cleanup_owned_docker_resources(
                context, time.monotonic_ns() + _RELEASE_SECONDS * 1_000_000_000 if bounded else None
            )
        _remove_private_root(context)
        with lock(project, project.project_id, shared=True), lock(project, f"run-{context.run_id}"):
            data = _load_run(context)
            if (
                data["pending_operation"] is not None
                or not _docker_receipts_terminal(data)
                or not _resources_removed(data["resources"])
                or not _owned_docker_removed(data)
            ):
                raise ResourceError("Retained release did not reach exact quiescence.")
            data.update(
                state="complete",
                recovery_actor=actor,
                release_authority=None,
                recovery_outcome="complete",
                cleanup_outcome="complete",
                quiescent=True,
            )
            data["revision"] += 1
            atomic_json(context.record, data)
            with lock(project, "registry"):
                registry = load(project)
                if registry["runs"].get(context.run_id) != str(context.record):
                    raise ResourceError("Retained project authority changed during release.")
                del registry["runs"][context.run_id]
                save(project, registry)
            result = run_status(context)
    return result


def recover_run(
    context: RunContext,
    *,
    drain_live_consumers: bool = True,
    takeover_attempt: str | None = None,
    _test_directory: Path | None = None,
    _deadline_ns: int | None = None,
) -> RunStatus:
    """Reconcile original authority without rewriting an old gate result.

    Live or ambiguous roots raise ResourceBusy. Exact resource-bearing grants use their durable identities;
    contradictory or unresolved operations remain errors. An operator drains live registered consumers
    (``drain_live_consumers=True``); a takeover worker passes ``False`` and its spawn ``takeover_attempt`` and refuses
    them instead. Every blocking lock wait and every phase boundary honors one recovery deadline (the leave budget plus
    60 s, or the test-only ``_deadline_ns``); when it expires the error names the run and the last completed phase, and
    the claim and ``recovery_actor`` stay so a later recoverer resumes after the claimant is proven dead.
    """
    try:
        return _recover_run(context, drain_live_consumers, takeover_attempt, _test_directory, _deadline_ns)
    except ResourceBusy as error:
        if error.reason != "lock-deadline":
            raise
        phase = _last_recovery_phase(context, _test_directory)
        raise ResourceBusy(
            f"Recovery deadline expired for run {context.run_id} after phase {phase}.", reason="lock-deadline"
        ) from error


def _recover_run(
    context: RunContext,
    drain_live_consumers: bool,
    takeover_attempt: str | None,
    _test_directory: Path | None,
    _deadline_ns: int | None,
) -> RunStatus:
    # Revocation may need the account lock. Finish it before taking this run lock
    # again; no nested run locks and no private deletion while either actor lives.
    data = _load_run(context)
    deadline_ns = (
        _deadline_ns
        if _deadline_ns is not None
        else time.monotonic_ns() + int((_leave_budget_seconds(data) + 60) * 1_000_000_000)
    )
    if finalization_pending_shape(data):
        return _finalize_legacy_run(context, _test_directory=_test_directory, deadline_ns=deadline_ns)
    if data.get("retained_grant") is not None:
        _require_ungranted(data)
        if observe_process(_identity_from_json(data["requester"])) == "executing":
            raise ResourceBusy("Resume preparation still has a live requester; recovery refused.", reason="actor-live")
        if data["guardian"] is not None and observe_process(_identity_from_json(data["guardian"])) == "executing":
            raise ResourceBusy("Resume preparation still has a live guardian; recovery refused.", reason="actor-live")
        return revoke_ungranted(
            context,
            _test_directory=_test_directory,
            _takeover_attempt=takeover_attempt,
            _deadline_ns=deadline_ns,
        )
    if data["generation"]:
        if (
            any(data.get(key) for key in ("resources", "endpoints", "consumers", "pending_operation", "last_operation"))
            or data["docker_receipts"]
            or any(data["owned_resources"].values())
        ):
            return _recover_resource_grant(
                context,
                _test_directory=_test_directory,
                drain_live_consumers=drain_live_consumers,
                takeover_attempt=takeover_attempt,
                deadline_ns=deadline_ns,
            )
        recovered = _recover_empty_grant(
            context,
            _test_directory=_test_directory,
            takeover_attempt=takeover_attempt,
            deadline_ns=deadline_ns,
        )
        _drain_after_recovery(context, deadline_ns)
        return recovered
    _require_ungranted(data)
    data = _repair_creation_prefix(context, data, deadline_ns)
    if data["cleanup_outcome"] != "complete":
        if observe_process(_identity_from_json(data["requester"])) == "executing":
            raise ResourceBusy("Prepared run still has a live requester; recovery refused.", reason="actor-live")
        if data["guardian"] is not None and observe_process(_identity_from_json(data["guardian"])) == "executing":
            raise ResourceBusy("Prepared run still has a live guardian; recovery refused.", reason="actor-live")
        revoke_ungranted(
            context,
            _test_directory=_test_directory,
            _takeover_attempt=takeover_attempt,
            _deadline_ns=deadline_ns,
        )
    project = context.project
    with (
        lock(project, project.project_id, shared=True, nonblocking=True),
        lock(project, f"run-{context.run_id}", deadline_ns=deadline_ns),
    ):
        data = _load_run(context)
        _require_ungranted(data)
        if data["cleanup_outcome"] == "complete":
            with lock(project, "registry", deadline_ns=deadline_ns):
                registry = load(project)
                location = registry["runs"].get(context.run_id)
                if location is not None:
                    if location != str(context.record):
                        raise ResourceError("Completed run index has changed identity.")
                    del registry["runs"][context.run_id]
                    save(project, registry)
            return run_status(context)
        if data["admission"] != "revoked":
            raise ResourceError("Private cleanup requires durable admission revocation.")
        if observe_process(_identity_from_json(data["requester"])) == "executing":
            raise ResourceBusy("Prepared run still has a live requester; recovery refused.", reason="actor-live")
        with lock(project, "registry", deadline_ns=deadline_ns):
            registry = load(project)
            if registry["runs"].get(context.run_id) != str(context.record):
                raise ResourceError("Prepared run is not indexed by its original project.")
            allowed = {
                root / project.project_id / context.run_id
                for root in temporary_roots(registry)
                if root.name == context.session_id
            }
            if context.private_root not in allowed:
                raise ResourceError("Prepared private root lacks original session ownership.")
        _remove_private_root(context)
        with lock(project, "registry", deadline_ns=deadline_ns):
            registry = load(project)
            if registry["runs"].get(context.run_id) != str(context.record):
                raise ResourceError("Run index changed during recovery.")
            data["cleanup_outcome"] = "complete"
            data["revision"] += 1
            atomic_json(context.record, data)
            del registry["runs"][context.run_id]
            save(project, registry)
        return run_status(context)


def _last_recovery_phase(context: RunContext, test_directory: Path | None) -> str:
    """Return the later (design 3.6 order) of the entry's stored phase and the record's derived phase, else ``none``.

    Reads without locks and writes nothing; a read or parse failure contributes nothing. The stored value can be stale
    (only a worker updates it) and the derived one can lag a phase in flight, so the later of the two is reported.
    """
    stored = derived = None
    try:
        directory = account_directory() if test_directory is None else test_directory.resolve()
        raw = json.loads((directory / "queue.json").read_text())
        stored = account_queue.to_logical(raw).entries[context.run_id].last_completed_phase
    except (OSError, RecursionError, ValueError, KeyError, TypeError, ResourceError):
        stored = None
    try:
        derived = derive_recovery_phase(_load_run(context))
    except (ResourceError, KeyError, TypeError):
        derived = None
    known = [phase for phase in (stored, derived) if phase in RECOVERY_PHASES]
    return max(known, key=RECOVERY_PHASES.index) if known else "none"


def _enter_phase(phase: str, deadline_ns: int | None) -> None:
    """Announce the start of one recovery phase (a failpoint seam) and refuse to start it past the deadline."""
    failpoint(f"recovery-phase-{phase}")
    if deadline_ns is not None and time.monotonic_ns() >= deadline_ns:
        raise ResourceBusy("Recovery deadline expired.", reason="lock-deadline")


def _record_phase(
    context: RunContext,
    directory: Path,
    phase: str,
    takeover_attempt: str | None,
    deadline_ns: int | None,
) -> None:
    """Store a worker's progress on its queue entry through one short metadata section.

    Only a takeover worker (``takeover_attempt`` set) whose entry carries a claim writes; an operator, a run without an
    entry and a version 1 queue contribute nothing.
    """
    if takeover_attempt is None:
        return
    with lock(Context(directory, directory), "metadata", deadline_ns=deadline_ns) as anchor:
        state = _load_queue(directory, anchor)
        entry = state.entries.get(context.run_id)
        if entry is None or entry.takeover is None:
            return
        entry.last_completed_phase = phase
        _save_queue(directory, state, anchor)


def _claim_observation(takeover: dict | None) -> str | None:
    """Observe a takeover claim's bound claimant; None when unbound or when the observation fails (not death)."""
    if takeover is None or takeover["claimant"] is None:
        return None
    try:
        return observe_process(_identity_from_json(takeover["claimant"]))
    except ResourceError:
        return None


def _require_claim_eligible(entry: QueueEntry | None, takeover_attempt: str | None) -> None:
    """Refuse ``claim-held`` unless the entry's takeover claim is free for this recoverer (design 3.9)."""
    if entry is None:
        return
    clock = NativeClock()
    if not claim_eligible(
        entry.takeover,
        caller_attempt=takeover_attempt,
        claimant_state=_claim_observation(entry.takeover),
        now_tick_ns=clock.read(),
        origin=clock.origin,
    ):
        raise ResourceError("The takeover claim of this run is held by another actor.", reason="claim-held")


def _bind_claim(entry: QueueEntry, actor: dict, takeover_attempt: str | None) -> None:
    """Bind the caller as claimant of an existing claim, keeping its attempt; no claim means nothing to bind."""
    if entry.takeover is None:
        return
    entry.takeover = {**entry.takeover, "claimant": actor}
    if takeover_attempt is not None:
        entry.last_completed_phase = "claim"


def _require_own_claim(entry: QueueEntry | None, actor: dict, takeover_attempt: str | None) -> None:
    """Refuse a worker whose claim was replaced or taken meanwhile; operators carry no attempt to fence."""
    if takeover_attempt is None or entry is None:
        return
    takeover = entry.takeover
    if takeover is None or takeover["attempt"] != takeover_attempt or takeover["claimant"] != actor:
        raise ResourceError("The takeover claim changed during recovery.", reason="claim-held")


def _require_actor_gone(raw: dict | None, label: str) -> None:
    """Refuse unless the recorded actor is proven dead: absent or replaced (design 3.9; a zombie is not yet reaped).

    ``actor-live`` for an actor that may run or has not been reaped, ``actor-unverifiable`` if it cannot be observed.
    """
    if raw is None:
        return
    try:
        observed = observe_process(_identity_from_json(raw))
    except ResourceError as error:
        raise ResourceError(f"The {label} cannot be observed.", reason="actor-unverifiable") from error
    if not actors_dead([observed]):
        raise ResourceBusy(f"The {label} is still live.", reason="actor-live")


def _repair_creation_prefix(context: RunContext, data: dict, deadline_ns: int | None = None) -> dict:
    """Restore the project index entry of an unindexed creation prefix, or refuse an unindexed run without any write.

    ``prepare_run`` writes the record before the index entry, so a crash between them leaves a prepared record no
    project index names, which the indexed recovery path cannot finish. Only an exact creation prefix whose requester
    is proven absent or replaced and whose private root lies under a root registered for its session is repaired, and
    only by writing the missing index entry under the registry lock alone; the unchanged wrappers then finish the run.
    An unindexed generation-0 record that is already complete takes today's fast path; any other unindexed record is
    refused with ``actor-live``, ``actor-unverifiable`` or ``record-changed`` before any write. Returns the record the
    caller must route on (re-read after the repair).
    """
    project = context.project
    if data["generation"] != 0 or data["cleanup_outcome"] == "complete":
        return data
    location = load(project)["runs"].get(context.run_id)
    if location == str(context.record):
        return data
    if location is not None:
        raise ResourceError("The run's project index entry changed identity.", reason="record-changed")
    try:
        requester = observe_process(_identity_from_json(data["requester"]))
    except ResourceError as error:
        raise ResourceError(
            "The requester of an unindexed run cannot be observed.", reason="actor-unverifiable"
        ) from error
    if not actors_dead([requester]):
        raise ResourceBusy("The requester of an unindexed run is not proven gone.", reason="actor-live")
    allowed_roots = {
        root / project.project_id / context.run_id
        for root in temporary_roots(load(project))
        if root.name == context.session_id
    }
    if not is_creation_prefix(data) or context.private_root not in allowed_roots:
        raise ResourceError("An unindexed run is not an exact creation prefix.", reason="record-changed")
    failpoint("prefix-read")
    with lock(project, "registry", deadline_ns=deadline_ns):
        current = _load_run(context)
        registry = load(project)
        if is_creation_prefix(current) and context.run_id not in registry["runs"]:
            registry["runs"][context.run_id] = str(context.record)
            save(project, registry)
            failpoint("prefix-index-restored")
    return _load_run(context)


def _prove_legacy_actors_dead(data: dict) -> None:
    """Require every actor of a legacy run to be proven gone: requester, guardian, consumers and recovery actor."""
    raw_identities = [
        data["requester"],
        data["guardian"],
        data["recovery_actor"],
        *(item["identity"] for item in data["consumers"].values()),
    ]
    for raw in raw_identities:
        try:
            observed = observe_process(_identity_from_json(raw))
        except ResourceError as error:
            raise ResourceError(
                "An actor of the legacy run cannot be observed.", reason="actor-unverifiable"
            ) from error
        if not actors_dead([observed]):
            raise ResourceBusy("An actor of the legacy run is still live.", reason="actor-live")


def _require_exact_legacy_references(
    context: RunContext, directory: Path, data: dict, deadline_ns: int | None = None
) -> None:
    """Refuse unless every account and project reference of the run is absent or exactly the original one.

    A queue entry must match the run (attempt, ticket, record, registry, project root) with the record's guardian as the
    slot holder; a project index entry must be exactly this record. Nothing is written.
    """
    project = context.project
    expected = _queue_reference(context, data["guardian_attempt"], data["ticket"])
    with lock(project, "registry", deadline_ns=deadline_ns):
        location = load(project)["runs"].get(context.run_id)
        if location not in (None, str(context.record)):
            raise ResourceError("The legacy run's project index entry changed identity.", reason="record-changed")
        with lock(Context(directory, directory), "metadata", deadline_ns=deadline_ns) as anchor:
            entry = _load_queue(directory, anchor).entries.get(context.run_id)
            if entry is not None and (account_queue.reference(entry) != expected or entry.holder != data["guardian"]):
                raise ResourceError("The legacy run's queue entry changed identity.", reason="record-changed")


def _finalize_legacy_run(
    context: RunContext, *, _test_directory: Path | None = None, deadline_ns: int | None = None
) -> RunStatus:
    """Finalize the legacy finalization-pending shape: prove it, write ``cleanup_outcome complete``, retire references.

    One locked section (project shared non-blocking, run exclusive) re-validates the shape, proves every actor gone and
    checks the references; only then is the record written (revision plus one), keeping ``recovery_actor`` and the null
    ``leave_ack`` as the evidence that toolkit recovery, not the requester, ended the run. The queue entry and the index
    entry then leave through the same idempotent path as any completed grant, so a crash between the steps resumes by
    repeating the recovery.
    """
    directory = account_directory() if _test_directory is None else _test_directory.resolve()
    project = context.project
    with (
        lock(project, project.project_id, shared=True, nonblocking=True),
        lock(project, f"run-{context.run_id}", deadline_ns=deadline_ns),
    ):
        data = _load_run(context)
        if not finalization_pending_shape(data) or data["account_directory"] != str(directory):
            raise ResourceError("Run is not the legacy finalization shape of this account.")
        _prove_legacy_actors_dead(data)
        _require_exact_legacy_references(context, directory, data, deadline_ns)
        data.update(cleanup_outcome="complete")
        data["revision"] += 1
        atomic_json(context.record, data)
        failpoint("finalization-record-written")
        return _retire_completed_resource_grant(context, directory, data, deadline_ns)


def _retire_completed_resource_grant(
    context: RunContext,
    directory: Path,
    data: dict,
    deadline_ns: int | None = None,
) -> RunStatus:
    """Finish exact indexes after terminal resource evidence won the crash race.

    A recoverer other than the recorded ``recovery_actor`` refuses while that actor may still run: the actor retires
    its own references, so a stranger never races it to the same removals.
    """
    prior = data.get("recovery_actor")
    if prior is not None and prior != asdict(current_process(NativeClock())):
        _require_actor_gone(prior, "recovery actor of this run")
    # The original guardian retires its own references in its leave section; while it may still run (live or an
    # unreaped zombie) recovery refuses as actor-live and writes nothing (I-LR3).
    _require_actor_gone(data["guardian"], "guardian of this run")
    project = context.project
    account = Context(directory, directory)
    retained = data["state"] == "stopped-retained"
    expected = _queue_reference(context, data["guardian_attempt"], data["ticket"])
    if (
        data["state"] not in ("complete", "stopped-retained")
        or data["admission"] != "revoked"
        or data["quiescent"] is not True
        or data["pending_operation"] is not None
        or not _docker_receipts_terminal(data)
        or not _consumers_quiescent(data["consumers"])
        or not (_resources_quiescent(data["resources"]) if retained else _resources_removed(data["resources"]))
        or not (_owned_docker_quiescent(data) if retained else _owned_docker_removed(data))
        or not _endpoints_quiescent(data["endpoints"])
        or (not retained and (context.private_root.exists() or context.private_root.is_symlink()))
    ):
        raise ResourceError("Completed grant recovery evidence is contradictory.")
    with ExitStack() as held:
        failpoint("recovery-registry-wait")
        held.enter_context(lock(project, "registry", deadline_ns=deadline_ns))
        registry = load(project)
        location = registry["runs"].get(context.run_id)
        expected_locations = (str(context.record),) if retained else (None, str(context.record))
        if location not in expected_locations:
            raise ResourceError("Completed grant project reference changed identity.")
        with lock(account, "metadata", deadline_ns=deadline_ns) as anchor:
            state = _load_queue(directory, anchor)
            entry = state.entries.get(context.run_id)
            if entry is not None and account_queue.reference(entry) != expected:
                raise ResourceError("Completed grant account reference changed identity.")
            if entry is not None and entry.holder is None:
                raise ResourceError("Completed grant account references are only partially retired.")
            # Queue removal is saved before the project index entry leaves (record, queue, index order); a crash
            # between the two saves resumes here with the entry absent and the index still present.
            if entry is not None:
                _repair_grant_log(directory, state, entry, data)
                if _record_hold_release(data, "recovery"):  # backfill a release lost to a crash, before the removal
                    data["revision"] += 1
                    atomic_json(context.record, data)
                account_queue.remove(state, context.run_id)
                _save_queue(directory, state, anchor)
                _drop_lease(directory, state)
                _drop_turn(directory, data)
                failpoint("retire-queue-removed")
            if not retained and location is not None:
                del registry["runs"][context.run_id]
                save(project, registry)
    return run_status(context)


def _begin_recovery_record(data: dict, actor: dict, release_recovery: bool, release_authority: dict | None) -> None:
    """Mutate a loaded record into "recovery started by ``actor``"; raises before anything is written."""
    if data["state"] == "active":
        clock = NativeClock()
        revision = data["revision"] + 1
        data.update(
            state="stopping",
            withdrawal={
                "generation": data["generation"],
                "revision": revision,
                "requester": data["requester"],
                "outcome": "cancelled",
                "deadline": asdict(Deadline.after(clock, _leave_budget_seconds(data))),
            },
            test_outcome="cancelled",
            cleanup_outcome="incomplete",
            infrastructure_failure="guardian-lost",
            revision=revision,
        )
    elif data["state"] != "stopping" or data["withdrawal"] is None:
        raise ResourceError("Resource-bearing grant is not recoverable from its current state.")
    data.update(
        recovery_actor=actor,
        release_authority=release_authority if release_recovery else data["release_authority"],
        recovery_outcome="pending",
    )
    data["revision"] += 1


def _hold_capacity_fence(held: ExitStack, account: Context, *, shared: bool = False) -> None:
    """Hold ``capacity.lock`` non-blocking for the rest of a recovery, in the dead run's mode (design 2.8).

    A dead shared holder is recovered under a shared fence, so live co-holders keep running; every other recovery
    holds it exclusively.

    A recovery that does Docker work or removes an unrecovered holder takes this before its first write. A busy lock
    raises ``ResourceBusy`` with reason ``capacity-lock-held`` and leaves record and queue untouched; the lock is never
    waited for, so it cannot join a lock cycle.
    """
    try:
        held.enter_context(lock(account, "capacity", shared=shared, nonblocking=True))
    except ResourceBusy as error:
        raise ResourceBusy("Another run holds the capacity lock.", reason="capacity-lock-held") from error
    failpoint("recovery-capacity-acquired")


def _recover_resource_grant(
    context: RunContext,
    *,
    _test_directory: Path | None = None,
    drain_live_consumers: bool = True,
    takeover_attempt: str | None = None,
    deadline_ns: int | None = None,
) -> RunStatus:
    """Drain an ended guardian's exact recorded consumers/resources and publish the terminal record.

    The first locked section checks everything before it writes anything (design 3.9): the prior recoverer is gone,
    the pending operation and open receipts are reconcilable, a worker's registered consumers are not running, and the
    entry's takeover claim is free for this recoverer. Only then, in one metadata section nested inside the run lock,
    the record gets ``recovery_actor`` and the claimant is bound. The slot holder's capacity lock is held for the whole
    recovery. Docker work runs outside the run lock; every later phase starts with a deadline check and a worker records
    its progress on its entry.
    """
    project = context.project
    directory = account_directory() if _test_directory is None else _test_directory.resolve()
    account = Context(directory, directory)
    actor = asdict(current_process(NativeClock()))
    release_recovery = False
    release_authority = None
    with ExitStack() as held:
        _enter_phase("authorize", deadline_ns)
        with (
            lock(project, project.project_id, shared=True, nonblocking=True),
            lock(project, f"run-{context.run_id}", deadline_ns=deadline_ns),
        ):
            data = _load_run(context)
            if data["account_directory"] != str(directory):
                raise ResourceError("Recovery cannot move original authority to another account domain.")
            if data.get("recovery_outcome") == "complete":
                return _retire_completed_resource_grant(context, directory, data, deadline_ns)
            pending = data["pending_operation"]
            if pending is not None and pending["kind"] not in _RECONCILABLE_OPERATION_KINDS:
                raise ResourceError(
                    "Interrupted Docker operation requires exact result reconciliation.",
                    reason="unsupported-operation-kind",
                )
            if any(
                receipt["state"] in ("pending", "unresolved") and receipt["kind"] not in _PROMOTABLE_RECEIPT_KINDS
                for receipt in data["docker_receipts"].values()
            ):
                raise ResourceError(
                    "An interrupted Docker receipt kind cannot be reconciled.", reason="unsupported-receipt-kind"
                )
            _require_actor_gone(data["guardian"], "original guardian")
            if data.get("withdrawal") is None:
                _require_actor_gone(data["requester"], "published root")
            prior = data["recovery_actor"]
            if prior is not None and prior != actor:
                _require_actor_gone(prior, "recoverer of this run")
            if not drain_live_consumers:
                for item in data["consumers"].values():
                    if item["state"] == "active":
                        _require_actor_gone(item["identity"], "registered consumer")
            failpoint("recovery-checks-passed")
            expected = _queue_reference(context, data["guardian_attempt"], data["ticket"])
            release_shape = _is_release_shape(data)
            with lock(account, "metadata", deadline_ns=deadline_ns) as anchor:
                state = _load_queue(directory, anchor)
                entry = state.entries.get(context.run_id)
                if release_shape:
                    if _release_authority_changed(state, context):
                        raise ResourceError("Release authority changed before exact recovery.")
                    _hold_capacity_fence(held, account)  # release recovery excludes every other run
                    release_recovery = True
                elif entry is not None and account_queue.reference(entry) == expected:
                    if entry.phase not in HOLDER_PHASES:
                        raise ResourceError("Original grant lost its capacity reservation history.")
                    _require_claim_eligible(entry, takeover_attempt)
                    _hold_capacity_fence(held, account, shared=_record_mode(data) == AdmissionMode.SHARED)
                else:
                    raise ResourceError("Original grant lost its account recovery reference.")
                if not release_recovery:
                    _begin_recovery_record(data, actor, False, None)
                    atomic_json(context.record, data)
                    failpoint("recovery-record-written")
                    if entry is not None and entry.takeover is not None:
                        _bind_claim(entry, actor, takeover_attempt)
                        _save_queue(directory, state, anchor)
                    failpoint("recovery-claim-bound")
            if release_recovery:
                release_authority = _verify_interrupted_release(context, data, deadline_ns)
                _begin_recovery_record(data, actor, True, release_authority)
                atomic_json(context.record, data)
                failpoint("release-recovery-record-written")
        failpoint("recovery-first-section-released")
        _enter_phase("operation", deadline_ns)
        if data["pending_operation"] is not None and data["pending_operation"]["kind"] in (
            "compose-up",
            "compose-observe",
            "compose-mutate",
            "container-control",
        ):
            (
                _reconcile_pending_container_control(context, deadline_ns)
                if data["pending_operation"]["kind"] == "container-control"
                else _reconcile_pending_compose(context, deadline_ns)
            )
            data = _load_run(context)
        _record_phase(context, directory, "operation", takeover_attempt, deadline_ns)
        _enter_phase("receipts", deadline_ns)
        if not _docker_receipts_terminal(data) or _archive_cleanup_deferred(data):
            _reconcile_docker_receipt(context, deadline_ns)
            data = _load_run(context)
        _record_phase(context, directory, "receipts", takeover_attempt, deadline_ns)
        _enter_phase("consumers", deadline_ns)
        if not _consumers_quiescent(data["consumers"]):
            _drain_consumers(context, deadline_ns)
        _record_phase(context, directory, "consumers", takeover_attempt, deadline_ns)
        _enter_phase("resources", deadline_ns)
        if data["retain"] and not release_recovery:
            if not _resources_quiescent(data["resources"]):
                _stop_compose_resources(context, deadline_ns)
            if not _owned_docker_quiescent(data):
                _stop_owned_docker_resources(context, deadline_ns)
        elif not _resources_removed(data["resources"]):
            _cleanup_compose_resources(context, deadline_ns)
        if (release_recovery or not data["retain"]) and _owned_cleanup_needed(_load_run(context)):
            _cleanup_owned_docker_resources(context, deadline_ns)
        _record_phase(context, directory, "resources", takeover_attempt, deadline_ns)
        _enter_phase("private-root", deadline_ns)
        if release_recovery or not data["retain"]:
            _remove_private_root(context)
        _record_phase(context, directory, "private-root", takeover_attempt, deadline_ns)
        _enter_phase("publish", deadline_ns)
        with (
            lock(project, project.project_id, shared=True, nonblocking=True),
            lock(project, f"run-{context.run_id}", deadline_ns=deadline_ns),
        ):
            data = _load_run(context)
            if (
                data["recovery_actor"] != actor
                or data["pending_operation"] is not None
                or not _docker_receipts_terminal(data)
                or not _consumers_quiescent(data["consumers"])
                or not (
                    _resources_quiescent(data["resources"])
                    if data["retain"] and not release_recovery
                    else _resources_removed(data["resources"])
                )
                or not (
                    _owned_docker_quiescent(data)
                    if data["retain"] and not release_recovery
                    else _owned_docker_removed(data)
                )
                or not _endpoints_quiescent(data["endpoints"])
            ):
                raise ResourceError("Recovered grant is not verifiably quiescent.")
            with lock(account, "metadata", deadline_ns=deadline_ns) as anchor:
                state = _load_queue(directory, anchor)
                entry = state.entries.get(context.run_id)
                if release_recovery:
                    if _release_authority_changed(state, context):
                        raise ResourceError("Release authority changed during exact recovery.")
                else:
                    if entry is None or account_queue.reference(entry) != expected:
                        raise ResourceError("Account authority changed during exact recovery.")
                    if entry.phase not in HOLDER_PHASES:
                        raise ResourceError("Capacity reservation changed during exact recovery.")
                    _require_own_claim(entry, actor, takeover_attempt)
                    _repair_grant_log(directory, state, entry, data)
                data.update(
                    state="stopped-retained" if data["retain"] and not release_recovery else "complete",
                    admission="revoked",
                    release_authority=None,
                    recovery_outcome="complete",
                    # The final check above proved every resource removed. A withdrawn run whose requester died before
                    # the acknowledgment still stores "pending", which no loaded terminal record may carry; an
                    # "incomplete" value records a real failure and a retained run keeps its stored value.
                    cleanup_outcome="complete"
                    if release_recovery or (not data["retain"] and data["cleanup_outcome"] == "pending")
                    else data["cleanup_outcome"],
                    quiescent=True,
                )
                if not release_recovery:
                    _record_hold_release(data, "recovery")  # the slot is released by the removal below
                data["revision"] += 1
                atomic_json(context.record, data)
                if not release_recovery:
                    account_queue.remove(state, context.run_id)
                    _save_queue(directory, state, anchor)
                    _drop_lease(directory, state)
                    _drop_turn(directory, data)
            if release_recovery or not data["retain"]:
                failpoint("recovery-registry-wait")
                with lock(project, "registry", deadline_ns=deadline_ns):
                    registry = load(project)
                    if registry["runs"].get(context.run_id) != str(context.record):
                        raise ResourceError("Project authority changed during exact recovery.")
                    del registry["runs"][context.run_id]
                    save(project, registry)
            return run_status(context)


def _release_authority_changed(state: QueueState, context: RunContext) -> bool:
    """Return whether account authority contradicts an interrupted retained release.

    The release shape needs no entry for this run and no slot holder from the same project; unrelated projects'
    entries are irrelevant, so recovery never waits for a globally empty queue.
    """
    project_id = context.project.project_id
    return context.run_id in state.entries or any(
        entry.holder is not None and entry.project_id == project_id for entry in state.entries.values()
    )


def _drain_after_recovery(context: RunContext, deadline_ns: int | None) -> None:
    """Drain handed-off images after an empty grant's recovery, holding no other lock while it waits for K.

    A run that only declared ``use_image`` can be the last user of a handed-off image; its recovery owns nothing to
    clean, so without this the image and its pending entry would stay (design 2.7, the last user drains). The recovery
    is already durable: a busy K, an unreadable pending list or a failed Docker call (``InfrastructureFailure``) leaves
    every entry in the list for the next image step and never fails the recovery.
    """
    try:
        account = Path(_load_run(context)["account_directory"])
        if pending_absent(account):
            return
        with image_lock(Context(account, account), deadline_ns=deadline_ns) as hold_fd:
            _drain_pending(context, account, deadline_ns, hold_fd)
    except (ResourceError, OSError):
        return  # the list is rewritten only after the whole pass: a failure part-way keeps every entry


def _recover_empty_grant(
    context: RunContext,
    *,
    _test_directory: Path | None = None,
    takeover_attempt: str | None = None,
    deadline_ns: int | None = None,
) -> RunStatus:
    """Recover an ended guardian's empty grant after root death or explicit withdrawal.

    This cannot clean a resource-bearing grant or acknowledge the original leave.
    Quiescence permits future admission; the old cleanup/telemetry failure persists. The whole body runs under the run
    lock; a takeover claim on the entry is checked and bound in the first metadata section, before any record write,
    and a worker confirms it is still the claimant before the entry leaves.
    """
    project = context.project
    directory = account_directory() if _test_directory is None else _test_directory.resolve()
    account = Context(directory, directory)
    actor = asdict(current_process(NativeClock()))
    with (
        ExitStack() as held,
        lock(project, project.project_id, shared=True, nonblocking=True),
        lock(project, f"run-{context.run_id}", deadline_ns=deadline_ns),
    ):
        data = _load_run(context)
        if data["account_directory"] != str(directory):
            raise ResourceError("Recovery cannot move original authority to another account domain.")
        if data.get("recovery_outcome") == "complete":
            return _retire_completed_resource_grant(context, directory, data, deadline_ns)
        if any(
            data.get(key)
            for key in (
                "resources",
                "endpoints",
                "consumers",
                "pending_operation",
                "last_operation",
            )
        ):
            raise ResourceError("Grant still has resource/consumer/operation evidence requiring exact recovery.")
        _require_actor_gone(data["guardian"], "original guardian")
        if data.get("withdrawal") is None:
            _require_actor_gone(data["requester"], "published root")
        expected = _queue_reference(context, data["guardian_attempt"], data["ticket"])
        failpoint("recovery-checks-passed")
        with lock(account, "metadata", deadline_ns=deadline_ns) as anchor:
            state = _load_queue(directory, anchor)
            entry = state.entries.get(context.run_id)
            reference = None if entry is None else account_queue.reference(entry)
            if reference != expected and not (entry is None and data.get("recovery_outcome") == "complete"):
                raise ResourceError("Original grant lost or changed its account recovery reference.")
            _require_claim_eligible(entry, takeover_attempt)
            _hold_capacity_fence(held, account, shared=_record_mode(data) == AdmissionMode.SHARED)
            failpoint("empty-grant-claim-checked")
            if entry is not None and entry.takeover is not None:
                _bind_claim(entry, actor, takeover_attempt)
                _save_queue(directory, state, anchor)
            failpoint("empty-grant-claim-written")
        with lock(project, "registry", deadline_ns=deadline_ns):
            registry = load(project)
            indexed = registry["runs"].get(context.run_id)
            if indexed != str(context.record):
                if indexed is None and data.get("recovery_outcome") == "complete" and entry is None:
                    return run_status(context)
                raise ResourceError("Original grant lost its project recovery reference.")
            allowed = {
                root / project.project_id / context.run_id
                for root in temporary_roots(registry)
                if root.name == context.session_id
            }
            if context.private_root not in allowed:
                raise ResourceError("Grant private root lacks original session ownership.")
        if data.get("recovery_outcome") != "complete":
            data["recovery_outcome"] = "pending"
            data["revision"] += 1
            atomic_json(context.record, data)
            failpoint("empty-grant-record-written")
            _remove_private_root(context)
        with lock(account, "metadata", deadline_ns=deadline_ns) as anchor:
            state = _load_queue(directory, anchor)
            entry = state.entries.get(context.run_id)
            reference = None if entry is None else account_queue.reference(entry)
            if reference != expected and not (entry is None and data.get("recovery_outcome") == "complete"):
                raise ResourceError("Grant account reference changed during original recovery.")
            _require_own_claim(entry, actor, takeover_attempt)
            if entry is not None:
                _repair_grant_log(directory, state, entry, data)
            if data.get("recovery_outcome") != "complete":
                data.update(
                    state="complete",
                    admission="revoked",
                    recovery_outcome="complete",
                    quiescent=True,
                    cleanup_outcome="incomplete",
                    test_outcome=data["test_outcome"] or "cancelled",
                    infrastructure_failure=data.get("infrastructure_failure") or "guardian-lost",
                )
                _record_hold_release(data, "recovery")
                data["revision"] += 1
                atomic_json(context.record, data)
            elif entry is not None and _record_hold_release(data, "recovery"):  # backfill before the removal
                data["revision"] += 1
                atomic_json(context.record, data)
            if account_queue.remove(state, context.run_id) is not None:
                _save_queue(directory, state, anchor)
            _drop_lease(directory, state)
            _drop_turn(directory, data)  # also after a crash between the queue removal and this line
        with lock(project, "registry", deadline_ns=deadline_ns):
            registry = load(project)
            if registry["runs"].get(context.run_id) != str(context.record):
                raise ResourceError("Grant project reference changed during original recovery.")
            del registry["runs"][context.run_id]
            save(project, registry)
        return run_status(context)


def reconcile_preparations(
    project: Context, *, _test_directory: Path | None = None, _deadline_ns: int | None = None
) -> None:
    """Recover proved-dead unenrolled runs before another same-project preparation.

    Live preparations remain indexed. Missing or ambiguous original records raise
    ResourceError instead of being mistaken for an empty project. Preparation itself runs no Docker (a recovered release
    does its own cleanup, under the capacity lock). A run whose entry a takeover worker
    holds (``claim-held``), or whose release recovery waits for a neighbour's capacity lock (``capacity-lock-held``), is
    waited for with no lock held: the pass is retried every 100 ms until the preparation deadline (60 s unless the
    caller passes its own), then ``lock-deadline``.
    """
    deadline_ns = _deadline_ns if _deadline_ns is not None else time.monotonic_ns() + 60_000_000_000
    while True:
        try:
            _reconcile_preparations_pass(project, _test_directory, deadline_ns)
            return
        except ResourceError as error:
            if error.reason not in ("claim-held", "capacity-lock-held"):
                raise
        if time.monotonic_ns() >= deadline_ns:
            raise ResourceBusy(
                "A takeover worker or a running neighbour still holds this project's dead run.", reason="lock-deadline"
            )
        failpoint("reconcile-preparations-wait")
        time.sleep(0.1)


def _reconcile_preparations_pass(project: Context, _test_directory: Path | None, deadline_ns: int) -> None:
    """One pass over the project's indexed runs under the shared project lock (see ``reconcile_preparations``)."""
    with lock(project, project.project_id, shared=True, nonblocking=True):
        with lock(project, "registry", deadline_ns=deadline_ns):
            indexed = dict(load(project)["runs"])
        for run_id, location in indexed.items():
            record = project.state / "runs" / f"{run_id}.json"
            try:
                if str(record) != location:
                    raise ResourceError("Indexed preparation has an unexpected record location.")
                data = _read_private_json(record)
                session = data.get("session_id")
                private = data.get("private_root")
                if not isinstance(session, str) or not UUID.fullmatch(session) or not isinstance(private, str):
                    raise ResourceError("Indexed preparation has invalid original ownership.")
                context = RunContext(project, run_id, session, record, Path(private))
                data = _load_run(context)
            except ResourceBusy:
                raise
            except ResourceError as error:
                raise IncompatibleState(
                    f"Run {run_id} of this project is blocked: its record is invalid ({error}).",
                    reason="blocked-entry",
                ) from error
            if data["state"] == "stopped-retained":
                continue
            if data["guardian"] is not None and observe_process(_identity_from_json(data["guardian"])) == "executing":
                continue
            if (
                data["cleanup_outcome"] == "complete"
                or observe_process(_identity_from_json(data["requester"])) != "executing"
            ):
                recover_run(context, _test_directory=_test_directory)


def start_guardian_attempt(context: RunContext) -> str:
    """Persist startup intent before a distinct guardian process may enroll.

    Only the original live requester can start once. Invalid identity, changed
    source, expired wait or an existing attempt raises ResourceError without spawn.
    """
    project = context.project
    with lock(project, project.project_id, shared=True), lock(project, f"run-{context.run_id}"):
        data = _load_run(context)
        clock = NativeClock()
        if data["requester"] != asdict(current_process(clock)):
            raise ResourceError("Guardian startup caller is not the original requester.")
        if data["state"] != "prepared" or data["guardian_attempt"] is not None:
            raise ResourceError("Run already has a guardian attempt or is terminal.")
        if data["source_identity"] != source_identity() or not _deadline_from_json(data["queue_deadline"]).remaining_ns(
            clock
        ):
            raise ResourceError("Runtime changed or the original queue budget expired.")
        attempt = str(uuid.uuid4())
        data.update(
            guardian_attempt=attempt,
            bootstrap_deadline=asdict(Deadline.after(clock, 30)),
            pending_operation={"kind": "guardian-start", "attempt": attempt},
        )
        data["revision"] += 1
        atomic_json(context.record, data)
        return attempt


def _load_queue(directory: Path, anchor) -> QueueState:
    return _admission_load_queue(directory, anchor, validate_identity=_identity_from_json)


def _save_queue(directory: Path, state: QueueState, anchor) -> None:
    _admission_save_queue(directory, state, anchor)


def _queue_reference(context: RunContext, attempt: str, ticket: int | None) -> dict:
    """Return the five identity fields a run's account queue entry must carry (``admission.reference`` of its entry)."""
    return {
        "attempt": attempt,
        "ticket": ticket,
        "record": str(context.record),
        "registry": str(context.project.state),
        "project_root": str(context.project.root),
    }


def _entry_record(run_id: str, entry: QueueEntry, directory: Path) -> dict | None:
    """Return the validated record one queue entry names, or None when it cannot be trusted this pass.

    An unreadable or hostile record, one that fails validation, or one whose identity contradicts the entry is None.
    Computed on read and never persisted; no lock is taken and nothing is written.
    """
    project = Context(Path(entry.project_root), Path(entry.registry))
    record = project.state / "runs" / f"{run_id}.json"
    try:
        raw = _read_private_json(record)
        session, private = raw.get("session_id"), raw.get("private_root")
        if not isinstance(session, str) or not UUID.fullmatch(session) or not isinstance(private, str):
            return None
        data = _load_run(RunContext(project, run_id, session, record, Path(private)))
        if (
            str(record) != entry.record
            or data["guardian_attempt"] != entry.attempt
            or (data["ticket"] is not None and data["ticket"] != entry.ticket)
            or data["account_directory"] not in (None, str(directory))
        ):
            return None
        return data
    except (ResourceError, KeyError, TypeError, ValueError, RecursionError):
        return None


@dataclass(frozen=True)
class EntryFacts:
    """What one queue entry's record says, read at the moment of the decision.

    An untrusted record blocks only its own entry; the facts are read on every call and never cached.
    """

    blocked: bool
    guardian_state: str | None
    mode: str | None = AdmissionMode.EXCLUSIVE  # from the record's ``sharing`` key; None when the record is untrusted
    version_ok: bool = True  # the record's artifact reads multi-holder queues (M1)


def _record_mode(data: dict) -> AdmissionMode:
    """Return the admission mode a run record carries: ``shared`` only with the exact ``sharing`` key."""
    return AdmissionMode.SHARED if data.get("sharing") == {"mode": AdmissionMode.SHARED} else AdmissionMode.EXCLUSIVE


def _record_version_ok(data: dict) -> bool:
    artifact = data.get("artifact")
    return version_at_least(artifact.get("version") if isinstance(artifact, dict) else None, SHARED_QUEUE_MIN_VERSION)


def _entry_facts(run_id: str, entry: QueueEntry, directory: Path) -> EntryFacts:
    """Return the blocked flag and guardian liveness of one entry from its original run record.

    A record that cannot be trusted blocks only that entry (a holder entry whose guardian differs from its record's
    is untrusted). An unprovable guardian observation is unknown, never death.
    """
    data = _entry_record(run_id, entry, directory)
    if data is None or (entry.holder is not None and data["guardian"] != entry.holder):
        return EntryFacts(True, None, None, False)  # a holder whose record names another guardian is untrusted too
    mode, version_ok = _record_mode(data), _record_version_ok(data)
    guardian = data["guardian"]
    if guardian is None:
        return EntryFacts(False, None, mode, version_ok)
    try:
        return EntryFacts(False, observe_process(_identity_from_json(guardian)), mode, version_ok)
    except ResourceError:
        return EntryFacts(False, None, mode, version_ok)


def _holder_facts(run_id: str, entry: QueueEntry, data: dict) -> HolderFacts:
    """Return what one holder contributes to the account's accounting: E(h) and the names attribution matches."""
    phases = _phase_budgets(data["phases"], data["initial_phase"])
    builder = builder_name(entry.project_id, run_id)
    removed = _builder_removed(data)
    return HolderFacts(
        run_id,
        effective_reservation(phases, builder_removed=removed),
        compose_project_name(entry.project_id, run_id),
        # Without a builder reserve there is no worker to attribute, and none of the reservation is set aside for it.
        buildkit_worker(builder) if admits_builder(phases) and not removed else None,
    )


def _coholder_snapshot(
    snapshot: "CapacitySnapshot", containers: Sequence[dict], holders: Sequence[HolderFacts], reserve: int
) -> "CapacitySnapshot":
    """Return the snapshot one shared decision reads: co-holder containers attributed, the reserve read under M."""
    attributed: frozenset[str] = frozenset()
    for holder in holders:
        attributed |= attributed_ids(containers, holder, run_label=RUN_LABEL)
    return replace(
        snapshot,
        external_limits=net_external_limits(snapshot.external_limits, containers, attributed),
        host_reserve_bytes=reserve,
    )


def _as_budget(reserved: PhaseReservation) -> PhaseBudget:
    return PhaseBudget(
        reserved.container_bytes, reserved.host_process_bytes, reserved.daemon_cpus, reserved.host_threads
    )


def admission_decision(
    phases: Mapping[str, PhaseBudget],
    snapshot: "CapacitySnapshot",
    coholders: Sequence[PhaseReservation],
    *,
    shared: bool = False,
) -> "CapacityDecision":
    """Decide whether a run fits beside its co-holders by declared budgets (design 2.4 fit, §5.5 C9).

    Each own phase must fit alone first (today's rule; a REFUSED stays REFUSED, ``insufficient-cpu`` included). With
    co-holders, ``E(self) + Σ E(co-holders)`` plus the reserves (counted once, by ``assess_capacity``) must fit too;
    any shortfall of that sum, a CPU-only one included, is WAITING ``coholder-capacity``. Without co-holders an
    exclusive run's result is today's; a ``shared`` run checks ``E(self)`` (its builder reserve included) even alone,
    and keeps that check's own decision: no co-holder can leave to make room, so a REFUSED stays REFUSED.
    """
    for budget in phases.values():
        alone = assess_capacity(budget, snapshot)
        if alone.state != CapacityState.READY:
            return alone
    if not coholders and not shared:
        return CapacityDecision(CapacityState.READY, "static-capacity")
    together = assess_capacity(_as_budget(combined([effective_reservation(phases), *coholders])), snapshot)
    if together.state == CapacityState.READY or not coholders:
        return together
    return CapacityDecision(CapacityState.WAITING, "coholder-capacity", together.guest_bytes, together.host_bytes)


def observation_decision(
    budget: PhaseBudget, snapshot: "CapacitySnapshot", coholders: Sequence[PhaseReservation], unknown: bool
) -> "CapacityDecision":
    """Check a shared run's current phase beside its co-holders' reservations.

    An unreadable co-holder record is account corruption: REFUSED ``unknown-external-capacity`` (sticky).
    """
    if unknown:
        return CapacityDecision(CapacityState.REFUSED, UNKNOWN_EXTERNAL_CAPACITY)
    if not coholders:
        return assess_capacity(budget, snapshot)
    current = PhaseReservation(
        budget.container_bytes, budget.host_process_bytes, budget.daemon_cpus, budget.host_threads
    )
    return assess_capacity(_as_budget(combined([current, *coholders])), snapshot)


@dataclass(frozen=True)
class CoholderRead:
    """One read of the co-holders' records: their facts, whether any record was unreadable, and the host reserve."""

    holders: tuple[HolderFacts, ...]
    unknown: bool
    host_reserve_bytes: int
    revisions: tuple[tuple[str, int], ...] = ()
    grants: tuple[Grant, ...] | None = ()  # None: the grants log was unreadable or invalid
    held: tuple[Grant, ...] = ()  # the current granted co-holders' own grants, read from their records


@dataclass(frozen=True)
class SharedPressure:
    """The pressure terms of one shared observation: whether the run has shared, and its allowance (design 2.5)."""

    shared: bool
    allowance_bytes: int
    unreadable: bool


def _shared_pressure_terms(
    grants: Sequence[Grant] | None, *, coholder_seen: bool, record: dict, held: Sequence[Grant] = ()
) -> SharedPressure:
    """Return the shared pressure terms for one observation of ``record``'s current generation.

    The allowance is the budgets granted beside the run after its admission baseline (kept after those runs leave),
    plus every co-holder holding during this pass (``held``, one budget per grant identity), whenever it was granted: an
    incumbent that ramps within its own budget after this run joined is a neighbour, not this run's breach. An
    unreadable grants log (``None``) means the run has shared with no shared-only breach for this pass (R3-8): the
    allowance is unbounded, and the caller records it once.
    """
    if grants is None:
        return SharedPressure(True, _UNBOUNDED_BYTES, True)
    baseline = record["admission_observation"]["memory"]
    keys = {"run_id": record["run_id"], "origin": baseline["origin"], "admission_baseline_ns": baseline["tick_ns"]}
    shared = has_shared(coholder_seen=coholder_seen, grants=grants, generation=record["generation"], **keys)
    if not shared:
        return SharedPressure(False, 0, False)
    after_baseline = allowance_bytes(grants, **keys)
    counted = {(grant.run_id, grant.generation, grant.origin) for grant in grants if counted_grant(grant, **keys)}
    incumbents = sum(
        {
            (grant.run_id, grant.generation, grant.origin): grant.budget_bytes
            for grant in held
            if grant.run_id != record["run_id"] and (grant.run_id, grant.generation, grant.origin) not in counted
        }.values()
    )
    return SharedPressure(True, after_baseline + incumbents, False)


def _gather_coholders(read_once: Callable[[], CoholderRead], attempts: int = 3) -> CoholderRead:
    """Return a co-holder read that two consecutive reads agree on, after at most ``attempts`` tries.

    Raises:
        ResourceError: ``coholder-records-unstable`` (classified like a late observation) when no two consecutive
            reads agree.
    """
    previous = read_once()
    for _ in range(attempts - 1):
        current = read_once()
        if current == previous:
            return current
        previous = current
    raise ResourceError("Co-holder records kept changing during the observation.", reason="coholder-records-unstable")


def _read_coholders(context: RunContext, directory: Path, deadline: "Deadline") -> CoholderRead:
    """Read the co-holders' queue entries and records and the host reserve under one short metadata section.

    The limit is not read: it governs future admissions, never an incumbent's observation (design 2.2).
    """
    deadline_ns = time.monotonic_ns() + deadline.remaining_ns(NativeClock())
    with lock(Context(directory, directory), "metadata", deadline_ns=deadline_ns) as anchor:
        state = _load_queue(directory, anchor)
        reserve = read_host_reserve(directory)
        holders: list[HolderFacts] = []
        revisions: list[tuple[str, int]] = []
        held: list[Grant] = []
        unknown = False
        for run_id, entry in sorted(state.entries.items()):
            if run_id == context.run_id or entry.phase != "granted":
                continue
            record = _entry_record(run_id, entry, directory)
            if record is None or _record_mode(record) != AdmissionMode.SHARED:
                unknown = True
                continue
            holders.append(_holder_facts(run_id, entry, record))
            revisions.append((run_id, record["revision"]))
            held.append(_grant_of(record, _phase_budgets(record["phases"], record["initial_phase"])))
        try:  # after the co-holder records, in the same section (R3-3)
            logged = read_grants(directory)
        except (ResourceError, OSError):
            grants: tuple[Grant, ...] | None = None
        else:
            # A granted co-holder counts from its own record too: a publisher that died before its log append (C4) is
            # in the allowance before its recovery repairs the log. One grant identity counts once.
            identities = {(grant.run_id, grant.generation, grant.origin) for grant in logged}
            grants = (*logged, *(g for g in held if (g.run_id, g.generation, g.origin) not in identities))
    return CoholderRead(tuple(holders), unknown, reserve, tuple(revisions), grants, tuple(held))


def enroll_guardian(context: RunContext, attempt: str, *, _test_directory: Path | None = None) -> int:
    """Bind this guardian and durably enroll once; enrollment never grants Docker use.

    The explicit private directory is a native host-test seam only. Docker grants
    must use the canonical account domain. A duplicate call by this same guardian
    keeps its ticket; another actor or a revoked/expired startup raises ResourceError.
    """
    directory = account_directory() if _test_directory is None else _test_directory.resolve()
    _private_directory(directory)
    account = Context(directory, directory)
    project = context.project
    deadline_ns = _queue_budget_ns(_load_run(context))  # the waits below end with the run's queue budget
    with _locked_run(context, deadline_ns):
        data = _load_run(context)
        clock = NativeClock()
        guardian = asdict(current_process(clock))
        if data["guardian_attempt"] != attempt or data["state"] not in ("prepared", "queued"):
            raise ResourceError("Guardian startup is obsolete or revoked.")
        if guardian["pid"] == data["requester"]["pid"] or data["guardian"] not in (None, guardian):
            raise ResourceError("Guardian attempt belongs to another actor.")
        if observe_process(_identity_from_json(data["requester"])) != "executing":
            raise ResourceError("Guardian requester no longer executes.")
        if data["source_identity"] != source_identity() or not _deadline_from_json(data["queue_deadline"]).remaining_ns(
            clock
        ):
            raise ResourceError("Runtime changed or the original queue budget expired.")
        if data["state"] == "prepared" and not _deadline_from_json(data["bootstrap_deadline"]).remaining_ns(clock):
            raise ResourceError("Guardian bootstrap deadline expired.")
        if data["account_directory"] not in (None, str(directory)):
            raise ResourceError("Guardian cannot move to another admission domain.")
        data["guardian"] = guardian
        data["account_directory"] = str(directory)
        data["revision"] += 1
        atomic_json(context.record, data)
        failpoint("enroll-record-written")
        with lock(account, "metadata", deadline_ns=deadline_ns) as anchor:
            state = _load_queue(directory, anchor)
            reference = {
                "attempt": attempt,
                "record": str(context.record),
                "registry": str(project.state),
                "project_root": str(project.root),
            }
            enrolled = context.run_id in state.entries
            ticket = account_queue.enroll(state, context.run_id, reference)
            if not enrolled:
                _save_queue(directory, state, anchor)
            data.update(ticket=ticket, state="queued", pending_operation=None)
            data["revision"] += 1
            atomic_json(context.record, data)
            return ticket


def _is_own_reference(entry: QueueEntry, context: RunContext, attempt: str, ticket: int) -> bool:
    """Return whether a queue entry is exactly this run's own enrolled reference (attempt, ticket and location)."""
    return account_queue.reference(entry) == _queue_reference(context, attempt, ticket)


@dataclass(frozen=True)
class _Refused:
    """A publication the version gate refused inside its section: nothing was written; the run must be revoked."""

    reason: str


@dataclass(frozen=True)
class _Waiting:
    """A publication that must wait inside its section (grant order, the limit, co-holders): nothing was written."""

    reason: str


def _entry_view(run_id: str, entry: QueueEntry, facts: "EntryFacts") -> QueueEntryView:
    return QueueEntryView(
        run_id,
        entry.ticket,
        entry.phase,
        entry.project_id,
        facts.blocked,
        facts.guardian_state,
        facts.mode,
        facts.version_ok,
    )


@dataclass(frozen=True)
class ReserveOutcome:
    """The result of asking for the capacity slot: its descriptor, or the one reason the run keeps waiting.

    ``waiting`` is ``queue-position`` (an earlier ticket or a holder blocks the head) or ``capacity-lock-busy`` (the
    physical capacity lock is held).
    """

    descriptor: TextIO | None
    waiting: str | None
    refusal: str | None = None  # the version gate's reason: the run must be revoked after leaving the sections


def _holders_present(data: dict) -> bool:
    """Return whether some queue entry holds the account, so a queued pass must sample for a joiner's shared fit.

    A lock-free read: the decision itself is taken again under the metadata lock; a stale read only costs one pass.
    """
    queue = Path(data["account_directory"]) / "queue.json"
    try:
        entries = json.loads(queue.read_bytes()).get("entries", {}) if queue.exists() else {}
    except (OSError, ValueError, AttributeError):
        return False
    return isinstance(entries, dict) and any(
        isinstance(entry, dict) and entry.get("phase") in HOLDER_PHASES for entry in entries.values()
    )


def _static_exclusive(data: dict) -> bool:
    """Return whether a run is exclusive whatever its verdict (design N8): a release helper or a declared connection."""
    return data["purpose"] == RELEASE_PURPOSE or any(
        item["relationship"] == "connect_owned_container" for item in data["external_resources"]
    )


def _predicted_shareable(context: RunContext, data: dict) -> bool:
    """Return the verdict store's prediction for this run's request; read without a lock (a prediction only)."""
    key = _verdict_key(data)
    if _static_exclusive(data) or key is None:
        return False
    return read_verdict(context.project.state, context.project.project_id, key) is True


def _never_fits_shared(data: dict, machine: "CapacitySnapshot", settings: AdmissionSettings) -> bool:
    """Return whether only the shared reservation ``E(self)`` is REFUSED on the sampled machine, even with nobody else.

    Such a run could only ever wait as a shared holder; admitted exclusive it is checked by today's per-phase rule. A
    run whose own phase is already REFUSED stays shared and is refused as today (``insufficient-cpu`` included).
    """
    snapshot = replace(machine, host_reserve_bytes=settings.host_reserve_bytes)
    phases = _phase_budgets(data["phases"], data["initial_phase"])
    if any(assess_capacity(budget, snapshot).state == CapacityState.REFUSED for budget in phases.values()):
        return False
    return assess_capacity(_as_budget(effective_reservation(phases)), snapshot).state == CapacityState.REFUSED


def reserve_capacity(
    context: RunContext,
    attempt: str,
    capacity: ExitStack,
    *,
    inputs: CapacityInputs | None = None,
    machine: "CapacitySnapshot | None" = None,
    disk: DiskInputs | None = None,
    _test_directory: Path | None = None,
) -> ReserveOutcome:
    """Reserve the head the capacity rule admits and retain this guardian's capacity descriptor.

    Reservation is not an execution grant. The guardian must reconcile all indexed
    authority and pass capacity checks before publishing one. Return the outcome: its private
    descriptor, or the reason it waits when an earlier ticket or the physical lock blocks: it then writes no entry and
    stays queued. Malformed evidence raises ResourceError.
    The caller holds its independently opened project lock for the guardian lifetime.
    ``machine`` is any capacity snapshot of this pass: a run predicted shareable whose ``E(self)`` it REFUSES is
    admitted exclusive. ``inputs`` is only a sample taken after holders were seen (a joiner's fit).

    Under the account metadata lock (design 2.4, in today's order): the machine settings, the run's mode (shared only
    above limit 1 with a positive verdict), the reservation verdict, and for a shared joiner beside holders the fit over
    ``inputs`` (the queued pass's sample; without one it waits ``coholder-capacity``); then ``capacity.lock`` in the
    run's mode without waiting; then the queue entry; then the record with ``sharing`` set or removed.
    ``disk`` (this pass's figures, read outside the locks) is decided once the run is chosen, against the leases of
    every other holder; a fitting run's lease is written in the same section (design 2.6). The release helper is never
    disk-gated: freeing disk never waits for disk.
    """
    record = _load_run(context)
    # The verdict store is read only when sharing is possible at all (a lock-free limit read; decided again under M).
    predicted = read_parallel_runs(Path(record["account_directory"])) != 1 and _predicted_shareable(context, record)
    deadline_ns = _queue_budget_ns(record)  # the waits below end with the run's queue budget
    with _locked_run(context, deadline_ns):
        data = _load_run(context)
        clock = NativeClock()
        guardian = asdict(current_process(clock))
        if data["state"] != "queued" or data["guardian_attempt"] != attempt or data["guardian"] != guardian:
            raise ResourceError("Capacity reservation caller is not the queued guardian.")
        if data["source_identity"] != source_identity():
            raise ResourceError("Runtime source changed before capacity reservation.")
        if observe_process(_identity_from_json(data["requester"])) != "executing" or not _deadline_from_json(
            data["queue_deadline"]
        ).remaining_ns(clock):
            raise ResourceError("Queued requester ended or original deadline expired.")
        directory = account_directory() if _test_directory is None else _test_directory.resolve()
        if data["account_directory"] != str(directory):
            raise ResourceError("Queued run belongs to another account admission domain.")
        account = Context(directory, directory)
        with lock(account, "metadata", deadline_ns=deadline_ns) as anchor:
            state = _load_queue(directory, anchor)
            own = state.entries.get(context.run_id)
            if own is None or not _is_own_reference(own, context, attempt, data["ticket"]):
                raise ResourceError("Queued run lost its account ticket identity.")
            settings = read_admission_settings(directory)  # read under M with the decision it serves (D8)
            mode = AdmissionMode.SHARED if settings.limit != 1 and predicted else AdmissionMode.EXCLUSIVE
            if mode == AdmissionMode.SHARED and machine is not None and _never_fits_shared(data, machine, settings):
                mode = (
                    AdmissionMode.EXCLUSIVE
                )  # E(self) exceeds the machine even alone; exclusive admission needs no reserve
            facts = {
                run_id: (
                    EntryFacts(False, "executing", mode, True)
                    if run_id == context.run_id
                    else _entry_facts(run_id, entry, directory)
                )
                for run_id, entry in state.entries.items()
            }
            views = [_entry_view(run_id, entry, facts[run_id]) for run_id, entry in state.entries.items()]
            refusal = _gate_refusal(_test_directory, release_helper=data["purpose"] == RELEASE_PURPOSE)
            if refusal is not None:  # gate point 3: inside the section, before the head is chosen
                return ReserveOutcome(None, None, refusal)
            turn = _turn_outcome(
                settings, state, views, directory, None if disk is None else disk.daemon_id, _test_directory
            )
            if turn is not None and turn.wait and turn.chosen != context.run_id:
                return ReserveOutcome(None, "repository-turn")
            verdict = reservation_verdict(views, head_mode=mode, limit=settings.limit)
            if turn is not None and turn.chosen == context.run_id:
                verdict = replace(verdict, run_id=context.run_id)  # the turn's run reserves in its own mode
            overtakes = verdict.run_id != context.run_id and _may_overtake(
                context.run_id, data, mode, settings, state, views, directory, _test_directory
            )
            if verdict.run_id != context.run_id and not overtakes:  # a head's reason about its own mode is its alone
                head = fifo_head(views) == context.run_id
                return ReserveOutcome(None, verdict.reason if head and verdict.reason else "queue-position")
            holding = [(run_id, entry) for run_id, entry in state.entries.items() if entry.phase in HOLDER_PHASES]
            if holding:  # a shared joiner (the verdict admits nothing else beside holders)
                if inputs is None:
                    return ReserveOutcome(None, "coholder-capacity")  # sampled while no holder existed
                holders = []
                for run_id, entry in holding:
                    holder_record = _entry_record(run_id, entry, directory)
                    if holder_record is None:
                        return ReserveOutcome(None, "exclusive-holder")
                    if _other_repository(settings, data, holder_record):
                        return ReserveOutcome(None, "other-repository")
                    holders.append(_holder_facts(run_id, entry, holder_record))
                snapshot = _coholder_snapshot(inputs.snapshot, inputs.containers, holders, settings.host_reserve_bytes)
                decided = admission_decision(
                    _phase_budgets(data["phases"], data["initial_phase"]),
                    snapshot,
                    [holder.reservation for holder in holders],
                )
                if decided.state == CapacityState.REFUSED:
                    return ReserveOutcome(None, None, decided.reason)
                if decided.state != CapacityState.READY:
                    return ReserveOutcome(None, decided.reason)
                if cpu_pressure_blocks_join(inputs.cpu_pressure):
                    return ReserveOutcome(None, "cpu-pressure")
            gated = disk is not None and data["purpose"] != RELEASE_PURPOSE
            leases: dict[str, Lease] = {}
            if disk is not None and gated:
                fit, leases = _disk_gate(disk, directory, state, context.run_id, clock.origin)
                if fit.state == CapacityState.REFUSED:
                    return ReserveOutcome(None, None, fit.reason)
                if fit.state != CapacityState.READY:
                    return ReserveOutcome(None, fit.reason)
            try:
                descriptor = capacity.enter_context(
                    lock(account, "capacity", shared=mode == AdmissionMode.SHARED, nonblocking=True)
                )
            except ResourceBusy:
                return ReserveOutcome(None, "capacity-lock-busy")
            account_queue.reserve(state, context.run_id, guardian, limit=settings.limit)
            _save_queue(directory, state, anchor)
            if disk is not None and gated:  # the lease, in the section that chose the run (a crash between: the floor)
                leases[context.run_id] = Lease(disk.estimate, disk.source, clock.origin, clock.read())
                holder_ids = {run_id for run_id, entry in state.entries.items() if entry.phase in HOLDER_PHASES}
                write_leases(directory, leases, holders=holder_ids, clock_origin=clock.origin)
            failpoint("reserve-queue-written")
            data["state"] = "admitting"
            data["revision"] += 1
            if mode == AdmissionMode.SHARED:
                data["sharing"] = {"mode": AdmissionMode.SHARED.value}
            else:
                data.pop("sharing", None)  # an exclusive generation never carries the key (S10)
            atomic_json(context.record, data)
            return ReserveOutcome(descriptor, None)


def _publish_grant(
    context: RunContext,
    attempt: str,
    capacity: TextIO | None,
    snapshot: "CapacitySnapshot",
    current: "HostMemoryObservation",
    window_start: "HostMemoryObservation",
    *,
    daemon_id: str,
    containers: Sequence[dict] = (),
    waits: _AdmissionWaits,
    reserved_ns: int,
    window_ns: int,
    _test_directory: Path | None = None,
    _host_script: "_HostScript | None" = None,
    _admission_baseline: "HostMemoryObservation | None" = None,
    disk: DiskInputs | None = None,
) -> "RunStatus | _Refused | _Waiting":
    """Publish one checked grant from the actual reserved guardian and descriptor.

    Returns ``_Refused`` when the version gate refuses inside the publication section; the caller revokes the run
    after leaving every section (``recovery.lock`` is not re-entrant).

    ``window_ns`` is the guardian's own admission window, which the publication's pressure re-check uses too.

    A private host-proof domain exercises native locking only and must never pass
    the canonical-domain check of a real Docker operation. The guardian supplies
    complete native observations; publication checks all declared phase budgets.
    """
    directory = account_directory() if _test_directory is None else _test_directory.resolve()
    canonical = canonical_account()
    if _test_directory is not None and canonical is not None and overlaps_account(directory, canonical):
        raise ResourceError("Host-proof observations cannot authorize canonical Docker work.")
    if not isinstance(daemon_id, str) or not NAME.fullmatch(daemon_id):
        raise ResourceError("Invalid observed daemon identity.")
    unlocked = _load_run(context)
    deadline_ns = _queue_budget_ns(unlocked)  # the waits below end with the run's queue budget
    inventory: Sequence[dict] = containers  # replaced by the refreshed snapshot's own inventory in production
    # The CPU pressure may need a probe run: taken before the run lock, never under it (design 3.1 (5), 3.2)
    if _test_directory is not None:
        join_pressure = _test_capacity_record(_test_directory).get("cpu_pressure")
    else:
        probe_clock = NativeClock()
        try:
            join_pressure = _join_pressure(unlocked, _docker(), daemon_id, Deadline.after(probe_clock, 30), probe_clock)
        except (ResourceError, OSError) as error:
            raise _admission_fault(error) from error
    with _locked_run(context, deadline_ns):
        data = _load_run(context)
        clock = NativeClock()
        guardian = asdict(current_process(clock))
        if (
            data["state"] != "admitting"
            or data["admission"] != "never-granted"
            or data["guardian_attempt"] != attempt
            or data["guardian"] != guardian
            or data["account_directory"] != str(directory)
        ):
            raise ResourceError("Grant publication caller is not the reserved original guardian.")
        if data["source_identity"] != source_identity():
            raise ResourceError("Runtime source changed before grant publication.")

        def daemon_changed() -> _AdmissionFault:
            """Build the classified refusal of a daemon that changed between reservation and publication."""
            return _AdmissionFault(
                "Docker daemon changed during grant reconciliation.",
                TelemetryFault("daemon-identity-changed", "engine"),
            )

        if _test_directory is None:
            try:
                read_deadline = Deadline.after(clock, 30)
                docker = _docker()
                inventory = docker.objects("container", deadline=read_deadline)  # the snapshot's own inventory
                refreshed_daemon_id, snapshot = observe_capacity(
                    docker, data["topology"], inventory=inventory, deadline=read_deadline
                )
                if refreshed_daemon_id != daemon_id:
                    raise daemon_changed()
                current = _host_read(clock, read_deadline)
            except (ResourceError, OSError) as error:
                raise _admission_fault(error) from error
        budgets = _phase_budgets(data["phases"], data["initial_phase"])
        if not budgets:
            raise ResourceError("Not every declared phase passed capacity admission.")
        now = clock.read()
        if (
            current.origin != clock.origin
            or current.physical_bytes != snapshot.host_memory_bytes
            or not 0 <= now - current.tick_ns <= 5_000_000_000
            or assess_pressure(current, window_start, admission_baseline=_admission_baseline, window_ns=window_ns).state
            != CapacityState.READY
        ):
            raise ResourceError("A fresh complete native pressure observation is required before grant.")
        if observe_process(_identity_from_json(data["requester"])) != "executing":
            raise ResourceError("Requester ended before grant publication.")
        account = Context(directory, directory)
        with lock(account, "metadata", deadline_ns=deadline_ns) as anchor:
            state = _load_queue(directory, anchor)
            slot = state.entries.get(context.run_id)
            if (
                slot is None
                or slot.phase != "admitting"
                or not _is_own_reference(slot, context, attempt, data["ticket"])
                or slot.holder != guardian
                or not _deadline_from_json(data["queue_deadline"]).remaining_ns(clock)
            ):
                raise ResourceError("Reservation changed or original queue deadline expired before grant.")
            refusal = _gate_refusal(_test_directory, release_helper=data["purpose"] == RELEASE_PURPOSE)
            failpoint("publish-gate-checked")  # gate point 4: under the lock the installer's switch also takes
            if refusal is not None:
                return _Refused(refusal)
            settings = read_admission_settings(directory)  # read under M with the decision it serves (D8)
            mode = _record_mode(data)
            views = [
                _entry_view(run_id, entry, EntryFacts(False, "executing", mode, True))
                if run_id == context.run_id
                else _entry_view(run_id, entry, _entry_facts(run_id, entry, directory))
                for run_id, entry in state.entries.items()
            ]
            waiting = publication_verdict(views, publisher=context.run_id, publisher_mode=mode, limit=settings.limit)
            if waiting is not None:
                return _Waiting(waiting)  # keeps its admitting slot and retries on the next pass (R3-2)
            decided = replace(snapshot, host_reserve_bytes=settings.host_reserve_bytes)
            holders: list[HolderFacts] = []
            baselines: dict[str, tuple[str, int]] = {}
            if mode != AdmissionMode.SHARED:
                if any(assess_capacity(budget, decided).state != CapacityState.READY for budget in budgets.values()):
                    raise ResourceError("Not every declared phase passed capacity admission.")
            else:  # E(self) + Σ E(co-holders), on the snapshot's own inventory, co-holders attributed (design 2.4)
                for run_id, entry in state.entries.items():
                    if entry.phase != "granted" or run_id == context.run_id:
                        continue
                    record = _entry_record(run_id, entry, directory)
                    if record is None:
                        return _Waiting("exclusive-holder")
                    if _other_repository(settings, data, record):
                        return _Waiting("other-repository")
                    holders.append(_holder_facts(run_id, entry, record))
                    memory = record["admission_observation"]["memory"]
                    baselines[run_id] = (memory["origin"], memory["tick_ns"])
                together = admission_decision(
                    budgets,
                    _coholder_snapshot(decided, inventory, holders, settings.host_reserve_bytes),
                    [holder.reservation for holder in holders],
                    shared=True,
                )
                if together.state == CapacityState.REFUSED:
                    raise ResourceError("Not every declared phase passed capacity admission.")
                if together.state != CapacityState.READY:
                    return _Waiting(together.reason)
                if holders and cpu_pressure_blocks_join(join_pressure):
                    return _Waiting("cpu-pressure")  # pressure may have risen since the reservation (re-checked here)
            if disk is not None and data["purpose"] != RELEASE_PURPOSE:  # the disk may have filled since reservation
                if not disk_figures_fresh(disk, clock):  # sampled before this section waited for its locks
                    raise _AdmissionFault(
                        "The disk figures aged past their freshness limit before the grant was written.",
                        TelemetryFault("observation-late", "host"),
                    )
                fit, _leases = _disk_gate(disk, directory, state, context.run_id, clock.origin)
                if fit.state == CapacityState.REFUSED:
                    return _Refused(fit.reason)
                if fit.state != CapacityState.READY:
                    return _Waiting(fit.reason)

            def require_fresh() -> None:
                """Refuse to grant from a sample that aged past its limit while this section waited or worked."""
                if not 0 <= clock.read() - current.tick_ns <= 5_000_000_000:
                    raise _AdmissionFault(
                        "The pressure sample aged past its freshness limit before the grant was written.",
                        TelemetryFault("observation-late", "host"),
                    )

            require_fresh()
            if capacity is None or capacity.closed:
                raise ResourceError("Guardian lost its capacity descriptor before publication.")
            held = os.fstat(capacity.fileno())
            named = (directory / "capacity.lock").stat(follow_symlinks=False)
            if (
                (held.st_dev, held.st_ino) != (named.st_dev, named.st_ino)
                or not stat.S_ISREG(named.st_mode)
                or held.st_uid != os.getuid()
                or held.st_mode & 0o077
            ):
                raise ResourceError("Guardian capacity descriptor does not match the permanent anchor.")
            try:
                # Re-assert the mode this guardian already holds: shared for a shared run, exclusive otherwise.
                fcntl.flock(
                    capacity.fileno(),
                    (fcntl.LOCK_SH if mode == AdmissionMode.SHARED else fcntl.LOCK_EX) | fcntl.LOCK_NB,
                )
            except BlockingIOError as error:
                raise ResourceBusy("Guardian cannot hold the physical capacity lock.") from error
            resuming = data.get("retained_grant") is not None
            generation = data["generation"] + 1 if resuming else 1
            authority = data["docker_authority"]
            if resuming and authority is not None:
                if authority["generation"] != data["generation"] or authority["daemon_id"] != daemon_id:
                    raise ResourceError("Retained Docker authority differs from the new admission grant.")
                _require_docker_authority_files(context, authority)
                identity = _bound_docker(data).observe_daemon()
                require_fresh()  # the daemon observation may have run long; nothing is written yet
                authority = _publish_docker_authority(
                    context,
                    data,
                    identity,
                    generation,
                )
            data.update(
                state="active",
                admission="published",
                generation=generation,
                revision=data["revision"] + 1,
                retained_grant=None,
                resume_origin=None,
                docker_authority=authority,
            )
            data["grant"] = {
                "generation": generation,
                "revision": data["revision"],
                "attempt": attempt,
                "ticket": data["ticket"],
                "requester": data["requester"],
                "guardian": guardian,
                "daemon_id": daemon_id,
                "scope": "managed" if _test_directory is None else "host-proof",
                "phase": data["initial_phase"],
                "requirements_identity": _requirements_identity(data),
            }
            data["admission_observation"] = {
                # the detected runtime is never recorded (design 2.5): the record keeps 0.6.20's capacity keys
                "capacity": {key: value for key, value in asdict(snapshot).items() if key != "runtime"},
                "memory": asdict(current),
                "window_start": asdict(window_start),
                "samples": [asdict(current)],
            }
            data["phase_history"] = [
                {"phase": data["initial_phase"], "revision": data["revision"], "memory": asdict(current)}
            ]
            clock = NativeClock()
            granted_ns = clock.read()
            timeline = {
                "schema": 1,
                "origin": clock.origin,
                "accounting_from_ns": waits.first_tick_ns,
                "reserved_ns": reserved_ns,
                "granted_ns": granted_ns,
                "waiting": waits.waiting_at(granted_ns),
                "release": None,
                "release_origin": None,
                "released_ns": None,
            }
            validate_hold_timeline(timeline)  # an impossible value raises before anything is written
            data["hold_timeline"] = timeline  # written in the grant's own atomic write (design 3.2, C5)
            atomic_json(context.record, data)
            account_queue.grant(state, context.run_id, guardian)
            _save_queue(directory, state, anchor)
            if holders:  # granted beside a co-holder: log it so co-holders' allowances remember it (design 2.5)
                failpoint("grant-published")
                _log_grant(
                    directory,
                    data,
                    budgets,
                    {**baselines, context.run_id: (current.origin, current.tick_ns)},
                    strict=False,
                )
        return run_status(context)


def _repair_grant_log(directory: Path, state: QueueState, entry: QueueEntry, data: dict) -> None:
    """Upsert a granted shared generation's grant before recovery removes its entry; the caller holds M (C4).

    A publisher that died between its grant write and its log append left no entry; the upsert is idempotent, so a
    recoverer that dies after it and its successor count the grant once. Only a grant some current holder can count
    (granted after that holder's admission baseline, on its clock) is logged, as at publication: a run granted alone
    is never in the log.
    """
    if entry.phase != "granted" or _record_mode(data) != AdmissionMode.SHARED:
        return
    budgets = _phase_budgets(data["phases"], data["initial_phase"])
    baselines = _holder_baselines(state, directory, exclude=data["run_id"])
    grant = _grant_of(data, budgets)
    if not any(origin == grant.origin and tick < grant.granted_ns for origin, tick in baselines.values()):
        return
    _log_grant(directory, data, budgets, baselines, strict=True)
    failpoint("recovery-grant-appended")


def _holder_baselines(state: QueueState, directory: Path, *, exclude: str) -> dict[str, tuple[str, int]]:
    """Return the admission-baseline origin and tick of every other granted entry with a readable record."""
    baselines = {}
    for run_id, entry in state.entries.items():
        record = _entry_record(run_id, entry, directory) if entry.phase == "granted" and run_id != exclude else None
        if record is not None:
            memory = record["admission_observation"]["memory"]
            baselines[run_id] = (memory["origin"], memory["tick_ns"])
    return baselines


def _grant_of(data: dict, budgets: Mapping[str, PhaseBudget]) -> Grant:
    """Return the grant a granted record's current generation counts as in co-holders' allowances (design 2.5)."""
    reserved = effective_reservation(budgets)
    return Grant(
        data["run_id"],
        data["generation"],
        data["hold_timeline"]["origin"],
        data["hold_timeline"]["granted_ns"],
        reserved.container_bytes + reserved.host_process_bytes,
    )


def _log_grant(
    directory: Path,
    data: dict,
    budgets: Mapping[str, PhaseBudget],
    baselines: Mapping[str, tuple[str, int]],
    *,
    strict: bool,
) -> None:
    """Upsert the granted generation of ``data`` in the grants log; the caller holds M (design 2.5, C4).

    An unreadable or invalid log is left as it is: every shared observer then counts as having shared with no
    shared-only breach (R3-8), and rewriting it would drop other runs' entries. A failed write of a readable log is
    ignored at publication (the grant is already written, and the run's leave or recovery repairs the log before its
    entry goes) and raised when ``strict`` (that repair), so the entry stays until the log holds the grant.
    """
    try:
        append_grant(directory, _grant_of(data, budgets), holder_baselines=baselines)
    except ResourceError:
        return
    except OSError:
        if strict:
            raise


def revoke_ungranted(
    context: RunContext,
    capacity: ExitStack | None = None,
    *,
    _test_directory: Path | None = None,
    _failure_reason: str | None = None,
    _takeover_attempt: str | None = None,
    _deadline_ns: int | None = None,
    _admission_evidence: Callable[[dict], dict | None] | None = None,
) -> RunStatus:
    """Revoke a dead/expired ungranted actor without erasing any execution authority.

    A living guardian can revoke its own attempt after requester loss/expiry and
    closes its capacity stack before unlocking metadata. Other actors need proof
    that the guardian ended (or never bound before bootstrap expiry/requester loss).
    Private material remains protected by the requesting process's separate lifetime. A pending guardian start within
    its bootstrap deadline refuses with ``guardian-start-pending`` even when the requester is gone, and an entry's
    takeover claim must be free for the caller (``claim-held``); revocation itself binds no claim.

    ``_admission_evidence`` builds the ``failure_evidence`` of an admission that ended (queue-deadline expiry or a
    classified observation fault) from the record as it is at the write, so the evidence and the revoke are one atomic
    write. It applies to a first revoke only: a resumed admission restores the retained record and keeps no evidence.
    """
    if _failure_reason is not None and (not isinstance(_failure_reason, str) or not NAME.fullmatch(_failure_reason)):
        raise ResourceError("Invalid pregrant failure reason.")
    project = context.project
    with (
        lock(project, project.project_id, shared=True, nonblocking=True),
        lock(project, f"run-{context.run_id}", deadline_ns=_deadline_ns),
    ):
        data = _load_run(context)
        _require_ungranted(data)
        clock = NativeClock()
        guardian = data["guardian"]
        requester_live = observe_process(_identity_from_json(data["requester"])) == "executing"
        pending = data["pending_operation"]
        bootstrap = data.get("bootstrap_deadline")
        if (
            guardian is None
            and isinstance(pending, dict)
            and pending.get("kind") == "guardian-start"
            and bootstrap is not None
            and _deadline_from_json(bootstrap).remaining_ns(clock)
        ):
            raise ResourceBusy(
                "A guardian start is still within its bootstrap deadline.", reason="guardian-start-pending"
            )
        if guardian is not None and observe_process(_identity_from_json(guardian)) == "executing":
            if guardian != asdict(current_process(clock)) or capacity is None:
                raise ResourceBusy("A live guardian must acknowledge its own cancellation.")
            if (
                _failure_reason is None
                and requester_live
                and _deadline_from_json(data["queue_deadline"]).remaining_ns(clock)
            ):
                raise ResourceBusy("Live queued requester still has its original wait budget.")
        elif guardian is None and requester_live:
            if bootstrap is None or _deadline_from_json(bootstrap).remaining_ns(clock):
                raise ResourceBusy(
                    "Live preparation has not exhausted guardian startup.", reason="guardian-start-pending"
                )
        directory = account_directory() if _test_directory is None else _test_directory.resolve()
        if data["account_directory"] not in (None, str(directory)):
            raise ResourceError("Revocation cannot move to another account admission domain.")
        account = Context(directory, directory)
        account_lock = (
            lock(account, "metadata", deadline_ns=_deadline_ns)
            if data["account_directory"] is not None
            else nullcontext()
        )
        with account_lock as anchor:
            state = _load_queue(directory, anchor) if data["account_directory"] is not None else None
            if state is not None:
                entry = state.entries.get(context.run_id)
                if entry is not None and (
                    entry.attempt != data["guardian_attempt"]
                    or entry.record != str(context.record)
                    or entry.registry != str(project.state)
                    or entry.project_root != str(project.root)
                    or (data["ticket"] is not None and entry.ticket != data["ticket"])
                ):
                    raise ResourceError("Revocation found contradictory original account authority.")
                _require_claim_eligible(entry, _takeover_attempt)
            resuming = data.get("retained_grant") is not None
            if resuming:
                _restore_stopped_retained(data)
                atomic_json(context.record, data)
            elif data["admission"] != "revoked":
                evidence = None
                if _admission_evidence is not None and "failure_evidence" not in data:
                    evidence = _admission_evidence(data)
                data.update(
                    state="complete",
                    admission="revoked",
                    test_outcome="cancelled",
                    cleanup_outcome="pending",
                    pending_operation=None,
                    infrastructure_failure=_failure_reason or data.get("infrastructure_failure"),
                )
                if evidence is not None:
                    data["failure_evidence"] = evidence
                data["revision"] += 1
                atomic_json(context.record, data)
                failpoint("revoke-record-written")
            if state is not None and account_queue.remove(state, context.run_id) is not None:
                _save_queue(directory, state, anchor)
                _drop_lease(directory, state)
            if capacity is not None:
                capacity.close()
        if resuming:
            return run_status(context)
        if not requester_live:
            finished = data["cleanup_outcome"] == "complete"
            if not finished:
                _remove_private_root(context)
                failpoint("private-root-removed")
                data.update(cleanup_outcome="complete", recovery_outcome="complete", quiescent=True)
                data["revision"] += 1
                atomic_json(context.record, data)
                failpoint("revoke-cleanup-written")
            with lock(project, "registry", deadline_ns=_deadline_ns):
                registry = load(project)
                location = registry["runs"].get(context.run_id)
                if location is None and finished:
                    return run_status(context)  # an earlier caller already retired the run: nothing left to write
                if location != str(context.record):
                    raise ResourceError("Revoked run lost its project recovery reference.")
                del registry["runs"][context.run_id]
                save(project, registry)
        return run_status(context)


def _entry_vitals(run_id: str, entry: QueueEntry, directory: Path) -> str:
    """Observe one other run's entry read-only and without locks: ``dead``, ``alive`` or ``blocked``.

    ``dead`` needs every actor (requester, guardian, slot holder, registered consumers and any recovery actor) proven
    absent or replaced. Any actor that may run, and any observation that cannot be made, is ``alive``; a record that
    cannot be trusted is ``blocked``. Nothing is written.
    """
    data = _entry_record(run_id, entry, directory)
    if data is None:
        return "blocked"
    actors = [data["requester"], data["guardian"], data.get("recovery_actor"), entry.holder]
    actors.extend(item["identity"] for item in data["consumers"].values())
    try:
        states = [observe_process(_identity_from_json(actor)) for actor in actors if actor is not None]
    except ResourceError:
        return "alive"
    return "dead" if actors_dead(states) else "alive"


def _note_deaths_and_claim(
    directory: Path, entries: dict[str, QueueEntry], observed: dict[str, str], deadline_ns: int | None = None
) -> tuple[str, str] | None:
    """Record what the unlocked reads saw and, in the same metadata section, claim at most one dead entry to recover.

    Entries seen dead get ``dead_since`` (restarted when its clock origin changed); anything else clears it. The first
    entry (ticket order) that ``spawn_eligible`` accepts gets the claim intent ``{attempt, 30 s deadline, claimant
    null}``; eligibility, the account cap and the intent never span two sections. Returns ``(run_id, attempt)``.
    """
    account = Context(directory, directory)
    with lock(account, "metadata", deadline_ns=deadline_ns) as anchor:
        state = _load_queue(directory, anchor)
        clock = NativeClock()
        now = clock.read()
        seen = {
            run_id: entry
            for run_id, entry in state.entries.items()
            if run_id in entries and account_queue.reference(entry) == account_queue.reference(entries[run_id])
        }
        changed = False
        for run_id, entry in seen.items():
            if observed[run_id] == "dead":
                if entry.dead_since is None or entry.dead_since["origin"] != clock.origin:
                    entry.dead_since = {"origin": clock.origin, "tick_ns": now}
                    changed = True
            elif entry.dead_since is not None:
                entry.dead_since = None
                changed = True
        held = sum(
            not claim_eligible(
                entry.takeover,
                caller_attempt=None,
                claimant_state=_claim_observation(entry.takeover),
                now_tick_ns=now,
                origin=clock.origin,
            )
            for entry in state.entries.values()
        )
        chosen = None
        for run_id, entry in sorted(seen.items(), key=lambda item: item[1].ticket):
            if observed[run_id] == "dead" and spawn_eligible(
                {"dead_since": entry.dead_since, "takeover": entry.takeover},
                claimant_state=_claim_observation(entry.takeover),
                now_tick_ns=now,
                origin=clock.origin,
                live_or_unbound_claims=held,
                spawned_this_pass=False,
            ):
                attempt = str(uuid.uuid4())
                entry.takeover = {
                    "attempt": attempt,
                    "deadline": asdict(Deadline.after(clock, UNBOUND_CLAIM_NS // 1_000_000_000)),
                    "claimant": None,
                }
                chosen, changed = (run_id, attempt), True
                break
        if changed:
            _save_queue(directory, state, anchor)
        return chosen


def _clear_claim_intent(directory: Path, run_id: str, attempt: str, deadline_ns: int | None = None) -> None:
    """Withdraw this spawner's own unbound claim intent after its worker could not start; anything else is left."""
    with lock(Context(directory, directory), "metadata", deadline_ns=deadline_ns) as anchor:
        state = _load_queue(directory, anchor)
        entry = state.entries.get(run_id)
        if entry is not None and entry.takeover is not None and entry.takeover["claimant"] is None:
            if entry.takeover["attempt"] == attempt:
                entry.takeover = None
                _save_queue(directory, state, anchor)


def _recovery_worker_directory(parent: Path | None = None) -> Path:
    return Path(tempfile.mkdtemp(prefix="docker-test-toolkit-recover-", dir=parent))  # created 0700


def _spawn_recovery_worker(context: RunContext, attempt: str, test_directory: Path | None) -> None:
    """Start one detached takeover worker for ``context`` from its own private temporary directory.

    The directory (0700, ``docker-test-toolkit-recover-*``) holds the worker's logs; the worker removes it at exit. The
    worker runs this process's own installed archive, which never changes in place. Nothing the spawner holds is
    inherited. Raises ``ResourceError`` after removing the directory when anything fails before the process exists.
    """
    try:
        directory = _recovery_worker_directory()
    except OSError as error:
        raise ResourceError("The recovery worker directory could not be created.") from error
    try:
        command = _toolkit_process_command("_recover", "--context", _context_payload(context), "--attempt", attempt)
        if test_directory is not None:
            command.extend(("--test-directory", str(test_directory.resolve())))
        stdout = _open_guardian_log(directory / "worker.stdout.log")
        stderr = _open_guardian_log(directory / "worker.stderr.log")
        failpoint("worker-spawn")
        try:
            child = subprocess.Popen(
                command,
                stdin=subprocess.DEVNULL,
                stdout=stdout,
                stderr=stderr,
                close_fds=True,
                start_new_session=True,
                env=_toolkit_child_environment(directory),
            )
        finally:
            stdout.close()
            stderr.close()
    except (OSError, ResourceError) as error:
        shutil.rmtree(directory, ignore_errors=True)
        raise ResourceError("The recovery worker could not start.") from error
    threading.Thread(target=child.wait, name=f"recovery-worker-reaper-{context.run_id}", daemon=True).start()


def reconcile_account(context: RunContext | None = None, *, _test_directory: Path | None = None) -> None:
    """Detect dead queue entries of other runs and spawn at most one takeover worker per pass (design 3.10).

    Run by a queued guardian in its admission pass; ``context`` is its own run, whose entry is never claimed by itself.
    Foreign records are read only, without any project or run lock, and a record that cannot be trusted just blocks that
    entry for this pass. A dead holder keeps the slot until its takeover publishes. The queue is read through the
    ordinary codec, so a non-empty version 1 queue (the recovery-only phase) is refused before any detection.
    Never recovers a run inline.
    """
    # ponytail: a dead entry lingers on an idle account until the next toolkit activity; ceiling: no always-on
    # daemon; upgrade path: a scheduled recovery pass
    directory = account_directory() if _test_directory is None else _test_directory.resolve()
    own = None if context is None else context.run_id
    deadline_ns = None if context is None else _queue_budget_ns(_load_run(context))
    with lock(Context(directory, directory), "metadata", deadline_ns=deadline_ns) as anchor:
        entries = {run_id: entry for run_id, entry in _load_queue(directory, anchor).entries.items() if run_id != own}
    observed = {run_id: _entry_vitals(run_id, entry, directory) for run_id, entry in entries.items()}
    failpoint("detection-read")
    claim = _note_deaths_and_claim(directory, entries, observed, deadline_ns)
    if claim is None:
        return
    run_id, attempt = claim
    entry = entries[run_id]
    registry = Path(entry.registry)
    try:
        worker = recorded_run_context(registry, run_id, _read_private_json(Path(entry.record)))
        if worker is None:
            raise ResourceError("The dead run's record names no usable context.")
        _spawn_recovery_worker(worker, attempt, _test_directory)
    except (OSError, ResourceError):
        _clear_claim_intent(directory, run_id, attempt, deadline_ns)


@dataclass
class JobMemoryLimit:
    """Carry one native job/cgroup memory ceiling and attributed usage."""

    memory_bytes: int
    managed_docker_bytes: int | None
    external_docker_bytes: int | None


# the runtime a synthetic private-account snapshot stands for, by its declared topology
_TOPOLOGY_RUNTIME = {"desktop": "docker-desktop", "native": "docker-engine", "remote": None}


def _bound_runtime(bound: dict, topology: str) -> str | None:
    """A private account's bound runtime: the one ``bind_test_capacity`` names, else the declared topology's."""
    return bound.get("runtime", _TOPOLOGY_RUNTIME[topology])


@dataclass
class CapacitySnapshot:
    """Carry one consistent daemon/host/job capacity observation."""

    daemon_memory_bytes: int
    daemon_cpus: int
    host_memory_bytes: int
    host_cpus: int
    topology: str
    external_limits: tuple[int | None, ...]
    job_limits: tuple[JobMemoryLimit, ...] | None = ()
    host_reserve_bytes: int = HOST_RESERVE_DEFAULT
    runtime: str | None = field(kw_only=True)  # the detected runtime (design 2.5); ``None`` for a remote daemon


def _cgroup_path(pid: int) -> PurePosixPath:
    rows = Path(f"/proc/{pid}/cgroup").read_text().splitlines()
    matches = [row.partition("::") for row in rows if row.startswith("0::")]
    if len(matches) != 1 or matches[0][0] != "0" or not matches[0][1]:
        raise ResourceError("Linux process cgroup identity is unavailable.")
    path = PurePosixPath(matches[0][2])
    if not path.is_absolute() or ".." in path.parts:
        raise ResourceError("Linux process cgroup identity is invalid.")
    return path


def _cgroup2_mount() -> tuple[PurePosixPath, Path]:
    matches = []
    for row in Path("/proc/self/mountinfo").read_text().splitlines():
        left, separator, right = row.partition(" - ")
        fields = left.split()
        if separator and right.split()[:1] == ["cgroup2"] and len(fields) >= 5:
            matches.append((PurePosixPath(fields[3]), Path(fields[4])))
    if len(matches) != 1 or not matches[0][0].is_absolute() or not matches[0][1].is_absolute():
        raise ResourceError("Linux cgroup-v2 mount identity is unavailable.")
    return matches[0]


def _linux_job_limits(
    containers: list[dict], owned_ids: set[str], is_stopped: Callable[[str], bool] | None = None
) -> tuple[JobMemoryLimit, ...] | None:
    """Read finite enclosing job limits and Docker commitments sharing each one.

    ``is_stopped(container_id)`` re-inspects one container whose inventory process ended before its cgroup was read;
    without it such a container keeps the containment unknown.
    """
    try:
        mount_root, mount = _cgroup2_mount()
        current = _cgroup_path(os.getpid())
        relative = current.relative_to(mount_root)
        current_fs = mount / Path(*relative.parts)
        read = _container_cgroup_paths(containers, is_stopped)
        if read is None:
            return None
        container_paths, stopped = read
        live = [item for item in containers if item["Id"] not in stopped]
        return _job_limits_in_tree(mount_root, mount, current_fs, live, container_paths, owned_ids)
    except (OSError, ValueError, ResourceError):
        return None


def _container_stopped(observed: DockerObservation) -> bool:
    """Whether a re-inspected container provably holds no process: removed, or reported neither running nor
    restarting. A restart under a new PID, or a state that cannot be read, is not stopped."""
    if not observed.exists:
        return True
    state = observed.attributes.get("State")
    return isinstance(state, dict) and state.get("Running") is False and state.get("Restarting") is False


def _container_cgroup_paths(
    containers: list[dict],
    is_stopped: Callable[[str], bool] | None,
    read: Callable[[int], PurePosixPath] = _cgroup_path,
) -> tuple[dict[str, PurePosixPath], set[str]] | None:
    """Return each running container's cgroup and the containers that stopped since the inventory, or None (unknown).

    A container that stops between the inventory and its ``/proc/<pid>/cgroup`` read is routine on a shared daemon (a
    co-holder's or another project's container). Its process being gone proves nothing by itself: a /proc without the
    daemon's PIDs, or a container that restarted under a new PID, looks the same. So the container is inspected again,
    and only one that is gone or neither running nor restarting is left out (it holds no memory); any other answer
    keeps the containment unknown.
    """
    paths: dict[str, PurePosixPath] = {}
    stopped: set[str] = set()
    for item in containers:
        if not item["State"]["Running"]:
            continue
        pid = item["State"].get("Pid")
        if type(pid) is not int or pid <= 0:
            return None
        try:
            paths[item["Id"]] = read(pid)
        except (FileNotFoundError, ProcessLookupError):
            if is_stopped is None or not is_stopped(item["Id"]):
                return None
            stopped.add(item["Id"])
    return paths, stopped


def _job_limits_in_tree(
    mount_root: PurePosixPath,
    mount: Path,
    current_fs: Path,
    containers: list[dict],
    container_paths: dict[str, PurePosixPath],
    owned_ids: set[str],
) -> tuple[JobMemoryLimit, ...] | None:
    """Walk from ``current_fs`` up to the cgroup-v2 ``mount`` and collect every finite ``memory.max``.

    The kernel gives only its root cgroup no ``memory.max`` (and no ``cgroup.type``), so a mount that shows that root
    adds no ceiling of its own. Anywhere else a missing file stays unknown, never unlimited: a cgroup-namespace root or
    a bind-mounted subtree without one hides an ancestor's ceiling. Returns None for an invalid limit; an unreadable
    level raises ``OSError``, which the caller reports as unknown containment.
    """
    result = []
    candidate = current_fs
    while candidate == mount or mount in candidate.parents:
        try:
            raw = (candidate / "memory.max").read_text().strip()
        except FileNotFoundError:
            if candidate != mount or (candidate / "cgroup.type").exists():
                raise
            (candidate / "cgroup.controllers").read_text()  # a live cgroup directory, not a vanished path
            break
        if raw != "max":
            if not raw.isdigit() or int(raw) <= 0:
                return None
            logical = mount_root / PurePosixPath(candidate.relative_to(mount).as_posix())
            managed = external = 0
            for item in containers:
                if not item["State"]["Running"]:
                    continue
                path = container_paths[item["Id"]]
                if path != logical and logical not in path.parents:
                    continue
                limit = item["HostConfig"]["Memory"]
                if item["Id"] in owned_ids:
                    managed += limit
                else:
                    external += limit
            result.append(JobMemoryLimit(int(raw), managed, external))
        if candidate == mount:
            break
        candidate = candidate.parent
    return tuple(result)


def _mutation_capacity_ids(
    containers: list[dict],
    owned_ids: set[str],
    plan: dict,
    limits: dict[str, int],
) -> set[str]:
    """Include one bounded Compose one-off that substitutes for its stopped service."""
    _validate_compose_plan(plan)
    services = set(plan["services"])
    if set(limits) != services or any(type(limit) is not int or limit <= 0 for limit in limits.values()):
        raise ResourceError("Compose mutation capacity limits are invalid.")
    expected = plan["labels"]
    running: dict[str, list[tuple[str, int, bool]]] = {service: [] for service in services}
    for item in containers:
        if not item["State"]["Running"]:
            continue
        labels = item.get("Config", {}).get("Labels") or {}
        service = labels.get("com.docker.compose.service")
        exact = (
            labels.get("com.docker.compose.project") == plan["project"]
            and all(labels.get(key) == value for key, value in expected.items())
            and isinstance(service, str)
            and service in services
        )
        container_id = item["Id"]
        if container_id in owned_ids and not exact:
            raise ResourceError("Owned mutation capacity identity lost its exact Compose labels.")
        if (
            exact
            and isinstance(service, str)
            and (container_id in owned_ids or labels.get("com.docker.compose.oneoff") == "True")
        ):
            running[service].append((container_id, item["HostConfig"]["Memory"], container_id in owned_ids))
    effective = set(owned_ids)
    for service, items in running.items():
        if all(limit > 0 for _, limit, _ in items) and sum(limit for _, limit, _ in items) <= limits[service]:
            effective.update(container_id for container_id, _, _ in items)
    return effective


def passwd_home() -> Path:
    """The account's home from the password database, not ``HOME`` (a run's environment may move ``HOME``)."""
    return Path(pwd.getpwuid(os.getuid()).pw_dir)


def _observed_runtime(
    info: Mapping[str, object], endpoint: str, topology: str, binding: PodmanBinding | None = None
) -> str | None:
    """Detect the daemon's runtime before any accounting and require it to match the declared topology (design 2.5).

    A remote endpoint (``None``) is accounted as remote whatever was declared, and a declared ``remote`` accepts any
    daemon, as before. Otherwise the runtime's topology must be the declared one (a Mac VM is ``desktop``, Docker
    Engine on Linux ``native``); an unsupported runtime and one whose overheads are not measured yet are refused.
    """
    detected = runtime_of(binding, info, endpoint)
    if isinstance(detected, RuntimeRefusal):
        raise ResourceError(detected.message, reason=detected.reason)
    if topology == "remote" or detected is None:
        return detected
    if runtime_topology(detected) != topology:
        raise ResourceError("Observed daemon does not match the declared profile topology.")
    if detected not in RUNTIME_OVERHEADS:
        raise ResourceError(
            f"Container runtime {detected} is recognised but its overheads are not measured yet.",
            reason="runtime-unqualified",
        )
    return detected


def _identified_info(docker, deadline: Deadline | None, expected: str | None = None) -> tuple[object, dict]:
    """Read the daemon's ``/info`` and decide whose it is (design 2.2, section 12): on Docker the body's ``ID``; on
    Podman, whose compat ``ID`` changes per call, a fresh verification of the bound machine before the read (the
    engine read's ``dial-stdio`` verifies nothing by itself) and the machine's identity. A machine other than
    ``expected`` is returned before any engine read, with an empty body."""
    if docker.engine == "podman":
        bound = _daemon_identity(docker, deadline)
        if expected is not None and bound != expected:
            return bound, {}
        return bound, _engine_read(docker, "/info", deadline).body
    body = _engine_read(docker, "/info", deadline).body
    return body.get("ID"), body


@_attributed("engine")
def observe_capacity(
    docker,
    topology: str,
    owned_ids: set[str] | frozenset[str] = frozenset(),
    *,
    mutation_plan: dict | None = None,
    mutation_limits: dict[str, int] | None = None,
    inventory: list[dict] | None = None,
    deadline: Deadline | None = None,
) -> tuple[str, CapacitySnapshot]:
    """Collect daemon, host and finite external commitments before admission.

    The caller supplies the profile's verified daemon relationship. Every other
    running container needs a positive hard memory limit; sampled usage never
    substitutes for that ceiling. Native Linux also records finite enclosing
    cgroup-v2 limits and only the Docker commitments that share each ancestor.

    ``inventory`` is a container inventory the caller already read (before it read other records that explain some
    of those containers); without it the inventory is read here.
    """
    if (
        topology not in ("desktop", "native", "remote")
        or not isinstance(owned_ids, (set, frozenset))
        or (mutation_plan is None) != (mutation_limits is None)
    ):
        raise ResourceError("Invalid declared daemon topology or owned container set.")
    if any(not isinstance(item, str) or not re.fullmatch(r"[0-9a-f]{64}", item) for item in owned_ids):
        raise ResourceError("Owned capacity inputs require exact container IDs.")
    podman = docker.engine == "podman"
    daemon_id, body = _identified_info(docker, deadline)
    if (
        not isinstance(daemon_id, str)
        or not NAME.fullmatch(daemon_id)
        or type(body.get("MemTotal")) is not int
        or body["MemTotal"] <= 0
        or type(body.get("NCPU")) is not int
        or body["NCPU"] <= 0
        or body.get("OSType") != "linux"
        or str(body.get("CgroupVersion")) != "2"
    ):
        raise ResourceError("Docker capacity/cgroup capabilities are incomplete.")
    endpoint = docker.binding.endpoint if podman else _daemon_endpoint(docker, daemon_id, deadline)
    runtime = _observed_runtime(body, endpoint, topology, docker.binding)
    if runtime is None:
        topology = "remote"  # a remote endpoint is accounted as remote from any client (design 2.5)
    containers = docker.objects("container", deadline=deadline) if inventory is None else inventory
    capacity_owned_ids = set(owned_ids)
    if mutation_plan is not None and mutation_limits is not None:
        capacity_owned_ids = _mutation_capacity_ids(
            containers,
            capacity_owned_ids,
            mutation_plan,
            mutation_limits,
        )
    observed_ids = set()
    external_limits = []
    for item in containers:
        try:
            container_id = item["Id"]
            running = item["State"]["Running"]
            limit = item["HostConfig"]["Memory"]
        except (KeyError, TypeError) as error:
            raise ResourceError("Docker container capacity evidence is incomplete.") from error
        if (
            not isinstance(container_id, str)
            or not re.fullmatch(r"[0-9a-f]{64}", container_id)
            or type(running) is not bool
            or type(limit) is not int
            or limit < 0
            or container_id in observed_ids
        ):
            raise ResourceError("Docker container capacity evidence is contradictory.")
        observed_ids.add(container_id)
        if running and container_id not in capacity_owned_ids:
            external_limits.append(limit or None)
    if not capacity_owned_ids <= observed_ids:
        raise ResourceError("An owned container is absent from the complete daemon inventory.")
    host = _host_read(NativeClock(), deadline)
    host_cpus = os.cpu_count()
    if type(host_cpus) is not int or host_cpus <= 0:
        raise ResourceError("Host CPU capacity is unavailable.")

    def is_stopped(container_id: str) -> bool:
        """Whether one container holds no process now (the job read asks once its inventory process seemed gone)."""
        return _container_stopped(docker.inspect_exact(ExactInspection("container", container_id), deadline=deadline))

    jobs = _job_read(containers, capacity_owned_ids, is_stopped) if topology == "native" else ()
    return daemon_id, CapacitySnapshot(
        body["MemTotal"],
        body["NCPU"],
        host.physical_bytes,
        host_cpus,
        topology,
        tuple(external_limits),
        jobs,
        runtime=runtime,
    )


def assess_capacity(budget: PhaseBudget, observed: CapacitySnapshot) -> CapacityDecision:
    """Apply separate guest, physical-host and job-cgroup commitment inequalities.

    Inputs must come from a calibrated phase and verified topology/limits. READY
    here is only static fit, never an execution grant or a native-pressure result.
    Finite external pressure waits; unknown evidence or an impossible base refuses.
    Job inputs include only Docker commitments proved to share that ancestor.
    """
    if observed.runtime is not None and observed.runtime not in RUNTIME_OVERHEADS:
        return CapacityDecision(CapacityState.REFUSED, "runtime-unqualified")
    guest_reserve, host_side = REMOTE_OVERHEADS if observed.runtime is None else RUNTIME_OVERHEADS[observed.runtime]
    host_reserve = observed.host_reserve_bytes  # the developer-machine default unless the account declared one
    # Reuse the conservative existing reserve inside a finite job boundary; no
    # smaller job allowance is inferred from unused host RAM or a passing sample.
    job_reserve = host_reserve
    for value in (budget.container_bytes, budget.host_process_bytes):
        if type(value) is not int or value < 0:
            raise ResourceError("Phase memory commitments must be nonnegative byte counts.")
    for value in (
        budget.daemon_cpus,
        budget.host_threads,
        observed.daemon_memory_bytes,
        observed.daemon_cpus,
        observed.host_memory_bytes,
        observed.host_cpus,
    ):
        if type(value) is not int or value <= 0:
            raise ResourceError("Effective capacity and CPU prerequisites must be positive integers.")
    if observed.topology not in ("desktop", "native", "remote"):
        return CapacityDecision(CapacityState.REFUSED, "unknown-daemon-relationship")
    if observed.daemon_cpus < budget.daemon_cpus or observed.host_cpus < budget.host_threads:
        return CapacityDecision(CapacityState.REFUSED, "insufficient-cpu")
    if any(type(limit) is not int or limit <= 0 for limit in observed.external_limits):
        return CapacityDecision(CapacityState.REFUSED, UNKNOWN_EXTERNAL_CAPACITY)
    if observed.job_limits is None:
        return CapacityDecision(CapacityState.REFUSED, "unknown-job-containment")
    for job in observed.job_limits:
        if type(job.memory_bytes) is not int or job.memory_bytes <= 0:
            raise ResourceError("Job memory ceiling is invalid.")
        managed = job.managed_docker_bytes
        external_docker = job.external_docker_bytes
        if managed is None or external_docker is None or managed < 0 or external_docker < 0:
            return CapacityDecision(CapacityState.REFUSED, "unknown-job-containment")
    guest_base = budget.container_bytes + guest_reserve
    host_base = budget.host_process_bytes + host_reserve
    if observed.topology != "remote":
        host_base += guest_base + host_side
    external = sum(cast(tuple[int, ...], observed.external_limits))
    guest = guest_base + external
    host = host_base + (external if observed.topology != "remote" else 0)
    if guest_base > observed.daemon_memory_bytes or host_base > observed.host_memory_bytes:
        return CapacityDecision(CapacityState.REFUSED, "unsupported-base-capacity", guest, host)
    for job in observed.job_limits:
        managed = job.managed_docker_bytes
        if managed is None:
            return CapacityDecision(CapacityState.REFUSED, "unknown-job-containment")
        if budget.host_process_bytes + job_reserve + managed > job.memory_bytes:
            return CapacityDecision(CapacityState.REFUSED, "job-cgroup-base-capacity", guest, host)
    if guest > observed.daemon_memory_bytes or host > observed.host_memory_bytes:
        return CapacityDecision(CapacityState.WAITING, "external-capacity", guest, host)
    for job in observed.job_limits:
        managed = job.managed_docker_bytes
        external_docker = job.external_docker_bytes
        if managed is None or external_docker is None:
            return CapacityDecision(CapacityState.REFUSED, "unknown-job-containment")
        if budget.host_process_bytes + job_reserve + managed + external_docker > job.memory_bytes:
            return CapacityDecision(CapacityState.WAITING, "job-cgroup-external-capacity", guest, host)
    return CapacityDecision(CapacityState.READY, "static-capacity", guest, host)


@dataclass
class HostMemoryObservation:
    """Carry one monotonic host-memory and swap-pressure sample."""

    origin: str
    tick_ns: int
    physical_bytes: int
    headroom_bytes: int
    swap_used_bytes: int
    swapout_bytes: int
    page_size: int
    raw: dict[str, int]

    @property
    def swap_total_bytes(self) -> int | None:
        """The host swap total from the native counter (Darwin ``swap_total_bytes``, Linux ``SwapTotal``), if known."""
        total = self.raw.get("swap_total_bytes", self.raw.get("SwapTotal"))
        return total if type(total) is int else None


class _VmStatistics64(ctypes.Structure):
    _fields_ = (
        [(name, ctypes.c_uint32) for name in ("free", "active", "inactive", "wire")]
        + [
            (name, ctypes.c_uint64)
            for name in (
                "zero_fill",
                "reactivations",
                "pageins",
                "pageouts",
                "faults",
                "cow_faults",
                "lookups",
                "hits",
                "purges",
            )
        ]
        + [("purgeable", ctypes.c_uint32), ("speculative", ctypes.c_uint32)]
        + [(name, ctypes.c_uint64) for name in ("decompressions", "compressions", "swapins", "swapouts")]
        + [(name, ctypes.c_uint32) for name in ("compressor", "throttled", "external", "internal")]
        + [("total_uncompressed", ctypes.c_uint64)]
    )


class _SwapUsage(ctypes.Structure):
    _fields_ = [(name, ctypes.c_uint64) for name in ("total", "available", "used")] + [
        ("page_size", ctypes.c_uint32),
        ("encrypted", ctypes.c_int32),
    ]


def _sysctl_value(lib, name: bytes, value):
    lib.sysctlbyname.argtypes = [
        ctypes.c_char_p,
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_size_t),
        ctypes.c_void_p,
        ctypes.c_size_t,
    ]
    lib.sysctlbyname.restype = ctypes.c_int
    size = ctypes.c_size_t(ctypes.sizeof(value))
    if lib.sysctlbyname(name, ctypes.byref(value), ctypes.byref(size), None, 0) or size.value != ctypes.sizeof(value):
        raise ResourceError(
            "Native memory sysctl has an unavailable or unsupported ABI.",
            reason="telemetry-unreadable",
            observer="host",
        )
    return value


def _darwin_memory_sample() -> tuple[int, int, int, int, int, dict[str, int]]:
    lib = _darwin_library()
    physical = _sysctl_value(lib, b"hw.memsize", ctypes.c_uint64()).value
    swap = _sysctl_value(lib, b"vm.swapusage", _SwapUsage())
    lib.mach_host_self.argtypes = []
    lib.mach_host_self.restype = ctypes.c_uint32
    lib.host_statistics64.argtypes = [ctypes.c_uint32, ctypes.c_int, ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint32)]
    lib.host_statistics64.restype = ctypes.c_int
    lib.host_page_size.argtypes = [ctypes.c_uint32, ctypes.POINTER(ctypes.c_size_t)]
    lib.host_page_size.restype = ctypes.c_int
    lib.mach_port_deallocate.argtypes = [ctypes.c_uint32, ctypes.c_uint32]
    lib.mach_port_deallocate.restype = ctypes.c_int
    host = lib.mach_host_self()
    if not host:
        raise ResourceError("Native host port is unavailable.", reason="telemetry-unreadable", observer="host")
    try:
        vm = _VmStatistics64()
        count = ctypes.c_uint32(ctypes.sizeof(vm) // ctypes.sizeof(ctypes.c_int32))
        page = ctypes.c_size_t()
        if lib.host_statistics64(host, 4, ctypes.byref(vm), ctypes.byref(count)) or count.value * 4 != ctypes.sizeof(
            vm
        ):
            raise ResourceError(
                "Native host VM statistics have an unsupported ABI.", reason="telemetry-unreadable", observer="host"
            )
        if lib.host_page_size(host, ctypes.byref(page)) or not page.value or not physical:
            raise ResourceError(
                "Native host page size or physical RAM is unavailable.", reason="telemetry-unreadable", observer="host"
            )
        headroom = max(0, physical - page.value * (vm.wire + vm.internal + vm.compressor))
        raw = {name: getattr(vm, name) for name, _ in vm._fields_}
        raw.update(swap_total_bytes=swap.total, swap_available_bytes=swap.available, swap_used_bytes=swap.used)
        return physical, headroom, swap.used, vm.swapouts * page.value, page.value, raw
    finally:
        task = ctypes.c_uint32.in_dll(lib, "mach_task_self_").value
        if lib.mach_port_deallocate(task, host):
            error = ResourceError("Native host-port release failed.", reason="telemetry-unreadable", observer="host")
            primary = sys.exc_info()[1]
            if primary is not None:
                primary.add_note(str(error))
            else:
                raise error


def _linux_memory_sample() -> tuple[int, int, int, int, int, dict[str, int]]:
    required = {"MemTotal", "MemAvailable", "SwapTotal", "SwapFree"}
    values = {}
    for line in Path("/proc/meminfo").read_text().splitlines():
        name, separator, tail = line.partition(":")
        if name not in required:
            continue
        words = tail.split()
        if not separator or name in values or len(words) != 2 or words[1] != "kB" or not words[0].isdigit():
            raise ResourceError("Malformed Linux memory observation.", reason="telemetry-unreadable", observer="host")
        values[name] = int(words[0]) * 1024
    swapouts = [line.split() for line in Path("/proc/vmstat").read_text().splitlines() if line.startswith("pswpout ")]
    if set(values) != required or len(swapouts) != 1 or len(swapouts[0]) != 2 or not swapouts[0][1].isdigit():
        raise ResourceError("Incomplete Linux memory/swap observation.", reason="telemetry-unreadable", observer="host")
    page = os.sysconf("SC_PAGE_SIZE")
    if page <= 0 or values["MemTotal"] <= 0 or values["SwapFree"] > values["SwapTotal"]:
        raise ResourceError("Contradictory Linux memory observation.", reason="telemetry-unreadable", observer="host")
    values["pswpout_pages"] = int(swapouts[0][1])
    return (
        values["MemTotal"],
        values["MemAvailable"],
        values["SwapTotal"] - values["SwapFree"],
        values["pswpout_pages"] * page,
        page,
        values,
    )


def observe_host_memory(clock: NativeClock, *, deadline: Deadline | None = None) -> HostMemoryObservation:
    """Read native physical-memory and swap counters without inspecting other processes.

    Headroom is the specified diagnostic index, not free RAM or an incremental
    reservation. Preserve raw counters and fail on missing native observations. The native calls cannot be
    interrupted, so a ``deadline`` is checked before and after the sample: an exhausted budget is never a complete
    sample.
    """
    if deadline is not None and not deadline.remaining_ns(clock):
        raise ResourceError(
            "Host sample deadline expired before the sample.", reason="observation-deadline", observer="host"
        )
    clock.read()
    try:
        failpoint("observation-host-sample")
        physical, headroom, swap, swapout, page, raw = (
            _darwin_memory_sample() if sys.platform == "darwin" else _linux_memory_sample()
        )
    except (OSError, ValueError) as error:
        raise ResourceError(
            "Native host memory telemetry is unavailable.", reason="telemetry-unreadable", observer="host"
        ) from error
    if deadline is not None and not deadline.remaining_ns(clock):
        raise ResourceError(
            "Host sample deadline expired during the sample.", reason="observation-deadline", observer="host"
        )
    return HostMemoryObservation(clock.origin, clock.read(), physical, headroom, swap, swapout, page, raw)


_WINDOW_NS = 30_000_000_000
_WINDOW_SAMPLES = 64


class _SampleWindow:
    """Guardian-memory samples: every sample within 30 s of the newest, plus the newest older one; at most 64.

    Retention assumes ascending ticks, which native reads guarantee. A test script with decreasing offsets can add an
    out-of-order tick; retention may then keep one extra sample or drop an older one, which only changes which
    continuous candidate ``select_window_start`` finds (P23).
    """

    def __init__(self) -> None:
        self._samples: list[HostMemoryObservation] = []

    def add(self, sample: HostMemoryObservation) -> None:
        """Keep ``sample`` and drop what can no longer be the newest sample at least 30 s old."""
        self._samples.append(sample)
        cutoff = sample.tick_ns - _WINDOW_NS
        while len(self._samples) >= 2 and self._samples[1].tick_ns <= cutoff:
            self._samples.pop(0)
        del self._samples[:-_WINDOW_SAMPLES]

    def clear(self) -> None:
        """Forget every sample: a pass that yields no sample falls back to a full post-reservation window."""
        self._samples.clear()

    def samples(self) -> tuple[HostMemoryObservation, ...]:
        """Return the kept samples, oldest first."""
        return tuple(self._samples)


def select_window_start(
    queued: Sequence[HostMemoryObservation],
    post: Sequence[HostMemoryObservation],
    current: HostMemoryObservation,
    *,
    window_ns: int = _WINDOW_NS,
) -> HostMemoryObservation | None:
    """Return the newest candidate at least ``window_ns`` (30 s) older than ``current`` (design 3.11, DD6), or None.

    Candidates are the post-reservation samples plus the queued samples that form a continuous chain back from the
    first post-reservation sample: walking from the newest queued sample to the oldest, each must have the same origin
    and physical size as the newer one and a tick and swap-out not after it (pairwise, P17). The first break ends the
    chain, so a queued anomaly, and everything before it, can only fall back to W0. The pressure rule itself
    (``assess_pressure``) is unchanged.
    """
    if not post:
        return None
    continuous: list[HostMemoryObservation] = []
    newer = post[0]
    for sample in reversed(queued):
        if (
            sample.origin != newer.origin
            or sample.physical_bytes != newer.physical_bytes
            or sample.tick_ns > newer.tick_ns
            or sample.swapout_bytes > newer.swapout_bytes
        ):
            break
        continuous.append(sample)
        newer = sample
    candidates = [sample for sample in (*continuous, *post) if current.tick_ns - sample.tick_ns >= window_ns]
    return max(candidates, key=lambda sample: sample.tick_ns, default=None)


def assess_pressure(
    current: HostMemoryObservation,
    window_start: HostMemoryObservation | None,
    *,
    granted: bool = False,
    admission_baseline: HostMemoryObservation | None = None,
    phase_baseline: HostMemoryObservation | None = None,
    host_rss_bytes: int | None = None,
    host_envelope_bytes: int | None = None,
    window_ns: int = _WINDOW_NS,
    growth_allowance_bytes: int = 0,
    shared_gate: bool = False,
) -> CapacityDecision:
    """Wait on initial pressure, but fail an admitted run without retry or baseline reset.

    A complete measured window (``window_ns``, 30 s outside a private test binding) is required; admission swap
    history survives phase transitions. RSS is checked in its own metric, never added to H.

    A granted run that has shared (``shared_gate``) is not failed by the headroom floor or the swap-out rate, which
    keep gating joiners' admission; its swap-growth threshold grows by the allowance (design 2.5). The defaults are a
    run that never shared.

    A decline of the host's headroom since a granted run's phase began is not a breach (owner, 2026-11-01): it makes
    later runs wait (the headroom floor gates every admission). A granted run ends for its own overuse (its process
    envelope here, its containers' limits elsewhere) and for host-wide pressure, whoever caused it: the floor and the
    swap-out rate unless it has shared, and swap growth from its admission.
    """
    gib = 1024**3
    failure = CapacityState.REFUSED if granted else CapacityState.WAITING
    baselines = [sample for sample in (window_start, admission_baseline, phase_baseline) if sample is not None]
    for sample in [current, *baselines]:
        if (
            any(
                type(value) is not int or value < 0
                for value in (
                    sample.tick_ns,
                    sample.physical_bytes,
                    sample.headroom_bytes,
                    sample.swap_used_bytes,
                    sample.swapout_bytes,
                )
            )
            or sample.physical_bytes <= 0
            or sample.headroom_bytes > sample.physical_bytes
        ):
            return CapacityDecision(CapacityState.REFUSED, "invalid-memory-observation")
        if (
            sample.origin != current.origin
            or sample.physical_bytes != current.physical_bytes
            or (sample.tick_ns > current.tick_ns or sample.swapout_bytes > current.swapout_bytes)
        ):
            return CapacityDecision(CapacityState.REFUSED, "telemetry-discontinuity")
    if window_start is None or current.tick_ns - window_start.tick_ns < window_ns:
        return CapacityDecision(failure, "incomplete-pressure-window")
    gated = granted and shared_gate
    if current.headroom_bytes < 3 * gib // 2 and not gated:
        return CapacityDecision(failure, "native-headroom")
    elapsed = current.tick_ns - window_start.tick_ns
    swapped = current.swapout_bytes - window_start.swapout_bytes
    if swapped * 1_000_000_000 > 128 * 1024**2 * elapsed and not gated:
        return CapacityDecision(failure, "swapout-rate")
    if not granted:
        return CapacityDecision(CapacityState.READY, "native-pressure")
    if admission_baseline is None or phase_baseline is None:
        return CapacityDecision(CapacityState.REFUSED, "incomplete-admission-baseline")
    if host_rss_bytes is None or host_envelope_bytes is None or host_rss_bytes < 0 or host_envelope_bytes < 0:
        return CapacityDecision(CapacityState.REFUSED, "incomplete-admission-baseline")
    if current.swap_used_bytes - admission_baseline.swap_used_bytes > 3 * gib + growth_allowance_bytes:
        return CapacityDecision(CapacityState.REFUSED, "admission-swap-growth")
    if host_rss_bytes > host_envelope_bytes:
        return CapacityDecision(CapacityState.REFUSED, "host-process-envelope")
    return CapacityDecision(CapacityState.READY, "native-pressure")


def _register_worker_cleanup() -> None:
    """Remove this worker's own ``docker-test-toolkit-recover-*`` directory (its logs) when it exits."""
    temporary = Path(os.environ.get("TMPDIR", ""))
    if temporary.name.startswith("docker-test-toolkit-recover-") and temporary.is_dir() and not temporary.is_symlink():
        atexit.register(shutil.rmtree, temporary, ignore_errors=True)


def _require_own_attempt(context: RunContext, attempt: str, directory: Path) -> None:
    """Refuse ``claim-held`` unless the claim predicate lets this worker proceed on its entry; reads, never writes."""
    clock = NativeClock()
    try:
        raw = json.loads((directory / "queue.json").read_text())
        takeover = account_queue.to_logical(raw).entries[context.run_id].takeover
        own = claim_eligible(
            takeover,
            caller_attempt=attempt,
            claimant_state=_claim_observation(takeover),
            now_tick_ns=clock.read(),
            origin=clock.origin,
        )
    except (OSError, RecursionError, ValueError, KeyError, TypeError, ResourceError):
        own = False
    if not own:
        raise ResourceError("This worker's takeover attempt is absent, replaced or expired.", reason="claim-held")


def _run_takeover_worker(
    payload: str,
    attempt: str,
    test_directory: Path | None,
) -> int:
    """Run one takeover worker (design 3.10): validate, confirm its attempt read-only, recover.

    The context is checked for shape only, so a blocked or legacy record yields a named refusal instead of a loader
    error. Any installed version of the account's state format may recover a run (installed-once design I5).
    """
    if not UUID.fullmatch(attempt):
        raise ResourceError("The takeover attempt is not a UUID.")
    _register_worker_cleanup()
    context = _context_from_payload(payload, validate_record=False)
    directory = account_directory() if test_directory is None else test_directory.resolve()
    _require_own_attempt(context, attempt, directory)
    recover_run(
        context,
        drain_live_consumers=False,
        takeover_attempt=attempt,
        _test_directory=test_directory,
    )
    return 0


def main(argv=None) -> int:
    """Run one internal child process: the guardian, a Python consumer child or a takeover worker."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["_guardian", "_python_child", "_recover"])
    parser.add_argument("--context")
    parser.add_argument("--request", type=Path)
    parser.add_argument("--attempt")
    parser.add_argument("--test-directory", type=Path)
    args = parser.parse_args(argv)
    if sys.version_info < (3, 12):
        print("Python 3.12+ is required.", file=sys.stderr)
        return 1
    try:
        if args.command == "_guardian":
            if args.context is None or args.request is not None:
                parser.error("_guardian requires only its fixed context locator")
            context = _context_from_payload(args.context)
            data = _load_run(context)
            attempt = data["guardian_attempt"]
            if not isinstance(attempt, str) or not UUID.fullmatch(attempt):
                raise ResourceError("Guardian startup attempt is unavailable.")
            try:
                _guardian_loop(context, attempt, args.test_directory)
            except (OSError, ResourceError):
                _record_guardian_failure(context, "guardian-error")
                raise
        elif args.command == "_recover":
            if args.context is None or args.request is not None or args.attempt is None:
                parser.error("_recover requires only its fixed context locator and attempt")
            return _run_takeover_worker(args.context, args.attempt, args.test_directory)
        else:
            if args.context is not None or args.request is None:
                parser.error("_python_child requires only its fixed private request")
            return _run_python_child(args.request.resolve(strict=True))
    except (OSError, ResourceError) as error:
        print(f"Lifecycle protocol unavailable: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
