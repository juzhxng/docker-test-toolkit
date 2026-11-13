"""Closed typed values accepted by the public facade."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping

from docker_test_toolkit.domain.errors import InvalidInput, ToolkitError
from docker_test_toolkit.domain.policy import NAME, VERSION, require_name

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_IMAGE_ID = re.compile(r"sha256:[0-9a-f]{64}\Z")
TOOLKIT_VERSION = "0.8.0"
SHARED_QUEUE_MIN_VERSION = "0.8.0"  # 0.8.0 changed the admission sums: no run holds beside an older version (C4)
SUPPORTED_PROTOCOLS = MappingProxyType(
    {
        "batch": 2,
        "cli": 1,
        "queue": 2,
        "readiness": 3,
        "receipt": 1,
        "registry": 2,
        "run": 1,
    }
)
_RELATIONSHIPS = frozenset({"inspect", "use_image", "connect_owned_container"})


@dataclass(frozen=True)
class ArtifactIdentity:
    """Bind a release version, exact artifact digest, and protocol set."""

    version: str
    sha256: str
    protocols: Mapping[str, int]

    def __post_init__(self) -> None:
        if not isinstance(self.version, str) or not VERSION.fullmatch(self.version):
            raise ToolkitError("Invalid artifact version.")
        if not isinstance(self.sha256, str) or not _SHA256.fullmatch(self.sha256):
            raise ToolkitError("Invalid artifact digest; expected lowercase SHA-256.")
        if (
            type(self.protocols) is not dict
            or not self.protocols
            or any(
                not isinstance(name, str) or NAME.fullmatch(name) is None or type(value) is not int or value < 1
                for name, value in self.protocols.items()
            )
        ):
            raise ToolkitError("Artifact protocols must be bounded names with positive integer versions.")
        object.__setattr__(self, "protocols", MappingProxyType(dict(self.protocols)))


def _require_batch_name(value: str, label: str) -> str:
    try:
        return require_name(value, label)
    except ToolkitError as error:
        raise InvalidInput(str(error)) from error


def _require_closure(value: tuple[str, ...]) -> tuple[str, ...]:
    if type(value) is not tuple or any(not isinstance(item, str) or not NAME.fullmatch(item) for item in value):
        raise InvalidInput("Batch closure must contain bounded public names.")
    if len(set(value)) != len(value):
        raise InvalidInput("Batch closure names must be unique.")
    return value


@dataclass(frozen=True)
class BatchItem:
    """Declare one opaque adapter item and its canonical service closure."""

    item_id: str
    closure: tuple[str, ...]

    def __post_init__(self) -> None:
        _require_batch_name(self.item_id, "batch item ID")
        _require_closure(self.closure)


@dataclass(frozen=True)
class BatchSegment:
    """Declare one ordered nonempty item segment and optional trailing boundary."""

    segment_id: str
    items: tuple[BatchItem, ...]
    trailing_boundary_id: str | None = None
    requires_previous_success: bool = False

    def __post_init__(self) -> None:
        _require_batch_name(self.segment_id, "batch segment ID")
        if (
            type(self.items) is not tuple
            or not self.items
            or any(not isinstance(item, BatchItem) for item in self.items)
        ):
            raise InvalidInput("Batch segment items must be a nonempty tuple of BatchItem values.")
        if len({item.item_id for item in self.items}) != len(self.items):
            raise InvalidInput("Batch segment item IDs must be unique.")
        if self.trailing_boundary_id is not None:
            _require_batch_name(self.trailing_boundary_id, "batch boundary ID")
        if type(self.requires_previous_success) is not bool:
            raise InvalidInput("Batch previous-success policy must be boolean.")


@dataclass(frozen=True)
class BatchPlan:
    """Declare one canonical project and its ordered finite batch segments."""

    project_root: Path
    artifact_identity: ArtifactIdentity | None = None  # accepted and ignored, like RunSpec.artifact
    segments: tuple[BatchSegment, ...] = ()

    def __post_init__(self) -> None:
        try:
            root = Path(self.project_root).resolve(strict=True)
        except (OSError, TypeError) as error:
            raise InvalidInput("Batch project root must be an existing canonical directory.") from error
        if not root.is_dir():
            raise InvalidInput("Batch project root must be an existing canonical directory.")
        if self.artifact_identity is not None and not isinstance(self.artifact_identity, ArtifactIdentity):
            raise InvalidInput("Batch plan artifact identity must be an ArtifactIdentity or None; it is ignored.")
        if (
            type(self.segments) is not tuple
            or not self.segments
            or any(not isinstance(segment, BatchSegment) for segment in self.segments)
        ):
            raise InvalidInput("Batch plan segments must be a nonempty tuple of BatchSegment values.")
        if self.segments[0].requires_previous_success:
            raise InvalidInput("Batch plan first segment cannot require previous success.")
        segment_ids = [segment.segment_id for segment in self.segments]
        boundary_ids = [
            segment.trailing_boundary_id for segment in self.segments if segment.trailing_boundary_id is not None
        ]
        item_ids = [item.item_id for segment in self.segments for item in segment.items]
        if len(set(segment_ids)) != len(segment_ids):
            raise InvalidInput("Batch plan requires unique segment IDs.")
        if len(set(boundary_ids)) != len(boundary_ids):
            raise InvalidInput("Batch plan requires unique boundary IDs.")
        if len(set(item_ids)) != len(item_ids):
            raise InvalidInput("Batch plan requires unique item IDs.")
        all_ids = [*segment_ids, *boundary_ids, *item_ids]
        if len(set(all_ids)) != len(all_ids):
            raise InvalidInput("Batch plan requires globally unique item, segment, and boundary IDs.")
        closure_orders: dict[frozenset[str], tuple[str, ...]] = {}
        for item in (item for segment in self.segments for item in segment.items):
            key = frozenset(item.closure)
            existing = closure_orders.setdefault(key, item.closure)
            if existing != item.closure:
                raise InvalidInput("Batch closures for one service set must use one canonical order.")
        object.__setattr__(self, "project_root", root)


@dataclass(frozen=True)
class PlannedBatch:
    """Expose one exact-closure group selected by the finite coordinator."""

    batch_id: str
    segment_id: str
    item_ids: tuple[str, ...]
    closure: tuple[str, ...]
    trailing_boundary_id: str | None


@dataclass(frozen=True)
class BatchRunEvidence:
    """Carry bounded ordinary-run identity, status, and stable error categories."""

    run_id: str | None
    run_status: Mapping[str, object] | None
    category: str | None
    cleanup_category: str | None

    def __post_init__(self) -> None:
        if self.run_id is not None:
            _require_batch_name(self.run_id, "batch run ID")
        if self.category not in {None, "incompatible_state", "contention", "infrastructure_failure"}:
            raise InvalidInput("Batch run evidence has an invalid primary category.")
        if self.cleanup_category not in {None, "incomplete_cleanup"}:
            raise InvalidInput("Batch run evidence has an invalid cleanup category.")
        if self.run_status is None:
            return
        fields = {
            "run_id",
            "state",
            "admission",
            "generation",
            "revision",
            "ticket",
            "test_outcome",
            "cleanup_outcome",
        }
        if not isinstance(self.run_status, Mapping) or not fields <= self.run_status.keys():
            raise InvalidInput("Batch run status must contain the ordinary RunStatus fields.")
        status = {key: value for key, value in self.run_status.items() if key in fields}  # newer fields dropped
        if (
            not isinstance(status["run_id"], str)
            or not NAME.fullmatch(status["run_id"])
            or not isinstance(status["state"], str)
            or not NAME.fullmatch(status["state"])
            or not isinstance(status["admission"], str)
            or not NAME.fullmatch(status["admission"])
            or type(status["generation"]) is not int
            or status["generation"] < 0
            or type(status["revision"]) is not int
            or status["revision"] < 0
            or (status["ticket"] is not None and (type(status["ticket"]) is not int or status["ticket"] < 0))
            or status["test_outcome"] not in {None, "passed", "failed", "cancelled"}
            or status["cleanup_outcome"] not in {None, "pending", "complete", "incomplete"}
        ):
            raise InvalidInput("Batch run status fields are invalid.")
        object.__setattr__(self, "run_status", MappingProxyType(status))


@dataclass(frozen=True)
class BatchExecutionReport:
    """Report one native batch primary result plus ordinary lifecycle evidence."""

    primary: str
    run_evidence: BatchRunEvidence | None

    def __post_init__(self) -> None:
        if self.primary not in {"passed", "test_failed", "infrastructure_failed"}:
            raise InvalidInput("Batch report primary is invalid.")
        if self.run_evidence is not None and not isinstance(self.run_evidence, BatchRunEvidence):
            raise InvalidInput("Batch report run evidence is invalid.")


@dataclass(frozen=True)
class BatchResult:
    """Record one planned batch's executed or closed non-executed outcome."""

    batch_id: str
    segment_id: str
    item_ids: tuple[str, ...]
    closure: tuple[str, ...]
    outcome: str
    report: BatchExecutionReport | None
    reason: str | None


@dataclass(frozen=True)
class BoundaryResult:
    """Record one declared adapter-owned trailing boundary outcome."""

    boundary_id: str
    segment_id: str
    outcome: str
    reason: str | None


@dataclass(frozen=True)
class SegmentResult:
    """Record one segment's complete, failed, skipped, or not-run outcome."""

    segment_id: str
    outcome: str
    reason: str | None


@dataclass(frozen=True)
class BatchAggregate:
    """Return one complete ordered finite-batch result without durable state."""

    exit_code: int
    outcome: str
    segments: tuple[SegmentResult, ...]
    batches: tuple[BatchResult, ...]
    boundaries: tuple[BoundaryResult, ...]


@dataclass(frozen=True)
class PhaseBudget:
    """Declare finite capacity needed during one named run phase."""

    container_bytes: int
    host_process_bytes: int
    daemon_cpus: int
    host_threads: int

    def __post_init__(self) -> None:
        values = (self.container_bytes, self.host_process_bytes, self.daemon_cpus, self.host_threads)
        if any(type(value) is not int for value in values):
            raise ToolkitError("Phase commitments must be integer limits.")
        if self.container_bytes < 0 or self.host_process_bytes < 0 or self.daemon_cpus < 1 or self.host_threads < 1:
            raise ToolkitError("Phase commitments require nonnegative bytes and positive CPU/thread limits.")


@dataclass(frozen=True)
class Endpoint:
    """Describe one exact endpoint published by an entered run."""

    service: str
    protocol: str
    host: str
    port: int
    container_id: str
    container_port: int
    audience: str
    namespace: str | None
    generation: int
    revision: int


@dataclass(frozen=True)
class ExternalDockerResource:
    """Declare a non-owned Docker object and its only permitted relationship."""

    kind: str
    identifier: str
    name: str
    relationship: str

    def __post_init__(self) -> None:
        patterns = {"container": _SHA256, "image": _IMAGE_ID, "network": _SHA256}
        relationships = {
            "inspect": frozenset(patterns),
            "use_image": frozenset({"image"}),
            "connect_owned_container": frozenset({"network"}),
        }
        if (
            self.kind not in patterns
            or not isinstance(self.identifier, str)
            or not patterns[self.kind].fullmatch(self.identifier)
        ):
            raise ToolkitError("Invalid exact external resource identity.")
        require_name(self.name, "external resource name")
        if self.relationship not in _RELATIONSHIPS or self.kind not in relationships[self.relationship]:
            raise ToolkitError("Invalid external resource relationship.")


@dataclass(frozen=True)
class RunSpec:
    """Declare one deterministic local test run before infrastructure mutation."""

    project_root: Path
    entrypoint: str
    purpose: str
    services: tuple[str, ...]
    closure: tuple[str, ...]
    profile: str
    phases: Mapping[str, PhaseBudget]
    initial_phase: str | None
    artifact: ArtifactIdentity | None = None  # accepted and ignored: the run records the running toolkit's identity
    external_resources: tuple[ExternalDockerResource, ...] = ()
    retain: bool = False
    wait_seconds: float = 14_400
    capacity: int = 1

    def __post_init__(self) -> None:
        root = Path(self.project_root).resolve(strict=True)
        if not root.is_dir():
            raise ToolkitError("Project root is not a directory.")
        for value, label in (
            (self.entrypoint, "entrypoint"),
            (self.purpose, "purpose"),
            (self.profile, "profile"),
        ):
            require_name(value, label)
        for names in (self.services, self.closure):
            if type(names) is not tuple or any(not isinstance(name, str) or not NAME.fullmatch(name) for name in names):
                raise ToolkitError("Services and closure must contain bounded names.")
            if len(set(names)) != len(names):
                raise ToolkitError("Services and closure must contain unique names.")
        if not set(self.services) <= set(self.closure):
            raise ToolkitError("Required services are absent from the declared closure.")
        if type(self.phases) is not dict or any(
            not isinstance(name, str) or not NAME.fullmatch(name) or not isinstance(budget, PhaseBudget)
            for name, budget in self.phases.items()
        ):
            raise ToolkitError("Run phases must be a named mapping of PhaseBudget values.")
        if bool(self.phases) != (self.initial_phase is not None) or (
            self.initial_phase is not None and self.initial_phase not in self.phases
        ):
            raise ToolkitError("Initial phase is absent from declared budgets.")
        if type(self.external_resources) is not tuple or any(
            not isinstance(resource, ExternalDockerResource) for resource in self.external_resources
        ):
            raise ToolkitError("External resources must be an explicit tuple.")
        identities = [(resource.kind, resource.identifier) for resource in self.external_resources]
        if len(set(identities)) != len(identities):
            raise ToolkitError("External resource identities must be unique.")
        if type(self.retain) is not bool or isinstance(self.wait_seconds, bool) or self.wait_seconds <= 0:
            raise ToolkitError("Invalid retention flag or admission budget.")
        if type(self.capacity) is not int:
            raise ToolkitError("Run capacity must be an integer; its value is ignored.")
        if self.artifact is not None and not isinstance(self.artifact, ArtifactIdentity):
            raise ToolkitError("Run artifact must be an ArtifactIdentity or None; its value is ignored.")
        object.__setattr__(self, "project_root", root)
        object.__setattr__(self, "phases", MappingProxyType(dict(self.phases)))


class ReadinessCode(str, Enum):
    """Name every closed lifecycle readiness success and failure state."""

    DOCKER_CLIENT_READY = "docker-client-ready"
    DOCKER_CLIENT_UNAVAILABLE = "docker-client-unavailable"
    DOCKER_CLIENT_TIMEOUT = "docker-client-timeout"
    DOCKER_CLIENT_RESPONSE_INVALID = "docker-client-response-invalid"
    DOCKER_CLIENT_IDENTITY_CHANGED = "docker-client-identity-changed"
    DAEMON_READY = "daemon-ready"
    DAEMON_UNAVAILABLE = "daemon-unavailable"
    DAEMON_TIMEOUT = "daemon-timeout"
    DAEMON_RESPONSE_INVALID = "daemon-response-invalid"
    DAEMON_UNSUPPORTED = "daemon-unsupported"
    DAEMON_IDENTITY_CHANGED = "daemon-identity-changed"
    COMPOSE_READY = "compose-ready"
    COMPOSE_UNAVAILABLE = "compose-unavailable"
    COMPOSE_TIMEOUT = "compose-timeout"
    COMPOSE_RESPONSE_INVALID = "compose-response-invalid"
    BUILDX_READY = "buildx-ready"
    BUILDX_UNAVAILABLE = "buildx-unavailable"
    BUILDX_TIMEOUT = "buildx-timeout"
    BUILDX_RESPONSE_INVALID = "buildx-response-invalid"
    CLOCK_READY = "clock-ready"
    CLOCK_UNSUPPORTED = "clock-unsupported"
    CLOCK_DISCONTINUOUS = "clock-discontinuous"
    CAPACITY_READY = "capacity-ready"
    CAPACITY_UNAVAILABLE = "capacity-unavailable"
    CAPACITY_RESPONSE_INVALID = "capacity-response-invalid"
    CAPACITY_UNSUPPORTED = "capacity-unsupported"


@dataclass(frozen=True)
class ReadinessFailure:
    """Carry one closed observation failure without raw infrastructure output."""

    code: ReadinessCode


@dataclass(frozen=True)
class DockerClientObservation:
    """Carry one exact executable identity for the selected Docker client."""

    path: str
    sha256: str
    version: str
    device: int
    inode: int
    size: int
    mtime_ns: int


@dataclass(frozen=True)
class DockerDaemonObservation:
    """Carry allowlisted daemon identity and raw capacity facts.

    ``architecture`` is the daemon's CPU architecture in OCI spelling (``amd64`` or ``arm64``), mapped from the
    Engine's ``uname -m`` form; any other architecture is a ``daemon-unsupported`` readiness failure.
    """

    daemon_id: str
    server_version: str
    api_version: str
    min_api_version: str
    os_type: str
    cgroup_version: str
    operating_system: str
    cpus: int
    memory_bytes: int
    architecture: str
    name: str | None = None  # the daemon's ``Name``, which names a Colima profile
    endpoint: str | None = None  # the client endpoint, resolved in memory; never recorded


@dataclass(frozen=True)
class ToolVersionObservation:
    """Carry one normalized bounded Compose or Buildx version."""

    version: str


@dataclass(frozen=True)
class DockerReadinessObservation:
    """Carry Docker-side readiness observations without wire-policy decisions."""

    client: DockerClientObservation | ReadinessFailure
    daemon: DockerDaemonObservation | ReadinessFailure
    compose: ToolVersionObservation | ReadinessFailure
    buildx: ToolVersionObservation | ReadinessFailure


@dataclass(frozen=True)
class ReadinessCheck:
    """Carry one closed readiness check in fixed wire order."""

    check: str
    code: ReadinessCode
    facts: dict[str, object]
    ok: bool


@dataclass(frozen=True)
class ReadinessReport:
    """Carry the complete versioned six-check readiness result (``readiness/4``).

        Version 2 adds the daemon check's ``architecture`` fact and the capacity check's ``host_swap_total_bytes`` (zero
        on a host without swap). Version 3 adds the capacity check's machine settings ``parallel_runs`` (an integer or
        ``"auto"``) and ``host_reserve_bytes``. Version 4 adds the capacity check's CPU pressure and daemon probe facts
    ; readiness itself never runs a container, so VM figures are the guardians' latest probe reading.
    """

    checks: tuple[ReadinessCheck, ...]
    ready: bool
    protocol: str = "docker-test-toolkit.readiness/4"


@dataclass(frozen=True)
class DaemonIdentity:
    """Bind the selected client and daemon before mutation."""

    daemon_id: str
    client_path: Path
    client_sha256: str
    client_version: str
    client_device: int
    client_inode: int
    client_size: int
    client_mtime_ns: int
    client_context: str | None


@dataclass(frozen=True)
class MachineFacts:
    """One running ``podman machine`` as ``podman machine inspect`` reports it.

    ``created`` is the JSON ``Created`` string exactly as returned (hashed, never parsed); ``socket_path`` is
    ``ConnectionInfo.PodmanSocket.Path``, compared as a string and never resolved.
    """

    name: str
    created: str
    rootful: bool
    socket_path: str
    memory_mib: int
    cpus: int


@dataclass(frozen=True)
class PodmanBinding:
    """The Podman machine a Docker adapter is bound to, held in memory only and never written."""

    identity: str
    endpoint: str
    machine: MachineFacts
    image_dir: Path


@dataclass(frozen=True)
class ExactInspection:
    """Request observation of one exact Docker resource."""

    kind: str
    identifier: str


@dataclass(frozen=True)
class DockerObservation:
    """Return bounded exact resource evidence."""

    kind: str
    identifier: str
    exists: bool
    # Docker JSON is an external recursive schema whose exact shape varies by kind.
    attributes: Mapping[str, Any]


@dataclass(frozen=True)
class ComposeMutation:
    """Carry a journal-authorized Compose argv and finite deadline."""

    operation_id: str
    argv: tuple[str, ...]
    deadline_ns: int


@dataclass(frozen=True)
class DockerResult:
    """Return bounded adapter completion evidence."""

    exit_code: int
    stdout: str
    stderr: str


@dataclass(frozen=True)
class ProbeRun:
    """The outcome of one deadline-bounded probe container run: its stdout, or a timeout that killed it."""

    stdout: str
    exit_code: int | None  # None when the run was killed at its deadline
    timed_out: bool
