"""Thin typed facade over the lifecycle application service."""

from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Callable, Mapping, TextIO

from docker_test_toolkit.application import lifecycle
from docker_test_toolkit.application.batch import BatchCoordinator
from docker_test_toolkit.application.build_cache import switch_private_addition
from docker_test_toolkit.application.ports.docker import DockerPort
from docker_test_toolkit.application.supervision import account_environment
from docker_test_toolkit.domain.errors import IncompleteCleanup, ResourceError, ToolkitError
from docker_test_toolkit.domain.models import (
    SUPPORTED_PROTOCOLS,
    TOOLKIT_VERSION,
    ArtifactIdentity,
    BatchPlan,
    Endpoint,
    ExternalDockerResource,
    RunSpec,
)
from docker_test_toolkit.domain.records import unique_object


def open_batches(plan: BatchPlan) -> BatchCoordinator:
    """Open one pure finite batch coordinator without resource mutation."""
    return BatchCoordinator(plan)


@dataclass(frozen=True)
class DockerExecutionContext:
    """Expose only the private Docker executable and child environment."""

    executable: Path
    environment: Mapping[str, str]
    compose_project: str
    builder: str


@dataclass(frozen=True)
class ConsumerRunContext:
    """Expose one live borrowed run without parent lifecycle authority."""

    run_id: str
    services: tuple[str, ...]
    artifact_identity: ArtifactIdentity
    docker_context: DockerExecutionContext
    _context: lifecycle.RunContext
    _consumer_id: str
    _identity: lifecycle.ProcessIdentity

    def status(self) -> lifecycle.RunStatus:
        """Return status only while this exact process still owns the consumer grant."""
        return lifecycle.current_python_consumer_status(self._context, self._consumer_id, self._identity)


def current_consumer_run() -> ConsumerRunContext | None:
    """Return this process's validated Docker-borrowing run, if one was granted."""
    current = lifecycle.current_python_consumer()
    if current is None:
        return None
    context, consumer_id, identity, services, data, wrapper = current
    artifact = data["artifact"]
    authority = data["docker_authority"]
    environment = account_environment(wrapper.parent, private_additions=switch_private_addition())
    environment["PATH"] = f"{wrapper.parent}{os.pathsep}{environment['PATH']}"
    return ConsumerRunContext(
        context.run_id,
        services,
        ArtifactIdentity(artifact["version"], artifact["sha256"], dict(artifact["protocols"])),
        DockerExecutionContext(
            wrapper,
            MappingProxyType(environment),
            authority["compose_project"],
            authority["builder"],
        ),
        context,
        consumer_id,
        identity,
    )


@dataclass
class RunHandle:
    """Hold non-serializable authority for one prepared in-process run."""

    _spec: RunSpec
    _context: lifecycle.RunContext
    _test_directory: Path | None = None
    _docker_factory: Callable[[], DockerPort] | None = None
    _lease: lifecycle.RunLease | None = None
    _docker_context: DockerExecutionContext | None = None

    @property
    def toolkit_version(self) -> str:
        """Return the version of the toolkit running this handle (additive since 0.5.0)."""
        return TOOLKIT_VERSION

    @property
    def project_id(self) -> str:
        """Return the stable canonical project identity."""
        return self._context.project.project_id

    @property
    def session_id(self) -> str:
        """Return this run's lifecycle session identity."""
        return self._context.session_id

    @property
    def run_id(self) -> str:
        """Return this run's unique identity."""
        return self._context.run_id

    @property
    def private_root(self) -> Path:
        """Return the account-private material directory for this run."""
        return self._context.private_root

    @property
    def generation(self) -> int:
        """Return the entered generation without serializing its authority."""
        if self._lease is None:
            raise ToolkitError("Run is not entered.")
        return self._lease.generation

    @property
    def compose_project(self) -> str:
        """Return the toolkit-derived exact Compose project identity."""
        return f"dtt-{self.project_id[:12]}-{self.run_id.replace('-', '')[:12]}"

    @property
    def docker_context(self) -> DockerExecutionContext:
        """Materialize and return this entered run's immutable private shim context."""
        if self._lease is None or self._docker_factory is None:
            raise ToolkitError("Run is not entered.")
        if self._docker_context is None:
            materialized = lifecycle.materialize_docker_authority(self._lease, self._docker_factory())
            environment = account_environment(self._context.private_root, private_additions=switch_private_addition())
            environment["PATH"] = f"{materialized['path']}{os.pathsep}{environment['PATH']}"
            self._docker_context = DockerExecutionContext(
                executable=Path(str(materialized["executable"])),
                environment=MappingProxyType(environment),
                compose_project=str(materialized["compose_project"]),
                builder=str(materialized["builder"]),
            )
        return self._docker_context

    def enter(self) -> "RunHandle":
        """Enter admission once and retain the exact granted lease."""
        if self._lease is not None:
            raise ToolkitError("Run is already entered.")
        topology = "native" if sys.platform == "linux" else "desktop"
        lifecycle.configure_run(
            self._context,
            services=self._spec.services,
            closure=self._spec.closure,
            phases={name: lifecycle.PhaseBudget(**vars(budget)) for name, budget in self._spec.phases.items()},
            initial_phase=self._spec.initial_phase or "",
            topology=topology,
        )
        self._lease = lifecycle.enter_run(self._context, _test_directory=self._test_directory)
        return self

    def status(self):
        """Return the current validated lifecycle status."""
        return lifecycle.run_status(self._context)

    def transition(self, phase: str):
        """Move an entered run to one of its declared phases."""
        if self._lease is None:
            raise ToolkitError("Run is not entered.")
        return lifecycle.transition_phase(self._lease, phase)

    def publish_endpoint(
        self,
        service: str,
        protocol: str,
        container_port: int,
        *,
        namespace: str | None = None,
    ) -> Endpoint:
        """Publish one exact dynamic host endpoint from the current generation."""
        if self._lease is None:
            raise ToolkitError("Run is not entered.")
        return lifecycle.publish_host_endpoint(
            self._lease,
            service,
            protocol,
            container_port,
            namespace=namespace,
        )

    def require_endpoint(self, service: str, *, namespace: str | None = None) -> Endpoint:
        """Resolve one current endpoint for this run's exact requester."""
        if self._lease is None:
            raise ToolkitError("Run is not entered.")
        return lifecycle.require_endpoint(self._lease, service, self._lease.requester, namespace=namespace)

    def launch_python_consumer(
        self,
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
        """Launch and register one birth-bound Python child behind the grant barrier."""
        if self._lease is None:
            raise ToolkitError("Run is not entered.")
        if borrow_docker:
            _ = self.docker_context
        return lifecycle.launch_python_consumer(
            self._lease,
            mode,
            target,
            args,
            services,
            cwd=cwd,
            env=env,
            stdout=stdout,
            stderr=stderr,
            borrow_docker=borrow_docker,
        )

    def complete_consumer(self, consumer_id: str):
        """Record one registered consumer complete after native exit observation."""
        if self._lease is None:
            raise ToolkitError("Run is not entered.")
        return lifecycle.finish_consumer(self._lease, consumer_id)

    def stop_consumer(self, consumer_id: str, *, wait_seconds: float = 5):
        """Stop one registered child through its birth-bound process identity."""
        if self._lease is None:
            raise ToolkitError("Run is not entered.")
        return lifecycle.stop_consumer(self._lease, consumer_id, wait_seconds=wait_seconds)

    def signal_consumer(self, consumer_id: str, signum: int) -> bool:
        """Signal one registered child through its birth-bound process identity."""
        if self._lease is None:
            raise ToolkitError("Run is not entered.")
        identity = lifecycle.consumer_identity(self._lease, consumer_id)
        return lifecycle.signal_process(identity, signum)

    def register_consumer(self, identity: lifecycle.ProcessIdentity, services: tuple[str, ...]) -> str:
        """Register one already-ready birth-bound host or container consumer."""
        if self._lease is None:
            raise ToolkitError("Run is not entered.")
        return lifecycle.register_consumer(self._lease, identity, services)

    def register_container_control(
        self,
        caller_id: str,
        caller_generation: dict[str, object],
        allowed_targets: tuple[str, ...],
    ) -> dict[str, dict]:
        """Bind exact owned container generations for bridge-mediated control."""
        if self._lease is None:
            raise ToolkitError("Run is not entered.")
        return lifecycle.register_container_control(
            self._lease,
            caller_id,
            caller_generation,
            allowed_targets,
        )

    def validate_container_control(
        self,
        caller_id: str,
        caller_generation: dict[str, object],
        allowed_targets: tuple[str, ...],
    ) -> None:
        """Revalidate an exact caller and target set immediately before mutation."""
        if self._lease is None:
            raise ToolkitError("Run is not entered.")
        lifecycle.validate_container_control(
            self._lease,
            caller_id,
            caller_generation,
            allowed_targets,
        )

    def control_containers(
        self,
        operation_id: str,
        action: str,
        targets: tuple[str, ...],
        expected: dict[str, dict],
        *,
        request_sha256: str,
        pre_dispatch: Callable[[], None] | None = None,
    ) -> dict[str, dict]:
        """Journal and execute one exact idempotent start or stop request."""
        if self._lease is None:
            raise ToolkitError("Run is not entered.")
        return lifecycle.control_compose_containers(
            self._lease,
            operation_id,
            action,
            targets,
            expected,
            request_sha256=request_sha256,
            pre_dispatch=pre_dispatch,
        )

    def leave(self, outcome: str = "passed"):
        """Withdraw the entered lease and require acknowledged cleanup."""
        if self._lease is None:
            raise ToolkitError("Run is not entered.")
        try:
            return lifecycle.leave_run(self._lease, outcome)
        finally:
            self._lease = None
            self._docker_context = None

    def retain(self, outcome: str = "passed"):
        """Leave one retention-enabled run after stopping its exact owned resources."""
        if not self._spec.retain:
            raise ToolkitError("Run was not prepared for retention.")
        return self.leave(outcome)

    def write_retained_locator(self, path: Path) -> None:
        """Persist a private non-authoritative locator for this stopped retained run."""
        if self.status().state != "stopped-retained":
            raise ToolkitError("Retained locator requires a stopped retained run.")
        payload = json.dumps(
            {
                "project_root": str(self._context.project.root),
                "project_state": str(self._context.project.state),
                "run_id": self._context.run_id,
                "session_id": self._context.session_id,
                "record": str(self._context.record),
                "private_root": str(self._context.private_root),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        try:
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                os.fchmod(stream.fileno(), 0o600)
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
        except OSError as error:
            raise ToolkitError("Retained locator could not be created exclusively.") from error

    def resume(self) -> "RunHandle":
        """Re-admit and restart this handle's exact stopped-retained run."""
        if self._lease is not None:
            raise ToolkitError("Run is already entered.")
        self._lease = lifecycle.resume_run(
            self._context,
            wait_seconds=self._spec.wait_seconds,
            _test_directory=self._test_directory,
        )
        return self

    def release(self):
        """Remove this handle's exact stopped-retained run under fresh admission."""
        if self._lease is not None:
            raise ToolkitError("Active run must leave before release.")
        return lifecycle.release_run(
            self._context,
            _test_state=self._context.project.state if self._test_directory is not None else None,
            _test_directory=self._test_directory,
        )

    def cancel(self):
        """Cancel a preparation that has not entered admission."""
        if self._lease is not None:
            raise ToolkitError("Entered run must leave instead of canceling.")
        return lifecycle.cancel_run(self._context)


def _declare_run_metadata(
    handle: RunHandle,
    context: lifecycle.RunContext,
    artifact: ArtifactIdentity,
    external_resources: tuple[ExternalDockerResource, ...],
    deadline_ns: int | None = None,
) -> None:
    """Declare metadata or terminally cancel the exact unentered preparation.

    A declaration that cannot take its locks by ``deadline_ns`` fails like any other and is cancelled; the cancellation
    itself is cleanup and waits for its locks.
    """
    try:
        lifecycle.declare_run_metadata(context, artifact, external_resources, deadline_ns=deadline_ns)
    except (OSError, ToolkitError) as error:
        try:
            status = handle.cancel()
        except (OSError, ToolkitError) as cleanup_error:
            raise IncompleteCleanup(
                "Run metadata declaration failed and preparation cleanup is uncertain."
            ) from cleanup_error
        if status.state != "complete" or status.cleanup_outcome != "complete":
            raise IncompleteCleanup("Run metadata declaration failed and preparation cleanup is uncertain.") from error
        raise ResourceError("Run metadata declaration failed before entry.") from error


def _open_run(
    spec: RunSpec,
    *,
    test_state: Path | None = None,
    test_directory: Path | None = None,
    docker_factory: Callable[[], DockerPort] | None = None,
    prepare_deadline_ns: int | None = None,
) -> RunHandle:
    """Prepare one run, optionally through the private test composition.

    ``prepare_deadline_ns`` (absolute ``time.monotonic_ns()``) bounds the registry-side preparation waits.
    """
    if not isinstance(spec, RunSpec):
        raise ToolkitError("open_run requires a RunSpec.")
    context = lifecycle.prepare_run(
        lifecycle.RunRequest(
            project_root=spec.project_root,
            entrypoint=spec.entrypoint,
            purpose=spec.purpose,
            services=spec.services,
            closure=spec.closure,
            profile=spec.profile,
            wait_seconds=spec.wait_seconds,
            retain=spec.retain,
        ),
        _test_state=test_state,
        _test_directory=test_directory,
        deadline_ns=prepare_deadline_ns,
    )
    handle = RunHandle(spec, context, test_directory, docker_factory)
    _declare_run_metadata(handle, context, toolkit_identity(), spec.external_resources, prepare_deadline_ns)
    return handle


def _read_retained_locator(path: Path) -> tuple[str, lifecycle.RunContext]:
    """Read one private exact context locator without following replacement links."""
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError as error:
        raise ToolkitError("Retained locator is unreadable or a symlink.") from error
    try:
        observed = os.fstat(descriptor)
        if not stat.S_ISREG(observed.st_mode) or observed.st_uid != os.getuid() or observed.st_mode & 0o077:
            raise ToolkitError("Retained locator must be a private account-owned regular file.")
        content = os.read(descriptor, 64 * 1024 + 1)
    finally:
        os.close(descriptor)
    if len(content) > 64 * 1024:
        raise ToolkitError("Retained locator exceeds its fixed bound.")
    try:
        value = json.loads(content.decode("utf-8"), object_pairs_hook=unique_object)
    except (RecursionError, UnicodeDecodeError, ValueError) as error:
        raise ToolkitError("Retained locator is not valid UTF-8 JSON.") from error
    locator = lifecycle.locator_fields(value, ToolkitError("Retained locator is not a run locator."))
    payload = json.dumps(locator, separators=(",", ":"))  # fields a newer writer adds are dropped
    return payload, lifecycle.load_operator_context(payload)


def _expected_retained_record(spec: RunSpec) -> dict[str, object]:
    return {
        "project_root": str(spec.project_root),
        "entrypoint": spec.entrypoint,
        "purpose": spec.purpose,
        "services": list(spec.services),
        "closure": list(spec.closure),
        "profile": spec.profile,
        "phases": {name: asdict(budget) for name, budget in spec.phases.items()},
        "initial_phase": spec.initial_phase,
        "external_resources": [asdict(resource) for resource in spec.external_resources],
        "retain": True,
    }


def _open_retained_run(
    spec: RunSpec,
    locator: Path,
    *,
    test_directory: Path | None = None,
    docker_factory: Callable[[], DockerPort] | None = None,
) -> RunHandle:
    """Reconstruct a facade, never lease authority, from one validated exact locator."""
    if not isinstance(spec, RunSpec) or not spec.retain:
        raise ToolkitError("Retained run requires a retention-enabled RunSpec.")
    _payload, context = _read_retained_locator(Path(locator))
    record = lifecycle.load_run_record(context)
    expected = _expected_retained_record(spec)
    if any(record.get(key) != value for key, value in expected.items()):
        raise ToolkitError("Retained run does not match the requested specification.")
    if context.project.root != spec.project_root or record.get("state") != "stopped-retained":
        raise ToolkitError("Retained locator does not name a compatible stopped run.")
    return RunHandle(spec, context, test_directory, docker_factory)


def toolkit_identity() -> ArtifactIdentity:
    """Return the identity of the running toolkit: its version, the archive's SHA-256 and its protocol versions.

    From the source tree the digest is the source revision. Projects record this identity in their reports.

    Raises:
        ResourceError: If the loaded archive or source changed after loading.
    """
    return ArtifactIdentity(TOOLKIT_VERSION, lifecycle.source_identity(), dict(SUPPORTED_PROTOCOLS))
