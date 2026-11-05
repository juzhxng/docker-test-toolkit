"""Shared application dispatcher for one-shot and serial machine protocols."""

from __future__ import annotations

import json
import re
import uuid
from dataclasses import MISSING, asdict, fields
from functools import partial
from pathlib import Path
from typing import Any, Callable, TypeVar, cast

from docker_test_toolkit.application import lifecycle
from docker_test_toolkit.application.api import RunHandle
from docker_test_toolkit.application.build_cache import remove_caches
from docker_test_toolkit.application.ports.docker import DockerPort
from docker_test_toolkit.application.recovery import UUID, Context, cleanup_project, cleanup_session
from docker_test_toolkit.application.retirement import retire_projects
from docker_test_toolkit.application.state import account_directory, registry_directory
from docker_test_toolkit.application.timeline import project_timeline
from docker_test_toolkit.domain.errors import InvalidInput, ResourceError, ToolkitError
from docker_test_toolkit.domain.models import (
    SUPPORTED_PROTOCOLS,
    TOOLKIT_VERSION,
    ExactInspection,
    ExternalDockerResource,
    PhaseBudget,
    RunSpec,
)
from docker_test_toolkit.domain.policy import known_fields

_INVALID_CONTEXT = "Command input requires one exact run context."


def _one_shot_context(payload: dict) -> lifecycle.RunContext:
    """Accept the bridge envelope or a retained-run locator, bound to the fixed registry before any I/O."""
    value = payload.get("context", payload)
    locator = lifecycle.locator_fields(value, InvalidInput(_INVALID_CONTEXT))
    state = Path(locator["project_state"])
    if (
        not UUID.fullmatch(locator["run_id"])  # an absolute or dotted id would replace or leave the registry prefix
        or state != registry_directory()
        or Path(locator["record"]) != state / "runs" / f"{locator['run_id']}.json"
    ):
        raise InvalidInput("Run locator is outside the registry.")
    return lifecycle.load_operator_context(json.dumps(locator, separators=(",", ":")))


def _protocol_result() -> dict[str, object]:
    return {
        "version": TOOLKIT_VERSION,
        "protocols": dict(SUPPORTED_PROTOCOLS),
        "source_identity": lifecycle.source_identity(),
        "toolkit": _TOOLKIT,
    }


_T = TypeVar("_T")
_TOOLKIT = {"version": TOOLKIT_VERSION}  # additive since 0.5.0 (the R3 proof release); projects ignore it


def _construct(cls: Callable[..., _T], value: object, invalid: InvalidInput) -> _T:
    """Build a public dataclass from a wire object: fields without a default are required, unknown fields ignored.

    A field a later version adds with a default therefore never becomes required for an older caller (rule 3).
    """
    parameters = fields(cast(Any, cls))
    required = {field.name for field in parameters if field.default is MISSING and field.default_factory is MISSING}
    optional = {field.name for field in parameters} - required
    return cls(**known_fields(value, required, optional, invalid=invalid))


_GENERATION = frozenset({"started_at", "restart_count"})


def _generation(value: object, invalid: InvalidInput) -> dict[str, object]:
    """Return a container generation's two fields; the strict lifecycle validators check their values."""
    return known_fields(value, _GENERATION, invalid=invalid)


def run_spec_from_payload(payload: dict) -> RunSpec:
    """Parse one JSON run specification into the typed public model; unknown fields are ignored at every level."""
    required = {
        "project_root",
        "entrypoint",
        "purpose",
        "services",
        "closure",
        "profile",
        "phases",
        "initial_phase",
        "external_resources",
        "retain",
        "wait_seconds",
    }
    payload = known_fields(payload, required, invalid=InvalidInput("Run preparation input is incomplete."))
    phases = payload["phases"]
    external = payload["external_resources"]
    if not isinstance(phases, dict) or not isinstance(external, list):
        raise InvalidInput("Run preparation input has invalid structured fields.")
    invalid = InvalidInput("Run preparation input is invalid.")
    try:
        return RunSpec(
            project_root=Path(payload["project_root"]),
            entrypoint=payload["entrypoint"],
            purpose=payload["purpose"],
            services=tuple(payload["services"]),
            closure=tuple(payload["closure"]),
            profile=payload["profile"],
            phases={name: _construct(PhaseBudget, value, invalid) for name, value in phases.items()},
            initial_phase=payload["initial_phase"],
            external_resources=tuple(_construct(ExternalDockerResource, value, invalid) for value in external),
            retain=payload["retain"],
            wait_seconds=payload["wait_seconds"],
        )
    except (TypeError, ValueError, ToolkitError) as error:
        raise invalid from error


class LeaseOperator:
    """Own one non-serializable run handle across a serial bridge lifetime."""

    def __init__(
        self,
        open_run_factory: Callable[[RunSpec], RunHandle],
        docker_factory: Callable[[], DockerPort],
    ) -> None:
        self._open_run = open_run_factory
        self._docker_factory = docker_factory
        self._handle: RunHandle | None = None
        self._entered = False
        self._preserve = False
        self._control_capabilities: dict[str, dict[str, object]] = {}

    def _current(self) -> RunHandle:
        if self._handle is None:
            raise InvalidInput("Run must be prepared before this command.")
        return self._handle

    def execute(self, command: str, payload: dict) -> dict[str, object]:
        """Execute one bridge command against the bridge-owned handle."""
        if command == "protocol":
            return execute(command, payload, docker_factory=self._docker_factory)
        if command == "run.prepare":
            if self._handle is not None:
                raise InvalidInput("Bridge already owns a prepared run.")
            self._handle = self._open_run(run_spec_from_payload(payload))
            return {
                "project_id": self._handle.project_id,
                "session_id": self._handle.session_id,
                "run_id": self._handle.run_id,
                "private_root": str(self._handle.private_root),
                "status": asdict(self._handle.status()),
                "toolkit": _TOOLKIT,
            }
        handle = self._current()
        if command == "run.cancel":
            if self._entered:
                raise InvalidInput("Run cancel requires one unentered preparation.")
            result = handle.cancel()
            self._handle = None
            self._control_capabilities.clear()
            return asdict(result)
        if command == "run.enter":
            invalid = InvalidInput("Run enter input is invalid.")
            if type(known_fields(payload, {"docker"}, invalid=invalid)["docker"]) is not bool:
                raise invalid
            handle.enter()
            self._entered = True
            docker = None
            if payload["docker"]:
                context = handle.docker_context
                docker = {
                    "executable": str(context.executable),
                    "path": context.environment["PATH"],
                    "compose_project": context.compose_project,
                    "builder": context.builder,
                }
            return {
                "project_id": handle.project_id,
                "session_id": handle.session_id,
                "run_id": handle.run_id,
                "generation": handle.generation,
                "private_root": str(handle.private_root),
                "status": asdict(handle.status()),
                "docker": docker,
                "toolkit": _TOOLKIT,
            }
        if command == "run.status":
            return asdict(handle.status())
        if command == "run.transition":
            invalid = InvalidInput("Run transition input is invalid.")
            if not isinstance(known_fields(payload, {"phase"}, invalid=invalid)["phase"], str):
                raise invalid
            return asdict(handle.transition(payload["phase"]))
        if command in {"run.leave", "run.retain"}:
            invalid = InvalidInput("Run leave input is invalid.")
            if not isinstance(known_fields(payload, {"outcome"}, invalid=invalid)["outcome"], str):
                raise invalid
            service = handle.retain if command == "run.retain" else handle.leave
            try:
                result = service(payload["outcome"])
            finally:
                self._entered = False
                self._control_capabilities.clear()
            self._preserve = result.state == "stopped-retained"
            return asdict(result)
        if command == "run.resume":
            handle.resume()
            self._entered = True
            return asdict(handle.status())
        if command == "run.release":
            result = handle.release()
            self._preserve = False
            self._control_capabilities.clear()
            return asdict(result)
        if command in {"run.attach", "consumer.register"}:
            invalid = InvalidInput("Consumer registration input is invalid.")
            payload = known_fields(payload, {"identity", "services"}, invalid=invalid)
            try:
                identity = _construct(lifecycle.ProcessIdentity, payload["identity"], invalid)
                services = tuple(payload["services"])
            except (TypeError, ValueError) as error:
                raise InvalidInput("Consumer registration input is invalid.") from error
            return {"consumer_id": handle.register_consumer(identity, services)}
        if command == "consumer.complete":
            invalid = InvalidInput("Consumer completion input is invalid.")
            if not isinstance(known_fields(payload, {"consumer_id"}, invalid=invalid)["consumer_id"], str):
                raise invalid
            return asdict(handle.complete_consumer(payload["consumer_id"]))
        if command == "container-control.register":
            invalid = InvalidInput("Container control registration input is invalid.")
            payload = known_fields(
                payload, {"caller_id", "caller_generation", "allowed_targets", "lifetime_ms"}, invalid=invalid
            )
            if (
                not isinstance(payload["allowed_targets"], list)
                or type(payload["lifetime_ms"]) is not int
                or not 0 < payload["lifetime_ms"] <= 14_400_000
            ):
                raise invalid
            caller_id = payload["caller_id"]
            caller_generation = _generation(payload["caller_generation"], invalid)
            allowed_targets = tuple(payload["allowed_targets"])
            generations = handle.register_container_control(caller_id, caller_generation, allowed_targets)
            clock = lifecycle.NativeClock()
            capability_id = str(uuid.uuid4())
            self._control_capabilities[capability_id] = {
                "peer": lifecycle.current_process(clock),
                "caller_id": caller_id,
                "caller_generation": caller_generation,
                "allowed_targets": allowed_targets,
                "deadline": lifecycle.Deadline.after(clock, payload["lifetime_ms"] / 1000),
            }
            return {
                "capability": capability_id,
                "lifetime_ms": payload["lifetime_ms"],
                "generations": generations,
            }
        if command == "container-control.execute":
            invalid = InvalidInput("Container control input is invalid.")
            payload = known_fields(
                payload,
                {"capability", "operation_id", "action", "targets", "expected", "request_sha256"},
                invalid=invalid,
            )
            if (
                not isinstance(payload["capability"], str)
                or not isinstance(payload["operation_id"], str)
                or not isinstance(payload["action"], str)
                or not isinstance(payload["targets"], list)
                or not isinstance(payload["expected"], dict)
                or not isinstance(payload["request_sha256"], str)
                or not re.fullmatch(r"[0-9a-f]{64}", payload["request_sha256"])
            ):
                raise invalid
            expected = {target: _generation(value, invalid) for target, value in payload["expected"].items()}
            grant = self._control_capabilities.get(payload["capability"])
            if grant is None:
                raise InvalidInput("Container control capability is invalid.")
            deadline = grant["deadline"]
            peer = grant["peer"]
            control_targets = tuple(payload["targets"])
            allowed = grant["allowed_targets"]
            if (
                not isinstance(deadline, lifecycle.Deadline)
                or not isinstance(peer, lifecycle.ProcessIdentity)
                or not isinstance(allowed, tuple)
                or deadline.remaining_ns(lifecycle.NativeClock()) <= 0
                or lifecycle.observe_process(peer) != "executing"
                or payload["action"] not in {"start", "stop"}
                or not control_targets
                or len(set(control_targets)) != len(control_targets)
                or not set(control_targets) <= set(allowed)
                or set(expected) != set(control_targets)
            ):
                raise ResourceError("Container control capability is expired or contradictory.")
            control_caller = grant["caller_id"]
            control_generation = grant["caller_generation"]
            if not isinstance(control_caller, str) or not isinstance(control_generation, dict):
                raise ResourceError("Container control capability is invalid.")
            handle.validate_container_control(control_caller, control_generation, allowed)

            def _revalidate() -> None:
                if lifecycle.observe_process(peer) != "executing":
                    raise ResourceError("Container control bridge peer is no longer executing.")
                handle.validate_container_control(control_caller, control_generation, allowed)

            return {
                "generations": handle.control_containers(
                    payload["operation_id"],
                    payload["action"],
                    control_targets,
                    expected,
                    request_sha256=payload["request_sha256"],
                    pre_dispatch=_revalidate,
                )
            }
        if command == "endpoint.publish":
            payload = known_fields(
                payload,
                {"service", "protocol", "container_port", "namespace"},
                invalid=InvalidInput("Endpoint publication input is invalid."),
            )
            endpoint = handle.publish_endpoint(
                payload["service"],
                payload["protocol"],
                payload["container_port"],
                namespace=payload["namespace"],
            )
            return asdict(endpoint)
        if command == "endpoint.require":
            payload = known_fields(
                payload, {"service", "namespace"}, invalid=InvalidInput("Endpoint request input is invalid.")
            )
            return asdict(handle.require_endpoint(payload["service"], namespace=payload["namespace"]))
        raise InvalidInput("Lease command is unsupported.")

    def close(self) -> None:
        """Fail and clean an entered run when its serial bridge disconnects."""
        if self._handle is not None and self._entered:
            self._handle.leave("failed")
            self._entered = False
        elif self._handle is not None and not self._preserve and self._handle.status().state == "prepared":
            self._handle.cancel()
        self._control_capabilities.clear()


def execute(
    command: str,
    payload: dict,
    *,
    docker_factory: Callable[..., DockerPort],
    execute_cleanup: bool = False,
) -> dict[str, object]:
    """Execute one closed operator command through the canonical services."""
    if not isinstance(payload, dict):
        raise InvalidInput("Command input must be a JSON object.")
    if command == "protocol":
        return _protocol_result()
    if command == "readiness.inspect":
        from docker_test_toolkit.application.lifecycle import Deadline, NativeClock
        from docker_test_toolkit.application.readiness import inspect_readiness

        clock = NativeClock()
        deadline = Deadline.after(clock, 30)
        return asdict(inspect_readiness(docker_factory(deadline), clock, deadline=deadline))
    if command == "project.retire":
        return retire_projects(registry_directory(), account_directory(), payload, execute=execute_cleanup)
    if command == "run.status":
        return asdict(lifecycle.run_status(_one_shot_context(payload)))
    if command in {"run.recover", "run.release"}:
        context = _one_shot_context(payload)
        if command == "run.recover":
            recovered = asdict(lifecycle.recover_run(context))
            report = lifecycle.retention_after_release(context, None, "recovery")  # no lock is held here (design 2.7)
            return {**recovered, "retention": None if report is None else asdict(report)}
        return asdict(lifecycle.release_run(context))
    if command == "resource.observe":
        payload = known_fields(
            payload,
            {"context", "kind", "identifier"},
            invalid=InvalidInput("Resource observation input is incomplete."),
        )
        context = _one_shot_context(payload)
        data = lifecycle.load_run_record(context)
        kind = payload["kind"]
        identifier = payload["identifier"]
        if not isinstance(kind, str) or not isinstance(identifier, str):
            raise InvalidInput("Resource observation identity is invalid.")
        owned = identifier in data["owned_resources"].get(f"{kind}s", {})
        external = any(item["kind"] == kind and item["identifier"] == identifier for item in data["external_resources"])
        if not owned and not external:
            raise InvalidInput("Resource observation target is neither owned nor declared external.")
        observed = docker_factory().inspect_exact(ExactInspection(kind, identifier))
        return {"kind": kind, "identifier": identifier, "exists": observed.exists}
    if command == "cache.remove":
        return remove_caches(registry_directory(), payload, execute=execute_cleanup)
    if command == "project.timeline":
        invalid = InvalidInput("Project timeline input is incomplete.")
        if not isinstance(known_fields(payload, {"project_root"}, invalid=invalid)["project_root"], str):
            raise invalid
        try:
            root = Path(payload["project_root"]).resolve(strict=True)
        except (OSError, RuntimeError, ValueError) as error:
            raise InvalidInput("Project timeline path is invalid.") from error
        return project_timeline(registry_directory(), root)
    if command in {"cleanup.project", "cleanup.session"}:
        expected = {"project_root"} if command == "cleanup.project" else {"project_root", "session_id"}
        invalid = InvalidInput("Cleanup input is incomplete.")
        if not all(isinstance(value, str) for value in known_fields(payload, expected, invalid=invalid).values()):
            raise invalid
        try:
            root = Path(payload["project_root"]).resolve(strict=True)
        except (OSError, RuntimeError, ValueError) as error:
            raise InvalidInput("Project cleanup path is invalid.") from error
        cleanup_context = Context(root, registry_directory())
        # Only an executing cleanup resolves the account: it takes the image lock K and, under it, decides per image
        # whether a live or retained run still uses it (2.6, 2.7); a dry run reads the registry alone.
        image_account = account_directory() if execute_cleanup else None
        image_in_use = (
            lifecycle.operator_image_interest(image_account, cleanup_context.state) if image_account else None
        )
        if command == "cleanup.session":
            return cleanup_session(
                cleanup_context,
                docker_factory(),
                payload["session_id"],
                execute=execute_cleanup,
                image_account=image_account,
                image_in_use=image_in_use,
            )
        return cleanup_project(
            cleanup_context,
            docker_factory(),
            execute=execute_cleanup,
            verify_run=partial(lifecycle.run_record_loads, cleanup_context.state),
            image_account=image_account,
            image_in_use=image_in_use,
        )
    raise InvalidInput("Operator command is unsupported.")
