"""Private shim dispatcher and serial NDJSON bridge entry points."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Callable, Mapping
from dataclasses import fields, is_dataclass
from pathlib import Path
from typing import BinaryIO, TextIO

from docker_test_toolkit.application import lifecycle
from docker_test_toolkit.application.api import RunHandle, open_batches
from docker_test_toolkit.application.batch import BatchCoordinator
from docker_test_toolkit.application.operator import LeaseOperator, execute
from docker_test_toolkit.application.ports.docker import DockerPort
from docker_test_toolkit.application.protocol import error_envelope, success_envelope
from docker_test_toolkit.domain.errors import IncompleteCleanup, InvalidInput, ToolkitError
from docker_test_toolkit.domain.models import (
    BatchExecutionReport,
    BatchItem,
    BatchPlan,
    BatchRunEvidence,
    BatchSegment,
)
from docker_test_toolkit.domain.policy import known_fields
from docker_test_toolkit.domain.records import unique_object
from docker_test_toolkit.factories.api import open_run
from docker_test_toolkit.factories.cli import bind_docker_adapter, build_docker_adapter

_MAX_LINE = 1024 * 1024
_ENVELOPE = frozenset({"command", "input"})
_MALFORMED = "Bridge request is malformed."
_SEGMENT_FIELDS = frozenset({"segment_id", "items", "trailing_boundary_id", "requires_previous_success"})
_EVIDENCE_FIELDS = frozenset({"run_id", "run_status", "category", "cleanup_category"})


def _request(raw: bytes) -> dict:
    try:
        request = json.loads(raw, object_pairs_hook=unique_object)
    except (RecursionError, UnicodeDecodeError, ValueError) as error:
        raise InvalidInput("Bridge request is invalid JSON.") from error
    if not isinstance(request, dict):
        raise InvalidInput("Bridge request is malformed.")
    return request


def _discard_line(requests: BinaryIO) -> None:
    while remainder := requests.readline(_MAX_LINE + 2):
        if remainder.endswith(b"\n"):
            return


def serve_requests(
    requests: BinaryIO,
    responses: TextIO,
    *,
    docker_factory: Callable[[], DockerPort] = build_docker_adapter,
) -> int:
    """Serve bounded serial NDJSON requests through the shared operator dispatcher."""
    result_code = 0
    while raw := requests.readline(_MAX_LINE + 2):
        command = "unknown"
        try:
            if len(raw) > _MAX_LINE + 1 or not raw.endswith(b"\n"):
                if not raw.endswith(b"\n"):
                    _discard_line(requests)
                raise InvalidInput("Bridge request exceeds its fixed line bound.")
            request = known_fields(_request(raw), _ENVELOPE, {"execute"}, invalid=InvalidInput(_MALFORMED))
            command = request["command"]
            payload = request["input"]
            cleanup = request.get("execute", False)
            if not isinstance(command, str) or not isinstance(payload, dict) or type(cleanup) is not bool:
                raise InvalidInput("Bridge request fields are invalid.")
            if command == "project.retire":
                raise InvalidInput("Project retirement runs only through the operator command line.")
            value = execute(command, payload, docker_factory=docker_factory, execute_cleanup=cleanup)
            cleanup_result = None
            result_code = 0
            if command.startswith("cleanup.") and cleanup and value.get("clean") is not True:
                cleanup_error, _ = error_envelope(command, IncompleteCleanup("Exact cleanup remains incomplete."))
                cleanup_result = {"ok": False, "error": cleanup_error["error"]}
                result_code = 7
            response = success_envelope(command, value, cleanup=cleanup_result)
        except (ToolkitError, OSError) as error:
            response, result_code = error_envelope(command, error)
        responses.write(json.dumps(response, sort_keys=True, separators=(",", ":")) + "\n")
        responses.flush()
    return result_code


def _serve_lease_operator(
    requests: BinaryIO,
    responses: TextIO,
    operator: LeaseOperator,
) -> int:
    result_code = 0
    try:
        while raw := requests.readline(_MAX_LINE + 2):
            command = "unknown"
            try:
                if len(raw) > _MAX_LINE + 1 or not raw.endswith(b"\n"):
                    if not raw.endswith(b"\n"):
                        _discard_line(requests)
                    raise InvalidInput("Bridge request exceeds its fixed line bound.")
                request = known_fields(_request(raw), _ENVELOPE, invalid=InvalidInput(_MALFORMED))
                command = request["command"]
                payload = request["input"]
                if not isinstance(command, str) or not isinstance(payload, dict):
                    raise InvalidInput("Bridge request fields are invalid.")
                response = success_envelope(command, operator.execute(command, payload))
                result_code = 0
            except (ToolkitError, OSError) as error:
                response, result_code = error_envelope(command, error)
            responses.write(json.dumps(response, sort_keys=True, separators=(",", ":")) + "\n")
            responses.flush()
    finally:
        try:
            operator.close()
        except (ToolkitError, OSError):
            result_code = 7
    return result_code


def serve_lease(
    requests: BinaryIO,
    responses: TextIO,
    *,
    open_run_factory: Callable[..., RunHandle] = open_run,
    docker_factory: Callable[[], DockerPort] = build_docker_adapter,
) -> int:
    """Serve one serial bridge whose process owns a non-serializable run handle."""
    return _serve_lease_operator(requests, responses, LeaseOperator(open_run_factory, docker_factory))


def _batch_plan(payload: object) -> BatchPlan:
    """Parse the wire representation of one finite batch plan; unknown fields are ignored at every level."""
    payload = known_fields(
        payload, {"project_root", "segments"}, invalid=InvalidInput("Batch plan input is malformed.")
    )
    segments = payload["segments"]
    if not isinstance(segments, list):
        raise InvalidInput("Batch plan input has invalid structured fields.")
    try:
        parsed_segments = []
        for segment in segments:
            segment = known_fields(segment, _SEGMENT_FIELDS, invalid=InvalidInput("Batch segment input is malformed."))
            items = segment["items"]
            if not isinstance(items, list):
                raise InvalidInput("Batch segment items are malformed.")
            parsed_items = []
            for item in items:
                item = known_fields(
                    item, {"item_id", "closure"}, invalid=InvalidInput("Batch item input is malformed.")
                )
                parsed_items.append(BatchItem(item["item_id"], tuple(item["closure"])))
            parsed_segments.append(
                BatchSegment(
                    segment["segment_id"],
                    tuple(parsed_items),
                    segment["trailing_boundary_id"],
                    segment["requires_previous_success"],
                )
            )
        return BatchPlan(Path(payload["project_root"]), None, tuple(parsed_segments))
    except (TypeError, ValueError, ToolkitError) as error:
        raise InvalidInput("Batch plan input is invalid.") from error


def _batch_report(payload: object) -> BatchExecutionReport:
    """Parse the wire representation of one bounded execution report; unknown fields are ignored."""
    payload = known_fields(
        payload, {"primary", "run_evidence"}, invalid=InvalidInput("Batch execution report is malformed.")
    )
    evidence = payload["run_evidence"]
    if evidence is not None:
        evidence = known_fields(evidence, _EVIDENCE_FIELDS, invalid=InvalidInput("Batch run evidence is malformed."))
    try:
        parsed = BatchRunEvidence(**evidence) if evidence is not None else None
        return BatchExecutionReport(payload["primary"], parsed)
    except (TypeError, ValueError, ToolkitError) as error:
        raise InvalidInput("Batch execution report is invalid.") from error


def _batch_json(value: object) -> object:
    """Convert closed batch carriers to their exact JSON-compatible shape."""
    if is_dataclass(value) and not isinstance(value, type):
        return {field.name: _batch_json(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, Mapping):
        return {str(key): _batch_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_batch_json(item) for item in value]
    return value


def _execute_batch(
    coordinator: BatchCoordinator | None,
    command: str,
    payload: dict,
) -> tuple[BatchCoordinator, dict[str, object]]:
    if command == "batch.open":
        invalid = InvalidInput("Batch open input is invalid or a plan is already open.")
        if coordinator is not None:
            raise invalid
        return open_batches(_batch_plan(known_fields(payload, {"plan"}, invalid=invalid)["plan"])), {"opened": True}
    if coordinator is None:
        raise InvalidInput("Batch plan must be opened before this command.")
    if command == "batch.next":
        return coordinator, {"batch": _batch_json(coordinator.next())}
    if command == "batch.report":
        invalid = InvalidInput("Batch report input is invalid.")
        payload = known_fields(payload, {"batch_id", "report"}, invalid=invalid)
        if not isinstance(payload["batch_id"], str):
            raise invalid
        boundary = coordinator.report(payload["batch_id"], _batch_report(payload["report"]))
        return coordinator, {"accepted": True, "boundary_required": boundary}
    if command == "batch.boundary":
        invalid = InvalidInput("Batch boundary input is invalid.")
        payload = known_fields(payload, {"boundary_id", "outcome"}, invalid=invalid)
        if not all(isinstance(value, str) for value in payload.values()):
            raise invalid
        coordinator.report_boundary(payload["boundary_id"], payload["outcome"])
        return coordinator, {"accepted": True}
    if command == "batch.finish":
        return coordinator, {"aggregate": _batch_json(coordinator.finish())}
    raise InvalidInput("Batch bridge accepts only batch operations.")


def serve_batch(requests: BinaryIO, responses: TextIO) -> int:
    """Serve one serial process-local batch coordinator until finish or EOF."""
    coordinator: BatchCoordinator | None = None
    result_code = 0
    failed = False
    while raw := requests.readline(_MAX_LINE + 2):
        command = "unknown"
        try:
            if failed:
                raise InvalidInput("Batch bridge is aborted; no aggregate is available.")
            if len(raw) > _MAX_LINE + 1 or not raw.endswith(b"\n"):
                if not raw.endswith(b"\n"):
                    _discard_line(requests)
                raise InvalidInput("Bridge request exceeds its fixed line bound.")
            request = known_fields(_request(raw), _ENVELOPE, invalid=InvalidInput(_MALFORMED))
            command = request["command"]
            payload = request["input"]
            if not isinstance(command, str) or not isinstance(payload, dict):
                raise InvalidInput("Bridge request fields are invalid.")
            coordinator, result = _execute_batch(coordinator, command, payload)
            response = success_envelope(command, result)
            result_code = 0
        except (ToolkitError, OSError) as error:
            failed = True
            response, result_code = error_envelope(command, error)
        responses.write(json.dumps(response, sort_keys=True, separators=(",", ":")) + "\n")
        responses.flush()
    return result_code


def dispatch_failure(command: str, error: ToolkitError) -> tuple[str, int]:
    """Return the stderr text and exit code of a failed dispatch.

    A cleanup error that accompanies the primary one is reported on a second line; the exit code stays the primary's.
    """
    envelope, exit_code = error_envelope(command, error, cleanup=error.cleanup)
    lines = [str(envelope["error"]["message"])]
    if envelope["cleanup"] is not None:
        lines.append(str(envelope["cleanup"]["error"]["message"]))
    return "\n".join(lines) + "\n", exit_code


def dispatch(
    descriptor_path: Path,
    dispatcher_path: Path,
    shim_path: Path,
    argv: tuple[str, ...],
    *,
    consumer_grant: Path | None = None,
) -> int:
    """Execute one Docker-fronted command through its exact private authority."""
    command = "docker.dispatch"
    try:
        context, descriptor, identity = lifecycle.load_docker_dispatch(descriptor_path, dispatcher_path, shim_path)
        if consumer_grant is not None:
            lifecycle.validate_consumer_docker_dispatch(context, consumer_grant)
        adapter = bind_docker_adapter(identity)
        result = lifecycle.dispatch_docker(context, descriptor, adapter, argv)
    except ToolkitError as error:
        text, exit_code = dispatch_failure(command, error)
        os.write(2, text.encode())
        return exit_code
    if result.stdout:
        os.write(1, result.stdout.encode())
    if result.stderr:
        os.write(2, result.stderr.encode())
    return result.exit_code


def main(argv: list[str] | None = None) -> int:
    """Parse the fixed private dispatcher invocation."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--descriptor", type=Path, required=True)
    parser.add_argument("--dispatcher", type=Path, required=True)
    parser.add_argument("--shim", type=Path, required=True)
    parser.add_argument("--consumer-grant", type=Path)
    parser.add_argument("arguments", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    arguments = tuple(args.arguments[1:] if args.arguments[:1] == ["--"] else args.arguments)
    return dispatch(
        args.descriptor,
        args.dispatcher,
        args.shim,
        arguments,
        consumer_grant=args.consumer_grant,
    )


if __name__ == "__main__":
    sys.exit(main())
