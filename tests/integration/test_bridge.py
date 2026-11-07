from __future__ import annotations

import json
from io import BytesIO, StringIO
from pathlib import Path

import pytest

from docker_test_toolkit.adapters.inbound.bridge import serve_batch, serve_lease, serve_requests
from docker_test_toolkit.application import lifecycle
from docker_test_toolkit.application.operator import LeaseOperator
from docker_test_toolkit.application.protocol import CLI_PROTOCOL
from docker_test_toolkit.domain.errors import InvalidInput
from docker_test_toolkit.domain.models import SUPPORTED_PROTOCOLS, TOOLKIT_VERSION
from docker_test_toolkit.factories.testing import build_test_services

pytestmark = pytest.mark.integration


def _no_docker():
    raise AssertionError("protocol requests must not inspect Docker")


def _spec_payload(project: Path, purpose: str) -> dict:
    return {
        "project_root": str(project),
        "entrypoint": "bridge",
        "purpose": purpose,
        "services": [],
        "closure": [],
        "profile": "host",
        "phases": {"tests": {"container_bytes": 0, "host_process_bytes": 1, "daemon_cpus": 1, "host_threads": 1}},
        "initial_phase": "tests",
        "artifact": {
            "version": "0.1.0",
            "sha256": "a" * 64,
            "protocols": {
                "activation": 1,
                "batch": 1,
                "cli": 1,
                "distribution": 1,
                "queue": 1,
                "receipt": 1,
                "registry": 2,
                "run": 1,
            },
        },
        "external_resources": [],
        "retain": False,
        "wait_seconds": 20,
    }


def test_serial_bridge_emits_one_shared_envelope_per_request() -> None:
    source = BytesIO(b'{"command":"protocol","input":{}}\n{"command":"unsupported","input":{}}\n')
    target = StringIO()

    assert serve_requests(source, target, docker_factory=_no_docker) == 2
    responses = [json.loads(line) for line in target.getvalue().splitlines()]

    assert len(responses) == 2
    assert responses[0]["protocol"] == CLI_PROTOCOL
    assert responses[0]["command"] == "protocol"
    assert responses[0]["ok"] is True
    assert responses[1]["command"] == "unsupported"
    assert responses[1]["error"]["category"] == "invalid_input"


def test_lease_bridge_owns_one_run_across_serial_requests(tmp_path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    services = build_test_services(tmp_path / "state")
    spec = _spec_payload(project, "serial")
    source = BytesIO(
        b"".join(
            json.dumps(request, separators=(",", ":")).encode() + b"\n"
            for request in (
                {"command": "run.prepare", "input": spec},
                {"command": "run.enter", "input": {"docker": False}},
                {"command": "run.leave", "input": {"outcome": "passed"}},
            )
        )
    )
    target = StringIO()

    assert serve_lease(source, target, open_run_factory=services.open_run) == 0
    responses = [json.loads(line) for line in target.getvalue().splitlines()]

    assert [response["command"] for response in responses] == ["run.prepare", "run.enter", "run.leave"]
    assert all(response["ok"] for response in responses)
    assert responses[0]["result"]["run_id"] == responses[1]["result"]["run_id"]
    assert responses[1]["result"]["docker"] is None
    assert responses[2]["result"]["state"] == "complete"


def test_lease_bridge_disconnect_cancels_an_unentered_preparation(tmp_path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    state = tmp_path / "state"
    services = build_test_services(state)
    request = {"command": "run.prepare", "input": _spec_payload(project, "disconnect")}
    target = StringIO()

    assert serve_lease(BytesIO(json.dumps(request).encode() + b"\n"), target, open_run_factory=services.open_run) == 0
    response = json.loads(target.getvalue())
    record = json.loads((state / "registry/runs" / f"{response['result']['run_id']}.json").read_text())

    assert record["state"] == "complete"
    assert record["cleanup_outcome"] == "complete"


def test_lease_bridge_disconnect_fails_and_cleans_an_entered_run(tmp_path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    state = tmp_path / "state"
    services = build_test_services(state)
    source = BytesIO(
        b"".join(
            json.dumps(request, separators=(",", ":")).encode() + b"\n"
            for request in (
                {"command": "run.prepare", "input": _spec_payload(project, "entered-disconnect")},
                {"command": "run.enter", "input": {"docker": False}},
            )
        )
    )
    target = StringIO()

    assert serve_lease(source, target, open_run_factory=services.open_run) == 0
    responses = [json.loads(line) for line in target.getvalue().splitlines()]
    record = json.loads((state / "registry/runs" / f"{responses[0]['result']['run_id']}.json").read_text())

    assert record["state"] == "complete"
    assert record["test_outcome"] == "failed"
    assert record["cleanup_outcome"] == "complete"


def _batch_plan_payload(project: Path) -> dict:
    return {
        "project_root": str(project),
        "artifact_identity": {
            "version": TOOLKIT_VERSION,
            "sha256": "a" * 64,
            "protocols": dict(SUPPORTED_PROTOCOLS),
        },
        "segments": [
            {
                "segment_id": "first-segment",
                "items": [{"item_id": "first", "closure": []}],
                "trailing_boundary_id": None,
                "requires_previous_success": False,
            },
            {
                "segment_id": "second-segment",
                "items": [{"item_id": "second", "closure": []}],
                "trailing_boundary_id": "reset",
                "requires_previous_success": False,
            },
        ],
    }


def test_batch_bridge_matches_the_serial_facade_and_refuses_lifecycle_verbs(tmp_path: Path) -> None:
    source = BytesIO(
        b"".join(
            json.dumps(request, separators=(",", ":")).encode() + b"\n"
            for request in (
                {"command": "batch.open", "input": {"plan": _batch_plan_payload(tmp_path)}},
                {"command": "batch.next", "input": {}},
                {
                    "command": "batch.report",
                    "input": {
                        "batch_id": "batch-1",
                        "report": {"primary": "passed", "run_evidence": None},
                    },
                },
                {"command": "batch.next", "input": {}},
                {
                    "command": "batch.report",
                    "input": {
                        "batch_id": "batch-2",
                        "report": {"primary": "test_failed", "run_evidence": None},
                    },
                },
                {"command": "batch.boundary", "input": {"boundary_id": "reset", "outcome": "passed"}},
                {"command": "batch.next", "input": {}},
                {"command": "batch.finish", "input": {}},
                {"command": "run.prepare", "input": {}},
            )
        )
    )
    target = StringIO()

    assert serve_batch(source, target) == 2
    responses = [json.loads(line) for line in target.getvalue().splitlines()]

    assert all(response["ok"] for response in responses[:-1])
    assert responses[0]["result"] == {"opened": True}
    assert responses[2]["result"] == {"accepted": True, "boundary_required": False}
    assert responses[4]["result"] == {"accepted": True, "boundary_required": True}
    assert responses[6]["result"] == {"batch": None}
    assert responses[7]["result"]["aggregate"]["exit_code"] == 6
    assert responses[-1]["error"]["category"] == "invalid_input"


def test_batch_bridge_rejects_incomplete_input_and_discards_state_at_eof(tmp_path: Path) -> None:
    source = BytesIO(
        json.dumps({"command": "batch.open", "input": {"plan": _batch_plan_payload(tmp_path)}}).encode()
        + b"\n"
        + json.dumps({"command": "batch.boundary", "input": {"extra": True}}).encode()
        + b"\n"
        + json.dumps({"command": "batch.next", "input": {}}).encode()
        + b"\n"
    )
    target = StringIO()

    assert serve_batch(source, target) == 2
    responses = [json.loads(line) for line in target.getvalue().splitlines()]
    assert responses[0]["ok"] is True
    assert responses[1]["error"]["category"] == "invalid_input"
    assert responses[2]["error"]["category"] == "invalid_input"


class _ControlHandle:
    def __init__(self) -> None:
        self.calls: list[tuple] = []
        self.project_id = "a" * 64
        self.session_id = "11111111-1111-4111-8111-111111111111"
        self.run_id = "22222222-2222-4222-8222-222222222222"
        self.generation = 1

    def status(self):
        return lifecycle.RunStatus(self.run_id, "active", "tests", 1, 1, None, None, None)

    def register_container_control(self, caller_id, caller_generation, targets):
        self.calls.append(("register", caller_id, caller_generation, targets))
        return {caller_id: caller_generation, **dict.fromkeys(targets, caller_generation)}

    def validate_container_control(self, caller_id, caller_generation, targets):
        self.calls.append(("validate", caller_id, caller_generation, targets))

    def control_containers(self, operation_id, action, targets, expected, **keywords):
        self.calls.append(("control", operation_id, action, targets, expected, keywords))
        keywords["pre_dispatch"]()
        return expected


def test_lease_operator_binds_container_control_to_private_capability(
    monkeypatch,
) -> None:
    handle = _ControlHandle()
    peer = lifecycle.current_process(lifecycle.NativeClock())
    monkeypatch.setattr(lifecycle, "current_process", lambda clock: peer)
    operator = LeaseOperator(lambda _spec: handle, _no_docker)
    operator._handle = handle
    operator._entered = True
    generation = {"started_at": "2026-09-17T00:00:00Z", "restart_count": 0}

    registered = operator.execute(
        "container-control.register",
        {
            "caller_id": "a" * 64,
            "caller_generation": generation,
            "allowed_targets": ["b" * 64],
            "lifetime_ms": 30_000,
        },
    )
    result = operator.execute(
        "container-control.execute",
        {
            "capability": registered["capability"],
            "operation_id": "33333333-3333-4333-8333-333333333333",
            "action": "stop",
            "targets": ["b" * 64],
            "expected": {"b" * 64: generation},
            "request_sha256": "c" * 64,
        },
    )

    assert registered["lifetime_ms"] == 30_000
    assert result["generations"] == {"b" * 64: generation}
    assert [call[0] for call in handle.calls] == ["register", "validate", "control", "validate"]

    with pytest.raises(InvalidInput, match="capability"):
        operator.execute(
            "container-control.execute",
            {
                "capability": "44444444-4444-4444-8444-444444444444",
                "operation_id": "33333333-3333-4333-8333-333333333333",
                "action": "stop",
                "targets": ["b" * 64],
                "expected": {"b" * 64: generation},
                "request_sha256": "c" * 64,
            },
        )


def test_bridge_and_facade_yield_identical_union_plans(tmp_path: Path) -> None:
    from docker_test_toolkit.application.api import open_batches
    from docker_test_toolkit.domain.models import (
        ArtifactIdentity,
        BatchExecutionReport,
        BatchItem,
        BatchPlan,
        BatchRunEvidence,
        BatchSegment,
    )

    def evidence(batch_id: str) -> dict:
        return {
            "run_id": f"run-{batch_id}",
            "run_status": {
                "run_id": f"run-{batch_id}",
                "state": "complete",
                "admission": "revoked",
                "generation": 1,
                "revision": 2,
                "ticket": None,
                "test_outcome": "passed",
                "cleanup_outcome": "complete",
            },
            "category": None,
            "cleanup_category": None,
        }

    segments = [
        {
            "segment_id": "shared",
            "items": [
                {"item_id": "a", "closure": ["redis"]},
                {"item_id": "h", "closure": []},
                {"item_id": "b", "closure": ["postgres", "redis"]},
                {"item_id": "c", "closure": ["qdrant"]},
            ],
            "trailing_boundary_id": "reset",
            "requires_previous_success": False,
        },
        {
            "segment_id": "trailing",
            "items": [{"item_id": "d", "closure": ["qdrant", "postgres"]}],
            "trailing_boundary_id": None,
            "requires_previous_success": False,
        },
    ]
    payload = {**_batch_plan_payload(tmp_path), "segments": segments}
    keys = ("batch_id", "segment_id", "item_ids", "closure", "trailing_boundary_id")

    facade = open_batches(
        BatchPlan(
            tmp_path,
            ArtifactIdentity(TOOLKIT_VERSION, "a" * 64, dict(SUPPORTED_PROTOCOLS)),
            tuple(
                BatchSegment(
                    segment["segment_id"],
                    tuple(BatchItem(item["item_id"], tuple(item["closure"])) for item in segment["items"]),
                    segment["trailing_boundary_id"],
                    segment["requires_previous_success"],
                )
                for segment in segments
            ),
        )
    )
    expected = []
    while (batch := facade.next()) is not None:
        expected.append(
            {
                key: list(value) if isinstance(value, tuple) else value
                for key, value in zip(
                    keys,
                    (batch.batch_id, batch.segment_id, batch.item_ids, batch.closure, batch.trailing_boundary_id),
                    strict=True,
                )
            }
        )
        report = BatchExecutionReport("passed", BatchRunEvidence(**evidence(batch.batch_id)) if batch.closure else None)
        if facade.report(batch.batch_id, report):
            facade.report_boundary(batch.trailing_boundary_id, "passed")

    requests = [{"command": "batch.open", "input": {"plan": payload}}]
    for batch in expected:
        requests += [
            {"command": "batch.next", "input": {}},
            {
                "command": "batch.report",
                "input": {
                    "batch_id": batch["batch_id"],
                    "report": {
                        "primary": "passed",
                        "run_evidence": evidence(batch["batch_id"]) if batch["closure"] else None,
                    },
                },
            },
        ]
        if batch["trailing_boundary_id"]:
            requests.append(
                {
                    "command": "batch.boundary",
                    "input": {"boundary_id": batch["trailing_boundary_id"], "outcome": "passed"},
                }
            )
    target = StringIO()
    serve_batch(BytesIO(b"".join(json.dumps(request).encode() + b"\n" for request in requests)), target)
    responses = [json.loads(line) for line in target.getvalue().splitlines()]
    served = [response["result"]["batch"] for response in responses if "batch" in response.get("result", {})]

    assert [set(batch) for batch in served] == [set(keys)] * len(served)
    assert served == expected
    assert [batch["closure"] for batch in expected] == [["redis", "postgres", "qdrant"], [], ["qdrant", "postgres"]]
