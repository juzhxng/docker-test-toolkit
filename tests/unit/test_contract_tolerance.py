"""unknown request fields are ignored at every level; required fields and identities are still checked.

Each site case sends its valid payload once as is and once with ``{"future": true}`` added at one level, and compares
what reaches the next layer. Lease verbs run against a recording handle supplied through ``LeaseOperator``'s
``open_run_factory`` seam; one-shot locators stop at ``lifecycle._load_run``, the record read behind the context check
(harness seam: no run record exists in a unit test).
"""

from __future__ import annotations

import copy
import json
from dataclasses import dataclass
from io import BytesIO, StringIO
from pathlib import Path
from types import SimpleNamespace

import pytest

from docker_test_toolkit.adapters.inbound.bridge import serve_batch, serve_lease, serve_requests
from docker_test_toolkit.application import lifecycle, retirement
from docker_test_toolkit.application import operator as operator_module
from docker_test_toolkit.application.api import open_batches
from docker_test_toolkit.application.build_cache import remove_caches
from docker_test_toolkit.application.operator import LeaseOperator, execute
from docker_test_toolkit.application.protocol import error_envelope
from docker_test_toolkit.domain.errors import REASONS, InvalidInput, ResourceError
from docker_test_toolkit.domain.models import BatchItem, BatchPlan, BatchSegment

EXTRA = {"future": True}
CALLER = "c" * 64
TARGET = "d" * 64
RUN_ID = "22222222-2222-4222-8222-222222222222"
SESSION_ID = "11111111-1111-4111-8111-111111111111"


def _extended(value: object, path: tuple) -> object:
    """Return a copy of ``value`` with ``EXTRA`` merged into the object at ``path``."""
    result = copy.deepcopy(value)
    cursor = result
    for step in path:
        cursor = cursor[step]
    cursor.update(EXTRA)
    return result


# ---- lease verbs ----------------------------------------------------------------------------------------------------


class _RecordingHandle:
    """Stand in for a prepared run: every call is recorded, every result is a fixed status."""

    project_id = "p" * 64
    session_id = SESSION_ID
    run_id = RUN_ID
    generation = 1
    private_root = Path("/private")
    docker_context = SimpleNamespace(
        executable=Path("/docker"), environment={"PATH": "/bin"}, compose_project="c", builder="b"
    )

    def __init__(self, calls: list) -> None:
        self.calls = calls

    def _record(self, name: str, *args: object, **kwargs: object) -> lifecycle.RunStatus:
        self.calls.append((name, args, {key: value for key, value in kwargs.items() if not callable(value)}))
        return lifecycle.RunStatus(RUN_ID, "entered", "granted", 1, 1, None, None, None)

    def status(self):
        return self._record("status")

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
        if name == "register_container_control":
            return lambda *args: (self._record(name, *args), {TARGET: {"started_at": "t", "restart_count": 0}})[1]
        if name in {"register_consumer", "control_containers"}:
            return lambda *args, **kwargs: (self._record(name, *args, **kwargs), "value")[1]
        return lambda *args, **kwargs: self._record(name, *args, **kwargs)


def _spec() -> dict:
    return {
        "project_root": str(Path(__file__).parent),  # an existing directory
        "entrypoint": "bridge",
        "purpose": "tolerance",
        "services": [],
        "closure": [],
        "profile": "host",
        "phases": {"tests": {"container_bytes": 0, "host_process_bytes": 1, "daemon_cpus": 1, "host_threads": 1}},
        "initial_phase": "tests",
        "external_resources": [
            {"kind": "network", "identifier": "e" * 64, "name": "shared", "relationship": "inspect"},
        ],
        "retain": False,
        "wait_seconds": 20,
    }


def _identity() -> dict:
    return {"origin": "boot", "pid": 1, "birth": [1, 2], "audit_token": None}


_LEASE = (
    ("run.prepare", _spec),
    ("run.enter", lambda: {"docker": True}),
    ("run.status", dict),
    ("run.transition", lambda: {"phase": "tests"}),
    ("run.attach", lambda: {"identity": _identity(), "services": ["db"]}),
    ("consumer.register", lambda: {"identity": _identity(), "services": ["db"]}),
    ("consumer.complete", lambda: {"consumer_id": "consumer"}),
    (
        "container-control.register",
        lambda: {
            "caller_id": CALLER,
            "caller_generation": {"started_at": "t", "restart_count": 0},
            "allowed_targets": [TARGET],
            "lifetime_ms": 60_000,
        },
    ),
    (
        "container-control.execute",
        lambda: {
            "capability": None,  # filled in from the registration's result
            "operation_id": "operation",
            "action": "stop",
            "targets": [TARGET],
            "expected": {TARGET: {"started_at": "t", "restart_count": 0}},
            "request_sha256": "f" * 64,
        },
    ),
    ("endpoint.publish", lambda: {"service": "db", "protocol": "tcp", "container_port": 5432, "namespace": "n"}),
    ("endpoint.require", lambda: {"service": "db", "namespace": "n"}),
    ("run.leave", lambda: {"outcome": "passed"}),
    ("run.resume", dict),
    ("run.retain", lambda: {"outcome": "passed"}),
    ("run.release", dict),
)
_CANCEL = (("run.prepare", _spec), ("run.cancel", dict))


def _lease_transcript(sequence: tuple, site: tuple[str, tuple] | None) -> tuple[list, list]:
    calls: list = []
    operator = LeaseOperator(lambda spec: (calls.append(("open", (spec,), {})), _RecordingHandle(calls))[1], None)
    results = []
    capability = None
    for command, payload_of in sequence:
        payload = payload_of()
        if command == "container-control.execute":
            payload["capability"] = capability
        if site is not None and site[0] == command:
            payload = _extended(payload, site[1])
        result = operator.execute(command, payload)
        if command == "container-control.register":
            capability = result["capability"]
            result = {**result, "capability": "masked"}
        results.append(result)
    return results, calls


_LEASE_SITES = [
    ("run.prepare", ()),
    ("run.prepare", ("phases", "tests")),
    ("run.prepare", ("external_resources", 0)),
    ("run.enter", ()),
    ("run.status", ()),
    ("run.transition", ()),
    ("run.attach", ()),
    ("run.attach", ("identity",)),
    ("consumer.register", ()),
    ("consumer.register", ("identity",)),
    ("consumer.complete", ()),
    ("container-control.register", ()),
    ("container-control.register", ("caller_generation",)),
    ("container-control.execute", ()),
    ("container-control.execute", ("expected", TARGET)),
    ("endpoint.publish", ()),
    ("endpoint.require", ()),
    ("run.leave", ()),
    ("run.resume", ()),
    ("run.retain", ()),
    ("run.release", ()),
]


@pytest.mark.parametrize("site", _LEASE_SITES, ids=lambda site: ".".join((site[0], *map(str, site[1]))) or None)
def test_lease_extra_is_accepted(site: tuple[str, tuple]) -> None:
    assert _lease_transcript(_LEASE, site) == _lease_transcript(_LEASE, None)


def test_lease_extra_is_accepted_on_cancel() -> None:
    assert _lease_transcript(_CANCEL, ("run.cancel", ())) == _lease_transcript(_CANCEL, None)


@pytest.mark.parametrize(
    ("command", "payload"),
    [
        ("run.prepare", {key: value for key, value in _spec().items() if key != "profile"}),
        ("run.transition", {"future": True}),
        ("consumer.register", {"identity": {"origin": "boot", "pid": 1, "birth": [1]}, "services": []}),
        ("container-control.register", {"caller_id": CALLER, "allowed_targets": [], "lifetime_ms": 1}),
    ],
    ids=["prepare-without-profile", "transition-without-phase", "identity-without-audit-token", "control-no-gen"],
)
def test_missing_required_field_is_still_invalid(command: str, payload: dict) -> None:
    calls: list = []
    operator = LeaseOperator(lambda spec: _RecordingHandle(calls), None)
    if command != "run.prepare":
        operator.execute("run.prepare", _spec())

    with pytest.raises(InvalidInput):
        operator.execute(command, payload)


def test_changed_capability_is_still_refused() -> None:
    operator = LeaseOperator(lambda spec: _RecordingHandle([]), None)
    operator.execute("run.prepare", _spec())
    payload = dict(_LEASE[8][1](), capability="00000000-0000-4000-8000-000000000000")

    with pytest.raises(InvalidInput, match="capability is invalid"):
        operator.execute("container-control.execute", payload)


def test_changed_caller_identity_is_still_refused() -> None:
    generation = {"started_at": "t", "restart_count": 0}
    resources = {
        "containers": {
            CALLER: {"state": "running", "observation": dict(generation)},
            TARGET: {"state": "running", "observation": dict(generation)},
        }
    }
    lifecycle._validate_control_generation(resources, CALLER, generation, (TARGET,))

    for caller, changed in (("a" * 64, generation), (CALLER, {"started_at": "t", "restart_count": 1})):
        with pytest.raises(ResourceError):
            lifecycle._validate_control_generation(resources, caller, changed, (TARGET,))
    with pytest.raises(ResourceError):  # the internal validator stays exact: callers normalize before it
        lifecycle._validate_control_generation(resources, CALLER, {**generation, **EXTRA}, (TARGET,))


# ---- envelopes ------------------------------------------------------------------------------------------------------


def _no_docker():
    raise AssertionError("protocol must remain daemon-free")


def test_envelope_extra_is_accepted_on_every_bridge() -> None:
    request = json.dumps({"command": "protocol", "input": {}, **EXTRA}).encode() + b"\n"
    plain = json.dumps({"command": "protocol", "input": {}}).encode() + b"\n"

    one_shot, lease = StringIO(), StringIO()
    assert serve_requests(BytesIO(request), one_shot, docker_factory=_no_docker) == 0
    assert serve_lease(BytesIO(request), lease, open_run_factory=None, docker_factory=_no_docker) == 0
    expected = StringIO()
    serve_requests(BytesIO(plain), expected, docker_factory=_no_docker)

    assert one_shot.getvalue() == lease.getvalue() == expected.getvalue()
    assert json.loads(one_shot.getvalue())["ok"] is True


def test_protocol_input_extra_is_ignored() -> None:
    assert execute("protocol", EXTRA, docker_factory=_no_docker) == execute("protocol", {}, docker_factory=_no_docker)


# ---- batch bridge ---------------------------------------------------------------------------------------------------


def _batch_requests(tmp_path: Path) -> list[dict]:
    plan = {
        "project_root": str(tmp_path),
        "segments": [
            {
                "segment_id": "s",
                "items": [{"item_id": "i", "closure": ["redis"]}],
                "trailing_boundary_id": "reset",
                "requires_previous_success": False,
            }
        ],
    }
    coordinator = open_batches(
        BatchPlan(tmp_path, None, (BatchSegment("s", (BatchItem("i", ("redis",)),), "reset", False),))
    )
    batch_id = coordinator.next().batch_id
    status = {
        "run_id": "run",
        "state": "complete",
        "admission": "revoked",
        "generation": 1,
        "revision": 2,
        "ticket": None,
        "test_outcome": "passed",
        "cleanup_outcome": "complete",
    }
    evidence = {"run_id": "run", "run_status": status, "category": None, "cleanup_category": None}
    return [
        {"command": "batch.open", "input": {"plan": plan}},
        {"command": "batch.next", "input": {}},
        {
            "command": "batch.report",
            "input": {"batch_id": batch_id, "report": {"primary": "passed", "run_evidence": evidence}},
        },
        {"command": "batch.boundary", "input": {"boundary_id": "reset", "outcome": "passed"}},
        {"command": "batch.next", "input": {}},
        {"command": "batch.finish", "input": {}},
    ]


def _batch_transcript(requests: list[dict]) -> list[dict]:
    target = StringIO()
    serve_batch(BytesIO(b"".join(json.dumps(request).encode() + b"\n" for request in requests)), target)
    return [json.loads(line) for line in target.getvalue().splitlines()]


_BATCH_SITES = {
    "envelope": (0, ()),
    "batch.open": (0, ("input",)),
    "batch.open.plan": (0, ("input", "plan")),
    "batch.open.plan.segment": (0, ("input", "plan", "segments", 0)),
    "batch.open.plan.segment.item": (0, ("input", "plan", "segments", 0, "items", 0)),
    "batch.next": (1, ("input",)),
    "batch.report": (2, ("input",)),
    "batch.report.report": (2, ("input", "report")),
    "batch.report.run_evidence": (2, ("input", "report", "run_evidence")),
    "batch.report.run_evidence.run_status": (2, ("input", "report", "run_evidence", "run_status")),
    "batch.boundary": (3, ("input",)),
    "batch.finish": (5, ("input",)),
}


@pytest.mark.parametrize("site", sorted(_BATCH_SITES))
def test_batch_extra_is_accepted(tmp_path: Path, site: str) -> None:
    requests = _batch_requests(tmp_path)
    index, path = _BATCH_SITES[site]
    extended = [*requests]
    extended[index] = _extended(requests[index], path)

    expected = _batch_transcript(requests)
    assert all(response["ok"] for response in expected)
    assert _batch_transcript(extended) == expected


def test_batch_run_status_missing_a_field_is_still_invalid(tmp_path: Path) -> None:
    requests = _batch_requests(tmp_path)
    del requests[2]["input"]["report"]["run_evidence"]["run_status"]["ticket"]

    responses = _batch_transcript(requests)

    assert responses[2]["error"]["category"] == "invalid_input"


# ---- one-shot operator verbs ----------------------------------------------------------------------------------------


@pytest.fixture
def locator(monkeypatch: pytest.MonkeyPatch) -> dict:
    """A registry-bound locator whose record read is recorded instead of performed."""
    state = Path("/state")
    monkeypatch.setattr(operator_module, "registry_directory", lambda: state)
    loaded: list = []
    monkeypatch.setattr(lifecycle, "_load_run", lambda context: (loaded.append(context), {})[1])
    for service in ("run_status", "recover_run", "release_run"):
        monkeypatch.setattr(lifecycle, service, lambda context, _name=service: {"service": _name, "context": context})
    monkeypatch.setattr(operator_module, "asdict", lambda value: value)
    return {
        "project_root": "/project",
        "project_state": str(state),
        "run_id": RUN_ID,
        "session_id": SESSION_ID,
        "record": f"/state/runs/{RUN_ID}.json",
        "private_root": "/private",
    }


@pytest.mark.parametrize("command", ["run.status", "run.recover", "run.release"])
@pytest.mark.parametrize("shape", ["locator", "context", "context.envelope"])
def test_locator_extra_is_accepted(locator: dict, command: str, shape: str) -> None:
    plain = locator if shape == "locator" else {"context": locator}
    extended = {
        "locator": {**locator, **EXTRA},
        "context": {"context": {**locator, **EXTRA}},
        "context.envelope": {"context": locator, **EXTRA},
    }[shape]

    assert execute(command, extended, docker_factory=None) == execute(command, plain, docker_factory=None)


@pytest.mark.parametrize(
    "change",
    [{"run_id": "33333333-3333-4333-8333-333333333333"}, {"record": "/state/runs/other.json"}, {"project_state": "/x"}],
    ids=["run_id", "record", "project_state"],
)
def test_changed_locator_identity_is_still_refused(locator: dict, change: dict) -> None:
    with pytest.raises(InvalidInput, match="outside the registry"):
        execute("run.status", {**locator, **EXTRA, **change}, docker_factory=None)


@pytest.mark.parametrize("change", [{"project_state": "/x"}, {"record": "/elsewhere/runs/x.json"}])
def test_resource_observation_is_bound_to_the_registry(locator: dict, change: dict) -> None:
    observation = {"context": {**locator, **change}, "kind": "container", "identifier": "a" * 64}

    with pytest.raises(InvalidInput, match="outside the registry"):  # before any record read or Docker inspection
        execute("resource.observe", observation, docker_factory=None)


def test_locator_missing_a_field_is_still_invalid(locator: dict) -> None:
    incomplete = {key: value for key, value in locator.items() if key != "session_id"}

    with pytest.raises(InvalidInput, match="run context"):
        execute("run.status", {**incomplete, **EXTRA}, docker_factory=None)


def test_resource_observe_extra_is_accepted(locator: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        lifecycle,
        "load_run_record",
        lambda context: {"owned_resources": {"containers": {TARGET: {}}}, "external_resources": []},
    )
    observed: list = []

    def docker_factory():
        return SimpleNamespace(
            inspect_exact=lambda inspection: (observed.append(inspection), SimpleNamespace(exists=True))[1]
        )

    payload = {"context": locator, "kind": "container", "identifier": TARGET}
    plain = execute("resource.observe", payload, docker_factory=docker_factory)
    for extended in (_extended(payload, ()), _extended(payload, ("context",))):
        assert execute("resource.observe", extended, docker_factory=docker_factory) == plain
    assert len(observed) == 3 and observed[0] == observed[1] == observed[2]


def test_retained_locator_file_extra_is_accepted(locator: dict, tmp_path: Path) -> None:
    from docker_test_toolkit.application.api import _read_retained_locator

    plain, extended = tmp_path / "plain.json", tmp_path / "extended.json"
    for path, value in ((plain, locator), (extended, {**locator, **EXTRA})):
        path.write_text(json.dumps(value))
        path.chmod(0o600)

    assert _read_retained_locator(extended)[1] == _read_retained_locator(plain)[1]


@pytest.mark.parametrize("command", ["project.timeline", "cleanup.project", "cleanup.session"])
def test_project_verb_extra_is_accepted(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, command: str) -> None:
    received: list = []
    monkeypatch.setattr(operator_module, "registry_directory", lambda: tmp_path / "state")
    monkeypatch.setattr(operator_module, "project_timeline", lambda *args: (received.append(args), {})[1])
    monkeypatch.setattr(operator_module, "cleanup_project", lambda *args, **kwargs: (received.append(args), {})[1])
    monkeypatch.setattr(operator_module, "cleanup_session", lambda *args, **kwargs: (received.append(args), {})[1])
    payload = {"project_root": str(tmp_path)}
    if command == "cleanup.session":
        payload["session_id"] = SESSION_ID

    execute(command, payload, docker_factory=lambda: "docker")
    execute(command, {**payload, **EXTRA}, docker_factory=lambda: "docker")

    assert len(received) == 2 and received[0] == received[1]


@pytest.mark.parametrize("payload", [{"project_root": None}, {"orphans": "all"}], ids=["project_root", "orphans"])
def test_cache_remove_extra_is_accepted(tmp_path: Path, payload: dict) -> None:
    if payload == {"project_root": None}:
        payload = {"project_root": str(tmp_path)}

    assert remove_caches(tmp_path / "state", {**payload, **EXTRA}, execute=False) == remove_caches(
        tmp_path / "state", payload, execute=False
    )


def test_cache_remove_with_both_selectors_is_still_invalid(tmp_path: Path) -> None:
    with pytest.raises(InvalidInput):
        remove_caches(tmp_path, {"project_root": str(tmp_path), "orphans": "all", **EXTRA}, execute=False)


def test_retirement_request_extra_is_accepted() -> None:
    project = "/project"
    project_id = retirement.project_id_for(project)
    evidence = {
        "project_id": project_id,
        "project_root": project,
        "project_record_sha256": "a" * 64,
        "runs": [{"run_id": RUN_ID, "sha256": "b" * 64}],
    }

    assert retirement._parse_request(EXTRA, execute=False) == retirement._parse_request({}, execute=False)
    assert retirement._parse_request({"project_ids": [project_id], **EXTRA}, execute=False) == ("ids", [project_id])
    plain = retirement._parse_request({"evidence": [evidence]}, execute=True)
    for extended in (
        {"evidence": [evidence], **EXTRA},
        {"evidence": [{**evidence, **EXTRA}]},
        {"evidence": [{**evidence, "runs": [{**evidence["runs"][0], **EXTRA}]}]},
    ):
        assert retirement._parse_request(extended, execute=True) == plain


def test_retirement_evidence_missing_a_field_is_still_invalid() -> None:
    with pytest.raises(InvalidInput):
        retirement._parse_request({"evidence": [{"project_id": "a" * 64, **EXTRA}]}, execute=True)


# ---- reasons --------------------------------------------------------------------------------------------------------


def test_claim_held_is_a_listed_reason() -> None:
    envelope, _ = error_envelope("run.enter", ResourceError("held", reason="claim-held"))

    assert "claim-held" in REASONS
    assert envelope["error"]["context"]["reason"] == "claim-held"


def test_a_defaulted_constructor_field_stays_optional_on_the_wire() -> None:
    """A field a later version adds with a default never becomes required for older callers (rule 3)."""

    @dataclass(frozen=True)
    class Later:
        required: int
        added: int = 0

    invalid = InvalidInput("invalid")

    assert operator_module._construct(Later, {"required": 1, **EXTRA}, invalid) == Later(1)
    assert operator_module._construct(Later, {"required": 1, "added": 2}, invalid) == Later(1, 2)
    with pytest.raises(InvalidInput):
        operator_module._construct(Later, {"added": 2}, invalid)
