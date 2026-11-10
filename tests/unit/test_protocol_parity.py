from __future__ import annotations

import json
from io import BytesIO, StringIO
from pathlib import Path
from types import SimpleNamespace

import pytest

from docker_test_toolkit.adapters.inbound.bridge import serve_requests
from docker_test_toolkit.adapters.inbound.cli import _parse, main
from docker_test_toolkit.application import lifecycle
from docker_test_toolkit.application import operator as operator_module
from docker_test_toolkit.application.operator import execute
from docker_test_toolkit.application.protocol import CLI_PROTOCOL, error_envelope, success_envelope
from docker_test_toolkit.domain.errors import (
    IncompleteCleanup,
    InfrastructureFailure,
    InvalidInput,
    ResourceBusy,
    ResourceError,
    ToolkitError,
)


def test_success_envelope_is_closed_and_machine_stable(capsys: pytest.CaptureFixture[str]) -> None:
    assert success_envelope("protocol", {"version": "0.1.0"}) == {
        "protocol": CLI_PROTOCOL,
        "command": "protocol",
        "ok": True,
        "result": {"version": "0.1.0"},
        "cleanup": None,
    }

    assert main(["protocol", "--json"]) == 0
    captured = capsys.readouterr()
    response = json.loads(captured.out)
    assert response["protocol"] == CLI_PROTOCOL
    assert response["command"] == "protocol"
    assert response["ok"] is True
    assert response["cleanup"] is None
    assert captured.out.count("\n") == 1
    assert captured.err == ""


def test_application_cli_and_bridge_share_the_exact_protocol_result(capsys: pytest.CaptureFixture[str]) -> None:
    def no_docker():
        raise AssertionError("protocol must remain daemon-free")

    application = execute("protocol", {}, docker_factory=no_docker)
    assert main(["protocol", "--json"]) == 0
    one_shot = json.loads(capsys.readouterr().out)
    output = StringIO()
    assert (
        serve_requests(
            BytesIO(b'{"command":"protocol","input":{}}\n'),
            output,
            docker_factory=no_docker,
        )
        == 0
    )
    serial = json.loads(output.getvalue())

    assert one_shot["result"] == application == serial["result"]


def test_cli_parse_failure_is_one_machine_envelope(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["protocol"]) == 2
    captured = capsys.readouterr()
    response = json.loads(captured.out)

    assert response["command"] == "unknown"
    assert response["error"]["category"] == "invalid_input"
    assert captured.out.count("\n") == 1
    assert captured.err == ""


def test_cli_file_input_is_bounded_before_reading_to_eof(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["cleanup", "project", "--input", str(Path("/dev/zero")), "--json"]) == 2
    response = json.loads(capsys.readouterr().out)

    assert response["error"]["category"] == "invalid_input"
    assert response["error"]["message"] == "Command input exceeds its fixed bound."


def test_bridge_translates_excessively_nested_json() -> None:
    output = StringIO()
    request = b'{"command":"protocol","input":' + b"[" * 10_000 + b"]" * 10_000 + b"}\n"

    assert serve_requests(BytesIO(request), output) == 2
    response = json.loads(output.getvalue())

    assert response["error"]["category"] == "invalid_input"


def test_bridge_discards_the_remainder_of_an_oversized_physical_line() -> None:
    valid = b'{"command":"protocol","input":{}}\n'
    requests = BytesIO(b"A" * (1024 * 1024 + 2) + valid + valid)
    output = StringIO()

    assert serve_requests(requests, output) == 0
    responses = [json.loads(line) for line in output.getvalue().splitlines()]

    assert len(responses) == 2
    assert responses[0]["error"]["category"] == "invalid_input"
    assert responses[1]["ok"] is True


def test_machine_boundaries_translate_embedded_nul_paths(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    payload = {"project_root": "\0"}
    request = tmp_path / "request.json"
    request.write_text(json.dumps(payload))

    assert main(["cleanup", "project", "--input", str(request), "--json"]) == 2
    assert json.loads(capsys.readouterr().out)["error"]["category"] == "invalid_input"

    output = StringIO()
    raw = json.dumps({"command": "cleanup.project", "input": payload}).encode() + b"\n"
    assert serve_requests(BytesIO(raw), output) == 2
    assert json.loads(output.getvalue())["error"]["category"] == "invalid_input"

    with pytest.raises(InvalidInput, match="cleanup path"):
        execute("cleanup.project", {"project_root": "\0"}, docker_factory=lambda: None)


def test_operator_translates_excessively_nested_context() -> None:
    context: dict[str, object] = {}
    cursor = context
    for _ in range(10_000):
        nested: dict[str, object] = {}
        cursor["nested"] = nested
        cursor = nested

    with pytest.raises(InvalidInput, match="run context"):
        execute("run.status", {"context": context}, docker_factory=lambda: None)

    with pytest.raises(ResourceError):
        lifecycle.load_operator_context("[" * 10_000 + "]" * 10_000)


def test_one_shot_run_commands_accept_a_direct_retained_locator(monkeypatch: pytest.MonkeyPatch) -> None:
    locator = {
        "project_root": "/project",
        "project_state": "/state",
        "run_id": "22222222-2222-4222-8222-222222222222",
        "session_id": "11111111-1111-4111-8111-111111111111",
        "record": "/state/runs/22222222-2222-4222-8222-222222222222.json",
        "private_root": "/private",
    }
    monkeypatch.setattr(operator_module, "registry_directory", lambda: Path("/state"))
    loaded = object()
    status = lifecycle.RunStatus("run", "stopped-retained", "idle", 1, 2, None, "passed", None)
    monkeypatch.setattr(lifecycle, "load_operator_context", lambda payload: loaded)
    monkeypatch.setattr(lifecycle, "run_status", lambda selected: status if selected is loaded else None)

    assert execute("run.status", locator, docker_factory=lambda: None)["state"] == "stopped-retained"


def test_readiness_cli_is_the_input_free_default_operation() -> None:
    assert _parse([]) == ("readiness.inspect", None, False)
    assert _parse(["readiness", "inspect", "--json"]) == ("readiness.inspect", None, False)
    with pytest.raises(InvalidInput, match="does not accept input"):
        _parse(["readiness", "inspect", "--input", "-", "--json"])


@pytest.mark.parametrize(
    ("error", "category", "exit_code", "next_action", "reason"),
    [
        (InvalidInput("bad request"), "invalid_input", 2, "check_input", "unspecified"),
        (ResourceError("wrong authority"), "incompatible_state", 3, "inspect_status", "unspecified"),
        (ResourceBusy("occupied"), "contention", 4, "inspect_status", "unspecified"),
        pytest.param(
            ResourceError("blocked", reason="blocked-entry"),
            "incompatible_state",
            3,
            "inspect_status",
            "blocked-entry",
            id="error4-incompatible_state-3-inspect_status-blocked-entry",
        ),
        pytest.param(
            ResourceBusy("slow", reason="lock-deadline"),
            "contention",
            4,
            "inspect_status",
            "lock-deadline",
            id="error5-contention-4-inspect_status-lock-deadline",
        ),
        pytest.param(
            ResourceError("odd", reason="not-in-the-vocabulary"),
            "incompatible_state",
            3,
            "inspect_status",
            "unspecified",
            id="error6-incompatible_state-3-inspect_status-unspecified",
        ),
        pytest.param(
            RuntimeError("plain exception"),
            "infrastructure_failure",
            5,
            "inspect_status",
            "unspecified",
            id="error7-infrastructure_failure-5-inspect_status-unspecified",
        ),
    ],
)
def test_error_envelope_maps_stable_exit_groups(
    error: Exception, category: str, exit_code: int, next_action: str, reason: str
) -> None:
    envelope, observed = error_envelope("run.enter", error)

    assert observed == exit_code
    assert envelope == {
        "protocol": CLI_PROTOCOL,
        "command": "run.enter",
        "ok": False,
        "error": {
            "category": category,
            "message": str(error),
            "context": {"next_action": next_action, "reason": reason},
        },
        "cleanup": None,
    }


@pytest.mark.parametrize("reason", ("observation-deadline", "telemetry-unreadable"))
def test_envelope_carries_observation_reasons(reason: str) -> None:
    envelope, _ = error_envelope("run.enter", ResourceError("x", reason=reason))

    assert envelope["error"]["context"]["reason"] == reason


def test_envelope_carries_the_identity_conflict() -> None:
    conflict = {"kind": "volume", "identifier": "v", "name": "v", "keys": ["evidence.created"]}

    envelope, _ = error_envelope(
        "run.recover", ResourceError("x", reason="owned-identity-conflict", details={"conflict": conflict})
    )

    assert envelope["error"]["context"]["reason"] == "owned-identity-conflict"
    assert envelope["error"]["context"]["conflict"] == conflict


def test_identity_conflict_message_keeps_its_resource_within_the_cut() -> None:
    resource = {"kind": "container", "identifier": "c" * 64, "name": "n"}
    error = lifecycle._identity_conflict(
        "Docker promotion collides with another exact owned identity.",
        resource,
        [f"evidence.key{index:02d}" for index in range(40)],
    )

    envelope, _ = error_envelope("run.recover", error)

    assert len(envelope["error"]["message"]) == 512
    assert f"Resource: container {'c' * 64};" in envelope["error"]["message"]


def test_every_reason_the_toolkit_raises_is_in_the_closed_vocabulary() -> None:
    import re
    from pathlib import Path

    from docker_test_toolkit.domain.errors import REASONS

    source = Path(__file__).parents[2] / "src" / "docker_test_toolkit"
    used = {match for path in source.rglob("*.py") for match in re.findall(r'reason="([a-z-]+)"', path.read_text())}
    assert used <= REASONS, f"reason codes missing from REASONS: {sorted(used - REASONS)}"


def test_every_observer_the_toolkit_names_is_in_the_closed_vocabulary() -> None:
    import re
    from pathlib import Path

    from docker_test_toolkit.domain.telemetry import OBSERVERS

    source = Path(__file__).parents[2] / "src" / "docker_test_toolkit"
    patterns = (
        r'observer="([a-z-]+)"',
        r'_attributed\("([a-z-]+)"\)',
        r'TelemetryFault\("[a-z-]+", "([a-z-]+)"',
    )
    used = {
        match
        for path in source.rglob("*.py")
        for pattern in patterns
        for match in re.findall(pattern, path.read_text())
    }
    assert used and used <= set(OBSERVERS), f"observers outside OBSERVERS: {sorted(used - set(OBSERVERS))}"


def test_cleanup_failure_stays_separate_from_primary_failure() -> None:
    envelope, exit_code = error_envelope(
        "run.leave",
        ResourceBusy("primary contention"),
        cleanup=ResourceError("cleanup incomplete"),
    )

    assert exit_code == 4
    assert envelope["error"]["category"] == "contention"
    assert envelope["cleanup"] == {
        "ok": False,
        "error": {
            "category": "incomplete_cleanup",
            "message": "cleanup incomplete",
            "context": {"next_action": "recover", "reason": "unspecified"},
        },
    }


def test_python_errors_expose_the_same_stable_categories_as_cli_envelopes() -> None:
    assert ToolkitError.category == "infrastructure_failure"
    assert ResourceError.category == "incompatible_state"
    assert ResourceBusy.category == "contention"
    assert InvalidInput.category == "invalid_input"
    assert InfrastructureFailure.category == "infrastructure_failure"
    assert IncompleteCleanup.category == "incomplete_cleanup"


def test_project_retire_cli_is_closed_and_execute_is_explicit() -> None:
    assert _parse(["project", "retire", "--input", "r.json", "--json"]) == ("project.retire", "r.json", False)
    assert _parse(["project", "retire", "--input", "r.json", "--execute", "--json"]) == (
        "project.retire",
        "r.json",
        True,
    )
    with pytest.raises(InvalidInput, match="requires --input"):
        _parse(["project", "retire", "--json"])
    with pytest.raises(InvalidInput, match="only to cleanup"):
        _parse(["run", "status", "--input", "r", "--execute", "--json"])
    with pytest.raises(InvalidInput, match="unsupported"):
        _parse(["project", "archive", "--input", "r.json", "--json"])


def test_project_timeline_cli_is_closed_and_refuses_execute() -> None:
    assert _parse(["project", "timeline", "--input", "t.json", "--json"]) == ("project.timeline", "t.json", False)
    with pytest.raises(InvalidInput, match="only to cleanup"):
        _parse(["project", "timeline", "--input", "t.json", "--execute", "--json"])
    with pytest.raises(InvalidInput, match="requires --input"):
        _parse(["project", "timeline", "--json"])


def test_cache_remove_cli_is_closed_and_execute_is_explicit(real_account: Path) -> None:
    assert _parse(["cache", "remove", "--input", "c.json", "--json"]) == ("cache.remove", "c.json", False)
    assert _parse(["cache", "remove", "--input", "c.json", "--execute", "--json"]) == ("cache.remove", "c.json", True)
    with pytest.raises(InvalidInput, match="requires --input"):
        _parse(["cache", "remove", "--json"])
    with pytest.raises(InvalidInput, match="unsupported"):
        _parse(["cache", "purge", "--input", "c.json", "--json"])
    for payload in ({}, {"orphans": "some"}, {"project_root": 1}, {"project_root": "/x", "orphans": "all"}):
        with pytest.raises(InvalidInput, match="Cache removal input is invalid"):
            execute("cache.remove", payload, docker_factory=lambda: None)


def test_serial_bridge_refuses_project_retirement() -> None:
    output = StringIO()
    request = json.dumps({"command": "project.retire", "input": {}}).encode() + b"\n"

    assert serve_requests(BytesIO(request), output) == 2
    response = json.loads(output.getvalue())

    assert response["error"]["category"] == "invalid_input"
    assert response["error"]["message"] == "Project retirement runs only through the operator command line."


def test_results_carry_the_toolkit_version(tmp_path: Path) -> None:
    """An additive result field that every project must tolerate unchanged."""
    from docker_test_toolkit.application.operator import LeaseOperator
    from docker_test_toolkit.domain.models import TOOLKIT_VERSION

    assert execute("protocol", {}, docker_factory=lambda: None)["toolkit"] == {"version": TOOLKIT_VERSION}

    status = lifecycle.RunStatus("run", "prepared", "pending", 0, 0, None, None, None)
    handle = SimpleNamespace(
        project_id="p" * 64,
        session_id="s",
        run_id="run",
        generation=1,
        private_root=tmp_path,
        status=lambda: status,
        enter=lambda: None,
        toolkit_version=TOOLKIT_VERSION,
    )
    operator = LeaseOperator(lambda spec: handle, lambda: None)
    spec = {
        "project_root": str(tmp_path),
        "entrypoint": "parity",
        "purpose": "parity",
        "services": [],
        "closure": [],
        "profile": "parity",
        "phases": {"tests": {"container_bytes": 0, "host_process_bytes": 1, "daemon_cpus": 1, "host_threads": 1}},
        "initial_phase": "tests",
        "external_resources": [],
        "retain": False,
        "wait_seconds": 20,
    }

    assert operator.execute("run.prepare", spec)["toolkit"] == {"version": TOOLKIT_VERSION}
    assert operator.execute("run.enter", {"docker": False})["toolkit"] == {"version": TOOLKIT_VERSION}


def test_error_details_reach_the_context_and_never_replace_its_closed_keys() -> None:
    runs = [{"run_id": "r"}]
    payload, code = _details_envelope(
        ResourceError("x", details={"runs": runs, "reason": "forged", "next_action": "x"})
    )

    assert code == 3
    assert payload["error"]["context"] == {"next_action": "inspect_status", "reason": "unspecified", "runs": runs}
    assert _details_envelope(ResourceError("x"))[0]["error"]["context"].keys() == {"next_action", "reason"}


def _details_envelope(error: ResourceError) -> tuple[dict, int]:
    from docker_test_toolkit.application.protocol import error_envelope

    return error_envelope("cleanup.project", error)


def test_an_error_message_redacts_a_credential_that_straddles_the_length_cut() -> None:
    message = error_envelope("docker.dispatch", InfrastructureFailure("x" * 495 + " https://user:pa55word@host/path"))[
        0
    ]

    text = message["error"]["message"]
    assert "pa55word" not in text and "user:pa" not in text
    assert len(text) <= 512


def test_an_error_message_stays_within_512_characters_after_redaction() -> None:
    message = error_envelope("docker.dispatch", InfrastructureFailure("token=abc " * 80))[0]

    assert len(message["error"]["message"]) <= 512
