from __future__ import annotations

import json
import shutil
import time
import uuid
from dataclasses import asdict
from pathlib import Path

import pytest

from docker_test_toolkit import ArtifactIdentity, PhaseBudget, RunSpec
from docker_test_toolkit.adapters.inbound import cli
from docker_test_toolkit.adapters.inbound.cli import main
from docker_test_toolkit.application import build_cache, lifecycle, operator
from docker_test_toolkit.application.protocol import error_envelope
from docker_test_toolkit.application.recovery import Context, atomic_json, cleanup_project, image_lock, register
from docker_test_toolkit.config.runtime import PROJECT_LABEL, SESSION_LABEL
from docker_test_toolkit.domain.errors import InvalidInput, ResourceBusy, ResourceError
from docker_test_toolkit.factories.testing import build_test_services, install_failpoint
from tests.integration.account_fixtures import (
    ScriptedDocker,
    current_identity,
    dead_identity,
    kill_guardian,
    plant_run_record,
    processes_mentioning,
    queue_entries,
    retire_actor,
    run_spec,
    seed_terminal_run_record,
    source_artifact,
    wait_until,
    write_raw_queue,
)

pytestmark = pytest.mark.integration


def _spec(project: Path) -> RunSpec:
    return RunSpec(
        project_root=project,
        entrypoint="operator",
        purpose="parity",
        services=(),
        closure=(),
        profile="host",
        phases={"tests": PhaseBudget(0, 1, 1, 1)},
        initial_phase="tests",
        artifact=ArtifactIdentity(
            "0.1.0",
            "a" * 64,
            {
                "activation": 1,
                "batch": 1,
                "cli": 1,
                "distribution": 1,
                "queue": 1,
                "receipt": 1,
                "registry": 2,
                "run": 1,
            },
        ),
        wait_seconds=20,
    )


def test_facade_and_one_shot_status_share_the_same_validated_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    handle = build_test_services(tmp_path / "state").open_run(_spec(project))
    monkeypatch.setattr(operator, "registry_directory", lambda: handle._context.project.state)
    payload = {
        "context": {
            "project_root": str(handle._context.project.root),
            "project_state": str(handle._context.project.state),
            "run_id": handle.run_id,
            "session_id": handle.session_id,
            "record": str(handle._context.record),
            "private_root": str(handle.private_root),
        }
    }
    request = tmp_path / "request.json"
    request.write_text(json.dumps(payload))

    assert main(["run", "status", "--input", str(request), "--json"]) == 0
    response = json.loads(capsys.readouterr().out)

    assert response["command"] == "run.status"
    assert response["result"] == asdict(handle.status())


def test_operator_project_cleanup_reports_unverified_history_never_clean(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project, run_id, record = seed_terminal_run_record(tmp_path)
    context = Context(project, tmp_path / "registry")
    monkeypatch.setattr(operator, "registry_directory", lambda: context.state)
    request = {"project_root": str(project)}
    plant_run_record(context, run_id, record)

    verified = operator.execute("cleanup.project", request, docker_factory=ScriptedDocker)
    assert (
        verified["history_unverified"] == [] and verified["clean"] is True
    )  # the production caller supplies the verdict

    broken = plant_run_record(context, run_id, {**record, "cleanup_outcome": "bogus"})
    unverified = operator.execute("cleanup.project", request, docker_factory=ScriptedDocker)

    assert unverified["history_unverified"] == [broken] and unverified["clean"] is False


def test_operator_project_timeline_routes_to_the_registry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project, run_id, record = seed_terminal_run_record(tmp_path)
    context = Context(project, tmp_path / "registry")
    monkeypatch.setattr(operator, "registry_directory", lambda: context.state)
    plant_run_record(context, run_id, record)

    timeline = operator.execute("project.timeline", {"project_root": str(project)}, docker_factory=ScriptedDocker)

    assert timeline["project_id"] == context.project_id
    assert [entry["run_id"] for entry in timeline["runs"]] == [run_id]
    extended = {"project_root": str(project), "extra": 1}  # a field a newer caller adds is ignored
    assert operator.execute("project.timeline", extended, docker_factory=ScriptedDocker) == timeline
    with pytest.raises(InvalidInput, match="timeline input is incomplete"):
        operator.execute("project.timeline", {"extra": 1}, docker_factory=ScriptedDocker)


def test_operator_cache_remove_routes_to_the_registry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = tmp_path / "project"
    project.mkdir()
    context = Context(project, tmp_path / "registry")
    monkeypatch.setattr(operator, "registry_directory", lambda: context.state)
    project_cache = build_cache.project_cache(context.state, context.project_id)
    for level in (build_cache.cache_root(context.state), project_cache, project_cache / "linux-arm64"):
        build_cache.require_safe_directory(level)

    listed = operator.execute("cache.remove", {"project_root": str(project)}, docker_factory=ScriptedDocker)
    removed = operator.execute(
        "cache.remove", {"project_root": str(project)}, docker_factory=ScriptedDocker, execute_cleanup=True
    )

    assert listed["mode"] == "dry-run" and listed["cache_root"] == str(build_cache.cache_root(context.state))
    assert [item["project_id"] for item in listed["projects"]] == [context.project_id]
    assert removed["mode"] == "execute" and removed["results"][0]["project_id"] == context.project_id
    assert not (project_cache / "linux-arm64").exists()


def test_session_cleanup_cli_reports_dry_run_and_incomplete_execution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    state = tmp_path / "state"
    context = Context(project.resolve(), state / "registry")
    selected = "11111111-1111-4111-8111-111111111111"
    other = "22222222-2222-4222-8222-222222222222"
    image_id = "sha256:" + "e" * 64
    register(context, session_id=selected, temporary_parent=tmp_path)
    register(context, session_id=other, temporary_parent=tmp_path)
    for session_id in (selected, other):
        root = tmp_path / "docker-test-toolkit" / session_id
        root.mkdir(parents=True)
        (root / "docker-image-ids").write_text(f"{image_id}\n")

    class SharedImageDocker:
        engine = "docker"  # the port's engine member

        @staticmethod
        def call(*arguments: str) -> str:
            if arguments == ("info", "--format", "{{.ID}}"):
                return "fixture-daemon\n"
            if arguments == ("buildx", "ls", "--format", "{{.Name}}"):
                return ""
            raise AssertionError(f"unexpected Docker command: {arguments}")

        @staticmethod
        def objects(kind: str) -> list[dict]:
            if kind != "image":
                return []
            return [
                {
                    "Id": image_id,
                    "Labels": {PROJECT_LABEL: context.project_id, SESSION_LABEL: selected},
                    "RepoTags": ["shared:image"],
                }
            ]

    monkeypatch.setattr(operator, "registry_directory", lambda: context.state)
    account = state / "test-admission"
    account.mkdir(mode=0o700)
    monkeypatch.setattr(operator, "account_directory", lambda: account)
    monkeypatch.setattr(cli, "build_docker_adapter", SharedImageDocker)
    request = tmp_path / "cleanup-session.json"
    request.write_text(json.dumps({"project_root": str(project), "session_id": selected}))

    assert main(["cleanup", "session", "--input", str(request), "--json"]) == 0
    dry_run = json.loads(capsys.readouterr().out)
    assert dry_run["command"] == "cleanup.session"
    assert dry_run["cleanup"] is None
    assert dry_run["result"]["executed"] is False

    assert main(["cleanup", "session", "--input", str(request), "--execute", "--json"]) == 7
    executed = json.loads(capsys.readouterr().out)
    assert executed["result"]["executed"] is True
    assert executed["result"]["clean"] is False
    assert executed["cleanup"]["error"]["category"] == "incomplete_cleanup"


_ORIGIN_KEYS = {"queue_deadline", "bootstrap_deadline", "admission_observation", "phase_history", "docker_receipts"}
_REWRITTEN_KEYS = {
    "account_directory",
    "artifact",
    "grant",
    "guardian",
    "private_root",
    "project_id",
    "project_root",
    "recovery_actor",
    "requester",
    "source_identity",
    "withdrawal",
}


@pytest.mark.parametrize("command", ["cleanup.project", "cleanup.session"], ids=["project", "session"])
def test_operator_cleanup_refuses_while_image_lock_held(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, command: str
) -> None:
    project, run_id, record = seed_terminal_run_record(tmp_path)
    context = Context(project, tmp_path / "registry")
    plant_run_record(context, run_id, record)
    session_id = "11111111-1111-4111-8111-111111111111"
    register(context, session_id=session_id, temporary_parent=tmp_path)
    account = tmp_path / "account"
    account.mkdir(mode=0o700)
    monkeypatch.setattr(operator, "registry_directory", lambda: context.state)
    monkeypatch.setattr(operator, "account_directory", lambda: account)
    request = {"project_root": str(project), **({"session_id": session_id} if command == "cleanup.session" else {})}
    docker = ScriptedDocker()

    with image_lock(Context(account, account)):
        started = time.monotonic()
        with pytest.raises(ResourceBusy) as raised:
            operator.execute(command, request, docker_factory=lambda: docker, execute_cleanup=True)
        elapsed = time.monotonic() - started

    assert raised.value.reason == "image-lock-held" and elapsed < 1
    assert docker.commands == []  # refused before any Docker call


def _project_with_image(tmp_path: Path, image_id: str) -> tuple[Context, ScriptedDocker, Path]:
    project = tmp_path / "project"
    project.mkdir()
    context = Context(project.resolve(), tmp_path / "registry")
    atomic_json(
        context.record,
        {"version": 2, "project_root": str(context.root), "sessions": {}, "images": {image_id: ["a:tag"]}, "runs": {}},
    )
    docker = ScriptedDocker(
        image=[{"Id": image_id, "RepoTags": ["a:tag"], "Labels": {PROJECT_LABEL: context.project_id}}]
    )
    account = tmp_path / "account"
    account.mkdir(mode=0o700)
    return context, docker, account


def test_operator_cleanup_image_removal_child_inherits_k(tmp_path: Path) -> None:
    """Every image removal's Docker child gets K's descriptor, so K outlives a killed operator until the child exits."""
    image_id = "sha256:" + "e" * 64
    context, docker, account = _project_with_image(tmp_path, image_id)

    cleanup_project(context, docker, execute=True, image_account=account)

    assert docker.removals() == [("image", "rm", "a:tag")]
    assert len(docker.held_fds) == 1 and docker.held_fds[0] is not None


def test_operator_cleanup_keeps_an_image_in_use(tmp_path: Path) -> None:
    image_id = "sha256:" + "e" * 64
    context, docker, account = _project_with_image(tmp_path, image_id)

    result = cleanup_project(
        context, docker, execute=True, image_account=account, image_in_use=lambda image: image == image_id
    )

    assert docker.removals() == [] and [item["Id"] for item in docker.objects("image")] == [image_id]
    assert result["clean"] is False


def _stranded_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[lifecycle.RunContext, Path]:
    """A granted run whose requester and guardian are dead, nothing queued, no death time seeded; operator bound."""
    services = build_test_services(tmp_path / "state")
    project = tmp_path / "project"
    project.mkdir()
    spec = RunSpec(
        project_root=project,
        entrypoint="integration",
        purpose="stranded",
        services=("postgres",),
        closure=("postgres",),
        profile="fixture",
        phases={"tests": PhaseBudget(0, 1, 1, 1)},
        initial_phase="tests",
        artifact=source_artifact(),
        wait_seconds=45,
    )
    context = services.open_run(spec).enter()._context
    kill_guardian(context)
    retire_actor(context.project.state, current_identity(), dead_identity())
    account = services.root / "admission"
    monkeypatch.setattr(operator, "registry_directory", lambda: context.project.state)
    monkeypatch.setattr(operator, "account_directory", lambda: account)
    return context, account


def _recover_workers(run_id: str) -> list[str]:
    return [line for line in processes_mentioning(run_id) if "_recover" in line]


def _assert_recovered(context: lifecycle.RunContext, account: Path) -> None:
    wait_until(lambda: context.run_id not in queue_entries(account), timeout=120, message="the worker recovers the run")
    record = json.loads(context.record.read_text())
    assert (record["state"], record["admission"]) == ("complete", "revoked")
    assert (record["recovery_outcome"], record["quiescent"]) == ("complete", True)
    assert (record["cleanup_outcome"], record["infrastructure_failure"]) == ("incomplete", "guardian-lost")


def _refused_cleanup(context: lifecycle.RunContext, *, execute_cleanup: bool = True) -> dict:
    request = {"project_root": str(context.project.root)}
    with pytest.raises(ResourceError) as refused:
        operator.execute("cleanup.project", request, docker_factory=ScriptedDocker, execute_cleanup=execute_cleanup)
    return error_envelope("cleanup.project", refused.value)[0]["error"]["context"]


def test_executing_cleanup_recovers_a_stranded_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    context, account = _stranded_run(tmp_path, monkeypatch)

    refusal = _refused_cleanup(context)

    assert refusal["recovery_trigger"] == {"spawned": 1, "spawn_failed": 0, "skipped": False}
    assert len(_recover_workers(context.run_id)) == 1
    takeover = queue_entries(account)[context.run_id].takeover
    assert takeover is not None and takeover["claimant"] is None
    _assert_recovered(context, account)  # no further command, and the cleanup's own lock never met the worker
    notes = operator.execute(
        "project.timeline", {"project_root": str(context.project.root)}, docker_factory=ScriptedDocker
    )
    assert "recovered-without-leave" in json.dumps(notes)


def test_executing_retire_recovers_a_stranded_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    context, account = _stranded_run(tmp_path, monkeypatch)
    vanished = tmp_path / "vanished"
    vanished.mkdir()
    services = build_test_services(tmp_path / "state")
    assert services.open_run(run_spec(vanished.resolve(), source_artifact(), "vanished")).cancel().state == "complete"
    shutil.rmtree(vanished)
    retire_actor(context.project.state, current_identity(), dead_identity())  # its requester (this process) is gone
    dry_run = operator.execute("project.retire", {}, docker_factory=ScriptedDocker)
    evidence = [item["evidence"] for item in dry_run["eligible"]]
    assert len(evidence) == 1

    result = operator.execute(
        "project.retire", {"evidence": evidence}, docker_factory=ScriptedDocker, execute_cleanup=True
    )

    assert result["recovery_trigger"] == {"spawned": 1, "spawn_failed": 0, "skipped": False}
    _assert_recovered(context, account)


@pytest.mark.parametrize(
    ("command", "payload"), [("cleanup.project", {}), ("cleanup.project", {"project_root": 1}), ("project.retire", {})]
)
def test_trigger_runs_only_after_a_validated_executing_command(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, command: str, payload: dict
) -> None:
    context, account = _stranded_run(tmp_path, monkeypatch)
    before = (account / "queue.json").read_bytes()

    with pytest.raises(InvalidInput) as invalid:
        operator.execute(command, payload, docker_factory=ScriptedDocker, execute_cleanup=True)

    assert "recovery_trigger" not in invalid.value.details
    assert (account / "queue.json").read_bytes() == before
    assert _recover_workers(context.run_id) == []


def test_a_hostile_queue_makes_the_pass_skip_not_fail(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    context, account = _stranded_run(tmp_path, monkeypatch)
    write_raw_queue(account, b"{")

    refusal = _refused_cleanup(context)

    assert refusal["recovery_trigger"] == {"spawned": 0, "spawn_failed": 0, "skipped": True}
    assert refusal["reason"] == "unspecified"
    assert (account / "queue.json").read_bytes() == b"{"
    assert _recover_workers(context.run_id) == []


def test_a_pass_that_fails_after_starting_never_replaces_the_commands_outcome(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    context, _account = _stranded_run(tmp_path, monkeypatch)

    def thread_exhausted(stage: str) -> None:
        if stage == "detection-read":
            raise RuntimeError("can't start new thread")

    install_failpoint(thread_exhausted)
    try:
        refusal = _refused_cleanup(context)
    finally:
        install_failpoint(None)

    assert refusal["recovery_trigger"] == {"spawned": 0, "spawn_failed": 0, "skipped": True}
    assert [item["run_id"] for item in refusal["runs"]] == [context.run_id]


def test_a_validated_commands_filesystem_failure_still_runs_the_pass(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    context, account = _stranded_run(tmp_path, monkeypatch)

    def unwritable(*_arguments: object, **_keywords: object) -> dict:
        raise PermissionError("record directory is read-only")

    monkeypatch.setattr(
        operator, "cleanup_project", unwritable
    )  # the composition seam: a real write failure needs root
    request = {"project_root": str(context.project.root)}

    with pytest.raises(PermissionError):
        operator.execute("cleanup.project", request, docker_factory=ScriptedDocker, execute_cleanup=True)

    assert len(_recover_workers(context.run_id)) == 1
    _assert_recovered(context, account)


def test_a_second_executing_command_does_not_respawn_a_held_intent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    context, account = _stranded_run(tmp_path, monkeypatch)

    first, second = _refused_cleanup(context), _refused_cleanup(context)

    assert first["recovery_trigger"]["spawned"] == 1
    assert second["recovery_trigger"] == {"spawned": 0, "spawn_failed": 0, "skipped": False}
    assert [item["run_id"] for item in first["runs"]] == [item["run_id"] for item in second["runs"]] == [context.run_id]
    assert len(_recover_workers(context.run_id)) == 1
    _assert_recovered(context, account)


def test_read_only_commands_start_no_recovery(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    context, account = _stranded_run(tmp_path, monkeypatch)
    root = str(context.project.root)
    before = (account / "queue.json").read_bytes()
    locator = lifecycle.run_locator(context)

    for command, payload, execute_cleanup in (
        ("cleanup.project", {"project_root": root}, False),
        ("cleanup.session", {"project_root": root, "session_id": context.session_id}, False),
        ("cleanup.session", {"project_root": root, "session_id": context.session_id}, True),
        ("run.status", {"context": locator}, False),
        ("project.timeline", {"project_root": root}, False),
        ("project.retire", {}, False),
        ("protocol", {}, False),
        ("readiness.inspect", {}, False),
    ):
        try:
            operator.execute(command, payload, docker_factory=ScriptedDocker, execute_cleanup=execute_cleanup)
        except (ResourceError, TypeError, AttributeError) as error:  # the outcome is not this test's subject; no report
            assert "recovery_trigger" not in getattr(error, "details", {}), command
        assert (account / "queue.json").read_bytes() == before, command
        assert _recover_workers(context.run_id) == [], command


def test_cleanup_refusal_names_every_indexed_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    services = build_test_services(tmp_path / "state")
    project, other = tmp_path / "project", tmp_path / "other"
    project.mkdir()
    other.mkdir()
    artifact = source_artifact()
    retained = services.open_run(run_spec(project.resolve(), artifact, "retained", retain=True)).enter()
    assert retained.retain().state == "stopped-retained"
    entered = services.open_run(run_spec(project.resolve(), artifact, "entered"))
    foreign = services.open_run(run_spec(other.resolve(), artifact, "foreign"))
    context = entered._context
    monkeypatch.setattr(operator, "registry_directory", lambda: context.project.state)
    monkeypatch.setattr(operator, "account_directory", lambda: services.root / "admission")

    registry = json.loads(context.project.record.read_text())
    unloadable_id, foreign_id = str(uuid.uuid4()), str(uuid.uuid4())
    unloadable_path = context.project.state / "runs" / f"{unloadable_id}.json"
    unloadable_path.write_bytes(b"{")
    unloadable_path.chmod(0o600)
    registry["runs"] = {
        **registry["runs"],
        unloadable_id: str(unloadable_path),
        foreign_id: str(foreign._context.record),
    }
    atomic_json(context.project.record, registry)

    refusal = _refused_cleanup(context, execute_cleanup=False)

    items = {item["run_id"]: item for item in refusal["runs"]}
    assert list(items) == list(registry["runs"])  # every indexed run, in index order
    assert items[retained.run_id]["action"] == "run.release"
    assert items[retained.run_id]["context"] == lifecycle.run_locator(retained._context)
    assert items[entered.run_id]["action"] == "run.recover"
    assert items[entered.run_id]["reason"] is None
    assert items[unloadable_id] == {
        "run_id": unloadable_id,
        "action": None,
        "context": None,
        "reason": "record-unloadable",
    }
    assert items[foreign_id] == {"run_id": foreign_id, "action": None, "context": None, "reason": "record-foreign"}
    assert operator._one_shot_context({"context": items[entered.run_id]["context"]}).run_id == entered.run_id
    assert operator._one_shot_context(items[retained.run_id]["context"]).run_id == retained.run_id
