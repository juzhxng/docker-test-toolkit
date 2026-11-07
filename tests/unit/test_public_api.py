from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

import docker_test_toolkit as toolkit
from docker_test_toolkit.application.lifecycle import bundle_manifest
from docker_test_toolkit.domain.errors import InvalidInput


def test_public_api_is_the_closed_typed_surface(tmp_path: Path) -> None:
    assert set(toolkit.__all__) == {
        "ArtifactIdentity",
        "BatchAggregate",
        "BatchCoordinator",
        "BatchExecutionReport",
        "BatchItem",
        "BatchPlan",
        "BatchResult",
        "BatchRunEvidence",
        "BatchSegment",
        "BoundaryResult",
        "ConsumerRunContext",
        "Endpoint",
        "ExternalDockerResource",
        "PhaseBudget",
        "PlannedBatch",
        "RunHandle",
        "RunSpec",
        "SegmentResult",
        "ToolkitError",
        "current_consumer_run",
        "open_batches",
        "open_retained_run",
        "open_run",
        "toolkit_identity",
    }
    identity = toolkit.ArtifactIdentity(
        version="0.1.0",
        sha256="a" * 64,
        protocols={
            "activation": 1,
            "batch": 1,
            "cli": 1,
            "distribution": 1,
            "queue": 1,
            "receipt": 1,
            "registry": 2,
            "run": 1,
        },
    )
    spec = toolkit.RunSpec(
        project_root=tmp_path,
        entrypoint="integration",
        purpose="public-api",
        services=("postgres",),
        closure=("postgres",),
        profile="fixture",
        phases={"tests": toolkit.PhaseBudget(1, 1, 1, 1)},
        initial_phase="tests",
        artifact=identity,
    )

    assert spec.artifact == identity
    assert spec.project_root == tmp_path


def test_retained_locator_reopens_only_the_exact_compatible_spec(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from types import SimpleNamespace

    from docker_test_toolkit.application import api

    identity = toolkit.ArtifactIdentity(
        "0.1.0",
        "a" * 64,
        {"activation": 1, "batch": 1, "cli": 1, "distribution": 1, "queue": 1, "receipt": 1, "registry": 2, "run": 1},
    )
    spec = toolkit.RunSpec(
        tmp_path,
        "e2e",
        "retained",
        ("postgres",),
        ("postgres",),
        "fixture",
        {"tests": toolkit.PhaseBudget(1, 2, 1, 1)},
        "tests",
        identity,
        retain=True,
    )
    locator = tmp_path / "retained.json"
    context = SimpleNamespace(
        project=SimpleNamespace(root=tmp_path, state=tmp_path / "state"),
        run_id="22222222-2222-4222-8222-222222222222",
        session_id="11111111-1111-4111-8111-111111111111",
        record=tmp_path / "run.json",
        private_root=tmp_path / "private",
    )
    handle = api.RunHandle(spec, context)
    monkeypatch.setattr(handle, "status", lambda: SimpleNamespace(state="stopped-retained"))

    handle.write_retained_locator(locator)

    payload = locator.read_text()
    monkeypatch.setattr(api.lifecycle, "load_operator_context", lambda value: context if value == payload else None)
    monkeypatch.setattr(
        api.lifecycle,
        "load_run_record",
        lambda selected: (
            {
                "state": "stopped-retained",
                "project_root": str(tmp_path),
                "entrypoint": "e2e",
                "purpose": "retained",
                "services": ["postgres"],
                "closure": ["postgres"],
                "profile": "fixture",
                "phases": {
                    "tests": {
                        "container_bytes": 1,
                        "host_process_bytes": 2,
                        "daemon_cpus": 1,
                        "host_threads": 1,
                    }
                },
                "initial_phase": "tests",
                "artifact": {
                    "version": "0.1.0",
                    "sha256": "a" * 64,
                    "protocols": dict(identity.protocols),
                },
                "external_resources": [],
                "retain": True,
            }
            if selected is context
            else {}
        ),
    )

    reopened = api._open_retained_run(spec, locator)

    assert reopened.run_id == context.run_id
    incompatible = toolkit.RunSpec(
        tmp_path,
        "e2e",
        "different-purpose",
        ("postgres",),
        ("postgres",),
        "fixture",
        {"tests": toolkit.PhaseBudget(1, 2, 1, 1)},
        "tests",
        identity,
        retain=True,
    )
    with pytest.raises(toolkit.ToolkitError, match="does not match"):
        api._open_retained_run(incompatible, locator)


def test_retained_locator_rejects_public_or_replaceable_files(tmp_path: Path) -> None:
    from docker_test_toolkit.application import api

    locator = tmp_path / "retained.json"
    locator.write_text("{}")
    locator.chmod(0o644)
    with pytest.raises(toolkit.ToolkitError, match="private"):
        api._read_retained_locator(locator)
    locator.unlink()
    locator.symlink_to(tmp_path / "missing")
    with pytest.raises(toolkit.ToolkitError, match="unreadable or a symlink"):
        api._read_retained_locator(locator)


def test_run_handle_keeps_consumer_signals_behind_birth_bound_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    from types import SimpleNamespace

    from docker_test_toolkit.application import api

    calls: list[tuple] = []
    handle = api.RunHandle(None, None, _lease="lease")
    monkeypatch.setattr(
        api.lifecycle,
        "stop_consumer",
        lambda lease, consumer_id, wait_seconds: calls.append(("stop", lease, consumer_id, wait_seconds)),
    )
    monkeypatch.setattr(
        api.lifecycle,
        "consumer_identity",
        lambda lease, consumer_id: calls.append(("identity", lease, consumer_id)) or SimpleNamespace(),
    )
    monkeypatch.setattr(
        api.lifecycle,
        "signal_process",
        lambda identity, signum: calls.append(("signal", identity, signum)) or True,
    )

    handle.stop_consumer("worker", wait_seconds=7)
    assert handle.signal_consumer("worker", 15)
    assert [call[0] for call in calls] == ["stop", "identity", "signal"]


def test_run_handle_keeps_container_control_behind_the_entered_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from docker_test_toolkit.application import api

    calls: list[tuple] = []
    handle = api.RunHandle(None, None, _lease="lease")
    generation = {"started_at": "2026-09-17T00:00:00Z", "restart_count": 0}
    monkeypatch.setattr(
        api.lifecycle,
        "register_container_control",
        lambda lease, caller_id, caller_generation, targets: (
            calls.append(("register", lease, caller_id, caller_generation, targets))
            or {caller_id: generation, **dict.fromkeys(targets, generation)}
        ),
    )
    monkeypatch.setattr(
        api.lifecycle,
        "validate_container_control",
        lambda lease, caller_id, caller_generation, targets: calls.append(
            ("validate", lease, caller_id, caller_generation, targets)
        ),
    )
    monkeypatch.setattr(
        api.lifecycle,
        "control_compose_containers",
        lambda lease, operation_id, action, targets, expected, **keywords: (
            calls.append(("control", lease, operation_id, action, targets, expected, keywords))
            or dict.fromkeys(targets, generation)
        ),
    )

    assert handle.register_container_control("a" * 64, generation, ("b" * 64,)) == {
        "a" * 64: generation,
        "b" * 64: generation,
    }
    handle.validate_container_control("a" * 64, generation, ("b" * 64,))
    assert handle.control_containers(
        "11111111-1111-4111-8111-111111111111",
        "stop",
        ("b" * 64,),
        {"b" * 64: generation},
        request_sha256="c" * 64,
    ) == {"b" * 64: generation}
    assert [call[0] for call in calls] == ["register", "validate", "control"]


def test_public_models_reject_ambiguous_or_unsafe_identity(tmp_path: Path) -> None:
    with pytest.raises(toolkit.ToolkitError, match="artifact digest"):
        toolkit.ArtifactIdentity(version="0.1.0", sha256="wrong", protocols={})
    identity = toolkit.ArtifactIdentity(
        version="0.1.0",
        sha256="a" * 64,
        protocols={
            "activation": 1,
            "batch": 1,
            "cli": 1,
            "distribution": 1,
            "queue": 1,
            "receipt": 1,
            "registry": 2,
            "run": 1,
        },
    )
    with pytest.raises(toolkit.ToolkitError, match="unique"):
        toolkit.RunSpec(
            project_root=tmp_path,
            entrypoint="integration",
            purpose="duplicate",
            services=("postgres", "postgres"),
            closure=("postgres",),
            profile="fixture",
            phases={},
            initial_phase=None,
            artifact=identity,
        )
    with pytest.raises(toolkit.ToolkitError, match="exact external resource identity"):
        toolkit.ExternalDockerResource("container", "named-container", "fixture", "inspect")
    with pytest.raises(toolkit.ToolkitError, match="relationship"):
        toolkit.ExternalDockerResource("container", "a" * 64, "fixture", "use_image")


def test_source_manifest_covers_every_python_module_recursively(tmp_path: Path) -> None:
    (tmp_path / "application").mkdir()
    (tmp_path / "domain").mkdir()
    (tmp_path / "__init__.py").write_text('"""package"""\n')
    (tmp_path / "application/lifecycle.py").write_text("VALUE = 1\n")
    (tmp_path / "domain/models.py").write_text("VALUE = 2\n")
    (tmp_path / "domain/ignored.txt").write_text("not production Python")

    manifest = bundle_manifest(tmp_path)

    assert set(manifest["files"]) == {"__init__.py", "application/lifecycle.py", "domain/models.py"}


def test_loaded_runtime_refuses_new_work_after_any_source_change(tmp_path: Path) -> None:
    source_root = Path(__file__).parents[2] / "src"
    copied_src = tmp_path / "src"
    shutil.copytree(source_root, copied_src)
    program = """
from pathlib import Path
from docker_test_toolkit.application.lifecycle import ResourceError, source_identity
target = Path.cwd() / 'src/docker_test_toolkit/config/runtime.py'
target.write_text(target.read_text() + '\\n')
try:
    source_identity()
except ResourceError:
    raise SystemExit(0)
raise SystemExit('source change was accepted')
"""

    result = subprocess.run(
        [sys.executable, "-c", program],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(copied_src)},
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def test_archive_detection_uses_the_rightmost_zipapp_boundary(tmp_path: Path) -> None:
    from docker_test_toolkit.application.state import containing_zipapp

    archive = tmp_path / "parent.pyz" / "nested" / "docker-test-toolkit.pyz"
    archive.parent.mkdir(parents=True)
    archive.write_bytes(b"fixture")

    assert containing_zipapp(str(archive / "docker_test_toolkit/application/lifecycle.py")) == archive
    assert containing_zipapp(str(tmp_path / "src/docker_test_toolkit/application/lifecycle.py")) is None


def test_toolkit_child_command_is_exact_for_archive_and_source_modes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from docker_test_toolkit.application import lifecycle

    archive = tmp_path / "docker-test-toolkit.pyz"
    monkeypatch.setattr(lifecycle, "_ARCHIVE_PATH", archive)
    assert lifecycle._toolkit_process_command("protocol", "--json") == [
        sys.executable,
        "-I",
        str(archive),
        "protocol",
        "--json",
    ]

    monkeypatch.setattr(lifecycle, "_ARCHIVE_PATH", None)
    assert lifecycle._toolkit_process_command("protocol", "--json") == [
        sys.executable,
        "-P",
        "-m",
        "docker_test_toolkit.factories.cli",
        "protocol",
        "--json",
    ]


def test_capacity_is_accepted_and_ignored(tmp_path: Path) -> None:
    from docker_test_toolkit.application.lifecycle import source_identity
    from docker_test_toolkit.domain.models import SUPPORTED_PROTOCOLS, TOOLKIT_VERSION, ArtifactIdentity

    identity = ArtifactIdentity(TOOLKIT_VERSION, source_identity(), dict(SUPPORTED_PROTOCOLS))
    spec = toolkit.RunSpec(tmp_path, "integration", "ignored", (), (), "fixture", {}, None, identity, capacity=7)

    assert spec.capacity == 7


def test_toolkit_identity_is_exported() -> None:
    from docker_test_toolkit.application.lifecycle import source_identity
    from docker_test_toolkit.domain.models import SUPPORTED_PROTOCOLS, TOOLKIT_VERSION

    identity = toolkit.toolkit_identity()

    assert identity == toolkit.ArtifactIdentity(TOOLKIT_VERSION, source_identity(), dict(SUPPORTED_PROTOCOLS))
    assert "toolkit_identity" in toolkit.__all__


def test_run_spec_without_artifact(tmp_path: Path) -> None:
    spec = toolkit.RunSpec(tmp_path, "integration", "no-artifact", (), (), "fixture", {}, None)

    assert spec.artifact is None
    with pytest.raises(toolkit.ToolkitError, match="artifact"):
        toolkit.RunSpec(tmp_path, "integration", "bad-artifact", (), (), "fixture", {}, None, {"version": "x"})


def test_batch_plan_positions_are_fixed(tmp_path: Path) -> None:
    segment = toolkit.BatchSegment("one", (toolkit.BatchItem("item", ()),), None, False)

    plan = toolkit.BatchPlan(tmp_path, None, (segment,))  # the three-argument positional call
    assert plan.segments == (segment,) and plan.artifact_identity is None
    with pytest.raises(InvalidInput):
        toolkit.BatchPlan(tmp_path, (segment,))  # two positions: segments landed in the ignored identity slot


def test_run_records_the_toolkit_identity(tmp_path: Path) -> None:
    from docker_test_toolkit.factories.testing import build_test_services

    stale = toolkit.ArtifactIdentity("0.0.1", "b" * 64, {"run": 1})  # an old pin a project still passes
    spec = toolkit.RunSpec(tmp_path, "integration", "identity", (), (), "fixture", {}, None, stale)
    handle = build_test_services(tmp_path / "state").open_run(spec)
    try:
        recorded = json.loads(handle._context.record.read_text())["artifact"]
    finally:
        handle.cancel()

    identity = toolkit.toolkit_identity()
    assert recorded == {"version": identity.version, "sha256": identity.sha256, "protocols": dict(identity.protocols)}
