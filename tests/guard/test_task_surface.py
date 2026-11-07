from __future__ import annotations

import ast
import json
import tomllib
from pathlib import Path
from typing import Any

import pytest
import yaml
from tests import conftest as root_conftest

ROOT = Path(__file__).parents[2]


def test_qa_tasks_keep_unit_integration_and_e2e_lanes_distinct() -> None:
    tasks = tomllib.loads((ROOT / "pyproject.toml").read_text())["tool"]["poe"]["tasks"]

    assert tasks["unit"] == "pytest tests/unit tests/guard --cov -p no:warnings"
    assert tasks["int"] == (
        "pytest --basetemp=.pytest-tmp/int -m 'integration and not e2e' tests/integration --cov -p no:warnings"
    )
    assert tasks["e2e"] == "pytest --basetemp=.pytest-tmp/e2e -m 'e2e' tests/e2e -v"
    assert tasks["qa"] == ["typecheck", "static", "unit", "int"]
    assert "test-distribution" not in tasks and "test-conformance" not in tasks


def test_linux_real_boundary_collection_requires_explicit_owned_basetemp(monkeypatch) -> None:
    class Config:
        def __init__(self, basetemp):
            self.basetemp = basetemp

        def getoption(self, name):
            assert name == "basetemp"
            return self.basetemp

    class Item:
        def __init__(self, marked):
            self.marked = marked

        def get_closest_marker(self, name):
            return object() if self.marked and name == "integration" else None

    monkeypatch.setattr(root_conftest.sys, "platform", "linux")

    root_conftest._require_linux_ext_basetemp(Config(None), [Item(False)])
    with pytest.raises(pytest.UsageError, match="explicit --basetemp"):
        root_conftest._require_linux_ext_basetemp(Config(None), [Item(True)])
    with pytest.raises(pytest.UsageError, match="repository or account home"):
        root_conftest._require_linux_ext_basetemp(Config("/var/tmp/toolkit-outside"), [Item(True)])
    assert root_conftest._EXT_SUPER_MAGIC == 0xEF53


def _markers(path: Path) -> set[str]:
    markers = set()
    for node in ast.walk(ast.parse(path.read_text())):
        if (
            isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Attribute)
            and isinstance(node.value.value, ast.Name)
            and node.value.value.id == "pytest"
            and node.value.attr == "mark"
        ):
            markers.add(node.attr)
    return markers


def test_active_test_modules_belong_to_exactly_one_real_boundary_lane() -> None:
    for path in sorted((ROOT / "tests/integration").glob("test_*.py")):
        assert "integration" in _markers(path), path
    for path in sorted((ROOT / "tests/e2e").glob("test_*.py")):
        assert _markers(path) & {"integration", "e2e"} in ({"integration"}, {"e2e"}), path


def test_builder_consumers_use_only_the_subcommand_cli() -> None:
    consumers = [ROOT / "README.md", ROOT / ".github/workflows/ci.yml", ROOT / ".github/workflows/publish.yml"]
    for path in consumers:
        for line in path.read_text().splitlines():
            if "scripts/build_zipapp.py" in line:
                assert " build" in line or " verify" in line, (path, line)


CI = ROOT / ".github/workflows/ci.yml"
PUBLISH = ROOT / ".github/workflows/publish.yml"
HOSTED = "ubuntu-24.04-arm"
MOVED_SCENARIOS = {
    f"tests/e2e/test_shared_runs.py::{name}"
    for name in (
        "test_a_host_decline_others_caused_fails_no_granted_run",
        "test_owner_leave_hands_off_and_the_last_user_drains",
        "test_owner_image_rm_hands_off_and_non_owner_rm_is_refused",
    )
}
TEST_RESERVE = {"DOCKER_TEST_TOOLKIT_TEST_HOST_RESERVE_BYTES": "1073741824"}


def _yaml(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text())


def _triggers(workflow: dict[str, Any]) -> dict[str, Any]:
    return workflow.get("on", workflow.get(True)) or {}


def test_ci_runs_four_hosted_jobs_without_containers() -> None:
    workflow = _yaml(CI)

    assert set(workflow["jobs"]) == {"scan", "quality", "e2e", "shared-runs"}
    for name, job in workflow["jobs"].items():
        assert job["runs-on"] == HOSTED, name
        assert "container" not in job and "name" not in job, name
    assert _triggers(workflow) == {
        "push": {"branches": ["main"]},
        "pull_request": {"branches": ["main"]},
        "workflow_dispatch": None,
    }
    assert workflow["concurrency"]["cancel-in-progress"] is True
    group = workflow["concurrency"]["group"]
    assert "github.event_name == 'pull_request'" in group and "github.run_id" in group
    assert workflow["permissions"] == {"contents": "read"}


def test_e2e_and_shared_runs_build_and_declare_the_test_reserve() -> None:
    jobs = _yaml(CI)["jobs"]

    for name in ("e2e", "shared-runs"):
        runs = [str(step.get("run", "")) for step in jobs[name]["steps"]]
        assert any("scripts/build_zipapp.py build --repository . --output-directory dist/final" in run for run in runs)
        tests = [step for step in jobs[name]["steps"] if "uv run pytest" in str(step.get("run", ""))]
        assert len(tests) == 1, name
        assert TEST_RESERVE.items() <= (tests[0].get("env") or {}).items(), name
        assert tests[0]["env"]["DOCKER_TEST_TOOLKIT_E2E_ARTIFACT"] == "dist/final/docker-test-toolkit.pyz", name
    assert "tests/e2e --ignore=tests/e2e/test_shared_runs.py" in str(jobs["e2e"]["steps"])


def test_shared_runs_names_the_moved_scenarios() -> None:
    lines = CI.read_text().splitlines()
    deselects = [index for index, line in enumerate(lines) if "--deselect" in line]

    assert {lines[index].split("--deselect ")[1].strip() for index in deselects} == MOVED_SCENARIOS
    assert len(deselects) == len(MOVED_SCENARIOS)
    step = lines[deselects[0] - 8 : deselects[0]]
    assert any("run in the CI-host" in line and line.strip().startswith("#") for line in step)
    assert any("qualification" in line for line in step)


def test_publish_is_dispatch_only_and_calls_the_script() -> None:
    workflow = _yaml(PUBLISH)
    triggers = _triggers(workflow)

    assert set(triggers) == {"workflow_dispatch"}
    inputs = triggers["workflow_dispatch"]["inputs"]
    assert inputs["tag"]["required"] is True and inputs["candidate_sha256"]["required"] is True
    assert {"qualification", "canary"} <= set(inputs)
    assert workflow["permissions"] == {"contents": "write", "actions": "read"}
    (job,) = workflow["jobs"].values()
    assert job["runs-on"] == HOSTED
    assert any("scripts/publish_release.py" in str(step.get("run", "")) for step in job["steps"])


def test_ci_and_publish_are_the_only_workflows() -> None:
    assert sorted(path.name for path in (ROOT / ".github/workflows").iterdir()) == ["ci.yml", "publish.yml"]


def test_ownership_manifest_lists_every_production_source_file() -> None:
    """The release build refuses a tree whose ownership manifest misses an input, so a new module must be listed."""
    manifest = json.loads((ROOT / "component-ownership.json").read_text())
    listed = {entry["path"] for entry in manifest["entries"]}
    sources = {
        path.relative_to(ROOT).as_posix()
        for path in (ROOT / "src").rglob("*")
        if path.is_file() and "__pycache__" not in path.parts and path.suffix not in {".pyc", ".pyo"}
    }

    assert sources | {"scripts/build_zipapp.py"} == listed


def test_toolkit_tests_never_resolve_the_machine_roots() -> None:
    """Toolkit tests use temporary installations only: no test resolves the machine's installation roots."""
    allowed = {
        "tests/installation_fixtures.py",
        "tests/unit/test_installation_layout.py",  # the layout rules' own test, on a temporary home
        "tests/guard/test_task_surface.py",  # this guard names what it searches for
    }
    needles = ("platform_root(", ".local/share/docker-test-toolkit", "/opt/docker-test-toolkit")
    offenders = [
        f"{path.relative_to(ROOT).as_posix()}: {needle}"
        for path in sorted((ROOT / "tests").rglob("*.py"))
        if path.relative_to(ROOT).as_posix() not in allowed
        for needle in needles
        if needle in path.read_text()
    ]

    assert offenders == []
