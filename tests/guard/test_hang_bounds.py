"""A hung test ends with a report: test steps are time-bounded and only one helper freezes a process.

A test step without its own bound runs until the job limit and reports nothing about which test hung.
SIGSTOP freezes a process together with every lock it holds; ``pause_guardian`` is the one helper that proves those
locks free first, so every test pauses through it.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).parents[2]
WORKFLOWS = ROOT / ".github" / "workflows"
_TEST_COMMAND = re.compile(r"\bpoe (?:unit|int|e2e)\b|\buv run pytest\b")
_PAUSE_HELPER = ROOT / "tests" / "integration" / "account_fixtures.py"


def workflow_files(directory: Path) -> list[Path]:
    """Every workflow file CI reads, whichever YAML extension it uses."""
    return sorted([*directory.glob("*.yml"), *directory.glob("*.yaml")])


def test_every_test_step_declares_a_time_bound() -> None:
    unbounded = [
        f"{path.name}: {name}: {step.get('name', step['run'])}"
        for path in workflow_files(WORKFLOWS)
        for name, job in (yaml.safe_load(path.read_text()).get("jobs") or {}).items()
        for step in job.get("steps", [])
        if _TEST_COMMAND.search(str(step.get("run", ""))) and "timeout-minutes" not in step
    ]
    assert unbounded == []


def test_only_the_pause_helper_sends_sigstop() -> None:
    senders = [
        path.relative_to(ROOT).as_posix()
        for path in sorted((ROOT / "tests").rglob("*.py"))
        if path != _PAUSE_HELPER and path != Path(__file__) and "SIGSTOP" in path.read_text()
    ]
    assert senders == []


def test_yaml_extension_workflows_are_checked(tmp_path: Path) -> None:
    (tmp_path / "a.yml").write_text("jobs: {}\n")
    (tmp_path / "check.yaml").write_text("jobs: {}\n")

    assert [path.name for path in workflow_files(tmp_path)] == ["a.yml", "check.yaml"]
