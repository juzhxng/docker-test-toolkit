"""Guard the E2E session sweep: component-wise containment, a fail-closed registry check and every sweep branch."""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).parents[2]
_SIX = ("project_root", "project_state", "run_id", "session_id", "record", "private_root")


def _harness() -> ModuleType:
    """Load tests/e2e/conftest.py as a module (tests/e2e has no __init__.py)."""
    spec = importlib.util.spec_from_file_location("e2e_session_harness", ROOT / "tests/e2e/conftest.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _registry(tmp_path: Path, **roots: str) -> Path:
    """Write a fake registry whose project records carry the given recorded roots."""
    registry = tmp_path / "registry"
    (registry / "projects").mkdir(parents=True)
    for name, root in roots.items():
        record = {"version": 2, "project_root": root, "sessions": {}, "images": {}, "runs": {}}
        (registry / "projects" / f"{name}.json").write_text(json.dumps(record))
    return registry


def test_session_candidates_use_component_wise_containment(tmp_path: Path) -> None:
    harness = _harness()
    base = tmp_path / ".pytest-tmp" / "e2e"
    registry = _registry(
        tmp_path,
        inside=str(base / "test_x0" / "p"),
        sibling=str(tmp_path / ".pytest-tmp" / "e2e-real" / "test_x0" / "p"),
        parent=str(tmp_path / ".pytest-tmp"),
    )

    assert harness._session_candidates(base, registry) == {"inside": str(base / "test_x0" / "p")}

    seven = tmp_path / "pytest-of-u" / "pytest-7"
    other = _registry(tmp_path / "second", older=str(tmp_path / "pytest-of-u" / "pytest-6" / "p"))
    assert harness._session_candidates(seven, other) == {}


def test_default_basetemp_is_a_child_of_the_user_temproot(tmp_path: Path) -> None:
    probe = tmp_path / "probe"
    probe.mkdir()
    (probe / "pytest.ini").write_text("")
    (probe / "test_probe.py").write_text(
        "def test_probe(tmp_path_factory):\n    print('BASETEMP=' + str(tmp_path_factory.getbasetemp()))\n"
    )
    temproot = tmp_path / "temproot"
    temproot.mkdir()

    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-s", "-q", "-p", "no:cacheprovider", "--rootdir", str(probe), str(probe)],
        cwd=probe,
        env={**os.environ, "PYTEST_DEBUG_TEMPROOT": str(temproot)},
        capture_output=True,
        text=True,
        check=True,
        timeout=120,
    )

    printed = Path(next(line for line in result.stdout.splitlines() if line.startswith("BASETEMP=")).split("=", 1)[1])
    assert printed.parent.name.startswith("pytest-of-") and printed.parent.parent == temproot.resolve()


# --- the sweep, driven against a fake registry and a scripted CLI ---


class _Scripted:
    """A scripted `toolkit` seam: records each call and answers from handlers keyed by the CLI arguments."""

    def __init__(self, registry: Path, handlers: dict) -> None:
        self.registry = registry
        self.handlers = handlers
        self.calls: list[tuple[tuple[str, ...], dict, bool]] = []

    def __call__(self, arguments: list[str], payload: dict, workspace: Path, *, execute: bool = False) -> dict:
        key = tuple(arguments)
        self.calls.append((key, payload, execute))
        handler = self.handlers[(key, execute)]
        return handler(payload, self) if callable(handler) else handler

    def count(self, *key: str) -> int:
        return sum(1 for called, _payload, _execute in self.calls if called == key)


def _ok(result: dict, code: int = 0) -> dict:
    return {"ok": True, "result": result, "_exit": code}


def _failure(category: str, code: int) -> dict:
    return {"ok": False, "error": {"category": category, "message": category}, "_exit": code}


def _evidence(project_id: str, root: str) -> dict:
    return {"project_id": project_id, "project_root": root, "project_record_sha256": "0" * 64, "runs": []}


def _locator(root: str, run_id: str = "11111111-1111-4111-8111-111111111111") -> dict:
    return {key: f"/x/{key}/{run_id}" for key in _SIX} | {"project_root": root, "run_id": run_id}


_SWEEP_CASES = [
    "indexed-complete-selection",
    "cleanup-not-clean",
    "cleanup-exit-zero-clean-false",
    "cleanup-nonzero-exit-clean-true",
    "retry-on-contention",
    "baseline-after-start-sweep",
    "own-end-retirement-is-not-foreign",
]


@pytest.mark.parametrize("case", _SWEEP_CASES)
def test_sweep_branches(case: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    harness = _harness()
    monkeypatch.setattr(time, "sleep", lambda _seconds: None)  # the retry spacing is one second per attempt
    base = (tmp_path / "base").resolve()
    root = base / "test_a0" / "p"
    root.mkdir(parents=True)
    pid = "a" * 64
    registry = _registry(tmp_path, **{pid: str(root)})
    record = registry / "projects" / f"{pid}.json"
    locator = _locator(str(root))
    state = {"recovered": False}

    def dry_run(payload: dict, _script: _Scripted) -> dict:
        if state["recovered"] and not root.exists():
            return _ok({"eligible": [{"evidence": _evidence(pid, str(root))}], "blocked": [], "not_found": []})
        reasons, locators = (
            (["root-present"], [])
            if state["recovered"]
            else (
                ["indexed-runs"],
                [{"action": "run.recover", "context": locator}],
            )
        )
        blocked = {
            "project_id": pid,
            "project_root": str(root),
            "category": "incompatible_state",
            "reasons": reasons,
            "archived_runs": [],
            "locators": locators,
        }
        return _ok({"eligible": [], "blocked": [blocked], "not_found": []})

    def recover(_payload: dict, script: _Scripted) -> dict:
        if case == "retry-on-contention" and script.count("run", "recover") <= 2:
            return _failure("contention", 4)
        state["recovered"] = True
        return _ok({"state": "complete"})

    def retire(payload: dict, _script: _Scripted) -> dict:
        record.unlink()  # the execute really removes the project record, as the real command does
        done = [
            {"project_id": item["project_id"], "outcome": "archived", "category": None} for item in payload["evidence"]
        ]
        return _ok({"results": done, "stopped": False})

    cleanup_execute = {
        "cleanup-not-clean": _ok({"clean": False}, 7),
        "cleanup-exit-zero-clean-false": _ok({"clean": False}, 0),
        "cleanup-nonzero-exit-clean-true": _ok({"clean": True}, 7),
    }.get(case, _ok({"clean": True}, 0))
    script = _Scripted(
        registry,
        {
            (("project", "retire"), False): dry_run,
            (("project", "retire"), True): retire,
            (("run", "recover"), False): recover,
            (("cleanup", "project"), False): _ok({"clean": True}),
            (("cleanup", "project"), True): cleanup_execute,
        },
    )

    if case == "baseline-after-start-sweep":
        shutil.rmtree(root)  # a leftover of an earlier session: its root is gone, the record remains
        baseline = harness._start_session(base, time.monotonic() + 60, registry=registry, toolkit=script)
        assert pid not in baseline  # taken after the start sweep retired the leftover
        leftovers, foreign = harness._end_session(
            base, baseline, time.monotonic() + 60, registry=registry, toolkit=script
        )
        assert leftovers == [] and foreign == set()
        return

    if case == "own-end-retirement-is-not-foreign":
        # The start sweep could not retire it (so the baseline holds it); the end sweep does, and that is not foreign.
        leftovers, foreign = harness._end_session(base, {pid}, time.monotonic() + 60, registry=registry, toolkit=script)
        assert leftovers == [] and foreign == set() and not root.exists()
        return

    leftovers = harness._sweep(base, "end", time.monotonic() + 60, registry=registry, toolkit=script)

    assert script.calls[1][:2] == (("run", "recover"), {"context": locator})  # selection is by locator
    if case == "retry-on-contention":
        assert script.count("run", "recover") == 3
    if case in {"cleanup-not-clean", "cleanup-exit-zero-clean-false", "cleanup-nonzero-exit-clean-true"}:
        # The root is kept because cleanup was not both exit 0 and clean, so the project stays a reported leftover.
        assert root.exists() and len(leftovers) == 1 and leftovers[0].startswith(f"{pid} {root}")
    else:
        assert not root.exists() and leftovers == []


@pytest.mark.parametrize("root_present", [True, False])
def test_release_locator_is_used_only_while_the_root_exists(
    root_present: bool, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _harness()
    monkeypatch.setattr(time, "sleep", lambda _seconds: None)
    base = (tmp_path / "base").resolve()
    root = base / "test_a0" / "p"
    if root_present:
        root.mkdir(parents=True)
    pid = "a" * 64
    registry = _registry(tmp_path, **{pid: str(root)})
    blocked = {
        "project_id": pid,
        "project_root": str(root),
        "category": "incompatible_state",
        "reasons": ["run-nonterminal"],
        "archived_runs": [],
        "locators": [{"action": "run.release", "context": _locator(str(root))}],
    }
    script = _Scripted(
        registry,
        {
            (("project", "retire"), False): _ok({"eligible": [], "blocked": [blocked], "not_found": []}),
            (("run", "release"), False): _ok({"state": "released"}),
        },
    )

    harness._sweep(base, "start", time.monotonic() + 60, registry=registry, toolkit=script)

    assert script.count("run", "release") == (1 if root_present else 0)


@pytest.mark.parametrize("case", ["foreign-evidence", "foreign-archived-result"])
def test_sweep_never_executes_or_accepts_a_foreign_identity(
    case: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _harness()
    monkeypatch.setattr(time, "sleep", lambda _seconds: None)
    base = (tmp_path / "base").resolve()
    root = base / "test_a0" / "p"
    pid, foreign = "a" * 64, "b" * 64
    registry = _registry(tmp_path, **{pid: str(root)})
    executed: list[dict] = []

    def retire(payload: dict, _script: _Scripted) -> dict:
        executed.append(payload)
        return _ok({"results": [{"project_id": foreign, "outcome": "archived", "category": None}], "stopped": False})

    eligible = foreign if case == "foreign-evidence" else pid
    script = _Scripted(
        registry,
        {
            (("project", "retire"), False): _ok(
                {"eligible": [{"evidence": _evidence(eligible, str(root))}], "blocked": [], "not_found": []}
            ),
            (("project", "retire"), True): retire,
        },
    )

    with pytest.raises(AssertionError):
        harness._sweep(base, "start", time.monotonic() + 60, registry=registry, toolkit=script)

    # A foreign identity in the evidence is stopped before execute; one in the results is stopped right after it.
    assert executed == ([] if case == "foreign-evidence" else [{"evidence": [_evidence(pid, str(root))]}])


@pytest.mark.parametrize("case", ["blocked-after-cleanup", "refused-by-execute"])
def test_leftover_report_names_the_latest_reason(case: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    harness = _harness()
    monkeypatch.setattr(time, "sleep", lambda _seconds: None)
    base = (tmp_path / "base").resolve()
    root = base / "test_a0" / "p"
    pid = "a" * 64
    phase = "end" if case == "blocked-after-cleanup" else "start"
    if phase == "end":
        root.mkdir(parents=True)
    registry = _registry(tmp_path, **{pid: str(root)})
    calls = {"dry": 0}

    def dry_run(_payload: dict, _script: _Scripted) -> dict:
        calls["dry"] += 1
        if case == "refused-by-execute":
            return _ok({"eligible": [{"evidence": _evidence(pid, str(root))}], "blocked": [], "not_found": []})
        reason = "root-present" if calls["dry"] == 1 else "archive-conflict"
        blocked = {
            "project_id": pid,
            "project_root": str(root),
            "reasons": [reason],
            "archived_runs": [],
            "locators": [],
        }
        return _ok({"eligible": [], "blocked": [blocked], "not_found": []})

    refusal = {"project_id": pid, "outcome": "refused", "category": "incompatible_state", "reasons": ["drift"]}
    script = _Scripted(
        registry,
        {
            (("project", "retire"), False): dry_run,
            (("project", "retire"), True): _ok({"results": [refusal], "stopped": False}),
            (("cleanup", "project"), False): _ok({"clean": True}),
            (("cleanup", "project"), True): _ok({"clean": True}, 0),
        },
    )

    leftovers = harness._sweep(base, phase, time.monotonic() + 60, registry=registry, toolkit=script)

    expected = "archive-conflict" if case == "blocked-after-cleanup" else "drift"
    assert len(leftovers) == 1 and f"['{expected}']" in leftovers[0] and "root-present" not in leftovers[0]
    if case == "blocked-after-cleanup":
        assert not root.exists()  # the cleanup really ran first, so the first dry-run's "root-present" is stale


def _never_called(*_arguments, **_keywords) -> dict:
    raise AssertionError("selection must fail before any toolkit command runs")


@pytest.mark.parametrize("content", ["[1, 2]", "{}", '{"project_root": null}'])
def test_session_selection_fails_closed_on_a_record_without_a_root(content: str, tmp_path: Path) -> None:
    harness = _harness()
    registry = _registry(tmp_path)
    (registry / "projects" / f"{'a' * 64}.json").write_text(content)

    with pytest.raises(ValueError, match="no recorded root"):
        harness._sweep(
            (tmp_path / "base").resolve(), "end", time.monotonic() + 60, registry=registry, toolkit=_never_called
        )


def test_root_removal_never_follows_a_link_out_of_the_session_base(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _harness()
    monkeypatch.setattr(time, "sleep", lambda _seconds: None)
    base = (tmp_path / "base").resolve()
    outside = tmp_path / "outside"
    (outside / "p").mkdir(parents=True)
    base.mkdir()
    (base / "link").symlink_to(outside)
    root = base / "link" / "p"  # lexically inside base, physically outside it
    pid = "a" * 64
    registry = _registry(tmp_path, **{pid: str(root)})
    blocked = {
        "project_id": pid,
        "project_root": str(root),
        "reasons": ["root-present"],
        "archived_runs": [],
        "locators": [],
    }
    script = _Scripted(
        registry,
        {
            (("project", "retire"), False): _ok({"eligible": [], "blocked": [blocked], "not_found": []}),
            (("cleanup", "project"), False): _ok({"clean": True}),
            (("cleanup", "project"), True): _ok({"clean": True}, 0),
        },
    )

    leftovers = harness._sweep(base, "end", time.monotonic() + 60, registry=registry, toolkit=script)

    assert (outside / "p").is_dir()  # nothing outside the base is deleted
    assert script.count("cleanup", "project") == 0  # and cleanup, which resolves the root, never runs on a rebound root
    assert len(leftovers) == 1 and "root-present" in leftovers[0]


def test_a_project_rooted_at_the_session_base_itself_is_never_removed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _harness()
    monkeypatch.setattr(time, "sleep", lambda _seconds: None)
    base = (tmp_path / "base").resolve()
    base.mkdir()
    pid = "a" * 64
    registry = _registry(tmp_path, **{pid: str(base)})  # inside itself by is_relative_to, but it is the base
    blocked = {
        "project_id": pid,
        "project_root": str(base),
        "reasons": ["root-present"],
        "archived_runs": [],
        "locators": [],
    }
    script = _Scripted(
        registry,
        {
            (("project", "retire"), False): _ok({"eligible": [], "blocked": [blocked], "not_found": []}),
            (("cleanup", "project"), False): _ok({"clean": True}),
            (("cleanup", "project"), True): _ok({"clean": True}, 0),
        },
    )

    leftovers = harness._sweep(base, "end", time.monotonic() + 60, registry=registry, toolkit=script)

    assert base.is_dir() and script.count("cleanup", "project") == 0
    assert len(leftovers) == 1


def test_execute_is_single_shot_even_when_it_reports_a_retryable_category(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _harness()
    monkeypatch.setattr(time, "sleep", lambda _seconds: None)
    base = (tmp_path / "base").resolve()
    root = base / "test_a0" / "p"  # absent root: the project is eligible
    pid = "a" * 64
    registry = _registry(tmp_path, **{pid: str(root)})
    script = _Scripted(
        registry,
        {
            (("project", "retire"), False): _ok(
                {"eligible": [{"evidence": _evidence(pid, str(root))}], "blocked": [], "not_found": []}
            ),
            (("project", "retire"), True): _failure("contention", 4),
        },
    )

    harness._sweep(base, "start", time.monotonic() + 1, registry=registry, toolkit=script)

    assert sum(1 for _key, _payload, execute in script.calls if execute) == 1  # execute is never retried blindly


@pytest.mark.parametrize("action", ["run.recover", "run.release"])
@pytest.mark.parametrize("case", ["rebound-link", "session-base-itself"])
def test_locator_actions_are_not_sent_for_a_root_outside_the_session_base(
    case: str, action: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _harness()
    monkeypatch.setattr(time, "sleep", lambda _seconds: None)
    base = (tmp_path / "base").resolve()
    outside = tmp_path / "outside"
    (outside / "p").mkdir(parents=True)
    base.mkdir()
    (base / "link").symlink_to(outside)
    root = base / "link" / "p" if case == "rebound-link" else base
    pid = "a" * 64
    registry = _registry(tmp_path, **{pid: str(root)})
    blocked = {
        "project_id": pid,
        "project_root": str(root),
        "reasons": ["run-nonterminal"],
        "archived_runs": [],
        "locators": [{"action": action, "context": _locator(str(root))}],
    }
    script = _Scripted(
        registry,
        {
            (("project", "retire"), False): _ok({"eligible": [], "blocked": [blocked], "not_found": []}),
            (("run", "recover"), False): _ok({"state": "complete"}),
            (("run", "release"), False): _ok({"state": "released"}),
        },
    )

    leftovers = harness._sweep(base, "start", time.monotonic() + 60, registry=registry, toolkit=script)

    assert script.count("run", "recover") == 0 and script.count("run", "release") == 0
    assert len(leftovers) == 1


def test_a_root_rebound_while_cleanup_runs_is_not_removed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    harness = _harness()
    monkeypatch.setattr(time, "sleep", lambda _seconds: None)
    base = (tmp_path / "base").resolve()
    outside = tmp_path / "outside"
    (outside / "p").mkdir(parents=True)
    (base / "link" / "p").mkdir(parents=True)  # a real directory inside the base when the sweep checks it
    root = base / "link" / "p"
    pid = "a" * 64
    registry = _registry(tmp_path, **{pid: str(root)})
    blocked = {
        "project_id": pid,
        "project_root": str(root),
        "reasons": ["root-present"],
        "archived_runs": [],
        "locators": [],
    }

    def cleanup_and_rebind(_payload: dict, _script: _Scripted) -> dict:
        shutil.rmtree(base / "link")  # while cleanup runs, an outside actor swaps the ancestor for a link
        (base / "link").symlink_to(outside)
        return _ok({"clean": True}, 0)

    script = _Scripted(
        registry,
        {
            (("project", "retire"), False): _ok({"eligible": [], "blocked": [blocked], "not_found": []}),
            (("cleanup", "project"), False): _ok({"clean": True}),
            (("cleanup", "project"), True): cleanup_and_rebind,
        },
    )

    leftovers = harness._sweep(base, "end", time.monotonic() + 60, registry=registry, toolkit=script)

    assert (outside / "p").is_dir()  # the removal re-checks containment after cleanup returned
    assert len(leftovers) == 1
