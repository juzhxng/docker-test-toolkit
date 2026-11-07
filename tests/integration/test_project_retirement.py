"""Project retirement: the retire command, its per-project clauses, execute and crash resumption."""

from __future__ import annotations

import errno
import functools
import hashlib
import json
import os
import shutil
import signal
import stat
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from docker_test_toolkit import PhaseBudget, RunSpec
from docker_test_toolkit.adapters.inbound import cli
from docker_test_toolkit.application import build_cache, lifecycle, operator, recovery, retirement, sharing
from docker_test_toolkit.application.api import _open_run
from docker_test_toolkit.application.protocol import error_envelope
from docker_test_toolkit.application.recovery import Context, atomic_json, lock, temporary_roots
from docker_test_toolkit.application.retirement import retire_projects
from docker_test_toolkit.application.state import archive_directory
from docker_test_toolkit.domain.errors import (
    InfrastructureFailure,
    InvalidInput,
    ResourceBusy,
    ResourceError,
)
from tests.integration.account_fixtures import (
    deny_access,
    give_account_its_queue,
    private_account_directory,
    queue_entry,
    reap_orphan,
    source_artifact,
    wait_guardians_exited,
    write_v2_queue,
)

pytestmark = pytest.mark.integration


@dataclass(frozen=True)
class _Account:
    """A private registry and account with the actor observer the retirement command uses."""

    state_directory: Path
    account_directory: Path
    observer: Callable[[Mapping[str, object]], str]

    @property
    def artifact(self):
        """The identity run specifications of this account name (the running source tree)."""
        return source_artifact()


def _activated(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, observer=None) -> _Account:
    """Create a private account with a queue and private roots under tmp_path.

    Runs created in this process have its live pid as requester, so the default observer reports every recorded actor
    ``exited``; a test that needs a live actor passes its own observer.
    """
    temporary = tmp_path / "temporary"
    temporary.mkdir()
    monkeypatch.setenv("TMPDIR", str(temporary))
    state = tmp_path / "registry"
    account = state / "test-admission"
    state.mkdir(mode=0o700)
    account.mkdir(mode=0o700)
    give_account_its_queue(account)
    return _Account(state.resolve(), account.resolve(), observer or (lambda _identity: "exited"))


def _retire(service: _Account, payload: object, *, execute: bool, **seams) -> dict[str, object]:
    """Run the retirement command against ``service`` with its actor observer."""
    return retire_projects(
        service.state_directory,
        service.account_directory,
        payload,
        execute=execute,
        _observe_actor=service.observer,
        **seams,
    )


def _run(service: _Account, project: Path, *, enter: bool = False, retain: bool = False):
    project.mkdir(exist_ok=True)
    handle = _open_run(
        RunSpec(
            project,
            "integration",
            "retirement",
            (),
            (),
            "fixture",
            {"tests": PhaseBudget(0, 1, 1, 1)},
            "tests",
            service.artifact,
            retain=retain,
        ),
        test_state=service.state_directory,
        test_directory=service.account_directory,
    )
    if enter:
        handle.enter()
        handle.leave()
        # The guardian's de-index is still in flight when ``leave`` returns; an inspection listing ``projects/`` in
        # that moment sees its temporary file and (correctly) calls it a corrupt cell.
        wait_guardians_exited(service.state_directory)
    return handle


def _archive(service: _Account, run_id: str) -> Path:
    """Move one run file into the retirement archive layout (validator-seam proofs only)."""
    source = service.state_directory / "runs" / f"{run_id}.json"
    data = source.read_bytes()
    archive = archive_directory(service.state_directory)
    runs = archive / json.loads(data)["project_id"] / "runs"
    for directory in (archive, runs.parent, runs):
        directory.mkdir(mode=0o700, exist_ok=True)
    destination = runs / f"{run_id}-{hashlib.sha256(data).hexdigest()}.json"
    os.replace(source, destination)
    return destination


def _tree(*roots: Path) -> dict[str, object]:
    """Snapshot every entry under the roots (type and bytes) without following links.

    An unreadable subdirectory raises instead of shrinking the snapshot, so a byte-identity assertion
    can never pass because a directory could not be listed.
    """

    def _raise(error: OSError) -> None:
        raise error

    snapshot: dict[str, object] = {}
    for root in roots:
        if not os.path.lexists(root):
            continue
        for directory, dirnames, filenames in os.walk(root, onerror=_raise):
            for name in [directory, *(os.path.join(directory, child) for child in dirnames + filenames)]:
                observed = os.lstat(name)
                snapshot[name] = (
                    Path(name).read_bytes() if stat.S_ISREG(observed.st_mode) else stat.S_IFMT(observed.st_mode)
                )
    return snapshot


def test_retirement_shares_the_hex64_pattern() -> None:
    assert retirement._SHA256 is recovery.DOCKER_ID


def _relocate(path: Path, destination: Path) -> None:
    """Replace a directory with a symlink to its relocated private copy."""
    destination.parent.mkdir(mode=0o700, exist_ok=True)
    path.rename(destination)
    path.symlink_to(destination)


def _mutate_archive(case: str, tmp_path: Path, archived: Path, run_id: str) -> None:
    runs_dir = archived.parent
    project_dir = runs_dir.parent
    archive_root = project_dir.parent
    data = archived.read_bytes()
    elsewhere = tmp_path / "elsewhere"
    if case == "unreadable-archive":
        deny_access(archive_root)
    elif case == "uppercase-name":
        archived.rename(runs_dir / (archived.name.removesuffix(".json").upper() + ".json"))
    elif case == "extra-suffix":
        archived.rename(runs_dir / (archived.name + ".bak"))
    elif case == "nonprivate-runs-dir":
        runs_dir.chmod(0o755)
    elif case == "project-dir-symlink":
        _relocate(project_dir, elsewhere / "project")
    elif case == "nonprivate-project-dir":
        project_dir.chmod(0o755)
    elif case == "file-outside-runs":
        for outside in (project_dir / archived.name, archive_root / archived.name):
            outside.write_bytes(data)
            outside.chmod(0o600)
        archived.unlink()
    elif case == "archive-root-symlink":
        _relocate(archive_root, elsewhere / "archive")
    elif case == "nonprivate-archive-root":
        archive_root.chmod(0o755)
    elif case == "runs-symlink":
        _relocate(runs_dir, elsewhere / "runs")
    elif case == "file-group-readable":
        archived.chmod(0o640)
    else:
        raise AssertionError(case)


def _name_an_image(service: _Account, project: Path) -> None:
    """Make the project record name an image, so any Docker-backed inspection would call the factory."""
    record_path = Context(project, service.state_directory).record
    record = json.loads(record_path.read_text())
    record["images"] = {"sha256:" + "e" * 64: []}
    atomic_json(record_path, record)


_GATE_REFUSALS = {
    "journal": "qualification is in progress",
    "uncataloged": "not exactly qualified",
    "archived-missing": "missing from runs/ and its verified archive",
    "corrupt-run": "differs from its allowed legacy cells",
    "absent-anchor": "activation marker is absent",
}


# --- Runs prepared by a short-lived worker process whose requester is then gone ---

_WORKER = r"""
import json, os, sys, time
from pathlib import Path
from docker_test_toolkit.application import lifecycle
from docker_test_toolkit.application.api import _open_run
from docker_test_toolkit.domain.models import SUPPORTED_PROTOCOLS, TOOLKIT_VERSION, ArtifactIdentity
from docker_test_toolkit.domain.models import PhaseBudget, RunSpec
state, project, control, action = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3]), sys.argv[4]
account = state / "test-admission"
artifact = ArtifactIdentity(TOOLKIT_VERSION, lifecycle.source_identity(), dict(SUPPORTED_PROTOCOLS))
if action == "undeclared":
    context = lifecycle.prepare_run(
        lifecycle.RunRequest(project, "integration", "undeclared", (), (), "fixture"),
        _test_state=state,
        _test_directory=account,
    )
    control.write_text(json.dumps({"run_id": context.run_id}))
    os._exit(0)
handle = _open_run(
    RunSpec(project, "integration", action, (), (), "fixture", {"tests": PhaseBudget(0, 1, 1, 1)}, "tests", artifact),
    test_state=state,
    test_directory=account,
)
control.write_text(json.dumps({"run_id": handle.run_id}))
if action == "prepared":
    os._exit(0)
handle.enter()
control.with_suffix(".entered").write_text("entered")
time.sleep(600)
"""


def _spawn_worker(service: _Account, project: Path, control: Path, action: str) -> subprocess.Popen:
    """Start a short-lived process that prepares (and for some actions enters) one run of the project."""
    project.mkdir(exist_ok=True)
    return subprocess.Popen(
        [sys.executable, "-c", _WORKER, str(service.state_directory), str(project), str(control), action],
        env={**os.environ},
    )


def _wait_for(condition, what: str, timeout: float = 90.0):
    """Poll until `condition()` returns something truthy; a stall fails the test with what it waited for."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = condition()
        if value:
            return value
        time.sleep(0.05)
    raise AssertionError(f"Timed out waiting for {what}.")


def _worker(service: _Account, project: Path, control: Path, action: str = "undeclared") -> str:
    """Run a worker that exits on its own and return the run id it prepared (its requester is then dead)."""
    process = _spawn_worker(service, project, control, action)
    try:
        assert process.wait(timeout=120) == 0
    finally:
        if process.poll() is None:  # a stalled preparation must not outlive the test and keep mutating the account
            process.kill()
            process.wait(timeout=30)
    return json.loads(control.read_text())["run_id"]


def _prepare_undeclared(service: _Account, project: Path, *, entrypoint="integration", purpose="undeclared"):
    """Prepare one run in-process without declaring its artifact (the record keeps `artifact: None`)."""
    project.mkdir(exist_ok=True)
    return lifecycle.prepare_run(
        lifecycle.RunRequest(project, entrypoint, purpose, (), (), "fixture"),
        _test_state=service.state_directory,
        _test_directory=service.account_directory,
    )


def _run_context(service: _Account, run_id: str) -> lifecycle.RunContext:
    """Rebuild the exact persisted context of one run from its record, as the operator locator would."""
    record = service.state_directory / "runs" / f"{run_id}.json"
    data = json.loads(record.read_text())
    return lifecycle.RunContext(
        Context(Path(data["project_root"]), service.state_directory),
        run_id,
        data["session_id"],
        record,
        Path(data["private_root"]),
    )


def _rewrite(service: _Account, run_id: str, **changes: object) -> None:
    path = service.state_directory / "runs" / f"{run_id}.json"
    data = json.loads(path.read_text())
    data.update(changes)
    atomic_json(path, data)


def _classes(service: _Account, project: Path, run_id: str) -> tuple[str, str]:
    """Return the run-cell and project-cell classifications of one single-run project."""
    cells = service.inspect(wait_for_metadata=True)["cells"]
    project_id = Context(project.resolve(), service.state_directory).project_id
    run = next(cell for cell in cells if cell["path"] == f"runs/{run_id}.json")
    owner = next(cell for cell in cells if cell["path"] == f"projects/{project_id}.json")
    return str(run["classification"]), str(owner["classification"])


def _build_shape(service: _Account, project: Path, shape: str, *, declared: bool) -> str:
    """Build one undeclared (or declared) generation-zero run in the named shape and return its run id."""
    if declared:
        handle = _run(service, project)
        context, run_id = handle._context, handle.run_id
    else:
        context = _prepare_undeclared(service, project)
        run_id = context.run_id
    if shape == "prepared":
        return run_id
    lifecycle.cancel_run(context)
    if shape.startswith("cancelled-pending"):
        _rewrite(service, run_id, cleanup_outcome="pending", recovery_outcome=None, quiescent=False)
        if shape == "cancelled-pending-indexed":
            owner = recovery.load(context.project)
            owner["runs"][run_id] = str(context.record)
            recovery.save(context.project, owner)
    return run_id


_NEGATIVES = [
    "foreign-source",
    "generation",
    "generation-false",
    "generation-float",
    "non-catalog-source",
    "foreign-artifact",
    "invalid-record",
    "guardian-attempt",
    "retained-grant",
]


# --- Task 6: the dry-run ---


def _vanished_project(service: _Account, project: Path, *, runs: int = 1) -> tuple[str, list[str]]:
    """Create a project with `runs` cancelled runs, then delete its root; return its id and run ids."""
    run_ids = []
    for _ in range(runs):
        handle = _run(service, project)
        assert handle.cancel().state == "complete"
        run_ids.append(handle.run_id)
    shutil.rmtree(project)
    return Context(project, service.state_directory).project_id, sorted(run_ids)


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_a_hostile_queue_blocks_the_project_instead_of_crashing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = _activated(tmp_path, monkeypatch)
    project_id, _run_ids = _vanished_project(service, tmp_path / "project")
    queue = service.account_directory / "queue.json"
    queue.write_bytes(b"[" * 200_000 + b"]" * 200_000)  # nested far beyond the parser's recursion limit
    queue.chmod(0o600)

    result = _retire(service, {"project_ids": [project_id]}, execute=False)

    assert result["eligible"] == []
    assert [item["reasons"] for item in result["blocked"]] == [["queue-unreadable"]]


def test_dry_run_lists_an_eligible_vanished_project_with_exact_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = _activated(tmp_path, monkeypatch)
    project = tmp_path / "project"
    project_id, run_ids = _vanished_project(service, project)
    registry = service.state_directory
    run_file = registry / "runs" / f"{run_ids[0]}.json"
    before = _tree(registry, archive_directory(registry))
    session_roots = temporary_roots(json.loads(Context(project, registry).record.read_text()))

    result = _retire(service, {}, execute=False)

    assert result == {
        "mode": "dry-run",
        "registry": str(registry),
        "archive": str(archive_directory(registry)),
        "eligible": [
            {
                "evidence": {
                    "project_id": project_id,
                    "project_root": str(project),
                    "project_record_sha256": _sha(Context(project, registry).record),
                    "runs": [
                        {
                            "run_id": run_ids[0],
                            "sha256": _sha(run_file),
                        }
                    ],
                },
                "archived_runs": [],
                "shared_session_ids": [],
                "build_cache": None,
            }
        ],
        "blocked": [],
        "not_found": [],
        "summary": {"eligible": 1, "blocked": 0},
    }
    assert _tree(registry, archive_directory(registry)) == before
    assert not archive_directory(registry).exists()
    # Normal completion removes the run's empty project and session folders as well.
    assert session_roots and not any(root.exists() for root in session_roots)


@pytest.mark.parametrize(
    "payload",
    [
        {"project_ids": ["x"]},
        {"project_ids": ["a" * 64, "a" * 64]},
        {"project_ids": [format(index, "064x") for index in range(1025)]},
        {"project_ids": []},
        {"project_ids": ["A" * 64]},
        {"project_ids": ["A" * 64], "extra": 1},  # an ignored extra field never rescues an invalid selection
        {"evidence": []},  # execute evidence without --execute: a known field of the other mode, refused
        {"evidence": [{"project_id": "a" * 64}], "extra": 1},
        [],
    ],
)
def test_dry_run_refuses_malformed_requests_before_any_io(
    payload: object, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = _activated(tmp_path, monkeypatch)
    before = _tree(service.state_directory, archive_directory(service.state_directory))

    with pytest.raises(InvalidInput):
        _retire(service, payload, execute=False)  # type: ignore[arg-type]

    assert _tree(service.state_directory, archive_directory(service.state_directory)) == before


@pytest.mark.parametrize("case", ["archive-symlink", "archive-nonprivate"])
def test_dry_run_refuses_an_invalid_archive_root(case: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    service = _activated(tmp_path, monkeypatch)
    _vanished_project(service, tmp_path / "project")
    archive = archive_directory(service.state_directory)
    if case == "archive-symlink":
        target = tmp_path / "elsewhere"
        target.mkdir(mode=0o700)
        archive.symlink_to(target)
    else:
        archive.mkdir(mode=0o755)
    before = _tree(service.state_directory)

    with pytest.raises(InfrastructureFailure):
        _retire(service, {}, execute=False)

    assert _tree(service.state_directory) == before


def _blocked(result: dict, project_id: str) -> dict:
    assert result["eligible"] == [] or all(item["evidence"]["project_id"] != project_id for item in result["eligible"])
    return next(item for item in result["blocked"] if item["project_id"] == project_id)


def _untouched(service: _Account) -> dict[str, object]:
    return _tree(service.state_directory, archive_directory(service.state_directory))


_CLAUSE_MUTATIONS = {
    "quiescent": {"quiescent": False},
    "recovery_outcome": {"recovery_outcome": None},
    "cleanup_outcome": {"cleanup_outcome": "pending"},
    "pending_operation": {"pending_operation": {"kind": "compose"}},
    "receipts": {"docker_receipts": {"r": {"state": "pending"}}},
    "consumers": {"consumers": {"c": {"state": "active"}}},
    "resources": {"resources": {"containers": {}, "networks": {"n": {"state": "present"}}, "volumes": {}}},
    "owned": {"owned_resources": {"containers": {"y": {"state": "stopped"}}}},
    "endpoints": {"endpoints": {"cache": [{"active": True}]}},
}
_GATE_LEVEL = [
    "group-readable-run",
    "negative-revision",
    "executing-pinned-requester",
    "pinned-private-root",
    "legacy-pinned-live",
    "archived-evidence-missing",
    "qualification-journal",
    "uncataloged-artifact",
    "runs-directory-unreadable",
    "project-stem-mismatch",
    "unattributable-unpinned",
    *[f"{pin}-gen1-{name}" for pin in ("unpinned", "pinned") for name in _CLAUSE_MUTATIONS],
]


def _run_path(service: _Account, run_id: str) -> Path:
    return service.state_directory / "runs" / f"{run_id}.json"


def _project_with_vanished_root(service: _Account, project: Path, *, cancel: bool = True) -> tuple[str, str]:
    """Create a project with one run (cancelled unless asked otherwise), delete its root, return ids."""
    handle = _run(service, project)
    if cancel:
        assert handle.cancel().state == "complete"
    shutil.rmtree(project)
    return Context(project, service.state_directory).project_id, handle.run_id


def _rewrite_project(service: _Account, project: Path, **changes: object) -> None:
    path = Context(project, service.state_directory).record
    data = json.loads(path.read_text())
    data.update(changes)
    atomic_json(path, data)


_PER_PROJECT = [
    ("run-nonterminal", "run-nonterminal", "incompatible_state", {"run.recover"}),
    ("run-retained", "run-retained", "incompatible_state", {"run.release"}),
    ("run-cleanup-pending", "run-cleanup-pending", "incompatible_state", {"run.recover"}),
    ("run-recovery-incomplete", "run-recovery-incomplete", "incompatible_state", {"run.recover"}),
    ("run-not-quiescent", "run-not-quiescent", "incompatible_state", {"run.recover"}),
    ("images-present", "images-present", "incompatible_state", set()),
    ("pending-present", "pending-present", "incompatible_state", set()),
    ("build-records-present", "build-records-present", "incompatible_state", set()),
    ("indexed-runs", "indexed-runs", "incompatible_state", {"run.recover"}),
    ("queue-reference-v2", "queue-reference", "incompatible_state", set()),
    ("private-root-present-current", "private-root-present", "incompatible_state", set()),
    ("private-root-present-dangling", "private-root-present", "incompatible_state", set()),
    ("session-root-present", "session-root-present", "incompatible_state", set()),
    ("actor-live", "actor-live", "contention", set()),
    ("actor-unverifiable-unknown", "actor-unverifiable", "incompatible_state", set()),
    ("root-unobservable", "root-unobservable", "infrastructure_failure", set()),
    ("root-present", "root-present", "incompatible_state", set()),
]


@pytest.mark.parametrize(("case", "code", "category", "actions"), _PER_PROJECT, ids=[row[0] for row in _PER_PROJECT])
def test_per_project_refusals_are_success_values(
    case: str, code: str, category: str, actions: set[str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = tmp_path / "project"
    restore: list[Path] = []
    gate_service: _Account | None = None
    service = _activated(tmp_path, monkeypatch)
    if case == "run-retained":
        handle = _run(service, project, enter=True, retain=True)
        shutil.rmtree(project)
        project_id = Context(project, service.state_directory).project_id
        assert handle.status().state == "stopped-retained"
    elif case == "root-unobservable":
        (tmp_path / "locked").mkdir()
        project = tmp_path / "locked" / "child"
        project_id, _run_id = _project_with_vanished_root(service, project)
        restore.append(tmp_path / "locked")
    elif case == "root-present":
        handle = _run(service, project)
        assert handle.cancel().state == "complete"
        project_id = Context(project, service.state_directory).project_id
    else:
        project_id, run_id = _project_with_vanished_root(service, project, cancel=case != "run-nonterminal")
        if case == "run-cleanup-pending":
            _rewrite(service, run_id, cleanup_outcome="pending")
        elif case in {"run-recovery-incomplete", "run-not-quiescent"}:
            _rewrite(service, run_id, recovery_outcome=None, quiescent=False)
        elif case == "images-present":
            _rewrite_project(service, project, images={"sha256:" + "e" * 64: []})
        elif case == "pending-present":
            _rewrite_project(service, project, pending={"containers": ["a" * 64]})
        elif case == "build-records-present":
            _rewrite_project(service, project, build_history={"recorded": True})
        elif case == "indexed-runs":
            context = Context(project, service.state_directory)
            owner_record = recovery.load(context)
            owner_record["runs"][run_id] = str(_run_path(service, run_id))
            recovery.save(context, owner_record)
        elif case == "private-root-present-current":
            Path(json.loads(_run_path(service, run_id).read_text())["private_root"]).mkdir(parents=True)
        elif case == "private-root-present-dangling":
            private = Path(json.loads(_run_path(service, run_id).read_text())["private_root"])
            private.parent.mkdir(parents=True, exist_ok=True)
            private.symlink_to(tmp_path / "missing")  # lstat succeeds on a dangling link: it is still present
        elif case == "session-root-present":
            owner_record = json.loads(Context(project, service.state_directory).record.read_text())
            session_root = temporary_roots(owner_record)[0]
            session_root.mkdir(parents=True, exist_ok=True)
            (session_root / "stray.txt").write_text("authority")
        elif case == "actor-live":
            gate_service = _Account(
                service.state_directory,
                service.account_directory,
                lambda identity: "executing" if identity.get("pid") == os.getpid() else "exited",
            )
        elif case == "actor-unverifiable-unknown":
            gate_service = _Account(
                service.state_directory,
                service.account_directory,
                lambda _identity: "unknown",
            )
        elif case == "queue-reference-v2":
            handle = _run(service, tmp_path / "queued", enter=True)
            record = json.loads(_run_path(service, handle.run_id).read_text())
            atomic_json(
                service.account_directory / "queue.json",
                {
                    "version": 2,
                    "next_ticket": record["ticket"] + 1,
                    "entries": {
                        handle.run_id: {
                            "attempt": record["guardian_attempt"],
                            "dead_since": None,
                            "holder": None,
                            "last_completed_phase": None,
                            "phase": "queued",
                            "project_id": hashlib.sha256(record["project_root"].encode()).hexdigest(),
                            "project_root": record["project_root"],
                            "record": str(_run_path(service, handle.run_id)),
                            "registry": str(service.state_directory),
                            "takeover": None,
                            "ticket": record["ticket"],
                        }
                    },
                },
            )
            shutil.rmtree(tmp_path / "queued")
            project_id = Context(tmp_path / "queued", service.state_directory).project_id
    before = _untouched(service)
    undo: list = []
    try:
        undo.extend(deny_access(directory, loop=True) for directory in restore)
        result = _retire(gate_service or service, {"project_ids": [project_id]}, execute=False)
    finally:
        for restore_directory in undo:
            restore_directory()

    blocked = _blocked(result, project_id)
    assert code in blocked["reasons"] and blocked["category"] == category
    assert {locator["action"] for locator in blocked["locators"]} == actions
    if case == "run-invalid":
        assert blocked["reasons"] == ["run-invalid"]
    assert _untouched(service) == before
    assert not archive_directory(service.state_directory).exists()


_REVIEW_FOCUS = [
    "relative-root",
    "dangling-root-symlink",
    "root-is-file",
    "root-under-file",
    "session-root-symlink",
    "symlink-inside-session-root",
]


@pytest.mark.parametrize("case", _REVIEW_FOCUS)
def test_review_focus_inputs(case: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    if case == "relative-root":
        service = _activated(tmp_path, monkeypatch)
        project_id = hashlib.sha256(b"relative/project").hexdigest()
        atomic_json(
            service.state_directory / "projects" / f"{project_id}.json",
            {"version": 2, "project_root": "relative/project", "sessions": {}, "images": {}, "runs": {}},
        )
        # A directory with that relative name exists under the working directory: it must never be consulted.
        (tmp_path / "relative" / "project").mkdir(parents=True)
        monkeypatch.chdir(tmp_path)
    else:
        service = _activated(tmp_path, monkeypatch)
        if case == "root-under-file":
            (tmp_path / "parent").mkdir()
            project = tmp_path / "parent" / "child"
        else:
            project = tmp_path / "project"
        project_id, _run_id = _project_with_vanished_root(service, project)
        owner_record = json.loads(Context(project, service.state_directory).record.read_text())
        session_root = temporary_roots(owner_record)[0]
        if case in ("session-root-symlink", "symlink-inside-session-root"):
            session_root.mkdir(mode=0o700)  # the run removed it; an older installed toolkit leaves it behind
        if case == "dangling-root-symlink":
            project.symlink_to(tmp_path / "missing")
        elif case == "root-is-file":
            project.write_text("not a directory")
        elif case == "root-under-file":
            shutil.rmtree(tmp_path / "parent")
            (tmp_path / "parent").write_text("a regular file where the parent directory was")
        elif case == "session-root-symlink":
            empty = tmp_path / "empty"
            empty.mkdir()
            shutil.rmtree(session_root)
            session_root.symlink_to(empty)
        elif case == "symlink-inside-session-root":
            empty = tmp_path / "empty"
            empty.mkdir()
            (session_root / "link").symlink_to(empty)
    before = _untouched(service)

    result = _retire(service, {"project_ids": [project_id]}, execute=False)

    assert _untouched(service) == before
    if case == "root-under-file":
        assert [item["evidence"]["project_id"] for item in result["eligible"]] == [project_id]
        assert result["blocked"] == []
        return
    blocked = _blocked(result, project_id)
    expected = {
        "relative-root": ["record-invalid"],
        "dangling-root-symlink": ["root-present"],
        "root-is-file": ["root-present"],
        "session-root-symlink": ["session-root-present"],
        "symlink-inside-session-root": ["session-root-present"],
        "root-only-attribution": ["run-invalid"],
    }[case]
    assert blocked["reasons"] == expected  # in particular never "run-unattributable" for a root-only record


def test_bulk_dry_run_lists_partial_projects_only_as_root_present(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = _activated(tmp_path, monkeypatch)
    project = tmp_path / "project"
    handle = _run(service, project)
    assert handle.cancel().state == "complete"
    project_id = Context(project, service.state_directory).project_id
    _archive(service, handle.run_id)  # a partial move: the root still exists
    survivor, _survivor_run = _project_with_vanished_root(service, tmp_path / "survivor")
    durable = tmp_path / "durable"  # a real project with terminal history and an existing root
    assert _run(service, durable).cancel().state == "complete"
    durable_id = Context(durable, service.state_directory).project_id

    result = _retire(service, {}, execute=False)

    assert [item["evidence"]["project_id"] for item in result["eligible"]] == [survivor]
    partial = _blocked(result, project_id)
    assert partial["reasons"] == ["root-present"] and partial["archived_runs"] == [handle.run_id]
    assert [item["project_id"] for item in result["blocked"]] == [project_id]  # never selected implicitly
    named = _retire(service, {"project_ids": [durable_id]}, execute=False)
    assert _blocked(named, durable_id)["reasons"] == ["root-present"]  # only an explicit request reports it


# --- Task 7: the locked execute ---


class _Crash(Exception):
    """Simulate a process dying at a durable transition; deliberately not an OSError."""


def _evidence(service: _Account) -> list[dict]:
    """The eligible evidence objects of a bulk dry-run, exactly as an operator would replay them."""
    return [item["evidence"] for item in _retire(service, {}, execute=False)["eligible"]]


def _execute(service: _Account, evidence: list[dict], **seams: object) -> dict:
    return _retire(service, {"evidence": evidence}, execute=True, **seams)  # type: ignore[arg-type]


def _two_run_project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, name: str = "project"):
    """One project with two cancelled runs, root deleted; returns the account, project, run ids and the first run."""
    service = _activated(tmp_path, monkeypatch)
    project = tmp_path / name
    first = _run(service, project)
    assert first.cancel().state == "complete"
    candidate = service
    second = _run(candidate, project)
    assert second.cancel().state == "complete"
    shutil.rmtree(project)
    return candidate, project, sorted([first.run_id, second.run_id]), first.run_id


def _archive_files(candidate: _Account) -> list[Path]:
    archive = archive_directory(candidate.state_directory)
    return sorted(path for path in archive.rglob("*") if path.is_file()) if archive.exists() else []


def _run_files(candidate: _Account) -> set[str]:
    return {path.stem for path in (candidate.state_directory / "runs").glob("*.json")}


def test_execute_archives_a_reviewed_project_and_leaves_the_account_valid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    candidate, project, run_ids, _first = _two_run_project(tmp_path, monkeypatch)
    project_id = Context(project, candidate.state_directory).project_id
    project_sha = _sha(Context(project, candidate.state_directory).record)

    result = _execute(candidate, _evidence(candidate))

    assert result == {
        "mode": "execute",
        "registry": str(candidate.state_directory),
        "archive": str(archive_directory(candidate.state_directory)),
        "results": [
            {
                "project_id": project_id,
                "outcome": "archived",
                "category": None,
                "reasons": [],
                "archived_runs": run_ids,
                "archived_project": f"project-{project_sha}.json",
                "build_cache": None,
            }
        ],
        "unreviewed_vanished_roots": [],
        "stopped": False,
    }
    assert not Context(project, candidate.state_directory).record.exists()
    assert not (_run_files(candidate) & set(run_ids))
    files = _archive_files(candidate)
    assert len(files) == 3
    for path in files:
        digest = path.name.removesuffix(".json").rsplit("-", 1)[-1]
        assert hashlib.sha256(path.read_bytes()).hexdigest() == digest


def test_retire_removes_verdicts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    candidate, project, _run_ids, _first = _two_run_project(tmp_path, monkeypatch)
    project_id = Context(project, candidate.state_directory).project_id
    other = "f" * 64
    for owner in (project_id, other):
        sharing.write_verdict(
            candidate.state_directory, owner, "k", shareable=True, reason=None, run_id="r", origin="o", tick_ns=1
        )

    assert _execute(candidate, _evidence(candidate))["results"][0]["outcome"] == "archived"

    assert not sharing.verdict_path(candidate.state_directory, project_id).exists()
    assert sharing.verdict_path(candidate.state_directory, other).exists()  # another project's predictions stay


def _only_in_one_place(candidate: _Account, run_ids: list[str]) -> None:
    """Every run id appears exactly once across the registry and the archive."""
    archived = [
        path.name.removesuffix(".json").rsplit("-", 1)[0] for path in _archive_files(candidate) if "runs" in path.parts
    ]
    for run_id in run_ids:
        assert (run_id in _run_files(candidate)) + archived.count(run_id) == 1, run_id


def test_execute_touches_only_reviewed_projects(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    service = _activated(tmp_path, monkeypatch)
    target, target_run = _project_with_vanished_root(service, tmp_path / "target")
    unreviewed, _unreviewed_run = _project_with_vanished_root(service, tmp_path / "unreviewed")
    pending_id, pending_run = _project_with_vanished_root(service, tmp_path / "pending")
    _rewrite(service, pending_run, cleanup_outcome="pending")
    existing = tmp_path / "existing"
    assert _run(service, existing).cancel().state == "complete"
    existing_id = Context(existing, service.state_directory).project_id
    evidence = [item for item in _evidence(service) if item["project_id"] == target]
    assert len(evidence) == 1
    named = _retire(service, {"project_ids": [existing_id, pending_id]}, execute=False)
    assert _blocked(named, existing_id)["reasons"] == ["root-present"]
    assert "run-cleanup-pending" in _blocked(named, pending_id)["reasons"]
    foreign = {
        path: data
        for path, data in _tree(service.state_directory).items()
        if target_run not in path and target not in path and not path.endswith(".lock")
    }

    result = _execute(service, evidence)

    assert [entry["outcome"] for entry in result["results"]] == ["archived"]
    assert result["unreviewed_vanished_roots"] == sorted([unreviewed, pending_id])
    after = _tree(service.state_directory)
    assert all(after.get(path) == data for path, data in foreign.items())
    archive = archive_directory(service.state_directory)
    assert sorted(entry.name for entry in archive.iterdir()) == [target]


def test_one_time_disposition_dry_run_execute_final_dry_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    service = _activated(tmp_path, monkeypatch)
    pinned_id, pinned_run = _project_with_vanished_root(service, tmp_path / "pinned")
    candidate = service
    unpinned_id, _unpinned_run = _project_with_vanished_root(candidate, tmp_path / "unpinned")
    pending_id, pending_run = _project_with_vanished_root(candidate, tmp_path / "pending")
    _rewrite(candidate, pending_run, cleanup_outcome="pending")
    indexed_id, indexed_run = _project_with_vanished_root(candidate, tmp_path / "indexed")
    indexed_context = Context(tmp_path / "indexed", candidate.state_directory)
    owner = recovery.load(indexed_context)
    owner["runs"][indexed_run] = str(_run_path(candidate, indexed_run))
    recovery.save(indexed_context, owner)
    existing = tmp_path / "existing"
    assert _run(candidate, existing).cancel().state == "complete"
    existing_record = Context(existing, candidate.state_directory).record
    existing_bytes = existing_record.read_bytes()
    dry = _retire(candidate, {}, execute=False)
    assert sorted(item["evidence"]["project_id"] for item in dry["eligible"]) == sorted([pinned_id, unpinned_id])

    result = _execute(candidate, [item["evidence"] for item in dry["eligible"]])

    assert [entry["outcome"] for entry in result["results"]] == ["archived", "archived"]
    final = _retire(candidate, {}, execute=False)
    assert final["eligible"] == []
    assert {item["project_id"] for item in final["blocked"]} == {pending_id, indexed_id}
    assert {code for item in final["blocked"] for code in item["reasons"]} >= {"run-cleanup-pending", "indexed-runs"}
    assert existing_record.read_bytes() == existing_bytes
    _only_in_one_place(candidate, [pinned_run])


@pytest.mark.parametrize(
    "stage", ["after-run-rename-first", "after-run-rename-last", "before-project-rename", "after-project-rename"]
)
def test_crash_at_each_durable_transition_resumes_to_fully_archived(
    stage: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    candidate, project, run_ids, _pinned = _two_run_project(tmp_path, monkeypatch)
    project_id = Context(project, candidate.state_directory).project_id
    seen: list[str] = []

    def failpoint(name: str, identity: str) -> None:
        if name == "after-run-rename":
            seen.append(identity)
            if (stage == "after-run-rename-first" and len(seen) == 1) or (
                stage == "after-run-rename-last" and len(seen) == 2
            ):
                raise _Crash
        elif name == stage:
            raise _Crash

    with pytest.raises(_Crash):
        _execute(candidate, _evidence(candidate), failpoint=failpoint)

    _only_in_one_place(candidate, run_ids)
    if stage == "after-project-rename":
        assert _retire(candidate, {"project_ids": [project_id]}, execute=False)["not_found"] == [project_id]
        return
    resumed = _execute(candidate, _evidence(candidate))
    assert resumed["results"][0]["outcome"] == "archived"
    assert not Context(project, candidate.state_directory).record.exists()
    _only_in_one_place(candidate, run_ids)


@pytest.mark.parametrize("case", ["root", "run", "unattributable"])
def test_interference_before_final_rederivation_refuses_the_project_move(
    case: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    candidate, project, run_ids, _pinned = _two_run_project(tmp_path, monkeypatch)
    project_id = Context(project, candidate.state_directory).project_id

    def failpoint(name: str, _identity: str) -> None:
        if name != "before-final-rederivation":
            return
        if case == "root":
            project.mkdir()
        elif case == "run":
            atomic_json(_run_path(candidate, str(uuid.uuid4())), {"project_id": project_id})
        else:
            stray = _run_path(candidate, str(uuid.uuid4()))
            stray.write_text("this is not json")
            stray.chmod(0o600)

    result = _execute(candidate, _evidence(candidate), failpoint=failpoint)["results"][0]

    expected = "run-unattributable" if case == "unattributable" else "reincarnated"
    assert result["outcome"] == "refused" and result["reasons"] == [expected]
    assert result["archived_runs"] == run_ids
    assert Context(project, candidate.state_directory).record.exists()


def test_reincarnation_after_a_partial_move_is_reported_not_reversed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    candidate, project, run_ids, _pinned = _two_run_project(tmp_path, monkeypatch)
    seen: list[str] = []

    def failpoint(name: str, identity: str) -> None:
        if name == "after-run-rename":
            seen.append(identity)
            raise _Crash

    with pytest.raises(_Crash):
        _execute(candidate, _evidence(candidate), failpoint=failpoint)
    project.mkdir()
    _run(candidate, project)  # the project is used again: the root and a new run exist

    result = _retire(candidate, {}, execute=False)

    partial = _blocked(result, Context(project, candidate.state_directory).project_id)
    # A present root must not short-circuit the derivation: the new run is still reported, with its recovery locator.
    assert {"root-present", "run-nonterminal"} <= set(partial["reasons"]) and partial["archived_runs"] == seen
    assert {locator["action"] for locator in partial["locators"]} == {"run.recover"}
    assert [path.name.startswith(seen[0]) for path in _archive_files(candidate)] == [True]  # nothing moved back
    assert len(run_ids) == 2


def test_cross_device_rename_stops_after_the_failed_project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    service = _activated(tmp_path, monkeypatch)
    first_ids = _vanished_project(service, tmp_path / "one", runs=2)
    second_ids = _vanished_project(service, tmp_path / "two", runs=2)
    evidence = _evidence(service)
    first_evidence = evidence[0]
    second_run = first_evidence["runs"][1]["run_id"]

    def failing_rename(source: Path, destination: Path) -> None:
        if source.name == f"{second_run}.json":
            raise OSError(errno.EXDEV, "cross-device")
        os.replace(source, destination)

    result = _execute(service, evidence, rename=failing_rename)

    first, second = result["results"]
    assert (first["outcome"], first["category"], first["reasons"]) == (
        "refused",
        "infrastructure_failure",
        ["rename-failed"],
    )
    assert first["archived_runs"] == [first_evidence["runs"][0]["run_id"]]
    assert (second["outcome"], second["archived_runs"]) == ("not-attempted", []) and result["stopped"] is True
    all_ids = first_ids[1] + second_ids[1]
    _only_in_one_place(service, all_ids)
    assert _execute(service, _evidence(service))["stopped"] is False
    _only_in_one_place(service, all_ids)


_REFUSALS = [
    "p-lock",
    "metadata-lock",
    "drift",
    "account-changed",
    "archive-conflict",
    "archive-conflict-later",
    "identical-destination",
    "archive-subdir-project-dir-symlink",
    "archive-subdir-nonprivate-runs-dir",
]


@pytest.mark.parametrize("case", _REFUSALS)
def test_execute_refuses_under_contention_drift_or_account_change(
    case: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    candidate, project, run_ids, _pinned = _two_run_project(tmp_path, monkeypatch)
    registry, account = candidate.state_directory, candidate.account_directory
    context = Context(project, registry)
    project_id = context.project_id
    evidence = _evidence(candidate)
    archive = archive_directory(registry)
    destinations = {
        entry["run_id"]: archive / project_id / "runs" / f"{entry['run_id']}-{entry['sha256']}.json"
        for entry in evidence[0]["runs"]
    }
    if case.startswith(("archive-", "identical-")):
        (archive / project_id / "runs").mkdir(parents=True, mode=0o700)
        for directory in (archive, archive / project_id, archive / project_id / "runs"):
            directory.chmod(0o700)
    other = None
    if case == "archive-conflict-later":
        other, _other_runs = _vanished_project(candidate, tmp_path / "other")
        everything = _evidence(candidate)
        assert {item["project_id"] for item in everything} == {project_id, other}
        # The conflicting project goes first, so "the next project is attempted" is a real claim, not hash order.
        evidence = sorted(everything, key=lambda item: item["project_id"] != project_id)
        assert [item["project_id"] for item in evidence] == [project_id, other]
    seams: dict[str, object] = {}
    conflict_run = evidence[0]["runs"][0]["run_id"]
    if case == "archive-conflict":
        destinations[conflict_run].write_bytes(b"not the archived bytes")
        destinations[conflict_run].chmod(0o600)
    elif case == "archive-conflict-later":
        conflict_run = evidence[[item["project_id"] for item in evidence].index(project_id)]["runs"][1]["run_id"]
        destinations[conflict_run].write_bytes(b"not the archived bytes")
        destinations[conflict_run].chmod(0o600)
    elif case == "identical-destination":
        first = evidence[0]["runs"][0]["run_id"]
        destinations[first].write_bytes(_run_path(candidate, first).read_bytes())
        destinations[first].chmod(0o600)
    elif case == "archive-subdir-project-dir-symlink":
        shutil.rmtree(archive / project_id)
        elsewhere = tmp_path / "elsewhere"
        elsewhere.mkdir(mode=0o700)
        (archive / project_id).symlink_to(elsewhere)
    elif case == "archive-subdir-nonprivate-runs-dir":
        (archive / project_id / "runs").chmod(0o755)
    elif case == "drift":
        recovery.register(context, session_id=str(uuid.uuid4()), temporary_parent=tmp_path / "temporary")
    elif case == "account-changed":
        anchor = account / "metadata.lock"

        def probing_observer(_identity: dict) -> str:
            try:
                with lock(context, project_id, shared=True, nonblocking=True):
                    pass
            except ResourceError:  # execute holds the project: change the account as a concurrent actor would
                anchor.write_bytes(anchor.read_bytes() + b"changed\n")
            return "exited"

        candidate = _Account(registry, account, probing_observer)
    before_foreign = {key: value for key, value in _tree(registry).items() if not key.endswith(".lock")}
    held = None
    try:
        if case == "p-lock":
            held = lock(context, project_id, shared=True)
            held.__enter__()
        elif case == "metadata-lock":
            held = lock(Context(account, account), "metadata", shared=True)
            held.__enter__()
        result = _execute(candidate, evidence, **seams)
    finally:
        if held is not None:
            held.__exit__(None, None, None)

    entry = next(item for item in result["results"] if item["project_id"] == project_id)
    run_sources_present = {run_id for run_id in run_ids if run_id in _run_files(candidate)}
    if case == "identical-destination":
        assert entry["outcome"] == "archived" and entry["archived_runs"] == run_ids
        return
    expected = {
        "p-lock": ("contention", ["lock-held"]),
        "metadata-lock": ("contention", ["lock-held"]),
        "drift": ("incompatible_state", ["drift"]),
        "account-changed": ("incompatible_state", ["account-changed"]),
        "archive-conflict": ("incompatible_state", ["archive-conflict"]),
        "archive-conflict-later": ("incompatible_state", ["archive-conflict"]),
        "archive-subdir-project-dir-symlink": ("infrastructure_failure", ["rename-failed"]),
        "archive-subdir-nonprivate-runs-dir": ("infrastructure_failure", ["rename-failed"]),
    }[case]
    assert entry["outcome"] == "refused" and (entry["category"], entry["reasons"]) == expected
    assert context.record.exists()
    if case == "archive-conflict-later":
        assert entry["archived_runs"] == [run_ids[0]] and run_sources_present == {run_ids[1]}
        assert destinations[run_ids[1]].read_bytes() == b"not the archived bytes"
        assert result["stopped"] is False
        assert [item["outcome"] for item in result["results"]] == ["refused", "archived"]
        destinations[run_ids[1]].unlink()
        assert _execute(candidate, _evidence(candidate))["results"][0]["outcome"] == "archived"
    elif case == "archive-conflict":
        assert entry["archived_runs"] == [] and run_sources_present == set(run_ids)
        assert destinations[conflict_run].read_bytes() == b"not the archived bytes"
        assert result["stopped"] is False
    elif case.startswith("archive-subdir"):
        assert entry["archived_runs"] == [] and run_sources_present == set(run_ids) and result["stopped"] is True
    else:
        assert entry["archived_runs"] == [] and run_sources_present == set(run_ids) and result["stopped"] is False
        after_foreign = {key: value for key, value in _tree(registry).items() if not key.endswith(".lock")}
        assert {key: after_foreign.get(key) for key in before_foreign} == before_foreign


def test_execute_retries_a_briefly_held_metadata_lock(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    candidate, _project, _run_ids, _pinned = _two_run_project(tmp_path, monkeypatch)
    account = candidate.account_directory
    counter = {"calls": 0, "trigger": -1}
    acquired = threading.Event()

    def hold_metadata_briefly() -> None:
        with lock(Context(account, account), "metadata"):
            acquired.set()
            time.sleep(0.4)

    def observer(_identity: dict) -> str:
        counter["calls"] += 1
        if counter["calls"] == counter["trigger"]:  # the first observation made by execute's own derivation
            threading.Thread(target=hold_metadata_briefly).start()
            assert acquired.wait(5)
        return "exited"

    observed = _Account(candidate.state_directory, account, observer)
    evidence = _evidence(observed)
    counter["calls"], counter["trigger"] = 0, 1

    result = _execute(observed, evidence)

    assert acquired.is_set() and result["results"][0]["outcome"] == "archived"


# --- Task 8: locator binding and the retirement command over the CLI ---

_LOCATOR_COMMANDS = ["run.status", "run.recover", "run.release"]


def _locator(handle) -> dict[str, str]:
    """The six-field locator the toolkit itself emits for a run."""
    return {
        "project_root": str(handle._context.project.root),
        "project_state": str(handle._context.project.state),
        "run_id": handle.run_id,
        "session_id": handle.session_id,
        "record": str(handle._context.record),
        "private_root": str(handle.private_root),
    }


@pytest.mark.parametrize("command", _LOCATOR_COMMANDS)
@pytest.mark.parametrize("form", ["direct", "envelope"])
@pytest.mark.parametrize(
    "fault", ["foreign-state", "symlinked-state", "other-record", "absolute-run-id", "traversal-run-id"]
)
def test_run_locator_commands_refuse_locators_outside_the_registry(
    command: str, form: str, fault: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = _activated(tmp_path, monkeypatch)
    handle = _run(service, tmp_path / "project")
    monkeypatch.setattr(operator, "registry_directory", lambda: service.state_directory)
    locator = _locator(handle)
    foreign = tmp_path / "foreign"
    foreign.mkdir(mode=0o700)
    if fault == "foreign-state":
        locator["project_state"] = str(foreign)
        locator["record"] = str(foreign / "runs" / f"{handle.run_id}.json")
    elif fault == "symlinked-state":  # a path that resolves to the registry is still not the registry
        alias = tmp_path / "alias"
        alias.symlink_to(service.state_directory)
        locator["project_state"] = str(alias)
        locator["record"] = str(alias / "runs" / f"{handle.run_id}.json")
    elif fault == "absolute-run-id":  # an absolute final operand would replace the registry prefix of the record path
        locator["run_id"] = str(foreign / "probe")
        locator["record"] = str(foreign / "probe.json")
    elif fault == "traversal-run-id":
        locator["run_id"] = "../escape"
        locator["record"] = str(service.state_directory / "runs" / "../escape.json")
    else:
        locator["record"] = str(service.state_directory / "runs" / f"{uuid.uuid4()}.json")
    payload = locator if form == "direct" else {"context": locator}
    before = _tree(foreign, service.state_directory, archive_directory(service.state_directory))

    with pytest.raises(InvalidInput, match="outside the registry"):
        operator.execute(command, payload, docker_factory=lambda *_: None)

    assert _tree(foreign, service.state_directory, archive_directory(service.state_directory)) == before
    assert not list(foreign.rglob("*.lock"))


@pytest.mark.parametrize("command", _LOCATOR_COMMANDS)
@pytest.mark.parametrize("fault", ["missing-field", "non-string", "envelope-missing-field"])
def test_run_locator_commands_refuse_incomplete_locators_before_any_io(
    command: str, fault: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = _activated(tmp_path, monkeypatch)
    handle = _run(service, tmp_path / "project")
    monkeypatch.setattr(operator, "registry_directory", lambda: service.state_directory)
    locator = _locator(handle)
    if fault == "non-string":
        locator["session_id"] = 7  # type: ignore[assignment]
    else:
        del locator["private_root"]
    payload = {"context": locator} if fault == "envelope-missing-field" else locator
    before = _untouched(service)

    with pytest.raises(InvalidInput, match="exact run context") as refused:
        operator.execute(command, payload, docker_factory=lambda *_: None)

    envelope, exit_code = error_envelope(command, refused.value)
    assert exit_code == 2 and envelope["error"]["category"] == "invalid_input"
    assert _untouched(service) == before


def test_project_retire_cli_round_trip_uses_dry_run_evidence_unmodified(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    candidate, project, run_ids, _pinned = _two_run_project(tmp_path, monkeypatch)
    monkeypatch.setattr(operator, "registry_directory", lambda: candidate.state_directory)
    monkeypatch.setattr(operator, "account_directory", lambda: candidate.account_directory)
    monkeypatch.setattr(
        operator, "retire_projects", functools.partial(retire_projects, _observe_actor=candidate.observer)
    )
    request = tmp_path / "request.json"
    request.write_text("{}")

    assert cli.main(["project", "retire", "--input", str(request), "--json"]) == 0
    dry = json.loads(capsys.readouterr().out)
    assert dry["command"] == "project.retire" and dry["cleanup"] is None and dry["result"]["mode"] == "dry-run"
    request.write_text(json.dumps({"evidence": [item["evidence"] for item in dry["result"]["eligible"]]}))

    assert cli.main(["project", "retire", "--input", str(request), "--execute", "--json"]) == 0
    executed = json.loads(capsys.readouterr().out)

    assert executed["cleanup"] is None and executed["ok"] is True
    assert [entry["outcome"] for entry in executed["result"]["results"]] == ["archived"]
    assert executed["result"]["results"][0]["archived_runs"] == run_ids
    assert not Context(project, candidate.state_directory).record.exists()


def test_emitted_locators_pass_the_registry_binding(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    service = _activated(tmp_path, monkeypatch)
    monkeypatch.setattr(operator, "registry_directory", lambda: service.state_directory)
    retained = _run(service, tmp_path / "retained", enter=True, retain=True)
    locator_file = tmp_path / "locator.json"
    retained.write_retained_locator(locator_file)

    status = operator.execute("run.status", json.loads(locator_file.read_text()), docker_factory=lambda *_: None)
    assert status["state"] == "stopped-retained"

    project_id, run_id = _project_with_vanished_root(service, tmp_path / "prepared", cancel=False)
    blocked = _blocked(_retire(service, {"project_ids": [project_id]}, execute=False), project_id)
    context = next(item["context"] for item in blocked["locators"] if item["context"]["run_id"] == run_id)
    prepared = operator.execute("run.status", {"context": context}, docker_factory=lambda *_: None)
    assert prepared["state"] == "prepared"


# --- an orphaned run is recovered by its exact recorded identity, without its root ---


def _locator_json(context: lifecycle.RunContext) -> str:
    return json.dumps(
        {
            "project_root": str(context.project.root),
            "project_state": str(context.project.state),
            "run_id": context.run_id,
            "session_id": context.session_id,
            "record": str(context.record),
            "private_root": str(context.private_root),
        }
    )


def _assert_recovered(service: _Account, context: lifecycle.RunContext, *, cleanup: str = "complete") -> None:
    """Recovery really happened: complete, quiescent, unindexed and the private root gone.

    A recovered crash (a killed guardian) keeps `cleanup_outcome == "incomplete"`, which the quiescence rules accept.
    """
    status = lifecycle.recover_run(context, _test_directory=service.account_directory)
    assert (status.state, status.cleanup_outcome) == ("complete", cleanup)
    assert lifecycle.quiescence_blockers(lifecycle.load_run_record(context)) == ()
    assert context.run_id not in recovery.load(context.project)["runs"]
    assert not context.private_root.exists() and not context.private_root.parent.parent.exists()


def _run_record(service: _Account, run_id: str) -> dict:
    return json.loads(_run_path(service, run_id).read_text())


def _kill(pid: int) -> None:
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def _exited(identity: dict) -> bool:
    return retirement._observe_actor(identity) == "exited"


def _reap_guardians(service: _Account) -> None:
    """Kill any guardian still alive in this test's private registry, also one that a timed-out wait never saw."""
    for path in (service.state_directory / "runs").glob("*.json"):
        guardian = json.loads(path.read_text()).get("guardian")
        if isinstance(guardian, dict) and retirement._observe_actor(guardian) == "executing":
            _kill(guardian["pid"])


def test_orphaned_declared_preparation_is_recovered_without_its_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = _activated(tmp_path, monkeypatch)
    project = tmp_path / "project"
    run_id = _worker(service, project, tmp_path / "control.json", "prepared")
    shutil.rmtree(project)

    context = lifecycle.load_operator_context(_locator_json(_run_context(service, run_id)))

    _assert_recovered(service, context)


def test_dead_owner_queued_run_is_recovered_without_its_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    service = _activated(tmp_path, monkeypatch)
    project = tmp_path / "project"
    account = service.account_directory
    worker = None
    try:
        with lock(Context(account, account), "capacity"):
            worker = _spawn_worker(service, project, tmp_path / "control.json", "queued")
            run_id = _wait_for(
                lambda: (
                    json.loads((tmp_path / "control.json").read_text())["run_id"]
                    if (tmp_path / "control.json").exists()
                    else None
                ),
                "the prepared run",
            )
            record = _wait_for(
                lambda: next(
                    (
                        data
                        for data in [_run_record(service, run_id)]
                        if data["state"] == "queued" and isinstance(data.get("guardian"), dict)
                    ),
                    None,
                ),
                "the queued run's guardian",
            )
            assert retirement._observe_actor(record["requester"]) == "executing"
            assert retirement._observe_actor(record["guardian"]) == "executing"
            _kill(record["guardian"]["pid"])
            worker.kill()
            worker.wait(timeout=30)
            reap_orphan(record["guardian"]["pid"])  # the guardian is a zombie of the killed worker, now ours
            _wait_for(lambda: _exited(record["guardian"]) and _exited(record["requester"]), "both owners to exit")
        shutil.rmtree(project)
        context = lifecycle.load_operator_context(_locator_json(_run_context(service, run_id)))
        _assert_recovered(service, context)
    finally:
        _reap_guardians(service)
        if worker is not None and worker.poll() is None:
            worker.kill()
            worker.wait(timeout=30)


def test_dead_owner_active_run_is_recovered_without_its_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    service = _activated(tmp_path, monkeypatch)
    project = tmp_path / "project"
    control = tmp_path / "control.json"
    worker = None
    try:
        worker = _spawn_worker(service, project, control, "active")
        _wait_for(lambda: control.with_suffix(".entered").exists(), "the run to enter")
        run_id = json.loads(control.read_text())["run_id"]
        record = _run_record(service, run_id)
        assert record["state"] == "active" and isinstance(record["guardian"], dict)
        _kill(record["guardian"]["pid"])
        worker.kill()
        worker.wait(timeout=30)
        reap_orphan(record["guardian"]["pid"])  # the guardian is a zombie of the killed worker, now ours
        _wait_for(lambda: _exited(record["guardian"]) and _exited(record["requester"]), "both owners to exit")
        shutil.rmtree(project)
        context = lifecycle.load_operator_context(_locator_json(_run_context(service, run_id)))
        _assert_recovered(service, context, cleanup="incomplete")
    finally:
        _reap_guardians(service)
        if worker is not None and worker.poll() is None:
            worker.kill()
            worker.wait(timeout=30)


def test_dry_run_locator_is_accepted_verbatim(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    service = _activated(tmp_path, monkeypatch)
    project = tmp_path / "project"
    run_id = _worker(service, project, tmp_path / "control.json", "prepared")
    shutil.rmtree(project)
    project_id = Context(project, service.state_directory).project_id

    blocked = _blocked(_retire(service, {"project_ids": [project_id]}, execute=False), project_id)
    (locator,) = blocked["locators"]
    assert locator["action"] == "run.recover" and locator["context"]["run_id"] == run_id
    context = lifecycle.load_operator_context(json.dumps(locator["context"]))
    _assert_recovered(service, context)

    again = _retire(service, {"project_ids": [project_id]}, execute=False)
    assert [item["evidence"]["project_id"] for item in again["eligible"]] == [project_id]
    result = _execute(service, [item["evidence"] for item in again["eligible"]])
    assert [entry["outcome"] for entry in result["results"]] == ["archived"]


@pytest.mark.parametrize("fault", ["foreign-root", "foreign-run"])
def test_wrong_locator_identity_is_refused_without_side_effects(
    fault: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = _activated(tmp_path, monkeypatch)
    first, second = _run(service, tmp_path / "first"), _run(service, tmp_path / "second")
    locator = json.loads(_locator_json(_run_context(service, first.run_id)))
    if fault == "foreign-root":
        locator["project_root"] = str(tmp_path / "second")
    else:
        locator["run_id"] = second.run_id
    foreign = tmp_path / "foreign"
    foreign.mkdir(mode=0o700)
    before = _tree(service.state_directory, foreign, archive_directory(service.state_directory))

    with pytest.raises(ResourceError):
        lifecycle.load_operator_context(json.dumps(locator))

    assert _tree(service.state_directory, foreign, archive_directory(service.state_directory)) == before


def test_live_requester_blocks_recovery_without_mutation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    service = _activated(tmp_path, monkeypatch)
    project = tmp_path / "project"
    handle = _run(service, project)  # the requester is this live process
    shutil.rmtree(project)
    context = _run_context(service, handle.run_id)
    before = _untouched(service)

    with pytest.raises(ResourceBusy):
        lifecycle.recover_run(context, _test_directory=service.account_directory)

    assert _untouched(service) == before


def test_failure_before_a_later_projects_rename_window_keeps_the_earlier_results(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = _activated(tmp_path, monkeypatch)
    _vanished_project(service, tmp_path / "one")
    _vanished_project(service, tmp_path / "two")
    evidence = _evidence(service)
    runs_directory = service.state_directory / "runs"

    undo: list = []

    def failpoint(name: str, _identity: str) -> None:
        if name == "after-project-rename":  # the first project is done; the listing of runs/ now fails
            undo.append(deny_access(runs_directory))

    try:
        result = _execute(service, evidence, failpoint=failpoint)
    finally:
        for restore_runs in undo:
            restore_runs()

    first, second = result["results"]
    assert first["outcome"] == "archived"
    assert (second["outcome"], second["category"], second["reasons"]) == (
        "refused",
        "infrastructure_failure",
        ["rename-failed"],
    )
    assert result["stopped"] is True and second["archived_runs"] == []


def test_dry_run_reports_shared_sessions_and_unknown_ids(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    service = _activated(tmp_path, monkeypatch)
    first, _ = _vanished_project(service, tmp_path / "one")
    second, _ = _vanished_project(service, tmp_path / "two")
    third, _ = _vanished_project(service, tmp_path / "three")
    shared_id = next(
        iter(json.loads(Context(tmp_path / "two", service.state_directory).record.read_text())["sessions"])
    )
    recovery.register(  # the same session id is also recorded by the first project
        Context(tmp_path / "one", service.state_directory),
        session_id=shared_id,
        temporary_parent=tmp_path / "temporary",
    )
    unknown = "f" * 64

    result = _retire(service, {"project_ids": [first, second, third, unknown]}, execute=False)

    reported = {item["evidence"]["project_id"]: item["shared_session_ids"] for item in result["eligible"]}
    assert reported == {first: [shared_id], second: [shared_id], third: []}
    assert result["not_found"] == [unknown] and result["blocked"] == []


def test_execute_archives_a_project_whose_registry_has_no_runs_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = _activated(tmp_path, monkeypatch)
    root = tmp_path / "never-ran"  # a registered project that never prepared a run, so there is no runs/ at all
    recovery.register(Context(root, service.state_directory), session_id=str(uuid.uuid4()), temporary_parent=tmp_path)
    runs = service.state_directory / "runs"
    if runs.exists():
        runs.rmdir()
    evidence = _evidence(service)
    assert [item["project_id"] for item in evidence] == [Context(root, service.state_directory).project_id]

    result = _execute(service, evidence)

    assert [entry["outcome"] for entry in result["results"]] == ["archived"]
    assert result["results"][0]["archived_runs"] == [] and result["results"][0]["archived_project"]


@pytest.mark.parametrize("record", ["run", "project"])
def test_progress_is_reported_when_a_directory_sync_fails_after_the_rename(
    record: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from docker_test_toolkit.application import retirement

    candidate, project, run_ids, _pinned = _two_run_project(tmp_path, monkeypatch)
    archive_project = (
        archive_directory(candidate.state_directory) / Context(project, candidate.state_directory).project_id
    )
    watched = archive_project / "runs" if record == "run" else archive_project
    real_sync = retirement._fsync_directory

    def failing_sync(path: Path) -> None:
        # Harness boundary: a failing fsync cannot be produced on a real filesystem, and there is no public seam for it.
        arrived = any(path.glob("*.json")) if path == watched and watched.exists() else False
        if arrived:
            raise OSError(errno.EIO, "directory sync failed")
        real_sync(path)

    monkeypatch.setattr(retirement, "_fsync_directory", failing_sync)

    result = _execute(candidate, _evidence(candidate))

    (entry,) = result["results"]
    assert (entry["outcome"], entry["reasons"], result["stopped"]) == ("refused", ["rename-failed"], True)
    if record == "run":
        assert entry["archived_runs"] == [run_ids[0]] and entry["archived_project"] is None
        assert run_ids[0] not in _run_files(candidate) and run_ids[1] in _run_files(candidate)
    else:  # both runs and then the project record had already been renamed when its directory sync failed
        assert entry["archived_runs"] == run_ids and (entry["archived_project"] or "").startswith("project-")
        assert not Context(project, candidate.state_directory).record.exists()


def test_a_run_that_vanishes_before_it_is_read_is_another_projects_not_an_unattributable_one(
    tmp_path: Path,
) -> None:
    present = tmp_path / "present.json"
    present.write_text(json.dumps({"project_id": "a" * 64}))
    present.chmod(0o600)
    unparseable = tmp_path / "unparseable.json"
    unparseable.write_text("not json")
    unparseable.chmod(0o600)

    assert retirement._read_owner(tmp_path / "gone.json") == (False, None)  # another command already moved it
    assert retirement._read_owner(present) == (True, "a" * 64)
    assert retirement._read_owner(unparseable) == (True, None)  # present but unattributable: blocks everything
    deep = tmp_path / "deep.json"
    deep.write_bytes(b"[" * 200_000 + b"]" * 200_000)  # far beyond the parser's recursion limit
    deep.chmod(0o600)
    assert retirement._read_owner(deep) == (True, None)  # hostile nesting is unattributable, never a crash


def test_retirement_reporting_ignores_unusable_archive_candidates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = _activated(tmp_path, monkeypatch)
    project_id, _ = _vanished_project(service, tmp_path / "p")
    archive = archive_directory(service.state_directory)
    runs = archive / project_id / "runs"
    runs.mkdir(parents=True)
    for directory in (archive, archive / project_id, runs):
        directory.chmod(0o700)
    nested = runs / f"{uuid.uuid4()}-{'0' * 64}.json"
    nested.write_bytes(b"[" * 200_000 + b"]" * 200_000)  # far beyond the parser's recursion limit, hash does not match
    nested.chmod(0o600)
    deep = b"[" * 200_000 + b"]" * 200_000  # a file named by its own hash passes the digest check and is still parsed
    self_hashed = runs / f"{uuid.uuid4()}-{hashlib.sha256(deep).hexdigest()}.json"
    self_hashed.write_bytes(deep)
    self_hashed.chmod(0o600)
    run_id = str(uuid.uuid4())
    body = json.dumps({"run_id": run_id, "session_id": ["not", "a", "string"]}).encode()
    unhashable = (
        runs / f"{run_id}-{hashlib.sha256(body).hexdigest()}.json"
    )  # a file can always be named by its own hash
    unhashable.write_bytes(body)
    unhashable.chmod(0o600)

    result = _retire(service, {"project_ids": [project_id]}, execute=False)

    assert [item["evidence"]["project_id"] for item in result["eligible"]] == [project_id]
    assert result["eligible"][0]["archived_runs"] == []


def test_a_run_that_vanishes_mid_execute_is_a_rename_failure_not_an_archived_project(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    candidate, project, run_ids, _pinned = _two_run_project(tmp_path, monkeypatch)
    moved: list[str] = []

    def failpoint(name: str, identity: str) -> None:
        if name == "after-run-rename" and not moved:
            moved.append(identity)
            (_run_path(candidate, next(run for run in run_ids if run != identity))).unlink()  # lost before its turn

    result = _execute(candidate, _evidence(candidate), failpoint=failpoint)

    entry = result["results"][0]
    assert (entry["outcome"], entry["reasons"], entry["archived_runs"]) == ("refused", ["rename-failed"], moved)
    assert entry["archived_project"] is None and result["stopped"] is True
    assert Context(project, candidate.state_directory).record.exists()  # the project record never moved


def test_an_archived_run_is_reported_once_however_many_verified_copies_exist(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = _activated(tmp_path, monkeypatch)
    project_id, _ = _vanished_project(service, tmp_path / "p")
    session_id = next(iter(json.loads(Context(tmp_path / "p", service.state_directory).record.read_text())["sessions"]))
    archive = archive_directory(service.state_directory)
    runs = archive / project_id / "runs"
    runs.mkdir(parents=True)
    for directory in (archive, archive / project_id, runs):
        directory.chmod(0o700)
    run_id = str(uuid.uuid4())
    for note in ("first", "second"):  # two different byte strings, each verified by its own digest
        body = json.dumps({"run_id": run_id, "session_id": session_id, "note": note}).encode()
        copy = runs / f"{run_id}-{hashlib.sha256(body).hexdigest()}.json"
        copy.write_bytes(body)
        copy.chmod(0o600)

    result = _retire(service, {"project_ids": [project_id]}, execute=False)

    assert result["eligible"][0]["archived_runs"] == [run_id]


def _queue_scope(account: Path) -> retirement._Scope:
    """A scope carrying only what ``_queue_reasons`` reads: the account directory."""
    return cast(retirement._Scope, SimpleNamespace(account=account))


def test_queue_reasons_reads_a_holder_queue_through_codec(tmp_path: Path) -> None:
    account = private_account_directory(tmp_path)
    first, second = queue_entry(1, 1, "granted"), queue_entry(2, 2)
    write_v2_queue(account, [first, second])

    named, unnamed = set(), set()
    retirement._queue_reasons(_queue_scope(account), {second[0]}, named)
    retirement._queue_reasons(_queue_scope(account), {"99999999-0000-4000-8000-000000000000"}, unnamed)

    assert named == {"queue-reference"} and unnamed == set()


def test_queue_reasons_treats_a_structurally_invalid_queue_as_unreadable(tmp_path: Path) -> None:
    account = private_account_directory(tmp_path)
    # Two holders are valid since shared admission (0.6.0); two entries sharing one ticket never are.
    entries = [queue_entry(1, 1, "granted"), queue_entry(2, 1)]
    write_v2_queue(account, entries)

    reasons: set[str] = set()
    retirement._queue_reasons(_queue_scope(account), {entries[0][0]}, reasons)

    assert reasons == {"queue-unreadable"}


def test_queue_reasons_reads_a_two_holder_queue(tmp_path: Path) -> None:
    account = private_account_directory(tmp_path)
    entries = [queue_entry(1, 1, "granted"), queue_entry(2, 2, "admitting")]
    write_v2_queue(account, entries)

    reasons: set[str] = set()
    retirement._queue_reasons(_queue_scope(account), {entries[1][0]}, reasons)

    assert reasons == {"queue-reference"}


def _project_cache(service: _Account, project_id: str) -> Path:
    """Give one project a published build-cache generation and return its platform directory."""
    from tests.unit.test_docker_receipt_service import write_cache_layout

    project_cache = build_cache.project_cache(service.state_directory, project_id)
    platform_dir = project_cache / "linux-arm64"
    for level in (build_cache.cache_root(service.state_directory), project_cache, platform_dir):
        build_cache.require_safe_directory(level)
    with build_cache.platform_anchor(project_cache, "linux-arm64", shared=True):
        pass
    generation = (
        platform_dir
        / "generations"
        / ("c" * 64)
        / "1-11111111-1111-1111-1111-111111111111-22222222-2222-2222-2222-222222222222"
    )
    for level in (platform_dir / "generations", generation.parent):
        build_cache.require_safe_directory(level)
    write_cache_layout(generation, {"a": b"layer"})
    return platform_dir


def test_retire_removes_the_project_cache_after_archiving(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    service = _activated(tmp_path, monkeypatch)
    project_id, _run_ids = _vanished_project(service, tmp_path / "project")
    platform_dir = _project_cache(service, project_id)
    size = sum(path.lstat().st_size for path in platform_dir.rglob("*") if path.is_file())

    result = _execute(service, _evidence(service))

    (entry,) = result["results"]
    assert entry["outcome"] == "archived"
    assert entry["build_cache"] == {"linux-arm64": size}
    assert not platform_dir.exists() and (platform_dir.parent / "linux-arm64.lock").exists()


def test_retire_dry_run_reports_the_cache_and_creates_nothing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    service = _activated(tmp_path, monkeypatch)
    project_id, _run_ids = _vanished_project(service, tmp_path / "vanished")
    platform_dir = _project_cache(service, project_id)
    size = sum(path.lstat().st_size for path in platform_dir.rglob("*") if path.is_file())
    blocked = _run(service, tmp_path / "present")
    assert blocked.cancel().state == "complete"
    blocked_id = Context(tmp_path / "present", service.state_directory).project_id
    before = sorted(build_cache.cache_root(service.state_directory).rglob("*"))

    result = _retire(service, {"project_ids": [project_id, blocked_id]}, execute=False)

    (eligible,) = result["eligible"]
    assert eligible["build_cache"] == {"linux-arm64": size}
    (entry,) = [item for item in result["blocked"] if item["project_id"] == blocked_id]
    assert entry["build_cache"] is None
    assert sorted(build_cache.cache_root(service.state_directory).rglob("*")) == before


def test_a_cache_failure_is_never_a_retire_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    service = _activated(tmp_path, monkeypatch)
    project_id, _run_ids = _vanished_project(service, tmp_path / "project")
    platform_dir = _project_cache(service, project_id)
    (platform_dir.parent / "linux-arm64.lock").chmod(0o644)  # the anchor is no longer private: the lock refuses

    listed = _retire(service, {}, execute=False)
    result = _execute(service, _evidence(service))

    assert listed["eligible"][0]["build_cache"] == "error"
    (entry,) = result["results"]
    assert entry["outcome"] == "archived" and entry["build_cache"] == "error"


def test_an_archived_record_gets_a_fresh_mtime(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # A rename keeps a record's own mtime; retention's 90-day age must count from archiving.
    candidate, _project, _run_ids, _first = _two_run_project(tmp_path, monkeypatch)
    old = time.time() - 100 * 86400
    for path in (candidate.state_directory / "runs").glob("*.json"):
        os.utime(path, (old, old))
    before = time.time()

    _execute(candidate, _evidence(candidate))

    files = _archive_files(candidate)
    assert files and all(path.stat().st_mtime >= before - 1 for path in files)
