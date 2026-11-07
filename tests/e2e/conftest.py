from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
import uuid
import warnings
from collections.abc import Callable, Iterator
from functools import partial
from pathlib import Path

import pytest
from tests.installation_fixtures import build_archive, temporary_installation

from docker_test_toolkit.config.installation import ARCHIVE_NAME, installation_of

_REDIS = "redis@sha256:becdda6c7f4b3fb42e42fd7f120bbf5c54c4caaaf16f26da24e4563d2c1f0576"
_REDIS_ID = "sha256:becdda6c7f4b3fb42e42fd7f120bbf5c54c4caaaf16f26da24e4563d2c1f0576"  # the pinned content digest
_RESOLVED: list[str] = []


def redis_id() -> str:
    """Return the daemon's image ID of the pinned Redis image, resolved once per session (``redis_image`` pulls it).

    Docker's image store reports the pinned index digest; Podman reports the config digest of the same content, which
    its repository digests prove.
    """
    if not _RESOLVED:
        docker = shutil.which("docker")
        assert docker is not None
        inspected = subprocess.run(
            [docker, "image", "inspect", _REDIS, "--format", "{{.Id}} {{json .RepoDigests}}"],
            check=True,
            capture_output=True,
            text=True,
        )
        image_id, digests = inspected.stdout.strip().split(" ", 1)
        assert image_id == _REDIS_ID or any(d.endswith(_REDIS_ID) for d in json.loads(digests)), inspected.stdout
        _RESOLVED.append(image_id)
    return _RESOLVED[0]


@pytest.fixture
def toolkit_domain() -> str:
    """Toolkit E2E runs use private test state; the real account belongs to the machine's installation."""
    return "private"


@pytest.fixture(scope="session")
def redis_image() -> Iterator[None]:
    """Provision the exact external E2E image and remove only a fixture-added copy."""
    docker = shutil.which("docker")
    assert docker is not None
    before = subprocess.run(
        [docker, "image", "inspect", _REDIS],
        check=False,
        capture_output=True,
        text=True,
    )
    provisioned = before.returncode != 0
    if provisioned:
        subprocess.run([docker, "pull", _REDIS], check=True, capture_output=True, text=True)
    redis_id()
    yield None
    if provisioned:
        subprocess.run([docker, "image", "rm", _REDIS], check=True, capture_output=True, text=True)


_ROOT = Path(__file__).parents[2]
_SWEEP_SECONDS = 120.0
_RETRY = frozenset({"contention", "incompatible_state"})
_KEEP_ROOT = frozenset({"actor-live", "actor-unverifiable"})


def _base_python() -> str:
    return str(Path(getattr(sys, "_base_executable", sys.executable)).resolve())


def _project_record_names(registry: Path) -> list[str]:
    """List project record file names; a missing projects/ is empty, any other OSError propagates."""
    try:
        with os.scandir(registry / "projects") as listing:
            return sorted(entry.name for entry in listing if entry.name.endswith(".json"))
    except FileNotFoundError:
        return []


def _rooted_in(root: object, base: Path) -> bool:
    """Compare a recorded root with base component-wise, never resolving the recorded string."""
    return isinstance(root, str) and Path(root).is_relative_to(base)


def _inside_physically(root: str, base: Path) -> bool:
    """Return whether the root really lies strictly inside base once links are followed.

    Selection never resolves a recorded root; every destructive step does, so a link below base cannot redirect it
    and the base itself is never removed.
    """
    real_root, real_base = Path(os.path.realpath(root)), Path(os.path.realpath(base))
    return real_root != real_base and real_root.is_relative_to(real_base)


def _session_candidates(base: Path, registry: Path) -> dict[str, str]:
    """Return registry projects whose recorded root lies inside base, compared component-wise."""
    selected: dict[str, str] = {}
    for name in _project_record_names(registry):
        try:
            record = json.loads((registry / "projects" / name).read_bytes())
        except FileNotFoundError:
            continue  # retired concurrently by another session
        root = record.get("project_root") if isinstance(record, dict) else None
        if not isinstance(root, str):  # a record that names no root cannot be ruled out as ours: fail closed
            raise ValueError(f"Project record {name} has no recorded root.")
        if _rooted_in(root, base):
            selected[name.removesuffix(".json")] = root
    return selected


def _toolkit(arguments: list[str], payload: dict, workspace: Path, *, archive: Path, execute: bool = False) -> dict:
    """Run one operator command of the session's installed archive and return the parsed envelope."""
    workspace.mkdir(mode=0o700, exist_ok=True)
    request = workspace / f"request-{uuid.uuid4().hex}.json"
    descriptor = os.open(request, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode() + b"\n")
    command = [
        _base_python(),
        "-I",
        str(archive),
        *arguments,
        "--input",
        str(request),
        *(["--execute"] if execute else []),
        "--json",
    ]
    completed = subprocess.run(
        command,
        # The sweep workspace, not a project root: roots are removed during the end sweep while later calls still run.
        cwd=workspace,
        env={
            "PATH": os.environ.get("PATH", ""),
            "TMPDIR": os.environ.get("TMPDIR", "/tmp"),
        },
        check=False,
        capture_output=True,
        text=True,
        timeout=180,
    )
    result = json.loads(completed.stdout)
    result["_exit"] = completed.returncode
    return result


def _retrying(call: Callable[[], dict], deadline: float) -> dict:
    """Repeat a CLI call while it reports a retryable category, at one-second spacing, until the deadline."""
    while True:
        response = call()
        category = None if response.get("ok") else response.get("error", {}).get("category")
        if category not in _RETRY or time.monotonic() >= deadline:
            return response
        time.sleep(1)


def _recover_locators(base: Path, blocked: dict[str, dict], call: Callable[..., dict]) -> None:
    """Recover or release every run a dry-run named by locator; a release is only meaningful while the root exists.

    Locators of a project whose root is not physically inside the session base are left alone, like its cleanup.
    """
    for item in blocked.values():
        if not item["project_root"] or not _inside_physically(item["project_root"], base):
            continue
        for locator in item["locators"]:
            if locator["action"] == "run.release" and not os.path.lexists(item["project_root"] or ""):
                continue
            call(locator["action"].split("."), {"context": locator["context"]})


def _remove_clean_roots(
    base: Path, candidates: dict[str, str], blocked: dict[str, dict], call: Callable[..., dict]
) -> None:
    """Clean each candidate project; remove its root only after a clean `cleanup project --execute` (exit 0)."""
    for pid in sorted(candidates):
        root = candidates[pid]
        reasons = set(blocked[pid]["reasons"]) if pid in blocked else set()
        if reasons & _KEEP_ROOT or not os.path.lexists(root) or not _inside_physically(root, base):
            continue  # an unsafe root stays a visible leftover; cleanup resolves the root, so it must not run on it
        payload = {"project_root": root}
        checked = call(["cleanup", "project"], payload)
        executed = call(["cleanup", "project"], payload, execute=True)
        clean = checked.get("ok") and executed.get("_exit") == 0 and executed.get("result", {}).get("clean") is True
        if clean and _inside_physically(root, base):  # checked again: the tree may have been rebound while cleanup ran
            shutil.rmtree(root)


def _execute_evidence(evidence: list[dict], candidates: dict[str, str], call: Callable[..., dict]) -> list[dict]:
    """Execute reviewed evidence once; guard (ii) lets only this session's projects in and out as archived."""
    assert {item["project_id"] for item in evidence} <= set(candidates)
    done = call(["project", "retire"], {"evidence": evidence}, execute=True, retry=False)
    results = done.get("result", {}).get("results", []) if done.get("ok") else []
    assert {item["project_id"] for item in results if item["outcome"] == "archived"} <= set(candidates)
    return results


def _retire_remaining(
    base: Path,
    registry: Path,
    candidates: dict[str, str],
    blocked: dict[str, dict],
    call: Callable[..., dict],
    deadline: float,
) -> tuple[dict[str, dict], dict[str, dict]]:
    """Retire what is eligible, repeating on contention; return the latest blocked and refused reports."""
    refused: dict[str, dict] = {}
    while remaining := sorted(_session_candidates(base, registry)):
        dry = call(["project", "retire"], {"project_ids": remaining})
        if dry.get("ok"):  # the report names what blocks a project now, not what blocked it before cleanup
            blocked = {item["project_id"]: item for item in dry["result"]["blocked"]}
        evidence = [item["evidence"] for item in dry.get("result", {}).get("eligible", [])] if dry.get("ok") else []
        if not evidence:
            break
        results = _execute_evidence(evidence, candidates, call)
        refused.update({item["project_id"]: item for item in results if item["outcome"] == "refused"})
        contended = any(item["outcome"] == "refused" and item["category"] == "contention" for item in results)
        if not contended or time.monotonic() >= deadline:
            break
        time.sleep(1)
    return blocked, refused


def _sweep(
    base: Path,
    phase: str,
    deadline: float,
    *,
    registry: Path,
    toolkit: Callable[..., dict],
) -> list[str]:
    """Recover, clean and retire every registry project rooted in `base`; return the leftovers, one line each.

    Only projects selected by component-wise containment are ever submitted (guard ii). At the "end" phase the
    project roots are removed too, but only after `cleanup project --execute` exited 0 with `clean: true`.
    """
    candidates = _session_candidates(base, registry)
    if not candidates:
        return []
    workspace = base / ".account-sweep"

    def call(arguments: list[str], payload: dict, *, execute: bool = False, retry: bool = True) -> dict:
        run = partial(toolkit, arguments, payload, workspace, execute=execute)
        return _retrying(run, deadline) if retry else run()

    dry = call(["project", "retire"], {"project_ids": sorted(candidates)})
    if not dry.get("ok"):
        message = dry.get("error", {}).get("message", "dry-run failed")
        return [f"{pid} {candidates[pid]} [{message}] []" for pid in sorted(candidates)]
    blocked = {item["project_id"]: item for item in dry["result"]["blocked"]}
    _recover_locators(base, blocked, call)
    if phase == "end":
        _remove_clean_roots(base, candidates, blocked, call)
    blocked, refused = _retire_remaining(base, registry, candidates, blocked, call, deadline)
    leftovers = []
    for pid in sorted(_session_candidates(base, registry)):
        item = blocked.get(pid) or refused.get(pid, {})
        leftovers.append(f"{pid} {candidates.get(pid)} {item.get('reasons', [])} {item.get('locators', [])}")
    return leftovers


def _start_session(base: Path, deadline: float, *, registry: Path, toolkit: Callable[..., dict]) -> set[str]:
    """Retire leftovers of an earlier session, warn about the ones that remain, then take the foreign baseline."""
    for line in _sweep(base, "start", deadline, registry=registry, toolkit=toolkit):
        warnings.warn(f"Earlier session project remains in the registry: {line}", stacklevel=2)
    return {name.removesuffix(".json") for name in _project_record_names(registry)}


def _end_session(
    base: Path, baseline: set[str], deadline: float, *, registry: Path, toolkit: Callable[..., dict]
) -> tuple[list[str], set[str]]:
    """Sweep this session's projects; return the leftovers and the changes made by anyone else."""
    end_candidates = set(_session_candidates(base, registry))
    leftovers = _sweep(base, "end", deadline, registry=registry, toolkit=toolkit)
    current = {name.removesuffix(".json") for name in _project_record_names(registry)}
    return leftovers, (current ^ baseline) - end_candidates


def _docker(*arguments: str) -> subprocess.CompletedProcess:
    docker = shutil.which("docker")
    assert docker is not None
    return subprocess.run([docker, *arguments], capture_output=True, text=True, check=False, timeout=120)


def remove_run_leftovers(registry: Path) -> list[str]:
    """Remove what Docker still holds for the runs a private installation recorded; return what was removed.

    Ownership comes from the installation's own run records, never from a resource name alone: each record names its
    project and run, from which the toolkit derives the run's exact builder and Compose project. A non-empty result
    means the run's own leave and the installation's cleanup missed it.
    """
    from docker_test_toolkit.application.lifecycle import builder_name, compose_project_name

    removed = []
    for path in sorted((registry / "runs").glob("*.json")):
        try:
            record = json.loads(path.read_text())
            project_id, run_id = record["project_id"], record["run_id"]
        except (OSError, ValueError, KeyError, TypeError):
            continue
        builder = builder_name(project_id, run_id)
        if _docker("buildx", "inspect", builder).returncode == 0:
            _docker("buildx", "rm", "--force", builder)
            removed.append(f"builder {builder}")
        worker = f"buildx_buildkit_{builder}0"
        if _docker("container", "inspect", worker).returncode == 0:  # a worker whose instance entry is gone
            _docker("container", "rm", "--force", "--volumes", worker)
            removed.append(f"container {worker}")
        if _docker("volume", "inspect", f"{worker}_state").returncode == 0:
            _docker("volume", "rm", "--force", f"{worker}_state")
            removed.append(f"volume {worker}_state")
        label = f"label=com.docker.compose.project={compose_project_name(project_id, run_id)}"
        for kind in ("container", "network", "volume"):
            listed = _docker(kind, "ls", *(["--all"] if kind == "container" else []), "--quiet", "--filter", label)
            for identifier in listed.stdout.split():
                _docker(kind, "rm", "--force", identifier)
                removed.append(f"{kind} {identifier}")
    return removed


def sweep_private_installation(root: Path, base: Path) -> list[str]:
    """Recover, clean and retire every project of one private installation, then remove what Docker still holds for
    its recorded runs; return one line per leftover (an empty list is clean)."""
    current = root / "current"
    if not current.exists():
        return []
    registry = root / "state"
    leftovers = _sweep(
        base,
        "end",
        time.monotonic() + _SWEEP_SECONDS,
        registry=registry,
        toolkit=partial(_toolkit, archive=current.resolve() / ARCHIVE_NAME),
    )
    return leftovers + [f"docker {line}" for line in remove_run_leftovers(registry)]


def pytest_sessionstart(session: pytest.Session) -> None:
    """Before pytest wipes a fixed ``--basetemp``, clean every private installation the previous session left there.

    A test that died or was killed before its teardown leaves its installation's records only in that directory; once
    it is wiped, nothing can prove what those runs still hold in Docker.
    """
    basetemp = session.config.option.basetemp
    if not basetemp or not Path(basetemp).is_dir():
        return
    for archive in sorted(Path(basetemp).glob("*/*/current/" + ARCHIVE_NAME)):
        root = archive.parent.parent
        lines = sweep_private_installation(root, Path(basetemp).resolve())
        for line in lines:
            warnings.warn(f"Previous session left in {root}: {line}", stacklevel=1)


@pytest.fixture(scope="session")
def session_installation(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Install the toolkit under test once per session into a temporary root; return its resolved archive.

    ``DOCKER_TEST_TOOLKIT_E2E_ARTIFACT`` names a release-built archive to install (the release workflow); without it the
    working tree is built exactly as a release does. No test touches the machine's own installation.
    """
    base = tmp_path_factory.mktemp("installation")
    supplied = os.environ.get("DOCKER_TEST_TOOLKIT_E2E_ARTIFACT")
    archive = Path(supplied).resolve() if supplied else build_archive(base / "build")
    root = temporary_installation(base, [archive])
    return (root / "current").resolve() / ARCHIVE_NAME


def _session_registry(archive: Path) -> Path:
    installation = installation_of(archive)
    assert installation is not None
    return installation.state_directory


@pytest.fixture(scope="session", autouse=True)
def _account_session_sweep(tmp_path_factory: pytest.TempPathFactory, session_installation: Path) -> Iterator[None]:
    """Leave no project rooted in this session's base directory in the session installation's registry."""
    base = tmp_path_factory.getbasetemp().resolve()
    registry = _session_registry(session_installation)
    toolkit = partial(_toolkit, archive=session_installation)
    baseline = _start_session(base, time.monotonic() + _SWEEP_SECONDS, registry=registry, toolkit=toolkit)
    yield None
    leftovers, foreign = _end_session(
        base, baseline, time.monotonic() + _SWEEP_SECONDS, registry=registry, toolkit=toolkit
    )
    if foreign:
        warnings.warn(f"Other sessions changed the registry meanwhile: {sorted(foreign)}", stacklevel=1)
    assert not leftovers, "Session projects remain in the shared registry:\n" + "\n".join(leftovers)


@pytest.fixture
def private_installation_sweep() -> Callable[[Path, Path], list[str]]:
    """Expose the private-installation sweep (records first, then what Docker still holds) to a test's teardown."""
    return sweep_private_installation


@pytest.fixture
def account_sweep(session_installation: Path) -> Callable[..., list[str]]:
    """Expose the sweep, bound to the session installation, so a test can retire its own abandoned projects."""
    return partial(
        _sweep,
        registry=_session_registry(session_installation),
        toolkit=partial(_toolkit, archive=session_installation),
    )
