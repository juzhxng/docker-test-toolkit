"""Shared runs on a real daemon through the production composition of a private installation.

Every cell installs the session archive into its own temporary root with that archive's own installer, at the default
limit (``auto``: no count cap, fit decides), and runs workers that load the toolkit through the reference loader and
call ``toolkit.open_run``: no private test domain, so admission reads the real daemon and host and the guardian observes
active runs every 5 s.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
from tests.e2e.conftest import redis_id
from tests.installation_fixtures import base_python, temporary_installation

from docker_test_toolkit.application import probe, sharing
from docker_test_toolkit.application.recovery import atomic_json, project_id_for
from docker_test_toolkit.config.installation import ARCHIVE_NAME, installation_of

pytestmark = pytest.mark.e2e
ROOT = Path(__file__).parents[2]
LOADER = ROOT / "docs" / "loader"
REDIS = "redis@sha256:becdda6c7f4b3fb42e42fd7f120bbf5c54c4caaaf16f26da24e4563d2c1f0576"
GIB = 1024**3
ENTRYPOINT, PROFILE, CLOSURE = "e2e", "shared-runs", ("cache",)

WORKER = r"""from __future__ import annotations
import json
import pathlib
import subprocess
import sys
import time

project, loader_directory, installation_root, control = map(pathlib.Path, sys.argv[1:5])
name, action = sys.argv[5:7]
container_bytes, host_bytes, service_bytes = (int(value) for value in sys.argv[7:10])
sys.path.insert(0, str(loader_directory))
import docker_test_toolkit_loader as loader

toolkit, _archive = loader.load_toolkit((installation_root,))
options = json.loads(sys.argv[12]) if len(sys.argv) > 12 else {}
phases = {
    phase: toolkit.PhaseBudget(*values)
    for phase, values in options.get("phases", {"tests": [container_bytes, host_bytes, 1, 1]}).items()
}
spec = toolkit.RunSpec(
    project_root=project,
    entrypoint="e2e",
    purpose=f"shared-{name}",
    services=("cache",),
    closure=("cache",),
    profile="shared-runs",
    phases=phases,
    initial_phase=options.get("initial", "tests"),
    artifact=toolkit.toolkit_identity(),
    external_resources=(
        toolkit.ExternalDockerResource("image", options.get("use_image", sys.argv[10]), "image", "use_image"),
    ),
    wait_seconds=600,
)
handle = toolkit.open_run(spec)
prepared = {"run_id": handle.run_id, "record": str(handle._context.record)}
(control / f"prepared-{name}.json").write_text(json.dumps(prepared))
entered = False
try:
    handle.enter()
    entered = True
    if service_bytes:
        compose = handle.private_root / "compose.yml"
        compose.write_text(
            "services:\n"
            "  cache:\n"
            f"    image: {sys.argv[11]}\n"
            f"    mem_limit: {service_bytes}\n"
            "    cpus: 1\n"
            "    command: [redis-server, --save, '', --appendonly, 'no']\n"
        )
        context = handle.docker_context
        subprocess.run(
            [str(context.executable), "compose", "--file", str(compose), "up", "--detach"],
            cwd=project, env=dict(context.environment), check=True, capture_output=True,
        )
    results = []
    for step in options.get("steps", []):  # each step: write files, run one docker command, or transition
        context = handle.docker_context
        def expand(value):
            markers = (("{private}", handle.private_root), ("{image}", sys.argv[11]), ("{builder}", context.builder))
            for marker, actual in markers:
                value = value.replace(marker, str(actual))
            if "{built}" in value:  # the image this run built, from its own record
                owned = json.loads(handle._context.record.read_text())["owned_resources"]["images"]
                value = value.replace("{built}", next(key for key, item in owned.items() if item["state"] != "removed"))
            return value
        if "await" in step:  # signal readiness, then block until the test says go (it may hold the image lock)
            (control / f"ready-{name}").write_text("")
            go = control / f"{step['await']}-{name}"
            deadline = time.monotonic() + 120
            while not go.exists() and time.monotonic() < deadline:
                time.sleep(0.1)
        for relative, text in step.get("write", {}).items():
            (project / relative).write_text(expand(text))
        if "docker" in step:
            done = subprocess.run(
                [str(context.executable), *map(expand, step["docker"])],
                cwd=project, env=dict(context.environment), capture_output=True, text=True,
            )
            results.append({"rc": done.returncode, "stderr": done.stderr[-2000:]})
        if "transition" in step:
            try:
                results.append({"rc": 0, "state": handle.transition(step["transition"]).state})
            except Exception as error:
                results.append({"rc": 1, "stderr": f"{type(error).__name__}: {error}"})
    (control / f"steps-{name}.json").write_text(json.dumps(results))
    ballast = None
    if action == "breach":
        ballast = bytearray(host_bytes * 4)  # four times the declared host-process envelope, every page touched
        for offset in range(0, len(ballast), 4096):
            ballast[offset] = 1
    (control / f"entered-{name}.json").write_text("{}")
    deadline = time.monotonic() + 300
    while not (control / f"release-{name}").exists() and time.monotonic() < deadline:
        if action == "breach" and handle.status().state != "active":
            break
        time.sleep(0.2)
    status = handle.leave("passed" if action != "breach" else "failed")
    entered = False
    (control / f"done-{name}.json").write_text(json.dumps({"state": status.state, "cleanup": status.cleanup_outcome}))
except BaseException as error:
    (control / f"error-{name}.json").write_text(json.dumps({"type": type(error).__name__, "message": str(error)}))
    raise
finally:
    if entered:
        try:
            handle.leave("failed")
        except Exception:
            pass
"""


@pytest.fixture
def private_installation(
    tmp_path: Path, session_installation: Path, private_installation_sweep: Callable[[Path, Path], list[str]]
) -> Iterator[Path]:
    """Install the session archive into this test's own root at the default limit, ``auto``; return the root."""
    root = temporary_installation(tmp_path / "installation", [session_installation])
    archive = (root / "current").resolve() / ARCHIVE_NAME
    limit = subprocess.run(
        [str(base_python()), "-I", str(archive), "install", "--json", "--root", str(root)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert limit.returncode == 0, limit.stdout + limit.stderr
    assert json.loads(limit.stdout)["result"]["parallel_runs"] == "auto"
    yield root
    swept = private_installation_sweep(root, tmp_path.resolve())
    assert not swept, "the private installation left work behind:\n" + "\n".join(swept)
    leftover = [path.name for path in (installation_of(archive).state_directory / "runs").glob("*.json")]
    records = [
        json.loads(path.read_text()) for path in (installation_of(archive).state_directory / "runs").glob("*.json")
    ]
    assert all(record["state"] in ("complete", "failed") for record in records), leftover


def _stage(tmp_path: Path, name: str) -> Path:
    project = tmp_path / name
    (project / ".git").mkdir(parents=True)  # share verdicts are keyed by the Git origin
    (project / ".git" / "config").write_text(f'[remote "origin"]\n\turl = https://example.invalid/{name}.git\n')
    (project / "worker.py").write_text(WORKER, encoding="utf-8")
    return project


def _key(project: Path) -> str:
    origin = sharing.repository_origin(str(project.resolve()))
    assert origin is not None, "staged projects carry a Git origin"
    return sharing.request_key(origin, ENTRYPOINT, PROFILE, CLOSURE)


def _registry(root: Path) -> Path:
    return installation_of((root / "current").resolve() / ARCHIVE_NAME).state_directory


def _verdict(root: Path, project: Path) -> dict | None:
    """Return the recorded verdict of the cells' one request key for ``project``, or None."""
    project_id = project_id_for(project.resolve())
    path = sharing.verdict_path(_registry(root), project_id)
    if not path.exists():
        return None
    return json.loads(path.read_text())["verdicts"].get(_key(project))


def seed_verdict(root: Path, project: Path) -> None:
    """Seed the positive verdict an earlier exclusive run of this request would have taught.

    Cells about admission seed it to stay short; ``test_warm_up_exclusive_run_teaches_positive_then_next_run_shared``
    proves the leave acknowledgement writes it.
    """
    registry = _registry(root)
    project_id = project_id_for(project.resolve())
    key = _key(project)
    path = sharing.verdict_path(registry, project_id)
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    verdict = {"shareable": True, "reason": None, "run_id": "seed", "origin": "seed", "tick_ns": 0}
    atomic_json(path, {"rules": 1, "schema": 1, "verdicts": {key: verdict}})


def _worker(
    root: Path,
    project: Path,
    control: Path,
    name: str,
    action: str,
    *,
    container: int,
    host: int,
    service: int,
    options: dict | None = None,
):
    temporary = project / "temporary"
    temporary.mkdir(exist_ok=True)
    return subprocess.Popen(
        [
            str(base_python()),
            "-I",
            str(project / "worker.py"),
            str(project),
            str(LOADER),
            str(root),
            str(control),
            name,
            action,
            str(container),
            str(host),
            str(service),
            redis_id(),  # the daemon's ID of the pinned image (Podman: its config digest)
            REDIS,
            json.dumps(options or {}),
        ],
        cwd=project,
        env={"PATH": os.environ.get("PATH", ""), "TMPDIR": str(temporary)},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _wait(path: Path, timeout: float = 300) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return json.loads(path.read_text() or "{}")
        time.sleep(0.1)
    raise AssertionError(f"timed out waiting for {path}")


def _finish(process: subprocess.Popen[str], timeout: float = 180) -> None:
    stdout, stderr = process.communicate(timeout=timeout)
    assert process.returncode == 0, f"stdout={stdout}\nstderr={stderr}"


def _record(control: Path, name: str) -> dict:
    return json.loads(Path(_wait(control / f"prepared-{name}.json")["record"]).read_text())


def granted_interval(record: dict) -> tuple[int, int | None]:
    timeline = record["hold_timeline"]
    return timeline["granted_ns"], timeline["released_ns"]


def observed_after(record: dict, tick: int) -> bool:
    samples = record["admission_observation"]["samples"]
    return bool(samples) and samples[-1]["tick_ns"] > tick


def _waited(record: dict, reason: str) -> bool:
    return any(item["reason"] == reason for item in (record.get("hold_timeline") or {}).get("waiting", []))


def _until(condition, timeout: float = 120, message: str = "condition") -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(0.2)
    raise AssertionError(f"timed out after {timeout} s waiting for {message}")


def _daemon_memory() -> int:
    docker = shutil.which("docker")
    assert docker is not None
    return int(
        subprocess.run([docker, "info", "--format", "{{.MemTotal}}"], capture_output=True, text=True, check=True).stdout
    )


def _pair_budget(root: Path, margin: int) -> int:
    """Each run's container budget so that two runs fit with ``margin`` to spare on whichever capacity inequality
    binds: the guest (Docker memory) or, on a native daemon where Docker memory is host memory, the host side, which
    also carries the account's host reserve and both runs' 256 MiB host processes (assess_capacity)."""
    inspected = subprocess.run(
        [str(base_python()), "-I", str((root / "current").resolve() / ARCHIVE_NAME), "readiness", "inspect", "--json"],
        capture_output=True,
        text=True,
        check=True,
    )
    facts = next(c["facts"] for c in json.loads(inspected.stdout)["result"]["checks"] if c["check"] == "capacity")
    running = _running_limits()
    guest = (facts["daemon_memory_bytes"] - running - GIB // 2 - margin) // 2
    # Desktop adds 0.25 GiB on the host side too, but there the guest side binds (Docker memory is a VM share).
    host_fixed = facts["host_reserve_bytes"] + GIB // 2 + 2 * 256 * 1024**2
    host = (facts["host_memory_bytes"] - running - host_fixed - margin) // 2
    return min(guest, host)


def _running_limits() -> int:
    """Sum the memory limits of the containers already running before a cell starts (another project's load on a
    shared daemon); a capacity cell budgets beside them. Load that starts during the cell is outside its premise."""
    docker = shutil.which("docker")
    assert docker is not None
    ids = subprocess.run([docker, "ps", "-q"], capture_output=True, text=True, check=True).stdout.split()
    if not ids:
        return 0
    limits = subprocess.run(
        [docker, "inspect", "--format", "{{.HostConfig.Memory}}", *ids], capture_output=True, text=True, check=True
    ).stdout.split()
    assert all(int(limit) > 0 for limit in limits), "an unlimited container is running: capacity cells cannot budget"
    return sum(int(limit) for limit in limits)


def test_two_warmed_runs_overlap(tmp_path: Path, redis_image: None, private_installation: Path) -> None:
    del redis_image
    root, control = private_installation, tmp_path / "control"
    control.mkdir()
    first, second = _stage(tmp_path, "overlap-a"), _stage(tmp_path, "overlap-b")
    seed_verdict(root, first)
    seed_verdict(root, second)
    workers = [
        _worker(root, project, control, name, "hold", container=GIB, host=256 * 1024**2, service=256 * 1024**2)
        for project, name in ((first, "a"), (second, "b"))
    ]
    try:
        _wait(control / "entered-a.json")
        _wait(control / "entered-b.json")
        a, b = _record(control, "a"), _record(control, "b")
        both = max(granted_interval(a)[0], granted_interval(b)[0])
        _until(
            lambda: observed_after(_record(control, "a"), both) and observed_after(_record(control, "b"), both),
            message="an observation of both runs after both grants",
        )
        assert a["sharing"] == b["sharing"] == {"mode": "shared"}
        (control / "release-a").touch()
        (control / "release-b").touch()
        for worker in workers:
            _finish(worker)
    finally:
        for worker in workers:
            if worker.poll() is None:
                worker.kill()
    a, b = _record(control, "a"), _record(control, "b")
    (granted_a, released_a), (granted_b, released_b) = granted_interval(a), granted_interval(b)
    assert a["hold_timeline"]["origin"] == b["hold_timeline"]["origin"]
    assert max(granted_a, granted_b) < min(released_a, released_b)  # the granted intervals intersect


def test_a_join_reads_the_daemons_vm_through_the_probe_and_leaves_nothing(
    tmp_path: Path, redis_image: None, private_installation: Path
) -> None:
    """on a Mac a join is decided on the Docker VM's CPU pressure, read by the probe; the
    reading reaches the account for readiness; no probe record or container remains. A Linux host keeps its own PSI."""
    del redis_image
    root, control = private_installation, tmp_path / "control"
    control.mkdir()
    first, second = _stage(tmp_path, "probe-a"), _stage(tmp_path, "probe-b")
    seed_verdict(root, first)
    seed_verdict(root, second)
    workers = [
        _worker(root, project, control, name, "hold", container=GIB // 2, host=128 * 1024**2, service=0)
        for project, name in ((first, "a"), (second, "b"))
    ]
    try:
        _wait(control / "entered-a.json")
        _wait(control / "entered-b.json")
        (control / "release-a").touch()
        (control / "release-b").touch()
        for worker in workers:
            _finish(worker)
    finally:
        for worker in workers:
            if worker.poll() is None:
                worker.kill()
    account = _account(root)
    readings = probe.read_probe_readings(account)
    if sys.platform == "darwin":
        assert len(readings) == 1, readings
        reading = next(iter(readings.values()))
        assert isinstance(reading["cpu_pressure"], float), reading
        assert reading["data_root_total_bytes"] > reading["data_root_free_bytes"] > 0, reading
    else:
        # The host's own PSI decided the join; the disk gate still probes this machine's daemon for its data root (a CI
        # runner reaches it over the loopback, and its data root is not on the runner's own disk)
        assert len(readings) <= 1, readings
        for reading in readings.values():
            assert reading["data_root_total_bytes"] > reading["data_root_free_bytes"] > 0, reading
    records = account / probe.PROBES_DIRECTORY
    assert not records.exists() or not any(records.iterdir())
    probes = ["docker", "ps", "-a", "-q", "--filter", f"label={probe.PROBE_LABEL}"]
    listed = subprocess.run(probes, capture_output=True, text=True, check=True)
    assert listed.stdout.split() == []


def test_non_fitting_pair_serialized(tmp_path: Path, redis_image: None, private_installation: Path) -> None:
    del redis_image
    root, control = private_installation, tmp_path / "control"
    control.mkdir()
    half = (_daemon_memory() - _running_limits() - GIB // 2) // 2 + 64 * 1024**2  # each fits alone, not the pair
    first, second = _stage(tmp_path, "large-a"), _stage(tmp_path, "large-b")
    seed_verdict(root, first)
    seed_verdict(root, second)
    worker_a = _worker(root, first, control, "a", "hold", container=half, host=256 * 1024**2, service=0)
    try:
        _wait(control / "entered-a.json")
        worker_b = _worker(root, second, control, "b", "hold", container=half, host=256 * 1024**2, service=0)
        _wait(control / "prepared-b.json")
        # B samples capacity every pass of its 30 s admission window; A holds through one whole window after B's
        # guardian started, so B always meets A's hold (a fixed 10 s from preparation lost the race under load).
        _until(lambda: _record(control, "b").get("guardian") is not None, message="B's guardian")
        bounded = time.monotonic() + 31
        while time.monotonic() < bounded:
            assert not (control / "entered-b.json").exists()
            time.sleep(0.2)
        (control / "release-a").touch()
        _finish(worker_a)
        _wait(control / "entered-b.json")
        (control / "release-b").touch()
        _finish(worker_b)
    finally:
        for worker in (worker_a, locals().get("worker_b")):
            if worker is not None and worker.poll() is None:
                worker.kill()
    a, b = _record(control, "a"), _record(control, "b")
    assert _waited(b, "coholder-capacity")
    assert granted_interval(b)[0] > granted_interval(a)[1]


@pytest.mark.parametrize("control_kind", ["attributed", "unlabeled"])
def test_coholder_containers_attributed_not_external(
    tmp_path: Path, redis_image: None, private_installation: Path, control_kind: str
) -> None:
    del redis_image
    root, control = private_installation, tmp_path / "control"
    control.mkdir()
    # B fits only when A's running 1 GiB service counts within A's own reservation: E(A) + E(B) fill the binding
    # capacity side to within 0.75 GiB, so one more external GiB would not fit (guest on the Mac, host on native Linux).
    budget = _pair_budget(root, 3 * GIB // 4)
    first, second = _stage(tmp_path, "attr-a"), _stage(tmp_path, "attr-b")
    seed_verdict(root, first)
    seed_verdict(root, second)
    docker = shutil.which("docker")
    assert docker is not None
    foreign = None
    service = GIB if control_kind == "attributed" else 0
    worker_a = _worker(root, first, control, "a", "hold", container=budget, host=256 * 1024**2, service=service)
    worker_b = None
    try:
        _wait(control / "entered-a.json")
        if control_kind == "unlabeled":
            foreign = subprocess.run(
                [docker, "run", "--detach", "--memory", str(GIB), REDIS, "redis-server", "--save", ""],
                capture_output=True,
                text=True,
                check=True,
            ).stdout.strip()
        worker_b = _worker(root, second, control, "b", "hold", container=budget, host=256 * 1024**2, service=0)
        if control_kind == "attributed":
            _wait(control / "entered-b.json")
        else:
            _wait(control / "prepared-b.json")
            bounded = time.monotonic() + 10
            while time.monotonic() < bounded:
                assert not (control / "entered-b.json").exists()
                time.sleep(0.2)
            subprocess.run([docker, "container", "rm", "-f", foreign], check=True, capture_output=True)
            foreign = None
        (control / "release-a").touch()
        _finish(worker_a)
        _wait(control / "entered-b.json")
        (control / "release-b").touch()
        _finish(worker_b)
    finally:
        if foreign is not None:
            subprocess.run([docker, "container", "rm", "-f", foreign], check=False, capture_output=True)
        for worker in (worker_a, worker_b):
            if worker is not None and worker.poll() is None:
                worker.kill()
    b = _record(control, "b")
    if control_kind == "unlabeled":
        assert _waited(b, "coholder-capacity")
    else:
        assert not _waited(b, "coholder-capacity")


CRASHING = (
    "services:\n"
    "  cache:\n"
    "    image: {image}\n"
    f"    mem_limit: {GIB // 4}\n"
    "    restart: always\n"
    "    command: [sh, -c, 'sleep 0.3; exit 1']\n"
)


def test_a_coholder_restarting_its_containers_ends_no_other_run(
    tmp_path: Path, redis_image: None, private_installation: Path
) -> None:
    """A co-holder whose service crash-loops (the daemon reports it Running with Pid 0 between restarts) ends no
    other run; a run that does not fit beside both waits with the figures that decided it, then runs."""
    del redis_image
    root, control = private_installation, tmp_path / "control"
    control.mkdir()
    budget = _pair_budget(root, 3 * GIB // 4)  # B and C fill the binding side to 0.75 GiB: A's 1 GiB does not fit
    a, b, c = (_stage(tmp_path, f"restart-{name}") for name in "abc")
    for project in (a, b, c):
        seed_verdict(root, project)
    up = {"write": {"compose.yml": CRASHING.format(image=REDIS), "test.env": "A=1\n"}, "docker": UP}
    worker_a = _held(root, a, control, "a", [up])
    worker_b = worker_c = None
    try:
        assert [step["rc"] for step in _steps(control, "a")] == [0], _steps(control, "a")
        worker_b = _worker(root, b, control, "b", "hold", container=budget, host=256 * 1024**2, service=0)
        _wait(control / "entered-b.json")
        for _ in range(3):  # three of B's observation passes while A's service keeps restarting
            tick = _record(control, "b")["admission_observation"]["samples"][-1]["tick_ns"]
            _until(lambda tick=tick: observed_after(_record(control, "b"), tick), message="a later observation of b")
        worker_c = _worker(root, c, control, "c", "hold", container=budget, host=256 * 1024**2, service=0)
        _wait(control / "prepared-c.json")
        _until(lambda: _record(control, "c")["state"] == "queued", message="c queued beside a and b")
        assert not (control / "entered-c.json").exists()
        assert _record(control, "b")["infrastructure_failure"] is None
        (control / "release-a").touch()  # A's own outcome is not asserted: its daemon restarts are its own state
        worker_a.communicate(timeout=180)
        _wait(control / "entered-c.json")
        _release(control, "b", worker_b)
        _release(control, "c", worker_c)
    finally:
        _killing(worker_a, worker_b, worker_c)

    records = {name: _record(control, name) for name in "abc"}
    assert records["b"]["infrastructure_failure"] is None and records["c"]["infrastructure_failure"] is None
    assert all(record["infrastructure_failure"] != "unknown-job-containment" for record in records.values())
    waits = [item for item in records["c"]["hold_timeline"]["waiting"] if item["reason"] == "coholder-capacity"]
    assert waits and {"coholders", "external_bytes"} <= set(waits[0]["numbers"])


def test_own_host_envelope_breach_fails_under_production_observation(
    tmp_path: Path, redis_image: None, private_installation: Path
) -> None:
    del redis_image
    root, control = private_installation, tmp_path / "control"
    control.mkdir()
    project = _stage(tmp_path, "breach")
    worker = _worker(root, project, control, "a", "breach", container=GIB, host=64 * 1024**2, service=0)
    try:
        _wait(control / "entered-a.json")
        _until(
            lambda: _record(control, "a")["infrastructure_failure"] is not None,
            timeout=60,
            message="the observation failed the run",
        )
        (control / "release-a").touch()  # the requester leaves its failed run
        worker.communicate(timeout=180)
    finally:
        if worker.poll() is None:
            worker.kill()

    assert _record(control, "a")["infrastructure_failure"] == "host-process-envelope"


# --- share rules on real renders ---

QUARTER = GIB // 4
COMPOSE = (
    "services:\n"
    "  cache:\n"
    "    image: {image}\n"
    f"    mem_limit: {QUARTER}\n"
    "    command: [redis-server, --save, '', --appendonly, 'no']\n"
)
PORT = COMPOSE + "    ports: ['127.0.0.1:18379:6379']\n"
UP = ["compose", "--file", "compose.yml", "--env-file", "test.env", "up", "--detach"]
DOWN = ["compose", "--file", "compose.yml", "--env-file", "test.env", "down", "--volumes"]


def _steps(control: Path, name: str) -> list[dict]:
    return _wait(control / f"steps-{name}.json")  # type: ignore[return-value]


def _held(root: Path, project: Path, control: Path, name: str, steps: list[dict], **options):
    """Start one worker that runs ``steps`` after entry and holds until released."""
    return _worker(
        root,
        project,
        control,
        name,
        "hold",
        container=GIB,
        host=256 * 1024**2,
        service=0,
        options={"steps": steps, **options},
    )


def _release(control: Path, name: str, worker) -> None:
    (control / f"release-{name}").touch()
    _finish(worker)


def _killing(*workers):
    for worker in workers:
        if worker is not None and worker.poll() is None:
            worker.kill()


def _up_steps(compose: str = COMPOSE, *extra: str) -> list[dict]:
    return [{"write": {"compose.yml": compose, "test.env": "A=1\n"}, "docker": [*UP, *extra]}, {"docker": DOWN}]


def test_warm_up_exclusive_run_teaches_positive_then_next_run_shared(
    tmp_path: Path, redis_image: None, private_installation: Path
) -> None:
    del redis_image
    root, control = private_installation, tmp_path / "control"
    control.mkdir()
    project = _stage(tmp_path, "warm")
    first = _held(root, project, control, "first", _up_steps())
    try:
        assert [step["rc"] for step in _steps(control, "first")] == [0, 0]
        assert "sharing" not in _record(control, "first")  # nothing predicted: exclusive
        _release(control, "first", first)
        verdict = _verdict(root, project)
        assert verdict is not None and verdict["shareable"] is True
        assert verdict["run_id"] == _record(control, "first")["run_id"]
        second = _held(root, project, control, "second", [])
        _wait(control / "entered-second.json")
        assert _record(control, "second")["sharing"] == {"mode": "shared"}
        _release(control, "second", second)
    finally:
        _killing(first, locals().get("second"))


def test_fixed_port_refused_then_next_run_exclusive(
    tmp_path: Path, redis_image: None, private_installation: Path
) -> None:
    del redis_image
    root, control = private_installation, tmp_path / "control"
    control.mkdir()
    project = _stage(tmp_path, "port")
    seed_verdict(root, project)
    shared = _held(root, project, control, "shared", _up_steps(PORT))
    try:
        up, _down = _steps(control, "shared")
        assert up["rc"] != 0 and "R1" in up["stderr"], up
        assert _record(control, "shared")["sharing"] == {"mode": "shared"}
        assert not any(
            receipt["plan"].get("subcommand") == "up"
            for receipt in _record(control, "shared")["docker_receipts"].values()
        )
        _release(control, "shared", shared)
        verdict = _verdict(root, project)
        assert verdict is not None and verdict["shareable"] is False and "R1" in verdict["reason"]
        after = _held(root, project, control, "after", [])
        _wait(control / "entered-after.json")
        assert "sharing" not in _record(control, "after")
        _release(control, "after", after)
    finally:
        _killing(shared, locals().get("after"))


def test_same_project_runs_overlap(tmp_path: Path, redis_image: None, private_installation: Path) -> None:
    del redis_image
    root, control = private_installation, tmp_path / "control"
    control.mkdir()
    project = _stage(tmp_path, "same")
    seed_verdict(root, project)
    hold = [{"write": {"compose.yml": COMPOSE, "test.env": "A=1\n"}, "docker": UP}]
    workers = [_held(root, project, control, name, hold) for name in ("a", "b")]
    try:
        assert [step["rc"] for step in _steps(control, "a") + _steps(control, "b")] == [0, 0]
        a, b = _record(control, "a"), _record(control, "b")
        both = max(granted_interval(a)[0], granted_interval(b)[0])
        _until(
            lambda: observed_after(_record(control, "a"), both) and observed_after(_record(control, "b"), both),
            message="an observation of both runs after both grants",
        )
        assert a["sharing"] == b["sharing"] == {"mode": "shared"}
        for name, worker in zip(("a", "b"), workers, strict=True):
            _release(control, name, worker)
    finally:
        _killing(*workers)
    (granted_a, released_a), (granted_b, released_b) = (
        granted_interval(_record(control, "a")),
        granted_interval(_record(control, "b")),
    )
    assert max(granted_a, granted_b) < min(released_a, released_b)


def test_same_project_fixed_port_second_render_refused(
    tmp_path: Path, redis_image: None, private_installation: Path
) -> None:
    # Within one run a changed render is already refused ("Compose configuration changed after exact resources were
    # promoted"); the share rules meet the second render in a second shared run of the same project.
    del redis_image
    root, control = private_installation, tmp_path / "control"
    control.mkdir()
    project = _stage(tmp_path, "second-render")
    seed_verdict(root, project)
    first = _held(
        root, project, control, "first", [{"write": {"compose.yml": COMPOSE, "test.env": "A=1\n"}, "docker": UP}]
    )
    try:
        assert [step["rc"] for step in _steps(control, "first")] == [0]
        second = _held(
            root, project, control, "second", [{"write": {"port.yml": PORT}, "docker": [*UP[:2], "port.yml", *UP[3:]]}]
        )
        (up,) = _steps(control, "second")
        assert up["rc"] != 0 and "R1" in up["stderr"], up
        assert _record(control, "second")["sharing"] == {"mode": "shared"}
        _release(control, "second", second)
        _release(control, "first", first)
    finally:
        _killing(first, locals().get("second"))


@pytest.mark.parametrize("changed", ["file", "env-file"])
def test_shared_relative_inputs_bound_and_teardown_after_change_not_refused(
    tmp_path: Path, redis_image: None, private_installation: Path, changed: str
) -> None:
    # Ruling: the change between hashing and execution is proven at the failpoint in the integration suite;
    # here the real dispatcher resolves the relative inputs against the worker's cwd and teardown stays unchecked.
    del redis_image
    root, control = private_installation, tmp_path / "control"
    control.mkdir()
    project = _stage(tmp_path, f"relative-{changed}")
    seed_verdict(root, project)
    target = "compose.yml" if changed == "file" else "test.env"
    steps = [
        {"write": {"compose.yml": COMPOSE, "test.env": "A=1\n"}, "docker": UP},
        {"write": {target: (COMPOSE if changed == "file" else "A=1\n") + "# changed\n"}, "docker": DOWN},
    ]
    shared = _held(root, project, control, "shared", steps)
    try:
        assert [step["rc"] for step in _steps(control, "shared")] == [0, 0], _steps(control, "shared")
        up = next(
            receipt["plan"]
            for receipt in _record(control, "shared")["docker_receipts"].values()
            if receipt["plan"].get("subcommand") == "up"
        )
        resolved = project.resolve()
        assert [item["path"] for item in up["files"]] == [str(resolved / "compose.yml"), str(resolved / "test.env")]
        _release(control, "shared", shared)
    finally:
        _killing(shared)


def test_up_scale_two_refused(tmp_path: Path, redis_image: None, private_installation: Path) -> None:
    del redis_image
    root, control = private_installation, tmp_path / "control"
    control.mkdir()
    project = _stage(tmp_path, "scale")
    seed_verdict(root, project)
    steps = [{"write": {"compose.yml": COMPOSE, "test.env": "A=1\n"}, "docker": [*UP, "--scale", "cache=2"]}]
    shared = _held(root, project, control, "shared", steps)
    try:
        (up,) = _steps(control, "shared")
        assert up["rc"] != 0 and "R14" in up["stderr"], up
        _release(control, "shared", shared)
    finally:
        _killing(shared)


def test_shared_run_with_live_builder_transitions_into_smaller_phase(
    tmp_path: Path, redis_image: None, private_installation: Path
) -> None:
    del redis_image
    root, control = private_installation, tmp_path / "control"
    control.mkdir()
    project = _stage(tmp_path, "builder")
    seed_verdict(root, project)
    phases = {"build": [2 * GIB + QUARTER, 256 * 1024**2, 2, 1], "tests": [GIB // 2, 256 * 1024**2, 1, 1]}
    steps = [
        {"docker": ["buildx", "create"]},
        {"transition": "tests"},  # the live 2 GiB worker exceeds this phase's 0.5 GiB container budget
        {"docker": ["buildx", "rm", "{builder}"]},
    ]
    shared = _held(root, project, control, "shared", steps, phases=phases, initial="build")
    try:
        create, transition, _remove = _steps(control, "shared")
        assert create["rc"] == 0, create
        assert transition == {"rc": 0, "state": "active"}, transition
        assert _record(control, "shared")["sharing"] == {"mode": "shared"}
        assert _record(control, "shared")["grant"]["phase"] == "tests"
        _release(control, "shared", shared)
    finally:
        _killing(shared)


# --- the image lock K on a real daemon ---

BUILD_FILES = {"Dockerfile": "FROM scratch\nCOPY marker /marker\n", "marker": "k\n"}
K_PHASES = {"tests": [2 * GIB + QUARTER, 256 * 1024**2, 2, 1]}  # admits the builder
K_OPERATIONS = {
    "pull": ([], ["pull", REDIS]),
    "builder-create": ([], ["buildx", "create"]),
    "build": ([{"docker": ["buildx", "create"]}], ["buildx", "build", "--load", "--tag", "shared-k:latest", "."]),
}


def _hold_image_lock(root: Path):
    from docker_test_toolkit.application.recovery import Context, image_lock

    account = installation_of((root / "current").resolve() / ARCHIVE_NAME).account_directory
    return image_lock(Context(account, account))


def _k_steps(operation: str) -> list[dict]:
    before, argv = K_OPERATIONS[operation]
    return [{"write": BUILD_FILES}, *before, {"await": "go"}, {"docker": argv}]


@pytest.mark.parametrize("operation", list(K_OPERATIONS))
def test_k_serializes_image_operations(
    tmp_path: Path, redis_image: None, private_installation: Path, operation: str
) -> None:
    del redis_image
    root, control = private_installation, tmp_path / "control"
    control.mkdir()
    project = _stage(tmp_path, f"k-{operation}")
    seed_verdict(root, project)
    shared = _held(root, project, control, "shared", _k_steps(operation), phases=K_PHASES)
    try:
        _wait(control / "ready-shared")
        assert _record(control, "shared")["sharing"] == {"mode": "shared"}
        with _hold_image_lock(root):
            (control / "go-shared").touch()
            time.sleep(3)  # the worker's image operation waits for K all this time
        steps = _steps(control, "shared")
        assert all(step["rc"] == 0 for step in steps), steps
        waits = [i for i in _record(control, "shared")["hold_timeline"]["waiting"] if i["reason"] == "image-lock"]
        assert len(waits) == 1 and waits[0]["waited_ns"] >= 2_000_000_000, waits  # the 3 s hold, less the go poll
        _release(control, "shared", shared)
    finally:
        _killing(shared)


def test_exclusive_build_takes_no_k(tmp_path: Path, redis_image: None, private_installation: Path) -> None:
    del redis_image
    root, control = private_installation, tmp_path / "control"
    control.mkdir()
    project = _stage(tmp_path, "k-exclusive")  # no verdict: exclusive
    exclusive = _held(root, project, control, "exclusive", _k_steps("build"), phases=K_PHASES)
    try:
        _wait(control / "ready-exclusive")
        with _hold_image_lock(root):
            (control / "go-exclusive").touch()
            steps = _steps(control, "exclusive")  # completes while the test still holds K
        assert all(step["rc"] == 0 for step in steps), steps
        record = _record(control, "exclusive")
        assert "sharing" not in record
        assert all(item["reason"] != "image-lock" for item in record["hold_timeline"]["waiting"])
        _release(control, "exclusive", exclusive)
    finally:
        _killing(exclusive)


# --- image interest, hand-off and drain on a real daemon ---


def _built_image(control: Path, name: str) -> str:
    owned = _record(control, name)["owned_resources"]["images"]
    return next(identity for identity, item in owned.items() if item["state"] != "removed")


def _image_exists(image_id: str) -> bool:
    docker = shutil.which("docker")
    assert docker is not None
    return subprocess.run([docker, "image", "inspect", image_id], capture_output=True, check=False).returncode == 0


def _pending(root: Path) -> list[dict]:
    path = installation_of((root / "current").resolve() / ARCHIVE_NAME).account_directory / sharing.PENDING_FILE
    return json.loads(path.read_text())["entries"] if path.exists() else []


def _owner_builds(tmp_path: Path, root: Path, control: Path):
    project = _stage(tmp_path, "img-owner")
    seed_verdict(root, project)
    steps = [
        {"write": BUILD_FILES},
        {"docker": ["buildx", "create"]},
        {"docker": ["buildx", "build", "--load", "--tag", f"shared-owned-{tmp_path.name.lower()}:latest", "."]},
    ]
    owner = _held(root, project, control, "owner", steps, phases=K_PHASES)
    assert all(step["rc"] == 0 for step in _steps(control, "owner")), _steps(control, "owner")
    return owner, _built_image(control, "owner")


def _user_of(tmp_path: Path, root: Path, control: Path, image_id: str, steps: list[dict] | None = None):
    project = _stage(tmp_path, "img-user")
    seed_verdict(root, project)
    user = _held(root, project, control, "user", steps or [], use_image=image_id)
    _wait(control / "entered-user.json")
    assert _record(control, "user")["sharing"] == {"mode": "shared"}
    return user


def test_owner_leave_hands_off_and_the_last_user_drains(
    tmp_path: Path, redis_image: None, private_installation: Path
) -> None:
    del redis_image
    root, control = private_installation, tmp_path / "control"
    control.mkdir()
    owner, image_id = _owner_builds(tmp_path, root, control)
    user = None
    try:
        user = _user_of(tmp_path, root, control, image_id)  # declares use_image of the owner's image
        _release(control, "owner", owner)

        assert _image_exists(image_id)  # kept for the live user
        assert [entry["image_id"] for entry in _pending(root)] == [image_id]
        _release(control, "user", user)

        assert not _image_exists(image_id)  # the last user drained it
        assert _pending(root) == []
    finally:
        _killing(owner, user)


def test_owner_image_rm_hands_off_and_non_owner_rm_is_refused(
    tmp_path: Path, redis_image: None, private_installation: Path
) -> None:
    del redis_image
    root, control = private_installation, tmp_path / "control"
    control.mkdir()
    project = _stage(tmp_path, "img-owner")
    seed_verdict(root, project)
    steps = [
        {"write": BUILD_FILES},
        {"docker": ["buildx", "create"]},
        {"docker": ["buildx", "build", "--load", "--tag", f"shared-owned-{tmp_path.name.lower()}:latest", "."]},
        {"await": "go"},
        {"docker": ["image", "rm", "{built}"]},
    ]
    owner = _held(root, project, control, "owner", steps, phases=K_PHASES)
    user = None
    try:
        _wait(control / "ready-owner")
        image_id = _built_image(control, "owner")
        user = _user_of(tmp_path, root, control, image_id, [{"docker": ["image", "rm", image_id]}])
        (control / "go-owner").touch()

        (refused,) = _steps(control, "user")
        assert refused["rc"] != 0  # not the user's image: refused before any mutation
        assert all(step["rc"] == 0 for step in _steps(control, "owner")), _steps(control, "owner")
        assert _image_exists(image_id)  # the owner's rm handed it off to the live user
        _release(control, "owner", owner)
        _release(control, "user", user)
        assert not _image_exists(image_id)
    finally:
        _killing(owner, user)


# --- pressure while sharing ---

HOG = r"""import os, pathlib, sys, time
size, ready, stop = int(sys.argv[1]), pathlib.Path(sys.argv[2]), pathlib.Path(sys.argv[3])
chunk = 64 * 1024 * 1024
pages = [bytearray(os.urandom(chunk)) for _ in range(size // chunk)]  # incompressible, every page touched
ready.write_text("")
deadline = time.monotonic() + 600
while not stop.exists() and time.monotonic() < deadline:
    for page in pages:  # re-touch every page each second so it stays resident
        for offset in range(0, chunk, 4096):
            page[offset] ^= 1
    time.sleep(1)
"""


def test_no_foreign_failure_including_after_departure(
    tmp_path: Path, redis_image: None, private_installation: Path
) -> None:
    """Proof 3: a warmed joiner beside a co-holder, both at full declared budgets, observed after both grants; neither
    is failed, and the remaining run is still not failed after the co-holder departs."""
    del redis_image
    root, control = private_installation, tmp_path / "control"
    control.mkdir()
    first, second = _stage(tmp_path, "full-a"), _stage(tmp_path, "full-b")
    seed_verdict(root, first)
    seed_verdict(root, second)
    workers = [
        _worker(root, project, control, name, "hold", container=GIB, host=256 * 1024**2, service=GIB // 2)
        for project, name in ((first, "a"), (second, "b"))
    ]
    try:
        _wait(control / "entered-a.json")
        _wait(control / "entered-b.json")
        both = max(granted_interval(_record(control, "a"))[0], granted_interval(_record(control, "b"))[0])
        _until(lambda: observed_after(_record(control, "a"), both), message="an observation of A after both grants")
        (control / "release-b").touch()
        _finish(workers[1])
        departed = _record(control, "b")["hold_timeline"]["released_ns"]
        _until(lambda: observed_after(_record(control, "a"), departed), message="an observation of A after B left")
        (control / "release-a").touch()
        _finish(workers[0])
    finally:
        _killing(*workers)

    assert _record(control, "a")["infrastructure_failure"] is None
    assert _record(control, "b")["infrastructure_failure"] is None
    # The later grant is beside a co-holder and is logged. The earlier one is logged too exactly when the later run's
    # admission baseline precedes it (both admitting at once) and the later run still holds when the earlier leaves:
    # the leave's repair logs it for that holder, and for nobody else.
    earlier, later = sorted((_record(control, name) for name in "ab"), key=lambda r: r["hold_timeline"]["granted_ns"])
    baseline = later["admission_observation"]["memory"]
    counted = (
        baseline["origin"] == earlier["hold_timeline"]["origin"]
        and baseline["tick_ns"] < earlier["hold_timeline"]["granted_ns"]
        and earlier["hold_timeline"]["released_ns"] < later["hold_timeline"]["released_ns"]
    )
    logged = {grant.run_id for grant in sharing.read_grants(_account(root))}
    assert logged == {later["run_id"], *([earlier["run_id"]] if counted else [])}


def test_own_breach_beside_coholder_fails_only_the_breaching_run(
    tmp_path: Path, redis_image: None, private_installation: Path
) -> None:
    del redis_image
    root, control = private_installation, tmp_path / "control"
    control.mkdir()
    steady, breaching = _stage(tmp_path, "steady"), _stage(tmp_path, "breaching")
    seed_verdict(root, steady)
    seed_verdict(root, breaching)
    holder = _worker(root, steady, control, "a", "hold", container=GIB, host=256 * 1024**2, service=0)
    breach = None
    try:
        _wait(control / "entered-a.json")
        breach = _worker(root, breaching, control, "b", "breach", container=GIB, host=64 * 1024**2, service=0)
        _wait(control / "entered-b.json")
        _until(lambda: _record(control, "b")["infrastructure_failure"] is not None, timeout=60, message="B failed")
        (control / "release-b").touch()
        breach.communicate(timeout=180)
        (control / "release-a").touch()
        _finish(holder)
    finally:
        _killing(holder, breach)

    assert _record(control, "b")["infrastructure_failure"] == "host-process-envelope"
    assert _record(control, "a")["infrastructure_failure"] is None


def _account(root: Path) -> Path:
    return installation_of((root / "current").resolve() / ARCHIVE_NAME).account_directory


def _decline(record: dict, sample: dict) -> int:
    return record["phase_history"][-1]["memory"]["headroom_bytes"] - sample["headroom_bytes"]


def test_a_host_decline_others_caused_fails_no_granted_run(
    tmp_path: Path, redis_image: None, private_installation: Path
) -> None:
    """A foreign 5 GiB host process takes memory while A and B hold: neither run is failed, before or after B's
    departure; such pressure only makes later runs wait."""
    del redis_image
    root, control = private_installation, tmp_path / "control"
    control.mkdir()
    reserve = subprocess.run(
        [
            str(base_python()),
            "-I",
            str((root / "current").resolve() / ARCHIVE_NAME),
            "install",
            "--json",
            "--root",
            str(root),
            "--host-reserve-bytes",
            str(GIB),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert reserve.returncode == 0, reserve.stdout + reserve.stderr
    small, large = _stage(tmp_path, "allowance-a"), _stage(tmp_path, "allowance-b")
    seed_verdict(root, small)
    seed_verdict(root, large)
    (tmp_path / "hog.py").write_text(HOG, encoding="utf-8")
    a = _worker(root, small, control, "a", "hold", container=GIB // 2, host=128 * 1024**2, service=0)
    b = hog = None
    try:
        _wait(control / "entered-a.json")
        granted_a = granted_interval(_record(control, "a"))[0]
        _until(lambda: observed_after(_record(control, "a"), granted_a), message="A's first sample after its grant")
        b = _worker(root, large, control, "b", "hold", container=GIB // 2, host=4 * GIB, service=0)
        _wait(control / "entered-b.json", timeout=600)  # a host without about 6 GiB headroom waits here and fails
        logged: list[sharing.Grant] = []

        def b_logged() -> bool:  # the grants log is appended just after the grant B's requester can already see
            run_id = _record(control, "b")["run_id"]
            logged[:] = [grant for grant in sharing.read_grants(_account(root)) if grant.run_id == run_id]
            return bool(logged)

        _until(b_logged, timeout=30, message="B's grant in the grants log")
        assert len(logged) == 1 and logged[0].budget_bytes >= 4 * GIB
        # Setup check, not a skip: a host decline can only reach the hog's size where the headroom holds it; below
        # that, macOS compresses and swaps instead and no run declines.
        headroom = _record(control, "b")["phase_history"][-1]["memory"]["headroom_bytes"]
        assert headroom >= 6 * GIB, f"setup: B's phase baseline headroom {headroom / GIB:.2f} GiB is below 6 GiB"
        hog = subprocess.Popen(
            [
                str(base_python()),
                "-I",
                str(tmp_path / "hog.py"),
                str(5 * GIB),  # about 1.4 GiB above B's 3 + 0.6 GiB threshold, 2.5 GiB below A's 3 + 4.5 GiB
                str(control / "hog-ready"),
                str(control / "hog-stop"),
            ]
        )
        _wait(control / "hog-ready", timeout=120)
        excused = []

        def a_excused_sample() -> bool:
            record = _record(control, "a")
            assert record["infrastructure_failure"] is None, record.get("failure_evidence")
            excused.extend(
                sample for sample in record["admission_observation"]["samples"] if _decline(record, sample) > 3 * GIB
            )
            return bool(excused)

        def b_declined_and_holds() -> bool:
            record = _record(control, "b")
            assert record["infrastructure_failure"] is None, record.get("failure_evidence")
            beyond = 3 * GIB + GIB // 2  # beyond the old breach threshold of 3 GiB plus A's budget
            return any(_decline(record, sample) > beyond for sample in record["admission_observation"]["samples"])

        _until(b_declined_and_holds, timeout=180, message="a sample of B beyond the old decline threshold")
        _until(a_excused_sample, timeout=60, message="a sample of A beyond the unadjusted threshold")
        (control / "release-b").touch()
        b.communicate(timeout=180)
        departed = _record(control, "b")["hold_timeline"]["released_ns"]
        after = []

        def two_declined_passes_after_departure() -> bool:
            record = _record(control, "a")
            assert record["infrastructure_failure"] is None, record.get("failure_evidence")
            after[:] = [
                sample
                for sample in record["admission_observation"]["samples"]
                if sample["tick_ns"] > departed and _decline(record, sample) > 3 * GIB
            ]
            return len(after) >= 2

        _until(two_declined_passes_after_departure, timeout=60, message="two declined passes of A after B left")
        (control / "hog-stop").touch()
        hog.wait(timeout=60)
        (control / "release-a").touch()
        _finish(a)
    finally:
        (control / "hog-stop").touch()
        if hog is not None and hog.poll() is None:
            hog.kill()
        _killing(a, b)

    assert _record(control, "a")["infrastructure_failure"] is None


def test_leftover_sweep_removes_a_recorded_runs_builder(tmp_path: Path) -> None:
    """The harness's own safety net: a builder of a recorded run that nothing removed is found from the record and
    removed with its worker and state, and named as a leftover."""
    from tests.e2e.conftest import remove_run_leftovers

    from docker_test_toolkit.application.lifecycle import builder_name

    registry = tmp_path / "state"
    (registry / "runs").mkdir(parents=True)
    project_id, run_id = "f" * 64, str(uuid.uuid4())  # a fresh identity: concurrent sessions never share a builder
    (registry / "runs" / f"{run_id}.json").write_text(json.dumps({"project_id": project_id, "run_id": run_id}))
    builder = builder_name(project_id, run_id)
    docker = shutil.which("docker")
    assert docker is not None
    created = subprocess.run(
        [docker, "buildx", "create", "--name", builder, "--driver", "docker-container", "--bootstrap"],
        capture_output=True,
        text=True,
        check=False,
        timeout=300,
    )
    try:
        assert created.returncode == 0, created.stderr

        removed = remove_run_leftovers(registry)

        assert removed and removed[0] == f"builder {builder}"
        worker = f"buildx_buildkit_{builder}0"
        assert subprocess.run([docker, "container", "inspect", worker], capture_output=True, check=False).returncode
        assert subprocess.run([docker, "buildx", "inspect", builder], capture_output=True, check=False).returncode
    finally:
        subprocess.run([docker, "buildx", "rm", "--force", builder], capture_output=True, check=False)
