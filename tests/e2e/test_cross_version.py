"""Two installed builds share one temporary root: switch, rollback and retained runs across versions.

Each pairing installs the old build, lets workers act, then switches ``current`` with the real installer. A worker
loads the toolkit through the reference loader and then waits for its go signal, so a test can hold a process that
loaded the old build across the switch. The pairings are N against N' (a renamed copy of the build under test) and,
from the second installed-era release on, the previous release against the build under test.
"""

from __future__ import annotations

import json
import os
import re
import signal
import subprocess
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from functools import partial, reduce
from pathlib import Path

import pytest
from tests.e2e.conftest import _REDIS, _base_python, _sweep, _toolkit, redis_id
from tests.installation_fixtures import base_python, previous_release_archive, renamed_archive
from tests.integration.account_fixtures import queue_entries, queue_phases, reap_orphan

from docker_test_toolkit.application.installation import archive_version, install
from docker_test_toolkit.config.installation import ARCHIVE_NAME
from docker_test_toolkit.domain.models import SHARED_QUEUE_MIN_VERSION
from docker_test_toolkit.domain.policy import version_at_least

pytestmark = pytest.mark.e2e


# Releases before 0.7.2 know no runtime but Docker Desktop on a Mac: they refuse Colima's and Podman's daemons, so a
# pairing that runs the previous release needs Docker Desktop or Linux; a run on another runtime deselects it with
# -m "e2e and not previous_release" and records the exclusion.
previous_release_runs_here = pytest.mark.previous_release
LOADER = Path(__file__).parents[2] / "docs" / "loader"

WORKER = r"""from __future__ import annotations
import json
import re
import os
import pathlib
import subprocess
import sys
import time

project, loader_directory, root, control = map(pathlib.Path, sys.argv[1:5])
name, action = sys.argv[5:7]
run_name = action.partition(":")[2] or name
sys.path.insert(0, str(loader_directory))
import docker_test_toolkit_loader as loader

toolkit, _archive = loader.load_toolkit((root,))
version = toolkit.toolkit_identity().version


def signal(kind, **values):
    # Publish a report atomically, stamped with the machine-wide monotonic clock so the test can order events across
    # processes without depending on the wall clock.
    target = control / f"{kind}-{name}.json"
    staged = control / f".{kind}-{name}.json.tmp"
    staged.write_text(json.dumps({"version": version, "at_ns": time.monotonic_ns(), **values}))
    os.replace(staged, target)


def wait_for(kind):
    deadline = time.monotonic() + 300
    while not (control / f"{kind}-{name}").exists():
        if time.monotonic() > deadline:
            raise RuntimeError(f"{kind} signal timed out")
        time.sleep(0.05)


signal("loaded")
wait_for("go")
retained = action.split(":")[0] in {"retain", "resume", "release"}
uses_cache = retained or action in ("compose", "build")
services = ("cache",) if uses_cache else ()
spec = toolkit.RunSpec(
    project_root=project,
    entrypoint="e2e",
    purpose=f"cross-version-{run_name}",
    services=services,
    closure=services,
    profile="cross-version",
    phases={"tests": (
        toolkit.PhaseBudget(2 * 1024 ** 3, 512 * 1024 ** 2, 2, 2) if action == "build"  # the Buildx worker envelope
        else toolkit.PhaseBudget(1024 ** 3 if uses_cache else 0, 512 * 1024 ** 2, 1, 1)  # two fit on a 2-CPU host
    )},
    initial_phase="tests",
    external_resources=(
        (toolkit.ExternalDockerResource("image", os.environ["CROSS_REDIS_ID"], "redis", "use_image"),)
        if uses_cache else ()
    ),
    retain=retained,
    wait_seconds=120,
)
locator = control / f"{run_name}.locator.json"
if action.startswith("resume:"):
    handle = toolkit.open_retained_run(spec, locator)
    handle.resume()
    resumed = handle.retain("passed")
    released = handle.release()
    signal("done", resumed=resumed.state, generation=resumed.generation, state=released.state)
    raise SystemExit(0)
if action.startswith("release:"):
    released = toolkit.open_retained_run(spec, locator).release()
    signal("done", state=released.state, cleanup=released.cleanup_outcome)
    raise SystemExit(0)
try:
    handle = toolkit.open_run(spec)
    signal("prepared", run_id=handle.run_id)
    handle.enter()
except toolkit.ToolkitError as error:
    signal("refused", category=error.category, reason=getattr(error, "reason", None))
    raise SystemExit(0)
entered = True
try:
    signal("entered", run_id=handle.run_id)
    if action in ("retain", "compose"):
        context = handle.docker_context
        compose = handle.private_root / "compose.yml"
        compose.write_text(
            "services:\n"
            "  cache:\n"
            f"    image: {os.environ['CROSS_REDIS_IMAGE']}\n"
            "    mem_limit: 1g\n"
            "    cpus: 1\n"
            "    command: [redis-server, --save, '', --appendonly, 'no']\n"
            "    healthcheck:\n"
            "      test: [CMD, redis-cli, ping]\n"
            "      interval: 1s\n"
            "      timeout: 1s\n"
            "      retries: 30\n",
            encoding="utf-8",
        )
        started = subprocess.run(
            [str(context.executable), "compose", "--file", str(compose), "up", "--detach", "--wait",
             "--wait-timeout", "60"],
            cwd=project, env=dict(context.environment), capture_output=True, text=True, check=False,
        )
        if started.returncode:
            raise RuntimeError(started.stderr)
    if action == "build":  # a Buildx builder and an owned image: the evidence additions of 0.7.2
        context = handle.docker_context
        (project / "Dockerfile").write_text(f"FROM {os.environ['CROSS_REDIS_IMAGE']}\nRUN echo cross > /marker\n")
        for argv in (("buildx", "create"), ("buildx", "build", "--tag", f"dtt-cross:{handle.run_id}", ".")):
            built = subprocess.run(
                [str(context.executable), *argv], cwd=project, env=dict(context.environment), capture_output=True,
                text=True, check=False, timeout=1200,
            )
            if built.returncode:
                raise RuntimeError(built.stderr)
        signal("built", context={
            "project_root": str(handle._context.project.root),
            "project_state": str(handle._context.project.state),
            "run_id": handle.run_id,
            "session_id": handle.session_id,
            "record": str(handle._context.record),
            "private_root": str(handle.private_root),
        })
    if action == "retain":
        status = handle.retain("passed")
        entered = False
        handle.write_retained_locator(locator)
    else:
        wait_for("release")
        signal("leaving")
        status = handle.leave("passed")
        entered = False
    signal("done", state=status.state, cleanup=status.cleanup_outcome)
finally:
    if entered:
        handle.leave("failed")
"""


@dataclass
class _Pairing:
    """One temporary root, an old and a new build, and the workers started in it."""

    tmp: Path
    old: Path
    new: Path

    def __post_init__(self) -> None:
        self.root = self.tmp / "root"
        self.control = self.tmp / "control"
        self.control.mkdir(parents=True)
        self.old_version, self.new_version = archive_version(self.old), archive_version(self.new)
        self.processes: dict[str, subprocess.Popen[str]] = {}

    def install(self, archive: Path, parallel_runs: int | None = None) -> None:
        """Make ``archive`` current with the real installer (its lock order and format check included)."""
        install(archive, root=self.root, python=base_python(), test_root=True, parallel_runs=parallel_runs)

    def worker(self, name: str, action: str, *, project: str | None = None) -> subprocess.Popen[str]:
        """Start a worker that loads the current build, reports ``loaded`` and then waits for ``go-<name>``."""
        directory = self.tmp / "projects" / (project or name)
        directory.mkdir(parents=True, exist_ok=True)
        temporary = self.tmp / "temporary" / name
        temporary.mkdir(parents=True)
        script = self.tmp / "worker.py"
        script.write_text(WORKER, encoding="utf-8")
        process = subprocess.Popen(
            [
                _base_python(),
                "-I",
                str(script),
                str(directory),
                str(LOADER),
                str(self.root),
                str(self.control),
                name,
                action,
            ],
            cwd=directory,
            env={
                "CROSS_REDIS_ID": redis_id(),
                "CROSS_REDIS_IMAGE": _REDIS,
                "PATH": os.environ.get("PATH", ""),
                "TMPDIR": str(temporary),
            },
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self.processes[name] = process
        return process

    def signal(self, kind: str, name: str) -> None:
        (self.control / f"{kind}-{name}").touch()

    def report(self, kind: str, name: str, timeout: float = 180) -> dict:
        path = self.control / f"{kind}-{name}.json"
        deadline = time.monotonic() + timeout
        while not path.exists():
            assert time.monotonic() < deadline, f"timed out waiting for {path.name}"
            process = self.processes.get(name)
            if process is not None and process.poll() not in (None, 0):
                raise AssertionError(f"{name} failed before {path.name}:\n{process.communicate()[1]}")
            time.sleep(0.05)
        report = json.loads(path.read_text(encoding="utf-8"))
        report.pop("at_ns")
        return report

    def at_ns(self, kind: str, name: str) -> int:
        """Return when a worker published a report (monotonic clock, shared by every process of the machine)."""
        return json.loads((self.control / f"{kind}-{name}.json").read_text(encoding="utf-8"))["at_ns"]

    def exited(self, name: str) -> None:
        """Wait until a worker process has ended successfully: a retained run's requester must be gone."""
        stdout, stderr = self.processes[name].communicate(timeout=120)
        assert self.processes[name].returncode == 0, f"stdout={stdout}\nstderr={stderr}"

    def queued(self, run_id: str, timeout: float = 60) -> None:
        """Wait until the run's queue entry is ``queued``: it waits behind the holder instead of being refused."""
        account = self.root / "state" / "test-admission"
        deadline = time.monotonic() + timeout
        while queue_phases(account).get(run_id) != "queued":
            assert time.monotonic() < deadline, f"{run_id} never queued"
            time.sleep(0.05)

    def started(self, name: str, action: str, *, project: str | None = None, go: bool = True) -> dict:
        """Start a worker, wait until it has loaded, release it unless ``go`` is False; return its ``loaded`` report."""
        self.worker(name, action, project=project)
        loaded = self.report("loaded", name)
        if go:
            self.signal("go", name)
        return loaded

    def close(self) -> list[str]:
        """End every worker, then recover, clean and retire every project of this root; return the leftovers."""
        for process in self.processes.values():
            if process.poll() is None:
                process.kill()
            process.communicate(timeout=60)
        if not (self.root / "current").exists():
            return []
        archive = (self.root / "current").resolve() / ARCHIVE_NAME
        return _sweep(
            self.tmp,
            "end",
            time.monotonic() + 120,
            registry=self.root / "state",
            toolkit=partial(_toolkit, archive=archive),
        )


@pytest.fixture(scope="session")
def builds(tmp_path_factory: pytest.TempPathFactory, session_installation: Path) -> list[tuple[str, Path, Path]]:
    """(label, old, new): N against N', and the previous release against N once one exists."""
    nprime = renamed_archive(session_installation, tmp_path_factory.mktemp("nprime"), "9.9.9")
    pairings = [("n-prime", session_installation, nprime)]
    previous = previous_release_archive(tmp_path_factory.mktemp("previous"))
    if previous is not None:
        pairings.append(("previous-release", previous, session_installation))
    return pairings


@pytest.fixture
def pairings(tmp_path: Path, builds: list[tuple[str, Path, Path]], redis_image: None) -> Iterator[list[_Pairing]]:
    del redis_image
    opened = [_Pairing(tmp_path / label, old, new) for label, old, new in builds]
    yield opened
    leftovers = [line for pairing in opened for line in pairing.close()]
    assert not leftovers, "Projects remain in a cross-version registry:\n" + "\n".join(leftovers)


@previous_release_runs_here
def test_x1_the_new_build_resumes_and_releases_and_the_old_build_releases_its_own(pairings: list[_Pairing]) -> None:
    for pairing in pairings:
        pairing.install(pairing.old)
        for name in ("moved", "kept"):
            pairing.started(name, "retain")
            assert pairing.report("done", name)["state"] == "stopped-retained"
            pairing.exited(name)
        old_release = pairing.started("kept-release", "release:kept", project="kept", go=False)
        assert old_release["version"] == pairing.old_version

        pairing.install(pairing.new)
        resumed = pairing.started("moved-resume", "resume:moved", project="moved")
        pairing.signal("go", "kept-release")

        assert resumed["version"] == pairing.new_version
        assert pairing.report("done", "moved-resume") == {
            "version": pairing.new_version,
            "resumed": "stopped-retained",
            "generation": 2,
            "state": "complete",
        }
        assert pairing.report("done", "kept-release")["state"] == "complete"  # the exempt release helper


@previous_release_runs_here
def test_x3_a_new_run_waits_for_the_old_holder_then_is_granted(pairings: list[_Pairing]) -> None:
    for pairing in pairings:
        pairing.install(pairing.old)
        pairing.started("holder", "prepare")
        assert pairing.report("entered", "holder")["version"] == pairing.old_version

        pairing.install(pairing.new)
        waiter = pairing.started("waiter", "prepare")
        pairing.queued(pairing.report("prepared", "waiter")["run_id"])  # the ordinary wait, not a refusal
        assert not (pairing.control / "entered-waiter.json").exists()
        pairing.signal("release", "holder")

        assert pairing.report("done", "holder")["state"] == "complete"
        assert pairing.report("entered", "waiter")["version"] == waiter["version"] == pairing.new_version
        assert pairing.at_ns("entered", "waiter") > pairing.at_ns("leaving", "holder")  # granted only after it left
        pairing.signal("release", "waiter")
        assert pairing.report("done", "waiter")["state"] == "complete"


@previous_release_runs_here
def test_x4_rollback_refuses_the_newer_build_and_admits_the_older(pairings: list[_Pairing]) -> None:
    for pairing in pairings:
        pairing.install(pairing.old)
        pairing.install(pairing.new)
        newer = pairing.started("newer", "prepare", go=False)
        assert newer["version"] == pairing.new_version

        pairing.install(pairing.old)  # rollback within a format: the old version folder becomes current again
        pairing.signal("go", "newer")
        older = pairing.started("older", "prepare")

        assert pairing.report("refused", "newer") == {
            "version": pairing.new_version,
            "category": "incompatible_state",
            "reason": "toolkit-upgraded",
        }
        assert older["version"] == pairing.old_version
        assert pairing.report("entered", "older")["version"] == pairing.old_version
        pairing.signal("release", "older")
        assert pairing.report("done", "older")["state"] == "complete"


@previous_release_runs_here
def test_x5_the_old_guardian_waits_while_the_new_build_recovers_a_dead_holder(pairings: list[_Pairing]) -> None:
    for pairing in pairings:
        pairing.install(pairing.new)  # the build stays installed beside the old one, which is current
        pairing.install(pairing.old)
        pairing.started("holder", "prepare")
        holder_id = pairing.report("entered", "holder")["run_id"]
        waiter = pairing.started("waiter", "prepare")
        pairing.queued(pairing.report("prepared", "waiter")["run_id"])
        assert waiter["version"] == pairing.old_version

        record = pairing.root / "state" / "runs" / f"{holder_id}.json"
        guardian = json.loads(record.read_text())["guardian"]["pid"]
        os.kill(guardian, signal.SIGKILL)  # both owners gone, inside the old guardian's own 30 s grace
        pairing.processes["holder"].kill()
        pairing.processes["holder"].wait(timeout=30)
        reap_orphan(guardian)
        # A queued guardian of either version claims a dead holder at first sight, so the build's command has nothing to
        # start; waiting for the claim keeps that from racing the command.
        account = pairing.root / "state" / "test-admission"
        deadline = time.monotonic() + 60
        while queue_entries(account)[holder_id].takeover is None:
            assert time.monotonic() < deadline, "the queued guardian did not claim the dead holder"
            time.sleep(0.05)

        refusal = _toolkit(
            ["cleanup", "project"],
            {"project_root": str((pairing.tmp / "projects" / "holder").resolve())},
            pairing.tmp / "projects" / "holder",
            archive=pairing.root / pairing.new_version / ARCHIVE_NAME,
            execute=True,
        )

        assert refusal["error"]["category"] == "incompatible_state"
        spawned = refusal["error"]["context"]["recovery_trigger"]["spawned"]
        assert spawned == 0
        assert [item["run_id"] for item in refusal["error"]["context"]["runs"]] == [holder_id]
        assert pairing.report("entered", "waiter", timeout=240)["version"] == pairing.old_version
        recovered = json.loads(record.read_text())
        assert recovered["recovery_outcome"] == "complete"  # the waiter was granted only after the build's worker
        pairing.signal("release", "waiter")
        assert pairing.report("done", "waiter")["state"] == "complete"


def test_one_executing_command_recovers_a_killed_run_with_nothing_queued(pairings: list[_Pairing]) -> None:
    """As the field saw it: real deaths, real containers, nothing queued, one real command and nothing after it."""
    pairing = pairings[0]
    pairing.install(pairing.new)
    pairing.started("holder", "compose")
    holder_id = pairing.report("entered", "holder")["run_id"]
    record = pairing.root / "state" / "runs" / f"{holder_id}.json"

    def stack_is_up() -> bool:
        data = json.loads(record.read_text())
        return bool(data["owned_resources"]["containers"]) and all(
            item["state"] not in {"pending", "unresolved"} for item in data["docker_receipts"].values()
        )

    deadline = time.monotonic() + 120
    while not stack_is_up():
        assert time.monotonic() < deadline, "the holder's stack did not come up"
        time.sleep(0.2)
    guardian = json.loads(record.read_text())["guardian"]["pid"]
    os.kill(guardian, signal.SIGKILL)
    pairing.processes["holder"].kill()
    pairing.processes["holder"].wait(timeout=30)
    reap_orphan(guardian)
    project = pairing.tmp / "projects" / "holder"
    commanded = time.monotonic()

    refusal = _toolkit(
        ["cleanup", "project"],
        {"project_root": str(project.resolve())},
        pairing.tmp / "operator",
        archive=pairing.root / pairing.new_version / ARCHIVE_NAME,
        execute=True,
    )

    assert refusal["error"]["context"]["recovery_trigger"]["spawned"] == 1
    assert [item["run_id"] for item in refusal["error"]["context"]["runs"]] == [holder_id]
    deadline = commanded + 30 + 30 + 600  # metadata wait, grace, and more than the run's recovery deadline
    while json.loads(record.read_text())["recovery_outcome"] != "complete":
        assert time.monotonic() < deadline, "no recovery without a further command"
        time.sleep(0.5)
    assert time.monotonic() - commanded >= 30  # the grace held
    recovered = json.loads(record.read_text())
    assert recovered["state"] == "complete" and recovered["quiescent"] is True
    assert all(item["state"] == "removed" for item in recovered["owned_resources"]["containers"].values())
    assert holder_id not in queue_phases(pairing.root / "state" / "test-admission")


# ---- shareable runs: limit-1 identity, the version gate and the older installer ----------------------------

_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
_HEX = re.compile(r"(?:sha256:)?[0-9a-f]{64}")
# Keys whose 64-hex values name an identity (Docker objects, projects, daemons): normalized bijectively.
_IDENTITY_KEYS = frozenset(
    {"project_id", "daemon_id", "id", "Id", "container_id", "network_id", "image_id", "identifier"}
)
# Digests whose preimage contains a masked value; every other digest compares raw. Each entry names that value.
DERIVED_DIGESTS = {
    "artifact.sha256": "the archive digest: its preimage is the archive, which embeds the toolkit version",
    "config_sha256": "the rendered Compose config: its preimage embeds the run-scoped project name and absolute paths",
    "source_identity": "the source digest: its preimage is the installed archive, which embeds the toolkit version",
    "descriptor_sha256": "the Docker authority descriptor: its preimage embeds the run's private-root paths",
    "dispatcher_sha256": "the dispatcher script: its preimage embeds the private-root path and the archive path",
}
_RUN_SCOPED = re.compile(r"dtt-[0-9a-f]{12}-[0-9a-f]{12}")  # compose project and builder names (project and run ids)
_PATH_KEYS = ("private_root", "_path")  # paths carrying the run's project-id directory
_WALL_CLOCK = frozenset({"created", "started_at", "finished_at"})  # Docker's wall-clock evidence


class _Projection:
    """Normalize one version's checkpoints together: identities bijectively by first appearance, classes masked.

    UUIDs, 64-hex Docker and project identities and process IDs map to numbered placeholders in a canonical traversal,
    so equal identities stay equal and different ones stay different. A 64-hex value is an identity only under a key in
    ``_IDENTITY_KEYS`` or as a map key; a digest is masked only when ``DERIVED_DIGESTS`` names it (the masked value in
    its preimage); every other digest compares raw. Absolute paths under the test root, ticks, clock origins, live host
    telemetry and the toolkit version are masked by class. Every other field compares exactly.
    """

    def __init__(self, root: Path, version: str) -> None:
        self.root, self.version = str(root), version
        self.tables: dict[str, dict[str, str]] = {"uuid": {}, "hex": {}, "pid": {}, "name": {}}
        self.hex_paths: set[str] = set()

    def _identity(self, kind: str, value: str) -> str:
        table = self.tables[kind]
        return table.setdefault(value, f"<{kind}:{len(table) + 1}>")

    def _string(self, value: str, key: str | None, *, identity: bool = False) -> str:
        if value == self.version:
            return "<version>"
        value = value.replace(self.root, "<ROOT>")
        value = _UUID.sub(lambda found: self._identity("uuid", found.group(0)), value)
        if key in DERIVED_DIGESTS:
            return f"<derived:{key}>"
        value = _RUN_SCOPED.sub(lambda found: self._identity("name", found.group(0)), value)
        if identity or key in _IDENTITY_KEYS or (key is not None and key.endswith(_PATH_KEYS)):
            return _HEX.sub(lambda found: self._identity("hex", found.group(0)), value)
        if _HEX.search(value):
            self.hex_paths.add(str(key))
        return value

    def __call__(self, value: object, key: str | None = None) -> object:
        # live host and daemon telemetry: two runs never read the same page counters, and another project's containers
        # on a shared daemon are its external limits (not created by this test); every live counter of a host sample is
        # masked, not only the ones seen moving (swapout_bytes grows whenever the host swaps between the two runs)
        if key in {"raw", "headroom_bytes", "swap_used_bytes", "swapout_bytes", "external_limits"}:
            return f"<{key}>"
        if key == "readiness":  # the approved protocol bumps (readiness/3, /4): the one intended difference
            return "<readiness-protocol>"
        if key == "artifact" and isinstance(value, dict) and "sha256" in value:  # masked only where present
            value = {**value, "sha256": "<derived:artifact.sha256>"}
        if key in _WALL_CLOCK:
            return f"<{key}>"
        if isinstance(value, dict):
            # A map keyed by identities is ordered by the shape of its values, never by the identity values themselves.
            keys = sorted(
                value, key=lambda k: (_shape(value[k]), k) if _UUID.fullmatch(k) or _HEX.fullmatch(k) else ("", k)
            )
            return {self._string(k, None, identity=True): self(value[k], k) for k in keys}
        if isinstance(value, list) and value and all(isinstance(item, str) and _HEX.fullmatch(item) for item in value):
            return sorted(self._string(item, key, identity=True) for item in value)  # a set of identities, raw-ordered
        if isinstance(value, list):
            return [self(item, key) for item in value]
        if key is not None and (key.endswith("_ns") or key in {"origin", "release_origin", "birth", "audit_token"}):
            return f"<{key}>"
        if key == "pid" and type(value) is int:
            return self._identity("pid", str(value))
        if isinstance(value, str):
            return self._string(value, key)
        return value


def _shape(value: object) -> str:
    """Canonical JSON of a value with every identity replaced by its class: an order that never reads identities."""
    text = json.dumps(value, sort_keys=True)
    return _HEX.sub("<hex>", _UUID.sub("<uuid>", _RUN_SCOPED.sub("<name>", text)))


def _differences(left: object, right: object, path: str = "$") -> list[str]:
    if isinstance(left, dict) and isinstance(right, dict):
        found = [f"{path}.{key}" for key in sorted(set(left) ^ set(right))]
        for key in sorted(set(left) & set(right)):
            found += _differences(left[key], right[key], f"{path}.{key}")
        return found
    if isinstance(left, list) and isinstance(right, list) and len(left) == len(right):
        return [
            item
            for index, pair in enumerate(zip(left, right, strict=True))
            for item in _differences(*pair, f"{path}[{index}]")
        ]
    return [] if left == right else [path]


def _intended_differences(previous: str, before: dict, after: dict) -> list[str]:
    """Return the differences between the releases, less a defect of ``previous`` that this release fixes.

    0.7.1 read a loopback ``tcp://`` daemon (the CI host's) as a remote one; 0.7.2 accounts it as this machine's.
    """
    found = _differences(before, after)
    if previous != "0.7.1":
        return found

    def at(document: dict, path: str) -> object:
        return reduce(lambda node, key: node[key], path.split(".")[1:], document)

    return [
        path
        for path in found
        if not (path.endswith(".capacity.topology") and (at(before, path), at(after, path)) == ("remote", "native"))
    ]


def _limit_one_checkpoints(pairing: _Pairing, archive: Path) -> object:
    """Run H (holds) and Q (queued behind H) at limit 1 on ``archive`` alone; return the projected checkpoints."""
    pairing.install(archive, parallel_runs=1)  # declared: the default is auto, and an older release ignores it
    state = pairing.root / "state"
    account = state / "test-admission"

    def record(run_id: str) -> dict:
        return json.loads((state / "runs" / f"{run_id}.json").read_text())

    def queue() -> dict:
        return json.loads((account / "queue.json").read_text())

    pairing.started("holder", "compose")
    holder = pairing.report("entered", "holder")["run_id"]
    pairing.started("waiter", "prepare")
    waiter = pairing.report("prepared", "waiter")["run_id"]
    pairing.queued(waiter)
    queued = {"entry": queue()["entries"][waiter], "record": record(waiter)}
    active = record(holder)
    pairing.signal("release", "holder")
    pairing.report("done", "holder")
    holder_done = {"record": record(holder)}  # the queue is racing the waiter's reservation here: compared at (a), (d)
    pairing.report("entered", "waiter")
    pairing.signal("release", "waiter")
    pairing.report("done", "waiter")
    waiter_done = {"record": record(waiter), "queue": queue()}
    return {"a": queued, "b": active, "c": holder_done, "d": waiter_done}


def _previous(builds: list[tuple[str, Path, Path]]) -> tuple[Path, Path]:
    found = [(old, new) for label, old, new in builds if label == "previous-release"]
    assert found, "no previous release was found to compare against"
    return found[0]


def _require_disk_for_one_run(tmp_path: Path) -> None:
    """Fail with the figures unless each place the new build leases has its reserve plus one floor run free (T4).

    The new build waits on disk where the previous release does not, so a full disk would differ for a reason this
    comparison is not about.
    """
    from docker_test_toolkit.application.lifecycle import Deadline, NativeClock
    from docker_test_toolkit.application.probe import DaemonProbe
    from docker_test_toolkit.domain.policy import DISK_FLOOR_BYTES, PRIVATE_ROOT_FLOOR_BYTES, derive_reserve
    from docker_test_toolkit.factories.cli import build_docker_adapter

    account = tmp_path / "disk-precondition"
    account.mkdir(mode=0o700)
    docker = build_docker_adapter()
    clock = NativeClock()
    data_root = docker.call("info", "--format", "{{.DockerRootDir}}").strip()
    reading = DaemonProbe(account).read(
        docker,
        daemon_id=docker.observe_daemon().daemon_id,
        data_root=data_root,
        deadline=Deadline.after(clock, 30),
        clock=clock,
    )
    figure = reading.data_root
    assert figure is not None and figure.free_bytes is not None and figure.total_bytes is not None, figure
    reserve = derive_reserve(figure.total_bytes, None)
    assert figure.free_bytes >= reserve + DISK_FLOOR_BYTES, (
        f"data root: free {figure.free_bytes}, reserve {reserve}, floor {DISK_FLOOR_BYTES}"
    )
    private = os.statvfs(tmp_path)
    assert private.f_bavail * private.f_frsize >= PRIVATE_ROOT_FLOOR_BYTES, "private root below its floor"


@previous_release_runs_here
def test_limit_one_matches_previous_release(tmp_path: Path, builds: list[tuple[str, Path, Path]], redis_image: None):
    del redis_image
    _require_disk_for_one_run(tmp_path)
    old, new = _previous(builds)
    pairings = [_Pairing(tmp_path / "old", old, old), _Pairing(tmp_path / "new", new, new)]
    try:
        raws = [_limit_one_checkpoints(pairing, pairing.old) for pairing in pairings]
        projections = [_Projection(pairing.tmp, archive_version(pairing.old)) for pairing in pairings]
        before, after = (project(raw) for project, raw in zip(projections, raws, strict=True))
        (tmp_path / "before.json").write_text(json.dumps(before, indent=1, sort_keys=True))
        (tmp_path / "after.json").write_text(json.dumps(after, indent=1, sort_keys=True))
        (tmp_path / "raw-digest-keys.json").write_text(json.dumps(sorted(projections[1].hex_paths)))

        assert _intended_differences(archive_version(old), before, after) == []
        receipts = raws[1]["c"]["record"]["docker_receipts"]  # the holder's journal after its compose up
        assert any(item["kind"] == "compose" for item in receipts.values())  # the holder's compose up journaled
        assert all("files" not in item["plan"] and "scale" not in item["plan"] for item in receipts.values())

        def plant(path: str, mutate) -> None:
            raw = json.loads(json.dumps(raws[1]))
            mutate(raw)
            mutated = _Projection(pairings[1].tmp, archive_version(pairings[1].old))(raw)
            assert path in _differences(before, mutated), path

        def waiting(raw: dict) -> None:
            raw["d"]["record"]["hold_timeline"]["waiting"][0].update(reason="parallel-limit")

        def phases(raw: dict) -> None:
            raw["b"]["phases"]["tests"].update(host_process_bytes=1)

        plant("$.d.record.hold_timeline.waiting[0].reason", waiting)
        plant("$.b.sharing", lambda raw: raw["b"].update(sharing={"mode": "shared"}))
        plant("$.b.phases.tests.host_process_bytes", phases)
        plant("$.d.record.run_id", lambda raw: raw["d"]["record"].update(run_id=str(uuid.uuid4())))
        plant("$.b.artifact.sha256", lambda raw: raw["b"]["artifact"].pop("sha256"))  # a missing digest field shows
    finally:
        leftovers = [line for pairing in pairings for line in pairing.close()]
    assert not leftovers, leftovers


def _seed_shared(pairing: _Pairing, name: str) -> None:
    """Seed a positive verdict for a ``prepare`` worker's request in this root's registry.

    Both releases read the store: the previous one keys a request by its checkout folder's name, this one by the
    checkout's Git origin, so the seed holds the verdict under both keys.
    """
    from docker_test_toolkit.application import sharing
    from docker_test_toolkit.application.recovery import atomic_json, project_id_for

    project = (pairing.tmp / "projects" / name).resolve()
    (project / ".git").mkdir(parents=True, exist_ok=True)
    (project / ".git" / "config").write_text(f'[remote "origin"]\n\turl = https://example.invalid/cross/{name}.git\n')
    project_id = project_id_for(project)
    path = sharing.verdict_path(pairing.root / "state", project_id)
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    origin = sharing.repository_origin(str(project))
    assert origin is not None
    verdict = {"shareable": True, "reason": None, "run_id": "seed", "origin": "seed", "tick_ns": 0}
    keys = (project.name, origin)  # the previous release's key component, then this release's
    keys = tuple(sharing.request_key(component, "e2e", "cross-version", ()) for component in keys)
    atomic_json(path, {"rules": 1, "schema": 1, "verdicts": dict.fromkeys(keys, verdict)})


def _own_installer(archive: Path, root: Path, *arguments: str) -> subprocess.CompletedProcess:
    """Run an archive's own installer (``python -I archive install``), never this process's imported ``install``."""
    return subprocess.run(
        [str(base_python()), "-I", str(archive), "install", "--json", "--root", str(root), *arguments],
        capture_output=True,
        text=True,
        check=False,
    )


@previous_release_runs_here
def test_m1_previous_release_queued_and_holding_entries_block_join(
    tmp_path: Path, builds: list[tuple[str, Path, Path]], redis_image: None
) -> None:
    del redis_image
    old, new = _previous(builds)
    pairing = _Pairing(tmp_path / "m1", old, new)
    try:
        pairing.install(old)
        pairing.started("old-holder", "prepare")
        assert pairing.report("entered", "old-holder")["version"] == pairing.old_version
        assert _own_installer(new, pairing.root, "--parallel-runs", "2").returncode == 0
        _seed_shared(pairing, "joiner")
        pairing.started("joiner", "prepare")
        joiner = pairing.report("prepared", "joiner")["run_id"]
        pairing.queued(joiner)
        bounded = time.monotonic() + 5
        while time.monotonic() < bounded:  # never beside the previous release's holder
            assert not (pairing.control / "entered-joiner.json").exists()
            time.sleep(0.1)
        pairing.signal("release", "old-holder")
        assert pairing.report("done", "old-holder")["state"] == "complete"
        pairing.report("entered", "joiner")
        assert pairing.at_ns("entered", "joiner") > pairing.at_ns("leaving", "old-holder")
        pairing.signal("release", "joiner")
        assert pairing.report("done", "joiner")["state"] == "complete"
    finally:
        leftovers = pairing.close()
    assert not leftovers, leftovers


def test_previous_installer_refuses_two_holders_and_accepts_one(
    tmp_path: Path, builds: list[tuple[str, Path, Path]], redis_image: None
) -> None:
    del redis_image
    old, new = _previous(builds)
    pairing = _Pairing(tmp_path / "rollback", old, new)
    try:
        pairing.install(new)
        assert _own_installer(new, pairing.root, "--parallel-runs", "2").returncode == 0
        for name in ("first", "second"):
            _seed_shared(pairing, name)
            pairing.started(name, "prepare")
            pairing.report("entered", name)
        switched = _own_installer(old, pairing.root)
        if version_at_least(pairing.old_version, SHARED_QUEUE_MIN_VERSION):
            # A previous release that reads shared queues (0.6.0 on) switches back with both holders in place.
            assert switched.returncode == 0, switched.stdout + switched.stderr
            assert json.loads(switched.stdout)["result"]["holder_versions"] == [pairing.new_version] * 2
            pairing.install(new)
        else:
            assert switched.returncode != 0
            assert json.loads(switched.stdout)["error"]["context"]["reason"] == "queue-invalid"
        assert os.readlink(pairing.root / "current") == pairing.new_version
        pairing.signal("release", "second")
        assert pairing.report("done", "second")["state"] == "complete"
        accepted = _own_installer(old, pairing.root)
        assert accepted.returncode == 0, accepted.stdout + accepted.stderr
        assert os.readlink(pairing.root / "current") == pairing.old_version
        pairing.install(new)  # back to the build under test before the remaining holder leaves
        pairing.signal("release", "first")
        assert pairing.report("done", "first")["state"] == "complete"
    finally:
        leftovers = pairing.close()
    assert not leftovers, leftovers


# ---- record compatibility: the previous release reads what this one writes ---------


def _old_operator(pairing: _Pairing, workspace: Path, arguments: list[str], payload: dict) -> dict:
    """Run one operator command of the previous release, which ``current`` points to after the rollback."""
    archive = pairing.root / pairing.old_version / ARCHIVE_NAME
    return _toolkit(arguments, payload, workspace, archive=archive)


@previous_release_runs_here
def test_previous_release_loads_every_record_and_file_the_new_release_writes(
    tmp_path: Path, builds: list[tuple[str, Path, Path]], redis_image: None
) -> None:
    del redis_image
    old, new = _previous(builds)
    # The build under test carries the previous version until the release bump; a renamed copy installs beside it.
    (tmp_path / "renamed").mkdir()
    new = renamed_archive(new, tmp_path / "renamed", "9.9.9")
    pairing = _Pairing(tmp_path / "records", old, new)
    workspace = tmp_path / "operator"
    try:
        pairing.install(old)
        pairing.install(new)
        assert pairing.started("builder", "build")["version"] == "9.9.9"
        context = pairing.report("built", "builder", timeout=1200)["context"]  # a Buildx builder starts first
        pairing.signal("release", "builder")
        assert pairing.report("done", "builder")["state"] == "complete"
        record_path = Path(context["record"])
        record = json.loads(record_path.read_text())
        builders = record["owned_resources"]["builders"].values()
        images = record["owned_resources"]["images"].values()
        assert any("state_volume" in item["evidence"] for item in builders)  # the evidence additions are present
        assert any("size_bytes" in item["evidence"] for item in images)
        account = pairing.root / "state" / "test-admission"
        for name in ("retention-stamp.json", "retention-report.json", "probe-image.json"):
            assert (account / name).is_file(), name  # the new account files exist for the old build to ignore

        rollback = _own_installer(old, pairing.root)
        assert rollback.returncode == 0, rollback.stdout + rollback.stderr
        project = {"project_root": context["project_root"]}
        calls = {
            "run status": (["run", "status"], {"context": context}),
            "cleanup project": (["cleanup", "project"], project),
            "project timeline": (["project", "timeline"], project),
        }
        for label, (arguments, payload) in calls.items():
            response = _old_operator(pairing, workspace, arguments, payload)
            assert response["ok"] and response["_exit"] == 0, (label, response)
        timeline = _old_operator(pairing, workspace, ["project", "timeline"], project)["result"]
        assert context["run_id"] in json.dumps(timeline)

        original = record_path.read_bytes()
        # what a Podman run writes: its identity wherever the record names one
        docker_id = json.loads(original)["grant"]["daemon_id"]
        podman = json.loads(original.decode().replace(docker_id, "podman-" + "a" * 40))
        promoted = next(item for item in podman["docker_receipts"].values() if item["state"] == "promoted")
        image_id = next(iter(podman["owned_resources"]["images"]))
        export_id = promoted["id"][:-1] + ("0" if promoted["id"][-1] != "0" else "1")
        podman["docker_receipts"][export_id] = {
            **promoted,
            "id": export_id,
            "kind": "image-save",
            "plan": {
                "image_id": image_id,
                "archive": {
                    "schema": 1,
                    "state": "published",
                    "cleanup": "complete",
                    "path": str(Path(podman["private_root"]) / "exports" / f"{export_id}.tar"),
                    "bytes": 1,
                    "sha256": "b" * 64,
                    "layout": "docker-archive",
                    "root_digest": image_id,
                    "manifest_digest": "sha256:" + "c" * 64,
                    "config_digest": image_id,
                    "platform": {"os": "linux", "architecture": "arm64"},
                },
            },
        }
        record_path.write_text(json.dumps(podman))
        try:
            for arguments, payload in (calls["run status"], calls["project timeline"]):
                loaded = _old_operator(pairing, workspace, arguments, payload)
                assert loaded["ok"] and loaded["_exit"] == 0, (arguments, loaded)
        finally:
            record_path.write_bytes(original)

        planted = json.loads(original)
        image = next(iter(planted["owned_resources"]["images"].values()))
        image["size_bytes"] = image["evidence"]["size_bytes"]  # a top-level item key: what the evidence rule forbids
        record_path.write_text(json.dumps(planted))
        try:
            refused = _old_operator(pairing, workspace, ["run", "status"], {"context": context})
            assert not refused["ok"] and refused["_exit"] != 0, refused  # the check above is decisive
        finally:
            record_path.write_bytes(original)
        pairing.install(new)  # the build under test sweeps the root
    finally:
        leftovers = pairing.close()
    assert not leftovers, leftovers
