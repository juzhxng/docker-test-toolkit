from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
import uuid
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from pathlib import Path

import pytest
from tests.e2e.conftest import redis_id
from tests.integration.account_fixtures import (
    DOCKER_CLASSES,
    ForeignSentinels,
    foreign_snapshot,
    queue_entries,
    reap_orphan,
    require_present,
)

from docker_test_toolkit.application.recovery import Context, build_records
from docker_test_toolkit.config.installation import installation_of
from docker_test_toolkit.domain.policy import HOLDER_PHASES

pytestmark = pytest.mark.e2e
ROOT = Path(__file__).parents[2]
LOADER = ROOT / "docs" / "loader"
REDIS = "redis@sha256:becdda6c7f4b3fb42e42fd7f120bbf5c54c4caaaf16f26da24e4563d2c1f0576"

WORKER = r"""from __future__ import annotations
import importlib
import importlib.util
import json
import os
import pathlib
import signal
import subprocess
import sys
import time

project, loader_directory, installation_root, state_root, control = map(pathlib.Path, sys.argv[1:6])
domain, name, action = sys.argv[6:9]
capacity = int(sys.argv[9]) if len(sys.argv) > 9 else 1
sys.path.insert(0, str(loader_directory))
import docker_test_toolkit_loader as loader

toolkit, _archive = loader.load_toolkit((installation_root,))
identity = toolkit.toolkit_identity()
retain = action in {"retain", "resume", "release"}
services = ("cache",) if action in {"hold", "retain", "resume", "release", "foreign"} else ("fixture",)
run_spec = toolkit.RunSpec(
    project_root=project,
    entrypoint="e2e",
    purpose=f"cross-{name}",
    services=services,
    closure=services,
    profile="cross-workspace",
    phases={"tests": toolkit.PhaseBudget(1024 ** 3, 512 * 1024 ** 2, 1, 2)},
    initial_phase="tests",
    artifact=identity,
    external_resources=(toolkit.ExternalDockerResource("image", os.environ["CROSS_REDIS_ID"], "redis", "use_image"),),
    retain=retain,
    wait_seconds=120,
    **({"capacity": capacity} if capacity != 1 else {}),  # the frozen artifact may predate the field
)
if domain == "private":
    testing = importlib.import_module("docker_test_toolkit.factories.testing")
    services_factory = testing.build_test_services(state_root)
    open_run = services_factory.open_run
    open_retained = services_factory.open_retained_run
else:
    open_run = toolkit.open_run
    open_retained = toolkit.open_retained_run
locator = control / f"{name}.locator.json"
if action in {"resume", "release"}:
    handle = open_retained(run_spec, locator)
    if action == "resume":
        handle.resume()
        context = handle.docker_context
        stale = subprocess.run(
            [str(handle.private_root / "docker"), "info", "--format", "{{.ID}}"],
            env=dict(context.environment), capture_output=True, text=True, check=False,
        )
        current = subprocess.run(
            [str(context.executable), "info", "--format", "{{.ID}}"],
            env=dict(context.environment), capture_output=True, text=True, check=False,
        )
        if stale.returncode == 0 or current.returncode != 0:
            raise RuntimeError("resumed Docker authority was not rotated exactly")
        status = handle.retain("passed")
    else:
        status = handle.release()
    (control / f"done-{name}.json").write_text(json.dumps({
        "state": status.state,
        "cleanup": status.cleanup_outcome,
        "generation": status.generation,
    }))
    raise SystemExit(0)
handle = open_run(run_spec, admit_docker=True) if domain == "private" else open_run(run_spec)
prepared = {
    "artifact": identity.sha256,
    "compose_project": handle.compose_project,
    "project_id": handle.project_id,
    "run_id": handle.run_id,
}
(control / f"prepared-{name}.json").write_text(json.dumps(prepared, sort_keys=True), encoding="utf-8")
entered = False
foreign_id = None
try:
    handle.enter()
    entered = True
    context = handle.docker_context
    if action in {"crash", "takeover"}:
        created = subprocess.run(
            [str(context.executable), "network", "create"], env=dict(context.environment),
            capture_output=True, text=True, check=True,
        )
        entered_record = {**prepared, "network_id": created.stdout.strip()}
        (control / f"entered-{name}.json").write_text(json.dumps(entered_record, sort_keys=True), encoding="utf-8")
        if action == "takeover":
            # Die only when the test says so (another project is queued by then): the guardian first, then this process.
            deadline = time.monotonic() + 120
            while not (control / f"die-{name}").exists() and time.monotonic() < deadline:
                time.sleep(0.05)
            guardian = json.loads(handle._context.record.read_text())["guardian"]["pid"]
            os.kill(guardian, signal.SIGKILL)
        os._exit(17)
    endpoint = None
    if action in {"hold", "retain", "foreign"}:
        compose = handle.private_root / "compose.yml"
        compose.write_text(
            "services:\n"
            "  cache:\n"
            f"    image: {os.environ['CROSS_REDIS_IMAGE']}\n"
            "    mem_limit: 1g\n"
            "    cpus: 1\n"
            + ("    tmpfs: [/data]\n" if capacity == 2 else "")  # the image's VOLUME would be an anonymous volume
            + "    command: [redis-server, --save, '', --appendonly, 'no']\n"
            "    ports: ['127.0.0.1::6379']\n"
            "    healthcheck:\n"
            "      test: [CMD, redis-cli, ping]\n"
            "      interval: 1s\n"
            "      timeout: 1s\n"
            "      retries: 30\n",
            encoding="utf-8",
        )
        if action == "foreign":
            real_docker = os.environ["CROSS_REAL_DOCKER"]
            foreign_name = f"{handle.compose_project}-cache-1"
            created = subprocess.run(
                [real_docker, "run", "--detach", "--name", foreign_name,
                 "--label", f"com.docker.compose.project={handle.compose_project}",
                 "--label", "com.docker.compose.service=cache", "--memory", "1g",
                 os.environ["CROSS_REDIS_IMAGE"],
                 "redis-server", "--save", "", "--appendonly", "no"],
                capture_output=True, text=True, check=True,
            )
            foreign_id = created.stdout.strip()
            refused = subprocess.run(
                [str(context.executable), "compose", "--file", str(compose), "up", "--detach"],
                cwd=project, env=dict(context.environment), capture_output=True, text=True, check=False,
            )
            if refused.returncode == 0:
                raise RuntimeError("foreign same-name Compose object was accepted")
            subprocess.run([real_docker, "container", "rm", "-f", foreign_id], check=True, capture_output=True)
            foreign_id = None
            status = handle.leave("passed")
            entered = False
            (control / f"done-{name}.json").write_text(json.dumps({
                "state": status.state, "cleanup": status.cleanup_outcome, "refused": True,
            }))
            raise SystemExit(0)
        started = subprocess.run(
            [str(context.executable), "compose", "--file", str(compose), "up", "--detach", "--wait",
             "--wait-timeout", "60"],
            cwd=project, env=dict(context.environment), capture_output=True, text=True, check=False,
        )
        if started.returncode:
            raise RuntimeError(started.stderr)
        endpoint = handle.publish_endpoint("cache", "redis", 6379)
    entered_record = {
        **prepared,
        "endpoint": None if endpoint is None else {"host": endpoint.host, "port": endpoint.port},
    }
    (control / f"entered-{name}.json").write_text(json.dumps(entered_record, sort_keys=True), encoding="utf-8")
    if action in {"hold", "retain", "idle"}:
        deadline = time.monotonic() + (600 if action == "idle" else 120)  # an idle holder outlives a takeover
        while not (control / f"release-{name}").exists() and time.monotonic() < deadline:
            time.sleep(0.05)
        if not (control / f"release-{name}").exists():
            raise RuntimeError("release signal timed out")
    status = handle.retain("passed") if action == "retain" else handle.leave("passed")
    entered = False
    if action == "retain":
        handle.write_retained_locator(locator)
    (control / f"done-{name}.json").write_text(json.dumps({"state": status.state, "cleanup": status.cleanup_outcome}))
finally:
    if foreign_id is not None:
        subprocess.run(
            [os.environ["CROSS_REAL_DOCKER"], "container", "rm", "-f", foreign_id],
            check=False, capture_output=True,
        )
    if entered:
        handle.leave("failed")
    elif handle.status().state == "prepared":
        handle.cancel()
"""


def _installation_root(archive: Path) -> Path:
    installation = installation_of(archive)
    assert installation is not None
    return installation.root


def _base_python() -> str:
    return str(Path(getattr(sys, "_base_executable", sys.executable)).resolve())


def _stage(tmp_path: Path, name: str, *, root: Path) -> Path:
    """A project with its worker; the worker loads the toolkit installed at ``root`` through the reference loader."""
    project = tmp_path / name
    project.mkdir()
    (project / "worker.py").write_text(WORKER, encoding="utf-8")
    (project / "installation-root").write_text(str(root), encoding="utf-8")
    return project


def _worker(
    project: Path,
    state: Path,
    control: Path,
    domain: str,
    name: str,
    action: str,
    capacity: int = 1,
) -> subprocess.Popen[str]:
    docker = shutil.which("docker")
    assert docker is not None
    temporary = project / "temporary"
    temporary.mkdir(exist_ok=True)
    return subprocess.Popen(
        [
            _base_python(),
            "-I",
            str(project / "worker.py"),
            str(project),
            str(LOADER),
            (project / "installation-root").read_text(encoding="utf-8"),
            str(state),
            str(control),
            domain,
            name,
            action,
            str(capacity),
        ],
        cwd=project,
        env={
            "CROSS_REAL_DOCKER": docker,
            "CROSS_REDIS_ID": redis_id(),
            "CROSS_REDIS_IMAGE": REDIS,
            "PATH": os.environ.get("PATH", ""),
            "TMPDIR": str(temporary),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _wait(path: Path, timeout: float = 120) -> dict[str, object]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
        time.sleep(0.05)
    raise AssertionError(f"timed out waiting for {path}")


def _finish(process: subprocess.Popen[str], timeout: float = 120) -> None:
    stdout, stderr = process.communicate(timeout=timeout)
    assert process.returncode == 0, f"stdout={stdout}\nstderr={stderr}"


def test_unrelated_workspaces_share_fifo_but_isolate_projects_retention_and_crash_recovery(
    tmp_path: Path, toolkit_domain: str, redis_image: None, session_installation: Path
) -> None:
    del redis_image
    control = tmp_path / "control"
    control.mkdir()
    state = tmp_path / "shared-state"
    first_project = _stage(tmp_path, "workspace-one", root=_installation_root(session_installation))
    second_project = _stage(tmp_path, "workspace-two", root=_installation_root(session_installation))

    first = _worker(first_project, state, control, toolkit_domain, "first", "hold")
    first_entered = _wait(control / "entered-first.json")
    second = _worker(second_project, state, control, toolkit_domain, "second", "retain")
    _wait(control / "prepared-second.json")
    bounded = time.monotonic() + 1
    while time.monotonic() < bounded:
        assert not (control / "entered-second.json").exists()
        time.sleep(0.05)

    (control / "release-first").touch()
    _finish(first)
    second_entered = _wait(control / "entered-second.json")
    assert first_entered["project_id"] != second_entered["project_id"]
    assert first_entered["compose_project"] != second_entered["compose_project"]
    assert first_entered["endpoint"] and second_entered["endpoint"]
    (control / "release-second").touch()
    _finish(second)
    assert _wait(control / "done-second.json")["state"] == "stopped-retained"

    resume = _worker(second_project, state, control, toolkit_domain, "second", "resume")
    _finish(resume)
    resumed = _wait(control / "done-second.json")
    assert resumed["state"] == "stopped-retained"
    assert resumed["generation"] == 2

    release = _worker(second_project, state, control, toolkit_domain, "second", "release")
    _finish(release)
    assert _wait(control / "done-second.json")["state"] == "complete"

    crash_project = _stage(tmp_path, "workspace-crash", root=_installation_root(session_installation))
    follower_project = _stage(tmp_path, "workspace-follower", root=_installation_root(session_installation))
    crashed = _worker(crash_project, state, control, toolkit_domain, "crash", "crash")
    crash_evidence = _wait(control / "entered-crash.json")
    crashed.communicate(timeout=30)
    assert crashed.returncode == 17
    follower = _worker(follower_project, state, control, toolkit_domain, "follower", "once")
    _finish(follower)
    inspect = subprocess.run(
        [shutil.which("docker") or "docker", "network", "inspect", str(crash_evidence["network_id"])],
        check=False,
        capture_output=True,
        text=True,
    )
    assert inspect.returncode != 0


def test_foreign_same_name_and_compose_label_is_refused_without_adoption(
    tmp_path: Path, toolkit_domain: str, redis_image: None, session_installation: Path
) -> None:
    del redis_image
    control = tmp_path / "control"
    control.mkdir()
    project = _stage(tmp_path, "workspace-foreign", root=_installation_root(session_installation))

    process = _worker(project, tmp_path / "state", control, toolkit_domain, "foreign", "foreign")
    _finish(process)

    assert _wait(control / "done-foreign.json") == {"state": "complete", "cleanup": "complete", "refused": True}


# --- Two real projects in one account: a dead holder is taken over while the other waits ---


class DockerCliInventory:
    """Exact Docker objects of the foreign project F, read through the real CLI.

    Only fields that a bystander can never legitimately change are returned (a container's uptime, a cache record's
    "last used" text and a builder's status are left out), so two equal digests mean the object was not touched.
    F's build history and cache records live on F's own ``builder``: every record there is F's, so none is inferred.
    """

    def __init__(self, docker: str, builder: str) -> None:
        self._docker = docker
        self._builder = builder

    def call(self, *arguments: str) -> str:
        return subprocess.run([self._docker, *arguments], check=True, capture_output=True, text=True).stdout

    def inspect(self, kind: str, identity: str) -> object:
        try:
            return self._inspect(kind, identity)
        except (subprocess.CalledProcessError, KeyError, IndexError):
            return None  # the object is gone

    def _inspect(self, kind: str, identity: str) -> object:
        if kind == "container":
            raw = json.loads(self.call("container", "inspect", identity))[0]
            return {key: raw[key] for key in ("Id", "Name", "Image", "HostConfig")} | {
                "Labels": raw["Config"]["Labels"],
                "Status": raw["State"]["Status"],
                "StartedAt": raw["State"]["StartedAt"],
                "Networks": sorted(raw["NetworkSettings"]["Networks"]),
            }
        if kind == "network":
            raw = json.loads(self.call("network", "inspect", identity))[0]
            return {key: raw[key] for key in ("Id", "Name", "Driver", "Labels", "Created")} | {
                "Containers": sorted(raw["Containers"])
            }
        if kind == "volume":
            raw = json.loads(self.call("volume", "inspect", identity))[0]
            return {key: raw[key] for key in ("Name", "Driver", "Labels", "CreatedAt")}
        if kind == "image":
            raw = json.loads(self.call("image", "inspect", identity))[0]
            return {"Id": raw["Id"]}  # the image is shared and its other tags are not F's; F's own tag is the identity
        if kind == "builder":
            rows = [json.loads(line) for line in self.call("buildx", "ls", "--format", "json").splitlines() if line]
            row = next((row for row in rows if row["Name"] == identity), None)
            return None if row is None else {
                "Name": row["Name"], "Driver": row["Driver"], "Nodes": [node["Name"] for node in row["Nodes"]]
            }  # fmt: skip
        records = build_records(self, kind.replace("-", "_"), self._builder)
        row = records[identity]
        stable = ("ref", "name", "status", "completed_steps", "total_steps", "created_at")  # a history record
        cache = ("ID", "CreatedAt", "Type", "Size", "Parents", "Description", "Mutable", "Shared")  # a cache record
        return {key: row[key] for key in (stable if kind == "build-history" else cache)}


@contextmanager
def _foreign_project(
    tmp_path: Path, state: Path, control: Path, domain: str, *, root: Path
) -> Iterator[tuple[ForeignSentinels, DockerCliInventory]]:
    """A bystander project F with one sentinel in every protected class, created for real and removed exactly after.

    F is a real toolkit project that completed one run (its project record, its terminal run record and a temporary
    root with a marker file); its Docker sentinels carry F's ownership label, so a recovery that inferred ownership from
    labels or names would reach them. F builds on a Buildx builder of its own, so the build history and cache records it
    protects are all F's by construction. Every named object is registered for removal before it is created, and
    removal is verified: whatever remains is reported instead of being left silently.
    """
    docker = shutil.which("docker")
    assert docker is not None
    project = _stage(tmp_path, "workspace-bystander", root=root)
    bystander = _worker(project, state, control, domain, "bystander", "once")
    try:
        _finish(bystander)
    finally:
        _reap(bystander)
    prepared = json.loads((control / "prepared-bystander.json").read_text())
    registry, _account = _domain_directories(state, domain)
    context = Context(project.resolve(), registry)
    (project / "temporary" / "sentinel.txt").write_text("a stranger's temporary root")
    label, suffix = context.label, uuid.uuid4().hex[:12]
    builder, tag = f"fsent-builder-{suffix}", f"fsent/image:{suffix}"
    inventory = DockerCliInventory(docker, builder)
    identities: dict[str, list[str]] = {name: [] for name in DOCKER_CLASSES}
    cleanup = [
        ("container", "rm", "-f", f"fsent-{suffix}"),
        ("network", "rm", f"fsent-net-{suffix}"),
        ("volume", "rm", f"fsent-vol-{suffix}"),
        ("image", "rm", tag),
        ("buildx", "rm", builder),  # removes the builder's own container and state: its history and cache go with it
    ]
    try:
        identities["container"].append(
            inventory.call(
                "run", "--detach", "--name", f"fsent-{suffix}", "--label", label, "--memory", "256m", REDIS,
                "redis-server", "--save", "", "--appendonly", "no",
            ).strip()
        )  # fmt: skip
        identities["network"].append(
            inventory.call("network", "create", "--label", label, f"fsent-net-{suffix}").strip()
        )
        identities["volume"].append(inventory.call("volume", "create", "--label", label, f"fsent-vol-{suffix}").strip())
        inventory.call("image", "tag", redis_id(), tag)
        identities["image"].append(tag)
        # A running container without a memory limit is unknown external capacity: the real account's admission refuses
        # every project while it runs (``assess_capacity``), so F's builder container is bounded like F's other one.
        inventory.call(
            "buildx", "create", "--name", builder, "--driver", "docker-container", "--driver-opt", "memory=256m"
        )
        identities["builder"].append(builder)
        context_directory = tmp_path / "foreign-build"
        context_directory.mkdir()
        (context_directory / "Dockerfile").write_text("FROM scratch\nCOPY marker /marker\n")
        (context_directory / "marker").write_text(f"foreign build {suffix}")
        inventory.call(
            "buildx", "build", "--builder", builder, "--output", "type=cacheonly", "--label", label,
            str(context_directory),
        )  # fmt: skip
        limit = inventory.call(
            "container", "inspect", "--format", "{{.HostConfig.Memory}}", f"buildx_buildkit_{builder}0"
        )
        assert int(limit) > 0, "F's builder container runs without a memory limit and would make admission refuse"
        identities["build-history"].extend(build_records(inventory, "build_history", builder))
        identities["build-cache"].extend(build_records(inventory, "build_cache", builder))  # every record, not one
        assert len(identities["build-history"]) == 1, "F's own builder holds exactly F's one build"
        assert identities["build-cache"], "the foreign build left no cache record"
        yield (
            ForeignSentinels(
                project=context,
                run_records=[registry / "runs" / f"{prepared['run_id']}.json"],
                temporary_root=project / "temporary",
                docker=identities,
            ),
            inventory,
        )
    finally:
        for command in reversed(cleanup):
            subprocess.run([docker, *command], check=False, capture_output=True)
        inventory.call("info", "--format", "{{.ID}}")  # an unreachable daemon would make every object read as gone
        left = [(kind, name) for kind, names in identities.items() for name in names if inventory.inspect(kind, name)]
        assert not left, f"the foreign sentinel cleanup left {left}"


def _domain_directories(state: Path, domain: str) -> tuple[Path, Path]:
    """The registry root (projects and runs) and the admission account directory of the domain under test."""
    assert domain == "private", "toolkit E2E runs on private test state; the real account belongs to an installation"
    return state / "registry", state / "admission"


def _reap(process: subprocess.Popen[str]) -> None:
    """Kill a helper child that still runs and reap it, so a failed test never leaves it behind."""
    if process.poll() is None:
        process.kill()
    process.communicate(timeout=10)


def _wait_queued_behind(account: Path, holder: str, waiter: str, timeout: float = 60) -> None:
    """Block until the account queue shows ``waiter`` queued behind ``holder``, which holds the slot."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        entries = queue_entries(account)
        if holder in entries and waiter in entries:
            assert entries[holder].phase in HOLDER_PHASES and entries[waiter].phase == "queued"
            assert entries[holder].ticket < entries[waiter].ticket  # FIFO: the holder came first
            return
        time.sleep(0.05)
    raise AssertionError("the waiting project never queued behind the holder")


def _recovered(registry: Path, run_id: str) -> bool:
    record = json.loads((registry / "runs" / f"{run_id}.json").read_text())
    return (record["state"], record["recovery_outcome"]) == ("complete", "complete")


def _network_gone(network_id: str) -> bool:
    """Whether Docker lists no network with this id; an unreachable daemon raises instead of reading as "gone"."""
    listed = subprocess.run(
        [shutil.which("docker") or "docker", "network", "ls", "--no-trunc", "--quiet", "--filter", f"id={network_id}"],
        check=True,
        capture_output=True,
        text=True,
    )
    return not listed.stdout.strip()


def _admission_follows_recovery(
    account: Path, registry: Path, dead: str, waiter: str, network_id: str, until: Path, timeout: float
) -> None:
    """Watch the account queue until ``until`` exists: ``waiter`` holds the slot only after ``dead`` is recovered.

    Every poll is one consistent snapshot of the queue (taken under its metadata lock). Recovery publishes the final
    record before the entry leaves the queue, so the dead entry is never absent while its record is not final, and
    whenever the waiter holds the slot the dead entry is gone, its record is final and its network is removed. A grant
    that overtook the recovery would be seen, because the recovery lasts seconds.
    """
    deadline = time.monotonic() + timeout
    queued_behind_the_dead = False
    while not until.exists():
        assert time.monotonic() < deadline, f"timed out waiting for {until}"
        entries = queue_entries(account)
        if dead not in entries:
            assert _recovered(registry, dead), "the dead holder's entry left the queue before its record was final"
        if waiter in entries and entries[waiter].phase in HOLDER_PHASES:
            assert dead not in entries, "the waiter held the slot beside the dead holder"
            assert _network_gone(network_id), "the waiter held the slot before the dead holder's network was removed"
        elif dead in entries and waiter in entries:
            queued_behind_the_dead = True
        time.sleep(0.05)
    assert queued_behind_the_dead, "the observer never saw the waiter queued behind the dead holder"
    assert dead not in queue_entries(account), "the dead holder's entry remained after the waiter entered"
    assert _recovered(registry, dead), "the waiter entered before the dead holder was recovered"
    assert _network_gone(network_id), "the waiter entered before the dead holder's network was removed"


def _snapshot_is_sensitive(
    stranger: ForeignSentinels, inventory: DockerCliInventory, before: dict[str, dict[str, str]]
) -> None:
    """The real projection must be able to fail: change one real container setting and one image tag, then restore."""
    container, tag = stranger.docker["container"][0], stranger.docker["image"][0]

    def changed() -> list[str]:
        after = foreign_snapshot(stranger, inventory)
        return [name for name in DOCKER_CLASSES if after[name] != before[name]]

    inventory.call("update", "--restart", "on-failure:3", container)  # a configuration field, not an identity field
    assert changed() == ["container"]
    inventory.call("update", "--restart", "no", container)
    inventory.call("image", "rm", tag)  # untags only: the image itself is still referenced
    assert changed() == ["image"]
    inventory.call("image", "tag", redis_id(), tag)
    assert foreign_snapshot(stranger, inventory) == before  # restored exactly


def test_dead_holder_is_taken_over_while_the_waiting_project_completes(
    tmp_path: Path, toolkit_domain: str, redis_image: None, session_installation: Path
) -> None:
    """kill one owner while another project queues; the toolkit recovers it; both projects run again."""
    del redis_image
    staged = {"root": _installation_root(session_installation)}
    control = tmp_path / "control"
    control.mkdir()
    state = tmp_path / "shared-state"
    registry, account = _domain_directories(state, toolkit_domain)
    holder_project = _stage(tmp_path, "workspace-holder", **staged)
    waiting_project = _stage(tmp_path, "workspace-waiting", **staged)

    with (
        _foreign_project(tmp_path, state, control, toolkit_domain, **staged) as (stranger, inventory),
        ExitStack() as children,  # reaped before the foreign sentinels are removed
    ):

        def start(project: Path, name: str, action: str) -> subprocess.Popen[str]:
            process = _worker(project, state, control, toolkit_domain, name, action)
            children.callback(_reap, process)
            return process

        before = foreign_snapshot(stranger, inventory)  # every protected class holds a sentinel, or this raises
        require_present(before)  # and each sentinel really exists: an absent one would digest the same twice
        assert foreign_snapshot(stranger, inventory) == before  # the digests are stable while nobody acts
        _snapshot_is_sensitive(stranger, inventory, before)

        holder = start(holder_project, "holder", "takeover")
        holder_entered = _wait(control / "entered-holder.json")
        children.callback(  # a failed test must not leave the killed holder's network behind; a no-op once recovered
            subprocess.run,
            [shutil.which("docker") or "docker", "network", "rm", str(holder_entered["network_id"])],
            check=False,
            capture_output=True,
        )
        waiting = start(waiting_project, "waiting", "once")
        waiting_prepared = _wait(control / "prepared-waiting.json")
        holder_run, waiting_run = str(holder_entered["run_id"]), str(waiting_prepared["run_id"])
        _wait_queued_behind(account, holder_run, waiting_run)  # the waiting project is in line when the holder dies
        assert not (control / "entered-waiting.json").exists()  # the slot is the holder's
        waiting_record = json.loads((registry / "runs" / f"{waiting_run}.json").read_text())

        (control / "die-holder").touch()  # the guardian is killed, then the requester: both owners are gone
        holder.communicate(timeout=30)
        assert holder.returncode == 17
        reap_orphan(json.loads((registry / "runs" / f"{holder_run}.json").read_text())["guardian"]["pid"])
        # Its guardian detects the dead holder, a worker recovers it, then the waiting project is granted.
        _admission_follows_recovery(
            account,
            registry,
            holder_run,
            waiting_run,
            str(holder_entered["network_id"]),
            control / "entered-waiting.json",
            timeout=240,
        )
        _finish(waiting, timeout=240)
        waiting_entered = _wait(control / "entered-waiting.json")

        recovered = json.loads((registry / "runs" / f"{holder_run}.json").read_text())
        assert (recovered["state"], recovered["recovery_outcome"]) == ("complete", "complete")
        assert (
            recovered["recovery_actor"] is not None and recovered["leave_ack"] is None
        )  # toolkit recovery, not a leave
        # The recoverer is neither the queued guardian (inline recovery) nor its requester: a detached worker.
        assert recovered["recovery_actor"]["pid"] not in (waiting.pid, waiting_record["guardian"]["pid"])
        assert holder_entered["project_id"] != waiting_entered["project_id"]

        again = [
            start(holder_project, "holder-again", "once"),
            start(waiting_project, "waiting-again", "once"),
        ]
        for process in again:
            _finish(process, timeout=240)

        after = foreign_snapshot(stranger, inventory)
        assert after == before  # zero cross-project mutation, in every one of the ten protected classes
        assert all(after[name] for name in after) and len(after) == 10
