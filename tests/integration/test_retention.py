"""Retention against real files and the real Docker daemon."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import time
import uuid
from pathlib import Path

import pytest

from docker_test_toolkit.application import build_cache, probe, retention
from docker_test_toolkit.application import build_cache as store
from docker_test_toolkit.application.lifecycle import NativeClock
from docker_test_toolkit.application.recovery import atomic_json, project_id_for
from docker_test_toolkit.factories.cli import build_docker_adapter
from tests.integration.account_fixtures import wait_until
from tests.integration.test_build_cache_store import _generation

pytestmark = pytest.mark.integration

GIB = 1024**3
DAY_NS = 86_400 * 1_000_000_000
PLATFORM = "linux-arm64"


def _world(tmp_path: Path) -> tuple[Path, Path]:
    account, registry = tmp_path / "admission", tmp_path / "registry"
    for directory in (account, registry, registry / "projects", registry / "runs"):
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    return account, registry


def _pass(account: Path, registry: Path, *, docker=None, total: int = 460 * GIB, **options) -> dict:
    report = retention.run_pass(
        account=account,
        registry=registry,
        trigger="test",
        docker=docker,
        clock=NativeClock(),
        force=True,
        fs_total=lambda _path: total,
        **options,
    )
    assert report is not None
    return report.kinds


def _cache_platform(registry: Path, project: str) -> Path:
    root = build_cache.cache_root(registry)
    platform = root / project / PLATFORM
    for level in (root, root / project, platform):
        store.require_safe_directory(level)
    return platform


# --- build cache ------------------------------------------------------------------------------------------------------


def test_retention_build_cache_evicts_the_oldest_generations_across_projects_until_under_the_cap(
    tmp_path: Path,
) -> None:
    account, registry = _world(tmp_path)
    one, two = _cache_platform(registry, "a" * 64), _cache_platform(registry, "b" * 64)
    oldest = _generation(one, "1" * 64, 10, sizes=(3 * GIB,))
    middle = _generation(two, "2" * 64, 20, sizes=(3 * GIB,))
    newest = _generation(one, "3" * 64, 30, sizes=(3 * GIB,))

    kinds = _pass(account, registry, total=460 * GIB)  # cap 460/64 = 7.19 GiB against 9 GiB stored

    assert not oldest.exists() and middle.exists() and newest.exists()
    assert kinds["build_cache"]["removed_count"] == 1


def test_retention_build_cache_keeps_an_orphan_under_the_cap_and_never_enters_foreign_entries(tmp_path: Path) -> None:
    account, registry = _world(tmp_path)
    platform = _cache_platform(registry, "c" * 64)  # no project record: an orphan (a CI checkout briefly absent)
    orphan = _generation(platform, "4" * 64, 10, sizes=(GIB,))
    stray = build_cache.cache_root(registry) / "not-a-project"
    stray.mkdir()
    (stray / "file").write_text("foreign")

    kinds = _pass(account, registry)

    assert orphan.exists() and (stray / "file").exists()
    assert kinds["build_cache"]["removed_count"] == 0


# --- archives ---------------------------------------------------------------------------------------------------------


def _archive_runs(registry: Path) -> Path:
    archive = registry.parent / f"{registry.name}-archive"
    runs = archive / ("e" * 64) / "runs"
    for level in (archive, archive / ("e" * 64), runs):  # private at every level, as retirement makes them
        level.mkdir(mode=0o700, exist_ok=True)
    return runs


def _archived(registry: Path, *, age_days: float) -> Path:
    """Write an archived run record as retirement names it: ``<run id>-<sha256 of its bytes>.json``."""
    run_id = str(uuid.uuid4())
    data = json.dumps({"run_id": run_id, "state": "complete"}).encode()
    path = _archive_runs(registry) / f"{run_id}-{hashlib.sha256(data).hexdigest()}.json"
    path.write_bytes(data)
    path.chmod(0o600)
    return path


def test_retention_archives_remove_records_archived_more_than_90_days_ago(tmp_path: Path) -> None:
    account, registry = _world(tmp_path)
    old, fresh = _archived(registry, age_days=100), _archived(registry, age_days=0)
    now = time.time_ns()

    kinds = _pass(account, registry, wall_ns=now + 100 * DAY_NS)  # both files look 100 days old by mtime and ctime

    assert not old.exists() and not fresh.exists() and kinds["archives"]["removed_count"] == 2
    young = _archived(registry, age_days=0)
    assert _pass(account, registry, wall_ns=time.time_ns() + DAY_NS)["archives"]["removed_count"] == 0
    assert young.exists()


# --- session roots ----------------------------------------------------------------------------------------------------


PROJECT_ROOT = "/srv/project-a"
PROJECT_ID = project_id_for(PROJECT_ROOT)  # a session folder holds one folder per project, named by its project ID


def _project_record(registry: Path, sessions: dict, runs: dict | None = None, root: str = PROJECT_ROOT) -> None:
    atomic_json(
        registry / "projects" / f"{project_id_for(root)}.json",
        {"project_root": root, "sessions": sessions, "runs": runs or {}},
    )


def test_retention_session_roots_remove_only_empty_recorded_folders_older_than_a_day(tmp_path: Path) -> None:
    account, registry = _world(tmp_path)
    parent = tmp_path / "tmp"
    base = parent / "docker-test-toolkit"
    empty, full, unrecorded = (str(uuid.uuid4()) for _ in range(3))
    for session in (empty, full, unrecorded):
        (base / session / PROJECT_ID).mkdir(parents=True)
    (base / full / PROJECT_ID / "file").write_text("left by a killed run")
    _project_record(registry, {empty: {"temporary_parent": str(parent)}, full: {"temporary_parent": str(parent)}})

    kinds = _pass(account, registry, wall_ns=time.time_ns() + 2 * DAY_NS)

    assert not (base / empty).exists()
    assert (base / full / PROJECT_ID / "file").exists() and (base / unrecorded).exists()
    reasons = {Path(item["id"]).name: item["reason"] for item in kinds["session_roots"]["skipped"]}
    assert reasons == {full: "has-content", unrecorded: "unrecorded"}


def test_only_the_projects_own_folder_in_a_session_is_removed(tmp_path: Path) -> None:
    account, registry = _world(tmp_path)
    parent = tmp_path / "tmp"
    session = str(uuid.uuid4())
    folder = parent / "docker-test-toolkit" / session
    for child in (PROJECT_ID, "foreign-empty", "8" * 64):  # its own, a stranger's and another project's folder
        (folder / child).mkdir(parents=True)
    _project_record(registry, {session: {"temporary_parent": str(parent)}})

    _pass(account, registry, wall_ns=time.time_ns() + 2 * DAY_NS)

    assert not (folder / PROJECT_ID).exists()
    assert (folder / "foreign-empty").is_dir() and (folder / ("8" * 64)).is_dir()


def test_a_symlinked_session_base_is_never_entered(tmp_path: Path) -> None:
    account, registry = _world(tmp_path)
    parent, elsewhere = tmp_path / "tmp", tmp_path / "elsewhere"
    session = str(uuid.uuid4())
    victim = elsewhere / session / PROJECT_ID
    victim.mkdir(parents=True)  # an empty folder that would qualify, if the base were followed
    parent.mkdir()
    (parent / "docker-test-toolkit").symlink_to(elsewhere)
    _project_record(registry, {session: {"temporary_parent": str(parent)}})

    _pass(account, registry, wall_ns=time.time_ns() + 2 * DAY_NS)

    assert victim.is_dir()


# --- throttle ---------------------------------------------------------------------------------------------------------


def test_retention_is_throttled_unless_forced_or_work_was_deferred(tmp_path: Path) -> None:
    account, registry = _world(tmp_path)

    def plain(**options) -> object:
        return retention.run_pass(
            account=account, registry=registry, trigger="test", docker=None, clock=NativeClock(), **options
        )

    assert plain() is not None
    assert plain() is None  # a pass of this boot younger than 10 min deferred nothing
    assert plain(force=True) is not None
    _archived(registry, age_days=0)
    _archived(registry, age_days=0)
    deferred = plain(force=True, max_deletions=0, wall_ns=time.time_ns() + 100 * DAY_NS)
    assert deferred is not None and deferred.kinds["archives"]["deferred"] is True
    assert plain() is not None  # deferred work makes the next trigger run at once


# --- Docker kinds (real daemon) ---------------------------------------------------------------------------------------


def _docker(*argv: str) -> subprocess.CompletedProcess:
    return subprocess.run(["docker", *argv], capture_output=True, text=True, check=False)


def _run_record(registry: Path, daemon_id: str, builder: str, evidence: dict) -> None:
    record = {
        "state": "complete",
        "grant": {"daemon_id": daemon_id},
        "owned_resources": {"builders": {builder: {"evidence": evidence}}},
    }
    path = registry / "runs" / f"{uuid.uuid4()}.json"
    path.write_text(json.dumps(record))
    path.chmod(0o600)


@pytest.fixture
def volumes():
    created: list[str] = []
    yield created
    for name in created:
        _docker("volume", "rm", "-f", name)


def test_retention_builder_volumes_remove_only_the_recorded_incarnation(tmp_path: Path, volumes: list[str]) -> None:
    account, registry = _world(tmp_path)
    docker = build_docker_adapter()
    daemon_id = docker.observe_daemon().daemon_id
    tag = uuid.uuid4().hex[:12]
    owned, recreated, foreign = (f"dtt-{tag}-{suffix}" for suffix in ("own", "new", "foreign"))
    for builder in (owned, recreated, foreign):
        volumes.append(f"buildx_buildkit_{builder}0_state")
        assert _docker("volume", "create", f"buildx_buildkit_{builder}0_state").returncode == 0

    def created_at(builder: str) -> str:
        return _docker(
            "volume", "inspect", "--format", "{{.CreatedAt}}", f"buildx_buildkit_{builder}0_state"
        ).stdout.strip()

    _run_record(
        registry,
        daemon_id,
        owned,
        {"state_volume": {"name": f"buildx_buildkit_{owned}0_state", "created_at": created_at(owned)}},
    )
    _run_record(
        registry,
        daemon_id,
        recreated,
        {"state_volume": {"name": f"buildx_buildkit_{recreated}0_state", "created_at": "2000-01-01T00:00:00Z"}},
    )

    kinds = _pass(account, registry, docker=docker)

    assert _docker("volume", "inspect", f"buildx_buildkit_{owned}0_state").returncode != 0  # removed
    assert _docker("volume", "inspect", f"buildx_buildkit_{recreated}0_state").returncode == 0  # another incarnation
    assert _docker("volume", "inspect", f"buildx_buildkit_{foreign}0_state").returncode == 0  # no record names it
    skipped = {item["id"]: item["reason"] for item in kinds["builder_volumes"]["skipped"]}
    assert skipped[f"buildx_buildkit_{recreated}0_state"] == "identity-changed"
    assert kinds["builder_volumes"]["removed_count"] == 1


def test_retention_probe_containers_remove_a_recorded_leftover_on_its_own_daemon_only(tmp_path: Path) -> None:
    account, registry = _world(tmp_path)
    docker = build_docker_adapter()
    daemon_id = docker.observe_daemon().daemon_id
    name, other = probe.probe_name(account), probe.probe_name(account)
    assert _docker("create", "--name", name, probe.PROBE_IMAGE, "true").returncode == 0
    try:
        records = account / probe.PROBES_DIRECTORY
        records.mkdir(mode=0o700)
        (records / name).write_text(daemon_id)
        (records / other).write_text("another-daemon")
        old = time.time() - 120
        for record in (records / name, records / other):
            os.utime(record, (old, old))

        kinds = _pass(account, registry, docker=docker)

        assert _docker("inspect", name).returncode != 0 and not (records / name).exists()
        assert (records / other).exists()
        assert kinds["probe_containers"]["skipped"] == [{"id": other, "reason": "other-daemon", "bytes": 0}]
    finally:
        _docker("rm", "-f", name)


def test_retention_reports_an_unpromoted_builder_volume_and_keeps_it(tmp_path: Path, volumes: list[str]) -> None:
    # A run killed between `buildx create` and promotion: a name and a client clock cannot prove the volume is ours.
    account, registry = _world(tmp_path)
    docker = build_docker_adapter()
    builder = f"dtt-{uuid.uuid4().hex[:12]}-unpromoted"
    volume = f"buildx_buildkit_{builder}0_state"
    volumes.append(volume)
    assert _docker("volume", "create", volume).returncode == 0
    _run_record(registry, docker.observe_daemon().daemon_id, builder, {})

    report = retention.run_pass(
        account=account, registry=registry, trigger="test", docker=docker, clock=NativeClock(), force=True
    )

    assert report is not None and _docker("volume", "inspect", volume).returncode == 0
    assert {"id": volume, "reason": "unpromoted-builder", "bytes": 0} in report.kinds["builder_volumes"]["skipped"]
    assert report.legacy["builder_volumes_unrecorded"] >= 1  # daemon-wide: other leftovers count too


def test_retention_runs_when_the_last_holder_leaves(tmp_path: Path) -> None:
    from tests.integration.share_fixtures import _leave, shared_world

    with shared_world(tmp_path) as world:
        account = world.services.root / "admission"
        first, second = world.open(shared=True), world.open(shared=True)

        _leave(world, first)
        after_first = (account / retention.STAMP_FILE).exists()
        _leave(world, second)

        assert not after_first  # a holder remained: the last one to leave runs the pass
        # The guardian runs the pass after acknowledging the leave, so the report can follow the leave's return.
        wait_until(lambda: retention.read_report(account) is not None, message="the leave's retention report")
        report = retention.read_report(account)
        assert report is not None and report["trigger"] == "leave"
        # The pass started after the leave was acknowledged: the release is written in the acknowledging write, on
        # the same machine-wide clock, so the requester never waits for retention.
        timeline = json.loads(second._context.record.read_text())["hold_timeline"]
        assert timeline["release_origin"] == report["clock_origin"] and report["tick"] >= timeline["released_ns"]


# --- review corrections (packet 10) -----------------------------------------------------------------------------------


def test_a_foreign_archive_shaped_file_is_kept_unverified(tmp_path: Path) -> None:
    account, registry = _world(tmp_path)
    foreign = _archive_runs(registry) / f"{uuid.uuid4()}-{'f' * 64}.json"  # a matching name over bytes it does not hash
    foreign.write_text('{"note": "not a toolkit record"}')
    foreign.chmod(0o600)

    kinds = _pass(account, registry, wall_ns=time.time_ns() + 100 * DAY_NS)

    assert foreign.exists()
    assert {"id": str(foreign), "reason": "unverified", "bytes": 0} in kinds["archives"]["skipped"]


def test_a_session_of_an_indexed_run_is_never_entered(tmp_path: Path) -> None:
    account, registry = _world(tmp_path)
    parent = tmp_path / "tmp"
    live, idle = str(uuid.uuid4()), str(uuid.uuid4())
    for session in (live, idle):
        (parent / "docker-test-toolkit" / session / PROJECT_ID).mkdir(parents=True)  # empty: a run creates it first
    other, run_id = "/srv/project-b", str(uuid.uuid4())
    run = registry / "runs" / f"{run_id}.json"  # indexed as registration writes it: its identity names its project
    identity = {"run_id": run_id, "project_id": project_id_for(other), "project_root": other}
    atomic_json(run, {**identity, "session_id": live, "state": "granted"})
    _project_record(registry, {live: {"temporary_parent": str(parent)}}, {run_id: str(run)}, root=other)
    _project_record(registry, {live: {"temporary_parent": str(parent)}, idle: {"temporary_parent": str(parent)}})

    _pass(account, registry, wall_ns=time.time_ns() + 2 * DAY_NS)

    # Another project indexes a run in the session: its folders, this project's included, are never entered.
    assert (parent / "docker-test-toolkit" / live / PROJECT_ID).is_dir()
    assert not (parent / "docker-test-toolkit" / idle).exists()


def test_an_indexed_run_whose_identity_names_another_project_defers_the_kind(tmp_path: Path) -> None:
    account, registry = _world(tmp_path)
    parent = tmp_path / "tmp"
    idle, run_id = str(uuid.uuid4()), str(uuid.uuid4())
    (parent / "docker-test-toolkit" / idle / PROJECT_ID).mkdir(parents=True)
    run = registry / "runs" / f"{run_id}.json"
    identity = {"run_id": run_id, "project_id": project_id_for("/srv/other"), "project_root": "/srv/other"}
    atomic_json(run, {**identity, "session_id": str(uuid.uuid4()), "state": "granted"})
    _project_record(registry, {idle: {"temporary_parent": str(parent)}}, {run_id: str(run)})

    kinds = _pass(account, registry, wall_ns=time.time_ns() + 2 * DAY_NS)

    # What is idle cannot be proven while an index entry is not this project's run: nothing is removed.
    assert kinds["session_roots"]["deferred"] is True
    assert (parent / "docker-test-toolkit" / idle / PROJECT_ID).is_dir()


def test_every_directory_removal_spends_the_deletion_budget(tmp_path: Path) -> None:
    account, registry = _world(tmp_path)
    parent = tmp_path / "tmp"
    session = str(uuid.uuid4())
    (parent / "docker-test-toolkit" / session / PROJECT_ID).mkdir(parents=True)
    _project_record(registry, {session: {"temporary_parent": str(parent)}})

    kinds = _pass(account, registry, wall_ns=time.time_ns() + 2 * DAY_NS, max_deletions=1)

    # The project's folder used the one removal; the session folder waits for the next pass.
    assert kinds["session_roots"]["deferred"] is True
    assert (parent / "docker-test-toolkit" / session).is_dir()
    assert not (parent / "docker-test-toolkit" / session / PROJECT_ID).exists()


def test_a_busy_generation_is_protected_and_the_next_oldest_goes(tmp_path: Path) -> None:
    account, registry = _world(tmp_path)
    busy_project, other_project = "a" * 64, "b" * 64
    one, two = _cache_platform(registry, busy_project), _cache_platform(registry, other_project)
    oldest = _generation(one, "1" * 64, 10, sizes=(3 * GIB,))
    middle = _generation(two, "2" * 64, 20, sizes=(3 * GIB,))
    newest = _generation(two, "3" * 64, 30, sizes=(3 * GIB,))
    root = build_cache.cache_root(registry)

    with build_cache.platform_anchor(root / busy_project, PLATFORM, shared=True):  # a live import reads it
        kinds = _pass(account, registry, total=460 * GIB)  # cap 7.19 GiB against 9 GiB stored
        assert oldest.exists() and not middle.exists() and newest.exists()
        assert kinds["build_cache"]["deferred"] is False  # 6 GiB left: under the cap
        with build_cache.platform_anchor(root / other_project, PLATFORM, shared=True):
            held = _pass(account, registry, total=64 * 4 * GIB)  # cap 4 GiB against 6 GiB, every generation busy

    assert oldest.exists() and newest.exists()
    assert held["build_cache"]["deferred"] is True
    (excess,) = [item["bytes"] for item in held["build_cache"]["skipped"] if item["reason"] == "over-cap-protected"]
    assert 2 * GIB <= excess < 2 * GIB + 1024**2  # 6 GiB of layers plus their metadata over a 4 GiB cap


def test_a_symlinked_cache_root_hides_even_over_cap_generations(tmp_path: Path) -> None:
    account, registry = _world(tmp_path)
    _other_account, elsewhere = _world(tmp_path / "elsewhere")
    victim = _generation(_cache_platform(elsewhere, "d" * 64), "5" * 64, 10, sizes=(5 * GIB,))
    build_cache.cache_root(registry).symlink_to(build_cache.cache_root(elsewhere))

    kinds = _pass(account, registry, total=1)  # the floor cap of 4 GiB against 5 GiB, if the root were entered

    assert victim.exists() and kinds["build_cache"]["removed_count"] == 0


def test_a_symlinked_probe_record_never_removes_the_container_it_names(tmp_path: Path) -> None:
    account, registry = _world(tmp_path)
    docker = build_docker_adapter()
    name = probe.probe_name(account)
    assert _docker("create", "--name", name, probe.PROBE_IMAGE, "true").returncode == 0  # someone else's container
    try:
        records = account / probe.PROBES_DIRECTORY
        records.mkdir(mode=0o700)
        target = tmp_path / "genuine-looking"
        target.write_text(docker.observe_daemon().daemon_id)
        (records / name).symlink_to(target)
        old = time.time() - 120  # older than a probe's lifetime: only the record check can keep the container
        os.utime(target, (old, old))
        os.utime(records / name, (old, old), follow_symlinks=False)

        kinds = _pass(account, registry, docker=docker)

        assert _docker("inspect", name).returncode == 0
        assert {"id": name, "reason": "unreadable", "bytes": 0} in kinds["probe_containers"]["skipped"]
    finally:
        _docker("rm", "-f", name)


def test_receipt_only_and_unrecorded_builder_volumes_are_reported_and_kept(tmp_path: Path, volumes: list[str]) -> None:
    account, registry = _world(tmp_path)
    docker = build_docker_adapter()
    planned = f"buildx_buildkit_dtt-{uuid.uuid4().hex[:12]}-{uuid.uuid4().hex[:12]}0_state"  # killed before promotion
    unrecorded = f"buildx_buildkit_dtt-{uuid.uuid4().hex[:12]}-{uuid.uuid4().hex[:12]}0_state"  # no record names it
    for volume in (planned, unrecorded):
        volumes.append(volume)
        assert _docker("volume", "create", volume).returncode == 0
    record = {
        "state": "complete",
        "grant": {"daemon_id": docker.observe_daemon().daemon_id},
        "docker_receipts": {"r": {"kind": "builder", "state": "unresolved", "plan": {"state_volume": planned}}},
        "owned_resources": {"builders": {}},
    }
    atomic_json(registry / "runs" / f"{uuid.uuid4()}.json", record)

    kinds = _pass(account, registry, docker=docker)

    assert (
        _docker("volume", "inspect", planned).returncode == 0
        and _docker("volume", "inspect", unrecorded).returncode == 0
    )
    skipped = kinds["builder_volumes"]["skipped"]
    assert {"id": planned, "reason": "unpromoted-builder", "bytes": 0} in skipped
    assert {"id": unrecorded, "reason": "unrecorded", "bytes": 0} in skipped


@pytest.fixture
def images():
    created: list[str] = []
    yield created
    for reference in created:
        _docker("image", "rm", "-f", reference)


def _image(tag: str, images: list[str]) -> tuple[str, str]:
    images.append(tag)
    built = subprocess.run(
        ["docker", "build", "--quiet", "--tag", tag, "-"],
        input=f"FROM scratch\nLABEL dtt.retention.test={tag}\n",
        capture_output=True,
        text=True,
        check=False,
    )
    assert built.returncode == 0, built.stderr
    inspected = json.loads(_docker("image", "inspect", tag).stdout)[0]
    return inspected["Id"], inspected.get("Created", "")  # a scratch image may carry no creation time


def test_retention_drains_pending_images_and_drops_a_retagged_one(tmp_path: Path, images: list[str]) -> None:
    from docker_test_toolkit.application.sharing import PendingImage, read_pending, write_pending

    account, registry = _world(tmp_path)
    docker = build_docker_adapter()
    daemon_id = docker.observe_daemon().daemon_id
    suffix = uuid.uuid4().hex[:12]
    unused, retagged = f"dtt-retention-unused:{suffix}", f"dtt-retention-retagged:{suffix}"
    entries = []
    for tag in (unused, retagged):
        image_id, created = _image(tag, images)
        entries.append(PendingImage(image_id, created, (tag,), str(uuid.uuid4()), "a" * 64, daemon_id))
    extra = f"dtt-retention-retagged-elsewhere:{suffix}"
    images.append(extra)
    assert _docker("tag", retagged, extra).returncode == 0  # a tag the hand-off never recorded
    write_pending(account, entries)

    report = retention.run_pass(
        account=account,
        registry=registry,
        trigger="test",
        docker=docker,
        clock=NativeClock(),
        force=True,
        image_interest=lambda: lambda _image_id: False,
    )

    assert report is not None and report.kinds["pending_images"]["removed_count"] == 1
    assert _docker("image", "inspect", unused).returncode != 0  # nobody used it: deleted
    assert _docker("image", "inspect", extra).returncode == 0  # re-tagged: kept, and its entry dropped
    assert {"id": entries[1].image_id, "reason": "re-tagged", "bytes": 0} in report.kinds["pending_images"]["skipped"]
    assert read_pending(account) == []


def _pending(tags: tuple[str, ...], images: list[str], daemon_id: str) -> list:
    from docker_test_toolkit.application.sharing import PendingImage

    entries = []
    for tag in tags:
        image_id, created = _image(tag, images)
        entries.append(PendingImage(image_id, created, (tag,), str(uuid.uuid4()), "a" * 64, daemon_id))
    return entries


def test_the_pending_drain_stays_within_the_deletion_budget(tmp_path: Path, images: list[str]) -> None:
    from docker_test_toolkit.application.sharing import read_pending, write_pending

    account, registry = _world(tmp_path)
    docker = build_docker_adapter()
    suffix = uuid.uuid4().hex[:12]
    tags = (f"dtt-retention-first:{suffix}", f"dtt-retention-second:{suffix}")
    entries = _pending(tags, images, docker.observe_daemon().daemon_id)
    write_pending(account, entries)

    report = retention.run_pass(
        account=account,
        registry=registry,
        trigger="test",
        docker=docker,
        clock=NativeClock(),
        force=True,
        max_deletions=1,
        image_interest=lambda: lambda _image_id: False,
    )

    assert report is not None and report.kinds["pending_images"]["removed_count"] == 1
    assert report.kinds["pending_images"]["deferred"] is True
    assert read_pending(account) == entries[1:]  # the second waits for the next pass


def test_a_busy_metadata_lock_defers_the_pending_drain_without_waiting(tmp_path: Path, images: list[str]) -> None:
    from docker_test_toolkit.application.lifecycle import retention_image_interest
    from docker_test_toolkit.application.recovery import Context, lock
    from docker_test_toolkit.application.sharing import read_pending, write_pending

    account, registry = _world(tmp_path)
    docker = build_docker_adapter()
    entries = _pending((f"dtt-retention-held:{uuid.uuid4().hex[:12]}",), images, docker.observe_daemon().daemon_id)
    write_pending(account, entries)

    with lock(Context(account, account), "metadata"):  # an admission pass is deciding
        started = time.monotonic()
        report = retention.run_pass(
            account=account,
            registry=registry,
            trigger="test",
            docker=docker,
            clock=NativeClock(),
            force=True,
            image_interest=retention_image_interest(account, registry),
        )
        waited = time.monotonic() - started

    assert report is not None and report.kinds["pending_images"]["deferred"] is True
    assert read_pending(account) == entries and _docker("image", "inspect", entries[0].image_id).returncode == 0
    assert waited < 5  # it never waited for the metadata lock while holding K


def test_a_registration_in_progress_defers_session_cleanup(tmp_path: Path) -> None:
    from docker_test_toolkit.application.recovery import Context, lock

    account, registry = _world(tmp_path)
    parent = tmp_path / "tmp"
    session = str(uuid.uuid4())
    own = parent / "docker-test-toolkit" / session / PROJECT_ID
    own.mkdir(parents=True)
    _project_record(registry, {session: {"temporary_parent": str(parent)}})

    with lock(Context(registry, registry), "registry"):  # a run is being indexed and its folders created
        kinds = _pass(account, registry, wall_ns=time.time_ns() + 2 * DAY_NS)

    assert own.is_dir() and kinds["session_roots"]["deferred"] is True


class _StoppedDaemon:
    """A daemon that fails every identity read mid-pass (stopped, or another one behind the endpoint)."""

    def observe_daemon(self, deadline: object = None) -> object:
        from docker_test_toolkit.domain.errors import InfrastructureFailure

        raise InfrastructureFailure("Docker daemon is unreachable.", reason="client-exit")


def test_a_daemon_failing_mid_pass_defers_its_kinds_and_the_pass_still_reports(tmp_path: Path) -> None:
    account, registry = _world(tmp_path)
    (account / probe.PROBES_DIRECTORY).mkdir(mode=0o700)

    kinds = _pass(account, registry, docker=_StoppedDaemon(), image_interest=lambda: lambda _image: False)

    # The file kinds ran and are reported; each Docker kind is deferred, nothing of it was deleted.
    assert kinds["session_roots"]["deferred"] is False
    for kind in ("builder_volumes", "probe_containers"):
        assert kinds[kind]["deferred"] is True
        assert kinds[kind]["skipped"] == [{"id": "*", "reason": "docker-unavailable", "bytes": 0}]
    assert retention.read_report(account) is not None and not retention.throttled(account, NativeClock())


def test_a_session_entry_of_another_shape_is_never_entered(tmp_path: Path) -> None:
    account, registry = _world(tmp_path)
    parent = tmp_path / "tmp"
    odd = str(uuid.uuid4())
    (parent / "docker-test-toolkit" / odd / PROJECT_ID).mkdir(parents=True)
    _project_record(registry, {odd: "not a session", str(uuid.uuid4()): {"previous_temporary_parents": "x"}})

    kinds = _pass(account, registry, wall_ns=time.time_ns() + 2 * DAY_NS)

    assert kinds["session_roots"]["deferred"] is False
    assert (parent / "docker-test-toolkit" / odd / PROJECT_ID).is_dir()


def test_the_archive_sweep_removes_only_the_toolkits_own_empty_folders(tmp_path: Path) -> None:
    account, registry = _world(tmp_path)
    old = _archived(registry, age_days=100)
    archive = old.parent.parent.parent
    notes = archive / "operator-notes"  # an operator's empty folder beside the project archives
    notes.mkdir(mode=0o700)
    stray = archive / ("d" * 64) / "spare"  # an empty folder inside a project archive that is not its runs folder
    stray.mkdir(mode=0o700, parents=True)

    _pass(account, registry, wall_ns=time.time_ns() + 100 * DAY_NS)

    assert not old.exists()
    assert not (archive / ("e" * 64)).exists()  # its own project archive, emptied by the pass, is removed
    assert notes.is_dir() and stray.is_dir()  # no record names them: never removed


def test_every_docker_read_of_a_pass_carries_the_pass_deadline(tmp_path: Path) -> None:
    from types import SimpleNamespace

    from docker_test_toolkit.domain.models import DockerObservation, DockerResult

    account, registry = _world(tmp_path)
    probes = account / probe.PROBES_DIRECTORY
    probes.mkdir(mode=0o700)
    record = probes / probe.probe_name(account)  # a probe a killed process left, on this daemon
    record.write_text("d1")
    record.chmod(0o600)
    old = time.time_ns() - 3600 * 1_000_000_000
    os.utime(record, ns=(old, old))
    pass_deadline = object()
    seen: list[tuple[str, object]] = []

    class _Recording:
        def observe_daemon(self, deadline: object = None) -> object:
            seen.append(("observe_daemon", deadline))
            return SimpleNamespace(daemon_id="d1")

        def inspect_exact(self, request: object, *, deadline: object = None) -> DockerObservation:
            seen.append(("inspect_exact", deadline))
            return DockerObservation("container", record.name, False, {})

        def execute(self, argv: tuple[str, ...], **_options: object) -> DockerResult:
            return DockerResult(1, "", "")  # no volume listing: the builder kind has nothing to remove

    report = retention.run_pass(
        account=account,
        registry=registry,
        trigger="test",
        docker=_Recording(),
        clock=NativeClock(),
        force=True,
        fs_total=lambda _path: 460 * GIB,
        deadline=pass_deadline,
    )

    assert report is not None and not record.exists()  # the probe kind ran through its inspections
    assert {name for name, _ in seen} == {"observe_daemon", "inspect_exact"}
    assert all(deadline is pass_deadline for _, deadline in seen), seen
