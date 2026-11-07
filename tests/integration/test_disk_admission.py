"""Disk admission through real guardian processes on a private account.

``bind_test_capacity(disk=...)`` measures the named places; unnamed places are unknown and admit.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from docker_test_toolkit import PhaseBudget, RunSpec
from docker_test_toolkit.application import retention, sharing
from docker_test_toolkit.application.timeline import project_timeline
from docker_test_toolkit.domain.errors import ResourceError
from docker_test_toolkit.factories.testing import bind_test_capacity
from tests.integration.account_fixtures import EnterThread, queue_phases, source_artifact, three_private_projects
from tests.integration.share_fixtures import _leave, _spec, shared_world

pytestmark = pytest.mark.integration

GIB = 1024**3
TIGHT = {"data-root": {"free_bytes": 10 * GIB, "total_bytes": 100 * GIB}}  # reserve 3.125 GiB: one floor run fits


def _solo_spec(project: Path, *, profile: str = "fixture", retain: bool = False) -> RunSpec:
    return RunSpec(
        project_root=project,
        entrypoint="integration",
        purpose="disk-admission",
        services=(),
        closure=(),
        profile=profile,
        phases={"tests": PhaseBudget(GIB, 1, 1, 1)},
        initial_phase="tests",
        artifact=source_artifact(),
        wait_seconds=20,
        retain=retain,
    )


def _leases(account: Path) -> dict[str, sharing.Lease]:
    return sharing.read_leases(account)


def _stays_queued(account: Path, run_id: str, seconds: float) -> None:
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        assert queue_phases(account).get(run_id) in (None, "queued"), queue_phases(account)
        time.sleep(0.05)


def _queued_joiner(world, account: Path):
    joiner = world.services.open_run(_spec(world.project), docker_factory=lambda: world.fake)
    world.write(joiner, True, origin="seed", tick_ns=0)
    entering = EnterThread(joiner)
    deadline = time.monotonic() + 15
    while queue_phases(account).get(joiner.run_id) is None:  # in the queue: queued, or already admitted
        assert time.monotonic() < deadline, "the joiner never entered the queue"
        time.sleep(0.05)
    return joiner, entering


def test_a_holders_lease_makes_a_joiner_wait_and_its_release_admits(tmp_path: Path) -> None:
    with shared_world(tmp_path) as world:
        account = world.services.root / "admission"
        bind_test_capacity(world.services, disk=TIGHT)
        holder = world.open(shared=True)
        assert _leases(account)[holder.run_id].bytes == 4 * GIB  # the floor: no earlier run of this request

        joiner, entering = _queued_joiner(world, account)
        _stays_queued(account, joiner.run_id, 3)  # 10 - 3.125 - 4 < 4: it waits beside the holder's lease
        _leave(world, holder)
        entering.join(timeout=30)

        assert entering.error is None
        assert set(_leases(account)) == {joiner.run_id}  # the holder's lease was dropped with the next write
        listed = project_timeline(world.registry, world.project)["runs"]  # what `project timeline` prints
        (entry,) = [run for run in listed if run["run_id"] == joiner.run_id]
        assert any(item["reason"] == "disk-capacity" and item["waited_ns"] > 0 for item in entry["waiting"])
        _leave(world, joiner)


@pytest.mark.parametrize("lease", ["missing", "another-boot"])
def test_a_holder_without_a_valid_lease_counts_the_floor(tmp_path: Path, lease: str) -> None:
    from docker_test_toolkit.application.recovery import atomic_json

    with shared_world(tmp_path) as world:
        account = world.services.root / "admission"
        bind_test_capacity(world.services, disk=TIGHT)
        holder = world.open(shared=True, phases={"tests": PhaseBudget(GIB, 1, 1, 1)})
        if lease == "missing":  # a 0.6.20 holder, or a crash between reservation and lease
            (account / sharing.LEASES_FILE).unlink()
        else:  # a small lease of another clock boot: not this boot's, so the floor counts
            stale = {"bytes": 1, "source": "floor", "clock_origin": "earlier-boot", "tick": 1}
            atomic_json(account / sharing.LEASES_FILE, {"schema": 1, "leases": {holder.run_id: stale}})

        joiner, entering = _queued_joiner(world, account)
        # 10 - 3.125 - 0 = 6.875 would admit; 10 - 3.125 - 4 (the floor for the holder) = 2.875 waits
        _stays_queued(account, joiner.run_id, 3)
        _leave(world, holder)
        entering.join(timeout=30)

        assert entering.error is None
        _leave(world, joiner)


def test_a_solo_run_is_not_charged_its_own_lease(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    reserve = sharing.DISK_RESERVE_FLOOR
    sharing.record_disk_reserve(services.root / "admission", reserve)
    bind_test_capacity(services, disk={"data-root": {"free_bytes": reserve + 4 * GIB, "total_bytes": 100 * GIB}})

    run = services.open_run(_solo_spec(projects[0])).enter()  # room for exactly one estimate

    assert sharing.read_leases(services.root / "admission")[run.run_id].bytes == 4 * GIB
    # The detected runtime is never recorded: the granted record keeps 0.6.20's capacity keys.
    recorded = json.loads(run._context.record.read_text())["admission_observation"]["capacity"]
    assert "runtime" not in recorded and "daemon_memory_bytes" in recorded
    run.leave()

    # The lease goes with the queue entry, not only at the next run's reservation.
    assert run.run_id not in sharing.read_leases(services.root / "admission")


def test_the_lease_is_derived_from_the_previous_runs_image_sizes(tmp_path: Path) -> None:
    from docker_test_toolkit.application.recovery import atomic_json

    services, projects = three_private_projects(tmp_path)
    previous = services.open_run(_solo_spec(projects[0])).enter()
    previous.leave()
    other = services.open_run(_solo_spec(projects[0], profile="other")).enter()  # another request key
    other.leave()
    for handle, sizes in ((previous, [GIB, 2 * GIB]), (other, [40 * GIB])):  # as if each had built images
        record = json.loads(handle._context.record.read_text())
        record["owned_resources"]["images"] = {
            f"sha256:{index:064x}": {"evidence": {"size_bytes": size}, "state": "removed"}
            for index, size in enumerate(sizes)
        }
        atomic_json(handle._context.record, record)

    run = services.open_run(_solo_spec(projects[0])).enter()

    lease = sharing.read_leases(services.root / "admission")[run.run_id]
    assert (lease.bytes, lease.source) == (3 * GIB + 3 * GIB // 4, "derived")  # its own request's run, not "other"
    run.leave()


def test_never_fit_is_refused_before_mutation_naming_bytes(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    bind_test_capacity(
        services,
        disk={"data-root": {"free_bytes": 50 * GIB, "total_bytes": 50 * GIB}},
        previous_image_sizes=[48 * GIB],  # 60 GiB estimated against 48 GiB usable
    )

    with pytest.raises(ResourceError) as refused:
        services.open_run(_solo_spec(projects[0])).enter()

    message = str(refused.value)
    assert "disk-capacity" in message
    for text in ("data-root", f"total {50 * GIB}", f"reserve {2 * GIB}", f"estimate {60 * GIB}"):
        assert text in message, message


def test_the_grant_decides_the_disk_again(tmp_path: Path) -> None:
    from tests.integration.account_fixtures import prepare_for_child_guardian, reap, start_guardian_child

    services, projects = three_private_projects(tmp_path)
    account = services.root / "admission"
    bind_test_capacity(services, disk={"data-root": {"free_bytes": 20 * GIB, "total_bytes": 100 * GIB}})
    run = services.open_run(_solo_spec(projects[0]))
    prepare_for_child_guardian(run)
    reserved, release = tmp_path / "reserved", tmp_path / "release"
    stages = {
        "slot-reserved": {
            "action": [{"do": "write-file", "path": str(reserved)}, {"do": "wait-file", "path": str(release)}]
        }
    }
    guardian, _ = start_guardian_child(tmp_path, run, account, stages, name="guardian")
    try:
        entering = EnterThread(run, existing_attempt=True)
        deadline = time.monotonic() + 20
        while not reserved.exists():
            assert time.monotonic() < deadline, "the run never reserved"
            time.sleep(0.05)
        # Another process filled the disk after the reservation: 5 - 3.125 < 4, and the run's own lease is not counted
        bind_test_capacity(services, disk={"data-root": {"free_bytes": 5 * GIB, "total_bytes": 100 * GIB}})
        release.write_text("go")
        _stays_admitting = time.monotonic() + 3
        while time.monotonic() < _stays_admitting:
            assert queue_phases(account).get(run.run_id) == "admitting", queue_phases(account)
            time.sleep(0.05)
        bind_test_capacity(services, disk={"data-root": {"free_bytes": 20 * GIB, "total_bytes": 100 * GIB}})
        entering.join(timeout=30)

        assert entering.error is None
        waiting = json.loads(run._context.record.read_text())["hold_timeline"]["waiting"]
        assert "disk-capacity" in {item["reason"] for item in waiting}
        run.leave()
        guardian.wait(timeout=30)
    finally:
        reap(guardian)


def test_releasing_a_retained_run_is_never_disk_gated(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    retained = services.open_run(_solo_spec(projects[0], retain=True)).enter()
    assert retained.retain().state == "stopped-retained"
    locator = tmp_path / "locator.json"
    retained.write_retained_locator(locator)
    bind_test_capacity(  # every run would be refused: 60 GiB estimated against 48 GiB usable
        services, disk={"data-root": {"free_bytes": 1 * GIB, "total_bytes": 50 * GIB}}, previous_image_sizes=[48 * GIB]
    )

    released = services.open_retained_run(_solo_spec(projects[0], retain=True), locator).release()

    assert released.state == "complete"


def test_a_disk_wait_with_no_holders_runs_retention_then_admits(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    account = services.root / "admission"
    bind_test_capacity(services, disk={"data-root": {"free_bytes": 5 * GIB, "total_bytes": 100 * GIB}})
    run = services.open_run(_solo_spec(projects[0]))
    entering = EnterThread(run)
    deadline = time.monotonic() + 15
    while (report := retention.read_report(account)) is None or report["trigger"] != "disk-wait":
        assert time.monotonic() < deadline, "no retention pass ran while the solo run waited on disk"
        time.sleep(0.05)
    assert queue_phases(account).get(run.run_id) == "queued"

    bind_test_capacity(services, disk={"data-root": {"free_bytes": 20 * GIB, "total_bytes": 100 * GIB}})
    entering.join(timeout=30)

    assert entering.error is None
    run.leave()
