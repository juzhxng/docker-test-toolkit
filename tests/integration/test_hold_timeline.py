"""The hold timeline on real private-domain run records: loader coupling, writers and the read command."""

from __future__ import annotations

import hashlib
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import pytest

from docker_test_toolkit.application import lifecycle, operator, sharing
from docker_test_toolkit.application.recovery import Context, atomic_json
from docker_test_toolkit.application.timeline import project_timeline
from docker_test_toolkit.domain.errors import InvalidInput, ResourceError
from docker_test_toolkit.domain.records import record_cell_id
from docker_test_toolkit.factories.testing import build_test_services, install_failpoint, install_lock_observer
from tests.integration.account_fixtures import (
    EnterThread,
    Interrupted,
    kill_guardian,
    pause_guardian,
    prepare_for_child_guardian,
    reap,
    resume_guardian,
    start_guardian_child,
    three_private_projects,
    wait_guardians_exited,
    wait_until,
)
from tests.integration.test_native_lifecycle import _capacity_project, _record_context, _record_path, _spec
from tests.integration.test_takeover import (
    _acknowledged_then_guardian_killed,
    _crash_at,
    _crashed_terminal_resource_grant,
    _empty_grant,
    _guardian_killed_after_grant_record,
    _recover,
    _resource_grant,
)

pytestmark = pytest.mark.integration


def _timeline(origin: str, granted_ns: int) -> dict[str, Any]:
    return {
        "schema": 1,
        "origin": origin,
        "accounting_from_ns": granted_ns - 2,
        "reserved_ns": granted_ns - 1,
        "granted_ns": granted_ns,
        "waiting": [],
        "release": None,
        "release_origin": None,
        "released_ns": None,
    }


def _entered(tmp_path: Path):
    services = build_test_services(tmp_path / "state")
    project = _capacity_project(tmp_path)
    handle = services.open_run(_spec(project, "timeline"))
    handle.enter()
    path = _record_path(services, handle.run_id)
    return handle, path, _record_context(services, project, handle.run_id)


def test_record_without_the_key_loads_unchanged(tmp_path: Path) -> None:
    handle, path, context = _entered(tmp_path)
    record = json.loads(path.read_text())
    record.pop("hold_timeline", None)
    atomic_json(path, record)

    loaded = lifecycle.load_run_record(context)

    assert "hold_timeline" not in loaded
    assert set(loaded) == set(json.loads(path.read_text()))
    handle.leave()


def test_hold_timeline_on_an_ungranted_record_is_refused(tmp_path: Path) -> None:
    services = build_test_services(tmp_path / "state")
    project = _capacity_project(tmp_path)
    handle = services.open_run(_spec(project, "timeline"))
    path = _record_path(services, handle.run_id)
    context = _record_context(services, project, handle.run_id)
    record = json.loads(path.read_text())
    record["hold_timeline"] = _timeline("darwin-continuous:11111111-2222-3333-4444-555555555555", 10)
    atomic_json(path, record)

    with pytest.raises(ResourceError, match="hold timeline is invalid"):
        lifecycle.load_run_record(context)
    record.pop("hold_timeline")
    atomic_json(path, record)
    handle.cancel()


def test_hold_timeline_origin_must_equal_the_grant_memory_origin(tmp_path: Path) -> None:
    handle, path, context = _entered(tmp_path)
    record = json.loads(path.read_text())
    memory = record["admission_observation"]["memory"]
    record["hold_timeline"] = _timeline(memory["origin"], memory["tick_ns"])
    atomic_json(path, record)
    assert lifecycle.load_run_record(context)["hold_timeline"] == record["hold_timeline"]

    # The records below are invalid for the guardian's own loader too: write them only while it is paused, and resume
    # it on the valid record ( its active loop reads every 50 ms).
    pid = pause_guardian(context)
    try:
        record["hold_timeline"] = _timeline(memory["origin"].replace("-", "0", 1), memory["tick_ns"])
        atomic_json(path, record)
        with pytest.raises(ResourceError, match="hold timeline is invalid"):
            lifecycle.load_run_record(context)

        record["hold_timeline"] = _timeline(memory["origin"], memory["tick_ns"] - 1)
        atomic_json(path, record)
        with pytest.raises(ResourceError, match="hold timeline is invalid"):
            lifecycle.load_run_record(context)

        record["hold_timeline"] = _timeline(_PRIOR_BOOT, memory["tick_ns"])  # a valid clock origin of another boot
        atomic_json(path, record)
        with pytest.raises(ResourceError, match="hold timeline is invalid"):
            lifecycle.load_run_record(context)

        released = _timeline(memory["origin"], memory["tick_ns"])
        released.update(release="leave", release_origin="not-a-clock-origin", released_ns=memory["tick_ns"])
        record["hold_timeline"] = released
        atomic_json(path, record)
        with pytest.raises(ResourceError, match="hold timeline is invalid"):
            lifecycle.load_run_record(context)
    finally:
        record["hold_timeline"] = _timeline(memory["origin"], memory["tick_ns"])
        atomic_json(path, record)
        resume_guardian(pid)
    handle.leave()


def _record(services, run_id: str) -> dict[str, Any]:
    return json.loads(_record_path(services, run_id).read_text())


def test_queued_run_records_its_waiting_reasons(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    sharing.record_parallel_runs(services.root / "admission", 1)  # one at a time: the waiter waits its queue position
    holder = services.open_run(_spec(projects[0], "holder"))
    holder.enter()
    waiter = services.open_run(_spec(projects[1], "waiter"))
    waiting = EnterThread(waiter)
    wait_until(lambda: waiter.status().state == "queued", message="the second run queued")
    time.sleep(1.5)  # at least one full queued pass
    holder.leave()
    waiting.join()
    assert waiting.error is None

    timeline = _record(services, waiter.run_id)["hold_timeline"]

    assert timeline["waiting"], "a queued run names why it waited"
    assert {item["reason"] for item in timeline["waiting"]} <= {"queue-position", "capacity-lock-busy"}
    for item in timeline["waiting"]:
        assert timeline["accounting_from_ns"] <= item["first_tick_ns"] <= item["last_tick_ns"] <= timeline["granted_ns"]
    waiter.leave()


def test_reserved_tick_is_read_at_reservation_not_at_grant(tmp_path: Path) -> None:
    services = build_test_services(tmp_path / "state")
    account = services.root / "admission"
    handle = services.open_run(_spec(_capacity_project(tmp_path), "timeline"))
    prepare_for_child_guardian(handle)
    guardian, _ = start_guardian_child(
        tmp_path, handle, account, {"slot-reserved": {"action": {"do": "sleep", "seconds": 2}}}
    )
    try:
        entering = EnterThread(handle, existing_attempt=True)
        entering.join(timeout=30)
        assert entering.error is None
        timeline = _record(services, handle.run_id)["hold_timeline"]
        assert timeline["granted_ns"] - timeline["reserved_ns"] >= 2_000_000_000
        handle.leave()
    finally:
        reap(guardian)


def test_every_new_grant_keeps_the_key_through_active_stopping_and_terminal(tmp_path: Path) -> None:
    services = build_test_services(tmp_path / "state")
    handle = services.open_run(_spec(_capacity_project(tmp_path), "timeline"))
    handle.enter()
    active = _record(services, handle.run_id)
    assert active["state"] == "active" and "hold_timeline" in active
    timeline = active["hold_timeline"]
    assert timeline["accounting_from_ns"] <= timeline["reserved_ns"] <= timeline["granted_ns"]
    assert timeline["origin"] == active["admission_observation"]["memory"]["origin"]
    assert timeline["waiting"] == []
    assert (timeline["release"], timeline["release_origin"], timeline["released_ns"]) == (None, None, None)
    handle.leave()
    final = _record(services, handle.run_id)
    assert final["state"] == "complete" and "hold_timeline" in final
    assert {key: final["hold_timeline"][key] for key in ("origin", "reserved_ns", "granted_ns")} == {
        key: active["hold_timeline"][key] for key in ("origin", "reserved_ns", "granted_ns")
    }
    released = final["hold_timeline"]
    assert released["release"] == "leave"
    assert released["release_origin"] == released["origin"]
    assert released["released_ns"] >= released["granted_ns"]


def _json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def _null_release(context: lifecycle.RunContext) -> None:
    record = _json(context.record)
    record["hold_timeline"].update(release=None, release_origin=None, released_ns=None)
    atomic_json(context.record, record)


def _strip_timeline(context: lifecycle.RunContext) -> None:
    record = _json(context.record)
    record.pop("hold_timeline", None)
    atomic_json(context.record, record)


def _entry_present(account: Path, run_id: str) -> bool:
    return run_id in _json(account / "queue.json")["entries"]


def test_empty_grant_recovery_records_release_recovery(tmp_path: Path) -> None:
    context, account = _empty_grant(tmp_path)
    _recover(context, account)

    timeline = _json(context.record)["hold_timeline"]

    assert timeline["release"] == "recovery" and timeline["released_ns"] >= timeline["granted_ns"]
    assert not _entry_present(account, context.run_id)


def test_resource_grant_recovery_records_release_recovery(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _handle, context, account = _resource_grant(tmp_path, monkeypatch)
    _recover(context, account)

    timeline = _json(context.record)["hold_timeline"]

    assert timeline["release"] == "recovery" and timeline["released_ns"] >= timeline["granted_ns"]
    assert not _entry_present(account, context.run_id)


def test_crash_between_release_write_and_removal_keeps_the_first_release(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    crashed = _acknowledged_then_guardian_killed(False)(tmp_path, monkeypatch)
    assert crashed.context is not None
    first = _json(crashed.context.record)["hold_timeline"]
    assert first["release"] == "leave"  # written in the acknowledgment, before the entry's removal
    assert _entry_present(crashed.account, crashed.context.run_id)

    crashed.resume()

    assert _json(crashed.context.record)["hold_timeline"] == first
    assert not _entry_present(crashed.account, crashed.context.run_id)


def test_retry_backfills_a_missing_release_before_removing_the_entry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    crashed = _acknowledged_then_guardian_killed(False)(tmp_path, monkeypatch)
    assert crashed.context is not None
    _null_release(crashed.context)

    crashed.resume()

    timeline = _json(crashed.context.record)["hold_timeline"]
    assert timeline["release"] == "recovery" and timeline["released_ns"] is not None
    assert not _entry_present(crashed.account, crashed.context.run_id)


_PRIOR_BOOT = (
    "darwin-continuous:00000000-0000-4000-8000-000000000000"
    if sys.platform == "darwin"
    else "linux-boottime:00000000-0000-4000-8000-000000000000:time:[4026531834]"
)


def test_prior_boot_recovery_writes_the_release_under_the_current_origin(tmp_path: Path) -> None:
    context, account = _empty_grant(tmp_path)
    record = _json(context.record)
    record["hold_timeline"]["origin"] = _PRIOR_BOOT
    observation = record["admission_observation"]
    for sample in (observation["memory"], observation["window_start"], *observation["samples"]):
        sample["origin"] = _PRIOR_BOOT
    for item in record["phase_history"]:
        item["memory"]["origin"] = _PRIOR_BOOT
    atomic_json(context.record, record)
    lifecycle.load_run_record(context)  # the planted prior-boot record is valid

    _recover(context, account)

    timeline = lifecycle.load_run_record(context)["hold_timeline"]
    assert timeline["release"] == "recovery"
    assert timeline["release_origin"] == lifecycle.NativeClock().origin != timeline["origin"]


def _leave_keyless(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> lifecycle.RunContext:
    services = build_test_services(tmp_path / "state")
    project = _capacity_project(tmp_path)
    handle = services.open_run(_spec(project, "timeline"))
    handle.enter()
    context = _record_context(services, project, handle.run_id)
    _strip_timeline(context)
    handle.leave()
    return context


def _empty_grant_keyless(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> lifecycle.RunContext:
    context, account = _empty_grant(tmp_path)
    _strip_timeline(context)
    _recover(context, account)
    return context


def _resource_grant_keyless(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> lifecycle.RunContext:
    _handle, context, account = _resource_grant(tmp_path, monkeypatch)
    _strip_timeline(context)
    _recover(context, account)
    return context


@pytest.mark.parametrize(
    "release",
    [_leave_keyless, _empty_grant_keyless, _resource_grant_keyless],
    ids=["leave", "empty-grant", "resource-grant"],
)
def test_keyless_record_stays_keyless_through_every_release_writer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, release
) -> None:
    context = release(tmp_path, monkeypatch)

    assert "hold_timeline" not in lifecycle.load_run_record(context)


def test_retire_completed_grant_backfills_a_missing_release(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    context, account = _crashed_terminal_resource_grant(tmp_path, monkeypatch)
    assert _entry_present(account, context.run_id)
    _null_release(context)

    _recover(context, account)

    timeline = _json(context.record)["hold_timeline"]
    assert timeline["release"] == "recovery" and timeline["released_ns"] is not None
    assert not _entry_present(account, context.run_id)


def test_retire_completed_grant_keeps_a_keyless_record_keyless(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    context, account = _crashed_terminal_resource_grant(tmp_path, monkeypatch)
    _strip_timeline(context)

    _recover(context, account)

    assert "hold_timeline" not in lifecycle.load_run_record(context)
    assert not _entry_present(account, context.run_id)


def test_retire_completed_grant_with_the_entry_already_absent_backfills_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    context, account = _crashed_terminal_resource_grant(tmp_path, monkeypatch)
    _crash_at("retire-queue-removed", lambda: _recover(context, account))
    assert not _entry_present(account, context.run_id)
    _null_release(context)

    _recover(context, account)

    timeline = _json(context.record)["hold_timeline"]
    assert (timeline["release"], timeline["release_origin"], timeline["released_ns"]) == (None, None, None)


def _leave_and_recover_one_each(tmp_path: Path):
    """One project, one registry: a run that left normally and an empty grant finished by recovery."""
    services = build_test_services(tmp_path / "state")
    project = _capacity_project(tmp_path)
    left = services.open_run(_spec(project, "left"))
    left.enter()
    left.leave()
    wait_guardians_exited(services.root / "registry")  # its guardian de-indexes the left run after leave returns
    lost = services.open_run(_spec(project, "lost")).enter()
    kill_guardian(lost._context)

    def requester_dies(stage: str) -> None:
        if stage == "leave-withdrawal-written":
            raise Interrupted

    install_failpoint(requester_dies)
    try:
        with pytest.raises(Interrupted):
            lost.leave()
    finally:
        install_failpoint(None)
    _recover(lost._context, services.root / "admission")
    return services, project, left.run_id, lost.run_id


def test_timeline_lists_runs_after_leave_and_recovery(tmp_path: Path) -> None:
    services, project, left, lost = _leave_and_recover_one_each(tmp_path)

    timeline = project_timeline(services.root / "registry", project.resolve())

    entries = {entry["run_id"]: entry for entry in timeline["runs"]}
    assert [entry["run_id"] for entry in timeline["runs"]] == sorted([left, lost])  # by run file name
    assert entries[left]["indexed"] is False and entries[lost]["indexed"] is False  # leave unindexes (DF4, X15)
    assert entries[left]["release"] == "leave" and entries[lost]["release"] == "recovery"
    segments = entries[left]["segments"]
    assert all(isinstance(segments[key], float) for key in ("queue_s", "window_s", "hold_s", "stopping_s"))
    assert timeline["unreadable"] == [] and timeline["unattributable_count"] == 0


def test_timeline_counts_but_does_not_list_unattributable_files(tmp_path: Path) -> None:
    services, project, _left, _lost = _leave_and_recover_one_each(tmp_path)
    garbage = services.root / "registry" / "runs" / "22222222-2222-4222-8222-222222222222.json"
    garbage.write_bytes(b"{not json")
    for name, content in (
        ("55555555-5555-4555-8555-555555555555", b"{}"),
        ("66666666-6666-4666-8666-666666666666", b"[]"),
    ):
        (garbage.parent / f"{name}.json").write_bytes(content)  # valid JSON without a project identity
        os.chmod(garbage.parent / f"{name}.json", 0o600)
    os.chmod(garbage, 0o600)

    timeline = project_timeline(services.root / "registry", project.resolve())

    assert timeline["unattributable_count"] == 3 and len(timeline["runs"]) == 2 and timeline["unreadable"] == []


def test_timeline_lists_unreadable_attributed_records_by_cell(tmp_path: Path) -> None:
    services, project, left, _lost = _leave_and_recover_one_each(tmp_path)
    path = services.root / "registry" / "runs" / f"{left}.json"
    record = _json(path)
    record["cleanup_outcome"] = "bogus"
    atomic_json(path, record)

    timeline = project_timeline(services.root / "registry", project.resolve())

    assert left not in {entry["run_id"] for entry in timeline["runs"]} and len(timeline["runs"]) == 1
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    assert timeline["unreadable"] == [record_cell_id("run", f"runs/{left}.json", digest)]  # by cell id only


def _tree_digest(root: Path) -> dict[str, str]:
    return {
        str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def test_timeline_takes_no_lock_and_writes_nothing(tmp_path: Path) -> None:
    services, project, _left, _lost = _leave_and_recover_one_each(tmp_path)
    before = _tree_digest(services.root)
    events: list[tuple] = []
    install_lock_observer(lambda *event: events.append(event))
    try:
        project_timeline(services.root / "registry", project.resolve())
    finally:
        install_lock_observer(None)

    assert events == []
    assert _tree_digest(services.root) == before


def test_timeline_refuses_a_missing_project_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(operator, "registry_directory", lambda: tmp_path / "registry")
    with pytest.raises(InvalidInput, match="timeline path is invalid"):
        operator.execute("project.timeline", {"project_root": str(tmp_path / "gone")}, docker_factory=object)


def test_timeline_ignores_the_archive(tmp_path: Path) -> None:
    services, project, left, _lost = _leave_and_recover_one_each(tmp_path)
    registry = services.root / "registry"
    archive = registry.parent / "registry-archive" / Context(project.resolve(), registry).project_id / "runs"
    archive.mkdir(parents=True, mode=0o700)
    (archive / f"33333333-3333-4333-8333-333333333333-{'0' * 64}.json").write_bytes(
        (registry / "runs" / f"{left}.json").read_bytes()
    )

    timeline = project_timeline(registry, project.resolve())

    assert len(timeline["runs"]) == 2


def test_timeline_resolves_a_non_canonical_input(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    services, project, _left, _lost = _leave_and_recover_one_each(tmp_path)
    monkeypatch.setattr(operator, "registry_directory", lambda: services.root / "registry")
    link = tmp_path / "link"
    link.symlink_to(project)

    canonical = operator.execute("project.timeline", {"project_root": str(project)}, docker_factory=object)
    via_link = operator.execute("project.timeline", {"project_root": str(link)}, docker_factory=object)
    dotted = operator.execute(
        "project.timeline", {"project_root": str(project / ".." / project.name)}, docker_factory=object
    )

    assert via_link == canonical == dotted and len(canonical["runs"]) == 2


def test_timeline_lists_a_relative_recorded_root_as_unreadable(tmp_path: Path) -> None:
    services, project, left, _lost = _leave_and_recover_one_each(tmp_path)
    registry = services.root / "registry"
    record = _json(registry / "runs" / f"{left}.json")
    record["project_root"] = os.path.relpath(record["project_root"])
    record["project_id"] = Context(project.resolve(), registry).project_id  # still attributed to the project
    copy = registry / "runs" / "44444444-4444-4444-8444-444444444444.json"
    copy.write_bytes(json.dumps(record).encode())
    os.chmod(copy, 0o600)

    timeline = project_timeline(registry, project.resolve())

    assert len(timeline["unreadable"]) == 1 and len(timeline["runs"]) == 2


def test_timeline_lists_a_malformed_release_as_unreadable_and_keeps_the_others(tmp_path: Path) -> None:
    services, project, left, lost = _leave_and_recover_one_each(tmp_path)
    path = services.root / "registry" / "runs" / f"{left}.json"
    record = _json(path)
    record["hold_timeline"]["release"] = []  # unhashable: must be a loader refusal, never a crash of the reader
    atomic_json(path, record)

    timeline = project_timeline(services.root / "registry", project.resolve())

    assert [entry["run_id"] for entry in timeline["runs"]] == [lost] and len(timeline["unreadable"]) == 1


def test_timeline_without_a_project_index_lists_runs_unindexed(tmp_path: Path) -> None:
    services, project, left, lost = _leave_and_recover_one_each(tmp_path)
    registry = services.root / "registry"
    (registry / "projects" / f"{Context(project.resolve(), registry).project_id}.json").unlink()

    timeline = project_timeline(registry, project.resolve())

    assert {entry["run_id"] for entry in timeline["runs"]} == {left, lost}
    assert all(entry["indexed"] is False for entry in timeline["runs"])


def test_timeline_lists_only_the_requested_project(tmp_path: Path) -> None:
    services, projects = three_private_projects(tmp_path)
    mine = services.open_run(_spec(projects[0], "mine"))
    mine.enter()
    mine.leave()
    other = services.open_run(_spec(projects[1], "other"))
    other.enter()
    other.leave()

    timeline = project_timeline(services.root / "registry", projects[0].resolve())

    assert [entry["run_id"] for entry in timeline["runs"]] == [mine.run_id]


def test_grant_and_timeline_are_one_atomic_write(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    crashed = _guardian_killed_after_grant_record(tmp_path, monkeypatch)
    assert crashed.context is not None
    record = _json(crashed.context.record)

    assert record["admission"] == "published"  # the grant record is durable, the queue entry is not yet granted
    assert record["hold_timeline"]["granted_ns"] >= record["admission_observation"]["memory"]["tick_ns"]
    crashed.resume()


def test_timeline_reports_the_verdict_scope(tmp_path: Path) -> None:
    # Share verdicts are keyed by the Git origin; a checkout without one teaches and reads nothing.
    services = build_test_services(tmp_path / "state")
    with_origin = tmp_path / "hostexecutor"
    (with_origin / ".git").mkdir(parents=True)
    (with_origin / ".git" / "config").write_text('[remote "origin"]\n\turl = git@github.com:example/project-a.git\n')
    without_origin = tmp_path / "plain"
    without_origin.mkdir()

    keyed = project_timeline(services.root / "registry", with_origin.resolve())
    unkeyed = project_timeline(services.root / "registry", without_origin.resolve())

    assert (keyed["repository"], keyed["verdict_scope"]) == ("github.com/example/project-a", "origin")
    assert (unkeyed["repository"], unkeyed["verdict_scope"]) == (None, "no-repository-origin")
