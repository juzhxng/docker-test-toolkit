"""The ``hold_timeline`` run-record key: closed shape, chronology within one origin, and the release triple."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest

from docker_test_toolkit.application import lifecycle
from docker_test_toolkit.application.lifecycle import _AdmissionWaits
from docker_test_toolkit.application.timeline import timeline_entry
from docker_test_toolkit.domain.errors import IncompleteCleanup, ResourceError
from docker_test_toolkit.domain.records import validate_failure_evidence, validate_hold_timeline
from tests.unit.test_docker_receipt_service import BUILT_IMAGE_ID, FakeDockerPort, _entered_run

ORIGIN = "darwin-continuous:11111111-2222-3333-4444-555555555555"
LATER_BOOT = "darwin-continuous:66666666-7777-8888-9999-000000000000"


def _timeline(**overrides: Any) -> dict[str, Any]:
    value: dict[str, Any] = {
        "schema": 1,
        "origin": ORIGIN,
        "accounting_from_ns": 1_000,
        "reserved_ns": 2_000,
        "granted_ns": 3_000,
        "waiting": [],
        "release": None,
        "release_origin": None,
        "released_ns": None,
    }
    value.update(overrides)
    return value


def _waiting(reason: str = "queue-position", first: int = 1_000, last: int = 1_500) -> dict[str, Any]:
    return {"reason": reason, "first_tick_ns": first, "last_tick_ns": last, "waited_ns": last - first}


@pytest.mark.parametrize(
    "value",
    [
        _timeline(),
        _timeline(waiting=[_waiting(), _waiting("incomplete-pressure-window", 2_000, 2_900)]),
        _timeline(release="leave", release_origin=ORIGIN, released_ns=4_000),
        _timeline(release="recovery", release_origin=LATER_BOOT, released_ns=5),
    ],
    ids=["empty-waiting", "two-waiting-items", "same-origin-release", "cross-origin-release"],
)
def test_valid_hold_timeline_shapes_load(value: dict[str, Any]) -> None:
    validate_hold_timeline(copy.deepcopy(value))


def _without(key: str) -> dict[str, Any]:
    value = _timeline()
    del value[key]
    return value


@pytest.mark.parametrize(
    "value",
    [
        _timeline(extra=1),
        _without("waiting"),
        _timeline(schema=2),
        _timeline(schema=True),
        _timeline(reserved_ns=True),
        _timeline(accounting_from_ns=-1),
        _timeline(accounting_from_ns=2_500),
        _timeline(reserved_ns=3_500),
        _timeline(waiting={}),
        _timeline(waiting=[{"reason": "queue-position", "first_tick_ns": 1, "last_tick_ns": 2}]),
        _timeline(waiting=[_waiting(first=1_500, last=1_000)]),
        _timeline(release="expired", release_origin=ORIGIN, released_ns=4_000),
        _timeline(release="leave", release_origin=None, released_ns=None),
        _timeline(release="leave", release_origin=ORIGIN, released_ns=2_999),
        _timeline(release=[], release_origin=ORIGIN, released_ns=4_000),
        _timeline(release={}, release_origin=ORIGIN, released_ns=4_000),
    ],
    ids=[
        "extra-key",
        "missing-key",
        "schema-2",
        "schema-bool",
        "bool-tick",
        "negative-tick",
        "accounting-after-reserved",
        "reserved-after-granted",
        "waiting-not-list",
        "waiting-item-missing-key",
        "waiting-last-before-first",
        "release-unknown",
        "partial-release",
        "same-origin-release-before-grant",
        "release-list",
        "release-object",
    ],
)
def test_malformed_hold_timeline_is_refused(value: Any) -> None:
    with pytest.raises(ResourceError, match="hold timeline is invalid"):
        validate_hold_timeline(value)


def test_empty_waiting_is_valid_here_but_never_for_failure_evidence() -> None:
    validate_hold_timeline(_timeline(waiting=[]))
    evidence = {
        "schema": 1,
        "category": "admission-expired",
        "observer": "admission",
        "reason": "queue-position",
        "detail": None,
        "first_observed": {"origin": ORIGIN, "tick_ns": 1, "state": "queued", "phase": None, "revision": 1},
        "waiting": [],
        "pressure": None,
    }
    with pytest.raises(ResourceError, match="failure evidence is invalid"):
        validate_failure_evidence(evidence)


def test_waiting_at_does_not_change_the_account() -> None:
    waits = _AdmissionWaits()
    waits.begin(100)
    waits.note("queue-position")
    first = waits.waiting_at(150)
    assert first == waits.waiting_at(150)  # repeatable: nothing was closed
    waits.close(400)  # the pass still spans its full interval after a waiting_at
    assert waits.waiting_at(400) == [
        {"reason": "queue-position", "first_tick_ns": 100, "last_tick_ns": 400, "waited_ns": 300}
    ]
    assert first == [{"reason": "queue-position", "first_tick_ns": 100, "last_tick_ns": 150, "waited_ns": 50}]


def test_waiting_at_orders_by_first_tick_then_reason() -> None:
    waits = _AdmissionWaits()
    waits.begin(10)
    waits.note("queue-position")
    waits.note("capacity-lock-busy")
    waits.begin(20)
    waits.note("incomplete-pressure-window")
    assert [item["reason"] for item in waits.waiting_at(30)] == [
        "capacity-lock-busy",
        "queue-position",
        "incomplete-pressure-window",
    ]


def test_first_tick_is_the_first_pass() -> None:
    waits = _AdmissionWaits()
    assert waits.first_tick_ns is None
    waits.begin(7)
    waits.begin(9)
    assert waits.first_tick_ns == 7
    assert waits.waiting_at(12) == []  # no reason was ever named: accounted and never waited


def _build(fake: FakeDockerPort, handle, descriptor: dict) -> dict:
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "build", "--tag", "fixture:latest", "."))
    record = json.loads(handle._context.record.read_text())
    return next(receipt for receipt in record["docker_receipts"].values() if receipt["kind"] == "build")


def test_dispatcher_finished_build_records_finished_ns(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "create"))

    receipt = _build(fake, handle, descriptor)

    assert receipt["state"] == "promoted"
    assert "finished_ns" not in receipt  # never at receipt level
    # The fake daemon reports no platform, so the cache verdict is recorded beside the finish tick.
    assert set(receipt["plan"]["cache"]) == {"eligible", "outcome", "finished_ns"}
    assert receipt["plan"]["cache"]["outcome"] == "not-eligible:platform-unknown"
    assert receipt["plan"]["cache"]["finished_ns"] >= receipt["deadline"]["created_ns"]
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("image", "rm", BUILT_IMAGE_ID))
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "rm", handle.docker_context.builder))
    handle.leave()


def test_dispatcher_failed_build_records_finished_ns(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "create"))
    fake.fail_build = True

    receipt = _build(fake, handle, descriptor)

    assert receipt["state"] == "failed" and set(receipt["plan"]["cache"]) == {"eligible", "outcome", "finished_ns"}
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "rm", handle.docker_context.builder))
    handle.leave()


def _reconciled_build(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict:
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)
    monkeypatch.setattr(lifecycle, "_BOUND_DOCKER_FACTORY", lambda _identity: fake)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "create"))
    fake.interrupt_build = True
    with pytest.raises(IncompleteCleanup):
        lifecycle.dispatch_docker(
            handle._context, descriptor, fake, ("buildx", "build", "--tag", "fixture:latest", ".")
        )
    lifecycle._reconcile_docker_receipt(handle._context)
    record = json.loads(handle._context.record.read_text())
    return next(receipt for receipt in record["docker_receipts"].values() if receipt["kind"] == "build")


def _network_create(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict:
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("network", "create"))
    record = json.loads(handle._context.record.read_text())
    return next(receipt for receipt in record["docker_receipts"].values() if receipt["kind"] == "network-create")


@pytest.mark.parametrize(
    "arrange",
    [_reconciled_build, _network_create],
    ids=["reconciled-build", "network-create"],
)
def test_receipts_without_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, arrange) -> None:
    receipt = arrange(tmp_path, monkeypatch)

    assert receipt["state"] not in {"pending", "unresolved"}
    assert "cache" not in receipt["plan"]


S = 1_000_000_000


def _deadline(created: int, origin: str = ORIGIN) -> dict[str, Any]:
    return {"origin": origin, "created_ns": created, "duration_ns": 600 * S, "expires_ns": created + 600 * S}


def _sample(tick: int, origin: str = ORIGIN) -> dict[str, Any]:
    return {"origin": origin, "tick_ns": tick}


def _run_record(**overrides: Any) -> dict[str, Any]:
    """A completed run: prepared at 100 s, reserved at 160 s, granted at 191 s, left at 300 s, released at 306 s."""
    record: dict[str, Any] = {
        "run_id": "11111111-1111-4111-8111-111111111111",
        "session_id": "s",
        "entrypoint": "integration",
        "purpose": "p",
        "state": "complete",
        "test_outcome": "passed",
        "cleanup_outcome": "complete",
        "queue_deadline": _deadline(100 * S),
        "admission_observation": {"memory": _sample(191 * S), "window_start": _sample(160 * S)},
        "phase_history": [
            {"phase": "build", "memory": _sample(191 * S)},
            {"phase": "tests", "memory": _sample(250 * S)},
        ],
        "withdrawal": {"deadline": _deadline(300 * S)},
        "docker_receipts": {},
        "hold_timeline": _timeline(
            accounting_from_ns=101 * S,
            reserved_ns=160 * S,
            granted_ns=191 * S,
            waiting=[_waiting(first=101 * S, last=160 * S)],
            release="leave",
            release_origin=ORIGIN,
            released_ns=306 * S,
        ),
    }
    record.update(overrides)
    return record


def test_timeline_entry_segments_for_a_new_record() -> None:
    entry = timeline_entry(_run_record(), indexed=False)

    assert entry["segments"] == {
        "startup_s": 1.0,
        "queue_s": 60.0,
        "window_s": 31.0,
        "active_s": 109.0,
        "stopping_s": 6.0,
        "hold_s": 146.0,
        "hold_to_leave_s": 140.0,
    }
    assert entry["reserved"] == "hold-timeline" and entry["release"] == "leave" and entry["granted"] is True
    assert entry["phases"] == [
        {"phase": "build", "start_ns": 191 * S, "seconds": 59.0},
        {"phase": "tests", "start_ns": 250 * S, "seconds": 50.0},
    ]
    assert entry["waiting"] == [_waiting(first=101 * S, last=160 * S)] and entry["notes"] == []
    assert entry["indexed"] is False


def test_timeline_entry_for_a_keyless_record_is_unrecorded() -> None:
    record = _run_record()
    del record["hold_timeline"]

    entry = timeline_entry(record, indexed=True)

    assert entry["waiting"] is None and "unrecorded" in entry["notes"]  # never shown as "no waiting" (X12)
    assert entry["reserved"] == "window-start" and entry["segments"]["queue_s"] == 60.0
    assert entry["segments"]["window_s"] == 31.0 and entry["segments"]["hold_to_leave_s"] == 140.0
    assert entry["segments"]["stopping_s"] is None and entry["segments"]["hold_s"] is None
    assert entry["segments"]["startup_s"] is None


def test_timeline_entry_recovered_without_leave() -> None:
    record = _run_record(withdrawal=None)
    record["hold_timeline"].update(release="recovery")

    entry = timeline_entry(record, indexed=False)

    assert "recovered-without-leave" in entry["notes"]
    assert {key: entry["segments"][key] for key in ("active_s", "stopping_s", "hold_to_leave_s")} == dict.fromkeys(
        ("active_s", "stopping_s", "hold_to_leave_s")
    )
    assert entry["segments"]["hold_s"] == 146.0


def test_timeline_entry_release_on_later_boot() -> None:
    record = _run_record()
    record["hold_timeline"].update(release="recovery", release_origin=LATER_BOOT, released_ns=5 * S)

    entry = timeline_entry(record, indexed=False)

    assert "release-on-later-boot" in entry["notes"]
    assert entry["segments"]["stopping_s"] is None and entry["segments"]["hold_s"] is None
    assert entry["segments"]["hold_to_leave_s"] == 140.0


def test_timeline_entry_expired_admission_shows_failure_waiting() -> None:
    waiting = [_waiting(first=101 * S, last=700 * S)]
    record = _run_record(admission_observation=None, phase_history=[], withdrawal=None, state="complete")
    del record["hold_timeline"]
    record["failure_evidence"] = {"category": "admission-expired", "waiting": waiting}

    entry = timeline_entry(record, indexed=False)

    assert entry["granted"] is False and entry["waiting"] == waiting and "unrecorded" not in entry["notes"]
    assert all(value is None for value in entry["segments"].values())


def test_timeline_entry_build_seconds_from_cache_finished_ns() -> None:
    receipts = {
        "op-1": {
            "id": "op-1",
            "kind": "build",
            "deadline": _deadline(200 * S),
            "plan": {"cache": {"finished_ns": 342 * S}},
        },
        "op-2": {"id": "op-2", "kind": "build", "deadline": _deadline(400 * S), "plan": {}},
        "op-3": {"id": "op-3", "kind": "network-create", "deadline": _deadline(410 * S), "plan": {}},
    }

    entry = timeline_entry(_run_record(docker_receipts=receipts), indexed=False)

    assert entry["builds"] == [
        {
            "operation_id": "op-1",
            "kind": "build",
            "start_ns": 200 * S,
            "seconds": 142.0,
            "cache": {"finished_ns": 342 * S},
        },
        {"operation_id": "op-2", "kind": "build", "start_ns": 400 * S, "seconds": None, "cache": None},
    ]
