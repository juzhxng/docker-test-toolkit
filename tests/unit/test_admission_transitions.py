"""Pure queue transitions on the logical model: enroll, reserve, grant, remove, holder."""

from __future__ import annotations

import pytest

from docker_test_toolkit.application import admission
from docker_test_toolkit.application.admission import QueueState
from docker_test_toolkit.domain.errors import ResourceError

RUN_A = "00000001-0000-4000-8000-000000000000"
RUN_B = "00000002-0000-4000-8000-000000000000"
RUN_C = "00000003-0000-4000-8000-000000000000"
GUARDIAN = {"pid": 7}
REFERENCE = {
    "attempt": "00000001-1111-4111-8111-111111111111",
    "record": f"/tmp/registry/runs/{RUN_A}.json",
    "registry": "/tmp/registry",
    "project_root": "/tmp/project",
}


def _state() -> QueueState:
    return QueueState(next_ticket=5, entries={})


def test_enroll_takes_the_next_ticket_and_starts_queued() -> None:
    state = _state()
    ticket = admission.enroll(state, RUN_A, REFERENCE)
    assert ticket == 5 and state.next_ticket == 6
    entry = state.entries[RUN_A]
    assert (entry.phase, entry.holder, entry.dead_since, entry.takeover, entry.last_completed_phase) == (
        "queued",
        None,
        None,
        None,
        None,
    )
    assert entry.project_id and entry.ticket == 5
    assert admission.reference(entry) == {**REFERENCE, "ticket": 5}


def test_enroll_is_idempotent_for_the_same_reference_and_refuses_another() -> None:
    state = _state()
    assert admission.enroll(state, RUN_A, REFERENCE) == 5
    assert admission.enroll(state, RUN_A, REFERENCE) == 5
    assert state.next_ticket == 6  # a retry keeps its ticket
    with pytest.raises(ResourceError, match="changed ownership"):
        admission.enroll(state, RUN_A, {**REFERENCE, "attempt": "00000009-1111-4111-8111-111111111111"})


def test_reserve_then_grant_makes_exactly_one_holder() -> None:
    state = _state()
    admission.enroll(state, RUN_A, REFERENCE)
    admission.enroll(state, RUN_B, {**REFERENCE, "record": f"/tmp/registry/runs/{RUN_B}.json"})
    assert admission.holders(state) == []
    admission.reserve(state, RUN_A, GUARDIAN)
    assert admission.holders(state) == [state.entries[RUN_A]]
    assert (state.entries[RUN_A].phase, state.entries[RUN_A].holder) == ("admitting", GUARDIAN)
    with pytest.raises(ResourceError, match="capacity is full"):
        admission.reserve(state, RUN_B, {"pid": 8})
    admission.grant(state, RUN_A, GUARDIAN)
    assert state.entries[RUN_A].phase == "granted"


def test_reserve_at_limit_two_admits_second_and_refuses_third() -> None:
    state = _state()
    run_c = "00000003-0000-4000-8000-000000000003"
    for run_id in (RUN_A, RUN_B, run_c):
        admission.enroll(state, run_id, {**REFERENCE, "record": f"/tmp/registry/runs/{run_id}.json"})
    admission.reserve(state, RUN_A, GUARDIAN, limit=2)
    admission.grant(state, RUN_A, GUARDIAN)
    admission.reserve(state, RUN_B, {"pid": 8}, limit=2)

    with pytest.raises(ResourceError, match="capacity is full"):
        admission.reserve(state, run_c, {"pid": 9}, limit=2)

    assert [entry.run_id for entry in admission.holders(state)] == [RUN_A, RUN_B]
    assert state.entries[run_c].phase == "queued" and state.entries[run_c].holder is None


def test_reserve_auto_never_full() -> None:
    state = _state()
    run_ids = [f"0000000{index}-0000-4000-8000-00000000000{index}" for index in range(1, 6)]
    for run_id in run_ids:
        admission.enroll(state, run_id, {**REFERENCE, "record": f"/tmp/registry/runs/{run_id}.json"})
        admission.reserve(state, run_id, {"pid": int(run_id[7])}, limit=None)

    assert len(admission.holders(state)) == 5


def test_grant_requires_the_reserving_guardian() -> None:
    state = _state()
    admission.enroll(state, RUN_A, REFERENCE)
    with pytest.raises(ResourceError, match="holder"):
        admission.grant(state, RUN_A, GUARDIAN)
    admission.reserve(state, RUN_A, GUARDIAN)
    with pytest.raises(ResourceError, match="holder"):
        admission.grant(state, RUN_A, {"pid": 99})


def test_remove_releases_the_slot_and_returns_the_entry() -> None:
    state = _state()
    admission.enroll(state, RUN_A, REFERENCE)
    admission.reserve(state, RUN_A, GUARDIAN)
    removed = admission.remove(state, RUN_A)
    assert removed is not None and removed.run_id == RUN_A
    assert admission.holders(state) == [] and state.entries == {}
    assert admission.remove(state, RUN_A) is None


def _other_project(run_id: str, root: str) -> dict:
    return {**REFERENCE, "record": f"/tmp/registry/runs/{run_id}.json", "project_root": root}


def test_reserve_refuses_a_full_account() -> None:
    state = _state()
    for run_id, root in ((RUN_A, "/tmp/project-a"), (RUN_B, "/tmp/project-b")):
        admission.enroll(state, run_id, _other_project(run_id, root))
    admission.reserve(state, RUN_A, GUARDIAN)

    with pytest.raises(ResourceError, match="capacity is full"):  # the one holder admits no one, any project
        admission.reserve(state, RUN_B, {"pid": 8})
    assert state.entries[RUN_B].phase == "queued" and admission.holders(state) == [state.entries[RUN_A]]
