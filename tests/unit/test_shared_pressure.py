"""Pressure while sharing: the grants log, "has shared", the allowances and the shared gate."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from hypothesis import given
from hypothesis import strategies as st

from docker_test_toolkit.application import lifecycle, sharing
from docker_test_toolkit.application.lifecycle import CapacityState
from docker_test_toolkit.application.recovery import atomic_json
from docker_test_toolkit.domain.errors import ResourceError
from docker_test_toolkit.domain.policy import Grant, allowance_bytes, has_shared

GIB = 1024**3
A, B, C = "a" * 8, "b" * 8, "c" * 8


def _sample(
    seconds: int, *, headroom: int = 8 * GIB, swap_used: int = 0, swapout: int = 0
) -> lifecycle.HostMemoryObservation:
    return lifecycle.HostMemoryObservation(
        "boot", seconds * 1_000_000_000, 16 * GIB, headroom, swap_used, swapout, 4096, {"swap_total_bytes": 8 * GIB}
    )


def _assess(current, *, phase=None, admission=None, **keywords) -> lifecycle.CapacityDecision:
    start = _sample(0)
    return lifecycle.assess_pressure(
        current,
        start,
        granted=True,
        admission_baseline=admission or start,
        phase_baseline=phase or start,
        host_rss_bytes=0,
        host_envelope_bytes=GIB,
        **keywords,
    )


def _grant(run_id: str, granted_s: int, budget_gib: int = 4, *, generation: int = 1, origin: str = "boot") -> Grant:
    return Grant(run_id, generation, origin, granted_s * 1_000_000_000, budget_gib * GIB)


def test_allowance_keyed_to_admission_baseline_survives_transition() -> None:
    """A's admission baseline is at 10 s; B was granted at 20 s. After A's phase transition at 40 s, B still counts."""
    grants = [_grant(B, 20)]
    allowance = allowance_bytes(grants, run_id=A, origin="boot", admission_baseline_ns=10_000_000_000)

    assert allowance == 4 * GIB
    before = _assess(_sample(60, headroom=2 * GIB), phase=_sample(30))
    after = _assess(_sample(60, headroom=2 * GIB), phase=_sample(40))
    assert before.state == after.state == CapacityState.READY  # a 6 GiB decline never fails a granted run


def test_allowance_excludes_own_grants_all_generations() -> None:
    grants = [_grant(A, 20, generation=1), _grant(A, 30, generation=2), _grant(B, 25, 1)]

    assert allowance_bytes(grants, run_id=A, origin="boot", admission_baseline_ns=10_000_000_000) == GIB


def test_allowance_ignores_grants_before_the_baseline_and_of_another_origin() -> None:
    grants = [_grant(B, 5), _grant(C, 20, origin="other-boot")]

    assert allowance_bytes(grants, run_id=A, origin="boot", admission_baseline_ns=10_000_000_000) == 0


def test_admitting_peer_never_makes_has_shared() -> None:
    """A peer that is only admitting (never published) leaves no grant and is no co-holder."""
    assert not has_shared(
        coholder_seen=False, grants=[], run_id=A, generation=1, origin="boot", admission_baseline_ns=10
    )
    floor = _assess(_sample(60, headroom=GIB), shared_gate=False)
    assert (floor.state, floor.reason) == (CapacityState.REFUSED, "native-headroom")


@pytest.mark.parametrize(
    ("coholder_seen", "grants", "expected"),
    [
        (True, [], True),  # a co-holder in this observation section
        (False, [_grant(A, 20)], True),  # its own current grant: granted beside a co-holder
        (False, [_grant(A, 20, generation=2)], False),  # an own grant of another generation
        (False, [_grant(B, 20)], True),  # another run granted after its admission baseline
        (False, [_grant(B, 5)], False),  # granted before its baseline
        (False, [_grant(B, 20, origin="other-boot")], False),
    ],
)
def test_has_shared_cases(coholder_seen: bool, grants: list[Grant], expected: bool) -> None:
    assert (
        has_shared(
            coholder_seen=coholder_seen,
            grants=grants,
            run_id=A,
            generation=1,
            origin="boot",
            admission_baseline_ns=10_000_000_000,
        )
        is expected
    )


def test_departure_keeps_shared_gate() -> None:
    """B left: no co-holder is seen, but B's grant after A's baseline keeps A shared (floor and swap-out excused)."""
    grants = [_grant(B, 20)]
    shared = has_shared(
        coholder_seen=False, grants=grants, run_id=A, generation=1, origin="boot", admission_baseline_ns=10_000_000_000
    )

    assert shared
    floor = _assess(_sample(60, headroom=GIB), shared_gate=shared)
    swapping = _assess(_sample(60, swapout=60 * GIB), shared_gate=shared)
    assert floor.state == swapping.state == CapacityState.READY


def test_host_decline_others_caused_never_fails_a_granted_run() -> None:
    """8 GiB of headroom gone since the phase began, no allowance: later runs wait, this one is not failed."""
    decision = _assess(_sample(60, headroom=2 * GIB), phase=_sample(30, headroom=10 * GIB))

    assert decision.state == CapacityState.READY


def test_growth_beyond_allowance_still_fails_and_within_passes() -> None:
    within = _assess(_sample(60, swap_used=6 * GIB), growth_allowance_bytes=4 * GIB)
    beyond = _assess(_sample(60, swap_used=8 * GIB), growth_allowance_bytes=4 * GIB)

    assert within.state == CapacityState.READY
    assert (beyond.state, beyond.reason) == (CapacityState.REFUSED, "admission-swap-growth")


def test_own_envelope_breach_fails_even_when_shared() -> None:
    decision = lifecycle.assess_pressure(
        _sample(60),
        _sample(0),
        granted=True,
        admission_baseline=_sample(0),
        phase_baseline=_sample(0),
        host_rss_bytes=2 * GIB,
        host_envelope_bytes=GIB,
        growth_allowance_bytes=64 * GIB,
        shared_gate=True,
    )

    assert (decision.state, decision.reason) == (CapacityState.REFUSED, "host-process-envelope")


def test_shared_gate_never_relaxes_admission() -> None:
    """Joiners still wait on the floor: the gate applies to granted runs only."""
    decision = lifecycle.assess_pressure(_sample(60, headroom=GIB), _sample(0), shared_gate=True)

    assert (decision.state, decision.reason) == (CapacityState.WAITING, "native-headroom")


_samples = st.builds(
    lambda tick, headroom, swap_used, swapout: lifecycle.HostMemoryObservation(
        "boot", tick, 16 * GIB, headroom, swap_used, swapout, 4096, {}
    ),
    st.integers(0, 120 * 10**9),
    st.integers(0, 16 * GIB),
    st.integers(0, 16 * GIB),
    st.integers(0, 64 * GIB),
)


@given(_samples, _samples, _samples, _samples, st.integers(0, 4 * GIB), st.integers(0, 4 * GIB), st.booleans())
def test_never_shared_unchanged(current, window, admission, phase, rss, envelope, granted) -> None:
    """The defaults are a run that never shared: explicit zero allowances and no gate give the same decision."""
    arguments = {
        "granted": granted,
        "admission_baseline": admission,
        "phase_baseline": phase,
        "host_rss_bytes": rss,
        "host_envelope_bytes": envelope,
    }

    assert lifecycle.assess_pressure(current, window, **arguments) == lifecycle.assess_pressure(
        current, window, **arguments, growth_allowance_bytes=0, shared_gate=False
    )


def _account(tmp_path: Path) -> Path:
    account = tmp_path / "account"
    account.mkdir(mode=0o700)
    return account


def test_read_grants_absent_is_empty(tmp_path: Path) -> None:
    assert sharing.read_grants(_account(tmp_path)) == []


@pytest.mark.parametrize(
    "document",
    [
        {"schema": 2, "grants": []},
        {"schema": 1, "grants": [], "extra": 1},
        {"schema": 1, "grants": {}},
        {"schema": 1, "grants": [{"run_id": A, "generation": 1, "origin": "boot", "granted_ns": 1}]},
        {
            "schema": 1,
            "grants": [{"run_id": A, "generation": True, "origin": "boot", "granted_ns": 1, "budget_bytes": 1}],
        },
        {"schema": 1, "grants": [{"run_id": A, "generation": 1, "origin": "", "granted_ns": 1, "budget_bytes": 1}]},
        {
            "schema": 1,
            "grants": [{"run_id": A, "generation": 1, "origin": "boot", "granted_ns": -1, "budget_bytes": 1}],
        },
    ],
)
def test_unreadable_grants_log_no_shared_breach(tmp_path: Path, document: dict) -> None:
    """An invalid log raises at the codec; the observer then treats the run as shared with no shared-only breach."""
    account = _account(tmp_path)
    atomic_json(account / sharing.GRANTS_FILE, document)

    with pytest.raises(ResourceError):
        sharing.read_grants(account)
    excused = lifecycle._shared_pressure_terms(None, coholder_seen=False, record=_record(A, 1, 10))
    decision = _assess(
        _sample(60, headroom=GIB, swap_used=12 * GIB),
        phase=_sample(30, headroom=8 * GIB),
        growth_allowance_bytes=excused.allowance_bytes,
        shared_gate=excused.shared,
    )
    assert excused.unreadable and decision.state == CapacityState.READY


def _record(run_id: str, generation: int, baseline_s: int) -> dict:
    return {
        "run_id": run_id,
        "generation": generation,
        "admission_observation": {"memory": {"origin": "boot", "tick_ns": baseline_s * 1_000_000_000}},
    }


def test_shared_pressure_terms_count_grants_after_the_baseline() -> None:
    terms = lifecycle._shared_pressure_terms(
        [_grant(B, 20), _grant(C, 5), _grant(A, 12)], coholder_seen=False, record=_record(A, 1, 10)
    )

    assert (terms.shared, terms.allowance_bytes, terms.unreadable) == (True, 4 * GIB, False)


def test_append_grant_upserts_by_run_generation_origin(tmp_path: Path) -> None:
    account = _account(tmp_path)
    baselines = {A: ("boot", 0), B: ("boot", 0)}
    sharing.append_grant(account, _grant(B, 20, 4), holder_baselines=baselines)
    sharing.append_grant(account, _grant(B, 20, 4), holder_baselines=baselines)
    sharing.append_grant(account, _grant(B, 30, 4, generation=2), holder_baselines=baselines)

    assert sharing.read_grants(account) == [_grant(B, 20, 4), _grant(B, 30, 4, generation=2)]
    assert allowance_bytes(sharing.read_grants(account), run_id=A, origin="boot", admission_baseline_ns=0) == 8 * GIB


def test_grants_from_other_origin_pruned(tmp_path: Path) -> None:
    account = _account(tmp_path)
    sharing.append_grant(account, _grant(B, 20, origin="old-boot"), holder_baselines={B: ("old-boot", 0)})
    sharing.append_grant(account, _grant(C, 3), holder_baselines={A: ("boot", 1), C: ("boot", 2)})

    assert sharing.read_grants(account) == [_grant(C, 3)]


def test_append_grant_prunes_records_older_than_earliest_active_baseline(tmp_path: Path) -> None:
    """A departed run's grant before the earliest holder baseline goes; a holder's older grant and newer ones stay."""
    account = _account(tmp_path)
    atomic_json(
        account / sharing.GRANTS_FILE,
        {
            "schema": 1,
            "grants": [
                {"run_id": B, "generation": 1, "origin": "boot", "granted_ns": 5, "budget_bytes": 1},  # departed, old
                {"run_id": A, "generation": 1, "origin": "boot", "granted_ns": 6, "budget_bytes": 1},  # holder
                {"run_id": "d" * 8, "generation": 1, "origin": "boot", "granted_ns": 15, "budget_bytes": 1},
            ],
        },
    )
    sharing.append_grant(
        account,
        Grant(C, 1, "boot", 20, 1),
        holder_baselines={A: ("boot", 10), C: ("boot", 18)},
    )

    assert [grant.run_id for grant in sharing.read_grants(account)] == [A, "d" * 8, C]
    assert json.loads((account / sharing.GRANTS_FILE).read_text())["schema"] == 1


def _record_with_grant(run_id: str) -> dict:
    return {
        "run_id": run_id,
        "generation": 1,
        "hold_timeline": {"origin": "boot", "granted_ns": 20 * 1_000_000_000},
    }


def test_repair_raises_on_a_failed_write_and_publication_does_not(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A readable log that cannot be rewritten: the repair before an entry's removal raises (the entry stays), the
    publication, whose grant is already written, carries on."""
    from docker_test_toolkit.domain.models import PhaseBudget

    account = _account(tmp_path)
    sharing.append_grant(account, _grant(C, 3), holder_baselines={})
    budgets = {"tests": PhaseBudget(GIB, GIB, 1, 1)}

    def full_disk(_path: Path, _document: object) -> None:
        raise OSError(28, "No space left on device")

    # I/O fault at the write boundary (a permission change does not stop root, which CI containers run as)
    monkeypatch.setattr(sharing, "atomic_json", full_disk)
    with pytest.raises(OSError):
        lifecycle._log_grant(account, _record_with_grant(B), budgets, {}, strict=True)
    lifecycle._log_grant(account, _record_with_grant(B), budgets, {}, strict=False)
    monkeypatch.undo()

    assert sharing.read_grants(account) == [_grant(C, 3)]
