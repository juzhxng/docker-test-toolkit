"""Pure shared-admission policy: reservation and publication predicates, M1 and attribution."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, precondition, rule

from docker_test_toolkit.application import admission, lifecycle
from docker_test_toolkit.domain.errors import ResourceError
from docker_test_toolkit.domain.models import PhaseBudget
from docker_test_toolkit.domain.policy import (
    BUILDER_MEMORY_BYTES,
    RUN_HOST_LABEL,
    HolderFacts,
    PhaseReservation,
    QueueEntryView,
    attributed_ids,
    external_commitments,
    net_external_limits,
    next_grantable,
    publication_verdict,
    reservation,
    reservation_verdict,
    version_at_least,
)

GIB = 1024**3
RUN_LABEL = "dev.docker-test-toolkit.installed.run"


def _view(
    run_id: str,
    ticket: int,
    phase: str,
    *,
    mode: str | None = "shared",
    state: str | None = "executing",
    blocked: bool = False,
    version_ok: bool = True,
) -> QueueEntryView:
    return QueueEntryView(run_id, ticket, phase, "p1", blocked, state, mode, version_ok)


_views = st.lists(
    st.builds(
        _view,
        st.sampled_from(["a", "b", "c", "d", "e"]),
        st.integers(1, 9),
        st.sampled_from(["queued", "admitting", "granted"]),
        mode=st.sampled_from(["shared", "exclusive", None]),
        state=st.sampled_from(["executing", "absent", "replaced", "zombie", None]),
        blocked=st.booleans(),
        version_ok=st.booleans(),
    ),
    max_size=6,
    unique_by=(lambda view: view.run_id, lambda view: view.ticket),
)


@given(entries=_views, head_mode=st.sampled_from(["shared", "exclusive"]))
def test_rule0_limit_one_equals_today(entries: list[QueueEntryView], head_mode: str) -> None:
    verdict = reservation_verdict(entries, head_mode=head_mode, limit=1)

    assert verdict.run_id == next_grantable(entries)
    assert verdict.reason == (None if verdict.run_id is not None else "queue-position")


def test_shared_head_joins_shared_holders_below_limit() -> None:
    entries = [_view("a", 1, "granted"), _view("b", 2, "queued"), _view("c", 3, "queued")]

    assert reservation_verdict(entries, head_mode="shared", limit=2).run_id == "b"
    assert reservation_verdict(entries, head_mode="shared", limit=None).run_id == "b"


def test_exclusive_head_waits_for_every_holder() -> None:
    entries = [_view("a", 1, "granted"), _view("b", 2, "queued")]

    assert reservation_verdict(entries, head_mode="exclusive", limit=3).run_id is None
    assert reservation_verdict(entries, head_mode="exclusive", limit=3).reason == "exclusive-holder"
    assert reservation_verdict([_view("b", 2, "queued")], head_mode="exclusive", limit=3).run_id == "b"


def test_shared_head_waits_beside_exclusive_holder() -> None:
    entries = [_view("a", 1, "granted", mode="exclusive"), _view("b", 2, "queued")]

    verdict = reservation_verdict(entries, head_mode="shared", limit=3)

    assert (verdict.run_id, verdict.reason) == (None, "exclusive-holder")


def test_unreadable_holder_record_blocks_join() -> None:
    entries = [_view("a", 1, "granted", mode=None), _view("b", 2, "queued")]

    verdict = reservation_verdict(entries, head_mode="shared", limit=3)

    assert (verdict.run_id, verdict.reason) == (None, "exclusive-holder")


def test_admitting_entry_counts_against_limit() -> None:
    entries = [_view("a", 1, "granted"), _view("b", 2, "admitting"), _view("c", 3, "queued")]

    verdict = reservation_verdict(entries, head_mode="shared", limit=2)

    assert (verdict.run_id, verdict.reason) == (None, "parallel-limit")
    assert reservation_verdict(entries[1:], head_mode="shared", limit=2).run_id == "c"


@pytest.mark.parametrize("older", ["holder", "queued"])
def test_older_version_holder_or_queued_entry_blocks_join(older: str) -> None:
    entries = [
        _view("a", 1, "granted", version_ok=older != "holder"),
        _view("b", 2, "queued"),
        _view("c", 3, "queued", version_ok=older != "queued"),
    ]

    verdict = reservation_verdict(entries, head_mode="shared", limit=3)

    assert (verdict.run_id, verdict.reason) == (None, "older-version-entry")


@pytest.mark.parametrize(
    ("entries", "publisher", "mode", "limit", "expected"),
    [
        ([_view("a", 1, "admitting"), _view("b", 2, "admitting")], "b", "shared", 2, "queue-position"),
        ([_view("a", 1, "admitting"), _view("b", 2, "admitting")], "a", "shared", 1, None),
        ([_view("a", 1, "admitting"), _view("b", 2, "admitting")], "b", "shared", 1, "queue-position"),
        ([_view("a", 1, "granted"), _view("b", 2, "admitting")], "b", "shared", 1, "queue-position"),
        (
            [_view("a", 1, "granted"), _view("b", 2, "granted"), _view("c", 3, "admitting")],
            "c",
            "shared",
            2,
            "parallel-limit",
        ),
        ([_view("a", 1, "granted"), _view("b", 2, "admitting")], "b", "exclusive", 2, "exclusive-holder"),
        ([_view("a", 1, "admitting", state="absent"), _view("b", 2, "admitting")], "b", "shared", 2, None),
    ],
    ids=[
        "earlier-admitting",
        "lowered-to-one-earliest",
        "lowered-to-one-later",
        "lowered-to-one-after-grant",
        "limit-reached",
        "exclusive-publisher",
        "dead-earlier-admitting",
    ],
)
def test_publication_counts_granted_coholders_and_waits_behind_earlier_admitting(
    entries: list[QueueEntryView], publisher: str, mode: str, limit: int, expected: str | None
) -> None:
    assert publication_verdict(entries, publisher=publisher, publisher_mode=mode, limit=limit) == expected


def test_auto_has_no_count_cap() -> None:
    entries = [_view(f"h{index}", index, "granted") for index in range(1, 51)] + [_view("w", 99, "queued")]

    assert reservation_verdict(entries, head_mode="shared", limit=None).run_id == "w"
    assert (
        publication_verdict(
            [*entries[:-1], _view("w", 99, "admitting")], publisher="w", publisher_mode="shared", limit=None
        )
        is None
    )


def test_version_at_least() -> None:
    assert version_at_least("0.6.0", "0.6.0") and version_at_least("0.10.0", "0.6.0")
    assert version_at_least("1.0.0", "0.6.0") and version_at_least("0.6.1-rc.1", "0.6.0")
    for older in ("0.6.0-rc.1", "0.6.0+build.1", "0.5.9", None, "x", "0.6", 6):
        assert not version_at_least(older, "0.6.0"), older


def _container(identifier: str, memory: int | None, *, project: str | None = None, run: str | None = None, name=None):
    labels = {}
    if project is not None:
        labels["com.docker.compose.project"] = project
    if run is not None:
        labels[RUN_LABEL] = run
    return {
        "Id": identifier * 64,
        "Name": "/" + (name or identifier),
        "State": {"Running": True},
        "HostConfig": {"Memory": memory},
        "Config": {"Labels": labels},
    }


def test_a_holders_builder_worker_is_attributed_by_name_within_its_reservation() -> None:
    """The BuildKit worker carries no label; it counts as the holder's by its name, inside the reservation."""
    holder = HolderFacts("r1", PhaseReservation(7 * GIB, 0, 2, 1), "dtt-p-r1", "buildx_buildkit_dtt-p-r10")
    containers = [
        _container("a", 2 * GIB, project="dtt-p-r1"),
        _container("b", GIB, run="r1"),
        _container("c", GIB, project="dtt-p-r1"),
        _container("d", BUILDER_MEMORY_BYTES, name="buildx_buildkit_dtt-p-r10"),
        _container("e", GIB, project="dtt-other"),  # a foreign container
        _container("f", 0, project="dtt-p-r1"),  # unlimited (Docker reports 0): stays external
        _container("0", GIB, project="dtt-other"),  # a second foreign container of the same size
        _container("1", GIB, project="dtt-p-r1"),  # the holder's, and labelled as a run host
    ]
    containers[-1]["Config"]["Labels"][RUN_HOST_LABEL] = "true"

    built = attributed_ids(containers, holder, run_label=RUN_LABEL)

    assert built == {"a" * 64, "b" * 64, "c" * 64, "d" * 64}  # "1" is a run host: never in a sum, never attributed
    limits = external_commitments(containers).limits
    external = net_external_limits(limits, containers, built)  # "1" was never in the sum: nothing subtracted for it
    assert sorted(item for item in external if item is not None) == [GIB, GIB]
    assert None in external


def test_a_holders_reservation_is_the_maximum_of_its_phases_and_all_its_containers_are_attributed() -> None:
    """A holder with a builder reserves the maximum of its phases, and every one of its containers is attributed."""
    e2e = {
        "build": PhaseBudget(2 * GIB, 25 * GIB // 4, 2, 1),
        "start": PhaseBudget(3425236418, 25 * GIB // 4, 2, 1),
        "tests": PhaseBudget(3350000000, 25 * GIB // 4, 2, 2),
    }
    record = {
        "phases": {name: vars(budget) for name, budget in e2e.items()},
        "initial_phase": "build",
        "grant": {"phase": "tests"},
        "owned_resources": {"builders": {"worker": {"state": "present"}}},
    }
    holder = lifecycle._holder_facts(
        "e0572458-0000-0000-0000-000000000000", SimpleNamespace(project_id="6" * 64), record
    )
    containers = [
        {
            "Id": f"{index:064x}",
            "Name": f"/{holder.compose_project}-{name}-1",
            "State": {"Running": True},
            "Config": {"Labels": {"com.docker.compose.project": holder.compose_project}},
            "HostConfig": {"Memory": limit},
        }
        for index, (name, limit) in enumerate(
            (("opensearch", 3 * GIB // 2), ("neo4j", 5 * GIB // 4), ("postgres", 384 * 1024**2))
        )
    ]

    assert len(attributed_ids(containers, holder, run_label="docker-test-toolkit.run")) == 3
    assert holder.reservation == reservation(e2e.values())


class SharedQueueMachine(RuleBasedStateMachine):
    """A queue driven only through ``reservation_verdict``, ``publication_verdict`` and the ``reserve`` codec.

    Every run is shared, so the limit alone bounds the holders. Invariants: grants happen in ticket order, the head is
    never passed, holder entries never exceed the limit at a reservation, and the queue drains when everyone leaves.
    """

    def __init__(self) -> None:
        super().__init__()
        self.state = admission.QueueState(1, {})
        self.limit: int | None = 1
        self.dead: set[str] = set()
        self.granted_order: list[int] = []
        self.count = 0

    def _views(self) -> list[QueueEntryView]:
        return [
            _view(entry.run_id, entry.ticket, entry.phase, state="absent" if entry.run_id in self.dead else "executing")
            for entry in self.state.entries.values()
        ]

    @rule()
    def enroll(self) -> None:
        self.count += 1
        run_id = f"{self.count:08d}-0000-4000-8000-000000000000"
        admission.enroll(
            self.state,
            run_id,
            {"attempt": run_id, "record": f"/r/{run_id}.json", "registry": "/r", "project_root": "/p"},
        )

    @rule()
    def reserve(self) -> None:
        verdict = reservation_verdict(self._views(), head_mode="shared", limit=self.limit)
        if verdict.run_id is None:
            return
        queued = [e for e in self._views() if e.phase == "queued" and e.run_id not in self.dead]
        assert verdict.run_id == min(queued, key=lambda e: e.ticket).run_id  # never past the head
        holding = len(admission.holders(self.state))
        assert self.limit is None or holding < self.limit
        admission.reserve(self.state, verdict.run_id, {"pid": 1}, limit=self.limit)

    @rule(index=st.integers(0, 7))
    def publish(self, index: int) -> None:
        admitting = sorted(
            (e for e in self.state.entries.values() if e.phase == "admitting"), key=lambda entry: entry.ticket
        )
        if not admitting:
            return
        entry = admitting[index % len(admitting)]
        if entry.run_id in self.dead:
            return
        if publication_verdict(self._views(), publisher=entry.run_id, publisher_mode="shared", limit=self.limit):
            return
        admission.grant(self.state, entry.run_id, {"pid": 1})
        self.granted_order.append(entry.ticket)

    @rule(index=st.integers(0, 7))
    def leave(self, index: int) -> None:
        holding = admission.holders(self.state)
        if holding:
            admission.remove(self.state, holding[index % len(holding)].run_id)

    @precondition(lambda self: any(e.phase == "queued" for e in self.state.entries.values()))
    @rule(index=st.integers(0, 7))
    def die(self, index: int) -> None:
        queued = [e for e in self.state.entries.values() if e.phase == "queued"]
        self.dead.add(queued[index % len(queued)].run_id)

    @rule(limit=st.sampled_from([1, 2, 3, None]))
    def set_limit(self, limit: int | None) -> None:
        self.limit = limit

    @invariant()
    def grants_in_ticket_order(self) -> None:
        assert self.granted_order == sorted(self.granted_order)

    def teardown(self) -> None:
        # liveness: once nobody new arrives and every holder leaves in turn, every live entry is granted and drains
        for _ in range(4 * len(self.state.entries) + 4):
            for entry in list(self.state.entries.values()):
                if entry.run_id in self.dead and entry.phase == "queued":
                    admission.remove(self.state, entry.run_id)
            self.reserve()
            for _ in range(len(self.state.entries)):
                self.publish(0)
            granted = [e for e in self.state.entries.values() if e.phase == "granted"]
            if granted:
                admission.remove(self.state, granted[0].run_id)
        assert not self.state.entries


TestSharedQueueMachine = SharedQueueMachine.TestCase
TestSharedQueueMachine.settings = settings(max_examples=150, stateful_step_count=40, deadline=None)


# ---- lifecycle helpers of the shared admission ---------------------------------------------------------------


def _read(revision: int) -> lifecycle.CoholderRead:
    holder = HolderFacts("r", PhaseReservation(GIB, 0, 1, 1), "dtt-p-r", "buildx_buildkit_dtt-p-r0")
    return lifecycle.CoholderRead((holder,), False, GIB, (("r", revision),))


def test_coholder_gather_unstable_after_three_attempts() -> None:
    reads = iter([_read(1), _read(2), _read(3), _read(4)])

    with pytest.raises(ResourceError) as unstable:
        lifecycle._gather_coholders(lambda: next(reads))

    assert unstable.value.reason == "coholder-records-unstable"


def test_coholder_gather_settles_on_consistent_read() -> None:
    reads = iter([_read(1), _read(2), _read(2)])

    assert lifecycle._gather_coholders(lambda: next(reads)) == _read(2)


@pytest.mark.parametrize(
    ("purpose", "relationship", "expected"),
    [
        ("tests", "inspect", False),
        ("tests", "connect_owned_container", True),
        (lifecycle.RELEASE_PURPOSE, None, True),
    ],
    ids=["ordinary", "connect-owned-container", "release-purpose"],
)
def test_static_exclusivity_despite_positive_verdict(purpose: str, relationship: str | None, expected: bool) -> None:
    resources = [] if relationship is None else [{"relationship": relationship}]

    assert lifecycle._static_exclusive({"purpose": purpose, "external_resources": resources}) is expected


def _snapshot(*, cpus: int = 4, external: tuple[int | None, ...] = ()) -> lifecycle.CapacitySnapshot:
    return lifecycle.CapacitySnapshot(
        8 * GIB, cpus, 32 * GIB, 8, "native", external, host_reserve_bytes=GIB, runtime="docker-engine"
    )


def test_admission_decision_waits_on_a_cpu_only_coholder_shortfall() -> None:
    phases = {"tests": PhaseBudget(GIB, 0, 2, 1)}

    together = lifecycle.admission_decision(phases, _snapshot(cpus=3), [PhaseReservation(GIB, 0, 2, 1)])
    alone_too_big = lifecycle.admission_decision({"tests": PhaseBudget(GIB, 0, 4, 1)}, _snapshot(cpus=3), [])

    assert (together.state, together.reason) == (lifecycle.CapacityState.WAITING, "coholder-capacity")
    assert (alone_too_big.state, alone_too_big.reason) == (lifecycle.CapacityState.REFUSED, "insufficient-cpu")
    assert lifecycle.admission_decision(phases, _snapshot(cpus=4), [PhaseReservation(GIB, 0, 2, 1)]).state == (
        lifecycle.CapacityState.READY
    )


def test_shared_publisher_alone_checks_the_maximum_of_its_phases() -> None:
    """A shared run reserves the field-wise maximum of its phases even with no co-holder; an exclusive one checks
    each phase alone."""
    phases = {"build": PhaseBudget(4 * GIB, 0, 2, 1), "tests": PhaseBudget(GIB, 6 * GIB, 1, 1)}  # E = (4, 6) GiB
    snapshot = lifecycle.CapacitySnapshot(
        6 * GIB, 4, 10 * GIB, 8, "native", (), host_reserve_bytes=GIB, runtime="docker-engine"
    )

    shared = lifecycle.admission_decision(phases, snapshot, [], shared=True)
    exclusive = lifecycle.admission_decision(phases, snapshot, [])

    assert shared.state == lifecycle.CapacityState.REFUSED  # nobody can leave to make room: never a hang
    assert shared.reason != "coholder-capacity"
    assert exclusive.state == lifecycle.CapacityState.READY


def test_shared_publisher_alone_under_external_pressure_keeps_its_own_reason() -> None:
    """Every phase fits alone, E(self) (the maximum of its phases) fits the machine but not beside the external load:
    the alone shared decision is that pressure's own WAITING, never ``coholder-capacity``."""
    phases = {"build": PhaseBudget(2 * GIB, 0, 2, 1), "tests": PhaseBudget(GIB, 4 * GIB, 1, 1)}  # E = (2, 4) GiB
    pressed = lifecycle.CapacitySnapshot(
        8 * GIB, 4, 11 * GIB, 8, "native", (4 * GIB,), host_reserve_bytes=GIB, runtime="docker-engine"
    )
    assert lifecycle.admission_decision(phases, pressed, []).state == lifecycle.CapacityState.READY  # phase alone

    shared = lifecycle.admission_decision(phases, pressed, [], shared=True)

    assert shared.state == lifecycle.CapacityState.WAITING
    assert shared.reason != "coholder-capacity"


def test_publication_verdict_without_the_publisher_raises_value_error() -> None:
    views = [_view("a", 1, "granted")]

    with pytest.raises(ValueError, match="no queue entry"):
        publication_verdict(views, publisher="missing", publisher_mode="shared", limit=2)


@pytest.mark.parametrize(
    ("unknown", "expected"),
    [
        (True, (lifecycle.CapacityState.REFUSED, "coholder-record-unreadable")),
        (False, (lifecycle.CapacityState.READY, "static-capacity")),
    ],
)
def test_after_the_grant_only_an_unreadable_coholder_record_ends_the_observer(unknown: bool, expected: tuple) -> None:
    """A granted run is judged on the toolkit's own records, never on the capacity sums of the moment."""
    decision = lifecycle.observation_decision(unknown)

    assert (decision.state, decision.reason) == expected
