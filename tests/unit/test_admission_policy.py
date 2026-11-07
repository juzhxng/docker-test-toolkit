"""Pure admission policy: FIFO selection, creation prefix, takeover predicates and a stateful queue model."""

from __future__ import annotations

import pytest
from hypothesis import settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, precondition, rule

from docker_test_toolkit.application import admission
from docker_test_toolkit.domain.errors import ResourceError
from docker_test_toolkit.domain.policy import (
    STORED_RECOVERY_PHASES,
    TAKEOVER_ACCOUNT_CAP,
    UNBOUND_CLAIM_NS,
    QueueEntryView,
    actors_dead,
    claim_eligible,
    clock_namespace,
    is_creation_prefix,
    next_grantable,
    spawn_eligible,
)


def _view(
    run_id: str, ticket: int, phase: str, *, state: str | None = "executing", blocked: bool = False
) -> QueueEntryView:
    return QueueEntryView(run_id, ticket, phase, "p1", blocked, state)


@pytest.mark.parametrize("phase", ["admitting", "granted"])
@pytest.mark.parametrize("holder_state", ["executing", "absent"])
def test_any_holder_blocks_everyone(phase: str, holder_state: str) -> None:
    """The account has one slot; a dead holder keeps it until its takeover removes the entry."""
    views = [_view("h", 1, phase, state=holder_state), _view("w", 2, "queued")]
    assert next_grantable(views) is None


def test_head_is_the_lowest_live_unblocked_ticket() -> None:
    views = [
        _view("later", 4, "queued"),
        _view("blocked", 1, "queued", blocked=True),
        _view("dead", 2, "queued", state="replaced"),
        _view("head", 3, "queued"),
    ]
    assert next_grantable(views) == "head"


@pytest.mark.parametrize("state", ["zombie", None])
def test_unprovable_head_keeps_its_place(state: str | None) -> None:
    views = [_view("head", 1, "queued", state=state), _view("later", 2, "queued")]
    assert next_grantable(views) == "head"


def test_reserve_refuses_a_second_holder() -> None:
    state = admission.QueueState(next_ticket=1, entries={})
    runs = []
    for number in (1, 2):
        run_id = f"{number:08d}-0000-4000-8000-000000000000"
        reference = {
            "attempt": f"guardian-{number}",
            "record": f"/r/registry/runs/{run_id}.json",
            "registry": "/r/registry",
            "project_root": f"/r/p{number}",
        }
        admission.enroll(state, run_id, reference)
        runs.append(run_id)
    admission.reserve(state, runs[0], {"pid": 1})
    with pytest.raises(ResourceError, match="capacity is full"):
        admission.reserve(state, runs[1], {"pid": 2})
    assert state.entries[runs[1]].phase == "queued" and state.entries[runs[1]].holder is None


def _prefix() -> dict:
    return {
        "state": "prepared",
        "admission": "never-granted",
        "generation": 0,
        "revision": 0,
        "guardian_attempt": None,
        "ticket": None,
        "account_directory": None,
        "artifact": None,
        "pending_operation": None,
        "bootstrap_deadline": None,
    }


def test_creation_prefix_predicate_exact_fields() -> None:
    assert is_creation_prefix(_prefix()) is True
    changes = {
        "state": "queued",
        "admission": "revoked",
        "generation": 1,
        "revision": 1,
        "guardian_attempt": "x",
        "ticket": 3,
        "account_directory": "/a",
        "artifact": {"sha256": "a"},
        "pending_operation": {"kind": "guardian-start"},
        "bootstrap_deadline": {"origin": "o"},
    }
    for field, value in changes.items():
        record = _prefix()
        record[field] = value
        assert is_creation_prefix(record) is False, field
    missing = _prefix()
    del missing["bootstrap_deadline"]
    assert is_creation_prefix(missing) is False


ORIGIN = "darwin-continuous:boot-a"
ATTEMPT = "11111111-1111-4111-8111-111111111111"
OTHER_ATTEMPT = "22222222-2222-4222-8222-222222222222"
CLAIMANT = {"pid": 7}


def _claim(
    *, attempt: str = OTHER_ATTEMPT, claimant: object = None, expires_ns: int = 1_000, origin: str = ORIGIN
) -> dict:
    deadline = {"origin": origin, "created_ns": 0, "duration_ns": expires_ns, "expires_ns": expires_ns}
    return {"attempt": attempt, "deadline": deadline, "claimant": claimant}


@pytest.mark.parametrize(
    ("takeover", "caller_attempt", "claimant_state", "now_tick_ns", "origin", "expected"),
    [
        (None, None, None, 0, ORIGIN, True),  # no claim
        (None, ATTEMPT, None, 0, ORIGIN, False),  # a worker whose attempt is gone from the entry (cleared) refuses
        (_claim(attempt=ATTEMPT), ATTEMPT, None, 0, ORIGIN, True),  # the worker's own attempt, unbound, unexpired
        (_claim(attempt=ATTEMPT), ATTEMPT, None, 1_000, ORIGIN, True),  # the deadline instant itself is not yet past
        (_claim(attempt=ATTEMPT), ATTEMPT, None, 1_001, ORIGIN, False),  # own attempt expired in place
        (_claim(attempt=ATTEMPT, origin="darwin-continuous:boot-b"), ATTEMPT, None, 0, ORIGIN, False),  # rebooted
        (
            _claim(attempt=ATTEMPT, claimant=CLAIMANT),
            ATTEMPT,
            "executing",
            0,
            ORIGIN,
            False,
        ),  # own attempt, live claimant
        (_claim(attempt=ATTEMPT, claimant=CLAIMANT), ATTEMPT, "zombie", 0, ORIGIN, False),  # a zombie is not death
        (_claim(attempt=ATTEMPT, claimant=CLAIMANT), ATTEMPT, None, 0, ORIGIN, False),  # unknown observation
        (
            _claim(attempt=ATTEMPT, claimant=CLAIMANT),
            ATTEMPT,
            "absent",
            0,
            ORIGIN,
            True,
        ),  # dead claimant of its attempt
        (_claim(), ATTEMPT, None, 999, ORIGIN, False),  # another attempt, unbound, unexpired
        (_claim(), ATTEMPT, None, 1_001, ORIGIN, False),  # another attempt that has since expired is still not its own
        (_claim(claimant=CLAIMANT), ATTEMPT, "absent", 0, ORIGIN, False),  # another attempt, dead claimant
        (_claim(), None, None, 999, ORIGIN, False),  # foreign unbound, unexpired
        (_claim(), None, None, 1_001, ORIGIN, True),  # foreign unbound, expired
        (_claim(), None, None, 1_000, ORIGIN, False),  # the deadline instant itself is not yet past
        (_claim(claimant=CLAIMANT), None, "executing", 5_000, ORIGIN, False),  # bound, claimant executing
        (_claim(claimant=CLAIMANT), None, "zombie", 5_000, ORIGIN, False),  # a zombie is not death
        (_claim(claimant=CLAIMANT), None, None, 5_000, ORIGIN, False),  # unknown observation is not death
        (_claim(claimant=CLAIMANT), None, "absent", 0, ORIGIN, True),  # bound, claimant gone
        (_claim(claimant=CLAIMANT), None, "replaced", 0, ORIGIN, True),  # bound, claimant's pid reused
        (_claim(origin="darwin-continuous:boot-b"), None, None, 0, ORIGIN, True),  # another clock origin: rebooted
    ],
)
def test_claim_eligibility_predicate_matrix(
    takeover: dict | None,
    caller_attempt: str | None,
    claimant_state: str | None,
    now_tick_ns: int,
    origin: str,
    expected: bool,
) -> None:
    assert (
        claim_eligible(
            takeover,
            caller_attempt=caller_attempt,
            claimant_state=claimant_state,
            now_tick_ns=now_tick_ns,
            origin=origin,
        )
        is expected
    )


@pytest.mark.parametrize(
    ("states", "expected"),
    [
        ((), True),
        (("absent",), True),
        (("absent", "replaced"), True),
        (("absent", "executing"), False),
        (("zombie",), False),
        (("replaced", None), False),
    ],
)
def test_actors_dead_requires_every_state_proven(states: tuple[str | None, ...], expected: bool) -> None:
    assert actors_dead(states) is expected


GRACE_NS = 30_000_000_000


def _dead_entry(*, tick_ns: int | None = 0, origin: str = ORIGIN, takeover: dict | None = None) -> dict:
    dead_since = None if tick_ns is None else {"origin": origin, "tick_ns": tick_ns}
    return {"dead_since": dead_since, "takeover": takeover}


@pytest.mark.parametrize(
    ("entry", "claimant_state", "now_tick_ns", "claims", "expected"),
    [
        (_dead_entry(), None, 1, 0, True),  # the worker waits out the grace, not the spawn
        (_dead_entry(tick_ns=None), None, 10 * GRACE_NS, 0, False),  # never observed dead
        (_dead_entry(origin="darwin-continuous:boot-b"), None, 10 * GRACE_NS, 0, False),  # origin changed: restart
        (_dead_entry(), None, GRACE_NS, 1, True),  # one live claim elsewhere: below the cap of two
        (_dead_entry(), None, GRACE_NS, 2, False),  # the account cap
        (_dead_entry(takeover=_claim(expires_ns=5 * GRACE_NS)), None, GRACE_NS, 0, False),  # unbound, unexpired
        (_dead_entry(takeover=_claim(expires_ns=GRACE_NS)), None, 2 * GRACE_NS, 0, True),  # unbound, expired
        (_dead_entry(takeover=_claim(claimant=CLAIMANT)), "executing", GRACE_NS, 0, False),  # bound, live
        (_dead_entry(takeover=_claim(claimant=CLAIMANT)), "zombie", GRACE_NS, 0, False),  # a zombie is not death
        (_dead_entry(takeover=_claim(claimant=CLAIMANT)), "absent", GRACE_NS, 0, True),  # bound, deadline passed
        (
            _dead_entry(takeover=_claim(claimant=CLAIMANT, expires_ns=2 * GRACE_NS)),
            "absent",
            GRACE_NS,
            0,
            False,
        ),  # a failed worker's back-off holds off automatic spawns until its deadline
        (
            _dead_entry(takeover=_claim(claimant=CLAIMANT, expires_ns=2 * GRACE_NS, origin="darwin-continuous:boot-b")),
            "absent",
            GRACE_NS,
            0,
            True,
        ),  # a clock-origin change ends the back-off
    ],
)
def test_spawn_eligibility_matrix(
    entry: dict, claimant_state: str | None, now_tick_ns: int, claims: int, expected: bool
) -> None:
    assert (
        spawn_eligible(
            entry,
            claimant_state=claimant_state,
            now_tick_ns=now_tick_ns,
            origin=ORIGIN,
            live_or_unbound_claims=claims,
        )
        is expected
    )


# --- Stateful model over the pure queue operations and takeover predicates ---

SECOND = 1_000_000_000
DEAD = ("absent", "replaced")


class QueueModel(RuleBasedStateMachine):
    """Drive the queue and the takeover predicates through every interleaving of death, claim and crash.

    The state is the logical queue (``admission.enroll/reserve/grant/remove``) plus who is alive. Each rule is one
    durable step; "crash at any step" is the death of the actor that would have taken the next one, so a run can be
    left between any two of its steps (reserved but not granted, record final but entry present, claimed but not
    recovered). Rules assert the decision when it is taken, invariants assert what stays true afterwards.

    The model's operator creates and binds its own claim; in production an operator against a null claim is fenced by
    the record's ``recovery_actor`` instead, which the integration crash matrix covers.
    """

    def __init__(self) -> None:
        super().__init__()
        self.state = admission.QueueState(next_ticket=1, entries={})
        self.now = 10 * SECOND
        self.references: dict[str, dict] = {}
        self.guardian: dict[str, str] = {}  # run id -> observe_process vocabulary
        self.requester: dict[str, str] = {}
        self.claimant_state: dict[str, str] = {}  # claimant identity -> observation
        self.active: dict[str, set[str]] = {}  # run id -> recoverers bound to it that have not finished
        self.final_record: set[str] = set()  # runs whose terminal record was written
        self.removed: set[str] = set()  # run ids whose queue entry was removed
        self.issued: list[int] = []
        self.phase_rank: dict[str, int] = {}
        self.serial = 0

    # -- helpers --------------------------------------------------------------------------------------------------

    def _entries(self) -> list[admission.QueueEntry]:
        return sorted(self.state.entries.values(), key=lambda entry: entry.ticket)

    def _pick(self, entries: list[admission.QueueEntry], index: int) -> admission.QueueEntry:
        return entries[index % len(entries)]

    def _views(self) -> list[QueueEntryView]:
        return [
            QueueEntryView(
                entry.run_id,
                entry.ticket,
                entry.phase,
                entry.project_id,
                False,
                self.guardian[entry.run_id],
            )
            for entry in self.state.entries.values()
        ]

    def _next(self) -> str | None:
        return next_grantable(self._views())

    def _actors_dead(self, run_id: str) -> bool:
        """The model's own proof of death (every actor absent or replaced), checked against the policy function."""
        states = [self.guardian[run_id], self.requester[run_id]]
        proven = all(state in DEAD for state in states)
        assert actors_dead(states) is proven
        return proven

    def _dead_actors(self) -> list[admission.QueueEntry]:
        return [entry for entry in self._entries() if self._actors_dead(entry.run_id)]

    def _claimant_observation(self, entry: admission.QueueEntry) -> str | None:
        takeover = entry.takeover
        return None if takeover is None or takeover["claimant"] is None else self.claimant_state[takeover["claimant"]]

    def _live_claimant(self, entry: admission.QueueEntry) -> bool:
        observation = self._claimant_observation(entry)
        return observation is not None and observation not in DEAD

    def _live_claimants(self) -> list[admission.QueueEntry]:
        return [entry for entry in self._entries() if self._live_claimant(entry)]

    def _held(self, entry: admission.QueueEntry) -> bool:
        """Whether the entry's claim is a live claimant or an unbound intent that has not expired."""
        takeover = entry.takeover
        if takeover is None:
            return False
        if takeover["claimant"] is not None:
            return self._live_claimant(entry)
        return takeover["deadline"]["origin"] == ORIGIN and self.now <= takeover["deadline"]["expires_ns"]

    def _operator_eligible(self, entry: admission.QueueEntry) -> bool:
        return claim_eligible(
            entry.takeover,
            caller_attempt=None,
            claimant_state=self._claimant_observation(entry),
            now_tick_ns=self.now,
            origin=ORIGIN,
        )

    def _new_claim(self, entry: admission.QueueEntry) -> dict:
        self.serial += 1
        deadline = {
            "origin": ORIGIN,
            "created_ns": self.now,
            "duration_ns": UNBOUND_CLAIM_NS,
            "expires_ns": self.now + UNBOUND_CLAIM_NS,
        }
        entry.takeover = {"attempt": f"attempt-{self.serial}", "deadline": deadline, "claimant": None}
        return entry.takeover

    def _bind(self, entry: admission.QueueEntry, kind: str) -> None:
        assert entry.takeover is not None
        self.serial += 1
        claimant = f"{kind}-{self.serial}"
        live = {name for name in self.active.get(entry.run_id, set()) if self.claimant_state[name] not in DEAD}
        assert live == set(), f"I4: {claimant} binds to {entry.run_id} while {live} still recovers it"
        entry.takeover["claimant"] = claimant
        self.claimant_state[claimant] = "executing"
        self.active.setdefault(entry.run_id, set()).add(claimant)

    # -- queue rules ----------------------------------------------------------------------------------------------

    @precondition(lambda self: len(self.state.entries) < 6)
    @rule(project=st.integers(0, 2))
    def enroll(self, project: int) -> None:
        self.serial += 1
        run_id = f"{self.serial:08d}-0000-4000-8000-000000000000"
        reference = {
            "attempt": f"guardian-{self.serial}",
            "record": f"/r/registry/runs/{run_id}.json",
            "registry": "/r/registry",
            "project_root": f"/r/project-{project}",
        }
        expected = self.state.next_ticket
        assert admission.enroll(self.state, run_id, reference) == expected
        self.references[run_id] = reference
        self.guardian[run_id] = self.requester[run_id] = "executing"
        self.issued.append(expected)

    @precondition(lambda self: bool(self.state.entries))
    @rule(index=st.integers(0, 7))
    def enroll_again(self, index: int) -> None:
        """A retried enrollment by the same guardian keeps its ticket and adds nothing."""
        entry = self._pick(self._entries(), index)
        before = (entry.ticket, self.state.next_ticket, len(self.state.entries))
        assert admission.enroll(self.state, entry.run_id, self.references[entry.run_id]) == entry.ticket
        assert (entry.ticket, self.state.next_ticket, len(self.state.entries)) == before

    @precondition(lambda self: self._next() is not None)
    @rule()
    def reserve(self) -> None:
        target = self._next()
        assert target is not None
        head = self._head()  # FIFO among the living queued entries
        assert head is not None and target == head.run_id
        admission.reserve(self.state, target, {"pid": 1, "run": target})

    def _head(self) -> admission.QueueEntry | None:
        """The model's own reading of the head: lowest ticket among live queued entries."""
        return next(
            (entry for entry in self._entries() if entry.phase == "queued" and self.guardian[entry.run_id] not in DEAD),
            None,
        )

    def _refused(self) -> list[admission.QueueEntry]:
        """Queued entries whose reservation must be refused now: the one slot is held."""
        if not admission.holders(self.state):
            return []
        return [entry for entry in self._entries() if entry.phase == "queued"]

    @precondition(lambda self: bool(self._refused()))
    @rule(index=st.integers(0, 7))
    def reserve_when_full(self, index: int) -> None:
        """A reservation while the slot is held is refused and changes nothing."""
        snapshot = [(entry.run_id, entry.phase, entry.holder) for entry in self._entries()]
        with pytest.raises(ResourceError):
            admission.reserve(self.state, self._pick(self._refused(), index).run_id, {"pid": 2})
        assert snapshot == [(entry.run_id, entry.phase, entry.holder) for entry in self._entries()]

    @precondition(
        lambda self: any(
            entry.phase == "admitting" and self.guardian[entry.run_id] == "executing" for entry in self._entries()
        )
    )
    @rule()
    def grant(self) -> None:
        entry = next(
            entry
            for entry in self._entries()
            if entry.phase == "admitting" and self.guardian[entry.run_id] == "executing"
        )
        assert entry.holder is not None
        with pytest.raises(ResourceError):  # only the reserving guardian may publish
            admission.grant(self.state, entry.run_id, {"pid": 99})
        assert entry.phase == "admitting"
        admission.grant(self.state, entry.run_id, entry.holder)
        assert entry.phase == "granted"

    # -- actor rules ----------------------------------------------------------------------------------------------

    @precondition(lambda self: bool(self.state.entries))
    @rule(
        index=st.integers(0, 7),
        who=st.sampled_from(["guardian", "requester", "both"]),
        how=st.sampled_from(["absent", "replaced", "zombie"]),
    )
    def die(self, index: int, who: str, how: str) -> None:
        """An actor ends; a zombie (or any unprovable state) is not death for the policy."""
        run_id = self._pick(self._entries(), index).run_id
        if who in ("guardian", "both"):
            self.guardian[run_id] = how
        if who in ("requester", "both"):
            self.requester[run_id] = how

    @rule(seconds=st.integers(1, 45))
    def tick(self, seconds: int) -> None:
        self.now += seconds * SECOND

    # -- takeover rules -------------------------------------------------------------------------------------------

    @precondition(lambda self: any(entry.dead_since is None for entry in self._dead_actors()))
    @rule(index=st.integers(0, 7))
    def mark_dead(self, index: int) -> None:
        candidates = [entry for entry in self._dead_actors() if entry.dead_since is None]
        self._pick(candidates, index).dead_since = {"origin": ORIGIN, "tick_ns": self.now}

    @precondition(lambda self: bool(self._dead_actors()))
    @rule(index=st.integers(0, 7))
    def spawn(self, index: int) -> None:
        """The queued guardian's pass: write a claim intent only when the policy allows a worker for the entry."""
        entry = self._pick(self._dead_actors(), index)
        counted = sum(1 for other in self._entries() if self._held(other))
        eligible = spawn_eligible(
            {"dead_since": entry.dead_since, "takeover": entry.takeover},
            claimant_state=self._claimant_observation(entry),
            now_tick_ns=self.now,
            origin=ORIGIN,
            live_or_unbound_claims=counted,
        )
        if not eligible:
            return
        assert entry.dead_since is not None and entry.dead_since["origin"] == ORIGIN
        assert counted < TAKEOVER_ACCOUNT_CAP  # never past the account cap
        assert not self._held(entry)  # never over a live claimant or an unexpired intent
        self._new_claim(entry)

    @precondition(lambda self: any(entry.takeover is not None for entry in self._entries()))
    @rule(index=st.integers(0, 7))
    def worker_binds(self, index: int) -> None:
        """A spawned worker proceeds only on its own attempt: unexpired while unbound, or after its claimant died."""
        entry = self._pick([entry for entry in self._entries() if entry.takeover is not None], index)
        takeover = entry.takeover
        assert takeover is not None
        observation = self._claimant_observation(entry)
        unexpired = takeover["deadline"]["origin"] == ORIGIN and self.now <= takeover["deadline"]["expires_ns"]
        own = takeover["claimant"] is None and unexpired
        after_dead_claimant = takeover["claimant"] is not None and observation in DEAD
        eligible = claim_eligible(
            takeover,
            caller_attempt=takeover["attempt"],
            claimant_state=observation,
            now_tick_ns=self.now,
            origin=ORIGIN,
        )
        assert eligible is (own or after_dead_claimant)  # an expired or live-claimed attempt is refused
        if eligible:
            self._bind(entry, "worker")

    @precondition(lambda self: bool(self.state.entries))
    @rule(index=st.integers(0, 7))
    def stale_worker_is_refused(self, index: int) -> None:
        """A worker whose attempt is not the entry's (replaced, cleared or never there) is refused in every state."""
        entry = self._pick(self._entries(), index)
        assert not claim_eligible(
            entry.takeover,
            caller_attempt="stale-attempt",
            claimant_state=self._claimant_observation(entry),
            now_tick_ns=self.now,
            origin=ORIGIN,
        )

    @precondition(lambda self: bool(self._dead_actors()))
    @rule(index=st.integers(0, 7))
    def operator_claims(self, index: int) -> None:
        """``run recover``: proceeds only when no live claimant and no unexpired intent holds the run."""
        entry = self._pick(self._dead_actors(), index)
        eligible = self._operator_eligible(entry)
        assert eligible is not self._held(
            entry
        )  # refused exactly while a live claimant or an unexpired intent holds it
        if eligible:
            self._new_claim(entry)
            self._bind(entry, "operator")

    @precondition(lambda self: bool(self._live_claimants()))
    @rule(index=st.integers(0, 7))
    def advance_phase(self, index: int) -> None:
        """A live claimant completes the next recovery phase; the stored phase only moves forward."""
        entry = self._pick(self._live_claimants(), index)
        rank = self.phase_rank.get(entry.run_id, 0)
        if rank < len(STORED_RECOVERY_PHASES):
            entry.last_completed_phase = STORED_RECOVERY_PHASES[rank]
            self.phase_rank[entry.run_id] = rank + 1

    @precondition(lambda self: bool(self._live_claimants()))
    @rule(index=st.integers(0, 7), how=st.sampled_from(["absent", "replaced", "zombie"]))
    def crash_claimant(self, index: int, how: str) -> None:
        entry = self._pick(self._live_claimants(), index)
        assert entry.takeover is not None
        self.claimant_state[entry.takeover["claimant"]] = how

    @precondition(lambda self: bool(self._live_claimants()))
    @rule(index=st.integers(0, 7))
    def write_final_record(self, index: int) -> None:
        """The claimant writes the terminal record; a crash here leaves a final record whose entry is still present."""
        self.final_record.add(self._pick(self._live_claimants(), index).run_id)

    @precondition(lambda self: any(run_id in self.state.entries for run_id in self.final_record))
    @rule(index=st.integers(0, 7))
    def retire_entry(self, index: int) -> None:
        """Remove the entry of a run whose terminal record is written: its live claimant, or the next recoverer."""
        entry = self._pick(
            [self.state.entries[run_id] for run_id in sorted(self.final_record) if run_id in self.state.entries], index
        )
        if not self._live_claimant(entry) and not (self._actors_dead(entry.run_id) and self._operator_eligible(entry)):
            return  # a second recoverer refuses while the first lives
        assert admission.remove(self.state, entry.run_id) is entry
        self.removed.add(entry.run_id)
        for name in self.active.pop(entry.run_id, set()):
            self.claimant_state[name] = "absent"  # the recoverer is done and its process exits

    # -- invariants -----------------------------------------------------------------------------------------------

    @invariant()
    def capacity_bound(self) -> None:
        """I1: at most one entry holds the account's slot."""
        assert len(admission.holders(self.state)) <= 1

    @invariant()
    def head_is_never_passed(self) -> None:
        """The decision is the head while the slot is free and nobody otherwise."""
        head = self._head()
        free = not admission.holders(self.state)
        assert self._next() == (head.run_id if head is not None and free else None)

    @invariant()
    def tickets_are_unique_and_never_reused(self) -> None:
        tickets = [entry.ticket for entry in self._entries()]
        assert len(tickets) == len(set(tickets))
        assert self.issued == list(range(1, self.state.next_ticket))  # one new ticket per new run, none reused

    @invariant()
    def one_recoverer_per_run(self) -> None:
        """I4: at most one live recoverer is bound to a run, and it is the entry's claimant."""
        for run_id, names in self.active.items():
            live = [name for name in names if self.claimant_state[name] not in DEAD]
            assert len(live) <= 1, f"I4: {live} all recover {run_id}"
            entry = self.state.entries.get(run_id)
            if live and entry is not None:
                assert entry.takeover is not None and entry.takeover["claimant"] == live[0]

    @invariant()
    def a_terminal_run_never_reenters_the_queue(self) -> None:
        assert self.removed.isdisjoint(self.state.entries)

    @invariant()
    def no_slot_leak(self) -> None:
        """A holder exists only for a run that is live, claimable now, or whose live claim will finish or lapse."""
        for holder in admission.holders(self.state):
            if self._actors_dead(holder.run_id):
                assert self._operator_eligible(holder) or self._held(holder), (
                    "a dead holder is neither claimable nor held"
                )

    @invariant()
    def a_live_waiter_is_never_stalled(self) -> None:
        """With no holder, a live queued entry is always grantable (a dead head never stalls the queue)."""
        if not admission.holders(self.state) and any(
            entry.phase == "queued" and self.guardian[entry.run_id] not in DEAD for entry in self._entries()
        ):
            assert self._next() is not None

    @invariant()
    def recorded_phases_stay_in_the_stored_vocabulary(self) -> None:
        for entry in self._entries():
            assert entry.last_completed_phase in (None, *STORED_RECOVERY_PHASES)


QueueModel.TestCase.settings = settings(max_examples=200, stateful_step_count=40, deadline=None)
TestQueueModel = QueueModel.TestCase


@pytest.mark.parametrize(
    ("link", "offsets", "expected"),
    [
        ("time:[4026531834]", "monotonic 0 0\nboottime 0 0\n", "time:[4026531834]"),  # the host's own
        (
            "time:[4026532941]",
            "monotonic           0         0\nboottime            0         0\n",
            "time:[4026531834]",
        ),
        ("time:[4026532941]", "monotonic 0 0\nboottime 0 0\n", "time:[4026531834]"),  # a container's, same clocks
        (
            "time:[4026532949]",
            "boottime 0 0\nmonotonic 0 0\n",
            "time:[4026531834]",
        ),  # another container's
        ("time:[4026532941]", "monotonic 0 0\nboottime 600 0\n", "time:[4026532941]"),  # its clock really differs
        ("time:[4026532941]", "monotonic 5 0\nboottime 0 0\n", "time:[4026532941]"),
        ("time:[4026532941]", "boottime 0 1\nmonotonic 0 0\n", "time:[4026532941]"),
        ("time:[4026532941]", None, "time:[4026532941]"),  # offsets unreadable: no proof the clocks are equal
        ("time:[4026532941]", "", "time:[4026532941]"),
        ("time:[4026532941]", "monotonic 0 0\n", "time:[4026532941]"),
        ("time:[4026532941]", "nonsense\nboottime 0 0\nmonotonic 0 0\n", "time:[4026532941]"),
    ],
)
def test_time_namespaces_with_zero_offsets_share_one_clock_identity(
    link: str, offsets: str | None, expected: str
) -> None:
    assert clock_namespace(link, offsets) == expected
