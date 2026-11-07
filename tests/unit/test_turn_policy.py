"""The repository turn decision: a leaving repository's next batch goes first for 120 s."""

from __future__ import annotations

from docker_test_toolkit.domain.policy import TURN_GRACE_NS, Turn, TurnCandidate, TurnDecision, turn_decision

NOW = 1_000_000_000_000
MINUTE = 60_000_000_000


def _turn(run_id: str = "left", *, released_ns: int = NOW - MINUTE, origin: str = "boot", daemon: str = "d1") -> Turn:
    return Turn(run_id, daemon, origin, released_ns)


def _decide(candidates, turns, **overrides) -> TurnDecision:
    options = {
        "now_ns": NOW,
        "clock_origin": "boot",
        "daemon_id": "d1",
        "oldest_waited_ns": MINUTE,
        "holders_present": False,
        "share_scope": "repository",
        "limit": 3,
    }
    return turn_decision(candidates, turns, **{**options, **overrides})


CANDIDATES = [TurnCandidate("two@3", 3, "two"), TurnCandidate("one@5", 5, "one"), TurnCandidate("one@7", 7, "one")]


def test_no_turns_or_holders_present_or_machine_scope_or_limit_one_decides_nothing() -> None:
    turns = {"one": _turn()}
    nothing = TurnDecision(None, False)

    assert _decide(CANDIDATES, {}) == nothing
    assert _decide(CANDIDATES, turns, holders_present=True) == nothing
    assert _decide(CANDIDATES, turns, share_scope="machine") == nothing
    assert _decide(CANDIDATES, turns, limit=1) == nothing


def test_a_valid_turn_chooses_the_earliest_candidate_of_its_repository_by_ticket() -> None:
    assert _decide(CANDIDATES, {"one": _turn()}) == TurnDecision("one@5", True)


def test_a_valid_turn_without_a_queued_run_holds_every_contender() -> None:
    assert _decide([TurnCandidate("two@3", 3, "two")], {"one": _turn()}) == TurnDecision(None, True)


def test_two_valid_turns_choose_the_earliest_ticket() -> None:
    turns = {"one": _turn(), "two": _turn("other")}  # dict order names "one" first; the ticket decides

    assert _decide(CANDIDATES, turns) == TurnDecision("two@3", True)


def test_an_expired_or_foreign_origin_or_other_daemon_turn_is_ignored() -> None:
    nothing = TurnDecision(None, False)

    assert _decide(CANDIDATES, {"one": _turn(released_ns=NOW - TURN_GRACE_NS)}) == nothing
    assert _decide(CANDIDATES, {"one": _turn(origin="earlier-boot")}) == nothing
    assert _decide(CANDIDATES, {"one": _turn(daemon="d2")}) == nothing


def test_turns_are_ignored_once_the_oldest_waited_the_window() -> None:
    assert _decide(CANDIDATES, {"one": _turn()}, oldest_waited_ns=30 * MINUTE) == TurnDecision(None, False)


def test_a_turn_with_a_future_release_tick_is_invalid() -> None:
    assert _decide(CANDIDATES, {"one": _turn(released_ns=NOW + 1)}) == TurnDecision(None, False)
