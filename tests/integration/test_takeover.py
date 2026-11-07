"""Recovery exclusion for takeover: the claim predicate, the atomic bind, D-DRAIN and the refusals.

Every refusal asserts zero bytes changed in the run record and the queue; the live actors are real processes.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import threading
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from pathlib import Path

import pytest

from docker_test_toolkit import PhaseBudget, RunSpec
from docker_test_toolkit.application import admission, lifecycle
from docker_test_toolkit.application.admission import STATE_MARKER
from docker_test_toolkit.application.recovery import Context, atomic_json, lock
from docker_test_toolkit.domain.errors import ResourceBusy, ResourceError
from docker_test_toolkit.domain.models import ArtifactIdentity
from docker_test_toolkit.domain.records import canonical_json_bytes
from docker_test_toolkit.factories.testing import build_test_services, install_failpoint, install_lock_observer
from tests.integration.account_fixtures import (
    EMPTY_QUEUE,
    EnterThread,
    Interrupted,
    RequesterChild,
    _hold_lock,
    add_active_consumer,
    authority_bytes,
    child_result,
    current_identity,
    dead_identity,
    foreign_snapshot,
    kill_guardian,
    open_file_names,
    pause_guardian,
    prepare_for_child_guardian,
    processes_mentioning,
    queue_entries,
    queue_phases,
    read_observer_log,
    reap,
    reap_orphan,
    require_present,
    retire_actor,
    run_lock_path,
    seed_dead_since,
    seed_foreign_sentinels,
    start_guardian_child,
    start_live_actor,
    start_recover_child,
    start_requester_child,
    start_takeover_worker_child,
    wait_until,
    write_claim_intent,
    write_raw_queue,
)
from tests.integration.test_docker_receipts import (
    _entered_resource_run,
    _interrupt_retained_release,
    _retained_owned_container,
    _withdraw_then_lose_the_requester,
)
from tests.integration.test_native_lifecycle import (
    _dead_requester_prefix,
    _identity,
    _prefix_account,
    _prepared_run_with_guardian_child,
    _spec,
)

pytestmark = pytest.mark.integration

ATTEMPT = "77777777-7777-4777-8777-777777777777"
OTHER_ATTEMPT = "88888888-8888-4888-8888-888888888888"


def _resource_grant(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """A resource-bearing grant with a dead guardian and a dead withdrawn requester: (handle, context, account)."""
    handle, context, account = _entered_resource_run(tmp_path, monkeypatch, retain=False)
    _withdraw_then_lose_the_requester(handle, context)
    return handle, context, account


def _empty_grant(tmp_path: Path, artifact: ArtifactIdentity | None = None) -> tuple[lifecycle.RunContext, Path]:
    """An empty grant whose guardian is dead and whose withdrawn requester died."""
    project = tmp_path / "empty-project"
    project.mkdir()
    services = build_test_services(tmp_path / "empty-state")
    spec = RunSpec(
        project_root=project,
        entrypoint="integration",
        purpose="takeover-empty",
        services=("postgres",),
        closure=("postgres",),
        profile="fixture",
        phases={"tests": PhaseBudget(0, 1, 1, 1)},
        initial_phase="tests",
        artifact=artifact or _identity(),
        wait_seconds=20,
    )
    handle = services.open_run(spec).enter()
    kill_guardian(handle._context)

    def requester_dies(stage: str) -> None:
        if stage == "leave-withdrawal-written":
            raise Interrupted

    install_failpoint(requester_dies)
    try:
        with pytest.raises(Interrupted):
            handle.leave()
    finally:
        install_failpoint(None)
    return handle._context, services.root / "admission"


def _set_record(context: lifecycle.RunContext, **changes: object) -> None:
    record = json.loads(context.record.read_text())
    record.update(changes)
    atomic_json(context.record, record)


def _recover(context: lifecycle.RunContext, account: Path, **arguments: object) -> lifecycle.RunStatus:
    return lifecycle.recover_run(context, _test_directory=account, **arguments)  # type: ignore[arg-type]


def test_second_recoverer_refuses_before_any_write(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _handle, context, account = _resource_grant(tmp_path, monkeypatch)
    live, identity = start_live_actor("recoverer")
    try:
        _set_record(context, recovery_actor=identity, recovery_outcome="pending")
        before = authority_bytes(context, account)

        with pytest.raises(ResourceBusy) as raised:
            _recover(context, account)

        assert raised.value.reason == "actor-live"
        assert authority_bytes(context, account) == before  # the second recoverer wrote nothing
    finally:
        reap(live)


def test_worker_mode_refuses_live_consumer_without_signal(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _handle, context, account = _resource_grant(tmp_path, monkeypatch)
    consumer, identity = start_live_actor("consumer")
    try:
        add_active_consumer(context, identity)
        before = authority_bytes(context, account)

        with pytest.raises(ResourceBusy) as raised:
            _recover(context, account, drain_live_consumers=False, takeover_attempt=ATTEMPT)

        assert raised.value.reason == "actor-live"
        assert authority_bytes(context, account) == before
        assert consumer.poll() is None  # the consumer was never signalled
        assert lifecycle.observe_process(lifecycle._identity_from_json(identity)) == "executing"
    finally:
        reap(consumer)


def test_operator_mode_drains_live_consumer(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _handle, context, account = _resource_grant(tmp_path, monkeypatch)
    consumer, identity = start_live_actor("consumer")
    try:
        add_active_consumer(context, identity)

        status = _recover(context, account)

        assert (status.state, status.cleanup_outcome) == ("complete", "complete")
        assert consumer.wait(timeout=20) == -signal.SIGTERM  # drained: SIGTERM, as before
    finally:
        reap(consumer)


@pytest.mark.parametrize("claimant", ["unbound-unexpired", "bound-live-claimant"])
def test_operator_cannot_take_a_held_claim_on_a_resource_grant(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, claimant: str
) -> None:
    _handle, context, account = _resource_grant(tmp_path, monkeypatch)
    live, identity = start_live_actor("claimant")
    try:
        write_claim_intent(
            account,
            context.run_id,
            ATTEMPT,
            expires_in_s=60,
            claimant=identity if claimant == "bound-live-claimant" else None,
        )
        before = authority_bytes(context, account)

        with pytest.raises(ResourceError) as raised:
            _recover(context, account)

        assert raised.value.reason == "claim-held"
        assert authority_bytes(context, account) == before
    finally:
        reap(live)


def test_operator_takes_an_expired_unbound_claim_on_a_resource_grant(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _handle, context, account = _resource_grant(tmp_path, monkeypatch)
    write_claim_intent(account, context.run_id, ATTEMPT, expires_in_s=-1)

    status = _recover(context, account)

    assert (status.state, status.cleanup_outcome) == ("complete", "complete")
    assert queue_entries(account) == {}


@pytest.mark.parametrize("claimant", ["unbound-unexpired", "bound-live-claimant"])
def test_operator_cannot_take_a_held_claim_on_an_empty_grant(tmp_path: Path, claimant: str) -> None:
    context, account = _empty_grant(tmp_path)
    live, identity = start_live_actor("claimant")
    try:
        write_claim_intent(
            account,
            context.run_id,
            ATTEMPT,
            expires_in_s=60,
            claimant=identity if claimant == "bound-live-claimant" else None,
        )
        before = authority_bytes(context, account)

        with pytest.raises(ResourceError) as raised:
            _recover(context, account)

        assert raised.value.reason == "claim-held"
        assert authority_bytes(context, account) == before
    finally:
        reap(live)


def test_unexpired_guardian_start_blocks_takeover(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    services = build_test_services(tmp_path / "state")
    handle = services.open_run(
        RunSpec(
            project_root=project,
            entrypoint="integration",
            purpose="guardian-start",
            services=(),
            closure=(),
            profile="fixture",
            phases={"tests": PhaseBudget(0, 1, 1, 1)},
            initial_phase="tests",
            artifact=_identity(),
            wait_seconds=20,
        )
    )
    context = handle._context
    account = services.root / "admission"
    lifecycle.configure_run(
        context,
        services=(),
        closure=(),
        phases={"tests": PhaseBudget(0, 1, 1, 1)},
        initial_phase="tests",
        topology="desktop",
    )
    lifecycle.start_guardian_attempt(context)  # pending_operation guardian-start with an unexpired bootstrap deadline
    _set_record(context, requester=dead_identity())
    before = context.record.read_bytes()

    with pytest.raises(ResourceBusy) as raised:
        _recover(context, account)

    assert raised.value.reason == "guardian-start-pending"
    assert context.record.read_bytes() == before


def test_unsupported_operation_kind_refuses_before_write(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _handle, context, account = _resource_grant(tmp_path, monkeypatch)
    record = json.loads(context.record.read_text())
    _set_record(context, pending_operation={"kind": "guardian-start", "attempt": record["guardian_attempt"]})
    before = authority_bytes(context, account)

    with pytest.raises(ResourceError) as raised:
        _recover(context, account)

    assert raised.value.reason == "unsupported-operation-kind"
    assert authority_bytes(context, account) == before


def test_unsupported_receipt_kind_refuses_before_write(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _handle, context, account = _resource_grant(tmp_path, monkeypatch)
    record = json.loads(context.record.read_text())
    receipt = next(iter(record["docker_receipts"].values()))
    receipt.update(kind="bogus-kind", state="unresolved")
    atomic_json(context.record, record)
    before = authority_bytes(context, account)

    with pytest.raises(ResourceError) as raised:
        _recover(context, account)

    assert raised.value.reason == "unsupported-receipt-kind"
    assert authority_bytes(context, account) == before


def _crash_at(stage: str, call):
    """Run ``call`` with a failpoint that raises ``Interrupted`` at ``stage`` (a process kill at that point)."""

    def crash(name: str) -> None:
        if name == stage:
            raise Interrupted

    install_failpoint(crash)
    try:
        with pytest.raises(Interrupted):
            call()
    finally:
        install_failpoint(None)


def _claim_of(account: Path, run_id: str) -> dict | None:
    """Read an entry's claim straight from the file: usable inside a failpoint, where the section holds metadata."""
    return json.loads((account / "queue.json").read_text())["entries"][run_id]["takeover"]


def _grant_without_its_guardian(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, grant: str, *, withdrawn: bool
) -> tuple[lifecycle.RunContext, Path]:
    """A resource-bearing or empty grant whose guardian was killed; ``withdrawn`` says whether its requester left."""
    if grant == "resource-grant":
        if withdrawn:
            _handle, context, account = _resource_grant(tmp_path, monkeypatch)
        else:
            handle, context, account = _entered_resource_run(tmp_path, monkeypatch, retain=False)
            kill_guardian(handle._context)
        return context, account
    if withdrawn:
        return _empty_grant(tmp_path)
    services = build_test_services(tmp_path / "empty-state")
    project = tmp_path / "empty-project"
    project.mkdir()
    handle = services.open_run(_empty_spec(project, "zombie-requester")).enter()
    kill_guardian(handle._context)
    return handle._context, services.root / "admission"


@pytest.mark.parametrize("grant", ["resource-grant", "empty-grant"])
@pytest.mark.parametrize("actor", ["guardian", "requester"])
def test_a_zombie_root_is_not_proof_of_death(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, grant: str, actor: str
) -> None:
    """Design 3.9 check 1: the guardian and a requester that has not withdrawn must be absent or replaced."""
    context, account = _grant_without_its_guardian(tmp_path, monkeypatch, grant, withdrawn=actor == "guardian")
    zombie_pid = _json(context.record)[actor]["pid"]
    original = lifecycle.observe_process

    def observe(identity: lifecycle.ProcessIdentity) -> str:
        # The OS seam: macOS reports a killed unreaped child as gone, so the unprovable state is stood in at the
        # observation boundary (as the creation-prefix tests do); every other identity is observed for real.
        return "zombie" if identity.pid == zombie_pid else original(identity)

    monkeypatch.setattr(lifecycle, "observe_process", observe)
    before = authority_bytes(context, account)

    with pytest.raises(ResourceBusy) as raised:
        _recover(context, account)

    assert raised.value.reason == "actor-live"
    assert authority_bytes(context, account) == before  # refused before any write


def test_attempt_replaced_at_nested_recheck_refuses(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _handle, context, account = _resource_grant(tmp_path, monkeypatch)
    write_claim_intent(account, context.run_id, ATTEMPT, expires_in_s=60)
    record_before = context.record.read_bytes()

    def replace_the_attempt(stage: str) -> None:
        if stage == "recovery-checks-passed":  # after the checks, before the metadata section: a spawner replaces it
            write_claim_intent(account, context.run_id, OTHER_ATTEMPT, expires_in_s=60)

    install_failpoint(replace_the_attempt)
    try:
        with pytest.raises(ResourceError) as raised:
            _recover(context, account, drain_live_consumers=False, takeover_attempt=ATTEMPT)
    finally:
        install_failpoint(None)

    assert raised.value.reason == "claim-held"
    assert context.record.read_bytes() == record_before  # the re-check inside the section refused before any write
    claim = _claim_of(account, context.run_id)
    assert claim is not None and claim["attempt"] == OTHER_ATTEMPT and claim["claimant"] is None


@pytest.mark.parametrize("grant", ["resource-grant", "empty-grant"])
@pytest.mark.parametrize("change", ["expires-in-place", "replaced-then-expired", "cleared"])
def test_worker_claim_changed_at_nested_recheck_refuses(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, grant: str, change: str
) -> None:
    """Case C: after the worker's read-only attempt check its claim lapses, is replaced and lapses, or disappears."""
    if grant == "resource-grant":
        _handle, context, account = _resource_grant(tmp_path, monkeypatch)
    else:
        context, account = _empty_grant(tmp_path)
    write_claim_intent(account, context.run_id, ATTEMPT, expires_in_s=60)
    record_before = context.record.read_bytes()

    def change_the_claim(stage: str) -> None:
        if stage != "recovery-checks-passed":  # after the checks, before the nested metadata section
            return
        if change == "expires-in-place":
            write_claim_intent(account, context.run_id, ATTEMPT, expires_in_s=-1)
        elif change == "replaced-then-expired":
            write_claim_intent(account, context.run_id, OTHER_ATTEMPT, expires_in_s=-1)
        else:
            lifecycle._clear_claim_intent(account, context.run_id, ATTEMPT)

    install_failpoint(change_the_claim)
    try:
        with pytest.raises(ResourceError) as raised:
            _recover(context, account, drain_live_consumers=False, takeover_attempt=ATTEMPT)
    finally:
        install_failpoint(None)

    assert raised.value.reason == "claim-held"
    assert context.record.read_bytes() == record_before  # no record write and no bind happened
    claim = _claim_of(account, context.run_id)
    assert (claim or {}).get("claimant") is None
    assert (claim is None) is (change == "cleared")


@pytest.mark.parametrize("path", ["resource-grant", "empty-grant", "ungranted"])
@pytest.mark.parametrize("claim", ["own-attempt-expired", "other-attempt-expired", "no-claim"])
def test_worker_without_a_live_attempt_on_its_entry_refuses_before_any_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, path: str, claim: str
) -> None:
    """The locked check alone (``recover_run`` called directly, no read-only pre-check) refuses on every path."""
    if path == "resource-grant":
        _handle, context, account = _resource_grant(tmp_path, monkeypatch)
    elif path == "empty-grant":
        context, account = _empty_grant(tmp_path)
    else:
        services = build_test_services(tmp_path / "state")
        account = services.root / "admission"
        project, _other = _two_projects(tmp_path)
        context = _dead_queued_entry(tmp_path, services, project, "expired-own-attempt")
    if claim != "no-claim":
        attempt = ATTEMPT if claim == "own-attempt-expired" else OTHER_ATTEMPT
        write_claim_intent(account, context.run_id, attempt, expires_in_s=-1)  # already past its deadline
    before = authority_bytes(context, account)

    with pytest.raises(ResourceError) as raised:
        _recover(context, account, drain_live_consumers=False, takeover_attempt=ATTEMPT)

    assert raised.value.reason == "claim-held"
    assert authority_bytes(context, account) == before


def test_claimant_crash_after_bind_is_reclaimable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _handle, context, account = _resource_grant(tmp_path, monkeypatch)
    write_claim_intent(account, context.run_id, ATTEMPT, expires_in_s=60)
    me = current_identity()

    _crash_at(
        "recovery-claim-bound",
        lambda: _recover(context, account, drain_live_consumers=False, takeover_attempt=ATTEMPT),
    )

    record = json.loads(context.record.read_text())
    claim = _claim_of(account, context.run_id)
    assert (record["recovery_actor"], record["recovery_outcome"]) == (me, "pending")
    assert claim is not None and claim["claimant"] == me and claim["attempt"] == ATTEMPT  # the attempt is preserved
    assert queue_entries(account)[context.run_id].last_completed_phase == "claim"
    with pytest.raises(ResourceError) as raised:  # the claimant is this live process: an operator may not take it
        _recover(context, account)
    assert raised.value.reason == "claim-held"

    retire_actor(context.project.state, me, dead_identity(), account=account)  # the claimant dies
    status = _recover(context, account)

    assert (status.state, status.cleanup_outcome) == ("complete", "complete")
    assert queue_entries(account) == {}


def test_empty_grant_claim_crash_is_reclaimable(tmp_path: Path) -> None:
    context, account = _empty_grant(tmp_path)
    write_claim_intent(account, context.run_id, ATTEMPT, expires_in_s=60)
    me = current_identity()
    record_before = context.record.read_bytes()

    _crash_at(
        "queue-saved",
        lambda: _recover(context, account, drain_live_consumers=False, takeover_attempt=ATTEMPT),
    )

    claim = _claim_of(account, context.run_id)
    assert claim is not None and claim["claimant"] == me  # entry claimed, record untouched
    assert context.record.read_bytes() == record_before
    retire_actor(context.project.state, me, dead_identity(), account=account)

    status = _recover(context, account)

    assert (status.state, status.cleanup_outcome) == ("complete", "incomplete")
    assert queue_entries(account) == {}


def test_retire_completed_refuses_live_recovery_actor(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _handle, context, account = _resource_grant(tmp_path, monkeypatch)
    me = current_identity()
    _crash_at("queue-tempfile-written", lambda: _recover(context, account))
    live, identity = start_live_actor("first-recoverer")
    try:
        retire_actor(context.project.state, me, identity, account=account)  # the terminal record names a live actor
        before = authority_bytes(context, account)

        with pytest.raises(ResourceBusy) as raised:
            _recover(context, account)

        assert raised.value.reason == "actor-live"
        assert authority_bytes(context, account) == before
    finally:
        reap(live)


@pytest.mark.parametrize("grant", ["resource-grant", "empty-grant"])
def test_claim_bind_is_one_metadata_section(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, grant: str) -> None:
    if grant == "resource-grant":
        _handle, context, account = _resource_grant(tmp_path, monkeypatch)
        first, second = "recovery-record-written", "recovery-claim-bound"
    else:
        context, account = _empty_grant(tmp_path)
        first, second = "empty-grant-claim-checked", "empty-grant-claim-written"
    write_claim_intent(account, context.run_id, ATTEMPT, expires_in_s=60)
    me = current_identity()
    log: list[tuple] = []
    seen: dict[str, tuple[dict, dict | None]] = {}

    def stage(name: str) -> None:
        log.append(("stage", name))
        if name in (first, second):
            seen[name] = (json.loads(context.record.read_text()), _claim_of(account, context.run_id))

    install_lock_observer(lambda *event: log.append(event))
    install_failpoint(stage)
    try:
        _recover(context, account, drain_live_consumers=False, takeover_attempt=ATTEMPT)
    finally:
        install_failpoint(None)
        install_lock_observer(None)

    record_at_first, claim_at_first = seen[first]
    _record_at_second, claim_at_second = seen[second]
    if grant == "resource-grant":
        assert record_at_first["recovery_actor"] == me  # the record is written first ...
        assert claim_at_first is not None and claim_at_first["claimant"] is None  # ... the claimant is not yet bound
    else:
        assert claim_at_first is not None and claim_at_first["claimant"] is None  # checked, nothing written
    assert claim_at_second is not None and claim_at_second["claimant"] == me  # ... and bound by the second stage
    stages = [index for index, event in enumerate(log) if event[0] == "stage" and event[1] in (first, second)]
    metadata = [(index, event[0]) for index, event in enumerate(log) if len(event) > 2 and event[1] == "metadata"]
    before_first = [kind for index, kind in metadata if index < stages[0]]
    between = [kind for index, kind in metadata if stages[0] < index < stages[1]]
    after_second = [kind for index, kind in metadata if index > stages[1]]
    assert before_first == ["acquire"]  # one metadata acquisition opens the section ...
    assert between == []  # ... nothing leaves or re-enters it between the two stages ...
    assert after_second[0] == "release"  # ... and the same section ends right after the bind


# --- Deadlines, phases and the claim bind across real processes -------------------------------------------------------


def _registry_lock(context: lifecycle.RunContext) -> Path:
    return context.project.state / "registry.lock"


def _crashed_terminal_resource_grant(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """A resource grant whose recovery died after the terminal record write: the entry and index still exist."""
    _handle, context, account = _resource_grant(tmp_path, monkeypatch)
    me = current_identity()
    _crash_at("queue-tempfile-written", lambda: _recover(context, account))
    retire_actor(context.project.state, me, dead_identity(), account=account)
    return context, account


def _marker(path: Path) -> dict:
    return json.loads(path.read_text())


def _authority(captured: dict) -> tuple[dict, dict | None]:
    """The record's ``recovery_actor`` and the entry claim out of a captured pair of file images."""
    record = json.loads(bytes.fromhex(captured["record"]))
    queue = json.loads(bytes.fromhex(captured["queue"]))
    entries = queue["entries"]
    claim = next(iter(entries.values()))["takeover"] if entries else None
    return record["recovery_actor"], claim


def _live_authority(context: lifecycle.RunContext, account: Path) -> tuple[dict, dict | None]:
    record = json.loads(context.record.read_text())
    entries = json.loads((account / "queue.json").read_text())["entries"]
    return record["recovery_actor"], (next(iter(entries.values()))["takeover"] if entries else None)


def test_recovery_g_section_honours_deadline(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    context, account = _crashed_terminal_resource_grant(tmp_path, monkeypatch)
    before = authority_bytes(context, account)
    holder = _hold_lock(_registry_lock(context))
    marker, log = tmp_path / "registry-wait", tmp_path / "observer.log"
    stages = {"recovery-registry-wait": {"action": {"do": "write-file", "path": str(marker)}}}
    child, result = start_recover_child(
        tmp_path, context, account, stages, budget_seconds=3, observer_log=log, fake_docker=tmp_path / "bin"
    )
    try:
        outcome = child_result(child, result, cap=23)
    finally:
        reap(child)
        reap(holder)

    assert outcome["reason"] == "lock-deadline"
    assert 3.0 <= outcome["elapsed_s"] < 15  # the wait lasted to the seeded deadline and no longer
    assert marker.exists()
    assert context.run_id in outcome["message"] and "publish" in outcome["message"]
    events = read_observer_log(log)
    after_marker = events[next(i for i, e in enumerate(events) if e.get("stage") == "recovery-registry-wait") :]
    assert not [
        e for e in after_marker if e.get("kind") == "lock" and e["event"] == "acquire" and e["name"] == "registry"
    ]
    assert authority_bytes(context, account) == before  # the timeout wrote nothing


def test_run_lock_holder_after_claim_publication_bounds_recovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _handle, context, account = _resource_grant(tmp_path, monkeypatch)
    write_claim_intent(account, context.run_id, ATTEMPT, expires_in_s=600)
    at_gate, holder_owns = tmp_path / "at-gate", tmp_path / "holder-owns"
    stages = {
        "recovery-first-section-released": {
            "action": [
                {
                    "do": "write-file",
                    "path": str(at_gate),
                    "capture": {"record": str(context.record), "queue": str(account / "queue.json")},
                },
                {"do": "wait-file", "path": str(holder_owns)},
            ]
        }
    }
    budget = 8
    child, result = start_recover_child(
        tmp_path,
        context,
        account,
        stages,
        budget_seconds=budget,
        drain_live_consumers=False,
        takeover_attempt=ATTEMPT,
        fake_docker=tmp_path / "bin",
    )
    holder = None
    try:
        wait_until(at_gate.exists, timeout=30, message="the recoverer published its claim")
        holder = _hold_lock(run_lock_path(context))  # takes the run lock only after the claim is published
        holder_owns.write_text("")
        outcome = child_result(child, result, cap=budget + 20)
    finally:
        reap(child)
        if holder is not None:
            reap(holder)

    assert outcome["reason"] == "lock-deadline"
    assert budget <= outcome["elapsed_s"] < budget + 7
    assert context.run_id in outcome["message"] and "after phase consumers" in outcome["message"]
    assert _live_authority(context, account) == _authority(_marker(at_gate))  # the claim and the actor stay
    status = _recover(context, account)  # the claimant is dead: a fresh recoverer resumes to the end
    assert (status.state, status.cleanup_outcome) == ("complete", "complete")


def test_recovery_budget_is_shared_across_callees(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _handle, context, account = _resource_grant(tmp_path, monkeypatch)
    add_active_consumer(context, dead_identity())  # drained without a signal: its process is gone
    at_gate, h1_owns = tmp_path / "at-gate", tmp_path / "h1-owns"
    consumers_entered, registry_wait, log = tmp_path / "consumers-entered", tmp_path / "registry-wait", tmp_path / "log"
    stages = {
        "recovery-first-section-released": {
            "action": [{"do": "write-file", "path": str(at_gate)}, {"do": "wait-file", "path": str(h1_owns)}]
        },
        "recovery-phase-consumers": {"action": {"do": "write-file", "path": str(consumers_entered)}},
        "recovery-registry-wait": {"action": {"do": "write-file", "path": str(registry_wait)}},
    }
    budget = 6
    h2 = _hold_lock(_registry_lock(context))  # holds registry.lock throughout
    child, result = start_recover_child(
        tmp_path, context, account, stages, budget_seconds=budget, observer_log=log, fake_docker=tmp_path / "bin"
    )
    h1 = None
    try:
        wait_until(at_gate.exists, timeout=30, message="first section released")
        h1 = _hold_lock(run_lock_path(context))
        h1_owns.write_text("")
        wait_until(consumers_entered.exists, timeout=30, message="consumers phase entered")
        release_at = _marker(consumers_entered)["monotonic_ns"] + 3_000_000_000
        while time.monotonic_ns() < release_at:
            time.sleep(0.05)
        reap(h1)  # H1 released three seconds after the recoverer entered the callee
        outcome = child_result(child, result, cap=budget + 20)
    finally:
        reap(child)
        reap(h2)
        if h1 is not None:
            reap(h1)

    assert outcome["reason"] == "lock-deadline"
    assert budget <= outcome["elapsed_s"] < 8.5  # a fresh budget per callee would end no earlier than 9 s
    assert context.run_id in outcome["message"] and "after phase publish" in outcome["message"]
    events = read_observer_log(log)
    entered = next(e["t"] for e in events if e.get("stage") == "recovery-phase-consumers")
    run_acquired = next(
        e["t"]
        for e in events
        if e.get("event") == "acquire" and e["name"] == f"run-{context.run_id}" and e["t"] > entered
    )
    assert run_acquired - entered >= 3_000_000_000  # the callee really waited for H1
    waited = next(i for i, e in enumerate(events) if e.get("stage") == "recovery-registry-wait")
    assert registry_wait.exists()
    assert not [e for e in events[waited:] if e.get("event") == "acquire" and e["name"] == "registry"]


def test_recovery_callee_waits_share_the_original_deadline(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _handle, context, account = _resource_grant(tmp_path, monkeypatch)
    add_active_consumer(context, dead_identity())  # the drain callee runs (the consumer is registered but gone)
    at_gate, h1_owns, consumers_entered = tmp_path / "at-gate", tmp_path / "h1-owns", tmp_path / "consumers-entered"
    stages = {
        "recovery-first-section-released": {
            "action": [{"do": "write-file", "path": str(at_gate)}, {"do": "wait-file", "path": str(h1_owns)}]
        },
        # entering the callee three seconds late: a callee that took a fresh budget would outlive the original one
        "recovery-phase-consumers": {
            "action": [{"do": "write-file", "path": str(consumers_entered)}, {"do": "sleep", "seconds": 3}]
        },
    }
    budget = 6
    child, result = start_recover_child(
        tmp_path, context, account, stages, budget_seconds=budget, fake_docker=tmp_path / "bin"
    )
    holder = None
    try:
        wait_until(at_gate.exists, timeout=30, message="first section released")
        holder = _hold_lock(run_lock_path(context))  # held until the recoverer has timed out
        h1_owns.write_text("")
        outcome = child_result(child, result, cap=budget + 20)
    finally:
        reap(child)
        if holder is not None:
            reap(holder)

    assert outcome["reason"] == "lock-deadline"
    assert budget <= outcome["elapsed_s"] < 8.5  # the callee's wait ended at the original deadline, not 3 + 6 s
    assert context.run_id in outcome["message"] and "after phase receipts" in outcome["message"]


def test_recovery_phase_boundary_expiry_leaves_claim(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _handle, context, account = _resource_grant(tmp_path, monkeypatch)
    write_claim_intent(account, context.run_id, ATTEMPT, expires_in_s=600)
    at_boundary = tmp_path / "at-boundary"
    stages = {
        "recovery-phase-resources": {
            "action": [
                {
                    "do": "write-file",
                    "path": str(at_boundary),
                    "capture": {"record": str(context.record), "queue": str(account / "queue.json")},
                },
                {"do": "wait-until-deadline"},
            ]
        }
    }
    child, result = start_recover_child(
        tmp_path,
        context,
        account,
        stages,
        budget_seconds=2,
        drain_live_consumers=False,
        takeover_attempt=ATTEMPT,
        fake_docker=tmp_path / "bin",
    )
    try:
        outcome = child_result(child, result, cap=22)
    finally:
        reap(child)

    assert outcome["reason"] == "lock-deadline"
    assert 2.0 <= outcome["elapsed_s"] < 15
    assert context.run_id in outcome["message"] and "after phase consumers" in outcome["message"]
    captured = _marker(at_boundary)
    assert authority_bytes(context, account) == (bytes.fromhex(captured["record"]), bytes.fromhex(captured["queue"]))
    record = json.loads(context.record.read_text())
    assert record["owned_resources"]["containers"]["a" * 64]["state"] != "removed"  # no resources work began


def test_recovery_timeout_on_queued_entry_reports_last_phase_and_resumes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _handle, context, account = _resource_grant(tmp_path, monkeypatch)
    write_claim_intent(account, context.run_id, ATTEMPT, expires_in_s=600)
    assert queue_entries(account)[context.run_id].last_completed_phase is None
    at_boundary = tmp_path / "at-boundary"
    stages = {
        "recovery-phase-resources": {
            "action": [
                {
                    "do": "write-file",
                    "path": str(at_boundary),
                    "capture": {"record": str(context.record), "queue": str(account / "queue.json")},
                },
                {"do": "wait-until-deadline"},
            ]
        }
    }
    child, result = start_recover_child(
        tmp_path,
        context,
        account,
        stages,
        budget_seconds=2,
        drain_live_consumers=False,
        takeover_attempt=ATTEMPT,
        fake_docker=tmp_path / "bin",
    )
    try:
        outcome = child_result(child, result, cap=22)
    finally:
        reap(child)

    assert outcome["reason"] == "lock-deadline"
    assert queue_entries(account)[context.run_id].last_completed_phase == "consumers"  # order, literal
    assert context.run_id in outcome["message"] and "after phase consumers" in outcome["message"]
    assert _live_authority(context, account) == _authority(_marker(at_boundary))  # the timeout changed neither
    status = _recover(context, account)  # an operator resumes once the claimant is gone
    assert (status.state, status.cleanup_outcome) == ("complete", "complete")
    assert set(asdict(status)) == {
        "admission",
        "cleanup_outcome",
        "generation",
        "revision",
        "run_id",
        "state",
        "test_outcome",
        "ticket",
    }


def test_recovery_timeout_on_entry_free_run_reports_derived_phase_and_resumes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    handle, context, _container, account = _retained_owned_container(tmp_path, monkeypatch)
    _interrupt_retained_release(handle, context, monkeypatch)
    retire_actor(context.project.state, current_identity(), dead_identity())  # the interrupted releaser died
    assert queue_entries(account).get(context.run_id) is None  # a release target has no queue entry
    at_boundary = tmp_path / "at-boundary"
    stages = {
        "recovery-phase-resources": {
            "action": [
                {
                    "do": "write-file",
                    "path": str(at_boundary),
                    "capture": {"record": str(context.record), "queue": str(account / "queue.json")},
                },
                {"do": "wait-until-deadline"},
            ]
        }
    }
    child, result = start_recover_child(
        tmp_path, context, account, stages, budget_seconds=2, fake_docker=tmp_path / "bin"
    )
    try:
        outcome = child_result(child, result, cap=22)
    finally:
        reap(child)

    record = json.loads(context.record.read_text())
    assert lifecycle.derive_recovery_phase(record) == "consumers"
    assert outcome["reason"] == "lock-deadline"
    assert context.run_id in outcome["message"] and "after phase consumers" in outcome["message"]
    assert record["recovery_actor"] == _authority(_marker(at_boundary))[0]  # unchanged by the timeout
    status = _recover(context, account)
    assert status.state == "complete"
    assert set(asdict(status)) == {
        "admission",
        "cleanup_outcome",
        "generation",
        "revision",
        "run_id",
        "state",
        "test_outcome",
        "ticket",
    }


def test_recovery_timeout_reports_the_later_phase_derived_later(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _handle, context, account = _resource_grant(tmp_path, monkeypatch)
    write_claim_intent(account, context.run_id, ATTEMPT, expires_in_s=600)
    worker_stage = {"recovery-phase-resources": {"action": {"do": "wait-until-deadline"}}}
    worker, worker_result = start_recover_child(
        tmp_path,
        context,
        account,
        worker_stage,
        name="worker",
        budget_seconds=2,
        drain_live_consumers=False,
        takeover_attempt=ATTEMPT,
        fake_docker=tmp_path / "bin",
    )
    try:
        child_result(worker, worker_result, cap=22)  # stored phase: consumers; the worker is now dead
    finally:
        reap(worker)
    assert queue_entries(account)[context.run_id].last_completed_phase == "consumers"
    operator_stage = {"recovery-phase-publish": {"action": {"do": "wait-until-deadline"}}}
    operator, operator_result = start_recover_child(
        tmp_path, context, account, operator_stage, name="operator", budget_seconds=3, fake_docker=tmp_path / "bin"
    )
    try:
        outcome = child_result(operator, operator_result, cap=23)
    finally:
        reap(operator)

    assert outcome["reason"] == "lock-deadline" and context.run_id in outcome["message"]
    assert "after phase private-root" in outcome["message"]  # the record is further along than the stored phase
    assert queue_entries(account)[context.run_id].last_completed_phase == "consumers"  # the operator stores nothing
    status = _recover(context, account)
    assert (status.state, status.cleanup_outcome) == ("complete", "complete")


def test_recovery_timeout_reports_the_later_phase_stored_later(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _handle, context, account = _resource_grant(tmp_path, monkeypatch)
    write_claim_intent(account, context.run_id, ATTEMPT, expires_in_s=600)
    me = current_identity()
    _crash_at(
        "recovery-phase-private-root",
        lambda: _recover(context, account, drain_live_consumers=False, takeover_attempt=ATTEMPT),
    )
    retire_actor(context.project.state, me, dead_identity(), account=account)  # the claimant died, resources removed
    record = json.loads(context.record.read_text())
    assert lifecycle.derive_recovery_phase(record) == "resources"  # derived: the private root is still present
    queue = json.loads((account / "queue.json").read_text())
    queue["entries"][context.run_id]["last_completed_phase"] = "private-root"  # stored: further along (and "later")
    write_raw_queue(account, canonical_json_bytes(queue))
    stages = {"recovery-phase-private-root": {"action": {"do": "wait-until-deadline"}}}
    child, result = start_recover_child(
        tmp_path, context, account, stages, budget_seconds=2, fake_docker=tmp_path / "bin"
    )
    try:
        outcome = child_result(child, result, cap=22)
    finally:
        reap(child)

    assert outcome["reason"] == "lock-deadline" and context.run_id in outcome["message"]
    assert "after phase private-root" in outcome["message"]  # not "resources", which sorts after it as a string


@pytest.mark.parametrize("mode", ["worker", "operator"])
@pytest.mark.parametrize("grant", ["resource-grant", "empty-grant"])
def test_recovery_path_waits_are_all_bounded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str, grant: str
) -> None:
    if grant == "resource-grant":
        _handle, context, account = _resource_grant(tmp_path, monkeypatch)
        add_active_consumer(context, dead_identity())  # registered but already exited: drained without a signal
    else:
        context, account = _empty_grant(tmp_path)
    arguments: dict = {}
    if mode == "worker":
        write_claim_intent(account, context.run_id, ATTEMPT, expires_in_s=600)
        arguments = {"drain_live_consumers": False, "takeover_attempt": ATTEMPT}
    log: list[tuple] = []
    install_lock_observer(lambda *event: log.append(event))
    install_failpoint(lambda stage: log.append(("stage", stage)))
    try:
        status = _recover(context, account, **arguments)
    finally:
        install_failpoint(None)
        install_lock_observer(None)

    assert status.state == "complete"
    assert [
        event for event in log if len(event) == 5 and event[0] == "acquire" and event[2:] == (False, True, False)
    ] == []
    unbounded = [event for event in log if len(event) == 5 and event[3] is True and event[4] is False]
    assert unbounded == []  # no wait on any recovery lock is unbounded
    if grant == "resource-grant":
        stage = log.index(("stage", "recovery-drain-second-section"))
        acquired = [event for event in log[stage:] if len(event) == 5 and event[0] == "acquire"]
        assert acquired[0] == ("acquire", context.project.project_id, True, True, True)
        assert acquired[1] == ("acquire", f"run-{context.run_id}", False, True, True)
    else:
        assert ("stage", "recovery-drain-second-section") not in log  # an empty grant never drains consumers


@pytest.mark.parametrize("grant", ["resource-grant", "empty-grant"])
def test_claim_bind_writes_nothing_until_metadata_is_held(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, grant: str
) -> None:
    if grant == "resource-grant":
        _handle, context, account = _resource_grant(tmp_path, monkeypatch)
    else:
        context, account = _empty_grant(tmp_path)
    write_claim_intent(account, context.run_id, ATTEMPT, expires_in_s=600)
    before = authority_bytes(context, account)
    holder = _hold_lock(account / "metadata.lock")
    checks_passed = tmp_path / "checks-passed"
    stages = {"recovery-checks-passed": {"action": {"do": "write-file", "path": str(checks_passed)}}}
    child, result = start_recover_child(
        tmp_path,
        context,
        account,
        stages,
        drain_live_consumers=False,
        takeover_attempt=ATTEMPT,
        fake_docker=tmp_path / "bin",
    )
    try:
        wait_until(checks_passed.exists, timeout=30, message="the checks passed")
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline:  # the recoverer waits at metadata: nothing may change while it does
            assert authority_bytes(context, account) == before
            time.sleep(0.05)
        reap(holder)
        outcome = child_result(child, result, cap=30)
    finally:
        reap(child)
        reap(holder)

    assert outcome["error_type"] is None
    after_record = json.loads(context.record.read_text())
    assert after_record["state"] == "complete"


# --- Detection and spawn -------------------------------------------------------------------------------


def _empty_spec(project: Path, purpose: str, wait_seconds: int = 45) -> RunSpec:
    return RunSpec(
        project_root=project,
        entrypoint="integration",
        purpose=purpose,
        services=("postgres",),
        closure=("postgres",),
        profile="fixture",
        phases={"tests": PhaseBudget(0, 1, 1, 1)},
        initial_phase="tests",
        artifact=_identity(),
        wait_seconds=wait_seconds,
    )


def _two_projects(tmp_path: Path) -> tuple[Path, Path]:
    first, second = tmp_path / "project-a", tmp_path / "project-b"
    first.mkdir()
    second.mkdir()
    return first, second


def _dead_granted_holder(services, project: Path) -> lifecycle.RunContext:
    """Enter a run, kill its guardian and make its requester (this process) a dead identity: a holder gone for good."""
    handle = services.open_run(_empty_spec(project, "dead-holder")).enter()
    kill_guardian(handle._context)
    retire_actor(handle._context.project.state, current_identity(), dead_identity())
    return handle._context


def test_queued_guardian_spawns_worker_for_dead_holder(tmp_path: Path) -> None:
    services = build_test_services(tmp_path / "state")
    account = services.root / "admission"
    stranger = seed_foreign_sentinels(tmp_path, services)  # a foreign project F that is neither actor
    first, second = _two_projects(tmp_path)
    dead = _dead_granted_holder(services, first)
    seed_dead_since(account, dead.run_id, age_s=31)
    foreign_before = foreign_snapshot(stranger)  # every protected file class holds a sentinel, or this raises
    require_present(foreign_before)  # and each of them exists, or the equality below would hold vacuously
    waiter_handle = services.open_run(_empty_spec(second, "waiter"))

    waiter = EnterThread(waiter_handle)
    waiter.join(60)  # the queued guardian detects the dead holder and spawns the worker; the waiter is then granted

    assert waiter.error is None
    assert foreign_snapshot(stranger) == foreign_before  # the takeover touched only its target
    record = json.loads(dead.record.read_text())
    assert (record["state"], record["recovery_outcome"]) == ("complete", "complete")
    assert dead.run_id not in queue_entries(account)  # the worker removed the dead holder's entry
    assert queue_entries(account)[waiter_handle.run_id].phase == "granted"
    waiter_handle.leave()


def _worker_processes(token: str) -> list[str]:
    """Takeover workers (``_recover`` processes) whose command line mentions ``token`` (a run id or an attempt)."""
    return [line for line in processes_mentioning(token) if "_recover" in line]


def _dead_queued_entry(tmp_path: Path, services, project: Path, name: str) -> lifecycle.RunContext:
    """A queued run whose guardian is killed right after enrolling and whose requester (this process) is made dead."""
    handle = services.open_run(_empty_spec(project, name))
    prepare_for_child_guardian(handle)
    child, _result = start_guardian_child(
        tmp_path,
        handle,
        services.root / "admission",
        {"queue-saved": {"action": "sigkill", "occurrence": 1}},
        name=name,
    )
    try:
        assert child.wait(timeout=30) == -signal.SIGKILL
    finally:
        reap(child)
    retire_actor(handle._context.project.state, current_identity(), dead_identity())
    return handle._context


def test_an_admission_pass_that_reserves_still_detects_a_dead_queued_entry(tmp_path: Path) -> None:
    services = build_test_services(tmp_path / "state")
    account = services.root / "admission"
    first, second = _two_projects(tmp_path)
    dead = _dead_queued_entry(tmp_path, services, first, "dead-queued")  # no holder: FIFO simply skips it
    seed_dead_since(account, dead.run_id, age_s=31)

    admitted = services.open_run(_empty_spec(second, "admitted-at-once")).enter()  # reserves on its very first pass

    try:
        wait_until(
            lambda: dead.run_id not in queue_entries(account),
            timeout=30,
            message="the dead queued entry was recovered by a pass that did not block",
        )
    finally:
        admitted.leave()
    assert json.loads(dead.record.read_text())["state"] == "complete"


def test_a_pass_that_outlasts_the_queue_budget_revokes_instead_of_crashing(tmp_path: Path) -> None:
    """The budget can end between the loop head's check and the pass's own check; the guardian must not die queued."""
    services = build_test_services(tmp_path / "state")
    account = services.root / "admission"
    project, _other = _two_projects(tmp_path)
    handle = services.open_run(_empty_spec(project, "slow-pass", wait_seconds=2))
    prepare_for_child_guardian(handle)
    stages = {
        "detection-read": {"action": {"do": "sleep", "seconds": 3}, "occurrence": 1}
    }  # the pass crosses the budget
    child, _result = start_guardian_child(tmp_path, handle, account, stages)
    waiter = EnterThread(handle, existing_attempt=True)
    try:
        waiter.join(30)
        assert isinstance(waiter.error, ResourceError)  # the requester's own budget ended
        assert child.wait(timeout=30) == 0  # the guardian revoked its expired run and ended cleanly
    finally:
        reap(child)
    record = _json(handle._context.record)
    assert (record["state"], record["admission"]) == ("complete", "revoked")
    assert queue_entries(account) == {}


def test_changed_clock_origin_restarts_dead_grace(tmp_path: Path) -> None:
    services = build_test_services(tmp_path / "state")
    account = services.root / "admission"
    first, second = _two_projects(tmp_path)
    dead = _dead_granted_holder(services, first)
    seed_dead_since(account, dead.run_id, age_s=300, origin="darwin-continuous:the-previous-boot")
    waiter_handle = services.open_run(_empty_spec(second, "waiter", wait_seconds=8))

    waiter = EnterThread(waiter_handle)
    try:
        wait_until(
            lambda: (
                (queue_entries(account)[dead.run_id].dead_since or {}).get("origin") == lifecycle.NativeClock().origin
            ),
            timeout=15,
            message="the grace restarted on the current origin",
        )
        wait_until(lambda: len(_worker_processes(dead.run_id)) == 1, message="one worker waits out the new grace")
        entry = queue_entries(account)[dead.run_id]
        assert entry.takeover is not None and entry.takeover["claimant"] is None  # an intent, not yet a recovery
        assert entry.dead_since is not None
        assert lifecycle.NativeClock().read() - entry.dead_since["tick_ns"] < 15_000_000_000  # restarted just now
    finally:
        waiter.join(30)
    assert json.loads(dead.record.read_text())["recovery_outcome"] != "complete"  # still waiting out the new grace
    wait_until(lambda: dead.run_id not in queue_entries(account), timeout=60, message="the worker recovers the run")


def test_concurrent_passes_spawn_exactly_one_worker(tmp_path: Path) -> None:
    services = build_test_services(tmp_path / "state")
    account = services.root / "admission"
    first, _second = _two_projects(tmp_path)
    dead = _dead_granted_holder(services, first)
    seed_dead_since(account, dead.run_id, age_s=31)
    barrier = threading.Barrier(2)
    spawns: list[str] = []

    def gate(stage: str) -> None:
        if stage == "detection-read":  # both passes finish their unlocked reads before either takes the spawn section
            barrier.wait(timeout=20)
        elif stage == "worker-spawn":
            spawns.append(stage)

    install_failpoint(gate)
    try:
        threads = [
            threading.Thread(target=lifecycle.reconcile_account, kwargs={"_test_directory": account}) for _ in range(2)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(60)
            assert not thread.is_alive()
    finally:
        install_failpoint(None)

    wait_until(lambda: dead.run_id not in queue_entries(account), timeout=45, message="the one worker recovers the run")
    assert json.loads(dead.record.read_text())["recovery_outcome"] == "complete"
    assert len(spawns) == 1  # the second pass saw the first pass's claim intent and spawned nothing


def _three_dead_entries(tmp_path: Path, services) -> tuple[lifecycle.RunContext, ...]:
    """A dead holder and two dead queued entries, in ticket order; no guardian is left to run a pass of its own."""
    projects = [tmp_path / name for name in ("project-a", "project-b", "project-c")]
    for project in projects:
        project.mkdir()
    dead = _dead_granted_holder(services, projects[0])
    return dead, *(
        _dead_queued_entry(tmp_path, services, project, f"dead-queued-{index}")
        for index, project in enumerate(projects[1:])
    )


def test_spawn_respects_the_account_cap(tmp_path: Path) -> None:
    services = build_test_services(tmp_path / "state")
    account = services.root / "admission"
    dead, first, second = _three_dead_entries(tmp_path, services)
    for context in (first, second):  # two live claims elsewhere: the account is at its cap
        write_claim_intent(account, context.run_id, ATTEMPT, expires_in_s=600)

    report = lifecycle.reconcile_account(_test_directory=account)

    assert report.spawned == 0
    assert queue_entries(account)[dead.run_id].takeover is None  # no worker for the dead holder
    assert _worker_processes(dead.run_id) == []

    write_claim_intent(account, first.run_id, ATTEMPT, expires_in_s=-1)  # one claim lapses: room for one

    assert lifecycle.reconcile_account(_test_directory=account).spawned == 1
    assert queue_entries(account)[dead.run_id].takeover is not None  # the first in ticket order
    assert queue_entries(account)[second.run_id].takeover["attempt"] == ATTEMPT
    wait_until(lambda: dead.run_id not in queue_entries(account), timeout=60, message="the dead holder recovered")
    wait_until(lambda: first.run_id in queue_entries(account), message="the lapsed entry keeps its place")


def test_one_pass_spawns_up_to_the_cap(tmp_path: Path) -> None:
    services = build_test_services(tmp_path / "state")
    account = services.root / "admission"
    dead, queued, third = _three_dead_entries(tmp_path, services)
    for context in (dead, queued, third):
        seed_dead_since(account, context.run_id, age_s=31)

    spawns: list[str] = []
    install_failpoint(lambda stage: spawns.append(stage) if stage == "worker-spawn" else None)
    try:
        report = lifecycle.reconcile_account(_test_directory=account)
    finally:
        install_failpoint(None)

    assert (report.spawned, report.spawn_failed, report.skipped) == (2, 0, False)
    assert len(spawns) == 2
    third_entry = queue_entries(account)[third.run_id]
    assert third_entry.takeover is None and third_entry.dead_since is not None  # it waits for a later pass
    recovered = {dead.run_id, queued.run_id}
    wait_until(lambda: not recovered & set(queue_entries(account)), timeout=60, message="both takeovers complete")


def test_one_pass_claims_a_dead_holder_for_the_grace(tmp_path: Path) -> None:
    services = build_test_services(tmp_path / "state")
    account = services.root / "admission"
    first, _second = _two_projects(tmp_path)
    dead = _dead_granted_holder(services, first)
    started = lifecycle.NativeClock().read()

    report = lifecycle.reconcile_account(_test_directory=account)  # nothing queued behind it, no seeded death time

    assert report.spawned == 1
    entry = queue_entries(account)[dead.run_id]
    assert entry.dead_since is not None and entry.takeover is not None and entry.takeover["claimant"] is None
    assert 59e9 <= entry.takeover["deadline"]["expires_ns"] - entry.dead_since["tick_ns"] <= 61e9
    assert len(_worker_processes(dead.run_id)) == 1
    wait_until(lambda: dead.run_id not in queue_entries(account), timeout=90, message="the worker recovers the run")
    assert lifecycle.NativeClock().read() - started >= lifecycle.DEAD_GRACE_NS
    assert json.loads(dead.record.read_text())["recovery_outcome"] == "complete"


def test_a_failed_spawn_is_not_retried_before_its_intent_expires(tmp_path: Path) -> None:
    services = build_test_services(tmp_path / "state")
    account = services.root / "admission"
    first, _second = _two_projects(tmp_path)
    dead = _dead_granted_holder(services, first)

    def fail_spawn(stage: str) -> None:
        if stage == "worker-spawn":
            raise OSError("no fork")

    install_failpoint(fail_spawn)
    try:
        report = lifecycle.reconcile_account(_test_directory=account)
        assert (report.spawned, report.spawn_failed) == (0, 1)
        takeover = queue_entries(account)[dead.run_id].takeover
        assert takeover is not None and takeover["claimant"] is None  # the intent stays to expire

        again = lifecycle.reconcile_account(_test_directory=account)
        assert (again.spawned, again.spawn_failed) == (0, 0)
        assert queue_entries(account)[dead.run_id].takeover == takeover
    finally:
        install_failpoint(None)

    write_claim_intent(account, dead.run_id, ATTEMPT, expires_in_s=-1)
    assert lifecycle.reconcile_account(_test_directory=account).spawned == 1
    wait_until(lambda: dead.run_id not in queue_entries(account), timeout=90, message="the new worker recovers the run")


def test_a_pass_that_misses_its_lock_budget_raises_and_writes_nothing(tmp_path: Path) -> None:
    services = build_test_services(tmp_path / "state")
    account = services.root / "admission"
    first, _second = _two_projects(tmp_path)
    dead = _dead_granted_holder(services, first)
    before = (account / "queue.json").read_bytes()
    missed: list[ResourceBusy] = []

    def run_pass() -> None:
        try:
            lifecycle.reconcile_account(_test_directory=account, _deadline_ns=time.monotonic_ns() + 200_000_000)
        except ResourceBusy as error:
            missed.append(error)

    with lock(Context(account, account), "metadata"):
        pass_thread = threading.Thread(target=run_pass)
        pass_thread.start()
        pass_thread.join(30)
        assert not pass_thread.is_alive()

    assert [error.reason for error in missed] == ["lock-deadline"]
    assert (account / "queue.json").read_bytes() == before
    assert _worker_processes(dead.run_id) == []


def test_claim_intent_crash_expires_and_respawns(tmp_path: Path) -> None:
    services = build_test_services(tmp_path / "state")
    account = services.root / "admission"
    first, _second = _two_projects(tmp_path)
    dead = _dead_granted_holder(services, first)
    write_claim_intent(account, dead.run_id, ATTEMPT, expires_in_s=-1)  # an intent whose spawner crashed, now lapsed
    before = authority_bytes(dead, account)

    exit_code = lifecycle.main(_worker_arguments(dead, ATTEMPT, account))  # the lapsed attempt's worker refuses
    assert exit_code == 1
    assert authority_bytes(dead, account) == before

    lifecycle.reconcile_account(_test_directory=account)  # a new pass claims afresh and spawns a new worker

    wait_until(lambda: dead.run_id not in queue_entries(account), timeout=90, message="the dead holder recovered")
    assert json.loads(dead.record.read_text())["recovery_outcome"] == "complete"


def _worker_arguments(context: lifecycle.RunContext, attempt: str, account: Path) -> list[str]:
    return [
        "_recover",
        "--context",
        lifecycle._context_payload(context),
        "--attempt",
        attempt,
        "--test-directory",
        str(account),
    ]


def test_worker_refuses_a_hostile_queue_instead_of_crashing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _handle, context, account = _resource_grant(tmp_path, monkeypatch)
    write_raw_queue(account, b"[" * 200_000 + b"]" * 200_000)  # nested far beyond the parser's recursion limit
    record_before = context.record.read_bytes()

    exit_code = lifecycle.main(_worker_arguments(context, ATTEMPT, account))  # its read-only attempt check reads it

    assert exit_code == 1  # a named refusal, not a RecursionError
    assert context.record.read_bytes() == record_before


def test_recovery_timeout_report_tolerates_a_hostile_queue(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _handle, context, account = _resource_grant(tmp_path, monkeypatch)
    write_raw_queue(account, b"[" * 200_000 + b"]" * 200_000)
    holder = _hold_lock(run_lock_path(context))  # the run lock stays held: the recovery ends at its deadline
    try:
        with pytest.raises(ResourceBusy) as raised:
            _recover(context, account, _deadline_ns=time.monotonic_ns() + 1_000_000_000)
    finally:
        reap(holder)

    assert raised.value.reason == "lock-deadline"  # the phase report survives the unreadable queue
    assert context.run_id in str(raised.value) and "after phase" in str(raised.value)


@pytest.mark.parametrize("claim", ["no-claim", "foreign-attempt"])
def test_worker_refuses_an_attempt_that_is_not_on_its_entry(tmp_path: Path, claim: str) -> None:
    services = build_test_services(tmp_path / "state")
    account = services.root / "admission"
    first, _second = _two_projects(tmp_path)
    dead = _dead_granted_holder(services, first)
    if claim == "foreign-attempt":
        write_claim_intent(account, dead.run_id, OTHER_ATTEMPT, expires_in_s=600)
    before = authority_bytes(dead, account)

    exit_code = lifecycle.main(
        _worker_arguments(dead, ATTEMPT, account)
    )  # with no claim at all `recover_run` would pass

    assert exit_code == 1
    assert authority_bytes(dead, account) == before
    assert json.loads(dead.record.read_text())["recovery_outcome"] != "complete"


def test_an_entry_seen_alive_again_loses_its_dead_since(tmp_path: Path) -> None:
    services = build_test_services(tmp_path / "state")
    account = services.root / "admission"
    first, _second = _two_projects(tmp_path)
    handle = services.open_run(_empty_spec(first, "alive-holder")).enter()  # its guardian and requester both run
    seed_dead_since(account, handle._context.run_id, age_s=31)  # a stale sighting from an earlier pass
    spawns: list[str] = []
    install_failpoint(lambda stage: spawns.append(stage) if stage == "worker-spawn" else None)
    try:
        lifecycle.reconcile_account(_test_directory=account)
    finally:
        install_failpoint(None)

    entry = queue_entries(account)[handle._context.run_id]
    assert entry.dead_since is None and entry.takeover is None
    assert spawns == []
    handle.leave()


def test_an_entry_whose_holder_still_runs_is_not_dead(tmp_path: Path) -> None:
    services = build_test_services(tmp_path / "state")
    account = services.root / "admission"
    first, _second = _two_projects(tmp_path)
    dead = _dead_granted_holder(services, first)  # the record's requester and guardian are gone
    live, identity = start_live_actor("slot-holder")
    try:
        queue = json.loads((account / "queue.json").read_text())
        queue["entries"][dead.run_id]["holder"] = identity  # but the entry names a process that still runs as holder
        write_raw_queue(account, canonical_json_bytes(queue))
        seed_dead_since(account, dead.run_id, age_s=31)
        spawns: list[str] = []
        install_failpoint(lambda stage: spawns.append(stage) if stage == "worker-spawn" else None)
        try:
            lifecycle.reconcile_account(_test_directory=account)
        finally:
            install_failpoint(None)

        entry = queue_entries(account)[dead.run_id]
        assert spawns == []  # every actor must be proven gone, the slot holder included
        assert entry.dead_since is None and entry.takeover is None
    finally:
        reap(live)


def test_reconcile_preparations_waits_outside_locks_on_claim_held(tmp_path: Path) -> None:
    services = build_test_services(tmp_path / "state")
    account = services.root / "admission"
    project = tmp_path / "project"
    project.mkdir()
    queued = _dead_queued_entry(tmp_path, services, project, "own-dead-run")
    claimant, identity = start_live_actor("claimant")
    free, held = [], []

    def probe(stage: str) -> None:
        if stage != "reconcile-preparations-wait":
            return
        try:  # at the wait itself the pass holds no lock: an exclusive project lock is free
            with lock(queued.project, queued.project.project_id, nonblocking=True):
                free.append(stage)
        except ResourceBusy:
            held.append(stage)

    try:
        write_claim_intent(account, queued.run_id, ATTEMPT, expires_in_s=600, claimant=identity)
        install_failpoint(probe)
        started = time.monotonic()
        with pytest.raises(ResourceBusy) as raised:
            lifecycle.reconcile_preparations(
                queued.project, _test_directory=account, _deadline_ns=time.monotonic_ns() + 2_000_000_000
            )
        elapsed = time.monotonic() - started
    finally:
        install_failpoint(None)
        reap(claimant)

    assert raised.value.reason == "lock-deadline" and elapsed >= 2.0
    assert len(free) >= 5 and held == []  # every wait between retries was lock-free


def test_spawned_worker_inherits_no_lock_descriptor(tmp_path: Path) -> None:
    services = build_test_services(tmp_path / "state")
    account = services.root / "admission"
    first, _second = _two_projects(tmp_path)
    dead = _dead_granted_holder(services, first)
    seed_dead_since(account, dead.run_id, age_s=31)
    holder = _hold_lock(run_lock_path(dead))  # the worker waits in its first project/run section
    try:
        lifecycle.reconcile_account(_test_directory=account)
        claim = _claim_of(account, dead.run_id)
        assert claim is not None
        deadline = time.monotonic() + 30
        worker = None
        while worker is None:
            lines = processes_mentioning(f"--attempt {claim['attempt']}")
            if lines:
                worker = int(lines[0].split()[0])
            assert time.monotonic() < deadline, f"worker with --attempt {claim['attempt']} did not appear within 30 s"
            time.sleep(0.1)
        run_lock = str(run_lock_path(dead))
        names: list[str] = []
        while time.monotonic() < deadline:  # the process exists before it reaches its first lock wait
            names = [name for name in open_file_names(worker) if name.endswith(".lock")]
            if run_lock in names:
                break
            time.sleep(0.1)
        assert run_lock in names, "the worker never reached its run lock wait"
        project_lock = str(dead.project.state / f"{dead.project.project_id}.lock")
        assert sorted(names) == sorted([project_lock, run_lock])  # exactly these two, one descriptor each
    finally:
        reap(holder)
    wait_until(lambda: dead.run_id not in queue_entries(account), timeout=45, message="the worker finished")


def test_enter_run_waits_while_a_worker_holds_its_claim(tmp_path: Path) -> None:
    services = build_test_services(tmp_path / "state")
    account = services.root / "admission"
    project = tmp_path / "project"
    project.mkdir()
    handle = services.open_run(_empty_spec(project, "waiting-requester", wait_seconds=30))
    prepare_for_child_guardian(handle)
    child, _result = start_guardian_child(
        tmp_path, handle, account, {"queue-saved": {"action": "sigkill", "occurrence": 1}}, name="guardian"
    )
    try:
        assert child.wait(timeout=30) == -signal.SIGKILL  # the guardian died right after enrolling; the requester lives
    finally:
        reap(child)
    claimant, identity = start_live_actor("worker")
    waiter = None
    try:
        write_claim_intent(account, handle._context.run_id, ATTEMPT, expires_in_s=600, claimant=identity)
        waiter = EnterThread(handle, existing_attempt=True)
        time.sleep(1.5)  # the requester wants to revoke its own run, but a live worker holds the entry's claim
        assert waiter.error is None  # `claim-held` is a wait here, never an error
        assert json.loads(handle._context.record.read_text())["state"] != "complete"
    finally:
        reap(claimant)  # the worker dies: the claim becomes reclaimable and the requester revokes its own run
    assert waiter is not None
    waiter.join(30)
    assert isinstance(waiter.error, ResourceError) and "Admission ended before grant" in str(waiter.error)
    assert json.loads(handle._context.record.read_text())["state"] == "complete"


# --- Real SIGKILL at the representative boundaries ---
#
# Each boundary kills a real process at a durable boundary, asserts the files between the kill and the resume against
# the documented prefix, and only then lets a fresh call finish. The failpoint callback lives in the killed child;
# production code carries no hook for it.


def _json(path: Path) -> dict:
    return json.loads(path.read_text())


def _slot_is_free(services, tmp_path: Path, name: str) -> None:
    """Another project is granted the slot and leaves again: no entry holds it."""
    other = tmp_path / f"slot-{name}"
    other.mkdir()
    follower = services.open_run(_empty_spec(other, name)).enter()
    assert follower.status().state == "active"
    follower.leave()


def _sigkill_claimant_after_actor_write(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _handle, context, account = _resource_grant(tmp_path, monkeypatch)
    child, _result = start_recover_child(
        tmp_path,
        context,
        account,
        {"recovery-record-written": {"action": "sigkill"}},
        fake_docker=tmp_path / "bin",
    )
    try:
        assert child.wait(timeout=60) == -signal.SIGKILL
    finally:
        reap(child)
    # The prefix: the record names the killed recoverer as its pending actor; nothing was cleaned up yet.
    record = _json(context.record)
    assert record["recovery_outcome"] == "pending" and record["recovery_actor"]["pid"] == child.pid
    assert lifecycle.observe_process(lifecycle._identity_from_json(record["recovery_actor"])) == "absent"
    assert (tmp_path / "bin" / "container.json").exists()
    assert set(queue_phases(account)) == {context.run_id}

    status = _recover(context, account)  # the actor is proven gone: a fresh recoverer takes over and finishes

    assert (status.state, status.cleanup_outcome) == ("complete", "complete")
    assert not (tmp_path / "bin" / "container.json").exists()
    assert queue_phases(account) == {}


def _sigkill_claimant_mid_docker_cleanup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _handle, context, account = _resource_grant(tmp_path, monkeypatch)
    binaries = tmp_path / "bin"
    (binaries / "mode").write_text("block-rm")  # the exact container removal starts and never finishes
    child, _result = start_recover_child(tmp_path, context, account, {}, fake_docker=binaries)
    try:
        wait_until(lambda: (binaries / "in-removal").exists(), timeout=60, message="the removal started")
        child.kill()
        assert child.wait(timeout=30) == -signal.SIGKILL
    finally:
        reap(child)
    # The prefix: the recoverer is the pending actor, the container still exists and the entry is still held.
    record = _json(context.record)
    assert record["recovery_outcome"] == "pending" and record["recovery_actor"]["pid"] == child.pid
    assert (binaries / "container.json").exists()
    assert set(queue_phases(account)) == {context.run_id}
    (binaries / "mode").write_text("")  # the Docker daemon answers normally again

    status = _recover(context, account)

    assert (status.state, status.cleanup_outcome) == ("complete", "complete")
    assert not (binaries / "container.json").exists()
    assert queue_phases(account) == {}


def _sigkill_first_queue_replacement(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, stage: str) -> None:
    services = build_test_services(tmp_path / "state")
    account = services.root / "admission"
    anchor = account / "metadata.lock"
    anchor.write_text(STATE_MARKER)
    anchor.chmod(0o600)
    write_raw_queue(account, EMPTY_QUEUE)  # the empty queue file a used account owns
    project = tmp_path / "project"
    project.mkdir()
    handle = services.open_run(_spec(project, "first-replacement"))
    prepare_for_child_guardian(handle)
    guardian, _result = start_guardian_child(tmp_path, handle, account, {stage: {"action": "sigkill", "occurrence": 1}})
    try:
        assert guardian.wait(timeout=30) == -signal.SIGKILL
        observed = (account / "queue.json").read_bytes()
        if stage == "queue-saved":  # killed after the atomic replace: the complete canonical version 2 bytes
            decoded = json.loads(observed)
            assert (decoded["version"], decoded["next_ticket"], list(decoded["entries"])) == (2, 2, [handle.run_id])
            assert observed == canonical_json_bytes(decoded)
        else:  # killed before it: the old bytes, whole
            assert observed == EMPTY_QUEUE
        entering = EnterThread(handle, existing_attempt=True)  # the live requester revokes the dead guardian's run
        entering.join()
    finally:
        reap(guardian)
    assert isinstance(entering.error, ResourceError)
    assert queue_phases(account) == {}
    _slot_is_free(services, tmp_path, "after-replacement-kill")


SIGKILL_BOUNDARIES = {
    "claimant-after-recovery-actor-write": _sigkill_claimant_after_actor_write,
    "claimant-mid-docker-cleanup": _sigkill_claimant_mid_docker_cleanup,
    "first-queue-replacement": lambda tmp_path, monkeypatch: _sigkill_first_queue_replacement(
        tmp_path, monkeypatch, stage="queue-saved"
    ),
    "first-queue-replacement-before-replace": lambda tmp_path, monkeypatch: _sigkill_first_queue_replacement(
        tmp_path, monkeypatch, stage="queue-tempfile-written"
    ),
}


@pytest.mark.parametrize("boundary", list(SIGKILL_BOUNDARIES))
def test_sigkill_boundaries(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, boundary: str) -> None:
    SIGKILL_BOUNDARIES[boundary](tmp_path, monkeypatch)


# --- The crash matrix ---
#
# Every row crashes one actor at one durable boundary, asserts the durable image (record, queue entry and project index)
# between the crash and the resume against the documented prefix, lets a fresh call finish and then asserts that exactly
# one authority finished: the run is terminal, no entry holds the slot, a second identical resume writes nothing.


@dataclass
class Crashed:
    """A crash and the calls that finish it; ``context`` is None for an account-level (promotion) row."""

    context: lifecycle.RunContext | None
    account: Path
    resume: Callable[[], None]
    again: Callable[[], None] | None = None  # a second identical resume that must change nothing
    follow_on: Callable[[], None] | None = None  # proves the slot is free (a private native account only)
    evidence: dict = field(default_factory=dict)  # what a row's own checks need (promotion rows)


@dataclass(frozen=True)
class CrashRow:
    """One crash-matrix row; ``prefix`` and ``final`` are expected run images or checks of a non-run state."""

    id: str
    arrange: Callable[[Path, pytest.MonkeyPatch], Crashed]
    prefix: dict | Callable[[Crashed], None]  # what is durable right after the crash
    final: dict | Callable[[Crashed], None]  # what is durable once a fresh call has finished


FINAL_RECOVERED = {"state": "complete", "cleanup": "complete", "entry": None, "claim": None, "indexed": False}


def _image(crashed: Crashed) -> dict:
    """The durable image of one run: its record, its queue entry and its project-index reference."""
    context = crashed.context
    record = _json(context.record)
    entry = queue_entries(crashed.account).get(context.run_id)
    claim = None if entry is None or entry.takeover is None else ("bound" if entry.takeover["claimant"] else "intent")
    return {
        "state": record["state"],
        "admission": record["admission"],
        "cleanup": record["cleanup_outcome"],
        "ticket": record["ticket"],
        "guardian_bound": record["guardian"] is not None,
        "acked": record["leave_ack"] is not None,
        "actor": record["recovery_actor"] is not None,
        "dead_since": entry is not None and entry.dead_since is not None,
        "outcome": record["recovery_outcome"],
        "derived": lifecycle.derive_recovery_phase(record),
        "entry": None if entry is None else entry.phase,
        "claim": claim,
        "stored_phase": None if entry is None else entry.last_completed_phase,
        "indexed": context.run_id in _json(context.project.record)["runs"],
        "root": context.private_root.exists() or context.private_root.is_symlink(),
        "session": context.private_root.parent.parent.exists(),  # pruned together with the root
    }


def _holders(account: Path) -> list[str]:
    return [run_id for run_id, entry in queue_entries(account).items() if entry.phase in ("admitting", "granted")]


def _claimant_crash(
    stage: str, *, worker: bool = False, consumer: bool = False
) -> Callable[[Path, pytest.MonkeyPatch], Crashed]:
    """A resource grant whose recoverer (the test process) dies at ``stage``; its resume is a fresh recoverer."""

    def arrange(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Crashed:
        _handle, context, account = _resource_grant(tmp_path, monkeypatch)
        if consumer:
            add_active_consumer(context, dead_identity(1))  # drained without a signal: its process is gone
        me = current_identity()
        arguments: dict = {}
        if worker:
            write_claim_intent(account, context.run_id, ATTEMPT, expires_in_s=60)
            arguments = {"drain_live_consumers": False, "takeover_attempt": ATTEMPT}
        _crash_at(stage, lambda: _recover(context, account, **arguments))
        evidence: dict = {}

        def resume() -> None:
            retire_actor(context.project.state, me, dead_identity(), account=account)  # the recoverer is gone
            evidence["record_before_resume"] = _json(context.record)
            _recover(context, account)

        return Crashed(context, account, resume, again=lambda: _recover(context, account), evidence=evidence)

    return arrange


_STOPPING = {"state": "stopping", "admission": "published", "entry": "granted", "indexed": True}
_BEFORE_RECORD = {**_STOPPING, "actor": False, "outcome": None, "derived": None, "claim": None}
_RECORDED = {**_STOPPING, "actor": True, "outcome": "pending", "claim": None, "stored_phase": None}


def _recovered_without_a_record_rewrite(crashed: Crashed) -> None:
    """The record was already final before the crash: the resumed call only removed the queue entry and the index."""
    _check(FINAL_RECOVERED, crashed, "resumed", "claimant-final-record-before-queue")
    assert _json(crashed.context.record) == crashed.evidence["record_before_resume"]  # the resumed call rewrote nothing


CLAIMANT_ROWS = [
    CrashRow("claimant-phase-authorize", _claimant_crash("recovery-phase-authorize"), _BEFORE_RECORD, FINAL_RECOVERED),
    CrashRow("claimant-checks-passed", _claimant_crash("recovery-checks-passed"), _BEFORE_RECORD, FINAL_RECOVERED),
    CrashRow(
        "claimant-recovery-actor-written",
        _claimant_crash("recovery-record-written"),
        {**_RECORDED, "derived": "consumers"},
        FINAL_RECOVERED,
    ),
    CrashRow(
        "claimant-claim-bound",
        _claimant_crash("recovery-claim-bound", worker=True),
        {**_RECORDED, "claim": "bound", "stored_phase": "claim", "derived": "consumers"},
        FINAL_RECOVERED,
    ),
    *[
        CrashRow(
            f"claimant-phase-{phase}",
            _claimant_crash(f"recovery-phase-{phase}"),
            {**_RECORDED, "derived": derived},
            FINAL_RECOVERED,
        )
        for phase, derived in (
            ("operation", "consumers"),
            ("receipts", "consumers"),
            ("consumers", "consumers"),
            ("resources", "consumers"),
            ("private-root", "resources"),
            ("publish", "private-root"),
        )
    ],
    CrashRow(
        "claimant-drain-second-section",
        _claimant_crash("recovery-drain-second-section", consumer=True),
        {**_RECORDED, "derived": "receipts"},
        FINAL_RECOVERED,
    ),
    CrashRow(
        "claimant-final-record-before-queue",
        _claimant_crash("queue-tempfile-written"),
        {
            **_RECORDED,
            "state": "complete",
            "admission": "revoked",
            "cleanup": "complete",
            "outcome": "complete",
            "derived": "publish",
        },
        _recovered_without_a_record_rewrite,
    ),
    CrashRow(
        "claimant-final-queue-before-index",
        _claimant_crash("recovery-registry-wait"),
        {
            "state": "complete",
            "cleanup": "complete",
            "outcome": "complete",
            "derived": "publish",
            "entry": None,
            "indexed": True,
        },
        FINAL_RECOVERED,
    ),
]


# Creation prefix (a requester that died after ``prepare_run`` wrote its record): exact ``run recover`` repairs it.
def _creation_prefix_crash(stage: str | None) -> Callable[[Path, pytest.MonkeyPatch], Crashed]:
    def arrange(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Crashed:
        service, project = _prefix_account(tmp_path)
        context, _child = _dead_requester_prefix(tmp_path, service, project)
        context.private_root.mkdir(mode=0o700, parents=True)  # the dead requester had made its private root
        account = service.account_directory

        def recover() -> None:
            lifecycle.recover_run(context, _test_directory=account)

        if stage is not None:
            _crash_at(stage, recover)
        return Crashed(context, account, recover, again=recover)

    return arrange


_PREFIX = {
    "state": "prepared",
    "admission": "never-granted",
    "entry": None,
    "actor": False,
    "indexed": False,
    "root": True,
    "session": True,
}
_FINALIZED = {
    "state": "complete",
    "admission": "revoked",
    "cleanup": "complete",
    "entry": None,
    "indexed": False,
    "root": False,
    "session": False,
}
_REPAIRING = {"entry": None, "indexed": True}  # the index entry is restored before anything else is written

CREATION_PREFIX_ROWS = [
    CrashRow("creation-prefix-record-written", _creation_prefix_crash(None), _PREFIX, _FINALIZED),
    CrashRow(
        "creation-prefix-index-restored",
        _creation_prefix_crash("prefix-index-restored"),
        {**_REPAIRING, "state": "prepared", "admission": "never-granted", "root": True, "session": True},
        _FINALIZED,
    ),
    CrashRow(
        "creation-prefix-revoked-record-written",
        _creation_prefix_crash("revoke-record-written"),
        {
            **_REPAIRING,
            "state": "complete",
            "admission": "revoked",
            "cleanup": "pending",
            "root": True,
            "session": True,
        },
        _FINALIZED,
    ),
    CrashRow(
        "creation-prefix-private-root-removed",
        _creation_prefix_crash("private-root-removed"),
        {
            **_REPAIRING,
            "state": "complete",
            "admission": "revoked",
            "cleanup": "pending",
            "root": False,
            "session": False,
        },
        _FINALIZED,
    ),
    CrashRow(
        "creation-prefix-cleanup-written",
        _creation_prefix_crash("revoke-cleanup-written"),
        {
            **_REPAIRING,
            "state": "complete",
            "admission": "revoked",
            "cleanup": "complete",
            "root": False,
            "session": False,
        },
        _FINALIZED,
    ),
]


# Promotion: the queue never changes, the runtime is refused until the transaction ends.


def _revoke_after_record_write(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Crashed:
    """A guardian killed holding the slot (admitting); the revocation dies after its terminal record write."""
    services, account, handle, child = _prepared_run_with_guardian_child(
        tmp_path, {"queue-saved": {"action": "sigkill", "occurrence": 2}}
    )
    try:
        assert child.wait(timeout=30) == -signal.SIGKILL
    finally:
        reap(child)
    context = handle._context

    def revoke() -> None:
        lifecycle.revoke_ungranted(context, _test_directory=account)

    _crash_at("queue-tempfile-written", revoke)  # the removal save: the record is already final
    return Crashed(context, account, revoke, again=revoke, follow_on=lambda: _slot_is_free(services, tmp_path, "after"))


def _spawner_crash(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Crashed:
    """A dead granted holder whose spawner dies right after saving its claim intent (no worker was started)."""
    services = build_test_services(tmp_path / "state")
    account = services.root / "admission"
    first, _second = _two_projects(tmp_path)
    dead = _dead_granted_holder(services, first)
    seed_dead_since(account, dead.run_id, age_s=31)
    _crash_at("queue-saved", lambda: lifecycle.reconcile_account(_test_directory=account))

    def respawn() -> None:
        claim = queue_entries(account)[dead.run_id].takeover
        assert claim is not None
        attempt = claim["attempt"]
        write_claim_intent(account, dead.run_id, attempt, expires_in_s=-1)  # time passes: the unbound intent lapses
        lifecycle.reconcile_account(_test_directory=account)  # a later pass claims afresh and spawns the worker
        index = dead.project.record
        wait_until(
            lambda: dead.run_id not in queue_entries(account) and dead.run_id not in _json(index)["runs"],
            timeout=45,
            message="the worker recovered the dead holder",
        )

    return Crashed(dead, account, respawn)


TAKEOVER_ROWS = [
    CrashRow(
        "revoke-after-record-write",
        _revoke_after_record_write,
        {"state": "complete", "admission": "revoked", "entry": "admitting", "indexed": True},
        {"state": "complete", "admission": "revoked", "entry": None, "indexed": True},
    ),
    CrashRow(
        "spawner-after-claim-intent",
        _spawner_crash,
        {"state": "active", "admission": "published", "actor": False, "entry": "granted", "claim": "intent"},
        {"state": "complete", "outcome": "complete", "entry": None, "claim": None, "indexed": False},
    ),
]


# Guardian and requester rows run real processes: the requester and the guardian are children, so a kill is a kill.
@dataclass
class ChildRun:
    services: object
    account: Path
    context: lifecycle.RunContext
    requester: RequesterChild
    guardian: subprocess.Popen[bytes] | None


def _child_run(
    tmp_path: Path,
    *,
    guardian_stages: dict | None = None,
    requester_stages: dict | None = None,
    leave: bool = False,
    gated: bool = False,
    new_session: bool = False,
) -> ChildRun:
    """Start a requester child; with ``guardian_stages`` also the guardian child (else the run launches its own)."""
    services = build_test_services(tmp_path / "state")
    account = services.root / "admission"
    project = tmp_path / "project"
    project.mkdir()
    requester = start_requester_child(
        tmp_path,
        services.root / "registry",
        account,
        project,
        requester_stages or {},
        launch_guardian=guardian_stages is None,
        leave=leave,
        gated=gated,
        new_session=new_session,
    )
    context = requester.context()
    guardian = None
    if guardian_stages is not None:
        wait_until(
            lambda: _json(context.record)["guardian_attempt"] is not None, message="the guardian attempt started"
        )
        guardian, _result = start_guardian_child(tmp_path, context, account, guardian_stages)
    return ChildRun(services, account, context, requester, guardian)


def _kill(child: subprocess.Popen[bytes]) -> None:
    child.kill()
    assert child.wait(timeout=30) == -signal.SIGKILL


def _guardian_dies(run: ChildRun) -> None:
    assert run.guardian is not None
    try:
        assert run.guardian.wait(timeout=60) == -signal.SIGKILL
    finally:
        reap(run.guardian)


def _requester_ended(run: ChildRun, error: str | None) -> None:
    outcome = child_result(run.requester.process, run.requester.result, cap=60)
    assert outcome["error_type"] == error


def _guardian_killed_before_grant(stage: str, occurrence: int) -> Callable[[Path, pytest.MonkeyPatch], Crashed]:
    """The guardian dies at a pre-grant save; the (gated) live requester then finds it gone and revokes its run."""

    def arrange(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Crashed:
        run = _child_run(tmp_path, guardian_stages={stage: {"action": "sigkill", "occurrence": occurrence}}, gated=True)
        _guardian_dies(run)

        def resume() -> None:
            run.requester.open_gate()
            _requester_ended(run, "ResourceError")

        return Crashed(
            run.context, run.account, resume, follow_on=lambda: _slot_is_free(run.services, tmp_path, "after")
        )

    return arrange


def _recover_after_requester_ends(run: ChildRun) -> Crashed:
    return Crashed(
        run.context,
        run.account,
        lambda: _recover(run.context, run.account),
        again=lambda: _recover(run.context, run.account),
    )


def _guardian_killed_after_grant_record(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Crashed:
    """The guardian dies between the published grant record and the entry's upgrade to granted."""
    run = _child_run(
        tmp_path, guardian_stages={"queue-tempfile-written": {"action": "sigkill", "occurrence": 3}}, gated=True
    )
    _guardian_dies(run)
    crashed = _recover_after_requester_ends(run)

    def resume() -> None:
        run.requester.open_gate()
        _requester_ended(run, "ResourceError")  # the requester finds its grant without a guardian and ends: both dead
        _recover(run.context, run.account)

    crashed.resume = resume
    return crashed


def _guardian_killed_after_grant_queue(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Crashed:
    """The guardian dies right after the entry became granted (the grant is complete, the guardian is gone)."""
    run = _child_run(tmp_path, guardian_stages={"queue-saved": {"action": "sigkill", "occurrence": 3}}, gated=True)
    _guardian_dies(run)
    crashed = _recover_after_requester_ends(run)

    def resume() -> None:
        run.requester.open_gate()
        _requester_ended(run, "ResourceError")  # entering a grant whose guardian is gone fails: both are dead
        assert not run.requester.entered_file.exists()
        _recover(run.context, run.account)

    crashed.resume = resume
    return crashed


def _acknowledged_then_guardian_killed(retire_crash: bool) -> Callable[[Path, pytest.MonkeyPatch], Crashed]:
    """The guardian writes the leave acknowledgment and dies before the entry's removal (optionally also mid-retire)."""

    def arrange(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Crashed:
        run = _child_run(
            tmp_path,
            guardian_stages={"queue-tempfile-written": {"action": "sigkill", "occurrence": 4}},
            leave=True,
        )
        wait_until(run.requester.entered_file.exists, timeout=30, message="the requester entered its granted run")
        run.requester.release()  # it leaves: the withdrawal, then the guardian's acknowledgment and its death
        _guardian_dies(run)
        # The acknowledgment is durable but the guardian died before retiring its queue entry: the requester's
        # verification finds the reference and refuses with retirement-incomplete (a ResourceError).
        _requester_ended(run, "ResourceError")
        crashed = _recover_after_requester_ends(run)
        if retire_crash:
            _crash_at("retire-queue-removed", crashed.resume)
        return crashed

    return arrange


def _requester_dies(point: str, *, guardian: str) -> Callable[[Path, pytest.MonkeyPatch], Crashed]:
    """A requester killed before its withdrawal, right after it, or while it waits for the acknowledgment.

    The guardian is stopped first, so the durable image is stable; afterwards it is either killed (a recoverer then
    finishes the run) or continued (it withdraws the lost requester and finishes the run itself).
    """

    def arrange(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Crashed:
        stages = {"leave-withdrawal-written": {"action": "sigkill"}} if point == "after-withdrawal" else {}
        run = _child_run(tmp_path, requester_stages=stages, leave=point != "before-withdrawal")
        wait_until(run.requester.entered_file.exists, timeout=30, message="the requester entered its granted run")
        guardian_pid = pause_guardian(run.context)
        try:
            if point == "before-withdrawal":
                _kill(run.requester.process)
            else:
                run.requester.release()  # it leaves
                if point == "after-withdrawal":
                    assert run.requester.process.wait(timeout=60) == -signal.SIGKILL
                else:
                    wait_until(
                        lambda: _json(run.context.record)["state"] == "stopping", message="the withdrawal is durable"
                    )
                    assert run.requester.process.poll() is None  # it waits for an acknowledgment that cannot come
                    _kill(run.requester.process)
        except BaseException:
            os.kill(guardian_pid, signal.SIGKILL)  # never leave a stopped guardian behind
            reap(run.requester.process)
            raise
        if guardian == "dead":
            kill_guardian(run.context)  # SIGKILL ends a stopped process too
            return _recover_after_requester_ends(run)

        def finished() -> None:
            os.kill(guardian_pid, signal.SIGCONT)  # the live guardian withdraws the lost requester and finishes alone
            index = run.context.project.record
            wait_until(
                lambda: (
                    _json(run.context.record)["state"] == "complete"
                    and run.context.run_id not in queue_entries(run.account)
                    and run.context.run_id not in _json(index)["runs"]
                ),
                timeout=60,
                message="the live guardian finished the run",
            )

        return Crashed(run.context, run.account, finished)

    return arrange


_PRE_GRANT = {"admission": "never-granted", "guardian_bound": True, "indexed": True}
_REVOKED = {"state": "complete", "admission": "revoked", "entry": None, "indexed": True}  # a live requester keeps it
_GRANTED = {"state": "active", "admission": "published", "ticket": 1, "indexed": True, "acked": False}
_WITHDRAWN = {"state": "stopping", "admission": "published", "entry": "granted", "indexed": True, "acked": False}
_ACKED = {"state": "complete", "acked": True, "indexed": True}
_RECOVERED_EMPTY = {"state": "complete", "entry": None, "claim": None, "indexed": False}

PROCESS_ROWS = [
    CrashRow(
        "guardian-enroll-record-written",
        _guardian_killed_before_grant("enroll-record-written", 1),
        {**_PRE_GRANT, "state": "prepared", "ticket": None, "entry": None},
        _REVOKED,
    ),
    CrashRow(
        "guardian-enroll-queue-written",
        _guardian_killed_before_grant("queue-saved", 1),
        {**_PRE_GRANT, "state": "prepared", "ticket": None, "entry": "queued"},
        _REVOKED,
    ),
    CrashRow(
        "guardian-reserve",
        _guardian_killed_before_grant("queue-saved", 2),
        {**_PRE_GRANT, "state": "queued", "ticket": 1, "entry": "admitting"},
        _REVOKED,
    ),
    CrashRow(
        "guardian-grant-record-written",
        _guardian_killed_after_grant_record,
        {**_GRANTED, "entry": "admitting"},
        _RECOVERED_EMPTY,
    ),
    CrashRow(
        "guardian-grant-queue-written",
        _guardian_killed_after_grant_queue,
        {**_GRANTED, "entry": "granted"},
        _RECOVERED_EMPTY,
    ),
    CrashRow(
        "guardian-ack-record-written",
        _acknowledged_then_guardian_killed(retire_crash=False),
        {**_ACKED, "entry": "granted"},
        {**_ACKED, "entry": None, "indexed": False},
    ),
    CrashRow(
        "guardian-ack-retired-queue-before-index",
        _acknowledged_then_guardian_killed(retire_crash=True),
        {**_ACKED, "entry": None},
        {**_ACKED, "entry": None, "indexed": False},
    ),
    *[
        CrashRow(
            f"requester-{point}-guardian-{guardian}",
            _requester_dies(point, guardian=guardian),
            {**_GRANTED, "entry": "granted"} if point == "before-withdrawal" else _WITHDRAWN,
            _RECOVERED_EMPTY,
        )
        for guardian in ("dead", "live")
        for point in ("before-withdrawal", "after-withdrawal", "during-leave-wait")
    ],
]


def _empty_grant_claim_crash(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Crashed:
    """A worker that dies right after it claimed an empty grant's entry (the claim is its only write)."""
    context, account = _empty_grant(tmp_path)
    write_claim_intent(account, context.run_id, ATTEMPT, expires_in_s=60)
    me = current_identity()
    _crash_at(
        "queue-saved",
        lambda: _recover(context, account, drain_live_consumers=False, takeover_attempt=ATTEMPT),
    )

    def resume() -> None:
        retire_actor(context.project.state, me, dead_identity(), account=account)  # the claimant is gone
        _recover(context, account)

    return Crashed(context, account, resume, again=lambda: _recover(context, account))


def _first_sighting_crash(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Crashed:
    """A dead granted holder whose first sighting and claim intent were saved at once, and then the observer died."""
    services = build_test_services(tmp_path / "state")
    account = services.root / "admission"
    first, _second = _two_projects(tmp_path)
    dead = _dead_granted_holder(services, first)
    _crash_at("queue-saved", lambda: lifecycle.reconcile_account(_test_directory=account))

    def resume() -> None:
        claim = queue_entries(account)[dead.run_id].takeover
        assert claim is not None
        seed_dead_since(account, dead.run_id, age_s=31)  # time passes: the grace elapsed since the saved sighting
        write_claim_intent(account, dead.run_id, claim["attempt"], expires_in_s=-1)  # and no worker took the intent
        lifecycle.reconcile_account(_test_directory=account)
        index = dead.project.record
        wait_until(
            lambda: dead.run_id not in queue_entries(account) and dead.run_id not in _json(index)["runs"],
            timeout=45,
            message="the worker recovered the dead holder",
        )

    return Crashed(dead, account, resume)


_WORKER_BOUND = {**_RECORDED, "claim": "bound"}

TAKEOVER_ROWS += [
    CrashRow(
        "claimant-empty-grant-claim-written",
        _empty_grant_claim_crash,
        {"state": "stopping", "entry": "granted", "claim": "bound", "actor": False},
        _RECOVERED_EMPTY,
    ),
    CrashRow(
        "spawner-after-first-sighting",
        _first_sighting_crash,
        {"state": "active", "entry": "granted", "claim": "intent", "dead_since": True, "actor": False},
        _RECOVERED_EMPTY,
    ),
    *[
        CrashRow(
            f"claimant-worker-phase-{phase}",
            _claimant_crash(f"recovery-phase-{phase}", worker=True),
            {**_WORKER_BOUND, "stored_phase": stored, "derived": derived},
            FINAL_RECOVERED,
        )
        for phase, stored, derived in (
            ("resources", "consumers", "consumers"),
            ("publish", "private-root", "private-root"),
        )
    ],
]

CRASH_ROWS = [*CLAIMANT_ROWS, *CREATION_PREFIX_ROWS, *TAKEOVER_ROWS, *PROCESS_ROWS]


def _check(expected: dict | Callable[[Crashed], None], crashed: Crashed, when: str, row: str) -> None:
    if callable(expected):
        expected(crashed)
        return
    observed = _image(crashed)
    mismatched = {key: (observed[key], value) for key, value in expected.items() if observed[key] != value}
    assert not mismatched, f"{row}: the {when} image differs (observed, expected): {mismatched}"


@pytest.mark.parametrize("row", CRASH_ROWS, ids=lambda row: row.id)
def test_crash_matrix(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, row: CrashRow) -> None:
    crashed = row.arrange(tmp_path, monkeypatch)
    _check(row.prefix, crashed, "crash prefix", row.id)

    crashed.resume()

    _check(row.final, crashed, "resumed", row.id)
    context = crashed.context
    if context is not None:
        final = _image(crashed)
        assert final["state"] == "complete" and final["entry"] is None  # one authority finished; the run left the queue
        assert _holders(crashed.account) == []  # no entry holds the slot
        lifecycle.load_run_record(context)  # the final record is one the loader accepts
        if crashed.again is not None:
            before = (*authority_bytes(context, crashed.account), context.project.record.read_bytes())
            crashed.again()
            after = (*authority_bytes(context, crashed.account), context.project.record.read_bytes())
            assert after == before  # a second resume wrote nothing
    if crashed.follow_on is not None:
        crashed.follow_on()


def test_operator_recovers_a_dead_consumer_after_the_withdrawal_deadline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _handle, context, account = _resource_grant(tmp_path, monkeypatch)
    add_active_consumer(context, dead_identity())
    record = json.loads(context.record.read_text())
    deadline = record["withdrawal"]["deadline"]
    record["withdrawal"]["deadline"] = {**deadline, "duration_ns": 1, "expires_ns": deadline["created_ns"] + 1}
    atomic_json(context.record, record)  # recovery runs long after the original leave's deadline expired

    status = _recover(context, account)

    assert (status.state, status.cleanup_outcome) == ("complete", "complete")


def test_operator_drains_a_live_consumer_after_the_withdrawal_deadline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _handle, context, account = _resource_grant(tmp_path, monkeypatch)
    consumer, identity = start_live_actor("consumer")
    try:
        add_active_consumer(context, identity)
        record = json.loads(context.record.read_text())
        deadline = record["withdrawal"]["deadline"]
        record["withdrawal"]["deadline"] = {**deadline, "duration_ns": 1, "expires_ns": deadline["created_ns"] + 1}
        atomic_json(context.record, record)

        status = _recover(context, account)

        assert (status.state, status.cleanup_outcome) == ("complete", "complete")
        assert consumer.wait(timeout=20) == -signal.SIGTERM  # a fresh grace window, not an instant refusal
    finally:
        reap(consumer)


def test_a_never_fitting_run_is_refused_before_a_dead_holder_is_taken_over(tmp_path: Path) -> None:
    from docker_test_toolkit.factories.testing import bind_test_capacity

    gib = 1024**3
    services = build_test_services(tmp_path / "state")
    account = services.root / "admission"
    first, second = _two_projects(tmp_path)
    dead = _dead_granted_holder(services, first)
    seed_dead_since(account, dead.run_id, age_s=31)  # a takeover is due in the next admission pass
    before = dead.record.read_bytes()
    bind_test_capacity(
        services,
        disk={"data-root": {"free_bytes": 50 * gib, "total_bytes": 50 * gib}},
        previous_image_sizes=[48 * gib],  # 60 GiB estimated against 48 GiB usable: it can never fit
    )

    with pytest.raises(ResourceError) as refused:
        services.open_run(_empty_spec(second, "never-fits")).enter()

    # the refused run caused no Docker mutation, not even another run's takeover in its pass.
    assert "disk-capacity" in str(refused.value)
    assert dead.run_id in queue_entries(account)
    assert dead.record.read_bytes() == before  # no takeover touched the dead holder's record
    assert not _worker_processes(dead.run_id)


@pytest.mark.parametrize("kill", [signal.SIGKILL, signal.SIGHUP], ids=["SIGKILL", "SIGHUP"])
def test_guardian_survives_a_requester_process_group_kill(tmp_path: Path, kill: signal.Signals) -> None:
    """Killing the requester's shell job leaves the guardian executing; it ends the run as requester-lost."""
    run = _child_run(tmp_path, new_session=True)
    try:
        wait_until(run.requester.entered_file.exists, timeout=30, message="the requester entered its granted run")
        guardian = lifecycle._identity_from_json(_json(run.context.record)["guardian"])
        assert lifecycle.observe_process(guardian) == "executing"

        os.killpg(run.requester.process.pid, kill)
        assert run.requester.process.wait(timeout=30) == -kill

        assert lifecycle.observe_process(guardian) == "executing"
        wait_until(lambda: _json(run.context.record)["state"] == "complete", timeout=60, message="the run ended")
        record = _json(run.context.record)
        assert record["infrastructure_failure"] == "requester-lost"
        assert run.context.run_id not in queue_entries(run.account)
        reap_orphan(guardian.pid, timeout=60)
        wait_until(
            lambda: lifecycle.observe_process(guardian) in {"absent", "replaced"},
            timeout=60,
            message="the guardian exited",
        )
    finally:
        reap(run.requester.process)


def _clear_dead_since(account: Path, run_id: str) -> None:
    with lock(Context(account, account), "metadata") as anchor:
        state = admission.load_queue(account, anchor)
        state.entries[run_id].dead_since = None
        admission.save_queue(account, state, anchor)


def _waiting_worker(tmp_path: Path) -> tuple[lifecycle.RunContext, Path, str]:
    """A dead holder and the worker a pass spawned for it, waiting out the grace: (context, account, attempt)."""
    services = build_test_services(tmp_path / "state")
    account = services.root / "admission"
    first, _second = _two_projects(tmp_path)
    dead = _dead_granted_holder(services, first)
    assert lifecycle.reconcile_account(_test_directory=account).spawned == 1
    takeover = queue_entries(account)[dead.run_id].takeover
    assert takeover is not None
    return dead, account, takeover["attempt"]


def test_a_sleeping_worker_renews_its_intent_and_recovers(tmp_path: Path) -> None:
    dead, account, attempt = _waiting_worker(tmp_path)
    write_claim_intent(account, dead.run_id, attempt, expires_in_s=-1)  # the host slept past the intent

    def renewed() -> bool:
        takeover = queue_entries(account)[dead.run_id].takeover
        return (
            takeover is not None
            and takeover["attempt"] == attempt
            and takeover["claimant"] is None
            and takeover["deadline"]["expires_ns"] > lifecycle.NativeClock().read()
        )

    wait_until(renewed, timeout=15, message="the worker renewed its own intent")
    wait_until(lambda: dead.run_id not in queue_entries(account), timeout=90, message="the worker recovers the run")
    assert json.loads(dead.record.read_text())["recovery_outcome"] == "complete"


def test_worker_withdraws_only_its_intent_when_dead_since_is_cleared(tmp_path: Path) -> None:
    dead, account, _attempt = _waiting_worker(tmp_path)
    record = dead.record.read_bytes()

    _clear_dead_since(account, dead.run_id)  # another pass saw the entry alive

    wait_until(lambda: _worker_processes(dead.run_id) == [], timeout=15, message="the worker exits")
    entry = queue_entries(account)[dead.run_id]
    assert entry.takeover is None and entry.dead_since is None and entry.phase == "granted"
    assert dead.record.read_bytes() == record  # no recovery phase, no Docker call


def test_worker_withdraws_only_its_intent_when_its_record_turns_hostile(tmp_path: Path) -> None:
    dead, account, _attempt = _waiting_worker(tmp_path)

    dead.record.write_bytes(b"{")

    wait_until(lambda: _worker_processes(dead.run_id) == [], timeout=15, message="the worker exits")
    assert queue_entries(account)[dead.run_id].takeover is None
    assert dead.record.read_bytes() == b"{"
    report = lifecycle.reconcile_account(_test_directory=account)  # the next pass leaves the blocked entry alone
    assert report.spawned == 0
    assert queue_entries(account)[dead.run_id].takeover is None


def test_worker_never_signals_a_reused_pid(tmp_path: Path) -> None:
    services = build_test_services(tmp_path / "state")
    account = services.root / "admission"
    first, _second = _two_projects(tmp_path)
    dead = _dead_granted_holder(services, first)
    bystander, identity = start_live_actor("reused-pid")
    try:
        reused = {**identity, "birth": [value + 1 for value in identity["birth"]]}  # same pid, another process's birth
        add_active_consumer(dead, reused)
        seed_dead_since(account, dead.run_id, age_s=31)

        assert lifecycle.reconcile_account(_test_directory=account).spawned == 1

        wait_until(lambda: dead.run_id not in queue_entries(account), timeout=60, message="the worker recovers the run")
        assert lifecycle.observe_process(lifecycle._identity_from_json(identity)) == "executing"
        assert bystander.poll() is None
    finally:
        reap(bystander)


_RECOVERY_FAILS = {"do": "raise", "type": "OSError", "message": "injected recovery failure"}


def _failed_worker(tmp_path: Path, account: Path, context: lifecycle.RunContext) -> dict:
    """Run one real worker whose recovery fails, and return its claim; the worker process is gone afterwards."""
    write_claim_intent(account, context.run_id, ATTEMPT, expires_in_s=60)
    seed_dead_since(account, context.run_id, age_s=31)
    child = start_takeover_worker_child(
        tmp_path, context, account, ATTEMPT, {"recovery-checks-passed": {"action": _RECOVERY_FAILS}}
    )
    try:
        assert child.wait(timeout=60) == 1
    finally:
        reap(child)
    takeover = queue_entries(account)[context.run_id].takeover
    assert takeover is not None
    return takeover


def test_a_failed_worker_leaves_an_owned_back_off(tmp_path: Path) -> None:
    services = build_test_services(tmp_path / "state")
    account = services.root / "admission"
    first, _second = _two_projects(tmp_path)
    dead = _dead_granted_holder(services, first)

    takeover = _failed_worker(tmp_path, account, dead)

    assert takeover["attempt"] == ATTEMPT and takeover["claimant"] is not None
    remaining = takeover["deadline"]["expires_ns"] - lifecycle.NativeClock().read()
    assert 290e9 <= remaining <= 310e9
    assert lifecycle.reconcile_account(_test_directory=account).spawned == 0  # no respawn per command
    assert _worker_processes(dead.run_id) == []

    write_claim_intent(account, dead.run_id, ATTEMPT, expires_in_s=-1, claimant=takeover["claimant"])  # its deadline
    assert lifecycle.reconcile_account(_test_directory=account).spawned == 1
    wait_until(lambda: dead.run_id not in queue_entries(account), timeout=60, message="the new worker recovers the run")


def test_an_operator_is_not_held_by_a_back_off(tmp_path: Path) -> None:
    services = build_test_services(tmp_path / "state")
    account = services.root / "admission"
    first, _second = _two_projects(tmp_path)
    dead = _dead_granted_holder(services, first)
    _failed_worker(tmp_path, account, dead)

    lifecycle.recover_run(dead, _test_directory=account)

    assert json.loads(dead.record.read_text())["recovery_outcome"] == "complete"
    assert dead.run_id not in queue_entries(account)


def test_a_failed_worker_never_overwrites_an_operator_claim(tmp_path: Path) -> None:
    services = build_test_services(tmp_path / "state")
    account = services.root / "admission"
    first, _second = _two_projects(tmp_path)
    dead = _dead_granted_holder(services, first)
    write_claim_intent(account, dead.run_id, ATTEMPT, expires_in_s=60)
    seed_dead_since(account, dead.run_id, age_s=31)
    release = tmp_path / "release"
    reached = tmp_path / "reached"
    child = start_takeover_worker_child(
        tmp_path,
        dead,
        account,
        ATTEMPT,
        {
            "recovery-checks-passed": {
                "action": [
                    {"do": "write-file", "path": str(reached)},
                    {"do": "wait-file", "path": str(release)},
                    _RECOVERY_FAILS,
                ]
            }
        },
    )
    try:
        wait_until(reached.exists, timeout=30, message="the worker passed its checks")
        write_claim_intent(account, dead.run_id, ATTEMPT, expires_in_s=600, claimant=current_identity())
        operator_write = (account / "queue.json").read_bytes()
        release.touch()
        assert child.wait(timeout=60) == 1
    finally:
        reap(child)
    assert (account / "queue.json").read_bytes() == operator_write


def test_back_offs_do_not_fill_the_cap(tmp_path: Path) -> None:
    services = build_test_services(tmp_path / "state")
    account = services.root / "admission"
    dead, first, second = _three_dead_entries(tmp_path, services)
    for context in (first, second):  # two failed workers' back-offs: bound to dead claimants, deadlines ahead
        write_claim_intent(account, context.run_id, ATTEMPT, expires_in_s=300, claimant=dead_identity())
        seed_dead_since(account, context.run_id, age_s=31)

    report = lifecycle.reconcile_account(_test_directory=account)

    assert report.spawned == 1  # only the entry free of a back-off; the two back-offs held no room
    assert queue_entries(account)[dead.run_id].takeover is not None
    wait_until(lambda: dead.run_id not in queue_entries(account), timeout=90, message="the holder recovered")
