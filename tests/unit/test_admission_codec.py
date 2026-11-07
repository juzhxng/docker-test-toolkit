"""Queue codec: raw classification, the per-mode codec rule, validity rules and canonical bytes."""

from __future__ import annotations

import fcntl
import json
from pathlib import Path

import pytest

from docker_test_toolkit.application import admission
from docker_test_toolkit.application.recovery import Context, lock
from docker_test_toolkit.domain.errors import IncompatibleState, ResourceError
from tests.integration.account_fixtures import (
    private_account_directory,
    queue_entry,
    write_raw_queue,
    write_v2_queue,
)

RUN_A = "00000001-0000-4000-8000-000000000000"


def _load(account: Path):
    with lock(Context(account, account), "metadata") as anchor:
        return admission.load_queue(account, anchor)


@pytest.mark.parametrize(
    "data",
    [
        pytest.param({"version": 1, "next_ticket": 7, "runs": {}, "reservation": None}, id="version-1"),
        pytest.param({"version": 3, "next_ticket": 1, "entries": {}}, id="unknown-version"),
        pytest.param({"version": 2, "next_ticket": 1}, id="missing-key"),
        pytest.param({"version": 2, "next_ticket": 1, "entries": {}, "runs": {}}, id="extra-key"),
        pytest.param(["not", "an", "object"], id="not-an-object"),
    ],
)
def test_only_queue_protocol_2_is_readable(data: object) -> None:
    with pytest.raises(IncompatibleState) as raised:
        admission.to_logical(data)
    assert raised.value.reason == "queue-invalid"


def test_one_holder_is_valid(tmp_path: Path) -> None:
    account = private_account_directory(tmp_path)
    holder = queue_entry(1, 1, "granted")
    write_v2_queue(account, [holder, queue_entry(2, 2)])

    state = _load(account)

    assert [entry.run_id for entry in admission.holders(state)] == [holder[0]]


def test_codec_accepts_two_holders(tmp_path: Path) -> None:
    """Validity is independent of the machine limit: any number of holders reads; the limit decides."""
    account = private_account_directory(tmp_path)
    first_id, first = queue_entry(1, 1, "granted")
    second_id, second = queue_entry(2, 2, "admitting")
    third_id, third = queue_entry(3, 3, "granted")
    third["project_root"], third["project_id"] = first["project_root"], first["project_id"]  # same project too
    write_v2_queue(account, [(first_id, first), (second_id, second), (third_id, third)])

    state = _load(account)

    assert [entry.run_id for entry in admission.holders(state)] == [first_id, second_id, third_id]


def test_holders_returns_only_the_holder(tmp_path: Path) -> None:
    account = private_account_directory(tmp_path)
    holder = queue_entry(7, 9, "admitting")
    write_v2_queue(account, [queue_entry(2, 3), holder, queue_entry(5, 5)])  # queued entries are not holders

    state = _load(account)

    assert [entry.run_id for entry in admission.holders(state)] == [holder[0]]
    assert admission.holders(admission.QueueState(1, {})) == []


def test_project_id_mismatch_is_queue_invalid(tmp_path: Path) -> None:
    account = private_account_directory(tmp_path)
    run_id, entry = queue_entry(1, 1)
    entry["project_id"] = "0" * 64
    write_v2_queue(account, [(run_id, entry)])
    with pytest.raises(IncompatibleState) as raised:
        _load(account)
    assert raised.value.reason == "queue-invalid"


def test_holder_must_be_set_exactly_for_admitting_and_granted(tmp_path: Path) -> None:
    account = private_account_directory(tmp_path)
    run_id, holder_missing = queue_entry(1, 1, "granted")
    holder_missing["holder"] = None
    write_v2_queue(account, [(run_id, holder_missing)])
    with pytest.raises(IncompatibleState):
        _load(account)
    run_id, queued_with_holder = queue_entry(2, 1)
    queued_with_holder["holder"] = {"pid": 2}
    write_v2_queue(account, [(run_id, queued_with_holder)])
    with pytest.raises(IncompatibleState):
        _load(account)


@pytest.mark.parametrize(
    ("phase", "accepted"),
    [
        (None, True),
        ("claim", True),
        ("operation", True),
        ("receipts", True),
        ("consumers", True),
        ("resources", True),
        ("private-root", True),
        ("publish", True),
        ("authorize", False),  # leaves no durable trace, so it is never stored
        ("Claim", False),
        ("bogus", False),
        ("", False),
        (3, False),
        (["claim"], False),
    ],
)
def test_last_completed_phase_is_a_closed_vocabulary(tmp_path: Path, phase: object, accepted: bool) -> None:
    account = private_account_directory(tmp_path)
    run_id, entry = queue_entry(1, 1)
    entry["last_completed_phase"] = phase
    write_v2_queue(account, [(run_id, entry)])
    if accepted:
        assert _load(account).entries[run_id].last_completed_phase == phase
        return
    with pytest.raises(IncompatibleState) as raised:
        _load(account)
    assert raised.value.reason == "queue-invalid"


def test_v2_with_an_extra_key_is_refused_and_left_unchanged(tmp_path: Path) -> None:
    account = private_account_directory(tmp_path)
    raw = b'{"entries":{},"next_ticket":1,"reservation":null,"version":2}\n'
    write_raw_queue(account, raw)
    with pytest.raises(IncompatibleState) as raised:
        _load(account)
    assert raised.value.reason == "queue-invalid"
    assert (account / "queue.json").read_bytes() == raw


def test_tickets_must_be_unique_and_below_next_ticket(tmp_path: Path) -> None:
    account = private_account_directory(tmp_path)
    write_v2_queue(account, [queue_entry(1, 1), queue_entry(2, 1)], next_ticket=3)
    with pytest.raises(IncompatibleState):
        _load(account)
    write_v2_queue(account, [queue_entry(1, 5)], next_ticket=5)
    with pytest.raises(IncompatibleState):
        _load(account)


def test_v2_bytes_are_canonical(tmp_path: Path) -> None:
    account = private_account_directory(tmp_path)
    entries = [queue_entry(1, 1, "granted"), queue_entry(2, 2)]
    write_v2_queue(account, entries)
    with lock(Context(account, account), "metadata") as anchor:
        state = admission.load_queue(account, anchor)
        assert state.next_ticket == 3
        assert state.entries[entries[0][0]].phase == "granted"
        admission.save_queue(account, state, anchor)
    written = (account / "queue.json").read_bytes()
    assert written.endswith(b"}\n") and not written.endswith(b"\n\n")
    assert written == json.dumps(json.loads(written), sort_keys=True, separators=(",", ":")).encode() + b"\n"
    assert json.loads(written)["entries"] == dict(entries)
    assert (account / "queue.json").stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize(
    ("deadline", "valid"),
    [
        pytest.param(
            {"origin": "boot:1", "created_ns": 1, "duration_ns": 30, "expires_ns": 31}, True, id="exact-shape"
        ),
        pytest.param({}, False, id="empty"),
        pytest.param({"origin": "boot:1", "created_ns": 1, "duration_ns": 30}, False, id="missing-key"),
        pytest.param(
            {"origin": "boot:1", "created_ns": "1", "duration_ns": 30, "expires_ns": 31}, False, id="string-tick"
        ),
        pytest.param(
            {"origin": 7, "created_ns": 1, "duration_ns": 30, "expires_ns": 31}, False, id="non-string-origin"
        ),
        pytest.param(
            {"origin": "boot:1", "created_ns": 1, "duration_ns": 30, "expires_ns": 31, "extra": 0},
            False,
            id="extra-key",
        ),
    ],
)
def test_takeover_claim_deadline_must_have_the_native_deadline_shape(
    tmp_path: Path, deadline: dict, valid: bool
) -> None:
    account = private_account_directory(tmp_path)
    run_id, entry = queue_entry(1, 1)
    entry["takeover"] = {"attempt": "66666666-6666-4666-8666-666666666666", "deadline": deadline, "claimant": None}
    write_v2_queue(account, [(run_id, entry)])

    if valid:
        assert _load(account).entries[run_id].takeover == entry["takeover"]
    else:
        with pytest.raises(IncompatibleState) as raised:
            _load(account)
        assert raised.value.reason == "queue-invalid"


# ---- the state-format marker ---------------------------------------------------------------------------


def _anchor(account: Path, text: str) -> Path:
    anchor = account / "metadata.lock"
    anchor.write_text(text)
    anchor.chmod(0o600)
    return anchor


def test_anchor_with_newer_format_is_refused(tmp_path: Path) -> None:
    account = private_account_directory(tmp_path)
    write_v2_queue(account, [queue_entry(1, 1)])
    _anchor(account, "docker-test-toolkit-state:2\n")
    before = (account / "queue.json").read_bytes()

    with pytest.raises(IncompatibleState) as raised:
        _load(account)

    assert raised.value.reason == "state-format-newer"
    assert (account / "queue.json").read_bytes() == before


@pytest.mark.parametrize("marker", ["codex-test-queue:1\n", "docker-test-toolkit-state:x\n", "other\n"])
def test_anchor_of_another_protocol_is_refused(tmp_path: Path, marker: str) -> None:
    account = private_account_directory(tmp_path)
    write_v2_queue(account, [])
    _anchor(account, marker)

    with pytest.raises(ResourceError, match="unsupported protocol"):
        _load(account)


def test_fresh_anchor_gets_the_state_marker_on_first_save(tmp_path: Path) -> None:
    account = private_account_directory(tmp_path)
    anchor = _anchor(account, "")

    with lock(Context(account, account), "metadata") as held:
        admission.save_queue(account, admission.QueueState(1, {}), held)

    assert anchor.read_text() == admission.STATE_MARKER == "docker-test-toolkit-state:1\n"


def test_check_account_format_passes_on_absent_anchor(tmp_path: Path) -> None:
    admission.check_account_format(tmp_path / "never-used")


def test_check_account_format_refuses_a_newer_format(tmp_path: Path) -> None:
    account = private_account_directory(tmp_path)
    _anchor(account, "docker-test-toolkit-state:7\n")

    with pytest.raises(IncompatibleState) as raised:
        admission.check_account_format(account)

    assert raised.value.reason == "state-format-newer"


def test_check_account_format_releases_its_descriptor(tmp_path: Path) -> None:
    account = private_account_directory(tmp_path)
    anchor = _anchor(account, admission.STATE_MARKER)

    admission.check_account_format(account)

    with anchor.open("r+") as descriptor:
        fcntl.flock(descriptor.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)  # no shared lock is left behind
