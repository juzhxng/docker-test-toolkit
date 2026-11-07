"""Unit tests of the machine-settings codecs in ``application/sharing.py``."""

from __future__ import annotations

import json
import stat
from dataclasses import replace
from pathlib import Path

import pytest

from docker_test_toolkit.application import sharing
from docker_test_toolkit.application.recovery import atomic_json
from docker_test_toolkit.domain.errors import ResourceError
from docker_test_toolkit.domain.share_policy import SHARE_RULES_VERSION


def _account(tmp_path: Path) -> Path:
    account = tmp_path / "account"
    account.mkdir(mode=0o700)
    return account


def test_parallel_runs_absent_is_auto(tmp_path: Path) -> None:
    assert sharing.read_parallel_runs(_account(tmp_path)) is None


def test_parallel_runs_auto_reads_none(tmp_path: Path) -> None:
    account = _account(tmp_path)
    atomic_json(account / "parallel-runs.json", {"parallel_runs": "auto", "schema": 1})

    assert sharing.read_parallel_runs(account) is None


@pytest.mark.parametrize(
    "document",
    [
        {"parallel_runs": 0, "schema": 1},
        {"parallel_runs": -1, "schema": 1},
        {"parallel_runs": "two", "schema": 1},
        {"parallel_runs": True, "schema": 1},
        {"parallel_runs": 2, "schema": 1, "extra": 1},
        {"parallel_runs": 2, "schema": 2},
    ],
    ids=["zero", "negative", "word", "bool", "extra-key", "schema-2"],
)
def test_parallel_runs_invalid_fails_closed(tmp_path: Path, document: dict) -> None:
    account = _account(tmp_path)
    atomic_json(account / "parallel-runs.json", document)

    with pytest.raises(ResourceError, match="parallel-runs"):
        sharing.read_parallel_runs(account)


def test_parallel_runs_record_round_trip_mode_0600(tmp_path: Path) -> None:
    account = _account(tmp_path)

    sharing.record_parallel_runs(account, 3)
    first = sharing.read_parallel_runs(account)
    sharing.record_parallel_runs(account, "auto")

    path = account / "parallel-runs.json"
    assert first == 3
    assert json.loads(path.read_text()) == {"parallel_runs": "auto", "schema": 1}
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    with pytest.raises(ResourceError):
        sharing.record_parallel_runs(account, 0)
    assert sharing.read_parallel_runs(account) is None


def test_request_key_depends_on_every_input() -> None:
    base = sharing.request_key("repo", "integration", "fixture", ("b", "a"))

    assert base == sharing.request_key("repo", "integration", "fixture", ("a", "b"))  # the closure is a set
    for changed in (
        sharing.request_key("other-repo", "integration", "fixture", ("a", "b")),
        sharing.request_key("repo", "e2e", "fixture", ("a", "b")),
        sharing.request_key("repo", "integration", "other", ("a", "b")),
        sharing.request_key("repo", "integration", "fixture", ("a",)),
    ):
        assert changed != base


def _store(registry: Path, document: object) -> None:
    path = sharing.verdict_path(registry, "p" * 64)
    path.parent.mkdir(mode=0o700, parents=True)
    atomic_json(path, document)


def _verdict(shareable: bool) -> dict:
    return {"shareable": shareable, "reason": None, "run_id": "r", "origin": "o", "tick_ns": 1}


def test_read_verdict_rules_mismatch_reads_unknown(tmp_path: Path) -> None:
    _store(tmp_path, {"rules": 2, "schema": 1, "verdicts": {"k": _verdict(True)}})

    assert sharing.read_verdict(tmp_path, "p" * 64, "k") is None


@pytest.mark.parametrize(
    "document",
    [
        {"rules": 1, "schema": 2, "verdicts": {"k": {}}},
        {"rules": 1, "schema": 1, "verdicts": []},
        {"rules": 1, "schema": 1, "verdicts": {"k": {"shareable": "yes"}}},
        {"rules": 1, "schema": 1, "verdicts": {"k": {**_verdict(True), "x": 1}}},
        ["not", "an", "object"],
    ],
    ids=["schema-2", "verdicts-list", "shareable-word", "extra-field", "list"],
)
def test_read_verdict_invalid_file_reads_unknown(tmp_path: Path, document: object) -> None:
    _store(tmp_path, document)

    assert sharing.read_verdict(tmp_path, "p" * 64, "k") is None


def test_read_verdict_returns_the_recorded_value(tmp_path: Path) -> None:
    _store(tmp_path, {"rules": 1, "schema": 1, "verdicts": {"yes": _verdict(True), "no": _verdict(False)}})

    assert sharing.read_verdict(tmp_path, "p" * 64, "yes") is True
    assert sharing.read_verdict(tmp_path, "p" * 64, "no") is False
    assert sharing.read_verdict(tmp_path, "p" * 64, "absent") is None
    assert sharing.read_verdict(tmp_path / "nowhere", "p" * 64, "yes") is None


def _write(registry: Path, key: str, *, shareable: bool = True, origin: str = "o", tick_ns: int = 1, **options):
    sharing.write_verdict(
        registry, "p" * 64, key, shareable=shareable, reason=None, run_id="r", origin=origin, tick_ns=tick_ns, **options
    )


def _verdicts(registry: Path) -> dict:
    return json.loads(sharing.verdict_path(registry, "p" * 64).read_text())["verdicts"]


def test_verdict_store_evicts_oldest_tick_beyond_256_keys(tmp_path: Path) -> None:
    for tick in range(sharing.VERDICTS_PER_PROJECT + 2):
        _write(tmp_path, f"k{tick}", tick_ns=tick)

    verdicts = _verdicts(tmp_path)
    assert len(verdicts) == sharing.VERDICTS_PER_PROJECT == 256
    assert {"k0", "k1"}.isdisjoint(verdicts) and "k257" in verdicts
    mode = stat.S_IMODE(sharing.verdict_path(tmp_path, "p" * 64).stat().st_mode)
    assert mode == 0o600


def test_eviction_takes_other_clock_origins_before_the_current_one(tmp_path: Path) -> None:
    """After a reboot the ticks restart low: the store keeps this boot's verdicts and evicts the earlier boot's."""
    for index in range(sharing.VERDICTS_PER_PROJECT):
        _write(tmp_path, f"old-{index}", origin="before-reboot", tick_ns=10_000 + index)

    _write(tmp_path, "negative", shareable=False, origin="after-reboot", tick_ns=1)
    _write(tmp_path, "later", origin="after-reboot", tick_ns=2)

    verdicts = _verdicts(tmp_path)
    assert len(verdicts) == sharing.VERDICTS_PER_PROJECT
    assert verdicts["negative"]["shareable"] is False and "later" in verdicts
    assert sum(item["origin"] == "before-reboot" for item in verdicts.values()) == sharing.VERDICTS_PER_PROJECT - 2


def test_eviction_keeps_negatives_over_positives(tmp_path: Path) -> None:
    for index in range(sharing.VERDICTS_PER_PROJECT // 2):
        _write(tmp_path, f"negative-{index}", shareable=False, origin="before-reboot", tick_ns=index)
        _write(tmp_path, f"positive-{index}", origin="after-reboot", tick_ns=1_000 + index)

    _write(tmp_path, "new", origin="after-reboot", tick_ns=5_000)

    verdicts = _verdicts(tmp_path)
    assert "positive-0" not in verdicts and "new" in verdicts
    assert sum(item["shareable"] is False for item in verdicts.values()) == sharing.VERDICTS_PER_PROJECT // 2


def test_write_verdict_takes_no_lock(tmp_path: Path) -> None:
    from docker_test_toolkit.factories.testing import install_lock_observer

    observed: list[object] = []
    install_lock_observer(lambda *arguments: observed.append(arguments))
    try:
        _write(tmp_path, "k")
        sharing.remove_verdicts(tmp_path, "p" * 64)
    finally:
        install_lock_observer(None)

    assert observed == []
    assert not sharing.verdict_path(tmp_path, "p" * 64).exists()


@pytest.mark.parametrize("existing_tick", [3, 5, 9])
def test_a_positive_never_replaces_a_negative(tmp_path: Path, existing_tick: int) -> None:
    # A request whose render varies (an environment-chosen bind source) breaks a rule in one variant only: once any
    # run of it broke a rule, no later clean run predicts it shared again under these rules.
    _write(tmp_path, "k", shareable=False, tick_ns=existing_tick)

    _write(tmp_path, "k", shareable=True, tick_ns=9)

    assert _verdicts(tmp_path)["k"]["shareable"] is False


def test_a_negative_replaces_a_positive(tmp_path: Path) -> None:
    _write(tmp_path, "k", shareable=True, tick_ns=3)

    _write(tmp_path, "k", shareable=False, tick_ns=9)

    assert _verdicts(tmp_path)["k"]["shareable"] is False


def test_invalid_store_is_replaced_by_the_next_writer(tmp_path: Path) -> None:
    path = sharing.verdict_path(tmp_path, "p" * 64)
    path.parent.mkdir(mode=0o700)
    path.write_text("not json")
    path.chmod(0o600)

    _write(tmp_path, "k")

    assert set(_verdicts(tmp_path)) == {"k"}


@pytest.mark.parametrize(
    "field", [("tick_ns", "bad"), ("tick_ns", True), ("origin", 1), ("run_id", None), ("reason", 3)]
)
def test_a_wrong_typed_entry_invalidates_the_store(tmp_path: Path, field: tuple[str, object]) -> None:
    _write(tmp_path, "good")
    path = sharing.verdict_path(tmp_path, "p" * 64)
    document = json.loads(path.read_text())
    document["verdicts"]["other"] = {**document["verdicts"]["good"], field[0]: field[1]}
    atomic_json(path, document)

    assert sharing.read_verdict(tmp_path, "p" * 64, "good") is None  # the whole file predicts nothing
    _write(tmp_path, "new", tick_ns=5)  # never a TypeError: the next writer replaces the file
    assert set(_verdicts(tmp_path)) == {"new"}


def _pending(image: str = "1") -> sharing.PendingImage:
    return sharing.PendingImage(
        image_id="sha256:" + image * 64,
        created="created-1",
        references=("example/x:tag",),
        owner_run="11111111-1111-4111-8111-111111111111",
        owner_project="a" * 64,
        daemon_id="11111111-1111-1111-1111-111111111111",
    )


def test_pending_codec_round_trip_and_absent_is_empty(tmp_path: Path) -> None:
    account = _account(tmp_path)
    assert sharing.read_pending(account) == []

    sharing.write_pending(account, [_pending("1"), _pending("2")])

    assert sharing.read_pending(account) == [_pending("1"), _pending("2")]
    assert stat.S_IMODE((account / sharing.PENDING_FILE).stat().st_mode) == 0o600
    sharing.write_pending(account, [])
    assert sharing.read_pending(account) == []
    assert not (account / sharing.PENDING_FILE).exists()  # an empty list leaves no file


@pytest.mark.parametrize(
    "document",
    [
        "not json",
        {"schema": 2, "entries": []},
        {"schema": 1, "entries": {}},
        {"schema": 1, "entries": [{"image_id": "x"}]},
        {"schema": 1, "entries": [], "extra": 1},
    ],
    ids=["text", "schema", "entries-object", "entry-fields", "extra-key"],
)
def test_pending_codec_invalid_raises(tmp_path: Path, document: object) -> None:
    account = _account(tmp_path)
    path = account / sharing.PENDING_FILE
    if isinstance(document, str):
        path.write_text(document)
        path.chmod(0o600)
    else:
        atomic_json(path, document)

    with pytest.raises(ResourceError):
        sharing.read_pending(account)


@pytest.mark.parametrize("document", ["grants", "pending", "verdicts"])
def test_a_boolean_schema_is_invalid_in_every_shared_document(tmp_path: Path, document: str) -> None:
    """``"schema": true`` equals 1 in Python but is not the closed schema: every reader refuses it alike."""
    account = _account(tmp_path)
    if document == "grants":
        atomic_json(account / sharing.GRANTS_FILE, {"schema": True, "grants": []})
        with pytest.raises(ResourceError):
            sharing.read_grants(account)
    elif document == "pending":
        atomic_json(account / sharing.PENDING_FILE, {"schema": True, "entries": []})
        with pytest.raises(ResourceError):
            sharing.read_pending(account)
    else:
        path = sharing.verdict_path(account, "p" * 64)
        path.parent.mkdir(mode=0o700)
        verdict = {"shareable": True, "reason": None, "run_id": "r", "origin": "o", "tick_ns": 1}
        atomic_json(path, {"schema": True, "rules": SHARE_RULES_VERSION, "verdicts": {"k": verdict}})
        assert sharing.read_verdict(account, "p" * 64, "k") is None


@pytest.mark.parametrize("project_id", ["../outside", ".hidden", "a/b"])
def test_every_verdict_store_access_stays_inside_the_store(tmp_path: Path, project_id: str) -> None:
    registry = _account(tmp_path)

    assert sharing.read_verdict(registry, project_id, "k") is None
    with pytest.raises(ResourceError):
        sharing.remove_verdicts(registry, project_id)
    with pytest.raises(ResourceError):
        sharing.write_verdict(registry, project_id, "k", shareable=True, reason=None, run_id="r", origin="o", tick_ns=1)


def _store_for(registry: Path, project_id: str, verdicts: dict) -> None:
    path = sharing.verdict_path(registry, project_id)
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    atomic_json(path, {"rules": SHARE_RULES_VERSION, "schema": 1, "verdicts": verdicts})


def test_a_checkout_without_a_verdict_reads_its_siblings(tmp_path: Path) -> None:
    # Two CI runners of one repository use two work folders, so two project stores; each batch had to run alone once in
    # each folder before it could share.
    _store_for(tmp_path, "a" * 64, {"shared": _verdict(True), "split": _verdict(True)})
    _store_for(tmp_path, "b" * 64, {"split": _verdict(False), "own": _verdict(False)})
    _store_for(tmp_path, "c" * 64, {"own": _verdict(True)})

    assert sharing.read_verdict(tmp_path, "c" * 64, "shared") is True  # learned in another checkout
    assert sharing.read_verdict(tmp_path, "c" * 64, "split") is False  # any negative sibling wins
    assert sharing.read_verdict(tmp_path, "c" * 64, "own") is False  # a sibling's negative outranks a local positive
    assert sharing.read_verdict(tmp_path, "c" * 64, "nowhere") is None


def test_a_clean_sibling_checkout_never_defeats_a_negative(tmp_path: Path) -> None:
    # Checkout A recorded a negative; checkout B of the same request runs exclusive, finishes clean and writes its own
    # positive: B's next run must still read the negative .
    _store_for(tmp_path, "a" * 64, {"k": _verdict(False)})
    assert sharing.read_verdict(tmp_path, "b" * 64, "k") is False
    sharing.write_verdict(tmp_path, "b" * 64, "k", shareable=True, reason=None, run_id="r", origin="o", tick_ns=9)

    assert sharing.read_verdict(tmp_path, "b" * 64, "k") is False


def _origin(root: Path, url: str) -> Path:
    (root / ".git").mkdir(parents=True)
    (root / ".git" / "config").write_text(f'[core]\n\tbare = false\n[remote "origin"]\n\turl = {url}\n')
    return root


def test_repository_identity_is_the_normalized_origin_address(tmp_path: Path) -> None:
    # Two checkouts of one repository in two CI work folders, by HTTPS and by SSH, are one repository.
    one = _origin(tmp_path / "project-a", "https://github.com/example/project-a.git")
    two = _origin(tmp_path / "project-a-2" / "hostexecutor", "git@GitHub.com:example/project-a")
    other = _origin(tmp_path / "project-b", "https://user@github.com/example/project-b/")

    assert sharing.repository_identity(str(one)) == "github.com/example/project-a"
    assert sharing.repository_identity(str(two)) == "github.com/example/project-a"
    assert sharing.repository_identity(str(other)) == "github.com/example/project-b"


def test_a_checkout_without_an_origin_is_only_itself(tmp_path: Path) -> None:
    plain = tmp_path / "plain"
    plain.mkdir()
    no_origin = _origin(tmp_path / "local", "unused")
    (no_origin / ".git" / "config").write_text("[core]\n\tbare = false\n")

    assert sharing.repository_identity(str(plain)) == str(plain.resolve())
    assert sharing.repository_identity(str(no_origin)) == str(no_origin.resolve())


def test_repository_origin_is_the_normalized_origin_or_none(tmp_path: Path) -> None:
    # Forgejo's host executor checks every repository out as `hostexecutor`: the folder name names nothing.
    one = _origin(tmp_path / "a" / "hostexecutor", "https://github.com/example/project-a.git")
    same = _origin(tmp_path / "b" / "hostexecutor", "git@github.com:example/project-a")
    other = _origin(tmp_path / "c" / "hostexecutor", "https://github.com/example/project-b.git")
    plain = tmp_path / "plain"
    plain.mkdir()

    assert sharing.repository_origin(str(one)) == sharing.repository_origin(str(same)) == "github.com/example/project-a"
    assert sharing.repository_origin(str(other)) == "github.com/example/project-b"
    assert sharing.repository_origin(str(plain)) is None
    assert sharing.repository_identity(str(plain)) == str(plain.resolve())  # the share scope keeps its path fallback


def test_request_keys_follow_the_origin_not_the_folder_name(tmp_path: Path) -> None:
    one = _origin(tmp_path / "a" / "hostexecutor", "https://github.com/example/project-a.git")
    same = _origin(tmp_path / "b" / "project-a-checkout", "git@github.com:example/project-a")
    other = _origin(tmp_path / "c" / "hostexecutor", "https://github.com/example/project-b.git")

    def key(root: Path) -> str:
        origin = sharing.repository_origin(str(root))
        assert origin is not None
        return sharing.request_key(origin, "integration", "fixture", ())

    assert key(one) == key(same) != key(other)


def test_a_worktree_reads_the_common_config(tmp_path: Path) -> None:
    main = _origin(tmp_path / "main", "https://github.com/example/project-a.git")
    worktree = tmp_path / "wt"
    worktree.mkdir()
    gitdir = main / ".git" / "worktrees" / "wt"
    gitdir.mkdir(parents=True)
    (gitdir / "commondir").write_text("../..\n")
    (worktree / ".git").write_text(f"gitdir: {gitdir}\n")

    assert sharing.repository_identity(str(worktree)) == "github.com/example/project-a"


def test_share_scope_defaults_to_machine_and_refuses_unknown_values(tmp_path: Path) -> None:
    assert sharing.read_share_scope(tmp_path) == "machine"
    sharing.record_share_scope(tmp_path, "repository")
    assert sharing.read_share_scope(tmp_path) == "repository"
    with pytest.raises(ResourceError):
        sharing.record_share_scope(tmp_path, "project")


def test_repository_identity_keeps_ports_and_resolves_local_origins(tmp_path: Path) -> None:
    # Review impl-0618: two ports can be two servers; relative local origins of unrelated trees must not collide.
    a = _origin(tmp_path / "a", "ssh://git@forge.example:2222/team/app.git")
    b = _origin(tmp_path / "b", "ssh://git@forge.example:2223/team/app.git")
    c = _origin(tmp_path / "one" / "c", "../upstream.git")
    d = _origin(tmp_path / "two" / "d", "../upstream.git")

    assert sharing.repository_identity(str(a)) == "forge.example:2222/team/app"
    assert sharing.repository_identity(str(a)) != sharing.repository_identity(str(b))
    assert sharing.repository_identity(str(c)) == str((tmp_path / "one" / "upstream.git").resolve())
    assert sharing.repository_identity(str(c)) != sharing.repository_identity(str(d))


def test_repository_identity_reads_quoted_and_tab_separated_values(tmp_path: Path) -> None:
    quoted = tmp_path / "quoted"
    (quoted / ".git").mkdir(parents=True)
    (quoted / ".git" / "config").write_text('[remote "origin"]\n\turl\t= "https://github.com/example/one.git"\n')
    plain = _origin(tmp_path / "plain", "https://github.com/example/one.git")

    assert (
        sharing.repository_identity(str(quoted)) == sharing.repository_identity(str(plain)) == "github.com/example/one"
    )


def test_disk_reserve_round_trip_and_floor(tmp_path: Path) -> None:
    account = _account(tmp_path)
    assert sharing.read_disk_reserve(account) is None  # undeclared: each place derives its own

    sharing.record_disk_reserve(account, 2 * 1024**3)

    path = account / "disk-reserve.json"
    assert json.loads(path.read_text()) == {"disk_reserve_bytes": 2 * 1024**3, "schema": 1}
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert sharing.read_disk_reserve(account) == 2 * 1024**3
    with pytest.raises(ResourceError, match="1 GiB"):
        sharing.record_disk_reserve(account, 1024**3 - 1)
    atomic_json(path, {"disk_reserve_bytes": 5, "schema": 1})
    with pytest.raises(ResourceError):
        sharing.read_disk_reserve(account)  # an invalid declaration fails closed


def test_stale_leases_are_dropped_on_every_write_so_the_file_stays_bounded(tmp_path: Path) -> None:
    account = _account(tmp_path)
    lease = sharing.Lease(4 * 1024**3, "floor", "origin-a", 7)
    stale = {f"stale-{index}": lease for index in range(5000)}

    sharing.write_leases(
        account,
        {**stale, "r1": lease, "r2": replace(lease, clock_origin="origin-b")},
        holders={"r1", "r2"},
        clock_origin="origin-a",
    )

    assert sharing.read_leases(account) == {"r1": lease}  # non-holders and another boot's entries are dropped
    assert len((account / "disk-leases.json").read_bytes()) < 1024
    sharing.write_leases(account, {}, holders=set(), clock_origin="origin-a")
    assert sharing.read_leases(account) == {}


def test_an_unreadable_lease_file_reads_empty(tmp_path: Path) -> None:
    account = _account(tmp_path)
    atomic_json(account / "disk-leases.json", {"schema": 1, "leases": {"r1": {"bytes": "many"}}})

    assert sharing.read_leases(account) == {}  # every holder then counts the floor


def test_turns_round_trip_drop_expired_entries_and_remove_only_their_run(tmp_path: Path) -> None:
    from docker_test_toolkit.domain.policy import TURN_GRACE_NS, Turn

    account = _account(tmp_path)
    now = 10 * TURN_GRACE_NS
    fresh, stale = Turn("r1", "d1", "boot", now - 1), Turn("r0", "d1", "boot", now - TURN_GRACE_NS)
    foreign = Turn("r2", "d1", "earlier-boot", now - 1)

    sharing.write_turns(account, {"one": fresh, "two": stale, "three": foreign}, now_ns=now, clock_origin="boot")

    path = account / "turn.json"
    assert sharing.read_turns(account) == {"one": fresh}  # expired and another boot's entries are dropped
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    sharing.remove_turn(account, "one", "another-run")  # a turn another run earned stays
    assert sharing.read_turns(account) == {"one": fresh}
    sharing.remove_turn(account, "one", "r1")
    assert sharing.read_turns(account) == {} and not path.exists()


def test_an_unreadable_turn_file_reads_empty(tmp_path: Path) -> None:
    account = _account(tmp_path)
    atomic_json(account / "turn.json", {"schema": 1, "turns": {"one": {"run_id": 5}}})

    assert sharing.read_turns(account) == {}  # no turn: today's order
