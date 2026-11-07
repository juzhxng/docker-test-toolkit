"""The image step under K: hand-off to the pending list, last-user drain, and its crash points."""

from __future__ import annotations

import contextlib
import dataclasses
import json
import threading
import time
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

from docker_test_toolkit.application import lifecycle, sharing
from docker_test_toolkit.application.recovery import Context, image_lock
from docker_test_toolkit.domain.errors import IncompleteCleanup, ResourceBusy, ResourceError
from docker_test_toolkit.factories.testing import install_failpoint
from tests.integration.account_fixtures import (
    current_identity,
    dead_identity,
    kill_guardian,
    replace_identity,
    seed_dead_since,
)
from tests.integration.share_fixtures import _dispatch, _leave, _record, _World, shared_world

pytestmark = pytest.mark.integration

REFERENCE = "example/cache@sha256:" + "2" * 64


@pytest.fixture
def world(tmp_path: Path) -> Iterator[_World]:
    with shared_world(tmp_path) as built:
        yield built


def _account(world: _World) -> Path:
    return world.services.root / "admission"


def _owner_and_binder(world: _World):
    """Shared owner A pulls X (owned); shared binder B binds the same, now cached, X."""
    owner = world.open(shared=True, declare_image=False)
    _dispatch(world, owner, "pull", REFERENCE)
    image_id = next(iter(_record(owner)["owned_resources"]["images"]))
    binder = world.open(shared=True, declare_image=False)
    _dispatch(world, binder, "pull", REFERENCE)
    return owner, binder, image_id


def _exit_at(stage: str) -> Callable[[str], None]:
    def callback(name: str) -> None:
        if name == stage:
            raise SystemExit(f"fixture exit at {stage}")

    return callback


def _image_present(world: _World, image_id: str) -> bool:
    return image_id in world.fake.images


def test_owner_hands_off_and_the_last_user_drains(world: _World) -> None:
    owner, binder, image_id = _owner_and_binder(world)

    owner.leave()  # A's guardian: B still uses X, so A hands X off (no Docker call) and leaves

    assert [entry.image_id for entry in sharing.read_pending(_account(world))] == [image_id]
    resource = _record(owner)["owned_resources"]["images"][image_id]
    assert (resource["state"], resource["evidence"]["pending_deletion"]) == ("removed", True)
    assert _image_present(world, image_id)

    lifecycle._cleanup_owned_docker_resources(binder._context)  # B is the last user: it drains X

    assert sharing.read_pending(_account(world)) == []
    assert not _image_present(world, image_id)
    _leave(world, binder)


def test_image_step_crash_after_pending_append_marks_only(world: _World) -> None:
    owner, binder, image_id = _owner_and_binder(world)
    install_failpoint(_exit_at("pending-appended"))
    try:
        with pytest.raises(SystemExit):
            lifecycle._cleanup_owned_docker_resources(owner._context)
    finally:
        install_failpoint(None)
    assert _record(owner)["owned_resources"]["images"][image_id]["state"] != "removed"  # crashed before the mark

    owner.leave()  # the re-run (A's guardian) finds the entry and only marks

    assert [entry.image_id for entry in sharing.read_pending(_account(world))] == [image_id]  # not appended twice
    assert _record(owner)["owned_resources"]["images"][image_id]["state"] == "removed"
    lifecycle._cleanup_owned_docker_resources(binder._context)
    assert sharing.read_pending(_account(world)) == []
    _leave(world, binder)


def test_drain_crash_after_remove_drops_entry(world: _World) -> None:
    owner, binder, image_id = _owner_and_binder(world)
    owner.leave()
    install_failpoint(_exit_at("pending-image-removed"))
    try:
        with pytest.raises(SystemExit):
            lifecycle._cleanup_owned_docker_resources(binder._context)
    finally:
        install_failpoint(None)
    assert not _image_present(world, image_id)
    assert [entry.image_id for entry in sharing.read_pending(_account(world))] == [image_id]  # not yet rewritten

    lifecycle._cleanup_owned_docker_resources(binder._context)  # the absent image drops the entry

    assert sharing.read_pending(_account(world)) == []
    _leave(world, binder)


def test_death_after_container_removal_before_image_step(world: _World) -> None:
    owner, binder, image_id = _owner_and_binder(world)
    install_failpoint(_exit_at("owned-containers-removed"))  # A dies between its containers and its image step
    try:
        with pytest.raises(SystemExit):
            lifecycle._cleanup_owned_docker_resources(owner._context)
    finally:
        install_failpoint(None)

    lifecycle._cleanup_owned_docker_resources(binder._context)  # B owns nothing: it deletes nothing A owns
    assert _image_present(world, image_id)
    _leave(world, binder)  # B ends for real (its image step makes no Docker call: nothing owned, nothing pending)
    lifecycle._cleanup_owned_docker_resources(owner._context)  # A's retried image step: no live interest, X deleted

    assert not _image_present(world, image_id)
    assert sharing.read_pending(_account(world)) == []
    _leave(world, owner)


def test_image_step_wait_bounded_by_the_callers_deadline(world: _World) -> None:
    owner, binder, image_id = _owner_and_binder(world)
    account = _account(world)

    with image_lock(Context(account, account)), pytest.raises(ResourceBusy) as raised:
        lifecycle._cleanup_owned_docker_resources(owner._context, time.monotonic_ns() + 500_000_000)

    assert raised.value.reason == "lock-deadline"
    assert _image_present(world, image_id) and sharing.read_pending(account) == []
    _leave(world, binder)
    _leave(world, owner)


def test_unreadable_pending_list_keeps_images_unresolved(world: _World) -> None:
    owner, binder, image_id = _owner_and_binder(world)
    (_account(world) / sharing.PENDING_FILE).write_text("{broken")
    (_account(world) / sharing.PENDING_FILE).chmod(0o600)

    with pytest.raises(IncompleteCleanup):
        lifecycle._cleanup_owned_docker_resources(owner._context)

    assert _image_present(world, image_id)
    (_account(world) / sharing.PENDING_FILE).unlink()
    _leave(world, binder)
    _leave(world, owner)


def test_owner_image_rm_hands_off_while_in_use(world: _World) -> None:
    owner, binder, image_id = _owner_and_binder(world)

    result = _dispatch(world, owner, "image", "rm", image_id)

    assert result.exit_code == 0  # the command succeeds
    assert _image_present(world, image_id)  # handed off: the binder still uses it
    assert [entry.image_id for entry in sharing.read_pending(_account(world))] == [image_id]
    assert _record(owner)["owned_resources"]["images"][image_id]["evidence"]["pending_deletion"] is True
    owner.leave()
    lifecycle._cleanup_owned_docker_resources(binder._context)
    assert not _image_present(world, image_id)
    _leave(world, binder)


# ---- every deletion path decides interest, identity and daemon under K ------------


def _pending_ids(world: _World) -> list[str]:
    return [entry.image_id for entry in sharing.read_pending(_account(world))]


def _image_lock_waited_ns(run) -> int:
    return sum(item["waited_ns"] for item in _record(run)["hold_timeline"]["waiting"] if item["reason"] == "image-lock")


def test_owner_image_rm_hand_off_records_the_k_wait(world: _World) -> None:
    owner, binder, image_id = _owner_and_binder(world)
    waited_before = _image_lock_waited_ns(owner)  # its uncontended pull already merged a tiny wait
    acquired, released = threading.Event(), threading.Event()

    def hold_k() -> None:
        with image_lock(Context(_account(world), _account(world))):
            acquired.set()
            released.wait(10)

    holder = threading.Thread(target=hold_k)
    holder.start()
    assert acquired.wait(10)
    timer = threading.Timer(1.0, released.set)
    timer.start()
    try:
        assert _dispatch(world, owner, "image", "rm", image_id).exit_code == 0  # handed off after waiting for K
    finally:
        released.set()
        holder.join(10)
        timer.cancel()

    assert _image_lock_waited_ns(owner) - waited_before >= 500_000_000  # the contended wait (timer: 1 s)
    assert _pending_ids(world) == [image_id]
    owner.leave()
    lifecycle._cleanup_owned_docker_resources(binder._context)  # the last user drains in-process (the fake daemon)
    _leave(world, binder)


def test_drain_of_the_last_entry_removes_the_pending_file(world: _World) -> None:
    owner, binder, _image_id = _owner_and_binder(world)
    owner.leave()
    assert (_account(world) / sharing.PENDING_FILE).exists()

    lifecycle._cleanup_owned_docker_resources(binder._context)

    assert not (_account(world) / sharing.PENDING_FILE).exists()  # an exclusive run later takes K for nothing
    _leave(world, binder)


@pytest.mark.parametrize("death", ["killed", "interrupted"])
def test_pull_client_death_recovery_records_image_present_before_reinspection(world: _World, death: str) -> None:
    """The pull's client dies after the image exists: under K the present image is the run's own, then deleted."""
    shared = world.open(shared=True, declare_image=False)
    world.fake.pull_client_dies = death
    with contextlib.suppress(OSError, IncompleteCleanup, ResourceError):
        _dispatch(world, shared, "pull", REFERENCE)
    lifecycle._reconcile_docker_receipt(shared._context)  # what the guardian or the recoverer does first, under K

    owned = _record(shared)["owned_resources"]["images"]
    assert [item["state"] for item in owned.values()] == ["present"]
    image_id = next(iter(owned))
    lifecycle._cleanup_owned_docker_resources(shared._context)
    assert not _image_present(world, image_id)
    _leave(world, shared)


def test_exclusive_pull_client_death_stays_today(world: _World) -> None:
    """Limit-1 identity: an exclusive run's killed pull is failed and owns nothing, as before (no presence rule)."""
    exclusive = world.open(shared=False, declare_image=False)
    world.fake.pull_client_dies = "killed"
    with contextlib.suppress(OSError, IncompleteCleanup, ResourceError):
        _dispatch(world, exclusive, "pull", REFERENCE)

    assert _record(exclusive)["owned_resources"]["images"] == {}
    _leave(world, exclusive)


def test_image_rm_reconciliation_hands_off_to_a_later_binder(world: _World) -> None:
    """A journals its removal of X and dies before Docker runs it; B binds X; A's reconciliation hands X off."""
    owner = world.open(shared=True, declare_image=False)
    _dispatch(world, owner, "pull", REFERENCE)
    image_id = next(iter(_record(owner)["owned_resources"]["images"]))
    world.fake.fail_before_remove_kind = "image"
    with contextlib.suppress(OSError, IncompleteCleanup, ResourceError):
        _dispatch(world, owner, "image", "rm", image_id)
    binder = world.open(shared=True, declare_image=False)
    _dispatch(world, binder, "pull", REFERENCE)  # binds the still present X

    lifecycle._reconcile_docker_receipt(owner._context)

    assert _image_present(world, image_id)
    assert _pending_ids(world) == [image_id]
    assert _record(owner)["owned_resources"]["images"][image_id]["evidence"]["pending_deletion"] is True
    _leave(world, owner)
    lifecycle._cleanup_owned_docker_resources(binder._context)
    assert not _image_present(world, image_id)
    _leave(world, binder)


def _plant_pending(world: _World, run, image_id: str, references: tuple[str, ...], **changes) -> None:
    entry = sharing.PendingImage(
        image_id, "pulled-created", references, "f" * 8, "g" * 8, _record(run)["grant"]["daemon_id"]
    )
    sharing.write_pending(_account(world), [dataclasses.replace(entry, **changes)])


def _plant_image(world: _World, image_id: str, *tags: str) -> dict:
    item = {"Id": image_id, "Created": "pulled-created", "Config": {"Labels": {}}, "RepoTags": list(tags)}
    world.fake.images.update(dict.fromkeys((image_id, *tags), item))
    return item


def test_drain_keeps_a_tag_moved_to_another_image(world: _World) -> None:
    drainer = world.open(shared=True, declare_image=False)
    _ = drainer.docker_context
    handed, other = "sha256:" + "4" * 64, "sha256:" + "5" * 64
    _plant_image(world, handed)
    _plant_image(world, other, "moved:latest")  # the recorded tag now names another image
    _plant_pending(world, drainer, handed, ("moved:latest",))

    lifecycle._cleanup_owned_docker_resources(drainer._context)

    assert not _image_present(world, handed)  # removed by its ID
    assert world.fake.images["moved:latest"]["Id"] == other  # the moved tag and its image are untouched
    assert sharing.read_pending(_account(world)) == []
    _leave(world, drainer)


def test_drain_keeps_entries_another_daemon_recorded(world: _World) -> None:
    drainer = world.open(shared=True, declare_image=False)
    _ = drainer.docker_context
    handed = "sha256:" + "4" * 64
    _plant_image(world, handed, "handed:latest")
    _plant_pending(world, drainer, handed, ("handed:latest",), daemon_id="22222222-2222-2222-2222-222222222222")

    lifecycle._cleanup_owned_docker_resources(drainer._context)

    assert _image_present(world, handed)  # same content ID, other daemon: nothing deleted, entry kept
    assert _pending_ids(world) == [handed]
    sharing.write_pending(_account(world), [])
    _leave(world, drainer)


def test_drain_identity_mismatch_drops_entry(world: _World) -> None:
    drainer = world.open(shared=True, declare_image=False)
    _ = drainer.docker_context
    handed = "sha256:" + "4" * 64
    _plant_image(world, handed, "handed:latest")
    _plant_pending(world, drainer, handed, ("handed:latest",), created="another-creation")

    lifecycle._cleanup_owned_docker_resources(drainer._context)

    assert _image_present(world, handed)  # a different image under the recorded ID: nothing deleted
    assert sharing.read_pending(_account(world)) == []  # and the entry is dropped
    _leave(world, drainer)


def test_unreadable_project_index_keeps_images(world: _World) -> None:
    owner = world.open(shared=True, declare_image=False)
    _dispatch(world, owner, "pull", REFERENCE)
    image_id = next(iter(_record(owner)["owned_resources"]["images"]))
    broken = world.registry / "projects" / "unreadable.json"
    broken.write_text("{broken")
    broken.chmod(0o600)

    lifecycle._cleanup_owned_docker_resources(owner._context)

    assert _image_present(world, image_id)  # a retained run indexed there may use it: handed off, not deleted
    assert _pending_ids(world) == [image_id]
    broken.unlink()
    sharing.write_pending(_account(world), [])
    _leave(world, owner)


def test_chained_binders_last_drains(world: _World) -> None:
    """O hands off X; B2 binds, B3 joins and binds; B2 exits and keeps X for B3; B3 exits and drains X."""
    owner = world.open(shared=True, declare_image=False)
    _dispatch(world, owner, "pull", REFERENCE)
    image_id = next(iter(_record(owner)["owned_resources"]["images"]))
    second = world.open(shared=True, declare_image=False)
    _dispatch(world, second, "pull", REFERENCE)
    owner.leave()
    assert _pending_ids(world) == [image_id]
    third = world.open(shared=True, declare_image=False)
    _dispatch(world, third, "pull", REFERENCE)

    lifecycle._cleanup_owned_docker_resources(second._context)
    assert _image_present(world, image_id) and _pending_ids(world) == [image_id]
    _leave(world, second)
    lifecycle._cleanup_owned_docker_resources(third._context)

    assert not _image_present(world, image_id) and sharing.read_pending(_account(world)) == []
    _leave(world, third)


def test_use_image_declared_by_coholder_kept(world: _World) -> None:
    """A co-holder that only declared use_image X (no Docker command) keeps the owner's X."""
    owner = world.open(shared=True, declare_image=False)
    _dispatch(world, owner, "pull", REFERENCE)
    image_id = next(iter(_record(owner)["owned_resources"]["images"]))
    record = _record(owner)
    assert image_id == record["owned_resources"]["images"][image_id]["identifier"]
    declarer = world.open(shared=True)  # declares use_image of the fixture image ID, which the pull produced

    owner.leave()

    assert _image_present(world, image_id) and _pending_ids(world) == [image_id]
    lifecycle._cleanup_owned_docker_resources(declarer._context)
    assert not _image_present(world, image_id)
    _leave(world, declarer)


def test_exclusive_leave_drains_leftover_without_new_record_keys(world: _World) -> None:
    """A leftover entry drains at an exclusive leave, which records exactly what an exclusive leave records today."""
    control = world.open(shared=False, declare_image=False)
    _leave(world, control)
    exclusive = world.open(shared=False, declare_image=False)
    _ = exclusive.docker_context
    handed = "sha256:" + "4" * 64
    _plant_image(world, handed, "handed:latest")
    _plant_pending(world, exclusive, handed, ("handed:latest",))

    lifecycle._cleanup_owned_docker_resources(exclusive._context)  # its leave's cleanup, in-process (the fake daemon)
    _leave(world, exclusive)

    assert not _image_present(world, handed)
    assert not (_account(world) / sharing.PENDING_FILE).exists()
    assert set(_record(exclusive)) == set(_record(control))
    assert _record(exclusive)["owned_resources"] == _record(control)["owned_resources"]


def test_operator_image_interest_applies_at_every_limit(world: _World) -> None:
    """Operator cleanup keeps an image a live run uses at limit 2 and at limit 1 alike (decided under K per image)."""
    owner, binder, image_id = _owner_and_binder(world)

    for limit in (2, 1):  # the account lowered to limit 1 with nothing pending still protects the image
        sharing.record_parallel_runs(_account(world), limit)
        in_use = lifecycle.operator_image_interest(_account(world), world.registry)
        assert in_use(image_id)
        assert not in_use("sha256:" + "9" * 64)
    assert not (_account(world) / sharing.PENDING_FILE).exists()
    lifecycle._cleanup_owned_docker_resources(owner._context)
    owner.leave()
    lifecycle._cleanup_owned_docker_resources(binder._context)
    _leave(world, binder)


def test_malformed_index_entry_keeps_images(world: _World) -> None:
    owner = world.open(shared=True, declare_image=False)
    _dispatch(world, owner, "pull", REFERENCE)
    image_id = next(iter(_record(owner)["owned_resources"]["images"]))
    malformed = world.registry / "projects" / "malformed.json"
    lifecycle.atomic_json(malformed, {"runs": {"retained-elsewhere": None}})  # valid JSON, a run it cannot locate

    lifecycle._cleanup_owned_docker_resources(owner._context)

    assert _image_present(world, image_id) and _pending_ids(world) == [image_id]
    malformed.unlink()
    sharing.write_pending(_account(world), [])
    _leave(world, owner)


def test_recovered_pull_does_not_claim_an_image_another_run_pulled_since(world: _World) -> None:
    """A's dispatcher dies before its pull produced anything; B pulls the same image; A's reconciliation, under K
    again but not continuously, must not take B's image as its own."""
    first = world.open(shared=True, declare_image=False)
    world.fake.pull_client_dies = "lost"
    with contextlib.suppress(OSError, IncompleteCleanup, ResourceError):
        _dispatch(world, first, "pull", REFERENCE)
    second = world.open(shared=True, declare_image=False)
    _dispatch(world, second, "pull", REFERENCE)
    image_id = next(iter(_record(second)["owned_resources"]["images"]))

    lifecycle._reconcile_docker_receipt(first._context)

    assert _record(first)["owned_resources"]["images"] == {}
    assert _record(second)["owned_resources"]["images"][image_id]["state"] == "present"
    lifecycle._cleanup_owned_docker_resources(second._context)
    _leave(world, first)
    _leave(world, second)


def test_retained_binder_keeps_image_until_release(world: _World) -> None:
    """A stopped-retained binder keeps the owner's X until its own release, which then drains X."""
    owner = world.open(shared=True, declare_image=False)
    _dispatch(world, owner, "pull", REFERENCE)
    image_id = next(iter(_record(owner)["owned_resources"]["images"]))
    retained = world.open(shared=True, declare_image=False, retain=True)
    _dispatch(world, retained, "pull", REFERENCE)
    assert retained.retain().state == "stopped-retained"

    owner.leave()
    assert _image_present(world, image_id) and _pending_ids(world) == [image_id]
    lifecycle._cleanup_owned_docker_resources(owner._context)  # a later image step still keeps it (drain skipped)
    assert _image_present(world, image_id)

    retained.release()  # the release's image step: the retained interest ends, so it drains X

    assert not _image_present(world, image_id)
    assert sharing.read_pending(_account(world)) == []


def test_hand_off_keeps_another_daemons_entry_for_the_same_image(world: _World) -> None:
    """A pending entry of image X on another daemon is a different obligation: the hand-off on this daemon adds its
    own entry, so this daemon's last user still drains X."""
    owner, binder, image_id = _owner_and_binder(world)
    other = sharing.PendingImage(image_id, "pulled-created", (REFERENCE,), "f" * 8, "g" * 8, "2" * 8 + "-other-daemon")
    sharing.write_pending(_account(world), [other])

    owner.leave()

    daemons = sorted(entry.daemon_id for entry in sharing.read_pending(_account(world)))
    assert daemons == sorted([other.daemon_id, _record(owner)["grant"]["daemon_id"]])
    lifecycle._cleanup_owned_docker_resources(binder._context)
    assert not _image_present(world, image_id)
    assert [entry.daemon_id for entry in sharing.read_pending(_account(world))] == [other.daemon_id]
    sharing.write_pending(_account(world), [])
    _leave(world, binder)


def test_empty_grant_recovery_of_the_last_user_drains(world: _World) -> None:
    """B only declared use_image X (no Docker command), so its recovery takes the empty-grant path; as X's last
    user it still drains X."""
    owner = world.open(shared=True, declare_image=False)
    _dispatch(world, owner, "pull", REFERENCE)
    image_id = next(iter(_record(owner)["owned_resources"]["images"]))
    declarer = world.open(shared=True)  # declares use_image of the image the pull produced
    owner.leave()
    assert _pending_ids(world) == [image_id] and _image_present(world, image_id)
    kill_guardian(declarer._context)
    record = declarer._context.record
    lifecycle.atomic_json(record, replace_identity(json.loads(record.read_text()), current_identity(), dead_identity()))
    seed_dead_since(_account(world), declarer.run_id, age_s=31)

    lifecycle.recover_run(declarer._context, _test_directory=_account(world))

    assert not _image_present(world, image_id)
    assert not (_account(world) / sharing.PENDING_FILE).exists()


@pytest.mark.parametrize("damage", ["invalid", "not-private"])
def test_a_bad_pending_list_never_fails_an_empty_grant_recovery(world: _World, damage: str) -> None:
    """The recovery is durable before its drain: a pending list it cannot read is left for the next image step."""
    declarer = world.open(shared=True)
    pending = _account(world) / sharing.PENDING_FILE
    kill_guardian(declarer._context)  # no live guardian can rewrite the list after it is damaged
    pending.write_text("{broken")
    pending.chmod(0o644 if damage == "not-private" else 0o600)  # refused unread, also for root (CI runs as root)
    record = declarer._context.record
    lifecycle.atomic_json(record, replace_identity(json.loads(record.read_text()), current_identity(), dead_identity()))
    seed_dead_since(_account(world), declarer.run_id, age_s=31)

    lifecycle.recover_run(declarer._context, _test_directory=_account(world))

    pending.chmod(0o600)
    assert pending.read_text() == "{broken"
    assert json.loads(record.read_text())["state"] != "active"
    pending.unlink()
