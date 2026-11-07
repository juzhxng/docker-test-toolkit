"""The image lock K at dispatch: shared runs serialize image operations; exclusive runs never take K."""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from docker_test_toolkit.application import lifecycle
from docker_test_toolkit.application.recovery import Context, atomic_json, image_lock
from docker_test_toolkit.domain.errors import ResourceBusy
from docker_test_toolkit.factories.testing import install_lock_observer
from tests.guard.lock_order import LockOrderChecker
from tests.integration.share_fixtures import _dispatch, _leave, _receipts, _record, _World, shared_world

pytestmark = pytest.mark.integration

REFERENCE = "example/cache@sha256:" + "2" * 64


@pytest.fixture
def world(tmp_path: Path) -> Iterator[_World]:
    with shared_world(tmp_path) as built:
        yield built


def _account(world: _World) -> Context:
    account = world.services.root / "admission"
    return Context(account, account)


def _images_events(run) -> list[tuple]:
    events: list[tuple] = []
    install_lock_observer(lambda *event: events.append(event))
    try:
        run()
    finally:
        install_lock_observer(None)
    return [event for event in events if event[1] == "images"]


def test_exclusive_run_takes_no_k(world: _World) -> None:
    exclusive = world.open(shared=False)

    events = _images_events(lambda: _dispatch(world, exclusive, "pull", REFERENCE))

    assert events == []
    assert _receipts(exclusive)[-1]["kind"] == "image-pull"
    assert all(item["reason"] != "image-lock" for item in _record(exclusive)["hold_timeline"]["waiting"])
    _leave(world, exclusive)


def test_shared_pull_takes_k_and_passes_it_to_the_docker_child(world: _World) -> None:
    shared = world.open(shared=True)

    events = _images_events(lambda: _dispatch(world, shared, "pull", REFERENCE))

    assert [event[0] for event in events] == ["acquire", "release"]
    assert world.fake.held_fds[-1] is not None  # the pull child inherited K's descriptor
    _leave(world, shared)


def test_image_lock_wait_recorded_in_hold_timeline_never_in_expiry_evidence(world: _World) -> None:
    shared = world.open(shared=True)
    _ = shared.docker_context
    released = threading.Event()

    def hold_k() -> None:
        with image_lock(_account(world)):
            released.wait(5)

    holder = threading.Thread(target=hold_k)
    holder.start()
    time.sleep(0.2)
    timer = threading.Timer(0.5, released.set)
    timer.start()
    try:
        _dispatch(world, shared, "pull", REFERENCE)
    finally:
        released.set()
        holder.join(10)
        timer.cancel()

    record = _record(shared)
    waits = [item for item in record["hold_timeline"]["waiting"] if item["reason"] == "image-lock"]
    assert len(waits) == 1 and waits[0]["waited_ns"] > 0
    assert "image-lock" not in str(record.get("failure_evidence"))
    _leave(world, shared)


def test_k_wait_bounded_by_command_deadline(world: _World, monkeypatch: pytest.MonkeyPatch) -> None:
    shared = world.open(shared=True)
    _ = shared.docker_context
    before = _receipts(shared)
    # the command's own bound (1200 s for a pull) shortened for the test; the wait itself is the production path
    monkeypatch.setitem(lifecycle._IMAGE_LOCK_SECONDS, "image-pull", 1)

    with image_lock(_account(world)), pytest.raises(ResourceBusy) as raised:
        _dispatch(world, shared, "pull", REFERENCE)

    assert raised.value.reason == "lock-deadline"
    assert _receipts(shared) == before  # nothing journaled
    _leave(world, shared)


def _ordered(run) -> LockOrderChecker:
    checker = LockOrderChecker()
    install_lock_observer(checker)
    try:
        run()
    finally:
        install_lock_observer(None)
    return checker


def test_row_dispatch_build_k_then_cache_project_run(world: _World) -> None:
    shared = world.open(shared=True)
    _dispatch(world, shared, "buildx", "create")

    checker = _ordered(lambda: _dispatch(world, shared, "buildx", "build", "--tag", "fixture:latest", "."))

    acquired = [event[1] for event in checker.events if event[0] == "acquire"]
    assert checker.violations == [] and acquired[0] == "images"
    _leave(world, shared)


def test_row_bind_existing_image_k_then_project_run(world: _World) -> None:
    shared = world.open(shared=True)
    _dispatch(world, shared, "pull", REFERENCE)
    other = world.open(shared=True)  # a second run binds the image the first one pulled
    _ = other.docker_context  # its Docker authority exists before the observed call

    checker = _ordered(lambda: _dispatch(world, other, "pull", REFERENCE))

    acquired = [event[1] for event in checker.events if event[0] == "acquire"]
    assert checker.violations == [] and acquired[0] == "images"
    _leave(world, other)
    _leave(world, shared)


def test_row_reconcile_image_receipt_k_first(world: _World) -> None:
    shared = world.open(shared=True)
    _dispatch(world, shared, "pull", REFERENCE)
    record = _record(shared)
    receipt = next(item for item in record["docker_receipts"].values() if item["kind"] == "image-pull")
    receipt.update(state="pending", result=None)  # as if the dispatcher died after its pull child exited
    atomic_json(shared._context.record, record)

    checker = _ordered(lambda: lifecycle._reconcile_docker_receipt(shared._context))

    acquired = [event[1] for event in checker.events if event[0] == "acquire"]
    assert checker.violations == [] and acquired[0] == "images"
    assert _receipts(shared)[-1]["state"] not in {"pending", "unresolved"}
    _leave(world, shared)


def test_shared_image_rm_waits_for_k(world: _World) -> None:
    shared = world.open(shared=True)
    _dispatch(world, shared, "buildx", "create")
    _dispatch(world, shared, "buildx", "build", "--tag", "fixture:latest", ".")
    image_id = next(
        key for key, item in _record(shared)["owned_resources"]["images"].items() if item["state"] != "removed"
    )
    released = threading.Event()

    def hold_k() -> None:
        with image_lock(_account(world)):
            released.wait(5)

    holder = threading.Thread(target=hold_k)
    holder.start()
    time.sleep(0.2)
    timer = threading.Timer(0.5, released.set)
    timer.start()
    try:
        _dispatch(world, shared, "image", "rm", image_id)
    finally:
        released.set()
        holder.join(10)
        timer.cancel()

    waits = [item for item in _record(shared)["hold_timeline"]["waiting"] if item["reason"] == "image-lock"]
    assert len(waits) == 1 and waits[0]["waited_ns"] > 0
    assert world.fake.held_fds[-1] is not None
    _leave(world, shared)


def test_binding_an_already_present_image_records_the_k_wait(world: _World) -> None:
    first = world.open(shared=True)
    _dispatch(world, first, "pull", REFERENCE)  # the image is now present in the daemon
    shared = world.open(shared=True)
    _ = shared.docker_context
    released = threading.Event()

    def hold_k() -> None:
        with image_lock(_account(world)):
            released.wait(5)

    holder = threading.Thread(target=hold_k)
    holder.start()
    time.sleep(0.2)
    timer = threading.Timer(0.5, released.set)
    timer.start()
    try:
        _dispatch(world, shared, "pull", REFERENCE)  # binds the cached image: no receipt
    finally:
        released.set()
        holder.join(10)
        timer.cancel()

    waits = [item for item in _record(shared)["hold_timeline"]["waiting"] if item["reason"] == "image-lock"]
    assert len(waits) == 1 and waits[0]["waited_ns"] > 0
    _leave(world, shared)
    _leave(world, first)
