"""The lock-order checker rejects inversions, ranks run locks by prefix and accepts non-waiting acquisitions."""

from __future__ import annotations

import time
from pathlib import Path

import pytest
from tests.guard.lock_order import CACHE, GUARD_RANKS, LockOrderChecker, rank

from docker_test_toolkit.application.recovery import Context, lock
from docker_test_toolkit.factories.testing import install_lock_observer

RUN_ID = "11111111-1111-4111-8111-111111111111"


def _context(tmp_path: Path) -> Context:
    return Context(tmp_path / "project", tmp_path / "state")


@pytest.mark.parametrize(
    "case",
    [
        pytest.param("registry-then-blocking-project", id="a-blocking-no-deadline"),
        pytest.param("registry-then-deadline-project", id="b-deadline-polled-wait"),
        pytest.param("run-then-blocking-project", id="run-then-project"),
        pytest.param("metadata-then-blocking-registry", id="metadata-then-registry"),
    ],
)
def test_checker_rejects_constructed_inversion(tmp_path: Path, case: str) -> None:
    ctx = _context(tmp_path)
    checker = LockOrderChecker()
    install_lock_observer(checker)
    try:
        if case == "registry-then-blocking-project":
            with lock(ctx, "registry"), lock(ctx, ctx.project_id):
                pass
        elif case == "registry-then-deadline-project":  # the syscall is LOCK_NB, but the logical wait is blocking
            with lock(ctx, "registry"), lock(ctx, ctx.project_id, deadline_ns=time.monotonic_ns() + 1_000_000_000):
                pass
        elif case == "run-then-blocking-project":
            with lock(ctx, f"run-{RUN_ID}"), lock(ctx, ctx.project_id):
                pass
        else:
            with lock(ctx, "metadata"), lock(ctx, "registry"):
                pass
    finally:
        install_lock_observer(None)

    assert len(checker.violations) == 1


def test_checker_accepts_the_documented_order_and_non_waiting_acquisitions(tmp_path: Path) -> None:
    ctx = _context(tmp_path)
    checker = LockOrderChecker()
    install_lock_observer(checker)
    try:
        with (
            lock(ctx, ctx.project_id, shared=True),
            lock(ctx, f"run-{RUN_ID}"),
            lock(ctx, "registry"),
            lock(ctx, "metadata"),
        ):
            pass
        with lock(ctx, "registry"), lock(ctx, "capacity", nonblocking=True):  # c: capacity never waits
            pass
        with (
            lock(ctx, "metadata"),
            lock(ctx, ctx.project_id, nonblocking=True),
        ):  # a non-waiting attempt cannot deadlock
            pass
    finally:
        install_lock_observer(None)

    assert checker.violations == []


def test_checker_ranks_run_locks_by_prefix() -> None:
    assert rank(f"run-{RUN_ID}") == GUARD_RANKS["run"]
    assert rank("runner") is None and rank("run") is None and rank("run-") is None  # not run locks
    assert rank(f"run-{RUN_ID}x") is None and rank(f"xrun-{RUN_ID}") is None
    assert rank("a" * 64) == GUARD_RANKS["project"]
    assert rank("registry") == GUARD_RANKS["registry"] and rank("metadata") == GUARD_RANKS["metadata"]
    assert rank("capacity") is None  # exempt in the checker, never ranked


def test_checker_flags_a_lock_outside_the_table(tmp_path: Path) -> None:
    ctx = _context(tmp_path)
    checker = LockOrderChecker()
    install_lock_observer(checker)
    try:
        with lock(ctx, "surprise"):
            pass
    finally:
        install_lock_observer(None)

    assert checker.violations == ["unranked lock 'surprise'"]


def test_checker_ranks_cache_anchors_lowest_without_overlap() -> None:
    assert rank("linux-arm64") == CACHE and rank("linux-x86_64") == CACHE
    assert min(GUARD_RANKS.values()) > CACHE
    for name in (f"run-{RUN_ID}", "a" * 64, "registry", "metadata", "capacity", "linux", "linux-arm64-v8"):
        assert rank(name) != CACHE, name


def test_checker_rejects_a_blocking_cache_anchor_under_a_project_lock(tmp_path: Path) -> None:
    ctx = _context(tmp_path)
    checker = LockOrderChecker()
    install_lock_observer(checker)
    try:
        with lock(ctx, ctx.project_id, shared=True), lock(ctx, "linux-arm64", shared=True):
            pass
    finally:
        install_lock_observer(None)

    assert len(checker.violations) == 1


def test_checker_accepts_project_and_run_locks_inside_a_cache_anchor(tmp_path: Path) -> None:
    ctx = _context(tmp_path)
    checker = LockOrderChecker()
    install_lock_observer(checker)
    try:
        with (
            lock(ctx, "linux-arm64", shared=True, deadline_ns=time.monotonic_ns() + 1_000_000_000),
            lock(ctx, ctx.project_id, shared=True),
            lock(ctx, f"run-{RUN_ID}"),
        ):
            pass
    finally:
        install_lock_observer(None)

    assert checker.violations == []


def _observe(steps) -> LockOrderChecker:
    checker = LockOrderChecker()
    for step in steps:
        checker(*step)
    return checker


def test_k_wait_with_capacity_and_shared_project_allowed() -> None:
    project = "a" * 64
    checker = _observe(
        [
            ("acquire", "capacity", True, False, False),
            ("acquire", project, True, True, False),
            ("acquire", "images", False, True, True),
        ]
    )

    assert checker.violations == []


@pytest.mark.parametrize("held", [f"run-{RUN_ID}", "a" * 64], ids=["run-lock", "exclusive-project"])
def test_k_wait_with_run_lock_is_violation(held: str) -> None:
    checker = _observe([("acquire", held, False, True, False), ("acquire", "images", False, True, True)])

    assert len(checker.violations) == 1


def test_install_rank_below_images() -> None:
    assert rank("install") < rank("images") < CACHE
    assert (
        len(
            _observe([("acquire", "images", False, True, False), ("acquire", "install", False, True, False)]).violations
        )
        == 1
    )


def test_nonblocking_k_with_higher_ranks_allowed() -> None:
    checker = _observe([("acquire", "metadata", False, True, False), ("acquire", "images", False, False, False)])

    assert checker.violations == []
