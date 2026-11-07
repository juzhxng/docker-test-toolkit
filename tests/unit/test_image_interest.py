"""Image interest: who still uses an image when a shared owner leaves, and when that use has ended."""

from __future__ import annotations

import pytest

from docker_test_toolkit.domain.policy import InterestView, image_in_use, interest_ended

X, Y = "sha256:" + "1" * 64, "sha256:" + "2" * 64


def _view(run_id: str = "b", *, state: str | None = "active", actor: bool = False, removed: bool = False, images=(X,)):
    return InterestView(run_id, state, actor, removed, None if images is None else frozenset(images))


@pytest.mark.parametrize(
    ("state", "actor", "removed", "ended"),
    [
        ("complete", False, False, True),  # terminal
        ("active", False, True, False),  # still accepts mutations
        ("stopping", False, False, False),  # stopping, a container still held
        ("stopping", False, True, True),  # stopping and every container removed
        ("active", True, True, True),  # a recovery actor is set and every container removed
        ("active", True, False, False),
        ("stopped-retained", False, True, False),  # a retained run keeps its interest until its release
        (None, False, True, False),  # an unreadable record never ended
    ],
)
def test_interest_ended_matrix(state: str | None, actor: bool, removed: bool, ended: bool) -> None:
    assert interest_ended(_view(state=state, actor=actor, removed=removed)) is ended


def test_image_in_use_excludes_actor_and_ended() -> None:
    owner = _view("a")
    live_binder = _view("b")
    ended_binder = _view("c", state="stopping", removed=True)
    other_image = _view("d", images=(Y,))

    assert image_in_use(X, [owner, live_binder], actor_run="a")
    assert not image_in_use(X, [owner], actor_run="a")  # the deciding record never protects its own image
    assert not image_in_use(X, [owner, ended_binder, other_image], actor_run="a")


def test_unreadable_record_counts_as_using_every_image() -> None:
    unreadable = _view("u", state=None, images=None)

    assert image_in_use(X, [unreadable], actor_run="a")
    assert image_in_use(Y, [unreadable], actor_run="a")
