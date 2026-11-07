"""Private state on a fresh host: every directory the toolkit creates is 0700, whatever the umask.

A fresh account (a CI runner, a new machine) has no toolkit state yet. ``Path.mkdir(parents=True)`` applies ``mode``
only to the last component, so the registry once came out 0755 under ``umask 022`` and the next privacy check refused
every run.
"""

from __future__ import annotations

import os
import stat
from collections.abc import Iterator
from pathlib import Path

import pytest

from docker_test_toolkit.application import lifecycle
from docker_test_toolkit.application.recovery import Context, atomic_bytes, lock
from docker_test_toolkit.application.state import make_private_directories

pytestmark = pytest.mark.integration


@pytest.fixture(params=[0o022, 0o002, 0o000, 0o077], ids=["umask-022", "umask-002", "umask-000", "umask-077"])
def umask(request: pytest.FixtureRequest) -> Iterator[int]:
    """A fresh account's umask (the usual 022 and permissive ones), restored afterwards."""
    previous = os.umask(request.param)
    try:
        yield request.param
    finally:
        os.umask(previous)


def _modes(base: Path, leaf: Path) -> dict[str, int]:
    chain = [leaf, *leaf.parents]
    return {str(path.relative_to(base)): stat.S_IMODE(path.stat().st_mode) for path in chain if base in path.parents}


@pytest.mark.usefixtures("umask")
def test_the_account_lock_creates_the_whole_missing_chain_private_and_the_registry_passes(tmp_path: Path) -> None:
    # Activation's first lock creates the account directory below a registry that does not exist yet.
    registry = tmp_path / "home" / ".local" / "state" / "docker-test-toolkit" / "registry"
    account = registry / "test-admission"

    with lock(Context(account, account), "metadata"):
        pass

    assert set(_modes(tmp_path, account).values()) == {0o700}, _modes(tmp_path, account)
    lifecycle._private_directory(registry)  # the run preparation check that refused every CI run


@pytest.mark.usefixtures("umask")
def test_a_durable_record_below_missing_directories_creates_them_private(tmp_path: Path) -> None:
    record = tmp_path / "state" / "projects" / "abc" / "record.json"

    atomic_bytes(record, b"{}")

    assert record.read_bytes() == b"{}"
    assert set(_modes(tmp_path, record.parent).values()) == {0o700}, _modes(tmp_path, record.parent)


def test_an_existing_directory_that_is_too_open_is_refused_not_repaired(tmp_path: Path) -> None:
    registry = tmp_path / "registry"
    registry.mkdir()
    registry.chmod(0o755)

    with pytest.raises(Exception, match="not private and account-owned"):
        lifecycle._private_directory(registry)
    assert stat.S_IMODE(registry.stat().st_mode) == 0o755


def test_an_existing_open_parent_is_left_unchanged_while_the_missing_child_is_private(tmp_path: Path) -> None:
    parent = tmp_path / "state"
    parent.mkdir()
    parent.chmod(0o755)

    make_private_directories(parent / "registry")

    assert stat.S_IMODE(parent.stat().st_mode) == 0o755
    assert stat.S_IMODE((parent / "registry").stat().st_mode) == 0o700


def test_a_non_directory_in_the_way_raises_the_same_error_as_path_mkdir(tmp_path: Path) -> None:
    blocker = tmp_path / "registry"
    blocker.write_text("not a directory")
    dangling = tmp_path / "dangling"
    dangling.symlink_to(tmp_path / "missing")

    for path in (blocker, blocker / "test-admission", dangling):
        with pytest.raises(OSError) as native:
            path.mkdir(mode=0o700, parents=True, exist_ok=True)
        with pytest.raises(type(native.value)):
            make_private_directories(path)
