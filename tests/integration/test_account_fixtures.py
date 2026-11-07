"""Self-tests of the foreign-resource fixtures: a snapshot must be able to fail, or its equality proves nothing."""

from __future__ import annotations

import copy
import os
import shutil
from dataclasses import replace
from pathlib import Path

import pytest

from docker_test_toolkit.factories.testing import build_test_services
from tests.integration.account_fixtures import (
    DOCKER_CLASSES,
    FILE_CLASSES,
    PROTECTED_CLASSES,
    ForeignSentinels,
    ScriptedDocker,
    ScriptedInventory,
    foreign_snapshot,
    observed_opens,
    require_present,
    seed_foreign_sentinels,
)

pytestmark = pytest.mark.integration


def _seeded(tmp_path: Path) -> tuple[ForeignSentinels, ScriptedDocker, ScriptedInventory]:
    services = build_test_services(tmp_path / "state")
    docker = ScriptedDocker()
    sentinels = seed_foreign_sentinels(tmp_path, services, docker=docker)
    return sentinels, docker, ScriptedInventory(docker)


@pytest.fixture(scope="module")
def module_seed(tmp_path_factory: pytest.TempPathFactory) -> tuple[ForeignSentinels, ScriptedDocker]:
    """One foreign seed per module for the nodes that change no sentinel file; a node gets a copy, never the seed."""
    sentinels, docker, _inventory = _seeded(tmp_path_factory.mktemp("module-seed"))
    return sentinels, docker


def _shared(
    seed: tuple[ForeignSentinels, ScriptedDocker],
) -> tuple[ForeignSentinels, ScriptedDocker, ScriptedInventory]:
    """A deep copy of the module seed: mutating its sentinels or Docker resources leaves the next node's seed intact."""
    sentinels, docker = copy.deepcopy(seed)
    return sentinels, docker, ScriptedInventory(docker)


def test_foreign_snapshot_holds_every_protected_class_and_is_stable(tmp_path: Path) -> None:
    sentinels, _docker, inventory = _seeded(tmp_path)

    snapshot = foreign_snapshot(sentinels, inventory)

    assert tuple(snapshot) == PROTECTED_CLASSES
    assert all(snapshot[name] for name in PROTECTED_CLASSES)
    require_present(snapshot)  # every sentinel really exists
    assert foreign_snapshot(sentinels, inventory) == snapshot
    assert tuple(foreign_snapshot(sentinels)) == FILE_CLASSES  # without Docker only the file classes are in scope


@pytest.mark.parametrize("empty", PROTECTED_CLASSES)
def test_foreign_snapshot_refuses_an_empty_class(module_seed, empty: str) -> None:
    sentinels, _docker, inventory = _shared(module_seed)
    emptied = {
        "project-record": replace(sentinels, project=None),
        "run-record": replace(sentinels, run_records=[]),
        "temporary-root": replace(sentinels, temporary_root=None),
    }.get(empty, replace(sentinels, docker={**sentinels.docker, empty: []}))

    with pytest.raises(AssertionError, match=f"empty protected class: {empty}"):
        foreign_snapshot(emptied, inventory)


@pytest.mark.parametrize("changed", DOCKER_CLASSES)
def test_foreign_snapshot_changes_when_a_foreign_label_is_added(module_seed, changed: str) -> None:
    sentinels, docker, inventory = _shared(module_seed)
    before = foreign_snapshot(sentinels, inventory)

    docker.resources[changed.replace("-", "_")][0]["Labels"] = {"added": "label"}

    after = foreign_snapshot(sentinels, inventory)
    assert [name for name in PROTECTED_CLASSES if after[name] != before[name]] == [changed]


@pytest.mark.parametrize("changed", DOCKER_CLASSES)
def test_foreign_snapshot_sees_a_removed_object(module_seed, changed: str) -> None:
    sentinels, docker, inventory = _shared(module_seed)
    before = foreign_snapshot(sentinels, inventory)

    docker.resources[changed.replace("-", "_")].clear()

    after = foreign_snapshot(sentinels, inventory)  # absent, not an error and not "empty"
    assert [name for name in PROTECTED_CLASSES if after[name] != before[name]] == [changed]


def test_foreign_snapshot_changes_when_a_foreign_file_changes(tmp_path: Path) -> None:
    sentinels, _docker, inventory = _seeded(tmp_path)
    assert sentinels.project is not None and sentinels.temporary_root is not None
    before = foreign_snapshot(sentinels, inventory)

    sentinels.project.record.write_bytes(sentinels.project.record.read_bytes() + b" ")
    assert [name for name in PROTECTED_CLASSES if foreign_snapshot(sentinels, inventory)[name] != before[name]] == [
        "project-record"
    ]
    sentinels.run_records[0].write_bytes(sentinels.run_records[0].read_bytes() + b" ")
    (sentinels.temporary_root / "stranger-file").write_text("added")

    after = foreign_snapshot(sentinels, inventory)
    assert [name for name in PROTECTED_CLASSES if after[name] != before[name]] == [
        "project-record",
        "run-record",
        "temporary-root",
    ]


@pytest.mark.parametrize("vanished", PROTECTED_CLASSES)
def test_a_baseline_with_an_absent_sentinel_is_refused(tmp_path: Path, module_seed, vanished: str) -> None:
    # a vanished file sentinel is deleted on disk, so it needs its own seed; a Docker class only empties its copy
    sentinels, docker, inventory = _seeded(tmp_path) if vanished in FILE_CLASSES else _shared(module_seed)
    if vanished == "project-record":
        assert sentinels.project is not None
        sentinels.project.record.unlink()
    elif vanished == "run-record":
        sentinels.run_records[0].unlink()
    elif vanished == "temporary-root":
        assert sentinels.temporary_root is not None
        shutil.rmtree(sentinels.temporary_root)
    else:
        docker.resources[vanished.replace("-", "_")].clear()

    baseline = foreign_snapshot(sentinels, inventory)  # not "empty": the identity is listed, the object is gone

    with pytest.raises(AssertionError, match="absent sentinel at baseline"):
        require_present(baseline)


def test_observed_opens_reports_every_way_of_reading_a_watched_file_and_nothing_else(tmp_path: Path) -> None:
    watched, other, link = tmp_path / "watched.json", tmp_path / "other.json", tmp_path / "link.json"
    watched.write_text("{}")
    other.write_text("{}")
    link.symlink_to(watched)

    with observed_opens([watched]) as opened:
        watched.read_bytes()
        with open(watched, encoding="utf-8"):
            pass
        os.close(os.open(watched, os.O_RDONLY))
        link.read_bytes()  # a symbolic link is the same file
        directory = os.open(tmp_path, os.O_RDONLY)
        try:
            os.close(os.open(watched.name, os.O_RDONLY, dir_fd=directory))  # descriptor-relative: only the name is seen
        finally:
            os.close(directory)
        other.read_bytes()
        len(list(os.scandir(tmp_path)))  # listing a directory opens none of its files

    assert opened == [watched] * 5
    watched.read_bytes()  # the hook is inert once the block has ended
    assert len(opened) == 5
