"""The recovery phase a run record proves complete: a pure derivation over the record."""

from __future__ import annotations

from pathlib import Path

import pytest

from docker_test_toolkit.application.lifecycle import derive_recovery_phase


def _record(root: Path, **changes: object) -> dict:
    """A resource-bearing record whose recovery has finished every phase except the published terminal write."""
    record: dict = {
        "recovery_actor": {"pid": 9},
        "recovery_outcome": "pending",
        "pending_operation": None,
        "docker_receipts": {"r1": {"state": "complete"}},
        "consumers": {"c1": {"state": "finished"}},
        "resources": {"containers": {"x": {"state": "removed"}}, "networks": {}, "volumes": {}},
        "owned_resources": {"containers": {"y": {"state": "removed"}}},
        "private_root": str(root),
        "retain": False,
    }
    record.update(changes)
    return record


@pytest.mark.parametrize(
    ("changes", "root_present", "expected"),
    [
        ({"recovery_actor": None}, False, None),  # a fresh record: no recovery write yet
        ({"pending_operation": {"kind": "cleanup"}}, True, "claim"),
        ({"docker_receipts": {"r1": {"state": "pending"}}}, True, "operation"),
        ({"docker_receipts": {"r1": {"state": "unresolved"}}}, True, "operation"),
        ({"consumers": {"c1": {"state": "active"}}}, True, "receipts"),
        ({"owned_resources": {"containers": {"y": {"state": "present"}}}}, True, "consumers"),
        (
            {"resources": {"containers": {"x": {"state": "present"}}, "networks": {}, "volumes": {}}},
            True,
            "consumers",
        ),
        ({}, True, "resources"),  # every resource removed, the private root still present
        ({}, False, "private-root"),  # the private root removed, the terminal record not yet written
        ({"recovery_outcome": "complete"}, False, "publish"),
    ],
    ids=[
        "no-recovery-actor",
        "pending-operation",
        "open-receipt",
        "unresolved-receipt",
        "active-consumer",
        "owned-resource-present",
        "compose-resource-present",
        "private-root-present",
        "private-root-removed",
        "terminal-written",
    ],
)
def test_derive_recovery_phase_matrix(tmp_path: Path, changes: dict, root_present: bool, expected: str | None) -> None:
    root = tmp_path / "private"
    if root_present:
        root.mkdir()

    assert derive_recovery_phase(_record(root, **changes)) == expected
