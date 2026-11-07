"""Pure contracts of project retirement: cell identity, quiescence clauses and the Q-2 predicate."""

from __future__ import annotations

import copy
from pathlib import Path

import pytest

from docker_test_toolkit.application import lifecycle
from docker_test_toolkit.application.state import archive_directory

_QUIESCENT = {
    "state": "complete",
    "cleanup_outcome": "complete",
    "recovery_outcome": "complete",
    "quiescent": True,
    "pending_operation": None,
    "docker_receipts": {"r": {"state": "removed"}},
    "consumers": {"c": {"state": "finished"}},
    "resources": {"containers": {"x": {"state": "removed"}}, "networks": {}, "volumes": {}},
    "owned_resources": {"containers": {"y": {"state": "removed"}}},
    "endpoints": {"cache": [{"active": False}]},
}
_ROWS = [
    ({}, ()),
    ({"cleanup_outcome": "incomplete"}, ()),
    ({"state": "prepared"}, ("run-nonterminal",)),
    ({"cleanup_outcome": "pending"}, ("run-cleanup-pending",)),
    ({"state": "stopped-retained"}, ("run-retained",)),
    ({"recovery_outcome": None}, ("run-recovery-incomplete",)),
    ({"quiescent": False}, ("run-not-quiescent",)),
    ({"pending_operation": {"kind": "compose"}}, ("run-operation-pending",)),
    ({"docker_receipts": {"r": {"state": "pending"}}}, ("run-receipts-open",)),
    ({"docker_receipts": {"r": {"state": "unresolved"}}}, ("run-receipts-open",)),
    ({"consumers": {"c": {"state": "active"}}}, ("run-consumers-active",)),
    (
        {"resources": {"containers": {}, "networks": {"n": {"state": "present"}}, "volumes": {}}},
        ("run-resources-present",),
    ),
    ({"owned_resources": {"containers": {"y": {"state": "stopped"}}}}, ("run-owned-docker-present",)),
    ({"endpoints": {"cache": [{"active": True}]}}, ("run-endpoints-active",)),
    ({"state": "prepared", "cleanup_outcome": "pending"}, ("run-nonterminal", "run-cleanup-pending")),
]


def _former_conjunction(row: dict) -> bool:
    """The pre-slice `completed_run_is_quiescent`, restated literally and independently of the production code."""
    return (
        row["state"] == "complete"
        and row["cleanup_outcome"] in {"complete", "incomplete"}
        and row.get("recovery_outcome") == "complete"
        and row.get("quiescent") is True
        and row["pending_operation"] is None
        and all(receipt["state"] not in {"pending", "unresolved"} for receipt in row["docker_receipts"].values())
        and all(consumer["state"] == "finished" for consumer in row["consumers"].values())
        and (
            row["resources"] == {}
            or all(
                item["state"] == "removed"
                for kind in ("containers", "networks", "volumes")
                for item in row["resources"][kind].values()
            )
        )
        and all(item["state"] == "removed" for group in row["owned_resources"].values() for item in group.values())
        and all(not item["active"] for history in row["endpoints"].values() for item in history)
    )


@pytest.mark.parametrize(("change", "expected"), _ROWS)
def test_quiescence_blockers_match_an_independent_clause_table(change: dict, expected: tuple[str, ...]) -> None:
    row = {**copy.deepcopy(_QUIESCENT), **change}

    assert lifecycle.quiescence_blockers(row) == expected
    assert lifecycle.completed_run_is_quiescent(row) is _former_conjunction(row) is (expected == ())


def test_archive_sits_beside_the_registry_under_a_fixed_name() -> None:
    """Retirement writes and the validator reads the same path; both derive it from this one function."""
    assert archive_directory(Path("/x/registry")) == Path("/x/registry-archive")


def test_retire_dry_run_on_a_fresh_account_lists_nothing(tmp_path: Path) -> None:
    from docker_test_toolkit.application.retirement import retire_projects

    registry = tmp_path / "registry"
    account = registry / "test-admission"
    account.mkdir(parents=True)
    registry.chmod(0o700)
    account.chmod(0o700)

    result = retire_projects(registry, account, {}, execute=False)

    assert result["eligible"] == [] and result["blocked"] == []
