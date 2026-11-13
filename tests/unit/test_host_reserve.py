"""Operator-set host reserve for single-purpose CI hosts."""

from __future__ import annotations

from pathlib import Path

import pytest

from docker_test_toolkit.application import lifecycle, sharing
from docker_test_toolkit.application.lifecycle import CapacitySnapshot, CapacityState, PhaseBudget
from docker_test_toolkit.application.recovery import atomic_json
from docker_test_toolkit.domain.errors import ResourceError

GIB, MIB = 1024**3, 1024**2
CI_RUNNER = CapacitySnapshot(16 * GIB, 2, int(7.5 * GIB), 2, "native", (), runtime="docker-engine")
HEAVY_TESTS = PhaseBudget(256 * MIB, 5 * GIB, 1, 1)


def test_default_reserve_refuses_a_small_ci_host() -> None:
    decision = lifecycle.assess_capacity(HEAVY_TESTS, CI_RUNNER)

    assert (decision.state, decision.reason) == (CapacityState.REFUSED, "unsupported-base-capacity")
    assert CI_RUNNER.host_reserve_bytes == 9 * GIB // 2  # unchanged default for developer machines


def test_account_reserve_defaults_and_reads_the_declaration(tmp_path: Path) -> None:
    account = tmp_path / "account"
    account.mkdir(mode=0o700)
    assert sharing.read_host_reserve(account) == 9 * GIB // 2

    sharing.record_host_reserve(account, 2 * GIB)

    assert sharing.read_host_reserve(account) == 2 * GIB
    assert (account / "host-reserve.json").stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize(
    "document",
    [
        {"schema": 1, "host_reserve_bytes": GIB - 1},
        {"schema": 1, "host_reserve_bytes": True},
        {"schema": 1, "host_reserve_bytes": "1073741824"},
        {"schema": 2, "host_reserve_bytes": GIB},
        {"schema": 1, "host_reserve_bytes": GIB, "extra": 1},
    ],
    ids=["below-floor", "bool", "string", "schema-2", "extra-key"],
)
def test_invalid_account_reserve_fails_closed(tmp_path: Path, document: dict) -> None:
    account = tmp_path / "account"
    account.mkdir(mode=0o700)
    atomic_json(account / "host-reserve.json", document)

    with pytest.raises(ResourceError, match="host reserve"):
        sharing.read_host_reserve(account)


def test_a_later_record_replaces_the_reserve_and_the_floor_holds(tmp_path: Path) -> None:
    account = tmp_path / "account"
    account.mkdir(mode=0o700)
    sharing.record_host_reserve(account, 4 * GIB)

    sharing.record_host_reserve(account, GIB)  # the operator's setting: a later install replaces it
    with pytest.raises(ResourceError, match="1 GiB"):
        sharing.record_host_reserve(account, GIB - 1)

    assert sharing.read_host_reserve(account) == GIB
