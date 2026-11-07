"""The pure retention rules: caps derived from the filesystem, oldest-first eviction over the cap."""

from __future__ import annotations

from docker_test_toolkit.domain.policy import (
    RetentionCandidate,
    archive_cap_bytes,
    cache_cap_bytes,
    eviction_plan,
)

GIB = 1024**3
MIB = 1024**2


def test_caps_are_clamped_fractions_of_the_filesystem_total() -> None:
    # The CI host's 193 GB disk, this Mac's 460 GiB data volume and a 4 TiB server: no machine size, only clamps.
    assert cache_cap_bytes(193_000_000_000) == 4 * GIB  # 1/64 is below the floor
    assert cache_cap_bytes(460 * GIB) == 460 * GIB // 64  # 7.19 GiB
    assert cache_cap_bytes(4096 * GIB) == 32 * GIB  # the ceiling
    assert archive_cap_bytes(193_000_000_000) == 193_000_000_000 // 1024
    assert archive_cap_bytes(16 * GIB) == 64 * MIB
    assert archive_cap_bytes(4096 * GIB) == GIB


def _candidate(name: str, gib: float, wall: int, *, protected: bool = False) -> RetentionCandidate:
    return RetentionCandidate(name, int(gib * GIB), wall, protected)


def test_eviction_is_oldest_first_and_stops_at_the_cap() -> None:
    candidates = [_candidate("c", 3, 30), _candidate("a", 3, 10), _candidate("b", 3, 20)]

    plan = eviction_plan(candidates, 7 * GIB)  # 9 GiB stored

    assert plan == ("a",)
    assert eviction_plan(candidates, 9 * GIB) == ()  # at the cap nothing goes


def test_an_orphaned_projects_newest_generation_outlives_older_ones() -> None:
    # A CI checkout whose folder is briefly absent keeps its warm cache while it is not the oldest:
    # eviction reads no project status at all, only age.
    candidates = [_candidate("g1", 3, 10), _candidate("g2", 3, 20), _candidate("orphan", 3, 30)]

    assert eviction_plan(candidates, int(7.2 * GIB)) == ("g1",)


def test_protected_excess_is_reported_not_evicted() -> None:
    candidates = [_candidate("live", 8, 10, protected=True), _candidate("old", 1, 5)]

    plan = eviction_plan(candidates, 4 * GIB)

    assert plan == ("old",)  # the only unprotected one, though 8 GiB protected alone exceed the 4 GiB cap
