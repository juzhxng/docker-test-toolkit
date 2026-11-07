"""The pure disk admission rules: reserve, estimate and fit per place."""

from __future__ import annotations

import pytest

from docker_test_toolkit.domain.policy import (
    DISK_FLOOR_BYTES,
    CapacityState,
    DiskFigure,
    derive_reserve,
    disk_estimate,
    disk_fit,
    private_root_fit,
)

GIB = 1024**3
MIB = 1024**2
CI_TOTAL = 193_000_000_000  # the CI host's disk
MAC_TOTAL = 482_797_652 * 1024  # this Mac's data volume, `df -k`


def _figure(free: int, total: int, *, declared: int | None = None, state: str = "measured") -> DiskFigure:
    return DiskFigure("data-root", free, total, derive_reserve(total, declared), state)


def test_reserve_defaults_from_the_total_and_is_clamped() -> None:
    assert derive_reserve(MAC_TOTAL, None) == 8 * GIB  # 1/32 is above the ceiling
    assert derive_reserve(CI_TOTAL, None) == CI_TOTAL // 32  # 6.03 GB, inside the clamp
    assert derive_reserve(GIB, None) == 512 * MIB  # never more than half the place
    assert derive_reserve(10 * GIB, 20 * GIB) == 5 * GIB  # a declared reserve is clamped to half too
    assert derive_reserve(100 * GIB, 3 * GIB) == 3 * GIB


def test_the_mac_and_ci_host_numeric_checks() -> None:
    floor, _source = disk_estimate(None)

    assert disk_fit(_figure(int(20.1 * GIB), MAC_TOTAL), 0, floor).state is CapacityState.READY
    waiting = disk_fit(_figure(8_300_000_000, CI_TOTAL), 0, floor)  # a state that ended in ENOSPC
    assert (waiting.state, waiting.reason) == (CapacityState.WAITING, "disk-capacity")
    assert disk_fit(_figure(10_400_000_000, CI_TOTAL), 0, floor).state is CapacityState.READY


def test_never_fit_is_refused_naming_bytes() -> None:
    decision = disk_fit(_figure(50 * GIB, 50 * GIB), 0, 60 * GIB)

    assert (decision.state, decision.reason) == (CapacityState.REFUSED, "disk-capacity")
    assert decision.detail is not None
    for text in ("data-root", f"free {50 * GIB}", f"total {50 * GIB}", f"reserve {2 * GIB}", f"estimate {60 * GIB}"):
        assert text in decision.detail


def test_other_leases_are_subtracted() -> None:
    figure = _figure(10 * GIB, 100 * GIB)  # reserve 3.125 GiB: 6.875 GiB beside no lease, 2.875 GiB beside one

    assert disk_fit(figure, 0, DISK_FLOOR_BYTES).state is CapacityState.READY
    assert disk_fit(figure, DISK_FLOOR_BYTES, DISK_FLOOR_BYTES).state is CapacityState.WAITING
    exact = 10 * GIB - derive_reserve(100 * GIB, None) - DISK_FLOOR_BYTES  # room for exactly one more estimate
    assert disk_fit(figure, DISK_FLOOR_BYTES, exact).state is CapacityState.READY


def test_unknown_is_ready() -> None:
    decision = disk_fit(DiskFigure("data-root", None, None, None, "unknown"), 10 * GIB, 60 * GIB)

    assert (decision.state, decision.reason) == (CapacityState.READY, "disk-unknown")


def test_private_root_floor() -> None:
    assert private_root_fit(GIB).state is CapacityState.READY
    waiting = private_root_fit(GIB - 1)
    assert (waiting.state, waiting.reason) == (CapacityState.WAITING, "disk-capacity")
    assert waiting.detail is not None and "private-root" in waiting.detail


def test_estimate_is_derived_with_a_quarter_margin_else_the_floor() -> None:
    assert disk_estimate([GIB, 2 * GIB]) == (3 * GIB + 3 * GIB // 4, "derived")
    assert disk_estimate([]) == (DISK_FLOOR_BYTES, "floor")
    assert disk_estimate(None) == (4 * GIB, "floor")


def test_a_refusal_at_any_place_wins_over_a_wait_at_another() -> None:
    from docker_test_toolkit.application.lifecycle import DiskInputs, _disk_decision

    waiting = DiskFigure("data-root", 5 * GIB, 100 * GIB, None, "measured")  # waits: 5 - 3.125 < 60
    never = DiskFigure("vm-disk-host", 50 * GIB, 50 * GIB, None, "measured")  # 60 > 50 - 2: can never fit

    decided = _disk_decision(DiskInputs((waiting, never), None, 60 * GIB, "derived"), 0, None)

    assert (decided.state, decided.reason) == (CapacityState.REFUSED, "disk-capacity")
    assert decided.detail is not None and decided.detail.startswith("vm-disk-host")


def test_the_declared_reserve_is_applied_at_the_decision() -> None:
    from docker_test_toolkit.application.lifecycle import DiskInputs, _disk_decision

    place = DiskFigure("data-root", 20 * GIB, 100 * GIB, None, "measured")
    disk = DiskInputs((place,), None, DISK_FLOOR_BYTES, "floor")

    assert _disk_decision(disk, 0, None).state is CapacityState.READY  # 20 - 3.125 >= 4
    assert _disk_decision(disk, 0, 18 * GIB).state is CapacityState.WAITING  # 20 - 18 < 4


def test_the_vm_disk_host_directory_follows_the_runtime() -> None:
    from pathlib import Path

    from docker_test_toolkit.domain.policy import vm_disk_host_directory

    home = Path("/Users/x")

    assert vm_disk_host_directory("colima", "colima", home) == home / ".colima/_lima/colima"
    assert vm_disk_host_directory("colima", "colima-work", home) == home / ".colima/_lima/colima-work"
    assert vm_disk_host_directory("colima", "../escape", home) is None  # never a folder the name did not make
    assert vm_disk_host_directory("docker-engine", "ci", home) is None  # no VM: no VM disk place
    assert vm_disk_host_directory(None, None, home) is None


def test_a_grant_decides_only_on_disk_figures_of_this_boot_at_most_ten_seconds_old() -> None:
    from docker_test_toolkit.application import lifecycle

    class _Clock:
        origin = "boot-1"

        def read(self) -> int:
            return 100_000_000_000

    def read_at(read_ns: int | None, origin: str | None = "boot-1") -> lifecycle.DiskInputs:
        return lifecycle.DiskInputs((), None, 1, "floor", None, read_ns, origin)

    clock = _Clock()

    assert lifecycle.disk_figures_fresh(read_at(90_000_000_000), clock)  # exactly 10 s old
    assert not lifecycle.disk_figures_fresh(read_at(89_999_999_999), clock)  # sampled before a long lock wait
    assert not lifecycle.disk_figures_fresh(read_at(95_000_000_000, "boot-0"), clock)  # another boot
    assert not lifecycle.disk_figures_fresh(read_at(None, None), clock)  # of unknown time


@pytest.mark.parametrize(
    ("endpoint", "probed"),
    [
        ("tcp://127.0.0.1:2375", True),  # the CI host's runners reach their own daemon over the loopback
        ("tcp://localhost:2375", True),
        ("tcp://[::1]:2375", True),
        ("tcp://10.0.0.5:2375", False),  # another machine's daemon: its disk stays unknown
        ("ssh://ci@build-box", False),
    ],
)
def test_a_remote_daemon_on_the_loopback_has_its_data_root_probed(
    endpoint: str, probed: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    from types import SimpleNamespace

    from docker_test_toolkit.application import lifecycle
    from docker_test_toolkit.domain.policy import DiskFigure

    figure = DiskFigure("data-root", 40 * GIB, 100 * GIB, None, "measured")
    calls: list[str] = []
    monkeypatch.setattr(lifecycle, "_daemon_endpoint", lambda _docker, _daemon_id, _deadline: endpoint)
    monkeypatch.setattr(
        lifecycle,
        "_probe_reading",
        lambda *_args: calls.append("probe") or SimpleNamespace(data_root=figure),
    )

    measured = lifecycle._data_root_figure({"topology": "remote", "account_directory": "/a"}, object(), "d1")

    assert (measured.state == "measured") is probed and (calls == ["probe"]) is probed


@pytest.mark.parametrize(
    ("endpoint", "loopback"),
    [
        ("tcp://127.0.0.1:2375", True),
        ("tcp://127.8.9.10:2375", True),
        ("tcp://localhost", True),
        ("tcp://[::1]:2375", True),
        ("tcp://127.build.example.com:2375", False),  # a DNS name that only starts like a loopback address
        ("tcp://[::1", False),  # malformed: never a loopback, never an exception
        ("tcp://10.0.0.5:2375", False),
        ("unix:///var/run/docker.sock", False),
    ],
)
def test_only_a_loopback_address_or_localhost_is_this_machines_tcp_daemon(endpoint: str, loopback: bool) -> None:
    from docker_test_toolkit.application import lifecycle

    assert lifecycle.loopback_endpoint(endpoint) is loopback
