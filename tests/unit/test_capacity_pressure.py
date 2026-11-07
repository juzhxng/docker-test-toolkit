"""Pressure accounting: the run reservation and the pressure decisions."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from docker_test_toolkit.application import lifecycle
from docker_test_toolkit.application.lifecycle import CapacitySnapshot, CapacityState
from docker_test_toolkit.domain.models import PhaseBudget
from docker_test_toolkit.domain.policy import (
    PhaseReservation,
    reservation,
)

GIB = 1024**3


def _budget(container: int = 1, host: int = 1, cpus: int = 1, threads: int = 1) -> PhaseBudget:
    return PhaseBudget(container * GIB, host * GIB, cpus, threads)


def _snapshot(
    *, daemon: int = 64, host: int = 64, cpus: int = 8, host_cpus: int = 8, external: tuple[int | None, ...] = ()
) -> CapacitySnapshot:
    return CapacitySnapshot(daemon * GIB, cpus, host * GIB, host_cpus, "native", external, runtime="docker-engine")


def _reservation(container: int = 1, host: int = 1, cpus: int = 1, threads: int = 1) -> PhaseReservation:
    return PhaseReservation(container * GIB, host * GIB, cpus, threads)


def test_reservation_is_field_wise_max() -> None:
    phases = [_budget(4, 1, 1, 8), _budget(2, 6, 3, 2)]

    assert reservation(phases) == PhaseReservation(4 * GIB, 6 * GIB, 3, 8)


def test_new_failure_strings_are_exact() -> None:
    from docker_test_toolkit.domain.policy import NAME

    assert lifecycle.UNKNOWN_EXTERNAL_CAPACITY == "unknown-external-capacity"
    assert NAME.fullmatch(lifecycle.UNKNOWN_EXTERNAL_CAPACITY)


def _host_sample(
    seconds: int, *, swapout: int = 0, swap_used: int = 0, swap_total: int = 8 * GIB, headroom: int = 8 * GIB
) -> lifecycle.HostMemoryObservation:
    return lifecycle.HostMemoryObservation(
        "boot",
        seconds * 1_000_000_000,
        16 * GIB,
        headroom,
        swap_used,
        swapout,
        4096,
        {"swap_total_bytes": swap_total},
    )


def _granted_pressure(current, window, *, admission=None, phase=None) -> lifecycle.CapacityDecision:
    return lifecycle.assess_pressure(
        current,
        window,
        granted=True,
        admission_baseline=admission or window,
        phase_baseline=phase or window,
        host_rss_bytes=0,
        host_envelope_bytes=GIB,
    )


def test_swap_total_change_alone_is_not_a_breach() -> None:
    """A swap-file resize moves the swap total, not the usage the decisions read."""
    window = _host_sample(0, swap_used=2 * GIB, swap_total=6 * GIB)
    current = _host_sample(40, swap_used=2 * GIB, swap_total=8 * GIB)

    assert _granted_pressure(current, window).state is CapacityState.READY


def test_swapout_burst_is_diluted_by_a_long_window() -> None:
    """Documents the limit: the rate is an average over the window, so a long window hides a burst."""
    burst = 5 * GIB
    short_window = _host_sample(0)
    long_window = _host_sample(0)
    short_current = _host_sample(30, swapout=burst)
    long_current = _host_sample(120, swapout=burst)

    assert _granted_pressure(short_current, short_window).reason == "swapout-rate"
    assert _granted_pressure(long_current, long_window).state is CapacityState.READY


_DESKTOP_INFO = {"Name": "docker-desktop", "OperatingSystem": "Docker Desktop"}
_ENGINE_INFO = {"Name": "ci", "OperatingSystem": "Ubuntu 24.04.3 LTS"}
_COLIMA_INFO = {"Name": "colima", "OperatingSystem": "Ubuntu 24.04.2 LTS"}


@pytest.mark.parametrize(
    ("topology", "info", "client_platform", "endpoint", "expected"),
    [
        ("native", _ENGINE_INFO, "linux", "unix:///var/run/docker.sock", "docker-engine"),
        ("desktop", _DESKTOP_INFO, "darwin", "unix:///Users/x/.docker/run/docker.sock", "docker-desktop"),
        ("desktop", _DESKTOP_INFO, "darwin", "tcp://10.0.0.5:2376", None),  # remote, whatever was declared
        ("remote", _ENGINE_INFO, "linux", "unix:///var/run/docker.sock", "docker-engine"),
        ("desktop", _COLIMA_INFO, "darwin", "unix:///Users/x/.colima/default/docker.sock", "colima"),
    ],
)
def test_the_observed_runtime_matches_the_declared_topology(
    monkeypatch: pytest.MonkeyPatch,
    topology: str,
    info: dict,
    client_platform: str,
    endpoint: str,
    expected: str | None,
) -> None:
    monkeypatch.setattr(lifecycle.sys, "platform", client_platform)
    monkeypatch.setattr(lifecycle, "passwd_home", lambda: Path("/Users/x"))

    assert lifecycle._observed_runtime(info, endpoint, topology) == expected


@pytest.mark.parametrize(
    ("topology", "info", "client_platform", "endpoint", "message", "reason"),
    [
        (
            "native",
            _DESKTOP_INFO,
            "linux",
            "unix:///var/run/docker.sock",
            "Docker Desktop daemon seen from a Linux client; use Docker Engine in the WSL2 distribution.",
            "unsupported-runtime",
        ),
        (
            "desktop",
            _ENGINE_INFO,
            "linux",
            "unix:///var/run/docker.sock",
            "Observed daemon does not match the declared profile topology.",
            "unspecified",
        ),
    ],
)
def test_an_unsupported_mismatched_or_unqualified_runtime_is_refused(
    monkeypatch: pytest.MonkeyPatch,
    topology: str,
    info: dict,
    client_platform: str,
    endpoint: str,
    message: str,
    reason: str,
) -> None:
    from docker_test_toolkit.application.lifecycle import ResourceError

    monkeypatch.setattr(lifecycle.sys, "platform", client_platform)
    monkeypatch.setattr(lifecycle, "passwd_home", lambda: Path("/Users/x"))

    with pytest.raises(ResourceError) as refused:
        lifecycle._observed_runtime(info, endpoint, topology)

    assert (str(refused.value), refused.value.reason) == (message, reason)


def test_base_capacity_boundary_refuses_a_daemon_that_holds_only_the_container() -> None:
    """Native, no job limit: a 2 GiB container needs 2.5 GiB of daemon memory, so 2.25 GiB cannot ever admit it."""
    snapshot = CapacitySnapshot(int(2.25 * GIB), 8, 64 * GIB, 8, "native", (), runtime="docker-engine")

    decision = lifecycle.assess_capacity(_budget(container=2), snapshot)

    assert (decision.state, decision.reason) == (CapacityState.REFUSED, "unsupported-base-capacity")


def test_external_capacity_boundary_waits_instead_of_refusing() -> None:
    """The same budget fits a 3 GiB daemon alone; one foreign 1 GiB limit makes it wait, not refuse."""
    snapshot = CapacitySnapshot(3 * GIB, 8, 64 * GIB, 8, "native", (GIB,), runtime="docker-engine")

    decision = lifecycle.assess_capacity(_budget(container=2), snapshot)

    assert (decision.state, decision.reason) == (CapacityState.WAITING, "external-capacity")


def test_observe_capacity_refuses_a_docker_desktop_daemon_seen_from_a_linux_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The WSL2 refusal is wired into the capacity observation, not only into the pure rule."""
    from docker_test_toolkit.application.lifecycle import EngineReply, ResourceError
    from docker_test_toolkit.factories.testing import bind_test_observation_reads

    def engine(_docker: object, _path: str, _deadline: object) -> EngineReply:
        body = {
            "ID": "11111111-1111-1111-1111-111111111111",
            "MemTotal": 8 * GIB,
            "NCPU": 4,
            "OSType": "linux",
            "CgroupVersion": "2",
            "OperatingSystem": "Docker Desktop",
        }
        return EngineReply(body, b"", 0)

    monkeypatch.setattr(lifecycle.sys, "platform", "linux")
    with bind_test_observation_reads(engine=engine), pytest.raises(ResourceError, match="Docker Engine in the WSL2"):
        lifecycle.observe_capacity(SimpleNamespace(engine="docker", binding=None), "native", inventory=[])


def test_a_recognised_runtime_without_measured_overheads_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    from docker_test_toolkit.application.lifecycle import ResourceError
    from docker_test_toolkit.domain.policy import RUNTIME_OVERHEADS

    monkeypatch.setattr(lifecycle.sys, "platform", "darwin")
    monkeypatch.setattr(lifecycle, "passwd_home", lambda: Path("/Users/x"))
    monkeypatch.delitem(RUNTIME_OVERHEADS, "colima")  # as before the qualification window

    with pytest.raises(ResourceError) as refused:
        lifecycle._observed_runtime(_COLIMA_INFO, "unix:///Users/x/.colima/default/docker.sock", "desktop")

    assert refused.value.reason == "runtime-unqualified"


def test_one_daemons_endpoint_is_read_once_per_process(monkeypatch: pytest.MonkeyPatch) -> None:
    reads: list[str] = []

    def endpoint(_docker: object, _deadline: object) -> str:
        reads.append("context inspect")
        return "unix:///Users/x/.docker/run/docker.sock"

    monkeypatch.setattr(lifecycle, "_DAEMON_ENDPOINTS", {})
    monkeypatch.setattr(lifecycle, "_endpoint_read", endpoint)

    first = lifecycle._daemon_endpoint(object(), "daemon-1", None)
    again = lifecycle._daemon_endpoint(object(), "daemon-1", None)  # every capacity observation of a waiting run
    other = lifecycle._daemon_endpoint(object(), "daemon-2", None)

    assert first == again == other and reads == ["context inspect", "context inspect"]
