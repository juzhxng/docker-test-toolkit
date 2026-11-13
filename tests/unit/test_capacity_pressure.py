"""Pressure accounting: the run reservation and the pressure decisions."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from docker_test_toolkit.application import lifecycle, sharing
from docker_test_toolkit.application.lifecycle import CapacitySnapshot, CapacityState
from docker_test_toolkit.domain import policy
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
        # a runner container inside Desktop's VM whose account declares it: native Docker Engine
        ("native", _DESKTOP_INFO, "linux", "unix:///var/run/docker.sock", "docker-engine"),
    ],
)
def test_the_observed_runtime_matches_the_declared_topology(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    topology: str,
    info: dict,
    client_platform: str,
    endpoint: str,
    expected: str | None,
) -> None:
    monkeypatch.setattr(lifecycle.sys, "platform", client_platform)
    monkeypatch.setattr(lifecycle, "passwd_home", lambda: Path("/Users/x"))
    account = tmp_path / "account"
    account.mkdir(mode=0o700)
    sharing.record_desktop_vm_client(account, True)  # it changes nothing but the Desktop-from-Linux row
    monkeypatch.setattr(lifecycle, "canonical_account", lambda: account)

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


@pytest.mark.parametrize(
    ("daemon", "cpus", "external", "budget", "expected"),
    [
        (2.25, 8, (), _budget(container=2), (CapacityState.REFUSED, "unsupported-base-capacity")),
        (3, 8, (GIB,), _budget(container=2), (CapacityState.WAITING, "external-capacity")),
        (3, 8, (None,), _budget(container=2), (CapacityState.WAITING, "unknown-external-capacity")),
        (2.25, 8, (None,), _budget(container=2), (CapacityState.REFUSED, "unsupported-base-capacity")),
        (3, 1, (None,), _budget(cpus=2), (CapacityState.REFUSED, "insufficient-cpu")),
    ],
)
def test_a_run_that_can_never_fit_is_refused_and_one_that_fits_waits_on_foreign_containers(
    daemon: float, cpus: int, external: tuple[int | None, ...], budget: PhaseBudget, expected: tuple
) -> None:
    """Native: a 2 GiB container needs 2.5 GiB of daemon memory. Foreign containers, limited or not, only make a run
    that fits alone wait; a run that can never fit is refused whatever else runs."""
    snapshot = CapacitySnapshot(int(daemon * GIB), cpus, 64 * GIB, 8, "native", external, runtime="docker-engine")

    decision = lifecycle.assess_capacity(budget, snapshot)

    assert (decision.state, decision.reason) == expected


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


def _running(identifier: str, memory: int, labels: dict[str, str] | None = None, *, running: bool = True) -> dict:
    return {
        "Id": identifier * 64,
        "State": {"Running": running},
        "HostConfig": {"Memory": memory},
        "Config": {"Labels": labels or {}},
    }


RUN_HOST = {"com.docker-test-toolkit.run-host": "true"}


@pytest.mark.parametrize(
    ("container", "expected"),
    [
        (_running("a", 12 * GIB, RUN_HOST), ((), 1)),  # a CI runner hosting a run of another account
        (_running("a", 0, RUN_HOST), ((), 1)),  # without a limit too
        (_running("a", 12 * GIB, {"com.docker-test-toolkit.run-host": "1"}), ((12 * GIB,), 0)),
        (_running("a", 12 * GIB, RUN_HOST, running=False), ((), 0)),
        (_running("a", GIB), ((GIB,), 0)),
    ],
)
def test_a_run_host_container_is_not_an_external_commitment(container: dict, expected: tuple) -> None:
    """A container the operator labels as the host of a run (a CI runner) is left out of every run's external sum."""
    commitments = policy.external_commitments([container])

    assert (commitments.limits, commitments.run_hosts) == expected
