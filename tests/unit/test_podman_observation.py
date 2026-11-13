"""Capacity and container telemetry on a Podman-bound adapter.

The engine reads are bound through ``bind_test_observation_reads``; the Podman port fake records the order of its
verification and the engine reads in one list.
"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from docker_test_toolkit.application import lifecycle
from docker_test_toolkit.application.lifecycle import Deadline, EngineReply, NativeClock
from docker_test_toolkit.domain.errors import InfrastructureFailure, ResourceError
from docker_test_toolkit.domain.models import DaemonIdentity, MachineFacts, PodmanBinding
from docker_test_toolkit.factories.testing import bind_test_observation_reads
from tests.unit.test_docker_receipt_service import FakeDockerPort

GIB = 1024**3
CONTAINER = "c" * 64
SOCKET = "/var/folders/nd/x/T/podman/dtt-w0-api.sock"
MACHINE = MachineFacts("dtt-w0", "2026-11-04T10:43:17.200188+01:00", False, SOCKET, 3072, 2)
BINDING = PodmanBinding("podman-" + "a" * 40, f"unix://{SOCKET}", MACHINE, Path("/Users/x/applehv"))
INFO = {"ID": "random-1", "MemTotal": 8 * GIB, "NCPU": 4, "OSType": "linux", "CgroupVersion": "2", "Name": "x"}


class PodmanFake(FakeDockerPort):
    """A port bound to a Podman machine; ``observe_daemon`` records its call beside the engine reads."""

    engine = "podman"
    binding = BINDING

    def __init__(self, calls: list[str], *, verified: str | Exception = BINDING.identity) -> None:
        super().__init__()
        self.calls = calls
        self.verified = verified

    def observe_daemon(self, deadline: Deadline | None = None) -> DaemonIdentity:
        self.calls.append("observe_daemon")
        if isinstance(self.verified, Exception):
            raise self.verified
        return replace(self.identity, daemon_id=self.verified)


def _engine(calls: list[str], *, stats: dict, hard_limit: int = 256 * 1024**2):
    inspected = {
        "Id": CONTAINER,
        "State": {"StartedAt": "2026-11-04T10:00:00Z", "Running": True, "OOMKilled": False},
        "HostConfig": {"Memory": hard_limit},
        "RestartCount": 0,
    }

    def read(_docker: object, path: str, _deadline: object) -> EngineReply:
        calls.append(f"engine {path.split('?')[0]}")
        if path == "/info":
            return EngineReply(dict(INFO), b"", 0)
        if path.endswith("/json"):
            return EngineReply(inspected, b"", 0)
        return EngineReply({"memory_stats": stats}, b"", 0)

    return read


def _host(clock: NativeClock, _deadline: object) -> lifecycle.HostMemoryObservation:
    return lifecycle.HostMemoryObservation(clock.origin, clock.read(), 64 * GIB, 32 * GIB, 0, 0, 4096, {})


def test_capacity_on_podman_verifies_the_binding_before_the_engine_read_and_never_uses_the_body_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    del monkeypatch  # the binding decides the runtime; the client platform is never read for it

    with bind_test_observation_reads(engine=_engine(calls, stats={}), host=_host):
        daemon_id, snapshot = lifecycle.observe_capacity(
            PodmanFake(calls), "desktop", inventory=[], deadline=Deadline.after(NativeClock(), 5)
        )

    assert daemon_id == BINDING.identity and snapshot.runtime == "podman"
    assert calls == ["observe_daemon", "engine /info"]


def test_capacity_on_docker_still_returns_the_body_id(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    # The Docker Desktop sample below is a macOS daemon: only its classification sees a macOS client (switching
    # sys.platform would make the native clock load macOS libraries on a Linux CI host).
    classify = lifecycle.detect_runtime
    monkeypatch.setattr(
        lifecycle,
        "detect_runtime",
        lambda info, _platform, endpoint, home, **declared: classify(info, "darwin", endpoint, home, **declared),
    )
    desktop_info = {**INFO, "OperatingSystem": "Docker Desktop"}

    def engine(_docker: object, path: str, _deadline: object) -> EngineReply:
        calls.append(f"engine {path}")
        return EngineReply(dict(desktop_info), b"", 0)

    with bind_test_observation_reads(engine=engine, host=_host):
        daemon_id, _snapshot = lifecycle.observe_capacity(FakeDockerPort(), "desktop", inventory=[])

    assert daemon_id == "random-1" and calls == ["engine /info"]


@pytest.mark.parametrize(
    ("stats", "hard_limit", "recorded"),
    [
        ({"usage": 1, "limit": 256 * 1024**2}, 256 * 1024**2, 256 * 1024**2),  # no counter map: oom_events 0
        ({"usage": 1, "limit": 8288989184}, 16 * GIB, 16 * GIB),  # the VM's memory for a larger hard limit
    ],
)
def test_container_telemetry_on_podman_accepts_the_measured_stats_shape_and_docker_still_refuses_it(
    stats: dict, hard_limit: int, recorded: int
) -> None:
    calls: list[str] = []

    with bind_test_observation_reads(engine=_engine(calls, stats=stats, hard_limit=hard_limit)):
        observed = lifecycle.observe_container(PodmanFake(calls), BINDING.identity, CONTAINER)
        with pytest.raises(ResourceError) as docker_refused:
            lifecycle.observe_container(FakeDockerPort(), "random-1", CONTAINER)

    assert (observed.memory_limit_bytes, observed.oom_events, observed.daemon_id) == (recorded, 0, BINDING.identity)
    assert calls[0] == "observe_daemon"
    assert docker_refused.value.reason == "telemetry-unreadable"


@pytest.mark.parametrize("limit", [0, 256 * 1024**2 + 1])
def test_container_telemetry_on_podman_fails_closed_outside_the_limit_rule(limit: int) -> None:
    calls: list[str] = []

    reads = bind_test_observation_reads(engine=_engine(calls, stats={"usage": 1, "limit": limit}))
    with reads, pytest.raises(ResourceError) as refused:
        lifecycle.observe_container(PodmanFake(calls), BINDING.identity, CONTAINER)

    assert refused.value.reason == "telemetry-unreadable"


def test_container_telemetry_on_podman_compares_the_binding_identity() -> None:
    calls: list[str] = []
    stats = {"usage": 1, "limit": 256 * 1024**2}

    with bind_test_observation_reads(engine=_engine(calls, stats=stats)):
        with pytest.raises(ResourceError) as changed:
            lifecycle.observe_container(PodmanFake(calls, verified="podman-" + "b" * 40), BINDING.identity, CONTAINER)
        stopped = PodmanFake(calls, verified=InfrastructureFailure("Machine stopped.", reason="client-exit"))
        with pytest.raises(InfrastructureFailure) as late:
            lifecycle.observe_container(stopped, BINDING.identity, CONTAINER)

    assert changed.value.reason == "daemon-identity-changed" and changed.value.observer == "engine"
    assert late.value.reason == "client-exit"
    assert calls == ["observe_daemon", "observe_daemon"]  # no engine read after a failed verification
