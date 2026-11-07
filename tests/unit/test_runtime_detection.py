"""Runtime detection before topology, overheads per runtime and the small-VM hint."""

from __future__ import annotations

from pathlib import Path

import pytest

from docker_test_toolkit.domain.policy import (
    RUNTIME_OVERHEADS,
    RuntimeRefusal,
    detect_runtime,
    runtime_topology,
    small_vm_hint,
    small_vm_memory_gib,
)

GIB = 1024**3
MIB = 1024**2
HOME = Path("/Users/x")
DESKTOP = {"Name": "docker-desktop", "OperatingSystem": "Docker Desktop", "KernelVersion": "6.12.76-linuxkit"}
COLIMA_ARM64 = {"Name": "colima", "OperatingSystem": "Ubuntu 24.04.4 LTS", "KernelVersion": "6.8.0-117-generic"}
COLIMA_1450 = {  # abiosoft/colima#1450 (x86_64, Colima 0.9.1)
    "Name": "colima",
    "OperatingSystem": "Ubuntu 24.04.2 LTS",
    "KernelVersion": "6.8.0-64-generic",
}
CI_HOST = {"Name": "ci-host", "OperatingSystem": "Ubuntu 24.04.3 LTS", "KernelVersion": "6.8.0-1030-generic"}
DESKTOP_SOCKET = "unix:///Users/x/.docker/run/docker.sock"
COLIMA_SOCKET = "unix:///Users/x/.colima/default/docker.sock"
UNSUPPORTED = "Unsupported container runtime for this client; supported on macOS: Docker Desktop, Colima, Podman."


@pytest.mark.parametrize(
    ("info", "platform", "endpoint", "expected"),
    [
        (DESKTOP, "darwin", DESKTOP_SOCKET, "docker-desktop"),
        (COLIMA_1450, "darwin", COLIMA_SOCKET, "colima"),
        (COLIMA_ARM64, "darwin", COLIMA_SOCKET, "colima"),  # a Mac, Colima 0.10.3 (measured)
        ({**COLIMA_1450, "Name": "colima-work"}, "darwin", "unix:///Users/x/.colima/work/docker.sock", "colima"),
        (CI_HOST, "linux", "unix:///var/run/docker.sock", "docker-engine"),
        (DESKTOP, "darwin", "tcp://10.0.0.5:2376", None),
        (CI_HOST, "linux", "ssh://ci@runner", None),
        (DESKTOP, "linux", "npipe:////./pipe/docker_engine", None),
        # The CI host's runners reach their own host's daemon over the loopback: native Docker Engine, as 0.6.20
        # accounted it (a remote classification dropped the host-side terms)
        (CI_HOST, "linux", "tcp://127.0.0.1:2375", "docker-engine"),
        (CI_HOST, "linux", "tcp://[::1]:2375", "docker-engine"),
        (DESKTOP, "darwin", "tcp://localhost:2375", "docker-desktop"),
        (CI_HOST, "darwin", "tcp://127.0.0.1:2375", None),  # a Mac's tunnel to an unrecognised daemon stays remote
        (CI_HOST, "linux", "tcp://127.build.example.com:2375", None),
        (CI_HOST, "linux", "tcp://127.0.0.1:not-a-port", None),  # a malformed endpoint names no machine
        (CI_HOST, "linux", "tcp://localhost:65536", None),
        (DESKTOP, "linux", "tcp://localhost:2375", None),  # WSL2's exposed Desktop daemon stays remote, as in 0.7.1
    ],
)
def test_the_runtime_is_detected_from_the_daemon_the_client_and_the_endpoint(
    info: dict, platform: str, endpoint: str, expected: str | None
) -> None:
    assert detect_runtime(info, platform, endpoint, HOME) == expected


@pytest.mark.parametrize(
    ("info", "platform", "endpoint", "message"),
    [
        (DESKTOP, "linux", "unix:///var/run/docker.sock", "use Docker Engine in the WSL2 distribution"),
        (COLIMA_1450, "linux", "unix:///var/run/docker.sock", UNSUPPORTED),  # a Mac VM daemon from a Linux client
        ({**CI_HOST, "Name": "orbstack"}, "darwin", "unix:///Users/x/.orbstack/run/docker.sock", UNSUPPORTED),
        (COLIMA_1450, "darwin", "unix:///var/run/docker.sock", UNSUPPORTED),  # the name without Colima's own socket
        ({**COLIMA_1450, "Name": "colima-work"}, "darwin", COLIMA_SOCKET, UNSUPPORTED),  # another profile's socket
        (CI_HOST, "win32", "unix:///var/run/docker.sock", UNSUPPORTED),
    ],
)
def test_other_runtimes_are_refused_naming_the_supported_ones(
    info: dict, platform: str, endpoint: str, message: str
) -> None:
    refused = detect_runtime(info, platform, endpoint, HOME)

    assert isinstance(refused, RuntimeRefusal) and message in refused.message
    assert refused.reason == "unsupported-runtime"


def test_each_runtime_names_its_topology() -> None:
    assert [runtime_topology(name) for name in ("docker-desktop", "colima", "podman", "docker-engine", None)] == [
        "desktop",
        "desktop",
        "desktop",
        "native",
        "remote",
    ]


def test_desktop_and_engine_keep_the_previous_overheads_and_colima_has_its_measured_ones() -> None:
    assert RUNTIME_OVERHEADS["docker-desktop"] == (512 * MIB, 256 * MIB)
    assert RUNTIME_OVERHEADS["docker-engine"] == (512 * MIB, 0)
    assert RUNTIME_OVERHEADS["colima"] == (512 * MIB, 256 * MIB)  # measured
    assert RUNTIME_OVERHEADS["podman"] == (640 * MIB, 256 * MIB)  # 485 MiB idle on a 2048 MiB machine


COLIMA_USABLE = 8307101696 / (8 * GIB)  # measured: an 8 GiB Colima VM reports this share as MemTotal


def test_the_small_vm_size_is_the_smallest_whole_gib_whose_usable_memory_fits() -> None:
    assert small_vm_memory_gib(6 * GIB, 512 * MIB) == 7
    assert small_vm_memory_gib(7 * GIB + 512 * MIB, 512 * MIB) == 9  # 8 GiB configured is only 7.74 GiB usable
    for need in (6 * GIB, 7 * GIB + 512 * MIB, 20 * GIB):  # the named size admits the guest side it was asked for
        assert small_vm_memory_gib(need, 512 * MIB) * GIB * COLIMA_USABLE >= need + 512 * MIB


def _snapshot(daemon_bytes: int, runtime: str | None, topology: str = "desktop"):
    from docker_test_toolkit.application.lifecycle import CapacitySnapshot

    return CapacitySnapshot(daemon_bytes, 8, 64 * GIB, 8, topology, (), runtime=runtime)


def test_the_hinted_vm_size_admits_the_run_and_one_gib_less_does_not() -> None:
    from docker_test_toolkit.application.lifecycle import assess_capacity
    from docker_test_toolkit.domain.models import PhaseBudget
    from docker_test_toolkit.domain.policy import CapacityState

    budget = PhaseBudget(6 * GIB, GIB, 1, 1)

    refused = assess_capacity(budget, _snapshot(6 * GIB, "colima"))
    named = small_vm_memory_gib(6 * GIB, 512 * MIB) * GIB
    admitted = assess_capacity(budget, _snapshot(int(named * COLIMA_USABLE), "colima"))  # MemTotal of that size

    assert (refused.state, refused.reason) == (CapacityState.REFUSED, "unsupported-base-capacity")
    assert small_vm_hint(6 * GIB, 512 * MIB) == "raise the VM memory: colima stop && colima start --memory 7"
    assert admitted.state is CapacityState.READY


def test_desktop_engine_and_remote_keep_the_previous_reservations() -> None:
    from docker_test_toolkit.application.lifecycle import assess_capacity
    from docker_test_toolkit.domain.models import PhaseBudget

    budget = PhaseBudget(2 * GIB, GIB, 1, 1)
    host_reserve = 9 * GIB // 2
    guest = 2 * GIB + 512 * MIB

    desktop = assess_capacity(budget, _snapshot(16 * GIB, "docker-desktop"))
    engine = assess_capacity(budget, _snapshot(16 * GIB, "docker-engine", "native"))
    remote = assess_capacity(budget, _snapshot(16 * GIB, None, "remote"))

    assert (desktop.guest_bytes, desktop.host_bytes) == (guest, GIB + host_reserve + guest + 256 * MIB)
    assert (engine.guest_bytes, engine.host_bytes) == (guest, GIB + host_reserve + guest)
    assert (remote.guest_bytes, remote.host_bytes) == (guest, GIB + host_reserve)


def test_a_runtime_without_measured_overheads_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    from docker_test_toolkit.application.lifecycle import assess_capacity
    from docker_test_toolkit.domain.models import PhaseBudget
    from docker_test_toolkit.domain.policy import CapacityState

    monkeypatch.delitem(RUNTIME_OVERHEADS, "colima")  # as before the qualification window
    decided = assess_capacity(PhaseBudget(GIB, GIB, 1, 1), _snapshot(16 * GIB, "colima"))

    assert (decided.state, decided.reason) == (CapacityState.REFUSED, "runtime-unqualified")


FEDORA = {"Name": "localhost.localdomain", "OperatingSystem": "fedora", "KernelVersion": "7.2.7-200.fc44.aarch64"}
PODMAN_SOCKET = "unix:///var/folders/nd/x/T/podman/dtt-w0-api.sock"


def _binding():
    from docker_test_toolkit.domain.models import MachineFacts, PodmanBinding

    machine = MachineFacts("dtt-w0", "2026-11-04T10:43:17.200188+01:00", False, PODMAN_SOCKET[7:], 3072, 2)
    return PodmanBinding("podman-" + "a" * 40, PODMAN_SOCKET, machine, Path("/Users/x/.local/share/containers"))


def test_a_bound_podman_engine_is_podman_whatever_its_info_says() -> None:
    from docker_test_toolkit.application.lifecycle import runtime_of

    desktop_info = {**FEDORA, "OperatingSystem": "Docker Desktop"}  # never classified by OperatingSystem

    assert runtime_of(_binding(), FEDORA, PODMAN_SOCKET) == "podman"
    assert runtime_of(_binding(), desktop_info, PODMAN_SOCKET) == "podman"


def test_a_podman_socket_without_a_binding_is_never_a_supported_runtime(monkeypatch: pytest.MonkeyPatch) -> None:
    from docker_test_toolkit.application import lifecycle

    monkeypatch.setattr(lifecycle.sys, "platform", "darwin")
    monkeypatch.setattr(lifecycle, "passwd_home", lambda: HOME)

    refused = lifecycle.runtime_of(None, FEDORA, PODMAN_SOCKET)  # the adapter refuses earlier; the rule stays closed

    assert isinstance(refused, RuntimeRefusal) and refused.reason == "unsupported-runtime"
