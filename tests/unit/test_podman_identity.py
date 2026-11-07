"""Podman identity, machine matching, engine parsing and the size hint
2.4); the vectors are measurements from a real Podman machine."""

from __future__ import annotations

import hashlib
from dataclasses import replace

import pytest

from docker_test_toolkit.domain.models import MachineFacts
from docker_test_toolkit.domain.policy import (
    NAME,
    RuntimeRefusal,
    match_machine,
    parse_version_components,
    podman_identity,
    podman_machine_size,
    podman_size_hint,
)

MIB = 1024**2
GIB = 1024**3
W0 = MachineFacts(
    "dtt-w0",
    "2026-11-04T10:43:17.200188+01:00",
    False,
    "/var/folders/nd/x/T/podman/dtt-w0-api.sock",
    3072,
    2,
)
W0B = MachineFacts(
    "dtt-w0b",
    "2026-11-04T10:52:01.000001+01:00",
    False,
    "/var/folders/nd/x/T/podman/dtt-w0b-api.sock",
    1024,
    1,
)
PODMAN_COMPONENTS = (
    '[{"Name":"Podman Engine","Version":"6.1.4","Details":{"APIVersion":"6.1.4"}},'
    '{"Name":"Conmon","Version":"conmon version 2.2.1"},'
    '{"Name":"Engine","Version":"6.1.4","Details":{"ApiVersion":"1.44"}}]'
)
DESKTOP_COMPONENTS = '[{"Name":"Engine","Version":"29.7.2","Details":{"ApiVersion":"1.55"}},{"Name":"containerd"}]'


def test_the_identity_hashes_only_name_created_and_rootful() -> None:
    identity = podman_identity(W0)

    assert identity == podman_identity(W0)
    assert len(identity) == 47 and identity.startswith("podman-") and NAME.fullmatch(identity)
    assert identity == podman_identity(replace(W0, socket_path="/tmp/podman/dtt-w0-api.sock", memory_mib=2048, cpus=4))


def test_the_identity_is_the_sha256_of_the_canonical_json() -> None:
    canonical = b'{"created":"2026-11-04T10:43:17.200188+01:00","name":"dtt-w0","rootful":false}'

    assert podman_identity(W0) == "podman-" + hashlib.sha256(canonical).hexdigest()[:40]


@pytest.mark.parametrize(
    "changed",
    [
        replace(W0, rootful=True),  # a rootful switch
        replace(W0, created="2026-11-04T11:02:44.913377+01:00"),  # rm + init, or reset + init
        replace(W0, name="dtt-w1"),
    ],
)
def test_a_rootful_switch_a_reinit_or_another_name_change_the_identity(changed: MachineFacts) -> None:
    assert podman_identity(changed) != podman_identity(W0)


def test_match_machine_binds_exactly_one_running_machine_by_socket_string() -> None:
    assert match_machine(f"unix://{W0B.socket_path}", (W0, W0B)) == W0B
    private = W0.socket_path.replace("/var/", "/private/var/", 1)  # the same file through /private: never resolved

    assert isinstance(match_machine(f"unix://{private}", (W0,)), RuntimeRefusal)


def test_match_machine_names_the_socket_only_when_one_machine_runs() -> None:
    one = match_machine("unix:///tmp/other.sock", (W0,))
    two = match_machine("unix:///tmp/other.sock", (W0, W0B))
    none = match_machine("unix:///tmp/other.sock", ())

    assert (
        isinstance(one, RuntimeRefusal) and one.reason == "podman-machine-not-found" and W0.socket_path in one.message
    )
    assert isinstance(two, RuntimeRefusal) and two.reason == "podman-machine-not-found"
    assert W0.socket_path not in two.message and W0B.socket_path not in two.message
    assert isinstance(none, RuntimeRefusal) and none.reason == "podman-machine-not-found"


def test_the_helper_socket_is_refused_before_any_match() -> None:
    helper = replace(W0, socket_path="/var/run/docker.sock")  # even a machine claiming that socket is not matched

    refused = match_machine("unix:///var/run/docker.sock", (helper,))

    assert isinstance(refused, RuntimeRefusal) and refused.reason == "podman-helper-unsupported"
    assert "docker context create podman" in refused.message


def test_a_non_unix_endpoint_matches_no_machine() -> None:
    refused = match_machine("tcp://10.0.0.5:2376", (W0,))

    assert isinstance(refused, RuntimeRefusal) and refused.reason == "podman-machine-not-found"


@pytest.mark.parametrize(
    ("output", "expected"),
    [
        (f"29.7.2 {PODMAN_COMPONENTS}", ("29.7.2", "podman")),
        (f"29.7.2 {DESKTOP_COMPONENTS}", ("29.7.2", "docker")),
        ("28.4.0 null", ("28.4.0", "docker")),
        ("28.4.0 []", ("28.4.0", "docker")),
        ('28.4.0 [{"Name":"Engine"},{"Name":"Podman Engine"}]', ("28.4.0", "podman")),  # any position
        (f"29.7.2 {DESKTOP_COMPONENTS}\n", ("29.7.2", "docker")),  # the client's trailing newline
    ],
)
def test_version_components_select_the_engine(output: str, expected: tuple[str, str]) -> None:
    assert parse_version_components(output) == expected


@pytest.mark.parametrize(
    "output",
    ["", "28.4.0", "28.4.0 {", "28.4.0 [1]", '28.4.0 {"Name":"Podman Engine"}', "[] 28.4.0", '28.4.0 [{"Name":7}]'],
)
def test_version_components_that_are_not_objects_are_a_client_fault(output: str) -> None:
    with pytest.raises(ValueError):
        parse_version_components(output)


@pytest.mark.parametrize(("configured", "memtotal_kib"), [(2048, 1987792), (3072, 2950624), (8192, 8094716)])
def test_the_hint_names_a_size_that_admits_what_the_measured_machine_refused(
    configured: int, memtotal_kib: int
) -> None:
    from docker_test_toolkit.application.lifecycle import CapacitySnapshot, assess_capacity
    from docker_test_toolkit.domain.models import PhaseBudget
    from docker_test_toolkit.domain.policy import CapacityState

    reserve = 640 * MIB
    over = memtotal_kib * 1024 - reserve + MIB  # one MiB more than the measured machine admits

    memory, cpus = podman_machine_size(over, reserve, 2, 1)
    smallest_guest = int(0.93 * memory * MIB)  # every measured guest share is at least 0.938
    decided = assess_capacity(
        PhaseBudget(over, GIB, 1, 1), CapacitySnapshot(smallest_guest, 2, 64 * GIB, 8, "desktop", (), runtime="podman")
    )

    assert memory > configured and memory % 256 == 0 and cpus == 2
    assert decided.state is CapacityState.READY


def test_the_hint_names_the_machine_and_the_larger_cpu_count() -> None:
    hint = podman_size_hint(*podman_machine_size(6 * GIB, 640 * MIB, 2, 4), 2048, "dtt-window")
    cpu_only = podman_size_hint(*podman_machine_size(GIB, 640 * MIB, 2, 4), 2048, "dtt-window")  # never smaller

    assert hint == (
        "resize the Podman machine: podman machine stop dtt-window && podman machine set --memory 7424 --cpus 4"
        " dtt-window && podman machine start dtt-window"
    )
    assert "--memory 2048 --cpus 4" in cpu_only
