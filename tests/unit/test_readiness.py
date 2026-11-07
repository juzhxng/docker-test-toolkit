from __future__ import annotations

from dataclasses import asdict, replace
from pathlib import Path

import pytest

from docker_test_toolkit.application.lifecycle import HostMemoryObservation
from docker_test_toolkit.application.operator import execute
from docker_test_toolkit.application.ports.docker import DockerPort
from docker_test_toolkit.application.sharing import AdmissionSettings
from docker_test_toolkit.domain.models import (
    DockerClientObservation,
    DockerDaemonObservation,
    DockerReadinessObservation,
    PodmanBinding,
    ReadinessCode,
    ReadinessFailure,
    ReadinessReport,
    ToolVersionObservation,
)

SETTINGS = AdmissionSettings(2, 3 * 1024**3)


class FakeDockerPort(DockerPort):
    """Return one readiness observation and reject every unrelated Docker seam."""

    engine = "docker"  # class attributes: the port declares these read-only
    binding: PodmanBinding | None = None

    def __init__(self, observation: DockerReadinessObservation) -> None:
        self.env: dict[str, str] = {}
        self.observation = observation
        self.deadlines: list[object] = []

    def observe_readiness(self, deadline: object) -> DockerReadinessObservation:
        """Return the configured observation and record the only allowed call."""
        assert callable(getattr(deadline, "remaining_seconds", None))
        self.deadlines.append(deadline)
        return self.observation

    def __getattribute__(self, name: str):
        forbidden = {"objects", "inspect_exact", "compose"}
        if name in forbidden | {"execute", "call"}:
            raise AssertionError(f"readiness touched forbidden Docker seam: {name}")
        return super().__getattribute__(name)


def _observation(*, daemon_cpus: int = 6, daemon_memory: int = 12_345_678) -> DockerReadinessObservation:
    return DockerReadinessObservation(
        client=DockerClientObservation("/usr/local/bin/docker", "a" * 64, "28.4.0", 1, 2, 3, 4),
        daemon=DockerDaemonObservation(
            "fixture-daemon",
            "28.4.0",
            "1.51",
            "1.24",
            "linux",
            "2",
            "Docker Desktop",
            daemon_cpus,
            daemon_memory,
            "arm64",
            "docker-desktop",
            "unix:///Users/x/.docker/run/docker.sock",
        ),
        compose=ToolVersionObservation("2.39.0"),
        buildx=ToolVersionObservation("0.28.0 build-fixture"),
    )


def _host(
    *, physical: int = 32_000_000, headroom: int = 18_000_000, swap_total: int | None = 8_000_000
) -> HostMemoryObservation:
    raw = {"SwapTotal": swap_total} if swap_total is not None else {}
    return HostMemoryObservation("fixture-clock", 1_000_000_001, physical, headroom, 0, 0, 4096, raw)


def test_build_readiness_checks_preserves_exact_nondefault_capacity_facts(monkeypatch: pytest.MonkeyPatch) -> None:
    from docker_test_toolkit.application import lifecycle
    from docker_test_toolkit.application.readiness import build_readiness_checks

    monkeypatch.setattr(lifecycle.sys, "platform", "darwin")  # the fixture daemon is this Mac's Docker Desktop

    first = build_readiness_checks(_observation(), "fixture-clock", 99, _host(), 10, SETTINGS)
    second = build_readiness_checks(
        _observation(daemon_cpus=7, daemon_memory=87_654_321),
        "fixture-clock",
        100,
        _host(physical=64_000_000, headroom=40_000_000),
        12,
        AdmissionSettings(None, 2 * 1024**3),
    )

    assert [check.check for check in first] == [
        "docker-client",
        "daemon",
        "compose",
        "buildx",
        "clock",
        "capacity",
    ]
    assert first[0].facts == {
        "client_device": 1,
        "client_inode": 2,
        "client_mtime_ns": 4,
        "client_path": "/usr/local/bin/docker",
        "client_sha256": "a" * 64,
        "client_size": 3,
        "client_version": "28.4.0",
    }
    assert first[1].facts == {
        "api_version": "1.51",
        "architecture": "arm64",
        "cgroup_version": "2",
        "daemon_id": "fixture-daemon",
        "endpoint": "unix:///Users/x/.docker/run/docker.sock",
        "min_api_version": "1.24",
        "name": "docker-desktop",
        "operating_system": "Docker Desktop",
        "os_type": "linux",
        "runtime": "docker-desktop",
        "server_version": "28.4.0",
    }
    assert first[2].facts == {"version": "2.39.0"}
    assert first[3].facts == {"version": "0.28.0 build-fixture"}
    assert first[4].facts == {"origin": "fixture-clock", "tick_ns": 99}
    assert first[-1].facts == {
        "daemon_cpus": 6,
        "daemon_memory_bytes": 12_345_678,
        "host_cpus": 10,
        "host_headroom_bytes": 18_000_000,
        "host_memory_bytes": 32_000_000,
        "host_page_size": 4096,
        "host_swap_total_bytes": 8_000_000,
        "host_swap_used_bytes": 0,
        "host_reserve_bytes": 3 * 1024**3,
        "parallel_runs": 2,
    }
    assert second[-1].facts != first[-1].facts
    assert (second[-1].facts["parallel_runs"], second[-1].facts["host_reserve_bytes"]) == ("auto", 2 * 1024**3)
    assert [check.code for check in first] == [
        ReadinessCode.DOCKER_CLIENT_READY,
        ReadinessCode.DAEMON_READY,
        ReadinessCode.COMPOSE_READY,
        ReadinessCode.BUILDX_READY,
        ReadinessCode.CLOCK_READY,
        ReadinessCode.CAPACITY_READY,
    ]
    assert all(check.ok for check in first)


def test_capacity_facts_accept_zero_swap_total() -> None:
    """A host without swap (common on Linux and WSL2) is a valid host: total and used may both be zero."""
    from docker_test_toolkit.application.readiness import build_readiness_checks

    capacity = build_readiness_checks(_observation(), "fixture-clock", 99, _host(swap_total=0), 10, SETTINGS)[-1]

    assert capacity.code is ReadinessCode.CAPACITY_READY and capacity.ok is True
    assert capacity.facts["host_swap_total_bytes"] == 0 and capacity.facts["host_swap_used_bytes"] == 0


def test_darwin_swap_total_comes_from_the_native_counter() -> None:
    from docker_test_toolkit.application.readiness import build_readiness_checks

    host = replace(_host(swap_total=None), raw={"swap_total_bytes": 4_000_000})

    assert (
        build_readiness_checks(_observation(), "fixture-clock", 99, host, 10, SETTINGS)[-1].facts[
            "host_swap_total_bytes"
        ]
        == 4_000_000
    )


def test_failed_observation_keeps_closed_empty_facts_and_false_aggregate() -> None:
    from docker_test_toolkit.application.readiness import build_readiness_checks

    observation = _observation()
    observation = DockerReadinessObservation(
        observation.client,
        ReadinessFailure(ReadinessCode.DAEMON_UNAVAILABLE),
        observation.compose,
        ReadinessFailure(ReadinessCode.BUILDX_TIMEOUT),
    )

    checks = build_readiness_checks(observation, "fixture-clock", 99, _host(), 10, SETTINGS)

    assert checks[1].ok is False and checks[1].facts == {}
    assert checks[1].code is ReadinessCode.DAEMON_UNAVAILABLE
    assert checks[3].ok is False and checks[3].facts == {}


@pytest.mark.parametrize(
    ("observation", "host", "host_cpus"),
    (
        (_observation(daemon_cpus=0), _host(), 10),
        (_observation(daemon_memory=0), _host(), 10),
        (_observation(), _host(), 0),
        (_observation(), replace(_host(), physical_bytes=0), 10),
        (_observation(), replace(_host(), headroom_bytes=0), 10),
        (_observation(), replace(_host(), page_size=0), 10),
        (_observation(), replace(_host(), swap_used_bytes=-1), 10),
        (_observation(), _host(swap_total=-1), 10),
        (_observation(), _host(swap_total=None), 10),
    ),
)
def test_capacity_rejects_missing_positive_or_negative_swap_observations(
    observation: DockerReadinessObservation,
    host: HostMemoryObservation,
    host_cpus: int,
) -> None:
    from docker_test_toolkit.application.readiness import build_readiness_checks

    capacity = build_readiness_checks(observation, "fixture-clock", 99, host, host_cpus, SETTINGS)[-1]

    assert capacity.code is ReadinessCode.CAPACITY_RESPONSE_INVALID
    assert capacity.ok is False
    assert capacity.facts == {}


def test_unreadable_machine_settings_report_an_invalid_capacity_response() -> None:
    from docker_test_toolkit.application.readiness import build_readiness_checks

    capacity = build_readiness_checks(_observation(), "fixture-clock", 99, _host(), 10, None)[-1]

    assert capacity.code is ReadinessCode.CAPACITY_RESPONSE_INVALID and capacity.ok is False and capacity.facts == {}


def test_missing_native_observations_fail_independently() -> None:
    from docker_test_toolkit.application.readiness import build_readiness_checks

    checks = build_readiness_checks(_observation(), None, None, None, None, SETTINGS)

    assert checks[-2].code is ReadinessCode.CLOCK_DISCONTINUOUS
    assert checks[-1].code is ReadinessCode.CAPACITY_UNAVAILABLE
    assert checks[-2].facts == checks[-1].facts == {}


def test_operator_readiness_shares_one_deadline_with_discovery_and_observation() -> None:
    docker = FakeDockerPort(_observation())
    factory_deadlines: list[object] = []

    def docker_factory(deadline: object) -> DockerPort:
        factory_deadlines.append(deadline)
        return docker

    result = execute(
        "readiness.inspect",
        {},
        docker_factory=docker_factory,
    )

    assert result["protocol"] == "docker-test-toolkit.readiness/4"
    assert result["ready"] is True
    assert len(factory_deadlines) == 1
    assert docker.deadlines == factory_deadlines
    assert asdict(_observation().client)["sha256"] == "a" * 64


def test_operator_readiness_ignores_input_fields() -> None:
    def inspect(payload: dict) -> dict:
        docker = FakeDockerPort(_observation())
        result = execute("readiness.inspect", payload, docker_factory=lambda deadline: docker)
        return {**result, "checks": [(check["check"], check["code"], check["ok"]) for check in result["checks"]]}

    assert inspect({"argv": [], "future": True}) == inspect({})  # fields a newer peer sends are ignored


def test_readiness_4_adds_pressure_and_probe_facts_without_changing_the_earlier_facts() -> None:
    from docker_test_toolkit.application.readiness import build_readiness_checks

    plain = build_readiness_checks(_observation(), "fixture-clock", 99, _host(), 10, SETTINGS)[-1].facts
    probe_facts = {
        "cpu_pressure_percent": 1.56,
        "cpu_pressure_source": "vm-probe",
        "probe_image": "present",
        "probe_reading_age_s": 3.5,
        "disk_data_root_free_bytes": 60 * 1024,
        "disk_data_root_total_bytes": 100 * 1024,
    }

    extended = build_readiness_checks(
        _observation(), "fixture-clock", 99, _host(), 10, SETTINGS, probe_facts=probe_facts
    )[-1].facts

    assert {key: extended[key] for key in plain} == plain  # readiness/3 facts keep their values (additive protocol)
    assert {key: extended[key] for key in probe_facts} == probe_facts
    assert ReadinessReport((), True).protocol == "docker-test-toolkit.readiness/4"


def test_pressure_facts_prefer_the_host_then_the_latest_vm_reading_of_this_boot(tmp_path: Path) -> None:
    from docker_test_toolkit.application import probe
    from docker_test_toolkit.application.readiness import pressure_facts
    from docker_test_toolkit.application.recovery import atomic_json

    class _Clock:
        origin = "boot-1"

        def read(self) -> int:
            return 20_000_000_000

    reading = {"cpu_pressure": 2.5, "data_root_free_bytes": 6, "data_root_total_bytes": 10, "tick": 15_000_000_000}
    atomic_json(
        tmp_path / probe.PROBE_READING_FILE, {"schema": 1, "daemons": {"d1": {**reading, "clock_origin": "boot-1"}}}
    )
    atomic_json(tmp_path / probe.PROBE_IMAGE_FILE, {"schema": 1, "daemons": {"d1": {"present": True}}})
    host_psi = tmp_path / "host-psi"
    host_psi.write_text("some avg10=0.00 avg60=7.00 avg300=0.00 total=1\n")

    on_linux = pressure_facts(tmp_path, "d1", _Clock(), host_psi=host_psi)
    on_mac = pressure_facts(tmp_path, "d1", _Clock(), host_psi=tmp_path / "absent")
    atomic_json(
        tmp_path / probe.PROBE_READING_FILE, {"schema": 1, "daemons": {"d1": {**reading, "clock_origin": "boot-0"}}}
    )
    after_reboot = pressure_facts(tmp_path, "d1", _Clock(), host_psi=tmp_path / "absent")

    assert (on_linux["cpu_pressure_percent"], on_linux["cpu_pressure_source"]) == (7.0, "host")
    assert (on_mac["cpu_pressure_percent"], on_mac["cpu_pressure_source"], on_mac["probe_reading_age_s"]) == (
        2.5,
        "vm-probe",
        5.0,
    )
    assert (on_mac["disk_data_root_free_bytes"], on_mac["probe_image"]) == (6, "present")
    assert (after_reboot["cpu_pressure_percent"], after_reboot["disk_data_root_free_bytes"]) == (None, None)


def test_readiness_reports_the_declared_disk_reserve(tmp_path: Path) -> None:
    from docker_test_toolkit.application import sharing
    from docker_test_toolkit.application.readiness import pressure_facts

    class _Clock:
        origin = "boot-1"

        def read(self) -> int:
            return 1

    absent = tmp_path / "absent"
    undeclared = pressure_facts(tmp_path, "d1", _Clock(), host_psi=absent)
    sharing.record_disk_reserve(tmp_path, 2 * 1024**3)
    declared = pressure_facts(tmp_path, "d1", _Clock(), host_psi=absent)
    (tmp_path / sharing.DISK_RESERVE_FILE).write_text('{"schema": 1, "disk_reserve_bytes": 1}')
    invalid = pressure_facts(tmp_path, "d1", _Clock(), host_psi=absent)

    assert undeclared["disk_reserve_declared_bytes"] is None
    assert declared["disk_reserve_declared_bytes"] == 2 * 1024**3
    assert invalid["disk_reserve_declared_bytes"] is None  # readiness reports, admission fails closed on it


def test_readiness_reports_disk_places(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from docker_test_toolkit.application import lifecycle, probe
    from docker_test_toolkit.application.readiness import pressure_facts
    from docker_test_toolkit.application.recovery import atomic_json
    from docker_test_toolkit.domain.policy import derive_reserve

    class _Clock:
        origin = "boot-1"

        def read(self) -> int:
            return 2_000_000_000

    total = 100 * 1024**3
    reading = {"data_root_free_bytes": 40 * 1024**3, "data_root_total_bytes": total, "tick": 1_000_000_000}
    atomic_json(
        tmp_path / probe.PROBE_READING_FILE, {"schema": 1, "daemons": {"d1": {**reading, "clock_origin": "boot-1"}}}
    )
    vm_folder = tmp_path / "vms"
    vm_folder.mkdir()
    monkeypatch.setattr(lifecycle, "vm_disk_folder", lambda: vm_folder)
    monkeypatch.setenv("TMPDIR", str(tmp_path / "not-yet-created"))

    facts = pressure_facts(tmp_path, "d1", _Clock(), host_psi=tmp_path / "absent")
    monkeypatch.setattr(lifecycle, "vm_disk_folder", lambda: tmp_path / "moved-away")
    remote = pressure_facts(tmp_path, None, _Clock(), host_psi=tmp_path / "absent")

    places = ("data_root", "vm_disk_host", "private_root")
    keys = {
        f"disk_{place}_{suffix}"
        for place in places
        for suffix in ("free_bytes", "total_bytes", "reserve_bytes", "state")
    }
    assert keys <= set(facts)
    assert (facts["disk_data_root_state"], facts["disk_data_root_reserve_bytes"]) == (
        "measured",
        derive_reserve(total, None),
    )
    assert facts["disk_vm_disk_host_state"] == "measured" and facts["disk_vm_disk_host_free_bytes"] > 0
    assert facts["disk_private_root_state"] == "measured"  # measured on the nearest existing parent
    assert (remote["disk_data_root_state"], remote["disk_data_root_free_bytes"]) == ("unknown", None)
    assert remote["disk_vm_disk_host_state"] == "unknown"  # a moved disk image is not guessed


def test_the_desktop_disk_folder_is_found_without_reading_another_apps_container(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import builtins
    import pwd

    from docker_test_toolkit.application import lifecycle

    home = tmp_path / "home"
    monkeypatch.setattr(pwd, "getpwuid", lambda _uid: type("Entry", (), {"pw_dir": str(home)})())
    opened: list[str] = []
    real_open = builtins.open
    monkeypatch.setattr(builtins, "open", lambda file, *a, **k: opened.append(str(file)) or real_open(file, *a, **k))

    folder = lifecycle.vm_disk_folder()

    assert folder == home / "Library/Containers/com.docker.docker/Data/vms/0/data"
    assert lifecycle.measure_folder("vm-disk-host", folder, None).state == "unknown"  # moved away: unknown
    assert not any("Group Containers" in path for path in opened)  # macOS 15+ would ask the user for access


def _podman_binding(image_dir: Path):
    from docker_test_toolkit.domain.models import MachineFacts, PodmanBinding

    socket = "/var/folders/nd/x/T/podman/dtt-w0-api.sock"
    machine = MachineFacts("dtt-w0", "2026-11-04T10:43:17.200188+01:00", False, socket, 3072, 2)
    return PodmanBinding("podman-" + "a" * 40, f"unix://{socket}", machine, image_dir)


def test_readiness_names_the_podman_runtime_from_the_adapters_binding(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from docker_test_toolkit.application import lifecycle
    from docker_test_toolkit.application.readiness import build_readiness_checks

    monkeypatch.setattr(lifecycle.sys, "platform", "darwin")  # a Podman machine is served to a macOS client only
    binding = _podman_binding(tmp_path)

    bound = build_readiness_checks(_observation(), "c", 1, _host(), 10, SETTINGS, binding=binding)
    unbound = build_readiness_checks(_observation(), "c", 1, _host(), 10, SETTINGS)

    assert bound[1].facts["runtime"] == "podman"
    assert unbound[1].facts["runtime"] == "docker-desktop"  # without a binding the daemon's /info decides


def test_readiness_disk_place_on_podman_is_the_machine_image_dir(tmp_path: Path) -> None:
    from docker_test_toolkit.application.readiness import pressure_facts

    class _Clock:
        origin = "boot-1"

        def read(self) -> int:
            return 2_000_000_000

    image_dir = tmp_path / "applehv"
    image_dir.mkdir()

    facts = pressure_facts(
        tmp_path, "d1", _Clock(), host_psi=tmp_path / "absent", runtime="podman", image_dir=image_dir
    )
    unknown = pressure_facts(tmp_path, "d1", _Clock(), host_psi=tmp_path / "absent", runtime="podman")

    assert facts["disk_vm_disk_host_state"] == "measured"
    assert unknown["disk_vm_disk_host_state"] == "unknown"


def test_readiness_reads_host_pressure_by_admissions_rule(tmp_path: Path) -> None:
    from docker_test_toolkit.application.readiness import pressure_facts

    class _Clock:
        origin = "boot-1"

        def read(self) -> int:
            return 1

    garbled = tmp_path / "psi"
    garbled.write_text("not a pressure line\n")

    facts = pressure_facts(None, None, _Clock(), host_psi=garbled)

    # Admission holds joins on an unparsable host PSI file (full pressure); readiness reports the same figure.
    assert (facts["cpu_pressure_percent"], facts["cpu_pressure_source"]) == (100.0, "host")


def test_readiness_measures_the_private_root_admission_measures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from docker_test_toolkit.application import lifecycle
    from docker_test_toolkit.application.readiness import pressure_facts

    class _Clock:
        origin = "boot-1"

        def read(self) -> int:
            return 1

    real = tmp_path / "real"
    (real / lifecycle.PRIVATE_ROOT_NAME).mkdir(parents=True)
    (tmp_path / "link").symlink_to(real)  # macOS: TMPDIR under /var, which is a link to /private/var
    monkeypatch.setenv("TMPDIR", str(tmp_path / "link"))
    measured: list[Path | None] = []
    original = lifecycle.measure_folder
    monkeypatch.setattr(
        lifecycle,
        "measure_folder",
        lambda place, path, declared: measured.append(path) or original(place, path, declared),
    )

    pressure_facts(None, None, _Clock(), host_psi=tmp_path / "absent")

    assert measured[-1] == real.resolve() / lifecycle.PRIVATE_ROOT_NAME
