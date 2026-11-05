"""Build one fixed read-only Docker and host readiness report."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import asdict
from pathlib import Path

from docker_test_toolkit.application import lifecycle
from docker_test_toolkit.application.lifecycle import Deadline, HostMemoryObservation, NativeClock, observe_host_memory
from docker_test_toolkit.application.ports.docker import DockerPort
from docker_test_toolkit.application.probe import latest_reading, read_probe_images
from docker_test_toolkit.application.retention import read_report
from docker_test_toolkit.application.sharing import (
    HOST_RESERVE_DEFAULT,
    AdmissionSettings,
    declared_disk_reserve,
    read_host_reserve,
    read_parallel_runs,
)
from docker_test_toolkit.application.state import canonical_account
from docker_test_toolkit.config.runtime import PRIVATE_ROOT_NAME
from docker_test_toolkit.domain.errors import ResourceError
from docker_test_toolkit.domain.models import (
    DockerClientObservation,
    DockerDaemonObservation,
    DockerReadinessObservation,
    PodmanBinding,
    ReadinessCheck,
    ReadinessCode,
    ReadinessFailure,
    ReadinessReport,
    ToolVersionObservation,
)
from docker_test_toolkit.domain.policy import (
    DiskFigure,
    RuntimeRefusal,
)


def _observed_check(
    name: str,
    observed: DockerClientObservation | DockerDaemonObservation | ToolVersionObservation | ReadinessFailure,
    success: ReadinessCode,
    facts: dict[str, object],
) -> ReadinessCheck:
    if isinstance(observed, ReadinessFailure):
        return ReadinessCheck(name, observed.code, {}, False)
    return ReadinessCheck(name, success, facts, True)


def _runtime_fact(daemon: DockerDaemonObservation, binding: PodmanBinding | None = None) -> str | None:
    """Name the detected runtime (design 2.5): ``None`` for a remote endpoint, ``unsupported`` for a refused one."""
    if daemon.endpoint is None:
        return None
    detected = lifecycle.runtime_of(
        binding, {"Name": daemon.name, "OperatingSystem": daemon.operating_system}, daemon.endpoint
    )
    return "unsupported" if isinstance(detected, RuntimeRefusal) else detected


def _docker_checks(
    observation: DockerReadinessObservation, binding: PodmanBinding | None = None
) -> tuple[ReadinessCheck, ...]:
    client_facts = (
        {f"client_{key}": value for key, value in asdict(observation.client).items()}
        if isinstance(observation.client, DockerClientObservation)
        else {}
    )
    daemon_facts = (
        {
            **{key: value for key, value in asdict(observation.daemon).items() if key not in {"cpus", "memory_bytes"}},
            "runtime": _runtime_fact(observation.daemon, binding),
        }
        if isinstance(observation.daemon, DockerDaemonObservation)
        else {}
    )
    compose_facts = asdict(observation.compose) if isinstance(observation.compose, ToolVersionObservation) else {}
    buildx_facts = asdict(observation.buildx) if isinstance(observation.buildx, ToolVersionObservation) else {}
    return (
        _observed_check("docker-client", observation.client, ReadinessCode.DOCKER_CLIENT_READY, client_facts),
        _observed_check("daemon", observation.daemon, ReadinessCode.DAEMON_READY, daemon_facts),
        _observed_check("compose", observation.compose, ReadinessCode.COMPOSE_READY, compose_facts),
        _observed_check("buildx", observation.buildx, ReadinessCode.BUILDX_READY, buildx_facts),
    )


def _capacity_check(
    observation: DockerReadinessObservation,
    host_memory: HostMemoryObservation | None,
    host_cpus: int | None,
    settings: AdmissionSettings | None,
    probe_facts: Mapping[str, object],
) -> ReadinessCheck:
    daemon = observation.daemon
    if not isinstance(daemon, DockerDaemonObservation) or host_memory is None or host_cpus is None:
        return ReadinessCheck("capacity", ReadinessCode.CAPACITY_UNAVAILABLE, {}, False)
    positive = (
        daemon.cpus,
        daemon.memory_bytes,
        host_cpus,
        host_memory.physical_bytes,
        host_memory.headroom_bytes,
        host_memory.page_size,
    )
    swap_total = host_memory.swap_total_bytes
    if (
        any(type(value) is not int or value <= 0 for value in positive)
        or type(host_memory.swap_used_bytes) is not int
        or host_memory.swap_used_bytes < 0
        or swap_total is None
        or swap_total < 0
        or settings is None
    ):
        return ReadinessCheck("capacity", ReadinessCode.CAPACITY_RESPONSE_INVALID, {}, False)
    return ReadinessCheck(
        "capacity",
        ReadinessCode.CAPACITY_READY,
        {
            "daemon_cpus": daemon.cpus,
            "daemon_memory_bytes": daemon.memory_bytes,
            "host_cpus": host_cpus,
            "host_headroom_bytes": host_memory.headroom_bytes,
            "host_memory_bytes": host_memory.physical_bytes,
            "host_page_size": host_memory.page_size,
            "host_swap_total_bytes": swap_total,
            "host_swap_used_bytes": host_memory.swap_used_bytes,
            "host_reserve_bytes": settings.host_reserve_bytes,
            "parallel_runs": "auto" if settings.limit is None else settings.limit,
            **probe_facts,
        },
        True,
    )


def build_readiness_checks(
    observation: DockerReadinessObservation,
    clock_origin: str | None,
    clock_tick_ns: int | None,
    host_memory: HostMemoryObservation | None,
    host_cpus: int | None,
    settings: AdmissionSettings | None,
    *,
    probe_facts: Mapping[str, object] | None = None,
    binding: PodmanBinding | None = None,
) -> tuple[ReadinessCheck, ...]:
    """Build the exact six checks from typed Docker and native host observations and the machine settings.

    ``settings`` is None when the account's settings are unreadable; the capacity check then reports an invalid
    response. ``probe_facts`` are the protocol-4 pressure and probe facts (``pressure_facts``).
    """
    clock = (
        ReadinessCheck(
            "clock",
            ReadinessCode.CLOCK_READY,
            {"origin": clock_origin, "tick_ns": clock_tick_ns},
            True,
        )
        if clock_origin and type(clock_tick_ns) is int and clock_tick_ns >= 0
        else ReadinessCheck("clock", ReadinessCode.CLOCK_DISCONTINUOUS, {}, False)
    )
    capacity = _capacity_check(observation, host_memory, host_cpus, settings, probe_facts or {})
    return (*_docker_checks(observation, binding), clock, capacity)


def pressure_facts(
    account: Path | None,
    daemon_id: str | None,
    clock: NativeClock,
    *,
    host_psi: Path = Path("/proc/pressure/cpu"),
    runtime: str | None = "docker-desktop",
    daemon_name: str | None = None,
    image_dir: Path | None = None,
) -> dict[str, object]:
    """Return the protocol-4 facts without any Docker call: host PSI where the host publishes it, else the guardians'
    latest VM probe reading of this daemon with its age, the probe image's state and the data-root figure.
    """
    host = lifecycle.host_cpu_pressure(host_psi)  # admission's rule: an unreadable file is full pressure
    images = read_probe_images(account) if account is not None and daemon_id is not None else {}
    # a reading of another boot is no figure rather than a stale one of unknown age
    reading = None if account is None or daemon_id is None else latest_reading(account, daemon_id, clock.origin)
    image = images.get(daemon_id or "")
    age = None if reading is None else max(0, clock.read() - reading.read_ns) / 1e9
    vm_pressure = None if reading is None else reading.cpu_pressure
    declared = (
        None if account is None else declared_disk_reserve(account)
    )  # read once for every fact that depends on it
    return {
        "cpu_pressure_percent": host if host is not None else vm_pressure,
        "cpu_pressure_source": "host" if host is not None else "vm-probe" if vm_pressure is not None else "none",
        "probe_image": "unknown" if image is None else "present" if image.get("present") else "failed",
        "probe_reading_age_s": age,
        **_disk_facts(
            None if reading is None else reading.data_root,
            declared,
            lifecycle.vm_disk_place(runtime, daemon_name, image_dir),
        ),
        "disk_reserve_declared_bytes": declared,
        **_retention_facts(account),
    }


def _disk_facts(reading: DiskFigure | None, declared: int | None, vm_folder: Path | None) -> dict[str, object]:
    """Return free, total, reserve and state of each disk place (design 2.6), without any Docker call.

    The data root is the guardians' latest probe reading of this boot; the runtime's VM disk folder and the private
    root's nearest existing parent are measured here. A place that cannot be measured is ``unknown``.
    """
    data_root = (
        lifecycle.measured_figure("data-root", reading.free_bytes, reading.total_bytes, declared)
        if reading is not None and reading.free_bytes is not None and reading.total_bytes is not None
        else DiskFigure("data-root", None, None, None, "unknown")
    )
    temporary = lifecycle.temporary_folder() / PRIVATE_ROOT_NAME
    figures = (
        data_root,
        lifecycle.measure_folder("vm-disk-host", vm_folder, declared),
        lifecycle.measure_folder("private-root", lifecycle.existing_ancestor(temporary), None),
    )
    return {
        f"disk_{figure.place.replace('-', '_')}_{suffix}": value
        for figure in figures
        for suffix, value in (
            ("free_bytes", figure.free_bytes),
            ("total_bytes", figure.total_bytes),
            ("reserve_bytes", figure.reserve_bytes),
            ("state", figure.state),
        )
    }


def _retention_facts(account: Path | None) -> dict[str, object]:
    """Return when the last retention pass ran (its tick and origin) and what it removed, from its report."""
    report = read_report(account) if account is not None else None
    kinds = report.get("kinds") if isinstance(report, dict) else None
    removed = sum(kind.get("removed_bytes", 0) for kind in kinds.values()) if isinstance(kinds, dict) else None
    return {
        "retention_last_tick_ns": report.get("tick") if isinstance(report, dict) else None,
        "retention_last_removed_bytes": removed,
    }


def _machine_settings() -> AdmissionSettings | None:
    """Read the running installation's machine settings lock-free; defaults outside an installation, None if invalid."""
    account = canonical_account()
    if account is None:
        return AdmissionSettings(1, HOST_RESERVE_DEFAULT)
    try:
        return AdmissionSettings(read_parallel_runs(account), read_host_reserve(account))
    except ResourceError:
        return None


def inspect_readiness(
    docker: DockerPort,
    clock: NativeClock,
    *,
    deadline: Deadline | None = None,
) -> ReadinessReport:
    """Observe the fixed readiness boundary once without durable state or object enumeration."""
    deadline = deadline or Deadline.after(clock, 30)
    observation = docker.observe_readiness(deadline)
    try:
        tick = clock.read()
        origin: str | None = clock.origin
    except ResourceError:
        origin = None
        tick = None
    try:
        host = observe_host_memory(clock)
        host_cpus = os.cpu_count()
    except ResourceError:
        host = None
        host_cpus = None
    daemon = observation.daemon
    if isinstance(daemon, DockerDaemonObservation):
        binding = docker.binding
        facts = pressure_facts(
            canonical_account(),
            daemon.daemon_id,
            clock,
            runtime=_runtime_fact(daemon, binding),
            daemon_name=daemon.name,
            image_dir=binding.image_dir if binding is not None else None,
        )
    else:
        facts = pressure_facts(canonical_account(), None, clock, runtime=None)
    checks = build_readiness_checks(
        observation,
        origin,
        tick,
        host,
        host_cpus,
        _machine_settings(),
        probe_facts=facts,
        binding=docker.binding,
    )
    return ReadinessReport(checks, all(check.ok for check in checks))
