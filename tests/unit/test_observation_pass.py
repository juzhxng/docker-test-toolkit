"""One guardian observation pass: its single deadline and the test-composition read seam."""

from __future__ import annotations

import errno
import json
import re
import subprocess
import sys
import time
from collections.abc import Callable
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace

import pytest

from docker_test_toolkit.application import lifecycle, sharing
from docker_test_toolkit.application.lifecycle import ControlError, Deadline, EngineReply, NativeClock
from docker_test_toolkit.domain.errors import ResourceError
from docker_test_toolkit.domain.models import PhaseBudget
from docker_test_toolkit.domain.policy import Grant
from docker_test_toolkit.factories.testing import bind_test_observation_reads, build_test_services, install_failpoint
from tests.integration.account_fixtures import pause_guardian, resume_guardian
from tests.integration.share_fixtures import _leave, shared_world
from tests.unit.test_docker_receipt_service import IMAGE_ID, FakeDockerPort, _container, _spec

GIB = 1024**3


class _Reads:
    """Scripted engine, RSS and host readers for the test composition seam; every read records its deadline.

    ``script[key]`` is a list of outcomes consumed per read (the last one repeats): ``ok``, ``late``, ``unreadable``,
    ``unclassified``, plus ``oom`` for a container and ``daemon-changed`` or ``tiny`` for ``info``. A container can
    also stop between the exact inspection and these reads: ``exited`` (its ``/json`` already shows it stopped),
    ``exiting`` (its stats come from the dying cgroup), ``exiting-oom`` (the same, killed by the OOM killer),
    ``restarted-exited`` (stopped in a newer generation) and ``dying`` (empty stats while it keeps running).
    Keys: ``info``, ``host``, ``rss`` or a 64-hex container id (consumed on its ``/json`` read).
    """

    def __init__(self, fake: FakeDockerPort, topology: str | None) -> None:
        self.fake = fake
        self.topology = topology
        self.deadlines: list[tuple[str, object]] = []
        self.paths: list[str] = []
        self.script: dict[str, list[str]] = {}
        self.host_factory = None
        self._last: dict[str, str] = {}

    def _next(self, key: str) -> str:
        outcomes = self.script.get(key)
        if not outcomes:
            return "ok"
        return outcomes.pop(0) if len(outcomes) > 1 else outcomes[0]

    @staticmethod
    def _late() -> ControlError:
        return ControlError(
            "Engine deadline expired.", subprocess.CompletedProcess([], 0, b"", b""), reason="observation-deadline"
        )

    def engine(self, _docker: object, path: str, deadline: object) -> EngineReply:
        kind = "info" if path == "/info" else "container-stats" if "/stats" in path else "container-json"
        self.deadlines.append((f"engine:{kind}", deadline))
        self.paths.append(path)
        if path == "/info":
            outcome = self._next("info")
            if outcome == "late":
                raise self._late()
            if outcome == "unclassified":
                raise ResourceError("Engine said no.")
            if outcome == "slow":
                while deadline.remaining_ns(NativeClock()):  # type: ignore[attr-defined]
                    time.sleep(0.02)
            return EngineReply(
                {
                    "ID": "22222222-2222-2222-2222-222222222222"
                    if outcome == "daemon-changed"
                    else self.fake.identity.daemon_id,
                    "MemTotal": 1 if outcome == "tiny" else 64 * GIB,
                    "NCPU": 64,
                    "OSType": "linux",
                    "CgroupVersion": "2",
                    "OperatingSystem": "Docker Desktop" if self.topology == "desktop" else "Ubuntu",
                },
                b"",
                0,
            )
        container_id = re.search(r"/containers/([0-9a-f]{64})/", path).group(1)  # type: ignore[union-attr]
        container = self.fake.containers[container_id]
        state = container["State"]
        if "/stats" in path:
            outcome = self._last.get(container_id, "ok")
            events = container["OomEvents"] + (1 if outcome == "oom" else 0)
            body = {
                "id": container_id,
                "memory_stats": {"usage": 1, "limit": container["HostConfig"]["Memory"], "stats": {"failcnt": events}},
            }
            if outcome in ("exiting", "exiting-oom", "dying"):
                body["memory_stats"] = {}
            if outcome in ("exiting", "exiting-oom"):
                state.update(Running=False, ExitCode=137 if outcome == "exiting-oom" else 70)
                state["OOMKilled"] = outcome == "exiting-oom"
        else:
            outcome = self._next(container_id)
            self._last[container_id] = outcome
            if outcome == "late":
                raise self._late()
            if outcome == "unreadable":
                raise OSError(errno.EIO, "broken pipe")
            if outcome in ("exited", "restarted-exited"):
                state.update(Running=False, ExitCode=70)
            if outcome == "restarted-exited":
                state["StartedAt"] = "2026-10-13T10:00:00.000000000Z"
            body = {
                "Id": container_id,
                "State": {"StartedAt": state["StartedAt"], "Running": state["Running"], "OOMKilled": outcome == "oom"},
                "HostConfig": {"Memory": container["HostConfig"]["Memory"]},
                "RestartCount": container["RestartCount"],
            }
        return EngineReply(body, b"", 0)

    def rss(self, _identity: object, deadline: object) -> int:
        self.deadlines.append(("rss", deadline))
        outcome = self._next("rss")
        if outcome == "unreadable":
            raise OSError(errno.EIO, "rss")
        if outcome == "late":
            raise self._late()
        return 0

    def host(self, _clock: object, deadline: object) -> lifecycle.HostMemoryObservation:
        self.deadlines.append(("host", deadline))
        outcome = self._next("host")
        if outcome == "late":
            raise ResourceError("Host sample late.", reason="observation-deadline", observer="host")
        if outcome == "unreadable":
            raise ResourceError("Host sample unreadable.", reason="telemetry-unreadable", observer="host")
        assert self.host_factory is not None
        return self.host_factory()


def _active_pass(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
    *,
    services: tuple[str, ...] = ("fixture",),
):
    """An entered run with shim-owned Compose containers, observed as its own guardian (harness exception)."""
    project = tmp_path / "project"
    project.mkdir()
    fake = FakeDockerPort()
    fake.compose_services = services
    services = build_test_services(tmp_path / "state")
    handle = services.open_run(_spec(project), docker_factory=lambda: fake, admit_docker=True).enter()
    _ = handle.docker_context
    descriptor = json.loads((handle.private_root / "docker-authority.json").read_text())
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "up", "--detach"))
    return services, handle, fake, _as_guardian(handle, fake, monkeypatch, request)


def _as_guardian(
    handle, fake: FakeDockerPort, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> _Reads:
    """Make this process the run's guardian on its isolated account, never the shared one; return scripted reads.

    The record gets a ``managed`` grant scope, which the real guardian's own loader refuses, so that guardian is paused
    for the test and resumed at teardown on the entry record; the teardown then
    asserts that it still executes.
    """
    context = handle._context
    pid = pause_guardian(context)
    entry = json.loads(context.record.read_text())
    record = json.loads(context.record.read_text())
    guardian = lifecycle._identity_from_json(record["guardian"])

    def teardown() -> None:
        lifecycle.atomic_json(context.record, entry)
        resume_guardian(pid)
        assert lifecycle.observe_process(guardian) == "executing", "the run's real guardian ended during the test"

    request.addfinalizer(teardown)
    record["grant"]["scope"] = "managed"
    lifecycle.atomic_json(context.record, record)
    baseline = lifecycle._host_observation(record["admission_observation"]["memory"])
    isolated_account = Path(record["account_directory"])
    monkeypatch.setattr(lifecycle, "current_process", lambda _clock: guardian)
    monkeypatch.setattr(lifecycle, "account_directory", lambda: isolated_account)
    monkeypatch.setattr(lifecycle, "canonical_account", lambda: isolated_account)
    monkeypatch.setattr(lifecycle, "_docker", lambda: fake)
    reads = _Reads(fake, record["topology"])
    reads.host_factory = lambda: lifecycle.HostMemoryObservation(
        baseline.origin,
        baseline.tick_ns + 1,
        baseline.physical_bytes,
        baseline.headroom_bytes,
        baseline.swap_used_bytes,
        baseline.swapout_bytes,
        baseline.page_size,
        baseline.raw,
    )
    return reads


def test_observation_pass_single_deadline_reaches_every_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> None:
    _services, handle, fake, reads = _active_pass(tmp_path, monkeypatch, request)
    fake.deadlines.clear()  # only the reads of the observation pass count, not the setup commands

    with bind_test_observation_reads(engine=reads.engine, rss=reads.rss, host=reads.host):
        lifecycle._observe_active_run(handle._context)

    recorded = reads.deadlines + fake.deadlines
    assert json.loads(handle._context.record.read_text())["infrastructure_failure"] is None
    labels = {label for label, _ in recorded}
    assert {"engine:info", "host", "rss"} <= labels
    assert "objects:container" not in labels  # after the grant no inventory is read: only the run's own containers
    assert any(label.startswith("engine:container") for label in labels)
    assert [label for label, _ in recorded].count("host") == 1
    first = recorded[0][1]
    assert isinstance(first, Deadline)
    assert all(deadline is first for _, deadline in recorded), [
        label for label, deadline in recorded if deadline is not first
    ]


def test_admission_capacity_inputs_share_one_deadline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> None:
    _services, handle, fake, reads = _active_pass(tmp_path, monkeypatch, request)
    data = lifecycle._load_run(handle._context)
    fake.deadlines.clear()

    with bind_test_observation_reads(engine=reads.engine, rss=reads.rss, host=reads.host):
        lifecycle._capacity_inputs(data, None)

    recorded = reads.deadlines + fake.deadlines
    assert {"engine:info", "host", "objects:container"} <= {label for label, _ in recorded}
    first = recorded[0][1]
    assert isinstance(first, Deadline)
    assert all(deadline is first for label, deadline in recorded if label != "probe"), [
        label for label, deadline in recorded if deadline is not first
    ]
    # The probe's own deadline ends by min(now + 10 s, the pass deadline - 2 s).
    clock = NativeClock()
    for label, deadline in recorded:
        if label == "probe":
            assert deadline.remaining_ns(clock) <= min(10e9, first.remaining_ns(clock) - 2e9)


def test_observation_pass_host_budget_exhaustion_is_late(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> None:
    _services, _handle, fake, reads = _active_pass(tmp_path, monkeypatch, request)
    topology = "native" if sys.platform == "linux" else "desktop"
    reads.topology = topology
    deadline = Deadline.after(NativeClock(), 0.3)

    def slow_engine(docker: object, path: str, given: object) -> EngineReply:
        assert given is deadline
        while deadline.remaining_ns(NativeClock()):
            time.sleep(0.02)
        return reads.engine(docker, path, given)

    with bind_test_observation_reads(engine=slow_engine), pytest.raises(ResourceError) as raised:
        lifecycle.observe_capacity(fake, topology, frozenset(), deadline=deadline)

    assert raised.value.reason == "observation-deadline"
    assert raised.value.observer == "host"


def test_native_capacity_counts_a_restarting_container_by_its_limit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> None:
    """A foreign container restarting (Running with Pid 0) counts by its hard limit and never refuses admission."""
    _services, handle, fake, reads = _active_pass(tmp_path, monkeypatch, request)
    topology = "native" if sys.platform == "linux" else "desktop"  # the client platform fixes it, as in production
    reads.topology = topology
    (target,) = _owned_targets(handle)
    restarting = {
        "Id": "9" * 64,
        "State": {"Running": True, "Restarting": True, "Pid": 0},
        "HostConfig": {"Memory": GIB},
        "Config": {"Labels": {}},
    }

    with bind_test_observation_reads(engine=reads.engine, host=reads.host):
        _daemon_id, snapshot = lifecycle.observe_capacity(
            fake,
            topology,
            frozenset({target}),
            inventory=[*fake.objects("container"), restarting],
            deadline=Deadline.after(NativeClock(), 30),
        )
    decision = lifecycle.assess_capacity(PhaseBudget(GIB, GIB, 1, 1), snapshot)

    assert snapshot.external_limits == (GIB,)
    assert (decision.state, decision.reason) == (lifecycle.CapacityState.READY, "static-capacity")


def test_observation_reads_binding_restores_defaults(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    first, second = (lambda *_args: None), (lambda *_args: 0)

    with bind_test_observation_reads(engine=first):
        assert lifecycle._READS.engine is first
    with bind_test_observation_reads(rss=second):
        assert lifecycle._READS.engine is None  # the first binding is not inherited
        assert lifecycle._READS.rss is second
    with (
        pytest.raises(RuntimeError, match="boom"),
        bind_test_observation_reads(engine=first, rss=second, host=first),
    ):
        raise RuntimeError("boom")

    assert (lifecycle._READS.engine, lifecycle._READS.rss, lifecycle._READS.host) == (None, None, None)


def _owned_targets(handle) -> list[str]:
    record = json.loads(handle._context.record.read_text())
    return sorted(
        identifier
        for identifier, item in record["owned_resources"]["containers"].items()
        if item["evidence"].get("control") is not None
    )


def _add_managed(handle, fake: FakeDockerPort, identifiers: tuple[str, ...]) -> None:
    """Record lease-facade Compose containers in ``resources`` so the pass observes them through its managed path."""
    record = json.loads(handle._context.record.read_text())
    labels = {
        lifecycle.PROJECT: record["project_id"],
        lifecycle.SESSION: record["session_id"],
        lifecycle.RUN: record["run_id"],
    }
    containers = {}
    for identifier in identifiers:
        fake.containers[identifier] = _container(identifier, f"managed-{identifier[:4]}", labels, running=True)
        containers[identifier] = {
            "service": "managed",
            "created": "container-created",
            "image_id": IMAGE_ID,
            "memory_bytes": 1,
            "nano_cpus": 1,
            "restart_policy": "no",
            "labels": labels,
            "network_ids": [],
            "volume_names": [],
            "bindings": {},
            "state": "running",
            "observation": asdict(
                lifecycle.ContainerObservation(
                    record["grant"]["daemon_id"], identifier, "started-0", 0, 1, 1, 0, False, 0
                )
            ),
        }
    record["resources"] = {
        "compose": {
            "project": "managed",
            "files": [{"path": str(handle._context.project.root / "compose.yml"), "sha256": "a" * 64}],
            "services": ["managed"],
            "labels": labels,
        },
        "containers": containers,
        "networks": {},
        "volumes": {},
    }
    lifecycle.atomic_json(handle._context.record, record)


def _pass(services, handle, reads: _Reads, late: lifecycle._LateAttempts | None = None, **keywords) -> dict:
    """Run one guardian observation pass with the scripted reads bound; return the record afterwards."""
    with bind_test_observation_reads(engine=reads.engine, rss=reads.rss, host=reads.host):
        lifecycle._observe_active_run(handle._context, late, **keywords)
    return json.loads(handle._context.record.read_text())


def _failure(record: dict) -> tuple[str | None, dict | None]:
    return record["infrastructure_failure"], record.get("failure_evidence")


def test_observation_pass_unclassified_error_is_telemetry_unreadable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> None:
    services, handle, _fake, reads = _active_pass(tmp_path, monkeypatch, request)
    reads.script["info"] = ["unclassified"]

    failure, evidence = _failure(_pass(services, handle, reads))

    assert failure == "telemetry-unreadable"
    assert evidence is not None
    assert (evidence["category"], evidence["detail"], evidence["observer"]) == (
        "telemetry-unreadable",
        "unclassified",
        "engine",
    )
    assert evidence["reason"] == "Engine said no."  # the error text survives the run's private logs
    assert '"telemetry-unavailable"' not in Path(lifecycle.__file__).read_text()


def test_observation_pass_raw_oserror_is_unclassified_with_step_observer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> None:
    services, handle, _fake, reads = _active_pass(tmp_path, monkeypatch, request)
    reads.script["rss"] = ["unreadable"]

    failure, evidence = _failure(_pass(services, handle, reads))

    assert failure == "telemetry-unreadable"
    assert evidence is not None
    assert (evidence["detail"], evidence["observer"]) == ("unclassified", "process")


def test_observation_pass_host_failure_inside_capacity_read_is_attributed_to_host(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> None:
    _services, handle, _fake, reads = _active_pass(tmp_path, monkeypatch, request)

    def deny(stage: str) -> None:
        if stage == "observation-host-sample":
            raise OSError(errno.EACCES, "denied")

    install_failpoint(deny)
    try:
        with bind_test_observation_reads(engine=reads.engine, rss=reads.rss):
            lifecycle._observe_active_run(handle._context)
    finally:
        install_failpoint(None)

    failure, evidence = _failure(json.loads(handle._context.record.read_text()))
    assert failure == "telemetry-unreadable"
    assert evidence is not None
    assert (evidence["category"], evidence["observer"]) == ("telemetry-unreadable", "host")


def test_observation_pass_host_budget_exhaustion_is_one_late_attempt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> None:
    _services, handle, _fake, reads = _active_pass(tmp_path, monkeypatch, request)
    reads.script["info"] = ["slow"]
    late = lifecycle._LateAttempts()

    with bind_test_observation_reads(engine=reads.engine, rss=reads.rss):
        lifecycle._observe_active_run(handle._context, late, _budget_seconds=0.4)

    assert _failure(json.loads(handle._context.record.read_text())) == (None, None)
    assert late.count == 1


def test_observation_pass_two_lates_write_nothing_third_writes_observation_late(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> None:
    services, handle, _fake, reads = _active_pass(tmp_path, monkeypatch, request)
    reads.script["info"] = ["late"]
    late = lifecycle._LateAttempts()

    assert _failure(_pass(services, handle, reads, late)) == (None, None)
    first_tick = late.first_tick
    assert _failure(_pass(services, handle, reads, late)) == (None, None)
    failure, evidence = _failure(_pass(services, handle, reads, late))

    assert first_tick is not None
    assert failure == "observation-late"
    assert evidence is not None
    assert (evidence["category"], evidence["reason"], evidence["observer"]) == (
        "observation-late",
        "observation-late",
        "engine",
    )
    assert evidence["first_observed"]["tick_ns"] == first_tick[1]
    assert evidence["first_observed"]["state"] == "active"


def test_observation_pass_complete_sample_resets_late_count(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> None:
    services, handle, _fake, reads = _active_pass(tmp_path, monkeypatch, request)
    reads.script["rss"] = ["late", "ok", "late", "late"]  # one RSS read per pass: the pass is the unit being scripted
    late = lifecycle._LateAttempts()

    for _ in range(4):
        record = _pass(services, handle, reads, late)

    assert _failure(record) == (None, None)
    assert late.count == 2


def test_observation_pass_unreadable_host_sample_is_sticky_on_first_pass(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> None:
    services, handle, _fake, reads = _active_pass(tmp_path, monkeypatch, request)
    reads.script["host"] = ["unreadable"]

    failure, evidence = _failure(_pass(services, handle, reads))

    assert failure == "telemetry-unreadable"
    assert evidence is not None
    assert (evidence["observer"], evidence["detail"]) == ("host", None)


def test_observation_pass_daemon_change_is_its_own_category(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> None:
    services, handle, _fake, reads = _active_pass(tmp_path, monkeypatch, request)
    reads.script["info"] = ["daemon-changed"]

    failure, evidence = _failure(_pass(services, handle, reads))

    assert failure == "daemon-identity-changed"
    assert evidence is not None
    assert (evidence["category"], evidence["observer"]) == ("daemon-identity-changed", "engine")


def test_observation_pass_a_capacity_change_ends_no_granted_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> None:
    """After the grant the daemon shrinking below the budget is no breach; the late container read still counts."""
    services, handle, _fake, reads = _active_pass(tmp_path, monkeypatch, request)
    (target,) = _owned_targets(handle)
    reads.script["info"] = ["tiny"]
    reads.script[target] = ["late"]
    late = lifecycle._LateAttempts()

    assert _failure(_pass(services, handle, reads, late)) == (None, None)
    assert late.count == 1


def test_observation_pass_container_oom_survives_later_container_timeout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> None:
    services, handle, fake, reads = _active_pass(tmp_path, monkeypatch, request)
    first, second = "1" * 64, "2" * 64
    _add_managed(handle, fake, (first, second))
    reads.script[first] = ["oom"]
    reads.script[second] = ["late"]
    late = lifecycle._LateAttempts()

    failure, evidence = _failure(_pass(services, handle, reads, late))

    assert failure == "container-oom"
    assert evidence is not None
    assert (evidence["category"], evidence["reason"], evidence["observer"]) == ("breach", "container-oom", "container")
    assert late.count == 0


def test_observation_pass_owned_control_oom_survives_later_owned_timeout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> None:
    services, handle, _fake, reads = _active_pass(tmp_path, monkeypatch, request, services=("alpha", "beta"))
    first, second = _owned_targets(handle)
    reads.script[first] = ["oom"]
    reads.script[second] = ["late"]
    late = lifecycle._LateAttempts()

    failure, evidence = _failure(_pass(services, handle, reads, late))

    assert failure == "container-oom"
    assert evidence is not None
    assert evidence["observer"] == "container"
    assert late.count == 0


@pytest.mark.parametrize("outcome", ("exited", "exiting"))
def test_observation_pass_owned_container_exiting_during_the_read_is_a_stop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest, outcome: str
) -> None:
    """An owned container may exit on its own between the exact inspection and the Engine reads."""
    services, handle, _fake, reads = _active_pass(tmp_path, monkeypatch, request)
    (target,) = _owned_targets(handle)
    reads.script[target] = [outcome]

    record = _pass(services, handle, reads, lifecycle._LateAttempts())

    assert _failure(record) == (None, None)
    owned = record["owned_resources"]["containers"][target]
    assert owned["state"] == "stopped"
    assert (owned["evidence"]["control"]["oom_killed"], owned["evidence"]["control"]["oom_events"]) == (False, 0)


def test_observation_pass_owned_container_oom_killed_during_the_read_is_an_oom(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> None:
    services, handle, _fake, reads = _active_pass(tmp_path, monkeypatch, request)
    (target,) = _owned_targets(handle)
    reads.script[target] = ["exiting-oom"]

    failure, evidence = _failure(_pass(services, handle, reads, lifecycle._LateAttempts()))

    assert failure == "container-oom"
    assert evidence is not None
    assert (evidence["category"], evidence["observer"]) == ("breach", "container")


@pytest.mark.parametrize("outcome", ("dying", "restarted-exited"))
def test_observation_pass_unreadable_owned_container_not_proven_stopped_still_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest, outcome: str
) -> None:
    """Only a stop in the inspected generation excuses the read: still running or a newer generation stays a failure."""
    services, handle, _fake, reads = _active_pass(tmp_path, monkeypatch, request)
    (target,) = _owned_targets(handle)
    reads.script[target] = [outcome]

    failure, evidence = _failure(_pass(services, handle, reads, lifecycle._LateAttempts()))

    assert failure is not None
    assert evidence is not None
    assert evidence["observer"] == "container"


def test_observation_pass_healthy_container_then_late_container_is_a_late_attempt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> None:
    services, handle, _fake, reads = _active_pass(tmp_path, monkeypatch, request, services=("alpha", "beta"))
    first, second = _owned_targets(handle)
    reads.script[first] = ["ok"]
    reads.script[second] = ["late"]
    late = lifecycle._LateAttempts()

    assert _failure(_pass(services, handle, reads, late)) == (None, None)
    assert late.count == 1


def test_observation_pass_publishes_an_oom_before_a_pressure_breach_of_the_pass(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> None:
    """A pass that measures both an OOM and 4 GiB of swap growth publishes the OOM, the run's own fault."""
    services, handle, _fake, reads = _active_pass(tmp_path, monkeypatch, request)
    (target,) = _owned_targets(handle)
    reads.script[target] = ["oom"]
    healthy = reads.host_factory
    assert healthy is not None
    reads.host_factory = lambda: _swapped(healthy(), 4 * GIB)

    failure, evidence = _failure(_pass(services, handle, reads))

    assert failure == "container-oom"
    assert evidence is not None
    assert evidence["observer"] == "container"


def test_observation_pass_complete_pass_with_oom_publishes_container_oom(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> None:
    services, handle, _fake, reads = _active_pass(tmp_path, monkeypatch, request)
    (target,) = _owned_targets(handle)
    reads.script[target] = ["oom"]
    late = lifecycle._LateAttempts(count=1, first_tick=("boot", 1))

    record = _pass(services, handle, reads, late)

    assert _failure(record)[0] == "container-oom"
    assert record["failure_evidence"]["first_observed"]["state"] == "active"
    assert late.count == 0  # a complete pass, even one that measured a breach, is not a late attempt


def test_observation_pass_mutation_pending_skips_container_reads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> None:
    services, handle, _fake, reads = _active_pass(tmp_path, monkeypatch, request)
    (target,) = _owned_targets(handle)
    record = json.loads(handle._context.record.read_text())
    control = record["owned_resources"]["containers"][target]["evidence"]["control"]
    record["pending_operation"] = {
        "id": "33333333-3333-4333-8333-333333333333",
        "kind": "container-control",
        "generation": record["generation"],
        "revision": record["revision"] + 1,
        "actor": record["requester"],
        "deadline": asdict(Deadline.after(NativeClock(), 30)),
        "plan": {
            "action": "stop",
            "targets": [target],
            "expected": {target: {"started_at": control["started_at"], "restart_count": control["restart_count"]}},
            "request_sha256": "e" * 64,
        },
    }
    record["revision"] = record["pending_operation"]["revision"]
    lifecycle.atomic_json(handle._context.record, record)
    late = lifecycle._LateAttempts(count=1, first_tick=("boot", 1))

    observed = _pass(services, handle, reads, late)

    assert not [path for path in reads.paths if path.startswith("/containers/")]
    assert _failure(observed) == (None, None)
    assert late.count == 0


def test_observation_pass_sustained_swap_growth_after_two_lates_is_detected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> None:
    services, handle, _fake, reads = _active_pass(tmp_path, monkeypatch, request)
    reads.script["info"] = ["late", "late", "ok"]
    late = lifecycle._LateAttempts()
    healthy = reads.host_factory
    assert healthy is not None
    reads.host_factory = lambda: _swapped(healthy(), 4 * GIB)

    assert _failure(_pass(services, handle, reads, late)) == (None, None)
    assert _failure(_pass(services, handle, reads, late)) == (None, None)
    failure, evidence = _failure(_pass(services, handle, reads, late))

    assert failure == "admission-swap-growth"
    assert evidence is not None
    assert (evidence["category"], evidence["observer"]) == ("breach", "host")


def _swapped(sample: lifecycle.HostMemoryObservation, extra: int) -> lifecycle.HostMemoryObservation:
    return lifecycle.HostMemoryObservation(
        sample.origin,
        sample.tick_ns,
        sample.physical_bytes,
        sample.headroom_bytes,
        sample.swap_used_bytes + extra,
        sample.swapout_bytes,
        sample.page_size,
        sample.raw,
    )


@pytest.mark.parametrize(
    ("raised", "reason"),
    (
        (ResourceError("late", reason="observation-deadline", observer="host"), "observation-deadline"),
        (OSError(errno.EACCES, "denied"), "telemetry-unreadable"),
    ),
    ids=("late", "unreadable"),
)
def test_observation_transition_sample_refuses_with_the_classified_reason_and_writes_nothing(
    tmp_path: Path, raised: Exception, reason: str
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    handle = build_test_services(tmp_path / "state").open_run(_spec(project), docker_factory=FakeDockerPort).enter()
    before = handle._context.record.read_bytes()

    def fail_sample(stage: str) -> None:
        if stage == "observation-host-sample":
            raise raised

    install_failpoint(fail_sample)
    try:
        with pytest.raises(ResourceError) as refused:
            lifecycle.transition_phase(handle._lease, "tests")
    finally:
        install_failpoint(None)

    assert (refused.value.reason, refused.value.observer) == (reason, "host")
    assert handle._context.record.read_bytes() == before  # the guardian's own observation owns the sticky failure
    handle.leave()


_DAEMON = "11111111-1111-1111-1111-111111111111"
_CONTAINER = "a" * 64


def _native_engine(stats: dict):
    """An Engine answering like Docker 28 on native Linux: the one-shot stats body carries no top-level ``id``."""

    def engine(_docker: object, path: str, _deadline: object) -> EngineReply:
        if path == "/info":
            return EngineReply({"ID": _DAEMON, "OSType": "linux", "CgroupVersion": "2"}, b"", 0)
        if path.endswith("/json"):
            return EngineReply(
                {
                    "Id": _CONTAINER,
                    "State": {"StartedAt": "2026-10-12T22:12:00Z", "Running": True, "OOMKilled": False},
                    "HostConfig": {"Memory": 268435456},
                    "RestartCount": 0,
                },
                b"",
                0,
            )
        return EngineReply(stats, b"", 0)

    return engine


def test_native_linux_stats_without_a_top_level_id_are_read() -> None:
    stats = {"memory_stats": {"usage": 17502208, "limit": 268435456, "stats": {"anon": 1}}}  # cgroup v2: no failcnt

    with bind_test_observation_reads(engine=_native_engine(stats)):
        observed = lifecycle.observe_container(SimpleNamespace(engine="docker", binding=None), _DAEMON, _CONTAINER)

    assert (observed.memory_usage_bytes, observed.memory_limit_bytes, observed.oom_events) == (17502208, 268435456, 0)


def test_stats_naming_another_container_stay_unreadable() -> None:
    stats = {"id": "b" * 64, "memory_stats": {"usage": 1, "limit": 268435456, "stats": {}}}

    with bind_test_observation_reads(engine=_native_engine(stats)), pytest.raises(ResourceError) as raised:
        lifecycle.observe_container(SimpleNamespace(engine="docker", binding=None), _DAEMON, _CONTAINER)

    assert raised.value.reason == "telemetry-unreadable"


# ---- pressure while sharing: the observer passes the allowance and the shared gate ---------------


def _controlled_baseline(handle, headroom: int) -> lifecycle.HostMemoryObservation:
    """Rewrite the run's admission and phase baselines to one controlled sample (the test record-write rule)."""
    record = json.loads(handle._context.record.read_text())
    sample = lifecycle._host_observation(record["admission_observation"]["memory"])
    controlled = lifecycle.HostMemoryObservation(
        sample.origin, sample.tick_ns, 64 * GIB, headroom, 0, sample.swapout_bytes, sample.page_size, sample.raw
    )
    window = lifecycle.HostMemoryObservation(
        sample.origin,
        sample.tick_ns - lifecycle._WINDOW_NS,
        64 * GIB,
        headroom,
        0,
        sample.swapout_bytes,
        sample.page_size,
        sample.raw,
    )
    record["admission_observation"].update(
        memory=asdict(controlled), window_start=asdict(window), samples=[asdict(controlled)]
    )
    record["phase_history"][-1]["memory"] = asdict(controlled)
    lifecycle.atomic_json(handle._context.record, record)
    return controlled


def _managed_coholder(handle) -> Callable[[], None]:
    """Give a live co-holder the observed run's ``managed`` scope, so the private account's canonical binding trusts
    its record; its guardian is paused. Returns the restore: entry record back, guardian resumed, then the leave (call
    it after the canonical binding is undone)."""
    context = handle._context
    pid = pause_guardian(context)
    entry = json.loads(context.record.read_text())

    def restore() -> None:
        lifecycle.atomic_json(context.record, entry)
        resume_guardian(pid)
        handle.leave()

    record = json.loads(context.record.read_text())
    record["grant"]["scope"] = "managed"
    lifecycle.atomic_json(context.record, record)
    return restore


@pytest.mark.parametrize(
    ("cell", "decline_gib", "headroom_gib", "grant_after_baseline", "coholder", "expected"),
    [
        ("allowance-excuses-decline", 3.5, 32, True, "present", None),
        ("empty-allowance-decline", 3.5, 32, False, "phantom", None),  # granted before, gone: a decline never fails
        ("after-departure", 3.5, 32, True, "departed", None),
        ("beyond-allowance-decline", 7.5, 32, True, "present", None),
        ("never-shared-floor-fails", 0.5, 1.5, None, "none", "native-headroom"),
        ("shared-floor-excused", 0.5, 1.5, True, "departed", None),
        ("coholder-seen-floor-excused", 0.5, 1.5, None, "present", None),  # granted, log entry not yet appended
        ("coholder-seen-decline-excused", 3.5, 32, None, "present", None),  # C4: its budget counts from its record
    ],
)
def test_shared_observation_passes_allowance_and_gate_to_assess_pressure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
    cell: str,
    decline_gib: float,
    headroom_gib: float,
    grant_after_baseline: bool | None,
    coholder: str,
    expected: str | None,
) -> None:
    del cell
    with shared_world(tmp_path) as world:
        observed = world.open(shared=True, declare_image=False)
        _ = observed.docker_context
        baseline = _controlled_baseline(observed, int(headroom_gib * GIB))
        account = world.services.root / "admission"
        if coholder == "phantom":  # a run granted before the observed run's baseline that has since left
            phantom = Grant("f" * 8, 1, baseline.origin, baseline.tick_ns - 1_000_000_000, 4 * GIB)
            sharing.append_grant(account, phantom, holder_baselines={})
        elif coholder != "none":
            other = world.open(shared=True, declare_image=False)
            sharing.write_pending(account, [])
            (account / sharing.GRANTS_FILE).unlink(missing_ok=True)  # its own publication beside the observed run
            if grant_after_baseline is not None:
                offset = 1_000_000_000 if grant_after_baseline else -1_000_000_000
                grant = Grant(other.run_id, 1, baseline.origin, baseline.tick_ns + offset, 4 * GIB)
                sharing.append_grant(account, grant, holder_baselines={})
            if coholder == "departed":
                _leave(world, other)
            else:
                restore = _managed_coholder(other)
                request.addfinalizer(restore)  # LIFO: runs after the monkeypatch undo below
        reads = _as_guardian(observed, world.fake, monkeypatch, request)
        reads.host_factory = lambda: lifecycle.HostMemoryObservation(
            baseline.origin,
            baseline.tick_ns + 1,
            baseline.physical_bytes,
            baseline.headroom_bytes - int(decline_gib * GIB) if decline_gib < headroom_gib else 0,
            0,
            baseline.swapout_bytes,
            baseline.page_size,
            baseline.raw,
        )
        if decline_gib < 1:  # the floor cells: headroom below 1.5 GiB with a small decline
            reads.host_factory = lambda: lifecycle.HostMemoryObservation(
                baseline.origin,
                baseline.tick_ns + 1,
                baseline.physical_bytes,
                GIB,
                0,
                baseline.swapout_bytes,
                baseline.page_size,
                baseline.raw,
            )

        try:
            failure, _evidence = _failure(_pass(world.services, observed, reads))
        finally:
            monkeypatch.undo()  # the co-holder's restore and leave need the real account binding

        assert failure == expected


def test_allowance_survives_a_phase_transition_after_the_coholder_grant(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> None:
    """Admission baseline before B's grant, phase baseline after it: B still counts (keyed to admission)."""
    with shared_world(tmp_path) as world:
        observed = world.open(shared=True, declare_image=False)
        _ = observed.docker_context
        baseline = _controlled_baseline(observed, 32 * GIB)
        other = world.open(shared=True, declare_image=False)
        account = world.services.root / "admission"
        (account / sharing.GRANTS_FILE).unlink(missing_ok=True)
        grant = Grant(other.run_id, 1, baseline.origin, baseline.tick_ns + 1_000_000_000, 4 * GIB)
        sharing.append_grant(account, grant, holder_baselines={})
        _leave(world, other)
        record = json.loads(observed._context.record.read_text())
        phase = {**record["phase_history"][-1]["memory"], "tick_ns": baseline.tick_ns + 2_000_000_000}
        record["phase_history"][-1]["memory"] = phase  # the transition happened after B's grant
        lifecycle.atomic_json(observed._context.record, record)
        reads = _as_guardian(observed, world.fake, monkeypatch, request)
        reads.host_factory = lambda: lifecycle.HostMemoryObservation(
            baseline.origin,
            baseline.tick_ns + 3_000_000_000,
            baseline.physical_bytes,
            baseline.headroom_bytes - int(3.5 * GIB),
            0,
            baseline.swapout_bytes,
            baseline.page_size,
            baseline.raw,
        )
        try:
            failure, _evidence = _failure(_pass(world.services, observed, reads))
        finally:
            monkeypatch.undo()

        assert failure is None


def test_unreadable_grants_log_is_recorded_once_and_fails_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> None:
    with shared_world(tmp_path) as world:
        observed = world.open(shared=True, declare_image=False)
        _ = observed.docker_context
        baseline = _controlled_baseline(observed, 32 * GIB)
        log = world.services.root / "admission" / sharing.GRANTS_FILE
        log.write_text("{broken")
        log.chmod(0o600)
        reads = _as_guardian(observed, world.fake, monkeypatch, request)
        reads.host_factory = lambda: lifecycle.HostMemoryObservation(
            baseline.origin,
            baseline.tick_ns + 1,
            baseline.physical_bytes,
            baseline.headroom_bytes - 5 * GIB,  # beyond the unadjusted threshold: excused only as "has shared"
            0,
            baseline.swapout_bytes,
            baseline.page_size,
            baseline.raw,
        )
        try:
            _pass(world.services, observed, reads)
            observed_twice = _pass(world.services, observed, reads)
        finally:
            monkeypatch.undo()
            log.unlink()

        assert _failure(observed_twice) == (None, None)
        items = [
            item for item in observed_twice["hold_timeline"]["waiting"] if item["reason"] == "shared-grants-unreadable"
        ]
        assert len(items) == 1


@pytest.mark.parametrize(("decline_gib", "expected"), [(7.5, None)])
def test_holding_incumbent_granted_before_admission_counts_while_it_holds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
    decline_gib: float,
    expected: str | None,
) -> None:
    """B was granted before A's admission and still holds: B ramping is a neighbour's pressure, and a host decline
    never fails A, within B's 4 GiB budget or beyond it."""
    with shared_world(tmp_path) as world:
        earlier = world.open(shared=True, declare_image=False)
        observed = world.open(shared=True, declare_image=False)
        _ = observed.docker_context
        (world.services.root / "admission" / sharing.GRANTS_FILE).unlink(missing_ok=True)
        baseline = _controlled_baseline(observed, 32 * GIB)
        assert json.loads(earlier._context.record.read_text())["hold_timeline"]["granted_ns"] < baseline.tick_ns
        request.addfinalizer(_managed_coholder(earlier))
        reads = _as_guardian(observed, world.fake, monkeypatch, request)
        reads.host_factory = lambda: lifecycle.HostMemoryObservation(
            baseline.origin,
            baseline.tick_ns + 1,
            baseline.physical_bytes,
            baseline.headroom_bytes - int(decline_gib * GIB),
            0,
            baseline.swapout_bytes,
            baseline.page_size,
            baseline.raw,
        )
        try:
            failure, _evidence = _failure(_pass(world.services, observed, reads))
        finally:
            monkeypatch.undo()

        assert failure == expected


def test_an_incumbent_leaving_between_the_sample_and_the_coholder_read_still_excuses_the_sample(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> None:
    """B held when the host sample was taken and its memory is in that sample; B's queue entry is gone by the later
    co-holder read. B's budget still excuses A's 3.5 GiB decline: the holders read just before the sample count."""
    with shared_world(tmp_path) as world:
        earlier = world.open(shared=True, declare_image=False)
        observed = world.open(shared=True, declare_image=False)
        _ = observed.docker_context
        account = world.services.root / "admission"
        (account / sharing.GRANTS_FILE).unlink(missing_ok=True)
        baseline = _controlled_baseline(observed, 32 * GIB)
        request.addfinalizer(_managed_coholder(earlier))
        queue = account / "queue.json"
        held_queue = queue.read_bytes()
        request.addfinalizer(lambda: queue.write_bytes(held_queue))  # LIFO: back before B's restore and leave
        reads = _as_guardian(observed, world.fake, monkeypatch, request)

        def sample_then_b_leaves() -> lifecycle.HostMemoryObservation:
            state = json.loads(queue.read_text())
            state["entries"].pop(earlier.run_id, None)
            queue.write_text(json.dumps(state))
            return lifecycle.HostMemoryObservation(
                baseline.origin,
                baseline.tick_ns + 1,
                baseline.physical_bytes,
                baseline.headroom_bytes - int(3.5 * GIB),
                0,
                baseline.swapout_bytes,
                baseline.page_size,
                baseline.raw,
            )

        reads.host_factory = sample_then_b_leaves
        try:
            failure, _evidence = _failure(_pass(world.services, observed, reads))
        finally:
            monkeypatch.undo()

        assert failure is None
