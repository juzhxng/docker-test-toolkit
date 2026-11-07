"""Unit tests of the daemon probe through a fake Docker port."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from docker_test_toolkit.application import probe
from docker_test_toolkit.domain.models import DockerObservation, DockerResult, ExactInspection, ProbeRun
from tests.unit.test_docker_receipt_service import FakeDockerPort

_PSI = "some avg10=0.00 avg60=1.56 avg300=0.00 total=1\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n"
_DF = "Filesystem 1024-blocks Used Available Capacity Mounted on\n/dev/vda1 474095688 21114988 428824492 5% /dtt-data\n"


class _Clock:
    """A fake native clock: a fixed origin and a tick the test moves."""

    origin = "fake-origin"

    def __init__(self) -> None:
        self.now = 1_000_000_000_000

    def read(self) -> int:
        return self.now


class _ProbePort(FakeDockerPort):
    """The receipt fake plus a scripted probe run and image presence; records every probe and pull call."""

    def __init__(self, account: Path, stdout: str = _PSI + "::dtt::\n" + _DF, *, timed_out: bool = False) -> None:
        super().__init__()
        self.account = account
        self.stdout = stdout
        self.timed_out = timed_out
        self.probe_calls: list[tuple[tuple[str, ...], bool]] = []
        self.image_present = True
        self.pull_exit = 0
        self.pulls = 0

    def run_probe(self, argv: tuple[str, ...], *, deadline: object, container_name: str) -> ProbeRun:
        recorded = (self.account / probe.PROBES_DIRECTORY / container_name).is_file()
        self.probe_calls.append((argv, recorded))
        if self.timed_out:
            return ProbeRun("", None, True)
        return ProbeRun(self.stdout, 0, False)

    def inspect_exact(self, request: ExactInspection, *, deadline: object | None = None) -> DockerObservation:
        if request == ExactInspection("image", probe.PROBE_IMAGE):
            return DockerObservation("image", request.identifier, self.image_present, {})
        return super().inspect_exact(request, deadline=deadline)

    def execute(self, argv: tuple[str, ...], **_options: object) -> DockerResult:
        assert argv == ("pull", probe.PROBE_IMAGE)
        self.pulls += 1
        return DockerResult(self.pull_exit, "", "")


def _read(port: _ProbePort, account: Path, clock: _Clock, data_root: object = "/var/lib/docker") -> probe.ProbeResult:
    return probe.DaemonProbe(account).read(
        port, daemon_id="daemon-1", data_root=data_root, deadline=object(), clock=clock
    )


def test_a_probe_reads_pressure_and_disk_and_records_its_container_before_starting_it(tmp_path: Path) -> None:
    port, clock = _ProbePort(tmp_path), _Clock()

    result = _read(port, tmp_path, clock)

    assert result.cpu_pressure == 1.56 and result.daemon_id == "daemon-1"
    assert result.data_root == probe.DiskFigure("data-root", 428824492 * 1024, 474095688 * 1024, None, "measured")
    argv, recorded_before_start = port.probe_calls[0]
    assert recorded_before_start  # a killed process never leaves an unrecorded probe container
    assert "--pull=never" in argv and "type=bind,src=/var/lib/docker,dst=/dtt-data,ro" in argv
    assert list((tmp_path / probe.PROBES_DIRECTORY).iterdir()) == []  # removed once `--rm` removed the container


def test_a_reading_is_reused_for_ten_seconds_of_one_daemon(tmp_path: Path) -> None:
    port, clock = _ProbePort(tmp_path), _Clock()
    reader = probe.DaemonProbe(tmp_path)

    first = reader.read(port, daemon_id="daemon-1", data_root="/var/lib/docker", deadline=object(), clock=clock)
    clock.now += probe.PROBE_CACHE_NS - 1
    again = reader.read(port, daemon_id="daemon-1", data_root="/var/lib/docker", deadline=object(), clock=clock)
    clock.now += 1
    fresh = reader.read(port, daemon_id="daemon-1", data_root="/var/lib/docker", deadline=object(), clock=clock)
    other = reader.read(port, daemon_id="daemon-2", data_root="/var/lib/docker", deadline=object(), clock=clock)

    assert again is first and fresh is not first and other.daemon_id == "daemon-2"
    assert len(port.probe_calls) == 3


def test_another_guardian_of_the_account_reuses_a_fresh_reading(tmp_path: Path) -> None:
    port, clock = _ProbePort(tmp_path), _Clock()

    first = _read(port, tmp_path, clock)  # one guardian probes
    clock.now += probe.PROBE_CACHE_NS - 1
    shared = _read(port, tmp_path, clock)  # another guardian's own reader: no second probe within the interval
    clock.now += 1
    fresh = _read(port, tmp_path, clock)

    assert (shared.cpu_pressure, shared.data_root, shared.read_ns) == (
        first.cpu_pressure,
        first.data_root,
        first.read_ns,
    )
    assert fresh.read_ns == clock.now
    assert len(port.probe_calls) == 2


def test_pressure_and_disk_figures_fail_independently(tmp_path: Path) -> None:
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()  # two accounts: a fresh reading of one account is shared by its guardians
    no_psi = _read(_ProbePort(tmp_path / "a", "::dtt::\n" + _DF), tmp_path / "a", _Clock())
    no_df = _read(_ProbePort(tmp_path / "b", _PSI + "::dtt::\ndf: not found\n"), tmp_path / "b", _Clock())

    assert no_psi.cpu_pressure is None and no_psi.data_root is not None and no_psi.data_root.state == "measured"
    assert no_df.cpu_pressure == 1.56 and no_df.data_root is not None and no_df.data_root.state == "unknown"


def test_a_timed_out_probe_has_no_figures_and_keeps_its_record_for_retention(tmp_path: Path) -> None:
    port = _ProbePort(tmp_path, timed_out=True)

    result = _read(port, tmp_path, _Clock())

    assert (result.cpu_pressure, result.data_root) == (None, None)
    assert len(list((tmp_path / probe.PROBES_DIRECTORY).iterdir())) == 1  # the container may still exist


def test_a_non_absolute_or_quoted_data_root_yields_no_disk_figure(tmp_path: Path) -> None:
    for index, data_root in enumerate(("relative/path", "/var lib/docker", "/a,b", "/a/../etc", None, 7)):
        account = tmp_path / str(index)  # one account each: a fresh reading of one account is shared
        account.mkdir()
        port = _ProbePort(account)

        result = _read(port, account, _Clock(), data_root)

        argv = port.probe_calls[0][0]
        assert not any(value.startswith("type=bind") for value in argv), data_root
        assert result.data_root is not None and result.data_root.state == "unknown"
        assert result.cpu_pressure == 1.56  # the pressure is still read


def test_the_probe_image_is_inspected_before_any_pull_and_never_pulled_when_present(tmp_path: Path) -> None:
    port = _ProbePort(tmp_path)
    lock = SimpleNamespace(state=tmp_path / "state")

    assert probe.ensure_probe_image(port, lock, tmp_path, daemon_id="d1", clock=_Clock()) == "present"
    assert port.pulls == 0


def test_a_failed_pull_backs_off_per_daemon(tmp_path: Path) -> None:
    port, clock = _ProbePort(tmp_path), _Clock()
    port.image_present, port.pull_exit = False, 1
    lock = SimpleNamespace(state=tmp_path / "state")

    first = probe.ensure_probe_image(port, lock, tmp_path, daemon_id="d1", clock=clock)
    clock.now += probe.PROBE_PULL_BACKOFF_NS - 1
    suppressed = probe.ensure_probe_image(port, lock, tmp_path, daemon_id="d1", clock=clock)
    other_daemon = probe.ensure_probe_image(port, lock, tmp_path, daemon_id="d2", clock=clock)
    clock.now += 1
    port.pull_exit = 0
    retried = probe.ensure_probe_image(port, lock, tmp_path, daemon_id="d1", clock=clock)

    assert (first, suppressed, other_daemon, retried) == ("failed", "backoff", "failed", "pulled")
    assert port.pulls == 3  # d1, d2, then d1 after its backoff
    assert probe.read_probe_images(tmp_path)["d1"]["pulled_by_toolkit"] is True


def _data(tmp_path: Path, topology: str) -> dict:
    return {"topology": topology, "account_directory": str(tmp_path)}


def test_a_mac_vm_join_decides_on_the_vms_pressure_and_an_unreadable_one_holds_it(tmp_path: Path) -> None:
    from docker_test_toolkit.application import lifecycle

    missing = tmp_path / "no-host-psi"  # macOS publishes no host PSI
    port = _ProbePort(tmp_path)

    measured = lifecycle._join_pressure(_data(tmp_path, "desktop"), port, "d1", object(), _Clock(), host_psi=missing)
    port.timed_out = True
    blind = lifecycle._join_pressure(
        _data(tmp_path / "other", "desktop"), port, "d1", object(), _Clock(), host_psi=missing
    )

    assert measured == 1.56  # read inside the VM, where the containers compete for CPU
    assert blind == 100.0  # an unreadable VM figure holds joins, as an unreadable host file does


def test_a_linux_host_keeps_its_own_psi_and_never_probes(tmp_path: Path) -> None:
    from docker_test_toolkit.application import lifecycle

    host = tmp_path / "pressure"
    host.write_text(_PSI)
    port = _ProbePort(tmp_path)

    with_file = lifecycle._join_pressure(_data(tmp_path, "native"), port, "d1", object(), _Clock(), host_psi=host)
    without = lifecycle._join_pressure(
        _data(tmp_path, "native"), port, "d1", object(), _Clock(), host_psi=tmp_path / "x"
    )

    assert (with_file, without) == (1.56, None)  # a Linux kernel without PSI keeps deciding on memory alone
    assert port.probe_calls == []


def test_a_held_image_lock_is_busy_not_a_failed_pull(tmp_path: Path) -> None:
    from docker_test_toolkit.application.recovery import Context, image_lock

    port, clock = _ProbePort(tmp_path), _Clock()
    port.image_present = False
    state = tmp_path / "state"
    with image_lock(Context(state, state)):  # another image operation of this account holds K
        outcome = probe.ensure_probe_image(
            port, SimpleNamespace(state=state), tmp_path, daemon_id="d1", clock=clock, pull_bound_s=0.05
        )

    assert outcome == "busy" and port.pulls == 0
    assert "d1" not in probe.read_probe_images(tmp_path)  # no backoff: the next run tries again


def test_a_probe_never_ends_after_its_callers_deadline_minus_two_seconds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from docker_test_toolkit.application import lifecycle

    clock = _Clock()
    seen: list[object] = []

    def read(self, docker, *, daemon_id, data_root, deadline, clock):
        seen.append(deadline)
        return probe.ProbeResult(1.0, None, clock.read(), clock.origin, daemon_id)

    monkeypatch.setattr(probe.DaemonProbe, "read", read)
    monkeypatch.setattr(lifecycle, "_DAEMON_PROBES", {})

    class _Docker:
        def call(self, *_argv: str, deadline: object = None) -> str:
            clock.now += 3_000_000_000  # a slow data-root read: time passes before the probe starts
            return "/var/lib/docker"

    caller = lifecycle.Deadline(clock.origin, clock.now, 8_000_000_000, clock.now + 8_000_000_000)
    lifecycle._probe_reading(str(tmp_path), _Docker(), "d1", caller, clock)
    late = lifecycle.Deadline(clock.origin, clock.now, 1_000_000_000, clock.now + 1_000_000_000)
    empty = lifecycle._probe_reading(str(tmp_path / "other"), _Docker(), "d2", late, clock)

    assert seen[0].expires_ns <= caller.expires_ns - 2_000_000_000  # never extended by the time already spent
    assert len(seen) == 1 and empty.cpu_pressure is None  # no time left: no probe run, no figures
