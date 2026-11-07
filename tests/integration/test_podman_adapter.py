"""The Podman machine port's CLI adapter against a scripted ``podman``."""

from __future__ import annotations

import os
import pwd
import subprocess
import sys
import time
from pathlib import Path

import pytest

from docker_test_toolkit.adapters.outbound import podman as podman_module
from docker_test_toolkit.adapters.outbound.podman import PodmanCliAdapter
from docker_test_toolkit.application.lifecycle import Deadline, NativeClock
from docker_test_toolkit.domain.errors import InfrastructureFailure
from docker_test_toolkit.domain.models import MachineFacts
from tests.integration.scripted_clis import IMAGE_DIR, W0_CREATED, W0_SOCKET, fake_podman, machine_inspect, machine_list

pytestmark = pytest.mark.integration


def _adapter(tmp_path: Path, **extra: str) -> tuple[PodmanCliAdapter, Path]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    fake_podman(bin_dir)
    log = tmp_path / "podman.log"
    environment = {
        "PATH": f"{bin_dir}:/usr/bin:/bin",
        "TMPDIR": str(tmp_path / "toolkit-private-tmp"),
        "HOME": "/elsewhere",
        "DTT_PODMAN_LOG": str(log),
        **extra,
    }
    return PodmanCliAdapter(environment), log


def _body(tmp_path: Path, name: str, text: str) -> str:
    path = tmp_path / name
    path.write_text(text)
    return str(path)


def test_the_adapter_reads_running_machines_facts_and_the_image_dir(tmp_path: Path) -> None:
    adapter, _log = _adapter(tmp_path)

    assert adapter.running(None) == ("dtt-w0",)
    assert adapter.inspect("dtt-w0", None) == MachineFacts("dtt-w0", W0_CREATED, False, W0_SOCKET, 3072, 2)
    assert adapter.image_dir(None) == Path(IMAGE_DIR)


def test_only_running_machines_are_listed(tmp_path: Path) -> None:
    listed = _body(tmp_path, "list.json", machine_list(("old", False), ("dtt-w0", True)))
    adapter, _log = _adapter(tmp_path, DTT_MACHINE_LIST=listed)

    assert adapter.running(None) == ("dtt-w0",)


def test_the_podman_child_sees_the_darwin_user_temporary_folder_verbatim(tmp_path: Path) -> None:
    adapter, log = _adapter(tmp_path)

    adapter.running(None)

    temporary, home, argv = log.read_text().splitlines()[0].split("|")
    if sys.platform == "darwin":  # the folder podman derives machine sockets from, never the toolkit's private one
        expected = subprocess.run(
            ["/usr/bin/getconf", "DARWIN_USER_TEMP_DIR"], capture_output=True, text=True, check=True
        ).stdout.strip()
    else:  # the adapter serves darwin clients; elsewhere TMPDIR passes through
        expected = str(tmp_path / "toolkit-private-tmp")
    assert (temporary, home, argv) == (expected, pwd.getpwuid(os.getuid()).pw_dir, "machine list --format json")


def test_a_failed_or_late_call_is_a_client_exit_with_bounded_stderr(tmp_path: Path) -> None:
    (tmp_path / "failing").mkdir()
    (tmp_path / "slow").mkdir()
    failing, _ = _adapter(tmp_path / "failing", DTT_PODMAN_FAIL="1")
    slow, _ = _adapter(tmp_path / "slow", DTT_PODMAN_SLEEP="1")

    with pytest.raises(InfrastructureFailure) as failed:
        failing.running(None)
    started = time.monotonic()
    with pytest.raises(InfrastructureFailure) as late:
        slow.running(Deadline.after(NativeClock(), 0.3))

    assert failed.value.reason == "client-exit" and "boom" in str(failed.value)
    assert len(str(failed.value).partition("podman said: ")[2]) <= podman_module.PODMAN_STDERR_LIMIT
    assert late.value.reason == "client-exit" and time.monotonic() - started < 3


def _running(pid: int) -> bool:
    """Whether a process runs: a killed child that nobody reaps yet (a container without an init) is a zombie, which
    ``kill(pid, 0)`` still finds, so its ``/proc`` state decides where the host has one."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    stat = Path(f"/proc/{pid}/stat")
    return not (stat.exists() and stat.read_text().rpartition(")")[2].split()[0] == "Z")


def test_a_late_call_ends_its_whole_process_group(tmp_path: Path) -> None:
    child = tmp_path / "child.pid"
    slow, _ = _adapter(tmp_path, DTT_PODMAN_SLEEP="1", DTT_PODMAN_CHILD=str(child))

    with pytest.raises(InfrastructureFailure):
        slow.running(Deadline.after(NativeClock(), 0.5))

    # The podman process's own children end with it: a hung helper never outlives the call's deadline.
    pid = int(child.read_text())
    for _ in range(50):
        if not _running(pid):
            break
        time.sleep(0.05)
    else:
        os.kill(pid, 9)
        pytest.fail("the timed-out call left its child running")


def test_a_machine_that_is_not_running_is_a_client_exit(tmp_path: Path) -> None:
    stopped = _body(tmp_path, "inspect.json", machine_inspect(state="stopped"))
    adapter, _log = _adapter(tmp_path, DTT_MACHINE_INSPECT=stopped)

    with pytest.raises(InfrastructureFailure) as refused:
        adapter.inspect("dtt-w0", None)

    assert refused.value.reason == "client-exit" and "stopped" in str(refused.value)


@pytest.mark.parametrize(
    ("variable", "body"),
    [
        ("DTT_MACHINE_LIST", '[{"Name": "dtt-w0"}]'),  # no Running
        ("DTT_MACHINE_INSPECT", "[]"),
        ("DTT_MACHINE_INSPECT", machine_inspect(name="another")),
        ("DTT_MACHINE_INSPECT", machine_inspect(socket="podman/dtt-w0-api.sock")),  # not absolute
        ("DTT_MACHINE_INSPECT", machine_inspect(socket=W0_SOCKET + "\n")),
        ("DTT_MACHINE_INSPECT", machine_inspect(memory=0)),
        ("DTT_MACHINE_INFO", '{"Host": {}}'),
        ("DTT_MACHINE_INFO", '{"Host": "applehv"}'),  # not an object
        ("DTT_MACHINE_LIST", "not json"),
    ],
)
def test_an_unexpected_shape_fails_closed(tmp_path: Path, variable: str, body: str) -> None:
    adapter, _log = _adapter(tmp_path, **{variable: _body(tmp_path, "body.json", body)})
    calls = {
        "DTT_MACHINE_LIST": lambda: adapter.running(None),
        "DTT_MACHINE_INSPECT": lambda: adapter.inspect("dtt-w0", None),
        "DTT_MACHINE_INFO": lambda: adapter.image_dir(None),
    }

    with pytest.raises(InfrastructureFailure) as refused:
        calls[variable]()

    assert refused.value.reason == "unspecified"


def test_a_changed_binary_is_an_identity_change(tmp_path: Path) -> None:
    adapter, _log = _adapter(tmp_path)
    adapter.running(None)
    script = tmp_path / "bin" / "podman"
    script.write_text(script.read_text() + "# replaced\n")

    with pytest.raises(InfrastructureFailure) as changed:
        adapter.running(None)

    assert changed.value.reason == "daemon-identity-changed"


def test_a_missing_binary_is_refused_naming_the_install(tmp_path: Path) -> None:
    adapter = PodmanCliAdapter({"PATH": str(tmp_path), "TMPDIR": str(tmp_path)})

    with pytest.raises(InfrastructureFailure) as missing:
        adapter.running(None)

    assert missing.value.reason == "podman-binary-missing" and "brew install podman" in str(missing.value)


def test_the_image_dir_is_read_once_per_process(tmp_path: Path) -> None:
    info = _body(tmp_path, "info.json", '{"Host": {"MachineImageDir": "/Users/x/once"}}')
    adapter, log = _adapter(tmp_path, DTT_MACHINE_INFO=info)

    first, second = adapter.image_dir(None), adapter.image_dir(None)

    assert first == second == Path("/Users/x/once")
    assert [line.split("|")[2] for line in log.read_text().splitlines()] == ["machine info --format json"]
