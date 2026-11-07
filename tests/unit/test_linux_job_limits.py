"""Native Linux job containment: the cgroup-v2 walk from the toolkit's process cgroup up to the mount."""

from __future__ import annotations

from pathlib import Path, PurePosixPath

import pytest

from docker_test_toolkit.application.lifecycle import (
    JobMemoryLimit,
    _container_cgroup_paths,
    _container_stopped,
    _job_limits_in_tree,
)
from docker_test_toolkit.domain.models import DockerObservation

GIB = 1024**3
RUNNER = PurePosixPath("/system.slice/runner.service")
CONTAINER = PurePosixPath("/system.slice/docker-abc.scope")


def _tree(tmp_path: Path, limits: dict[str, str | None], *, kernel_root: bool = True) -> Path:
    """A cgroup-v2 mount with one directory per level; a None limit leaves that level without ``memory.max``.

    Like the kernel, every level has ``cgroup.controllers`` and every non-root level ``cgroup.type``; with
    ``kernel_root=False`` the mount shows a non-root cgroup (a cgroup-namespace root or a bind-mounted subtree).
    """
    mount = tmp_path / "cgroup"
    for level, limit in limits.items():
        directory = mount / level.lstrip("/")
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "cgroup.controllers").write_text("cpu memory\n")
        if level != "/" or not kernel_root:
            (directory / "cgroup.type").write_text("domain\n")
        if limit is not None:
            (directory / "memory.max").write_text(f"{limit}\n")
    return mount


def _walk(
    mount: Path,
    *,
    containers: list[dict] | None = None,
    owned: set[str] | None = None,
    current: PurePosixPath = RUNNER,
):
    containers = containers or []
    paths = {item["Id"]: CONTAINER for item in containers}
    return _job_limits_in_tree(
        PurePosixPath("/"), mount, mount / current.relative_to("/"), containers, paths, owned or set()
    )


def _container(container_id: str, memory: int) -> dict:
    return {"Id": container_id, "State": {"Running": True, "Pid": 4242}, "HostConfig": {"Memory": memory}}


def test_root_without_memory_max_adds_no_ceiling(tmp_path: Path) -> None:
    mount = _tree(tmp_path, {"/": None, "/system.slice": str(8 * GIB), str(RUNNER): "max", str(CONTAINER): "max"})
    containers = [_container("owned", GIB), _container("foreign", 2 * GIB)]

    limits = _walk(mount, containers=containers, owned={"owned"})

    assert limits == (JobMemoryLimit(8 * GIB, GIB, 2 * GIB),)  # the slice limit is still read and attributed


def test_root_alone_without_memory_max_is_unlimited(tmp_path: Path) -> None:
    mount = _tree(tmp_path, {"/": None, "/system.slice": "max", str(RUNNER): "max"})

    assert _walk(mount) == ()


def test_process_in_the_root_cgroup_is_unlimited(tmp_path: Path) -> None:
    mount = _tree(tmp_path, {"/": None})

    assert _walk(mount, current=PurePosixPath("/")) == ()


def test_namespace_root_without_memory_max_stays_unknown(tmp_path: Path) -> None:
    # The mount shows a non-root cgroup whose parent did not enable the memory controller: an ancestor's ceiling is
    # invisible, not absent.
    mount = _tree(tmp_path, {"/": None, "/system.slice": "max", str(RUNNER): "max"}, kernel_root=False)

    with pytest.raises(FileNotFoundError):
        _walk(mount)
    with pytest.raises(FileNotFoundError):
        _walk(mount, current=PurePosixPath("/"))


def test_vanished_mount_directory_stays_unknown(tmp_path: Path) -> None:
    mount = tmp_path / "cgroup"  # no live cgroup directory at the mount

    with pytest.raises(FileNotFoundError):
        _walk(mount, current=PurePosixPath("/"))


def test_missing_memory_max_below_the_root_stays_unknown(tmp_path: Path) -> None:
    mount = _tree(tmp_path, {"/": None, "/system.slice": None, str(RUNNER): "max"})

    with pytest.raises(FileNotFoundError):  # the caller reports unknown-job-containment
        _walk(mount)


def test_unreadable_root_limit_stays_unknown(tmp_path: Path) -> None:
    mount = _tree(tmp_path, {"/": None, "/system.slice": "max", str(RUNNER): "max"})
    (mount / "memory.max").mkdir()

    with pytest.raises(IsADirectoryError):
        _walk(mount)


def test_finite_limit_at_a_namespace_root_is_counted(tmp_path: Path) -> None:
    mount = _tree(tmp_path, {"/": str(4 * GIB), "/system.slice": "max", str(RUNNER): "max"}, kernel_root=False)

    assert _walk(mount) == (JobMemoryLimit(4 * GIB, 0, 0),)


def test_unlimited_namespace_root_adds_no_ceiling(tmp_path: Path) -> None:
    mount = _tree(tmp_path, {"/": "max", "/system.slice": "max", str(RUNNER): "max"}, kernel_root=False)

    assert _walk(mount) == ()


@pytest.mark.parametrize("raw", ["0", "-1", "lots"])
def test_invalid_root_limit_stays_unknown(tmp_path: Path, raw: str) -> None:
    mount = _tree(tmp_path, {"/": raw, "/system.slice": "max", str(RUNNER): "max"})

    assert _walk(mount) is None


def _running(identity: str, pid: int) -> dict:
    return {"Id": identity, "State": {"Running": True, "Pid": pid}, "HostConfig": {"Memory": 1}}


def _read(gone: set[int]):
    def read(pid: int) -> PurePosixPath:
        if pid in gone:
            raise FileNotFoundError(f"/proc/{pid}/cgroup")
        return PurePosixPath(f"/docker/{pid}")

    return read


def test_a_container_that_stopped_since_the_inventory_is_left_out() -> None:
    """A co-holder's container stopping between the inventory and its /proc read holds no memory any more (the
    CI host's unknown-job-containment failure); the other containers are still read."""
    containers = [_running("a" * 64, 10), _running("b" * 64, 11)]
    asked = []

    def is_stopped(identity: str) -> bool:
        asked.append(identity)
        return True

    assert _container_cgroup_paths(containers, is_stopped, _read({11})) == (
        {"a" * 64: PurePosixPath("/docker/10")},
        {"b" * 64},
    )
    assert asked == ["b" * 64]


def test_a_vanished_process_of_a_container_not_proven_stopped_stays_unknown() -> None:
    """A /proc that cannot see the daemon's PIDs, or a restart under a new PID, looks like a vanished process."""
    assert _container_cgroup_paths([_running("a" * 64, 10)], lambda _identity: False, _read({10})) is None


def test_without_a_re_inspection_a_vanished_process_stays_unknown() -> None:
    assert _container_cgroup_paths([_running("a" * 64, 10)], None, _read({10})) is None


def test_a_running_container_without_a_pid_stays_unknown() -> None:
    assert _container_cgroup_paths([_running("a" * 64, 0)], lambda _identity: True, _read(set())) is None


@pytest.mark.parametrize(
    ("exists", "state", "stopped"),
    [
        (False, None, True),  # removed
        (True, {"Running": False, "Restarting": False, "Pid": 0}, True),  # exited
        (True, {"Running": True, "Restarting": False, "Pid": 10}, False),  # still the same process
        (True, {"Running": True, "Restarting": False, "Pid": 20}, False),  # restarted under a new PID
        (True, {"Running": False, "Restarting": True, "Pid": 0}, False),  # between two runs of a restart
        (True, None, False),  # no readable state
        (True, {"Running": "false", "Restarting": False}, False),  # not a boolean
    ],
    ids=["removed", "exited", "same-pid", "new-pid", "restarting", "no-state", "malformed"],
)
def test_only_a_removed_or_exited_container_counts_as_stopped(exists: bool, state: dict | None, stopped: bool) -> None:
    attributes = {} if state is None else {"State": state}
    observed = DockerObservation("container", "a" * 64, exists, attributes)
    assert _container_stopped(observed) is stopped
