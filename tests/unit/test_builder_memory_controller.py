"""A builder whose memory controller did not come up is restarted before its first memory-limited build needs it."""

from __future__ import annotations

import pytest

from docker_test_toolkit.application import lifecycle
from docker_test_toolkit.domain.errors import InfrastructureFailure
from docker_test_toolkit.domain.models import DockerResult

ROOT = "cpuset cpu io memory pids\n"
READY = ROOT + "cpuset cpu io memory pids\n"


class _Adapter:
    """Answers each controller read with the next scripted output; records every other command."""

    def __init__(self, *reads: DockerResult | Exception) -> None:
        self.reads = list(reads)
        self.commands: list[tuple[str, ...]] = []

    def execute(self, argv: tuple[str, ...], *, timeout_seconds: float) -> DockerResult:
        if argv[0] != "exec":
            self.commands.append(argv)
            return DockerResult(0, "", "")
        read = self.reads.pop(0)
        if isinstance(read, Exception):
            raise read
        return read


@pytest.fixture(autouse=True)
def _no_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(lifecycle, "_BUILDER_SETUP_SECONDS", 0)


RESTART = ("restart", "-t", "0", "worker")
BOOTSTRAP = ("buildx", "inspect", "--bootstrap", "--builder", "builder")


@pytest.mark.parametrize(
    ("reads", "commands"),
    (
        ([DockerResult(0, READY, "")], []),
        ([DockerResult(0, ROOT, ""), DockerResult(0, READY, "")], [RESTART, BOOTSTRAP]),
        ([DockerResult(0, ROOT + "cpu pids\n", ""), DockerResult(0, READY, "")], [RESTART, BOOTSTRAP]),
        ([DockerResult(1, "", "no such file")], []),
        ([DockerResult(0, "cpu pids\n", "")], []),
        ([DockerResult(0, ROOT, "")] * 4, [RESTART, BOOTSTRAP] * 3),
        ([DockerResult(0, ROOT, ""), InfrastructureFailure("late")], [RESTART, BOOTSTRAP]),
    ),
    ids=(
        "ready",
        "empty-subtree-restarts",
        "partial-subtree-restarts",
        "no-cgroup-v2-files",
        "controller-not-delegated",
        "bounded-and-no-new-failure",
        "failed-read-is-no-new-failure",
    ),
)
def test_builder_restarts_only_for_a_missing_memory_controller(
    reads: list[DockerResult | Exception], commands: list[tuple[str, ...]]
) -> None:
    adapter = _Adapter(*reads)

    lifecycle._restore_builder_memory_controller(adapter, "builder", "worker")

    assert adapter.commands == commands
    assert adapter.reads == []
