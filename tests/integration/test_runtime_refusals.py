"""A Colima VM too small for a run is refused naming the VM size that admits it."""

from __future__ import annotations

from pathlib import Path

import pytest

from docker_test_toolkit import PhaseBudget, RunSpec
from docker_test_toolkit.domain.errors import ToolkitError
from docker_test_toolkit.factories.testing import bind_test_capacity
from tests.integration.account_fixtures import source_artifact, three_private_projects

pytestmark = pytest.mark.integration

GIB = 1024**3


SIX_GIB_RUN = PhaseBudget(6 * GIB, GIB, 1, 1)


def _spec(project: Path, budget: PhaseBudget = SIX_GIB_RUN) -> RunSpec:
    return RunSpec(
        project_root=project,
        entrypoint="integration",
        purpose="small-vm",
        services=(),
        closure=(),
        profile="fixture",
        phases={"tests": budget},
        initial_phase="tests",
        artifact=source_artifact(),
        wait_seconds=20,
    )


@pytest.mark.parametrize(("runtime", "hint"), [("colima", True), ("docker-desktop", False)])
def test_a_small_vm_refusal_names_the_colima_size_that_admits_the_run(tmp_path: Path, runtime: str, hint: bool) -> None:
    services, projects = three_private_projects(tmp_path)
    bind_test_capacity(services, daemon_memory_bytes=6 * GIB, runtime=runtime)

    with pytest.raises(ToolkitError) as refused:
        services.open_run(_spec(projects[0])).enter()

    assert "unsupported-base-capacity" in str(refused.value)
    assert ("colima stop && colima start --memory 7" in str(refused.value)) is hint


@pytest.mark.parametrize(
    ("capacity", "budget", "machine_mib", "failure", "hint"),
    [
        (  # the guest side: a larger machine fixes it
            {"daemon_memory_bytes": 2 * GIB, "daemon_cpus": 2},
            PhaseBudget(6 * GIB, GIB, 1, 1),
            2048,
            "unsupported-base-capacity",
            "podman machine stop dtt-window && podman machine set --memory 7424 --cpus 2 dtt-window"
            " && podman machine start dtt-window",
        ),
        (  # CPUs only: the memory is never shrunk
            {"daemon_memory_bytes": 2 * GIB, "daemon_cpus": 2},
            PhaseBudget(GIB, GIB, 4, 1),
            2048,
            "insufficient-cpu",
            "podman machine set --memory 2048 --cpus 4 dtt-window",
        ),
        (  # the host side: resizing the machine cannot fix it, so nothing is named
            {"daemon_memory_bytes": 64 * GIB, "daemon_cpus": 2, "host_memory_bytes": 6 * GIB},
            PhaseBudget(2 * GIB, GIB, 1, 1),
            65536,
            "unsupported-base-capacity",
            None,
        ),
    ],
)
def test_a_small_podman_machine_refusal_names_the_resize_only_when_it_fixes_the_refusal(
    tmp_path: Path, capacity: dict, budget: PhaseBudget, machine_mib: int, failure: str, hint: str | None
) -> None:
    services, projects = three_private_projects(tmp_path)
    machine = {"name": "dtt-window", "cpus": 2, "memory_mib": machine_mib}
    bind_test_capacity(services, runtime="podman", machine=machine, **capacity)
    spec = _spec(projects[0], budget)

    with pytest.raises(ToolkitError) as refused:
        services.open_run(spec).enter()

    assert failure in str(refused.value)
    if hint is None:
        assert "podman machine" not in str(refused.value)
    else:
        assert hint in str(refused.value)
