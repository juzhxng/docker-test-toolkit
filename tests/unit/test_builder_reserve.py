from docker_test_toolkit.application.lifecycle import _builder_removed
from docker_test_toolkit.domain.models import PhaseBudget
from docker_test_toolkit.domain.policy import BUILDER_MEMORY_BYTES, effective_reservation
from docker_test_toolkit.domain.share_policy import builder_verdict

GIB = 1024**3
# An integration run on the CI host: a build phase with the builder envelope, then 6.75 GiB of containers.
_PHASES = {
    "build": PhaseBudget(2 * GIB, 3 * GIB, 2, 1),
    "start": PhaseBudget(27 * GIB // 4, 3 * GIB, 1, 1),
    "tests": PhaseBudget(27 * GIB // 4, 3 * GIB, 1, 2),
}


def _record(*states: str, phase: str = "build", version: str = "0.6.17") -> dict:
    builders = {f"builder-{index}": {"state": state} for index, state in enumerate(states)}
    return {
        "owned_resources": {"builders": builders},
        "initial_phase": "build",
        "grant": {"phase": phase},
        "artifact": {"version": version},
    }


def test_a_run_that_removed_its_builder_holds_no_builder_reserve() -> None:
    # An e2e and an integration run did not fit together: each kept 2 GiB for a builder it had already removed.
    assert effective_reservation(_PHASES).container_bytes == 27 * GIB // 4 + BUILDER_MEMORY_BYTES
    assert effective_reservation(_PHASES, builder_removed=True).container_bytes == 27 * GIB // 4


def test_the_reserve_stays_through_the_first_phase_and_for_older_holders() -> None:
    # A run may create, remove and create a builder again within its first phase (review impl-0616); a holder older
    # than 0.6.17 does not refuse a later builder, so its reserve stays too.
    assert not _builder_removed(_record())
    assert not _builder_removed(_record("removed"))  # still in its first phase
    assert not _builder_removed(_record("live", phase="tests"))
    assert _builder_removed(_record("removed", phase="tests"))
    assert not _builder_removed(_record("removed", phase="tests", version="0.6.16"))


def test_a_builder_after_its_removal_breaks_share_rule_r16() -> None:
    assert builder_verdict(removed=False).shareable
    verdict = builder_verdict(removed=True)
    assert (verdict.rule, verdict.shareable) == ("R16", False)


def test_a_run_that_left_its_first_phase_without_a_builder_holds_no_reserve() -> None:
    # A batch that only pulls images still "admits" a builder by its budget (3 GiB, 2 CPUs); it kept 2 GiB beside its
    # co-holder for a builder it never creates.
    assert not _builder_removed(_record(phase="build"))  # it may still build in its first phase
    assert _builder_removed(_record(phase="tests"))
    assert not _builder_removed(_record("live", phase="tests"))  # a builder kept past the first phase stays reserved


def test_a_holder_without_a_builder_reserve_has_all_its_containers_attributed() -> None:
    # 0.6.13 released the reserve but still set 2 GiB of the reservation aside for a worker: OpenSearch and Neo4j of a
    # An e2e holder then counted as foreign and a co-holder's join waited (CI host replay).
    from types import SimpleNamespace

    from docker_test_toolkit.application.lifecycle import _holder_facts
    from docker_test_toolkit.domain.policy import attributed_ids

    e2e = {
        "build": PhaseBudget(2 * GIB, 25 * GIB // 4, 2, 1),
        "start": PhaseBudget(3425236418, 25 * GIB // 4, 2, 1),
        "tests": PhaseBudget(3350000000, 25 * GIB // 4, 2, 2),
    }
    record = {
        "phases": {name: vars(budget) for name, budget in e2e.items()},
        "initial_phase": "build",
        "grant": {"phase": "tests"},
        "owned_resources": {"builders": {"worker": {"state": "removed"}}},
        "artifact": {"version": "0.6.17"},
    }
    holder = _holder_facts("e0572458-0000-0000-0000-000000000000", SimpleNamespace(project_id="6" * 64), record)
    containers = [
        {
            "Id": f"{index:064x}",
            "Name": f"/{holder.compose_project}-{name}-1",
            "State": {"Running": True},
            "Config": {"Labels": {"com.docker.compose.project": holder.compose_project}},
            "HostConfig": {"Memory": limit},
        }
        for index, (name, limit) in enumerate(
            (("opensearch", 3 * GIB // 2), ("neo4j", 5 * GIB // 4), ("postgres", 384 * 1024**2))
        )
    ]

    assert holder.builder_worker is None
    assert len(attributed_ids(containers, holder, run_label="docker-test-toolkit.run")) == 3
