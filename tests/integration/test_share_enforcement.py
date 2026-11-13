"""Share-rule enforcement: shared runs refuse, exclusive runs record, leave teaches positives."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest

from docker_test_toolkit import PhaseBudget, RunHandle
from docker_test_toolkit.application import lifecycle, sharing
from docker_test_toolkit.application.recovery import atomic_json
from docker_test_toolkit.domain.errors import InvalidInput
from docker_test_toolkit.domain.share_policy import SHARE_RULES_VERSION
from docker_test_toolkit.factories.testing import (
    install_failpoint,
)
from tests.integration.share_fixtures import (
    GIB,
    NETWORK_ID,
    _dispatch,
    _key,
    _leave,
    _receipts,
    _record,
    _store,
    _verdict,
    _World,
    shared_world,
)
from tests.unit.test_docker_receipt_service import IMAGE_ID, _container

pytestmark = pytest.mark.integration


@pytest.fixture
def world(tmp_path: Path) -> Iterator[_World]:
    with shared_world(tmp_path) as built:
        yield built


RECORDED = "; this request is now recorded as not shareable, so its next run is exclusive while that verdict is kept"


def _recorded_refusal(error: InvalidInput, rule: str) -> None:
    """The refusal names the rule, says the verdict was recorded and asks for a rerun, then gives the reason."""
    assert str(error).startswith(f"Shared run breaks share rule {rule}{RECORDED}; rerun it. Reason: ")


def _refused(world: _World, handle: RunHandle, *argv: str, rule: str) -> None:
    """A shared run's violation: the negative verdict names the rule, the refusal follows, no receipt is appended."""
    before = _receipts(handle)
    with pytest.raises(InvalidInput) as raised:
        _dispatch(world, handle, *argv)
    assert raised.value.reason == "share-rule"
    _recorded_refusal(raised.value, rule)
    assert _receipts(handle) == before
    verdict = _verdict(world, handle)
    assert verdict is not None and verdict["shareable"] is False and rule in verdict["reason"]


def _running_owned(world: _World, handle: RunHandle, memory: int, *, name: str = "own") -> str:
    """Plant one running container carrying the run's label, as an earlier dispatch would have left it."""
    container_id = "c" * 64
    labels = {lifecycle.RUN: handle.run_id}
    world.fake.containers[container_id] = _container(
        container_id, name, labels, running=True, host_config={"Memory": memory}
    )
    return container_id


def test_direct_run_without_memory_refused_for_shared_recorded_for_exclusive(world: _World) -> None:
    shared = world.open(shared=True)
    _refused(world, shared, "run", IMAGE_ID, "true", rule="R9")
    _leave(world, shared)

    exclusive = world.open(shared=False)  # the negative verdict admits the next run of the key exclusive
    assert "sharing" not in _record(exclusive)
    _dispatch(world, exclusive, "run", IMAGE_ID, "true")
    assert _receipts(exclusive)[-1]["kind"] == "container-create"
    assert "R9" in _verdict(world, exclusive)["reason"]
    _leave(world, exclusive)


def test_direct_run_with_adequate_memory_allowed_for_shared(world: _World) -> None:
    shared = world.open(shared=True)

    _dispatch(world, shared, "run", "--memory=512m", IMAGE_ID, "true")

    assert _receipts(shared)[-1]["kind"] == "container-create"
    assert _verdict(world, shared)["origin"] == "seed"  # nothing negative was written
    _leave(world, shared)


def test_direct_run_bind_outside_allowlist_refused_r10(world: _World) -> None:
    shared = world.open(shared=True)
    outside = world.project.parent / "elsewhere"
    outside.mkdir()

    _refused(
        world,
        shared,
        "run",
        "--memory=256m",
        "--mount",
        f"type=bind,source={outside},target=/x,readonly",
        IMAGE_ID,
        "true",
        rule="R10",
    )
    _leave(world, shared)


def test_direct_run_over_own_budget_refused_r12(world: _World) -> None:
    shared = world.open(shared=True)
    _running_owned(world, shared, GIB + GIB // 2)

    _refused(world, shared, "run", "--memory=1g", IMAGE_ID, "true", rule="R12")
    _leave(world, shared)


def test_a_running_builder_worker_counts_against_the_runs_own_budget(world: _World) -> None:
    """The worker carries no label: it is the run's own container by its name."""
    shared = world.open(shared=True, phases={"tests": PhaseBudget(3 * GIB, 1, 2, 1)})
    _builder_worker(world, shared)

    _refused(world, shared, "run", "--memory=1536m", IMAGE_ID, "true", rule="R12")
    _leave(world, shared)


def test_a_build_in_a_phase_without_room_is_refused_for_shared_and_recorded_for_exclusive(world: _World) -> None:
    small = {"tests": PhaseBudget(GIB, 1, 2, 1)}
    shared = world.open(shared=True, phases=small)
    _refused(world, shared, "buildx", "create", rule="R12")
    _leave(world, shared)

    exclusive = world.open(shared=False, phases=small)
    _dispatch(world, exclusive, "buildx", "create")
    assert _receipts(exclusive)[-1]["kind"] == "builder-create"
    assert "R12" in _verdict(world, exclusive)["reason"]
    _leave(world, exclusive)


def test_direct_start_over_budget_refused(world: _World) -> None:
    shared = world.open(shared=True)
    created = _create_stopped(world, shared, 3 * GIB)

    _refused(world, shared, "container", "start", created, rule="R12")
    _leave(world, shared)


def _create_stopped(world: _World, handle: RunHandle, memory: int) -> str:
    """Create one owned stopped container through the dispatcher in an exclusive-looking way: a short mutation."""
    world.fake.next_container_mutation = lambda item: item["HostConfig"].update(Memory=memory)
    _dispatch(world, handle, "run", "--memory=256m", IMAGE_ID, "true")
    return next(key for key, item in _record(handle)["owned_resources"]["containers"].items())


def test_compose_exec_privileged_refused(world: _World) -> None:
    shared = world.open(shared=True)

    with pytest.raises(InvalidInput) as raised:
        _dispatch(world, shared, "compose", "--file", "compose.yml", "exec", "--privileged", "fixture", "true")

    assert raised.value.reason == "share-rule"
    assert "R7" in _verdict(world, shared)["reason"]
    _leave(world, shared)


def test_network_connect_external_is_static_exclusive_and_recorded(world: _World) -> None:
    # Ruling: a run that declares connect_owned_container is static exclusive, so a shared run can never
    # connect; the exclusive run's connection executes and records R5.
    run = world.open(shared=True, connect=True)
    world.fake.networks[NETWORK_ID] = {"Id": NETWORK_ID, "Name": "external", "Created": "x", "Labels": {}}
    container = _create_stopped(world, run, GIB // 4)

    _dispatch(world, run, "network", "connect", NETWORK_ID, container)

    assert _receipts(run)[-1]["kind"] == "network-connect"
    assert "R5" in _verdict(world, run)["reason"]
    _leave(world, run)


def _two_phases(small: int) -> dict[str, PhaseBudget]:
    return {"tests": PhaseBudget(2 * GIB, 1, 2, 1), "small": PhaseBudget(small, 1, 2, 1)}


def test_transition_into_smaller_phase_with_owned_container_refused(world: _World) -> None:
    shared = world.open(shared=True, phases=_two_phases(GIB))
    _ = shared.docker_context  # Docker authority exists, as before any dispatch
    _running_owned(world, shared, GIB + 1)

    with pytest.raises(InvalidInput) as raised:
        lifecycle.transition_phase(shared._lease, "small")

    assert raised.value.reason == "share-rule"
    _recorded_refusal(raised.value, "R12")
    assert _record(shared)["grant"]["phase"] == "tests"
    assert "R12" in _verdict(world, shared)["reason"]
    _leave(world, shared)


def _builder_worker(world: _World, handle: RunHandle) -> None:
    """Plant the run's running 2 GiB BuildKit worker, which carries no toolkit label."""
    builder = lifecycle.builder_name(handle._context.project.project_id, handle.run_id)
    worker = "8" * 64
    world.fake.containers[worker] = _container(
        worker,
        f"buildx_buildkit_{builder}0",
        {"com.docker.buildx.builder": builder},
        running=True,
        host_config={"Memory": 2 * GIB},
    )


def test_transition_into_a_phase_smaller_than_the_running_builder_worker_refused(world: _World) -> None:
    shared = world.open(shared=True, phases=_two_phases(GIB))
    _ = shared.docker_context  # Docker authority exists, as before any dispatch
    _builder_worker(world, shared)

    with pytest.raises(InvalidInput) as raised:
        lifecycle.transition_phase(shared._lease, "small")

    assert raised.value.reason == "share-rule"
    _recorded_refusal(raised.value, "R12")
    assert _record(shared)["grant"]["phase"] == "tests"
    _leave(world, shared)


@contextmanager
def _unwritable(store: Path) -> Iterator[None]:
    """Make a verdict store unwritable, also for root (CI jobs run as root, which ignores modes).

    A regular file takes the store directory's place, so creating or replacing a verdict raises an OSError; the
    directory and its seed come back afterwards.
    """
    aside = store.with_name(store.name + ".aside")
    if store.exists():
        store.rename(aside)
    store.write_text("")
    try:
        yield
    finally:
        store.unlink()
        if aside.exists():
            aside.rename(store)


def test_verdict_write_failure_still_refuses(world: _World) -> None:
    shared = world.open(shared=True)
    store = sharing.verdict_path(world.registry, shared._context.project.project_id).parent
    with _unwritable(store), pytest.raises(InvalidInput) as raised:
        _dispatch(world, shared, "run", IMAGE_ID, "true")

    assert raised.value.reason == "share-rule"
    assert str(raised.value).startswith(
        "Shared run breaks share rule R9; the verdict could not be recorded; rerun it. "
    )
    assert "recorded as not shareable" not in str(raised.value)
    assert _verdict(world, shared)["shareable"] is True  # the seed survived: nothing was written
    _leave(world, shared)


def test_exclusive_verdict_write_failure_does_not_fail_the_run(world: _World) -> None:
    exclusive = world.open(shared=False)
    store = sharing.verdict_path(world.registry, exclusive._context.project.project_id).parent
    with _unwritable(store):
        _dispatch(world, exclusive, "run", IMAGE_ID, "true")

    assert _receipts(exclusive)[-1]["kind"] == "container-create"
    assert "share-rule" not in json.dumps(_record(exclusive))
    _leave(world, exclusive)


def _exit_at(stage: str) -> Callable[[str], None]:
    def callback(name: str) -> None:
        if name == stage:
            raise SystemExit(f"fixture exit at {stage}")

    return callback


def test_crash_after_negative_verdict_before_refusal(world: _World) -> None:
    shared = world.open(shared=True)
    before = _receipts(shared)
    install_failpoint(_exit_at("share-verdict-written"))
    try:
        with pytest.raises(SystemExit):
            _dispatch(world, shared, "run", IMAGE_ID, "true")
    finally:
        install_failpoint(None)

    assert _verdict(world, shared)["shareable"] is False
    assert _receipts(shared) == before  # nothing journaled
    _leave(world, shared)


def _teach(world: _World, handle: RunHandle) -> None:
    """One checked mutation that breaks no rule, then leave: the guardian's acknowledgement may teach a positive."""
    _dispatch(world, handle, "run", "--memory=256m", IMAGE_ID, "true")
    _leave(world, handle)


def test_leave_acknowledgement_records_positive_after_checked_mutation(world: _World) -> None:
    exclusive = world.open(shared=False)

    _teach(world, exclusive)

    verdict = _verdict(world, exclusive)
    assert verdict["shareable"] is True and verdict["run_id"] == exclusive.run_id


def test_a_checkout_without_an_origin_runs_exclusive_and_teaches_nothing(world: _World) -> None:
    # Forgejo's host executor names every checkout `hostexecutor`; without an origin there is no repository to key on.
    (world.project / ".git" / "config").unlink()
    exclusive = world.open(shared=False)

    _teach(world, exclusive)

    assert "sharing" not in _record(exclusive)  # exclusive; the record keeps its closed shape
    assert _store(world, exclusive).get("verdicts", {}) == {}  # neither a positive nor any other verdict was written


@pytest.mark.parametrize("case", ["no-receipts", "teardown-only"])
def test_leave_acknowledgement_does_not_record_positive_verdict_without_checked_mutations(
    world: _World, case: str
) -> None:
    exclusive = world.open(shared=False)
    if case == "teardown-only":
        _dispatch(world, exclusive, "network", "create")
        network_id = next(iter(_record(exclusive)["owned_resources"]["networks"]))
        _dispatch(world, exclusive, "network", "rm", network_id)

    _leave(world, exclusive)

    assert _verdict(world, exclusive) is None


def test_positive_does_not_overwrite_later_negative(world: _World) -> None:
    exclusive = world.open(shared=False)
    _dispatch(world, exclusive, "run", "--memory=256m", IMAGE_ID, "true")
    granted = _record(exclusive)["hold_timeline"]
    # another run of the key learned a violation after this run's grant (same clock origin)
    world.write(exclusive, False, origin=granted["origin"], tick_ns=granted["granted_ns"] + 1, reason="R1 later")

    _leave(world, exclusive)

    assert _verdict(world, exclusive)["shareable"] is False


def test_a_clean_run_never_replaces_an_earlier_negative(world: _World) -> None:
    # A relay: a plan-mode run binds inside its own root (clean), a provider-mode run of the same request binds
    # the checkout's ledger. The clean run must not teach the request shared over the refusal.
    exclusive = world.open(shared=False)
    granted = _record(exclusive)["hold_timeline"]
    world.write(exclusive, False, origin=granted["origin"], tick_ns=granted["granted_ns"] - 1, reason="R10 earlier")

    _teach(world, exclusive)

    assert _verdict(world, exclusive)["shareable"] is False


def test_rules_mismatch_drops_all_verdicts(world: _World) -> None:
    exclusive = world.open(shared=False)
    path = sharing.verdict_path(world.registry, exclusive._context.project.project_id)
    path.parent.mkdir(mode=0o700, exist_ok=True)
    stale = {"shareable": True, "reason": None, "run_id": exclusive.run_id, "origin": "o", "tick_ns": 1}
    atomic_json(path, {"schema": 1, "rules": SHARE_RULES_VERSION + 1, "verdicts": {"other-key": stale}})

    _teach(world, exclusive)

    document = _store(world, exclusive)
    assert document["rules"] == SHARE_RULES_VERSION
    assert set(document["verdicts"]) == {_key(exclusive)}


def test_exclusive_compose_receipt_plan_has_no_share_keys(world: _World) -> None:
    exclusive = world.open(shared=False)
    (world.project / "compose.yml").write_text("services: {}\n")

    _dispatch(world, exclusive, "compose", "--file", str(world.project / "compose.yml"), "up", "--detach")

    plan = _receipts(exclusive)[-1]["plan"]
    assert {"files", "scale", "watch"}.isdisjoint(plan)
    _dispatch(world, exclusive, "compose", "--file", str(world.project / "compose.yml"), "down", "--volumes")
    _leave(world, exclusive)


def _compose_world(world: _World, **extra) -> tuple[RunHandle, Path, Path]:
    """A shared run whose rendered service has a hard limit; returns it, its Compose file and its env file."""
    world.fake.compose_service_extra = {"mem_limit": str(GIB // 4), **extra}
    compose, env = world.project / "compose.yml", world.project / "test.env"
    compose.write_text("services: {}\n")
    env.write_text("A=1\n")
    return world.open(shared=True), compose, env


def _up(world: _World, handle: RunHandle, compose: Path, env: Path, *options: str):
    return _dispatch(
        world, handle, "compose", "--file", str(compose), "--env-file", str(env), "up", "--detach", *options
    )


def _down(world: _World, handle: RunHandle, compose: Path) -> None:
    _dispatch(world, handle, "compose", "--file", str(compose), "down", "--volumes")


def test_shared_compose_up_binds_its_input_files(world: _World) -> None:
    shared, compose, env = _compose_world(world)

    _up(world, shared, compose, env)

    files = _receipts(shared)[-1]["plan"]["files"]
    assert [item["path"] for item in files] == [str(compose.resolve()), str(env.resolve())]
    _down(world, shared, compose)
    _leave(world, shared)


@pytest.mark.parametrize("changed", ["file", "env-file"])
def test_compose_file_mutated_after_hash_refused(world: _World, changed: str) -> None:
    shared, compose, env = _compose_world(world)
    target = compose if changed == "file" else env

    def mutate(stage: str) -> None:
        if stage == "compose-files-hashed":
            target.write_text(target.read_text() + "# changed\n")

    install_failpoint(mutate)
    try:
        with pytest.raises(InvalidInput, match="changed after validation"):
            _up(world, shared, compose, env)
    finally:
        install_failpoint(None)

    assert not any(command[0] == "compose" and "up" in command for command in world.fake.commands)
    _leave(world, shared)


@pytest.mark.parametrize("changed", ["file", "env-file"])
def test_shared_teardown_after_file_change_not_refused(world: _World, changed: str) -> None:
    shared, compose, env = _compose_world(world)
    _up(world, shared, compose, env)
    (compose if changed == "file" else env).write_text("# changed after up\n")

    _down(world, shared, compose)

    assert _receipts(shared)[-1]["plan"]["subcommand"] == "down"
    _leave(world, shared)


def test_up_scale_two_refused(world: _World) -> None:
    shared, compose, _env = _compose_world(world)

    _refused(world, shared, "compose", "--file", str(compose), "up", "--detach", "--scale", "fixture=2", rule="R14")
    _leave(world, shared)


def test_fixed_port_refused_for_shared(world: _World) -> None:
    shared, compose, _env = _compose_world(world, ports=[{"target": 80, "published": "8080", "protocol": "tcp"}])

    _refused(world, shared, "compose", "--file", str(compose), "up", "--detach", rule="R1")
    _leave(world, shared)


def test_unbound_compose_input_refused_r15(world: _World) -> None:
    shared, _compose, _env = _compose_world(world)

    _refused(world, shared, "compose", "up", "--detach", rule="R15")
    _leave(world, shared)


def test_retained_restart_over_budget_refused(world: _World) -> None:
    # The restart helper runs on an active lease once a resumed generation is admitted; this cell gives it one stopped
    # owned service container (the evidence a retained Compose service carries) instead of a whole retain/resume.
    shared = world.open(shared=True)
    container = _create_stopped(world, shared, 3 * GIB)
    record = _record(shared)
    record["owned_resources"]["containers"][container]["evidence"].update(service="fixture", completed_service=False)
    atomic_json(shared._context.record, record)
    before = list(world.fake.commands)

    with pytest.raises(InvalidInput) as raised:
        lifecycle.restart_retained_resources(shared._lease)

    assert raised.value.reason == "share-rule"
    _recorded_refusal(raised.value, "R12")
    assert "R12" in _verdict(world, shared)["reason"]
    assert not any(command[:2] == ("container", "start") for command in world.fake.commands[len(before) :])
    _leave(world, shared)


def test_input_swapped_only_for_the_render_is_refused(world: _World) -> None:
    """The render reads a replaced file that is restored before execution: only the post-render check sees it."""
    shared, compose, env = _compose_world(world)
    original = compose.read_text()

    def swap_for_render() -> None:
        compose.write_text("services: {} # read by the render only\n")

    def restore(stage: str) -> None:
        if stage == "compose-files-hashed":
            compose.write_text(original)

    world.fake.on_compose_config = swap_for_render
    install_failpoint(restore)
    try:
        with pytest.raises(InvalidInput, match="changed"):
            _up(world, shared, compose, env)
    finally:
        install_failpoint(None)
        world.fake.on_compose_config = None
        compose.write_text(original)

    assert not any(command[0] == "compose" and "up" in command for command in world.fake.commands)
    _leave(world, shared)


def test_symlinked_input_retargeted_after_hash_is_refused(world: _World) -> None:
    shared, compose, env = _compose_world(world)
    other = world.project / "other.yml"
    other.write_text("services: {} # another file\n")
    link = world.project / "current.yml"
    link.symlink_to(compose)

    def retarget(stage: str) -> None:
        if stage == "compose-files-hashed":
            link.unlink()
            link.symlink_to(other)

    install_failpoint(retarget)
    try:
        with pytest.raises(InvalidInput, match="changed after validation"):
            _up(world, shared, link, env)
    finally:
        install_failpoint(None)
    _leave(world, shared)


def test_compose_run_counts_the_running_service_container(world: _World) -> None:
    # `up` starts a 1.5 GiB service container; a one-off of the same service adds its own 1.5 GiB (budget 2 GiB)
    shared, compose, env = _compose_world(world, mem_limit=str(3 * GIB // 2))
    world.fake.next_container_mutation = lambda item: item["HostConfig"].update(Memory=3 * GIB // 2)
    _up(world, shared, compose, env)

    _refused(
        world, shared, "compose", "--file", str(compose), "run", "--rm", "--no-deps", "--entrypoint", "sh",
        "fixture", "-c", "true", rule="R12",
    )  # fmt: skip
    _down(world, shared, compose)
    _leave(world, shared)


def test_exclusive_violation_whose_verdict_was_not_recorded_teaches_nothing(world: _World) -> None:
    exclusive = world.open(shared=False)
    store = sharing.verdict_path(world.registry, exclusive._context.project.project_id).parent
    with _unwritable(store):  # the store recovers before leave
        _dispatch(world, exclusive, "run", IMAGE_ID, "true")  # R9, the negative cannot be written
    _dispatch(world, exclusive, "run", "--memory=256m", IMAGE_ID, "true")  # a clean checked mutation too

    _leave(world, exclusive)

    assert _verdict(world, exclusive) is None


def test_unevaluable_rules_never_fail_an_exclusive_run(world: _World) -> None:
    exclusive = world.open(shared=False)
    foreign = world.registry / "projects" / ("f" * 64 + ".json")
    foreign.write_text("{not json")  # another project's unreadable record: the private-root trees cannot be listed
    foreign.chmod(0o600)

    _dispatch(world, exclusive, "run", "--memory=256m", IMAGE_ID, "true")

    assert _receipts(exclusive)[-1]["kind"] == "container-create"
    foreign.unlink()
    _leave(world, exclusive)
    assert _verdict(world, exclusive) is None  # an unevaluated mutation teaches nothing


def test_unevaluable_rules_refuse_a_shared_run(world: _World) -> None:
    shared = world.open(shared=True)
    foreign = world.registry / "projects" / ("f" * 64 + ".json")
    foreign.write_text("{not json")
    foreign.chmod(0o600)

    with pytest.raises(InvalidInput) as raised:
        _dispatch(world, shared, "run", "--memory=256m", IMAGE_ID, "true")

    assert raised.value.reason == "share-rule"
    foreign.unlink()
    _leave(world, shared)


def test_negative_written_by_dispatch_survives_the_same_runs_leave(world: _World) -> None:
    exclusive = world.open(shared=False)
    _dispatch(world, exclusive, "run", "--memory=256m", IMAGE_ID, "true")  # clean checked mutation first
    _dispatch(world, exclusive, "run", IMAGE_ID, "true")  # then a real R9 negative through dispatch

    _leave(world, exclusive)

    verdict = _verdict(world, exclusive)
    assert verdict is not None and verdict["shareable"] is False and "R9" in verdict["reason"]


def test_input_reached_through_a_symlink_and_dotdot_is_the_file_compose_reads(world: _World) -> None:
    shared, compose, env = _compose_world(world)
    elsewhere = world.project / "elsewhere" / "subdir"
    elsewhere.mkdir(parents=True)
    real = world.project / "elsewhere" / "compose.yml"  # what `link/../compose.yml` reaches through the link
    real.write_text(compose.read_text())
    (world.project / "link").symlink_to(elsewhere)
    through = world.project / "link" / ".." / "compose.yml"

    def change_read_file(stage: str) -> None:
        if stage == "compose-files-hashed":
            real.write_text(real.read_text() + "# changed after the render\n")

    install_failpoint(change_read_file)
    try:
        with pytest.raises(InvalidInput, match="changed after validation"):
            _up(world, shared, through, env)
    finally:
        install_failpoint(None)
    _leave(world, shared)


def test_compose_start_counts_the_existing_containers_own_limit(world: _World) -> None:
    # the render now says 256 MiB, but the stopped container `start` reuses was created with 3 GiB (budget 2 GiB)
    shared, compose, env = _compose_world(world)
    _up(world, shared, compose, env)
    container = next(
        key
        for key, item in world.fake.containers.items()
        if item["Config"]["Labels"].get("com.docker.compose.service") == "fixture"
    )
    world.fake.containers[container]["State"]["Running"] = False
    world.fake.containers[container]["HostConfig"] = {"Memory": 3 * GIB}

    _refused(world, shared, "compose", "--file", str(compose), "start", "fixture", rule="R12")
    world.fake.containers[container]["HostConfig"] = {"Memory": 1}
    _down(world, shared, compose)
    _leave(world, shared)


def test_relative_input_under_project_directory_is_unbound(world: _World) -> None:
    shared, _compose, _env = _compose_world(world)

    _refused(
        world, shared, "compose", "--project-directory", str(world.project), "--file", "compose.yml", "up",
        "--detach", rule="R15",
    )  # fmt: skip
    _leave(world, shared)


def test_compose_restart_counts_the_existing_containers_own_limit(world: _World) -> None:
    shared, compose, env = _compose_world(world)
    _up(world, shared, compose, env)
    container = next(
        key
        for key, item in world.fake.containers.items()
        if item["Config"]["Labels"].get("com.docker.compose.service") == "fixture"
    )
    world.fake.containers[container]["State"]["Running"] = False
    world.fake.containers[container]["HostConfig"] = {"Memory": 3 * GIB}

    _refused(world, shared, "compose", "--file", str(compose), "restart", "fixture", rule="R12")
    world.fake.containers[container]["HostConfig"] = {"Memory": 1}
    _down(world, shared, compose)
    _leave(world, shared)


def test_relative_input_through_a_symlink_and_dotdot_is_the_file_compose_reads(
    world: _World, monkeypatch: pytest.MonkeyPatch
) -> None:
    shared, compose, env = _compose_world(world)
    elsewhere = world.project / "elsewhere" / "subdir"
    elsewhere.mkdir(parents=True)
    real = world.project / "elsewhere" / "compose.yml"
    real.write_text(compose.read_text())
    (world.project / "link").symlink_to(elsewhere)
    monkeypatch.chdir(world.project)  # the dispatcher resolves relative inputs against its own cwd

    def change_read_file(stage: str) -> None:
        if stage == "compose-files-hashed":
            real.write_text(real.read_text() + "# changed after the render\n")

    install_failpoint(change_read_file)
    try:
        with pytest.raises(InvalidInput, match="changed after validation"):
            _dispatch(
                world, shared, "compose", "--file", "link/../compose.yml", "--env-file", str(env), "up", "--detach"
            )
    finally:
        install_failpoint(None)
    _leave(world, shared)


@pytest.mark.parametrize(
    "argv",
    [
        ("exec", "--privileged=true", "fixture", "true"),
        ("up", "--watch=true"),
        ("up", "-dw"),
    ],
    ids=["exec-privileged-equals", "up-watch-equals", "up-short-cluster"],
)
def test_alternative_flag_spellings_are_judged_like_the_long_form(world: _World, argv: tuple[str, ...]) -> None:
    """Every spelling the classifier accepts is judged: an `=true` value or a short-option cluster is not a bypass."""
    shared = world.open(shared=True)

    with pytest.raises(InvalidInput) as raised:
        _dispatch(world, shared, "compose", "--file", "compose.yml", *argv)

    assert raised.value.reason == "share-rule"
    _leave(world, shared)


def test_concurrent_starts_of_one_run_cannot_both_pass_r12(world: _World) -> None:
    """Two dispatchers of one shared run start a 1.5 GiB container each under a 2 GiB phase budget. B runs to the end
    while A is between its rule check and its receipt; A's re-check under its own pending receipt refuses."""
    shared = world.open(shared=True)
    first = _create_stopped(world, shared, GIB + GIB // 2)
    world.fake.next_container_mutation = lambda item: item["HostConfig"].update(Memory=GIB + GIB // 2)
    _dispatch(world, shared, "run", "--memory=256m", IMAGE_ID, "true")
    second = next(key for key in _record(shared)["owned_resources"]["containers"] if key != first)
    started = []

    def start_second_inside(name: str) -> None:
        if name == "share-rules-evaluated" and not started:
            started.append(second)
            _dispatch(world, shared, "container", "start", second)

    install_failpoint(start_second_inside)
    try:
        with pytest.raises(InvalidInput) as raised:
            _dispatch(world, shared, "container", "start", first)
    finally:
        install_failpoint(None)

    assert raised.value.reason == "share-rule" and "R12" in _verdict(world, shared)["reason"]
    _recorded_refusal(raised.value, "R12")
    running = [
        cid for cid, item in world.fake.containers.items() if item["State"]["Running"] and cid in (first, second)
    ]
    assert running == [second]  # B, a single dispatcher, passed both checks: its own receipt never counts against it
    states = [item["state"] for item in _record(shared)["docker_receipts"].values()]
    assert states.count("failed") == 1 and not {"pending", "unresolved"} & set(states)  # A's receipt is resolved
    failed = next(item for item in _record(shared)["docker_receipts"].values() if item["state"] == "failed")
    assert failed["result"]["exit_code"] == 2  # synthetic: the child never started
    _leave(world, shared)


def test_a_compose_file_after_the_subcommand_is_bound_and_judged(world: _World, tmp_path: Path) -> None:
    """``up --file=x`` makes Compose read x: the shared run binds and judges it like a leading ``--file`` (a missing
    one is an R15 refusal with the verdict recorded first, never an unjudged execution)."""
    shared = world.open(shared=True)
    compose = tmp_path / "compose.yml"
    compose.write_text("services: {}\n")
    missing = tmp_path / "override.yml"

    with pytest.raises(InvalidInput) as raised:
        _dispatch(world, shared, "compose", "--file", str(compose), "up", "--detach", f"--file={missing}")

    assert raised.value.reason == "share-rule"
    _recorded_refusal(raised.value, "R15")
    verdict = _verdict(world, shared)
    assert verdict is not None and verdict["shareable"] is False and "R15" in verdict["reason"]
    _leave(world, shared)


def test_the_receipt_render_sees_exactly_the_persistent_options_compose_applies(world: _World) -> None:
    """The render runs on the toolkit's injected argv (``--project-name`` first): a trailing ``--file`` after ``up``
    reaches it, and an ``exec`` payload's own ``-f`` (``psql -f``) never does."""
    exclusive = world.open(shared=False)
    compose = world.project / "compose.yml"
    override = world.project / "override.yml"
    compose.write_text("services: {}\n")
    override.write_text("services: {}\n")

    def render_of(*arguments: str) -> tuple[str, ...]:
        before = len(world.fake.commands)
        _dispatch(world, exclusive, "compose", "--file", str(compose), *arguments)
        renders = [c for c in world.fake.commands[before:] if c[0] == "compose" and "config" in c]
        assert len(renders) == 1 and renders[0][:2] == ("compose", "--project-name")  # one receipt render per dispatch
        return renders[0][3:]  # after the toolkit's project name

    leading = ("--file", str(compose))
    assert render_of("up", "--detach", f"--file={override}") == (
        *leading,
        f"--file={override}",
        "config",
        "--format",
        "json",
    )
    assert render_of("exec", "-T", "fixture", "psql", "-f", "/run/x.sql") == (*leading, "config", "--format", "json")
    _dispatch(world, exclusive, "compose", "--file", str(compose), "down", "--volumes")
    _leave(world, exclusive)
