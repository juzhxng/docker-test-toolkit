from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import textwrap
import threading
import time
import uuid
from pathlib import Path

import pytest

from docker_test_toolkit import ArtifactIdentity, PhaseBudget, RunSpec, ToolkitError
from docker_test_toolkit.application import lifecycle
from docker_test_toolkit.application.recovery import Context, failpoint, lock
from docker_test_toolkit.domain.errors import ResourceError
from docker_test_toolkit.factories import testing
from docker_test_toolkit.factories.testing import build_test_services, install_failpoint, install_lock_observer
from tests.integration.account_fixtures import (
    hold_lock_until_released,
    pause_guardian,
    release_lock,
    resume_guardian,
    run_lock_path,
    wait_guardians_exited,
    wait_until,
)
from tests.unit.test_docker_receipt_service import FakeDockerPort
from tests.unit.test_docker_receipt_service import _spec as _receipt_spec


def _spec(root: Path) -> RunSpec:
    return RunSpec(
        project_root=root,
        entrypoint="integration",
        purpose="private-proof",
        services=("postgres",),
        closure=("postgres",),
        profile="fixture",
        phases={"tests": PhaseBudget(1, 1, 1, 1)},
        initial_phase="tests",
        artifact=ArtifactIdentity(
            "0.1.0",
            "a" * 64,
            {
                "activation": 1,
                "batch": 1,
                "cli": 1,
                "distribution": 1,
                "queue": 1,
                "receipt": 1,
                "registry": 2,
                "run": 1,
            },
        ),
    )


def test_private_test_composition_owns_a_caller_selected_root(tmp_path: Path) -> None:
    state = tmp_path / "state"
    handle = build_test_services(state).open_run(_spec(tmp_path))

    assert handle._context.project.state == state / "registry"
    assert handle._test_directory == state / "admission"


def test_private_test_composition_reuses_one_shared_private_domain(tmp_path: Path) -> None:
    state = tmp_path / "state"

    first = build_test_services(state)
    second = build_test_services(state)

    assert first.root == second.root == state


def test_default_poll_binding_is_written_once(tmp_path: Path) -> None:
    state = tmp_path / "state"
    services = build_test_services(state)
    admission = services.root / "admission"

    assert json.loads((admission / "test-poll.json").read_text()) == {"schema": 1, "interval_ns": 50_000_000}
    assert lifecycle._poll_interval_ns(admission, canonical=None) == 50_000_000
    testing.bind_test_poll_interval(services, 20_000_000)
    build_test_services(state)  # the shared-root reuse never overwrites an explicit binding
    assert lifecycle._poll_interval_ns(admission, canonical=None) == 20_000_000


def test_private_test_composition_rejects_the_real_account_domain(real_account: Path) -> None:
    with pytest.raises(ToolkitError, match="real account"):
        build_test_services(real_account)


def test_failpoint_and_lock_observer_install_and_reset(tmp_path: Path) -> None:
    ctx = Context(tmp_path / "project", tmp_path / "state")
    events: list[tuple] = []
    stages: list[str] = []
    install_lock_observer(lambda *event: events.append(event))
    install_failpoint(stages.append)
    try:
        with lock(ctx, "registry"):
            pass
        with lock(ctx, "registry", deadline_ns=time.monotonic_ns() + 5_000_000_000):
            pass
        failpoint("queue-saved")
    finally:
        install_lock_observer(None)
        install_failpoint(None)
    assert events == [
        ("acquire", "registry", False, True, False),
        ("release", "registry", False, True, False),
        ("acquire", "registry", False, True, True),
        ("release", "registry", False, True, True),
    ]
    assert stages == ["queue-saved"]
    with lock(ctx, "registry"):
        pass
    failpoint("queue-saved")
    assert len(events) == 4
    assert stages == ["queue-saved"]


def test_lock_observer_reports_each_acquisition_mode_and_releases_on_error(tmp_path: Path) -> None:
    ctx = Context(tmp_path / "project", tmp_path / "state")
    events: list[tuple] = []
    install_lock_observer(lambda *event: events.append(event))
    try:
        with lock(ctx, "registry", nonblocking=True):
            pass
        # A deadline makes the acquisition a polled wait even when nonblocking was also requested.
        with lock(ctx, "registry", nonblocking=True, deadline_ns=time.monotonic_ns() + 5_000_000_000):
            pass
        with pytest.raises(RuntimeError), lock(ctx, "registry", shared=True):
            raise RuntimeError("failure inside the held section")
    finally:
        install_lock_observer(None)
    assert events == [
        ("acquire", "registry", False, False, False),
        ("release", "registry", False, False, False),
        ("acquire", "registry", False, True, True),
        ("release", "registry", False, True, True),
        ("acquire", "registry", True, True, False),
        ("release", "registry", True, True, False),  # reported although the held section raised
    ]


# --- the withdrawal teardown and the session guard

REPO = Path(__file__).resolve().parents[2]


def _enter(root: Path, name: str = "run"):
    """Enter one resource-free run in a fresh private root under ``root``; return the services and the handle."""
    project = root / f"{name}-project"
    project.mkdir(parents=True)
    services = build_test_services(root / f"{name}-state")
    return services, services.open_run(_spec(project)).enter()


def _record(handle) -> dict:
    return json.loads(handle._context.record.read_text())


def _named(handle, survivors: list[str]) -> bool:
    return any(str(handle._context.record) in line for line in survivors)


def _assert_no_survivor_under(directory: Path) -> None:
    """the outer census names no guardian whose run record lies under ``directory``."""
    inside = str(directory.resolve())
    wait_until(
        lambda: (
            not [
                line
                for line in testing.surviving_test_guardians(parent=os.getpid(), roots=[directory])
                if inside in line
            ]
        ),
        timeout=30,
        message=f"no toolkit guardian left under {inside}",
    )


def test_end_test_runs_withdraws_a_fake_resource_run_as_its_leave_would(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    fake = FakeDockerPort()
    services = build_test_services(tmp_path / "state")
    handle = services.open_run(_receipt_spec(project), docker_factory=lambda: fake, admit_docker=True).enter()
    _ = handle.docker_context
    descriptor = json.loads((handle.private_root / "docker-authority.json").read_text())
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("network", "create"))
    before = _record(handle)
    assert before["state"] == "active" and before["owned_resources"]["networks"]

    assert testing.end_test_runs(tmp_path) == [handle.run_id]

    withdrawal = _record(handle)["withdrawal"]  # kept by every later write of the guardian
    assert withdrawal["generation"] == before["generation"]
    assert withdrawal["revision"] == before["revision"] + 1
    assert withdrawal["requester"] == before["requester"]
    assert withdrawal["outcome"] == "cancelled"
    assert withdrawal["deadline"]["duration_ns"] == lifecycle._leave_budget_seconds(before) * 1_000_000_000
    # Nothing is signalled: the guardian acts on the withdrawal itself. Its cleanup's `_docker()` asks the host client
    # for the daemon id and refuses the fake's grant before any mutation, so it leaves through its guardian-error path
    # (a granted run's record keeps the withdrawal, unacknowledged) and exits, as it does today at session end.
    wait_guardians_exited(services.root / "registry")
    after = _record(handle)
    assert after["state"] == "stopping" and after["test_outcome"] == "cancelled"
    assert after["withdrawal"] == withdrawal
    _assert_no_survivor_under(tmp_path)


def test_end_test_runs_leaves_roots_outside_the_test_untouched(tmp_path: Path) -> None:
    inside_services, inside = _enter(tmp_path / "inside")
    outside_services, outside = _enter(tmp_path / "outside")

    assert testing.end_test_runs(tmp_path / "inside") == [inside.run_id]

    assert _record(outside)["state"] == "active" and _record(outside).get("withdrawal") is None
    outside.leave()
    wait_guardians_exited(inside_services.root / "registry")
    wait_guardians_exited(outside_services.root / "registry")
    _assert_no_survivor_under(tmp_path)


def test_end_test_runs_treats_a_vanished_registry_as_nothing_to_withdraw(tmp_path: Path) -> None:
    services = build_test_services(tmp_path / "state")
    registry = services.root / "registry"
    shutil.rmtree(registry)

    assert testing.end_test_runs(tmp_path) == []
    assert lifecycle.withdraw_test_runs(registry, recorded=registry) == []
    assert not registry.exists()


def test_withdraw_test_runs_refuses_a_runs_symlink_to_the_real_account(tmp_path: Path, real_account: Path) -> None:
    services = build_test_services(tmp_path / "state")
    runs = services.root / "registry" / "runs"
    shutil.rmtree(runs, ignore_errors=True)
    runs.symlink_to(real_account.parent / "runs", target_is_directory=True)
    try:
        with pytest.raises(ResourceError, match="outside their private registry"):
            testing.end_test_runs(tmp_path)
    finally:
        runs.unlink()


def test_withdraw_test_runs_refuses_a_runs_symlink_to_another_tests_registry(tmp_path: Path) -> None:
    other_services, other = _enter(tmp_path / "other")
    foreign = other._context.record.read_bytes()
    mine = build_test_services(tmp_path / "mine" / "state")
    runs = mine.root / "registry" / "runs"
    shutil.rmtree(runs, ignore_errors=True)
    runs.symlink_to(other_services.root / "registry" / "runs", target_is_directory=True)
    try:
        with pytest.raises(ResourceError, match="outside their private registry"):
            testing.end_test_runs(tmp_path / "mine")
        assert other._context.record.read_bytes() == foreign
    finally:
        runs.unlink()
    other.leave()
    wait_guardians_exited(other_services.root / "registry")
    _assert_no_survivor_under(tmp_path)


def test_end_test_runs_refuses_a_registry_symlink_to_another_tests_registry(tmp_path: Path) -> None:
    other_services, other = _enter(tmp_path / "other")
    foreign = other._context.record.read_bytes()
    mine = build_test_services(tmp_path / "mine" / "state")

    # (a) the test's registry itself replaced by a symlink to the other test's registry
    registry = mine.root / "registry"
    kept = registry.rename(mine.root / "registry-kept")
    registry.symlink_to(other_services.root / "registry", target_is_directory=True)
    try:
        with pytest.raises(ResourceError, match="no longer its recorded"):
            testing.end_test_runs(tmp_path / "mine")
        assert other._context.record.read_bytes() == foreign
    finally:
        registry.unlink()
        kept.rename(registry)

    # (b) the test's root re-pointed: the registry is no symlink itself but resolves into the other test's root
    root = mine.root
    moved = root.rename(root.parent / "state-kept")
    root.symlink_to(other_services.root, target_is_directory=True)
    try:
        with pytest.raises(ResourceError, match="no longer its recorded"):
            testing.end_test_runs(tmp_path / "mine")
        assert other._context.record.read_bytes() == foreign
    finally:
        root.unlink()
        moved.rename(root)

    other.leave()
    wait_guardians_exited(other_services.root / "registry")
    _assert_no_survivor_under(tmp_path)


def test_withdraw_test_runs_decides_on_the_record_reloaded_under_the_locks(tmp_path: Path) -> None:
    services, handle = _enter(tmp_path)
    context = handle._context
    events: list[tuple] = []
    holder = hold_lock_until_released(run_lock_path(context))
    result: dict[str, list[str]] = {}
    seam = threading.Thread(target=lambda: result.update(ids=testing.end_test_runs(tmp_path)))
    install_lock_observer(lambda *event: events.append(event))
    try:
        seam.start()
        # The seam has read the run active and holds the project lock; it now waits for the run lock.
        wait_until(lambda: ("acquire", context.project.project_id, True, True, False) in events, message="seam waits")
        record = _record(handle)
        lifecycle._apply_withdrawal(
            record, requester=record["requester"], outcome="passed", clock=lifecycle.NativeClock()
        )
        lifecycle.atomic_json(context.record, record)  # one atomic, loader-valid replacement (record-write rule)
    finally:
        release_lock(holder)
        seam.join(timeout=30)
        install_lock_observer(None)

    assert result == {"ids": []}
    assert _record(handle)["withdrawal"]["outcome"] == "passed"
    wait_guardians_exited(services.root / "registry")  # the guardian finished that withdrawal on its own
    _assert_no_survivor_under(tmp_path)


def test_session_guard_names_a_guardian_whose_withdrawal_cannot_be_written(tmp_path: Path) -> None:
    services, handle = _enter(tmp_path)
    context = handle._context
    entry = context.record.read_bytes()
    pid = pause_guardian(context)
    try:
        context.record.write_text("{")  # the loader refuses it; the guardian is paused (record-write rule)
        assert testing.end_test_runs(tmp_path) == []
        survivors = testing.surviving_test_guardians(parent=os.getpid(), roots=[services.root])
        assert any(f"pid {pid} " in line for line in survivors)
    finally:
        lifecycle.atomic_json(context.record, json.loads(entry))
        resume_guardian(pid)
    handle.leave()
    wait_guardians_exited(services.root / "registry")
    _assert_no_survivor_under(tmp_path)


def test_session_guard_census_names_an_open_run_guardian_outside_recorded_roots(tmp_path: Path) -> None:
    services, handle = _enter(tmp_path)

    assert _named(handle, testing.surviving_test_guardians(parent=os.getpid(), roots=[]))

    handle.leave()
    wait_guardians_exited(services.root / "registry")
    _assert_no_survivor_under(tmp_path)


def test_census_names_a_guardian_by_its_context_record_alone(tmp_path: Path) -> None:
    services, handle = _enter(tmp_path)
    stranger = os.getppid()  # not the guardian's parent

    assert not _named(handle, testing.surviving_test_guardians(parent=stranger, roots=[]))
    assert _named(handle, testing.surviving_test_guardians(parent=stranger, roots=[services.root]))

    handle.leave()
    wait_guardians_exited(services.root / "registry")
    _assert_no_survivor_under(tmp_path)


_INNER_SPEC = """
import os
from pathlib import Path

import pytest

from docker_test_toolkit import ArtifactIdentity, PhaseBudget, RunSpec
from docker_test_toolkit.factories import testing
from docker_test_toolkit.factories.testing import build_test_services


def _enter(root: Path):
    project = root / "project"
    project.mkdir(parents=True)
    services = build_test_services(root / "state")
    handle = services.open_run(
        RunSpec(
            project_root=project,
            entrypoint="integration",
            purpose="private-proof",
            services=("postgres",),
            closure=("postgres",),
            profile="fixture",
            phases={"tests": PhaseBudget(1, 1, 1, 1)},
            initial_phase="tests",
            artifact=ArtifactIdentity(
                "0.1.0",
                "a" * 64,
                {"activation": 1, "batch": 1, "cli": 1, "distribution": 1,
                 "queue": 1, "receipt": 1, "registry": 2, "run": 1},
            ),
        )
    ).enter()
    with open(os.environ["INNER_REPORT"], "a") as report:
        report.write(f"{services.root / 'registry'} {handle._context.record}\\n")
    return services, handle
"""


def _inner_session(tmp_path: Path, name: str, body: str, *options: str):
    """Run ``body`` as an inner pytest session in a subprocess with the repository's conftest plugins.

    Return the completed process, the (registry, record) pairs its runs reported, and the inner basetemp (on Linux under
    the repository's ``.pytest-tmp/``, where conftest requires ext-family storage for an e2e item).
    """
    directory = tmp_path / name
    directory.mkdir()
    source = directory / f"test_inner_{name}.py"
    source.write_text(_INNER_SPEC + textwrap.dedent(body))
    report = directory / "runs.txt"
    report.touch()
    if sys.platform.startswith("linux"):
        basetemp = REPO / ".pytest-tmp" / f"inner-{uuid.uuid4().hex}"
        basetemp.parent.mkdir(exist_ok=True)  # a fresh checkout has no .pytest-tmp; pytest creates only the leaf
    else:
        basetemp = directory / "basetemp"
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-c",
            str(REPO / "pyproject.toml"),
            "-p",
            "tests.conftest",
            "-p",
            "no:cacheprovider",
            "-q",
            f"--basetemp={basetemp}",
            "--toolkit-guardian-wait=1",
            *options,
            str(source),
        ],
        cwd=REPO,
        env={**os.environ, "INNER_REPORT": str(report)},
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    runs = [tuple(Path(part) for part in line.split()) for line in report.read_text().splitlines()]
    return completed, runs, basetemp


def _end_inner_runs(runs: list[tuple[Path, Path]], basetemp: Path) -> None:
    """End every run an inner session left (its requester is gone), wait for the exits, then drop a Linux basetemp."""
    for registry, _record_path in runs:
        lifecycle.withdraw_test_runs(registry, recorded=registry.resolve())
    for registry, _record_path in runs:
        wait_guardians_exited(registry)
    if basetemp.is_relative_to(REPO):
        shutil.rmtree(basetemp, ignore_errors=True)


def test_teardown_hook_does_nothing_for_a_test_without_tmp_path(tmp_path: Path) -> None:
    completed, runs, basetemp = _inner_session(
        tmp_path,
        "module",
        """
        @pytest.fixture(scope="module")
        def module_run(tmp_path_factory):
            _services, handle = _enter(tmp_path_factory.mktemp("module-root"))
            yield handle
            handle.leave()


        def test_without_tmp_path(module_run):
            assert module_run._context.record.exists()


        def test_neighbour_with_its_own_tmp_path(module_run, tmp_path):
            assert module_run._context.record.exists() and tmp_path.is_dir()


        def test_after_their_teardowns(module_run):
            import json

            assert json.loads(module_run._context.record.read_text())["state"] == "active"
        """,
    )
    try:
        assert completed.returncode == 0, completed.stdout + completed.stderr
        assert "3 passed" in completed.stdout
        assert "toolkit guardian" not in completed.stdout
    finally:
        _end_inner_runs(runs, basetemp)
    _assert_no_survivor_under(tmp_path)


def test_session_guard_names_a_non_e2e_leak_in_a_mixed_session(tmp_path: Path) -> None:
    completed, runs, basetemp = _inner_session(
        tmp_path,
        "mixed",
        """
        @pytest.mark.e2e
        def test_e2e_item_keeps_its_run(tmp_path):
            _enter(tmp_path / "e2e")


        def test_non_e2e_item_leaks_outside_every_recorded_path(tmp_path_factory):
            services, _handle = _enter(tmp_path_factory.mktemp("unrecorded"))
            del testing._BUILT_ROOTS[services.root]  # neither a recorded root nor a test's tmp_path: parentage only
        """,
    )
    try:
        assert completed.returncode == 1, completed.stdout + completed.stderr
        records = [str(record) for _registry, record in runs]  # the inner session may run its tests in any order
        (leak_record,) = [record for record in records if "/unrecorded" in record]
        (e2e_record,) = [record for record in records if record != leak_record]
        assert leak_record in completed.stdout
        assert e2e_record not in completed.stdout
    finally:
        _end_inner_runs(runs, basetemp)
    _assert_no_survivor_under(tmp_path)


def test_session_guard_fails_a_session_whose_teardown_hook_is_removed(tmp_path: Path) -> None:
    body = """
        def test_leaves_its_run_active(tmp_path):
            _enter(tmp_path)
        """
    guarded, guarded_runs, guarded_basetemp = _inner_session(tmp_path, "guarded", body)
    removed, removed_runs, removed_basetemp = _inner_session(
        tmp_path, "removed", body, "-p", "no:toolkit-guardian-teardown"
    )
    try:
        assert guarded.returncode == 0, guarded.stdout + guarded.stderr
        assert "toolkit guardian" not in guarded.stdout
        assert removed.returncode == 1, removed.stdout + removed.stderr
        assert str(removed_runs[0][1]) in removed.stdout
    finally:
        _end_inner_runs(guarded_runs, guarded_basetemp)
        _end_inner_runs(removed_runs, removed_basetemp)
    _assert_no_survivor_under(tmp_path)
