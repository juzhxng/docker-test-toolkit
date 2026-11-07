"""Run one toolkit entry in a child process with failpoints installed in THIS process.

``recovery.failpoint`` is a no-op unless a callback is installed in the same process, so a test whose failpoint must
fire in another process starts this program: ``python -P failpoint_child.py [--archive PYZ] SPEC.json``.

The spec is JSON: ``{"entry": <name>, "arguments": {...}, "stages": {<stage>: {"action": <step or list>,
"occurrence": <n>}}, "result": <path>, "observer_log": <path>}``. A step is ``{"do": "write-file", "path": ...,
"capture": {"record": <path>, "queue": <path>}}`` (the marker holds this process's ``time.monotonic_ns()`` and, with
``capture``, the hex of the named files), ``{"do": "wait-file", "path": ...}`` (30 s cap, then fail),
``{"do": "wait-until-deadline"}`` (block until the entry's ``budget_seconds`` deadline plus 50 ms has passed),
``{"do": "sleep", "seconds": n}``, ``{"do": "sigkill"}`` or ``{"do": "raise", "type": "ResourceError" |
"InfrastructureFailure" | "ControlError" | "OSError", "reason": ..., "observer": ..., "message": ...}`` (raise that
error on the stage's own stack); a list runs in order and ``occurrence`` selects the n-th call of that stage (a stage
may also map to a list of such entries, one per occurrence). With ``observer_log`` every lock event and every stage is
appended, in order, as one JSON line stamped with this process's ``time.monotonic_ns()``. No production code reads this
file or any environment variable for failpoints: the callback is installed here, in the process that runs the entry.
"""

from __future__ import annotations

import errno
import glob
import hashlib
import json
import os
import signal
import subprocess
import sys
import time
from collections.abc import Callable
from pathlib import Path

START_NS = time.monotonic_ns()
DEADLINE_NS: int | None = None  # START_NS + the entry's budget; set by main for entries that take one
ARCHIVE: str | None = None  # the ``--archive`` snapshot this program runs the toolkit from, if any


def _raised(step: dict) -> BaseException:
    """Build the error a ``raise`` step throws: the toolkit's own error types, or a raw ``OSError``."""
    from docker_test_toolkit.application import lifecycle
    from docker_test_toolkit.domain.errors import InfrastructureFailure, ResourceError

    if step["type"] == "OSError":
        return OSError(errno.EACCES, step["message"])
    keywords = {"reason": step["reason"], "observer": step["observer"]}
    if step["type"] == "ControlError":
        return lifecycle.ControlError(step["message"], subprocess.CompletedProcess([], 1, b"", b""), **keywords)
    if step["type"] == "ResourceError":
        return ResourceError(step["message"], **keywords)
    if step["type"] == "InfrastructureFailure":
        return InfrastructureFailure(step["message"], **keywords)
    raise ValueError(f"unknown raise type {step['type']!r}")


def _run_step(step: dict | str) -> None:
    kind = step if isinstance(step, str) else step["do"]
    if kind == "sigkill":
        os.kill(os.getpid(), signal.SIGKILL)
    elif kind == "raise":
        raise _raised(step)
    elif kind == "write-file":
        marker: dict = {"monotonic_ns": time.monotonic_ns()}
        for name, path in step.get("capture", {}).items():
            marker[name] = Path(path).read_bytes().hex()
        Path(step["path"]).write_text(json.dumps(marker))
    elif kind == "sleep":
        time.sleep(step["seconds"])
    elif kind == "chmod":
        for match in glob.glob(step["glob"]):
            os.chmod(match, step["mode"])
    elif kind == "swap-copy-inode":
        # The single recovery copy under the test-owned TMPDIR: record whether it still equals the loaded archive's
        # digest, then replace it with byte-identical content on a new inode (the zip stays valid; only the inode the
        # attestation bound changes).
        (copy,) = glob.glob(step["glob"])
        original = Path(copy).read_bytes()
        matched = (
            ARCHIVE is not None
            and hashlib.sha256(original).hexdigest() == hashlib.sha256(Path(ARCHIVE).read_bytes()).hexdigest()
        )
        Path(step["marker"]).write_text("digest-matched" if matched else "digest-mismatched")
        sibling = Path(copy).with_name(Path(copy).name + ".swap")
        descriptor = os.open(sibling, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(original)
        os.replace(sibling, copy)
    elif kind == "wait-until-deadline":
        if DEADLINE_NS is None:
            raise ValueError("wait-until-deadline needs an entry budget")
        while time.monotonic_ns() <= DEADLINE_NS + 50_000_000:
            time.sleep(0.02)
    elif kind == "wait-file":
        deadline = time.monotonic() + 30
        while not Path(step["path"]).exists():
            if time.monotonic() > deadline:
                raise TimeoutError(f"wait-file {step['path']} expired")
            time.sleep(0.02)
    else:
        raise ValueError(f"unknown failpoint step {kind!r}")


def _install(spec: dict) -> Callable[[str], None]:
    """Install the stage callback for this process and return it (an entry may also hand it to a constructor seam)."""
    from docker_test_toolkit.application import recovery

    counts: dict[str, int] = {}
    stages = spec.get("stages", {})
    log_path = spec.get("observer_log")

    def log(**line: object) -> None:
        with open(log_path, "a") as stream:
            stream.write(json.dumps({"t": time.monotonic_ns(), **line}) + "\n")

    if log_path:
        recovery.configure_lock_observer(
            lambda event, name, shared, wait, bounded: log(
                kind="lock", event=event, name=name, shared=shared, wait=wait, bounded=bounded
            )
        )

    def callback(stage: str) -> None:
        if log_path:
            log(kind="stage", stage=stage)
        counts[stage] = counts.get(stage, 0) + 1
        configured = stages.get(stage)
        entries = configured if isinstance(configured, list) else [] if configured is None else [configured]
        for entry in entries:
            if entry.get("occurrence", counts[stage]) != counts[stage]:
                continue
            actions = entry["action"]
            for step in actions if isinstance(actions, list) else [actions]:
                _run_step(step)

    recovery.configure_failpoint(callback)
    return callback


def _facade_build(arguments: dict) -> int:
    """Open and enter a private run with a fake Docker CLI, build through the shim with the facade's environment
    unchanged, and write the facade's environment keys and the build receipt's cache object to ``output``."""
    from docker_test_toolkit import ArtifactIdentity, PhaseBudget, RunSpec
    from docker_test_toolkit.adapters.outbound.docker import DockerCliAdapter
    from docker_test_toolkit.application.supervision import private_process_environment
    from docker_test_toolkit.factories.testing import build_test_services

    def docker():
        environment = private_process_environment(private_additions={"DOCKER_CONTEXT": "fixture"})
        environment["PATH"] = f"{arguments['bin']}{os.pathsep}{environment['PATH']}"
        return DockerCliAdapter.discover(environment=environment)

    project = Path(arguments["project_root"])
    run = RunSpec(
        project_root=project,
        entrypoint="integration",
        purpose="build-cache-switch",
        services=(),
        closure=(),
        profile="fixture",
        phases={"tests": PhaseBudget(2 * 1024**3, 128 * 1024**2, 2, 1)},
        initial_phase="tests",
        artifact=ArtifactIdentity(**arguments["artifact"]),
        wait_seconds=20,
    )
    handle = build_test_services(Path(arguments["state"])).open_run(run, docker_factory=docker).enter()
    try:
        context = handle.docker_context
        subprocess.run(
            [str(context.executable), "buildx", "build", "--tag", "switch:1", "."],
            env=dict(context.environment),
            cwd=project,
            capture_output=True,
            check=False,
        )
        record = json.loads(handle._context.record.read_text())
        receipt = next(item for item in record["docker_receipts"].values() if item["kind"] == "build")
        result = {"keys": sorted(context.environment), "cache": receipt["plan"].get("cache")}
        Path(arguments["output"]).write_text(json.dumps(result))
    finally:
        handle.leave()
    return 0


def _cache_entry(entry: str, arguments: dict) -> int:
    """Run one build-cache store operation; ``output`` receives its JSON result."""
    from docker_test_toolkit.application import build_cache

    project_cache, platform = Path(arguments["project_cache"]), arguments["platform"]
    platform_dir = project_cache / platform
    if entry == "cache-hold-shared":
        # Hold the shared anchor as a build does, signal through ``acquired`` and hold until ``release`` exists.
        with build_cache.platform_anchor(project_cache, platform, shared=True):
            Path(arguments["acquired"]).write_text(str(os.stat(project_cache / f"{platform}.lock").st_ino))
            _run_step({"do": "wait-file", "path": arguments["release"]})
            if "anchor_inode" in arguments:
                Path(arguments["anchor_inode"]).write_text(str(os.stat(project_cache / f"{platform}.lock").st_ino))
        return 0
    if entry == "cache-reconcile":
        with build_cache.platform_anchor(project_cache, platform, shared=False):
            Path(arguments.get("acquired", os.devnull)).write_text("acquired")
            build_cache.reconcile(platform_dir)
        result: object = "reconciled"
    elif entry == "cache-remove":
        result = build_cache.remove_platform(project_cache, platform)
    else:
        result = build_cache.publish(
            platform_dir,
            project_cache,
            platform,
            arguments["plan"],
            run_id=arguments["run_id"],
            operation_id=arguments["operation_id"],
        )
    Path(arguments["output"]).write_text(json.dumps(result))
    return 0


def _entry(spec: dict, callback: Callable[[str], None]) -> int:
    from docker_test_toolkit.application import lifecycle

    arguments = spec["arguments"]
    if spec["entry"] == "guardian":
        return lifecycle.main(
            ["_guardian", "--context", arguments["context"], "--test-directory", arguments["test_directory"]]
        )
    if spec["entry"] == "prepare-run":
        # The requester side of a run: prepare one ungranted run and exit; the requester is then gone.

        root = Path(arguments["project_root"])
        request = lifecycle.RunRequest(
            root,
            "integration",
            arguments["purpose"],
            (),
            (),
            "fixture",
            parent_session=arguments.get("parent_session"),
            wait_seconds=60,
        )
        state, directory = Path(arguments["state"]), Path(arguments["test_directory"])
        lifecycle.prepare_run(request, _test_state=state, _test_directory=directory)
        return 0
    if spec["entry"] == "facade-build":
        return _facade_build(arguments)
    if spec["entry"] in ("cache-publish", "cache-remove", "cache-reconcile", "cache-hold-shared"):
        return _cache_entry(spec["entry"], arguments)
    if spec["entry"] == "requester":
        # A requester that prepares, configures and enters one empty run, holds until released or killed, then
        # (with ``leave``) leaves it.
        from docker_test_toolkit.domain.models import PhaseBudget

        root = Path(arguments["project_root"])
        request = lifecycle.RunRequest(
            root,
            "integration",
            arguments["purpose"],
            (),
            (),
            "fixture",
            wait_seconds=arguments.get("wait_seconds", 60),
        )
        state, directory = Path(arguments["state"]), Path(arguments["test_directory"])
        context = lifecycle.prepare_run(request, _test_state=state, _test_directory=directory)
        Path(arguments["context_file"]).write_text(lifecycle._context_payload(context))
        lifecycle.configure_run(
            context,
            services=(),
            closure=(),
            phases={"tests": PhaseBudget(0, 1, 1, 1)},
            initial_phase="tests",
            topology="native" if sys.platform == "linux" else "desktop",
        )
        if not arguments.get("launch_guardian", True):
            lifecycle.start_guardian_attempt(context)
        if arguments.get("gate_file"):
            _run_step({"do": "wait-file", "path": arguments["gate_file"]})  # the test enters the run when it chooses
        lease = lifecycle.enter_run(context, _test_directory=directory)
        Path(arguments["entered_file"]).write_text("entered")
        _run_step({"do": "wait-file", "path": arguments["release_file"]})
        if arguments.get("leave"):
            lifecycle.leave_run(lease, "passed")  # a stage such as leave-withdrawal-written ends it here
        return 0
    if spec["entry"] == "reconcile-account":
        # One detection-and-spawn pass of an observer with no queue entry of its own (the spawner side of takeover).
        from docker_test_toolkit.application import lifecycle

        lifecycle.reconcile_account(_test_directory=Path(arguments["test_directory"]))
        return 0
    if spec["entry"] == "recover-run":
        # One recoverer: operator mode (default) or worker mode (``drain_live_consumers`` false plus its attempt).
        from docker_test_toolkit.application import lifecycle

        if arguments.get("fake_docker"):
            _configure_fake_docker(Path(arguments["fake_docker"]))
        context = lifecycle.load_operator_context(arguments["context"])
        extra = {} if DEADLINE_NS is None else {"_deadline_ns": DEADLINE_NS}
        lifecycle.recover_run(
            context,
            _test_directory=Path(arguments["test_directory"]),
            drain_live_consumers=arguments.get("drain_live_consumers", True),
            takeover_attempt=arguments.get("takeover_attempt"),
            **extra,
        )
        return 0
    if spec["entry"] == "takeover-worker":
        # The worker exactly as the spawner starts it: the child command with its fixed arguments.
        from docker_test_toolkit.application import lifecycle

        worker = ("--context", arguments["context"], "--attempt", arguments["attempt"])
        return lifecycle.main(["_recover", *worker, "--test-directory", arguments["test_directory"]])
    raise ValueError(f"unknown failpoint_child entry {spec['entry']!r}")


def _configure_fake_docker(binary_directory: Path) -> None:
    """Compose the same fake Docker CLI the test process configured (a stateful shim in ``binary_directory``)."""
    from docker_test_toolkit.adapters.outbound.docker import DockerCliAdapter
    from docker_test_toolkit.application import lifecycle
    from docker_test_toolkit.application.supervision import private_process_environment

    def environment() -> dict[str, str]:
        environ = private_process_environment(private_additions={"DOCKER_CONTEXT": "fixture"})
        environ["PATH"] = f"{binary_directory}{os.pathsep}{environ['PATH']}"
        return environ

    lifecycle.configure_docker_factories(
        lambda: DockerCliAdapter.discover(environment=environment()),
        lambda identity: DockerCliAdapter.from_identity(identity, environment=environment()),
    )


def main(argv: list[str]) -> int:
    archive = None
    if argv[:1] == ["--archive"]:
        archive, argv = argv[1], argv[2:]
        sys.path.insert(0, archive)
    spec = json.loads(Path(argv[0]).read_text())
    global ARCHIVE, DEADLINE_NS
    ARCHIVE = archive
    budget = spec.get("arguments", {}).get("budget_seconds")
    DEADLINE_NS = None if budget is None else START_NS + int(budget * 1_000_000_000)
    error: BaseException | None = None
    exit_code = 0
    try:
        exit_code = _entry(spec, _install(spec))
    except BaseException as caught:  # the test reads the outcome from the result file
        error = caught
        exit_code = 1
    finally:
        Path(spec["result"]).write_text(
            json.dumps(
                {
                    "elapsed_s": (time.monotonic_ns() - START_NS) / 1e9,
                    "exit_code": exit_code,
                    "error_type": None if error is None else type(error).__name__,
                    "reason": getattr(error, "reason", None),
                    "message": None if error is None else str(error),
                }
            )
        )
    return exit_code


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
