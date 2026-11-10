"""Shared integration helpers for private-account, lock and takeover tests."""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import re
import signal
import stat
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import ClassVar, Protocol

from docker_test_toolkit import PhaseBudget, RunSpec
from docker_test_toolkit.application import admission, lifecycle
from docker_test_toolkit.application.admission import STATE_MARKER
from docker_test_toolkit.application.api import RunHandle
from docker_test_toolkit.application.recovery import Context, atomic_json, build_records, lock
from docker_test_toolkit.domain.errors import ResourceError
from docker_test_toolkit.domain.models import SUPPORTED_PROTOCOLS, TOOLKIT_VERSION, ArtifactIdentity, DockerResult
from docker_test_toolkit.domain.records import canonical_json_bytes, record_cell_id
from docker_test_toolkit.factories.testing import _TestServices, build_test_services


def _hold_lock(path: Path) -> subprocess.Popen[str]:
    """Start a real process that holds one exclusive flock on ``path`` until it is killed."""
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import fcntl, os, sys, time\n"
            "fd = os.open(sys.argv[1], os.O_RDWR | os.O_CREAT, 0o600)\n"
            "fcntl.flock(fd, fcntl.LOCK_EX)\nprint('held', flush=True)\ntime.sleep(30)\n",
            str(path),
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert process.stdout is not None
    assert process.stdout.readline().strip() == "held"
    return process


def hold_lock_until_released(path: Path) -> subprocess.Popen[str]:
    """Start a real process that holds one exclusive flock on ``path`` until ``release_lock`` is called.

    Unlike ``_hold_lock`` it never lets go by itself: the lock is held exactly until the test says so.
    """
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import fcntl, os, sys\n"
            "fd = os.open(sys.argv[1], os.O_RDWR | os.O_CREAT, 0o600)\n"
            "fcntl.flock(fd, fcntl.LOCK_EX)\nprint('held', flush=True)\nsys.stdin.readline()\n",
            str(path),
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    assert process.stdout is not None
    assert process.stdout.readline().strip() == "held"
    return process


def release_lock(holder: subprocess.Popen[str]) -> None:
    """Release a ``hold_lock_until_released`` lock; the release has completed (the holder exited) on return."""
    assert holder.stdin is not None
    if not holder.stdin.closed:
        with contextlib.suppress(BrokenPipeError):
            holder.stdin.write("\n")
            holder.stdin.flush()
        with contextlib.suppress(BrokenPipeError):
            holder.stdin.close()
    holder.wait(timeout=10)
    if holder.stdout is not None:
        holder.stdout.close()


def run_lock_path(context: lifecycle.RunContext) -> Path:
    """Return the run's permanent lock anchor."""
    return context.project.state / f"run-{context.run_id}.lock"


def try_lock(path: Path) -> bool:
    """Whether nobody holds ``path`` exclusively right now (the probe lock is released at once)."""
    with path.open("r+") as stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        fcntl.flock(stream, fcntl.LOCK_UN)
        return True


PROJECT_LOCK = re.compile(r"[0-9a-f]{64}\.lock")  # a project's lifetime lock, named by its project id


def _process_stopped(pid: int) -> bool:
    """Whether ``pid`` is in the stopped state now (``/proc`` on Linux, ``ps`` on macOS)."""
    proc_stat = Path(f"/proc/{pid}/stat")
    if proc_stat.exists():
        return proc_stat.read_text().rsplit(")", 1)[1].split()[0] in {"T", "t"}
    ps = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True, check=False)
    return ps.stdout.strip().startswith("T")


def _guardian_lock_anchors(context: lifecycle.RunContext) -> list[Path]:
    """Every project and account lock anchor the guardian can hold inside a section.

    Two locks span the guardian's lifetime and are left out: its shared project lock and the account capacity lock it
    holds while granted (taken only non-blocking, so nobody can wait on it). Other projects' lifetime locks in a shared
    state directory are left out too: their own guardians hold them, and this guardian never takes them.
    """
    account = Path(json.loads(context.record.read_text())["account_directory"])
    lifetime = {context.project.state / f"{context.project.project_id}.lock", account / "capacity.lock"}
    project_locks = {path for path in context.project.state.glob("*.lock") if PROJECT_LOCK.fullmatch(path.name)}
    anchors = {*context.project.state.glob("*.lock"), *account.glob("*.lock")}
    return sorted(anchors - lifetime - project_locks)


def pause_guardian(context: lifecycle.RunContext) -> int:
    """SIGSTOP the run's guardian while it idles (it holds no lock but its lifetime shared project lock).

    The probe runs only once the kernel reports the guardian stopped, and covers every project and account anchor, not
    only the run lock: a guardian stopped inside a registry or metadata section would block the test's own lock forever.
    A stopped guardian still observes as executing. Return its pid for ``resume_guardian``.
    """
    pid = json.loads(context.record.read_text())["guardian"]["pid"]
    for _attempt in range(200):
        os.kill(pid, signal.SIGSTOP)
        stop_deadline = time.monotonic() + 5
        while not _process_stopped(pid):
            assert time.monotonic() < stop_deadline, "the guardian did not stop within 5 s"
            time.sleep(0.005)
        busy = [anchor.name for anchor in _guardian_lock_anchors(context) if not try_lock(anchor)]
        if not busy:
            return pid
        os.kill(pid, signal.SIGCONT)  # it was inside a section: let it finish and try again
        time.sleep(0.02)
    raise AssertionError(f"the guardian never idled; still held: {busy}")


def resume_guardian(pid: int) -> None:
    """SIGCONT a guardian stopped by ``pause_guardian`` (a no-op for a process that already ended)."""
    with contextlib.suppress(ProcessLookupError):
        os.kill(pid, signal.SIGCONT)


def prewrite_withdrawal(context: lifecycle.RunContext, outcome: str, seconds: int) -> dict:
    """Write the requester's withdrawal with a ``seconds`` budget, exactly as ``leave_run`` would; return it.

    ``leave_run`` then takes its existing-withdrawal branch. The budget is test input (production writes at least
    30 s); call it only while the guardian is paused, so it cannot act on the active record during the write.
    """
    project = context.project
    with lock(project, project.project_id, shared=True), lock(project, f"run-{context.run_id}"):
        data = json.loads(context.record.read_text())
        withdrawal = {
            "generation": data["generation"],
            "revision": data["revision"] + 1,
            "requester": data["requester"],
            "outcome": outcome,
            "deadline": asdict(lifecycle.Deadline.after(lifecycle.NativeClock(), seconds)),
        }
        data.update(state="stopping", withdrawal=withdrawal, test_outcome=outcome, cleanup_outcome="pending")
        data["revision"] = withdrawal["revision"]
        atomic_json(context.record, data)
    return withdrawal


def private_account_directory(tmp_path: Path) -> Path:
    """Create a 0700 private account directory whose metadata anchor carries the queue initialization marker."""
    account = tmp_path / "account"
    account.mkdir(mode=0o700)
    anchor = account / "metadata.lock"
    anchor.write_text(STATE_MARKER)
    anchor.chmod(0o600)
    return account


def queue_entry(run: int, ticket: int, phase: str = "queued") -> tuple[str, dict]:
    """Build one coherent version 2 queue entry and its run id; the holder is set exactly for admitting/granted."""
    run_id = f"{run:08d}-0000-4000-8000-000000000000"
    project_root = f"/tmp/project-{run}"
    registry = f"/tmp/registry-{run}"
    holder = {"pid": run, "origin": "test", "birth": [run], "audit_token": None} if phase != "queued" else None
    return run_id, {
        "ticket": ticket,
        "attempt": f"{run:08d}-1111-4111-8111-111111111111",
        "record": f"{registry}/runs/{run_id}.json",
        "registry": registry,
        "project_root": project_root,
        "project_id": hashlib.sha256(project_root.encode()).hexdigest(),
        "phase": phase,
        "holder": holder,
        "dead_since": None,
        "takeover": None,
        "last_completed_phase": None,
    }


def write_v2_queue(account: Path, entries: list[tuple[str, dict]], *, next_ticket: int | None = None) -> bytes:
    """Write a version 2 queue file exactly as the codec would and return its bytes."""
    highest = max((entry["ticket"] for _, entry in entries), default=0)
    data = canonical_json_bytes(
        {
            "version": 2,
            "next_ticket": next_ticket if next_ticket is not None else highest + 1,
            "entries": dict(entries),
        }
    )
    write_raw_queue(account, data)
    return data


def write_raw_queue(account: Path, data: bytes) -> None:
    """Write arbitrary queue bytes privately (for classification and refusal cases), under an initialized anchor."""
    anchor = account / "metadata.lock"
    anchor.touch(mode=0o600)
    if not anchor.read_bytes():
        anchor.write_text(STATE_MARKER)
    path = account / "queue.json"
    path.write_bytes(data)
    path.chmod(0o600)


EMPTY_QUEUE = b'{"entries":{},"next_ticket":1,"version":2}\n'


def give_account_its_queue(account: Path) -> None:
    """Give an account the initialization marker and the empty queue file a used account owns."""
    write_raw_queue(account, EMPTY_QUEUE)


def three_private_projects(tmp_path: Path) -> tuple[_TestServices, list[Path]]:
    """Create one private test account and three distinct project roots under it, each a checkout of its own
    repository (a distinct Git origin, as share verdicts are keyed by it)."""
    projects = []
    for name in ("project-a", "project-b", "project-c"):
        root = tmp_path / name
        (root / ".git").mkdir(parents=True)
        (root / ".git" / "config").write_text(f'[remote "origin"]\n\turl = https://example.invalid/{name}.git\n')
        projects.append(root)
    return build_test_services(tmp_path / "state"), projects


def wait_until(condition: Callable[[], bool], *, timeout: float = 20.0, message: str = "condition") -> None:
    """Poll a test-side observation until it holds, failing loudly on expiry (never a silent hang)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(0.05)
    raise AssertionError(f"timed out after {timeout} s waiting for {message}")


class EnterThread:
    """Run the requester's entry in a thread and keep its outcome for later assertions.

    By default it is ``handle.enter()`` (which configures the run and launches the toolkit's own guardian). With
    ``existing_attempt`` the run was configured and its guardian attempt started by ``prepare_for_child_guardian``, so
    ``enter_run`` only waits and the test's own guardian process (``start_guardian_child``) enrolls.
    """

    def __init__(self, handle: RunHandle, *, existing_attempt: bool = False) -> None:
        self.error: BaseException | None = None
        self.ended_tick: int | None = None  # the requester's monotonic tick when its entry returned or raised
        self._thread = threading.Thread(target=self._run, args=(handle, existing_attempt))
        self._thread.start()

    def _run(self, handle: RunHandle, existing_attempt: bool) -> None:
        try:
            if existing_attempt:
                handle._lease = lifecycle.enter_run(handle._context, _test_directory=handle._test_directory)
            else:
                handle.enter()
        except BaseException as error:  # the test inspects it
            self.error = error
        finally:
            self.ended_tick = lifecycle.NativeClock().read()

    def join(self, timeout: float = 20.0) -> None:
        self._thread.join(timeout)
        assert not self._thread.is_alive(), "enter() did not finish"


def prepare_for_child_guardian(handle: RunHandle) -> None:
    """Configure a prepared run as ``RunHandle.enter`` does, then start its guardian attempt without launching."""
    spec = handle._spec
    lifecycle.configure_run(
        handle._context,
        services=spec.services,
        closure=spec.closure,
        phases={name: lifecycle.PhaseBudget(**vars(budget)) for name, budget in spec.phases.items()},
        initial_phase=spec.initial_phase or "",
        topology="native" if sys.platform == "linux" else "desktop",
    )
    lifecycle.start_guardian_attempt(handle._context)


def queue_entries(account: Path) -> dict[str, admission.QueueEntry]:
    """Read the account queue through the codec and return its entries by run id (for assertions only)."""
    with lock(Context(account, account), "metadata") as anchor:
        # An assertion-only read: the no-op validator stands in for the activation gate the code under test installs.
        state = admission.load_queue(account, anchor)
    return dict(state.entries)


def queue_phases(account: Path) -> dict[str, str]:
    """Return each queue entry's phase by run id (for assertions only)."""
    return {run_id: entry.phase for run_id, entry in queue_entries(account).items()}


FAILPOINT_CHILD = Path(__file__).with_name("failpoint_child.py")
OPEN_RUN_CHILD = Path(__file__).with_name("open_run_child.py")


def start_guardian_child(
    tmp_path: Path,
    handle: RunHandle | lifecycle.RunContext,
    account: Path,
    stages: dict,
    *,
    name: str = "guardian",
    observer_log: Path | None = None,
) -> tuple[subprocess.Popen[bytes], Path]:
    """Start ``failpoint_child.py`` as the guardian of an already prepared run (so failpoints fire in it).

    Call ``prepare_for_child_guardian`` first, so ``enter_run`` waits on the attempt instead of launching a guardian of
    its own. Returns the child and its result file.
    """
    result = tmp_path / f"{name}.result.json"
    spec = tmp_path / f"{name}.spec.json"
    context = handle if isinstance(handle, lifecycle.RunContext) else handle._context
    document = {
        "entry": "guardian",
        "arguments": {"context": lifecycle._context_payload(context), "test_directory": str(account)},
        "stages": stages,
        "result": str(result),
    }
    if observer_log is not None:
        document["observer_log"] = str(observer_log)
    spec.write_text(json.dumps(document))
    return subprocess.Popen([sys.executable, "-P", str(FAILPOINT_CHILD), str(spec)]), result


@dataclass
class RequesterChild:
    """A requester process (the ``requester`` entry of ``failpoint_child.py``) and the files it signals through."""

    process: subprocess.Popen[bytes]
    result: Path
    context_file: Path
    entered_file: Path
    release_file: Path
    gate_file: Path

    def context(self) -> lifecycle.RunContext:
        """Wait until the child has prepared its run and return the run's context."""
        wait_until(self.context_file.exists, timeout=30, message="the requester prepared its run")
        return lifecycle.load_operator_context(self.context_file.read_text())

    def open_gate(self) -> None:
        """Let a gated requester call ``enter_run`` (it waits for this after configuring its run)."""
        self.gate_file.write_text("")

    def release(self) -> None:
        """Let the entered requester continue (to leave, when it was started with ``leave``)."""
        self.release_file.write_text("")


def start_requester_child(
    tmp_path: Path,
    state: Path,
    account: Path,
    project_root: Path,
    stages: dict,
    *,
    name: str = "requester",
    launch_guardian: bool = True,
    leave: bool = False,
    gated: bool = False,
    new_session: bool = False,
) -> RequesterChild:
    """Start one requester that prepares, configures and enters an empty run, then holds until released or killed.

    With ``launch_guardian`` false only the guardian attempt is started, so the test composes the guardian itself
    (``start_guardian_child``); with ``leave`` the requester leaves its run after the release; with ``gated`` it waits
    for ``open_gate`` before it enters, so a test can observe the state the other actors leave first; with
    ``new_session`` it leads its own process group, as a shell's job does.
    """
    result, spec = tmp_path / f"{name}.result.json", tmp_path / f"{name}.spec.json"
    files = {key: tmp_path / f"{name}.{key}" for key in ("context", "entered", "release", "gate")}
    spec.write_text(
        json.dumps(
            {
                "entry": "requester",
                "arguments": {
                    "state": str(state),
                    "test_directory": str(account),
                    "project_root": str(project_root),
                    "purpose": name,
                    "context_file": str(files["context"]),
                    "entered_file": str(files["entered"]),
                    "release_file": str(files["release"]),
                    "gate_file": str(files["gate"]) if gated else None,
                    "launch_guardian": launch_guardian,
                    "leave": leave,
                },
                "stages": stages,
                "result": str(result),
            }
        )
    )
    process = subprocess.Popen([sys.executable, "-P", str(FAILPOINT_CHILD), str(spec)], start_new_session=new_session)
    return RequesterChild(process, result, files["context"], files["entered"], files["release"], files["gate"])


def start_recover_child(
    tmp_path: Path,
    context: lifecycle.RunContext,
    account: Path,
    stages: dict,
    *,
    name: str = "recoverer",
    budget_seconds: float | None = None,
    drain_live_consumers: bool = True,
    takeover_attempt: str | None = None,
    fake_docker: Path | None = None,
    observer_log: Path | None = None,
) -> tuple[subprocess.Popen[bytes], Path]:
    """Start ``failpoint_child.py`` as one recoverer (operator or worker mode) with failpoints and a seeded budget.

    ``budget_seconds`` seeds the child's recovery deadline from its own start instant; ``fake_docker`` names the
    stateful shim directory the test composed, so the child's recovery drives the same fake Docker CLI.
    """
    result = tmp_path / f"{name}.result.json"
    spec = tmp_path / f"{name}.spec.json"
    arguments: dict = {
        "context": lifecycle._context_payload(context),
        "test_directory": str(account),
        "drain_live_consumers": drain_live_consumers,
        "takeover_attempt": takeover_attempt,
        "budget_seconds": budget_seconds,
        "fake_docker": None if fake_docker is None else str(fake_docker),
    }
    document = {"entry": "recover-run", "arguments": arguments, "stages": stages, "result": str(result)}
    if observer_log is not None:
        document["observer_log"] = str(observer_log)
    spec.write_text(json.dumps(document))
    return subprocess.Popen([sys.executable, "-P", str(FAILPOINT_CHILD), str(spec)]), result


def start_takeover_worker_child(
    tmp_path: Path, context: lifecycle.RunContext, account: Path, attempt: str, stages: dict, *, name: str = "worker"
) -> subprocess.Popen[bytes]:
    """Start ``failpoint_child.py`` as one takeover worker (the ``_recover`` child command) with failpoints."""
    spec = tmp_path / f"{name}.spec.json"
    arguments = {"context": lifecycle._context_payload(context), "attempt": attempt, "test_directory": str(account)}
    document = {
        "entry": "takeover-worker",
        "arguments": arguments,
        "stages": stages,
        "result": str(tmp_path / f"{name}.result.json"),
    }
    spec.write_text(json.dumps(document))
    return subprocess.Popen([sys.executable, "-P", str(FAILPOINT_CHILD), str(spec)])


def child_result(child: subprocess.Popen[bytes], result: Path, *, cap: float) -> dict:
    """Wait at most ``cap`` seconds for a failpoint child's result file; kill the child and fail on expiry."""
    deadline = time.monotonic() + cap
    while not result.exists():
        if time.monotonic() > deadline:
            reap(child)
            raise AssertionError(f"the child produced no result within {cap} s (it blocked)")
        time.sleep(0.05)
    child.wait(timeout=10)
    return json.loads(result.read_text())


def read_observer_log(path: Path) -> list[dict]:
    """Read a failpoint child's ordered observer log (lock events and stages) as dicts."""
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def dead_identity(offset: int = 0) -> dict:
    """Return a process identity (this host's clock origin and birth shape) whose pid cannot be alive.

    A different ``offset`` gives a different dead identity, for records that must name two dead actors.
    """
    identity = lifecycle.current_process(lifecycle.NativeClock())
    value = {
        "origin": identity.origin,
        "pid": 2_147_483_647 - offset,
        "birth": identity.birth,
        "audit_token": identity.audit_token,
    }
    if value["audit_token"] is not None:
        value["audit_token"] = list(value["audit_token"])
        value["audit_token"][5] = value["pid"]
    return value


def distinct_process_identity(offset: int) -> lifecycle.ProcessIdentity:
    """Return a native identity that differs from this process only by pid (and the audit token's pid slot)."""
    current = lifecycle.current_process(lifecycle.NativeClock())
    pid = current.pid + offset
    token = None if current.audit_token is None else [*current.audit_token]
    if token is not None:
        token[5] = pid
    return lifecycle.ProcessIdentity(current.origin, pid, current.birth, token)


class Interrupted(Exception):
    """Stand-in for a process kill at a failpoint stage (raised by the installed failpoint callback)."""


def reap_orphan(pid: int, *, timeout: float = 10.0) -> None:
    """Reap a process this test killed once it was reparented to this process (see ``tests/conftest.py``).

    Where the process is not our child (macOS: ``launchd`` reaped it) there is nothing to do.
    """
    deadline = time.monotonic() + timeout
    while True:
        try:
            reaped, _ = os.waitpid(pid, os.WNOHANG)
        except ChildProcessError:
            return
        if reaped:
            return
        assert time.monotonic() < deadline, f"process {pid} did not exit"
        time.sleep(0.02)


@contextlib.contextmanager
def reaping_group_orphans() -> Iterator[None]:
    """On Linux, reap zombies of foreign process groups that were reparented to this (subreaper) process.

    A gate whose leader exits leaves descendants that the cleanup kills; they stay zombies, which still count as group
    members, until their new parent waits for them. macOS has no ``/proc`` and its ``launchd`` reaps them already.
    Children that lead their own group or share this process's group are never touched, so ``Popen`` objects keep
    their exit statuses.
    """
    stop = threading.Event()

    def reap() -> None:
        me, mine = os.getpid(), os.getpgrp()
        while not stop.wait(0.005):
            for entry in os.scandir("/proc"):
                if not entry.name.isdigit():
                    continue
                try:
                    fields = Path(entry.path, "stat").read_text().rsplit(") ", 1)[1].split()
                except (OSError, IndexError):
                    continue
                pid = int(entry.name)
                if fields[0] == "Z" and int(fields[1]) == me and int(fields[2]) not in (pid, mine):
                    with contextlib.suppress(ChildProcessError):
                        os.waitpid(pid, os.WNOHANG)

    thread = threading.Thread(target=reap, daemon=True) if sys.platform.startswith("linux") else None
    if thread is not None:
        thread.start()
    try:
        yield
    finally:
        stop.set()
        if thread is not None:
            thread.join(5)


def deny_access(directory: Path, *, loop: bool = False) -> Callable[[], None]:
    """Make a directory unusable for this account and return the function that undoes it.

    Mode 0 stops every account but root, which reads any mode. For root the directory is moved aside and replaced by
    a regular file (listing it fails with ``NotADirectoryError``) or, with ``loop``, by a symlink to itself (every
    lookup through it fails with ``ELOOP``, an error that is neither "missing" nor "not a directory").
    """
    if os.getuid() != 0:
        mode = stat.S_IMODE(directory.stat().st_mode)
        directory.chmod(0)
        return lambda: directory.chmod(mode)
    moved = directory.with_name(directory.name + ".denied")
    directory.rename(moved)
    if loop:
        directory.symlink_to(directory.name)
    else:
        directory.write_text("")

    def restore() -> None:
        directory.unlink()
        moved.rename(directory)

    return restore


def wait_guardians_exited(state_directory: Path, *, timeout: float = 30.0) -> None:
    """Wait until no recorded guardian is executing any more.

    ``leave`` returns while its guardian is still finishing: it holds the shared metadata lock, de-indexes the run and
    writes the project record for a moment longer. A test that inspects the account right after ``leave`` must first
    wait for the guardian to exit, and on a slow runner that window is wide.
    """
    deadline = time.monotonic() + timeout
    while True:
        executing = False
        for path in (state_directory / "runs").glob("*.json"):
            guardian = json.loads(path.read_text()).get("guardian")
            if not isinstance(guardian, dict):
                continue
            try:
                executing = (
                    executing or lifecycle.observe_process(lifecycle._identity_from_json(guardian)) == "executing"
                )
            except ResourceError:
                continue  # an unreadable identity is not a running guardian
        if not executing:
            return
        assert time.monotonic() < deadline, "a guardian was still running after the wait"
        time.sleep(0.05)


def open_file_names(pid: int) -> list[str]:
    """List the file names a process holds open (``/proc`` on Linux, which images may lack ``lsof`` for)."""
    if sys.platform.startswith("linux"):
        names = []
        for entry in os.scandir(f"/proc/{pid}/fd"):
            with contextlib.suppress(OSError):
                names.append(os.readlink(entry.path))
        return names
    listing = subprocess.run(["lsof", "-a", "-p", str(pid), "-Fn"], capture_output=True, text=True).stdout
    return [line[1:] for line in listing.splitlines() if line.startswith("n")]


def kill_guardian(context: lifecycle.RunContext) -> None:
    """SIGKILL the run's recorded guardian and wait until it is gone and its capacity lock is released.

    macOS reports a killed process as absent before the kernel has closed its descriptors, so the guardian's capacity
    flock can outlive the identity by tens of milliseconds under load; a recovery that fences right away would see it
    held (docs/test-bloat-lessons.md, "dead is not yet unlocked").
    """
    data = json.loads(context.record.read_text())
    identity = lifecycle._identity_from_json(data["guardian"])
    os.kill(identity.pid, signal.SIGKILL)
    reap_orphan(identity.pid)
    deadline = time.monotonic() + 20
    while lifecycle.observe_process(identity) == "executing":
        assert time.monotonic() < deadline, "guardian did not end"
        time.sleep(0.05)
    capacity = Path(data["account_directory"]) / "capacity.lock" if data.get("account_directory") else None
    while capacity is not None and capacity.exists() and not try_lock(capacity):
        assert time.monotonic() < deadline, "the killed guardian's capacity lock was never released"
        time.sleep(0.01)


def start_prepare_child(
    tmp_path: Path,
    service: PrivateAccount,
    project_root: Path,
    stages: dict,
    *,
    name: str = "requester",
    parent_session: str | None = None,
) -> tuple[subprocess.Popen[bytes], Path]:
    """Start ``failpoint_child.py`` as the requester that prepares one run in ``service``'s private account.

    The child exits after ``prepare_run`` returns, so its requester identity is gone afterwards; a ``sigkill`` stage
    kills it mid-preparation instead. ``parent_session`` prepares it into that session, as ``release_run`` does.
    Returns the child and its result file.
    """
    result = tmp_path / f"{name}.result.json"
    spec = tmp_path / f"{name}.spec.json"
    spec.write_text(
        json.dumps(
            {
                "entry": "prepare-run",
                "arguments": {
                    "state": str(service.state_directory),
                    "test_directory": str(service.account_directory),
                    "project_root": str(project_root),
                    "purpose": name,
                    "parent_session": parent_session,
                },
                "stages": stages,
                "result": str(result),
            }
        )
    )
    return subprocess.Popen([sys.executable, "-P", str(FAILPOINT_CHILD), str(spec)]), result


def run_context_of(
    service: PrivateAccount, project_root: Path, existing: set[str] | None = None
) -> lifecycle.RunContext:
    """Return the context of the one run record in ``service``'s registry that is not in ``existing``."""
    runs = service.state_directory / "runs"
    names = {path.stem for path in runs.glob("*.json")} - (existing or set())
    assert len(names) == 1, names
    run_id = names.pop()
    record = json.loads((runs / f"{run_id}.json").read_text())
    return lifecycle.RunContext(
        Context(project_root.resolve(), service.state_directory),
        run_id,
        record["session_id"],
        runs / f"{run_id}.json",
        Path(record["private_root"]),
    )


def reap(child: subprocess.Popen[bytes]) -> None:
    """Kill (if still running) and reap a helper child; safe to call in ``finally``."""
    if child.poll() is None:
        child.kill()
    child.wait(timeout=10)


@dataclass(frozen=True)
class PrivateAccount:
    """A private test registry and its account directory (``registry/test-admission``)."""

    state_directory: Path
    account_directory: Path


def source_artifact() -> ArtifactIdentity:
    """Return the identity of the running source tree, for run specifications that still name one."""
    return ArtifactIdentity(TOOLKIT_VERSION, lifecycle.source_identity(), dict(SUPPORTED_PROTOCOLS))


def private_account(tmp_path: Path) -> PrivateAccount:
    """Create an empty private registry and account under ``tmp_path``."""
    state = tmp_path / "registry"
    account = state / "test-admission"
    state.mkdir(mode=0o700)
    account.mkdir(mode=0o700)
    return PrivateAccount(state.resolve(), account.resolve())


def run_spec(
    project: Path,
    artifact: ArtifactIdentity,
    purpose: str = "hot-path",
    *,
    wait_seconds: int = 20,
    retain: bool = False,
) -> RunSpec:
    """Build a minimal Docker-free run specification bound to ``artifact``."""
    return RunSpec(
        project_root=project,
        entrypoint="integration",
        purpose=purpose,
        services=(),
        closure=(),
        profile="fixture",
        phases={"tests": PhaseBudget(0, 1, 1, 1)},
        initial_phase="tests",
        artifact=artifact,
        wait_seconds=wait_seconds,
        retain=retain,
    )


class ScriptedDocker:
    """Docker fake with a mutable inventory: each removal deletes the exact object and every command is recorded."""

    engine = "docker"  # the port's engine member

    def __init__(self, **resources: list[dict]) -> None:
        self.resources = {kind: list(items) for kind, items in resources.items()}
        self.commands: list[tuple[str, ...]] = []
        self.held_fds: list[int | None] = []
        """The ``hold_fd`` each ``execute`` received (the image lock K an operator removal's child inherits)."""

    def call(self, *arguments: str) -> str:
        self.commands.append(arguments)
        if arguments == ("info", "--format", "{{.ID}}"):
            return "fixture-daemon\n"
        if arguments == ("buildx", "ls", "--format", "{{.Name}}"):
            return ""
        if arguments[:2] == ("buildx", "history") and arguments[2:3] == ("ls",):
            return "".join(json.dumps(row) + "\n" for row in self.resources.get("build_history", []))
        if arguments[:2] == ("buildx", "du"):
            return "".join(json.dumps(row) + "\n" for row in self.resources.get("build_cache", []))
        match arguments:
            case ("container", "rm", "-f", target):
                self._drop("container", lambda item: item["Id"] == target)
            case ("volume", "rm", target):
                self._drop("volume", lambda item: item["Name"] == target)
            case ("image", "rm", reference):
                self._drop("image", lambda item: reference in item.get("RepoTags", []))
            case _:
                raise AssertionError(f"unexpected Docker command: {arguments}")
        return ""

    def execute(
        self, argv: tuple[str, ...], *, timeout_seconds: float, stream_output: bool = False, hold_fd: int | None = None
    ) -> DockerResult:
        del timeout_seconds, stream_output
        self.held_fds.append(hold_fd)
        self.call(*argv)
        return DockerResult(0, "", "")

    def objects(self, kind: str) -> list[dict]:
        return [dict(item) for item in self.resources.get(kind, [])]

    def removals(self) -> list[tuple[str, ...]]:
        return [command for command in self.commands if command[1:2] == ("rm",) or command[:2] == ("container", "rm")]

    def _drop(self, kind: str, matches) -> None:
        self.resources[kind] = [item for item in self.resources.get(kind, []) if not matches(item)]


def plant_run_record(context: Context, run_id: str, body: dict) -> str:
    """Write one private run record into the registry and return its activation cell id."""
    runs = context.state / "runs"
    runs.mkdir(parents=True, mode=0o700, exist_ok=True)
    path = runs / f"{run_id}.json"
    encoded = json.dumps(body).encode()
    path.write_bytes(encoded)
    path.chmod(0o600)
    return record_cell_id("run", f"runs/{run_id}.json", hashlib.sha256(encoded).hexdigest())


def replace_identity(value: object, old: dict, new: dict) -> object:
    """Return ``value`` with every nested dict equal to the process identity ``old`` replaced by ``new``."""
    if isinstance(value, dict):
        return new if value == old else {key: replace_identity(item, old, new) for key, item in value.items()}
    if isinstance(value, list):
        return [replace_identity(item, old, new) for item in value]
    return value


def retire_actor(registry: Path, identity: dict, replacement: dict, *, account: Path | None = None) -> None:
    """Rewrite every run record (and the account queue) so a once-live actor's identity names a dead ``replacement``.

    Models "that process died" for tests that drive the actor from the test process itself.
    """
    for path in sorted((registry / "runs").glob("*.json")):
        record = json.loads(path.read_text())
        atomic_json(path, replace_identity(record, identity, replacement))
    if account is not None:
        queue = json.loads((account / "queue.json").read_text())
        write_raw_queue(account, canonical_json_bytes(replace_identity(queue, identity, replacement)))


def abandon_run(context: lifecycle.RunContext, account: Path) -> None:
    """Model a killed run: its guardian is killed and the test process, its requester, counts as dead."""
    kill_guardian(context)
    retire_actor(context.project.state, current_identity(), dead_identity(), account=account)


def start_live_actor(name: str = "actor") -> tuple[subprocess.Popen[str], dict]:
    """Start a sleeping process and return it with its own native identity (as it would record itself)."""
    program = (
        "import json, time\nfrom dataclasses import asdict\n"
        "from docker_test_toolkit.application import lifecycle\n"
        "print(json.dumps(asdict(lifecycle.current_process(lifecycle.NativeClock()))), flush=True)\ntime.sleep(120)\n"
    )
    child = subprocess.Popen([sys.executable, "-P", "-c", program, name], stdout=subprocess.PIPE, text=True)
    assert child.stdout is not None
    return child, json.loads(child.stdout.readline())


def add_active_consumer(context: lifecycle.RunContext, identity: dict) -> str:
    """Register one active consumer with ``identity`` in a run record (a valid record shape, written directly)."""
    record = json.loads(context.record.read_text())
    consumer_id = "66666666-6666-4666-8666-666666666666"
    record["consumers"][consumer_id] = {
        "id": consumer_id,
        "generation": record["generation"],
        "revision": 1,
        "services": record["closure"][:1],
        "identity": identity,
        "stop_on_root_loss": True,
        "state": "active",
        "finished_revision": None,
    }
    atomic_json(context.record, record)
    return consumer_id


def write_claim_intent(
    account: Path, run_id: str, attempt: str, *, expires_in_s: float, claimant: dict | None = None
) -> None:
    """Write a takeover claim on an entry under the metadata lock; a negative ``expires_in_s`` is already expired."""
    clock = lifecycle.NativeClock()
    now = clock.read()
    expires = now + int(expires_in_s * 1_000_000_000)
    deadline = {
        "origin": clock.origin,
        "created_ns": now,
        "duration_ns": abs(expires - now) or 1,
        "expires_ns": expires,
    }
    with lock(Context(account, account), "metadata") as anchor:
        state = admission.load_queue(account, anchor)
        state.entries[run_id].takeover = {"attempt": attempt, "deadline": deadline, "claimant": claimant}
        admission.save_queue(account, state, anchor)


def seed_dead_since(account: Path, run_id: str, *, age_s: float, origin: str | None = None) -> None:
    """Record that an entry's holder was first seen dead ``age_s`` seconds ago (on ``origin``, default the current)."""
    clock = lifecycle.NativeClock()
    with lock(Context(account, account), "metadata") as anchor:
        state = admission.load_queue(account, anchor)
        state.entries[run_id].dead_since = {
            "origin": origin or clock.origin,
            "tick_ns": clock.read() - int(age_s * 1_000_000_000),
        }
        admission.save_queue(account, state, anchor)


def processes_mentioning(token: str) -> list[str]:
    """List ``ps`` lines (full argv) of processes whose command line contains ``token``."""
    output = subprocess.run(["ps", "-axww", "-o", "pid=,command="], capture_output=True, text=True, check=True).stdout
    return [line for line in output.splitlines() if token in line and "ps -axww" not in line]


def current_identity() -> dict:
    """Return this process's own native identity as a record would store it."""
    return asdict(lifecycle.current_process(lifecycle.NativeClock()))


def authority_bytes(context: lifecycle.RunContext, account: Path) -> tuple[bytes, bytes]:
    """Return the run record and queue bytes (for byte-identity assertions around a refusal)."""
    return context.record.read_bytes(), (account / "queue.json").read_bytes()


def seed_terminal_run_record(tmp_path: Path) -> tuple[Path, str, dict]:
    """Drive one real run to a terminal record; return its project root, run id and record body to plant elsewhere."""
    project = tmp_path / "terminal-project"
    project.mkdir()
    services = build_test_services(tmp_path / "terminal-state")
    handle = services.open_run(run_spec(project.resolve(), source_artifact()))
    handle.cancel()
    record = json.loads((services.root / "registry" / "runs" / f"{handle.run_id}.json").read_text())
    return project.resolve(), handle.run_id, record


# --- Foreign-resource preservation: sentinels of a stranger project and one honest snapshot of them ---

FILE_CLASSES = ("project-record", "run-record", "temporary-root")
DOCKER_CLASSES = ("container", "network", "volume", "image", "builder", "build-history", "build-cache")
PROTECTED_CLASSES = (*FILE_CLASSES, *DOCKER_CLASSES)
FOREIGN_BUILDER = "foreign-builder"
ABSENT = "absent"  # the digest of a sentinel that does not exist (a vanished sentinel, or one never created)


class DockerInventory(Protocol):
    """Read one exact Docker object (None when it is gone); the snapshot hashes what it returns."""

    def inspect(self, kind: str, identity: str) -> object: ...


@dataclass
class ForeignSentinels:
    """A foreign project F that is neither actor of a test, and the exact identities it must keep."""

    project: Context | None
    run_records: list[Path]
    temporary_root: Path | None
    docker: dict[str, list[str]]  # protected Docker class -> exact identities (empty when none were seeded)


class ScriptedInventory:
    """The fake Docker's inventory: its exact objects, and build records through the toolkit's own reader."""

    _KEYS: ClassVar[dict[str, str]] = {"container": "Id", "network": "Id", "volume": "Name", "builder": "Name"}

    def __init__(self, docker: ScriptedDocker) -> None:
        self._docker = docker

    def inspect(self, kind: str, identity: str) -> object:
        if kind in ("build-history", "build-cache"):
            records = build_records(self._docker, kind.replace("-", "_"), FOREIGN_BUILDER)
            return records.get(identity)
        if kind == "image":
            return next((item for item in self._docker.objects("image") if identity in item.get("RepoTags", [])), None)
        return next((item for item in self._docker.objects(kind) if item[self._KEYS[kind]] == identity), None)


def seed_foreign_sentinels(
    tmp_path: Path,
    services: _TestServices,
    *,
    docker: ScriptedDocker | None = None,
) -> ForeignSentinels:
    """Create foreign project F with one sentinel per protected class.

    The file classes are real toolkit state in ``services``: F's project record, a terminal F run and a retained F run
    (its record and its private temporary root). With a ``ScriptedDocker`` the Docker classes are seeded in the fake.
    """
    root = tmp_path / "foreign-project"
    root.mkdir()
    artifact = source_artifact()
    terminal = services.open_run(run_spec(root, artifact, "foreign-terminal")).enter()
    assert terminal.leave().state == "complete"
    retained = services.open_run(run_spec(root, artifact, "foreign-retained", retain=True)).enter()
    assert retained.retain().state == "stopped-retained"
    wait_guardians_exited(services.root / "registry")  # a late de-index would change F's project record digest
    sentinels = ForeignSentinels(
        project=terminal._context.project,
        run_records=[terminal._context.record, retained._context.record],
        temporary_root=retained.private_root,
        docker={name: [] for name in DOCKER_CLASSES},
    )
    if docker is not None:
        labels = {"toolkit.project": "foreign", "toolkit.run": "foreign"}
        seeds: dict[str, tuple[list[dict], str]] = {
            "container": ([{"Id": "f" * 64, "Name": "/foreign-container", "Labels": labels}], "Id"),
            "network": ([{"Id": "e" * 64, "Name": "foreign-network", "Labels": labels}], "Id"),
            "volume": ([{"Name": "foreign-volume", "CreatedAt": "2026-01-01T00:00:00Z", "Labels": labels}], "Name"),
            "image": ([{"Id": "sha256:" + "d" * 64, "RepoTags": ["foreign/image:1"], "Labels": labels}], "RepoTags"),
            "builder": ([{"Name": FOREIGN_BUILDER, "Driver": "docker-container"}], "Name"),
            "build_history": ([{"ref": f"{FOREIGN_BUILDER}/{FOREIGN_BUILDER}/build1", "Labels": labels}], "ref"),
            "build_cache": ([{"ID": "cache-1", "Size": 1024, "Labels": labels}], "ID"),
        }
        for kind, (objects, key) in seeds.items():
            name = kind.replace("_", "-")
            docker.resources.setdefault(kind, []).extend(objects)
            identity = objects[0][key]
            sentinels.docker[name].append(identity[0] if isinstance(identity, list) else identity.rsplit("/", 1)[-1])
    return sentinels


def _digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def _tree_digest(root: Path) -> str:
    """One digest of every file under ``root`` (relative path and bytes); a missing root is its own value."""
    if not root.exists():
        return ABSENT
    return _digest(
        {str(path.relative_to(root)): path.read_bytes().hex() for path in sorted(root.rglob("*")) if path.is_file()}
    )


def foreign_snapshot(sentinels: ForeignSentinels, docker: DockerInventory | None = None) -> dict[str, dict[str, str]]:
    """Digest every protected object of F by its exact identity; an empty protected class is refused.

    Without ``docker`` only the file classes are in scope. A class with no sentinel raises
    ``AssertionError("empty protected class: <name>")``, so an equality of two snapshots can never hold vacuously. A
    sentinel that disappears is not "empty": its digest becomes ``ABSENT`` and the snapshots differ. A baseline whose
    sentinel never existed digests ``ABSENT`` as well, so pass the first snapshot through ``require_present``.
    """

    def file_digest(path: Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else ABSENT

    def docker_digest(kind: str, identity: str) -> str:
        assert docker is not None
        observed = docker.inspect(kind, identity)
        return ABSENT if observed is None else _digest(observed)

    snapshot: dict[str, dict[str, str]] = {
        "project-record": {}
        if sentinels.project is None
        else {str(sentinels.project.record): file_digest(sentinels.project.record)},
        "run-record": {str(path): file_digest(path) for path in sentinels.run_records},
        "temporary-root": {}
        if sentinels.temporary_root is None
        else {str(sentinels.temporary_root): _tree_digest(sentinels.temporary_root)},
    }
    scope = FILE_CLASSES
    if docker is not None:
        scope = PROTECTED_CLASSES
        for kind in DOCKER_CLASSES:
            snapshot[kind] = {identity: docker_digest(kind, identity) for identity in sentinels.docker[kind]}
    for name in scope:
        if not snapshot[name]:
            raise AssertionError(f"empty protected class: {name}")
    return {name: snapshot[name] for name in scope}


def require_present(baseline: dict[str, dict[str, str]]) -> None:
    """Refuse a baseline snapshot in which a sentinel is already absent: its later equality would prove nothing."""
    for name, digests in baseline.items():
        for identity, digest in digests.items():
            if digest == ABSENT:
                raise AssertionError(f"absent sentinel at baseline: {name} {identity}")


# --- Reads observed without patching anything: an audit hook that only reports while a block asks it to --------------

_OBSERVERS: list[tuple[dict[str, str], dict[str, str], list[Path]]] = []  # real path -> path, name -> path, opened


def _report_open(event: str, arguments: tuple[object, ...]) -> None:
    if not _OBSERVERS or event != "open" or not isinstance(arguments[0], str | bytes | os.PathLike):
        return
    named = os.fsdecode(arguments[0])
    resolved = os.path.realpath(named)
    for by_path, by_name, opened in _OBSERVERS:
        if resolved in by_path:
            opened.append(Path(by_path[resolved]))
        elif not os.path.isabs(named) and os.path.basename(named) in by_name:
            # A descriptor-relative open reports only the name it was given, so the directory is unknown: the watched
            # names must be unique (run records are named by their UUID) and a match counts conservatively.
            opened.append(Path(by_name[os.path.basename(named)]))


sys.addaudithook(_report_open)  # audit hooks cannot be removed; this one is inert unless ``observed_opens`` is active


@contextmanager
def observed_opens(paths: Iterable[Path]) -> Iterator[list[Path]]:
    """Collect the watched paths this process opens while the block runs (``open``, ``Path.read_*``, ``os.open``).

    Byte-identity of a file cannot show that nothing read it; this can. Symbolic links resolve to their target and a
    descriptor-relative open matches by the file's name. Reads by other processes are not seen.
    """
    watched = [Path(path) for path in paths]
    entry = (
        {os.path.realpath(path): str(path) for path in watched},
        {path.name: str(path) for path in watched},
        [],
    )
    _OBSERVERS.append(entry)
    try:
        yield entry[2]
    finally:
        _OBSERVERS.remove(entry)
