"""Build a real process group that no signal can clear until a controlled moment.

The leader forks a helper P that leaves the group and keeps one *dead* member in it: a child C
that joined the leader's group and exited, whose reaper (P) is outside the group. P reports
readiness only after C's end of a pipe closed, so C is already dead when a test looks. On macOS
``killpg`` then reports ``EPERM`` (no live member can be signalled) until P exits; on Linux the
dead member is reported as present. The tests assert outcomes that hold on both kernels, so
the group works as a failpoint without any monkeypatching.

``LEADER`` takes two arguments: how P waits (``stdin``: until its stdin closes, or a number of
seconds) and whether the leader holds (``hold``) or exits at once (``exit``).
"""

from __future__ import annotations

import contextlib
import os
import select
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from dataclasses import dataclass

LEADER = r"""
import os, sys, time
leader = os.getpid()
joined_read, joined_write = os.pipe()
helper = os.fork()
if helper == 0:
    os.setpgid(0, 0)
    member = os.fork()
    if member == 0:
        os.setpgid(0, leader)
        os.close(joined_read)
        os._exit(0)
    os.close(joined_write)
    os.read(joined_read, 1)
    time.sleep(0.05)
    sys.stdout.write(f"ready {os.getpid()} {member}\n")
    sys.stdout.flush()
    if sys.argv[1] == "stdin":
        sys.stdin.read()
    else:
        time.sleep(float(sys.argv[1]))
    os._exit(0)
os.close(joined_write)
os.close(joined_read)
if sys.argv[2] == "hold":
    time.sleep(60)
os._exit(0)
"""


def group_state(pgid: int) -> str:
    """Return ``gone`` (ESRCH), ``unsignalable`` (EPERM) or ``present`` for a process group."""
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return "gone"
    except PermissionError:
        return "unsignalable"
    return "present"


def _reap_nohang(pid: int) -> bool:
    """Reap ``pid`` if it already exited and is currently our child; ``False`` while there is nothing to reap yet."""
    try:
        return os.waitpid(pid, os.WNOHANG)[0] == pid
    except ChildProcessError:
        return False  # not (yet) our child: the dead member becomes ours when its parent exits


def _reap_until_gone(pids: tuple[int, ...], stop: threading.Event) -> None:
    """Reap the helper and its dead member as they exit, until both are reaped or the group is released.

    The helper's parent exits at once, so on Linux both end up reparented to this process (a child subreaper, see
    ``tests/conftest.py``) and stay zombies, which still count as group members, until somebody waits for them. On
    macOS ``launchd`` reaps them, ``waitpid`` never succeeds and this thread idles until ``stop``.
    """
    pending = set(pids)
    while pending and not stop.is_set():
        pending = {pid for pid in pending if not _reap_nohang(pid)}
        time.sleep(0.005)


@dataclass
class UnsignalableGroup:
    process: subprocess.Popen[bytes]
    helper_pid: int

    @property
    def pgid(self) -> int:
        return self.process.pid

    def release(self) -> None:
        """Let the helper exit so the dead member is reaped and the group disappears."""
        if self.process.stdin is not None and not self.process.stdin.closed:
            self.process.stdin.close()

    def wait_gone(self, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if group_state(self.pgid) == "gone":
                return True
            time.sleep(0.01)
        return group_state(self.pgid) == "gone"


@contextlib.contextmanager
def unsignalable_group() -> Iterator[UnsignalableGroup]:
    """Yield a group held by a helper until ``release``; always reap everything."""
    process = subprocess.Popen(
        [sys.executable, "-c", LEADER, "stdin", "exit"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        start_new_session=True,
    )
    assert process.stdout is not None
    group: UnsignalableGroup | None = None
    stop = threading.Event()
    reaper: threading.Thread | None = None
    try:
        readable, _, _ = select.select([process.stdout], [], [], 10)
        assert readable, "the group helper never reported readiness"
        ready = process.stdout.readline().split()
        assert ready[:1] == [b"ready"], ready
        assert int(ready[1]) > 1, ready
        group = UnsignalableGroup(process, int(ready[1]))
        reaper = threading.Thread(target=_reap_until_gone, args=((int(ready[1]), int(ready[2])), stop), daemon=True)
        reaper.start()
        yield group
    finally:
        if process.stdin is not None and not process.stdin.closed:
            process.stdin.close()
        if group is not None:
            with contextlib.suppress(ProcessLookupError):
                os.kill(group.helper_pid, signal.SIGKILL)
        with contextlib.suppress(OSError):
            os.killpg(process.pid, signal.SIGKILL)
        process.wait()
        process.stdout.close()
        deadline = time.monotonic() + 10
        try:
            while group_state(process.pid) != "gone":
                assert time.monotonic() < deadline, "the group was not cleared"
                time.sleep(0.01)
        finally:
            stop.set()
            if reaper is not None:
                reaper.join(5)
