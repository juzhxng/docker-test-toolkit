"""Helper process for the image-lock proofs: take K, run one Docker child that inherits it, then unwind or wait.

Usage: image_lock_holder.py <fake docker binary> <account directory> <barrier directory> <captured|streaming>
SIGUSR1 raises inside the holder while the barrier child runs; the holder records ``unwound`` and exits normally.
"""

from __future__ import annotations

import os
import signal
import sys
import time
from pathlib import Path

from docker_test_toolkit.adapters.outbound.docker import DockerCliAdapter
from docker_test_toolkit.application.recovery import Context, image_lock


def _injected(_signum: int, _frame: object) -> None:
    raise RuntimeError("injected")


def main() -> None:
    binary, account, barrier, mode = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3]), sys.argv[4]
    adapter = DockerCliAdapter.discover(binary, environment={"PATH": os.environ["PATH"]})
    signal.signal(signal.SIGUSR1, _injected)
    try:
        with image_lock(Context(account, account)) as descriptor:
            (barrier / "holding").write_text(str(os.getpid()))
            adapter.execute(
                ("barrier",),
                timeout_seconds=60,
                environment={"DTT_BARRIER": str(barrier)},
                stream_output=mode == "streaming",
                hold_fd=descriptor,
            )
    except RuntimeError:
        (barrier / "unwound").write_text("")  # the holder's `with` has exited; its descriptor is closed
        time.sleep(30)  # stay alive, so only the child can still be holding K


if __name__ == "__main__":
    main()
