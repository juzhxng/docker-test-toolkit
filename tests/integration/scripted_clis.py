"""Scripted command-line clients for adapter integration tests (the repository's pattern, see ``_fake_docker``).

The ``podman`` script answers the three read-only ``podman machine`` commands with bodies in the shape the
measurements recorded on a Podman machine; environment variables change one answer
per test.
"""

from __future__ import annotations

import json
from pathlib import Path

W0_SOCKET = "/var/folders/nd/x/T/podman/dtt-w0-api.sock"
W0_CREATED = "2026-11-04T10:43:17.200188+01:00"
IMAGE_DIR = "/Users/x/.local/share/containers/podman/machine/applehv"


def machine_list(*machines: tuple[str, bool]) -> str:
    """Return a ``podman machine list --format json`` body for ``(name, running)`` pairs."""
    return json.dumps(
        [
            {"Name": name, "Created": W0_CREATED, "Running": running, "Memory": "3221225472", "CPUs": 2}
            for name, running in machines
        ]
    )


def machine_inspect(
    name: str = "dtt-w0",
    *,
    created: str = W0_CREATED,
    rootful: bool = False,
    state: str = "running",
    socket: str = W0_SOCKET,
    memory: int = 3072,
    cpus: int = 2,
) -> str:
    """Return a ``podman machine inspect <name>`` body."""
    return json.dumps(
        [
            {
                "ConfigDir": {"Path": "/Users/x/.config/containers/podman/machine/applehv"},
                "ConnectionInfo": {"PodmanSocket": {"Path": socket}, "PodmanPipe": None},
                "Created": created,
                "LastUp": "0001-01-01T00:00:00Z",
                "Name": name,
                "Resources": {"CPUs": cpus, "DiskSize": 20, "Memory": memory, "USBs": []},
                "Rootful": rootful,
                "State": state,
            }
        ]
    )


MACHINE_INFO = json.dumps({"Host": {"Arch": "arm64", "MachineImageDir": IMAGE_DIR, "VMType": "applehv"}})


def fake_podman(directory: Path) -> Path:
    """Write a scripted ``podman`` into ``directory`` and return its path.

    Every call appends ``TMPDIR|HOME|argv`` to ``$DTT_PODMAN_LOG`` when set. ``DTT_PODMAN_FAIL=1`` fails with ``boom``,
    and so does every call while the file ``DTT_PODMAN_FAIL_FILE`` exists; ``DTT_PODMAN_SLEEP=1`` sleeps 5 s in a child
    whose PID goes to the file ``DTT_PODMAN_CHILD`` when set.
    ``DTT_MACHINE_LIST``, ``DTT_MACHINE_INSPECT`` (or ``DTT_MACHINE_INSPECT_<name>`` with ``-`` as ``_``) and
    ``DTT_MACHINE_INFO`` name files that replace the default bodies. Anything else exits 9.
    """
    bodies = directory / "podman-bodies"
    bodies.mkdir(exist_ok=True)
    (bodies / "list.json").write_text(machine_list(("dtt-w0", True)))
    (bodies / "inspect.json").write_text(machine_inspect())
    (bodies / "info.json").write_text(MACHINE_INFO)
    podman = directory / "podman"
    podman.write_text(
        f"""#!/bin/sh
set -eu
bodies={bodies}
[ -z "${{DTT_PODMAN_LOG:-}}" ] || printf '%s|%s|%s\\n' "${{TMPDIR:-}}" "${{HOME:-}}" "$*" >> "$DTT_PODMAN_LOG"
[ "${{DTT_PODMAN_FAIL:-0}}" != 1 ] && [ ! -e "${{DTT_PODMAN_FAIL_FILE:-/nonexistent}}" ] || {{
  printf 'boom %0300d\\n' 0 >&2; exit 1; }}
[ "${{DTT_PODMAN_SLEEP:-0}}" != 1 ] || {{ sleep 5 & printf '%s' "$!" > "${{DTT_PODMAN_CHILD:-/dev/null}}"; wait; }}
case "$*" in
  "machine list --format json") cat "${{DTT_MACHINE_LIST:-$bodies/list.json}}" ;;
  "machine inspect "*)
    key="DTT_MACHINE_INSPECT_$(printf '%s' "$3" | tr '-' '_')"
    eval "specific=\\${{$key:-}}"
    cat "${{specific:-${{DTT_MACHINE_INSPECT:-$bodies/inspect.json}}}}" ;;
  "machine info --format json") cat "${{DTT_MACHINE_INFO:-$bodies/info.json}}" ;;
  *) exit 9 ;;
esac
"""
    )
    podman.chmod(0o700)
    return podman
