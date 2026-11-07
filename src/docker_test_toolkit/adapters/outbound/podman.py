"""Read the account's Podman machines through the ``podman`` CLI."""

from __future__ import annotations

import functools
import hashlib
import json
import math
import os
import pwd
import shutil
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any, Mapping

from docker_test_toolkit.adapters.outbound.docker import DockerCliAdapter
from docker_test_toolkit.application.ports.podman import PodmanMachinePort
from docker_test_toolkit.domain.errors import InfrastructureFailure
from docker_test_toolkit.domain.models import MachineFacts
from docker_test_toolkit.domain.policy import PODMAN_BINARY_TEXT

if TYPE_CHECKING:
    from docker_test_toolkit.application.lifecycle import Deadline

PODMAN_STDERR_LIMIT = 256  # NOTE: characters of a failed call's stderr kept in its message
_OUTPUT_LIMIT = 1024**2
_DEFAULT_SECONDS = 30
_IMAGE_DIRS: dict[str, Path] = {}  # binary path -> Host.MachineImageDir, read once per process


def darwin_user_temp(environment: Mapping[str, str]) -> str:
    """Return the account's Darwin user temporary folder (``getconf DARWIN_USER_TEMP_DIR``) verbatim, once per process.

    ``podman`` derives a machine's API socket path from ``TMPDIR``, and the user's shell starts machines with this
    folder; the toolkit's private ``TMPDIR`` would report another path. On another platform ``TMPDIR`` passes through.
    """
    if sys.platform != "darwin":
        return environment.get("TMPDIR", "/tmp")
    return _darwin_temp()


@functools.cache
def _darwin_temp() -> str:
    """Read ``getconf DARWIN_USER_TEMP_DIR`` once per process; a failed read is not cached."""
    try:
        folder = subprocess.run(
            ["/usr/bin/getconf", "DARWIN_USER_TEMP_DIR"], capture_output=True, text=True, timeout=10, check=True
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError) as error:
        raise InfrastructureFailure("The Darwin user temporary folder is unreadable.", reason="client-exit") from error
    if not folder.startswith("/"):
        raise InfrastructureFailure("The Darwin user temporary folder is malformed.")
    return folder


class PodmanCliAdapter(PodmanMachinePort):
    """Run bounded, read-only ``podman machine`` commands with the user's temporary folder and home.

    The binary is resolved on the given ``PATH`` at the first call and its SHA-256 pinned; every later call re-reads
    it, so a replaced binary is ``daemon-identity-changed`` (the identity source changed).
    """

    def __init__(self, environment: Mapping[str, str]) -> None:
        self._environment = dict(environment)
        self._binary: Path | None = None
        self._digest: str | None = None

    def running(self, deadline: Deadline | None) -> tuple[str, ...]:
        """Return the names of the running machines (see the port)."""
        listed = self._json(("machine", "list", "--format", "json"), deadline)
        if not isinstance(listed, list) or any(
            not isinstance(item, dict) or not isinstance(item.get("Name"), str) or type(item.get("Running")) is not bool
            for item in listed
        ):
            raise InfrastructureFailure("podman machine list returned an unexpected shape.")
        return tuple(item["Name"] for item in listed if item["Running"])

    def inspect(self, name: str, deadline: Deadline | None) -> MachineFacts:
        """Return the facts of one running machine (see the port)."""
        inspected = self._json(("machine", "inspect", name), deadline)
        item = inspected[0] if isinstance(inspected, list) and len(inspected) == 1 else None
        if not isinstance(item, dict) or item.get("Name") != name:
            raise InfrastructureFailure(f"podman machine inspect {name} returned an unexpected shape.")
        try:
            created, rootful, state = item["Created"], item["Rootful"], item["State"]
            socket = item["ConnectionInfo"]["PodmanSocket"]["Path"]
            memory, cpus = item["Resources"]["Memory"], item["Resources"]["CPUs"]
        except (KeyError, TypeError) as error:
            raise InfrastructureFailure(f"podman machine inspect {name} returned an unexpected shape.") from error
        if (
            not isinstance(created, str)
            or not created
            or type(rootful) is not bool
            or not isinstance(state, str)
            or not isinstance(socket, str)
            or not socket.startswith("/")
            or "\n" in socket
            or type(memory) is not int
            or memory <= 0
            or type(cpus) is not int
            or cpus <= 0
        ):
            raise InfrastructureFailure(f"podman machine inspect {name} returned an unexpected shape.")
        if state != "running":
            raise InfrastructureFailure(f"Podman machine {name} is {state}, not running.", reason="client-exit")
        return MachineFacts(name, created, rootful, socket, memory, cpus)

    def image_dir(self, deadline: Deadline | None) -> Path:
        """Return ``Host.MachineImageDir``, read once per process and binary (see the port)."""
        binary = self._pinned()
        key = str(binary)
        if key not in _IMAGE_DIRS:
            info = self._json(("machine", "info", "--format", "json"), deadline, binary)
            host = info.get("Host") if isinstance(info, dict) else None
            folder = host.get("MachineImageDir") if isinstance(host, dict) else None
            if not isinstance(folder, str) or not folder.startswith("/"):
                raise InfrastructureFailure("podman machine info returned an unexpected shape.")
            _IMAGE_DIRS[key] = Path(folder)
        return _IMAGE_DIRS[key]

    def _pinned(self) -> Path:
        if self._binary is None:
            found = shutil.which("podman", path=self._environment.get("PATH"))
            if found is None:
                raise InfrastructureFailure(PODMAN_BINARY_TEXT, reason="podman-binary-missing")
            self._binary = Path(found).resolve()
            self._digest = self._read_digest(self._binary)
        elif self._read_digest(self._binary) != self._digest:
            raise InfrastructureFailure("The podman binary changed.", reason="daemon-identity-changed")
        return self._binary

    @staticmethod
    def _read_digest(binary: Path) -> str:
        try:
            return hashlib.sha256(binary.read_bytes()).hexdigest()
        except OSError as error:
            raise InfrastructureFailure("The podman binary is unreadable.", reason="client-exit") from error

    def _json(self, argv: tuple[str, ...], deadline: Deadline | None, binary: Path | None = None) -> Any:
        binary = self._pinned() if binary is None else binary  # a caller that just pinned it passes it
        seconds = float(_DEFAULT_SECONDS) if deadline is None else deadline.remaining_seconds()
        if not math.isfinite(seconds) or seconds <= 0:
            raise InfrastructureFailure(f"podman {' '.join(argv)} ran out of time.", reason="client-exit")
        child = {
            **self._environment,
            "TMPDIR": darwin_user_temp(self._environment),
            "HOME": pwd.getpwuid(os.getuid()).pw_dir,
        }
        try:  # the Docker client's bounded call: its own process group, terminated as a whole at the timeout
            result = DockerCliAdapter.run_captured([str(binary), *argv], environment=child, timeout_seconds=seconds)
        except subprocess.TimeoutExpired as error:
            raise InfrastructureFailure(f"podman {' '.join(argv)} ran out of time.", reason="client-exit") from error
        except OSError as error:
            raise InfrastructureFailure(f"podman {' '.join(argv)} could not start.", reason="client-exit") from error
        if result.returncode:
            stderr = result.stderr.decode("utf-8", "replace").strip()[:PODMAN_STDERR_LIMIT]
            raise InfrastructureFailure(
                f"podman {' '.join(argv)} exited {result.returncode}; podman said: {stderr}", reason="client-exit"
            )
        if len(result.stdout) > _OUTPUT_LIMIT:
            raise InfrastructureFailure(f"podman {' '.join(argv)} exceeded its output limit.")
        try:
            return json.loads(result.stdout)
        except ValueError as error:
            raise InfrastructureFailure(f"podman {' '.join(argv)} returned malformed JSON.") from error
