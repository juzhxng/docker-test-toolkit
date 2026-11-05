"""The host-side Podman machine port (design 2026-11-03-podman-runtime, PD3)."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Callable, Mapping, Protocol

from docker_test_toolkit.domain.models import MachineFacts

if TYPE_CHECKING:
    from docker_test_toolkit.application.lifecycle import Deadline


class PodmanMachinePort(Protocol):
    """Read the account's Podman machines on the host; never a VM round trip and never a mutation."""

    def running(self, deadline: Deadline | None) -> tuple[str, ...]:
        """Return the names of the machines ``podman machine list`` reports running.

        Raises:
            InfrastructureFailure: Reason ``client-exit`` when the call fails or runs out of time,
                ``podman-binary-missing`` when no ``podman`` is on ``PATH``, ``daemon-identity-changed`` when the
                pinned binary changed, ``unspecified`` for any other output shape.
        """
        ...

    def inspect(self, name: str, deadline: Deadline | None) -> MachineFacts:
        """Return the facts of one running machine.

        Raises:
            InfrastructureFailure: As ``running``; also ``client-exit`` when the machine is not running.
        """
        ...

    def image_dir(self, deadline: Deadline | None) -> Path:
        """Return ``Host.MachineImageDir`` of ``podman machine info``, read once per process.

        Raises:
            InfrastructureFailure: As ``running``.
        """
        ...


MachineFactory = Callable[[Mapping[str, str]], PodmanMachinePort]
"""Build the port for a Docker adapter's child environment; called only once the engine is Podman."""
