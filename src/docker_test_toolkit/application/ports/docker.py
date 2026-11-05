"""The single Docker infrastructure port."""

from __future__ import annotations

from typing import TYPE_CHECKING, Mapping, Protocol

from docker_test_toolkit.domain.models import (
    CleanupMutation,
    ComposeMutation,
    ContainerMutation,
    DaemonIdentity,
    DockerObservation,
    DockerReadinessObservation,
    DockerResult,
    ExactInspection,
    ExactInspectionOutcome,
    PodmanBinding,
    ProbeRun,
)

if TYPE_CHECKING:
    from docker_test_toolkit.application.lifecycle import Deadline


class DockerPort(Protocol):
    """Observe one daemon and perform only journal-authorized exact operations."""

    env: dict[str, str]

    @property
    def engine(self) -> str:
        """``docker`` or ``podman``, decided by discovery's version read (design PD1)."""
        ...

    @property
    def binding(self) -> PodmanBinding | None:
        """The bound Podman machine on a Podman engine, held in memory and never written; ``None`` on Docker."""
        ...

    def observe_readiness(self, deadline: Deadline) -> DockerReadinessObservation:
        """Return the fixed client, daemon, Compose, and Buildx readiness observations."""
        ...

    def observe_daemon(self, deadline: Deadline | None = None) -> DaemonIdentity:
        """Verify and return the exact daemon and client identity selected at composition.

        Raises:
            InfrastructureFailure: With reason ``daemon-identity-changed`` when the client or daemon changed.
        """
        ...

    def endpoint(self, deadline: Deadline | None = None) -> str:
        """Return the client's endpoint (``DOCKER_HOST``, else the bound context's), resolved in memory per call."""
        ...

    def run_probe(self, argv: tuple[str, ...], *, deadline: object, container_name: str) -> ProbeRun:
        """Verify the client and daemon, run ``docker <argv>`` and, at a timeout, remove ``container_name`` once.

        One absolute ``deadline`` bounds all three steps (design 2.3): no identity read is retried and no step starts
        after it expired.

        Raises:
            InfrastructureFailure: With reason ``observation-deadline`` when the deadline expires before the run
                starts, or ``daemon-identity-changed`` when the bound client or daemon changed.
        """
        ...

    def inspect_exact(self, request: ExactInspection, *, deadline: object | None = None) -> DockerObservation:
        """Inspect one exact resource identity without ambient enumeration.

        Args:
            request: The exact resource identity to inspect.
            deadline: The observation pass's absolute deadline; ``None`` keeps the fixed 30 second bound.

        Raises:
            InfrastructureFailure: With reason ``observation-deadline`` when the deadline expires.
        """
        ...

    def inspect_exact_many(self, requests: tuple[ExactInspection, ...]) -> tuple[ExactInspectionOutcome, ...]:
        """Verify once, inspect each identity without per-call verification, verify again.

        Returns one outcome per request in request order. A per-identity inspection failure becomes that outcome's
        ``failure``; a daemon or client identity change between the two verifications raises ``InfrastructureFailure``
        and discards every outcome. Absence that finishes a cleanup must still come from a per-call ``inspect_exact``.
        """
        ...

    def compose(self, request: ComposeMutation) -> DockerResult:
        """Perform one journal-authorized bounded Compose mutation."""
        ...

    def control_container(self, request: ContainerMutation) -> DockerResult:
        """Start or stop one exact owned container generation."""
        ...

    def cleanup_exact(self, request: CleanupMutation) -> DockerResult:
        """Remove one exact journal-owned resource."""
        ...

    def execute(
        self,
        argv: tuple[str, ...],
        *,
        timeout_seconds: float,
        environment: Mapping[str, str] | None = None,
        stream_output: bool = False,
        hold_fd: int | None = None,
    ) -> DockerResult:
        """Execute bounded argv; main-thread streaming inherits I/O and forwards signals.

        ``hold_fd`` is a descriptor the Docker child inherits (the image lock), held until the child exits.
        """
        ...

    def call(self, *args: str, deadline: object | None = None) -> str:
        """Run one bounded legacy observation during extraction compatibility."""
        ...

    def objects(self, kind: str, *, deadline: object | None = None) -> list[dict]:
        """Return the complete inspected inventory for one supported resource kind."""
        ...
