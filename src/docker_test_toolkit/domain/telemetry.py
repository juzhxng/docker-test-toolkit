"""Closed telemetry categories and observers, and the pure mapping from a raised reason to a fault."""

from __future__ import annotations

from dataclasses import dataclass

CATEGORIES = (
    "breach",
    "daemon-identity-changed",
    "observation-late",
    "telemetry-unreadable",
    "admission-expired",
    "admission-refused",
)
OBSERVERS = ("host", "engine", "container", "process", "admission")  # "process": older records only (before 0.8.0)


@dataclass(frozen=True)
class TelemetryFault:
    """One classified observation failure; ``detail`` is ``None`` or ``"unclassified"``."""

    category: str
    observer: str
    detail: str | None = None


def classify_fault(reason: str, *, observer: str) -> TelemetryFault:
    """Map an exception reason to its category; every unknown reason fails closed as unreadable.

    Args:
        reason: The ``reason`` carried by the raised toolkit error.
        observer: The observation source, taken from the error and otherwise from the step being performed.

    Raises:
        ValueError: If ``observer`` is not one of ``OBSERVERS``.
    """
    if observer not in OBSERVERS:
        raise ValueError(f"Unknown observer {observer!r}.")
    # ``client-exit``: a read-only Docker client call exited non-zero (a busy shared daemon); nothing was read, so the
    # pass waits and the next one reads again, never granting on the missing read.
    if reason in ("observation-deadline", "lock-deadline", "coholder-records-unstable", "client-exit"):
        return TelemetryFault("observation-late", observer)
    if reason == "daemon-identity-changed":
        return TelemetryFault("daemon-identity-changed", observer)
    if reason == "telemetry-unreadable":
        return TelemetryFault("telemetry-unreadable", observer)
    return TelemetryFault("telemetry-unreadable", observer, "unclassified")
