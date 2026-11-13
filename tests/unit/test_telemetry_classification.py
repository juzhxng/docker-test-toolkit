"""Telemetry classification: raise-site attribution and the pure fault mapping."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from docker_test_toolkit.domain.telemetry import CATEGORIES, OBSERVERS, TelemetryFault, classify_fault
from docker_test_toolkit.factories.testing import install_failpoint


@pytest.fixture(autouse=True)
def _clear_failpoint() -> Iterator[None]:
    yield
    install_failpoint(None)


@pytest.mark.parametrize(
    ("reason", "observer", "category", "detail"),
    (
        ("observation-deadline", "engine", "observation-late", None),
        ("lock-deadline", "host", "observation-late", None),
        ("daemon-identity-changed", "engine", "daemon-identity-changed", None),
        ("telemetry-unreadable", "host", "telemetry-unreadable", None),
        ("coholder-records-unstable", "engine", "observation-late", None),  # handled like a late read
        ("client-exit", "engine", "observation-late", None),  # a read-only client exit: the next pass reads again
        ("unspecified", "host", "telemetry-unreadable", "unclassified"),
        ("record-invalid", "process", "telemetry-unreadable", "unclassified"),
    ),
)
def test_classify_fault_is_total_and_fails_closed(
    reason: str, observer: str, category: str, detail: str | None
) -> None:
    fault = classify_fault(reason, observer=observer)

    assert fault == TelemetryFault(category, observer, detail)


def test_classify_fault_rejects_an_unknown_observer() -> None:
    with pytest.raises(ValueError, match="observer"):
        classify_fault("observation-deadline", observer="gpu")


def test_closed_value_sets_match_the_design() -> None:
    assert CATEGORIES == (
        "breach",
        "daemon-identity-changed",
        "observation-late",
        "telemetry-unreadable",
        "admission-expired",
        "admission-refused",
    )
    assert OBSERVERS == ("host", "engine", "container", "process", "admission")
