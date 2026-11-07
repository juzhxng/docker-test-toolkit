"""The version gate's pure decision: only the version ``current`` points at admits new test runs."""

from __future__ import annotations

from pathlib import Path

import pytest

from docker_test_toolkit.domain.policy import admission_gate

OLD, NEW = Path("/root/0.5.0"), Path("/root/0.5.1")


@pytest.mark.parametrize(
    ("test_composition", "release_helper", "loaded", "current", "expected"),
    [
        (False, False, NEW, NEW, None),  # the current version admits
        (False, False, OLD, NEW, "toolkit-upgraded"),  # replaced: old code refuses
        (False, True, OLD, NEW, None),  # the release helper is cleanup: exempt, checked before the mismatch
        (False, True, None, None, None),  # exempt even outside an installation
        (False, False, None, None, "toolkit-not-installed"),  # not running from an installation
        (False, False, None, NEW, "toolkit-not-installed"),
        (False, False, OLD, None, "toolkit-not-installed"),  # an installation without current
        (True, False, None, None, None),  # a test composition without a bound installation admits
        (True, False, OLD, NEW, "toolkit-upgraded"),  # a bound test installation is gated like a real one
        (True, False, NEW, NEW, None),
        (True, True, OLD, NEW, None),
    ],
)
def test_admission_gate_decision_table(
    test_composition: bool, release_helper: bool, loaded: Path | None, current: Path | None, expected: str | None
) -> None:
    decision = admission_gate(
        test_composition=test_composition, release_helper=release_helper, loaded=loaded, current=current
    )

    assert decision == expected
