"""`run recover` returns the retention pass it triggered."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from docker_test_toolkit.application import operator
from docker_test_toolkit.application.retention import RetentionReport


@dataclass
class _Recovered:
    state: str


def test_run_recover_includes_the_retention_report(monkeypatch: pytest.MonkeyPatch) -> None:
    report = RetentionReport("recovery", "origin", 1, {"archives": {"removed_count": 2}}, {})
    monkeypatch.setattr(operator, "_one_shot_context", lambda payload: object())
    monkeypatch.setattr(operator.lifecycle, "recover_run", lambda context: _Recovered("complete"))
    monkeypatch.setattr(operator.lifecycle, "retention_after_release", lambda context, directory, trigger: report)

    result = operator.execute("run.recover", {}, docker_factory=lambda *_: None)

    assert result["state"] == "complete"
    assert result["retention"]["trigger"] == "recovery"
    assert result["retention"]["kinds"]["archives"]["removed_count"] == 2


def test_run_recover_reports_no_retention_when_no_pass_ran(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(operator, "_one_shot_context", lambda payload: object())
    monkeypatch.setattr(operator.lifecycle, "recover_run", lambda context: _Recovered("complete"))
    monkeypatch.setattr(operator.lifecycle, "retention_after_release", lambda context, directory, trigger: None)

    assert operator.execute("run.recover", {}, docker_factory=lambda *_: None)["retention"] is None
