"""`run recover` returns the retention pass it triggered."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from docker_test_toolkit.application import operator
from docker_test_toolkit.application.retention import RetentionReport
from docker_test_toolkit.domain.errors import InvalidInput

_EMPTY_RECORD = {"owned_resources": {kind: {} for kind in ("builders", "containers", "images", "networks", "volumes")}}


@dataclass
class _Recovered:
    state: str


def test_run_recover_includes_the_retention_report(monkeypatch: pytest.MonkeyPatch) -> None:
    report = RetentionReport("recovery", "origin", 1, {"archives": {"removed_count": 2}}, {})
    monkeypatch.setattr(operator, "_one_shot_context", lambda payload: object())
    monkeypatch.setattr(operator.lifecycle, "recover_run", lambda context, disown: _Recovered("complete"))
    monkeypatch.setattr(operator.lifecycle, "retention_after_release", lambda context, directory, trigger: report)
    monkeypatch.setattr(operator.lifecycle, "load_run_record", lambda context: _EMPTY_RECORD)

    result = operator.execute("run.recover", {}, docker_factory=lambda *_: None)

    assert result["state"] == "complete"
    assert result["retention"]["trigger"] == "recovery"
    assert result["retention"]["kinds"]["archives"]["removed_count"] == 2


def test_run_recover_reports_no_retention_when_no_pass_ran(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(operator, "_one_shot_context", lambda payload: object())
    monkeypatch.setattr(operator.lifecycle, "recover_run", lambda context, disown: _Recovered("complete"))
    monkeypatch.setattr(operator.lifecycle, "retention_after_release", lambda context, directory, trigger: None)
    monkeypatch.setattr(operator.lifecycle, "load_run_record", lambda context: _EMPTY_RECORD)

    assert operator.execute("run.recover", {}, docker_factory=lambda *_: None)["retention"] is None


def _recover(monkeypatch: pytest.MonkeyPatch, payload: dict, record: dict = _EMPTY_RECORD) -> tuple[dict, list]:
    calls: list = []
    monkeypatch.setattr(operator, "_one_shot_context", lambda payload: object())
    monkeypatch.setattr(
        operator.lifecycle, "recover_run", lambda context, disown: (calls.append(disown), _Recovered("complete"))[1]
    )
    monkeypatch.setattr(operator.lifecycle, "retention_after_release", lambda context, directory, trigger: None)
    monkeypatch.setattr(operator.lifecycle, "load_run_record", lambda context: record)
    return operator.execute("run.recover", payload, docker_factory=lambda *_: None), calls


def test_run_recover_passes_the_disown_request(monkeypatch: pytest.MonkeyPatch) -> None:
    disown = [{"kind": "volume", "identifier": "v", "extra": 1}, {"kind": "volume", "identifier": "v"}]

    _result, calls = _recover(monkeypatch, {"context": {}, "disown": disown})

    assert calls == [(("volume", "v"),)]


def test_run_recover_without_disown_passes_none(monkeypatch: pytest.MonkeyPatch) -> None:
    for payload in ({}, {"disown": []}):
        assert _recover(monkeypatch, payload)[1] == [()]


def test_run_recover_reports_disowned_resources(monkeypatch: pytest.MonkeyPatch) -> None:
    def volume(name: str, **evidence: object) -> dict:
        return {"kind": "volume", "identifier": name, "name": name, "state": "removed", "evidence": evidence}

    record = {
        "owned_resources": {
            **_EMPTY_RECORD["owned_resources"],
            "volumes": {"v": volume("v", disowned=True), "w": volume("w")},
        }
    }

    assert _recover(monkeypatch, {}, record)[0]["disowned"] == [{"kind": "volume", "identifier": "v", "name": "v"}]
    assert _recover(monkeypatch, {})[0]["disowned"] == []


@pytest.mark.parametrize(
    "disown",
    [
        {},
        "x",
        [{"kind": "pod", "identifier": "v"}],
        [{"kind": "volume"}],
        [{"kind": "volume", "identifier": 1}],
        [{"kind": [], "identifier": "v"}],
        ["volume"],
        [{"kind": "volume", "identifier": str(index)} for index in range(65)],
    ],
    ids=["object", "string", "kind", "missing", "type", "unhashable-kind", "entry", "too-many"],
)
def test_run_recover_refuses_a_malformed_disown_request(monkeypatch: pytest.MonkeyPatch, disown: object) -> None:
    with pytest.raises(InvalidInput, match="disown request"):
        _recover(monkeypatch, {"disown": disown})
