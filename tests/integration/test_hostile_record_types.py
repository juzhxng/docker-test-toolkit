"""Private records of valid JSON whose fields have the wrong types are refused or classified, never a crash.

The defect family: readers of untrusted account records that hash, order or index a field before checking its type
(``value in {a, b}`` with a dict, ``set(list_of_lists)``), so one hostile file would stop an inspection, an exact
status read or a project's history check for everyone.
Every JSON path of every record kind a cold inspection reads is replaced by each wrong-typed value; the readers may
refuse (a typed ``ToolkitError``) or classify the cell, nothing else.
"""

from __future__ import annotations

import copy
import json
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from docker_test_toolkit.application import admission, lifecycle
from docker_test_toolkit.application.recovery import atomic_json, build_records
from docker_test_toolkit.domain.errors import ToolkitError
from tests.integration.account_fixtures import (
    add_active_consumer,
    current_identity,
    pause_guardian,
    resume_guardian,
)
from tests.integration.test_docker_receipts import (
    _entered_resource_run,
    _interrupt_retained_release,
    _retained_owned_container,
    _withdraw_then_lose_the_requester,
)

pytestmark = pytest.mark.integration

Finalize = Callable[[Callable[[], None]], None]  # registers a teardown step (request.addfinalizer)

# Unhashable and hashable, empty and not, string, number and null: each breaks a different assumption.
WRONG: tuple[Any, ...] = ({}, [], [[]], "x", 1, True, None)
MAX_DEPTH = 5


def _paths(value: Any, prefix: tuple[Any, ...] = ()) -> Iterator[tuple[Any, ...]]:
    """Every path into a JSON document down to ``MAX_DEPTH`` (a dict key or a list index per step)."""
    if prefix:
        yield prefix
    if len(prefix) >= MAX_DEPTH:
        return
    if isinstance(value, dict):
        for key, child in value.items():
            yield from _paths(child, (*prefix, key))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from _paths(child, (*prefix, index))


def _get(document: Any, path: tuple[Any, ...]) -> Any:
    for step in path:
        document = document[step]
    return document


def _mutations(original: Any) -> Iterator[tuple[str, str, Any]]:
    """Every (path, replacement, mutated copy) that changes the value or its type, the whole document included."""
    for wrong in WRONG:
        if not isinstance(original, type(wrong)) or original != wrong:
            yield "<root>", repr(wrong), copy.deepcopy(wrong)
    for path in _paths(original):
        current = _get(original, path)
        for wrong in WRONG:
            if type(current) is type(wrong) and current == wrong:
                continue
            mutated = copy.deepcopy(original)
            _get(mutated, path[:-1])[path[-1]] = wrong
            yield ".".join(map(str, path)), repr(wrong), mutated


def _crashes(original: Any, probe: Callable[[Any], None]) -> tuple[list[tuple[str, str, str]], int]:
    """Run ``probe`` on every mutation; return the (path, value, exception) of each one that raised, and how many
        mutations the reader classified (the probe returned instead of refusing).

        A sweep in which no mutation is classified proves nothing about the reader: every mutation was refused before it
    . Each sweep asserts the count.
    """
    failures = []
    classified = 0
    for path, wrong, mutated in _mutations(original):
        try:
            probe(mutated)
        except ToolkitError:
            pass  # a typed refusal is the contract
        except Exception as error:  # anything but a refusal is the crash this suite exists to find
            failures.append((path, wrong, type(error).__name__))
        else:
            classified += 1
    return failures, classified


VACUOUS = "no mutation reached the reader and was classified: every one was refused before it, the sweep proved nothing"


def _entered(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, finalize: Finalize) -> lifecycle.RunContext:
    """A granted run with one exact owned container, a receipt journal and one registered consumer.

    The consumer carries the requester's own identity, which the run's guardian refuses to load, and the probes write
    invalid records: the guardian stays paused throughout and resumes, after the test, on the record as it was before
    the registration.
    """
    _handle, context, _account = _entered_resource_run(tmp_path, monkeypatch, retain=False)
    valid = json.loads(context.record.read_text())
    pid = pause_guardian(context)

    def restore() -> None:
        atomic_json(context.record, valid)
        resume_guardian(pid)

    finalize(restore)
    add_active_consumer(context, current_identity())
    return context


def _withdrawn(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, _finalize: Finalize) -> lifecycle.RunContext:
    """A stopping run whose requester wrote its leave withdrawal and died (leave and recovery fields set)."""
    handle, context, _account = _entered_resource_run(tmp_path, monkeypatch, retain=False)
    _withdraw_then_lose_the_requester(handle, context)
    return context


def _retained(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, _finalize: Finalize) -> lifecycle.RunContext:
    """A stopped-retained run with its retained grant and one exact owned container."""
    _handle, context, _container, _account = _retained_owned_container(tmp_path, monkeypatch)
    return context


def _release_interrupted(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, _finalize: Finalize) -> lifecycle.RunContext:
    """A retained run whose release was interrupted: release authority and a resume origin are on the record."""
    handle, context, _container, _account = _retained_owned_container(tmp_path, monkeypatch)
    _interrupt_retained_release(handle, context, monkeypatch)
    return context


RUN_SHAPES: dict[str, Callable[[Path, pytest.MonkeyPatch, Finalize], lifecycle.RunContext]] = {
    "entered": _entered,
    "withdrawn": _withdrawn,
    "retained": _retained,
    "release-interrupted": _release_interrupted,
}


@pytest.mark.parametrize("shape", list(RUN_SHAPES))
def test_a_wrong_typed_field_never_crashes_the_run_record_loader(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest, shape: str
) -> None:
    context = RUN_SHAPES[shape](tmp_path, monkeypatch, request.addfinalizer)
    original = json.loads(context.record.read_text())

    def probe(mutated: dict) -> None:
        atomic_json(context.record, mutated)
        if isinstance(mutated, dict):  # every caller hands over an object: it was parsed and checked first
            lifecycle.run_record_loads(context.record.parent.parent, context.run_id, mutated)
        lifecycle.load_run_record(context)

    failures, classified = _crashes(original, probe)
    atomic_json(context.record, original)
    assert classified, VACUOUS
    assert not failures, f"{len(failures)} wrong-typed fields crash the loader: {failures[:12]}"


def test_a_wrong_typed_queue_field_never_crashes_the_codec(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> None:
    context = _entered(tmp_path, monkeypatch, request.addfinalizer)
    original = json.loads((context.record.parent.parent.parent / "admission" / "queue.json").read_text())
    assert admission.to_logical(original).entries

    failures, classified = _crashes(original, admission.to_logical)
    assert classified, VACUOUS
    assert not failures, f"{len(failures)} wrong-typed queue fields crash the codec: {failures[:12]}"


class _Buildx:
    """The one Docker call ``build_records`` makes, answering with fixed standard output."""

    def __init__(self, output: str) -> None:
        self.output = output

    def call(self, *_arguments: str) -> str:
        return self.output


BUILDX_ROWS = {
    "build_history": {"ref": "builder/builder0/" + "a" * 25, "CreatedAt": "2026-01-01T00:00:00Z"},
    "build_cache": {"ID": "a" * 25, "CreatedAt": "2026-01-01T00:00:00Z"},
}


@pytest.mark.parametrize("kind", list(BUILDX_ROWS))
def test_a_wrong_typed_buildx_row_is_a_refusal_not_a_crash(kind: str) -> None:
    original = BUILDX_ROWS[kind]
    assert build_records(_Buildx(json.dumps(original)), kind, "builder") == {"a" * 25: original}

    failures, classified = _crashes(
        original, lambda mutated: build_records(_Buildx(json.dumps(mutated)), kind, "builder")
    )
    assert classified, VACUOUS
    assert not failures, f"{len(failures)} wrong-typed buildx fields crash the inventory: {failures[:12]}"


@pytest.mark.parametrize("kind", list(BUILDX_ROWS))
def test_a_deeply_nested_buildx_line_is_a_refusal_not_a_crash(kind: str) -> None:
    with pytest.raises(ToolkitError, match=r"Invalid .* inventory"):
        build_records(_Buildx("[" * 200_000), kind, "builder")


@pytest.mark.parametrize(
    "sharing",
    [{"mode": "exclusive"}, {"mode": "shared", "x": 1}, "shared", None, []],
    ids=["exclusive-mode", "extra-key", "string", "null", "list"],
)
def test_sharing_key_shape_is_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest, sharing: object
) -> None:
    """Only the exact ``{"mode": "shared"}`` is a valid ``sharing`` key; every other shape is an invalid record."""
    _handle, context, _account = _entered_resource_run(tmp_path, monkeypatch, retain=False)
    original = json.loads(context.record.read_text())
    pid = pause_guardian(context)  # the guardian stays paused while the probes write; it resumes on the original record
    request.addfinalizer(lambda: (atomic_json(context.record, original), resume_guardian(pid)))
    try:
        atomic_json(context.record, {**original, "sharing": {"mode": "shared"}})
        assert lifecycle.load_run_record(context)["sharing"] == {"mode": "shared"}
        atomic_json(context.record, {**original, "sharing": sharing})
        with pytest.raises(ToolkitError, match="sharing"):
            lifecycle.load_run_record(context)
    finally:
        atomic_json(context.record, original)
