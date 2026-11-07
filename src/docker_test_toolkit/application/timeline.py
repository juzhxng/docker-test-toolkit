"""Read-only hold timeline of a project's runs: queue, admission window, phases, builds and release.

The reader works on run record files only. It takes no lock, loads no queue and writes
nothing, so it can never block or change admission (DF15). Attribution scans every record file under the
registry's ``runs/``, not the project index, because an ordinary leave removes a completed run from the index (DF4).
"""

from __future__ import annotations

import hashlib
from pathlib import Path

from docker_test_toolkit.application import lifecycle
from docker_test_toolkit.application.recovery import (
    private_file_bytes,
    project_id_for,
    read_record,
    run_attribution,
    run_files,
)
from docker_test_toolkit.application.sharing import repository_origin
from docker_test_toolkit.domain.errors import ResourceError
from docker_test_toolkit.domain.records import record_cell_id

_NS = 1_000_000_000


def _tick(value: object) -> tuple[str, int] | None:
    """Return ``(origin, tick)`` of a deadline or observation mapping, or None when it carries no tick."""
    if not isinstance(value, dict):
        return None
    origin = value.get("origin")
    tick = value.get("created_ns", value.get("tick_ns"))
    return (origin, tick) if isinstance(origin, str) and isinstance(tick, int) else None


def _seconds(start: tuple[str, int] | None, end: tuple[str, int] | None) -> float | None:
    """Return the seconds between two ticks of the same clock origin; ticks of different boots are never compared."""
    if start is None or end is None or start[0] != end[0]:
        return None
    return (end[1] - start[1]) / _NS


def _leave_request(record: dict) -> tuple[str, int] | None:
    withdrawal = record.get("withdrawal")
    return _tick(withdrawal.get("deadline")) if isinstance(withdrawal, dict) else None


def _phases(record: dict, leave: tuple[str, int] | None) -> list[dict]:
    starts = [(item["phase"], _tick(item.get("memory"))) for item in record.get("phase_history") or []]
    if not starts:
        return []
    ends = [start for _phase, start in starts[1:]] + [leave]
    return [
        {"phase": phase, "start_ns": None if start is None else start[1], "seconds": _seconds(start, end)}
        for (phase, start), end in zip(starts, ends, strict=True)
    ]


def _builds(record: dict) -> list[dict]:
    builds = []
    for operation_id, receipt in sorted((record.get("docker_receipts") or {}).items()):
        if receipt.get("kind") != "build":
            continue
        start = _tick(receipt.get("deadline"))
        cache = (receipt.get("plan") or {}).get("cache")
        finished = cache.get("finished_ns") if isinstance(cache, dict) else None
        end = (start[0], finished) if start is not None and isinstance(finished, int) else None
        builds.append(
            {
                "operation_id": operation_id,
                "kind": receipt["kind"],
                "start_ns": None if start is None else start[1],
                "seconds": _seconds(start, end),
                "cache": cache,
            }
        )
    return builds


def timeline_entry(record: dict, *, indexed: bool) -> dict:
    """Derive one run's timeline entry from its loaded record.

    Facts an older record lacks are ``None`` with a note naming why; nothing is estimated from file times.
    """
    notes: set[str] = set()
    timeline = record.get("hold_timeline")
    observation = record.get("admission_observation")
    prepared = _tick(record.get("queue_deadline"))
    leave = _leave_request(record)
    granted = timeline is not None or isinstance(observation, dict)
    accounting: tuple[str, int] | None = None
    released: tuple[str, int] | None = None
    if timeline is not None:
        origin = timeline["origin"]
        accounting = (origin, timeline["accounting_from_ns"])
        reserved: tuple[str, int] | None = (origin, timeline["reserved_ns"])
        grant: tuple[str, int] | None = (origin, timeline["granted_ns"])
        reserved_source: str | None = "hold-timeline"
        released = None if timeline["released_ns"] is None else (timeline["release_origin"], timeline["released_ns"])
        if released is not None and released[0] != origin:
            notes.add("release-on-later-boot")
        waiting = timeline["waiting"]
    else:
        reserved = _tick(observation.get("window_start")) if isinstance(observation, dict) else None
        grant = _tick(observation.get("memory")) if isinstance(observation, dict) else None
        reserved_source = None if reserved is None else "window-start"
        evidence = record.get("failure_evidence")
        if isinstance(evidence, dict) and evidence.get("category") == "admission-expired":
            waiting = evidence.get("waiting")
        else:
            waiting = None
            notes.add("unrecorded")
    if granted and leave is None and record.get("state") in ("complete", "stopped-retained"):
        notes.add("recovered-without-leave")
    return {
        "run_id": record.get("run_id"),
        "session_id": record.get("session_id"),
        "entrypoint": record.get("entrypoint"),
        "purpose": record.get("purpose"),
        "state": record.get("state"),
        "test_outcome": record.get("test_outcome"),
        "cleanup_outcome": record.get("cleanup_outcome"),
        "indexed": indexed,
        "origin": None if prepared is None else prepared[0],
        "granted": granted,
        "release": None if timeline is None else timeline["release"],
        "segments": {
            "startup_s": _seconds(prepared, accounting),
            "queue_s": _seconds(prepared, reserved),
            "window_s": _seconds(reserved, grant),
            "active_s": _seconds(grant, leave),
            "stopping_s": _seconds(leave, released),
            "hold_s": _seconds(reserved, released),
            "hold_to_leave_s": _seconds(reserved, leave),
        },
        "reserved": reserved_source,
        "phases": _phases(record, leave),
        "builds": _builds(record),
        "waiting": waiting,
        "notes": sorted(notes),
    }


def _indexed_runs(registry: Path, project_id: str) -> frozenset[str]:
    """Return the run ids of the project index, read without a lock; a missing or unreadable index names none."""
    path = registry / "projects" / f"{project_id}.json"
    try:
        record = read_record(path, encoded=private_file_bytes(path))
    except ResourceError:
        return frozenset()
    return frozenset(record.get("runs") or ())


def project_timeline(registry: Path, root: Path) -> dict:
    """List the hold timeline of every run record attributed to one project.

    Every record file under ``registry/runs`` is read once; a file naming this project is loaded under the identity it
    records for itself (never under the caller's path), and one that does not load is listed by cell id only. Files
    that carry no project identity at all are counted account-wide and never listed.
    """
    project_id = project_id_for(root)
    indexed = _indexed_runs(registry, project_id)
    runs, unreadable, unattributable = [], [], 0
    for name, run_id, data, raw in run_files(registry):
        if raw is None:
            unattributable += 1
            continue
        attribution = run_attribution(raw)
        if attribution is None:
            unattributable += 1
            continue
        if attribution != project_id:
            continue
        try:
            context = lifecycle.recorded_run_context(registry, run_id, raw)
            if context is None:
                raise ResourceError("Run record names no usable identity.")
            record = lifecycle.load_run_record(context)
        except ResourceError:
            unreadable.append(record_cell_id("run", f"runs/{name}", hashlib.sha256(data).hexdigest()))
            continue
        runs.append(timeline_entry(record, indexed=run_id in indexed))
    repository = repository_origin(str(root))
    return {
        "project_id": project_id,
        "project_root": str(root),
        "repository": repository,  # the key of the project's share verdicts
        "verdict_scope": "origin" if repository is not None else "no-repository-origin",
        "runs": runs,
        "unreadable": unreadable,
        "unattributable_count": unattributable,
    }
