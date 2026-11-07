from __future__ import annotations

import json
from pathlib import Path

import pytest

from docker_test_toolkit.application.api import open_batches
from docker_test_toolkit.domain.errors import InvalidInput
from docker_test_toolkit.domain.models import (
    SUPPORTED_PROTOCOLS,
    ArtifactIdentity,
    BatchExecutionReport,
    BatchItem,
    BatchPlan,
    BatchRunEvidence,
    BatchSegment,
)


def _artifact() -> ArtifactIdentity:
    return ArtifactIdentity("0.2.0", "a" * 64, dict(SUPPORTED_PROTOCOLS))


def _plan(root: Path, *segments: BatchSegment) -> BatchPlan:
    return BatchPlan(root, _artifact(), segments)


def _evidence(
    run_id: str,
    *,
    primary: str = "passed",
    cleanup: str = "complete",
    category: str | None = None,
    cleanup_category: str | None = None,
) -> BatchExecutionReport:
    return BatchExecutionReport(
        primary,
        BatchRunEvidence(
            run_id,
            {
                "run_id": run_id,
                "state": "complete",
                "admission": "revoked",
                "generation": 1,
                "revision": 2,
                "ticket": None,
                "test_outcome": "passed" if primary == "passed" else "failed",
                "cleanup_outcome": cleanup,
            },
            category,
            cleanup_category,
        ),
    )


def _passed(batch_id: str) -> BatchExecutionReport:
    return _evidence(f"run-{batch_id}")


def _drain(coordinator) -> list[tuple[str, str, tuple[str, ...], tuple[str, ...], str | None]]:
    """Run a plan to the end with passing reports; return each batch's identity tuple."""
    sequence = []
    while (batch := coordinator.next()) is not None:
        sequence.append((batch.batch_id, batch.segment_id, batch.item_ids, batch.closure, batch.trailing_boundary_id))
        report = _passed(batch.batch_id) if batch.closure else BatchExecutionReport("passed", None)
        if coordinator.report(batch.batch_id, report):
            coordinator.report_boundary(batch.trailing_boundary_id, "passed")
    return sequence


def test_groups_each_segment_into_one_union_batch_then_one_host_batch(tmp_path: Path) -> None:
    coordinator = open_batches(
        _plan(
            tmp_path,
            BatchSegment(
                "segment-a",
                (
                    BatchItem("one", ("postgres",)),
                    BatchItem("two", ("redis",)),
                    BatchItem("three", ("postgres",)),
                    BatchItem("host", ()),
                    BatchItem("atomic", ("postgres", "qdrant")),
                ),
                trailing_boundary_id="reset-a",
            ),
        )
    )

    assert _drain(coordinator) == [
        ("batch-1", "segment-a", ("one", "two", "three", "atomic"), ("postgres", "redis", "qdrant"), None),
        ("batch-2", "segment-a", ("host",), (), "reset-a"),
    ]
    aggregate = coordinator.finish()
    assert aggregate.exit_code == 0
    assert [result.outcome for result in aggregate.batches] == ["passed"] * 2
    assert [(result.boundary_id, result.outcome) for result in aggregate.boundaries] == [("reset-a", "passed")]
    assert [(result.segment_id, result.outcome) for result in aggregate.segments] == [("segment-a", "passed")]


def test_union_uses_a_declared_tuple_for_the_same_set(tmp_path: Path) -> None:
    items = (
        BatchItem("a", ("qdrant",)),
        BatchItem("b", ("postgres",)),
        BatchItem("c", ("postgres", "qdrant")),
    )

    sequence = _drain(open_batches(_plan(tmp_path, BatchSegment("s", items))))

    assert sequence[0][3] == ("postgres", "qdrant")


def test_union_without_a_declaring_item_uses_first_appearance_across_the_plan(tmp_path: Path) -> None:
    sequence = _drain(
        open_batches(
            _plan(
                tmp_path,
                BatchSegment("one", (BatchItem("a", ("postgres",)), BatchItem("b", ("redis",)))),
                BatchSegment("two", (BatchItem("c", ("redis",)), BatchItem("d", ("postgres",)))),
            )
        )
    )

    assert [entry[3] for entry in sequence] == [("postgres", "redis"), ("postgres", "redis")]
    # P3: set-equal unions in two segments stay two batches, each stamped with its own segment.
    assert [(entry[1], entry[2]) for entry in sequence] == [("one", ("a", "b")), ("two", ("c", "d"))]


@pytest.mark.parametrize("host_first", [True, False])
def test_trailing_boundary_rides_the_last_batch_whatever_the_item_order(tmp_path: Path, host_first: bool) -> None:
    docker, host = BatchItem("d", ("postgres",)), BatchItem("h", ())
    items = (host, docker) if host_first else (docker, host)

    sequence = _drain(open_batches(_plan(tmp_path, BatchSegment("s", items, trailing_boundary_id="reset"))))

    assert sequence == [
        ("batch-1", "s", ("d",), ("postgres",), None),
        ("batch-2", "s", ("h",), (), "reset"),
    ]


def test_same_plan_opens_to_identical_batch_sequences(tmp_path: Path) -> None:
    segments = (
        BatchSegment("one", (BatchItem("a", ("redis", "postgres")), BatchItem("h", ()), BatchItem("b", ("qdrant",)))),
        BatchSegment("two", (BatchItem("c", ("qdrant",)), BatchItem("d", ("redis",))), trailing_boundary_id="r"),
    )

    first = _drain(open_batches(_plan(tmp_path, *segments)))
    second = _drain(open_batches(_plan(tmp_path, *segments)))

    assert first == second


def test_union_infrastructure_failure_keeps_the_host_batch_eligible(tmp_path: Path) -> None:
    coordinator = open_batches(_plan(tmp_path, BatchSegment("s", (BatchItem("a", ("postgres",)), BatchItem("h", ())))))
    union = coordinator.next()
    assert union is not None and union.closure == ("postgres",)
    coordinator.report(
        union.batch_id,
        _evidence("run-u", primary="infrastructure_failed", category="infrastructure_failure"),
    )

    host = coordinator.next()

    assert host is not None and host.item_ids == ("h",)
    coordinator.report(host.batch_id, BatchExecutionReport("passed", None))
    assert coordinator.next() is None
    aggregate = coordinator.finish()
    assert [(result.item_ids, result.outcome) for result in aggregate.batches] == [
        (("a",), "infrastructure_failed"),
        (("h",), "passed"),
    ]


def test_one_segment_per_service_set_reproduces_exact_grouping(tmp_path: Path) -> None:
    fixture = Path(__file__).parents[1] / "fixtures" / "batch" / "sample-a-closures.json"
    closures = [tuple(closure) for closure in json.loads(fixture.read_text())["closures"]]
    assert len({closure for closure in closures if closure}) == 15 and () in closures  # today's 15 runs plus host
    items = [BatchItem(f"item-{index}", closure) for index, closure in enumerate(closures, 1)]
    groups: dict[tuple[str, ...], list[BatchItem]] = {}
    for item in items:
        groups.setdefault(item.closure, []).append(item)
    expected = [
        (f"batch-{number}", tuple(item.item_id for item in group), closure)
        for number, (closure, group) in enumerate(groups.items(), 1)
    ]
    segments = tuple(
        BatchSegment(
            f"set-{number}",
            tuple(group),
            trailing_boundary_id="reset" if number == len(groups) else None,
        )
        for number, group in enumerate(groups.values(), 1)
    )

    sequence = _drain(open_batches(_plan(tmp_path, *segments)))

    assert [(batch_id, item_ids, closure) for batch_id, _, item_ids, closure, _ in sequence] == expected
    assert [boundary for *_, boundary in sequence] == [None] * (len(groups) - 1) + ["reset"]


def test_all_empty_and_all_docker_segments_yield_one_batch_each(tmp_path: Path) -> None:
    sequence = _drain(
        open_batches(
            _plan(
                tmp_path,
                BatchSegment("host", (BatchItem("h1", ()), BatchItem("h2", ()))),
                BatchSegment("docker", (BatchItem("d1", ("redis",)), BatchItem("d2", ("postgres",)))),
            )
        )
    )

    assert sequence == [
        ("batch-1", "host", ("h1", "h2"), (), None),
        ("batch-2", "docker", ("d1", "d2"), ("redis", "postgres"), None),
    ]


def test_test_failure_continues_then_skips_dependent_segment_and_runs_independent_host(
    tmp_path: Path,
) -> None:
    coordinator = open_batches(
        _plan(
            tmp_path,
            BatchSegment(
                "tests",
                (BatchItem("postgres-test", ("postgres",)), BatchItem("tests-host", ())),
                trailing_boundary_id="reset-tests",
            ),
            BatchSegment("dependent", (BatchItem("dependent-host", ()),), requires_previous_success=True),
            BatchSegment("independent", (BatchItem("independent-host", ()),)),
        )
    )

    failed = coordinator.next()
    assert failed is not None
    assert coordinator.report(failed.batch_id, _evidence("run-failed", primary="test_failed")) is False
    successor = coordinator.next()
    assert successor is not None and successor.item_ids == ("tests-host",)
    assert coordinator.report(successor.batch_id, BatchExecutionReport("passed", None)) is True
    coordinator.report_boundary("reset-tests", "passed")

    independent = coordinator.next()
    assert independent is not None
    assert independent.item_ids == ("independent-host",)
    coordinator.report(independent.batch_id, BatchExecutionReport("passed", None))
    assert coordinator.next() is None

    aggregate = coordinator.finish()
    assert aggregate.exit_code == 6
    assert [result.outcome for result in aggregate.segments] == ["failed", "skipped", "passed"]
    skipped = next(result for result in aggregate.batches if result.segment_id == "dependent")
    assert (skipped.outcome, skipped.reason, skipped.report) == (
        "skipped",
        "previous_segment_not_passed",
        None,
    )


@pytest.mark.parametrize(
    ("category", "expected_exit", "expected_reason"),
    (
        ("incompatible_state", 3, "docker_stopped_incompatible_state"),
        ("contention", 4, "docker_stopped_contention_or_timeout"),
        ("infrastructure_failure", 5, "docker_stopped_infrastructure"),
    ),
)
def test_docker_stop_marks_later_docker_not_run_but_allows_independent_host(
    tmp_path: Path,
    category: str,
    expected_exit: int,
    expected_reason: str,
) -> None:
    coordinator = open_batches(
        _plan(
            tmp_path,
            BatchSegment(
                "first",
                (BatchItem("docker", ("postgres",)), BatchItem("same-segment-host", ())),
                trailing_boundary_id="reset-first",
            ),
            BatchSegment("later-docker", (BatchItem("later", ("redis",)),)),
            BatchSegment("later-host", (BatchItem("host", ()),)),
        )
    )
    first = coordinator.next()
    assert first is not None
    report = BatchExecutionReport(
        "infrastructure_failed",
        BatchRunEvidence(None, None, category, None),
    )
    assert coordinator.report(first.batch_id, report) is False

    same_segment_host = coordinator.next()
    assert same_segment_host is not None
    assert same_segment_host.item_ids == ("same-segment-host",)
    coordinator.report(same_segment_host.batch_id, BatchExecutionReport("passed", None))
    later_host = coordinator.next()
    assert later_host is not None
    assert later_host.item_ids == ("host",)
    coordinator.report(later_host.batch_id, BatchExecutionReport("passed", None))
    assert coordinator.next() is None

    aggregate = coordinator.finish()
    assert aggregate.exit_code == expected_exit
    later = next(result for result in aggregate.batches if result.segment_id == "later-docker")
    assert (later.outcome, later.reason) == ("not_run", expected_reason)
    boundary = next(result for result in aggregate.boundaries if result.boundary_id == "reset-first")
    assert (boundary.outcome, boundary.reason) == ("not_run", expected_reason)


def test_cleanup_only_failure_stops_docker_and_uses_exit_seven(tmp_path: Path) -> None:
    coordinator = open_batches(
        _plan(
            tmp_path,
            BatchSegment("one", (BatchItem("first", ("postgres",)),)),
            BatchSegment("two", (BatchItem("second", ("redis",)),)),
        )
    )
    first = coordinator.next()
    assert first is not None
    coordinator.report(
        first.batch_id,
        _evidence(
            "run-cleanup",
            cleanup="incomplete",
            cleanup_category="incomplete_cleanup",
        ),
    )
    assert coordinator.next() is None

    aggregate = coordinator.finish()
    assert aggregate.exit_code == 7
    assert [result.outcome for result in aggregate.batches] == ["cleanup_failed", "not_run"]
    assert aggregate.batches[1].reason == "docker_stopped_cleanup"


def test_primary_failure_keeps_exit_precedence_over_incomplete_cleanup(tmp_path: Path) -> None:
    coordinator = open_batches(_plan(tmp_path, BatchSegment("one", (BatchItem("first", ("postgres",)),))))
    first = coordinator.next()
    assert first is not None
    coordinator.report(
        first.batch_id,
        _evidence(
            "run-both",
            primary="test_failed",
            cleanup="incomplete",
            cleanup_category="incomplete_cleanup",
        ),
    )
    assert coordinator.finish().exit_code == 6


def test_boundary_failure_is_safety_exit_and_stops_later_docker(tmp_path: Path) -> None:
    coordinator = open_batches(
        _plan(
            tmp_path,
            BatchSegment(
                "first",
                (BatchItem("first-item", ("postgres",)),),
                trailing_boundary_id="reset",
            ),
            BatchSegment("second", (BatchItem("second-item", ("redis",)),)),
        )
    )
    first = coordinator.next()
    assert first is not None
    assert coordinator.report(first.batch_id, _passed(first.batch_id)) is True
    coordinator.report_boundary("reset", "safety_failed")
    assert coordinator.next() is None

    aggregate = coordinator.finish()
    assert aggregate.exit_code == 5
    assert aggregate.boundaries[0].outcome == "safety_failed"
    assert aggregate.batches[1].reason == "docker_stopped_safety"


def test_invalid_order_or_evidence_aborts_the_coordinator(tmp_path: Path) -> None:
    coordinator = open_batches(_plan(tmp_path, BatchSegment("one", (BatchItem("first", ("postgres",)),))))
    batch = coordinator.next()
    assert batch is not None

    with pytest.raises(InvalidInput, match="evidence"):
        coordinator.report(batch.batch_id, BatchExecutionReport("passed", None))
    with pytest.raises(InvalidInput, match="aborted"):
        coordinator.next()
    with pytest.raises(InvalidInput, match="aggregate"):
        coordinator.finish()


def test_plan_rejects_duplicates_empty_segments_and_alternate_closure_order(tmp_path: Path) -> None:
    with pytest.raises(InvalidInput, match="nonempty"):
        BatchPlan(tmp_path, _artifact(), ())
    with pytest.raises(InvalidInput, match="nonempty"):
        BatchSegment("empty", ())
    with pytest.raises(InvalidInput, match="unique item"):
        _plan(
            tmp_path,
            BatchSegment("one", (BatchItem("duplicate", ()),)),
            BatchSegment("two", (BatchItem("duplicate", ()),)),
        )
    with pytest.raises(InvalidInput, match="canonical order"):
        _plan(
            tmp_path,
            BatchSegment(
                "one",
                (
                    BatchItem("item-one", ("postgres", "qdrant")),
                    BatchItem("item-two", ("qdrant", "postgres")),
                ),
            ),
        )
    with pytest.raises(InvalidInput, match="globally unique"):
        _plan(
            tmp_path,
            BatchSegment(
                "shared-id",
                (BatchItem("item", ()),),
                trailing_boundary_id="shared-id",
            ),
        )


def test_first_segment_cannot_require_previous_success(tmp_path: Path) -> None:
    with pytest.raises(InvalidInput, match="first segment"):
        _plan(
            tmp_path,
            BatchSegment("one", (BatchItem("host", ()),), requires_previous_success=True),
        )


def test_finish_refuses_eligible_or_outstanding_work_and_is_single_use(tmp_path: Path) -> None:
    coordinator = open_batches(_plan(tmp_path, BatchSegment("one", (BatchItem("host", ()),))))
    with pytest.raises(InvalidInput, match="eligible"):
        coordinator.finish()
    batch = coordinator.next()
    assert batch is not None
    with pytest.raises(InvalidInput, match="outstanding"):
        coordinator.finish()
    coordinator.report(batch.batch_id, BatchExecutionReport("passed", None))
    aggregate = coordinator.finish()
    assert aggregate.exit_code == 0
    with pytest.raises(InvalidInput, match="finished"):
        coordinator.finish()


@pytest.mark.parametrize(
    ("primary", "status_changes", "category", "cleanup_category", "message"),
    (
        ("passed", {"run_id": "other"}, None, None, "identities"),
        ("passed", {"state": "active"}, None, None, "terminal"),
        ("passed", {"test_outcome": "failed"}, None, None, "primary"),
        ("passed", {"cleanup_outcome": "incomplete"}, None, None, "cleanup"),
        ("passed", {}, "infrastructure_failure", None, "primary category"),
        ("infrastructure_failed", {}, None, None, "primary category"),
    ),
)
def test_docker_evidence_contradictions_abort(
    tmp_path: Path,
    primary: str,
    status_changes: dict[str, object],
    category: str | None,
    cleanup_category: str | None,
    message: str,
) -> None:
    coordinator = open_batches(_plan(tmp_path, BatchSegment("one", (BatchItem("docker", ("postgres",)),))))
    batch = coordinator.next()
    assert batch is not None
    evidence = _evidence("run-one", primary=primary).run_evidence
    assert evidence is not None and evidence.run_status is not None
    status = dict(evidence.run_status)
    status.update(status_changes)
    report = BatchExecutionReport(
        primary,
        BatchRunEvidence("run-one", status, category, cleanup_category),
    )

    with pytest.raises(InvalidInput, match=message):
        coordinator.report(batch.batch_id, report)
    with pytest.raises(InvalidInput, match="aborted"):
        coordinator.finish()


def test_early_run_identity_requires_incomplete_cleanup_and_stops_docker(tmp_path: Path) -> None:
    coordinator = open_batches(
        _plan(
            tmp_path,
            BatchSegment("one", (BatchItem("opened", ("postgres",)),)),
            BatchSegment("two", (BatchItem("later", ("redis",)),)),
        )
    )
    opened = coordinator.next()
    assert opened is not None

    with pytest.raises(InvalidInput, match="incomplete-cleanup"):
        coordinator.report(
            opened.batch_id,
            BatchExecutionReport(
                "infrastructure_failed",
                BatchRunEvidence("run-opened", None, "infrastructure_failure", None),
            ),
        )


def test_cancelled_terminal_status_is_valid_infrastructure_evidence(tmp_path: Path) -> None:
    coordinator = open_batches(_plan(tmp_path, BatchSegment("one", (BatchItem("docker", ("postgres",)),))))
    batch = coordinator.next()
    assert batch is not None
    report = _evidence("run-cancelled", primary="infrastructure_failed", category="contention")
    evidence = report.run_evidence
    assert evidence is not None and evidence.run_status is not None
    status = dict(evidence.run_status)
    status["test_outcome"] = "cancelled"

    coordinator.report(
        batch.batch_id,
        BatchExecutionReport(
            "infrastructure_failed",
            BatchRunEvidence("run-cancelled", status, "contention", None),
        ),
    )

    assert coordinator.finish().exit_code == 4


def test_preopen_failure_cannot_invent_incomplete_cleanup(tmp_path: Path) -> None:
    coordinator = open_batches(_plan(tmp_path, BatchSegment("one", (BatchItem("docker", ("postgres",)),))))
    batch = coordinator.next()
    assert batch is not None

    with pytest.raises(InvalidInput, match="cleanup"):
        coordinator.report(
            batch.batch_id,
            BatchExecutionReport(
                "infrastructure_failed",
                BatchRunEvidence(None, None, "contention", "incomplete_cleanup"),
            ),
        )


def test_fully_stopped_segment_uses_the_closed_not_run_outcome(tmp_path: Path) -> None:
    coordinator = open_batches(
        _plan(
            tmp_path,
            BatchSegment("first", (BatchItem("host", ()),)),
            BatchSegment("stopped", (BatchItem("docker", ("postgres",)),)),
        )
    )
    host = coordinator.next()
    assert host is not None
    coordinator.report(host.batch_id, BatchExecutionReport("infrastructure_failed", None))
    assert coordinator.next() is None

    assert coordinator.finish().segments[1].outcome == "not_run"


def test_aggregate_exit_uses_documented_primary_precedence(tmp_path: Path) -> None:
    coordinator = open_batches(
        _plan(
            tmp_path,
            BatchSegment("one", (BatchItem("test", ()),)),
            BatchSegment("two", (BatchItem("infrastructure", ()),)),
        )
    )
    first = coordinator.next()
    assert first is not None
    coordinator.report(first.batch_id, BatchExecutionReport("test_failed", None))
    second = coordinator.next()
    assert second is not None
    coordinator.report(second.batch_id, BatchExecutionReport("infrastructure_failed", None))

    assert coordinator.finish().exit_code == 5


def test_first_global_stop_reason_is_not_rewritten_by_a_later_host_failure(tmp_path: Path) -> None:
    coordinator = open_batches(
        _plan(
            tmp_path,
            BatchSegment("first", (BatchItem("first-docker", ("postgres",)),)),
            BatchSegment("host-segment", (BatchItem("host", ()),)),
            BatchSegment("last", (BatchItem("last-docker", ("redis",)),)),
        )
    )
    first = coordinator.next()
    assert first is not None
    coordinator.report(
        first.batch_id,
        BatchExecutionReport(
            "infrastructure_failed",
            BatchRunEvidence(None, None, "contention", None),
        ),
    )
    host = coordinator.next()
    assert host is not None
    coordinator.report(host.batch_id, BatchExecutionReport("infrastructure_failed", None))
    assert coordinator.next() is None

    aggregate = coordinator.finish()
    assert aggregate.exit_code == 4
    assert aggregate.batches[-1].reason == "docker_stopped_contention_or_timeout"


def test_trailing_boundary_rides_the_union_batch_of_a_docker_only_segment(tmp_path: Path) -> None:
    items = (BatchItem("a", ("postgres",)), BatchItem("b", ("redis",)))

    sequence = _drain(open_batches(_plan(tmp_path, BatchSegment("s", items, trailing_boundary_id="reset"))))

    assert sequence == [("batch-1", "s", ("a", "b"), ("postgres", "redis"), "reset")]


def test_union_uses_a_tuple_declared_in_a_later_segment(tmp_path: Path) -> None:
    sequence = _drain(
        open_batches(
            _plan(
                tmp_path,
                BatchSegment("first", (BatchItem("a", ("qdrant",)), BatchItem("b", ("postgres",)))),
                BatchSegment("second", (BatchItem("c", ("postgres", "qdrant")),)),
            )
        )
    )

    assert [entry[3] for entry in sequence] == [("postgres", "qdrant"), ("postgres", "qdrant")]
