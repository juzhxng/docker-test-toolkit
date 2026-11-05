"""Pure process-local coordination for finite adapter-owned test batches."""

from __future__ import annotations

from collections.abc import Sequence
from typing import NoReturn

from docker_test_toolkit.domain.errors import InvalidInput
from docker_test_toolkit.domain.models import (
    BatchAggregate,
    BatchExecutionReport,
    BatchPlan,
    BatchResult,
    BoundaryResult,
    PlannedBatch,
    SegmentResult,
)

_CATEGORY_RESULTS = {
    "incompatible_state": (3, "docker_stopped_incompatible_state"),
    "contention": (4, "docker_stopped_contention_or_timeout"),
    "infrastructure_failure": (5, "docker_stopped_infrastructure"),
}


class BatchCoordinator:
    """Sequence a validated finite plan without owning commands or resources."""

    def __init__(self, plan: BatchPlan) -> None:
        if not isinstance(plan, BatchPlan):
            raise InvalidInput("open_batches requires a BatchPlan.")
        self._plan = plan
        self._segments = self._group(plan)
        self._segment_index = 0
        self._batch_index = 0
        self._pending_batch: PlannedBatch | None = None
        self._pending_boundary: str | None = None
        self._batch_results: list[BatchResult] = []
        self._boundary_results: list[BoundaryResult] = []
        self._segment_results: list[SegmentResult] = []
        self._exit_codes: set[int] = set()
        self._docker_stop_reason: str | None = None
        self._aborted = False
        self._finished = False

    @staticmethod
    def _group(plan: BatchPlan) -> tuple[tuple[PlannedBatch, ...], ...]:
        """Plan one union batch of a segment's Docker items, then one batch of its host-only items.

        Items keep the adapter's order inside each batch; segments never merge. The segment's trailing boundary rides
        its last emitted batch, so it runs after the host-only work too.
        """
        grouped_segments: list[tuple[PlannedBatch, ...]] = []
        batch_number = 0
        for segment in plan.segments:
            docker = [item for item in segment.items if item.closure]
            host = [item for item in segment.items if not item.closure]
            groups: list[tuple[tuple[str, ...], list[str]]] = []
            if docker:
                closures = [item.closure for item in docker]
                groups.append((BatchCoordinator._canonical_union(plan, closures), [item.item_id for item in docker]))
            if host:
                groups.append(((), [item.item_id for item in host]))
            batches: list[PlannedBatch] = []
            for position, (closure, item_ids) in enumerate(groups, 1):
                batch_number += 1
                batches.append(
                    PlannedBatch(
                        batch_id=f"batch-{batch_number}",
                        segment_id=segment.segment_id,
                        item_ids=tuple(item_ids),
                        closure=closure,
                        trailing_boundary_id=segment.trailing_boundary_id if position == len(groups) else None,
                    )
                )
            grouped_segments.append(tuple(batches))
        return tuple(grouped_segments)

    @staticmethod
    def _canonical_union(plan: BatchPlan, closures: Sequence[tuple[str, ...]]) -> tuple[str, ...]:
        """Order a segment's union the same way wherever the same service set appears in the plan.

        A tuple that an item of the plan declares for exactly this set wins (the first such item in plan order);
        otherwise services follow their first appearance across the whole plan.
        """
        union = {service for closure in closures for service in closure}
        plan_closures = [item.closure for segment in plan.segments for item in segment.items]
        for closure in plan_closures:
            if set(closure) == union:
                return closure
        order: dict[str, None] = {}
        for closure in plan_closures:
            order.update(dict.fromkeys(closure))
        return tuple(service for service in order if service in union)

    def _require_active(self) -> None:
        if self._aborted:
            raise InvalidInput("Batch coordinator is aborted; no aggregate is available.")
        if self._finished:
            raise InvalidInput("Batch coordinator is already finished.")

    def _abort(self, message: str) -> NoReturn:
        self._aborted = True
        raise InvalidInput(message)

    def _skip_segment(self) -> None:
        segment = self._plan.segments[self._segment_index]
        reason = "previous_segment_not_passed"
        for batch in self._segments[self._segment_index]:
            self._batch_results.append(
                BatchResult(
                    batch.batch_id,
                    batch.segment_id,
                    batch.item_ids,
                    batch.closure,
                    "skipped",
                    None,
                    reason,
                )
            )
        if segment.trailing_boundary_id is not None:
            self._boundary_results.append(
                BoundaryResult(segment.trailing_boundary_id, segment.segment_id, "skipped", reason)
            )
        self._segment_results.append(SegmentResult(segment.segment_id, "skipped", reason))
        self._advance_segment()

    def _advance_segment(self) -> None:
        self._segment_index += 1
        self._batch_index = 0

    def _record_stopped_batch(self, batch: PlannedBatch) -> None:
        self._batch_results.append(
            BatchResult(
                batch.batch_id,
                batch.segment_id,
                batch.item_ids,
                batch.closure,
                "not_run",
                None,
                self._docker_stop_reason,
            )
        )
        self._batch_index += 1

    def _finish_segment(self) -> None:
        segment = self._plan.segments[self._segment_index]
        batches = [result for result in self._batch_results if result.segment_id == segment.segment_id]
        boundaries = [result for result in self._boundary_results if result.segment_id == segment.segment_id]
        if all(result.outcome == "not_run" for result in batches):
            outcome = "not_run"
            reason = self._docker_stop_reason
        elif all(result.outcome == "passed" for result in batches) and all(
            result.outcome == "passed" for result in boundaries
        ):
            outcome = "passed"
            reason = None
        else:
            outcome = "failed"
            reason = None
        self._segment_results.append(SegmentResult(segment.segment_id, outcome, reason))
        self._advance_segment()

    def _close_segment(self) -> bool:
        segment = self._plan.segments[self._segment_index]
        if segment.trailing_boundary_id is None:
            self._finish_segment()
            return False
        if self._docker_stop_reason is not None:
            self._boundary_results.append(
                BoundaryResult(
                    segment.trailing_boundary_id,
                    segment.segment_id,
                    "not_run",
                    self._docker_stop_reason,
                )
            )
            self._finish_segment()
            return False
        self._pending_boundary = segment.trailing_boundary_id
        return True

    def next(self) -> PlannedBatch | None:
        """Return the next runnable exact-closure batch, or ``None`` when closed."""
        self._require_active()
        if self._pending_batch is not None or self._pending_boundary is not None:
            self._abort("A batch report or trailing boundary is still outstanding.")
        while self._segment_index < len(self._segments):
            segment = self._plan.segments[self._segment_index]
            if self._batch_index == 0 and segment.requires_previous_success:
                if self._segment_results[-1].outcome != "passed":
                    self._skip_segment()
                    continue
            batches = self._segments[self._segment_index]
            if self._batch_index == len(batches):
                if self._close_segment():
                    self._abort("A trailing boundary must be reported before requesting another batch.")
                continue
            batch = batches[self._batch_index]
            if batch.closure and self._docker_stop_reason is not None:
                self._record_stopped_batch(batch)
                continue
            self._pending_batch = batch
            return batch
        return None

    def _validate_report(self, batch: PlannedBatch, report: BatchExecutionReport) -> None:
        if not isinstance(report, BatchExecutionReport):
            self._abort("Batch report must be a BatchExecutionReport.")
        evidence = report.run_evidence
        if not batch.closure:
            if evidence is not None:
                self._abort("Host-only batch evidence must not contain run evidence.")
            return
        if evidence is None:
            self._abort("Docker batch evidence requires ordinary run evidence.")
        status = evidence.run_status
        if status is not None:
            if evidence.run_id is None or status["run_id"] != evidence.run_id:
                self._abort("Batch run evidence contains unequal run identities.")
            if status["state"] != "complete" or status["admission"] != "revoked":
                self._abort("Batch run evidence is not terminal ordinary lifecycle evidence.")
            expected_tests = {
                "passed": {"passed"},
                "test_failed": {"failed"},
                "infrastructure_failed": {"failed", "cancelled"},
            }[report.primary]
            if status["test_outcome"] not in expected_tests:
                self._abort("Batch run evidence contradicts the reported primary outcome.")
            expected_cleanup = "incomplete" if evidence.cleanup_category else "complete"
            if status["cleanup_outcome"] != expected_cleanup:
                self._abort("Batch run evidence contradicts the reported cleanup outcome.")
        elif report.primary != "infrastructure_failed" or evidence.category is None:
            self._abort("Batch run evidence lacks terminal status or a lifecycle failure category.")
        elif evidence.run_id is not None and evidence.cleanup_category != "incomplete_cleanup":
            self._abort("Opened batch without terminal status requires incomplete-cleanup evidence.")
        elif evidence.run_id is None and evidence.cleanup_category is not None:
            self._abort("Pre-open batch failure cannot contain cleanup evidence.")
        if report.primary == "infrastructure_failed":
            if evidence.category is None:
                self._abort("Infrastructure failure evidence requires one primary category.")
        elif evidence.category is not None:
            self._abort("Passed or test-failed evidence cannot contain a primary category.")

    def report(self, batch_id: str, report: BatchExecutionReport) -> bool:
        """Accept one adapter result and say whether its trailing boundary must run."""
        self._require_active()
        batch = self._pending_batch
        if batch is None or batch.batch_id != batch_id:
            self._abort("Batch report does not match the outstanding batch.")
        self._validate_report(batch, report)
        evidence = report.run_evidence
        if report.primary == "passed":
            outcome = "cleanup_failed" if evidence and evidence.cleanup_category else "passed"
            if outcome == "cleanup_failed":
                self._exit_codes.add(7)
                self._docker_stop_reason = self._docker_stop_reason or "docker_stopped_cleanup"
        elif report.primary == "test_failed":
            outcome = "test_failed"
            self._exit_codes.add(6)
            if evidence and evidence.cleanup_category:
                self._docker_stop_reason = self._docker_stop_reason or "docker_stopped_cleanup"
        else:
            outcome = "infrastructure_failed"
            category = evidence.category if evidence is not None else "infrastructure_failure"
            if category is None:
                self._abort("Infrastructure failure evidence requires one primary category.")
            exit_code, reason = _CATEGORY_RESULTS[category]
            self._exit_codes.add(exit_code)
            self._docker_stop_reason = self._docker_stop_reason or reason
        self._batch_results.append(
            BatchResult(batch.batch_id, batch.segment_id, batch.item_ids, batch.closure, outcome, report, None)
        )
        self._pending_batch = None
        self._batch_index += 1
        if self._batch_index == len(self._segments[self._segment_index]):
            return self._close_segment()
        return False

    def report_boundary(self, boundary_id: str, outcome: str) -> None:
        """Accept one required adapter-owned trailing boundary result."""
        self._require_active()
        if self._pending_boundary is None or boundary_id != self._pending_boundary:
            self._abort("Boundary report does not match the outstanding trailing boundary.")
        if outcome not in {"passed", "safety_failed"}:
            self._abort("Boundary outcome must be passed or safety_failed.")
        segment = self._plan.segments[self._segment_index]
        self._boundary_results.append(BoundaryResult(boundary_id, segment.segment_id, outcome, None))
        self._pending_boundary = None
        if outcome == "safety_failed":
            self._exit_codes.add(5)
            self._docker_stop_reason = self._docker_stop_reason or "docker_stopped_safety"
        self._finish_segment()

    def finish(self) -> BatchAggregate:
        """Return the complete normal aggregate exactly once."""
        self._require_active()
        if self._pending_batch is not None or self._pending_boundary is not None:
            raise InvalidInput("Batch coordinator has outstanding work.")
        if self._segment_index < len(self._segments):
            raise InvalidInput("Batch coordinator still has eligible work.")
        self._finished = True
        exit_code = min(self._exit_codes) if self._exit_codes else 0
        return BatchAggregate(
            exit_code,
            "passed" if exit_code == 0 else "failed",
            tuple(self._segment_results),
            tuple(self._batch_results),
            tuple(self._boundary_results),
        )
