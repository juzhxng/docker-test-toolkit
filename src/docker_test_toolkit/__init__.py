"""Closed public facade for the reusable local Docker test toolkit."""

from docker_test_toolkit.application.api import (
    ConsumerRunContext,
    RunHandle,
    current_consumer_run,
    open_batches,
    toolkit_identity,
)
from docker_test_toolkit.application.batch import BatchCoordinator
from docker_test_toolkit.domain.errors import ToolkitError
from docker_test_toolkit.domain.models import (
    ArtifactIdentity,
    BatchAggregate,
    BatchExecutionReport,
    BatchItem,
    BatchPlan,
    BatchResult,
    BatchRunEvidence,
    BatchSegment,
    BoundaryResult,
    Endpoint,
    ExternalDockerResource,
    PhaseBudget,
    PlannedBatch,
    RunSpec,
    SegmentResult,
)
from docker_test_toolkit.factories.api import open_retained_run, open_run

__all__ = [
    "ArtifactIdentity",
    "BatchAggregate",
    "BatchCoordinator",
    "BatchExecutionReport",
    "BatchItem",
    "BatchPlan",
    "BatchResult",
    "BatchRunEvidence",
    "BatchSegment",
    "BoundaryResult",
    "ConsumerRunContext",
    "Endpoint",
    "ExternalDockerResource",
    "PhaseBudget",
    "PlannedBatch",
    "RunHandle",
    "RunSpec",
    "SegmentResult",
    "ToolkitError",
    "current_consumer_run",
    "open_batches",
    "open_retained_run",
    "open_run",
    "toolkit_identity",
]
