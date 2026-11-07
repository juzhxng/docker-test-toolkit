"""Stable public error categories and the closed reason vocabulary."""

# Machine-readable refusal reasons carried by `context.reason` of the CLI envelope. A code outside
# this set reaches callers as "unspecified"; the category set stays closed and separate.
REASONS = frozenset(
    {
        "actor-live",
        "actor-unverifiable",
        "archive-commit-unconfirmed",
        "archive-exists",
        "archive-record-invalid",
        "blocked-entry",
        "capacity-lock-held",
        "claim-held",
        "client-exit",
        "coholder-records-unstable",
        "compose-construct-refused",
        "daemon-identity-changed",
        "export-storage",
        "guardian-start-pending",
        "image-lock-held",
        "lock-deadline",
        "observation-deadline",
        "podman-binary-missing",
        "podman-helper-unsupported",
        "podman-machine-not-found",
        "queue-invalid",
        "record-changed",
        "record-invalid",
        "resource-present",
        "retirement-incomplete",
        "runtime-unqualified",
        "shape-mismatch",
        "share-rule",
        "state-format-newer",
        "telemetry-unreadable",
        "toolkit-load-unresolved",
        "toolkit-not-installed",
        "toolkit-upgraded",
        "unspecified",
        "unsupported-image-store",
        "unsupported-operation-kind",
        "unsupported-receipt-kind",
        "unsupported-runtime",
    }
)


class ToolkitError(RuntimeError):
    """Report a refused or incomplete toolkit operation."""

    category = "infrastructure_failure"
    cleanup: "ToolkitError | None" = None
    """A separate cleanup failure that accompanies this primary error, reported on its own."""

    def __init__(
        self,
        message: str = "",
        *,
        reason: str = "unspecified",
        observer: str | None = None,
        details: dict[str, object] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason = reason
        self.observer = observer  # the observation source, set once at the innermost boundary that knows it
        self.details = {} if details is None else details  # structured facts for ``error.context``, additive


class ResourceError(ToolkitError):
    """Report incomplete lifecycle authority or infrastructure evidence."""

    category = "incompatible_state"


class ResourceBusy(ResourceError):
    """Report a live authority owner that prevents immediate progress."""

    category = "contention"


class InvalidInput(ToolkitError):
    """Report a malformed or unsupported caller request."""

    category = "invalid_input"


class IncompatibleState(ResourceError):
    """Report state or authority that cannot service the requested operation."""

    category = "incompatible_state"


class InfrastructureFailure(ResourceError):
    """Report a bounded local infrastructure failure."""

    category = "infrastructure_failure"


class TestFailure(ToolkitError):
    """Report a completed test command whose assertions failed."""

    category = "test_failure"


class IncompleteCleanup(ResourceError):
    """Report cleanup that could not prove exact quiescence."""

    category = "incomplete_cleanup"


class ArchiveRejected(InfrastructureFailure):
    """Report an archive export step that failed for a named, closed failure class."""

    def __init__(self, failure_class: str, detail: str = "", *, reason: str = "unspecified") -> None:
        super().__init__(f"Archive export failed ({failure_class}){': ' + detail if detail else ''}.", reason=reason)
        self.failure_class = failure_class
