"""Stable CLI response envelopes and secret-safe diagnostics."""

from __future__ import annotations

import re
from typing import Any

from docker_test_toolkit.domain.errors import (
    REASONS,
    IncompleteCleanup,
    InfrastructureFailure,
    InvalidInput,
    ResourceBusy,
    ResourceError,
    TestFailure,
)

CLI_PROTOCOL = "docker-test-toolkit.cli/1"
_URL_CREDENTIAL = re.compile(r"(?P<scheme>https?://)[^/@\s]+@", re.IGNORECASE)
_QUERY_SECRET = re.compile(r"(?i)(token|key|secret|password)=([^&\s]+)")
_AUTHORIZATION = re.compile(r"(?i)(authorization\s*:\s*(?:bearer|basic)\s+)[^\s]+")


def _safe_message(error: BaseException) -> str:
    # Redact before cutting: a cut can split a credential past recognition, and redaction can lengthen the text.
    message = str(error).replace("\r", " ").replace("\n", " ")
    message = _URL_CREDENTIAL.sub(r"\g<scheme><redacted>@", message)
    message = _QUERY_SECRET.sub(r"\1=<redacted>", message)
    return _AUTHORIZATION.sub(r"\1<redacted>", message)[:512]


def _error_category(error: BaseException, *, cleanup: bool = False) -> tuple[str, int, str]:
    if cleanup or isinstance(error, IncompleteCleanup):
        return "incomplete_cleanup", 7, "recover"
    if isinstance(error, InvalidInput):
        return "invalid_input", 2, "check_input"
    if isinstance(error, ResourceBusy):
        return "contention", 4, "inspect_status"
    if isinstance(error, InfrastructureFailure):
        return "infrastructure_failure", 5, "inspect_status"
    if isinstance(error, TestFailure):
        return "test_failure", 6, "inspect_status"
    if isinstance(error, ResourceError):
        return "incompatible_state", 3, "inspect_status"
    return "infrastructure_failure", 5, "inspect_status"


def _error_payload(error: BaseException, *, cleanup: bool = False) -> tuple[dict[str, Any], int]:
    category, exit_code, next_action = _error_category(error, cleanup=cleanup)
    reason = getattr(error, "reason", "unspecified")
    return {
        "category": category,
        "message": _safe_message(error),
        "context": {
            **getattr(error, "details", {}),
            "next_action": next_action,
            "reason": reason if reason in REASONS else "unspecified",
        },
    }, exit_code


def success_envelope(command: str, result: Any, *, cleanup: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return one closed successful version-1 response envelope."""
    return {
        "protocol": CLI_PROTOCOL,
        "command": command,
        "ok": True,
        "result": result,
        "cleanup": cleanup,
    }


def error_envelope(
    command: str,
    error: BaseException,
    *,
    cleanup: BaseException | None = None,
) -> tuple[dict[str, Any], int]:
    """Return a closed error envelope while preserving the primary exit group."""
    payload, exit_code = _error_payload(error)
    cleanup_payload = None
    if cleanup is not None:
        cleanup_error, _ = _error_payload(cleanup, cleanup=True)
        cleanup_payload = {"ok": False, "error": cleanup_error}
    return {
        "protocol": CLI_PROTOCOL,
        "command": command,
        "ok": False,
        "error": payload,
        "cleanup": cleanup_payload,
    }, exit_code
