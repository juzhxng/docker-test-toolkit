"""Production composition for the public in-process facade."""

import time
from pathlib import Path

from docker_test_toolkit.application import lifecycle
from docker_test_toolkit.application.admission import check_account_format
from docker_test_toolkit.application.api import RunHandle, _open_retained_run, _open_run
from docker_test_toolkit.application.state import account_directory
from docker_test_toolkit.domain.models import RunSpec
from docker_test_toolkit.factories.cli import bind_docker_adapter, build_docker_adapter

_PREPARE_BUDGET_SECONDS = (
    60.0  # ponytail: 60 s preparation budget; ceiling: fixed per artifact; upgrade path: configurable bound
)


def _private_account_directory(_test_state: Path) -> Path:
    """Return the account directory of a private test state, which owns both under one root."""
    return _test_state / "test-admission"


def _check_real_account_format() -> None:
    """Derive the real account and refuse a newer state format (the start check, design 3.4).

    Deriving refuses outside an installation (``toolkit-not-installed``) and through ``current``
    (``toolkit-load-unresolved``), so ``open_retained_run`` is gated like ``open_run`` (invariants I2, I3).
    """
    check_account_format(account_directory())


def open_run(
    spec: RunSpec,
    *,
    _test_state: Path | None = None,
    _prepare_budget_seconds: float = _PREPARE_BUDGET_SECONDS,
) -> RunHandle:
    """Prepare one ungranted run.

    The preparation phase has its own budget for the registry-side waits before the queue wait. The private keywords
    are test seams (a private account and a shortened budget).
    """
    prepare_deadline_ns = time.monotonic_ns() + int(_prepare_budget_seconds * 1_000_000_000)
    if _test_state is None:
        _check_real_account_format()
        lifecycle.configure_docker_factories(build_docker_adapter, bind_docker_adapter)
        return _open_run(spec, docker_factory=build_docker_adapter, prepare_deadline_ns=prepare_deadline_ns)
    return _open_run(
        spec,
        test_state=_test_state,
        test_directory=_private_account_directory(_test_state),
        docker_factory=build_docker_adapter,
        prepare_deadline_ns=prepare_deadline_ns,
    )


def open_retained_run(spec: RunSpec, locator: Path, *, _test_state: Path | None = None) -> RunHandle:
    """Open one exact stopped retained run for explicit resume or release."""
    if _test_state is None:
        _check_real_account_format()
        lifecycle.configure_docker_factories(build_docker_adapter, bind_docker_adapter)
    test_directory = None if _test_state is None else _private_account_directory(_test_state)
    return _open_retained_run(spec, locator, test_directory=test_directory, docker_factory=build_docker_adapter)
