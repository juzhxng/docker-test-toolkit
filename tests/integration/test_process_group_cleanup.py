"""Process-group cleanup must wait out a transient ``EPERM`` and never report a false clean."""

from __future__ import annotations

import signal

import pytest

from docker_test_toolkit.adapters.outbound.docker import _forward_signal
from tests.integration.unsignalable_group import group_state, unsignalable_group

pytestmark = pytest.mark.integration


def test_forwarded_signal_is_deferred_instead_of_raised_when_no_member_can_be_signalled() -> None:
    with unsignalable_group() as group:
        group.process.wait()
        reported = group_state(group.pgid)

        forwarded = _forward_signal(group.process, signal.SIGCONT)

        assert forwarded is (reported != "unsignalable")
        group.release()
        assert group.wait_gone(10)
        assert _forward_signal(group.process, signal.SIGCONT) is False
