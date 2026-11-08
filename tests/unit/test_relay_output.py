"""The relay codec: a captured mutation's stream as the caller sees it, and the stderr tail named in an error."""

from __future__ import annotations

import pytest

from docker_test_toolkit.adapters.outbound.docker import RELAY_STREAM_BYTES, _relay_stream
from docker_test_toolkit.domain.errors import STDERR_TAIL_LIMIT, stderr_tail

HALF = RELAY_STREAM_BYTES // 2


@pytest.mark.parametrize(
    "invalid",
    (
        128 * 1024,  # under half the budget raw, over the budget once replaced
        HALF + 1000,  # between half and the whole budget raw
        RELAY_STREAM_BYTES,  # exactly the budget raw
        3 * RELAY_STREAM_BYTES,  # over the budget raw
    ),
)
def test_a_cut_stream_keeps_its_true_start_and_end(invalid: int) -> None:
    relayed = _relay_stream(b"BEGIN" + b"\xff" * invalid + b"END", "stderr")

    assert relayed.startswith("BEGIN�") and relayed.endswith("�END")
    head, marker, tail = relayed.split("\n")
    assert marker.startswith(f"[docker-test-toolkit: stderr cut from {invalid + 8} bytes")
    assert len(head.encode()) <= HALF and len(tail.encode()) <= HALF


def test_a_stream_within_the_budget_is_whole_and_replaced() -> None:
    assert _relay_stream(b"ok \xff\n", "stdout") == "ok �\n"


def test_the_stderr_tail_is_the_flattened_end_of_a_large_partial_stream() -> None:
    tail = stderr_tail(b"\xff" * (64 * 1024 * 1024) + b"\r\nfatal:\ncannot reach registry\n")

    assert tail.endswith("fatal: cannot reach registry") and "\n" not in tail and "\r" not in tail
    assert len(tail) == STDERR_TAIL_LIMIT


@pytest.mark.parametrize("stderr", (None, b"", "", b"\n\n"))
def test_an_empty_stderr_has_no_tail(stderr: bytes | str | None) -> None:
    assert stderr_tail(stderr) == ""
