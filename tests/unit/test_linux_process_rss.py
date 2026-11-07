import pytest

from docker_test_toolkit.application.lifecycle import _linux_status_rss
from docker_test_toolkit.domain.errors import ResourceError

_RUNNING = (
    "Name:\tpytest\nState:\tR (running)\nVmPeak:\t  20480 kB\nVmLck:\t       0 kB\nVmRSS:\t   1536 kB\n"
    "RssAnon:\t    1024 kB\nRssFile:\t     448 kB\nRssShmem:\t      64 kB\nThreads:\t1\n"
)
# A task that has begun exiting releases its address space before it turns into a zombie: no Vm* rows remain.
_EXITING = "Name:\tpytest\nState:\tR (running)\nThreads:\t1\n"
# Kernels before 4.5 print no Rss* rows.
_BEFORE_RSS_ROWS = "Name:\tpytest\nState:\tS (sleeping)\nVmLck:\t       0 kB\nVmRSS:\t   1536 kB\nThreads:\t1\n"


def test_resident_memory_is_the_anonymous_and_shared_memory_in_bytes() -> None:
    # File-backed pages (mapped models and libraries) are reclaimable page cache, which the host read already counts
    # as available; charging them to the run as well counted them twice (an e2e run on the CI host).
    assert _linux_status_rss(_RUNNING) == (1024 + 64) * 1024


def test_locked_pages_are_charged_because_they_cannot_be_reclaimed() -> None:
    # Locked file pages stay in RssFile but are unevictable; locked anonymous pages are then charged twice, which
    # over-charges only a process that locks memory.
    assert _linux_status_rss(_RUNNING.replace("VmLck:\t       0 kB", "VmLck:\t     256 kB")) == (1024 + 64 + 256) * 1024


def test_a_fully_swapped_out_task_reports_zero_from_its_rss_rows() -> None:
    swapped = _RUNNING.replace("1536 kB", "0 kB").replace("1024 kB", "0 kB").replace("448 kB", "0 kB")
    assert _linux_status_rss(swapped.replace("64 kB", "0 kB")) == 0


def test_a_task_without_an_address_space_holds_no_resident_memory() -> None:
    assert _linux_status_rss(_EXITING) == 0


def test_a_kernel_without_rss_rows_is_charged_its_whole_resident_set() -> None:
    assert _linux_status_rss(_BEFORE_RSS_ROWS) == 1536 * 1024


@pytest.mark.parametrize(
    "status",
    [
        "",
        "Name:\tx\nThreads:\t1\n",  # neither memory rows nor State: not a process status document
        _RUNNING.replace("1024 kB", "1024 MB"),
        _RUNNING.replace("1024 kB", "lots kB"),
        _RUNNING.replace("    64 kB", "    6.4 kB"),
        _RUNNING + "RssAnon:\t2 kB\n",
        _RUNNING + "RssShmem:\t2 kB\n",
        _RUNNING + "VmLck:\t2 kB\n",
        _RUNNING.replace("RssAnon:\t    1024 kB\n", ""),  # one Rss row without the other
        _RUNNING.replace("RssShmem:\t      64 kB\n", ""),
        _RUNNING.replace("VmLck:\t       0 kB\n", ""),
        _BEFORE_RSS_ROWS.replace("VmRSS:\t   1536 kB\n", ""),
        # A memory-bearing status needs exactly one recognizable State row too.
        _RUNNING.replace("State:\tR (running)\n", ""),
        _RUNNING.replace("R (running)", "Q (unknown)"),
        _RUNNING + "State:\tZ (zombie)\n",
        # Rss rows without Vm rows contradict a released address space too.
        "Name:\tx\nState:\tR (running)\nRssAnon:\t1 kB\nRssShmem:\t0 kB\n",
        # Memory rows without the Rss rows contradict a released address space.
        "Name:\tx\nState:\tR (running)\nVmSize:\t20480 kB\nVmHWM:\t1536 kB\n",
        "Name:\tx\nState:\n",
        "Name:\tx\nState:\tQ (unknown)\n",
        "Name:\tx\nState:\tR (running)\nState:\tZ (zombie)\n",
    ],
)
def test_an_unrecognized_status_still_refuses(status: str) -> None:
    with pytest.raises(ResourceError, match="unsupported Linux format"):
        _linux_status_rss(status)
