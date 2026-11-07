"""A test-only lock-order checker driven by ``recovery.configure_lock_observer`` events.

Blocking acquisitions must take a lock of higher rank than every lock already held: installation (I) < image lock (K)
< build-cache platform anchor (C) < project (P) < run (R) < registry (G) < metadata (M). ``capacity`` is exempt because
it is only ever taken without waiting, and it never counts as held in the comparison; any acquisition that does not wait
(``wait=False``) cannot deadlock and is exempt too, while ``bounded`` never exempts a wait: a deadline-polled attempt is
still a wait. One documented exception: K may be waited for while holding only ``capacity`` and/or a
*shared* project lock (a shared run's dispatcher and recoverer). A lock name outside the table is itself a violation,
so a new lock cannot slip in unranked.
"""

from __future__ import annotations

import re
import threading

INSTALL, IMAGES, CACHE, PROJECT, RUN, REGISTRY, METADATA = -2, -1, 0, 1, 2, 3, 4
GUARD_RANKS = {"project": PROJECT, "run": RUN, "registry": REGISTRY, "metadata": METADATA}
_PROJECT_ID = re.compile(r"[0-9a-f]{64}\Z")
_CACHE_ANCHOR = re.compile(r"[a-z0-9_]+-[a-z0-9_]+")  # a platform anchor such as ``linux-arm64``
_RUN_LOCK = re.compile(r"run-[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}\Z")


def rank(name: str) -> int | None:
    """Return the rank of a lock name, or None for a name outside the table (``capacity`` is handled by the caller)."""
    if name == "install":
        return INSTALL
    if name == "images":
        return IMAGES
    if name == "registry":
        return REGISTRY
    if name == "metadata":
        return METADATA
    if _RUN_LOCK.fullmatch(name):
        return RUN
    if _CACHE_ANCHOR.fullmatch(name):
        return CACHE
    if _PROJECT_ID.fullmatch(name):
        return PROJECT
    return None


class LockOrderChecker:
    """Observer callback keeping a per-thread stack of held locks and the violations it saw."""

    def __init__(self) -> None:
        self._local = threading.local()
        self.violations: list[str] = []
        self.events: list[tuple[str, str, bool, bool, bool]] = []
        self._guard = threading.Lock()

    def _stack(self) -> list[tuple[str, bool]]:
        if not hasattr(self._local, "stack"):
            self._local.stack = []
        return self._local.stack

    def __call__(self, event: str, name: str, shared: bool, wait: bool, bounded: bool) -> None:
        with self._guard:
            self.events.append((event, name, shared, wait, bounded))
        stack = self._stack()
        if event == "release":
            for index in range(len(stack) - 1, -1, -1):
                if stack[index][0] == name:
                    del stack[index]
                    break
            return
        if name != "capacity":
            new = rank(name)
            if new is None:
                self.violations.append(f"unranked lock {name!r}")
            elif wait:
                held = [
                    other
                    for other, other_shared in stack
                    if other != "capacity"
                    and (rank(other) or 0) > new
                    and not (name == "images" and other_shared and rank(other) == PROJECT)
                ]
                if held:
                    self.violations.append(f"blocking {name!r} (rank {new}) while holding {held}")
        stack.append((name, shared))

    def acquired(self, name: str) -> list[tuple[str, str, bool, bool, bool]]:
        """Return the acquire events observed for one lock name."""
        return [event for event in self.events if event[0] == "acquire" and event[1] == name]
