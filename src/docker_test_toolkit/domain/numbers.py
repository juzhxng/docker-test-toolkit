"""The numbers behind a refusal, wait or breach: a closed set of named non-negative integers.

A decision records the operands it compared, so a reader can tell why a run waited or ended without rerunning it.
Only keys of the closed set below are stored, and only integers: no command line, argument or environment value
can enter a record or a message through them.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeGuard

MAX_NUMBERS = 24

_MEMORY = (
    "guest_needed_bytes",
    "daemon_memory_bytes",
    "host_needed_bytes",
    "host_memory_bytes",
    "container_bytes",
    "host_process_bytes",
    "host_reserve_bytes",
)
_EXTERNAL = (*_MEMORY, "external_bytes", "external_containers", "run_host_containers")
_CPU = ("daemon_cpus_needed", "daemon_cpus", "host_threads_needed", "host_cpus")
_DISK = ("free_bytes", "total_bytes", "reserve_bytes")

# (reason, deciding operand) -> the exact keys that decision records; operand "" for a reason with one row.
REASON_KEYS: Mapping[tuple[str, str], frozenset[str]] = {
    ("external-capacity", ""): frozenset(_EXTERNAL),
    ("unknown-external-capacity", ""): frozenset({"unlimited_containers", "external_containers"}),
    ("unsupported-base-capacity", ""): frozenset(_MEMORY),
    ("insufficient-cpu", ""): frozenset(_CPU),
    ("coholder-capacity", "memory"): frozenset(
        {*_EXTERNAL, "coholders", "coholder_container_bytes", "coholder_host_process_bytes"}
    ),
    ("coholder-capacity", "cpu"): frozenset(_CPU),
    ("cpu-pressure", ""): frozenset({"cpu_pressure_permille", "cpu_pressure_limit_permille"}),
    **{
        ("disk-capacity", place): frozenset(
            {*(f"{place}_{key}" for key in _DISK), "estimate_bytes", "other_leases_bytes"}
        )
        for place in ("data_root", "vm_disk_host")
    },
    ("disk-capacity", "private_root"): frozenset({"private_root_free_bytes", "private_root_reserve_bytes"}),
    ("native-headroom", ""): frozenset({"headroom_bytes", "headroom_floor_bytes", "host_memory_bytes"}),
    ("swapout-rate", ""): frozenset({"swapout_bytes_per_s", "swapout_limit_bytes_per_s", "window_ns"}),
    ("admission-swap-growth", ""): frozenset(
        {"swap_growth_bytes", "swap_growth_limit_bytes", "growth_allowance_bytes", "host_swap_used_bytes"}
    ),
    ("invalid-memory-observation", ""): frozenset(
        {"headroom_bytes", "host_memory_bytes", "host_swap_used_bytes", "swapout_bytes"}
    ),
    ("telemetry-discontinuity", ""): frozenset(
        {"host_memory_bytes", "baseline_host_memory_bytes", "swapout_bytes", "baseline_swapout_bytes"}
    ),
    ("container-oom", ""): frozenset({"container_limit_bytes", "oom_events"}),
    # every positional wait records the queue figures (``holder_numbers``), with the account's limit when one is set
    **{
        (reason, operand): frozenset({"holders", "limit"} if operand else {"holders"})
        for reason in (
            "queue-position",
            "exclusive-holder",
            "older-version-entry",
            "other-repository",
            "parallel-limit",
        )
        for operand in ("", "limit")
    },
    # reasons that compare no quantity: an explicit empty row (design K3); such a record stays schema 1
    **{
        (reason, ""): frozenset()
        for reason in (
            "runtime-unqualified",
            "unknown-daemon-relationship",
            "toolkit-not-installed",
            "toolkit-upgraded",
            "coholder-record-unreadable",
            "incomplete-pressure-window",
        )
    },
}
NUMBER_KEYS = frozenset().union(*REASON_KEYS.values())


def non_negative_int(value: object) -> TypeGuard[int]:
    """Whether a record value is a non-negative integer: the only shape a number or a timeline count may have."""
    return type(value) is int and value >= 0  # ``bool`` is an ``int`` subclass and is excluded by the exact type


def schema_for(has_numbers: bool) -> int:
    """The record schema of evidence or a hold timeline: 2 only when it carries numbers, so 1 stays readable by 0.7."""
    return 2 if has_numbers else 1


def numbers_for(reason: str, values: Mapping[str, object], operand: str = "") -> dict[str, int]:
    """Return the keys of ``reason``'s row whose value is a non-negative integer; a reason without a row gives ``{}``.

    A missing or invalid operand (a negative swap figure in an invalid sample) is left out, so the result always
    validates.
    """
    keys = REASON_KEYS.get((reason, operand), frozenset())
    picked: dict[str, int] = {}
    for key in sorted(keys):
        value = values.get(key)
        if non_negative_int(value):
            picked[key] = value
    return picked


def valid_numbers(value: object) -> bool:
    """Return whether ``value`` is a non-empty object of at most ``MAX_NUMBERS`` known keys with integer values."""
    return (
        isinstance(value, dict)
        and 0 < len(value) <= MAX_NUMBERS
        and all(key in NUMBER_KEYS and non_negative_int(item) for key, item in value.items())
    )


def render_numbers(numbers: Mapping[str, int]) -> str:
    """Render ``name=value`` pairs by name; a ``_bytes`` key loses its suffix and shows GiB with two decimals."""
    parts = []
    for key in sorted(numbers):
        value = numbers[key]
        if key.endswith("_bytes"):
            parts.append(f"{key.removesuffix('_bytes')}={value / 1024**3:.2f}GiB")
        else:
            parts.append(f"{key}={value}")
    return " ".join(parts)


def oom_numbers(record: Mapping[str, Any]) -> dict[str, int]:
    """The OOM figures a record holds: the largest limit among its containers that recorded one, and their events.

    Managed containers keep their limit beside their observation; owned containers keep it in their evidence
    (``memory_bytes``, recorded at promotion; absent in records of versions before 0.8.0). A record without such a
    container gives ``{}``.
    """
    limits, events = [], 0
    for resource in _items(record, "resources"):
        observation = resource.get("observation")
        if isinstance(observation, Mapping) and (
            observation.get("oom_killed") is True or _positive(observation, "oom_events")
        ):
            events += observation["oom_events"] if non_negative_int(observation.get("oom_events")) else 0
            if non_negative_int(resource.get("memory_bytes")):
                limits.append(resource["memory_bytes"])
    for resource in _items(record, "owned_resources"):
        evidence = resource.get("evidence")
        control = evidence.get("control") if isinstance(evidence, Mapping) else None
        if isinstance(control, Mapping) and (control.get("oom_killed") is True or _positive(control, "oom_events")):
            events += control["oom_events"] if non_negative_int(control.get("oom_events")) else 0
            if isinstance(evidence, Mapping) and _positive(evidence, "memory_bytes"):
                limits.append(evidence["memory_bytes"])
    found = {"oom_events": events} if events or limits else {}
    return numbers_for("container-oom", {**found, "container_limit_bytes": max(limits)} if limits else found)


def _items(record: Mapping[str, Any], key: str) -> list[Mapping[str, Any]]:
    containers = record.get(key, {}).get("containers", {}) if isinstance(record.get(key), Mapping) else {}
    return (
        [item for item in containers.values() if isinstance(item, Mapping)] if isinstance(containers, Mapping) else []
    )


def _positive(observation: Mapping[str, Any], key: str) -> bool:
    return non_negative_int(observation.get(key)) and observation[key] > 0
