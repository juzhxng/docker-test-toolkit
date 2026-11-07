"""Every daemon-identity read is a reviewed one.

On Podman the compat ``/info`` ``ID`` changes on every call, so it may be read only where identity is decided: the
Docker adapter and the one helper that keeps today's read on Docker. Every other identity read goes through the
adapter's verification, and the set of its consumers is frozen here so a new one is a reviewed change.
"""

from __future__ import annotations

import ast
from pathlib import Path

SOURCE = Path(__file__).parents[2] / "src" / "docker_test_toolkit"
_IDENTITY_TEMPLATE = "{{.ID}}"
_IDENTITY_CALLS = frozenset({"observe_daemon", "daemon_identity", "_daemon_identity"})
CONSUMERS = frozenset(
    {
        "application/lifecycle.py:_cleanup_compose_resources",
        "application/lifecycle.py:_control_owned_containers",
        "application/lifecycle.py:_disk_inputs",
        "application/lifecycle.py:_drain_pending",
        "application/lifecycle.py:_identified_info",
        "application/lifecycle.py:_publish_grant",
        "application/lifecycle.py:_reconcile_pending_compose",
        "application/lifecycle.py:_reconcile_pending_container_control",
        "application/lifecycle.py:_small_vm_detail",
        "application/lifecycle.py:_stop_compose_resources",
        "application/lifecycle.py:_wait_resumed_containers",
        "application/lifecycle.py:control_compose_containers",
        "application/lifecycle.py:materialize_docker_authority",
        "application/probe.py:ensure_on_current_daemon",
        "application/recovery.py:_cleanup",
        "application/recovery.py:bind_daemon",
        "application/recovery.py:daemon_identity",
        "application/recovery.py:inventory",
        "application/retention.py:_retain_builder_volumes",
        "application/retention.py:_retain_pending_images",
        "application/retention.py:_retain_probe_containers",
        "factories/testing.py:open_run",
    }
)


def template_reads(source: str, relative: str) -> list[str]:
    """Return ``path:line`` of every ``{{.ID}}`` constant outside the Docker adapter and ``daemon_identity``."""
    if relative == "adapters/outbound/docker.py":
        return []
    tree = ast.parse(source)
    allowed: set[int] = set()
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.FunctionDef)
            and node.name == "daemon_identity"
            and relative == "application/recovery.py"
        ):
            allowed.update(range(node.lineno, (node.end_lineno or node.lineno) + 1))
    return [
        f"{relative}:{node.lineno}"
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and _IDENTITY_TEMPLATE in node.value
        and node.lineno not in allowed
    ]


def identity_consumers(source: str, relative: str) -> set[str]:
    """Return ``path:function`` of every function calling an identity read."""
    owners = set()
    for function in ast.walk(ast.parse(source)):
        if not isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for node in ast.walk(function):
            if not isinstance(node, ast.Call):
                continue
            name = getattr(node.func, "attr", None) or getattr(node.func, "id", None)
            if name in _IDENTITY_CALLS:
                owners.add(f"{relative}:{function.name}")
    return owners


def _sources() -> list[tuple[str, str]]:
    return [(path.relative_to(SOURCE).as_posix(), path.read_text()) for path in sorted(SOURCE.rglob("*.py"))]


def test_the_info_id_template_is_read_only_where_identity_is_decided() -> None:
    found = [read for relative, source in _sources() for read in template_reads(source, relative)]

    assert found == []


def test_every_identity_consumer_is_listed() -> None:
    found = set().union(
        *(
            identity_consumers(source, relative)
            for relative, source in _sources()
            if not relative.startswith("adapters/")
        )
    )

    assert found == CONSUMERS, f"new: {sorted(found - CONSUMERS)}; gone: {sorted(CONSUMERS - found)}"


def test_the_guards_report_a_planted_read() -> None:
    planted = 'def leftover(docker):\n    return docker.call("info", "--format", "{{.ID}}")\n'
    consumer = "def reader(docker):\n    return docker.observe_daemon().daemon_id\n"

    assert template_reads(planted, "application/lifecycle.py") == ["application/lifecycle.py:2"]
    assert identity_consumers(consumer, "application/lifecycle.py") == {"application/lifecycle.py:reader"}
