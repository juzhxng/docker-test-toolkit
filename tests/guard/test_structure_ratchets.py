"""Structure ratchets: module size, the adapters' and ports' dependency direction, and fixed sleeps in tests.

Each check compares the tree with a recorded allowance. A new case fails; a recorded case may only shrink, and a
shrunk one fails until its record is lowered, so the record always states today's tree. Raising a record is a
deliberate edit of this file with its reason beside the entry, visible in the diff.
"""

from __future__ import annotations

import ast
import json
from pathlib import Path

ROOT = Path(__file__).parents[2]
PACKAGE = ROOT / "src" / "docker_test_toolkit"
TESTS = ROOT / "tests"

# The ceiling and the modules recorded above it, at their size. A coding harness's after-edit hook reads the same file,
# so an edit that passes the ceiling is reported when it is made, not only here.
SIZES = json.loads((ROOT / ".claude" / "repository.json").read_text())

# Outbound adapters and ports depend on ports and the domain, never on an application service.
# (file, imported module) -> the names it still imports.
INWARD_IMPORTS = {
    ("adapters/outbound/docker.py", "docker_test_toolkit.application.lifecycle"): {
        "ControlError",
        "Deadline",
        "engine_get",
    },
    ("adapters/outbound/podman.py", "docker_test_toolkit.application.lifecycle"): {"Deadline"},
    ("application/ports/docker.py", "docker_test_toolkit.application.lifecycle"): {"Deadline"},
    ("application/ports/podman.py", "docker_test_toolkit.application.lifecycle"): {"Deadline"},
}

# `time.sleep` outside a loop is a fixed wait; inside a loop it spaces a bounded poll and is not counted.
FIXED_SLEEPS = {
    "e2e/test_shared_runs.py": 1,
    "integration/failpoint_child.py": 1,
    "integration/image_lock_holder.py": 1,
    "integration/test_build_cache_store.py": 2,
    "integration/test_capacity_fencing.py": 1,
    "integration/test_docker_adapter.py": 1,
    "integration/test_hold_timeline.py": 1,
    "integration/test_image_lock_dispatch.py": 3,
    "integration/test_native_lifecycle.py": 7,
    "integration/test_project_retirement.py": 1,
    "integration/test_repository_turns.py": 1,
    "integration/test_takeover.py": 1,
    "integration/test_window_overlap.py": 6,
    "unit/test_image_export_service.py": 5,
}

_PORTS = "docker_test_toolkit.application.ports"
_APPLICATION = "docker_test_toolkit.application"


def _oversize_modules(package: Path, ceiling: int) -> dict[str, int]:
    sizes = {
        source.relative_to(ROOT).as_posix(): len(source.read_text().splitlines()) for source in package.rglob("*.py")
    }
    return {name: size for name, size in sizes.items() if size > ceiling}


def _application_service_imports(tree: ast.AST) -> dict[str, set[str]]:
    """Return application modules outside the ports that this tree imports, with the imported names."""
    found: dict[str, set[str]] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module is not None:
            modules = (
                [(f"{node.module}.{alias.name}", alias.name) for alias in node.names]
                if node.module == _APPLICATION
                else [(node.module, alias.name) for alias in node.names]
            )
        elif isinstance(node, ast.Import):
            modules = [(alias.name, alias.name) for alias in node.names]
        else:
            continue
        for module, name in modules:
            inside = module == _APPLICATION or module.startswith(f"{_APPLICATION}.")
            if inside and module != _PORTS and not module.startswith(f"{_PORTS}."):
                found.setdefault(module, set()).add(name)
    return found


def _fixed_sleeps(tree: ast.AST) -> int:
    """Count `time.sleep` calls that no enclosing loop of the same function repeats."""

    def count(node: ast.AST, in_loop: bool) -> int:
        total = 0
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
                looped = False
            else:
                looped = in_loop or isinstance(child, (ast.While, ast.For))
            if (
                isinstance(child, ast.Call)
                and isinstance(child.func, ast.Attribute)
                and child.func.attr == "sleep"
                and isinstance(child.func.value, ast.Name)
                and child.func.value.id == "time"
                and not looped
            ):
                total += 1
            total += count(child, looped)
        return total

    return count(tree, False)


def test_checks_recognize_their_cases() -> None:
    imports = _application_service_imports(
        ast.parse(
            "from docker_test_toolkit.application import recovery\n"
            "from docker_test_toolkit.application.ports.docker import DockerPort\n"
            "def late():\n    from docker_test_toolkit.application.lifecycle import Deadline\n"
        )
    )
    assert imports == {
        "docker_test_toolkit.application.recovery": {"recovery"},
        "docker_test_toolkit.application.lifecycle": {"Deadline"},
    }
    sleeps = "import time\ntime.sleep(1)\nwhile True:\n    time.sleep(0.05)\n    def later():\n        time.sleep(2)\n"
    assert _fixed_sleeps(ast.parse(sleeps)) == 2


def test_no_module_grows_past_its_recorded_size() -> None:
    assert _oversize_modules(PACKAGE, SIZES["module_line_ceiling"]) == SIZES["oversize_modules"]


def test_outbound_adapters_and_ports_import_no_application_service() -> None:
    found = {
        (source.relative_to(PACKAGE).as_posix(), module): names
        for folder in ("adapters/outbound", "application/ports")
        for source in sorted((PACKAGE / folder).rglob("*.py"))
        for module, names in _application_service_imports(ast.parse(source.read_text())).items()
    }
    assert found == INWARD_IMPORTS


def test_tests_add_no_fixed_sleep() -> None:
    found = {
        source.relative_to(TESTS).as_posix(): count
        for source in sorted(TESTS.rglob("*.py"))
        if (count := _fixed_sleeps(ast.parse(source.read_text())))
    }
    assert found == FIXED_SLEEPS
