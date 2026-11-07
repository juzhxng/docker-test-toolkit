from __future__ import annotations

import ast
from pathlib import Path

PACKAGE = Path(__file__).parents[2] / "src/docker_test_toolkit"


def _imported_modules(tree: ast.AST) -> set[str]:
    modules = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            modules.add(node.module)
            modules.update(f"{node.module}.{alias.name}" for alias in node.names if alias.name != "*")
    return modules


def _is_broad_exception_type(node: ast.expr | None) -> bool:
    if node is None:
        return True
    if isinstance(node, (ast.Name, ast.Attribute)):
        return (
            node.id in {"Exception", "BaseException"}
            if isinstance(node, ast.Name)
            else node.attr
            in {
                "Exception",
                "BaseException",
            }
        )
    return isinstance(node, ast.Tuple) and any(_is_broad_exception_type(item) for item in node.elts)


def test_package_has_only_the_five_architectural_roots() -> None:
    roots = {path.name for path in PACKAGE.iterdir() if path.is_dir() and path.name != "__pycache__"}
    assert roots == {"adapters", "application", "config", "domain", "factories"}


def test_domain_does_not_import_outward() -> None:
    for package, namespace in ((PACKAGE, "docker_test_toolkit"),):
        forbidden = {f"{namespace}.{root}" for root in ("adapters", "application", "config", "factories")}
        for source in (package / "domain").glob("*.py"):
            imports = _imported_modules(ast.parse(source.read_text()))
            assert not any(
                name == blocked or name.startswith(f"{blocked}.") for name in imports for blocked in forbidden
            )


def test_import_discovery_covers_package_submodule_aliases() -> None:
    tree = ast.parse("from docker_test_toolkit import adapters, factories as composition")
    assert _imported_modules(tree) >= {
        "docker_test_toolkit.adapters",
        "docker_test_toolkit.factories",
    }


def test_application_does_not_import_adapters_or_factories() -> None:
    for package, namespace in ((PACKAGE, "docker_test_toolkit"),):
        forbidden = {f"{namespace}.adapters", f"{namespace}.factories"}
        for source in (package / "application").rglob("*.py"):
            imports = _imported_modules(ast.parse(source.read_text()))
            assert not any(
                name == blocked or name.startswith(f"{blocked}.") for name in imports for blocked in forbidden
            )


def test_batch_coordinator_has_no_lifecycle_process_or_persistence_dependency() -> None:
    imports = _imported_modules(ast.parse((PACKAGE / "application/batch.py").read_text()))
    forbidden = {
        "docker_test_toolkit.application.lifecycle",
        "docker_test_toolkit.application.ports.docker",
        "docker_test_toolkit.application.recovery",
        "docker_test_toolkit.application.state",
        "os",
        "pathlib",
        "subprocess",
    }
    assert imports.isdisjoint(forbidden)


def test_batch_protocol_adds_no_durable_record() -> None:
    durable_sources = (PACKAGE / "domain/records.py",)
    for source in durable_sources:
        tree = ast.parse(source.read_text())
        assert not any(isinstance(node, ast.ClassDef) and "batch" in node.name.lower() for node in ast.walk(tree)), (
            source
        )
        assert not any(
            isinstance(node, ast.Dict)
            and any(isinstance(key, ast.Constant) and key.value == "batch" for key in node.keys)
            for node in ast.walk(tree)
        ), source


def test_public_production_contracts_have_docstrings() -> None:
    missing = []
    for package in (PACKAGE,):
        for source in package.rglob("*.py"):
            tree = ast.parse(source.read_text())
            missing.extend(
                f"{package.name}/{source.relative_to(package)}:{node.lineno}:{node.name}"
                for node in ast.walk(tree)
                if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
                and not node.name.startswith("_")
                and ast.get_docstring(node) is None
            )
    assert not missing


def test_broad_catches_are_limited_to_the_reraising_cleanup_boundaries() -> None:
    expected = {"managed_run", "resume_run", "launch_python_consumer", "_guarded", "_run_takeover_worker"}
    observed = set()
    for source in PACKAGE.rglob("*.py"):
        tree = ast.parse(source.read_text())
        parents = {child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}
        for handler in (
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.ExceptHandler) and _is_broad_exception_type(node.type)
        ):
            owner = parents.get(handler)
            while owner is not None and not isinstance(owner, (ast.FunctionDef, ast.AsyncFunctionDef)):
                owner = parents.get(owner)
            assert owner is not None
            observed.add(owner.name)
            assert any(isinstance(node, ast.Raise) and node.exc is None for node in ast.walk(handler))
    assert observed == expected
