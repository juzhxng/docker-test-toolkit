import ast
from pathlib import Path


def _contains_pytest_skip(tree: ast.AST) -> bool:
    forbidden = {"skip", "skipif", "importorskip"}
    pytest_names = {"pytest"}
    mark_names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            pytest_names.update(alias.asname or alias.name for alias in node.names if alias.name == "pytest")
        elif isinstance(node, ast.ImportFrom) and node.module == "pytest":
            if any(alias.name in forbidden for alias in node.names):
                return True
            mark_names.update(alias.asname or alias.name for alias in node.names if alias.name == "mark")
    for node in ast.walk(tree):
        if not isinstance(node, ast.Attribute) or node.attr not in forbidden:
            continue
        owner = node.value
        while isinstance(owner, ast.Attribute):
            owner = owner.value
        if isinstance(owner, ast.Name) and owner.id in pytest_names | mark_names:
            return True
    return False


def test_skip_guard_recognizes_decorators_and_import_aliases() -> None:
    for source in (
        "@pytest.mark.skipif(True, reason='fixture')\ndef example(): pass\n",
        "from pytest import skip as omit\nomit('fixture')\n",
        "import pytest as pt\npt.importorskip('fixture')\n",
    ):
        assert _contains_pytest_skip(ast.parse(source))


def test_active_tests_do_not_skip() -> None:
    tests = Path(__file__).parents[1]
    for source in tests.rglob("test_*.py"):
        tree = ast.parse(source.read_text())
        assert not _contains_pytest_skip(tree), source
