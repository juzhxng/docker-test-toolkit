"""No documentation page decides sharing by a machine size: sizes are derived from live capacity."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).parents[2]
_SIZING = re.compile(
    r"\b\d+(?:\.\d+)?\s*GB\s+(?:machine|mac|host)s?\b"
    r"|\b\d+\s+runs?\s+fits?\b"
    r"|\bfits?\s+on\s+an?\s+\d+(?:\.\d+)?\s*GB\b",
    re.IGNORECASE,
)


def sizing_phrases(text: str) -> list[str]:
    """Return every machine-size sizing phrase in ``text``."""
    return [match.group(0) for match in _SIZING.finditer(text)]


def _pages() -> list[Path]:
    docs = ROOT / "docs"
    pages = list(docs.rglob("*.md"))
    return [*sorted(pages), ROOT / ".agents" / "skills" / "docker-test-toolkit" / "SKILL.md"]


def test_docs_and_skill_name_no_machine_size() -> None:
    found = {str(page.relative_to(ROOT)): sizing_phrases(page.read_text()) for page in _pages()}

    assert {page: phrases for page, phrases in found.items() if phrases} == {}
    assert len(found) > 3  # the scan reached the pages, not an empty set


def test_guard_refuses_planted_sizing_line(tmp_path: Path) -> None:
    planted = tmp_path / "planted.md"
    planted.write_text("Two runs fit on a 16 GB Mac.\nA 32 GB host takes three; 2 runs fit there.\n")

    assert sizing_phrases(planted.read_text()) == ["fit on a 16 GB", "32 GB host", "2 runs fit"]


# --- sizing constants: clamps of a live figure, floors, or per-run and temporal values -------------------

_SIZING_MODULES = (
    "src/docker_test_toolkit/domain/policy.py",
    "src/docker_test_toolkit/application/probe.py",
    "src/docker_test_toolkit/application/retention.py",
)
_CONSTANT = re.compile(r"^([A-Z][A-Z0-9_]*_(?:BYTES|NS|S))\s*=", re.MULTILINE)
_OUT_OF_SCOPE = frozenset({"BUILDER_MEMORY_BYTES", "UNBOUND_CLAIM_NS"})  # older: a calibrated envelope, a timeout
_DERIVED = ("cache_cap_bytes", "archive_cap_bytes", "derive_reserve")  # clamp functions of a live filesystem total


def _definition(text: str, start: int) -> str:
    """Return a constant's definition from ``start``: its line, and the following ones while a parenthesis is open."""
    lines = text[start:].splitlines()
    taken = [lines[0]]
    while taken[-1].count("(") + sum(line.count("(") for line in taken[:-1]) > sum(line.count(")") for line in taken):
        taken.append(lines[len(taken)])
    return "\n".join(taken)


def unmarked_constants(text: str) -> list[str]:
    """Return the size and time constants defined without a ``NOTE:`` note on their definition."""
    return [
        match.group(1)
        for match in _CONSTANT.finditer(text)
        if match.group(1) not in _OUT_OF_SCOPE and "NOTE:" not in _definition(text, match.start())
    ]


def test_size_constants_are_clamps_floors_or_per_run_values() -> None:
    texts = {module: (ROOT / module).read_text() for module in _SIZING_MODULES}
    named = {match.group(1) for text in texts.values() for match in _CONSTANT.finditer(text)}

    assert {module: unmarked_constants(text) for module, text in texts.items() if unmarked_constants(text)} == {}
    assert {"DISK_FLOOR_BYTES", "PRIVATE_ROOT_FLOOR_BYTES", "PROBE_CACHE_NS", "RETENTION_BUDGET_S"} <= named
    policy = texts["src/docker_test_toolkit/domain/policy.py"]
    for function in _DERIVED:
        body = policy[policy.index(f"def {function}(") :]
        assert "NOTE:" in body[: body.index('"""', body.index('"""') + 3)], function


def test_the_constant_guard_refuses_a_planted_machine_size() -> None:
    planted = (ROOT / _SIZING_MODULES[0]).read_text() + "\nFOO_BYTES = 16 * _GIB\n"

    assert unmarked_constants(planted) == ["FOO_BYTES"]
