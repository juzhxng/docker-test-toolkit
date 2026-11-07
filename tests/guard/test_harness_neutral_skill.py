"""The toolkit ships one harness-neutral skill: any agent harness can load it, and no harness has a copy."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).parents[2]
SKILL = ROOT / ".agents" / "skills" / "docker-test-toolkit" / "SKILL.md"
_NAME = re.compile(r"^[a-z0-9]+(-[a-z0-9]+)*$")
_RELATIVE_LINK = re.compile(r"\]\((?![a-z][a-z0-9+.-]*:|#)[^)]*\)")


def _frontmatter_and_body(text: str) -> tuple[dict[str, object], str]:
    assert text.startswith("---\n"), "the skill must open with its frontmatter"
    _, frontmatter, body = text.split("---\n", 2)
    loaded = yaml.safe_load(frontmatter)
    assert isinstance(loaded, dict)
    return loaded, body


def _check_frontmatter(text: str, directory: str) -> None:
    frontmatter, _ = _frontmatter_and_body(text)
    assert set(frontmatter) == {"name", "description"}
    assert frontmatter["name"] == directory
    assert isinstance(frontmatter["name"], str) and _NAME.fullmatch(frontmatter["name"])
    assert isinstance(frontmatter["description"], str) and 1 <= len(frontmatter["description"]) <= 1024


def test_canonical_skill_frontmatter_is_spec_valid() -> None:
    _check_frontmatter(SKILL.read_text(encoding="utf-8"), SKILL.parent.name)


def test_canonical_skill_has_no_relative_markdown_links() -> None:
    _, body = _frontmatter_and_body(SKILL.read_text(encoding="utf-8"))

    assert _RELATIVE_LINK.findall(body) == []
    assert _RELATIVE_LINK.findall("see [Adopters](docs/adopters.md) and [x](../x)") == [
        "](docs/adopters.md)",
        "](../x)",
    ]


def test_repository_tracks_no_harness_specific_skill_copy() -> None:
    tracked = subprocess.run(
        ["git", "ls-files"], cwd=ROOT, capture_output=True, text=True, check=True
    ).stdout.splitlines()

    harness_skills = [
        path for path in tracked if path.startswith((".codex/", ".claude/", ".opencode/")) and "/skills/" in path
    ]

    assert harness_skills == []


def test_agents_pointer_resolves() -> None:
    pointer = re.search(r"`(\.agents/skills/[^`]+/SKILL\.md)`", (ROOT / "AGENTS.md").read_text(encoding="utf-8"))

    assert pointer is not None
    assert (ROOT / pointer.group(1)) == SKILL
    assert SKILL.is_file()


@pytest.mark.parametrize(
    "text",
    [
        "---\nname: docker-test-toolkit\ndescription:\n---\nbody\n",
        "---\nname: docker-test-toolkit\ndescription: 7\n---\nbody\n",
        "intro\n---\nname: docker-test-toolkit\ndescription: ok\n---\nbody\n",
    ],
)
def test_frontmatter_check_refuses_invalid_skills(text: str) -> None:
    with pytest.raises(AssertionError):
        _check_frontmatter(text, "docker-test-toolkit")
