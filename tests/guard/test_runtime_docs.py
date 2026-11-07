"""The docs describe Podman support, its prerequisites, memory, licence and refusals."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).parents[2]
README_TERMS = ("Podman", "Apache-2.0")
OPERATIONS_TERMS = (
    "Podman",
    "Apache-2.0",
    "Buildx",
    "Compose",
    "docker context create podman",
    "docker context use",
    "podman machine set --memory",
    "rootful",
    "podman-mac-helper",
    "podman-helper-unsupported",
    "podman-binary-missing",
    "podman-machine-not-found",
    "docker-archive",
    "supported on macOS: Docker Desktop, Colima, Podman.",
)


def missing_terms(text: str, terms: tuple[str, ...]) -> list[str]:
    """Return the terms a page does not contain."""
    return [term for term in terms if term not in text]


def test_the_docs_describe_podman_support() -> None:
    assert missing_terms((ROOT / "README.md").read_text(), README_TERMS) == []
    assert missing_terms((ROOT / "docs" / "operations.md").read_text(), OPERATIONS_TERMS) == []
    assert "docker-archive" in (ROOT / "docs" / "compatibility.md").read_text()
    assert "podman" in (ROOT / ".agents" / "skills" / "docker-test-toolkit" / "SKILL.md").read_text()


def test_the_guard_reports_a_page_without_the_terms() -> None:
    assert missing_terms("Podman is supported.", ("Podman", "podman machine set --memory")) == [
        "podman machine set --memory"
    ]
