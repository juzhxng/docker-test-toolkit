import tomllib
from pathlib import Path

from docker_test_toolkit.domain.models import SUPPORTED_PROTOCOLS, TOOLKIT_VERSION

PACKAGE = Path(__file__).parents[2] / "src/docker_test_toolkit"


def test_runtime_version_matches_project_metadata() -> None:
    metadata = tomllib.loads((PACKAGE.parents[1] / "pyproject.toml").read_text())
    assert metadata["project"]["version"] == TOOLKIT_VERSION


def test_current_release_has_the_exact_batch_protocol_identity() -> None:
    assert dict(SUPPORTED_PROTOCOLS) == {
        "batch": 2,
        "cli": 1,
        "queue": 2,
        "readiness": 3,
        "receipt": 1,
        "registry": 2,
        "run": 1,
    }


def test_production_logic_names_no_harness() -> None:
    for source in PACKAGE.rglob("*.py"):
        text = source.read_text().lower()
        assert "codex" not in text, source
        assert "session" + "-cleanup" not in text, source
