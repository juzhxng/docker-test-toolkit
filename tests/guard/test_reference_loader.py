"""The reference loader is copied verbatim into projects and never changes there.

It must therefore pass the strictest checks a project applies to its own code, and every published copy of its text
 must equal the file.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parents[2]
LOADER = ROOT / "docs" / "loader" / "docker_test_toolkit_loader.py"
_RUFF = (sys.executable, "-m", "ruff", "check", "--no-cache", "--select", "C901")
ADOPTERS = ROOT / "docs/adopters.md"


def test_loader_passes_strict_mypy() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "mypy", "--strict", "--no-incremental", str(LOADER)],
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr


def test_loader_passes_the_project_complexity_bound() -> None:
    result = subprocess.run(
        [*_RUFF, "--config", "lint.mccabe.max-complexity=10", str(LOADER)],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr  # adopters may lint at 10


def test_the_adopter_guide_carries_the_loader_verbatim() -> None:
    blocks = re.findall(r"```python\n(.*?)```", ADOPTERS.read_text(), flags=re.S)
    assert [block for block in blocks if "def load_toolkit" in block] == [LOADER.read_text()]
