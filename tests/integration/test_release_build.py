"""The release build writes exactly one archive: ``docker-test-toolkit.pyz``."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration


def test_build_writes_exactly_the_archive(synthetic_distribution_repository: Path, tmp_path: Path) -> None:
    output = tmp_path / "dist"

    built = subprocess.run(
        [
            sys.executable,
            str(synthetic_distribution_repository / "scripts/build_zipapp.py"),
            "build",
            "--repository",
            str(synthetic_distribution_repository),
            "--output-directory",
            str(output),
        ],
        check=True,
        capture_output=True,
    )

    assert sorted(os.listdir(output)) == ["docker-test-toolkit.pyz"]
    identity = json.loads(built.stdout)
    assert set(identity) == {"protocols", "sha256", "size", "version"}
    assert identity["sha256"] == hashlib.sha256((output / "docker-test-toolkit.pyz").read_bytes()).hexdigest()
    with zipfile.ZipFile(output / "docker-test-toolkit.pyz") as archive:
        tops = {name.split("/", 1)[0] for name in archive.namelist()}
    assert tops == {"__main__.py", "docker_test_toolkit"}
