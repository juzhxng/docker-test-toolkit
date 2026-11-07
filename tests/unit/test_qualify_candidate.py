"""The CI-host qualification checks its kit before it installs anything."""

from __future__ import annotations

import hashlib
import os
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).parents[2] / "scripts" / "qualify_candidate.sh"
NAMES = ("docker-test-toolkit.pyz", "toolkit.bundle")


def _kit(tmp_path: Path, bundle: bytes | None = None, listed: tuple[str, ...] = NAMES) -> Path:
    kit = tmp_path / "kit"
    kit.mkdir()
    (kit / "docker-test-toolkit.pyz").write_bytes(b"docker-test-toolkit.pyz bytes")
    (kit / "toolkit.bundle").write_bytes(bundle if bundle is not None else b"toolkit.bundle bytes")
    lines = [f"{hashlib.sha256((kit / name).read_bytes()).hexdigest()}  {name}\n" for name in listed]
    (kit / "SHA256SUMS").write_text("".join(lines))
    return kit


def _bundle(tmp_path: Path) -> tuple[bytes, str]:
    repository = tmp_path / "source"
    repository.mkdir()

    def git(*args: str) -> str:
        return subprocess.run(
            ["git", "-C", str(repository), *args], check=True, capture_output=True, text=True
        ).stdout.strip()

    git("init", "-q", "-b", "main")
    (repository / "a.txt").write_text("a\n")
    git("add", "a.txt")
    git("-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-q", "-m", "a")
    git("bundle", "create", str(tmp_path / "toolkit.bundle"), "main")
    return (tmp_path / "toolkit.bundle").read_bytes(), git("rev-parse", "HEAD")


def _run(kit: Path, tmp_path: Path, *extra: str, commit: str = "0" * 40) -> subprocess.CompletedProcess[str]:
    # A python3.12 stand-in on PATH records any install attempt.
    stub = tmp_path / "bin"
    stub.mkdir(exist_ok=True)
    (stub / "python3.12").write_text(f"#!/bin/sh\ntouch {tmp_path / 'installed'}\n")
    (stub / "python3.12").chmod(0o755)
    environment = {**os.environ, "PATH": f"{stub}:{os.environ['PATH']}"}
    return subprocess.run(
        ["bash", str(SCRIPT), str(kit), str(tmp_path / "work"), commit, *extra],
        capture_output=True,
        text=True,
        check=False,
        env=environment,
        timeout=60,
    )


def test_a_matching_kit_is_verified(tmp_path: Path) -> None:
    result = _run(_kit(tmp_path), tmp_path, "--verify-only")

    assert result.returncode == 0, result.stderr
    assert all(name in result.stdout for name in NAMES)
    assert not (tmp_path / "installed").exists()


def test_a_changed_archive_stops_before_any_install(tmp_path: Path) -> None:
    kit = _kit(tmp_path)
    (kit / "docker-test-toolkit.pyz").write_bytes(b"tampered bytes")

    result = _run(kit, tmp_path)

    assert result.returncode == 3
    assert "SHA256SUMS: mismatch" in result.stderr
    assert not (tmp_path / "installed").exists()


def test_a_kit_whose_sums_omit_the_archive_stops_before_any_install(tmp_path: Path) -> None:
    result = _run(_kit(tmp_path, listed=("toolkit.bundle",)), tmp_path)

    assert result.returncode == 3
    assert "docker-test-toolkit.pyz is not listed" in result.stderr
    assert not (tmp_path / "installed").exists()


def test_a_bundle_at_another_commit_stops_before_any_install(tmp_path: Path) -> None:
    bundle, head = _bundle(tmp_path)
    other = "f" * 40 if head != "f" * 40 else "e" * 40

    result = _run(_kit(tmp_path, bundle=bundle), tmp_path, commit=other)

    assert result.returncode == 5
    assert "not the candidate commit" in result.stderr
    assert not (tmp_path / "installed").exists()
