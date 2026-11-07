"""The secret scan is pinned and proven on a synthetic corpus.

The repository config holds gitleaks' default rules and one fixture allowlist; owner-specific terms never enter the
repository and are appended from a private list, so these tests use a synthetic term in their place. A finding never
prints the term it matched (``--redact``).
"""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).parents[2]
CONFIG = ROOT / ".gitleaks.toml"
GITLEAKS_VERSION = "8.30.1"
SYNTHETIC_TERM = "example-owner-name"
TERM_FRAGMENT = f"""
[[rules]]
id = "owner-term-synthetic"
description = "owner term (synthetic)"
regex = '''(?i)\\b{SYNTHETIC_TERM}\\b'''
"""
IDENTITY = "juzhxng <juzhxng@users.noreply.github.com>"


def _gitleaks() -> str:
    executable = shutil.which("gitleaks")
    if executable is None:
        pytest.fail(f"install gitleaks {GITLEAKS_VERSION}")
    return executable


def _combined(tmp_path: Path, fragment: str) -> Path:
    combined = tmp_path / "gitleaks.toml"
    combined.write_text(CONFIG.read_text() + fragment)
    return combined


def _scan(config: Path, target: Path, report: Path) -> tuple[int, list[dict[str, object]], str]:
    completed = subprocess.run(
        [_gitleaks(), "dir", "--config", str(config), "--no-banner", "--redact", "--report-format", "json",
         "--report-path", str(report), str(target)],
        capture_output=True, text=True, check=False, timeout=120,
    )  # fmt: skip
    text = report.read_text()
    return completed.returncode, json.loads(text), text + completed.stdout + completed.stderr


def test_gitleaks_is_pinned() -> None:
    version = subprocess.run([_gitleaks(), "version"], capture_output=True, text=True, check=True, timeout=30)
    assert version.stdout.strip() == GITLEAKS_VERSION


def test_synthetic_corpus_findings_are_redacted(tmp_path: Path) -> None:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "token.txt").write_text('token = "ghp_' + "A1b2C3d4E5f6G7h8I9j0K1l2M3n4O5p6Q7r8" + '"\n')
    (corpus / "name.txt").write_text(f"written by {SYNTHETIC_TERM} on the CI host\n")
    code, findings, output = _scan(_combined(tmp_path, TERM_FRAGMENT), corpus, tmp_path / "report.json")
    assert code == 1
    assert sorted(str(finding["RuleID"]) for finding in findings) == ["github-pat", "owner-term-synthetic"]
    assert SYNTHETIC_TERM not in output


def test_identity_and_empty_fragment_are_clean(tmp_path: Path) -> None:
    clean = tmp_path / "clean"
    clean.mkdir()
    (clean / "identity.txt").write_text(IDENTITY + "\n")
    code, findings, _ = _scan(_combined(tmp_path, TERM_FRAGMENT), clean, tmp_path / "clean.json")
    assert (code, findings) == (0, [])
    # Without the private fragment (a fork pull request) the repository config alone still finds a secret.
    (clean / "token.txt").write_text('token = "ghp_' + "A1b2C3d4E5f6G7h8I9j0K1l2M3n4O5p6Q7r8" + '"\n')
    code, findings, _ = _scan(_combined(tmp_path, ""), clean, tmp_path / "bare.json")
    assert code == 1
    assert [finding["RuleID"] for finding in findings] == ["github-pat"]


def test_scan_job_uses_the_combined_config() -> None:
    workflow = yaml.safe_load((ROOT / ".github" / "workflows" / "ci.yml").read_text())
    scan = workflow["jobs"]["scan"]
    run = "\n".join(str(step.get("run", "")) for step in scan["steps"])

    assert any(step.get("with", {}).get("fetch-depth") == 0 for step in scan["steps"])
    assert 'cat .gitleaks.toml > "$RUNNER_TEMP/gitleaks.toml"' in run
    assert 'gitleaks git --config "$RUNNER_TEMP/gitleaks.toml" --log-opts=--all --no-banner --redact .' in run
    assert f"gitleaks_{GITLEAKS_VERSION}_linux_arm64.tar.gz" in run and "sha256sum -c" in run
