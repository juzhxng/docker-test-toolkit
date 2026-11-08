"""Publish a canaried release candidate as a GitHub Release, failing closed.

Run by ``.github/workflows/publish.yml`` (or locally with ``--dry-run``) from a checkout of the release tag. In order:

1. Binding: the local tag is annotated, peels to the checked-out commit and names the tree's ``TOOLKIT_VERSION``; the
   remote tag peels to the same commit; the commit is reachable from ``main``.
2. Green CI: a completed ``ci.yml`` run of that commit concluded ``success`` with every required job ``success``.
3. Same bytes: the archive rebuilt from the commit hashes to the installed candidate's SHA-256.
4. Publish: an existing published release is accepted only with equal bytes; a leftover draft is deleted; a new draft
   gets the archive, which is downloaded back and compared, the remote tag is checked again, and only then is the draft
   published. A failure after the draft exists deletes it.

Every refusal exits 2 with one line naming the mismatch. That the candidate passed the
canary is the release runbook's obligation; the reference only goes into the notes.

Usage:
    publish_release.py --tag v0.7.5 --candidate-sha256 HEX --repository owner/name [--repository-path PATH]
        [--canary TEXT] [--dry-run]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import tempfile
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import build_zipapp  # the sibling build script: scripts/ is sys.path[0] when this file runs

RUNNERS = ("ubuntu-24.04-arm", "ubuntu-24.04")
REQUIRED_JOBS = ("scan", *(f"{job} ({runner})" for job in ("quality", "e2e", "shared-runs") for runner in RUNNERS))
WORKFLOW = "ci.yml"
ASSET = "docker-test-toolkit.pyz"


class PublishError(RuntimeError):
    """A refused publication; the message is the one line printed before exit 2."""


class GhError(RuntimeError):
    """A ``gh`` command that exited non-zero."""

    def __init__(self, argv: Sequence[str], code: int, stderr: str) -> None:
        super().__init__(f"gh {' '.join(argv[:2])} exited {code}: {stderr.strip()[:200]}")


Gh = Callable[[Sequence[str]], bytes]


def run_gh(argv: Sequence[str]) -> bytes:
    """Run ``gh`` with ``argv`` and return its stdout; a non-zero exit raises ``GhError``."""
    completed = subprocess.run(["gh", *argv], capture_output=True, check=False)
    if completed.returncode != 0:
        raise GhError(argv, completed.returncode, completed.stderr.decode(errors="replace"))
    return completed.stdout


def _json(gh: Gh, *argv: str) -> Any:
    return json.loads(gh(argv))


@dataclass(frozen=True)
class LocalTag:
    """The local tag's peeled commit and whether it is annotated."""

    commit: str
    annotated: bool


def _git(path: Path, *argv: str) -> str:
    return subprocess.run(["git", "-C", str(path), *argv], capture_output=True, check=True, text=True).stdout.strip()


def local_tag(path: Path, tag: str) -> LocalTag:
    """Read the tag from the checkout at ``path``; it must be the checked-out commit."""
    try:
        commit = _git(path, "rev-parse", f"refs/tags/{tag}^{{commit}}")
        head = _git(path, "rev-parse", "HEAD")
        annotated = _git(path, "cat-file", "-t", f"refs/tags/{tag}") == "tag"
    except subprocess.CalledProcessError as error:
        raise PublishError(f"tag {tag} cannot be read from {path}: {error.stderr!r}") from error
    if commit != head:
        raise PublishError(f"tag {tag} peels to {commit}, but the checkout is at {head}")
    return LocalTag(commit=commit, annotated=annotated)


def verify_remote_tag(tag: str, commit: str, gh: Gh, repository: str) -> None:
    """Refuse unless the remote tag peels to ``commit``."""
    ref = _json(gh, "api", f"repos/{repository}/git/ref/tags/{tag}")["object"]
    while ref.get("type") == "tag":
        ref = _json(gh, "api", f"repos/{repository}/git/tags/{ref['sha']}")["object"]
    if ref.get("sha") != commit:
        raise PublishError(f"remote tag {tag} points to {ref.get('sha')}, not {commit}")


def verify_binding(tag: str, local: LocalTag, gh: Gh, repository: str) -> None:
    """Step 1: the tag is annotated, peels remotely to the commit, and is on ``main``."""
    if not local.annotated:
        raise PublishError(f"{tag} is not an annotated tag")
    verify_remote_tag(tag, local.commit, gh, repository)
    status = _json(gh, "api", f"repos/{repository}/compare/{local.commit}...main")["status"]
    if status not in {"ahead", "identical"}:
        raise PublishError(f"commit {local.commit} is not on main (compare status {status})")


def green_run(sha: str, gh: Gh, repository: str) -> int:
    """Step 2: the id of the newest green ``ci.yml`` run of ``sha`` whose required jobs all succeeded."""
    runs = _json(gh, "api", f"repos/{repository}/actions/workflows/{WORKFLOW}/runs?head_sha={sha}&per_page=100")
    candidates = [
        run
        for run in runs["workflow_runs"]
        if run.get("head_sha") == sha and run.get("status") == "completed" and run.get("conclusion") == "success"
    ]
    if not candidates:
        raise PublishError(f"no green CI run of {sha}")
    problems = []
    for run in candidates:
        listed = _json(gh, "api", f"repos/{repository}/actions/runs/{run['id']}/jobs?per_page=50")["jobs"]
        jobs = {job["name"]: job.get("conclusion") for job in listed}
        failing = next((name for name in REQUIRED_JOBS if jobs.get(name) != "success"), None)
        if failing is None:
            return int(run["id"])
        problems.append(f"run {run['id']}: required job {failing} is {jobs.get(failing, 'missing')}")
    raise PublishError(f"no green CI run of {sha} with every required job ({'; '.join(problems)})")


def same_bytes(tag: str, repository_path: Path, sha: str, candidate_sha256: str, output: Path) -> Path:
    """Step 3: rebuild the archive from ``sha``; refuse unless it is the candidate and of the tag's version."""
    result = build_zipapp.build(repository_path, output, treeish=sha)
    if tag.removeprefix("v") != result["version"]:
        raise PublishError(f"tag {tag} does not match the tree's version {result['version']}")
    if result["sha256"] != candidate_sha256:
        raise PublishError(f"the archive built from {sha} ({result['sha256']}) differs from the candidate")
    return output / ASSET


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _released_bytes_hash(tag: str, gh: Gh, repository: str, work: Path) -> str:
    directory = work / "download"
    directory.mkdir(parents=True, exist_ok=True)
    gh(["release", "download", tag, "-R", repository, "-p", ASSET, "--dir", str(directory), "--clobber"])
    return _sha256(directory / ASSET)


def publish(
    tag: str,
    archive: Path,
    candidate_sha256: str,
    notes: str,
    gh: Gh,
    repository: str,
    *,
    recheck: Callable[[], None],
    work: Path,
) -> str:
    """Step 4: publish ``archive`` through a draft; return ``"published"`` or ``"already published"``."""
    work.mkdir(parents=True, exist_ok=True)
    try:
        existing = _json(gh, "release", "view", tag, "-R", repository, "--json", "isDraft")
    except GhError:
        existing = None
    if existing is not None and not existing["isDraft"]:
        if _released_bytes_hash(tag, gh, repository, work) != candidate_sha256:
            raise PublishError(f"{tag} is already published with other bytes; it is never replaced")
        return "already published"
    if existing is not None:
        gh(["release", "delete", tag, "-R", repository, "--yes"])
    notes_file = work / "notes.md"
    notes_file.write_text(notes)
    create = ["release", "create", tag, "-R", repository, "--draft", "--verify-tag", "--title", tag]
    try:
        # Inside the cleanup: a create that fails after making the draft (an upload error) leaves none behind.
        gh([*create, "--notes-file", str(notes_file), str(archive)])
        if _released_bytes_hash(tag, gh, repository, work) != candidate_sha256:
            raise PublishError(f"the uploaded asset differs from the candidate {candidate_sha256}")
        recheck()
        gh(["release", "edit", tag, "-R", repository, "--draft=false"])
    except BaseException:
        try:
            gh(["release", "delete", tag, "-R", repository, "--yes"])
        except GhError:
            pass  # the create failed before any draft existed
        raise
    return "published"


def changes_since_previous_tag(path: Path, tag: str) -> str:
    """The subjects of the commits ``tag`` adds to the previous tag, or to the start of history, as a Markdown list."""
    try:
        revisions = f"{_git(path, 'describe', '--tags', '--abbrev=0', '--match', 'v*', f'{tag}^')}..{tag}"
    except subprocess.CalledProcessError:
        revisions = tag
    return _git(path, "log", "--no-merges", "--format=- %s", revisions)


def release_notes(version: str, commit: str, run_id: int, canary: str, archive: Path, changes: str) -> str:
    """The release notes: what changed, then identity, the CI run, the canary reference, and the hash table."""
    lines = [
        "## What changed",
        "",
        changes,
        "",
        "## Provenance",
        "",
        f"docker-test-toolkit {version} from commit {commit}.",
        "",
        f"- CI run {run_id} passed every required job ({', '.join(REQUIRED_JOBS)}).",
        f"- Canary: {canary or 'not given'}.",
        "",
        "| File | Bytes | SHA-256 |",
        "|---|---|---|",
        f"| {ASSET} | {archive.stat().st_size} | {_sha256(archive)} |",
    ]
    return "\n".join(lines) + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    """Run steps 1-4 (1-3 with ``--dry-run``); exit 0 when published or already published, 2 on a refusal."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--candidate-sha256", required=True)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--repository-path", type=Path, default=Path("."))
    parser.add_argument("--canary", default="")
    parser.add_argument("--dry-run", action="store_true")
    options = parser.parse_args(argv)
    path = options.repository_path.resolve()
    gh: Gh = run_gh
    try:
        local = local_tag(path, options.tag)
        verify_binding(options.tag, local, gh, options.repository)
        run_id = green_run(local.commit, gh, options.repository)
        with tempfile.TemporaryDirectory() as temporary:
            work = Path(temporary)
            archive = same_bytes(options.tag, path, local.commit, options.candidate_sha256, work / "dist")
            if options.dry_run:
                print(f"dry run: {options.tag} at {local.commit} is publishable (CI run {run_id})")
                return 0
            changes = changes_since_previous_tag(path, options.tag)
            notes = release_notes(options.tag.removeprefix("v"), local.commit, run_id, options.canary, archive, changes)
            outcome = publish(
                options.tag,
                archive,
                options.candidate_sha256,
                notes,
                gh,
                options.repository,
                recheck=lambda: verify_remote_tag(options.tag, local.commit, gh, options.repository),
                work=work / "release",
            )
    except (PublishError, GhError, ValueError, OSError) as error:
        print(f"publication refused: {error}", file=sys.stderr)
        return 2
    print(f"{options.tag}: {outcome}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
