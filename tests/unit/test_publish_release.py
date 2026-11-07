"""Publication fails closed and never leaves a partial public release."""

from __future__ import annotations

import hashlib
import importlib
import json
import re
import sys
from collections.abc import Callable, Sequence
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).parents[2] / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))
publish_release = importlib.import_module("publish_release")
PublishError = publish_release.PublishError

REPO = "owner/toolkit"
TAG = "v0.7.3"
SHA = "a" * 40
TAG_OBJECT = "b" * 40
ARCHIVE = b"zipapp bytes"
CANDIDATE = hashlib.sha256(ARCHIVE).hexdigest()
JOBS = [{"name": name, "conclusion": "success"} for name in publish_release.REQUIRED_JOBS]
LAST_JOB = publish_release.REQUIRED_JOBS[-1]


class _FakeGh:
    """Answers ``gh`` argv by prefix and records every call; a release is modelled as published, draft or absent."""

    def __init__(self, tmp: Path, **state: object) -> None:
        self.tmp = tmp
        self.calls: list[list[str]] = []
        self.remote_commit: str = str(state.get("remote_commit", SHA))
        self.moved_commit: str | None = state.get("moved_commit")  # type: ignore[assignment]
        self.compare_status = state.get("compare_status", "ahead")
        self.runs = state.get("runs", [{"id": 7, "head_sha": SHA, "conclusion": "success", "status": "completed"}])
        self.jobs = state.get("jobs", JOBS)
        self.release: str | None = state.get("release")  # type: ignore[assignment]  # "published", "draft" or None
        self.release_bytes: bytes = state.get("release_bytes", ARCHIVE)  # type: ignore[assignment]
        self.downloaded_bytes: bytes | None = state.get("downloaded_bytes")  # type: ignore[assignment]
        self.ref_reads = 0
        self.create_fails = bool(state.get("create_fails", False))

    def __call__(self, argv: Sequence[str]) -> bytes:
        args = list(argv)
        self.calls.append(args)
        if args[:2] == ["api", f"repos/{REPO}/git/ref/tags/{TAG}"]:
            self.ref_reads += 1
            return json.dumps({"object": {"type": "tag", "sha": TAG_OBJECT}}).encode()
        if args[:2] == ["api", f"repos/{REPO}/git/tags/{TAG_OBJECT}"]:
            commit = self.moved_commit if self.moved_commit and self.ref_reads > 1 else self.remote_commit
            return json.dumps({"object": {"type": "commit", "sha": commit}}).encode()
        if args[:2] == ["api", f"repos/{REPO}/compare/{SHA}...main"]:
            return json.dumps({"status": self.compare_status}).encode()
        if args[0] == "api" and "/actions/workflows/ci.yml/runs" in args[1]:
            return json.dumps({"workflow_runs": self.runs}).encode()
        if args[0] == "api" and args[1].startswith(f"repos/{REPO}/actions/runs/"):
            return json.dumps({"jobs": self.jobs}).encode()
        if args[:2] == ["release", "view"]:
            if self.release is None:
                raise publish_release.GhError(args, 1, "release not found")
            return json.dumps({"isDraft": self.release == "draft"}).encode()
        if args[:2] == ["release", "download"]:
            directory = Path(args[args.index("--dir") + 1])
            data = self.downloaded_bytes if self.downloaded_bytes is not None else self.release_bytes
            (directory / publish_release.ASSET).write_bytes(data)
            return b""
        if args[:2] == ["release", "create"]:
            self.release = "draft"
            if self.create_fails:
                raise publish_release.GhError(args, 1, "upload failed after the draft was made")
            self.release_bytes = Path(args[-1]).read_bytes()
            return b""
        if args[:2] == ["release", "delete"]:
            self.release = None
            return b""
        if args[:2] == ["release", "edit"]:
            self.release = "published"
            return b""
        raise AssertionError(f"unexpected gh call: {args}")

    def verbs(self) -> list[str]:
        return [" ".join(call[:2]) for call in self.calls if call[0] == "release"]


def _archive(tmp_path: Path, data: bytes = ARCHIVE) -> Path:
    path = tmp_path / "dist" / publish_release.ASSET
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path


def _local(**overrides: object) -> object:
    values = {"commit": SHA, "annotated": True}
    return publish_release.LocalTag(**{**values, **overrides})


def _publish(gh: _FakeGh, tmp_path: Path, recheck: Callable[[], None] | None = None) -> str:
    return publish_release.publish(
        TAG,
        _archive(tmp_path),
        CANDIDATE,
        "notes",
        gh,
        REPO,
        recheck=recheck or (lambda: publish_release.verify_remote_tag(TAG, SHA, gh, REPO)),
        work=tmp_path / "work",
    )


def test_a_bound_green_candidate_is_published_through_a_draft(tmp_path: Path) -> None:
    gh = _FakeGh(tmp_path)

    publish_release.verify_binding(TAG, _local(), gh, REPO)
    assert publish_release.green_run(SHA, gh, REPO) == 7
    assert _publish(gh, tmp_path) == "published"

    assert gh.verbs() == ["release view", "release create", "release download", "release edit"]
    assert gh.calls[-1][-1] == "--draft=false"
    create = next(call for call in gh.calls if call[:2] == ["release", "create"])
    assert "--draft" in create and "--verify-tag" in create
    assert gh.release == "published"


@pytest.mark.parametrize(
    ("local", "state", "message"),
    [
        ({"annotated": False}, {}, "not an annotated tag"),
        ({}, {"remote_commit": "c" * 40}, "remote tag v0.7.3 points to"),
        ({}, {"compare_status": "diverged"}, "is not on main"),
        ({}, {"compare_status": "behind"}, "is not on main"),
    ],
)
def test_binding_refuses_a_tag_that_is_not_the_checked_commit_on_main(
    tmp_path: Path, local: dict[str, object], state: dict[str, object], message: str
) -> None:
    with pytest.raises(PublishError, match=message):
        publish_release.verify_binding(TAG, _local(**local), _FakeGh(tmp_path, **state), REPO)


@pytest.mark.parametrize(
    ("runs", "jobs", "message"),
    [
        ([], JOBS, "no green CI run"),
        ([{"id": 8, "head_sha": "d" * 40, "conclusion": "success", "status": "completed"}], JOBS, "no green CI run"),
        ([{"id": 8, "head_sha": SHA, "conclusion": "failure", "status": "completed"}], JOBS, "no green CI run"),
        (None, JOBS[:-1], f"required job {LAST_JOB} is missing"),
        (None, [*JOBS[:-1], {"name": LAST_JOB, "conclusion": "skipped"}], f"{LAST_JOB} is skipped"),
        (None, [{"name": "scan", "conclusion": "cancelled"}, *JOBS[1:]], "scan is cancelled"),
    ],
)
def test_green_run_refuses_a_run_missing_a_required_job(
    tmp_path: Path, runs: list[dict[str, object]] | None, jobs: list[dict[str, str]], message: str
) -> None:
    state: dict[str, object] = {"jobs": jobs}
    if runs is not None:
        state["runs"] = runs
    with pytest.raises(PublishError, match=re.escape(message)):
        publish_release.green_run(SHA, _FakeGh(tmp_path, **state), REPO)


@pytest.mark.parametrize(
    ("version", "built", "message"),
    [("0.7.3", b"other bytes", "differs from the candidate"), ("0.7.2", ARCHIVE, r"tree's version 0\.7\.2")],
)
def test_same_bytes_refuses_another_version_or_other_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, version: str, built: bytes, message: str
) -> None:
    def build(repository: Path, output: Path, *, treeish: str) -> dict[str, object]:
        assert treeish == SHA
        _archive(output.parent, built)
        return {"sha256": hashlib.sha256(built).hexdigest(), "version": version}

    monkeypatch.setattr(publish_release.build_zipapp, "build", build)
    with pytest.raises(PublishError, match=message):
        publish_release.same_bytes(TAG, tmp_path, SHA, CANDIDATE, tmp_path / "dist")


def test_an_already_published_release_with_equal_bytes_is_accepted(tmp_path: Path) -> None:
    gh = _FakeGh(tmp_path, release="published")

    assert _publish(gh, tmp_path) == "already published"
    assert gh.verbs() == ["release view", "release download"]


def test_a_published_release_with_other_bytes_is_refused_and_left_unchanged(tmp_path: Path) -> None:
    gh = _FakeGh(tmp_path, release="published", release_bytes=b"older bytes")

    with pytest.raises(PublishError, match="already published with other bytes"):
        _publish(gh, tmp_path)
    assert gh.verbs() == ["release view", "release download"]
    assert gh.release == "published"


def test_a_leftover_draft_is_deleted_before_the_new_draft(tmp_path: Path) -> None:
    gh = _FakeGh(tmp_path, release="draft")

    assert _publish(gh, tmp_path) == "published"
    assert gh.verbs()[:3] == ["release view", "release delete", "release create"]


def test_publish_deletes_the_draft_when_the_uploaded_bytes_differ(tmp_path: Path) -> None:
    gh = _FakeGh(tmp_path, downloaded_bytes=b"corrupted upload")

    with pytest.raises(PublishError, match="uploaded asset differs"):
        _publish(gh, tmp_path)
    assert gh.verbs()[-1] == "release delete"
    assert gh.release is None


def test_publish_deletes_the_draft_when_the_remote_tag_moved(tmp_path: Path) -> None:
    gh = _FakeGh(tmp_path, moved_commit="e" * 40)
    publish_release.verify_remote_tag(TAG, SHA, gh, REPO)  # the binding check passes first

    with pytest.raises(PublishError, match=r"remote tag v0\.7\.3 points to"):
        _publish(gh, tmp_path)
    assert gh.verbs()[-1] == "release delete"
    assert gh.release is None


def test_release_notes_carry_the_run_the_references_and_the_hash_table(tmp_path: Path) -> None:
    notes = publish_release.release_notes("0.7.3", SHA, 7, "adopter run 160", _archive(tmp_path))

    assert f"| {publish_release.ASSET} | {len(ARCHIVE)} | {CANDIDATE} |" in notes
    assert "CI run 7" in notes and "adopter run 160" in notes and SHA in notes


def test_required_jobs_match_the_ci_workflow() -> None:
    import yaml

    workflow = yaml.safe_load((Path(__file__).parents[2] / ".github" / "workflows" / "ci.yml").read_text())

    expected = {
        f"{name} ({runner})" if "matrix" in job.get("strategy", {}) else name
        for name, job in workflow["jobs"].items()
        for runner in job.get("strategy", {}).get("matrix", {}).get("runner", [""])
    }
    assert set(publish_release.REQUIRED_JOBS) == expected


def test_a_create_that_fails_after_making_the_draft_leaves_no_draft(tmp_path: Path) -> None:
    gh = _FakeGh(tmp_path, create_fails=True)

    with pytest.raises(publish_release.GhError, match="upload failed"):
        _publish(gh, tmp_path)
    assert gh.verbs()[-1] == "release delete"
    assert gh.release is None


def test_local_tag_reads_a_real_annotated_tag_at_head(tmp_path: Path) -> None:
    import subprocess

    def git(*args: str) -> None:
        subprocess.run(["git", "-C", str(tmp_path), *args], check=True, capture_output=True)

    git("init", "-q")
    (tmp_path / "a.txt").write_text("a\n")
    git("add", "a.txt")
    git("-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-q", "-m", "a")
    git("-c", "user.name=t", "-c", "user.email=t@example.invalid", "tag", "-a", "v1.0.0", "-m", "v1.0.0")
    git("tag", "v1.0.1")

    head = subprocess.run(["git", "-C", str(tmp_path), "rev-parse", "HEAD"], check=True, capture_output=True, text=True)
    assert publish_release.local_tag(tmp_path, "v1.0.0") == publish_release.LocalTag(head.stdout.strip(), True)
    assert publish_release.local_tag(tmp_path, "v1.0.1").annotated is False
    with pytest.raises(PublishError, match="cannot be read"):
        publish_release.local_tag(tmp_path, "v9.9.9")
