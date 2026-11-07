from __future__ import annotations

import importlib
import os
import re
import shutil
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[2]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))
build_zipapp = importlib.import_module("build_zipapp")

REAL_GIT = shutil.which("git")
REFUSED = re.escape("Git blob batch answer contradicts the requested blob.")
IRREGULAR = re.escape("Distribution inputs must be regular committed files.")
Answer = tuple[str, bytes]
Craft = Callable[[bytes], int]


def _git(repository: Path, *arguments: str) -> bytes:
    assert REAL_GIT is not None
    return subprocess.run([REAL_GIT, *arguments], cwd=repository, check=True, capture_output=True).stdout


def _per_blob_oracle(repository: Path, commit: str) -> dict[str, tuple[bytes, int]]:
    """Read the builder's inputs the pre-batch way: one `git cat-file blob` child per listed entry."""
    blobs: dict[str, tuple[bytes, int]] = {}
    listing = _git(repository, "ls-tree", "-r", "-z", commit, "--", "scripts/build_zipapp.py", "src")
    for raw in filter(None, listing.split(b"\0")):
        metadata, path = raw.split(b"\t", 1)
        mode, kind, object_id = metadata.decode().split()
        assert kind == "blob" and mode in {"100644", "100755"}
        blobs[os.fsdecode(path)] = (_git(repository, "cat-file", "blob", object_id), int(mode[-3:], 8))
    return blobs


def _commit(repository: Path, files: dict[str, bytes], executable: frozenset[str] = frozenset()) -> str:
    _git(repository.parent, "init", "-q", str(repository))
    for name, content in files.items():
        path = repository / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        path.chmod(0o755 if name in executable else 0o644)
    _git(repository, "add", "-A")
    return _commit_index(repository)


def _commit_index(repository: Path) -> str:
    _git(
        repository,
        "-c",
        "user.name=t",
        "-c",
        "user.email=t@example.invalid",
        "-c",
        "commit.gpgsign=false",
        "commit",
        "-q",
        "--no-verify",
        "-m",
        "tree",
    )
    return _git(repository, "rev-parse", "HEAD").decode().strip()


def _synthetic(tmp_path: Path) -> tuple[Path, str]:
    repository = tmp_path / "synthetic"
    files = {
        "README.txt": b"outside the listing\n",
        "scripts/build_zipapp.py": b"print('recipe')\n",
        "scripts/other.py": b"outside the listing\n",
        "src/pkg/__init__.py": b"",
        "src/pkg/binary.bin": bytes(range(256)) * 4 + b"\n\n0123 blob 7\n",
        "src/pkg/large.py": b"x = 1\n" * 40_000,
        "src/pkg/run": b"#!/bin/sh\necho run\n",
        "src/pkg/same_a.py": b"same\n",
        "src/pkg/same_b.py": b"same\n",
    }
    return repository, _commit(repository, files, executable=frozenset({"src/pkg/run"}))


def _two_blobs(tmp_path: Path) -> tuple[Path, str, list[Answer]]:
    repository = tmp_path / "crafted"
    commit = _commit(repository, {"scripts/build_zipapp.py": b"print('recipe')\n", "src/a.py": b"alpha = 1\n"})
    answers = [
        (
            _git(repository, "rev-parse", f"{commit}:{name}").decode().strip(),
            _git(repository, "show", f"{commit}:{name}"),
        )
        for name in ("scripts/build_zipapp.py", "src/a.py")
    ]
    return repository, commit, answers


def _answer(object_id: str, content: bytes, *, kind: str = "blob", size: int | None = None) -> bytes:
    return f"{object_id} {kind} {len(content) if size is None else size}\n".encode() + content + b"\n"


@pytest.fixture
def crafted_batch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Craft:
    """Put a `git` on PATH whose `cat-file --batch` prints a crafted answer; every other call reaches the real git.

    The PATH change is this harness boundary's only seam: the builder spawns `git` by name, and a crafted batch answer
    cannot come from a real repository.
    """
    assert REAL_GIT is not None
    directory = tmp_path / "fake-git"
    directory.mkdir()
    answer = directory / "answer.bin"
    fake = directory / "git"
    fake.write_text(
        "#!/bin/sh\n"
        'if [ "$1" = cat-file ] && [ "$2" = --batch ]; then\n'
        f"  cat >/dev/null\n  exec cat '{answer}'\n"
        "fi\n"
        f"exec '{REAL_GIT}' \"$@\"\n"
    )
    fake.chmod(0o755)
    monkeypatch.setenv("PATH", f"{directory}{os.pathsep}{os.environ['PATH']}")
    return answer.write_bytes


@pytest.mark.parametrize("tree", ["synthetic", "repository"])
def test_batched_reader_matches_the_per_blob_oracle(tree: str, tmp_path: Path) -> None:
    repository, commit = _synthetic(tmp_path) if tree == "synthetic" else (ROOT, "HEAD")

    blobs = build_zipapp._tree_blobs(repository, commit)

    assert blobs == _per_blob_oracle(repository, commit)
    if tree == "synthetic":
        assert sorted(blobs) == [
            "scripts/build_zipapp.py",
            "src/pkg/__init__.py",
            "src/pkg/binary.bin",
            "src/pkg/large.py",
            "src/pkg/run",
            "src/pkg/same_a.py",
            "src/pkg/same_b.py",
        ]
        assert blobs["src/pkg/run"][1] == 0o755
        assert blobs["src/pkg/large.py"][1] == 0o644
    else:
        assert "scripts/build_zipapp.py" in blobs
        assert len(blobs) > 30


def test_batched_reader_refuses_a_wrong_type(tmp_path: Path, crafted_batch: Craft) -> None:
    repository, commit, ((first_id, first), (second_id, second)) = _two_blobs(tmp_path)
    crafted_batch(_answer(first_id, first) + _answer(second_id, second, kind="tag"))

    with pytest.raises(ValueError, match=REFUSED):
        build_zipapp._tree_blobs(repository, commit)


def test_batched_reader_refuses_a_short_body(tmp_path: Path, crafted_batch: Craft) -> None:
    repository, commit, ((first_id, first), (second_id, second)) = _two_blobs(tmp_path)
    crafted_batch(_answer(first_id, first) + _answer(second_id, second[:-1], size=len(second)))

    with pytest.raises(ValueError, match=REFUSED):
        build_zipapp._tree_blobs(repository, commit)


def test_batched_reader_refuses_missing(tmp_path: Path, crafted_batch: Craft) -> None:
    repository, commit, ((first_id, first), (second_id, _)) = _two_blobs(tmp_path)
    crafted_batch(_answer(first_id, first) + f"{second_id} missing\n".encode())

    with pytest.raises(ValueError, match=REFUSED):
        build_zipapp._tree_blobs(repository, commit)


def test_batched_reader_refuses_a_swapped_id(tmp_path: Path, crafted_batch: Craft) -> None:
    repository, commit, ((first_id, first), (second_id, second)) = _two_blobs(tmp_path)
    crafted_batch(_answer(second_id, second) + _answer(first_id, first))

    with pytest.raises(ValueError, match=REFUSED):
        build_zipapp._tree_blobs(repository, commit)


def test_batched_reader_refuses_symlink_and_gitlink(tmp_path: Path) -> None:
    linked = tmp_path / "linked"
    _commit(linked, {"scripts/build_zipapp.py": b"recipe\n", "src/a.py": b"a\n"})
    (linked / "src/link.py").symlink_to("a.py")
    _git(linked, "add", "-A")
    linked_commit = _commit_index(linked)
    submodule = tmp_path / "submodule"
    submodule_commit = _commit(submodule, {"scripts/build_zipapp.py": b"recipe\n", "src/a.py": b"a\n"})
    _git(submodule, "update-index", "--add", "--cacheinfo", f"160000,{submodule_commit},src/vendored")
    submodule_commit = _commit_index(submodule)

    for repository, commit, mode in ((linked, linked_commit, "120000"), (submodule, submodule_commit, "160000")):
        assert mode.encode() in _git(repository, "ls-tree", "-r", commit, "--", "src")
        with pytest.raises(ValueError, match=IRREGULAR):
            build_zipapp._tree_blobs(repository, commit)
