#!/usr/bin/env python3
"""Build the deterministic toolkit zip application ``docker-test-toolkit.pyz`` from one committed tree."""

from __future__ import annotations

import argparse
import ast
import hashlib
import io
import json
import os
import stat
import subprocess
import sys
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

_OWNERSHIP = "component-ownership.json"
_OWNERS = frozenset({"lifecycle", "shared"})
_RECIPE = "scripts/build_zipapp.py"
_LIFECYCLE_NAMESPACE = "src/docker_test_toolkit"
_MODELS = "src/docker_test_toolkit/domain/models.py"
_MAIN = b"from docker_test_toolkit.factories.cli import main\n\nraise SystemExit(main())\n"
ARCHIVE_NAME = "docker-test-toolkit.pyz"
_RUNNING_RECIPE = Path(__file__).read_bytes()


@dataclass(frozen=True)
class OwnershipEntry:
    """Bind one committed production input to its owner and mode."""

    path: str
    owner: str
    mode: int


def _git(repository: Path, *arguments: str, stdin: bytes | None = None) -> bytes:
    result = subprocess.run(["git", *arguments], cwd=repository, check=False, capture_output=True, input=stdin)
    if result.returncode != 0:
        raise ValueError("Git tree identity is unavailable.")
    return result.stdout


def _canonical(record: object) -> bytes:
    return (json.dumps(record, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("JSON record contains a duplicate field.")
        value[key] = item
    return value


def _committed_identity(models: bytes, version_name: str) -> tuple[str, dict[str, int]]:
    try:
        tree = ast.parse(models)
        version: object = None
        protocols: object = None
        for statement in tree.body:
            if not isinstance(statement, ast.Assign) or len(statement.targets) != 1:
                continue
            target = statement.targets[0]
            if not isinstance(target, ast.Name):
                continue
            if target.id == version_name:
                version = ast.literal_eval(statement.value)
            elif target.id == "SUPPORTED_PROTOCOLS" and isinstance(statement.value, ast.Call) and statement.value.args:
                protocols = ast.literal_eval(statement.value.args[0])
    except (RecursionError, SyntaxError, ValueError) as error:
        raise ValueError("Committed identity source is invalid.") from error
    if (
        not isinstance(version, str)
        or type(protocols) is not dict
        or not protocols
        or any(not isinstance(key, str) or type(value) is not int or value < 1 for key, value in protocols.items())
    ):
        raise ValueError("Committed version and protocol identity are invalid or inconsistent.")
    return version, dict(sorted(protocols.items()))


def _tree_blobs(repository: Path, commit: str) -> dict[str, tuple[bytes, int]]:
    """Read every listed input blob through one `git cat-file --batch` child.

    The listing is refused before any blob is read. Each batch answer must name the requested id, the type `blob` and
    a size whose exact body is followed by a newline; `missing`, another type, a short body or trailing bytes refuse.
    """
    entries: list[tuple[str, str, int]] = []
    raw_tree = _git(repository, "ls-tree", "-r", "-z", commit, "--", _RECIPE, "src")
    for raw in raw_tree.split(b"\0"):
        if not raw:
            continue
        metadata, encoded_path = raw.split(b"\t", 1)
        mode_text, kind, object_id = metadata.decode().split()
        path = os.fsdecode(encoded_path)
        if kind != "blob" or mode_text not in {"100644", "100755"}:
            raise ValueError("Distribution inputs must be regular committed files.")
        if not path.isascii() or path.endswith((".pyc", ".pyo")) or "__pycache__" in PurePosixPath(path).parts:
            raise ValueError("Distribution input paths must be normalized ASCII source paths.")
        entries.append((path, object_id, 0o755 if mode_text == "100755" else 0o644))
    answers = _git(repository, "cat-file", "--batch", stdin="".join(f"{entry[1]}\n" for entry in entries).encode())
    blobs: dict[str, tuple[bytes, int]] = {}
    position = 0
    for path, object_id, mode in entries:
        header_end = answers.find(b"\n", position)
        header = answers[position:header_end].split(b" ") if header_end >= 0 else []
        if len(header) != 3 or header[:2] != [object_id.encode(), b"blob"] or not header[2].isdigit():
            raise ValueError("Git blob batch answer contradicts the requested blob.")
        body_end = header_end + 1 + int(header[2])
        if answers[body_end : body_end + 1] != b"\n":
            raise ValueError("Git blob batch answer contradicts the requested blob.")
        blobs[path] = (answers[header_end + 1 : body_end], mode)
        position = body_end + 1
    if position != len(answers):
        raise ValueError("Git blob batch answer contradicts the requested blob.")
    return blobs


def _ownership(repository: Path, commit: str, blobs: dict[str, tuple[bytes, int]]) -> tuple[OwnershipEntry, ...]:
    try:
        record = json.loads(
            _git(repository, "show", f"{commit}:{_OWNERSHIP}"),
            object_pairs_hook=_unique,
        )
    except (RecursionError, UnicodeDecodeError, ValueError) as error:
        raise ValueError("Component ownership manifest is invalid JSON.") from error
    if set(record) != {"entries", "schema"} or record["schema"] != 1 or not isinstance(record["entries"], list):
        raise ValueError("Component ownership manifest has an unsupported contract.")
    entries: list[OwnershipEntry] = []
    for item in record["entries"]:
        if not isinstance(item, dict) or set(item) != {"mode", "owner", "path"}:
            raise ValueError("Component ownership entry is invalid.")
        path, owner, mode_text = item["path"], item["owner"], item["mode"]
        if (
            not isinstance(path, str)
            or not path.isascii()
            or path.startswith("/")
            or ".." in PurePosixPath(path).parts
            or owner not in _OWNERS
            or mode_text not in {"0644", "0755"}
        ):
            raise ValueError("Component ownership entry is invalid.")
        entries.append(OwnershipEntry(path, owner, int(mode_text, 8)))
    paths = [entry.path for entry in entries]
    if paths != sorted(paths) or len(paths) != len(set(paths)):
        raise ValueError("Component ownership entries are duplicate or noncanonical.")
    if set(blobs) != set(paths):
        raise ValueError("Component ownership manifest does not exhaust the production inputs.")
    if any(blobs[entry.path][1] != entry.mode for entry in entries):
        raise ValueError("Component ownership mode contradicts the committed blob.")
    shared = {entry.path for entry in entries if entry.owner == "shared"}
    if shared != {_RECIPE}:
        raise ValueError("Shared component ownership is not the fixed allowlist.")
    for entry in entries:
        if entry.path in shared:
            continue
        if entry.owner == "lifecycle" and entry.path.startswith(f"{_LIFECYCLE_NAMESPACE}/"):
            continue
        raise ValueError("Component ownership entry contradicts the allowed namespaces.")
    return tuple(entries)


def _require_clean(repository: Path, entries: tuple[OwnershipEntry, ...]) -> None:
    dirty = _git(
        repository,
        "status",
        "--porcelain=v1",
        "-z",
        "--untracked-files=all",
        "--",
        _OWNERSHIP,
        *(e.path for e in entries),
    )
    if dirty:
        raise ValueError("Production inputs are not committed.")


def _zip_info(name: str, mode: int) -> zipfile.ZipInfo:
    info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
    info.compress_type = zipfile.ZIP_STORED
    info.create_system = 3
    info.create_version = 20
    info.extract_version = 20
    info.flag_bits = 0
    info.volume = 0
    info.internal_attr = 0
    info.external_attr = (stat.S_IFREG | mode) << 16
    info.extra = b""
    info.comment = b""
    return info


def _archive_bytes(members: list[tuple[str, bytes, int]]) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_STORED) as archive:
        for name, content, mode in sorted(members):
            archive.writestr(_zip_info(name, mode), content)
    return output.getvalue()


def _stage(directory: Path, target: Path, content: bytes) -> Path:
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{target.name}.", dir=directory)
    temporary = Path(temporary_name)
    complete = False
    try:
        with os.fdopen(descriptor, "wb") as stream:
            os.fchmod(stream.fileno(), 0o600)
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        complete = True
    finally:
        if not complete:
            temporary.unlink(missing_ok=True)
    return temporary


def _publish(files: list[tuple[Path, bytes]]) -> None:
    directory = files[0][0].parent
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    staged: list[tuple[Path, Path]] = []
    try:
        for target, content in files:
            staged.append((_stage(directory, target, content), target))
        for temporary, target in staged:
            os.replace(temporary, target)
        descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        for temporary, _target in staged:
            temporary.unlink(missing_ok=True)


def build(repository: Path, output_directory: Path, *, treeish: str = "HEAD") -> dict[str, object]:
    """Build the archive from ``treeish`` and publish it atomically as ``output_directory/docker-test-toolkit.pyz``.

    The archive holds ``__main__.py`` and the committed ``docker_test_toolkit`` package, nothing else. Returns the
    version, protocols, size and SHA-256 of the archive.

    Raises:
        ValueError: The tree lacks this recipe, its inputs are dirty or the ownership manifest contradicts them.
    """
    repository = repository.resolve(strict=True)
    commit = _git(repository, "rev-parse", f"{treeish}^{{commit}}").decode().strip()
    if _git(repository, "show", f"{commit}:{_RECIPE}") != _RUNNING_RECIPE:
        raise ValueError("Selected tree does not contain the running build recipe.")
    blobs = _tree_blobs(repository, commit)
    entries = _ownership(repository, commit, blobs)
    _require_clean(repository, entries)
    version, protocols = _committed_identity(blobs[_MODELS][0], "TOOLKIT_VERSION")
    members = [
        ("__main__.py", _MAIN, 0o644),
        *(
            (PurePosixPath(entry.path).relative_to("src").as_posix(), blobs[entry.path][0], entry.mode)
            for entry in entries
            if entry.path.startswith(f"{_LIFECYCLE_NAMESPACE}/")
        ),
    ]
    artifact = _archive_bytes(members)
    _publish([(output_directory.resolve() / ARCHIVE_NAME, artifact)])
    return {"protocols": protocols, "sha256": _sha256(artifact), "size": len(artifact), "version": version}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    command = subparsers.add_parser("build")
    command.add_argument("--repository", type=Path, default=Path.cwd())
    command.add_argument("--output-directory", type=Path, required=True)
    command.add_argument("--tree", default="HEAD")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Build the archive and print its identity as one JSON line."""
    arguments = _parser().parse_args(argv)
    try:
        output = build(arguments.repository, arguments.output_directory, treeish=arguments.tree)
    except (OSError, ValueError) as error:
        print(str(error), file=sys.stderr)
        return 2
    print(json.dumps(output, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
