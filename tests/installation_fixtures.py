"""Build real toolkit archives from the working tree (or a committed tree) and install them into temporary roots."""

from __future__ import annotations

import io
import os
import re
import shutil
import subprocess
import sys
import tarfile
import zipfile
from collections.abc import Sequence
from pathlib import Path

from docker_test_toolkit.application.installation import install
from docker_test_toolkit.domain.models import TOOLKIT_VERSION

ROOT = Path(__file__).parents[1]
_INPUTS = ("scripts/build_zipapp.py", "component-ownership.json", "pyproject.toml")
_MODELS = "docker_test_toolkit/domain/models.py"
FIRST_INSTALLED_RELEASE = "0.5.0"
# Hosted CI sets a smaller host reserve for the test installations than a developer machine keeps.
TEST_HOST_RESERVE_ENV = "DOCKER_TEST_TOOLKIT_TEST_HOST_RESERVE_BYTES"


def _git(repository: Path, *arguments: str) -> None:
    subprocess.run(["git", *arguments], cwd=repository, check=True, capture_output=True)


def _copy_tree(tree: str, repository: Path) -> None:
    """Copy the release inputs of a committed toolkit tree (``git archive``) into the repository."""
    archived = subprocess.run(
        ["git", "archive", "--format=tar", tree, "src/docker_test_toolkit", *_INPUTS],
        cwd=ROOT,
        check=True,
        capture_output=True,
    ).stdout
    with tarfile.open(fileobj=io.BytesIO(archived)) as members:
        members.extractall(repository, filter="data")


def build_archive(
    tmp: Path, *, version: str | None = None, tree: str | None = None, variant: str | None = None
) -> Path:
    """Build ``docker-test-toolkit.pyz`` exactly as a release does, from the working tree or from ``tree``.

    The inputs are committed into a disposable repository under ``tmp`` (the builder refuses dirty inputs); ``version``
    rewrites ``TOOLKIT_VERSION`` there first, so one source can stand for several releases; ``variant`` appends a
    comment, which changes the bytes and nothing else. Returns the archive.
    """
    repository = tmp / "archive-source"
    repository.mkdir(parents=True)
    if tree is None:
        ignored = shutil.ignore_patterns("__pycache__", "*.pyc", "*.pyo")
        shutil.copytree(ROOT / "src/docker_test_toolkit", repository / "src/docker_test_toolkit", ignore=ignored)
        (repository / "scripts").mkdir()
        for name in _INPUTS:
            shutil.copy2(ROOT / name, repository / name)
    else:
        _copy_tree(tree, repository)
    models = repository / "src/docker_test_toolkit/domain/models.py"
    if version is not None:
        text, count = re.subn(
            r'^TOOLKIT_VERSION = "[^"]*"$', f'TOOLKIT_VERSION = "{version}"', models.read_text(), count=1, flags=re.M
        )
        assert count == 1, "TOOLKIT_VERSION was not found"
        models.write_text(text)
    if variant is not None:
        models.write_text(models.read_text() + f"# variant: {variant}\n")
    _git(repository, "init", "-q")
    _git(repository, "add", ".")
    _git(repository, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-qm", "fixture")
    output = tmp / "archive-output"
    subprocess.run(
        [
            sys.executable,
            str(repository / "scripts/build_zipapp.py"),
            "build",
            "--repository",
            str(repository),
            "--output-directory",
            str(output),
            "--tree",
            "HEAD",
        ],
        check=True,
        capture_output=True,
    )
    return output / "docker-test-toolkit.pyz"


def base_python() -> Path:
    """The base interpreter (outside any virtual environment) the tests install with."""
    return Path(sys._base_executable).resolve()


def temporary_installation(tmp: Path, archives: Sequence[Path], *, python: Path | None = None) -> Path:
    """Install each archive in order into ``tmp/root`` (a test root) and return the root; the last one is current.

    ``DOCKER_TEST_TOOLKIT_TEST_HOST_RESERVE_BYTES``, when set, is the installations' declared host reserve.
    """
    root = tmp / "root"
    declared = os.environ.get(TEST_HOST_RESERVE_ENV)
    for archive in archives:
        install(
            archive,
            root=root,
            python=python or base_python(),
            host_reserve_bytes=int(declared) if declared else None,
            test_root=True,
        )
    return root


def renamed_archive(archive: Path, tmp: Path, version: str) -> Path:
    """Return N': a copy of ``archive`` whose ``TOOLKIT_VERSION`` reads ``version`` (other bytes, same implementation).

    Rewrites only the ``domain/models.py`` member, so it works for a release-built archive as well.
    """
    target = tmp / "renamed" / "docker-test-toolkit.pyz"
    target.parent.mkdir(parents=True)
    with zipfile.ZipFile(archive) as source, zipfile.ZipFile(target, "w") as copy:
        for member in source.infolist():
            data = source.read(member)
            if member.filename == _MODELS:
                text, count = re.subn(
                    r'^TOOLKIT_VERSION = "[^"]*"$', f'TOOLKIT_VERSION = "{version}"', data.decode(), count=1, flags=re.M
                )
                assert count == 1, "TOOLKIT_VERSION was not found"
                data = text.encode()
            copy.writestr(member, data)
    return target


def _release(version: str) -> tuple[int, ...] | None:
    """Return a version's numeric release triple (a pre-release such as ``0.5.0-rc.1`` counts as its release)."""
    match = re.match(r"(\d+)\.(\d+)\.(\d+)", version)
    return None if match is None else tuple(int(part) for part in match.groups())


def previous_release_archive(tmp: Path) -> Path | None:
    """Build the newest installed-era release older than this tree from its tag, with the tag's own builder.

    Returns None only while this tree is the first installed-era release. Later, a checkout without the tag fails the
    test instead of silently testing N against N' alone (the CI checks out full history).
    """
    first, current = _release(FIRST_INSTALLED_RELEASE), _release(TOOLKIT_VERSION)
    assert first is not None and current is not None
    tags = subprocess.run(["git", "tag", "--list", "v*"], cwd=ROOT, check=True, capture_output=True, text=True)
    releases = [
        (key, name)
        for name in tags.stdout.split()
        if (key := _release(name.removeprefix("v"))) is not None and first <= key < current
    ]
    if not releases:
        assert current <= first, f"no release tag between v{FIRST_INSTALLED_RELEASE} and {TOOLKIT_VERSION}; fetch tags"
        return None
    return build_archive(tmp, tree=max(releases)[1])
