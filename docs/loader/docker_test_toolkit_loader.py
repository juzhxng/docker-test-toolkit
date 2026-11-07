"""Load the machine-installed Docker Test Toolkit. Project-owned; it never needs to change."""

import importlib
import importlib.machinery
import importlib.util
import os
import pwd
import sys
from pathlib import Path
from types import ModuleType

HOME = Path(pwd.getpwuid(os.getuid()).pw_dir)
ROOTS = (HOME / ".local/share/docker-test-toolkit", Path("/opt/docker-test-toolkit"))


def load_toolkit(roots: tuple[Path, ...] = ROOTS) -> tuple[ModuleType, Path]:
    """Return ``(toolkit module, archive path)``; the first load in a process fixes its version."""
    if "docker_test_toolkit" in sys.modules:
        return _imported(roots)
    root = next((root for root in roots if (root / "current").exists()), None)
    if root is None:
        looked_up = " or ".join(str(root / "current") for root in roots)
        raise RuntimeError(f"Docker Test Toolkit is not installed: no {looked_up}")
    version_folder = (root / "current").resolve(strict=True)
    if version_folder.parent != root.resolve():  # `current` names a version folder of this installation, nothing else
        raise RuntimeError(f"Docker Test Toolkit is not installed: {version_folder} is outside {root}")
    archive = version_folder / "docker-test-toolkit.pyz"
    if archive.is_symlink():  # the installed file itself, never a link to another copy
        raise RuntimeError(f"Docker Test Toolkit is not installed: {archive} is a link")
    if not archive.is_file():
        raise RuntimeError(f"Docker Test Toolkit is not installed: {archive} is missing")
    spec = importlib.machinery.PathFinder.find_spec("docker_test_toolkit", [str(archive)])  # this archive only
    package = str(archive / "docker_test_toolkit" / "__init__.py")
    if spec is None or spec.loader is None or spec.origin != package:
        raise RuntimeError(f"Docker Test Toolkit is not installed: {archive} holds no toolkit")
    sys.path.insert(0, str(archive))
    toolkit = importlib.util.module_from_spec(spec)
    sys.modules["docker_test_toolkit"] = toolkit
    try:
        spec.loader.exec_module(toolkit)
    except BaseException:
        _forget(archive)
        raise
    return toolkit, archive


def _imported(roots: tuple[Path, ...]) -> tuple[ModuleType, Path]:
    """Return the toolkit this process already imported, if it came from an installation's version folder."""
    toolkit = importlib.import_module("docker_test_toolkit")
    origin = toolkit.__file__
    archive = Path(origin).parents[1] if origin else Path()
    installed = {root.resolve() for root in roots}
    version_folder = archive.parent
    if (
        archive.name != "docker-test-toolkit.pyz"
        or archive.is_symlink()
        or not archive.is_file()  # the installed archive itself, never an unpacked folder of that name
        or version_folder.is_symlink()  # `current` is a moving alias, never the version this process runs
        or version_folder.parent.resolve() not in installed
    ):
        raise RuntimeError(f"Docker Test Toolkit was not imported from an installation: {origin}")
    return toolkit, archive


def _forget(archive: Path) -> None:
    """Remove every trace of a failed import, so a later load cannot mix it into another version."""
    for name in [name for name in sys.modules if name.partition(".")[0] == "docker_test_toolkit"]:
        del sys.modules[name]
    if str(archive) in sys.path:
        sys.path.remove(str(archive))
