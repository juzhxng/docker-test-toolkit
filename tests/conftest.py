from __future__ import annotations

import ctypes
import json
import os
import pwd
import shutil
import subprocess
import sys
import time
from collections.abc import Generator, Iterator
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[1]
_EXT_SUPER_MAGIC = 0xEF53


def pytest_configure(config: pytest.Config) -> None:
    """Adopt orphaned descendants on Linux so a test can reap a process it killed.

    The toolkit never counts a zombie as dead. macOS reparents an orphan to ``launchd``, which reaps it at once; a CI
    container's idle PID 1 never does, so a SIGKILLed orphan would stay a zombie forever. As a child subreaper this
    process receives such orphans and ``account_fixtures.reap_orphan`` reaps the ones a test killed.
    """
    if sys.platform.startswith("linux"):
        libc = ctypes.CDLL(None, use_errno=True)
        if libc.prctl(36, 1, 0, 0, 0) != 0:  # PR_SET_CHILD_SUBREAPER
            raise pytest.UsageError(f"Cannot become a child subreaper: errno {ctypes.get_errno()}.")
    guardians = _GuardianSession()
    config.pluginmanager.register(_GuardianTeardown(guardians), "toolkit-guardian-teardown")
    config.pluginmanager.register(_GuardianCensus(guardians), "toolkit-guardian-census")


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--toolkit-guardian-wait",
        type=float,
        default=30.0,
        help="seconds the session guard waits for toolkit guardians to exit (test-only; inner test sessions pass 1)",
    )


@pytest.fixture(scope="session", autouse=True)
def _toolkit_temporary_parent(tmp_path_factory: pytest.TempPathFactory) -> Iterator[None]:
    """Put every run folder this session's toolkit code creates inside pytest's own rotated temporary area.

    Run roots follow ``TMPDIR``, and a test run that never finishes (a fake
    Docker run whose cleanup stays unresolved) used to leave its root in the system temp folder for good. This sets no
    service up and changes no test's subject, only where the process's temporary files go: a harness boundary.
    """
    previous = os.environ.get("TMPDIR")
    os.environ["TMPDIR"] = str(tmp_path_factory.mktemp("toolkit-tmp", numbered=False))
    yield
    if previous is None:
        os.environ.pop("TMPDIR", None)
    else:
        os.environ["TMPDIR"] = previous


# tests that enter a private run and never leave it kept its guardian
# alive until pytest exited. These are hook wrappers, not autouse
# fixtures; they set nothing up and inherit nothing, and end only runs the test itself started under its own tmp_path.
_TMP_PATH = pytest.StashKey[Path]()


class _GuardianSession:
    """What the teardown and the census share for one session."""

    def __init__(self) -> None:
        self.test_paths: set[Path] = set()  # the tmp_path of every non-e2e test
        self.e2e_roots: set[Path] = set()  # e2e items' tmp_paths and the private roots built while they run


class _GuardianTeardown:
    """Withdraw the runs a non-e2e test left active under its tmp_path, after its own fixtures are torn down."""

    def __init__(self, session: _GuardianSession) -> None:
        self._session = session

    @pytest.hookimpl(wrapper=True)
    def pytest_runtest_setup(self, item: pytest.Item) -> Generator[None, None, None]:
        try:
            return (yield)
        finally:
            tmp_path = getattr(item, "funcargs", {}).get("tmp_path")  # present only once the setup has run
            if isinstance(tmp_path, Path):
                if item.get_closest_marker("e2e") is None:
                    item.stash[_TMP_PATH] = tmp_path
                    self._session.test_paths.add(tmp_path)
                else:
                    self._session.e2e_roots.add(tmp_path)

    @pytest.hookimpl(wrapper=True)
    def pytest_runtest_teardown(self, item: pytest.Item) -> Generator[None, None, None]:
        try:
            return (yield)
        finally:
            if item.get_closest_marker("e2e") is None and _TMP_PATH in item.stash:
                from docker_test_toolkit.factories.testing import end_test_runs

                end_test_runs(item.stash[_TMP_PATH])

    @pytest.hookimpl(wrapper=True)
    def pytest_runtest_protocol(self, item: pytest.Item) -> Generator[None, None, object]:
        if item.get_closest_marker("e2e") is None:
            return (yield)
        from docker_test_toolkit.factories import testing

        before = set(testing._BUILT_ROOTS)
        try:
            return (yield)
        finally:
            self._session.e2e_roots |= set(testing._BUILT_ROOTS) - before


class _GuardianCensus:
    """Fail the session when a toolkit guardian outlived the tests, naming each one (the session guard)."""

    def __init__(self, session: _GuardianSession) -> None:
        self._session = session

    def pytest_sessionfinish(self, session: pytest.Session) -> None:
        from docker_test_toolkit.factories import testing

        excluded = set(self._session.e2e_roots)
        roots = (set(testing._BUILT_ROOTS) - excluded) | self._session.test_paths
        deadline = time.monotonic() + session.config.getoption("--toolkit-guardian-wait")
        while True:
            survivors = testing.surviving_test_guardians(parent=os.getpid(), roots=roots, excluded=excluded)
            if not survivors or time.monotonic() >= deadline:
                break
            time.sleep(0.2)
        if survivors:
            reporter = session.config.pluginmanager.get_plugin("terminalreporter")
            lines = ["toolkit guardians outlived the tests (session guard, TK-121):", *survivors]
            for line in lines:
                if reporter is not None:
                    reporter.write_line(line, red=True)
                else:
                    print(line, file=sys.stderr)
            session.exitstatus = pytest.ExitCode.TESTS_FAILED


@pytest.fixture(scope="session", autouse=True)
def fixture_creation_mask() -> Iterator[None]:
    """Own the default fixture permissions independently of the invoking runner."""
    previous = os.umask(0o022)
    try:
        yield
    finally:
        os.umask(previous)


class _LinuxStatFs(ctypes.Structure):
    _fields_ = [
        ("f_type", ctypes.c_long),
        ("f_bsize", ctypes.c_long),
        ("f_blocks", ctypes.c_ulong),
        ("f_bfree", ctypes.c_ulong),
        ("f_bavail", ctypes.c_ulong),
        ("f_files", ctypes.c_ulong),
        ("f_ffree", ctypes.c_ulong),
        ("f_fsid", ctypes.c_int * 2),
        ("f_namelen", ctypes.c_long),
        ("f_frsize", ctypes.c_long),
        ("f_flags", ctypes.c_long),
        ("f_spare", ctypes.c_long * 4),
    ]


def _inside(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def _require_linux_ext_basetemp(config: pytest.Config, items: list[pytest.Item]) -> None:
    if not any(
        item.get_closest_marker("integration") is not None or item.get_closest_marker("e2e") is not None
        for item in items
    ):
        return
    configured = config.getoption("basetemp")
    if not sys.platform.startswith("linux"):
        if configured is not None:
            Path(configured).resolve().mkdir(mode=0o700, parents=True, exist_ok=True)
        return
    if configured is None:
        raise pytest.UsageError("Integration and E2E tests require an explicit --basetemp on ext-family storage.")
    basetemp = Path(configured).resolve()
    repository = ROOT.resolve()
    account_home = Path(pwd.getpwuid(os.getuid()).pw_dir).resolve(strict=True)
    if not (_inside(basetemp, repository) or _inside(basetemp, account_home)):
        raise pytest.UsageError("Integration and E2E --basetemp must be inside the repository or account home.")
    basetemp.mkdir(mode=0o700, parents=True, exist_ok=True)
    descriptor = os.open(basetemp, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        value = _LinuxStatFs()
        libc = ctypes.CDLL(None, use_errno=True)
        libc.fstatfs.argtypes = [ctypes.c_int, ctypes.POINTER(_LinuxStatFs)]
        libc.fstatfs.restype = ctypes.c_int
        if libc.fstatfs(descriptor, ctypes.byref(value)) != 0:
            raise pytest.UsageError("Integration and E2E --basetemp filesystem could not be classified.")
        if value.f_type != _EXT_SUPER_MAGIC:
            raise pytest.UsageError("Integration and E2E tests require ext-family --basetemp storage on Linux.")
    finally:
        os.close(descriptor)


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Require supported Linux storage only for real-boundary test processes."""
    _require_linux_ext_basetemp(config, items)


def _git(repository: Path, *arguments: str) -> str:
    result = subprocess.run(
        ["git", *arguments],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


@pytest.fixture
def synthetic_distribution_repository(tmp_path: Path) -> Path:
    """Commit the working tree's release inputs in an isolated repository: the archive source for build tests."""
    repository = tmp_path / "distribution-source"
    ignored = shutil.ignore_patterns("__pycache__", "*.pyc", "*.pyo")
    shutil.copytree(ROOT / "src/docker_test_toolkit", repository / "src/docker_test_toolkit", ignore=ignored)
    (repository / "scripts").mkdir()
    shutil.copy2(ROOT / "scripts/build_zipapp.py", repository / "scripts/build_zipapp.py")
    shutil.copy2(ROOT / "component-ownership.json", repository / "component-ownership.json")
    shutil.copy2(ROOT / "pyproject.toml", repository / "pyproject.toml")
    _git(repository, "init", "-q")
    _git(repository, "add", ".")
    _git(
        repository,
        "-c",
        "user.name=Fixture",
        "-c",
        "user.email=fixture@example.invalid",
        "commit",
        "-qm",
        "fixture",
    )
    ownership = repository / "component-ownership.json"
    assert ownership.is_file()
    for entry in json.loads(ownership.read_bytes())["entries"]:
        assert _git(repository, "ls-files", "--error-unmatch", entry["path"])
    return repository


@pytest.fixture
def real_account(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Stand in for an installation's real account; the source tree has none, so its refusals need one to refuse.

    A harness seam (the account is derived from the running archive's path, which a source-tree test cannot have):
    every module that reads ``canonical_account`` sees this path, and so do ``account_directory`` and
    ``registry_directory``.
    """
    from docker_test_toolkit.application import lifecycle, state
    from docker_test_toolkit.factories import testing

    account = tmp_path / "installed" / "state" / "test-admission"
    for module in (lifecycle, state, testing):
        monkeypatch.setattr(module, "canonical_account", lambda: account)
    return account
