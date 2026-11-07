"""The installation boundary from the source tree: private compositions run without an installation."""

from __future__ import annotations

import hashlib
import json
import os
import stat
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

from docker_test_toolkit.application import admission, lifecycle, probe, sharing
from docker_test_toolkit.application.installation import install
from docker_test_toolkit.application.state import canonical_account
from docker_test_toolkit.config.installation import ARCHIVE_NAME, Installation, installation_of
from docker_test_toolkit.domain.errors import IncompatibleState, InvalidInput, ResourceError
from docker_test_toolkit.factories.testing import build_test_services, install_failpoint, install_lock_observer
from tests.installation_fixtures import TEST_HOST_RESERVE_ENV, base_python, build_archive, temporary_installation
from tests.integration.account_fixtures import EnterThread, queue_entries, queue_entry, seed_dead_since, write_v2_queue
from tests.integration.test_docker_receipts import IMAGE_ID, _configure_fake_docker, _spec, _stateful_fake_docker
from tests.integration.test_takeover import _dead_granted_holder, _empty_spec, _two_projects

pytestmark = pytest.mark.integration


def test_private_composition_runs_from_the_source_tree_without_an_installation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The guardian, a Python consumer and a shim Docker call all dispatch from the source tree with no installation."""
    assert canonical_account() is None  # the source tree belongs to no installation
    project = tmp_path / "project"
    project.mkdir()
    binaries = tmp_path / "bin"
    binaries.mkdir()
    _stateful_fake_docker(binaries)
    monkeypatch.setenv("PATH", f"{binaries}{os.pathsep}{os.environ['PATH']}")
    handle = (
        build_test_services(tmp_path / "state")
        .open_run(_spec(project), docker_factory=_configure_fake_docker(binaries))
        .enter()
    )
    assert handle.status().state == "active"

    docker = handle.docker_context
    created = subprocess.run(
        [docker.executable, "run", IMAGE_ID, "true"],
        env=dict(docker.environment),
        capture_output=True,
        text=True,
        check=False,
    )
    child, consumer_id = handle.launch_python_consumer(
        "code",
        "print('consumer ran')",
        (),
        ("fixture",),
        cwd=project,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    out, err = child.communicate(timeout=60)
    assert (child.returncode, out) == (0, b"consumer ran\n"), err
    handle.complete_consumer(consumer_id)
    final = handle.leave("passed")

    assert created.returncode == 0, created.stderr
    assert (final.state, final.cleanup_outcome) == ("complete", "complete")


def test_private_recovery_child_runs_from_the_source_tree(tmp_path: Path) -> None:
    """A dead holder's takeover worker (``_recover … --test-directory``) recovers it from the source tree."""
    services = build_test_services(tmp_path / "state")
    account = services.root / "admission"
    first, second = _two_projects(tmp_path)
    dead = _dead_granted_holder(services, first)
    seed_dead_since(account, dead.run_id, age_s=31)
    waiter_handle = services.open_run(_empty_spec(second, "waiter"))

    waiter = EnterThread(waiter_handle)
    waiter.join(60)  # bounded: the queued guardian spawns the worker, the worker recovers, the waiter is granted

    assert waiter.error is None
    record = json.loads(dead.record.read_text())
    assert (record["state"], record["recovery_outcome"]) == ("complete", "complete")
    assert dead.run_id not in queue_entries(account)
    waiter_handle.leave()


def test_open_run_on_a_newer_format_account_is_refused(tmp_path: Path) -> None:
    """An installation whose account anchor says format 2 refuses ``open_run`` before anything is written."""
    root = temporary_installation(tmp_path, [build_archive(tmp_path / "a")])
    archive = (root / "current").resolve() / ARCHIVE_NAME
    account = root / "state" / "test-admission"
    (account / "metadata.lock").write_text("docker-test-toolkit-state:2\n")
    project = tmp_path / "project"
    project.mkdir()
    before = sorted(path.relative_to(root) for path in root.rglob("*"))
    script = (
        "import json, sys\n"
        f"sys.path.insert(0, {str(archive)!r})\n"
        "from docker_test_toolkit import PhaseBudget, RunSpec\n"
        "from docker_test_toolkit.application.lifecycle import source_identity\n"
        "from docker_test_toolkit.domain.errors import ToolkitError\n"
        "from docker_test_toolkit.domain.models import SUPPORTED_PROTOCOLS, TOOLKIT_VERSION, ArtifactIdentity\n"
        "from docker_test_toolkit.factories.api import open_run\n"
        "artifact = ArtifactIdentity(TOOLKIT_VERSION, source_identity(), dict(SUPPORTED_PROTOCOLS))\n"
        f"spec = RunSpec({str(project)!r}, 'integration', 'newer', (), (), 'fixture', "
        "{'tests': PhaseBudget(0, 1, 1, 1)}, 'tests', artifact)\n"
        "try:\n"
        "    open_run(spec)\n"
        "except ToolkitError as error:\n"
        "    print(json.dumps({'type': type(error).__name__, 'reason': getattr(error, 'reason', None)}))\n"
    )

    result = subprocess.run([sys.executable, "-I", "-c", script], capture_output=True, text=True, check=False)

    assert result.stdout.strip(), result.stderr
    assert json.loads(result.stdout) == {"type": IncompatibleState.__name__, "reason": "state-format-newer"}
    assert sorted(path.relative_to(root) for path in root.rglob("*")) == before  # nothing was written


# ---- the installer and the launcher --------------------------------------------------------------------------

GIB = 1024**3


def _install(archive: Path, root: Path, *arguments: str, python: Path | None = None) -> subprocess.CompletedProcess:
    """Run ``install`` the documented way: the archive installs itself (``python -I archive install --json``)."""
    interpreter = python or base_python()
    return subprocess.run(
        [str(interpreter), "-I", str(archive), "install", "--json", "--root", str(root), *arguments],
        capture_output=True,
        text=True,
        check=False,
    )


def _ok(result: subprocess.CompletedProcess) -> dict:
    assert result.returncode == 0, result.stdout + result.stderr
    response = json.loads(result.stdout)
    assert response["ok"] is True, response
    return response["result"]


def _error(result: subprocess.CompletedProcess) -> dict:
    assert result.returncode != 0, result.stdout
    return json.loads(result.stdout)["error"]


def test_install_places_an_immutable_version_folder_and_switches_current(tmp_path: Path) -> None:
    archive = build_archive(tmp_path / "a", version="9.9.1")
    root = tmp_path / "root"

    result = _ok(_install(archive, root))

    folder = root / "9.9.1"
    installed = folder / "docker-test-toolkit.pyz"
    assert result["version"] == "9.9.1" and result["previous"] is None and result["root"] == str(root)
    assert installed.read_bytes() == archive.read_bytes()
    assert stat.S_IMODE(installed.stat().st_mode) == 0o444 and stat.S_IMODE(folder.stat().st_mode) == 0o555
    assert os.readlink(root / "current") == "9.9.1"
    launched = subprocess.run(
        [str(root / "bin/docker-test-toolkit"), "protocol", "--json"], capture_output=True, text=True
    )
    assert launched.returncode == 0, launched.stderr
    assert json.loads(launched.stdout)["result"]["version"] == "9.9.1"


def test_install_switches_and_rolls_back_between_versions(tmp_path: Path) -> None:
    older, newer = build_archive(tmp_path / "a", version="9.9.1"), build_archive(tmp_path / "b", version="9.9.2")
    root = tmp_path / "root"
    _ok(_install(older, root))

    upgraded = _ok(_install(newer, root))
    rolled_back = _ok(_install(root / "9.9.1" / "docker-test-toolkit.pyz", root))

    assert (upgraded["previous"], upgraded["version"]) == ("9.9.1", "9.9.2")
    assert (rolled_back["previous"], rolled_back["version"]) == ("9.9.2", "9.9.1")
    assert os.readlink(root / "current") == "9.9.1"


def test_install_refuses_different_bytes_for_an_installed_version(tmp_path: Path) -> None:
    root = tmp_path / "root"
    _ok(_install(build_archive(tmp_path / "a", version="9.9.1"), root))
    before = (root / "9.9.1" / "docker-test-toolkit.pyz").read_bytes()
    different = build_archive(tmp_path / "b", version="9.9.1", variant="other bytes")

    error = _error(_install(different, root))

    assert "version folders are immutable" in error["message"]
    assert (root / "9.9.1" / "docker-test-toolkit.pyz").read_bytes() == before
    assert os.readlink(root / "current") == "9.9.1"


def test_reinstall_of_the_same_version_keeps_current(tmp_path: Path) -> None:
    archive = build_archive(tmp_path / "a", version="9.9.1")
    root = tmp_path / "root"
    _ok(_install(archive, root))
    link = (root / "current").lstat()

    again = _ok(_install(archive, root))

    assert again["previous"] == "9.9.1" and again["version"] == "9.9.1"
    assert (root / "current").lstat().st_ino == link.st_ino


def test_install_refuses_a_virtual_environment_interpreter(tmp_path: Path) -> None:
    archive = build_archive(tmp_path / "a", version="9.9.1")
    venv = tmp_path / "venv"
    subprocess.run([str(base_python()), "-m", "venv", "--without-pip", str(venv)], check=True)

    error = _error(_install(archive, tmp_path / "root", python=venv / "bin" / "python"))

    assert "virtual environment" in error["message"]
    assert not (tmp_path / "root" / "current").exists()


def test_install_refuses_a_non_platform_root_without_test_flag(tmp_path: Path) -> None:
    archive = build_archive(tmp_path / "a", version="9.9.1")

    with pytest.raises(ResourceError, match="platform's installation root"):
        install(archive, root=tmp_path / "root", python=base_python())

    assert not (tmp_path / "root").exists()


def test_launcher_runs_the_current_version_and_reports_a_missing_interpreter(tmp_path: Path) -> None:
    archive = build_archive(tmp_path / "a", version="9.9.1")
    interpreter = tmp_path / "python3"
    interpreter.symlink_to(base_python())  # a test-owned link; only the link is removed below, never an interpreter
    root = tmp_path / "root"
    _ok(_install(archive, root, "--python", str(interpreter)))
    launcher = root / "bin" / "docker-test-toolkit"
    assert subprocess.run([str(launcher), "protocol", "--json"], capture_output=True).returncode == 0

    interpreter.unlink()
    missing = subprocess.run([str(launcher), "protocol", "--json"], capture_output=True, text=True)

    assert missing.returncode == 5
    assert "reinstall the toolkit" in missing.stderr


def test_install_records_the_host_reserve_and_the_guardian_reads_it(tmp_path: Path) -> None:
    archive = build_archive(tmp_path / "a", version="9.9.1")
    root = tmp_path / "root"
    account = Installation(root, root / "9.9.1", root / "9.9.1" / ARCHIVE_NAME).account_directory

    _ok(_install(archive, root, "--host-reserve-bytes", str(2 * GIB)))
    assert sharing.read_host_reserve(account) == 2 * GIB
    _ok(_install(archive, root, "--host-reserve-bytes", str(3 * GIB)))  # the operator's setting: replaced
    assert sharing.read_host_reserve(account) == 3 * GIB
    error = _error(_install(archive, root, "--host-reserve-bytes", str(GIB // 2)))

    assert "1 GiB" in error["message"]
    assert sharing.read_host_reserve(account) == 3 * GIB


def test_install_records_the_disk_reserve(tmp_path: Path) -> None:
    archive = build_archive(tmp_path / "a", version="9.9.1")
    root = tmp_path / "root"
    account = Installation(root, root / "9.9.1", root / "9.9.1" / ARCHIVE_NAME).account_directory

    _ok(_install(archive, root, "--disk-reserve-bytes", str(2 * GIB)))
    assert sharing.read_disk_reserve(account) == 2 * GIB
    _ok(_install(archive, root))  # an install without the option keeps it
    assert sharing.read_disk_reserve(account) == 2 * GIB
    error = _error(_install(archive, root, "--disk-reserve-bytes", str(GIB - 1)))

    assert error["message"] == "A disk reserve must be at least 1 GiB."
    assert sharing.read_disk_reserve(account) == 2 * GIB


def test_install_failure_before_switch_changes_neither_current_nor_the_reserve(tmp_path: Path) -> None:
    older, newer = build_archive(tmp_path / "a", version="9.9.1"), build_archive(tmp_path / "b", version="9.9.2")
    root = tmp_path / "root"
    install(older, root=root, python=base_python(), test_root=True, host_reserve_bytes=2 * GIB)
    reserve = root / "state" / "test-admission" / "host-reserve.json"
    before = (os.readlink(root / "current"), reserve.read_bytes())

    def crash(stage: str) -> None:
        if stage == "install-before-switch":
            raise RuntimeError("injected install failure")

    install_failpoint(crash)
    try:
        with pytest.raises(RuntimeError, match="injected"):
            install(newer, root=root, python=base_python(), test_root=True, host_reserve_bytes=4 * GIB)
    finally:
        install_failpoint(None)

    assert (os.readlink(root / "current"), reserve.read_bytes()) == before
    install(newer, root=root, python=base_python(), test_root=True, host_reserve_bytes=4 * GIB)  # a rerun completes
    assert os.readlink(root / "current") == "9.9.2"
    assert sharing.read_host_reserve(reserve.parent) == 4 * GIB


def test_install_does_not_deadlock_on_its_own_metadata_lock(tmp_path: Path) -> None:
    archive = build_archive(tmp_path / "a", version="9.9.1")
    root = tmp_path / "root"
    _ok(_install(archive, root))
    command = [str(base_python()), "-I", str(archive), "install", "--json", "--root", str(root)]

    finished = subprocess.run(command, capture_output=True, text=True, timeout=30, check=False)

    assert finished.returncode == 0, finished.stdout + finished.stderr


# ---- identity and the load path ------------------------------------------------------------------------------

_OPEN_RUN = (
    "import json, sys\n"
    "sys.path.insert(0, sys.argv[1])\n"
    "from docker_test_toolkit import PhaseBudget, RunSpec, toolkit_identity\n"
    "from docker_test_toolkit.factories.api import open_run\n"
    "from docker_test_toolkit.domain.errors import ToolkitError\n"
    "spec = RunSpec(sys.argv[2], 'integration', 'load-path', (), (), 'fixture', "
    "{'tests': PhaseBudget(0, 1, 1, 1)}, 'tests', toolkit_identity())\n"
    "try:\n"
    "    open_run(spec)\n"
    "    print(json.dumps({'opened': True}))\n"
    "except ToolkitError as error:\n"
    "    print(json.dumps({'category': error.category, 'reason': error.reason}))\n"
)


def _open_run_from(archive: Path, project: Path) -> dict:
    result = subprocess.run(
        [str(base_python()), "-I", "-c", _OPEN_RUN, str(archive), str(project)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.stdout.strip(), result.stderr
    return json.loads(result.stdout)


def test_protocol_reports_the_archive_digest(tmp_path: Path) -> None:
    archive = build_archive(tmp_path / "a", version="9.9.1")
    root = temporary_installation(tmp_path, [archive])

    launched = subprocess.run([str(root / "bin/docker-test-toolkit"), "protocol", "--json"], capture_output=True)

    assert launched.returncode == 0, launched.stderr
    digest = hashlib.sha256((root / "9.9.1" / ARCHIVE_NAME).read_bytes()).hexdigest()
    assert json.loads(launched.stdout)["result"]["source_identity"] == digest


def test_import_through_current_is_refused_for_the_real_account(tmp_path: Path) -> None:
    root = temporary_installation(tmp_path, [build_archive(tmp_path / "a", version="9.9.1")])
    project = tmp_path / "project"
    project.mkdir()
    runs = root / "state" / "runs"

    refused = _open_run_from(root / "current" / ARCHIVE_NAME, project)

    assert refused == {"category": "incompatible_state", "reason": "toolkit-load-unresolved"}
    assert not runs.exists() or not any(runs.iterdir())  # nothing was recorded


def test_loose_archive_is_refused_for_the_real_account(tmp_path: Path) -> None:
    archive = build_archive(tmp_path / "a", version="9.9.1")  # built, never installed
    project = tmp_path / "project"
    project.mkdir()

    refused = _open_run_from(archive, project)

    assert refused == {"category": "incompatible_state", "reason": "toolkit-not-installed"}
    assert sorted(path.name for path in archive.parent.iterdir()) == [ARCHIVE_NAME]


# ---- the reference loader ------------------------------------------------------------------------------------

LOADER = Path(__file__).parents[2] / "docs" / "loader"


def _load(script: str, *arguments: str) -> list[str]:
    """Run ``script`` with the reference loader importable; return its printed lines."""
    preamble = f"import sys; sys.path.insert(0, {str(LOADER)!r})\nimport docker_test_toolkit_loader as loader\n"
    result = subprocess.run(
        [str(base_python()), "-I", "-c", preamble + script, *arguments],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout.splitlines()


def test_loader_imports_the_resolved_version(tmp_path: Path) -> None:
    root = temporary_installation(tmp_path, [build_archive(tmp_path / "a", version="9.9.1")])

    module, archive = _load(
        "from pathlib import Path\nm, a = loader.load_toolkit((Path(sys.argv[1]),))\nprint(m.__file__); print(a)\n",
        str(root),
    )

    expected = (root / "9.9.1" / ARCHIVE_NAME).resolve()
    assert archive == str(expected)
    assert module.startswith(str(expected) + os.sep)


def test_loader_names_both_roots_when_missing(tmp_path: Path) -> None:
    first, second = tmp_path / "home-root", tmp_path / "opt-root"

    (message,) = _load(
        "from pathlib import Path\n"
        "try:\n"
        "    loader.load_toolkit((Path(sys.argv[1]), Path(sys.argv[2])))\n"
        "except RuntimeError as error:\n"
        "    print(error)\n",
        str(first),
        str(second),
    )

    assert str(first / "current") in message and str(second / "current") in message
    assert "not installed" in message


def test_loader_refuses_a_toolkit_imported_from_outside_an_installation(tmp_path: Path) -> None:
    loose = build_archive(tmp_path / "loose", version="9.9.0")
    root = temporary_installation(tmp_path, [build_archive(tmp_path / "a", version="9.9.1")])

    (message,) = _load(
        "from pathlib import Path\n"
        "sys.path.insert(0, sys.argv[1]); import docker_test_toolkit\n"
        "try:\n"
        "    loader.load_toolkit((Path(sys.argv[2]),))\n"
        "except RuntimeError as error:\n"
        "    print(error)\n",
        str(loose),
        str(root),
    )

    assert "not imported from an installation" in message and str(loose) in message


def test_loader_keeps_its_version_across_a_switch(tmp_path: Path) -> None:
    root = temporary_installation(
        tmp_path, [build_archive(tmp_path / "b", version="9.9.2"), build_archive(tmp_path / "a", version="9.9.1")]
    )

    first, second, same = _load(
        "import os\n"
        "from pathlib import Path\n"
        "root = Path(sys.argv[1])\n"
        "m1, a1 = loader.load_toolkit((root,))\n"
        "(root / '.switch').symlink_to('9.9.2'); os.replace(root / '.switch', root / 'current')\n"
        "m2, a2 = loader.load_toolkit((root,))\n"
        "print(a1); print(a2); print(m1 is m2)\n",
        str(root),
    )

    assert first == second == str((root / "9.9.1" / ARCHIVE_NAME).resolve())
    assert same == "True"
    assert os.readlink(root / "current") == "9.9.2"  # the switch happened; the process kept its version


# ---- corrections ------------------------------------------------------------------------------------------

_OPEN_RETAINED = (
    "import json, sys\n"
    "sys.path.insert(0, sys.argv[1])\n"
    "from docker_test_toolkit import PhaseBudget, RunSpec\n"
    "from docker_test_toolkit.factories.api import open_retained_run\n"
    "from docker_test_toolkit.domain.errors import ToolkitError\n"
    "spec = RunSpec(sys.argv[2], 'integration', 'retained', (), (), 'fixture', "
    "{'tests': PhaseBudget(0, 1, 1, 1)}, 'tests', retain=True)\n"
    "try:\n"
    "    open_retained_run(spec, sys.argv[2] + '/locator.json')\n"
    "    print(json.dumps({'opened': True}))\n"
    "except ToolkitError as error:\n"
    "    print(json.dumps({'category': error.category, 'reason': error.reason}))\n"
)


@pytest.mark.parametrize("load", ["through-current", "loose"])
def test_open_retained_run_derives_the_real_account(tmp_path: Path, load: str) -> None:
    """``open_retained_run`` is refused like ``open_run``: through ``current`` and outside an installation."""
    archive = build_archive(tmp_path / "a", version="9.9.1")
    if load == "through-current":
        archive = temporary_installation(tmp_path, [archive]) / "current" / ARCHIVE_NAME
    project = tmp_path / "project"
    project.mkdir()

    result = subprocess.run(
        [str(base_python()), "-I", "-c", _OPEN_RETAINED, str(archive), str(project)],
        capture_output=True,
        text=True,
        check=False,
    )

    reason = "toolkit-load-unresolved" if load == "through-current" else "toolkit-not-installed"
    assert json.loads(result.stdout) == {"category": "incompatible_state", "reason": reason}, result.stderr


def _newer_format_installation(tmp_path: Path) -> tuple[Path, Path]:
    root = temporary_installation(tmp_path, [build_archive(tmp_path / "a", version="9.9.1")])
    (root / "state" / "test-admission" / "metadata.lock").write_text("docker-test-toolkit-state:2\n")
    return root, (root / "current").resolve() / ARCHIVE_NAME


@pytest.mark.parametrize(
    "arguments",
    [
        pytest.param(["bridge", "lease"], id="bridge-lease"),
        pytest.param(["bridge", "batch"], id="bridge-batch"),
        pytest.param(["run", "status", "--input", "absent.json", "--json"], id="operator"),
        pytest.param(["_recover", "--context", "{}", "--attempt", "a"], id="recover"),
        pytest.param(["_python_child", "--request", "absent.json"], id="python-child"),
        pytest.param(
            ["_docker_dispatch", "--descriptor", "d", "--dispatcher", "x", "--shim", "s", "--", "--test-directory"],
            id="docker-dispatch-with-a-user-argument",
        ),
        pytest.param(
            ["_guardian", "--context", "{}", "--", "--test-directory=/private"], id="guardian-option-after-dashes"
        ),
    ],
)
def test_cli_start_check_refuses_every_real_account_entry_on_a_newer_format(
    tmp_path: Path, arguments: list[str]
) -> None:
    root, archive = _newer_format_installation(tmp_path)
    anchor = (root / "state" / "test-admission" / "metadata.lock").read_bytes()

    result = subprocess.run(
        [str(base_python()), "-I", str(archive), *arguments], capture_output=True, text=True, stdin=subprocess.DEVNULL
    )

    response = json.loads(result.stdout.splitlines()[0])
    assert (response["error"]["category"], response["error"]["context"]["reason"]) == (
        "incompatible_state",
        "state-format-newer",
    )
    assert result.returncode != 0
    assert (root / "state" / "test-admission" / "metadata.lock").read_bytes() == anchor


def _state_bytes(root: Path) -> dict[str, bytes]:
    return {str(path.relative_to(root)): path.read_bytes() for path in (root / "state").rglob("*") if path.is_file()}


def test_x5_install_refuses_a_newer_format_account_without_touching_state_or_current(tmp_path: Path) -> None:
    root, _archive = _newer_format_installation(tmp_path)
    before = _state_bytes(root)

    with pytest.raises(IncompatibleState) as refused:
        install(build_archive(tmp_path / "b", version="9.9.2"), root=root, python=base_python(), test_root=True)

    assert refused.value.reason == "state-format-newer"
    assert os.readlink(root / "current") == "9.9.1"
    assert _state_bytes(root) == before


@pytest.mark.parametrize(
    ("arguments", "stdin"),
    [
        pytest.param(["cleanup", "project", "--input", "-", "--json"], '{"project_root": "PROJECT"}', id="cleanup"),
        pytest.param(["bridge", "lease"], '{"command": "run.prepare", "input": {}}\n', id="bridge-lease"),
    ],
)
def test_x5_launcher_entries_refuse_a_newer_format_account_without_writing(
    tmp_path: Path, arguments: list[str], stdin: str
) -> None:
    root, _archive = _newer_format_installation(tmp_path)
    project = tmp_path / "project"
    project.mkdir()
    before = _state_bytes(root)

    result = subprocess.run(
        [str(root / "bin" / "docker-test-toolkit"), *arguments],
        input=stdin.replace("PROJECT", str(project)),
        capture_output=True,
        text=True,
        cwd=project,
        check=False,
    )

    response = json.loads(result.stdout.splitlines()[0])
    assert response["error"]["context"]["reason"] == "state-format-newer"
    assert result.returncode != 0
    assert _state_bytes(root) == before


@pytest.mark.parametrize("spelling", [["--test-directory", "PRIVATE"], ["--test-directory=PRIVATE"]])
def test_cli_start_check_lets_a_private_child_through(tmp_path: Path, spelling: list[str]) -> None:
    _root, archive = _newer_format_installation(tmp_path)
    private = tmp_path / "private"
    private.mkdir(mode=0o700)
    arguments = [value.replace("PRIVATE", str(private)) for value in spelling]

    result = subprocess.run(
        [str(base_python()), "-I", str(archive), "_guardian", "--context", "{}", *arguments],
        capture_output=True,
        text=True,
        stdin=subprocess.DEVNULL,
    )

    assert "state-format-newer" not in result.stdout + result.stderr  # it reached the guardian, which refuses "{}"
    assert result.returncode != 0


def test_loader_refuses_a_missing_archive_even_with_another_copy_on_the_path(tmp_path: Path) -> None:
    root = tmp_path / "root"
    (root / "9.9.1").mkdir(parents=True)  # current resolves, but no archive is there
    (root / "current").symlink_to("9.9.1")
    elsewhere = Path(__file__).parents[2] / "src"  # another importable copy of the toolkit

    (message,) = _load(
        "from pathlib import Path\n"
        "sys.path.append(sys.argv[2])\n"
        "try:\n"
        "    loader.load_toolkit((Path(sys.argv[1]),))\n"
        "except RuntimeError as error:\n"
        "    print(error)\n",
        str(root),
        str(elsewhere),
    )

    assert "is missing" in message and str((root / "9.9.1").resolve()) in message


def test_loader_never_runs_another_copy_when_the_archive_is_unusable(tmp_path: Path) -> None:
    root = tmp_path / "root"
    (root / "9.9.1").mkdir(parents=True)
    (root / "9.9.1" / ARCHIVE_NAME).write_bytes(b"not a zip archive")  # present, but holds no toolkit
    (root / "current").symlink_to("9.9.1")
    other = tmp_path / "other" / "docker_test_toolkit"
    other.mkdir(parents=True)
    (other / "__init__.py").write_text("print('OTHER COPY RAN')\n")

    lines = _load(
        "from pathlib import Path\n"
        "sys.path.append(sys.argv[2])\n"
        "for attempt in range(2):\n"
        "    try:\n"
        "        loader.load_toolkit((Path(sys.argv[1]),))\n"
        "    except RuntimeError as error:\n"
        "        print(error)\n",
        str(root),
        str(other.parent),
    )

    assert "OTHER COPY RAN" not in lines
    assert len(lines) == 2 and all("holds no toolkit" in line for line in lines)


def test_loader_never_reuses_a_toolkit_imported_through_current(tmp_path: Path) -> None:
    root = temporary_installation(tmp_path, [build_archive(tmp_path / "a", version="9.9.1")])

    (message,) = _load(
        "from pathlib import Path\n"
        "sys.path.insert(0, str(Path(sys.argv[1]) / 'current' / 'docker-test-toolkit.pyz'))\n"
        "import docker_test_toolkit\n"
        "try:\n"
        "    loader.load_toolkit((Path(sys.argv[1]),))\n"
        "except RuntimeError as error:\n"
        "    print(error)\n",
        str(root),
    )

    assert "not imported from an installation" in message  # a moving alias, never a fixed version folder


def test_loader_never_runs_a_spec_from_outside_the_archive(tmp_path: Path) -> None:
    root = temporary_installation(tmp_path, [build_archive(tmp_path / "a", version="9.9.1")])
    other = tmp_path / "other" / "docker_test_toolkit"
    other.mkdir(parents=True)
    (other / "__init__.py").write_text("print('OTHER COPY RAN')\n")

    lines = _load(
        "import importlib.machinery\n"
        "from pathlib import Path\n"
        "foreign = importlib.machinery.FileFinder(sys.argv[2], (importlib.machinery.SourceFileLoader, ['.py']))\n"
        "sys.path_hooks.insert(0, lambda entry: foreign)  # a hook that answers every path with another copy\n"
        "sys.path_importer_cache.clear()\n"
        "try:\n"
        "    loader.load_toolkit((Path(sys.argv[1]),))\n"
        "except RuntimeError as error:\n"
        "    print(error)\n",
        str(root),
        str(other.parent),
    )

    assert "OTHER COPY RAN" not in lines
    assert len(lines) == 1 and "holds no toolkit" in lines[0]


# ---- corrections ---------------------------------------------------------------------------------------


@pytest.mark.parametrize("target", ["loose", "foreign", "archive-link"])
def test_loader_never_runs_an_archive_outside_its_own_root(tmp_path: Path, target: str) -> None:
    root = temporary_installation(tmp_path, [build_archive(tmp_path / "a", version="9.9.1")])
    if target == "loose":
        elsewhere = build_archive(tmp_path / "loose", version="9.9.0").parent  # a valid archive, never installed
    elif target == "foreign":
        foreign = temporary_installation(tmp_path / "other", [build_archive(tmp_path / "b", version="9.9.0")])
        elsewhere = (foreign / "9.9.0").resolve()  # another installation's version folder
    else:
        elsewhere = build_archive(tmp_path / "loose", version="9.9.0")  # linked in place of the installed file
        (root / "9.9.0").mkdir()
        (root / "9.9.0" / ARCHIVE_NAME).symlink_to(elsewhere)
        elsewhere = root / "9.9.0" / ARCHIVE_NAME
    if target != "archive-link":
        (root / ".switch").symlink_to(elsewhere)
    else:
        (root / ".switch").symlink_to("9.9.0")
    os.replace(root / ".switch", root / "current")

    lines = _load(
        "from pathlib import Path\n"
        "try:\n"
        "    loader.load_toolkit((Path(sys.argv[1]),))\n"
        "except RuntimeError as error:\n"
        "    print(error)\n"
        "print(any(name.partition('.')[0] == 'docker_test_toolkit' for name in sys.modules))\n",
        str(root),
    )

    assert len(lines) == 2 and "is not installed" in lines[0] and str(elsewhere) in lines[0]
    assert lines[1] == "False"  # refused before any toolkit code ran


def test_loader_never_reuses_a_toolkit_imported_through_a_linked_archive(tmp_path: Path) -> None:
    root = temporary_installation(tmp_path, [build_archive(tmp_path / "a", version="9.9.1")])
    (root / "9.9.0").mkdir()
    (root / "9.9.0" / ARCHIVE_NAME).symlink_to(build_archive(tmp_path / "loose", version="9.9.0"))

    (message,) = _load(
        "from pathlib import Path\n"
        "sys.path.insert(0, str(Path(sys.argv[1]) / '9.9.0' / 'docker-test-toolkit.pyz'))\n"
        "import docker_test_toolkit\n"
        "try:\n"
        "    loader.load_toolkit((Path(sys.argv[1]),))\n"
        "except RuntimeError as error:\n"
        "    print(error)\n",
        str(root),
    )

    assert "not imported from an installation" in message


def test_loader_never_reuses_a_toolkit_imported_from_a_directory_named_like_the_archive(tmp_path: Path) -> None:
    root = temporary_installation(tmp_path, [build_archive(tmp_path / "a", version="9.9.1")])
    unpacked = tmp_path / "unpacked"
    with zipfile.ZipFile(build_archive(tmp_path / "loose", version="9.9.0")) as bundle:
        bundle.extractall(unpacked)
    (root / "9.9.0" / ARCHIVE_NAME).mkdir(parents=True)  # a folder, not the installed file
    (root / "9.9.0" / ARCHIVE_NAME / "docker_test_toolkit").symlink_to(unpacked / "docker_test_toolkit")

    (message,) = _load(
        "from pathlib import Path\n"
        "sys.path.insert(0, str(Path(sys.argv[1]) / '9.9.0' / 'docker-test-toolkit.pyz'))\n"
        "import docker_test_toolkit\n"
        "try:\n"
        "    loader.load_toolkit((Path(sys.argv[1]),))\n"
        "except RuntimeError as error:\n"
        "    print(error)\n",
        str(root),
    )

    assert "not imported from an installation" in message


def _failing_archive(directory: Path) -> Path:
    """An archive whose package imports one submodule and then fails, as an interrupted import would."""
    directory.mkdir(parents=True)
    archive = directory / ARCHIVE_NAME
    with zipfile.ZipFile(archive, "w") as bundle:
        bundle.writestr(
            "docker_test_toolkit/__init__.py", "from docker_test_toolkit import part\nraise OSError('cut')\n"
        )
        bundle.writestr("docker_test_toolkit/part.py", "BROKEN = True\n")
    return archive


def test_loader_failure_leaves_no_toolkit_module_or_path_behind(tmp_path: Path) -> None:
    root = temporary_installation(tmp_path, [build_archive(tmp_path / "a", version="9.9.1")])
    (root / "9.9.0").mkdir()
    broken = _failing_archive(tmp_path / "broken")
    os.link(broken, root / "9.9.0" / ARCHIVE_NAME)
    (root / ".switch").symlink_to("9.9.0")
    os.replace(root / ".switch", root / "current")

    lines = _load(
        "import os\n"
        "from pathlib import Path\n"
        "root = Path(sys.argv[1])\n"
        "try:\n"
        "    loader.load_toolkit((root,))\n"
        "except OSError as error:\n"
        "    print(error)\n"
        "print(sorted(name for name in sys.modules if name.partition('.')[0] == 'docker_test_toolkit'))\n"
        "print(any('9.9.0' in entry for entry in sys.path))\n"
        "(root / '.switch').symlink_to('9.9.1'); os.replace(root / '.switch', root / 'current')\n"
        "toolkit, archive = loader.load_toolkit((root,))\n"
        "print(toolkit.toolkit_identity().version, 'docker_test_toolkit.part' in sys.modules)\n",
        str(root),
    )

    assert lines == ["cut", "[]", "False", "9.9.1 False"]


@pytest.mark.parametrize("where", ["account", "inside", "parent"])
def test_host_proof_publication_refuses_any_directory_overlapping_the_real_account(
    real_account: Path, where: str
) -> None:
    directory = {"account": real_account, "inside": real_account / "proof", "parent": real_account.parent}[where]
    directory.mkdir(parents=True, exist_ok=True)

    with pytest.raises(ResourceError, match="Host-proof observations cannot authorize canonical Docker work"):
        lifecycle._publish_grant(  # the check precedes every use of the grant's inputs
            None,
            "attempt",
            None,
            None,
            None,
            None,
            daemon_id="d",
            waits=None,
            reserved_ns=0,
            window_ns=0,
            _test_directory=directory,
        )


def test_reinstall_repairs_a_current_link_that_points_into_another_root(tmp_path: Path) -> None:
    archive = build_archive(tmp_path / "a", version="9.9.1")
    other = temporary_installation(tmp_path / "other", [archive])
    root = temporary_installation(tmp_path, [archive])
    os.replace(_absolute_link(root, other / "9.9.1"), root / "current")  # a same-named folder elsewhere

    install(archive, root=root, python=base_python(), test_root=True)

    assert (root / "current").resolve() == (root / "9.9.1").resolve()


def _absolute_link(root: Path, target: Path) -> Path:
    link = root / ".misdirected"
    link.symlink_to(target)
    return link


def test_install_refuses_a_version_folder_without_its_archive(tmp_path: Path) -> None:
    archive = build_archive(tmp_path / "a", version="9.9.1")
    root = tmp_path / "root"
    (root / "9.9.1").mkdir(parents=True)

    with pytest.raises(ResourceError, match="holds no toolkit archive"):
        install(archive, root=root, python=base_python(), test_root=True)

    assert not (root / "current").exists()


def test_install_reports_the_versions_still_holding_the_slot(tmp_path: Path) -> None:
    root = temporary_installation(tmp_path, [build_archive(tmp_path / "a", version="9.9.1")])
    account = root / "state" / "test-admission"
    holder_id, holder = queue_entry(1, 1, "granted")
    record = tmp_path / "registry" / "runs" / f"{holder_id}.json"
    record.parent.mkdir(parents=True)
    record.write_text(json.dumps({"artifact": {"version": "9.9.0"}}))
    holder.update(record=str(record), registry=str(tmp_path / "registry"))
    write_v2_queue(account, [(holder_id, holder)])
    (account / "metadata.lock").write_text(admission.STATE_MARKER)

    result = install(build_archive(tmp_path / "b", version="9.9.2"), root=root, python=base_python(), test_root=True)

    assert result["holder_versions"] == ["9.9.0"]  # the old version's running test explains why new runs wait


# ---- the per-machine parallel-runs limit (shareable runs) ----------------------------------------------------


def _settings(account: Path) -> tuple[bytes | None, bytes | None]:
    """Return the raw bytes of the account's two machine settings files (None when absent)."""
    reserve, limit = account / "host-reserve.json", account / "parallel-runs.json"
    return (reserve.read_bytes() if reserve.exists() else None, limit.read_bytes() if limit.exists() else None)


def _account_of(root: Path) -> Path:
    return root / "state" / "test-admission"


def test_install_writes_settings_inside_metadata_section_after_switch(tmp_path: Path) -> None:
    older, newer = build_archive(tmp_path / "a", version="9.9.1"), build_archive(tmp_path / "b", version="9.9.2")
    root = tmp_path / "root"
    install(older, root=root, python=base_python(), test_root=True)
    held: list[str] = []
    seen: dict[str, tuple[list[str], str]] = {}

    def observe(event: str, name: str, shared: bool, wait: bool, bounded: bool) -> None:
        if event == "acquire":
            held.append(name)
        elif event == "release" and name in held:
            held.remove(name)

    def stage(name: str) -> None:
        if name in {"install-switched", "install-reserve-written"}:
            seen[name] = (list(held), os.readlink(root / "current"))

    install_lock_observer(observe)
    install_failpoint(stage)
    try:
        install(newer, root=root, python=base_python(), test_root=True, host_reserve_bytes=2 * GIB, parallel_runs=2)
    finally:
        install_failpoint(None)
        install_lock_observer(None)

    assert set(seen) == {"install-switched", "install-reserve-written"}
    assert all("metadata" in locks and current == "9.9.2" for locks, current in seen.values())
    assert sharing.read_host_reserve(_account_of(root)) == 2 * GIB
    assert sharing.read_parallel_runs(_account_of(root)) == 2


def test_install_without_flag_preserves_limit_bytes(tmp_path: Path) -> None:
    archive = build_archive(tmp_path / "a", version="9.9.1")
    root = tmp_path / "root"
    install(archive, root=root, python=base_python(), test_root=True, parallel_runs="auto")
    before = _settings(_account_of(root))

    result = install(archive, root=root, python=base_python(), test_root=True)

    assert _settings(_account_of(root)) == before and before[1] is not None
    assert result["parallel_runs"] == "auto"


def test_install_crash_after_switch_before_settings_rerun_fixes(tmp_path: Path) -> None:
    older, newer = build_archive(tmp_path / "a", version="9.9.1"), build_archive(tmp_path / "b", version="9.9.2")
    root = tmp_path / "root"
    install(older, root=root, python=base_python(), test_root=True, host_reserve_bytes=2 * GIB, parallel_runs=1)
    before = _settings(_account_of(root))

    def crash(stage: str) -> None:
        if stage == "install-switched":
            raise RuntimeError("injected install failure")

    install_failpoint(crash)
    try:
        with pytest.raises(RuntimeError, match="injected"):
            install(newer, root=root, python=base_python(), test_root=True, host_reserve_bytes=3 * GIB, parallel_runs=2)
    finally:
        install_failpoint(None)

    assert os.readlink(root / "current") == "9.9.2" and _settings(_account_of(root)) == before
    install(newer, root=root, python=base_python(), test_root=True, host_reserve_bytes=3 * GIB, parallel_runs=2)
    assert sharing.read_host_reserve(_account_of(root)) == 3 * GIB
    assert sharing.read_parallel_runs(_account_of(root)) == 2


def test_install_crash_between_settings_writes_rerun_fixes(tmp_path: Path) -> None:
    archive = build_archive(tmp_path / "a", version="9.9.1")
    root = tmp_path / "root"
    install(archive, root=root, python=base_python(), test_root=True, host_reserve_bytes=2 * GIB, parallel_runs=1)

    def crash(stage: str) -> None:
        if stage == "install-reserve-written":
            raise RuntimeError("injected install failure")

    install_failpoint(crash)
    try:
        with pytest.raises(RuntimeError, match="injected"):
            install(
                archive, root=root, python=base_python(), test_root=True, host_reserve_bytes=3 * GIB, parallel_runs=2
            )
    finally:
        install_failpoint(None)

    assert sharing.read_host_reserve(_account_of(root)) == 3 * GIB  # the new reserve, the old limit
    assert sharing.read_parallel_runs(_account_of(root)) == 1
    install(archive, root=root, python=base_python(), test_root=True, host_reserve_bytes=3 * GIB, parallel_runs=2)
    assert sharing.read_parallel_runs(_account_of(root)) == 2


@pytest.mark.parametrize(
    ("value", "cli"),
    [(0, None), (-1, None), (True, None), ("two", None), (None, "0"), (None, "two")],
    ids=["api-zero", "api-negative", "api-bool", "api-word", "cli-zero", "cli-word"],
)
def test_install_refuses_invalid_parallel_runs_before_any_write(tmp_path: Path, value: object, cli: str | None) -> None:
    archive = build_archive(tmp_path / "a", version="9.9.1")
    root = tmp_path / "root"

    if cli is None:
        with pytest.raises(InvalidInput, match="parallel-runs"):
            install(archive, root=root, python=base_python(), test_root=True, parallel_runs=value)
    else:
        error = _error(_install(archive, root, "--parallel-runs", cli))
        assert error["category"] == "invalid_input" and "parallel-runs" in error["message"]

    assert not (root / "current").exists() and not (_account_of(root) / "parallel-runs.json").exists()


def test_install_output_reports_parallel_runs(tmp_path: Path) -> None:
    archive = build_archive(tmp_path / "a", version="9.9.1")
    root = tmp_path / "root"

    default = _ok(_install(archive, root))
    auto = _ok(_install(archive, root, "--parallel-runs", "auto"))
    three = _ok(_install(archive, root, "--parallel-runs", "3"))

    assert (default["parallel_runs"], auto["parallel_runs"], three["parallel_runs"]) == ("auto", "auto", 3)
    assert json.loads((_account_of(root) / "parallel-runs.json").read_text()) == {"parallel_runs": 3, "schema": 1}


def test_install_takes_metadata_once(tmp_path: Path) -> None:
    archive = build_archive(tmp_path / "a", version="9.9.1")
    root = tmp_path / "root"
    events: list[tuple[str, str]] = []
    install_lock_observer(lambda event, name, shared, wait, bounded: events.append((event, name)))
    try:
        install(archive, root=root, python=base_python(), test_root=True, host_reserve_bytes=2 * GIB, parallel_runs=2)
    finally:
        install_lock_observer(None)

    metadata = [event for event, name in events if name == "metadata"]
    assert metadata == ["acquire", "release"]


def test_install_makes_the_probe_image_present_on_the_daemon(tmp_path: Path) -> None:
    # Admission never pulls; install and a fresh run's entry do, so the Mac's CPU gate has its probe.
    archive = build_archive(tmp_path / "a", version="9.9.2")

    result = _ok(_install(archive, tmp_path / "root"))

    assert result["probe_image"] in ("present", "pulled")
    inspected = subprocess.run(
        ["docker", "image", "inspect", "--format", "{{.Id}}", probe.PROBE_IMAGE],
        capture_output=True,
        text=True,
        check=False,
    )
    assert inspected.returncode == 0, inspected.stderr


def test_install_runs_retention_and_reports_it(tmp_path: Path) -> None:
    archive = build_archive(tmp_path / "a", version="9.9.3")

    result = _ok(_install(archive, tmp_path / "root"))

    assert result["retention"] is not None and result["retention"]["trigger"] == "install"
    assert set(result["retention"]["kinds"]) >= {"build_cache", "archives", "session_roots"}


def _installed_account(tmp_path: Path) -> Path:
    root = temporary_installation(tmp_path, [build_archive(tmp_path / "build")])
    installation = installation_of((root / "current").resolve() / ARCHIVE_NAME)
    assert installation is not None
    return installation.account_directory


def test_temporary_installation_takes_the_test_host_reserve_from_the_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Hosted CI runners have 8 GB; the 4.5 GiB developer default would leave a shared-runs pair no room.
    monkeypatch.setenv(TEST_HOST_RESERVE_ENV, str(1024**3))

    assert sharing.read_host_reserve(_installed_account(tmp_path)) == 1024**3
