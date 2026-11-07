"""The frozen Python corpus and wire transcript run against the build under test.

Both fixtures under ``tests/fixtures/contract`` only gain cases. Responses are checked by ``ok``, the closed category
set and required result keys, never by exact key sets, so additive fields and values never break them.
"""

from __future__ import annotations

import contextlib
import json
import os
import queue
import subprocess
import threading
from pathlib import Path

import pytest
from tests.e2e.conftest import _REDIS, _base_python, redis_id

from docker_test_toolkit.config.installation import installation_of

pytestmark = pytest.mark.e2e
CONTRACT = Path(__file__).parents[1] / "fixtures" / "contract"
CATEGORIES = {
    "invalid_input",
    "incompatible_state",
    "contention",
    "infrastructure_failure",
    "test_failure",
    "incomplete_cleanup",
}
RESPONSE_SECONDS = 300


def _root(archive: Path) -> Path:
    installation = installation_of(archive)
    assert installation is not None
    return installation.root


def test_python_baseline_corpus_passes(tmp_path: Path, session_installation: Path, redis_image: None) -> None:
    del redis_image
    project = tmp_path / "project"
    project.mkdir()

    result = subprocess.run(
        [
            _base_python(),
            "-I",
            str(CONTRACT / "python_baseline.py"),
            str(_root(session_installation)),
            str(project),
            redis_id(),
        ],
        cwd=project,
        env={"PATH": os.environ.get("PATH", ""), "TMPDIR": str(tmp_path)},
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.splitlines()[-1] == "corpus: ok"


def _substituted(value: object, bindings: dict[str, object]) -> object:
    """Replace every string ``"$name"``, value or key, by its bound value, at any depth."""
    if isinstance(value, str) and value.startswith("$"):
        return bindings[value[1:]]
    if isinstance(value, dict):
        return {str(_substituted(key, bindings)): _substituted(item, bindings) for key, item in value.items()}
    if isinstance(value, list):
        return [_substituted(item, bindings) for item in value]
    return value


def _at(value: object, path: list[str]) -> object:
    for step in path:
        assert isinstance(value, dict), path
        value = value[step]
    return value


class _Bridge:
    """One serial bridge of the installed launcher, driven a request at a time with a bounded wait per reply."""

    def __init__(self, launcher: Path, kind: str, cwd: Path) -> None:
        self.process = subprocess.Popen(
            [str(launcher), "bridge", kind],
            cwd=cwd,
            env={"PATH": os.environ.get("PATH", ""), "TMPDIR": str(cwd)},
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        self.replies: queue.Queue[str] = queue.Queue()
        self.reader = threading.Thread(target=self._read, name=f"bridge-{kind}-reader")
        self.reader.start()

    def _read(self) -> None:
        assert self.process.stdout is not None
        for line in self.process.stdout:
            self.replies.put(line)
        self.replies.put("")  # end of stream

    def send(self, request: dict) -> dict:
        assert self.process.stdin is not None
        self.process.stdin.write(json.dumps(request) + "\n")
        self.process.stdin.flush()
        line = self.replies.get(timeout=RESPONSE_SECONDS)
        assert line, "the bridge closed without a reply"
        return json.loads(line)

    def close(self) -> None:
        """End the bridge at EOF, kill it if it does not end in time, then prove the process and its reader ended."""
        with contextlib.suppress(OSError):  # a bridge that already died has a broken stdin
            if self.process.stdin is not None:
                self.process.stdin.close()
        try:
            self.process.wait(timeout=120)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=30)
        self.reader.join(timeout=30)  # the child's stdout is closed now, so the reader reaches end of stream
        assert not self.reader.is_alive(), "the bridge reader did not end"
        if self.process.stdout is not None:
            self.process.stdout.close()


def _operator(launcher: Path, command: list[str], payload: object, cwd: Path) -> tuple[dict, int]:
    """Run one operator command of the installed launcher with its input file; return its envelope and exit code."""
    request = cwd / f"operator-{'-'.join(command)}.json"
    request.write_text(json.dumps(payload), encoding="utf-8")
    completed = subprocess.run(
        [str(launcher), *command, "--input", str(request), "--json"],
        cwd=cwd,
        env={"PATH": os.environ.get("PATH", ""), "TMPDIR": str(cwd)},
        capture_output=True,
        text=True,
        timeout=RESPONSE_SECONDS,
        check=False,
    )
    return json.loads(completed.stdout), completed.returncode


def _start_caller_and_target(bindings: dict[str, object], project: Path) -> None:
    """Start two owned containers through the run's own docker command, as a Compose stack does."""
    compose = Path(str(bindings["private_root"])) / "compose.yml"
    service = (
        f"    image: {_REDIS}\n"
        "    mem_limit: 256m\n"
        "    cpus: 0.5\n"
        "    command: [redis-server, --save, '', --appendonly, 'no']\n"
    )
    compose.write_text(f"services:\n  caller:\n{service}  target:\n{service}", encoding="utf-8")
    docker = [str(bindings["docker_executable"])]
    environment = {"PATH": str(bindings["docker_path"])}

    def run(*arguments: str) -> str:
        completed = subprocess.run(
            [*docker, *arguments], cwd=project, env=environment, capture_output=True, text=True, check=False
        )
        assert completed.returncode == 0, completed.stderr
        return completed.stdout.strip()

    run("compose", "--file", str(compose), "up", "--detach")
    for name in ("caller", "target"):
        bindings[f"{name}_id"] = container = run("compose", "--file", str(compose), "ps", "--quiet", name)
        started_at, restart_count = run(
            "container", "inspect", "--format", "{{.State.StartedAt}} {{.RestartCount}}", container
        ).split()
        bindings[f"{name}_started_at"], bindings[f"{name}_restart_count"] = started_at, int(restart_count)


def test_wire_baseline_transcript_passes(tmp_path: Path, session_installation: Path, redis_image: None) -> None:
    del redis_image
    project = tmp_path / "project"
    project.mkdir()
    root = _root(session_installation)
    launcher = root / "bin" / "docker-test-toolkit"
    registry = root / "state"
    bindings: dict[str, object] = {"project_root": str(project), "redis_id": redis_id(), "registry": str(registry)}
    bridges: dict[str, _Bridge] = {}
    try:
        for number, line in enumerate((CONTRACT / "wire_baseline.jsonl").read_text().splitlines(), start=1):
            entry = json.loads(line)
            if entry.get("action") == "start-caller-and-target":
                _start_caller_and_target(bindings, project)
                continue
            exit_code = None  # a bridge answers on its stream; only a one-shot command has an exit code
            if entry["bridge"] == "operator":
                response, exit_code = _operator(
                    launcher, entry["command"], _substituted(entry["input"], bindings), tmp_path
                )
            else:
                bridge = bridges.get(entry["bridge"]) or bridges.setdefault(
                    entry["bridge"], _Bridge(launcher, entry["bridge"], project)
                )
                response = bridge.send(_substituted(entry["request"], bindings))
            expect = entry["expect"]
            assert response["ok"] is expect["ok"], f"line {number}: {response}"
            if not response["ok"]:
                assert response["error"]["category"] in CATEGORIES
                assert response["error"]["category"] == expect["category"], f"line {number}: {response}"
                context = response["error"]["context"]
                assert set(expect.get("context_keys", ())) <= set(context), f"line {number}: {response}"
                if "exit_code" in expect:
                    assert exit_code == expect["exit_code"], f"line {number}: {exit_code}"
                for item in context.get("runs", ()):
                    assert set(item) == set(expect["runs_item_keys"]), f"line {number}: {item}"
                continue
            missing = set(expect.get("result_keys", ())) - set(response["result"])
            assert not missing, f"line {number}: result lacks {sorted(missing)}"
            for name, path in entry.get("bind", {}).items():
                bindings[name] = _at(response, path)
            if "run_id" in bindings:
                bindings["record"] = str(registry / "runs" / f"{bindings['run_id']}.json")
    finally:
        failures = []
        for bridge in bridges.values():  # every bridge is closed, even when an earlier one fails to
            try:
                bridge.close()
            except (AssertionError, OSError, subprocess.SubprocessError) as error:
                failures.append(error)
        assert not failures, failures


@pytest.mark.parametrize(
    "script",
    ["#!/bin/sh\nexec cat >/dev/null\n", "#!/bin/sh\nexit 3\n"],
    ids=["ends-at-eof", "already-dead"],
)
def test_bridge_teardown_ends_the_process_and_its_reader(tmp_path: Path, script: str) -> None:
    command = tmp_path / "bridge-command"
    command.write_text(script)
    command.chmod(0o755)
    bridge = _Bridge(command, "lease", tmp_path)

    bridge.close()

    assert bridge.process.poll() is not None and not bridge.reader.is_alive()
