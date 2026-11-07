"""Machine-readable command-line adapter."""

from __future__ import annotations

import argparse
import json
import os
import pwd
import sys
from pathlib import Path
from typing import Literal, NoReturn

from docker_test_toolkit.application.installation import command_link_path, install
from docker_test_toolkit.application.operator import execute
from docker_test_toolkit.application.protocol import error_envelope, success_envelope
from docker_test_toolkit.application.state import RUNNING_ARCHIVE
from docker_test_toolkit.config.installation import platform_root
from docker_test_toolkit.domain.errors import IncompleteCleanup, InvalidInput, ToolkitError
from docker_test_toolkit.domain.records import unique_object
from docker_test_toolkit.factories.cli import build_docker_adapter

_MAX_INPUT = 1024 * 1024
_COMMANDS = {
    ("readiness", "inspect"),
    ("run", "status"),
    ("run", "recover"),
    ("run", "release"),
    ("resource", "observe"),
    ("cleanup", "project"),
    ("cleanup", "session"),
    ("project", "retire"),
    ("project", "timeline"),
    ("cache", "remove"),
}


_EXECUTABLE_PROJECT_COMMANDS = {("project", "retire"), ("cache", "remove")}


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        """Translate argparse exits into the stable machine error protocol."""
        del message
        raise InvalidInput("Command arguments are invalid.")


def _read_input(path: str | None) -> dict:
    if path is None:
        return {}
    try:
        if path == "-":
            content = sys.stdin.buffer.read(_MAX_INPUT + 1)
        else:
            with Path(path).open("rb") as stream:
                content = stream.read(_MAX_INPUT + 1)
    except OSError as error:
        raise InvalidInput("Command input is unreadable.") from error
    if len(content) > _MAX_INPUT:
        raise InvalidInput("Command input exceeds its fixed bound.")
    try:
        value = json.loads(content, object_pairs_hook=unique_object)
    except (RecursionError, UnicodeDecodeError, ValueError) as error:
        raise InvalidInput("Command input is invalid JSON.") from error
    if not isinstance(value, dict):
        raise InvalidInput("Command input must be a JSON object.")
    return value


def _parse(argv: list[str] | None) -> tuple[str, str | None, bool]:
    values = list(sys.argv[1:] if argv is None else argv)
    if not values:
        values = ["readiness", "inspect", "--json"]
    parser = _ArgumentParser(description=__doc__, add_help=False)
    parser.add_argument("family")
    parser.add_argument("action", nargs="?")
    parser.add_argument("--input")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--json", action="store_true", required=True)
    args = parser.parse_args(values)
    if args.family == "protocol" and args.action is None:
        if args.input is not None or args.execute:
            raise InvalidInput("Protocol does not accept input or mutation flags.")
        return "protocol", None, False
    if (args.family, args.action) not in _COMMANDS:
        raise InvalidInput("Operator command is unsupported.")
    if (args.family, args.action) == ("readiness", "inspect"):
        if args.input is not None or args.execute:
            raise InvalidInput("Readiness inspection does not accept input or mutation flags.")
        return "readiness.inspect", None, False
    if args.input is None:
        raise InvalidInput("Operator command requires --input JSON.")
    if args.execute and args.family != "cleanup" and (args.family, args.action) not in _EXECUTABLE_PROJECT_COMMANDS:
        raise InvalidInput("--execute applies only to cleanup, project retire and cache remove commands.")
    return f"{args.family}.{args.action}", args.input, args.execute


def main(argv: list[str] | None = None) -> int:
    """Execute one bounded command and emit exactly one JSON document."""
    command = "unknown"
    try:
        command, input_path, execute_cleanup = _parse(argv)
        result = execute(
            command,
            _read_input(input_path),
            docker_factory=build_docker_adapter,
            execute_cleanup=execute_cleanup,
        )
        cleanup = None
        exit_code = 0
        if command.startswith("cleanup.") and execute_cleanup and result.get("clean") is not True:
            cleanup_error, _ = error_envelope(command, IncompleteCleanup("Exact cleanup remains incomplete."))
            cleanup = {"ok": False, "error": cleanup_error["error"]}
            exit_code = 7
        response = success_envelope(command, result, cleanup=cleanup)
    except (ToolkitError, OSError) as error:
        response, exit_code = error_envelope(command, error)
    print(json.dumps(response, sort_keys=True, separators=(",", ":")))
    return exit_code


def _parallel_runs_argument(value: str | None) -> int | Literal["auto"] | None:
    """Parse ``--parallel-runs N|auto``: a decimal integer of at least 1, or ``auto`` for no count cap."""
    if value is None:
        return None
    if value == "auto":
        return "auto"
    if not value.isdecimal() or int(value) < 1:
        raise InvalidInput("--parallel-runs takes an integer of at least 1 or auto.")
    return int(value)


def install_main(argv: list[str]) -> int:
    """Install the running archive (``python3 -I docker-test-toolkit.pyz install --json``) and emit one JSON document.

    ``--root`` exists for tests only: the loader and the launcher know only the platform's fixed root.
    """
    command = "install"
    try:
        parser = argparse.ArgumentParser(prog="docker-test-toolkit install", add_help=False, exit_on_error=False)
        parser.add_argument("--json", action="store_true", required=True)
        parser.add_argument("--host-reserve-bytes", type=int)
        parser.add_argument("--disk-reserve-bytes", type=int)
        parser.add_argument("--parallel-runs")
        parser.add_argument("--share-scope", choices=("machine", "repository"))
        parser.add_argument("--python", type=Path)
        parser.add_argument("--root", type=Path)
        try:
            arguments = parser.parse_args(argv)
        except (argparse.ArgumentError, SystemExit) as error:
            raise InvalidInput(
                "install accepts --json, --host-reserve-bytes, --disk-reserve-bytes, --parallel-runs, --share-scope,"
                " --python and --root."
            ) from error
        parallel_runs = _parallel_runs_argument(arguments.parallel_runs)
        if RUNNING_ARCHIVE is None:
            raise InvalidInput(
                "install runs from the archive it installs: python3 -I docker-test-toolkit.pyz install --json."
            )
        home = Path(pwd.getpwuid(os.getuid()).pw_dir)
        test_root = arguments.root is not None
        root = arguments.root if test_root else platform_root(home, sys.platform)
        link = None if test_root else command_link_path(home, sys.platform)
        result = install(
            RUNNING_ARCHIVE,
            root=root,
            python=arguments.python or Path(sys.executable),
            host_reserve_bytes=arguments.host_reserve_bytes,
            disk_reserve_bytes=arguments.disk_reserve_bytes,
            parallel_runs=parallel_runs,
            share_scope=arguments.share_scope,
            command_link=link,
            test_root=test_root,
            docker_factory=build_docker_adapter,
        )
        response, exit_code = success_envelope(command, result), 0
    except (ToolkitError, OSError) as error:
        response, exit_code = error_envelope(command, error)
    print(json.dumps(response, sort_keys=True, separators=(",", ":")))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
