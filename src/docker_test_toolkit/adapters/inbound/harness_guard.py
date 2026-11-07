"""Coding-assistant hook: refuse shell commands that change Docker state directly.

`docker-test-toolkit guard docker-command` reads one pre-tool-use hook payload on stdin: the shell command in
`tool_input.command`, answered with the deny reply `hookSpecificOutput.permissionDecision: "deny"` that the common
coding-assistant harnesses share (docs/adopters.md, "Harness guard"). Any other harness or a Git hook uses
`--exit-code` (payload on stdin) or `--command TEXT`: a refusal exits 2 with the reason on stderr. An allowed command
prints nothing and exits 0. An unreadable payload is allowed: a broken hook input must not stop the assistant's work.
"""

import argparse
import json
import sys
from typing import TextIO

from docker_test_toolkit.domain.command_guard import OVERRIDE_MARKER, direct_docker_change

_MAX_PAYLOAD = 1024 * 1024


def refusal_reason(change: str) -> str:
    """Return the reason shown to the assistant for one refused direct Docker change."""
    return (
        f"`{change}` changes Docker state directly. Docker work goes through the Docker Test Toolkit: the repository's "
        "adapter or the `docker-test-toolkit` command. Reading state (ps, inspect, logs, info, buildx ls, compose ps) "
        f"stays allowed. Only on the user's explicit go add `# {OVERRIDE_MARKER}` to the command."
    )


def hook_command(payload: str) -> str:
    """Return the shell command of a pre-tool-use hook payload, or an empty string when it carries none."""
    try:
        value = json.loads(payload)
    except ValueError:
        return ""
    tool_input = value.get("tool_input") if isinstance(value, dict) else None
    command = tool_input.get("command") if isinstance(tool_input, dict) else None
    return command if isinstance(command, str) else ""


def deny_reply(reason: str) -> str:
    """Return the shared pre-tool-use deny reply."""
    return json.dumps(
        {
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": "deny",
                "permissionDecisionReason": reason,
            }
        }
    )


def main(
    argv: list[str], stdin: TextIO | None = None, stdout: TextIO | None = None, stderr: TextIO | None = None
) -> int:
    """Check one command and answer in the harness's form; return the process exit code."""
    stdin, stdout, stderr = stdin or sys.stdin, stdout or sys.stdout, stderr or sys.stderr
    parser = argparse.ArgumentParser(prog="docker-test-toolkit guard docker-command", exit_on_error=False)
    parser.add_argument("--exit-code", action="store_true")
    parser.add_argument("--command")
    try:
        args = parser.parse_args(argv)
    except argparse.ArgumentError as error:
        print(f"guard: {error}", file=stderr)
        return 64
    command = args.command if args.command is not None else hook_command(stdin.read(_MAX_PAYLOAD))
    change = direct_docker_change(command)
    if change is None:
        return 0
    if args.exit_code or args.command is not None:
        print(refusal_reason(change), file=stderr)
        return 2
    print(deny_reply(refusal_reason(change)), file=stdout)
    return 0
