"""The coding-assistant Docker guard: which commands it refuses and how it answers each harness."""

import io
import json

import pytest

from docker_test_toolkit.adapters.inbound.harness_guard import deny_reply, hook_command, refusal_reason
from docker_test_toolkit.adapters.inbound.harness_guard import main as guard_main
from docker_test_toolkit.domain.command_guard import direct_docker_change
from docker_test_toolkit.factories.cli import main


@pytest.mark.parametrize(
    "command",
    [
        'docker ps -a --format "{{.Names}}"',
        "docker --context desktop-linux inspect foo",
        "docker buildx du --verbose",
        "docker compose -p x ps",
        "docker-compose -f a.yml logs web",
        "docker system df",
        "docker logs dtt-1 2>&1 | tail -5",
        "cd docker-test-toolkit && git status",
        'git commit -m "docker run fix"',
        'grep -rn "docker run" src',
        "uv run poe int",
        'ssh ci-host "docker ps"',
        "docker run --rm alpine true # docker-direct-ok",
        "cat > t.py <<'EOF'\nassert '`docker rm`' in err\nEOF",
        "cat <<< 'docker ps'",
        "podman ps -a",
        "podman machine list",
        "podman machine inspect dtt-window --format '{{.Name}}'",
        "podman image ls",
    ],
)
def test_reads_and_unrelated_commands_pass(command: str) -> None:
    assert direct_docker_change(command) is None


@pytest.mark.parametrize(
    ("command", "change"),
    [
        ("docker run --rm alpine true", "docker run"),
        ("docker exec dtt-1 ls", "docker exec"),
        ("docker rm -f abc", "docker rm"),
        ("docker buildx create --name x", "docker buildx create"),
        ("docker compose -p x up -d", "docker compose up"),
        ("docker-compose down", "docker compose down"),
        ("docker system prune -af", "docker system prune"),
        ("docker context use desktop-linux", "docker context use"),
        ("FOO=1 sudo docker kill x", "docker kill"),
        ("ls; docker stop x", "docker stop"),
        ("echo $(docker run alpine true)", "docker run"),
        ("ssh ci-host docker rm -f x", "docker rm"),
        ('bash -c "docker image prune -f"', "docker image prune"),
        ("/usr/local/bin/docker run x", "docker run"),
        ("timeout 30 docker build .", "docker build"),
        ("cat <<EOF > f\nok\nEOF\ndocker rm x", "docker rm"),
        ("podman rm -f abc", "podman rm"),
        ("podman system prune -af", "podman system prune"),
        ("podman volume rm v", "podman volume rm"),
        ("podman machine reset --force", "podman machine reset"),
        ("podman machine rm -f dtt-window", "podman machine rm"),
        ("podman-compose down", "podman compose down"),
        ("sudo -n podman rm -f abc", "podman rm"),
        ("sudo -E docker run --rm alpine true", "docker run"),
        ("sudo -u builder docker rm x", "docker rm"),
        ("env -i docker rm x", "docker rm"),
        ("env -u HOME FOO=1 docker rm x", "docker rm"),
        ("nice -n 5 docker rm x", "docker rm"),
        ("timeout -s KILL 30 docker rm x", "docker rm"),
        ("timeout --kill-after=5 30 docker rm x", "docker rm"),
        ('bash -lc "docker rm x"', "docker rm"),
        ('sh -ec "docker rm x"', "docker rm"),
        ("bash -lc -- 'docker rm x'", "docker rm"),
        ("sudo -nu builder docker rm x", "docker rm"),
        ("env -iu HOME docker rm x", "docker rm"),
        ("env -uFOOu docker rm x", "docker rm"),  # an attached value ending in a value letter
    ],
)
def test_direct_changes_are_named(command: str, change: str) -> None:
    assert direct_docker_change(command) == change


def test_hook_payload_yields_its_command_and_nothing_else() -> None:
    assert hook_command(json.dumps({"tool_input": {"command": "docker rm x"}})) == "docker rm x"
    assert hook_command("not json") == ""
    assert hook_command(json.dumps({"tool_input": {"file_path": "a.py"}})) == ""
    assert hook_command(json.dumps(["docker rm x"])) == ""


def test_deny_reply_has_the_shared_pre_tool_use_shape() -> None:
    reply = json.loads(deny_reply(refusal_reason("docker rm")))
    assert reply["hookSpecificOutput"]["hookEventName"] == "PreToolUse"
    assert reply["hookSpecificOutput"]["permissionDecision"] == "deny"
    assert "`docker rm`" in reply["hookSpecificOutput"]["permissionDecisionReason"]


def _run(argv: list[str], payload: str = "") -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    code = guard_main(argv, stdin=io.StringIO(payload), stdout=out, stderr=err)
    return code, out.getvalue(), err.getvalue()


def test_a_hook_payload_is_denied_in_the_shared_shape() -> None:
    code, out, err = _run([], json.dumps({"tool_input": {"command": "docker run x"}}))
    assert code == 0
    assert json.loads(out)["hookSpecificOutput"]["permissionDecision"] == "deny"
    assert err == ""


def test_exit_code_mode_and_command_argument_refuse_with_exit_two() -> None:
    code, out, err = _run(["--exit-code"], json.dumps({"tool_input": {"command": "docker rm x"}}))
    assert (code, out) == (2, "")
    assert "`docker rm`" in err
    code, out, err = _run(["--command", "docker volume rm v"])
    assert (code, out) == (2, "")
    assert "`docker volume rm`" in err


def test_reads_are_allowed_silently() -> None:
    assert _run([], json.dumps({"tool_input": {"command": "docker ps"}})) == (0, "", "")
    assert _run(["--command", "docker info"]) == (0, "", "")


def test_the_toolkit_command_dispatches_the_guard(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["guard", "docker-command", "--command", "docker exec x sh"]) == 2
    assert "`docker exec`" in capsys.readouterr().err
