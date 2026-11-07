"""Reason propagation through the real Engine transport and the Docker adapter."""

from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from docker_test_toolkit.adapters.outbound.docker import DockerCliAdapter
from docker_test_toolkit.application import lifecycle
from docker_test_toolkit.application.lifecycle import ControlError, Deadline, NativeClock
from docker_test_toolkit.domain.errors import InfrastructureFailure

pytestmark = pytest.mark.integration

_DAEMON = "11111111-1111-1111-1111-111111111111"
_CONTAINER = "a" * 64


def _engine(tmp_path: Path, **environment: str) -> SimpleNamespace:
    """A minimal fake Docker CLI whose ``system dial-stdio`` answers or hangs according to ``DTT_MODE``."""
    script = tmp_path / "docker-engine"
    script.write_text(
        """#!/bin/sh
set -eu
[ "$*" = "system dial-stdio" ] || exit 9
[ "${DTT_MODE:-}" != hang ] || sleep 5
IFS= read -r request
while IFS= read -r line; do [ "$line" = "$(printf '\\r')" ] && break; done
case "$request" in
  "GET /info "*) body="$DTT_INFO" ;;
  *) exit 8 ;;
esac
[ "${DTT_MODE:-}" != malformed ] || body='{'
printf 'HTTP/1.1 200 OK\\r\\nContent-Length: %s\\r\\nContent-Type: application/json\\r\\n' "${#body}"
printf 'Connection: close\\r\\n\\r\\n%s' "$body"
"""
    )
    script.chmod(0o700)
    return SimpleNamespace(
        engine="docker", binding=None, prefix=[str(script)], env={"PATH": os.environ["PATH"], **environment}
    )


def _info(**fields: str) -> str:
    body = {"ID": _DAEMON, "OSType": "linux", "CgroupVersion": "2", **fields}
    return "{" + ",".join(f'"{key}":"{value}"' for key, value in body.items()) + "}"


def test_engine_deadline_expiry_is_classified_late(tmp_path: Path) -> None:
    docker = _engine(tmp_path, DTT_MODE="hang", DTT_INFO=_info())

    with pytest.raises(ControlError) as raised:
        lifecycle.engine_get(docker, "/info", deadline=Deadline.after(NativeClock(), 0.5))

    assert raised.value.reason == "observation-deadline"


def test_engine_malformed_body_is_not_late(tmp_path: Path) -> None:
    docker = _engine(tmp_path, DTT_MODE="malformed", DTT_INFO=_info())

    with pytest.raises(ControlError) as raised:
        lifecycle.engine_get(docker, "/info", deadline=Deadline.after(NativeClock(), 5))

    assert raised.value.reason == "unspecified"  # unclassified: the guardian records it as unreadable with detail


def test_adapter_identity_verification_timeout_is_classified_late() -> None:
    with pytest.raises(InfrastructureFailure) as raised:
        DockerCliAdapter._run_unbound(
            ["/bin/sleep", "5"], environment={"PATH": os.environ["PATH"]}, timeout_seconds=0.2
        )

    assert raised.value.reason == "observation-deadline"


_ECHOED = "tok-3f9a"  # stands in for an environment value the client may echo; it must never reach a message


@pytest.mark.parametrize(
    ("stderr", "kind"),
    (
        ("error during connect: dial tcp 127.0.0.1:2375: connect: connection refused", "connect-refused"),
        ("read tcp 127.0.0.1:41234->127.0.0.1:2375: read: connection reset by peer", "connect-reset"),
        (
            "Cannot connect to the Docker daemon at tcp://127.0.0.1:2375. Is the docker daemon running?",
            "daemon-unreachable",
        ),
        ('context "toolkit-ci": context not found: open /root/.docker/contexts/meta: no such file', "context-missing"),
        ('error during connect: Get "http://127.0.0.1:2375/v1.47/info": EOF', "connection-eof"),
        ("Error response from daemon: Bad response from Docker engine", "daemon-error"),
        ("context deadline exceeded", "client-timeout"),
        (  # Docker 29's wording when the selected context's socket is absent (a runner without a context)
            "failed to connect to the docker API at unix:///var/run/docker.sock; check if the path is correct"
            " and if the daemon is running: dial unix /var/run/docker.sock: connect: no such file or directory",
            "daemon-unreachable",
        ),
        # the daemon's envelope must not hide a more specific cause inside it
        ("Error response from daemon: context deadline exceeded", "client-timeout"),
        ('error during connect: Get "http://127.0.0.1:2375/v1.47/info": unexpected EOF', "connection-eof"),
        ("something else entirely", "other"),
        ("", "other"),
    ),
)
def test_client_exit_is_a_named_late_read_with_a_closed_cause(tmp_path: Path, stderr: str, kind: str) -> None:
    script = tmp_path / "docker-failing"
    script.write_text(f"#!/bin/sh\nprintf '%s {_ECHOED}\\n' '{stderr}' >&2\nexit 1\n")
    script.chmod(0o700)

    with pytest.raises(InfrastructureFailure) as raised:
        DockerCliAdapter._run_unbound(
            [str(script), "info", "--format", "{{.ID}}"], environment={"PATH": os.environ["PATH"]}
        )

    assert raised.value.reason == "client-exit"
    assert str(raised.value) == f"Docker client `info` exited with status 1 ({kind})."
    assert _ECHOED not in str(raised.value)


def test_client_exit_names_a_two_word_command_without_flags_or_values(tmp_path: Path) -> None:
    script = tmp_path / "docker-failing"
    script.write_text("#!/bin/sh\nexit 3\n")
    script.chmod(0o700)

    with pytest.raises(InfrastructureFailure) as raised:
        DockerCliAdapter._run_unbound([str(script), "context", "show"], environment={"PATH": os.environ["PATH"]})
    with pytest.raises(InfrastructureFailure) as flagged:
        DockerCliAdapter._run_unbound(
            [str(script), "version", "--format", "x"], environment={"PATH": os.environ["PATH"]}
        )

    assert str(raised.value) == "Docker client `context show` exited with status 3 (other)."
    assert str(flagged.value) == "Docker client `version` exited with status 3 (other)."


@pytest.mark.parametrize(
    ("info", "reason"),
    (
        (_info(ID="22222222-2222-2222-2222-222222222222"), "daemon-identity-changed"),
        (_info(OSType="windows"), "telemetry-unreadable"),
        (_info(CgroupVersion="1"), "telemetry-unreadable"),
    ),
    ids=("daemon-id", "os-type", "cgroup"),
)
def test_container_observation_identity_split(tmp_path: Path, info: str, reason: str) -> None:
    docker = _engine(tmp_path, DTT_INFO=info)

    with pytest.raises(lifecycle.ResourceError) as raised:
        lifecycle.observe_container(docker, _DAEMON, _CONTAINER, deadline=Deadline.after(NativeClock(), 5))

    assert raised.value.reason == reason
    # a changed daemon is what the /info read saw: the engine; the other capability failures concern the container
    assert raised.value.observer == ("engine" if reason == "daemon-identity-changed" else "container")


def _flaky_client(tmp_path: Path, failures: int) -> tuple[list[str], Path]:
    """A client that exits 1 for its first ``failures`` calls, then prints an identity value; it counts every call."""
    calls = tmp_path / "calls"
    script = tmp_path / "docker-flaky"
    script.write_text(
        f"""#!/bin/sh
n=$(($(cat {calls} 2>/dev/null || echo 0) + 1)); echo $n > {calls}
[ $n -gt {failures} ] || exit 1
echo "{_DAEMON}"
"""
    )
    script.chmod(0o700)
    return [str(script), "info", "--format", "{{.ID}}"], calls


@pytest.mark.parametrize("failures", [1, 2], ids=["one", "two"])
def test_identity_read_rides_out_transient_client_failures(tmp_path: Path, failures: int) -> None:
    command, calls = _flaky_client(tmp_path, failures)

    assert DockerCliAdapter._identity_read(command, {"PATH": os.environ["PATH"]}) == _DAEMON
    assert calls.read_text().strip() == str(failures + 1)


def test_identity_read_names_the_command_after_its_retries(tmp_path: Path) -> None:
    command, calls = _flaky_client(tmp_path, failures=3)

    with pytest.raises(InfrastructureFailure) as raised:
        DockerCliAdapter._identity_read(command, {"PATH": os.environ["PATH"]})

    assert str(raised.value).startswith("Docker client `info` identity read failed")
    assert raised.value.reason == "client-exit" and calls.read_text().strip() == "3"  # still a late read


def test_identity_read_never_retries_a_timeout(tmp_path: Path) -> None:
    calls = tmp_path / "calls"
    script = tmp_path / "docker-slow"
    script.write_text(f"#!/bin/sh\necho x >> {calls}\nsleep 5\n")
    script.chmod(0o700)

    with pytest.raises(InfrastructureFailure) as raised:
        DockerCliAdapter._identity_read([str(script), "version"], {"PATH": os.environ["PATH"]}, timeout_seconds=0.2)

    assert raised.value.reason == "observation-deadline" and len(calls.read_text().splitlines()) == 1
