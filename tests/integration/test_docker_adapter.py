from __future__ import annotations

import dataclasses
import hashlib
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from docker_test_toolkit.adapters.outbound.docker import RELAY_STREAM_BYTES, DockerCliAdapter
from docker_test_toolkit.application.lifecycle import Deadline, NativeClock
from docker_test_toolkit.application.recovery import Context, image_lock
from docker_test_toolkit.domain.errors import InfrastructureFailure, ResourceBusy, ResourceError
from docker_test_toolkit.domain.models import (
    DockerDaemonObservation,
    ExactInspection,
    ReadinessCode,
    ReadinessFailure,
    ToolVersionObservation,
)

pytestmark = pytest.mark.integration


class _MacClientSys:
    """The adapter module's ``sys`` with a macOS client platform: Podman machines are served to a macOS client only,
    and the suite also runs on the Linux CI host. Only this module's view changes, never the process's ``sys``."""

    platform = "darwin"

    def __getattr__(self, name: str) -> object:
        return getattr(sys, name)


@pytest.fixture(autouse=True)
def _mac_client(monkeypatch: pytest.MonkeyPatch) -> None:
    import docker_test_toolkit.adapters.outbound.docker as adapter_module

    monkeypatch.setattr(adapter_module, "sys", _MacClientSys())


class _ForeignClockDeadline:
    expires_ns = 10**30

    @staticmethod
    def remaining_seconds() -> float:
        return 5.0


class _DeadlineValue:
    def __init__(self, value: object):
        self.value = value

    def remaining_seconds(self) -> object:
        return self.value


class _ChangedOriginDeadline:
    @staticmethod
    def remaining_seconds() -> float:
        raise ResourceError("Deadline belongs to another clock origin.")


class _ReadinessOriginFailureDeadline:
    @staticmethod
    def remaining_ns(_clock: NativeClock) -> int:
        return 10**30

    @staticmethod
    def remaining_seconds() -> float:
        raise ResourceError("Deadline belongs to another clock origin.")


class _CountingDeadline:
    def __init__(self) -> None:
        self.calls = 0

    def remaining_seconds(self) -> float:
        self.calls += 1
        return 5.0


class _ExpiredReadinessDeadline:
    @staticmethod
    def remaining_ns(_clock: NativeClock) -> int:
        return 0

    @staticmethod
    def remaining_seconds() -> float:
        return 0


class _ExpiresAfterBuildxDeadline:
    def __init__(self) -> None:
        self.calls = 0

    @staticmethod
    def remaining_ns(_clock: NativeClock) -> int:
        return 10**30

    def remaining_seconds(self) -> float:
        self.calls += 1
        return 0 if self.calls >= 4 else 5.0


def _fake_docker(path: Path) -> Path:
    docker = path / "docker-real"
    docker.write_text(
        """#!/bin/sh
set -eu
[ -z "${DTT_LOG:-}" ] || printf '%s\\n' "$*" >> "$DTT_LOG"
case "${DOCKER_CONTEXT:-}|$*" in
  "|context show") printf '%s\\n' fixture ;;
  "fixture|version --format {{.Client.Version}}") printf '%s\\n' 28.4.0 ;;
  "fixture|version --format {{.Client.Version}} {{json .Server.Components}}")
    [ "${DTT_VERSION_EXIT:-0}" = 0 ] || exit 1
    printf '28.4.0 %s\\n' "${DTT_COMPONENTS:-null}" ;;
  "fixture|context inspect --format {{.Endpoints.docker.Host}}")
    if [ -n "${DTT_ENDPOINT_FILE:-}" ]; then cat "$DTT_ENDPOINT_FILE"
    else printf '%s\\n' "${DTT_ENDPOINT:-unix:///Users/x/.docker/run/docker.sock}"; fi ;;
  "fixture|info --format {{.ID}}")
    case "${DTT_COMPONENTS:-}" in *Podman*) printf 'unexpected identity read\\n' >&2; exit 9 ;; esac
    [ ! -e "${DTT_STALL_IDENTITY:-/nonexistent}" ] || sleep 5
    printf '%s\\n' 11111111-1111-1111-1111-111111111111 ;;
  "fixture|run --rm --pull=never --name probe-"*)
    [ "${DTT_PROBE_SLEEP:-0}" != 1 ] || sleep 5
    printf '%s\\n' 'some avg10=0.00 avg60=1.56 avg300=0.00 total=1' '::dtt::' \\
      'Filesystem 1024-blocks Used Available Capacity Mounted on' '/dev/vda1 100 40 60 40% /dtt-data' ;;
  "fixture|container rm -f probe-"*) : ;;
  "fixture|system dial-stdio")
    [ "${DTT_ENGINE_DELAY:-0}" != 1 ] || sleep 3
    IFS= read -r request
    [ -z "${DTT_LOG:-}" ] || printf 'engine:%s\\n' "$request" >> "$DTT_LOG"
    while IFS= read -r line; do [ "$line" = "$(printf '\\r')" ] && break; done
    case "$request" in
      "GET /version "*) body='{"ApiVersion":"1.51","MinAPIVersion":"1.24","Os":"linux","Version":"28.4.0"}' ;;
      "GET /info "*)
        if [ "${DTT_ENGINE_MALFORMED:-0}" = 1 ]; then
          body='{'
        elif [ "${DTT_INFO_INVALID:-0}" = 1 ]; then
          body='{"Architecture":"aarch64","CgroupVersion":"2","ID":"11111111-1111-1111-1111-111111111111",'\\
'"MemTotal":12345678,"NCPU":0,"OperatingSystem":"Docker Desktop"}'
        elif [ "${DTT_DAEMON_CHANGED:-0}" = 1 ]; then
          body='{"Architecture":"aarch64","CgroupVersion":"2","ID":"22222222-2222-2222-2222-222222222222",'\\
'"MemTotal":12345678,"NCPU":6,"OperatingSystem":"Docker Desktop"}'
        else
          body='{"Architecture":"'"${DTT_ARCH:-aarch64}"'","CgroupVersion":"2","ID":"11111111-1111-1111-1111-111111111111",'\\
'"MemTotal":12345678,"NCPU":6,"OperatingSystem":"Docker Desktop"}'
        fi ;;
      *) exit 8 ;;
    esac
    printf 'HTTP/1.1 200 OK\\r\\nContent-Length: %s\\r\\nContent-Type: application/json\\r\\n'\\
'Connection: close\\r\\n\\r\\n%s' "${#body}" "$body"
    ;;
  "fixture|compose version --format json")
    [ "${DTT_COMPOSE_DELAY:-0}" != 1 ] || sleep 3
    if [ "${DTT_COMPOSE_MALFORMED:-0}" = 1 ]; then
      printf '%s\\n' '{"version":"v2.39.0"'
    elif [ "${DTT_COMPOSE_CONTROL:-0}" = 1 ]; then
      printf '%s\\n' '{"version":"v2.39.0\\u001bunsafe"}'
    else
      printf '%s\\n' '{"version":"v2.39.0"}'
    fi ;;
  "fixture|buildx version")
    if [ "${DTT_BUILDX_OVERSIZED:-0}" = 1 ]; then
      printf '%0257d\\n' 0
    elif [ "${DTT_BUILDX_CONTROL:-0}" = 1 ]; then
      printf '0.28.0\\nunsafe\\n'
    else
      printf '%s\\n' '0.28.0 build-fixture'
    fi ;;
  "fixture|barrier")
    : > "$DTT_BARRIER/started"
    i=0
    while [ ! -e "$DTT_BARRIER/release" ] && [ "$i" -lt 300 ]; do sleep 0.1; i=$((i + 1)); done ;;
  "fixture|container inspect missing")
    printf '%s\\n' '[]'
    printf '%s\\n' 'Error response from daemon: No such container: missing' >&2
    exit 1 ;;
  "fixture|image inspect missing")
    printf '%s\\n' '[]'
    printf '%s\\n' 'Error response from daemon: No such image: missing:latest' >&2
    exit 1 ;;
  "fixture|network inspect missing")
    printf '%s\\n' '[]'
    printf '%s\\n' 'Error response from daemon: network missing not found' >&2
    exit 1 ;;
  "fixture|volume inspect missing")
    printf '%s\\n' '[]'
    printf '%s\\n' 'Error response from daemon: get missing: no such volume' >&2
    exit 1 ;;
  "fixture|plugin inspect missing")
    printf '%s\\n' '[]'
    printf '%s\\n' 'Error response from daemon: plugin "missing" not found' >&2
    exit 1 ;;
  "fixture|builder inspect missing")
    printf '%s\\n' 'ERROR: no builder "missing" found' >&2
    exit 1 ;;
  "fixture|builder inspect denied") printf '%s\\n' 'permission denied' >&2; exit 1 ;;
  "fixture|builder inspect empty") : ;;
  "fixture|builder inspect present") printf '%s\\n' 'Name: present' ;;
  "fixture|builder inspect warning")
    printf '%s\\n' 'Name: warning'
    printf '%s\\n' 'warning: fixture' >&2 ;;
  "fixture|container ls -q -a"|"fixture|image ls -q -a"|"fixture|network ls -q"|"fixture|volume ls -q")
    [ -n "${DTT_INVENTORY:-}" ] || { printf '%s\\n' "$*"; exit 0; }
    if [ "${DTT_INVENTORY}" = ls-reset ]; then
      printf '%s\\n' 'error during connect: read: connection reset by peer' >&2
      exit 1
    fi
    printf '%s\\n' kept gone ;;
  "fixture|container inspect gone kept"|"fixture|image inspect gone kept"|\\
  "fixture|network inspect gone kept"|"fixture|volume inspect gone kept")
    kind=${1:-}
    case "${DTT_INVENTORY:-}" in
      vanished)
        printf '%s\\n' '[{"Id":"kept","Name":"kept"}]'
        case "$kind" in
          container) printf '%s\\n' 'Error response from daemon: No such container: gone' >&2 ;;
          image) printf '%s\\n' 'Error response from daemon: No such image: gone:latest' >&2 ;;
          network) printf '%s\\n' 'Error response from daemon: network gone not found' >&2 ;;
          volume) printf '%s\\n' 'Error response from daemon: get gone: no such volume' >&2 ;;
        esac
        exit 1 ;;
      denied)
        printf '%s\\n' '[{"Id":"kept","Name":"kept"}]'
        printf '%s\\n' 'Error response from daemon: No such container: gone' 'permission denied' >&2
        exit 1 ;;
      foreign)
        printf '%s\\n' '[{"Id":"kept","Name":"kept"}]'
        printf '%s\\n' 'Error response from daemon: No such container: other' >&2
        exit 1 ;;
      short)
        printf '%s\\n' '[]'
        printf '%s\\n' 'Error response from daemon: No such container: gone' >&2
        exit 1 ;;
      both)
        printf '%s\\n' '[]'
        printf '%s\\n' 'Error response from daemon: No such container: gone' >&2
        printf '%s\\n' 'Error response from daemon: No such container: kept' >&2
        exit 1 ;;
      substituted)
        printf '%s\\n' '[{"Id":"gone","Name":"gone"}]'
        printf '%s\\n' 'Error response from daemon: No such container: gone' >&2
        exit 1 ;;
      garbled)
        printf '%s\\n' '[{"Id":"kept"'
        printf '%s\\n' 'Error response from daemon: No such container: gone' >&2
        exit 1 ;;
      prefixed)
        printf '%s\\n' '[{"Name":"kept-other"}]'
        printf '%s\\n' 'Error response from daemon: get gone: no such volume' >&2
        exit 1 ;;
      exit2)
        printf '%s\\n' '[{"Id":"kept","Name":"kept"}]'
        printf '%s\\n' 'Error response from daemon: No such container: gone' >&2
        exit 2 ;;
      *) printf '%s\\n' '[{"Id":"gone"},{"Id":"kept"}]' ;;
    esac ;;
  "fixture|container inspect denied") printf '%s\\n' 'permission denied' >&2; exit 1 ;;
  "fixture|container inspect malformed") printf '%s\\n' 'not json' ;;
  "fixture|container inspect "*) printf '%s\\n' '[{}]' ;;
  "fixture|flood") yes x | head -c 1100000 ;;
  "fixture|invalid-utf8") printf '\\377boom' >&2; exit 1 ;;
  "fixture|flood-invalid") head -c 1048575 /dev/zero | LC_ALL=C tr '\\000' '\\377' >&2; exit 1 ;;
  "fixture|flood-both")
    head -c 2097152 /dev/zero | LC_ALL=C tr '\\000' '\\377'
    head -c 2097152 /dev/zero | LC_ALL=C tr '\\000' '\\377' >&2 ;;
  "fixture|flood-exact") yes x | head -c 393216 ;;
  "fixture|stall-stderr") printf 'TOKEN-IN-STDOUT\\n'; printf 'fatal: cannot reach registry\\n' >&2; sleep 5 ;;
  "fixture|stall-invalid") head -c 64 /dev/zero | LC_ALL=C tr '\\000' '\\377' >&2; sleep 5 ;;
  "fixture|linger")
    parent=$PPID
    trap 'exit 42' TERM
    : > "$DTT_MARKER"
    while kill -0 "$parent" 2>/dev/null; do :; done
    ;;
  "fixture|ignore-term") parent=$PPID; trap '' TERM; while kill -0 "$parent" 2>/dev/null; do :; done ;;
  "fixture|orphan-descendant")
    "$DTT_PYTHON" -c 'import os, signal
signal.signal(signal.SIGHUP, signal.SIG_IGN)
signal.signal(signal.SIGTERM, signal.SIG_IGN)
open(os.environ["DTT_MARKER"], "w").write(str(os.getpid()))
signal.pause()' &
    wait ;;
  "fixture|signal-descendant")
    trap '' TERM
    "$DTT_PYTHON" -c 'import os, signal
from pathlib import Path
marker = Path(os.environ["DTT_MARKER"])
def finish(_signum, _frame):
    marker.write_text("terminated")
    raise SystemExit(23)
signal.signal(signal.SIGTERM, finish)
Path(os.environ["DTT_READY"]).touch()
signal.pause()' &
    wait ;;
  *"--context"*) exit 9 ;;
  *) printf '%s\\n' "$*" ;;
esac
"""
    )
    docker.chmod(0o700)
    return docker


def test_adapter_binds_and_revalidates_exact_client_and_daemon(tmp_path: Path) -> None:
    binary = _fake_docker(tmp_path)
    adapter = DockerCliAdapter.discover(binary, environment={"PATH": os.environ["PATH"]})

    assert adapter.identity.client_path == binary.resolve()
    assert adapter.identity.client_version == "28.4.0"
    assert adapter.identity.daemon_id == "11111111-1111-1111-1111-111111111111"
    assert adapter.call("container", "inspect", "a" * 64).startswith("[")

    binary.write_text(binary.read_text() + "\n")
    with pytest.raises(InfrastructureFailure, match="changed"):
        adapter.call("info")


def test_readiness_observes_one_fixed_engine_info_without_resource_commands(tmp_path: Path) -> None:
    log = tmp_path / "commands.log"
    adapter = DockerCliAdapter.discover(
        _fake_docker(tmp_path),
        environment={"PATH": os.environ["PATH"], "DTT_LOG": str(log)},
    )
    log.write_text("")

    observed = adapter.observe_readiness(Deadline.after(NativeClock(), 30))

    assert isinstance(observed.daemon, DockerDaemonObservation)
    assert observed.daemon.cpus == 6
    assert observed.daemon.memory_bytes == 12_345_678
    assert observed.daemon.operating_system == "Docker Desktop"
    assert observed.daemon.architecture == "arm64"  # the daemon's `aarch64`, in OCI spelling
    assert observed.compose == ToolVersionObservation("2.39.0")
    assert observed.buildx == ToolVersionObservation("0.28.0 build-fixture")
    assert log.read_text().splitlines() == [
        "system dial-stdio",
        "engine:GET /version HTTP/1.1",
        "system dial-stdio",
        "engine:GET /info HTTP/1.1",
        "context inspect --format {{.Endpoints.docker.Host}}",  # the endpoint fact, a read
        "compose version --format json",
        "buildx version",
    ]


def test_readiness_does_not_execute_a_changed_client(tmp_path: Path) -> None:
    log = tmp_path / "commands.log"
    binary = _fake_docker(tmp_path)
    adapter = DockerCliAdapter.discover(
        binary,
        environment={"PATH": os.environ["PATH"], "DTT_LOG": str(log)},
    )
    binary.write_text(binary.read_text() + "\n")
    log.write_text("")

    observed = adapter.observe_readiness(Deadline.after(NativeClock(), 30))

    assert observed.client == ReadinessFailure(ReadinessCode.DOCKER_CLIENT_IDENTITY_CHANGED)
    assert observed.daemon == ReadinessFailure(ReadinessCode.DAEMON_UNAVAILABLE)
    assert observed.compose == ReadinessFailure(ReadinessCode.COMPOSE_UNAVAILABLE)
    assert observed.buildx == ReadinessFailure(ReadinessCode.BUILDX_UNAVAILABLE)
    assert log.read_text() == ""


@pytest.mark.parametrize(
    ("environment", "field", "code"),
    (
        ({"DTT_DAEMON_CHANGED": "1"}, "daemon", ReadinessCode.DAEMON_IDENTITY_CHANGED),
        ({"DTT_ENGINE_MALFORMED": "1"}, "daemon", ReadinessCode.DAEMON_RESPONSE_INVALID),
        ({"DTT_INFO_INVALID": "1"}, "daemon", ReadinessCode.DAEMON_RESPONSE_INVALID),
        ({"DTT_ARCH": "riscv64"}, "daemon", ReadinessCode.DAEMON_UNSUPPORTED),
        ({"DTT_COMPOSE_MALFORMED": "1"}, "compose", ReadinessCode.COMPOSE_RESPONSE_INVALID),
        ({"DTT_COMPOSE_CONTROL": "1"}, "compose", ReadinessCode.COMPOSE_RESPONSE_INVALID),
        ({"DTT_BUILDX_OVERSIZED": "1"}, "buildx", ReadinessCode.BUILDX_RESPONSE_INVALID),
        ({"DTT_BUILDX_CONTROL": "1"}, "buildx", ReadinessCode.BUILDX_RESPONSE_INVALID),
    ),
)
def test_readiness_classifies_changed_and_invalid_responses(
    tmp_path: Path,
    environment: dict[str, str],
    field: str,
    code: ReadinessCode,
) -> None:
    adapter = DockerCliAdapter.discover(
        _fake_docker(tmp_path),
        environment={"PATH": os.environ["PATH"], **environment},
    )

    observed = adapter.observe_readiness(Deadline.after(NativeClock(), 30))

    assert getattr(observed, field) == ReadinessFailure(code)


@pytest.mark.parametrize(
    ("environment", "field", "code"),
    (
        ({"DTT_ENGINE_DELAY": "1"}, "daemon", ReadinessCode.DAEMON_TIMEOUT),
        ({"DTT_COMPOSE_DELAY": "1"}, "compose", ReadinessCode.COMPOSE_TIMEOUT),
    ),
)
def test_readiness_classifies_shared_deadline_timeouts(
    tmp_path: Path,
    environment: dict[str, str],
    field: str,
    code: ReadinessCode,
) -> None:
    adapter = DockerCliAdapter.discover(
        _fake_docker(tmp_path),
        environment={"PATH": os.environ["PATH"], **environment},
    )

    observed = adapter.observe_readiness(Deadline.after(NativeClock(), 0.75))

    assert getattr(observed, field) == ReadinessFailure(code)


def test_readiness_continues_after_deadline_origin_failure(tmp_path: Path) -> None:
    adapter = DockerCliAdapter.discover(_fake_docker(tmp_path), environment={"PATH": os.environ["PATH"]})

    observed = adapter.observe_readiness(_ReadinessOriginFailureDeadline())

    assert observed.compose == ReadinessFailure(ReadinessCode.COMPOSE_UNAVAILABLE)
    assert observed.buildx == ReadinessFailure(ReadinessCode.BUILDX_UNAVAILABLE)


def test_readiness_classifies_engine_pre_spawn_expiry_as_timeout(tmp_path: Path) -> None:
    adapter = DockerCliAdapter.discover(_fake_docker(tmp_path), environment={"PATH": os.environ["PATH"]})

    observed = adapter.observe_readiness(_ExpiredReadinessDeadline())

    assert observed.daemon == ReadinessFailure(ReadinessCode.DAEMON_TIMEOUT)


def test_readiness_rechecks_native_deadline_after_the_final_command(tmp_path: Path) -> None:
    adapter = DockerCliAdapter.discover(_fake_docker(tmp_path), environment={"PATH": os.environ["PATH"]})

    observed = adapter.observe_readiness(_ExpiresAfterBuildxDeadline())

    assert observed.buildx == ReadinessFailure(ReadinessCode.BUILDX_TIMEOUT)


def test_discovery_uses_one_deadline_for_every_identity_command(tmp_path: Path) -> None:
    deadline = _CountingDeadline()

    DockerCliAdapter.discover(
        _fake_docker(tmp_path),
        environment={"PATH": os.environ["PATH"]},
        deadline=deadline,
    )

    assert deadline.calls == 6


def test_adapter_output_is_bounded(tmp_path: Path) -> None:
    binary = _fake_docker(tmp_path)
    adapter = DockerCliAdapter.discover(binary, environment={"PATH": os.environ["PATH"]})

    with pytest.raises(InfrastructureFailure, match="output limit"):
        adapter.execute(("flood",), timeout_seconds=5)


def _adapter(tmp_path: Path) -> DockerCliAdapter:
    return DockerCliAdapter.discover(_fake_docker(tmp_path), environment={"PATH": os.environ["PATH"]})


def test_relayed_output_replaces_invalid_bytes_and_keeps_the_exit_code(tmp_path: Path) -> None:
    result = _adapter(tmp_path).execute(("invalid-utf8",), timeout_seconds=5, relay_output=True)

    assert (result.exit_code, result.stdout, result.stderr) == (1, "", "\ufffdboom")


def test_strict_execute_still_refuses_invalid_bytes(tmp_path: Path) -> None:
    with pytest.raises(InfrastructureFailure, match="not valid UTF-8"):
        _adapter(tmp_path).execute(("invalid-utf8",), timeout_seconds=5)


@pytest.mark.parametrize(("case", "name"), (("flood", "stdout"), ("flood-invalid", "stderr")))
def test_relayed_stream_over_budget_keeps_head_marker_and_tail(tmp_path: Path, case: str, name: str) -> None:
    result = _adapter(tmp_path).execute((case,), timeout_seconds=5, relay_output=True)

    stream = getattr(result, name)
    marker = next(line for line in stream.splitlines() if line.startswith("[docker-test-toolkit: "))
    head, tail = stream.split("\n" + marker + "\n")
    assert marker.startswith(f"[docker-test-toolkit: {name} cut from ")
    assert len(head.encode()) <= RELAY_STREAM_BYTES // 2
    assert len(tail.encode()) <= RELAY_STREAM_BYTES // 2
    assert len(stream.encode()) <= RELAY_STREAM_BYTES + len(marker) + 2


def test_relayed_streams_over_budget_stay_under_one_mib_together(tmp_path: Path) -> None:
    result = _adapter(tmp_path).execute(("flood-both",), timeout_seconds=5, relay_output=True)

    assert result.exit_code == 0
    assert "[docker-test-toolkit: stdout cut from 2097152 bytes" in result.stdout
    assert "[docker-test-toolkit: stderr cut from 2097152 bytes" in result.stderr
    assert len(result.stdout.encode()) + len(result.stderr.encode()) < 1024 * 1024


def test_relayed_stream_at_the_budget_is_whole(tmp_path: Path) -> None:
    result = _adapter(tmp_path).execute(("flood-exact",), timeout_seconds=5, relay_output=True)

    assert result.stdout == "x\n" * (RELAY_STREAM_BYTES // 2)


def test_relayed_timeout_names_the_stderr_tail_never_stdout(tmp_path: Path) -> None:
    with pytest.raises(InfrastructureFailure, match="bounded execution") as raised:
        _adapter(tmp_path).execute(("stall-stderr",), timeout_seconds=0.5, relay_output=True)

    assert str(raised.value).endswith("; stderr ended: fatal: cannot reach registry")
    assert "TOKEN-IN-STDOUT" not in str(raised.value)


def test_relayed_timeout_with_invalid_partial_stderr_yields_replacement_characters(tmp_path: Path) -> None:
    with pytest.raises(InfrastructureFailure) as raised:
        _adapter(tmp_path).execute(("stall-invalid",), timeout_seconds=0.5, relay_output=True)

    assert str(raised.value).endswith("; stderr ended: " + "\ufffd" * 64)


def test_strict_timeout_text_is_unchanged(tmp_path: Path) -> None:
    with pytest.raises(InfrastructureFailure) as raised:
        _adapter(tmp_path).execute(("stall-stderr",), timeout_seconds=0.5)

    assert str(raised.value) == "Docker operation did not complete within its bounded execution."


def test_adapter_accepts_the_four_hour_finite_ceiling(tmp_path: Path) -> None:
    adapter = DockerCliAdapter.discover(_fake_docker(tmp_path), environment={"PATH": os.environ["PATH"]})

    result = adapter.execute(("version", "--format", "{{.Client.Version}}"), timeout_seconds=14_400)

    assert result.exit_code == 0
    assert result.stdout == "28.4.0\n"


def test_adapter_uses_the_deadlines_own_clock_conversion(tmp_path: Path) -> None:
    adapter = DockerCliAdapter.discover(_fake_docker(tmp_path), environment={"PATH": os.environ["PATH"]})

    assert adapter.call("info", "--format", "{{.ID}}", deadline=_ForeignClockDeadline()).strip() == (
        "11111111-1111-1111-1111-111111111111"
    )


@pytest.mark.parametrize(
    "deadline",
    (
        object(),
        _DeadlineValue(None),
        _DeadlineValue(True),
        _DeadlineValue(-1),
        _DeadlineValue(float("nan")),
        _DeadlineValue(float("inf")),
    ),
    ids=("missing", "none", "boolean", "negative", "nan", "infinity"),
)
def test_adapter_rejects_invalid_deadline_conversions(tmp_path: Path, deadline: object) -> None:
    adapter = DockerCliAdapter.discover(_fake_docker(tmp_path), environment={"PATH": os.environ["PATH"]})

    with pytest.raises(InfrastructureFailure, match="Docker deadline is invalid"):
        adapter.call("info", "--format", "{{.ID}}", deadline=deadline)


def test_adapter_preserves_a_deadline_origin_failure(tmp_path: Path) -> None:
    adapter = DockerCliAdapter.discover(_fake_docker(tmp_path), environment={"PATH": os.environ["PATH"]})

    with pytest.raises(ResourceError, match="another clock origin"):
        adapter.call("info", "--format", "{{.ID}}", deadline=_ChangedOriginDeadline())


def test_exact_inspection_with_an_expired_deadline_fails_as_a_late_observation(tmp_path: Path) -> None:
    adapter = DockerCliAdapter.discover(_fake_docker(tmp_path), environment={"PATH": os.environ["PATH"]})

    with pytest.raises(InfrastructureFailure, match="deadline expired") as raised:
        adapter.inspect_exact(ExactInspection("container", "missing"), deadline=_DeadlineValue(0))

    assert raised.value.reason == "observation-deadline"


def test_exact_inspection_accepts_the_lifecycle_deadline_object(tmp_path: Path) -> None:
    adapter = DockerCliAdapter.discover(_fake_docker(tmp_path), environment={"PATH": os.environ["PATH"]})

    observed = adapter.inspect_exact(
        ExactInspection("container", "missing"), deadline=Deadline.after(NativeClock(), 20)
    )

    assert observed.exists is False


class _SpentAfterTheFirstRead:
    """A deadline with budget when a command starts and none left when it returns."""

    def __init__(self) -> None:
        self.reads = 0

    def remaining_seconds(self) -> float:
        self.reads += 1
        return 5.0 if self.reads == 1 else 0.0


@pytest.mark.parametrize("operation", ("inspect_exact", "call"))
def test_a_command_that_finishes_after_its_deadline_is_a_late_observation(tmp_path: Path, operation: str) -> None:
    """Identity verification inside ``execute`` is not part of the command timeout: check the deadline afterwards."""
    adapter = DockerCliAdapter.discover(_fake_docker(tmp_path), environment={"PATH": os.environ["PATH"]})
    deadline = _SpentAfterTheFirstRead()

    with pytest.raises(InfrastructureFailure) as raised:
        if operation == "inspect_exact":
            adapter.inspect_exact(ExactInspection("container", "missing"), deadline=deadline)
        else:
            adapter.call("info", "--format", "{{.ID}}", deadline=deadline)

    assert raised.value.reason == "observation-deadline"


def test_a_captured_command_that_outlives_its_bound_is_a_late_observation(tmp_path: Path) -> None:
    adapter = DockerCliAdapter.discover(
        _fake_docker(tmp_path), environment={"PATH": os.environ["PATH"], "DTT_COMPOSE_DELAY": "1"}
    )

    with pytest.raises(InfrastructureFailure, match="bounded execution") as raised:
        adapter.execute(("compose", "version", "--format", "json"), timeout_seconds=0.3)

    assert raised.value.reason == "observation-deadline"


@pytest.mark.parametrize("kind", ("builder", "container", "image", "network", "volume"))
def test_exact_inspection_accepts_only_the_exact_missing_response(tmp_path: Path, kind: str) -> None:
    adapter = DockerCliAdapter.discover(_fake_docker(tmp_path), environment={"PATH": os.environ["PATH"]})

    assert adapter.inspect_exact(ExactInspection(kind, "missing")).exists is False


@pytest.mark.parametrize("kind", ("builder", "container"))
def test_exact_inspection_does_not_treat_other_failures_as_absence(tmp_path: Path, kind: str) -> None:
    adapter = DockerCliAdapter.discover(_fake_docker(tmp_path), environment={"PATH": os.environ["PATH"]})

    with pytest.raises(InfrastructureFailure, match="exact inspection failed"):
        adapter.inspect_exact(ExactInspection(kind, "denied"))


def test_exact_inspection_hashes_present_builder_output(tmp_path: Path) -> None:
    adapter = DockerCliAdapter.discover(_fake_docker(tmp_path), environment={"PATH": os.environ["PATH"]})

    observed = adapter.inspect_exact(ExactInspection("builder", "present"))

    assert observed.exists is True
    assert observed.attributes == {"inspect_sha256": hashlib.sha256(b"Name: present\n").hexdigest()}


@pytest.mark.parametrize("identifier", ("empty", "warning"))
def test_exact_inspection_rejects_ambiguous_builder_success(tmp_path: Path, identifier: str) -> None:
    adapter = DockerCliAdapter.discover(_fake_docker(tmp_path), environment={"PATH": os.environ["PATH"]})

    with pytest.raises(InfrastructureFailure, match="ambiguous evidence"):
        adapter.inspect_exact(ExactInspection("builder", identifier))


def test_adapter_streaming_output_is_not_captured_or_size_limited(
    tmp_path: Path, capfd: pytest.CaptureFixture[str]
) -> None:
    binary = _fake_docker(tmp_path)
    adapter = DockerCliAdapter.discover(binary, environment={"PATH": os.environ["PATH"]})

    result = adapter.execute(("flood",), timeout_seconds=5, stream_output=True)

    captured = capfd.readouterr()
    assert result.exit_code == 0
    assert result.stdout == result.stderr == ""
    assert len(captured.out) == 1_100_000

    relayed = adapter.execute(("flood-invalid",), timeout_seconds=5, stream_output=True, relay_output=True)

    assert relayed.exit_code == 1
    assert relayed.stdout == relayed.stderr == ""
    assert len(capfd.readouterr().err) == 1_048_575  # passed through: neither replaced nor cut


def test_adapter_forwards_targeted_sigterm_to_streamed_child(tmp_path: Path) -> None:
    binary = _fake_docker(tmp_path)
    marker = tmp_path / "ready"
    program = """
import os
import sys
from pathlib import Path
from docker_test_toolkit.adapters.outbound.docker import RELAY_STREAM_BYTES, DockerCliAdapter
adapter = DockerCliAdapter.discover(
    Path(sys.argv[1]),
    environment={"PATH": os.environ["PATH"], "DTT_MARKER": sys.argv[2]},
)
raise SystemExit(adapter.execute(("linger",), timeout_seconds=5, stream_output=True).exit_code)
"""
    process = subprocess.Popen([sys.executable, "-c", program, str(binary), str(marker)])
    try:
        deadline = time.monotonic() + 3
        while not marker.exists() and time.monotonic() < deadline:
            time.sleep(0.001)
        assert marker.exists()
        process.send_signal(signal.SIGTERM)
        assert process.wait(timeout=3) == 42
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()


def test_adapter_forwards_targeted_sigterm_to_streamed_descendant(tmp_path: Path) -> None:
    binary = _fake_docker(tmp_path)
    ready = tmp_path / "ready"
    marker = tmp_path / "terminated"
    program = """
import os
import sys
from pathlib import Path
from docker_test_toolkit.adapters.outbound.docker import RELAY_STREAM_BYTES, DockerCliAdapter
adapter = DockerCliAdapter.discover(
    Path(sys.argv[1]),
    environment={
        "PATH": os.environ["PATH"],
        "DTT_MARKER": sys.argv[2],
        "DTT_READY": sys.argv[3],
        "DTT_PYTHON": sys.executable,
    },
)
raise SystemExit(adapter.execute(("signal-descendant",), timeout_seconds=5, stream_output=True).exit_code)
"""
    process = subprocess.Popen([sys.executable, "-c", program, str(binary), str(marker), str(ready)])
    try:
        deadline = time.monotonic() + 3
        while not ready.exists() and time.monotonic() < deadline:
            time.sleep(0.001)
        assert ready.exists()
        process.send_signal(signal.SIGTERM)
        process.wait(timeout=3)
        assert marker.read_text() == "terminated"
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()


def test_adapter_kills_streamed_child_that_ignores_timeout_termination(tmp_path: Path) -> None:
    binary = _fake_docker(tmp_path)
    program = """
import os
import sys
from pathlib import Path
from docker_test_toolkit.adapters.outbound.docker import RELAY_STREAM_BYTES, DockerCliAdapter
from docker_test_toolkit.domain.errors import InfrastructureFailure
adapter = DockerCliAdapter.discover(Path(sys.argv[1]), environment={"PATH": os.environ["PATH"]})
try:
    adapter.execute(("ignore-term",), timeout_seconds=0.05, stream_output=True)
except InfrastructureFailure as error:
    raise SystemExit(0 if "bounded execution" in str(error) else 98)
raise SystemExit(99)
"""
    started = time.monotonic()

    result = subprocess.run([sys.executable, "-c", program, str(binary)], check=False, timeout=8)

    assert result.returncode == 0
    assert time.monotonic() - started < 7


@pytest.mark.parametrize("mode", ("unbound", "captured", "streamed"))
def test_adapter_timeout_kills_descendants(tmp_path: Path, mode: str) -> None:
    binary = _fake_docker(tmp_path)
    marker = tmp_path / f"{mode}-descendant-pid"
    program = """
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from docker_test_toolkit.adapters.outbound.docker import RELAY_STREAM_BYTES, DockerCliAdapter
from docker_test_toolkit.domain.errors import InfrastructureFailure
marker = Path(sys.argv[2])
mode = sys.argv[3]
environment = {"PATH": os.environ["PATH"], "DTT_MARKER": str(marker), "DTT_PYTHON": sys.executable}
try:
    if mode == "unbound":
        DockerCliAdapter._run_unbound(
            [sys.argv[1], "orphan-descendant"],
            environment={**environment, "DOCKER_CONTEXT": "fixture"},
            timeout_seconds=0.75,
        )
    else:
        adapter = DockerCliAdapter.discover(Path(sys.argv[1]), environment=environment)
        adapter.execute(
            ("orphan-descendant",),
            timeout_seconds=0.75,
            stream_output=mode == "streamed",
        )
except InfrastructureFailure as error:
    if "bounded execution" not in str(error):
        raise SystemExit(98)
else:
    raise SystemExit(99)
deadline = time.monotonic() + 2
while not marker.exists() and time.monotonic() < deadline:
    time.sleep(0.01)
if not marker.exists():
    raise SystemExit(96)
pid = int(marker.read_text())
deadline = time.monotonic() + 2
while time.monotonic() < deadline:
    observed = subprocess.run(
        ["ps", "-o", "state=", "-p", str(pid)],
        check=False,
        capture_output=True,
        text=True,
    )
    if observed.returncode or observed.stdout.strip().startswith("Z"):
        raise SystemExit(0)
    time.sleep(0.01)
os.kill(pid, signal.SIGKILL)
raise SystemExit(97)
"""
    process = subprocess.Popen(
        [sys.executable, "-c", program, str(binary), str(marker), mode],
        start_new_session=True,
    )
    timed_out = False
    try:
        returncode = process.wait(timeout=3)
    except subprocess.TimeoutExpired:
        timed_out = True
        os.killpg(process.pid, signal.SIGKILL)
        returncode = process.wait()

    assert not timed_out
    assert returncode == 0


@pytest.mark.parametrize(
    ("mode", "interrupt_signal"),
    (("unbound", signal.SIGTERM), ("captured", signal.SIGINT)),
)
def test_adapter_interrupt_kills_descendants(tmp_path: Path, mode: str, interrupt_signal: signal.Signals) -> None:
    binary = _fake_docker(tmp_path)
    marker = tmp_path / f"{mode}-interrupted-descendant-pid"
    program = """
import os
import sys
from pathlib import Path
from docker_test_toolkit.adapters.outbound.docker import RELAY_STREAM_BYTES, DockerCliAdapter
marker = Path(sys.argv[2])
mode = sys.argv[3]
environment = {"PATH": os.environ["PATH"], "DTT_MARKER": str(marker), "DTT_PYTHON": sys.executable}
if mode == "unbound":
    DockerCliAdapter._run_unbound(
        [sys.argv[1], "orphan-descendant"],
        environment={**environment, "DOCKER_CONTEXT": "fixture"},
        timeout_seconds=30,
    )
else:
    adapter = DockerCliAdapter.discover(Path(sys.argv[1]), environment=environment)
    adapter.execute(("orphan-descendant",), timeout_seconds=30)
raise SystemExit(99)
"""
    process = subprocess.Popen([sys.executable, "-c", program, str(binary), str(marker), mode])
    descendant_pid = 0
    process_group = 0
    try:
        deadline = time.monotonic() + 3
        while not marker.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert marker.exists()
        descendant_pid = int(marker.read_text())
        process_group = os.getpgid(descendant_pid)

        process.send_signal(interrupt_signal)
        time.sleep(0.1)
        if process.poll() is None:
            process.kill()
        assert process.wait(timeout=3) != 0

        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            observed = subprocess.run(
                ["ps", "-o", "state=", "-p", str(descendant_pid)],
                check=False,
                capture_output=True,
                text=True,
            )
            if observed.returncode or observed.stdout.strip().startswith("Z"):
                break
            time.sleep(0.01)
        else:
            pytest.fail("interrupted Docker command descendant remained alive")
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
        if process_group:
            try:
                os.killpg(process_group, signal.SIGKILL)
            except ProcessLookupError:
                pass


@pytest.mark.parametrize("failure", ("spawn", "timeout"))
def test_adapter_redelivers_deferred_signal_on_streaming_failure(tmp_path: Path, failure: str) -> None:
    binary = _fake_docker(tmp_path)
    program = """
import os
import signal
import subprocess
import sys
from pathlib import Path
import docker_test_toolkit.adapters.outbound.docker as docker_module

class HarnessAdapter(docker_module.DockerCliAdapter):
    def _verify(self):
        pass

class TimedOutProcess:
    def __init__(self):
        self.pid = 2_147_483_647
        self.returncode = None
        self.waits = 0
    def poll(self):
        return self.returncode
    def send_signal(self, _signum):
        pass
    def terminate(self):
        pass
    def kill(self):
        self.returncode = -signal.SIGKILL
    def communicate(self, timeout=None):
        self.wait(timeout)
        return b"", b""
    def wait(self, timeout=None):
        self.waits += 1
        if self.waits == 1:
            raise subprocess.TimeoutExpired("fixture", timeout)
        self.returncode = -signal.SIGTERM
        os.kill(os.getpid(), signal.SIGTERM)
        return self.returncode

discovered = docker_module.DockerCliAdapter.discover(
    Path(sys.argv[1]), environment={"PATH": os.environ["PATH"]}
)
adapter = HarnessAdapter(discovered.identity, environment={"PATH": os.environ["PATH"]})
if sys.argv[2] == "spawn":
    def fail_spawn(*_args, **_kwargs):
        os.kill(os.getpid(), signal.SIGTERM)
        raise OSError("fixture")
    docker_module.subprocess.Popen = fail_spawn
else:
    docker_module.subprocess.Popen = lambda *_args, **_kwargs: TimedOutProcess()
adapter.execute(("fixture",), timeout_seconds=0.05, stream_output=True)
raise SystemExit(99)
"""

    result = subprocess.run([sys.executable, "-c", program, str(binary), failure], check=False, timeout=3)

    assert result.returncode == -signal.SIGTERM


def _count(log: Path, *needles: str) -> int:
    return sum(1 for line in log.read_text().splitlines() if all(needle in line for needle in needles))


def test_a_bound_client_is_the_same_program_in_another_container(tmp_path: Path) -> None:
    """Another runner container's copy of the same client (other device, inode and mtime) may act for the bound run, so
    a dead run can be recovered from any runner that talks to the same daemon."""
    bound = DockerCliAdapter.discover(_fake_docker(tmp_path), environment={"PATH": os.environ["PATH"]}).identity
    other_copy = dataclasses.replace(
        bound,
        client_device=bound.client_device + 1,
        client_inode=bound.client_inode + 1,
        client_mtime_ns=bound.client_mtime_ns + 1,
    )

    adapter = DockerCliAdapter.from_identity(other_copy, environment={"PATH": os.environ["PATH"]})

    assert adapter.observe_daemon() == other_copy  # verified against the client's content and the same daemon


@pytest.mark.parametrize("field", ["client_sha256", "client_size"])
def test_a_bound_client_with_other_content_is_refused(tmp_path: Path, field: str) -> None:
    bound = DockerCliAdapter.discover(_fake_docker(tmp_path), environment={"PATH": os.environ["PATH"]}).identity
    changed = dataclasses.replace(bound, **{field: "0" * 64 if field == "client_sha256" else bound.client_size + 1})
    adapter = DockerCliAdapter.from_identity(changed, environment={"PATH": os.environ["PATH"]})

    with pytest.raises(InfrastructureFailure) as raised:
        adapter.observe_daemon()

    assert raised.value.reason == "daemon-identity-changed"


_INVENTORY_KINDS = ("container", "image", "network", "volume")


def _inventory_adapter(tmp_path: Path, case: str) -> DockerCliAdapter:
    environment = {"PATH": os.environ["PATH"], "DTT_INVENTORY": case}
    return DockerCliAdapter.discover(_fake_docker(tmp_path), environment=environment)


@pytest.mark.parametrize("kind", _INVENTORY_KINDS)
def test_inventory_drops_an_object_that_vanished_between_list_and_inspect(tmp_path: Path, kind: str) -> None:
    assert _inventory_adapter(tmp_path, "vanished").objects(kind) == [{"Id": "kept", "Name": "kept"}]


def test_inventory_keeps_every_object_when_nothing_vanished(tmp_path: Path) -> None:
    assert _inventory_adapter(tmp_path, "complete").objects("container") == [{"Id": "gone"}, {"Id": "kept"}]


def test_inventory_of_only_vanished_objects_is_empty(tmp_path: Path) -> None:
    assert _inventory_adapter(tmp_path, "both").objects("container") == []


@pytest.mark.parametrize(
    ("case", "message"),
    [
        # a failed read-only inspection is a client exit with a closed cause, never the child's text
        ("denied", "Docker client `container inspect` exited with status 1 (daemon-error)."),
        ("foreign", "Docker client `container inspect` exited with status 1 (daemon-error)."),
        ("exit2", "Docker client `container inspect` exited with status 2 (daemon-error)."),
        ("short", "Incomplete Docker container inventory."),  # a proved absence never excuses a missing survivor
        ("substituted", "Incomplete Docker container inventory."),  # the vanished object cannot stand in for a survivor
        ("garbled", "Invalid Docker container inventory."),
    ],
)
def test_inventory_refuses_any_other_inspect_failure(tmp_path: Path, case: str, message: str) -> None:
    with pytest.raises(InfrastructureFailure) as raised:
        _inventory_adapter(tmp_path, case).objects("container")

    assert message in str(raised.value)
    assert raised.value.reason == ("client-exit" if "exited with status" in message else "unspecified")
    assert "No such container" not in str(raised.value)


@pytest.mark.parametrize("kind", _INVENTORY_KINDS)
def test_inventory_list_failure_is_a_late_read(tmp_path: Path, kind: str) -> None:
    with pytest.raises(InfrastructureFailure) as raised:
        _inventory_adapter(tmp_path, "ls-reset").objects(kind)

    assert raised.value.reason == "client-exit"
    assert str(raised.value) == f"Docker client `{kind} ls` exited with status 1 (connect-reset)."


def test_inventory_failure_names_the_command_without_its_flags(tmp_path: Path) -> None:
    adapter = _inventory_adapter(tmp_path, "complete")

    with pytest.raises(InfrastructureFailure) as raised:
        adapter.call("info", "--context", "x")

    assert str(raised.value) == "Docker control command `info` failed with exit status 9."  # no flags, no empty colon


def test_exact_inspection_of_an_unknown_kind_fails_without_proving_absence(tmp_path: Path) -> None:
    adapter = DockerCliAdapter.discover(_fake_docker(tmp_path), environment={"PATH": os.environ["PATH"]})

    with pytest.raises(InfrastructureFailure, match="without proving absence"):
        adapter.inspect_exact(ExactInspection("plugin", "missing"))


def test_inventory_refuses_a_volume_that_only_shares_a_listed_name_prefix(tmp_path: Path) -> None:
    with pytest.raises(InfrastructureFailure, match=r"Incomplete Docker volume inventory\."):
        _inventory_adapter(tmp_path, "prefixed").objects("volume")  # `kept-other` is not the listed `kept`


def test_inventory_failure_never_carries_docker_output(tmp_path: Path) -> None:
    with pytest.raises(InfrastructureFailure) as raised:
        _inventory_adapter(tmp_path, "denied").objects("container")

    assert "permission denied" not in str(raised.value) and "No such container" not in str(raised.value)


def _image_lock_busy(account: Path) -> bool:
    try:
        with image_lock(Context(account, account), nonblocking=True):
            return False
    except ResourceBusy as error:
        assert error.reason == "image-lock-held"
        return True


def _wait_for(path: Path, timeout: float = 20) -> None:
    deadline = time.monotonic() + timeout
    while not path.exists():
        assert time.monotonic() < deadline, f"timed out waiting for {path}"
        time.sleep(0.05)


def _holder(tmp_path: Path, mode: str) -> tuple[subprocess.Popen[bytes], Path, Path]:
    account, barrier = tmp_path / "account", tmp_path / "barrier"
    account.mkdir(mode=0o700)
    barrier.mkdir()
    holder = subprocess.Popen(
        [
            sys.executable,
            str(Path(__file__).with_name("image_lock_holder.py")),
            str(_fake_docker(tmp_path)),
            str(account),
            str(barrier),
            mode,
        ],
        env={**os.environ, "PYTHONPATH": str(Path(__file__).parents[2] / "src")},
    )
    _wait_for(barrier / "started")
    return holder, account, barrier


def _release_child(account: Path, barrier: Path) -> None:
    (barrier / "release").touch()
    deadline = time.monotonic() + 20
    while _image_lock_busy(account):
        assert time.monotonic() < deadline, "the image lock stayed held after the child exited"
        time.sleep(0.05)


@pytest.mark.parametrize("mode", ["captured", "streaming"])
def test_hold_fd_keeps_k_after_exception_while_child_runs(tmp_path: Path, mode: str) -> None:
    holder, account, barrier = _holder(tmp_path, mode)
    try:
        holder.send_signal(signal.SIGUSR1)
        _wait_for(barrier / "unwound")

        assert _image_lock_busy(account)  # the holder's `with` exited; the child still holds K
        _release_child(account, barrier)
    finally:
        holder.kill()
        holder.wait(timeout=10)


@pytest.mark.parametrize("mode", ["captured", "streaming"])
def test_hold_fd_keeps_k_after_holder_sigkill(tmp_path: Path, mode: str) -> None:
    holder, account, barrier = _holder(tmp_path, mode)
    holder.kill()
    holder.wait(timeout=10)

    assert _image_lock_busy(account)  # the holder is gone; its Docker child still holds K
    _release_child(account, barrier)


def test_image_lock_nonblocking_busy_raises_image_lock_held(tmp_path: Path) -> None:
    account = tmp_path / "account"
    account.mkdir(mode=0o700)
    with image_lock(Context(account, account)):
        assert _image_lock_busy(account)
    assert not _image_lock_busy(account)


_PROBE = ("run", "--rm", "--pull=never", "--name", "probe-1", "alpine", "sh", "-c", "probe")


def test_run_probe_returns_the_probe_output_within_its_deadline(tmp_path: Path) -> None:
    adapter = DockerCliAdapter.discover(_fake_docker(tmp_path), environment={"PATH": os.environ["PATH"]})

    run = adapter.run_probe(_PROBE, deadline=Deadline.after(NativeClock(), 10), container_name="probe-1")

    assert (run.exit_code, run.timed_out) == (0, False) and "::dtt::" in run.stdout


def test_run_probe_kills_and_removes_a_container_at_its_deadline(tmp_path: Path) -> None:
    log = tmp_path / "docker.log"
    adapter = DockerCliAdapter.discover(
        _fake_docker(tmp_path), environment={"PATH": os.environ["PATH"], "DTT_PROBE_SLEEP": "1", "DTT_LOG": str(log)}
    )
    started = time.monotonic()

    run = adapter.run_probe(_PROBE, deadline=Deadline.after(NativeClock(), 1.5), container_name="probe-1")

    assert time.monotonic() - started < 4  # the probe sleeps 5 s: the deadline, not the probe, ended it
    assert (run.exit_code, run.timed_out, run.stdout) == (None, True, "")
    assert log.read_text().splitlines()[-1] == "container rm -f probe-1"  # one removal by name after the kill


def test_a_stalled_identity_read_does_not_exceed_the_probe_deadline(tmp_path: Path) -> None:
    # execute() would retry identity reads under their own 30 s bounds; a probe answers inside an admission pass.
    stall = tmp_path / "stall"
    log = tmp_path / "docker.log"
    adapter = DockerCliAdapter.discover(
        _fake_docker(tmp_path),
        environment={"PATH": os.environ["PATH"], "DTT_STALL_IDENTITY": str(stall), "DTT_LOG": str(log)},
    )
    stall.touch()
    started = time.monotonic()

    with pytest.raises(InfrastructureFailure) as raised:
        adapter.run_probe(_PROBE, deadline=Deadline.after(NativeClock(), 1.5), container_name="probe-1")

    assert raised.value.reason == "observation-deadline" and time.monotonic() - started < 4
    assert not any(line.startswith("run ") for line in log.read_text().splitlines())  # the probe never started


_DISCOVERY_VERSION = "version --format {{.Client.Version}} {{json .Server.Components}}"


def test_the_docker_branch_issues_exactly_todays_commands_and_never_runs_podman(tmp_path: Path) -> None:
    """discovery, a verified command and the probe issue today's commands; podman is never looked up or run."""
    from tests.integration.scripted_clis import fake_podman

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    fake_podman(bin_dir)
    log, podman_log = tmp_path / "commands.log", tmp_path / "podman.log"
    environment = {"PATH": f"{bin_dir}:{os.environ['PATH']}", "DTT_LOG": str(log), "DTT_PODMAN_LOG": str(podman_log)}

    adapter = DockerCliAdapter.discover(_fake_docker(tmp_path), environment=environment)
    discovered = log.read_text().splitlines()
    adapter.call("container", "inspect", "a" * 64)
    verified = log.read_text().splitlines()[len(discovered) :]
    adapter.run_probe(
        ("run", "--rm", "--pull=never", "--name", "probe-x", "alpine"),
        deadline=Deadline.after(NativeClock(), 5),
        container_name="probe-x",
    )
    probed = log.read_text().splitlines()[len(discovered) + len(verified) :]

    assert discovered == ["context show", _DISCOVERY_VERSION, "info --format {{.ID}}"]
    assert verified == [
        "version --format {{.Client.Version}}",
        "info --format {{.ID}}",
        "container inspect " + "a" * 64,
    ]
    assert probed[:2] == ["version --format {{.Client.Version}}", "info --format {{.ID}}"]
    assert probed[2].startswith("run --rm --pull=never --name probe-x") and len(probed) == 3
    assert (adapter.engine, adapter.binding, adapter.identity.client_version) == ("docker", None, "28.4.0")
    assert not podman_log.exists()


@pytest.mark.parametrize("components", ["null", "[]", '[{"Name":"Engine"},{"Name":"containerd"}]'])
def test_an_empty_null_or_engine_component_list_is_the_docker_branch(tmp_path: Path, components: str) -> None:
    adapter = DockerCliAdapter.discover(
        _fake_docker(tmp_path), environment={"PATH": os.environ["PATH"], "DTT_COMPONENTS": components}
    )

    assert adapter.engine == "docker"
    assert adapter.identity.daemon_id == "11111111-1111-1111-1111-111111111111"


def test_a_malformed_zero_exit_version_output_is_a_client_fault_not_a_late_read(tmp_path: Path) -> None:
    with pytest.raises(InfrastructureFailure) as malformed:
        DockerCliAdapter.discover(
            _fake_docker(tmp_path), environment={"PATH": os.environ["PATH"], "DTT_COMPONENTS": "{"}
        )
    with pytest.raises(InfrastructureFailure) as failed:
        DockerCliAdapter.discover(
            _fake_docker(tmp_path), environment={"PATH": os.environ["PATH"], "DTT_VERSION_EXIT": "1"}
        )

    assert malformed.value.reason == "unspecified"
    assert failed.value.reason == "client-exit"


_PODMAN_COMPONENTS = '[{"Name":"Podman Engine","Version":"6.1.4"},{"Name":"Engine","Version":"6.1.4"}]'


def _podman_setup(tmp_path: Path, **extra: str) -> tuple[Path, dict[str, str], Path, Path]:
    """A scripted docker reporting a Podman engine and a scripted podman with a machine on PATH."""
    from tests.integration.scripted_clis import W0_SOCKET, fake_podman

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    fake_podman(bin_dir)
    log, podman_log = tmp_path / "commands.log", tmp_path / "podman.log"
    environment = {
        "PATH": f"{bin_dir}:/usr/bin:/bin",
        "DTT_LOG": str(log),
        "DTT_PODMAN_LOG": str(podman_log),
        "DTT_COMPONENTS": _PODMAN_COMPONENTS,
        "DTT_ENDPOINT": f"unix://{W0_SOCKET}",
        **extra,
    }
    return _fake_docker(tmp_path), environment, log, podman_log


def _machines(environment):
    from docker_test_toolkit.adapters.outbound.podman import PodmanCliAdapter

    return PodmanCliAdapter(environment)


def test_discovery_on_podman_binds_the_machine_hash_and_never_reads_the_compat_id(tmp_path: Path) -> None:
    from docker_test_toolkit.domain.models import MachineFacts
    from docker_test_toolkit.domain.policy import podman_identity
    from tests.integration.scripted_clis import IMAGE_DIR, W0_CREATED, W0_SOCKET

    binary, environment, log, podman_log = _podman_setup(tmp_path)

    adapter = DockerCliAdapter.discover(binary, environment=environment, machines=_machines)

    machine = MachineFacts("dtt-w0", W0_CREATED, False, W0_SOCKET, 3072, 2)
    assert adapter.engine == "podman"
    assert adapter.identity.daemon_id == podman_identity(machine)
    assert adapter.binding is not None and adapter.binding.machine == machine
    assert (adapter.binding.endpoint, adapter.binding.image_dir) == (f"unix://{W0_SOCKET}", Path(IMAGE_DIR))
    assert not any(line.startswith("info") for line in log.read_text().splitlines())
    assert [line.split("|")[2] for line in podman_log.read_text().splitlines()] == [
        "machine list --format json",
        "machine inspect dtt-w0",
        "machine info --format json",
    ]


def test_discovery_on_podman_binds_the_endpoint_machine_among_several(tmp_path: Path) -> None:
    from tests.integration.scripted_clis import machine_inspect, machine_list

    second_socket = "/var/folders/nd/x/T/podman/dtt-w1-api.sock"
    (tmp_path / "list.json").write_text(machine_list(("dtt-w0", True), ("dtt-w1", True)))
    (tmp_path / "w1.json").write_text(machine_inspect("dtt-w1", socket=second_socket))
    binary, environment, _log, _plog = _podman_setup(
        tmp_path,
        DTT_MACHINE_LIST=str(tmp_path / "list.json"),
        DTT_MACHINE_INSPECT_dtt_w1=str(tmp_path / "w1.json"),
        DTT_ENDPOINT=f"unix://{second_socket}",
    )

    adapter = DockerCliAdapter.discover(binary, environment=environment, machines=_machines)

    assert adapter.binding is not None and adapter.binding.machine.name == "dtt-w1"


@pytest.mark.parametrize(
    ("change", "reason", "text", "podman_runs"),
    [
        ({"platform": "linux"}, "unsupported-runtime", "supported on macOS: Docker Desktop, Colima, Podman", False),
        (
            {"DTT_ENDPOINT": "unix:///var/run/docker.sock"},
            "podman-helper-unsupported",
            "docker context create podman",
            False,
        ),
        ({"no_podman": "1"}, "podman-binary-missing", "brew install podman", False),
        ({"DTT_ENDPOINT": "unix:///tmp/other.sock"}, "podman-machine-not-found", "dtt-w0-api.sock", True),
        ({"empty_list": "1"}, "podman-machine-not-found", "start one machine", True),
        ({"DTT_PODMAN_FAIL": "1"}, "client-exit", "boom", True),
    ],
)
def test_each_podman_refusal_names_its_reason_and_fix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: dict[str, str], reason: str, text: str, podman_runs: bool
) -> None:
    from docker_test_toolkit.adapters.outbound import docker as docker_module

    extra = {key: value for key, value in change.items() if key.startswith("DTT_")}
    if "empty_list" in change:
        (tmp_path / "empty.json").write_text("[]")
        extra["DTT_MACHINE_LIST"] = str(tmp_path / "empty.json")
    binary, environment, _log, podman_log = _podman_setup(tmp_path, **extra)
    if "no_podman" in change:
        (tmp_path / "bin" / "podman").unlink()
    if "platform" in change:  # the client platform is a process fact; this is the composition-root seam the test owns
        monkeypatch.setattr(docker_module.sys, "platform", change["platform"])

    def machines(machine_environment):  # the missing-binary case must not find a podman the host itself has
        path = str(tmp_path / "bin") if "no_podman" in change else machine_environment["PATH"]
        return _machines({**machine_environment, "PATH": path})

    with pytest.raises(InfrastructureFailure) as refused:
        DockerCliAdapter.discover(binary, environment=environment, machines=machines)

    assert refused.value.reason == reason and text in str(refused.value)
    assert podman_log.exists() is podman_runs


def _bound_podman(tmp_path: Path):
    """A Podman-bound adapter whose endpoint and machine facts the test can change through files."""
    from tests.integration.scripted_clis import W0_SOCKET, machine_inspect

    endpoint, inspected, failing = tmp_path / "endpoint", tmp_path / "inspect.json", tmp_path / "podman-fails"
    endpoint.write_text(f"unix://{W0_SOCKET}\n")
    inspected.write_text(machine_inspect())
    binary, environment, log, podman_log = _podman_setup(
        tmp_path,
        DTT_ENDPOINT_FILE=str(endpoint),
        DTT_MACHINE_INSPECT=str(inspected),
        DTT_PODMAN_FAIL_FILE=str(failing),
    )
    adapter = DockerCliAdapter.discover(binary, environment=environment, machines=_machines)
    log.write_text("")
    podman_log.write_text("")
    return adapter, log, podman_log, endpoint, inspected, failing


def _podman_commands(podman_log: Path) -> list[str]:
    return [line.split("|")[2] for line in podman_log.read_text().splitlines()]


def test_every_podman_command_re_reads_the_endpoint_and_the_machine(tmp_path: Path) -> None:
    adapter, log, podman_log, *_ = _bound_podman(tmp_path)

    adapter.call("container", "inspect", "a" * 64)
    first = log.read_text().splitlines()
    adapter.call("container", "inspect", "a" * 64)

    assert first == [
        "version --format {{.Client.Version}}",
        "context inspect --format {{.Endpoints.docker.Host}}",
        "container inspect " + "a" * 64,
    ]
    assert log.read_text().splitlines()[len(first) :] == first  # no cache: every command reads again
    assert _podman_commands(podman_log) == ["machine inspect dtt-w0", "machine inspect dtt-w0"]


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ("created", "daemon-identity-changed"),
        ("endpoint", "daemon-identity-changed"),
        ("stopped", "client-exit"),
        ("fails", "client-exit"),
    ],
)
def test_a_changed_machine_or_endpoint_is_an_identity_change_and_a_stopped_or_failing_read_is_late(
    tmp_path: Path, change: str, reason: str
) -> None:
    from tests.integration.scripted_clis import machine_inspect

    adapter, _log, _plog, endpoint, inspected, failing = _bound_podman(tmp_path)
    if change == "created":
        inspected.write_text(machine_inspect(created="2026-11-04T10:43:18.200188+01:00"))
    elif change == "endpoint":  # the context updated in place to another socket
        endpoint.write_text("unix:///var/folders/nd/x/T/podman/other-api.sock\n")
    elif change == "stopped":
        inspected.write_text(machine_inspect(state="stopped"))
    else:
        failing.touch()

    with pytest.raises(InfrastructureFailure) as refused:
        adapter.call("container", "inspect", "a" * 64)

    assert refused.value.reason == reason


def test_the_probe_verifies_the_machine_under_its_deadline_without_reading_the_compat_id(tmp_path: Path) -> None:
    from tests.integration.scripted_clis import machine_inspect

    adapter, log, podman_log, _endpoint, inspected, _failing = _bound_podman(tmp_path)
    argv = ("run", "--rm", "--pull=never", "--name", "probe-x", "alpine")

    adapter.run_probe(argv, deadline=Deadline.after(NativeClock(), 5), container_name="probe-x")
    probed = log.read_text().splitlines()
    inspected.write_text(machine_inspect(created="2026-11-04T10:43:18.200188+01:00"))
    with pytest.raises(InfrastructureFailure) as changed:
        adapter.run_probe(argv, deadline=Deadline.after(NativeClock(), 5), container_name="probe-x")

    assert probed[:2] == ["version --format {{.Client.Version}}", "context inspect --format {{.Endpoints.docker.Host}}"]
    assert probed[2].startswith("run --rm --pull=never --name probe-x") and len(probed) == 3
    assert changed.value.reason == "daemon-identity-changed"
    assert not any(line.startswith("run ") for line in log.read_text().splitlines()[len(probed) :])
    assert _podman_commands(podman_log)[0] == "machine inspect dtt-w0"


def test_readiness_on_podman_reports_the_bound_identity_not_the_compat_id(tmp_path: Path) -> None:
    from tests.integration.scripted_clis import machine_inspect

    adapter, _log, _plog, _endpoint, inspected, _failing = _bound_podman(tmp_path)

    observed = adapter.observe_readiness(Deadline.after(NativeClock(), 30))
    inspected.write_text(machine_inspect(created="2026-11-04T10:43:18.200188+01:00"))
    changed = adapter.observe_readiness(Deadline.after(NativeClock(), 30))

    assert isinstance(observed.daemon, DockerDaemonObservation)
    assert observed.daemon.daemon_id == adapter.identity.daemon_id and observed.daemon.daemon_id.startswith("podman-")
    assert changed.daemon == ReadinessFailure(ReadinessCode.DAEMON_IDENTITY_CHANGED)


def test_a_recovered_adapter_learns_the_engine_with_one_combined_read_and_continues_only_on_the_same_machine(
    tmp_path: Path,
) -> None:
    from tests.integration.scripted_clis import machine_inspect

    adapter, log, _plog, _endpoint, inspected, _failing = _bound_podman(tmp_path)
    recovered = DockerCliAdapter.from_identity(adapter.identity, environment=adapter.env, machines=_machines)

    recovered.observe_daemon()
    first = log.read_text().splitlines()
    recovered.observe_daemon()
    second = log.read_text().splitlines()[len(first) :]
    inspected.write_text(machine_inspect(created="2026-11-04T10:43:18.200188+01:00"))
    elsewhere = DockerCliAdapter.from_identity(adapter.identity, environment=adapter.env, machines=_machines)
    with pytest.raises(InfrastructureFailure) as changed:
        elsewhere.observe_daemon()

    assert first == [_DISCOVERY_VERSION, "context inspect --format {{.Endpoints.docker.Host}}"]
    assert second == ["version --format {{.Client.Version}}", "context inspect --format {{.Endpoints.docker.Host}}"]
    assert (recovered.engine, recovered.binding) == ("podman", adapter.binding)
    assert changed.value.reason == "daemon-identity-changed"


def test_a_recovered_docker_adapter_reads_the_combined_template_once(tmp_path: Path) -> None:
    log = tmp_path / "commands.log"
    adapter = DockerCliAdapter.discover(
        _fake_docker(tmp_path), environment={"PATH": os.environ["PATH"], "DTT_LOG": str(log)}
    )
    recovered = DockerCliAdapter.from_identity(adapter.identity, environment=adapter.env, machines=_machines)
    log.write_text("")

    recovered.observe_daemon()
    recovered.observe_daemon()

    assert log.read_text().splitlines() == [
        _DISCOVERY_VERSION,
        "info --format {{.ID}}",
        "version --format {{.Client.Version}}",
        "info --format {{.ID}}",
    ]
    assert (recovered.engine, recovered.binding) == ("docker", None)


def test_daemon_identity_issues_todays_read_on_docker_and_the_binding_on_podman(tmp_path: Path) -> None:
    from docker_test_toolkit.application.recovery import daemon_identity

    (tmp_path / "docker").mkdir()
    log = tmp_path / "docker" / "commands.log"
    docker = DockerCliAdapter.discover(
        _fake_docker(tmp_path / "docker"), environment={"PATH": os.environ["PATH"], "DTT_LOG": str(log)}
    )
    log.write_text("")
    (tmp_path / "podman").mkdir()
    podman, podman_docker_log, *_ = _bound_podman(tmp_path / "podman")

    docker_id = daemon_identity(docker)
    podman_id = daemon_identity(podman, Deadline.after(NativeClock(), 5))

    assert docker_id == "11111111-1111-1111-1111-111111111111"
    assert log.read_text().splitlines() == [  # call() verifies (version, info), then today's read
        "version --format {{.Client.Version}}",
        "info --format {{.ID}}",
        "info --format {{.ID}}",
    ]
    assert podman_id == podman.identity.daemon_id and podman_id.startswith("podman-")
    assert not any(line.startswith("info") for line in podman_docker_log.read_text().splitlines())
