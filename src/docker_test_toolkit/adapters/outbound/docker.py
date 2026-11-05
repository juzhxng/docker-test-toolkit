"""Bounded Docker CLI adapter with an immutable client/daemon binding."""

from __future__ import annotations

import hashlib
import json
import math
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Mapping, Sequence, cast

from docker_test_toolkit.application.ports.docker import DockerPort
from docker_test_toolkit.application.ports.podman import MachineFactory, PodmanMachinePort
from docker_test_toolkit.domain.errors import InfrastructureFailure, ResourceError
from docker_test_toolkit.domain.models import (
    CleanupMutation,
    ComposeMutation,
    ContainerMutation,
    DaemonIdentity,
    DockerClientObservation,
    DockerDaemonObservation,
    DockerObservation,
    DockerReadinessObservation,
    DockerResult,
    ExactInspection,
    ExactInspectionOutcome,
    PodmanBinding,
    ProbeRun,
    ReadinessCode,
    ReadinessFailure,
    ToolVersionObservation,
)
from docker_test_toolkit.domain.platform import daemon_platform
from docker_test_toolkit.domain.policy import (
    PODMAN_HELPER_ENDPOINT,
    PODMAN_HELPER_TEXT,
    UNSUPPORTED_RUNTIME_TEXT,
    RuntimeRefusal,
    match_machine,
    parse_version_components,
    podman_identity,
)

if TYPE_CHECKING:
    from docker_test_toolkit.application.lifecycle import Deadline

_MAX_OUTPUT = 1024 * 1024
_TERMINATION_GRACE_SECONDS = 0.25
_REAP_TIMEOUT_SECONDS = 1.0


def _valid_tool_version(value: object) -> bool:
    return isinstance(value, str) and 0 < len(value) <= 256 and value.isascii() and value.isprintable()


DISCOVERY_VERSION_FORMAT = "{{.Client.Version}} {{json .Server.Components}}"  # the engine rides on the version read


class _DockerOperationInterrupted(Exception):
    """Escape subprocess communication without its delayed KeyboardInterrupt handling."""


class _DockerCommandTimeout(InfrastructureFailure):
    """Distinguish a bounded Docker timeout from other infrastructure failures."""

    def __init__(self, message: str = "") -> None:
        super().__init__(message, reason="observation-deadline")


def _forward_signal(process: subprocess.Popen[bytes], signum: int) -> bool:
    """Signal the child's command group; report False when the signal must be redelivered later.

    ``ProcessLookupError`` means the group is gone. ``PermissionError`` means no live member can
    be signalled yet (an exited child awaiting its reaper); neither may escape the signal handler,
    so the caller defers the signal and redelivers it to itself once the child has been waited for.
    """
    try:
        os.killpg(process.pid, signum)
    except (ProcessLookupError, PermissionError):
        return False
    return True


class DockerCliAdapter(DockerPort):
    """Invoke one bound Docker client without making ownership decisions."""

    @staticmethod
    def _terminate_process_group(process: subprocess.Popen[bytes]) -> OSError | None:
        """Signal an owned command group before its leader can be reaped."""
        failure = None
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        except OSError as error:
            failure = error
        time.sleep(_TERMINATION_GRACE_SECONDS)
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        except OSError as error:
            failure = failure or error
        return failure

    @staticmethod
    def _finish_terminated_process(process: subprocess.Popen[bytes]) -> BaseException | None:
        """Bound pipe draining and direct-child reaping after group termination."""
        try:
            process.communicate(timeout=_REAP_TIMEOUT_SECONDS)
            return None
        except (OSError, subprocess.TimeoutExpired) as error:
            for stream in (process.stdin, process.stdout, process.stderr):
                if stream is not None:
                    stream.close()
            try:
                process.kill()
                process.wait(timeout=_REAP_TIMEOUT_SECONDS)
            except (OSError, subprocess.TimeoutExpired) as reap_error:
                error.add_note(f"Direct-child reaping also failed: {type(reap_error).__name__}.")
            return error

    @classmethod
    def run_captured(
        cls,
        command: list[str],
        *,
        environment: Mapping[str, str],
        timeout_seconds: float,
        hold_fd: int | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        """Run one client command in its own process group within ``timeout_seconds``; at the timeout or an interrupt
        the whole group is ended and reaped, and an interrupting signal is redelivered to this process."""
        forwarded_signals = (signal.SIGINT, signal.SIGTERM)
        owns_signals = threading.current_thread() is threading.main_thread()
        previous_mask: set[int | signal.Signals] | None = None
        handlers: dict[signal.Signals, Any] = {}
        interrupted_signal: int | None = None
        process: subprocess.Popen[bytes] | None = None

        def _interrupt(signum: int, _frame: object) -> None:
            nonlocal interrupted_signal
            interrupted_signal = signum
            raise _DockerOperationInterrupted

        if owns_signals:
            previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, forwarded_signals)
            for signum in forwarded_signals:
                handlers[signum] = signal.signal(signum, _interrupt)
        try:
            if previous_mask is not None:
                signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
            process = subprocess.Popen(
                command,
                env=dict(environment),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True,
                pass_fds=() if hold_fd is None else (hold_fd,),  # the child keeps the image lock until it exits
            )
            stdout, stderr = process.communicate(timeout=timeout_seconds)
        except (
            OSError,
            subprocess.TimeoutExpired,
            _DockerOperationInterrupted,
            KeyboardInterrupt,
            SystemExit,
        ) as error:
            if previous_mask is not None:
                signal.pthread_sigmask(signal.SIG_BLOCK, forwarded_signals)
            if process is not None:
                if interrupted_signal is None:
                    termination_error = cls._terminate_process_group(process)
                else:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                        termination_error = None
                    except ProcessLookupError:
                        termination_error = None
                    except OSError as termination_failure:
                        termination_error = termination_failure
                reap_error = cls._finish_terminated_process(process)
                if termination_error is not None:
                    error.add_note("Docker command-group termination could not be verified.")
                if reap_error is not None:
                    error.add_note("Docker direct-child reaping could not be verified.")
            if interrupted_signal is None:
                raise
        finally:
            if previous_mask is not None:
                signal.pthread_sigmask(signal.SIG_BLOCK, forwarded_signals)
                for signum, handler in handlers.items():
                    signal.signal(signum, handler)
                signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
        if interrupted_signal is not None:
            os.kill(os.getpid(), interrupted_signal)
            raise InfrastructureFailure("Docker operation interruption could not be redelivered.")
        if process is None:
            raise InfrastructureFailure("Docker operation was interrupted before it started.")
        return subprocess.CompletedProcess(command, process.returncode, stdout, stderr)

    def __init__(
        self,
        identity: DaemonIdentity | None = None,
        *,
        environment: Mapping[str, str] | None = None,
        context: str | None = None,
        engine: str | None = "docker",
        binding: PodmanBinding | None = None,
        machines: PodmanMachinePort | None = None,
        machine_factory: MachineFactory | None = None,
    ) -> None:
        if environment is None:
            raise InfrastructureFailure("Docker adapter requires an explicit environment.")
        if identity is None:
            discovered = type(self).discover(environment=environment)
            self.__dict__.update(discovered.__dict__)
            return
        self.identity = identity
        self._engine = engine  # "docker" or "podman" (design PD1); None until a recovered adapter's first verification
        self._binding = binding  # the bound Podman machine, in memory only
        self._machines = machines  # the bound machine's port (Podman)
        self._machine_factory = machine_factory  # builds the port for a recovered adapter that learns it is on Podman
        self.binary = str(identity.client_path)
        self.env = dict(environment)
        self.prefix = [self.binary]
        self._context = context
        if context is not None:
            self.env["DOCKER_CONTEXT"] = context
        else:
            self.env.pop("DOCKER_CONTEXT", None)

    @staticmethod
    def _file_identity(path: Path) -> tuple[str, os.stat_result]:
        try:
            observed = path.stat(follow_symlinks=False)
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
        except OSError as error:
            raise InfrastructureFailure("Docker client identity is unreadable.") from error
        if not path.is_file() or not os.access(path, os.X_OK):
            raise InfrastructureFailure("Docker client is not an executable regular file.")
        return digest, observed

    @staticmethod
    def _identity_read(command: list[str], environment: Mapping[str, str], *, timeout_seconds: float = 30) -> str:
        """Read one identity value for ``_verify``, retrying a failed client exit twice after a short pause.

        Every Docker command re-reads ``version`` and ``info`` first, so a busy CI host makes thousands of these reads
        a minute, and one that exits non-zero once (seen on 2026-10-31 with eight parallel jobs) must not revoke every
        queued run. Only a failed exit is retried: a timeout is not, and a value that differs is a real identity change
        for the caller. The last failure names the command, never its output, which can carry secrets.
        """
        for pause in _IDENTITY_RETRY_PAUSES:
            try:
                return DockerCliAdapter._run_unbound(
                    command, environment=environment, timeout_seconds=timeout_seconds
                ).strip()
            except _DockerCommandTimeout:
                raise
            except InfrastructureFailure:
                time.sleep(pause)
        try:
            return DockerCliAdapter._run_unbound(
                command, environment=environment, timeout_seconds=timeout_seconds
            ).strip()
        except _DockerCommandTimeout:
            raise
        except InfrastructureFailure as error:
            raise InfrastructureFailure(
                f"Docker client `{command[1]}` identity read failed: {error}", reason=error.reason
            ) from error

    @staticmethod
    def _run_unbound(
        command: list[str],
        *,
        environment: Mapping[str, str],
        timeout_seconds: float = 30,
    ) -> str:
        try:
            result = DockerCliAdapter.run_captured(
                command,
                environment=environment,
                timeout_seconds=timeout_seconds,
            )
        except subprocess.TimeoutExpired as error:
            raise _DockerCommandTimeout("Docker client did not complete within its bounded execution.") from error
        except OSError as error:
            raise InfrastructureFailure("Docker client did not complete within its bounded execution.") from error
        if len(result.stdout) > _MAX_OUTPUT or len(result.stderr) > _MAX_OUTPUT:
            raise InfrastructureFailure("Docker client exceeded its bounded output limit.")
        if result.returncode:
            raise _client_exit(command[1:], result.returncode, result.stderr.decode("utf-8", "replace"))
        try:
            return result.stdout.decode("utf-8")
        except UnicodeError as error:
            raise InfrastructureFailure("Docker client output is not valid UTF-8.") from error

    @classmethod
    def discover(
        cls,
        candidate: Path | None = None,
        *,
        environment: Mapping[str, str] | None = None,
        deadline: Deadline | None = None,
        machines: MachineFactory | None = None,
    ) -> "DockerCliAdapter":
        """Resolve and bind one canonical client, version, context, and daemon.

        On a Podman engine the daemon is the running machine whose API socket is the context's endpoint, identified
        by ``podman_identity`` (design 2.2); ``machines`` builds the machine port for the child environment. An
        unsupported Podman setup raises ``InfrastructureFailure`` with its named reason and fix.
        """
        if environment is None:
            raise InfrastructureFailure("Docker adapter requires an explicit environment.")
        child_environment = dict(environment)
        selected = candidate or (
            Path(found) if (found := shutil.which("docker", path=child_environment.get("PATH"))) else None
        )
        if selected is None:
            raise InfrastructureFailure("Docker client is unavailable.")
        try:
            path = selected.resolve(strict=True)
        except OSError as error:
            raise InfrastructureFailure("Docker client path is unavailable.") from error
        digest, observed = cls._file_identity(path)
        context = child_environment.get("DOCKER_CONTEXT")
        if not context and not child_environment.get("DOCKER_HOST"):
            argv = ("context", "show")
            context = cls._run_with_deadline(
                [str(path), *argv],
                environment=child_environment,
                deadline=deadline,
                argv=argv,
            ).strip()
            if not context:
                raise InfrastructureFailure("Docker client did not identify its active context.")
        prefix = [str(path)]
        if context is not None:
            child_environment["DOCKER_CONTEXT"] = context
        version_argv = ("version", "--format", DISCOVERY_VERSION_FORMAT)
        try:
            version, engine = parse_version_components(
                cls._run_with_deadline(
                    [*prefix, *version_argv],
                    environment=child_environment,
                    deadline=deadline,
                    argv=version_argv,
                )
            )
        except ValueError as error:  # a client that exited 0 with another shape: deterministic, never a late read
            raise InfrastructureFailure("Docker client version output is malformed.") from error
        if engine == "podman":
            binding, port = _bind_podman(machines, prefix, child_environment, deadline)
            return cls(
                cls._identity_of(binding.identity, path, digest, version, observed, context),
                environment=child_environment,
                context=context,
                engine=engine,
                binding=binding,
                machines=port,
            )
        info_argv = ("info", "--format", "{{.ID}}")
        daemon_id = cls._run_with_deadline(
            [*prefix, *info_argv],
            environment=child_environment,
            deadline=deadline,
            argv=info_argv,
        ).strip()
        if not version or not daemon_id:
            raise InfrastructureFailure("Docker client or daemon identity is incomplete.")
        identity = cls._identity_of(daemon_id, path, digest, version, observed, context)
        return cls(identity, environment=child_environment, context=context, engine=engine)

    @staticmethod
    def _identity_of(
        daemon_id: str, path: Path, digest: str, version: str, observed: os.stat_result, context: str | None
    ) -> DaemonIdentity:
        return DaemonIdentity(
            daemon_id,
            path,
            digest,
            version,
            observed.st_dev,
            observed.st_ino,
            observed.st_size,
            observed.st_mtime_ns,
            context,
        )

    @classmethod
    def read_endpoint(cls, prefix: list[str], environment: Mapping[str, str], deadline: Deadline | None) -> str:
        """Return ``DOCKER_HOST``, else the Docker endpoint of the client's bound context (read, never recorded)."""
        host = environment.get("DOCKER_HOST")
        if host:
            return host
        argv = ("context", "inspect", "--format", "{{.Endpoints.docker.Host}}")
        endpoint = cls._run_with_deadline([*prefix, *argv], environment=environment, deadline=deadline, argv=argv)
        if not endpoint.strip():
            raise InfrastructureFailure("Docker client did not name its context endpoint.")
        return endpoint.strip()

    @classmethod
    def from_identity(
        cls,
        identity: DaemonIdentity,
        *,
        environment: Mapping[str, str] | None = None,
        context: str | None = None,
        machines: MachineFactory | None = None,
    ) -> "DockerCliAdapter":
        """Reconstruct an adapter only from a previously durable exact identity.

        The engine is learned on the first verification (design PD1): one combined version read, and on Podman the
        endpoint's machine, which must hash to the recorded identity.
        """
        if environment is None:
            raise InfrastructureFailure("Docker adapter requires an explicit environment.")
        selected_context = identity.client_context if context is None else context
        if selected_context != identity.client_context:
            raise InfrastructureFailure("Docker context differs from its bound identity.")
        return cls(identity, environment=environment, context=selected_context, engine=None, machine_factory=machines)

    @property
    def engine(self) -> str:
        """``docker`` or ``podman``; a recovered adapter learns it on its first verification (design PD1)."""
        if self._engine is None:
            self._verify()
        return cast(str, self._engine)

    @property
    def binding(self) -> PodmanBinding | None:
        """The bound Podman machine on a Podman engine, held in memory only; ``None`` on Docker."""
        if self._engine is None:
            self._verify()
        return self._binding

    def _verify(self, deadline: Deadline | None = None) -> None:
        """Verify the bound client and daemon before every command.

        The client is the same program when its content is (SHA-256 and size at the bound path): another runner
        container's copy has its own device, inode and mtime, and must still act for a run bound on the same daemon,
        or a run whose job was cancelled could never be recovered from another runner (2026-11-01). On Podman the
        daemon is the bound machine, read again every time (design PD6); a recovered adapter first learns the engine
        from discovery's combined version read, once.
        """
        self._check_client_file()
        if self._engine is None:
            self._learn_engine(deadline)
            return
        self._check_identity(lambda argv: self._identity_read([*self.prefix, *argv], self.env), deadline)

    def _check_identity(self, read: Callable[[tuple[str, ...]], str], deadline: Deadline | None) -> None:
        """Compare the client version and the daemon (on Podman the bound machine, PD6) with the bound identity.

        ``read`` runs one client command: ``_verify``'s retried read or ``run_probe``'s one-deadline read. The engine
        is already learned.
        """
        if read(("version", "--format", "{{.Client.Version}}")) != self.identity.client_version:
            raise _identity_changed()
        self._check_daemon(read, deadline)

    def _check_daemon(self, read: Callable[[tuple[str, ...]], str], deadline: Deadline | None) -> None:
        if self._engine == "podman":
            self._verify_machine(deadline)
        elif read(("info", "--format", "{{.ID}}")) != self.identity.daemon_id:
            raise _identity_changed("Docker client or daemon changed after authority was bound.")

    def _check_client_file(self) -> None:
        """Require the bound client program's content (SHA-256 and size at its bound path)."""
        digest, observed = self._file_identity(self.identity.client_path)
        if (digest, observed.st_size) != (self.identity.client_sha256, self.identity.client_size):
            raise _identity_changed()

    def _learn_engine(self, deadline: Deadline | None, read: Callable[[tuple[str, ...]], str] | None = None) -> None:
        """Decide a recovered adapter's engine from one combined version read and bind it to the recorded daemon.

        ``read`` runs one client command; by default the retried identity read, inside a probe its one-deadline read.
        """

        def retried(argv: tuple[str, ...]) -> str:
            """Run one identity read with the adapter's own retried bounds."""
            return self._identity_read([*self.prefix, *argv], self.env)

        reader = retried if read is None else read
        try:
            version, engine = parse_version_components(reader(("version", "--format", DISCOVERY_VERSION_FORMAT)))
        except ValueError as error:
            raise InfrastructureFailure("Docker client version output is malformed.") from error
        if version != self.identity.client_version:
            raise _identity_changed()
        if engine == "docker":
            self._engine = engine
            self._check_daemon(reader, deadline)
            return
        binding, port = _bind_podman(self._machine_factory, self.prefix, self.env, deadline)
        if binding.identity != self.identity.daemon_id:
            raise _identity_changed("The Podman machine differs from the one the run was bound to.")
        self._engine, self._binding, self._machines = engine, binding, port

    def _verify_machine(self, deadline: object | None) -> None:
        """Read the endpoint and the bound machine again; another one is ``daemon-identity-changed`` (design PD6).

        A failing or late ``podman`` call, and a machine that is not running, are the port's ``client-exit``.
        """
        binding, port = self._binding, self._machines
        if binding is None or port is None:
            raise InfrastructureFailure("Podman binding is unavailable.")
        bounded = cast("Deadline | None", deadline)
        if self.read_endpoint(self.prefix, self.env, bounded) != binding.endpoint:
            raise _identity_changed("The Docker context now points at another endpoint.")
        if podman_identity(port.inspect(binding.machine.name, bounded)) != binding.identity:
            raise _identity_changed("The Podman machine changed after authority was bound.")

    def _machine_unchanged(self, deadline: Deadline) -> bool:
        """Whether the bound Podman machine is still the endpoint's (readiness; other failures propagate)."""
        try:
            self._verify_machine(deadline)
        except InfrastructureFailure as error:
            if error.reason == "daemon-identity-changed":
                return False
            raise
        return True

    def run_probe(self, argv: tuple[str, ...], *, deadline: object, container_name: str) -> ProbeRun:
        """Verify identity once, run the probe and remove it at a timeout, all within one absolute deadline.

        ``execute`` retries identity reads with their own 30 second bounds; a probe answers inside an admission pass,
        so here each read and the run take only what remains of ``deadline``, and the single removal at a timeout
        does too (design 2.3).
        """
        self._check_client_file()
        bounded = cast("Deadline", deadline)  # the port types it ``object``; admission passes its ``Deadline``

        def one_shot(command: tuple[str, ...]) -> str:
            """Run one identity read under the probe's single deadline, without a retry."""
            return self._run_with_deadline(
                [*self.prefix, *command], environment=self.env, deadline=deadline, argv=argv
            ).strip()

        if self._engine is None:
            self._learn_engine(bounded, one_shot)  # a recovered adapter learns its engine within this deadline
        self._check_identity(one_shot, bounded)
        remaining = self._remaining_seconds(deadline, argv)
        try:  # the run ends before the deadline by the removal's share, so a removal at a timeout still fits in it
            result = self._execute_verified(
                argv,
                timeout_seconds=max(remaining - _PROBE_REMOVAL_SECONDS, remaining / 2),
                environment=None,
                stream_output=False,
            )
        except _DockerCommandTimeout:
            removal = ("container", "rm", "-f", container_name)
            try:
                self._execute_verified(
                    removal,
                    timeout_seconds=self._remaining_seconds(deadline, removal),
                    environment=None,
                    stream_output=False,
                )
            except InfrastructureFailure:
                pass  # the caller's record of the name lets retention remove it later (design 2.7)
            return ProbeRun("", None, True)
        return ProbeRun(result.stdout, result.exit_code, False)

    def execute(
        self,
        argv: tuple[str, ...],
        *,
        timeout_seconds: float,
        environment: Mapping[str, str] | None = None,
        stream_output: bool = False,
        hold_fd: int | None = None,
    ) -> DockerResult:
        """Execute bounded argv; main-thread streaming forwards SIGINT and SIGTERM.

        ``hold_fd`` (the image lock's descriptor) is inherited by the Docker child, so the lock stays held until the
        child exits even when this process dies or unwinds first (design 2.6).
        """
        if (
            not argv
            or any(not isinstance(value, str) or not value or "\0" in value for value in argv)
            or isinstance(timeout_seconds, bool)
            or not 0 < timeout_seconds <= 14_400
        ):
            raise InfrastructureFailure("Docker adapter request is invalid or unbounded.")
        self._verify()
        return self._execute_verified(
            argv, timeout_seconds=timeout_seconds, environment=environment, stream_output=stream_output, hold_fd=hold_fd
        )

    def _execute_verified(
        self,
        argv: tuple[str, ...],
        *,
        timeout_seconds: float,
        environment: Mapping[str, str] | None,
        stream_output: bool,
        hold_fd: int | None = None,
    ) -> DockerResult:
        """Run one already-validated argv; the caller has verified the client and daemon identity."""
        child_environment = dict(self.env)
        if environment is not None:
            child_environment.update(environment)
        if stream_output:
            if threading.current_thread() is not threading.main_thread():
                raise InfrastructureFailure("Streaming Docker operations require the main thread.")
            forwarded_signals = (signal.SIGINT, signal.SIGTERM)
            previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, forwarded_signals)
            handlers: dict[signal.Signals, Any] = {}
            process: subprocess.Popen[bytes] | None = None
            deferred_signal: int | None = None
            exit_code = 0

            def _forward(signum: int, _frame: object) -> None:
                nonlocal deferred_signal
                if process is None or process.returncode is not None or not _forward_signal(process, signum):
                    deferred_signal = signum

            try:
                for signum in forwarded_signals:
                    handlers[signum] = signal.signal(signum, _forward)
                signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
                if deferred_signal is None:
                    try:
                        process = subprocess.Popen(
                            [*self.prefix, *argv],
                            env=child_environment,
                            start_new_session=True,
                            pass_fds=() if hold_fd is None else (hold_fd,),
                        )
                    except OSError as error:
                        raise InfrastructureFailure("Docker operation could not start.") from error
                    if deferred_signal is not None:
                        pending_signal = deferred_signal
                        deferred_signal = None
                        _forward(pending_signal, None)
                    try:
                        exit_code = process.wait(timeout=timeout_seconds)
                    except subprocess.TimeoutExpired as error:
                        termination_error = self._terminate_process_group(process)
                        reap_error = self._finish_terminated_process(process)
                        if termination_error is not None:
                            error.add_note("Docker command-group termination could not be verified.")
                        if reap_error is not None:
                            error.add_note("Docker direct-child reaping could not be verified.")
                        raise InfrastructureFailure(
                            "Docker operation did not complete within its bounded execution."
                        ) from error
            finally:
                signal.pthread_sigmask(signal.SIG_BLOCK, forwarded_signals)
                for signum, handler in handlers.items():
                    signal.signal(signum, handler)
                signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
                if deferred_signal is not None:
                    os.kill(os.getpid(), deferred_signal)
            if process is None:
                raise InfrastructureFailure("Docker operation was interrupted before it started.")
            return DockerResult(exit_code, "", "")
        try:
            result = self.run_captured(
                [*self.prefix, *argv],
                environment=child_environment,
                timeout_seconds=timeout_seconds,
                hold_fd=hold_fd,
            )
        except subprocess.TimeoutExpired as error:
            raise _DockerCommandTimeout("Docker operation did not complete within its bounded execution.") from error
        except OSError as error:
            raise InfrastructureFailure("Docker operation did not complete within its bounded execution.") from error
        if len(result.stdout) > _MAX_OUTPUT or len(result.stderr) > _MAX_OUTPUT:
            raise InfrastructureFailure("Docker operation exceeded its bounded output limit.")
        try:
            stdout = result.stdout.decode("utf-8")
            stderr = result.stderr.decode("utf-8")
        except UnicodeError as error:
            raise InfrastructureFailure("Docker operation output is not valid UTF-8.") from error
        return DockerResult(result.returncode, stdout, stderr)

    @staticmethod
    def _remaining_seconds(deadline: object | None, argv: tuple[str, ...]) -> float:
        if deadline is None:
            return 1200 if argv[:1] in (("pull",), ("build",)) or argv[:2] == ("buildx", "build") else 30
        remaining = getattr(deadline, "remaining_seconds", None)
        if not callable(remaining):
            raise InfrastructureFailure("Docker deadline is invalid.")
        seconds = remaining()
        if type(seconds) not in (int, float) or not math.isfinite(seconds) or seconds < 0:
            raise InfrastructureFailure("Docker deadline is invalid.")
        if seconds == 0:
            raise _DockerCommandTimeout("Docker deadline expired before subprocess spawn.")
        return seconds

    @classmethod
    def _run_with_deadline(
        cls,
        command: list[str],
        *,
        environment: Mapping[str, str],
        deadline: object | None,
        argv: tuple[str, ...],
    ) -> str:
        """Run once and reject success observed after the original native deadline."""
        output = cls._run_unbound(
            command,
            environment=environment,
            timeout_seconds=cls._remaining_seconds(deadline, argv),
        )
        cls._remaining_seconds(deadline, argv)
        return output

    @staticmethod
    def _readiness_timed_out(deadline: Deadline) -> bool:
        try:
            return deadline.remaining_seconds() == 0
        except ResourceError:
            return False

    def _require_unspent(self, deadline: object | None, argv: tuple[str, ...]) -> None:
        """Refuse a result that arrived after its deadline: ``execute`` verifies the client before the command runs.

        The command's own timeout is computed before that verification, so a deadline can pass inside it.
        """
        if deadline is None:
            return
        try:
            self._remaining_seconds(deadline, argv)
        except _DockerCommandTimeout as error:
            raise _DockerCommandTimeout("Docker command finished after its deadline.") from error

    def _run(self, argv: tuple[str, ...], deadline: object | None) -> DockerResult:
        """Execute one control command within ``deadline`` and return its result, whatever its exit status."""
        result = self.execute(argv, timeout_seconds=self._remaining_seconds(deadline, argv))
        self._require_unspent(deadline, argv)
        return result

    def call(self, *args: str, deadline: object | None = None) -> str:
        """Compatibility boundary for existing lifecycle observation code."""
        argv = tuple(args)
        result = self._run(argv, deadline)
        if result.exit_code:
            raise _control_failure(argv, result)
        return result.stdout

    def objects(self, kind: str, *, deadline: object | None = None) -> list[dict]:
        """Return complete inspected objects for one supported Docker kind."""
        if kind not in {"container", "image", "network", "volume"}:
            raise ResourceError("Unsupported Docker inventory kind.")
        listing_argv = (kind, "ls", "-q", *(("-a",) if kind in {"container", "image"} else ()))
        listing = self._run(listing_argv, deadline)
        if listing.exit_code:
            raise _client_exit(listing_argv, listing.exit_code, listing.stderr)
        ids = sorted(set(listing.stdout.split()))
        if not ids:
            return []
        argv = (kind, "inspect", *ids)
        inspected = self._run(argv, deadline)
        vanished = _vanished(kind, ids, inspected, self.engine)
        if inspected.exit_code and not vanished:
            raise _client_exit(argv, inspected.exit_code, inspected.stderr)
        try:
            result = json.loads(inspected.stdout)
        except ValueError as error:
            raise InfrastructureFailure(f"Invalid Docker {kind} inventory.") from error
        if (
            not isinstance(result, list)
            or len(result) != len(ids) - len(vanished)
            or not all(isinstance(item, dict) for item in result)
            or (vanished and not _survivors_match(kind, sorted(set(ids) - vanished), result))
        ):
            raise InfrastructureFailure(f"Incomplete Docker {kind} inventory.")
        if kind == "image" and self.engine == "podman":
            return [_familiar_image(item) for item in result]
        return result

    def image(self, reference: str) -> str:
        """Resolve one image reference to its immutable image ID."""
        try:
            rows = json.loads(self.call("image", "inspect", reference))
            image_id = rows[0]["Id"]
        except (ValueError, KeyError, IndexError, TypeError) as error:
            raise InfrastructureFailure("Invalid Docker image identity.") from error
        if not isinstance(image_id, str) or not image_id.startswith("sha256:") or len(image_id) != 71:
            raise InfrastructureFailure("Docker did not return an immutable image ID.")
        return image_id

    def observe_daemon(self, deadline: Deadline | None = None) -> DaemonIdentity:
        """Verify and return the exact daemon and client identity selected at composition."""
        self._verify(deadline)
        return self.identity

    def endpoint(self, deadline: Deadline | None = None) -> str:
        """Return the client's endpoint: ``DOCKER_HOST``, else the bound context's Docker endpoint (design 2.5).

        Resolved per call and kept in memory only; it is never part of the recorded identity.
        """
        return self.read_endpoint(self.prefix, self.env, deadline)

    def observe_readiness(self, deadline: Deadline) -> DockerReadinessObservation:
        """Observe only the fixed client, Engine, Compose, and Buildx readiness facts."""
        try:
            digest, observed = self._file_identity(self.identity.client_path)
            actual = (digest, observed.st_dev, observed.st_ino, observed.st_size, observed.st_mtime_ns)
            expected = (
                self.identity.client_sha256,
                self.identity.client_device,
                self.identity.client_inode,
                self.identity.client_size,
                self.identity.client_mtime_ns,
            )
            if actual != expected:
                client: DockerClientObservation | ReadinessFailure = ReadinessFailure(
                    ReadinessCode.DOCKER_CLIENT_IDENTITY_CHANGED
                )
            else:
                client = DockerClientObservation(
                    str(self.identity.client_path),
                    digest,
                    self.identity.client_version,
                    observed.st_dev,
                    observed.st_ino,
                    observed.st_size,
                    observed.st_mtime_ns,
                )
        except InfrastructureFailure:
            client = ReadinessFailure(ReadinessCode.DOCKER_CLIENT_UNAVAILABLE)
        if isinstance(client, ReadinessFailure):
            return DockerReadinessObservation(
                client,
                ReadinessFailure(ReadinessCode.DAEMON_UNAVAILABLE),
                ReadinessFailure(ReadinessCode.COMPOSE_UNAVAILABLE),
                ReadinessFailure(ReadinessCode.BUILDX_UNAVAILABLE),
            )

        daemon: DockerDaemonObservation | ReadinessFailure
        try:
            from docker_test_toolkit.application.lifecycle import ControlError, engine_get

            version = engine_get(self, "/version", deadline=deadline).body
            info = engine_get(self, "/info", deadline=deadline).body
            fields: tuple[object, ...] = (
                version.get("Version"),
                version.get("ApiVersion"),
                version.get("MinAPIVersion"),
                version.get("Os"),
                info.get("ID"),
                info.get("CgroupVersion"),
                info.get("OperatingSystem"),
                info.get("NCPU"),
                info.get("MemTotal"),
                info.get("Architecture"),
            )
            platform = daemon_platform(fields[3], fields[9])
            if (
                any(not isinstance(value, str) or not value or len(value) > 256 for value in (*fields[:7], fields[9]))
                or type(fields[7]) is not int
                or type(fields[8]) is not int
                or fields[7] <= 0
                or fields[8] <= 0
            ):
                daemon = ReadinessFailure(ReadinessCode.DAEMON_RESPONSE_INVALID)
            elif (self.engine == "docker" and fields[4] != self.identity.daemon_id) or (
                self.engine == "podman" and not self._machine_unchanged(deadline)
            ):
                daemon = ReadinessFailure(ReadinessCode.DAEMON_IDENTITY_CHANGED)
            elif platform is None:
                daemon = ReadinessFailure(ReadinessCode.DAEMON_UNSUPPORTED)
            else:
                daemon = DockerDaemonObservation(
                    self.identity.daemon_id,  # the compat ID on Docker (equal, checked above); the machine's on Podman
                    cast(str, fields[0]),
                    cast(str, fields[1]),
                    cast(str, fields[2]),
                    cast(str, fields[3]),
                    cast(str, fields[5]),
                    cast(str, fields[6]),
                    cast(int, fields[7]),
                    cast(int, fields[8]),
                    platform.architecture,
                    info["Name"] if isinstance(info.get("Name"), str) else None,
                    # on Podman the machine check just read the endpoint and found it the bound one
                    self._binding.endpoint if self._binding is not None else self.endpoint(deadline),
                )
        except ControlError:
            daemon = ReadinessFailure(
                ReadinessCode.DAEMON_TIMEOUT
                if self._readiness_timed_out(deadline)
                else ReadinessCode.DAEMON_RESPONSE_INVALID
            )
        except (InfrastructureFailure, ResourceError):
            daemon = ReadinessFailure(
                ReadinessCode.DAEMON_TIMEOUT
                if self._readiness_timed_out(deadline)
                else ReadinessCode.DAEMON_UNAVAILABLE
            )

        compose: ToolVersionObservation | ReadinessFailure
        try:
            output = self._run_with_deadline(
                [*self.prefix, "compose", "version", "--format", "json"],
                environment=self.env,
                deadline=deadline,
                argv=("compose", "version"),
            )
        except _DockerCommandTimeout:
            compose = ReadinessFailure(ReadinessCode.COMPOSE_TIMEOUT)
        except (InfrastructureFailure, ResourceError):
            compose = ReadinessFailure(ReadinessCode.COMPOSE_UNAVAILABLE)
        else:
            try:
                compose_record = json.loads(output)
            except ValueError:
                compose_record = None
            compose_version = compose_record.get("version") if isinstance(compose_record, dict) else None
            normalized_compose = compose_version.removeprefix("v") if isinstance(compose_version, str) else None
            if not _valid_tool_version(normalized_compose):
                compose = ReadinessFailure(ReadinessCode.COMPOSE_RESPONSE_INVALID)
            else:
                compose = ToolVersionObservation(cast(str, normalized_compose))

        buildx: ToolVersionObservation | ReadinessFailure
        try:
            buildx_version = self._run_with_deadline(
                [*self.prefix, "buildx", "version"],
                environment=self.env,
                deadline=deadline,
                argv=("buildx", "version"),
            ).strip()
            if not _valid_tool_version(buildx_version):
                buildx = ReadinessFailure(ReadinessCode.BUILDX_RESPONSE_INVALID)
            else:
                buildx = ToolVersionObservation(buildx_version)
        except _DockerCommandTimeout:
            buildx = ReadinessFailure(ReadinessCode.BUILDX_TIMEOUT)
        except (InfrastructureFailure, ResourceError):
            buildx = ReadinessFailure(ReadinessCode.BUILDX_UNAVAILABLE)
        return DockerReadinessObservation(client, daemon, compose, buildx)

    def inspect_exact(self, request: ExactInspection, *, deadline: object | None = None) -> DockerObservation:
        """Inspect one exact resource identity without ambient enumeration.

        Docker reports an omitted image tag as ``:latest`` in its exact
        not-found response; normalize only that diagnostic identity. A given ``deadline`` bounds the command
        instead of the fixed 30 seconds.
        """
        return self._inspect(request, verify=True, deadline=deadline)

    def _inspect(self, request: ExactInspection, *, verify: bool, deadline: object | None = None) -> DockerObservation:
        """Inspect one exact identity; ``verify`` selects the per-call identity verification of ``execute``."""
        argv = (request.kind, "inspect", request.identifier)
        if verify:
            result = self.execute(argv, timeout_seconds=self._remaining_seconds(deadline, argv))
            self._require_unspent(deadline, argv)
        else:
            result = self._execute_verified(argv, timeout_seconds=30, environment=None, stream_output=False)
        if result.exit_code:
            empty = "" if request.kind == "builder" else "[]"
            absence = _absence_line(request.kind, request.identifier, self.engine)
            if (
                absence is not None
                and result.exit_code == 1
                and (empty, absence)
                == (
                    result.stdout.strip(),
                    result.stderr.strip(),
                )
            ):
                return DockerObservation(request.kind, request.identifier, False, {})
            raise InfrastructureFailure("Docker exact inspection failed without proving absence.")
        if request.kind == "builder":
            if not result.stdout.strip() or result.stderr.strip():
                raise InfrastructureFailure("Docker builder inspection returned ambiguous evidence.")
            return DockerObservation(
                request.kind,
                request.identifier,
                True,
                {"inspect_sha256": hashlib.sha256(result.stdout.encode()).hexdigest()},
            )
        try:
            rows = json.loads(result.stdout)
        except ValueError as error:
            raise InfrastructureFailure("Docker inspection returned invalid JSON.") from error
        if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], dict):
            raise InfrastructureFailure("Docker inspection returned ambiguous evidence.")
        item = _familiar_image(rows[0]) if request.kind == "image" and self.engine == "podman" else rows[0]
        return DockerObservation(request.kind, request.identifier, True, item)

    def inspect_exact_many(self, requests: tuple[ExactInspection, ...]) -> tuple[ExactInspectionOutcome, ...]:
        """Verify once, inspect every identity without per-call verification, verify again.

        A per-identity failure is carried by that identity's outcome (its exception text only); the two verifications
        are not caught, so a daemon or client change between them discards every outcome.
        """
        self._verify()
        outcomes: list[ExactInspectionOutcome] = []
        for request in requests:
            try:
                outcomes.append(ExactInspectionOutcome(request, self._inspect(request, verify=False), None))
            except InfrastructureFailure as error:
                outcomes.append(ExactInspectionOutcome(request, None, str(error)))
        self._verify()
        return tuple(outcomes)

    def compose(self, request: ComposeMutation) -> DockerResult:
        """Perform one journal-authorized bounded Compose mutation."""
        return self.execute(request.argv, timeout_seconds=self._seconds_until(request.deadline_ns))

    def control_container(self, request: ContainerMutation) -> DockerResult:
        """Start or stop one exact owned container generation."""
        return self.execute(
            ("container", request.action, request.container_id),
            timeout_seconds=self._seconds_until(request.deadline_ns),
        )

    def cleanup_exact(self, request: CleanupMutation) -> DockerResult:
        """Remove one exact journal-owned resource."""
        return self.execute(
            (request.kind, "rm", request.identifier), timeout_seconds=self._seconds_until(request.deadline_ns)
        )

    @staticmethod
    def _seconds_until(deadline_ns: int) -> float:
        if type(deadline_ns) is not int:
            raise InfrastructureFailure("Docker mutation deadline is invalid.")
        return max(0.001, (deadline_ns - time.monotonic_ns()) / 1_000_000_000)


def _identity_changed(message: str = "Docker client changed after authority was bound.") -> InfrastructureFailure:
    """Return the failure for a client, daemon or machine that is no longer the bound one."""
    return InfrastructureFailure(message, reason="daemon-identity-changed")


def _familiar_name(name: str) -> str:
    """Return Docker's familiar spelling of an image name Podman qualified (measured W0e, design 2.6)."""
    for prefix in ("localhost/", "docker.io/library/", "docker.io/"):
        if name.startswith(prefix):
            return name[len(prefix) :]
    return name


def _familiar_image(item: dict) -> dict:
    """Spell a Podman image's ``RepoTags`` and ``RepoDigests`` as Docker does, so every consumer compares them as on
    Docker: Podman gives a locally built name ``localhost/`` and a pulled one ``docker.io/library/``."""
    spelled = dict(item)
    for key in ("RepoTags", "RepoDigests"):
        values = item.get(key)
        if isinstance(values, list):
            spelled[key] = [_familiar_name(value) if isinstance(value, str) else value for value in values]
    return spelled


def _bind_podman(
    machines: MachineFactory | None, prefix: list[str], environment: Mapping[str, str], deadline: Deadline | None
) -> tuple[PodmanBinding, PodmanMachinePort]:
    """Build the machine port for the client's environment and bind the endpoint's running machine (design 2.2)."""
    if machines is None:
        raise InfrastructureFailure("Podman machine port is unavailable.")
    port = machines(environment)
    return resolve_podman_identity(prefix, environment, port, deadline), port


def resolve_podman_identity(
    prefix: list[str], environment: Mapping[str, str], machines: PodmanMachinePort, deadline: Deadline | None
) -> PodmanBinding:
    """Bind the running Podman machine whose API socket is the client's endpoint (design 2.2, in order).

    A non-``darwin`` client, the ``podman-mac-helper`` socket, a missing ``podman`` and an endpoint that is no running
    machine's socket are refusals with their named reasons; a failing or late ``podman`` call is ``client-exit``.
    """
    if sys.platform != "darwin":
        raise InfrastructureFailure(UNSUPPORTED_RUNTIME_TEXT, reason="unsupported-runtime")
    endpoint = DockerCliAdapter.read_endpoint(prefix, environment, deadline)
    if endpoint == PODMAN_HELPER_ENDPOINT:
        raise InfrastructureFailure(PODMAN_HELPER_TEXT, reason="podman-helper-unsupported")
    facts = [machines.inspect(name, deadline) for name in machines.running(deadline)]
    matched = match_machine(endpoint, facts)
    if isinstance(matched, RuntimeRefusal):
        raise InfrastructureFailure(matched.message, reason=matched.reason)
    return PodmanBinding(podman_identity(matched), endpoint, matched, machines.image_dir(deadline))


def _absence_line(kind: str, identifier: str, engine: str = "docker") -> str | None:
    """Return the engine's exact not-found diagnostic for one identity of ``kind``, or None for an unknown kind.

    Docker reports an omitted image tag as ``:latest``; only that diagnostic identity is normalized. Podman's compat
    API words them differently (measured 2026-11-04, design W4); its volume line names no volume.
    """
    if kind == "builder":
        return f'ERROR: no builder "{identifier}" found'
    if engine == "podman":
        daemon = "Error response from daemon: "
        return {
            "container": f'{daemon}no container with name or ID "{identifier}" found: no such container',
            "image": f"{daemon}failed to find image {identifier}: docker.io/library/{identifier}: No such image",
            "network": f"{daemon}unable to find network with name or ID {identifier}: network not found",
            "volume": f"{daemon}no such volume",
        }.get(kind)
    if kind == "image" and "@" not in identifier and ":" not in identifier.rsplit("/", 1)[-1]:
        identifier += ":latest"
    return {
        "container": f"Error response from daemon: No such container: {identifier}",
        "image": f"Error response from daemon: No such image: {identifier}",
        "network": f"Error response from daemon: network {identifier} not found",
        "volume": f"Error response from daemon: get {identifier}: no such volume",
    }.get(kind)


def _survivors_match(kind: str, listed: list[str], survivors: list[dict]) -> bool:
    """Report whether the inspected survivors are exactly the listed, not vanished identities, one each.

    ``ls -q`` lists short IDs (names for volumes); ``inspect`` returns full IDs (``sha256:``-prefixed for images).
    """
    keys = []
    for item in survivors:
        key = item.get("Name") if kind == "volume" else item.get("Id")
        if not isinstance(key, str):
            return False
        keys.append(key.removeprefix("sha256:"))
    matched = []
    for key in keys:
        owners = [
            identifier
            for identifier in listed
            if key == identifier or (kind != "volume" and key.startswith(identifier))  # volume names are exact
        ]
        if len(owners) != 1:
            return False
        matched.append(owners[0])
    return sorted(matched) == listed


_IDENTITY_RETRY_PAUSES = (0.2, 0.4)  # seconds before the second and the third identity read
_PROBE_REMOVAL_SECONDS = 1.0  # NOTE: fixed share of a probe's deadline kept for `rm -f` at its timeout


def _vanished(kind: str, ids: list[str], result: DockerResult, engine: str = "docker") -> set[str]:
    """Return the listed identities an inventory inspection proved absent, or an empty set.

    Another project may remove an object between ``ls`` and ``inspect``. Docker then exits 1, prints the survivors and
    one exact not-found line per vanished identity. Only that exact shape proves absence; any other exit status or
    diagnostic line proves nothing, so the caller keeps failing.
    """
    lines = [line for line in result.stderr.splitlines() if line.strip()]
    if engine == "podman" and kind == "volume" and len(ids) > 1:
        return set()  # Podman's volume line names no volume: it cannot tell which of several vanished
    absences = {identifier: _absence_line(kind, identifier, engine) for identifier in ids}
    vanished = {identifier for identifier, line in absences.items() if line is not None and line in lines}
    if result.exit_code != 1 or not vanished or sorted(lines) != sorted(str(absences[i]) for i in vanished):
        return set()
    return vanished


# Closed causes of a failed client read, matched on the lowercased diagnostic (first match wins). Only the code leaves
# the adapter: the child's output can carry secrets from the command's environment.
_CLIENT_EXIT_CAUSES = (
    ("connection refused", "connect-refused"),
    ("connection reset", "connect-reset"),
    ("cannot connect to the docker daemon", "daemon-unreachable"),
    ("failed to connect to the docker api", "daemon-unreachable"),  # Docker 29's wording
    ("context not found", "context-missing"),
    ("unexpected eof", "connection-eof"),
    (": eof", "connection-eof"),
    ("deadline exceeded", "client-timeout"),
    ("error response from daemon", "daemon-error"),  # last: the daemon's envelope wraps the more specific causes
)


def _client_exit(argv: Sequence[str], exit_code: int, stderr: str) -> InfrastructureFailure:
    """Name a read-only client call that exited non-zero: its leading words, exit status and a closed cause.

    The reason ``client-exit`` makes it a late read for admission and observation passes: nothing was read, so the
    next pass reads again.
    """
    text = stderr.lower()
    cause = next((code for marker, code in _CLIENT_EXIT_CAUSES if marker in text), "other")
    command = " ".join(word for word in argv[:2] if not word.startswith("-"))
    return InfrastructureFailure(
        f"Docker client `{command}` exited with status {exit_code} ({cause}).", reason="client-exit"
    )


def _control_failure(argv: tuple[str, ...], result: DockerResult) -> InfrastructureFailure:
    """Name the failed control command by its leading words (never flags or values) and its exit status.

    The child's output never enters the message: it can carry secrets from the command's environment.
    """
    command = " ".join(word for word in argv[:2] if not word.startswith("-"))
    return InfrastructureFailure(f"Docker control command `{command}` failed with exit status {result.exit_code}.")
