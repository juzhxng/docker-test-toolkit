"""Composition roots for the CLI and public facade."""

import json
import sys

from docker_test_toolkit.adapters.outbound.docker import DockerCliAdapter
from docker_test_toolkit.adapters.outbound.podman import PodmanCliAdapter
from docker_test_toolkit.application.lifecycle import Deadline
from docker_test_toolkit.application.supervision import private_process_environment
from docker_test_toolkit.domain.models import DaemonIdentity


def _docker_environment() -> dict[str, str]:
    return private_process_environment()


def build_docker_adapter(deadline: Deadline | None = None) -> DockerCliAdapter:
    """Discover the one exact Docker client before a private shim changes PATH."""
    return DockerCliAdapter.discover(environment=_docker_environment(), deadline=deadline, machines=PodmanCliAdapter)


def bind_docker_adapter(identity: DaemonIdentity) -> DockerCliAdapter:
    """Reconstruct the concrete adapter from one durable exact identity."""
    return DockerCliAdapter.from_identity(identity, environment=_docker_environment(), machines=PodmanCliAdapter)


_PRIVATE_CAPABLE = frozenset({"_guardian", "_recover", "_python_child"})  # the commands that accept --test-directory


def _private_invocation(values: list[str]) -> bool:
    """Return whether an internal child was launched for a private test domain (``--test-directory`` as its option).

    Only the lifecycle children accept the option, and only toolkit-written argv reaches them; everything after a
    ``--`` (the Docker shim's user arguments) is data, never an option.
    """
    if values[:1] == [] or values[0] not in _PRIVATE_CAPABLE:
        return False
    options = values[1 : values.index("--")] if "--" in values else values[1:]
    return any(value == "--test-directory" or value.startswith("--test-directory=") for value in options)


def _refuse_unreadable_state(values: list[str]) -> int | None:
    """Run the start check (design 3.4): refuse every real-account entry whose state format this toolkit cannot read.

    The check applies only inside an installation and never to a private invocation (``--test-directory``),
    ``install`` or ``protocol``; ``readiness inspect`` is dispatched before it. Returns the refusal's exit code after
    printing one error envelope, or None to proceed.
    """
    from docker_test_toolkit.application.admission import check_account_format
    from docker_test_toolkit.application.protocol import error_envelope
    from docker_test_toolkit.application.state import canonical_account
    from docker_test_toolkit.domain.errors import ToolkitError

    account = canonical_account()
    if account is None or values[:1] in (["install"], ["protocol"]) or _private_invocation(values):
        return None
    try:
        check_account_format(account)
    except ToolkitError as error:
        response, exit_code = error_envelope(" ".join(values[:2]) or "unknown", error)
        print(json.dumps(response, sort_keys=True, separators=(",", ":")))
        return exit_code
    return None


def main(argv: list[str] | None = None) -> int:
    """Compose and invoke the machine-readable CLI adapter."""
    from docker_test_toolkit.application import lifecycle

    values = list(sys.argv[1:] if argv is None else argv)
    readiness = not values or values[:2] == ["readiness", "inspect"]
    lifecycle.configure_docker_factories(build_docker_adapter, bind_docker_adapter)
    if readiness:
        from docker_test_toolkit.adapters.inbound.cli import main as cli_main

        return cli_main(values)
    if values[:2] == ["guard", "docker-command"]:  # a coding-assistant hook: reads no account state
        from docker_test_toolkit.adapters.inbound.harness_guard import main as guard_main

        return guard_main(values[2:])
    if values[:1] == ["install"]:  # the installer runs its own format check under the account lock (design 2.3)
        from docker_test_toolkit.adapters.inbound.cli import install_main

        return install_main(values[1:])
    refused = _refuse_unreadable_state(values)
    if refused is not None:
        return refused
    if values[:1] in [["_guardian"], ["_python_child"], ["_recover"]]:
        return lifecycle.main(values)
    if values[:1] == ["_docker_dispatch"]:
        from docker_test_toolkit.adapters.inbound.bridge import main as dispatch_main

        return dispatch_main(values[1:])
    if values[:2] == ["bridge", "lease"]:
        if len(values) != 2:
            from docker_test_toolkit.adapters.inbound.cli import main as cli_main

            return cli_main(values)
        from docker_test_toolkit.adapters.inbound.bridge import serve_lease

        return serve_lease(sys.stdin.buffer, sys.stdout)
    if values[:2] == ["bridge", "batch"]:
        if len(values) != 2:
            from docker_test_toolkit.adapters.inbound.cli import main as cli_main

            return cli_main(values)
        from docker_test_toolkit.adapters.inbound.bridge import serve_batch

        return serve_batch(sys.stdin.buffer, sys.stdout)
    from docker_test_toolkit.adapters.inbound.cli import main as cli_main

    return cli_main(values)


if __name__ == "__main__":
    raise SystemExit(main())
