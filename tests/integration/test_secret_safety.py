from __future__ import annotations

import os
from pathlib import Path

import pytest

from docker_test_toolkit.adapters.outbound.docker import DockerCliAdapter
from docker_test_toolkit.domain.errors import InfrastructureFailure

pytestmark = pytest.mark.integration


def test_child_output_and_environment_secrets_never_enter_adapter_errors(tmp_path: Path) -> None:
    sentinel = "never-print-this-secret"
    binary = tmp_path / "docker"
    binary.write_text(
        "#!/bin/sh\n"
        'case "${DOCKER_CONTEXT:-}|$*" in\n'
        '  "|context show") printf "fixture\\n" ;;\n'
        '  "fixture|version --format {{.Client.Version}}") printf "1\\n" ;;\n'
        '  "fixture|version --format {{.Client.Version}} {{json .Server.Components}}") printf "1 null\\n" ;;\n'
        '  "fixture|info --format {{.ID}}") printf "11111111-1111-1111-1111-111111111111\\n" ;;\n'
        f'  *) printf "{sentinel} $TOKEN\\n" >&2; exit 9 ;;\n'
        "esac\n"
    )
    binary.chmod(0o700)
    adapter = DockerCliAdapter.discover(binary, environment={"PATH": os.environ["PATH"], "TOKEN": sentinel})

    with pytest.raises(InfrastructureFailure) as raised:
        adapter.call("failing")

    assert sentinel not in str(raised.value)
