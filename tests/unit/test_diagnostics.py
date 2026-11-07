from __future__ import annotations

from docker_test_toolkit.application.protocol import error_envelope
from docker_test_toolkit.domain.errors import InfrastructureFailure


def test_diagnostics_redact_urls_tokens_and_environment_values() -> None:
    sentinel = "sentinel-do-not-print"
    envelope, exit_code = error_envelope(
        "resource.observe",
        InfrastructureFailure(
            f"failed https://user:{sentinel}@example.invalid/path?token={sentinel} Authorization: Bearer {sentinel}"
        ),
    )
    rendered = repr(envelope)

    assert exit_code == 5
    assert sentinel not in rendered
    assert "<redacted>" in rendered
