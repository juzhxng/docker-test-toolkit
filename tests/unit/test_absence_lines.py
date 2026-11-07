"""Not-found diagnostics that prove absence, per engine."""

from __future__ import annotations

import pytest

from docker_test_toolkit.adapters.outbound.docker import _absence_line, _vanished
from docker_test_toolkit.domain.models import DockerResult

ID = "0" * 64


@pytest.mark.parametrize(
    ("kind", "identifier", "line"),
    [
        ("container", ID, f'Error response from daemon: no container with name or ID "{ID}" found: no such container'),
        (
            "image",
            f"sha256:{ID}",
            "Error response from daemon: failed to find image"
            f" sha256:{ID}: docker.io/library/sha256:{ID}: No such image",
        ),
        ("network", ID, f"Error response from daemon: unable to find network with name or ID {ID}: network not found"),
        ("volume", f"dtt-{ID}", "Error response from daemon: no such volume"),
        ("builder", "dtt-absent", 'ERROR: no builder "dtt-absent" found'),  # Buildx is the client's, on every engine
    ],
)
def test_podman_not_found_lines_are_the_measured_ones(kind: str, identifier: str, line: str) -> None:
    assert _absence_line(kind, identifier, "podman") == line
    assert _absence_line(kind, identifier) != line or kind == "builder"  # Docker's wording is unchanged


def test_an_identity_less_line_never_proves_which_of_several_volumes_vanished() -> None:
    line = "Error response from daemon: no such volume"
    one = DockerResult(1, "[]", line)
    two = DockerResult(1, "[]", f"{line}\n")

    assert _vanished("volume", ["a"], one, "podman") == {"a"}
    assert _vanished("volume", ["a", "b"], two, "podman") == set()  # either could be the vanished one
    assert _vanished(
        "container", [ID], DockerResult(1, "[]", _absence_line("container", ID, "podman") or ""), "podman"
    ) == {ID}


def test_podman_image_names_read_in_dockers_familiar_spelling() -> None:
    """W0e: Podman prefixes a built name with localhost/ and a pulled one with docker.io/library/."""
    from docker_test_toolkit.adapters.outbound.docker import _familiar_image

    digest = "sha256:" + "b" * 64
    built = {
        "Id": "sha256:" + "2" * 64,
        "RepoTags": ["localhost/team/app:v3", "registry.example.com/team/app:v2", "localhost/dtt-export:run-1"],
        "RepoDigests": [f"localhost/dtt-export@{digest}", f"registry.example.com/team/app@{digest}"],
    }
    pulled = {"RepoTags": [], "RepoDigests": [f"docker.io/library/redis@{digest}", f"docker.io/team/cache@{digest}"]}

    assert _familiar_image(built)["RepoTags"] == ["team/app:v3", "registry.example.com/team/app:v2", "dtt-export:run-1"]
    assert _familiar_image(built)["RepoDigests"] == [f"dtt-export@{digest}", f"registry.example.com/team/app@{digest}"]
    assert _familiar_image(pulled)["RepoDigests"] == [f"redis@{digest}", f"team/cache@{digest}"]
    assert _familiar_image({"Id": "x"}) == {"Id": "x"}  # no names: unchanged
