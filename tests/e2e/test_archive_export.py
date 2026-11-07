"""Real-Docker proof of the archive exports through the private shim: ``image save`` (B) and ``--output type=oci`` (C).

Images are built ``FROM`` the pinned redis digest plus a marker file on the run's owned builder. Archives are checked by
a stdlib reader that applies the layers (whiteouts included), and the effective filesystem is compared with what the
same daemon reports when it runs the image through the shim.
"""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import shutil
import subprocess
import tarfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import pytest

from docker_test_toolkit import ArtifactIdentity, PhaseBudget, RunSpec
from docker_test_toolkit.application import image_archive
from docker_test_toolkit.domain.errors import ArchiveRejected
from docker_test_toolkit.factories.testing import build_test_services

pytestmark = pytest.mark.e2e

REDIS = "redis@sha256:becdda6c7f4b3fb42e42fd7f120bbf5c54c4caaaf16f26da24e4563d2c1f0576"
INVENTORY = "lib/apk/db/installed"
RELEASE = "etc/alpine-release"


@dataclass
class Exported:
    """One entered run with its shim and the helpers that talk to it."""

    handle: object
    project: Path
    left: bool = False

    def leave(self) -> object:
        self.left = True
        return self.handle.leave("passed")

    def shim(self, *argv: str) -> subprocess.CompletedProcess[str]:
        context = self.handle.docker_context
        return subprocess.run(
            [str(context.executable), *argv],
            check=False,
            cwd=self.project,
            env=dict(context.environment),
            text=True,
            capture_output=True,
            timeout=1200,
        )

    def build(self, marker: str) -> str:
        """Build ``FROM`` the pinned base plus a marker and return the owned image ID."""
        (self.project / "Dockerfile").write_text(f"FROM {REDIS}\nRUN echo {marker} > /marker.txt\n")
        tag = f"dtt-export:{self.handle.run_id}-{marker}"
        built = self.shim("buildx", "build", "--tag", tag, ".")
        assert built.returncode == 0, built.stderr
        record = json.loads(self.handle._context.record.read_text())
        (image_id,) = [
            key
            for key, item in record["owned_resources"]["images"].items()
            if item["state"] == "present" and tag in item["evidence"]["repo_tags"]
        ]
        return image_id


@contextmanager
def _exported_run(root: Path) -> Iterator[Exported]:
    """One entered run with an owned Buildx builder; it is left with exact cleanup when the caller is done."""
    project = root / "project"
    project.mkdir()
    spec = RunSpec(
        project_root=project,
        entrypoint="e2e",
        purpose="image-export",
        services=("fixture",),
        closure=("fixture",),
        profile="fixture",
        phases={"tests": PhaseBudget(2 * 1024**3, 128 * 1024**2, 2, 1)},
        initial_phase="tests",
        artifact=ArtifactIdentity(
            "0.1.0",
            "a" * 64,
            {
                "activation": 1,
                "batch": 1,
                "cli": 1,
                "distribution": 1,
                "queue": 1,
                "receipt": 1,
                "registry": 2,
                "run": 1,
            },
        ),
        wait_seconds=60,
    )
    handle = build_test_services(root / "state").open_run(spec, admit_docker=True).enter()
    run = Exported(handle, project)
    try:
        assert run.shim("buildx", "create").returncode == 0
        yield run
    finally:
        if not run.left:
            assert run.leave().cleanup_outcome == "complete"


@pytest.fixture
def exported(tmp_path: Path, redis_image: None) -> Iterator[Exported]:
    with _exported_run(tmp_path) as run:
        yield run


@pytest.fixture(scope="module")
def shared_exported(tmp_path_factory: pytest.TempPathFactory, redis_image: None) -> Iterator[Exported]:
    """One run for tests that change nothing in it: each asserts it left no receipt and no exports directory."""
    with _exported_run(tmp_path_factory.mktemp("shared-export")) as run:
        yield run


def _blob(outer: tarfile.TarFile, digest: str) -> bytes:
    return outer.extractfile(f"blobs/sha256/{digest.removeprefix('sha256:')}").read()  # type: ignore[union-attr]


def _oci_layers(outer: tarfile.TarFile) -> list[str]:
    """Return the layer member names of the one image manifest an OCI layout selects (an index picks arm64)."""
    (root,) = json.loads(outer.extractfile("index.json").read())["manifests"]  # type: ignore[union-attr]
    document = json.loads(_blob(outer, root["digest"]))
    if "manifests" in document:
        (root,) = [
            child
            for child in document["manifests"]
            if child.get("annotations", {}).get("vnd.docker.reference.type") != "attestation-manifest"
        ]
        document = json.loads(_blob(outer, root["digest"]))
    return [f"blobs/sha256/{layer['digest'].removeprefix('sha256:')}" for layer in document["layers"]]


def effective_files(archive: Path, wanted: set[str]) -> dict[str, bytes]:
    """Apply the image layers in order (whiteouts and opaque directories included) and read only ``wanted`` files."""
    with tarfile.open(archive) as outer:
        names = outer.getnames()
        # an OCI layout stores gzip layers; Podman's Docker-format archive stores them uncompressed
        unpack = gzip.decompress if "oci-layout" in names else bytes
        layers = (
            json.loads(outer.extractfile("manifest.json").read())[0]["Layers"]  # type: ignore[union-attr]
            if "manifest.json" in names
            else _oci_layers(outer)
        )
        located: dict[str, tuple[str, str]] = {}
        for layer in layers:
            blob = outer.extractfile(layer).read()  # type: ignore[union-attr]
            with tarfile.open(fileobj=io.BytesIO(unpack(blob))) as inner:
                for member in inner:
                    name = member.name.removeprefix("./").rstrip("/")
                    directory, _, base = name.rpartition("/")
                    if base == ".wh..wh..opq":
                        for path in [path for path in located if path.startswith(directory + "/")]:
                            del located[path]
                    elif base.startswith(".wh."):
                        removed = f"{directory}/{base[4:]}".lstrip("/")
                        for path in [path for path in located if path == removed or path.startswith(removed + "/")]:
                            del located[path]
                    elif member.isfile():
                        located[name] = (layer, member.name)
        contents: dict[str, bytes] = {}
        for path in wanted & located.keys():
            layer, member_name = located[path]
            blob = outer.extractfile(layer).read()  # type: ignore[union-attr]
            with tarfile.open(fileobj=io.BytesIO(unpack(blob))) as inner:
                contents[path] = inner.extractfile(member_name).read()  # type: ignore[union-attr]
    return contents


def _binding(exported: Exported, image_id: str) -> image_archive.ImageBinding:
    inspected = subprocess.run(["docker", "image", "inspect", image_id], check=True, capture_output=True, text=True)
    item = json.loads(inspected.stdout)[0]
    return image_archive.ImageBinding(
        image_id, image_archive.normalized_platform(item["Os"], item["Architecture"], item.get("Variant"))
    )


def _verify(path: Path, binding: image_archive.ImageBinding) -> image_archive.ArchiveFacts:
    descriptor = path.open("rb")
    try:
        return image_archive.verify_archive(descriptor.fileno(), binding)
    finally:
        descriptor.close()


def test_export_is_bound_to_the_daemon_image_and_rejects_tampering(exported: Exported, tmp_path: Path) -> None:
    image_id = exported.build("marker-one")

    completed = exported.shim("image", "save", image_id)

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.count("\n") == 1
    line = json.loads(completed.stdout)
    published = Path(line["path"])
    receipt = next(
        item
        for item in json.loads(exported.handle._context.record.read_text())["docker_receipts"].values()
        if item["kind"] == "image-save"
    )
    assert receipt["state"] == "promoted"
    assert line["sha256"] == hashlib.sha256(published.read_bytes()).hexdigest() == receipt["plan"]["archive"]["sha256"]
    assert line["bytes"] == published.stat().st_size
    assert line["root_digest"] == line["image_id"] == image_id
    assert line["layout"] in {"oci+docker-manifest", "docker-archive"} and line["format"] == "docker-image-save"
    if line["layout"] == "docker-archive":  # Podman: the image ID is the config digest
        assert line["root_digest"] == line["config_digest"]
    assert list(published.parent.iterdir()) == [published]  # no staging directory is left

    # The reader's view of the archive equals the daemon's view of the running image.
    files = effective_files(published, {"marker.txt", INVENTORY, RELEASE})
    assert files["marker.txt"] == b"marker-one\n"
    assert files[RELEASE].strip().startswith(b"3.")
    ran = exported.shim("run", "--rm", image_id, "cat", f"/{INVENTORY}")
    assert ran.returncode == 0, ran.stderr
    assert ran.stdout.encode() == files[INVENTORY]
    release = exported.shim("run", "--rm", image_id, "cat", f"/{RELEASE}")
    assert release.returncode == 0 and release.stdout.encode() == files[RELEASE]

    # The ownership of the exported image is unchanged and a repeat export is refused with the recorded facts.
    record = json.loads(exported.handle._context.record.read_text())
    assert record["owned_resources"]["images"][image_id]["state"] == "present"
    repeat = exported.shim("image", "save", image_id)
    assert repeat.returncode == 2 and line["sha256"] in repeat.stderr

    # Tampering with a copy is rejected by the verifier the shim itself used.
    binding = _binding(exported, image_id)
    copy = tmp_path / "copy.tar"
    shutil.copyfile(published, copy)
    assert _verify(copy, binding).sha256 == line["sha256"]
    data = bytearray(copy.read_bytes())
    data[len(data) // 2] ^= 0xFF
    flipped = tmp_path / "flipped.tar"
    flipped.write_bytes(bytes(data))
    truncated = tmp_path / "truncated.tar"
    truncated.write_bytes(bytes(data[: len(data) - 4096]))
    for damaged in (flipped, truncated):
        with pytest.raises(ArchiveRejected):
            _verify(damaged, binding)

    # A well-formed archive of another image is not this image.
    other_id = exported.build("marker-two")
    other = json.loads(exported.shim("image", "save", other_id).stdout)
    with pytest.raises(ArchiveRejected):
        _verify(Path(other["path"]), binding)

    # The adopter copies the archive out before leave; leave removes the private copy and leaves the copy intact.
    kept = tmp_path / "kept.tar"
    shutil.copyfile(published, kept)
    assert hashlib.sha256(kept.read_bytes()).hexdigest() == line["sha256"]
    assert exported.leave().cleanup_outcome == "complete"
    assert not published.exists()
    assert hashlib.sha256(kept.read_bytes()).hexdigest() == line["sha256"]


def test_a_foreign_image_is_never_exported(exported: Exported) -> None:
    exported.build("marker-owner")
    foreign = "sha256:" + "0" * 64

    completed = exported.shim("image", "save", foreign)

    assert completed.returncode == 2
    record = json.loads(exported.handle._context.record.read_text())
    assert not [item for item in record["docker_receipts"].values() if item["kind"] == "image-save"]
    assert not (exported.handle.private_root / "exports").exists()


def _build_receipts(exported: Exported) -> list[dict]:
    record = json.loads(exported.handle._context.record.read_text())
    return [item for item in record["docker_receipts"].values() if item["kind"] == "build"]


@pytest.mark.parametrize(
    "arguments",
    [
        ("--output", "type=docker"),
        ("--output", "type=oci"),
        ("-o", "type=oci,dest=/tmp/outside.tar"),
        ("--load", "-o", "type=oci"),
        ("-o", "type=oci", "--tag", "x:1"),
    ],
)
def test_build_outputs_are_refused_before_any_receipt(shared_exported: Exported, arguments: tuple[str, ...]) -> None:
    (shared_exported.project / "Dockerfile").write_text(f"FROM {REDIS}\n")

    refused = shared_exported.shim("buildx", "build", *arguments, ".")

    assert refused.returncode == 2
    assert _build_receipts(shared_exported) == []
    assert not (shared_exported.handle.private_root / "exports").exists()
