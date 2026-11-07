"""Build synthetic OCI image-layout archives for the verifier and the export service tests."""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import tarfile
from pathlib import Path

OCI_MANIFEST = "application/vnd.oci.image.manifest.v1+json"
OCI_INDEX = "application/vnd.oci.image.index.v1+json"


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def json_bytes(value: object) -> bytes:
    return json.dumps(value, separators=(",", ":")).encode()


class Layout:
    """Describe one synthetic OCI layout; each field can be mutated before it is written."""

    def __init__(self, *, docker_manifest: bool = True, attestation: bool = False, index_root: bool = False) -> None:
        self.layer = gzip.compress(b"layer-bytes")
        self.config = json_bytes(
            {"os": "linux", "architecture": "arm64", "rootfs": {"type": "layers", "diff_ids": [f"sha256:{'0' * 64}"]}}
        )
        self.manifest = json_bytes(
            {
                "schemaVersion": 2,
                "mediaType": OCI_MANIFEST,
                "config": self._descriptor("application/vnd.oci.image.config.v1+json", self.config),
                "layers": [self._descriptor("application/vnd.oci.image.layer.v1.tar+gzip", self.layer)],
            }
        )
        self.blobs: dict[str, bytes] = {
            digest(self.layer): self.layer,
            digest(self.config): self.config,
            digest(self.manifest): self.manifest,
        }
        self.root_bytes = self.manifest
        root_media = OCI_MANIFEST
        if index_root or attestation:
            attestation_config = b"{}"
            statement = b'{"_type":"https://in-toto.io/Statement/v0.1"}'
            attestation_manifest = json_bytes(
                {
                    "schemaVersion": 2,
                    "mediaType": OCI_MANIFEST,
                    "config": self._descriptor("application/vnd.oci.empty.v1+json", attestation_config),
                    "layers": [self._descriptor("application/vnd.in-toto+json", statement)],
                }
            )
            children = [
                {
                    **self._descriptor(OCI_MANIFEST, self.manifest),
                    "platform": {"os": "linux", "architecture": "arm64"},
                }
            ]
            if attestation:
                self.blobs[digest(attestation_config)] = attestation_config
                self.blobs[digest(statement)] = statement
                self.blobs[digest(attestation_manifest)] = attestation_manifest
                children.append(
                    {
                        **self._descriptor(OCI_MANIFEST, attestation_manifest),
                        "platform": {"os": "unknown", "architecture": "unknown"},
                        "annotations": {"vnd.docker.reference.type": "attestation-manifest"},
                    }
                )
            self.root_bytes = json_bytes({"schemaVersion": 2, "mediaType": OCI_INDEX, "manifests": children})
            self.blobs[digest(self.root_bytes)] = self.root_bytes
            root_media = OCI_INDEX
        self.root_digest = f"sha256:{digest(self.root_bytes)}"
        self.index = json_bytes({"schemaVersion": 2, "manifests": [self._descriptor(root_media, self.root_bytes)]})
        self.oci_layout = json_bytes({"imageLayoutVersion": "1.0.0"})
        self.docker_manifest: bytes | None = None
        if docker_manifest:
            self.docker_manifest = json_bytes(
                [
                    {
                        "Config": f"blobs/sha256/{digest(self.config)}",
                        "RepoTags": None,
                        "Layers": [f"blobs/sha256/{digest(self.layer)}"],
                    }
                ]
            )

    @staticmethod
    def _descriptor(media: str, data: bytes) -> dict[str, object]:
        return {"mediaType": media, "digest": f"sha256:{digest(data)}", "size": len(data)}

    def members(self) -> list[tuple[str, bytes, bytes]]:
        """Return ``(name, body, typeflag)`` rows in the order Docker writes them."""
        rows: list[tuple[str, bytes, bytes]] = [
            ("blobs", b"", tarfile.DIRTYPE),
            ("blobs/sha256", b"", tarfile.DIRTYPE),
        ]
        rows += [(f"blobs/sha256/{name}", body, tarfile.REGTYPE) for name, body in sorted(self.blobs.items())]
        rows.append(("index.json", self.index, tarfile.REGTYPE))
        if self.docker_manifest is not None:
            rows.append(("manifest.json", self.docker_manifest, tarfile.REGTYPE))
        rows.append(("oci-layout", self.oci_layout, tarfile.REGTYPE))
        return rows


class DockerArchive:
    """Describe one synthetic Docker-format archive in the layout Podman's ``image save`` writes: uncompressed
    layer tars and the config named by their SHA-256, ``manifest.json``, ``repositories`` and per layer a legacy
    ``<id>/`` folder whose ``layer.tar`` links to ``../<hex>.tar``. Each field can be mutated before it is written."""

    def __init__(self) -> None:
        self.layers = [b"base-layer-tar" * 64, b"top-layer-tar"]
        self.config = json_bytes(
            {
                "os": "linux",
                "architecture": "arm64",
                "rootfs": {"type": "layers", "diff_ids": [f"sha256:{digest(layer)}" for layer in self.layers]},
            }
        )
        self.listed = [f"{digest(layer)}.tar" for layer in self.layers]
        self.extra: list[tuple[str, bytes, bytes]] = []

    @property
    def image_id(self) -> str:
        return f"sha256:{digest(self.config)}"

    @property
    def root_digest(self) -> str:
        """The image ID under the name the export harness reads: Podman's image ID is its config digest."""
        return self.image_id

    @property
    def manifest(self) -> bytes:
        return json_bytes(
            [{"Config": f"{digest(self.config)}.json", "RepoTags": ["localhost/built:1"], "Layers": self.listed}]
        )

    def members(self) -> list[tuple[str, bytes, bytes]]:
        """Return ``(name, body, typeflag)`` rows in the order Podman writes them; a link row's body is its target."""
        rows: list[tuple[str, bytes, bytes]] = [
            (f"{digest(layer)}.tar", layer, tarfile.REGTYPE) for layer in self.layers
        ]
        rows.append((f"{digest(self.config)}.json", self.config, tarfile.REGTYPE))
        for index, layer in enumerate(self.layers):
            legacy = f"{index:064x}"
            rows += [
                (legacy, b"", tarfile.DIRTYPE),
                (f"{legacy}/VERSION", b"1.0", tarfile.REGTYPE),
                (f"{legacy}/json", json_bytes({"id": legacy}), tarfile.REGTYPE),
                (f"{legacy}/layer.tar", f"../{digest(layer)}.tar".encode(), tarfile.SYMTYPE),
            ]
        rows += [
            ("manifest.json", self.manifest, tarfile.REGTYPE),
            ("repositories", json_bytes({"localhost/built": {"1": "0" * 64}}), tarfile.REGTYPE),
        ]
        return rows + self.extra


def write_archive(
    path: Path, rows: list[tuple[str, bytes, bytes]], *, tail: bytes = b"", terminate: bool = True
) -> Path:
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w", format=tarfile.USTAR_FORMAT) as handle:
        for name, body, kind in rows:
            info = tarfile.TarInfo(name)
            info.type = kind
            content = kind not in (tarfile.DIRTYPE, tarfile.SYMTYPE)
            info.size = len(body) if content else 0
            info.mode = 0o755 if kind == tarfile.DIRTYPE else 0o444
            if kind == tarfile.SYMTYPE:  # a link row carries its target as its body
                info.linkname = body.decode()
            handle.addfile(info, io.BytesIO(body) if content else None)
    data = output.getvalue()
    if not terminate:
        data = data.rstrip(b"\0")
        data += b"\0" * (-len(data) % 512)
    path.write_bytes(data + tail)
    return path
