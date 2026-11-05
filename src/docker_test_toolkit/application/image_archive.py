"""Verify, stage and classify exported image archives without extracting or decompressing them.

The module is stdlib-only and knows nothing about receipts, Docker or the run record: callers hand it descriptors
and names, and it answers with facts, closed failure classes or the first ``OSError`` of a cleanup step.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import tarfile
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from docker_test_toolkit.domain.errors import ArchiveRejected, ResourceError
from docker_test_toolkit.domain.platform import Platform
from docker_test_toolkit.domain.records import unique_object

# NOTE: per-archive cap is checked at admission and after the write, not during it; an export can
# overshoot until it ends and is rejected; upgrade path: an adapter stdout pump with a byte limit.
ARCHIVE_MAX_BYTES = 4 * 1024**3
METADATA_MEMBER_MAX = 4 * 1024**2
METADATA_TOTAL_MAX = 64 * 1024**2
MEMBER_COUNT_MAX = 65_536

_BLOCK = 512
_ZERO_BLOCK = bytes(_BLOCK)
_CHUNK = 1024**2
_DIGEST = re.compile(r"sha256:([0-9a-f]{64})\Z")
_BLOB_NAME = re.compile(r"blobs/sha256/([0-9a-f]{64})\Z")
_FIXED_FILES = frozenset({"oci-layout", "index.json", "manifest.json"})
_DIRECTORIES = frozenset({"blobs", "blobs/sha256"})
# The Docker archive format Podman's `image save` writes (design 2026-11-03-podman-runtime 2.7, measured W0d)
_DOCKER_ID_DIR = re.compile(r"[0-9a-f]{64}\Z")
_DOCKER_CONTENT = re.compile(r"([0-9a-f]{64})\.(?:tar|json)\Z")
_DOCKER_LEGACY = re.compile(r"[0-9a-f]{64}/(?:VERSION|json)\Z")
_DOCKER_LAYER_LINK = re.compile(r"[0-9a-f]{64}/layer\.tar\Z")
_LAYER_LINK_TARGET = re.compile(r"\.\./([0-9a-f]{64}\.tar)\Z")
_OCI_MANIFEST = "application/vnd.oci.image.manifest.v1+json"
_DOCKER_MANIFEST = "application/vnd.docker.distribution.manifest.v2+json"
_OCI_INDEX = "application/vnd.oci.image.index.v1+json"
_DOCKER_LIST = "application/vnd.docker.distribution.manifest.list.v2+json"
_MANIFEST_TYPES = frozenset({_OCI_MANIFEST, _DOCKER_MANIFEST})
_INDEX_TYPES = frozenset({_OCI_INDEX, _DOCKER_LIST})
_CONFIG_TYPES = frozenset(
    {
        "application/vnd.oci.image.config.v1+json",
        "application/vnd.docker.container.image.v1+json",
        "application/vnd.oci.empty.v1+json",
    }
)
_LAYER_TYPES = frozenset(
    {
        "application/vnd.oci.image.layer.v1.tar",
        "application/vnd.oci.image.layer.v1.tar+gzip",
        "application/vnd.oci.image.layer.v1.tar+zstd",
        "application/vnd.docker.image.rootfs.diff.tar.gzip",
        "application/vnd.in-toto+json",
        "application/vnd.dsse.envelope.v1+json",
    }
)
_ATTESTATION_ANNOTATION = "vnd.docker.reference.type"
_ATTESTATION = "attestation-manifest"


def normalized_platform(os_name: object, architecture: object, variant: object = None) -> Platform:
    """Return the platform of the given OCI fields or refuse a malformed one."""
    platform = Platform.from_fields(os_name, architecture, variant)
    if platform is None:
        raise ArchiveRejected("verification-failed", "platform is malformed")
    return platform


@dataclass(frozen=True)
class ImageBinding:
    """Bind an export to the exact image identity the daemon reported (Story B)."""

    image_id: str
    platform: Platform


@dataclass(frozen=True)
class ArchiveFacts:
    """Describe a verified archive."""

    bytes: int
    sha256: str
    layout: str
    root_digest: str
    manifest_digest: str
    config_digest: str
    platform: Platform

    def as_record(self) -> dict[str, object]:
        """Return the closed ``published`` fields of ``plan["archive"]`` that the verifier knows."""
        return {
            "bytes": self.bytes,
            "sha256": self.sha256,
            "layout": self.layout,
            "root_digest": self.root_digest,
            "manifest_digest": self.manifest_digest,
            "config_digest": self.config_digest,
            "platform": self.platform.as_dict(),
        }


@dataclass(frozen=True)
class _Member:
    kind: bytes
    offset: int
    size: int
    sha256: str
    link_target: str | None = None


@dataclass
class _Walk:
    members: dict[str, _Member] = field(default_factory=dict)
    retained: dict[str, bytes] = field(default_factory=dict)
    total: int = 0
    sha256: str = ""
    grammars: set[str] = field(default_factory=set)  # "oci" and/or "docker": which layout the members belong to


@dataclass(frozen=True)
class ArchivePaths:
    """Name every path of one export operation, derived from the private root and the receipt id alone."""

    exports: str
    directory: str
    output: str
    final: str


def archive_paths(private_root: str, operation_id: str) -> ArchivePaths:
    """Derive the paths of one export operation; the reconciler trusts these and never the recorded plan."""
    exports = os.path.join(private_root, "exports")
    directory = os.path.join(exports, operation_id)
    return ArchivePaths(
        exports=exports,
        directory=directory,
        output=os.path.join(directory, "archive.tar"),
        final=os.path.join(exports, f"{operation_id}.tar"),
    )


def open_private_directory(path: str, *, directory: int | None = None, blocking: bool = True) -> int:
    """Open a directory without following a symlink and require the account as owner with mode 0700."""
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | (0 if blocking else os.O_NONBLOCK)
    descriptor = os.open(path, flags, dir_fd=directory)
    accepted = False
    try:
        observed = os.fstat(descriptor)
        if observed.st_uid != os.getuid() or stat.S_IMODE(observed.st_mode) != 0o700:
            raise ResourceError("The export directory is not private and account-owned.")
        accepted = True
        return descriptor
    finally:
        if not accepted:
            os.close(descriptor)


def stamp(descriptor: int) -> tuple[int, int, int]:
    """Return ``(size, mtime_ns, ctime_ns)`` of an open file."""
    observed = os.fstat(descriptor)
    return observed.st_size, observed.st_mtime_ns, observed.st_ctime_ns


def accept_output(descriptor: int, *, max_bytes: int = ARCHIVE_MAX_BYTES) -> tuple[int, int, int]:
    """V3: require one private regular file of an acceptable size, make it mode 0600 and return its stamp.

    The mode is forced rather than trusted, so the result does not depend on the Docker client's umask.
    """
    observed = os.fstat(descriptor)
    if (
        not stat.S_ISREG(observed.st_mode)
        or observed.st_uid != os.getuid()
        or observed.st_nlink != 1
        or observed.st_size <= 0
    ):
        raise ArchiveRejected("invalid-output", "the archive is not a single regular account-owned file")
    if observed.st_size > max_bytes:
        raise ArchiveRejected("over-cap", "the archive is larger than the per-archive cap")
    os.fchmod(descriptor, 0o600)
    if stat.S_IMODE(os.fstat(descriptor).st_mode) != 0o600:
        raise ArchiveRejected("invalid-output", "the archive mode could not be made private")
    return stamp(descriptor)


def _reject(detail: str) -> ArchiveRejected:
    return ArchiveRejected("verification-failed", detail)


def _pread_exact(descriptor: int, size: int, offset: int) -> bytes:
    """Read exactly ``size`` bytes at ``offset`` or refuse a truncated archive."""
    chunks: list[bytes] = []
    remaining = size
    while remaining:
        block = os.pread(descriptor, remaining, offset)
        if not block:
            raise _reject("the archive ends early")
        chunks.append(block)
        offset += len(block)
        remaining -= len(block)
    return b"".join(chunks)


def _walk(descriptor: int, total_size: int, max_bytes: int) -> _Walk:
    """Walk the tar once: hash every byte, record every member and keep only the fixed small files."""
    walk = _Walk()
    whole = hashlib.sha256()
    offset = 0
    zero_blocks = 0
    while True:
        block = _pread_exact(descriptor, _BLOCK, offset)
        whole.update(block)
        offset += _BLOCK
        if block == _ZERO_BLOCK:
            zero_blocks += 1
            if zero_blocks == 2:
                break
            continue
        if zero_blocks:
            raise _reject("a zero block is followed by data")
        info = tarfile.TarInfo.frombuf(block, "utf-8", "strict")
        name, kind, size = info.name, info.type, info.size
        if len(walk.members) >= MEMBER_COUNT_MAX:
            raise _reject("the archive has too many members")
        if name in walk.members:
            raise _reject("the archive has a duplicate member")
        link_target = None
        content = _DOCKER_CONTENT.fullmatch(name)
        if name in _DIRECTORIES or _DOCKER_ID_DIR.fullmatch(name):
            if kind != tarfile.DIRTYPE or size != 0:
                raise _reject("a directory member has the wrong type or a size")
            walk.grammars.add("oci" if name in _DIRECTORIES else "docker")
        elif _DOCKER_LAYER_LINK.fullmatch(name):
            target = _LAYER_LINK_TARGET.fullmatch(info.linkname)
            if kind != tarfile.SYMTYPE or size != 0 or target is None:
                raise _reject("a legacy layer link has the wrong type or target")
            link_target = target.group(1)
            walk.grammars.add("docker")
        elif (
            name in _FIXED_FILES
            or name == "repositories"
            or _BLOB_NAME.fullmatch(name)
            or content
            or _DOCKER_LEGACY.fullmatch(name)
        ):
            if kind not in {tarfile.REGTYPE, tarfile.AREGTYPE}:
                raise _reject("a file member is not a regular file")
            if size > max_bytes - offset:
                raise _reject("a member is larger than the remaining budget")
            metadata = name in _FIXED_FILES or name == "repositories" or name.endswith(".json") or "/" in name
            if metadata and not _BLOB_NAME.fullmatch(name) and size > METADATA_MEMBER_MAX:
                raise _reject("a metadata member is too large")
            if name != "manifest.json":
                oci = name in _FIXED_FILES or _BLOB_NAME.fullmatch(name)
                walk.grammars.add("oci" if oci else "docker")
        else:
            raise _reject("the archive has an unexpected member")
        body = offset
        digest = hashlib.sha256()
        remaining = size
        retained = name in _FIXED_FILES or (content is not None and name.endswith(".json"))
        keep = bytearray() if retained else None
        position = body
        while remaining:
            chunk = _pread_exact(descriptor, min(_CHUNK, remaining), position)
            whole.update(chunk)
            digest.update(chunk)
            if keep is not None:
                keep.extend(chunk)
            position += len(chunk)
            remaining -= len(chunk)
        padding = (-size) % _BLOCK
        if padding:
            pad = _pread_exact(descriptor, padding, position)
            if pad.strip(b"\0"):
                raise _reject("a member padding is not zero")
            whole.update(pad)
            position += padding
        offset = position
        member_hash = digest.hexdigest()
        matched = _BLOB_NAME.fullmatch(name) or content
        if matched is not None and matched.group(1) != member_hash:
            raise _reject("a blob does not match its content address")
        walk.members[name] = _Member(kind, body, size, member_hash, link_target)
        if keep is not None:
            walk.retained[name] = bytes(keep)
    while offset < total_size:
        tail = _pread_exact(descriptor, min(_CHUNK, total_size - offset), offset)
        if tail.strip(b"\0"):
            raise _reject("the archive has data after its terminator")
        whole.update(tail)
        offset += len(tail)
    if offset != total_size:
        raise _reject("the archive size does not match its tar structure")
    walk.total = offset
    walk.sha256 = whole.hexdigest()
    return walk


def _json(raw: bytes) -> Any:
    try:
        return json.loads(raw, object_pairs_hook=unique_object)
    except (UnicodeDecodeError, ValueError) as error:
        raise _reject("a JSON member is malformed") from error


class _Graph:
    """Resolve the descriptors of one walked archive, reading JSON nodes within a fixed budget."""

    def __init__(self, descriptor: int, walk: _Walk) -> None:
        self._descriptor = descriptor
        self._walk = walk
        self._budget = METADATA_TOTAL_MAX
        self.referenced: set[str] = set()

    @staticmethod
    def entry(value: object, allowed: frozenset[str]) -> tuple[str, str, int, dict[str, Any]]:
        """Validate one descriptor and return ``(media type, hex digest, size, descriptor)``."""
        if not isinstance(value, dict):
            raise _reject("a descriptor is not an object")
        media, digest, size = value.get("mediaType"), value.get("digest"), value.get("size")
        matched = _DIGEST.fullmatch(digest) if isinstance(digest, str) else None
        if not isinstance(media, str) or media not in allowed or matched is None or type(size) is not int or size < 0:
            raise _reject("a descriptor is malformed or has an unsupported media type")
        return media, matched.group(1), size, value

    def present(self, hexdigest: str, size: int, *, required: bool) -> bool:
        """Return whether the blob is in the archive, refusing a size mismatch or a required absence."""
        member = self._walk.members.get(f"blobs/sha256/{hexdigest}")
        if member is None:
            if required:
                raise _reject("a referenced blob is missing")
            return False
        if member.size != size:
            raise _reject("a blob does not match its descriptor size")
        self.referenced.add(hexdigest)
        return True

    def node(self, hexdigest: str, size: int) -> Any:
        """Read one referenced JSON blob by its recorded offset and verify its digest again."""
        member = self._walk.members[f"blobs/sha256/{hexdigest}"]
        if size > METADATA_MEMBER_MAX or size > self._budget:
            raise _reject("the JSON nodes exceed the metadata budget")
        self._budget -= size
        raw = _pread_exact(self._descriptor, size, member.offset)
        if hashlib.sha256(raw).hexdigest() != hexdigest:
            raise _reject("a JSON node changed while it was verified")
        return _json(raw)

    def manifest(self, hexdigest: str, size: int, *, selected: bool) -> tuple[str, list[tuple[str, int]], int]:
        """Validate a manifest's blobs; return ``(config digest, layers, config size)``."""
        node = self.node(hexdigest, size)
        if not isinstance(node, dict):
            raise _reject("a manifest is not an object")
        _, config_digest, config_size, _ = self.entry(node.get("config"), _CONFIG_TYPES)
        layers = node.get("layers")
        if not isinstance(layers, list):
            raise _reject("a manifest has no layer list")
        entries = [self.entry(layer, _LAYER_TYPES) for layer in layers]
        self.present(config_digest, config_size, required=selected)
        for _, layer_digest, layer_size, _ in entries:
            self.present(layer_digest, layer_size, required=selected)
        return config_digest, [(item[1], item[2]) for item in entries], config_size


def _select(
    graph: _Graph,
    root: tuple[str, str, int, dict[str, Any]],
    platform: Platform | None,
) -> tuple[str, int]:
    """Choose the one image manifest of the root descriptor."""
    media, hexdigest, size, _ = root
    if media in _MANIFEST_TYPES:
        return hexdigest, size
    index = graph.node(hexdigest, size)
    children = index.get("manifests") if isinstance(index, dict) else None
    if not isinstance(children, list):
        raise _reject("the root index has no manifest list")
    candidates: list[tuple[str, int]] = []
    for child in children:
        _, child_digest, child_size, raw = graph.entry(child, _MANIFEST_TYPES)
        annotations = raw.get("annotations")
        attestation = isinstance(annotations, dict) and annotations.get(_ATTESTATION_ANNOTATION) == _ATTESTATION
        if attestation:
            if graph.present(child_digest, child_size, required=False):
                graph.manifest(child_digest, child_size, selected=True)
            continue
        declared = raw.get("platform")
        if platform is None:
            candidates.append((child_digest, child_size))
        elif isinstance(declared, dict):
            candidate = normalized_platform(declared.get("os"), declared.get("architecture"), declared.get("variant"))
            if candidate == platform:
                candidates.append((child_digest, child_size))
        if graph.present(child_digest, child_size, required=False):
            graph.manifest(child_digest, child_size, selected=False)
    if len(candidates) != 1:
        raise _reject("the root index does not select exactly one image manifest")
    graph.present(candidates[0][0], candidates[0][1], required=True)
    return candidates[0]


def verify_archive(
    descriptor: int,
    binding: ImageBinding,
    *,
    max_bytes: int = ARCHIVE_MAX_BYTES,
) -> ArchiveFacts:
    """Verify an archive file in one bounded pass and return its facts.

    The descriptor must be a seekable regular file that already passed the caller's ownership and mode checks. The
    archive is never extracted or decompressed: only tar headers, content addresses, descriptor graphs and the
    binding are checked. Every verifier failure is an ``ArchiveRejected("verification-failed")``; an ``OSError`` from
    the descriptor is not converted.
    """
    try:
        return _verify(descriptor, binding, max_bytes)
    except (tarfile.TarError, ValueError, KeyError, TypeError, AttributeError, IndexError, RecursionError) as error:
        raise ArchiveRejected("verification-failed", "the archive could not be parsed") from error


def _verify(descriptor: int, binding: ImageBinding, max_bytes: int) -> ArchiveFacts:
    total = os.fstat(descriptor).st_size
    if total <= 0 or total > max_bytes:
        raise ArchiveRejected("over-cap" if total > max_bytes else "verification-failed", "the archive size is invalid")
    walk = _walk(descriptor, total, max_bytes)
    if "docker" in walk.grammars:
        if "oci" in walk.grammars or "oci-layout" in walk.retained:
            raise _reject("the archive mixes layouts")
        return _verify_docker_archive(walk, binding)
    layout = walk.retained.get("oci-layout")
    if layout is None or _json(layout) != {"imageLayoutVersion": "1.0.0"}:
        raise _reject("the OCI layout header is missing or unsupported")
    index = _json(walk.retained.get("index.json", b""))
    manifests = index.get("manifests") if isinstance(index, dict) else None
    if not isinstance(index, dict) or index.get("schemaVersion") != 2 or not isinstance(manifests, list):
        raise _reject("index.json is malformed")
    if len(manifests) != 1:
        raise _reject("index.json must have exactly one root descriptor")
    graph = _Graph(descriptor, walk)
    root = graph.entry(manifests[0], _MANIFEST_TYPES | _INDEX_TYPES)
    graph.present(root[1], root[2], required=True)
    root_digest = f"sha256:{root[1]}"
    platform = binding.platform
    manifest_hex, manifest_size = _select(graph, root, platform)
    config_hex, layers, config_size = graph.manifest(manifest_hex, manifest_size, selected=True)
    config = graph.node(config_hex, config_size)
    if not isinstance(config, dict):
        raise _reject("the image config is not an object")
    actual = normalized_platform(config.get("os"), config.get("architecture"), config.get("variant"))
    if platform is not None and actual != platform:
        raise _reject("the image config platform differs from the requested platform")
    unreferenced = [
        name
        for name in walk.members
        if (matched := _BLOB_NAME.fullmatch(name)) is not None and matched.group(1) not in graph.referenced
    ]
    if unreferenced:
        raise _reject("the archive has an unreferenced blob")
    docker_manifest = walk.retained.get("manifest.json")
    if root_digest != binding.image_id:
        raise _reject("the root descriptor is not the requested image")
    if docker_manifest is not None:
        entries = _json(docker_manifest)
        if (
            not isinstance(entries, list)
            or len(entries) != 1
            or not isinstance(entries[0], dict)
            or entries[0].get("Config") != f"blobs/sha256/{config_hex}"
            or entries[0].get("Layers") != [f"blobs/sha256/{digest}" for digest, _ in layers]
        ):
            raise _reject("manifest.json does not match the selected image")
    layout_name = "oci+docker-manifest" if docker_manifest is not None else "oci"
    return ArchiveFacts(
        bytes=walk.total,
        sha256=walk.sha256,
        layout=layout_name,
        root_digest=root_digest,
        manifest_digest=f"sha256:{manifest_hex}",
        config_digest=f"sha256:{config_hex}",
        platform=actual,
    )


def _verify_docker_archive(walk: _Walk, binding: ImageBinding) -> ArchiveFacts:
    """Verify Podman's Docker-format archive: one image whose config is the binding's image and whose layers are the
    config's ``diff_ids`` in order, every member named by its content (design 2.7)."""
    entries = _json(walk.retained.get("manifest.json", b""))
    if not isinstance(entries, list) or len(entries) != 1 or not isinstance(entries[0], dict):
        raise _reject("manifest.json must name exactly one image")
    config_name, layers = entries[0].get("Config"), entries[0].get("Layers")
    if not isinstance(config_name, str) or not config_name.endswith(".json") or config_name not in walk.retained:
        raise _reject("manifest.json names no config member")
    config_match = _DOCKER_CONTENT.fullmatch(config_name)
    if config_match is None:
        raise _reject("manifest.json names no config member")
    if f"sha256:{config_match.group(1)}" != binding.image_id:
        raise _reject("the config is not the requested image")
    tars = {name for name in walk.members if name.endswith(".tar") and _DOCKER_CONTENT.fullmatch(name)}
    if (
        not isinstance(layers, list)
        or any(not isinstance(layer, str) for layer in layers)
        or len(set(layers)) != len(layers)
        or set(layers) != tars
    ):
        raise _reject("manifest.json lists other layers than the archive holds")
    if any(member.link_target not in (None, *layers) for member in walk.members.values()):
        raise _reject("a legacy layer link names an unlisted layer")
    config = _json(walk.retained[config_name])
    rootfs = config.get("rootfs") if isinstance(config, dict) else None
    diff_ids = rootfs.get("diff_ids") if isinstance(rootfs, dict) else None
    if diff_ids != [f"sha256:{layer[:-4]}" for layer in layers]:
        raise _reject("the layers are not the config's diff_ids")
    actual = normalized_platform(config.get("os"), config.get("architecture"), config.get("variant"))
    if binding.platform is not None and actual != binding.platform:
        raise _reject("the image config platform differs from the requested platform")
    return ArchiveFacts(
        bytes=walk.total,
        sha256=walk.sha256,
        layout="docker-archive",
        root_digest=binding.image_id,
        manifest_digest=f"sha256:{walk.members['manifest.json'].sha256}",
        config_digest=binding.image_id,
        platform=actual,
    )


def require_free_space(
    path: str,
    needed: int,
    *,
    available_bytes: Callable[[str], int] | None = None,
) -> None:
    """Refuse an export when the private root's filesystem cannot hold ``needed`` bytes (admission check only)."""
    measure = available_bytes if available_bytes is not None else _available_bytes
    try:
        available = measure(path)
    except OSError as error:
        raise ResourceError("Export storage could not be measured.", reason="export-storage") from error
    if available < needed:
        raise ResourceError("Not enough free space to export an archive.", reason="export-storage")


def _available_bytes(path: str) -> int:
    observed = os.statvfs(path)
    return observed.f_bavail * observed.f_frsize


def identity(observed: os.stat_result) -> tuple[int, int]:
    """Return the ``(device, inode)`` identity of a stat result."""
    return observed.st_dev, observed.st_ino


def lstat_at(name: str, directory: int) -> os.stat_result | None:
    """Return ``lstat`` of ``name`` relative to a directory descriptor, or ``None`` when it is gone."""
    try:
        return os.stat(name, dir_fd=directory, follow_symlinks=False)
    except FileNotFoundError:
        return None


def rollback_final(
    exports: int,
    final_name: str,
    verified: int | None,
    *,
    created: bool,
    expected: tuple[int, int] | None = None,
) -> OSError | None:
    """Unlink ``final`` only when this invocation created it and it is still the verified inode.

    An already-gone ``final`` is success. A pre-existing, substituted or symlinked ``final`` is left in place and
    reported. The function never raises: it returns the first ``OSError`` or ``None``.
    """
    try:
        observed = lstat_at(final_name, exports)
        if observed is None:
            return None
        wanted = expected if expected is not None else (identity(os.fstat(verified)) if verified is not None else None)
        if not created or wanted is None or identity(observed) != wanted or not stat.S_ISREG(observed.st_mode):
            return OSError("The published archive was left in place: it is not the verified inode.")
        os.unlink(final_name, dir_fd=exports)
    except OSError as error:
        return error
    return None


def lstat_or_raise(name: str, directory: int) -> os.stat_result:
    """Return ``lstat`` relative to a directory descriptor."""
    return os.stat(name, dir_fd=directory, follow_symlinks=False)


def _empty_directory(directory: int) -> None:
    for entry in os.listdir(directory):
        os.unlink(entry, dir_fd=directory)


def _remove_directory(exports: int, name: str, held: int) -> None:
    """Empty and remove one operation directory under the identity rules of the design."""
    _empty_directory(held)
    observed = lstat_at(name, exports)
    if observed is None:
        return
    if identity(observed) == identity(os.fstat(held)):
        os.rmdir(name, dir_fd=exports)
        return
    if stat.S_ISDIR(observed.st_mode) and observed.st_uid == os.getuid():
        replaced = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=exports)
        try:
            _empty_directory(replaced)
            if identity(os.fstat(replaced)) == identity(lstat_or_raise(name, exports)):
                os.rmdir(name, dir_fd=exports)
        finally:
            os.close(replaced)
        return
    raise OSError(f"The staging directory name {name} no longer names the held directory.")


def dispose_staging(
    exports: int,
    name: str,
    held: int | None,
    *,
    remove_tree: Callable[[int, str, int], None] | None = None,
) -> OSError | None:
    """Remove one operation's staging directory; return the first ``OSError`` instead of raising.

    A missing directory is success. ``remove_tree`` is the injected seam of the tests.
    """
    work = remove_tree if remove_tree is not None else _remove_directory
    try:
        if held is None:
            if lstat_at(name, exports) is None:
                return None
            reopened = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=exports)
            try:
                work(exports, name, reopened)
            finally:
                os.close(reopened)
        else:
            work(exports, name, held)
    except FileNotFoundError:
        return None
    except OSError as error:
        return error
    return None


def classify_interrupted(exports: int, op_name: str, final_name: str) -> tuple[str, tuple[int, int] | None]:
    """Classify an interrupted export from the disk alone, never opening ``final``.

    Returns ``(class, final identity or None)`` following the state machine's rule: ``final`` present at all means
    complete-but-unpublished, otherwise any staging entry means partial, otherwise absent.
    """
    final = lstat_at(final_name, exports)
    if final is not None:
        return "recovered-complete-unpublished", identity(final)
    staging = lstat_at(op_name, exports)
    if staging is not None and stat.S_ISDIR(staging.st_mode):
        try:
            held = os.open(op_name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=exports)
        except OSError:
            return "recovered-partial", None
        try:
            if os.listdir(held):
                return "recovered-partial", None
        finally:
            os.close(held)
    return "recovered-absent", None


def remove_final_by_identity(exports: int, final_name: str, recorded: tuple[int, int] | None) -> OSError | None:
    """Unlink any non-directory entry at ``final`` that still has the recorded identity, never opening it."""
    try:
        observed = lstat_at(final_name, exports)
        if observed is None:
            return None
        if recorded is None or identity(observed) != recorded or stat.S_ISDIR(observed.st_mode):
            return OSError("The entry at the published name is not the one recorded and was left in place.")
        os.unlink(final_name, dir_fd=exports)
    except OSError as error:
        return error
    return None


_RECORD_STATES = frozenset({"published", "failed"})
_CLEANUP = frozenset({"complete", "incomplete", "deferred"})
_FAILURE_CLASSES = frozenset(
    {
        "not-started",
        "child-failed",
        "absent",
        "unexpected-entry",
        "invalid-output",
        "over-cap",
        "verification-failed",
        "metadata-missing",
        "publish-collision",
        "publish-substituted",
        "deadline",
        "io-error",
        "cleanup-incomplete",
        "recovered-absent",
        "recovered-partial",
        "recovered-complete-unpublished",
        "archive-record-invalid",
    }
)
_PUBLISHED_KEYS = frozenset(
    {
        "schema",
        "state",
        "cleanup",
        "path",
        "bytes",
        "sha256",
        "layout",
        "root_digest",
        "manifest_digest",
        "config_digest",
        "platform",
    }
)
_PLATFORM_KEYS = frozenset({"os", "architecture", "variant"})


def _invalid(detail: str) -> ResourceError:
    return ResourceError(f"The recorded archive facts are invalid: {detail}.", reason="archive-record-invalid")


def validate_archive_record(record: object, *, final_path: str, image_id: str | None = None) -> None:
    """Validate one ``plan["archive"]`` record before it is written or quoted.

    Raises ``ResourceError`` with reason ``archive-record-invalid``.
    """
    if not isinstance(record, dict):
        raise _invalid("it is not an object")
    state, cleanup = record.get("state"), record.get("cleanup")
    if (
        type(record.get("schema")) is not int
        or record["schema"] != 1
        or not isinstance(state, str)
        or state not in _RECORD_STATES
        or not isinstance(cleanup, str)
        or cleanup not in _CLEANUP
    ):
        raise _invalid("schema, state or cleanup")
    if state == "published":
        if cleanup != "complete":
            raise _invalid("a published record has complete cleanup")
        if set(record) != _PUBLISHED_KEYS:
            raise _invalid("the published keys differ")
        if record["path"] != final_path:
            raise _invalid("the path is not the derived published path")
        if (
            type(record["bytes"]) is not int
            or record["bytes"] <= 0
            or not isinstance(record["sha256"], str)
            or re.fullmatch(r"[0-9a-f]{64}", record["sha256"]) is None
            or not isinstance(record["layout"], str)
            or record["layout"] not in {"oci", "oci+docker-manifest", "docker-archive"}
        ):
            raise _invalid("bytes, sha256 or layout")
        for key in ("root_digest", "manifest_digest", "config_digest"):
            value = record[key]
            if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
                raise _invalid(key)
        platform = record["platform"]
        if (
            not isinstance(platform, dict)
            or not {"os", "architecture"} <= set(platform) <= _PLATFORM_KEYS
            or any(not isinstance(value, str) or not value for value in platform.values())
        ):
            raise _invalid("platform")
        if image_id is not None and record["root_digest"] != image_id:
            raise _invalid("the root digest is not the exported image")
        return
    allowed = {"schema", "state", "cleanup", "failure", "final_identity"}
    failure = record.get("failure")
    if not set(record) <= allowed or not isinstance(failure, str) or failure not in _FAILURE_CLASSES:
        raise _invalid("the failure record keys or class")
    recorded = record.get("final_identity")
    if recorded is not None and (
        not isinstance(recorded, dict)
        or set(recorded) != {"dev", "ino"}
        or type(recorded["dev"]) is not int
        or type(recorded["ino"]) is not int
    ):
        raise _invalid("final_identity")
