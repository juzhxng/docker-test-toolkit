from __future__ import annotations

import json
import os
import stat
import tarfile
from collections.abc import Callable
from pathlib import Path

import pytest

from docker_test_toolkit.application import image_archive as archive
from docker_test_toolkit.application.image_archive import (
    ImageBinding,
    Platform,
    verify_archive,
)
from docker_test_toolkit.domain.errors import ArchiveRejected, ResourceError
from tests.archive_fixtures import Layout, digest, json_bytes, write_archive

PLATFORM = Platform("linux", "arm64")


def _verify(path: Path, binding: ImageBinding) -> archive.ArchiveFacts:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        return verify_archive(descriptor, binding)
    finally:
        os.close(descriptor)


def _image_binding(layout: Layout) -> ImageBinding:
    return ImageBinding(layout.root_digest, PLATFORM)


def test_valid_image_archive_yields_its_facts(tmp_path: Path) -> None:
    layout = Layout()
    facts = _verify(write_archive(tmp_path / "a.tar", layout.members()), _image_binding(layout))

    assert facts.layout == "oci+docker-manifest"
    assert facts.root_digest == layout.root_digest == facts.manifest_digest
    assert facts.config_digest == f"sha256:{digest(layout.config)}"
    assert facts.platform == PLATFORM
    assert facts.sha256 == digest((tmp_path / "a.tar").read_bytes())
    assert facts.bytes == (tmp_path / "a.tar").stat().st_size


def test_valid_image_archive_without_a_docker_manifest(tmp_path: Path) -> None:
    layout = Layout(docker_manifest=False)

    facts = _verify(write_archive(tmp_path / "c.tar", layout.members()), _image_binding(layout))

    assert facts.layout == "oci"


@pytest.mark.parametrize("index_root,attestation", [(True, False), (True, True)])
def test_root_index_selects_the_image_manifest_and_excludes_attestations(
    tmp_path: Path, index_root: bool, attestation: bool
) -> None:
    layout = Layout(docker_manifest=False, index_root=index_root, attestation=attestation)
    facts = _verify(write_archive(tmp_path / "i.tar", layout.members()), _image_binding(layout))

    assert facts.root_digest == layout.root_digest
    assert facts.manifest_digest == f"sha256:{digest(layout.manifest)}"


def test_substituted_image_is_rejected_even_when_the_archive_is_self_consistent(tmp_path: Path) -> None:
    layout = Layout()
    other = ImageBinding("sha256:" + "f" * 64, PLATFORM)

    with pytest.raises(ArchiveRejected) as raised:
        _verify(write_archive(tmp_path / "a.tar", layout.members()), other)

    assert raised.value.failure_class == "verification-failed"


def test_wrong_platform_is_rejected(tmp_path: Path) -> None:
    layout = Layout()

    with pytest.raises(ArchiveRejected):
        _verify(
            write_archive(tmp_path / "a.tar", layout.members()),
            ImageBinding(layout.root_digest, Platform("linux", "amd64")),
        )


def test_manifest_json_must_match_the_selected_image(tmp_path: Path) -> None:
    layout = Layout()
    layout.docker_manifest = json_bytes([{"Config": "blobs/sha256/" + "0" * 64, "RepoTags": None, "Layers": []}])

    with pytest.raises(ArchiveRejected):
        _verify(write_archive(tmp_path / "a.tar", layout.members()), _image_binding(layout))


def _mutations() -> list[
    tuple[str, Callable[[Layout, list[tuple[str, bytes, bytes]]], list[tuple[str, bytes, bytes]]]]
]:
    def add_unreferenced(_: Layout, rows: list[tuple[str, bytes, bytes]]) -> list[tuple[str, bytes, bytes]]:
        extra = b"unreferenced"
        return [*rows, (f"blobs/sha256/{digest(extra)}", extra, tarfile.REGTYPE)]

    def duplicate(_: Layout, rows: list[tuple[str, bytes, bytes]]) -> list[tuple[str, bytes, bytes]]:
        return [*rows, rows[-1]]

    def corrupt_layer(layout: Layout, rows: list[tuple[str, bytes, bytes]]) -> list[tuple[str, bytes, bytes]]:
        name = f"blobs/sha256/{digest(layout.layer)}"
        return [(n, b"x" * len(b), k) if n == name else (n, b, k) for n, b, k in rows]

    def index_as_directory(_: Layout, rows: list[tuple[str, bytes, bytes]]) -> list[tuple[str, bytes, bytes]]:
        return [(n, b"", tarfile.DIRTYPE) if n == "index.json" else (n, b, k) for n, b, k in rows]

    def blobs_as_file(_: Layout, rows: list[tuple[str, bytes, bytes]]) -> list[tuple[str, bytes, bytes]]:
        return [(n, b"not-a-directory", tarfile.REGTYPE) if n == "blobs" else (n, b, k) for n, b, k in rows]

    def unexpected_name(_: Layout, rows: list[tuple[str, bytes, bytes]]) -> list[tuple[str, bytes, bytes]]:
        return [*rows, ("evil/../x", b"x", tarfile.REGTYPE)]

    def symlink(_: Layout, rows: list[tuple[str, bytes, bytes]]) -> list[tuple[str, bytes, bytes]]:
        return [*rows, ("blobs/sha256/" + "b" * 64, b"", tarfile.SYMTYPE)]

    def two_roots(layout: Layout, rows: list[tuple[str, bytes, bytes]]) -> list[tuple[str, bytes, bytes]]:
        doubled = json_bytes({"schemaVersion": 2, "manifests": json.loads(layout.index)["manifests"] * 2})
        return [(n, doubled, k) if n == "index.json" else (n, b, k) for n, b, k in rows]

    def bad_layout_header(_: Layout, rows: list[tuple[str, bytes, bytes]]) -> list[tuple[str, bytes, bytes]]:
        return [(n, b'{"imageLayoutVersion":"9"}', k) if n == "oci-layout" else (n, b, k) for n, b, k in rows]

    return [
        ("unreferenced-blob", add_unreferenced),
        ("duplicate-member", duplicate),
        ("blob-content-mismatch", corrupt_layer),
        ("index-json-as-directory", index_as_directory),
        ("blobs-as-regular-file", blobs_as_file),
        ("unexpected-name", unexpected_name),
        ("symlink-member", symlink),
        ("two-root-descriptors", two_roots),
        ("bad-layout-header", bad_layout_header),
    ]


@pytest.mark.parametrize(("label", "mutate"), _mutations(), ids=[label for label, _ in _mutations()])
def test_structural_violations_are_rejected_with_the_verification_class(
    tmp_path: Path, label: str, mutate: Callable[..., list[tuple[str, bytes, bytes]]]
) -> None:
    layout = Layout()
    rows = mutate(layout, layout.members())

    with pytest.raises(ArchiveRejected) as raised:
        _verify(write_archive(tmp_path / f"{label}.tar", rows), _image_binding(layout))

    assert raised.value.failure_class == "verification-failed"


def test_truncated_and_trailing_garbage_archives_are_rejected(tmp_path: Path) -> None:
    layout = Layout()
    good = write_archive(tmp_path / "good.tar", layout.members())
    data = good.read_bytes()
    cases = {
        "cut-in-a-body": data[:1800],
        "no-terminator": write_archive(tmp_path / "nt.tar", layout.members(), terminate=False).read_bytes(),
        "garbage-after-terminator": data + b"garbage",
    }
    for label, content in cases.items():
        path = tmp_path / f"{label}.tar"
        path.write_bytes(content)
        with pytest.raises(ArchiveRejected, match="verification-failed"):
            _verify(path, _image_binding(layout))


def test_bad_header_checksum_and_an_empty_file_are_rejected(tmp_path: Path) -> None:
    layout = Layout()
    data = bytearray(write_archive(tmp_path / "good.tar", layout.members()).read_bytes())
    data[148:156] = b"0000000\0"
    broken = tmp_path / "bad.tar"
    broken.write_bytes(bytes(data))
    empty = tmp_path / "empty.tar"
    empty.write_bytes(b"")

    for path in (broken, empty):
        with pytest.raises(ArchiveRejected):
            _verify(path, _image_binding(layout))


@pytest.mark.parametrize("kind", [b"x", b"L", b"K", b"g", b"1"])
def test_extension_and_link_headers_are_rejected_at_the_header_without_reading_the_payload(
    tmp_path: Path, kind: bytes
) -> None:
    info = tarfile.TarInfo("././@PaxHeader")
    info.type = kind
    info.size = 64 * 1024 * 1024
    path = tmp_path / "ext.tar"
    path.write_bytes(info.tobuf(tarfile.USTAR_FORMAT, "utf-8", "strict") + b"\0" * 1024)
    layout = Layout()

    with pytest.raises(ArchiveRejected, match="verification-failed"):
        _verify(path, _image_binding(layout))


def test_frombuf_strips_the_trailing_slash_of_directory_names() -> None:
    info = tarfile.TarInfo("blobs/")
    info.type = tarfile.DIRTYPE
    block = info.tobuf(tarfile.USTAR_FORMAT, "utf-8", "strict")

    assert tarfile.TarInfo.frombuf(block, "utf-8", "strict").name == "blobs"


def test_an_oserror_from_the_descriptor_is_not_converted() -> None:
    layout = Layout()

    with pytest.raises(OSError) as raised:
        verify_archive(-1, _image_binding(layout))

    assert not isinstance(raised.value, ArchiveRejected)


def test_oversized_archive_is_rejected_as_over_cap(tmp_path: Path) -> None:
    layout = Layout()
    path = write_archive(tmp_path / "a.tar", layout.members())
    descriptor = os.open(path, os.O_RDONLY)
    try:
        with pytest.raises(ArchiveRejected) as raised:
            verify_archive(descriptor, _image_binding(layout), max_bytes=1024)
    finally:
        os.close(descriptor)

    assert raised.value.failure_class == "over-cap"


def test_free_space_admission_uses_the_injected_measure() -> None:
    archive.require_free_space("/", 100, available_bytes=lambda _: 100)
    with pytest.raises(ResourceError) as raised:
        archive.require_free_space("/", 100, available_bytes=lambda _: 99)

    assert raised.value.reason == "export-storage"

    def failing(_: str) -> int:
        raise OSError("statvfs")

    with pytest.raises(ResourceError):
        archive.require_free_space("/", 1, available_bytes=failing)


def _exports(tmp_path: Path) -> tuple[Path, int]:
    exports = tmp_path / "exports"
    exports.mkdir(mode=0o700)
    return exports, os.open(exports, os.O_RDONLY | os.O_DIRECTORY)


def test_rollback_final_removes_only_what_this_invocation_created(tmp_path: Path) -> None:
    exports, e_fd = _exports(tmp_path)
    try:
        (exports / "op.tar").write_bytes(b"verified")
        verified = os.open(exports / "op.tar", os.O_RDONLY)
        assert archive.rollback_final(e_fd, "op.tar", verified, created=False) is not None
        assert (exports / "op.tar").exists()
        assert archive.rollback_final(e_fd, "op.tar", verified, created=True) is None
        assert not (exports / "op.tar").exists()
        assert archive.rollback_final(e_fd, "op.tar", verified, created=True) is None  # already gone is success
        (exports / "op.tar").write_bytes(b"substituted")
        assert archive.rollback_final(e_fd, "op.tar", verified, created=True) is not None
        assert (exports / "op.tar").read_bytes() == b"substituted"
        os.close(verified)
    finally:
        os.close(e_fd)


def test_dispose_staging_removes_files_tolerates_absence_and_reports_failure(tmp_path: Path) -> None:
    exports, e_fd = _exports(tmp_path)
    try:
        (exports / "op").mkdir(mode=0o700)
        (exports / "op" / "archive.tar").write_bytes(b"x")
        held = os.open(exports / "op", os.O_RDONLY | os.O_DIRECTORY)
        assert archive.dispose_staging(e_fd, "op", held) is None
        assert not (exports / "op").exists()
        assert archive.dispose_staging(e_fd, "op", held) is None  # already gone
        os.close(held)
        assert archive.dispose_staging(e_fd, "never", None) is None

        (exports / "op2").mkdir(mode=0o700)
        held2 = os.open(exports / "op2", os.O_RDONLY | os.O_DIRECTORY)

        def failing(_e: int, _n: str, _h: int) -> None:
            raise PermissionError("injected")

        failure = archive.dispose_staging(e_fd, "op2", held2, remove_tree=failing)
        os.close(held2)
        assert isinstance(failure, OSError)
        assert (exports / "op2").exists()
    finally:
        os.close(e_fd)


def test_dispose_staging_removes_a_directory_recreated_under_the_same_name(tmp_path: Path) -> None:
    exports, e_fd = _exports(tmp_path)
    try:
        (exports / "op").mkdir(mode=0o700)
        held = os.open(exports / "op", os.O_RDONLY | os.O_DIRECTORY)
        (exports / "op").rmdir()
        (exports / "op").mkdir(mode=0o755)
        (exports / "op" / "late.tar").write_bytes(b"x")

        assert archive.dispose_staging(e_fd, "op", held) is None
        assert not (exports / "op").exists()
        os.close(held)
    finally:
        os.close(e_fd)


def test_classification_follows_final_then_staging_and_never_opens_final(tmp_path: Path) -> None:
    exports, e_fd = _exports(tmp_path)
    try:
        assert archive.classify_interrupted(e_fd, "op", "op.tar") == ("recovered-absent", None)
        (exports / "op").mkdir()
        assert archive.classify_interrupted(e_fd, "op", "op.tar") == ("recovered-absent", None)
        (exports / "op" / ".tmp-archive.tar123").write_bytes(b"x")
        assert archive.classify_interrupted(e_fd, "op", "op.tar") == ("recovered-partial", None)
        os.mkfifo(exports / "op.tar")  # a FIFO would block an open() forever
        os.link(exports / "op.tar", exports / "keep")  # keeps the inode alive: Linux reuses a freed inode number
        kind, recorded = archive.classify_interrupted(e_fd, "op", "op.tar")
        assert kind == "recovered-complete-unpublished"
        assert recorded is not None
        assert archive.remove_final_by_identity(e_fd, "op.tar", recorded) is None
        assert not (exports / "op.tar").exists()
        assert archive.remove_final_by_identity(e_fd, "op.tar", recorded) is None  # already gone
        (exports / "op.tar").write_bytes(b"swapped")
        assert archive.remove_final_by_identity(e_fd, "op.tar", recorded) is not None
        assert stat.S_ISREG((exports / "op.tar").lstat().st_mode)
    finally:
        os.close(e_fd)


def _published(**changes: object) -> dict[str, object]:
    record: dict[str, object] = {
        "schema": 1,
        "state": "published",
        "cleanup": "complete",
        "path": "/root/exports/op.tar",
        "bytes": 10,
        "sha256": "a" * 64,
        "layout": "oci",
        "root_digest": "sha256:" + "b" * 64,
        "manifest_digest": "sha256:" + "b" * 64,
        "config_digest": "sha256:" + "c" * 64,
        "platform": {"os": "linux", "architecture": "arm64"},
    }
    record.update(changes)
    return record


def test_archive_record_validation_is_closed() -> None:
    archive.validate_archive_record(_published(), final_path="/root/exports/op.tar", image_id="sha256:" + "b" * 64)
    archive.validate_archive_record(
        {"schema": 1, "state": "failed", "cleanup": "deferred", "failure": "recovered-partial", "final_identity": None},
        final_path="/x",
    )
    bad = [
        _published(path="/elsewhere"),
        _published(bytes=0),
        _published(sha256="short"),
        _published(extra=1),
        _published(metadata_sha256="d" * 64),
        _published(layout="docker-classic"),
        _published(root_digest="sha256:" + "0" * 64),
        _published(cleanup="done"),
        {"schema": 1, "state": "failed", "cleanup": "complete", "failure": "invented"},
        {"schema": 1, "state": "unresolved", "cleanup": "complete", "failure": "io-error"},
        "not-an-object",
        _published(platform={"os": None, "architecture": []}),
        _published(platform={"os": "", "architecture": "arm64"}),
        _published(schema=True),
        _published(layout=[]),
        _published(layout={}),
        _published(cleanup="deferred"),
        _published(cleanup="incomplete"),
        {"schema": 1, "state": ["failed"], "cleanup": "complete", "failure": "io-error"},
        {"schema": 1, "state": "failed", "cleanup": {"x": 1}, "failure": "io-error"},
        {"schema": 1, "state": "failed", "cleanup": "complete", "failure": ["io-error"]},
        {"schema": 1, "state": "unresolved", "cleanup": "incomplete", "failure": "worker-ambiguous"},
    ]
    for record in bad:
        with pytest.raises(ResourceError) as raised:
            archive.validate_archive_record(record, final_path="/root/exports/op.tar", image_id="sha256:" + "b" * 64)
        assert raised.value.reason == "archive-record-invalid"


def test_a_docker_archive_yields_its_facts(tmp_path: Path) -> None:
    from tests.archive_fixtures import DockerArchive

    layout = DockerArchive()
    path = write_archive(tmp_path / "podman.tar", layout.members())

    facts = _verify(path, ImageBinding(layout.image_id, PLATFORM))

    assert facts.layout == "docker-archive"
    assert facts.root_digest == facts.config_digest == layout.image_id
    assert facts.manifest_digest == f"sha256:{digest(layout.manifest)}"
    assert facts.sha256 == digest(path.read_bytes()) and facts.bytes == path.stat().st_size
    assert facts.platform == PLATFORM


def _docker_archive_violation(case: str):
    from tests.archive_fixtures import DockerArchive

    layout = DockerArchive()
    image_id = layout.image_id
    rows = layout.members()
    if case == "changed-layer":  # the bytes no longer hash to the layer's name
        rows = [(n, b + b"x" if n == layout.listed[1] else b, k) for n, b, k in rows]
    elif case == "other-image":
        image_id = "sha256:" + "f" * 64
    elif case == "extra-member":
        rows.append(("extra.txt", b"x", tarfile.REGTYPE))
    elif case == "link-to-unlisted-layer":
        stray = b"stray-layer"
        rows.append((f"{digest(stray)}.tar", stray, tarfile.REGTYPE))
        rows.append((f"{'9' * 64}/layer.tar", f"../{digest(stray)}.tar".encode(), tarfile.SYMTYPE))
    elif case == "link-outside":
        rows = [(n, b"/etc/passwd" if k == tarfile.SYMTYPE else b, k) for n, b, k in rows]
    elif case == "mixed-layout":
        rows.append(("oci-layout", json_bytes({"imageLayoutVersion": "1.0.0"}), tarfile.REGTYPE))
    elif case == "layer-twice":
        layout.listed = [layout.listed[0], layout.listed[0]]
        rows = layout.members()
    elif case == "unlisted-layer":
        layout.listed = layout.listed[:1]
        rows = layout.members()
    elif case == "reversed-diff-ids":
        config = json.loads(layout.config)
        config["rootfs"]["diff_ids"].reverse()
        layout.config = json_bytes(config)
        image_id, rows = layout.image_id, layout.members()
    elif case == "other-platform":
        config = json.loads(layout.config)
        config["architecture"] = "amd64"
        layout.config = json_bytes(config)
        image_id, rows = layout.image_id, layout.members()
    return image_id, rows


@pytest.mark.parametrize(
    "case",
    [
        "changed-layer",
        "other-image",
        "extra-member",
        "link-to-unlisted-layer",
        "link-outside",
        "mixed-layout",
        "layer-twice",
        "unlisted-layer",
        "reversed-diff-ids",
        "other-platform",
    ],
)
def test_docker_archive_violations_are_rejected(tmp_path: Path, case: str) -> None:
    image_id, rows = _docker_archive_violation(case)
    path = write_archive(tmp_path / "podman.tar", rows)

    with pytest.raises(ArchiveRejected) as rejected:
        _verify(path, ImageBinding(image_id, PLATFORM))

    assert rejected.value.failure_class == "verification-failed"


def test_an_oci_archive_with_a_docker_archive_member_is_rejected(tmp_path: Path) -> None:
    layout = Layout()
    rows = [*layout.members(), ("repositories", b"{}", tarfile.REGTYPE)]  # once allowed only in the docker grammar

    with pytest.raises(ArchiveRejected):
        _verify(write_archive(tmp_path / "mixed.tar", rows), _image_binding(layout))
