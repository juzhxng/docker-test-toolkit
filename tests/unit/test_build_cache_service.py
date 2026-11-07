"""Build-cache store primitives on a real private filesystem tree."""

from __future__ import annotations

import hashlib
import json
import os
import stat
import subprocess
import sys
import uuid
from contextlib import contextmanager
from pathlib import Path

import pytest

from docker_test_toolkit.application import build_cache as store
from docker_test_toolkit.application import lifecycle
from docker_test_toolkit.domain import build_cache
from docker_test_toolkit.domain.errors import IncompleteCleanup, InvalidInput, ResourceBusy, ResourceError
from docker_test_toolkit.factories.testing import bind_test_observation_reads, install_failpoint
from tests.archive_harness import observed_locks
from tests.unit.test_docker_receipt_service import BUILT_IMAGE_ID, FakeDockerPort, _entered_run, write_cache_layout
from tests.unit.test_receipt_platform_facts import _info

CACHE_CONFIG = "application/vnd.buildkit.cacheconfig.v0"
LAYER = "application/vnd.oci.image.layer.v1.tar+gzip"
KEY = "a" * 64
PLATFORM = {"os": "linux", "architecture": "arm64"}


def _blob(layout: Path, data: bytes) -> dict:
    digest = hashlib.sha256(data).hexdigest()
    path = layout / "blobs" / "sha256" / digest
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.write_bytes(data)
    return {"digest": f"sha256:{digest}", "size": len(data)}


def _write_layout(
    path: Path,
    layers: dict[str, bytes],
    *,
    form: str = "manifest",
    annotation: tuple[str, str] = build_cache.CACHE_INDEX_ANNOTATION,
    index_padding: int = 0,
) -> list[str]:
    """Write a structurally valid cache export in ``form`` and return its layer digests."""
    path.mkdir(parents=True, mode=0o700)
    (path / "oci-layout").write_text('{"imageLayoutVersion":"1.0.0"}')
    config = {"mediaType": CACHE_CONFIG, **_blob(path, b'{"layers":[]}')}
    descriptors = [{"mediaType": LAYER, **_blob(path, data)} for data in layers.values()]
    if form == "manifest":
        manifest = {"schemaVersion": 2, "config": config, "layers": descriptors}
        manifest_type = "application/vnd.oci.image.manifest.v1+json"
    else:
        manifest = {"schemaVersion": 2, "manifests": [config, *descriptors]}
        manifest_type = "application/vnd.oci.image.index.v1+json"
    manifest_descriptor = {"mediaType": manifest_type, **_blob(path, json.dumps(manifest).encode())}
    index = {
        "schemaVersion": 2,
        "manifests": [{**manifest_descriptor, "annotations": dict([annotation])}],
        "padding": " " * index_padding,
    }
    (path / "index.json").write_text(json.dumps(index))
    return [item["digest"] for item in descriptors]


def _generation_dir(platform_dir: Path, key: str, wall_ns: int, run: str | None = None) -> Path:
    name = build_cache.generation_name(wall_ns, run or str(uuid.uuid4()), str(uuid.uuid4()))
    return platform_dir / "generations" / key / name


@pytest.mark.parametrize("form", ["manifest", "index"])
def test_valid_generation_forms(tmp_path: Path, form: str) -> None:
    digests = _write_layout(tmp_path / "g", {"a": b"layer-a", "b": b"layer-b"}, form=form)

    assert store.validate_generation(tmp_path / "g") == tuple(digests)


def _break(case: str, layout: Path) -> None:
    blobs = layout / "blobs" / "sha256"
    index = json.loads((layout / "index.json").read_text())
    manifest_digest = index["manifests"][0]["digest"].split(":")[1]
    layer = next(path for path in sorted(blobs.iterdir()) if path.name != manifest_digest)
    if case == "symlinked-blob":
        target = layout.parent / "elsewhere"
        target.write_bytes(layer.read_bytes())
        layer.unlink()
        layer.symlink_to(target)
    elif case == "unknown-file":
        (layout / "notes.txt").write_text("x")
    elif case == "unknown-directory":
        (layout / "ingest").mkdir()
    elif case == "wrong-size":
        layer.chmod(0o600)
        layer.write_bytes(layer.read_bytes() + b"x")
    elif case == "missing-blob":
        layer.unlink()
    elif case == "two-latest":
        index["manifests"].append(index["manifests"][0])
        (layout / "index.json").write_text(json.dumps(index))
    elif case == "duplicate-key":
        text = (layout / "index.json").read_text()
        (layout / "index.json").write_text(text[:-1] + ', "schemaVersion": 2}')
    elif case == "metadata-over-bound":
        (layout / "index.json").write_text(" " * (build_cache.CACHE_METADATA_MAX + 1))
    elif case == "too-many-blobs":
        for number in range(build_cache.CACHE_BLOB_COUNT_MAX):
            (blobs / hashlib.sha256(str(number).encode()).hexdigest()).write_bytes(b"")
    elif case == "not-json":
        (layout / "index.json").write_bytes(b"\xff\xfe")
    elif case == "foreign-hex-name":
        (blobs / ("A" * 64)).write_bytes(b"")


@pytest.mark.parametrize(
    "case",
    [
        "symlinked-blob", "unknown-file", "unknown-directory", "wrong-size", "missing-blob", "two-latest",
        "duplicate-key", "metadata-over-bound", "too-many-blobs", "not-json", "foreign-hex-name",
    ],
)  # fmt: skip
def test_structural_refusals(tmp_path: Path, case: str) -> None:
    layout = tmp_path / "g"
    _write_layout(layout, {"a": b"layer-a"})
    _break(case, layout)

    with pytest.raises(ValueError, match="Cache generation is structurally invalid"):
        store.validate_generation(layout)


def test_staging_may_hold_an_empty_ingest_directory(tmp_path: Path) -> None:
    _write_layout(tmp_path / "g", {"a": b"layer-a"})
    (tmp_path / "g" / "ingest").mkdir()

    assert store.validate_generation(tmp_path / "g", staging=True)
    (tmp_path / "g" / "ingest" / "partial").write_bytes(b"x")
    with pytest.raises(ValueError, match="structurally invalid"):
        store.validate_generation(tmp_path / "g", staging=True)


@pytest.mark.parametrize("lock", ["index.json.lock", "cache.lock"])
def test_buildkit_lock_files_are_part_of_the_layout(tmp_path: Path, lock: str) -> None:
    _write_layout(tmp_path / "g", {"a": b"layer-a"})
    (tmp_path / "g" / lock).touch()

    assert store.validate_generation(tmp_path / "g")


def test_metadata_bound_replaces_the_index_cap(tmp_path: Path) -> None:
    _write_layout(tmp_path / "g", {"a": b"layer-a"}, index_padding=2 * 1024**2)

    assert (tmp_path / "g" / "index.json").stat().st_size > 1024**2
    assert store.validate_generation(tmp_path / "g")


def test_more_than_28_blobs_is_valid(tmp_path: Path) -> None:
    layers = {str(number): f"layer-{number}".encode() for number in range(40)}

    _write_layout(tmp_path / "g", layers)

    assert len(store.validate_generation(tmp_path / "g")) == 40


def test_index_annotation_must_equal_the_probe_pair(tmp_path: Path) -> None:
    _write_layout(tmp_path / "g", {"a": b"layer-a"}, annotation=("org.opencontainers.image.ref.name", "other"))

    with pytest.raises(ValueError, match="structurally invalid"):
        store.validate_generation(tmp_path / "g")


def test_anchor_lives_beside_the_data_subtree(tmp_path: Path) -> None:
    registry = tmp_path / "account" / "registry-2"
    project_cache = store.project_cache(registry, KEY)
    platform = store.platform_name(PLATFORM)

    with store.platform_anchor(project_cache, platform, shared=True):
        pass

    assert store.cache_root(registry) == tmp_path / "account" / "registry-2-build-cache"
    assert project_cache == store.cache_root(registry) / KEY
    assert platform == "linux-arm64"
    anchor = project_cache / "linux-arm64.lock"
    assert stat.S_ISREG(anchor.lstat().st_mode) and stat.S_IMODE(anchor.lstat().st_mode) == 0o600
    assert not (project_cache / "linux-arm64").exists()


def test_unsafe_directories_are_refused_never_repaired(tmp_path: Path) -> None:
    open_directory = tmp_path / "open"
    open_directory.mkdir(mode=0o700)
    open_directory.chmod(0o755)
    link = tmp_path / "link"
    link.symlink_to(open_directory)
    store.require_safe_directory(tmp_path / "fresh" / "nested")

    for unsafe in (open_directory, link, tmp_path / "with,comma", tmp_path / 'with"quote'):
        with pytest.raises(ValueError):
            store.require_safe_directory(unsafe)
    assert stat.S_IMODE(open_directory.stat().st_mode) == 0o755
    assert not (tmp_path / "with,comma").exists()
    assert stat.S_IMODE((tmp_path / "fresh" / "nested").stat().st_mode) == 0o700


def test_importable_generations_newest_valid_per_key_and_at_most_n_max(tmp_path: Path) -> None:
    platform_dir = tmp_path / "linux-arm64"
    newest_per_key = []
    for number in range(build_cache.N_MAX + 2):
        key = f"{number:064x}"
        _write_layout(_generation_dir(platform_dir, key, 100 + number), {"a": b"layer"})
        newest = _generation_dir(platform_dir, key, 1000 + number)
        _write_layout(newest, {"a": b"layer"})
        newest_per_key.append(newest)
    broken = _generation_dir(platform_dir, f"{0:064x}", 5000)
    _write_layout(broken, {"a": b"layer-a"})
    (broken / "oci-layout").unlink()

    importable = store.importable_generations(platform_dir)

    assert importable == list(reversed(newest_per_key))[: build_cache.N_MAX]


def test_list_generations_marks_foreign_shapes_invalid(tmp_path: Path) -> None:
    platform_dir = tmp_path / "linux-arm64"
    valid = _generation_dir(platform_dir, KEY, 7)
    digests = _write_layout(valid, {"a": b"layer-a"})
    (platform_dir / "generations" / "not-a-key" / "x").mkdir(parents=True)
    (platform_dir / "generations" / KEY / "bad-name").mkdir()

    listed = {(item.key, item.name): item for item in store.list_generations(platform_dir)}

    assert listed[(KEY, valid.name)].valid and listed[(KEY, valid.name)].wall_ns == 7
    assert listed[(KEY, valid.name)].bytes == sum(path.lstat().st_size for path in valid.rglob("*") if path.is_file())
    assert not listed[("not-a-key", "x")].valid
    assert not listed[(KEY, "bad-name")].valid
    assert digests


def test_create_staging_creates_the_staging_directory(tmp_path: Path) -> None:
    platform_dir = tmp_path / "linux-arm64"
    platform_dir.mkdir(mode=0o700)
    run, operation = str(uuid.uuid4()), str(uuid.uuid4())

    staging = store.create_staging(platform_dir, run, operation)

    assert staging == platform_dir / "staging" / f"{run}-{operation}"
    for directory in (staging.parent, staging):
        observed = directory.lstat()
        assert stat.S_IMODE(observed.st_mode) == 0o700 and observed.st_uid == os.getuid()
    with pytest.raises(FileExistsError):
        store.create_staging(platform_dir, run, operation)


def test_export_space_ok(tmp_path: Path) -> None:
    needed = build_cache.C_RET_BYTES + build_cache.MARGIN_BYTES

    assert store.export_space_ok(tmp_path, available_bytes=lambda _: needed)
    assert not store.export_space_ok(tmp_path, available_bytes=lambda _: needed - 1)


# Build-path wiring (Task 2.7): every eligible test binds an arm64 Engine so the receipt knows its daemon platform.

BUILD = ("buildx", "build", "--tag", "fixture:latest", ".")
CACHE_FAILURE_TYPES = [
    OSError("synthetic"),
    ValueError("synthetic"),
    ResourceError("synthetic"),
    InvalidInput("synthetic"),
    RecursionError("synthetic"),
]


class Run:
    """One entered run with a builder, dispatching loaded builds through the real dispatcher."""

    def __init__(self, tmp_path: Path, fake: FakeDockerPort | None = None) -> None:
        self.fake = fake or FakeDockerPort()
        self.handle, self.descriptor = _entered_run(tmp_path, self.fake)
        lifecycle.dispatch_docker(self.handle._context, self.descriptor, self.fake, ("buildx", "create"))

    @property
    def platform_dir(self) -> Path:
        data = json.loads(self.handle._context.record.read_text())
        return store.project_cache(self.handle._context.project.state, data["project_id"]) / "linux-arm64"

    def build(self, argv: tuple[str, ...] = BUILD, **seams) -> dict:
        with bind_test_observation_reads(engine=_info("aarch64")):
            lifecycle.dispatch_docker(self.handle._context, self.descriptor, self.fake, argv, **seams)
        return self.receipt()

    def receipt(self) -> dict:
        record = json.loads(self.handle._context.record.read_text())
        builds = [item for item in record["docker_receipts"].values() if item["kind"] == "build"]
        return max(builds, key=lambda item: item["deadline"]["created_ns"])

    def executed(self) -> tuple[str, ...]:
        return next(command for command in reversed(self.fake.commands) if command[:2] == ("buildx", "build"))

    def anchor_is_free(self) -> bool:
        project_cache = self.platform_dir.parent
        try:
            with store.platform_anchor(project_cache, "linux-arm64", shared=False, nonblocking=True):
                return True
        except ResourceBusy:
            return False


def _flags(argv: tuple[str, ...], option: str) -> list[str]:
    return [argv[index + 1] for index, value in enumerate(argv[:-1]) if value == option]


@contextmanager
def _failpoint_at(stage: str, error: BaseException):
    def callback(name: str) -> None:
        if name == stage:
            raise error

    install_failpoint(callback)
    try:
        yield
    finally:
        install_failpoint(None)


def test_hit_and_miss_outcomes(tmp_path: Path) -> None:
    run = Run(tmp_path)
    run.fake.cache_layers = {"a": b"A"}

    first = run.build()
    second = run.build()
    run.fake.cache_layers = {"a": b"A", "c": b"C"}
    third = run.build()

    assert first["plan"]["daemon_platform"] == {"os": "linux", "architecture": "arm64"}
    assert first["plan"]["cache"]["outcome"] == "miss" and first["plan"]["cache"]["imports"] == []
    assert not first["plan"]["cache"]["published"].startswith("not-published")
    assert second["plan"]["cache"]["outcome"] == "hit"
    assert second["plan"]["cache"]["imports"] == [first["plan"]["cache"]["published"]]
    assert third["plan"]["cache"]["outcome"] == "miss"  # one new layer despite non-empty imports
    assert set(first["plan"]["cache"]) == {
        "eligible", "key", "imports", "export", "outcome", "bytes", "published", "finished_ns",
    }  # fmt: skip


def test_planning_seam_return_value_is_executed(tmp_path: Path) -> None:
    run = Run(tmp_path)
    run.fake.cache_layers = {"a": b"A"}
    run.build()

    run.build()

    executed = run.executed()
    cache_to = executed.index("--cache-to")
    assert executed[cache_to + 1].startswith(f"type=local,dest={run.platform_dir / 'staging'}/")
    assert executed[cache_to + 1].endswith(",mode=max,ignore-error=true")
    assert cache_to < executed.index("--tag")  # injected before the caller arguments (DF17)
    assert len(_flags(executed, "--cache-from")) == 1


def test_disk_space_omits_only_the_export(tmp_path: Path) -> None:
    run = Run(tmp_path)
    run.fake.cache_layers = {"a": b"A"}
    run.build()

    receipt = run.build(_available_bytes=lambda _path: 0)

    assert "--cache-to" not in run.executed() and len(_flags(run.executed(), "--cache-from")) == 1
    assert receipt["plan"]["cache"]["outcome"] == "skipped:disk-space"
    assert receipt["plan"]["cache"]["published"] is None and receipt["state"] == "promoted"


@pytest.mark.parametrize(
    ("stage", "error"),
    [("cache-planned-lock", error) for error in CACHE_FAILURE_TYPES]
    + [
        (stage, OSError("synthetic")) for stage in ("cache-planned-lock", "cache-planned-staging", "cache-planned-argv")
    ],
)
def test_planning_failure_releases_the_anchor(tmp_path: Path, stage: str, error: BaseException) -> None:
    run = Run(tmp_path)
    run.fake.cache_layers = {"a": b"A"}

    with _failpoint_at(stage, error):
        receipt = run.build()

    assert receipt["state"] == "promoted"
    assert receipt["plan"]["cache"]["outcome"] == "skipped:error"
    assert "--cache-to" not in run.executed() and "--cache-from" not in run.executed()
    assert run.anchor_is_free()  # X28


@pytest.mark.parametrize("stage", ["cache-lock-release", "cache-publishing"])
@pytest.mark.parametrize("error", CACHE_FAILURE_TYPES, ids=lambda error: type(error).__name__)
def test_every_seam_fails_open(tmp_path: Path, stage: str, error: BaseException) -> None:
    run = Run(tmp_path)
    run.fake.cache_layers = {"a": b"A"}

    with _failpoint_at(stage, error):
        receipt = run.build()

    assert receipt["state"] == "promoted"
    assert receipt["plan"]["cache"]["outcome"] == "skipped:error"
    assert receipt["plan"]["cache"]["published"] == "not-published:error"
    assert run.anchor_is_free()
    assert not any((run.platform_dir / "staging").iterdir())


def test_unsafe_cache_root_is_never_passed_or_repaired(tmp_path: Path) -> None:
    run = Run(tmp_path)
    root = run.platform_dir.parent.parent
    root.mkdir(mode=0o700)
    root.chmod(0o755)

    receipt = run.build()

    assert receipt["plan"]["cache"]["outcome"] == "skipped:cache-root-unsafe"
    assert "--cache-from" not in run.executed() and "--cache-to" not in run.executed()
    assert stat.S_IMODE(root.stat().st_mode) == 0o755 and not run.platform_dir.parent.exists()  # (a)
    run.platform_dir.parent.mkdir(mode=0o700)
    assert run.build()["plan"]["cache"]["outcome"] == "skipped:cache-root-unsafe"  # (b) a safe project below
    assert stat.S_IMODE(root.stat().st_mode) == 0o755


def test_malformed_generation_is_not_imported(tmp_path: Path) -> None:
    run = Run(tmp_path)
    run.fake.cache_layers = {"a": b"A"}
    published = (
        run.platform_dir
        / "generations"
        / ("c" * 64)
        / build_cache.generation_name(1, str(uuid.uuid4()), str(uuid.uuid4()))
    )
    run.build()
    write_cache_layout(published, {"a": b"A"})
    (published / "index.json").write_text("{not json")

    receipt = run.build()

    assert published.name not in receipt["plan"]["cache"]["imports"]
    assert not receipt["plan"]["cache"]["published"].startswith("not-published")


def test_execute_failure_releases_the_anchor(tmp_path: Path) -> None:
    run = Run(tmp_path)
    run.fake.cache_layers = {"a": b"A"}
    run.fake.execute_error = OSError("synthetic")

    with pytest.raises(IncompleteCleanup):
        run.build()

    assert run.receipt()["state"] == "unresolved"
    assert run.anchor_is_free()  # X25


def test_shared_lock_held_until_execute_returns(tmp_path: Path) -> None:
    run = Run(tmp_path)
    observed: list[bool] = []
    run.fake.on_build = lambda _argv: observed.append(run.anchor_is_free())

    run.build()

    assert observed == [False]
    assert run.anchor_is_free()


def test_build_path_respects_the_lock_order(tmp_path: Path) -> None:
    run = Run(tmp_path)
    run.fake.cache_layers = {"a": b"A"}

    with observed_locks() as checker:
        run.build()

    assert checker.violations == []
    anchors = [event for event in checker.events if event[1] == "linux-arm64" and event[0] == "acquire"]
    assert [shared for _, _, shared, _, _ in anchors] == [True, False]


def test_planning_imports_at_most_n_max_generations(tmp_path: Path) -> None:
    run = Run(tmp_path)
    run.build()  # creates the private cache tree
    planted = []
    for number in range(build_cache.N_MAX + 2):
        path = (
            run.platform_dir
            / "generations"
            / f"{number:064x}"
            / build_cache.generation_name(100 + number, str(uuid.uuid4()), str(uuid.uuid4()))
        )
        store.require_safe_directory(path.parent.parent)
        store.require_safe_directory(path.parent)
        write_cache_layout(path, {"a": f"layer-{number}".encode()})
        planted.append(path)

    run.build()

    sources = _flags(run.executed(), "--cache-from")
    assert sources == [f"type=local,src={path}" for path in reversed(planted[2:])]


def test_comma_in_cache_root_disables_the_cache(tmp_path: Path) -> None:
    (tmp_path / "with,comma").mkdir()
    run = Run(tmp_path / "with,comma")

    receipt = run.build()

    assert receipt["plan"]["cache"]["outcome"] == "skipped:cache-root-unsafe"
    assert "--cache-from" not in run.executed() and "--cache-to" not in run.executed()


def test_foreign_platform_is_not_eligible(tmp_path: Path) -> None:
    run = Run(tmp_path)

    receipt = run.build(("buildx", "build", "--platform", "linux/amd64", "--tag", "fixture:latest", "."))

    assert receipt["plan"]["cache"] == {
        "eligible": False,
        "outcome": "not-eligible:foreign-platform",
        "finished_ns": receipt["plan"]["cache"]["finished_ns"],
    }
    assert "--cache-from" not in run.executed() and "--cache-to" not in run.executed()


def test_non_native_image_is_not_published(tmp_path: Path) -> None:
    run = Run(tmp_path)
    run.fake.cache_layers = {"a": b"A"}
    run.fake.default_image_platform = "linux/amd64"

    receipt = run.build()

    assert receipt["plan"]["cache"]["published"] == "not-published:not-native"
    assert not any((run.platform_dir / "staging").iterdir())
    assert store.list_generations(run.platform_dir) == []


def test_switch_private_addition_reads_only_the_switch() -> None:
    environ = {build_cache.SWITCH_ENV: "off", "DOCKER_TEST_TOOLKIT_OTHER": "x", "HOME": "/h"}

    assert store.switch_private_addition(environ) == {build_cache.SWITCH_ENV: "off"}
    assert store.switch_private_addition({"HOME": "/h"}) == {}


@pytest.mark.parametrize("value", ["off", None])
def test_child_environment_forwards_the_switch(tmp_path: Path, value: str | None) -> None:
    script = (
        "import json, sys\n"
        "from pathlib import Path\n"
        "from docker_test_toolkit.application import lifecycle\n"
        "print(json.dumps(lifecycle._toolkit_child_environment(Path(sys.argv[1]))))\n"
    )
    environment = {key: item for key, item in os.environ.items() if key != build_cache.SWITCH_ENV}
    if value is not None:
        environment[build_cache.SWITCH_ENV] = value
    printed = subprocess.run(
        [sys.executable, "-I", "-c", script, str(tmp_path)], env=environment, check=True, capture_output=True, text=True
    ).stdout

    assert json.loads(printed).get(build_cache.SWITCH_ENV) == value


def test_digest_corrupt_generation_is_skipped_and_superseded(tmp_path: Path) -> None:
    project_cache = tmp_path / "cache" / KEY
    platform_dir = project_cache / "linux-arm64"
    store.require_safe_directory(platform_dir)
    corrupt = _generation_dir(platform_dir, KEY, 1)
    store.require_safe_directory(corrupt.parent)
    _write_layout(corrupt, {"nonce": b"nonce-1"})
    blob = corrupt / "blobs" / "sha256" / hashlib.sha256(b"nonce-1").hexdigest()
    blob.write_bytes(b"nonce-2")  # one byte changed, size kept
    other = _generation_dir(platform_dir, "b" * 64, 2)
    store.require_safe_directory(other.parent)
    _write_layout(other, {"a": b"layer"})

    assert build_cache.DIGEST_VERIFICATION is True
    assert store.validate_generation(corrupt)  # structurally valid: only the import hashes
    assert store.importable_generations(platform_dir) == [other]
    staging = store.create_staging(platform_dir, str(uuid.uuid4()), str(uuid.uuid4()))
    staging.rmdir()
    _write_layout(staging, {"nonce": b"nonce-3"})
    plan = {"key": KEY, "staging": str(staging)}
    result = store.publish(
        platform_dir, project_cache, "linux-arm64", plan, run_id=str(uuid.uuid4()), operation_id=str(uuid.uuid4())
    )

    assert not corrupt.exists()
    assert platform_dir / "generations" / KEY / str(result["published"]) in store.importable_generations(platform_dir)


def test_a_file_in_place_of_a_cache_directory_is_unsafe(tmp_path: Path) -> None:
    run = Run(tmp_path)
    run.platform_dir.parent.mkdir(parents=True, mode=0o700)
    run.platform_dir.parent.parent.chmod(0o700)
    run.platform_dir.write_text("not a directory")

    receipt = run.build()

    assert receipt["plan"]["cache"]["outcome"] == "skipped:cache-root-unsafe"


def test_a_default_dockerfile_resolving_into_the_private_root_is_ineligible(tmp_path: Path) -> None:
    run = Run(tmp_path)
    owned = run.handle.private_root / "generation-2"
    owned.mkdir(mode=0o700)
    (owned / "Dockerfile").write_text("FROM scratch\n")
    linked, ordinary = tmp_path / "linked", tmp_path / "ordinary"
    linked.mkdir()
    (linked / "Dockerfile").symlink_to(owned / "Dockerfile")
    ordinary.mkdir()
    (ordinary / "Dockerfile").write_text("FROM scratch\n")

    escaped = run.build(("buildx", "build", "--tag", "fixture:latest", str(linked)))
    plain = run.build(("buildx", "build", "--tag", "fixture:latest", str(ordinary)))

    assert escaped["plan"]["cache"]["outcome"] == "not-eligible:private-root-context"
    assert plain["plan"]["cache"]["eligible"] is True


def _images(run: Run) -> dict:
    return json.loads(run.handle._context.record.read_text())["owned_resources"]["images"]


def test_an_identical_rebuild_under_a_second_tag_promotes_and_removes_every_tag(tmp_path: Path) -> None:
    run = Run(tmp_path)

    first = run.build(("buildx", "build", "--tag", "fixture:one", "."))
    second = run.build(("buildx", "build", "--tag", "fixture:two", "."))

    assert (first["state"], second["state"]) == ("promoted", "promoted")
    (image,) = _images(run).values()
    assert image["evidence"]["repo_tags"] == ["fixture:one", "fixture:two"]
    assert image["evidence"]["tags"] == ["fixture:one", "fixture:two"]
    assert image["receipt_id"] == first["id"]
    lifecycle.dispatch_docker(run.handle._context, run.descriptor, run.fake, ("image", "rm", image["identifier"]))
    removal = next(command for command in reversed(run.fake.commands) if command[:2] == ("image", "rm"))
    assert {"fixture:one", "fixture:two"} <= set(removal)
    lifecycle.dispatch_docker(
        run.handle._context, run.descriptor, run.fake, ("buildx", "rm", run.handle.docker_context.builder)
    )
    assert run.handle.leave().cleanup_outcome == "complete"


def test_a_removed_image_can_be_built_again_in_the_same_run(tmp_path: Path) -> None:
    run = Run(tmp_path)
    first = run.build(("buildx", "build", "--tag", "fixture:one", "."))
    image_id = next(iter(_images(run)))
    lifecycle.dispatch_docker(run.handle._context, run.descriptor, run.fake, ("image", "rm", image_id))

    again = run.build(("buildx", "build", "--tag", "fixture:one", "."))

    assert again["state"] == "promoted"
    assert _images(run)[image_id]["state"] == "present" and _images(run)[image_id]["receipt_id"] == again["id"]
    assert first["id"] != again["id"]


def test_a_foreign_tag_on_a_rebuilt_image_still_leaves_the_receipt_unresolved(tmp_path: Path) -> None:
    run = Run(tmp_path)
    run.build(("buildx", "build", "--tag", "fixture:one", "."))
    run.fake.images[BUILT_IMAGE_ID]["RepoTags"].append("foreign:tag")  # a tag this run never created

    with pytest.raises(IncompleteCleanup):
        run.build(("buildx", "build", "--tag", "fixture:two", "."))

    assert run.receipt()["state"] == "unresolved"


def test_a_lowercase_default_dockerfile_resolving_into_the_private_root_is_ineligible(tmp_path: Path) -> None:
    run = Run(tmp_path)
    owned = run.handle.private_root / "generation-2"
    owned.mkdir(mode=0o700)
    (owned / "Dockerfile").write_text("FROM scratch\n")
    linked = tmp_path / "linked"
    linked.mkdir()
    (linked / "dockerfile").symlink_to(owned / "Dockerfile")  # BuildKit's lowercase fallback

    escaped = run.build(("buildx", "build", "--tag", "fixture:latest", str(linked)))

    assert escaped["plan"]["cache"]["outcome"] == "not-eligible:private-root-context"


def test_an_image_loaded_by_a_failed_build_is_owned_and_cleaned_up(tmp_path: Path) -> None:
    run = Run(tmp_path)
    run.fake.fail_build_after_load = True

    failed = run.build(("buildx", "build", "--tag", "fixture:late", "."))

    assert failed["state"] == "failed"
    assert _images(run)[BUILT_IMAGE_ID]["state"] == "present"  # owned, so exact cleanup removes it
    run.fake.fail_build_after_load = False
    again = run.build(("buildx", "build", "--tag", "fixture:late", "."))  # no "collides with an unowned image"
    assert again["state"] == "promoted"
    lifecycle.dispatch_docker(run.handle._context, run.descriptor, run.fake, ("image", "rm", BUILT_IMAGE_ID))
    lifecycle.dispatch_docker(
        run.handle._context, run.descriptor, run.fake, ("buildx", "rm", run.handle.docker_context.builder)
    )
    assert run.handle.leave().cleanup_outcome == "complete"
    assert _images(run)[BUILT_IMAGE_ID]["state"] == "removed" and BUILT_IMAGE_ID not in run.fake.images


def test_a_failed_build_never_claims_an_image_with_foreign_labels(tmp_path: Path) -> None:
    run = Run(tmp_path)
    run.fake.fail_build_after_load = True
    run.fake.build_foreign_labels = True

    failed = run.build(("buildx", "build", "--tag", "fixture:late", "."))

    assert failed["state"] == "failed" and _images(run) == {}


def test_a_builder_worker_stopped_but_not_yet_removed_is_never_observed_as_running(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = Run(tmp_path)
    monkeypatch.setattr(lifecycle, "_BOUND_DOCKER_FACTORY", lambda _identity: run.fake)  # as the sibling cleanup test
    run.build()
    run.fake.stop_buildkit_after_builder_remove = True
    worker_id = next(iter(_owned_containers(run)))

    lifecycle.dispatch_docker(
        run.handle._context, run.descriptor, run.fake, ("buildx", "rm", run.handle.docker_context.builder)
    )

    assert _owned_containers(run)[worker_id]["state"] == "stopped"  # the active observer only reads running ones
    lifecycle._cleanup_owned_docker_resources(run.handle._context)
    assert _owned_containers(run)[worker_id]["state"] == "removed"


def _owned_containers(run: Run) -> dict:
    return json.loads(run.handle._context.record.read_text())["owned_resources"]["containers"]


def test_a_compose_wait_timeout_extends_the_dispatch_deadline() -> None:
    from docker_test_toolkit.application.docker_commands import ClassifiedDockerCommand

    waiting = ClassifiedDockerCommand("compose", ("compose", "up"), {"wait_seconds": 120})
    plain = ClassifiedDockerCommand("compose", ("compose", "up"), {})
    build = ClassifiedDockerCommand("build", ("buildx", "build"), {})

    assert lifecycle._dispatch_seconds(waiting) == 150  # the wait plus the ordinary 30 s mutation budget
    assert lifecycle._dispatch_seconds(plain) == 30
    assert lifecycle._dispatch_seconds(build) == 1200
