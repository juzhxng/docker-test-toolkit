"""Build-cache exclusive operations under SIGKILL and real cross-process lock contention."""

from __future__ import annotations

import hashlib
import json
import os
import stat
import subprocess
import sys
import time
import uuid
from collections.abc import Mapping
from dataclasses import fields
from pathlib import Path

import pytest

from docker_test_toolkit.application import build_cache as store
from docker_test_toolkit.application.recovery import Context, cleanup_project, project_id_for
from docker_test_toolkit.domain import build_cache
from tests.integration.account_fixtures import FAILPOINT_CHILD, ScriptedDocker, _hold_lock, wait_until
from tests.integration.test_docker_receipts import _identity as receipts_identity
from tests.integration.test_project_retirement import _activated, _vanished_project

pytestmark = pytest.mark.integration

GIB = 1024**3
PLATFORM = "linux-arm64"
OTHER_RUN = str(uuid.UUID(int=7))
OUR_RUN = str(uuid.UUID(int=8))


def _key(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def _store(tmp_path: Path) -> tuple[Path, Path]:
    project_cache = tmp_path / "cache" / ("b" * 64)
    for level in (tmp_path / "cache", project_cache, project_cache / PLATFORM):
        store.require_safe_directory(level)
    return project_cache, project_cache / PLATFORM


def _layout(path: Path, sizes: list[int]) -> None:
    """Write a structurally valid cache export whose layer blobs are sparse files of the given sizes."""
    path.mkdir(mode=0o700)
    blobs = path / "blobs" / "sha256"
    blobs.mkdir(parents=True, mode=0o700)

    def blob(name: str, data: bytes | None = None, size: int = 0) -> dict:
        """Write real bytes, or a unique header followed by sparse zeros, under the blob's true SHA-256."""
        header = data if data is not None else hashlib.sha256(name.encode()).digest()
        digest = hashlib.sha256(header)
        for remaining in range(max(size - len(header), 0), 0, -(1 << 24)):
            digest.update(bytes(min(remaining, 1 << 24)))
        with open(blobs / digest.hexdigest(), "wb") as stream:
            stream.write(header)
            if data is None:
                stream.truncate(max(size, len(header)))
        return {"digest": f"sha256:{digest.hexdigest()}", "size": max(size, len(header)) if data is None else len(data)}

    config = {"mediaType": "application/vnd.buildkit.cacheconfig.v0", **blob("config", b"{}")}
    layers = [
        {"mediaType": "application/vnd.oci.image.layer.v1.tar+gzip", **blob(f"{path}-{index}", size=size)}
        for index, size in enumerate(sizes)
    ]
    manifest = blob("manifest", json.dumps({"schemaVersion": 2, "config": config, "layers": layers}).encode())
    annotation = dict([build_cache.CACHE_INDEX_ANNOTATION])
    (path / "index.json").write_text(
        json.dumps({"schemaVersion": 2, "manifests": [{**manifest, "annotations": annotation}]})
    )
    (path / "oci-layout").write_text('{"imageLayoutVersion":"1.0.0"}')


def _generation(platform_dir: Path, key: str, wall_ns: int, *, run: str = OTHER_RUN, sizes=(1024,)) -> Path:
    for level in (platform_dir / "generations", platform_dir / "generations" / key):
        store.require_safe_directory(level)
    path = platform_dir / "generations" / key / build_cache.generation_name(wall_ns, run, str(uuid.uuid4()))
    _layout(path, list(sizes))
    return path


def _staging(platform_dir: Path, *, run: str = OUR_RUN, sizes=(1024,)) -> Path:
    staging = store.create_staging(platform_dir, run, str(uuid.uuid4()))
    staging.rmdir()
    _layout(staging, list(sizes))
    return staging


def _plan(key: str, staging: Path) -> dict:
    return {"key": key, "staging": str(staging)}


def _child(tmp_path: Path, entry: str, arguments: dict, stages: dict | None = None, *, name: str):
    """Start one ``failpoint_child.py`` cache entry; return the process and its JSON output path."""
    output = tmp_path / f"{name}.output.json"
    spec = tmp_path / f"{name}.spec.json"
    document = {
        "entry": entry,
        "arguments": {**arguments, "output": str(output)},
        "stages": stages or {},
        "result": str(tmp_path / f"{name}.result.json"),
    }
    spec.write_text(json.dumps(document))
    return subprocess.Popen([sys.executable, "-P", str(FAILPOINT_CHILD), str(spec)]), output


def _killed_publish(tmp_path: Path, project_cache: Path, key: str, staging: Path, stages: dict, *, name: str) -> None:
    arguments = {
        "project_cache": str(project_cache),
        "platform": PLATFORM,
        "plan": _plan(key, staging),
        "run_id": OUR_RUN,
        "operation_id": str(uuid.uuid4()),
    }
    process, _ = _child(tmp_path, "cache-publish", arguments, stages, name=name)
    assert process.wait(timeout=60) == -9


def _reconcile(project_cache: Path) -> None:
    with store.platform_anchor(project_cache, PLATFORM, shared=False):
        store.reconcile(project_cache / PLATFORM)


def _assert_caps(platform_dir: Path) -> list[build_cache.GenerationFacts]:
    listed = store.list_generations(platform_dir)
    assert all(item.valid for item in listed)
    assert len(listed) <= build_cache.N_MAX
    assert sum(item.bytes for item in listed) <= build_cache.C_RET_BYTES
    assert len({item.key for item in listed}) == len(listed)
    return listed


def _tree_digest(path: Path) -> str:
    digest = hashlib.sha256()
    for item in sorted(path.rglob("*")):
        digest.update(str(item.relative_to(path)).encode())
        if item.is_file():
            digest.update(item.read_bytes())
    return digest.hexdigest()


def _publish(project_cache: Path, key: str, staging: Path) -> dict:
    return store.publish(
        project_cache / PLATFORM,
        project_cache,
        PLATFORM,
        _plan(key, staging),
        run_id=OUR_RUN,
        operation_id=str(uuid.uuid4()),
    )


def test_publication_extends_the_importable_set(tmp_path: Path) -> None:
    project_cache, platform_dir = _store(tmp_path)
    previous = [_generation(platform_dir, _key(f"k{index}"), 10 + index) for index in range(3)]

    result = _publish(project_cache, _key("new"), _staging(platform_dir))

    published = platform_dir / "generations" / _key("new") / str(result["published"])
    assert set(store.importable_generations(platform_dir)) == {*previous, published}
    assert result["bytes"] == sum(path.lstat().st_size for path in published.rglob("*") if path.is_file())


def test_sigkill_before_rename_keeps_remaining_generations_valid(tmp_path: Path) -> None:
    project_cache, platform_dir = _store(tmp_path)
    superseded = _generation(platform_dir, _key("k"), 1)
    kept = _generation(platform_dir, _key("k"), 2)
    invalid = _generation(platform_dir, _key("broken"), 3)
    (invalid / "oci-layout").unlink()
    staging = _staging(platform_dir)

    _killed_publish(
        tmp_path, project_cache, _key("new"), staging, {"cache-reconciled": {"action": "sigkill"}}, name="p"
    )

    assert not superseded.exists() and not invalid.exists()
    assert [item.name for item in store.list_generations(platform_dir)] == [kept.name]
    assert staging.is_dir()  # left for the next reconcile (T12)
    _reconcile(project_cache)
    assert not staging.exists()


def test_repeated_interruption_of_one_key(tmp_path: Path) -> None:
    project_cache, platform_dir = _store(tmp_path)
    for attempt in range(3):
        staging = _staging(platform_dir)
        _killed_publish(
            tmp_path, project_cache, _key("k"), staging, {"cache-renamed": {"action": "sigkill"}}, name=f"p{attempt}"
        )

    _reconcile(project_cache)

    assert len([item for item in store.list_generations(platform_dir) if item.key == _key("k")]) == 1
    assert not any((platform_dir / "staging").iterdir())


def test_sigkill_during_publication_deletions(tmp_path: Path) -> None:
    project_cache, platform_dir = _store(tmp_path)
    marked = _generation(platform_dir, _key("big"), 1, sizes=(5 * GIB // 2,))
    superseded = _generation(platform_dir, _key("k"), 2, sizes=(GIB,))
    staging = _staging(platform_dir, sizes=(2 * GIB,))
    stages = {"cache-publication-deleting": {"action": "sigkill", "occurrence": 2}}

    _killed_publish(tmp_path, project_cache, _key("k"), staging, stages, name="p")

    new = [path for path in store.importable_generations(platform_dir) if path.parent.name == _key("k")]
    assert len(new) == 1 and new[0] != superseded
    assert not superseded.exists()
    store.validate_generation(marked)  # the second planned deletion never started: still whole
    _reconcile(project_cache)
    _assert_caps(platform_dir)
    assert not marked.exists()


def test_sigkill_during_reconcile_superseding(tmp_path: Path) -> None:
    project_cache, platform_dir = _store(tmp_path)
    for wall_ns in (1, 2):
        _generation(platform_dir, _key("k"), wall_ns)
    newest_k = _generation(platform_dir, _key("k"), 3)
    _generation(platform_dir, _key("l"), 1)
    newest_l = _generation(platform_dir, _key("l"), 2)
    arguments = {"project_cache": str(project_cache), "platform": PLATFORM}
    stages = {"cache-reconcile-superseding": {"action": "sigkill", "occurrence": 2}}

    process, _ = _child(tmp_path, "cache-reconcile", arguments, stages, name="r")

    assert process.wait(timeout=60) == -9
    assert set(store.importable_generations(platform_dir)) == {newest_k, newest_l}
    for path in (newest_k, newest_l):
        store.validate_generation(path)


def test_interrupted_cross_key_eviction_new_key(tmp_path: Path) -> None:
    project_cache, platform_dir = _store(tmp_path)
    for index in range(build_cache.N_MAX):
        _generation(platform_dir, _key(f"k{index}"), 10 + index)

    _killed_publish(
        tmp_path, project_cache, _key("new"), _staging(platform_dir), {"cache-renamed": {"action": "sigkill"}}, name="p"
    )
    _reconcile(project_cache)

    assert _key("new") in {item.key for item in _assert_caps(platform_dir)}


def test_interrupted_cross_key_eviction_larger_replacement(tmp_path: Path) -> None:
    project_cache, platform_dir = _store(tmp_path)
    _generation(platform_dir, _key("big"), 1, sizes=(5 * GIB // 2,))
    _generation(platform_dir, _key("k"), 2, sizes=(GIB,))
    staging = _staging(platform_dir, sizes=(3 * GIB,))

    _killed_publish(tmp_path, project_cache, _key("k"), staging, {"cache-renamed": {"action": "sigkill"}}, name="p")
    _reconcile(project_cache)

    assert [item.key for item in _assert_caps(platform_dir)] == [_key("k")]


def test_partial_staging_is_not_published(tmp_path: Path) -> None:
    project_cache, platform_dir = _store(tmp_path)
    staging = _staging(platform_dir)
    next(path for path in (staging / "blobs" / "sha256").iterdir() if path.stat().st_size == 1024).unlink()

    assert _publish(project_cache, _key("k"), staging) == {"published": "not-published:invalid-export", "bytes": None}
    assert not staging.exists()
    assert store.list_generations(platform_dir) == []


def test_publication_times_out_against_a_shared_holder(tmp_path: Path) -> None:
    project_cache, platform_dir = _store(tmp_path)
    reader = _generation(platform_dir, _key("k"), 1)
    before = _tree_digest(reader)
    staging = _staging(platform_dir)
    acquired, release = tmp_path / "acquired", tmp_path / "release"
    arguments = {"project_cache": str(project_cache), "platform": PLATFORM, "acquired": str(acquired)}
    holder, _ = _child(tmp_path, "cache-hold-shared", {**arguments, "release": str(release)}, name="h")
    try:
        wait_until(acquired.exists, timeout=30, message="the shared holder acquired the anchor")
        started = time.monotonic()
        result = _publish(project_cache, _key("k"), staging)
        elapsed = time.monotonic() - started
    finally:
        release.write_text("")
        holder.wait(timeout=30)

    assert result == {"published": "not-published:lock-busy", "bytes": None}
    assert elapsed >= build_cache.EXCLUSIVE_LOCK_SECONDS
    assert _tree_digest(reader) == before and not staging.exists()


def test_waiter_that_opened_the_anchor_before_removal_contends_on_the_same_inode(tmp_path: Path) -> None:
    project_cache, platform_dir = _store(tmp_path)
    for index in range(2):
        _generation(platform_dir, _key(f"k{index}"), index + 1)
    removing, release_removal = tmp_path / "removing", tmp_path / "release-removal"
    base = {"project_cache": str(project_cache), "platform": PLATFORM}
    stages = {"cache-removing": {"occurrence": 1, "action": [
        {"do": "write-file", "path": str(removing)}, {"do": "wait-file", "path": str(release_removal)},
    ]}}  # fmt: skip
    remover, removed = _child(tmp_path, "cache-remove", base, stages, name="remove")
    wait_until(removing.exists, timeout=30, message="the removal holds the anchor exclusively")
    a_acquired, a_release, a_inode = tmp_path / "a-acquired", tmp_path / "a-release", tmp_path / "a-inode"
    waiter, _ = _child(
        tmp_path,
        "cache-hold-shared",
        {**base, "acquired": str(a_acquired), "release": str(a_release), "anchor_inode": str(a_inode)},
        name="a",
    )
    b_acquired = tmp_path / "b-acquired"
    try:
        time.sleep(0.5)
        assert not a_acquired.exists()  # A blocks on the anchor the removal holds
        release_removal.write_text("")
        assert remover.wait(timeout=30) == 0
        wait_until(a_acquired.exists, timeout=30, message="A acquired after the removal")
        reconciler, _ = _child(tmp_path, "cache-reconcile", {**base, "acquired": str(b_acquired)}, name="b")
        time.sleep(0.5)
        assert not b_acquired.exists()  # B cannot take the anchor exclusively while A holds it shared
    finally:
        a_release.write_text("")
        waiter.wait(timeout=30)
    assert reconciler.wait(timeout=30) == 0 and b_acquired.exists()
    anchor = os.stat(project_cache / f"{PLATFORM}.lock").st_ino
    assert int(a_acquired.read_text()) == int(a_inode.read_text()) == anchor
    assert json.loads(removed.read_text()) > 0 and not platform_dir.exists()


def test_removal_killed_midway_leaves_no_tombstone(tmp_path: Path) -> None:
    project_cache, platform_dir = _store(tmp_path)
    generations = [_generation(platform_dir, _key(f"k{index}"), index + 1) for index in range(3)]
    base = {"project_cache": str(project_cache), "platform": PLATFORM}

    remover, _ = _child(
        tmp_path, "cache-remove", base, {"cache-removing": {"action": "sigkill", "occurrence": 2}}, name="r"
    )

    assert remover.wait(timeout=60) == -9
    assert sorted(path.name for path in project_cache.iterdir()) == [PLATFORM, f"{PLATFORM}.lock"]
    assert sorted(path.name for path in platform_dir.iterdir()) == ["generations"]
    assert sum(path.exists() for path in generations) == 2
    _staging(platform_dir)
    _reconcile(project_cache)
    assert all(item.valid for item in store.list_generations(platform_dir))
    assert not any((platform_dir / "staging").iterdir())


def test_staging_removed_before_publication(tmp_path: Path) -> None:
    project_cache, platform_dir = _store(tmp_path)
    staging = _staging(platform_dir)

    assert isinstance(store.remove_platform(project_cache, PLATFORM), int)
    assert _publish(project_cache, _key("k"), staging) == {"published": "not-published:removed", "bytes": None}


def test_reconcile_reads_no_layer_content(tmp_path: Path) -> None:
    project_cache, platform_dir = _store(tmp_path)
    unreadable = _generation(platform_dir, _key("k1"), 1)
    layer = next(path for path in (unreadable / "blobs" / "sha256").iterdir() if path.stat().st_size == 1024)
    layer.chmod(0)
    try:
        result = _publish(project_cache, _key("k2"), _staging(platform_dir))
        assert not str(result["published"]).startswith("not-published")
        assert unreadable.exists()  # structurally valid; only the import, which hashes, skips it (T6, DF13)
    finally:
        layer.chmod(0o600)


def test_two_publishers_of_one_key(tmp_path: Path) -> None:
    project_cache, platform_dir = _store(tmp_path)
    children = []
    for name in ("p1", "p2"):
        arguments = {
            "project_cache": str(project_cache),
            "platform": PLATFORM,
            "plan": _plan(_key("k"), _staging(platform_dir)),
            "run_id": OUR_RUN,
            "operation_id": str(uuid.uuid4()),
        }
        children.append(_child(tmp_path, "cache-publish", arguments, name=name))
    for process, _ in children:
        assert process.wait(timeout=60) == 0

    _reconcile(project_cache)

    assert len([item for item in store.list_generations(platform_dir) if item.key == _key("k")]) == 1
    assert not any((platform_dir / "staging").iterdir())


def test_partially_deleted_generation_is_not_imported_and_is_reconciled(tmp_path: Path) -> None:
    project_cache, platform_dir = _store(tmp_path)
    partial = _generation(platform_dir, _key("k1"), 1)
    whole = _generation(platform_dir, _key("k2"), 2)
    before = _tree_digest(whole)
    next(path for path in (partial / "blobs" / "sha256").iterdir() if path.stat().st_size == 1024).unlink()

    assert store.importable_generations(platform_dir) == [whole]
    _reconcile(project_cache)

    assert not partial.exists() and _tree_digest(whole) == before


def test_publication_is_a_rename(tmp_path: Path) -> None:
    project_cache, platform_dir = _store(tmp_path)
    staging = _staging(platform_dir)
    identity = (staging.stat().st_dev, staging.stat().st_ino)

    result = _publish(project_cache, _key("k"), staging)

    key_directory = platform_dir / "generations" / _key("k")
    published = key_directory / str(result["published"])
    assert (published.stat().st_dev, published.stat().st_ino) == identity
    observed = key_directory.lstat()
    assert stat.S_IMODE(observed.st_mode) == 0o700 and observed.st_uid == os.getuid()


def _switch_fake_docker(directory: Path) -> None:
    """A Docker CLI that proves the private default daemon, reports every tag absent and refuses everything else."""
    docker = directory / "docker"
    docker.write_text(
        """#!/bin/sh
set -eu
case "$*" in
  "context show") printf '%s\\n' fixture ;;
  "version --format {{.Client.Version}}") printf '%s\\n' 28.4.0 ;;
  "version --format {{.Client.Version}} {{json .Server.Components}}") printf '%s\\n' '28.4.0 null' ;;
  "info --format {{.ID}}") printf '%s\\n' 11111111-1111-1111-1111-111111111111 ;;
  "image inspect "*)
    for last; do :; done
    printf '[]\\n'
    printf 'Error response from daemon: No such image: %s\\n' "$last" >&2
    exit 1 ;;
  *) exit 71 ;;
esac
"""
    )
    docker.chmod(0o700)


@pytest.mark.parametrize("switch", ["off", None], ids=["off", "unset"])
def test_facade_environment_carries_the_switch_to_the_dispatcher(tmp_path: Path, switch: str | None) -> None:
    binary_directory, project, temporary = tmp_path / "bin", tmp_path / "project", tmp_path / "tmp"
    for directory in (binary_directory, project, temporary):
        directory.mkdir()
    _switch_fake_docker(binary_directory)
    environment = {"PATH": f"{binary_directory}{os.pathsep}{os.environ['PATH']}", "TMPDIR": str(temporary)}
    if switch is not None:
        environment[build_cache.SWITCH_ENV] = switch
    output = tmp_path / "facade.output.json"
    spec = tmp_path / "facade.spec.json"
    identity = receipts_identity()
    arguments = {
        "state": str(tmp_path / "state"),
        "project_root": str(project),
        "bin": str(binary_directory),
        "artifact": {
            item.name: dict(value) if isinstance(value := getattr(identity, item.name), Mapping) else value
            for item in fields(identity)
        },
        "output": str(output),
    }
    spec.write_text(json.dumps({"entry": "facade-build", "arguments": arguments, "result": str(tmp_path / "r.json")}))

    completed = subprocess.run(
        [sys.executable, "-P", str(FAILPOINT_CHILD), str(spec)], env=environment, capture_output=True, text=True
    )

    assert completed.returncode == 0, (completed.stdout, completed.stderr, (tmp_path / "r.json").read_text())
    observed = json.loads(output.read_text())
    if switch == "off":
        assert build_cache.SWITCH_ENV in observed["keys"]
        assert observed["cache"]["outcome"] == "skipped:disabled"
    else:
        assert build_cache.SWITCH_ENV not in observed["keys"]
        assert observed["cache"]["outcome"] == "not-eligible:platform-unknown"


def _registered_cache(tmp_path: Path) -> tuple[Path, Path, str, Path]:
    """A registry, a live project root and that project's cache with one generation and its anchor."""
    registry, root = tmp_path / "registry", tmp_path / "project"
    root.mkdir()
    project_id = project_id_for(root.resolve())
    project_cache = store.project_cache(registry, project_id)
    for level in (store.cache_root(registry), project_cache, project_cache / PLATFORM):
        store.require_safe_directory(level)
    with store.platform_anchor(project_cache, PLATFORM, shared=True):
        pass
    _generation(project_cache / PLATFORM, _key("k"), 1)
    return registry, root, project_id, project_cache


def test_cache_remove_project_dry_run_then_execute(tmp_path: Path) -> None:
    registry, root, project_id, project_cache = _registered_cache(tmp_path)
    size = sum(path.lstat().st_size for path in (project_cache / PLATFORM).rglob("*") if path.is_file())

    listed = store.remove_caches(registry, {"project_root": str(root)}, execute=False)
    removed = store.remove_caches(registry, {"project_root": str(root)}, execute=True)

    platform = {"bytes": size, "generations": 1, "staging": 0, "busy": False}
    assert listed == {
        "mode": "dry-run",
        "cache_root": str(store.cache_root(registry)),
        "projects": [{"project_id": project_id, "platforms": {PLATFORM: platform}}],
    }
    assert removed == {
        "mode": "execute",
        "cache_root": str(store.cache_root(registry)),
        "results": [{"project_id": project_id, "platforms": {PLATFORM: size}}],
        "busy": False,
    }
    assert not (project_cache / PLATFORM).exists() and (project_cache / f"{PLATFORM}.lock").exists()


def test_cache_remove_dry_runs_create_nothing(tmp_path: Path) -> None:
    registry, root = tmp_path / "registry", tmp_path / "project"
    root.mkdir()

    orphans = store.remove_caches(registry, {"orphans": "all"}, execute=False)
    project = store.remove_caches(registry, {"project_root": str(root)}, execute=False)

    assert orphans["projects"] == [] and project["projects"] == []
    assert not store.cache_root(registry).exists()


@pytest.mark.parametrize("holder", ["shared-holder", "exclusive-holder"])
def test_cache_remove_reports_busy_and_removes_nothing(tmp_path: Path, holder: str) -> None:
    registry, root, project_id, project_cache = _registered_cache(tmp_path)
    before = _tree_digest(project_cache / PLATFORM)
    release = tmp_path / "release"
    if holder == "shared-holder":
        acquired = tmp_path / "acquired"
        arguments = {"project_cache": str(project_cache), "platform": PLATFORM, "acquired": str(acquired)}
        process, _ = _child(tmp_path, "cache-hold-shared", {**arguments, "release": str(release)}, name="h")
        wait_until(acquired.exists, timeout=30, message="the shared holder acquired the anchor")
    else:
        process = _hold_lock(project_cache / f"{PLATFORM}.lock")
    try:
        listed = store.remove_caches(registry, {"project_root": str(root)}, execute=False)
        removed = store.remove_caches(registry, {"project_root": str(root)}, execute=True)
    finally:
        release.write_text("")
        if holder == "exclusive-holder":
            process.kill()
        process.wait(timeout=30)

    assert listed["projects"][0]["platforms"][PLATFORM]["busy"] is True
    assert removed["results"] == [{"project_id": project_id, "platforms": {PLATFORM: "busy"}}] and removed["busy"]
    assert _tree_digest(project_cache / PLATFORM) == before


def test_orphan_removal_rederives_and_keeps_a_reappeared_project(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = _activated(tmp_path, monkeypatch)
    registry = service.state_directory
    reappearing, gone = tmp_path / "reappearing", tmp_path / "gone"
    caches = {}
    for project in (reappearing, gone):
        project_id, _ = _vanished_project(service, project)
        caches[project] = store.project_cache(registry, project_id)
        for level in (store.cache_root(registry), caches[project], caches[project] / PLATFORM):
            store.require_safe_directory(level)
        _generation(caches[project] / PLATFORM, _key("k"), 1)
    unregistered = store.project_cache(registry, "d" * 64)
    store.require_safe_directory(unregistered / PLATFORM)
    _generation(unregistered / PLATFORM, _key("k"), 1)

    listed = store.remove_caches(registry, {"orphans": "all"}, execute=False)
    reappearing.mkdir()
    removed = store.remove_caches(registry, {"orphans": "all"}, execute=True)

    expected = {caches[reappearing].name, caches[gone].name, "d" * 64}
    assert {item["project_id"] for item in listed["projects"]} == expected
    assert {item["project_id"] for item in removed["results"]} == {caches[gone].name, "d" * 64}
    assert (caches[reappearing] / PLATFORM).exists() and not (caches[gone] / PLATFORM).exists()


def test_cleanup_project_keeps_the_cache(tmp_path: Path) -> None:
    registry, root, _project_id, project_cache = _registered_cache(tmp_path)
    before = _tree_digest(project_cache)
    context = Context(root.resolve(), registry)

    for execute in (False, True):
        cleanup_project(context, ScriptedDocker(), execute=execute, verify_run=lambda _run_id: None)

    assert _tree_digest(project_cache) == before


def test_removal_and_reconcile_never_follow_a_symlink_out_of_the_cache(tmp_path: Path) -> None:
    project_cache, platform_dir = _store(tmp_path)
    outside = tmp_path / "outside"
    for name in ("platform", "staging"):
        (outside / name / "keep").mkdir(parents=True)
        (outside / name / "keep" / "file").write_text("x")
    (platform_dir / "staging").symlink_to(outside / "staging")
    other = project_cache / "linux-amd64"
    other.symlink_to(outside / "platform")
    (project_cache / "linux-amd64.lock").touch(mode=0o600)

    _reconcile(project_cache)
    removed = store.remove_platform(project_cache, "linux-amd64")

    assert (outside / "staging" / "keep" / "file").exists() and (outside / "platform" / "keep" / "file").exists()
    assert removed == 0


def test_post_rename_deletion_failure_still_reports_the_publication(tmp_path: Path) -> None:
    project_cache, platform_dir = _store(tmp_path)
    marked = _generation(platform_dir, _key("big"), 1, sizes=(5 * GIB // 2,))
    staging = _staging(platform_dir, sizes=(2 * GIB,))
    marked.parent.chmod(0o500)  # the marked generation cannot be deleted
    try:
        result = _publish(project_cache, _key("k"), staging)
    finally:
        marked.parent.chmod(0o700)

    assert not str(result["published"]).startswith("not-published") and isinstance(result["bytes"], int)
    _reconcile(project_cache)
    assert not marked.exists()


def test_inventory_and_removal_never_follow_a_symlinked_cache_root_or_staging(tmp_path: Path) -> None:
    registry, root, _project_id, project_cache = _registered_cache(tmp_path)
    outside = tmp_path / "outside"
    (outside / "keep").mkdir(parents=True)
    (outside / "keep" / "file").write_text("x")
    (project_cache / PLATFORM / "staging").symlink_to(outside)
    listed = store.remove_caches(registry, {"project_root": str(root)}, execute=False)
    assert listed["projects"][0]["platforms"][PLATFORM]["staging"] == 0

    other_registry = tmp_path / "other" / "registry"
    other_registry.parent.mkdir()
    store.cache_root(other_registry).symlink_to(store.cache_root(registry))
    for execute in (False, True):
        result = store.remove_caches(other_registry, {"project_root": str(root)}, execute=execute)
        assert result.get("projects", result.get("results")) == []
    assert (project_cache / PLATFORM / "generations").is_dir() and (outside / "keep" / "file").exists()


def test_only_platform_shaped_names_are_inventoried_or_removed(tmp_path: Path) -> None:
    registry, root, _project_id, project_cache = _registered_cache(tmp_path)
    (project_cache / ".lock").touch(mode=0o600)
    (project_cache / "notes").mkdir(mode=0o700)
    (project_cache / "notes" / "keep").write_text("x")

    listed = store.remove_caches(registry, {"project_root": str(root)}, execute=False)
    removed = store.remove_caches(registry, {"project_root": str(root)}, execute=True)

    assert set(listed["projects"][0]["platforms"]) == {PLATFORM}
    assert set(removed["results"][0]["platforms"]) == {PLATFORM}
    assert (project_cache / "notes" / "keep").exists()


def test_a_symlinked_project_directory_is_never_an_orphan(tmp_path: Path) -> None:
    registry = tmp_path / "registry"
    store.require_safe_directory(store.cache_root(registry))
    outside = tmp_path / "outside-project"
    (outside / PLATFORM).mkdir(parents=True)
    (store.cache_root(registry) / ("e" * 64)).symlink_to(outside)

    assert store.orphans(registry) == []
