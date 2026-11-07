"""Real-BuildKit proof of the project build cache through the private shim.

Every run builds the fixture project below on the run's own builder in a private account domain under ``tmp_path``.
Nonces written by ``RUN`` steps tell a reused layer (same nonce) from a rebuilt one (new nonce); they are read from
``image save`` archives through the shim.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import signal
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

import pytest
from tests.e2e.test_archive_export import effective_files

from docker_test_toolkit import ArtifactIdentity, PhaseBudget, RunSpec
from docker_test_toolkit.application import build_cache as store
from docker_test_toolkit.application import retention
from docker_test_toolkit.application.lifecycle import NativeClock
from docker_test_toolkit.domain import build_cache
from docker_test_toolkit.domain.errors import ResourceBusy
from docker_test_toolkit.factories.testing import build_test_services

pytestmark = pytest.mark.e2e

REDIS = "redis@sha256:becdda6c7f4b3fb42e42fd7f120bbf5c54c4caaaf16f26da24e4563d2c1f0576"
DOCKERFILE = f"""FROM {REDIS} AS deps
COPY lock.txt /lock.txt
RUN {{ head -c 16 /dev/urandom | od -An -tx1 | tr -d ' \\n'; echo; sha256sum /lock.txt; }} > /deps-nonce.txt
FROM {REDIS} AS sentinel
ARG SENTINEL
RUN echo "$SENTINEL" > /sentinel.txt && sha256sum /sentinel.txt > /sentinel.sha
FROM deps AS source
ARG FLAVOR=base
COPY --from=sentinel /sentinel.sha /sentinel.sha
COPY src.txt /src.txt
RUN {{ head -c 16 /dev/urandom | od -An -tx1 | tr -d ' \\n'; echo "$FLAVOR"; sha256sum /src.txt; }} > /source-nonce.txt
"""
NONCES = {"deps-nonce.txt", "source-nonce.txt"}
ARTIFACT = ArtifactIdentity(
    "0.1.0",
    "a" * 64,
    {"activation": 1, "batch": 1, "cli": 1, "distribution": 1, "queue": 1, "receipt": 1, "registry": 2, "run": 1},
)


def _project(tmp_path: Path, name: str) -> Path:
    project = tmp_path / name
    project.mkdir()
    (project / "lock.txt").write_text("lock-1\n")
    (project / "src.txt").write_text("src-1\n")
    (project / "Dockerfile").write_text(DOCKERFILE)
    return project


def _spec(project: Path) -> RunSpec:
    return RunSpec(
        project_root=project,
        entrypoint="e2e",
        purpose="build-cache",
        services=("fixture",),
        closure=("fixture",),
        profile="fixture",
        phases={"tests": PhaseBudget(2 * 1024**3, 128 * 1024**2, 2, 1)},
        initial_phase="tests",
        artifact=ARTIFACT,
        wait_seconds=120,
    )


@dataclass
class Built:
    """What one finished run left behind: its build receipt's cache object, its nonces and its final record."""

    cache: dict
    nonces: dict[str, bytes]
    record: dict
    registry: Path
    stderr: str


class CacheRun:
    """One entered run in the shared private account domain ``state``."""

    def __init__(self, state: Path, project: Path) -> None:
        self.project = project
        self.handle = build_test_services(state).open_run(_spec(project), admit_docker=True).enter()
        self.registry = self.handle._context.project.state
        assert self.shim("buildx", "create").returncode == 0

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

    def record(self) -> dict:
        return json.loads(self.handle._context.record.read_text())

    def builds(self) -> list[dict]:
        receipts = [item for item in self.record()["docker_receipts"].values() if item["kind"] == "build"]
        return sorted(receipts, key=lambda item: item["deadline"]["created_ns"])

    def image(self, tag: str) -> str:
        if ":" not in tag.rsplit("/", 1)[-1]:
            tag += ":latest"  # Docker records a tag without a version as ``:latest``
        (image_id,) = [
            key
            for key, item in self.record()["owned_resources"]["images"].items()
            if item["state"] == "present" and tag in item["evidence"]["repo_tags"]
        ]
        return image_id

    def nonces(self, image_id: str) -> dict[str, bytes]:
        saved = self.shim("image", "save", image_id)
        assert saved.returncode == 0, saved.stderr
        return effective_files(Path(json.loads(saved.stdout)["path"]), NONCES)

    def leave(self, outcome: str = "passed") -> dict:
        assert self.handle.leave(outcome).cleanup_outcome == "complete"
        return self.record()


def _platform_dir(registry: Path, record: dict, receipt: dict) -> Path:
    return store.project_cache(registry, record["project_id"]) / store.platform_name(receipt["plan"]["daemon_platform"])


def _assert_exact_cleanup(registry: Path, record: dict) -> None:
    """Every owned resource removed, every receipt terminal, nothing points into the cache, no staging left."""
    assert all(
        item["state"] == "removed" for resources in record["owned_resources"].values() for item in resources.values()
    )
    assert all(item["state"] not in {"pending", "unresolved"} for item in record["docker_receipts"].values())
    cache_root = str(store.cache_root(registry))
    assert cache_root not in json.dumps(record["owned_resources"]) and cache_root not in json.dumps(
        [item["resources"] for item in record["docker_receipts"].values() if "resources" in item]
    )
    for staging in store.cache_root(registry).glob("*/*/staging"):
        assert not any(staging.iterdir()), staging


def _cycle(state: Path, project: Path, sentinel: str, *extra: str, tag: str | None = None) -> Built:
    """Open a run, build once (cached by default), read the nonces, leave and check the exact cleanup."""
    run = CacheRun(state, project)
    try:
        tag = tag or f"cache-e2e:{run.handle.run_id}"
        if "{compose}" in tag:
            tag = tag.replace("{compose}", run.handle.docker_context.compose_project)
        built = run.shim("buildx", "build", "--build-arg", f"SENTINEL={sentinel}", *extra, "--tag", tag, ".")
        assert built.returncode == 0, built.stderr
        nonces = run.nonces(run.image(tag))
    finally:
        record = run.leave()
    _assert_exact_cleanup(run.registry, record)
    (receipt,) = [item for item in record["docker_receipts"].values() if item["kind"] == "build"]
    return Built({**receipt["plan"]["cache"], "_receipt": receipt}, nonces, record, run.registry, built.stderr)


def _outcome(built: Built) -> str:
    return built.cache["outcome"]


def test_identity_and_false_clean_cases(tmp_path: Path, redis_image: None) -> None:
    state, sentinel = tmp_path / "state", str(uuid.uuid4())
    project = _project(tmp_path, "p")

    cold = _cycle(state, project, sentinel)  # 1
    platform_dir = _platform_dir(cold.registry, cold.record, cold.cache["_receipt"])
    assert _outcome(cold) == "miss", cold.stderr
    assert [item.valid for item in store.list_generations(platform_dir)] == [True]  # native platform dir

    unchanged = _cycle(state, project, sentinel)  # 2 (X1)
    assert _outcome(unchanged) == "hit" and unchanged.nonces == cold.nonces

    (project / "lock.txt").write_text("lock-2\n")  # 3 (X4)
    lock = _cycle(state, project, sentinel)
    assert _outcome(lock) == "miss" and lock.nonces["deps-nonce.txt"] != unchanged.nonces["deps-nonce.txt"]
    assert hashlib.sha256(b"lock-2\n").hexdigest().encode() in lock.nonces["deps-nonce.txt"]

    (project / "src.txt").write_text("src-2\n")  # 4
    source = _cycle(state, project, sentinel)
    assert _outcome(source) == "miss"
    assert source.nonces["deps-nonce.txt"] == lock.nonces["deps-nonce.txt"]
    assert source.nonces["source-nonce.txt"] != lock.nonces["source-nonce.txt"]

    dockerfile = DOCKERFILE.replace("COPY lock.txt /lock.txt\n", "COPY lock.txt /lock.txt\nENV CHANGED=1\n", 1)
    (project / "Dockerfile").write_text(dockerfile)  # 5
    instruction = _cycle(state, project, sentinel)
    assert _outcome(instruction) == "miss"
    assert instruction.nonces["deps-nonce.txt"] != source.nonces["deps-nonce.txt"]

    again = _cycle(state, project, sentinel)  # 6 (X2)
    assert _outcome(again) == "hit" and again.nonces["deps-nonce.txt"] == instruction.nonces["deps-nonce.txt"]

    other = _project(tmp_path, "q")  # 7 (X5)
    for name in ("lock.txt", "src.txt", "Dockerfile"):
        (other / name).write_bytes((project / name).read_bytes())
    isolated = _cycle(state, other, sentinel)
    assert _outcome(isolated) == "miss" and isolated.nonces["deps-nonce.txt"] != again.nonces["deps-nonce.txt"]

    scoped = _cycle(state, project, sentinel, tag="cache-e2e-test-{compose}")  # 8 (X3)
    assert _outcome(scoped) == "hit"

    flavored = _cycle(state, project, sentinel, "--build-arg", "FLAVOR=other")  # 10 (X17, DF18)
    assert flavored.nonces["deps-nonce.txt"] == scoped.nonces["deps-nonce.txt"]


def test_digest_corrupt_generation_degrades_cold(tmp_path: Path, redis_image: None) -> None:
    state, sentinel = tmp_path / "state", str(uuid.uuid4())
    project = _project(tmp_path, "p")
    cold = _cycle(state, project, sentinel)
    platform_dir = _platform_dir(cold.registry, cold.record, cold.cache["_receipt"])
    (generation,) = store.importable_generations(platform_dir)
    for layer in store.validate_generation(generation):
        blob = generation / "blobs" / "sha256" / layer.removeprefix("sha256:")
        data = bytearray(blob.read_bytes())
        data[-1] ^= 0xFF  # one byte changed, size kept
        blob.chmod(0o600)
        blob.write_bytes(bytes(data))

    rebuilt = _cycle(state, project, sentinel)

    assert generation.name not in rebuilt.cache["imports"]
    assert rebuilt.nonces["deps-nonce.txt"] != cold.nonces["deps-nonce.txt"]
    assert not generation.exists()  # the key's next publication superseded it


def _layer_contents(blob: Path) -> bytes:
    data = blob.read_bytes()
    try:
        return gzip.decompress(data)
    except (OSError, EOFError):
        return data


def test_published_generations_carry_no_private_material(tmp_path: Path, redis_image: None) -> None:
    state, sentinel = tmp_path / "state", str(uuid.uuid4())
    project = _project(tmp_path, "p")
    run = CacheRun(state, project)
    try:
        built = run.shim("buildx", "build", "--build-arg", f"SENTINEL={sentinel}", "--tag", "cache-e2e:private", ".")
        assert built.returncode == 0, built.stderr
        record = run.record()
        authority = record["docker_authority"]
        secrets = [
            record["private_root"].encode(),
            authority["descriptor_sha256"].encode(),
            str(run.handle.docker_context.executable).encode(),
        ]
        identities = [record["run_id"].encode(), record["session_id"].encode()]
    finally:
        record = run.leave()
    (receipt,) = [item for item in record["docker_receipts"].values() if item["kind"] == "build"]
    platform_dir = _platform_dir(run.registry, record, receipt)

    found_sentinel = False
    for generation in store.importable_generations(platform_dir):
        layers = {layer.removeprefix("sha256:") for layer in store.validate_generation(generation)}
        for blob in (generation / "blobs" / "sha256").iterdir():
            content = _layer_contents(blob) if blob.name in layers else blob.read_bytes()
            for secret in secrets:
                assert secret not in content, (blob, secret)
            if blob.name in layers:  # LABELS_IN_GENERATIONS: run labels may appear in cache metadata, never in layers
                for identity in identities:
                    assert identity not in content, (blob, identity)
            found_sentinel = found_sentinel or sentinel.encode() in content
    assert found_sentinel  # positive control: the mode=max export holds the intermediate stage (X7)


def test_ineligible_builds_publish_nothing(tmp_path: Path, redis_image: None) -> None:
    state, sentinel = tmp_path / "state", str(uuid.uuid4())
    project = _project(tmp_path, "p")
    secret = tmp_path / "secret.txt"
    secret.write_text("s3cr3t\n")
    run = CacheRun(state, project)
    try:
        private = Path(run.record()["private_root"])
        owned = private / f"python-{uuid.uuid4()}-docker"
        owned.mkdir(mode=0o700)
        generation_two = private / "generation-2"
        generation_two.mkdir(mode=0o700)
        for directory in (owned, generation_two):
            for name in ("lock.txt", "src.txt", "Dockerfile"):
                (directory / name).write_bytes((project / name).read_bytes())
        # One sentinel per build: an identical image under a second tag in one run cannot promote (follow-up F-1).
        cases = [
            (("--secret", f"id=s,src={secret}", "--tag", "cache-e2e:secret", "."), "build-secret"),
            (("--build-context", f"w={owned}", "--tag", "cache-e2e:named", "."), "private-root-context"),
            (("--tag", "cache-e2e:positional", str(generation_two)), "private-root-context"),
            (
                ("--file", str(project / "Dockerfile"), "--tag", "cache-e2e:root", str(private)),
                "private-root-context",
            ),
        ]
        for index, (argv, reason) in enumerate(cases):
            # a context outside the project may fail the build: the verdict stays on the receipt
            run.shim("buildx", "build", "--build-arg", f"SENTINEL={sentinel}-{index}", *argv)
            cache = run.builds()[-1]["plan"]["cache"]
            assert cache["outcome"] == f"not-eligible:{reason}", cache
    finally:
        record = run.leave()
    assert not list(store.project_cache(run.registry, record["project_id"]).glob("*/generations/*/*"))


def _buildx_child(shim: subprocess.Popen, deadline: float) -> int:
    pid = shim.pid
    while time.monotonic() < deadline:
        if shim.poll() is not None:
            stderr = shim.communicate()[1][-3000:]
            raise AssertionError(f"the build ended before its Buildx client was seen: {stderr!r}")
        children = subprocess.run(["pgrep", "-P", str(pid)], capture_output=True, text=True).stdout.split()
        for child in children:
            command = subprocess.run(["ps", "-o", "command=", "-p", child], capture_output=True, text=True).stdout
            if "buildx build" in command:
                return int(child)
        time.sleep(0.05)
    raise AssertionError("the dispatcher never started its Buildx client")


def test_buildx_client_killed_during_a_cached_build_recovers_exactly(tmp_path: Path, redis_image: None) -> None:
    state, sentinel = tmp_path / "state", str(uuid.uuid4())
    project = _project(tmp_path, "p")
    run = CacheRun(state, project)
    try:
        context = run.handle.docker_context
        shim = subprocess.Popen(
            [
                str(context.executable),
                "buildx",
                "build",
                "--build-arg",
                f"SENTINEL={sentinel}",
                "--tag",
                f"k:{run.handle.run_id}",
                ".",
            ],
            cwd=project,
            env=dict(context.environment),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        os.kill(_buildx_child(shim, time.monotonic() + 120), signal.SIGKILL)
        shim.communicate(timeout=120)
        assert shim.returncode != 0
        receipt = run.builds()[-1]
        assert receipt["plan"]["cache"]["outcome"] == "skipped:not-promoted"
        platform_dir = _platform_dir(run.registry, run.record(), receipt)
        assert not any((platform_dir / "staging").iterdir())
        with store.platform_anchor(platform_dir.parent, platform_dir.name, shared=False, nonblocking=True):
            pass
    except ResourceBusy as error:
        raise AssertionError("the anchor stayed held after the dispatcher's failure path") from error
    finally:
        record = run.leave("failed")
    _assert_exact_cleanup(run.registry, record)
    leaked = subprocess.run(["docker", "image", "inspect", f"k:{run.handle.run_id}"], capture_output=True, check=False)
    assert leaked.returncode != 0  # an image the killed client still loaded was owned and removed (no leak)


CONSUMER = r"""import json, subprocess, sys
import docker_test_toolkit as toolkit
context = toolkit.current_consumer_run().docker_context
sentinel = "SENTINEL=consumer-" + sys.argv[1]
argv = [str(context.executable), "buildx", "build", "--build-arg", sentinel, "--tag", "c:1", "."]
built = subprocess.run(argv, env=dict(context.environment), capture_output=True, text=True)
print(json.dumps({"returncode": built.returncode, "stderr": built.stderr[-2000:]}))
"""

SWITCHED = r"""import json, subprocess, sys
from pathlib import Path
from docker_test_toolkit import ArtifactIdentity, PhaseBudget, RunSpec
from docker_test_toolkit.factories.testing import build_test_services
state, project, sentinel, protocols = Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3], json.loads(sys.argv[4])
spec = RunSpec(project_root=project, entrypoint="e2e", purpose="build-cache-switch", services=("fixture",),
               closure=("fixture",), profile="fixture", phases={"tests": PhaseBudget(2 * 1024**3, 128 * 1024**2, 2, 1)},
               initial_phase="tests", artifact=ArtifactIdentity("0.1.0", "a" * 64, protocols), wait_seconds=120)
handle = build_test_services(state).open_run(spec, admit_docker=True).enter()
context = handle.docker_context
def shim(*argv):
    return subprocess.run([str(context.executable), *argv], cwd=project, env=dict(context.environment),
                          capture_output=True, text=True)
try:
    assert shim("buildx", "create").returncode == 0
    facade = shim("buildx", "build", "--build-arg", "SENTINEL=" + sentinel, "--tag", "cache-e2e:facade", ".")
    child, _ = handle.launch_python_consumer("script", str(project / "consumer.py"), (sentinel,), ("fixture",),
                                             cwd=project, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                             borrow_docker=True)
    out, err = (stream.decode(errors="replace") for stream in child.communicate(timeout=1200))
    record = json.loads(handle._context.record.read_text())
    caches = [item["plan"]["cache"] for item in sorted(record["docker_receipts"].values(),
              key=lambda item: item["deadline"]["created_ns"]) if item["kind"] == "build"]
    print(json.dumps({"facade": facade.returncode, "consumer": json.loads(out or "{}"), "consumer_err": err[-2000:],
                      "caches": caches, "project_id": record["project_id"],
                      "registry": str(handle._context.project.state)}))
finally:
    handle.leave("passed")
"""


def test_off_switch_through_the_facade_and_a_borrowed_consumer(tmp_path: Path, redis_image: None) -> None:
    state, sentinel = tmp_path / "state", str(uuid.uuid4())
    project = _project(tmp_path, "p")
    (project / "consumer.py").write_text(CONSUMER)
    environment = {**os.environ, build_cache.SWITCH_ENV: "off"}

    completed = subprocess.run(
        [sys.executable, "-c", SWITCHED, str(state), str(project), sentinel, json.dumps(dict(ARTIFACT.protocols))],
        env=environment,
        capture_output=True,
        text=True,
        timeout=2400,
    )

    assert completed.returncode == 0, completed.stderr[-4000:]
    observed = json.loads(completed.stdout.strip().splitlines()[-1])
    assert observed["facade"] == 0 and observed["consumer"]["returncode"] == 0, observed
    assert [cache["outcome"] for cache in observed["caches"]] == ["skipped:disabled", "skipped:disabled"]
    project_cache = store.project_cache(Path(observed["registry"]), observed["project_id"])
    assert not list(project_cache.glob("*/generations/*/*"))


def test_an_identical_rebuild_under_a_second_tag_promotes_and_cleans_up(tmp_path: Path, redis_image: None) -> None:
    state, sentinel = tmp_path / "state", str(uuid.uuid4())
    project = _project(tmp_path, "p")
    run = CacheRun(state, project)
    try:
        for tag in ("cache-e2e:first", "cache-e2e:second"):
            built = run.shim("buildx", "build", "--build-arg", f"SENTINEL={sentinel}", "--tag", tag, ".")
            assert built.returncode == 0, built.stderr
        assert [receipt["state"] for receipt in run.builds()] == ["promoted", "promoted"]
        (image,) = [item for item in run.record()["owned_resources"]["images"].values() if item["state"] == "present"]
        assert {"cache-e2e:first", "cache-e2e:second"} <= set(image["evidence"]["repo_tags"])
    finally:
        record = run.leave()
    _assert_exact_cleanup(run.registry, record)


def test_a_retention_pass_after_a_build_cache_run_keeps_the_newest_generation(
    tmp_path: Path, redis_image: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    state, project = tmp_path / "state", _project(tmp_path, "p")
    _cycle(state, project, str(uuid.uuid4()))
    sentinel = str(uuid.uuid4())  # another build argument, so another build key: a second generation
    newer = _cycle(state, project, sentinel)
    platform_dir = _platform_dir(newer.registry, newer.record, newer.cache["_receipt"])
    generations = sorted(store.list_generations(platform_dir), key=lambda item: item.wall_ns)
    assert [item.valid for item in generations] == [True, True]
    monkeypatch.setattr(retention, "cache_cap_bytes", lambda _total: generations[-1].bytes)  # the real floor is 4 GiB
    account = tmp_path / "account"
    account.mkdir(mode=0o700)

    report = retention.run_pass(
        account=account, registry=newer.registry, trigger="test", docker=None, clock=NativeClock(), force=True
    )

    assert report is not None and report.kinds["build_cache"]["removed_count"] == 1
    assert [item.name for item in store.list_generations(platform_dir)] == [generations[-1].name]
    again = _cycle(state, project, sentinel)  # the kept generation still imports
    assert _outcome(again) == "hit" and again.nonces == newer.nonces
