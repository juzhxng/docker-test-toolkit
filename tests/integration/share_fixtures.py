"""Shared harness for the share-rule and image-lock integration suites: a private account at limit 2 whose every
Docker path reaches one in-process scripted Docker (``FakeDockerPort``)."""

from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from docker_test_toolkit import ArtifactIdentity, ExternalDockerResource, PhaseBudget, RunHandle, RunSpec
from docker_test_toolkit.application import lifecycle, sharing
from docker_test_toolkit.application.recovery import atomic_json
from docker_test_toolkit.domain.share_policy import SHARE_RULES_VERSION
from docker_test_toolkit.factories.cli import bind_docker_adapter, build_docker_adapter
from docker_test_toolkit.factories.testing import _TestServices, build_test_services
from tests.unit.test_docker_receipt_service import IMAGE_ID, FakeDockerPort

GIB = 1024**3
NETWORK_ID = "e" * 64
ARTIFACT = ArtifactIdentity(
    "0.1.0",
    "a" * 64,
    {"activation": 1, "batch": 1, "cli": 1, "distribution": 1, "queue": 1, "receipt": 1, "registry": 2, "run": 1},
)


def _spec(
    project: Path,
    *,
    connect: bool = False,
    phases: dict[str, PhaseBudget] | None = None,
    declare_image: bool = True,
    retain: bool = False,
) -> RunSpec:
    external = [ExternalDockerResource("image", IMAGE_ID, "fixture", "use_image")] if declare_image else []
    if connect:
        external.append(ExternalDockerResource("network", NETWORK_ID, "external", "connect_owned_container"))
    return RunSpec(
        project_root=project,
        entrypoint="integration",
        purpose="share-enforcement",
        services=("fixture",),
        closure=("fixture",),
        profile="fixture",
        phases=phases or {"tests": PhaseBudget(2 * GIB, 1, 2, 1)},
        initial_phase="tests",
        artifact=ARTIFACT,
        external_resources=tuple(external),
        retain=retain,
        wait_seconds=20,
    )


@dataclass
class _World:
    services: _TestServices
    project: Path
    fake: FakeDockerPort

    def open(self, *, shared: bool, **spec_options) -> RunHandle:
        """Open and enter one run: shared after a positive verdict for its key under limit 2, else exclusive."""
        spec = _spec(self.project, **spec_options)
        handle = self.services.open_run(spec, docker_factory=lambda: self.fake)
        if shared:
            self.write(handle, True, origin="seed", tick_ns=0)
        handle = handle.enter()
        assert ("sharing" in _record(handle)) is (shared and not spec_options.get("connect"))
        return handle

    def write(self, handle: RunHandle, shareable: bool, *, origin: str, tick_ns: int, reason: str | None = None):
        path = sharing.verdict_path(self.registry, handle._context.project.project_id)
        path.parent.mkdir(mode=0o700, exist_ok=True)
        verdicts = _store(self, handle).get("verdicts", {}) if path.exists() else {}
        verdicts[_key(handle)] = {
            "shareable": shareable,
            "reason": reason,
            "run_id": handle.run_id,
            "origin": origin,
            "tick_ns": tick_ns,
        }
        atomic_json(path, {"schema": 1, "rules": SHARE_RULES_VERSION, "verdicts": verdicts})

    @property
    def registry(self) -> Path:
        return self.services.root / "registry"


@contextmanager
def shared_world(tmp_path: Path) -> Iterator[_World]:
    """A private account at limit 2 whose every Docker path (dispatch, transition, restart) reaches one fake."""
    project = tmp_path / "project"
    (project / ".git").mkdir(parents=True)  # verdicts are keyed by the Git origin
    (project / ".git" / "config").write_text('[remote "origin"]\n\turl = https://example.invalid/shared/world.git\n')
    services = build_test_services(tmp_path / "state")
    sharing.record_parallel_runs(services.root / "admission", 2)
    fake = FakeDockerPort()
    lifecycle.configure_docker_factories(lambda: fake, lambda _identity: fake)
    try:
        yield _World(services, project, fake)
    finally:
        lifecycle.configure_docker_factories(build_docker_adapter, bind_docker_adapter)


def _record(handle: RunHandle) -> dict:
    return json.loads(handle._context.record.read_text())


def _key(handle: RunHandle) -> str:
    spec = handle._spec
    origin = sharing.repository_origin(spec.project_root)
    assert origin is not None, "shared_world projects carry a Git origin"
    return sharing.request_key(origin, spec.entrypoint, spec.profile, spec.closure)


def _store(world: _World, handle: RunHandle) -> dict:
    path = sharing.verdict_path(world.registry, handle._context.project.project_id)
    return json.loads(path.read_text()) if path.exists() else {}


def _verdict(world: _World, handle: RunHandle) -> dict | None:
    return _store(world, handle).get("verdicts", {}).get(_key(handle))


def _dispatch(world: _World, handle: RunHandle, *argv: str):
    _ = handle.docker_context
    descriptor = json.loads((handle.private_root / "docker-authority.json").read_text())
    return lifecycle.dispatch_docker(handle._context, descriptor, world.fake, argv)


def _receipts(handle: RunHandle) -> list[dict]:
    return list(_record(handle)["docker_receipts"].values())


def _leave(world: _World, handle: RunHandle) -> None:
    """Remove what the run's dispatches left (the guardian process cannot reach the fake daemon), then leave."""
    owned = _record(handle)["owned_resources"]
    for kind in ("containers", "networks", "images", "builders"):
        for identifier, item in owned[kind].items():
            if item["state"] != "removed":
                _dispatch(world, handle, *(("buildx", "rm") if kind == "builders" else (kind[:-1], "rm")), identifier)
    handle.leave()
