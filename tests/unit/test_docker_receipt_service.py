from __future__ import annotations

import copy
import hashlib
import json
import re
import subprocess
import sys
from collections.abc import Callable
from dataclasses import asdict
from pathlib import Path

import pytest

from docker_test_toolkit import ArtifactIdentity, ExternalDockerResource, PhaseBudget, RunSpec
from docker_test_toolkit.application import lifecycle
from docker_test_toolkit.application.docker_commands import ClassifiedDockerCommand
from docker_test_toolkit.application.ports.docker import DockerPort
from docker_test_toolkit.domain.errors import (
    IncompleteCleanup,
    InfrastructureFailure,
    InvalidInput,
    ResourceBusy,
    ResourceError,
)
from docker_test_toolkit.domain.models import (
    DaemonIdentity,
    DockerObservation,
    DockerResult,
    ExactInspection,
    PodmanBinding,
    ProbeRun,
)
from docker_test_toolkit.factories.testing import build_test_services, install_failpoint
from tests.integration.account_fixtures import (
    abandon_run,
    current_identity,
    dead_identity,
    kill_guardian,
    pause_guardian,
    prewrite_withdrawal,
    resume_guardian,
    retire_actor,
)
from tests.unit.test_failure_evidence import WRITER_ROWS

IMAGE_ID = "sha256:" + "f" * 64
BUILT_IMAGE_ID = "sha256:" + "9" * 64


def write_cache_layout(path: Path, layers: dict[str, bytes]) -> None:
    """Write a structurally valid index-form BuildKit cache export of ``layers`` into ``path`` (created if absent)."""
    blobs = path / "blobs" / "sha256"
    blobs.mkdir(parents=True, exist_ok=True, mode=0o700)

    def blob(data: bytes, media_type: str) -> dict:
        digest = hashlib.sha256(data).hexdigest()
        (blobs / digest).write_bytes(data)
        return {"mediaType": media_type, "digest": f"sha256:{digest}", "size": len(data)}

    config = blob(b'{"layers":[]}', "application/vnd.buildkit.cacheconfig.v0")
    descriptors = [blob(data, "application/vnd.oci.image.layer.v1.tar+gzip") for data in layers.values()]
    manifest = blob(
        json.dumps({"schemaVersion": 2, "manifests": [config, *descriptors]}).encode(),
        "application/vnd.oci.image.index.v1+json",
    )
    annotations = {"org.opencontainers.image.ref.name": "latest"}
    (path / "index.json").write_text(
        json.dumps({"schemaVersion": 2, "manifests": [{**manifest, "annotations": annotations}]})
    )
    (path / "oci-layout").write_text('{"imageLayoutVersion":"1.0.0"}')


class FakeDockerPort(DockerPort):
    """Exercise application receipt behavior without replacing its public seams."""

    engine = "docker"  # class attributes: the port declares these read-only
    binding: PodmanBinding | None = None

    def __init__(self) -> None:
        self.env: dict[str, str] = {}
        self.prefix = (sys.executable, "-c", "raise SystemExit(1)")
        """A transport that always fails: an unbound Engine read (receipt platform facts) fails open."""
        client = Path(sys.executable).resolve()
        observed = client.stat()
        self.identity = DaemonIdentity(
            "11111111-1111-1111-1111-111111111111",
            client,
            hashlib.sha256(client.read_bytes()).hexdigest(),
            "fixture",
            observed.st_dev,
            observed.st_ino,
            observed.st_size,
            observed.st_mtime_ns,
            None,
        )
        self.containers: dict[str, dict] = {}
        self.networks: dict[str, dict] = {
            "e" * 64: {
                "Id": "e" * 64,
                "Name": "external",
                "Created": "external-created",
                "Labels": {},
            }
        }
        self.volumes: dict[str, dict] = {}
        self.builders: dict[str, dict] = {}
        external_image = {
            "Id": IMAGE_ID,
            "Created": "external-created",
            "Config": {"Labels": {}},
            "RepoDigests": ["example/cache@sha256:" + "1" * 64],
        }
        self.images: dict[str, dict] = {
            IMAGE_ID: external_image,
            "fixture": external_image,
            "example/cache@sha256:" + "1" * 64: external_image,
        }
        self.commands: list[tuple[str, ...]] = []
        self.timeouts: list[float] = []
        self.streamed_commands: list[tuple[str, ...]] = []
        self.compose_generation = 0
        self.compose_config_variant = False
        self.compose_anonymous_volume = False
        self.compose_image_volume = False
        self.compose_runtime_image_id = IMAGE_ID
        self.compose_ports: dict[str, list[dict[str, str]] | None] = {}
        self._stopped_ports: dict[str, dict] = {}
        self.in_use: set[str] = set()  # networks Docker refuses to remove because something still uses them
        self.compose_services = ("fixture",)
        self.compose_run_retained = False
        self.compose_run_exit_code = 0
        self.fail_after_image_remove = False
        self.fail_after_builder_remove = False
        self.fail_before_remove_kind: str | None = None
        self.keep_builder_after_remove_once = False
        self.fail_image_inspection = False
        self.leave_buildkit_after_builder_remove = False
        self.stop_buildkit_after_builder_remove = False
        """Native Linux: ``buildx rm`` stops the BuildKit worker and the daemon removes it only later."""
        self.build_metadata_mode: int | None = None
        self.build_extra_reference: tuple[str, str] | None = None
        self.build_metadata: dict | None = None  # the build's metadata file; Docker's image-store form when None
        self.builder_limits_valid = True
        self.builder_worker_created = True
        self.builder_host_overrides: dict[str, object] = {}
        self.omit_build_created = False
        self.default_image_platform = "linux/arm64"
        """The ``os/architecture`` a built image reports unless the build asked for ``--platform``."""
        self.fail_build = False
        self.interrupt_build = False
        """When set, the next ``buildx build`` raises after the child started (an interrupted dispatcher's receipt)."""
        self.build_reports_platform: str | None = None
        """When set, the built image reports this ``os/architecture[/variant]`` whatever the build asked for."""
        self.cache_layers: dict[str, bytes] | None = None
        """When set, a build carrying ``--cache-to type=local,dest=…`` exports these layers there (index form)."""
        self.execute_error: Exception | None = None
        """When set, a ``buildx build`` raises it after the child started."""
        self.fail_build_after_load = False
        self.fail_image_inspection_after_build = False
        self.build_foreign_labels = False
        """When set, a build loads its image and tags, then exits 137 without metadata (a client killed late)."""
        self.on_build: Callable[[tuple[str, ...]], None] | None = None
        """When set, called with the executed build argv while the build runs."""
        self.omit_pull_created = False
        self.pull_client_dies: str | None = None
        """``killed``: a pull registers its image, then its client exits 137; ``interrupted``: the dispatcher sees the
        client's transport break instead (no recorded outcome); ``lost``: the transport breaks before any image."""
        self.hardened = False
        self.compose_service_extra: dict[str, object] = {}
        self.next_container_mutation: Callable[[dict], None] | None = None
        self.on_compose_config: Callable[[], None] | None = None
        self.deadlines: list[tuple[str, object]] = []
        self.held_fds: list[int | None] = []
        """The ``hold_fd`` every ``execute`` received, in call order (the image lock passed to a Docker child)."""
        """Every ``(read, deadline)`` the observation pass handed to this port, in call order."""

    def objects(self, kind: str, *, deadline: object = None) -> list[dict]:
        """Return deep copies of every fake object of one kind, as the Engine inventory does."""
        self.deadlines.append((f"objects:{kind}", deadline))
        stores = {"container": self.containers, "network": self.networks, "volume": self.volumes}
        return [json.loads(json.dumps(item)) for item in stores[kind].values()]

    def _store(self, container: dict, *, network: str = "none") -> None:
        """Record a created container, optionally hardened to the capacity-two rows, then apply the pending mutation."""
        if self.hardened:
            _harden(container, network)
        self._mutate(container)
        self.containers[str(container["Id"])] = container

    def stop_container(self, container_id: str) -> None:
        """Stop a container as Docker does: an exited container reports an empty port map (moby daemon/inspect.go)."""
        container = self.containers[container_id]
        container["State"]["Running"] = False
        self._stopped_ports.setdefault(container_id, container["NetworkSettings"]["Ports"])
        container["NetworkSettings"]["Ports"] = {}

    def _start(self, container_id: str) -> None:
        container = self.containers[container_id]
        container["State"]["Running"] = True
        if container_id in self._stopped_ports:
            container["NetworkSettings"]["Ports"] = self._stopped_ports.pop(container_id)

    def _mutate(self, container: dict) -> None:
        mutation, self.next_container_mutation = self.next_container_mutation, None
        if mutation is not None:
            mutation(container)

    def observe_readiness(self, deadline: object):
        """Reject readiness because receipt tests never cross that seam."""
        del deadline
        raise AssertionError("receipt tests must not inspect readiness")

    def observe_daemon(self, deadline: lifecycle.Deadline | None = None) -> DaemonIdentity:
        return self.identity

    def call(self, *argv: str, deadline: lifecycle.Deadline) -> str:
        if argv[:2] == ("info", "--format"):
            return self.identity.daemon_id
        result = self.execute(
            tuple(argv), timeout_seconds=max(0.001, deadline.remaining_ns(lifecycle.NativeClock()) / 1e9)
        )
        if result.exit_code:
            raise ResourceError(f"Synthetic Docker call failed: {argv!r}")
        return result.stdout.strip()

    def run_probe(self, argv: tuple[str, ...], *, deadline: object, container_name: str) -> ProbeRun:
        """A daemon probe reading a quiet VM: low pressure, one data-root row."""
        self.deadlines.append(("probe", deadline))
        stdout = (
            "some avg10=0.00 avg60=1.00 avg300=0.00 total=1\n::dtt::\n"
            "Filesystem 1024-blocks Used Available Capacity Mounted on\n/dev/vda1 100 40 60 40% /dtt-data\n"
        )
        return ProbeRun(stdout, 0, False)

    def inspect_exact(self, request: ExactInspection, *, deadline: object | None = None) -> DockerObservation:
        self.deadlines.append((f"inspect_exact:{request.kind}", deadline))
        if self.fail_image_inspection and request.kind == "image":
            raise ResourceError("synthetic image inspection failure")
        stores = {
            "container": self.containers,
            "network": self.networks,
            "volume": self.volumes,
            "image": self.images,
        }
        store = stores.get(request.kind, {})
        item = store.get(request.identifier)
        if item is None:
            item = next(
                (value for value in store.values() if str(value.get("Name", "")).lstrip("/") == request.identifier),
                None,
            )
        return DockerObservation(request.kind, request.identifier, item is not None, item or {})

    def execute(
        self,
        argv: tuple[str, ...],
        *,
        timeout_seconds: float,
        environment=None,
        stream_output: bool = False,
        hold_fd: int | None = None,
        relay_output: bool = False,
    ) -> DockerResult:
        assert 0 < timeout_seconds <= 14_400
        self.held_fds.append(hold_fd)
        self.timeouts.append(timeout_seconds)
        self.commands.append(argv)
        if stream_output:
            self.streamed_commands.append(argv)
        if argv[:2] == ("container", "inspect"):
            item = self.containers.get(argv[-1])
            return DockerResult(0, json.dumps([item]), "") if item else DockerResult(1, "", "missing")
        if argv[:2] == ("container", "logs"):
            return DockerResult(0, "logs\n", "")
        if argv[:2] == ("network", "inspect"):
            item = self.inspect_exact(ExactInspection("network", argv[-1]))
            return DockerResult(0, json.dumps([item.attributes]), "") if item.exists else DockerResult(1, "", "")
        if argv[:2] == ("volume", "inspect"):
            item = self.inspect_exact(ExactInspection("volume", argv[-1]))
            return DockerResult(0, json.dumps([item.attributes]), "") if item.exists else DockerResult(1, "", "")
        if argv[:2] == ("image", "inspect"):
            item = self.inspect_exact(ExactInspection("image", argv[-1]))
            return DockerResult(0, json.dumps([item.attributes]), "") if item.exists else DockerResult(1, "", "")
        if argv[:1] == ("inspect",):
            item = self.inspect_exact(ExactInspection("image", argv[-1]))
            return DockerResult(0, json.dumps([item.attributes]), "") if item.exists else DockerResult(1, "", "")
        if argv[0] == "run":
            container_id = next(char * 64 for char in "a0123456" if char * 64 not in self.containers)
            name = argv[argv.index("--name") + 1]
            labels = _labels(argv)
            Path(argv[argv.index("--cidfile") + 1]).write_text(container_id + "\n")
            self._store(_container(container_id, name, labels))
            return DockerResult(0, container_id + "\n", "")
        if argv[:2] == ("container", "rm"):
            if self.fail_before_remove_kind == "container":
                self.fail_before_remove_kind = None
                raise OSError("synthetic interruption before container removal")
            self.containers.pop(argv[-1], None)
            return DockerResult(0, argv[-1] + "\n", "")
        if argv[:2] in {("container", "start"), ("container", "stop")}:
            container = self.containers[argv[-1]]
            if argv[1] == "stop":
                self.stop_container(argv[-1])
                return DockerResult(0, argv[-1] + "\n", "")
            self._start(argv[-1])
            container["State"]["StartedAt"] = f"{container['State']['StartedAt']}-restarted"
            self._mutate(container)
            return DockerResult(0, argv[-1] + "\n", "")
        if argv[:2] == ("network", "connect"):
            assert argv[2] in self.networks and argv[3] in self.containers
            return DockerResult(0, "", "")
        if argv[:2] == ("network", "create"):
            network_id = "b" * 64
            self.networks[network_id] = {
                "Id": network_id,
                "Name": argv[-1],
                "Created": "network-created",
                "Labels": _labels(argv),
            }
            return DockerResult(0, network_id + "\n", "")
        if argv[:2] == ("network", "rm"):
            if self.fail_before_remove_kind == "network":
                self.fail_before_remove_kind = None
                raise OSError("synthetic interruption before network removal")
            if argv[-1] in self.in_use:
                return DockerResult(1, "", "network is in use\n")  # Docker refuses; the network stays
            self.networks.pop(argv[-1], None)
            return DockerResult(0, argv[-1] + "\n", "")
        if argv[:2] == ("volume", "create"):
            self.volumes[argv[-1]] = {
                "Name": argv[-1],
                "CreatedAt": "volume-created",
                "Driver": "local",
                "Labels": _labels(argv),
            }
            return DockerResult(0, argv[-1] + "\n", "")
        if argv[:2] == ("volume", "rm"):
            if self.fail_before_remove_kind == "volume":
                self.fail_before_remove_kind = None
                raise OSError("synthetic interruption before volume removal")
            self.volumes.pop(argv[-1], None)
            return DockerResult(0, argv[-1] + "\n", "")
        if argv[:2] == ("buildx", "create"):
            name = argv[argv.index("--name") + 1]
            self.builders[name] = {
                "Name": name,
                "Driver": "docker-container",
                "Nodes": [{"Name": f"{name}0", "Endpoint": "fixture"}],
            }
            if self.builder_worker_created:
                container_id = "8" * 64
                state = f"buildx_buildkit_{name}0_state"  # Buildx's worker mounts its state volume, as on Docker
                self.volumes[state] = {"Name": state, "CreatedAt": "2026-11-03T18:00:00Z", "Labels": {}}
                self.containers[container_id] = {
                    **_container(
                        container_id,
                        f"buildx_buildkit_{name}0",
                        {"com.docker.buildx.builder": name},
                        running=True,
                        host_config=self._builder_host_config(),
                    ),
                    "Mounts": [{"Type": "volume", "Name": state, "Destination": "/var/lib/buildkit"}],
                }
            return DockerResult(0, name + "\n", "")
        if argv[:2] == ("buildx", "ls"):
            return DockerResult(0, "\n".join(json.dumps(item) for item in self.builders.values()), "")
        if argv[:2] == ("buildx", "build"):
            name = argv[argv.index("--builder") + 1]
            container_id = "8" * 64
            self.containers.setdefault(
                container_id,
                _container(
                    container_id,
                    f"buildx_buildkit_{name}0",
                    {"com.docker.buildx.builder": name},
                    running=True,
                    host_config=self._builder_host_config(),
                ),
            )
            if self.interrupt_build:
                self.interrupt_build = False
                raise OSError("synthetic interruption during the build")
            if self.on_build is not None:
                self.on_build(argv)
            if self.execute_error is not None:
                raise self.execute_error
            if self.fail_build:
                return DockerResult(1, "", "build failed")
            tags = [argv[index + 1] for index, value in enumerate(argv) if value in {"--tag", "-t"}]
            item = {
                "Id": BUILT_IMAGE_ID,
                "Config": {"Labels": {"other": "project"} if self.build_foreign_labels else _labels(argv)},
                # Docker keeps every tag of an image ID: a rebuild of identical content adds its tags to the old ones.
                "RepoTags": list(dict.fromkeys([*self.images.get(BUILT_IMAGE_ID, {}).get("RepoTags", []), *tags])),
                "Size": 9_437_184,
            }
            requested = None
            for i, value in enumerate(argv):
                if value == "--platform" and i + 1 < len(argv):
                    requested = argv[i + 1]
                elif value.startswith("--platform="):
                    requested = value.split("=", 1)[1]
            reported_value = self.build_reports_platform or requested or self.default_image_platform
            reported = reported_value.split("/")
            if "," not in reported_value and len(reported) >= 2:
                item["Os"], item["Architecture"] = reported[0], reported[1]
                if len(reported) == 3:
                    item["Variant"] = reported[2]
            if not self.omit_build_created:
                item["Created"] = "build-created"
            if self.build_extra_reference is not None:
                field, reference = self.build_extra_reference
                item.setdefault(field, []).append(reference)
                self.images[reference] = item
            self.images[BUILT_IMAGE_ID] = item
            self.images.update(dict.fromkeys(tags, item))
            if self.fail_build_after_load:
                return DockerResult(137, "", "killed")
            Path(argv[argv.index("--metadata-file") + 1]).write_text(
                json.dumps(self.build_metadata or {"containerimage.digest": BUILT_IMAGE_ID}) + "\n"
            )
            if self.build_metadata_mode is not None:
                Path(argv[argv.index("--metadata-file") + 1]).chmod(self.build_metadata_mode)
            for position, value in enumerate(argv[:-1]):
                if value == "--cache-to" and self.cache_layers is not None:
                    destination = argv[position + 1].split(",")[1].removeprefix("dest=")
                    write_cache_layout(Path(destination), self.cache_layers)
            self.fail_image_inspection = self.fail_image_inspection_after_build
            return DockerResult(0, "built\n", "")
        if argv[:2] == ("image", "rm"):
            if self.fail_before_remove_kind == "image":
                self.fail_before_remove_kind = None
                raise OSError("synthetic interruption before image removal")
            targets = [target for target in argv[2:] if target != "--force"]
            missing = False
            for target in targets:
                image = self.images.get(target)
                if image is None:
                    missing = True
                    continue
                if not target.startswith("sha256:"):
                    if "--force" not in argv and any(
                        container.get("Image") == image.get("Id") for container in self.containers.values()
                    ):
                        return DockerResult(1, "", "image is being used by a stopped container")
                    self.images.pop(target)
                    image["RepoTags"] = [value for value in image.get("RepoTags", []) if value != target]
                    repository_digest = lifecycle._repository_digest(target)
                    image["RepoDigests"] = [
                        value for value in image.get("RepoDigests", []) if value != repository_digest
                    ]
                    referenced = image.get("RepoTags", []) or image.get("RepoDigests", [])
                    used = any(container.get("Image") == image.get("Id") for container in self.containers.values())
                    if not referenced and not used:
                        self.images = {key: item for key, item in self.images.items() if item is not image}
                else:
                    references = [*(image.get("RepoTags") or []), *(image.get("RepoDigests") or [])]
                    if "--force" not in argv and len(references) > 1:
                        return DockerResult(1, "", "image is referenced by multiple tags")
                    self.images = {key: item for key, item in self.images.items() if item.get("Id") != target}
            if self.fail_after_image_remove:
                self.fail_after_image_remove = False
                raise OSError("synthetic interruption after image removal")
            return DockerResult(1 if missing else 0, "\n".join(targets) + "\n", "missing image" if missing else "")
        if argv[:2] == ("image", "pull"):
            if self.pull_client_dies == "lost":
                self.pull_client_dies = None
                raise OSError("synthetic pull client death before any image")
            reference = argv[2]
            repository_digest = lifecycle._repository_digest(reference)
            item = {
                "Id": IMAGE_ID,
                "Config": {"Labels": {}},
                "RepoDigests": [repository_digest],
                "Size": 7_340_032,
            }
            if not self.omit_pull_created:
                item["Created"] = "pulled-created"
            self.images[IMAGE_ID] = item
            self.images[reference] = item
            dies, self.pull_client_dies = self.pull_client_dies, None
            if dies == "killed":
                return DockerResult(137, "", "killed")
            if dies == "interrupted":
                raise OSError("synthetic pull client death")
            return DockerResult(0, "pulled\n", "")
        if argv[:2] == ("buildx", "rm"):
            if self.fail_before_remove_kind == "builder":
                self.fail_before_remove_kind = None
                raise OSError("synthetic interruption before builder removal")
            name = argv[2]
            if self.keep_builder_after_remove_once:
                self.keep_builder_after_remove_once = False
                return DockerResult(0, argv[2] + "\n", "")
            self.builders.pop(name, None)
            if self.fail_after_builder_remove:
                self.fail_after_builder_remove = False
                raise OSError("synthetic interruption after builder removal")
            if self.stop_buildkit_after_builder_remove:
                for item in self.containers.values():
                    if item.get("Name") == f"/buildx_buildkit_{name}0":
                        item["State"] = {**item["State"], "Running": False, "Status": "exited"}
            elif not self.leave_buildkit_after_builder_remove:
                self.containers = {
                    key: item
                    for key, item in self.containers.items()
                    if item.get("Name") != f"/buildx_buildkit_{name}0"
                }
            return DockerResult(0, argv[2] + "\n", "")
        if argv[0] == "compose":
            return self._compose(argv)
        if argv[:2] == ("container", "exec") and argv[2] in self.containers:
            return DockerResult(0, "", "")
        if argv[0] == "exec" and argv[2:4] == ("cat", "/sys/fs/cgroup/cgroup.controllers"):
            return DockerResult(0, "memory\nmemory\n", "")  # the builder's cgroup check after a create
        raise AssertionError(f"unexpected fake Docker command: {argv!r}")

    def _builder_host_config(self) -> dict[str, object]:
        config: dict[str, object] = {
            "Memory": 2 * 1024**3 if self.builder_limits_valid else 0,
            "MemorySwap": 2 * 1024**3 if self.builder_limits_valid else 0,
            "CpuPeriod": 100000 if self.builder_limits_valid else 0,
            "CpuQuota": 200000 if self.builder_limits_valid else 0,
            "RestartPolicy": {"Name": "no" if self.builder_limits_valid else "unless-stopped"},
        }
        config.update(self.builder_host_overrides)
        return config

    def _compose(self, argv: tuple[str, ...]) -> DockerResult:
        project = argv[argv.index("--project-name") + 1]
        position = 3
        while argv[position] in {"--file", "-f", "--env-file", "--profile", "--project-directory"}:
            position += 2
        subcommand = next(value for value in argv[position:] if not value.startswith("-"))
        if subcommand == "config":
            if self.on_compose_config is not None:
                self.on_compose_config()
            service: dict[str, object] = {"image": "fixture", **self.compose_service_extra}
            if self.compose_config_variant:
                service["environment"] = {"VARIANT": "changed"}
            if self.compose_anonymous_volume:
                service["volumes"] = [{"type": "volume", "target": "/anonymous"}]
            config = {
                "name": project,
                "services": dict.fromkeys(self.compose_services, service),
                "networks": {"default": {"name": f"{project}_default"}},
                "volumes": {"data": {"name": f"{project}_data"}},
            }
            return DockerResult(0, json.dumps(config), "")
        if subcommand == "ps":
            identifiers = [
                key
                for key, item in self.containers.items()
                if item["Config"]["Labels"].get("com.docker.compose.project") == project
            ]
            return DockerResult(0, "\n".join(identifiers) + ("\n" if identifiers else ""), "")
        if subcommand in {"cp", "exec"}:
            return DockerResult(0, "", "")
        if subcommand == "run":
            container_id = "f" * 64
            name = argv[argv.index("--name") + 1]
            entrypoint = argv.index("--entrypoint")
            service_name = argv[entrypoint + 2]
            self._store(
                _container(
                    container_id,
                    name,
                    {
                        "com.docker.compose.project": project,
                        "com.docker.compose.service": service_name,
                        "com.docker.compose.oneoff": "True",
                    },
                    image_id=self.compose_runtime_image_id,
                ),
                network=f"{project}_default",
            )
            if not self.compose_run_retained:
                self.containers.pop(container_id)
            return DockerResult(self.compose_run_exit_code, "", "")
        if subcommand in {"create", "up"}:
            self.compose_generation += 1
            mounts = []
            if self.compose_image_volume:
                name = "7" * 64
                self.volumes[name] = {
                    "Name": name,
                    "CreatedAt": "anonymous-volume",
                    "Driver": "local",
                    "Labels": {"com.docker.volume.anonymous": ""},
                }
                mounts.append({"Type": "volume", "Name": name})
            for index, service_name in enumerate(self.compose_services):
                container_id = chr(ord("c") + index) * 64
                self._store(
                    _container(
                        container_id,
                        f"{project}-{service_name}-1",
                        {
                            "com.docker.compose.project": project,
                            "com.docker.compose.service": service_name,
                        },
                        running=subcommand == "up",
                        image_id=self.compose_runtime_image_id,
                        mounts=mounts,
                        ports=self.compose_ports if index == 0 else {},
                    ),
                    network=f"{project}_default",
                )
            network_id = "d" * 64
            self.networks[network_id] = {
                "Id": network_id,
                "Name": f"{project}_default",
                "Created": "compose-network",
                "Labels": {"com.docker.compose.project": project},
            }
            self.volumes.setdefault(
                f"{project}_data",
                {
                    "Name": f"{project}_data",
                    "CreatedAt": f"compose-volume-{self.compose_generation}",
                    "Driver": "local",
                    "Labels": {"com.docker.compose.project": project},
                },
            )
            return DockerResult(0, "", "")
        if subcommand in {"restart", "start"}:
            for container in self.containers.values():
                if container["Config"]["Labels"].get("com.docker.compose.project") == project:
                    self._start(container["Id"])
                    container["State"]["StartedAt"] += f"-{subcommand}ed"
                    self._mutate(container)
            return DockerResult(0, "", "")
        if subcommand == "down":
            self.containers = {
                key: item
                for key, item in self.containers.items()
                if item["Config"]["Labels"].get("com.docker.compose.project") != project
            }
            self.networks = {key: item for key, item in self.networks.items() if item["Name"] != f"{project}_default"}
            if "--volumes" in argv:
                self.volumes.pop(f"{project}_data", None)
                self.volumes.pop("7" * 64, None)
            return DockerResult(0, "", "")
        raise AssertionError(f"unexpected fake Compose command: {argv!r}")


def _labels(argv: tuple[str, ...]) -> dict[str, str]:
    return {
        argv[index + 1].split("=", 1)[0]: argv[index + 1].split("=", 1)[1]
        for index, value in enumerate(argv)
        if value == "--label"
    }


HARDENED_MEMORY = 256 * 1024**2
HARDENED_NANO_CPUS = 500_000_000


def _harden(container: dict, network: str) -> None:
    """Give a fake container the inspected shape of a capacity-two-compliant one (every row E1 to E12 satisfied)."""
    container["HostConfig"] = {
        "Memory": HARDENED_MEMORY,
        "NanoCpus": HARDENED_NANO_CPUS,
        "Privileged": False,
        "NetworkMode": network,
        "PidMode": "",
        "IpcMode": "private",
        "UTSMode": "",
        "UsernsMode": "",
        "CgroupnsMode": "private",
        "CapAdd": [],
        "Devices": [],
        "DeviceRequests": [],
        "CgroupParent": "",
        "PortBindings": {},
        "SecurityOpt": [],
        "RestartPolicy": {"Name": "no"},
    }
    container["NetworkSettings"]["Networks"] = {network: {"NetworkID": "d" * 64 if network != "none" else "0" * 64}}


def _container(
    container_id: str,
    name: str,
    labels: dict[str, str],
    *,
    running: bool = False,
    image_id: str = IMAGE_ID,
    mounts: list[dict[str, str]] | None = None,
    ports: dict[str, list[dict[str, str]]] | None = None,
    host_config: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "Id": container_id,
        "Name": "/" + name,
        "Created": "container-created",
        "Image": image_id,
        "Mounts": mounts or [],
        "Config": {"Labels": labels},
        "HostConfig": host_config or {"Memory": 1},
        "State": {
            "Running": running,
            "StartedAt": "started-0",
            "OOMKilled": False,
        },
        "OomEvents": 0,
        "RestartCount": 0,
        "NetworkSettings": {"Ports": ports or {}},
    }


def _observe_fake_container(
    docker: FakeDockerPort,
    daemon_id: str,
    container_id: str,
    **_keywords,
) -> lifecycle.ContainerObservation:
    container = docker.containers[container_id]
    state = container["State"]
    return lifecycle.ContainerObservation(
        daemon_id,
        container_id,
        state["StartedAt"],
        lifecycle.NativeClock().read(),
        1,
        2,
        container["OomEvents"],
        state["OOMKilled"],
        container["RestartCount"],
    )


def _spec(
    project: Path,
    *,
    include_image: bool = True,
    services: tuple[str, ...] = ("fixture",),
    container_bytes: int = 2 * 1024**3,
    daemon_cpus: int = 2,
    capacity: int = 1,
    retain: bool = False,
) -> RunSpec:
    external_resources = (
        [ExternalDockerResource("network", "e" * 64, "external", "connect_owned_container")] if capacity == 1 else []
    )
    if include_image:
        external_resources.append(ExternalDockerResource("image", IMAGE_ID, "fixture", "use_image"))
    return RunSpec(
        project_root=project,
        entrypoint="unit",
        purpose="all-receipts",
        services=services,
        closure=services,
        profile="fixture",
        phases={"tests": PhaseBudget(container_bytes, 1, daemon_cpus, 1)},
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
        external_resources=tuple(external_resources),
        wait_seconds=20,
        capacity=capacity,
        retain=retain,
    )


def test_all_supported_mutation_classes_are_receipt_first_and_exact(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    fake = FakeDockerPort()
    handle = build_test_services(tmp_path / "state").open_run(_spec(project), docker_factory=lambda: fake).enter()
    _ = handle.docker_context
    descriptor = json.loads((handle.private_root / "docker-authority.json").read_text())

    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("run", IMAGE_ID, "true"))
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("network", "connect", "e" * 64, "a" * 64))
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("container", "rm", "a" * 64))

    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("network", "create"))
    record = json.loads(handle._context.record.read_text())
    network_id = next(key for key, item in record["owned_resources"]["networks"].items() if item["state"] != "removed")
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("network", "rm", network_id))

    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("volume", "create"))
    record = json.loads(handle._context.record.read_text())
    volume_name = next(key for key, item in record["owned_resources"]["volumes"].items() if item["state"] != "removed")
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("volume", "rm", volume_name))

    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "create"))
    lifecycle.dispatch_docker(
        handle._context,
        descriptor,
        fake,
        ("buildx", "build", "--tag", "fixture:latest", "."),
    )
    record = json.loads(handle._context.record.read_text())
    image_id = next(key for key, item in record["owned_resources"]["images"].items() if item["state"] != "removed")
    inspected = lifecycle.dispatch_docker(
        handle._context,
        descriptor,
        fake,
        ("image", "inspect", "fixture:latest"),
    )
    assert json.loads(inspected.stdout)[0]["Id"] == image_id
    with pytest.raises(InvalidInput, match="neither owned nor declared external"):
        lifecycle.dispatch_docker(
            handle._context,
            descriptor,
            fake,
            ("image", "inspect", "foreign:latest"),
        )
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("image", "rm", image_id))
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "rm", handle.docker_context.builder))

    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "up", "--detach"))
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "down", "--volumes"))
    record = json.loads(handle._context.record.read_text())

    assert record["docker_receipts"]
    assert all(item["state"] not in {"pending", "unresolved"} for item in record["docker_receipts"].values())
    assert all(
        item["state"] == "removed" for resources in record["owned_resources"].values() for item in resources.values()
    )
    assert "e" * 64 in fake.networks
    assert IMAGE_ID in fake.images
    assert not any("prune" in command for command in fake.commands)
    handle.leave()


def _pause_for_invalid_records(request: pytest.FixtureRequest, context: lifecycle.RunContext) -> Callable[[], None]:
    """Pause the run's guardian while the test writes records its own loader refuses; return the call that resumes it.

    A private run's record with a ``managed`` grant scope contradicts the guardian's account, and its active loop reads
    the record every 50 ms. Resume once the record is valid again; a failing test restores the entry record and resumes
    the guardian at teardown.
    """
    pid = pause_guardian(context)
    entry = json.loads(context.record.read_text())
    resumed = False

    def resume() -> None:
        nonlocal resumed
        resumed = True
        resume_guardian(pid)

    def teardown() -> None:
        if not resumed:
            lifecycle.atomic_json(context.record, entry)
            resume_guardian(pid)

    request.addfinalizer(teardown)
    return resume


def test_active_observation_handles_shim_receipts_and_owned_control(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    fake = FakeDockerPort()
    handle = (
        build_test_services(tmp_path / "state")
        .open_run(_spec(project), docker_factory=lambda: fake, admit_docker=True)
        .enter()
    )
    _ = handle.docker_context
    descriptor = json.loads((handle.private_root / "docker-authority.json").read_text())
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "up", "--detach"))
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "create"))
    lifecycle.dispatch_docker(
        handle._context,
        descriptor,
        fake,
        ("buildx", "build", "--tag", "fixture-built:latest", "."),
    )
    record = json.loads(handle._context.record.read_text())
    guardian = lifecycle._identity_from_json(record["guardian"])
    requester = lifecycle._identity_from_json(record["requester"])
    original_account_directory = lifecycle.account_directory
    isolated_account = Path(record["account_directory"])
    resume = _pause_for_invalid_records(request, handle._context)
    record["grant"]["scope"] = "managed"
    handle._context.record.write_text(json.dumps(record))
    baseline = lifecycle._host_observation(record["admission_observation"]["memory"])
    current = lifecycle.HostMemoryObservation(
        baseline.origin,
        baseline.tick_ns + 1,
        baseline.physical_bytes,
        baseline.headroom_bytes,
        baseline.swap_used_bytes,
        baseline.swapout_bytes,
        baseline.page_size,
        baseline.raw,
    )
    capacity_owned_ids: list[frozenset[str]] = []
    mutation_plans: list[dict | None] = []

    def observe_capacity(_docker, topology, owned_ids=frozenset(), **_keywords):
        capacity_owned_ids.append(frozenset(owned_ids))
        mutation_plans.append(_keywords.get("mutation_plan"))
        external = tuple(
            None
            for identifier, container in fake.containers.items()
            if container["State"]["Running"] and identifier not in owned_ids
        )
        gib = 1024**3
        return fake.identity.daemon_id, lifecycle.CapacitySnapshot(
            64 * gib, 64, 64 * gib, 64, topology, external, runtime=lifecycle._TOPOLOGY_RUNTIME[topology]
        )

    monkeypatch.setattr(lifecycle, "current_process", lambda _clock: guardian)
    monkeypatch.setattr(lifecycle, "account_directory", lambda: isolated_account)
    monkeypatch.setattr(lifecycle, "canonical_account", lambda: isolated_account)
    monkeypatch.setattr(lifecycle, "_docker", lambda: fake)
    monkeypatch.setattr(lifecycle, "observe_capacity", observe_capacity)
    monkeypatch.setattr(
        lifecycle,
        "observe_host_memory",
        lambda _clock, deadline=None: current,
    )
    monkeypatch.setattr(lifecycle, "_active_process_rss", lambda _identity, _deadline: 0)
    monkeypatch.setattr(
        lifecycle,
        "assess_pressure",
        lambda *_args, **_keywords: lifecycle.CapacityDecision(lifecycle.CapacityState.READY, "stable"),
    )
    monkeypatch.setattr(lifecycle, "observe_container", _observe_fake_container)

    lifecycle._observe_active_run(handle._context)
    observed = json.loads(handle._context.record.read_text())
    owned_failure = observed["infrastructure_failure"]
    expected_owned_ids = frozenset(
        container_id
        for container_id, resource in observed["owned_resources"]["containers"].items()
        if resource["state"] == "running"
    )
    first_capacity_ids = capacity_owned_ids[-1]
    controlled_id = next(
        container_id
        for container_id, resource in observed["owned_resources"]["containers"].items()
        if resource["evidence"].get("control") is not None
    )
    control = observed["owned_resources"]["containers"][controlled_id]["evidence"]["control"]
    operation = {
        "id": "33333333-3333-4333-8333-333333333333",
        "kind": "container-control",
        "generation": observed["generation"],
        "revision": observed["revision"] + 1,
        "actor": observed["requester"],
        "deadline": asdict(lifecycle.Deadline.after(lifecycle.NativeClock(), 30)),
        "plan": {
            "action": "stop",
            "targets": [controlled_id],
            "expected": {
                controlled_id: {
                    "started_at": control["started_at"],
                    "restart_count": control["restart_count"],
                }
            },
            "request_sha256": "e" * 64,
        },
    }
    observed["pending_operation"] = operation
    observed["revision"] = operation["revision"]
    handle._context.record.write_text(json.dumps(observed))
    control_current = lifecycle.HostMemoryObservation(
        current.origin,
        current.tick_ns + 1,
        current.physical_bytes,
        current.headroom_bytes,
        current.swap_used_bytes,
        current.swapout_bytes,
        current.page_size,
        current.raw,
    )
    monkeypatch.setattr(lifecycle, "observe_host_memory", lambda _clock, deadline=None: control_current)

    lifecycle._observe_active_run(handle._context)
    observed = json.loads(handle._context.record.read_text())
    owned_control_failure = observed["infrastructure_failure"]
    owned_control_mutation_plan = mutation_plans[-1]
    observed["pending_operation"] = None
    handle._context.record.write_text(json.dumps(observed))
    receipt = next(item for item in observed["docker_receipts"].values() if item["kind"] == "build")
    receipt_result = receipt["result"]
    receipt["state"] = "pending"
    receipt["result"] = None
    external_id = "7" * 64
    fake.containers[external_id] = _container(external_id, "external", {}, running=True)
    handle._context.record.write_text(json.dumps(observed))
    later = lifecycle.HostMemoryObservation(
        control_current.origin,
        control_current.tick_ns + 1,
        control_current.physical_bytes,
        control_current.headroom_bytes,
        control_current.swap_used_bytes,
        control_current.swapout_bytes,
        control_current.page_size,
        control_current.raw,
    )
    monkeypatch.setattr(lifecycle, "observe_host_memory", lambda _clock, deadline=None: later)

    lifecycle._observe_active_run(handle._context)
    observed = json.loads(handle._context.record.read_text())
    pending_failure = observed["infrastructure_failure"]
    pending_capacity_call_count = len(capacity_owned_ids)
    receipt = next(item for item in observed["docker_receipts"].values() if item["kind"] == "build")
    receipt["state"] = "promoted"
    receipt["result"] = receipt_result
    fake.containers.pop(external_id)
    observed["infrastructure_failure"] = None
    observed.pop("failure_evidence", None)  # a reset failure leaves no first observation behind
    service_id = next(
        container_id
        for container_id, resource in observed["owned_resources"]["containers"].items()
        if resource["evidence"].get("control") is not None
    )
    fake.containers[service_id]["OomEvents"] = 1
    handle._context.record.write_text(json.dumps(observed))
    oom_current = lifecycle.HostMemoryObservation(
        later.origin,
        later.tick_ns + 1,
        later.physical_bytes,
        later.headroom_bytes,
        later.swap_used_bytes,
        later.swapout_bytes,
        later.page_size,
        later.raw,
    )
    monkeypatch.setattr(lifecycle, "observe_host_memory", lambda _clock, deadline=None: oom_current)

    lifecycle._observe_active_run(handle._context)
    observed = json.loads(handle._context.record.read_text())
    oom_failure = observed["infrastructure_failure"]
    observed["owned_resources"]["containers"][service_id]["evidence"]["control"]["oom_events"] = 0
    fake.containers[service_id]["OomEvents"] = 0
    observed["infrastructure_failure"] = None
    observed.pop("failure_evidence", None)  # a reset failure leaves no first observation behind
    observed["grant"]["scope"] = "host-proof"
    handle._context.record.write_text(json.dumps(observed))
    resume()  # the record is valid for the guardian again
    monkeypatch.setattr(lifecycle, "account_directory", original_account_directory)
    monkeypatch.setattr(lifecycle, "canonical_account", lambda: None)  # back to the source tree: no installation
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "down", "--volumes"))
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("image", "rm", BUILT_IMAGE_ID))
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "rm", handle.docker_context.builder))
    monkeypatch.setattr(lifecycle, "current_process", lambda _clock: requester)
    handle.leave()

    assert (owned_failure, owned_control_failure, pending_failure, oom_failure) == (
        None,
        None,
        "unknown-external-capacity",
        "container-oom",
    )
    assert owned_control_mutation_plan is None
    assert first_capacity_ids == expected_owned_ids
    assert pending_capacity_call_count == 3


@pytest.mark.parametrize(
    ("open_receipts", "expected"),
    [
        ([], False),
        ([{"kind": "build", "state": "pending"}], True),
        ([{"kind": "image-save", "state": "pending"}], True),
        ([{"kind": "image-save", "state": "unresolved"}], False),
        ([{"kind": "container-create", "state": "pending"}], False),
        ([{"kind": "image-save", "state": "pending"}, {"kind": "volume-create", "state": "pending"}], False),
    ],
)
def test_pending_builds_and_exports_keep_host_pressure_observable(open_receipts: list[dict], expected: bool) -> None:
    assert lifecycle._only_observable_receipts_pending(open_receipts) is expected


@pytest.mark.parametrize(
    ("headroom", "swap", "swapout", "rss", "state", "reason"),
    [
        # 4 GiB below the phase baseline: memory others took makes later runs wait, it never fails this run
        (4 * 1024**3, 0, 0, 0, lifecycle.CapacityState.READY, "native-pressure"),
        (1024**3, 0, 0, 0, lifecycle.CapacityState.REFUSED, "native-headroom"),
        (4 * 1024**3, 4 * 1024**3, 0, 0, lifecycle.CapacityState.REFUSED, "admission-swap-growth"),
        (4 * 1024**3, 0, 4 * 1024**3, 0, lifecycle.CapacityState.REFUSED, "swapout-rate"),
        (4 * 1024**3, 0, 0, 2 * 1024**3, lifecycle.CapacityState.REFUSED, "host-process-envelope"),
    ],
)
def test_a_granted_run_fails_only_for_its_own_overuse_or_real_host_pressure(
    headroom: int, swap: int, swapout: int, rss: int, state: lifecycle.CapacityState, reason: str
) -> None:
    gib = 1024**3
    baseline = lifecycle.HostMemoryObservation("host", 0, 16 * gib, 8 * gib, 0, 0, 4096, {})
    current = lifecycle.HostMemoryObservation("host", 31_000_000_000, 16 * gib, headroom, swap, swapout, 4096, {})

    decision = lifecycle.assess_pressure(
        current,
        baseline,
        granted=True,
        admission_baseline=baseline,
        phase_baseline=baseline,
        host_rss_bytes=rss,
        host_envelope_bytes=gib,
    )

    assert (decision.state, decision.reason) == (state, reason)


@pytest.mark.parametrize(
    ("advance_revision", "expected_failure"),
    [(True, None), (False, "telemetry-unreadable")],
)
def test_active_observation_only_publishes_failure_for_current_revision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
    *,
    advance_revision: bool,
    expected_failure: str | None,
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    fake = FakeDockerPort()
    handle = (
        build_test_services(tmp_path / "state")
        .open_run(_spec(project), docker_factory=lambda: fake, admit_docker=True)
        .enter()
    )
    record = json.loads(handle._context.record.read_text())
    guardian = lifecycle._identity_from_json(record["guardian"])
    requester = lifecycle._identity_from_json(record["requester"])
    original_account_directory = lifecycle.account_directory
    isolated_account = Path(record["account_directory"])
    resume = _pause_for_invalid_records(request, handle._context)
    record["grant"]["scope"] = "managed"
    handle._context.record.write_text(json.dumps(record))

    def fail_observation(*_args, **_kwargs):
        if advance_revision:
            current = json.loads(handle._context.record.read_text())
            current["revision"] += 1
            handle._context.record.write_text(json.dumps(current))
        raise ResourceError("synthetic telemetry failure")

    monkeypatch.setattr(lifecycle, "current_process", lambda _clock: guardian)
    monkeypatch.setattr(lifecycle, "account_directory", lambda: isolated_account)
    monkeypatch.setattr(lifecycle, "canonical_account", lambda: isolated_account)
    monkeypatch.setattr(lifecycle, "_docker", lambda: fake)
    monkeypatch.setattr(lifecycle, "observe_capacity", fail_observation)

    lifecycle._observe_active_run(handle._context)

    observed = json.loads(handle._context.record.read_text())
    assert observed["infrastructure_failure"] == expected_failure
    observed["infrastructure_failure"] = None
    observed.pop("failure_evidence", None)  # a reset failure leaves no first observation behind
    observed["grant"]["scope"] = "host-proof"
    handle._context.record.write_text(json.dumps(observed))
    resume()  # the record is valid for the guardian again
    monkeypatch.setattr(lifecycle, "account_directory", original_account_directory)
    monkeypatch.setattr(lifecycle, "canonical_account", lambda: None)  # back to the source tree: no installation
    monkeypatch.setattr(lifecycle, "current_process", lambda _clock: requester)
    handle.leave()


def test_digest_pinned_pull_reuses_existing_image_as_external_without_mutation(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    fake = FakeDockerPort()
    reference = "example/cache@sha256:" + "1" * 64
    handle = (
        build_test_services(tmp_path / "state")
        .open_run(_spec(project, include_image=False), docker_factory=lambda: fake)
        .enter()
    )
    _ = handle.docker_context
    descriptor = json.loads((handle.private_root / "docker-authority.json").read_text())

    result = lifecycle.dispatch_docker(handle._context, descriptor, fake, ("image", "pull", reference))
    record = json.loads(handle._context.record.read_text())

    assert result.exit_code == 0
    assert record["docker_receipts"] == {}
    assert record["owned_resources"]["images"] == {}
    assert record["image_bindings"][reference] == {
        "created": "external-created",
        "identifier": IMAGE_ID,
        "ownership": "external",
    }
    assert len(record["external_resources"]) == 1
    assert lifecycle.dispatch_docker(handle._context, descriptor, fake, ("image", "inspect", reference)).exit_code == 0
    assert ("image", "pull", reference) not in fake.commands
    handle.leave()


def test_digest_pinned_pull_accepts_missing_optional_created_metadata(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    fake = FakeDockerPort()
    reference = "example/cache@sha256:" + "1" * 64
    fake.images[IMAGE_ID].pop("Created")
    handle = (
        build_test_services(tmp_path / "state")
        .open_run(_spec(project, include_image=False), docker_factory=lambda: fake)
        .enter()
    )
    _ = handle.docker_context
    descriptor = json.loads((handle.private_root / "docker-authority.json").read_text())

    result = lifecycle.dispatch_docker(handle._context, descriptor, fake, ("image", "pull", reference))
    record = json.loads(handle._context.record.read_text())

    assert result.exit_code == 0
    assert record["image_bindings"][reference] == {
        "created": None,
        "identifier": IMAGE_ID,
        "ownership": "external",
    }
    handle.leave()


def test_digest_pinned_pull_accepts_docker_tag_free_repo_digest(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    fake = FakeDockerPort()
    reference = "example/cache:stable@sha256:" + "1" * 64
    fake.images[reference] = {
        "Id": IMAGE_ID,
        "Created": "external-created",
        "Config": {"Labels": {}},
        "RepoDigests": ["example/cache@sha256:" + "1" * 64],
    }
    handle = (
        build_test_services(tmp_path / "state")
        .open_run(_spec(project, include_image=False), docker_factory=lambda: fake)
        .enter()
    )
    _ = handle.docker_context
    descriptor = json.loads((handle.private_root / "docker-authority.json").read_text())

    result = lifecycle.dispatch_docker(handle._context, descriptor, fake, ("image", "pull", reference))

    assert result.exit_code == 0
    assert ("image", "pull", reference) not in fake.commands
    handle.leave()


def test_digest_pinned_pull_promotes_new_image_and_cleanup_removes_only_that_image(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    fake = FakeDockerPort()
    fake.images.clear()
    reference = "example/cache@sha256:" + "2" * 64
    handle = (
        build_test_services(tmp_path / "state")
        .open_run(_spec(project, include_image=False), docker_factory=lambda: fake)
        .enter()
    )
    _ = handle.docker_context
    descriptor = json.loads((handle.private_root / "docker-authority.json").read_text())

    result = lifecycle.dispatch_docker(handle._context, descriptor, fake, ("pull", reference))
    record = json.loads(handle._context.record.read_text())

    assert result.exit_code == 0
    assert record["owned_resources"]["images"][IMAGE_ID]["evidence"]["repository_digest"] == reference
    assert record["image_bindings"][reference]["ownership"] == "owned"
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("image", "rm", IMAGE_ID))
    assert handle.leave().cleanup_outcome == "complete"
    assert IMAGE_ID not in fake.images


def test_new_digest_pull_accepts_missing_optional_created_metadata(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    fake = FakeDockerPort()
    fake.images.clear()
    fake.omit_pull_created = True
    reference = "example/cache@sha256:" + "2" * 64
    handle = (
        build_test_services(tmp_path / "state")
        .open_run(_spec(project, include_image=False), docker_factory=lambda: fake)
        .enter()
    )
    _ = handle.docker_context
    descriptor = json.loads((handle.private_root / "docker-authority.json").read_text())

    result = lifecycle.dispatch_docker(handle._context, descriptor, fake, ("pull", reference))
    record = json.loads(handle._context.record.read_text())

    assert result.exit_code == 0
    assert record["image_bindings"][reference] == {
        "created": None,
        "identifier": IMAGE_ID,
        "ownership": "owned",
    }
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("image", "rm", IMAGE_ID))
    handle.leave()


def test_pulled_digest_cleanup_force_releases_alias_without_deleting_shared_image_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    fake = FakeDockerPort()
    reference = "example/cache@sha256:" + "2" * 64
    fake.images = {
        IMAGE_ID: {
            "Id": IMAGE_ID,
            "Created": "shared-created",
            "Config": {"Labels": {}},
            "RepoDigests": [],
        }
    }
    foreign_id = "d" * 64
    fake.containers[foreign_id] = _container(foreign_id, "foreign", {})
    handle = (
        build_test_services(tmp_path / "state")
        .open_run(_spec(project, include_image=False), docker_factory=lambda: fake)
        .enter()
    )
    _ = handle.docker_context
    descriptor = json.loads((handle.private_root / "docker-authority.json").read_text())
    monkeypatch.setattr(lifecycle, "_BOUND_DOCKER_FACTORY", lambda _identity: fake)

    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("pull", reference))
    lifecycle._cleanup_owned_docker_resources(handle._context)
    status = handle.leave()

    assert status.cleanup_outcome == "complete"
    assert IMAGE_ID in fake.images
    assert reference not in fake.images
    assert foreign_id in fake.containers
    assert ("image", "rm", "--force", reference) in fake.commands


def test_interrupted_digest_alias_cleanup_reconciles_reference_absence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    fake = FakeDockerPort()
    reference = "example/cache@sha256:" + "2" * 64
    fake.images = {
        IMAGE_ID: {
            "Id": IMAGE_ID,
            "Created": "shared-created",
            "Config": {"Labels": {}},
            "RepoDigests": [],
        }
    }
    foreign_id = "d" * 64
    fake.containers[foreign_id] = _container(foreign_id, "foreign", {})
    handle = (
        build_test_services(tmp_path / "state")
        .open_run(_spec(project, include_image=False), docker_factory=lambda: fake)
        .enter()
    )
    _ = handle.docker_context
    descriptor = json.loads((handle.private_root / "docker-authority.json").read_text())
    monkeypatch.setattr(lifecycle, "_BOUND_DOCKER_FACTORY", lambda _identity: fake)

    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("pull", reference))
    fake.fail_after_image_remove = True
    with pytest.raises(ResourceError, match="cleanup result is unresolved"):
        lifecycle._cleanup_owned_docker_resources(handle._context)

    interrupted = json.loads(handle._context.record.read_text())
    receipt = next(item for item in interrupted["docker_receipts"].values() if item["kind"] == "image-rm")
    assert receipt["state"] == "unresolved"
    assert receipt["plan"] == {"image_id": IMAGE_ID, "reference": reference}
    lifecycle._reconcile_docker_receipt(handle._context)

    assert handle.leave().cleanup_outcome == "complete"
    assert IMAGE_ID in fake.images
    assert reference not in fake.images
    assert foreign_id in fake.containers


def test_cached_digest_pull_refuses_incomplete_identity_before_record_mutation(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    fake = FakeDockerPort()
    reference = "example/cache@sha256:" + "1" * 64
    fake.images[reference]["Created"] = ""
    handle = (
        build_test_services(tmp_path / "state")
        .open_run(_spec(project, include_image=False), docker_factory=lambda: fake)
        .enter()
    )
    _ = handle.docker_context
    descriptor = json.loads((handle.private_root / "docker-authority.json").read_text())

    with pytest.raises(InvalidInput, match="does not match"):
        lifecycle.dispatch_docker(handle._context, descriptor, fake, ("pull", reference))

    record = json.loads(handle._context.record.read_text())
    assert record["image_bindings"] == {}
    assert record["docker_receipts"] == {}
    handle.leave("failed")


def test_shim_compose_resources_back_public_dynamic_endpoints(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    project = tmp_path / "project"
    project.mkdir()
    handle = (
        build_test_services(tmp_path / "state")
        .open_run(_spec(project), docker_factory=lambda: fake, admit_docker=True)
        .enter()
    )
    _ = handle.docker_context
    descriptor = json.loads((handle.private_root / "docker-authority.json").read_text())
    fake.compose_ports = {"8080/tcp": [{"HostIp": "127.0.0.1", "HostPort": "28080"}]}

    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "up", "--detach"))
    published = handle.publish_endpoint("fixture", "http", 8080)
    required = handle.require_endpoint("fixture")

    assert (published.host, published.port, published.container_id) == ("127.0.0.1", 28080, "c" * 64)
    assert required == published
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "down", "--volumes"))
    handle.leave()


def test_resume_loader_rejects_non_quiescent_managed_resources(tmp_path: Path, request: pytest.FixtureRequest) -> None:
    fake = FakeDockerPort()
    project = tmp_path / "project"
    project.mkdir()
    handle = (
        build_test_services(tmp_path / "state")
        .open_run(_spec(project, services=("fixture", "sibling")), docker_factory=lambda: fake, admit_docker=True)
        .enter()
    )
    original = json.loads(handle._context.record.read_text())
    record = json.loads(json.dumps(original))
    compose_file = project / "compose.yml"
    compose_file.write_text("services: {}\n")
    plan = lifecycle._compose_plan(handle._context, (compose_file,), ("fixture", "sibling"))
    containers = {}
    for container_id, service, state in (("c" * 64, "fixture", "stopped"), ("d" * 64, "sibling", "running")):
        containers[container_id] = {
            "service": service,
            "created": "container-created",
            "image_id": IMAGE_ID,
            "memory_bytes": 2,
            "nano_cpus": 1,
            "restart_policy": "no",
            "labels": plan["labels"],
            "network_ids": [],
            "volume_names": [],
            "bindings": {},
            "state": state,
            "observation": {
                "daemon_id": original["grant"]["daemon_id"],
                "container_id": container_id,
                "started_at": "started-0",
                "tick_ns": 1,
                "memory_usage_bytes": 1,
                "memory_limit_bytes": 2,
                "oom_events": 0,
                "oom_killed": False,
                "restart_count": 0,
            },
        }
    record["resources"] = {"compose": plan, "containers": containers, "networks": {}, "volumes": {}}
    origin = {key: record.get(key) for key in lifecycle._RESUME_ORIGIN_FIELDS}
    record.update(
        state="prepared",
        admission="never-granted",
        guardian_attempt=None,
        guardian=None,
        bootstrap_deadline=None,
        account_directory=None,
        ticket=None,
        grant=None,
        retained_grant=origin["grant"],
        resume_origin=origin,
        phase_history=[],
        withdrawal=None,
        leave_ack=None,
        recovery_actor=None,
        test_outcome=None,
        cleanup_outcome=None,
        recovery_outcome=None,
        quiescent=False,
        endpoints={},
        consumers={},
        pending_operation=None,
        last_operation=None,
        admission_observation=None,
        infrastructure_failure=None,
    )
    record["revision"] += 1
    resume = _pause_for_invalid_records(request, handle._context)  # the live guardian must not read this record
    lifecycle.atomic_json(handle._context.record, record)

    with pytest.raises(ResourceError, match="Retained resume preparation is contradictory"):
        lifecycle.load_run_record(handle._context)

    lifecycle.atomic_json(handle._context.record, original)
    resume()
    handle.leave()


def test_shim_compose_resources_back_durable_container_control(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = FakeDockerPort()
    fake.compose_services = ("caller", "target")
    project = tmp_path / "project"
    project.mkdir()
    handle = (
        build_test_services(tmp_path / "state")
        .open_run(_spec(project, services=fake.compose_services), docker_factory=lambda: fake, admit_docker=True)
        .enter()
    )
    _ = handle.docker_context
    descriptor = json.loads((handle.private_root / "docker-authority.json").read_text())
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "up", "--detach"))
    monkeypatch.setattr(lifecycle, "_bound_docker", lambda _data: fake)
    monkeypatch.setattr(lifecycle, "observe_container", _observe_fake_container)
    caller_id, target_id = "c" * 64, "d" * 64
    generation = {"started_at": "started-0", "restart_count": 0}

    assert handle.register_container_control(caller_id, generation, ()) == {
        caller_id: generation,
        target_id: generation,
    }
    registered = handle.register_container_control(caller_id, generation, (target_id,))
    observe_owned_control = lifecycle._observe_owned_control
    revised = False

    def observe_with_guardian_revision(data, docker, targets, deadline):
        nonlocal revised
        observed = observe_owned_control(data, docker, targets, deadline)
        if not revised:
            current = json.loads(handle._context.record.read_text())
            current["revision"] += 1
            lifecycle.atomic_json(handle._context.record, current)
            revised = True
        return observed

    monkeypatch.setattr(lifecycle, "_observe_owned_control", observe_with_guardian_revision)
    stopped = handle.control_containers(
        "11111111-1111-4111-8111-111111111111",
        "stop",
        (target_id,),
        {target_id: generation},
        request_sha256="e" * 64,
    )
    replayed = handle.control_containers(
        "11111111-1111-4111-8111-111111111111",
        "stop",
        (target_id,),
        {target_id: generation},
        request_sha256="e" * 64,
    )
    started = handle.control_containers(
        "22222222-2222-4222-8222-222222222222",
        "start",
        (target_id,),
        stopped,
        request_sha256="f" * 64,
    )
    started_record = json.loads(handle._context.record.read_text())

    def observe_with_authority_change(data, docker, targets, deadline):
        observed = observe_owned_control(data, docker, targets, deadline)
        current = json.loads(handle._context.record.read_text())
        current["owned_resources"]["containers"][target_id]["state"] = "stopped"
        current["revision"] += 1
        lifecycle.atomic_json(handle._context.record, current)
        return observed

    monkeypatch.setattr(lifecycle, "_observe_owned_control", observe_with_authority_change)

    with pytest.raises(ResourceBusy, match="authority changed before control acceptance"):
        handle.control_containers(
            "33333333-3333-4333-8333-333333333333",
            "stop",
            (target_id,),
            started,
            request_sha256="a" * 64,
        )
    lifecycle.atomic_json(handle._context.record, started_record)

    assert registered == {caller_id: generation, target_id: generation}
    assert stopped == replayed == {target_id: generation}
    assert started == {target_id: {"started_at": "started-0-restarted", "restart_count": 0}}
    assert fake.containers[target_id]["State"]["Running"] is True
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "down", "--volumes"))
    handle.leave()


def test_legacy_control_accepts_telemetry_refresh_but_rejects_authority_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = FakeDockerPort()
    project = tmp_path / "project"
    project.mkdir()
    compose_file = project / "compose.yml"
    compose_file.write_text("services: {}\n")
    handle = (
        build_test_services(tmp_path / "state")
        .open_run(_spec(project), docker_factory=lambda: fake, admit_docker=True)
        .enter()
    )
    record = json.loads(handle._context.record.read_text())
    plan = lifecycle._compose_plan(handle._context, (compose_file,), ("fixture",))
    target_id = "c" * 64
    generation = {"started_at": "started-0", "restart_count": 0}
    record["resources"] = {
        "compose": plan,
        "containers": {
            target_id: {
                "service": "fixture",
                "created": "container-created",
                "image_id": IMAGE_ID,
                "memory_bytes": 2,
                "nano_cpus": 1,
                "restart_policy": "no",
                "labels": plan["labels"],
                "network_ids": [],
                "volume_names": [],
                "bindings": {},
                "state": "running",
                "observation": asdict(
                    lifecycle.ContainerObservation(
                        record["grant"]["daemon_id"], target_id, "started-0", 1, 1, 2, 0, False, 0
                    )
                ),
            }
        },
        "networks": {},
        "volumes": {},
    }
    lifecycle.atomic_json(handle._context.record, record)
    fake.containers[target_id] = _container(target_id, "fixture", plan["labels"], running=True)
    monkeypatch.setattr(lifecycle, "_docker", lambda: fake)
    collect_count = 0

    def collect_with_telemetry_refresh(_docker, data, _plan, _deadline, *, partial):
        nonlocal collect_count
        assert partial is True
        observed = json.loads(json.dumps(data["resources"]))
        observation = observed["containers"][target_id]["observation"]
        collect_count += 1
        observation.update(tick_ns=collect_count + 1, memory_usage_bytes=collect_count + 1)
        if collect_count == 1:
            current = json.loads(handle._context.record.read_text())
            current_observation = current["resources"]["containers"][target_id]["observation"]
            current_observation.update(tick_ns=9, memory_usage_bytes=9)
            current["revision"] += 1
            lifecycle.atomic_json(handle._context.record, current)
        else:
            observed["containers"][target_id]["state"] = "stopped"
        return observed

    monkeypatch.setattr(lifecycle, "_collect_compose_resources", collect_with_telemetry_refresh)

    stopped = handle.control_containers(
        "44444444-4444-4444-8444-444444444444",
        "stop",
        (target_id,),
        {target_id: generation},
        request_sha256="b" * 64,
    )
    stopped_record = json.loads(handle._context.record.read_text())

    def collect_with_authority_change(_docker, data, _plan, _deadline, *, partial):
        assert partial is True
        observed = json.loads(json.dumps(data["resources"]))
        current = json.loads(handle._context.record.read_text())
        current["resources"]["containers"][target_id]["state"] = "running"
        current["revision"] += 1
        lifecycle.atomic_json(handle._context.record, current)
        return observed

    monkeypatch.setattr(lifecycle, "_collect_compose_resources", collect_with_authority_change)

    with pytest.raises(ResourceBusy, match="authority changed before control acceptance"):
        handle.control_containers(
            "55555555-5555-4555-8555-555555555555",
            "start",
            (target_id,),
            stopped,
            request_sha256="c" * 64,
        )

    stopped_record["resources"] = {}
    lifecycle.atomic_json(handle._context.record, stopped_record)
    handle.leave()


def test_compose_start_updates_mutable_container_evidence_without_changing_identity(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    project = tmp_path / "project"
    project.mkdir()
    handle = (
        build_test_services(tmp_path / "state")
        .open_run(_spec(project), docker_factory=lambda: fake, admit_docker=True)
        .enter()
    )
    _ = handle.docker_context
    descriptor = json.loads((handle.private_root / "docker-authority.json").read_text())

    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "create", "fixture"))
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "start", "fixture"))

    record = json.loads(handle._context.record.read_text())
    container = record["owned_resources"]["containers"]["c" * 64]
    assert container["state"] == "running"
    assert container["evidence"]["control"]["started_at"] == "started-0-started"
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "down", "--volumes"))
    handle.leave()


def test_compose_restart_updates_mutable_container_evidence_without_changing_identity(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    project = tmp_path / "project"
    project.mkdir()
    handle = (
        build_test_services(tmp_path / "state")
        .open_run(_spec(project), docker_factory=lambda: fake, admit_docker=True)
        .enter()
    )
    _ = handle.docker_context
    descriptor = json.loads((handle.private_root / "docker-authority.json").read_text())

    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "up", "--detach"))
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "restart", "fixture"))

    record = json.loads(handle._context.record.read_text())
    container = record["owned_resources"]["containers"]["c" * 64]
    assert container["state"] == "running"
    assert container["evidence"]["control"]["started_at"] == "started-0-restarted"
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "down", "--volumes"))
    handle.leave()


def test_exec_runs_in_the_blocking_window_like_compose_run(tmp_path: Path) -> None:
    """A test process started by exec may run as long as one started by ``compose run`` (a 40 min test file)."""
    fake = FakeDockerPort()
    project = tmp_path / "project"
    project.mkdir()
    handle = (
        build_test_services(tmp_path / "state")
        .open_run(_spec(project), docker_factory=lambda: fake, admit_docker=True)
        .enter()
    )
    _ = handle.docker_context
    descriptor = json.loads((handle.private_root / "docker-authority.json").read_text())
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "up", "--detach"))
    (owned,) = json.loads(handle._context.record.read_text())["owned_resources"]["containers"]
    before = len(json.loads(handle._context.record.read_text())["docker_receipts"])

    result = lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "exec", "fixture", "true"))
    compose_exec = fake.timeouts[-1]
    assert result.exit_code == 0
    assert fake.streamed_commands == [fake.commands[-1]]  # container I/O streams through the owned project
    assert len(json.loads(handle._context.record.read_text())["docker_receipts"]) == before  # no mutation receipt
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("container", "exec", owned, "true"))
    container_exec = fake.timeouts[-1]

    assert compose_exec > lifecycle._BLOCKING_DOCKER_SECONDS - 1
    assert container_exec > lifecycle._BLOCKING_DOCKER_SECONDS - 1
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "down", "--volumes"))
    handle.leave()


def test_shim_container_control_recovery_publishes_observed_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = FakeDockerPort()
    fake.compose_services = ("caller", "target")
    project = tmp_path / "project"
    project.mkdir()
    handle = (
        build_test_services(tmp_path / "state")
        .open_run(_spec(project, services=fake.compose_services), docker_factory=lambda: fake, admit_docker=True)
        .enter()
    )
    _ = handle.docker_context
    descriptor = json.loads((handle.private_root / "docker-authority.json").read_text())
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "up", "--detach"))
    target_id = "d" * 64
    generation = {"started_at": "started-0", "restart_count": 0}
    data = json.loads(handle._context.record.read_text())
    operation = {
        "id": "33333333-3333-4333-8333-333333333333",
        "kind": "container-control",
        "generation": data["generation"],
        "revision": data["revision"] + 1,
        "actor": data["guardian"],
        "deadline": asdict(lifecycle.Deadline.after(lifecycle.NativeClock(), 30)),
        "plan": {
            "action": "stop",
            "targets": [target_id],
            "expected": {target_id: generation},
            "request_sha256": "e" * 64,
        },
    }
    data["pending_operation"] = operation
    data["revision"] = operation["revision"]
    lifecycle.atomic_json(handle._context.record, data)  # one atomic replacement: the guardian reads it live
    fake.containers[target_id]["State"]["Running"] = False
    guardian = lifecycle._identity_from_json(data["guardian"])

    with monkeypatch.context() as patcher:
        patcher.setattr(lifecycle, "current_process", lambda _clock: guardian)
        patcher.setattr(lifecycle, "_bound_docker", lambda _data: fake)
        patcher.setattr(lifecycle, "_docker", lambda: fake)
        patcher.setattr(lifecycle, "observe_container", _observe_fake_container)
        lifecycle._reconcile_pending_container_control(handle._context)

    recovered = json.loads(handle._context.record.read_text())
    assert recovered["pending_operation"] is None
    assert recovered["last_operation"]["outcome"] == "failed"
    assert recovered["owned_resources"]["containers"][target_id]["state"] == "stopped"
    assert recovered["infrastructure_failure"] == "container-control-incomplete"
    with pytest.raises(IncompleteCleanup, match="cleanup incomplete"):
        handle.leave("failed")


def test_recovery_reconciles_a_pending_container_control_in_its_operation_phase(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A recoverer settles an interrupted exact stop before it cleans up: the journal is reconciled, not skipped."""
    fake = FakeDockerPort()
    fake.compose_services = ("caller", "target")
    project = tmp_path / "project"
    project.mkdir()
    services = build_test_services(tmp_path / "state")
    handle = services.open_run(
        _spec(project, services=fake.compose_services), docker_factory=lambda: fake, admit_docker=True
    ).enter()
    _ = handle.docker_context
    descriptor = json.loads((handle.private_root / "docker-authority.json").read_text())
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "up", "--detach"))
    target_id = "d" * 64
    data = json.loads(handle._context.record.read_text())
    operation = {
        "id": "44444444-4444-4444-8444-444444444444",
        "kind": "container-control",
        "generation": data["generation"],
        "revision": data["revision"] + 1,
        "actor": data["guardian"],
        "deadline": asdict(lifecycle.Deadline.after(lifecycle.NativeClock(), 30)),
        "plan": {
            "action": "stop",
            "targets": [target_id],
            "expected": {target_id: {"started_at": "started-0", "restart_count": 0}},
            "request_sha256": "e" * 64,
        },
    }
    data["pending_operation"] = operation
    data["revision"] = operation["revision"]
    lifecycle.atomic_json(handle._context.record, data)  # one atomic replacement: the guardian reads it live
    fake.containers[target_id]["State"]["Running"] = False
    kill_guardian(handle._context)
    retire_actor(
        handle._context.project.state, current_identity(), dead_identity(), account=services.root / "admission"
    )  # the requester is gone too

    seen: dict[str, dict] = {}

    def at_receipts_phase(stage: str) -> None:
        if stage == "recovery-phase-receipts":  # the operation phase is over; no cleanup has started
            seen["record"] = json.loads(handle._context.record.read_text())

    install_failpoint(at_receipts_phase)
    try:
        # Docker reaches the recovery through the supported composition seam. One harness boundary is patched:
        # ``observe_container`` talks to the daemon through a real ``docker system dial-stdio`` subprocess
        # (``engine_get``), which no in-process fake port can serve; the file's other container-control tests replace
        # it the same way.
        lifecycle.configure_docker_factories(lambda: fake, lambda _identity: fake)
        monkeypatch.setattr(lifecycle, "observe_container", _observe_fake_container)
        status = lifecycle.recover_run(handle._context, _test_directory=services.root / "admission")
    finally:
        install_failpoint(None)

    settled = seen["record"]  # settled by the operation phase itself, not by the later cleanup
    assert settled["pending_operation"] is None
    assert settled["last_operation"]["id"] == operation["id"] and settled["last_operation"]["outcome"] == "failed"
    assert lifecycle.derive_recovery_phase(settled) == "consumers"
    assert status.state == "complete"


def test_shim_container_control_persists_child_oom_before_refusing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = FakeDockerPort()
    fake.compose_services = ("caller", "target")
    project = tmp_path / "project"
    project.mkdir()
    handle = (
        build_test_services(tmp_path / "state")
        .open_run(_spec(project, services=fake.compose_services), docker_factory=lambda: fake, admit_docker=True)
        .enter()
    )
    _ = handle.docker_context
    descriptor = json.loads((handle.private_root / "docker-authority.json").read_text())
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "up", "--detach"))
    monkeypatch.setattr(lifecycle, "_bound_docker", lambda _data: fake)
    monkeypatch.setattr(lifecycle, "observe_container", _observe_fake_container)
    caller_id, target_id = "c" * 64, "d" * 64
    fake.containers[target_id]["OomEvents"] = 1

    with pytest.raises(ResourceError, match="sticky OOM"):
        handle.register_container_control(
            caller_id,
            {"started_at": "started-0", "restart_count": 0},
            (target_id,),
        )

    recorded = json.loads(handle._context.record.read_text())
    assert recorded["owned_resources"]["containers"][target_id]["evidence"]["control"]["oom_events"] == 1
    assert recorded["infrastructure_failure"] == "container-oom"
    assert (recorded["failure_evidence"]["category"], recorded["failure_evidence"]["first_observed"]["state"]) == (
        "breach",
        "active",
    )
    recorded["owned_resources"]["containers"][target_id]["evidence"]["control"]["oom_events"] = 0
    recorded["infrastructure_failure"] = None
    recorded.pop("failure_evidence")
    lifecycle.atomic_json(handle._context.record, recorded)  # one atomic replacement: the guardian reads it live
    fake.containers[target_id]["OomEvents"] = 0
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "down", "--volumes"))
    handle.leave("failed")


def test_acknowledged_leave_exposes_recorded_infrastructure_category(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    fake = FakeDockerPort()
    handle = build_test_services(tmp_path / "state").open_run(_spec(project), docker_factory=lambda: fake).enter()
    lease = handle._lease
    assert lease is not None
    handle.leave("failed")
    recorded = json.loads(handle._context.record.read_text())
    recorded["infrastructure_failure"] = "synthetic-terminal-failure"
    handle._context.record.write_text(json.dumps(recorded))

    with pytest.raises(InfrastructureFailure, match="synthetic-terminal-failure"):
        lifecycle.leave_run(lease, "failed")


def _evidence_for(reason: str, observer: str, *, seconds_ago: float, **overrides) -> dict:
    """A valid ``failure_evidence`` value first observed ``seconds_ago`` on this process's clock origin."""
    clock = lifecycle.NativeClock()
    evidence = copy.deepcopy(WRITER_ROWS["breach-active"])
    evidence.update(reason=reason, observer=observer, **overrides)
    evidence["first_observed"].update(origin=clock.origin, tick_ns=max(0, clock.read() - int(seconds_ago * 1e9)))
    return evidence


def test_leave_message_names_the_evidence_and_leaves_the_record_untouched(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    fake = FakeDockerPort()
    handle = build_test_services(tmp_path / "state").open_run(_spec(project), docker_factory=lambda: fake).enter()
    lease = handle._lease
    assert lease is not None
    handle.leave("failed")
    recorded = json.loads(handle._context.record.read_text())
    recorded["infrastructure_failure"] = "admission-swap-growth"
    recorded["failure_evidence"] = _evidence_for("admission-swap-growth", "host", seconds_ago=2)
    handle._context.record.write_text(json.dumps(recorded))
    before = handle._context.record.read_bytes()

    with pytest.raises(InfrastructureFailure) as raised:
        lifecycle.leave_run(lease, "failed")

    message = str(raised.value)
    assert "admission-swap-growth" in message and "observer host" in message
    assert "first failure breach" in message  # the evidence category, which a breach does not share with its reason
    assert re.search(r"first observed [2-9] s before this report", message)
    assert "state active" in message and "phase tests" in message
    assert handle._context.record.read_bytes() == before  # nothing is written when the failure is surfaced


def test_leave_message_states_when_the_clock_origin_changed(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    handle = build_test_services(tmp_path / "state").open_run(_spec(project), docker_factory=FakeDockerPort).enter()
    lease = handle._lease
    assert lease is not None
    handle.leave("failed")
    recorded = json.loads(handle._context.record.read_text())
    recorded["infrastructure_failure"] = "observation-late"
    recorded["failure_evidence"] = _evidence_for(
        "observation-late", "engine", seconds_ago=1, category="observation-late"
    )
    recorded["failure_evidence"]["first_observed"]["origin"] = "another-boot"
    handle._context.record.write_text(json.dumps(recorded))

    with pytest.raises(InfrastructureFailure, match=r"interval unavailable \(clock origin changed\)"):
        lifecycle.leave_run(lease, "failed")


@pytest.mark.parametrize(
    ("failure", "evidence", "expected"),
    (
        ("observation-late", ("observation-late", "engine", "observation-late"), "(observation-late, observer engine)"),
        (
            "guardian-lost",
            ("observation-late", "engine", "observation-late"),
            "(guardian-lost; first observed observation-late, observer engine)",
        ),
        ("synthetic-failure", None, "(synthetic-failure)"),
    ),
    ids=("matching-evidence", "later-overwrite", "no-evidence"),
)
def test_lease_message_names_the_recorded_cause(
    tmp_path: Path, failure: str, evidence: tuple[str, str, str] | None, expected: str
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    fake = FakeDockerPort()
    handle = (
        build_test_services(tmp_path / "state")
        .open_run(_spec(project), docker_factory=lambda: fake, admit_docker=True)
        .enter()
    )
    recorded = json.loads(handle._context.record.read_text())
    recorded["infrastructure_failure"] = failure
    if evidence is not None:
        reason, observer, category = evidence
        recorded["failure_evidence"] = _evidence_for(reason, observer, seconds_ago=1, category=category)
    lifecycle.atomic_json(handle._context.record, recorded)  # one atomic replacement: the guardian reads it live

    # a lease-facade operation: the shim dispatch checks descriptor authority only and never reaches the lease check
    with pytest.raises(ResourceError) as raised:
        lifecycle.materialize_docker_authority(handle._lease, fake)

    assert f"Docker operation refused: managed infrastructure already failed {expected}" in str(raised.value)


def test_lease_message_stays_generic_without_a_recorded_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    fake = FakeDockerPort()
    handle = (
        build_test_services(tmp_path / "state")
        .open_run(_spec(project), docker_factory=lambda: fake, admit_docker=True)
        .enter()
    )
    monkeypatch.setattr(lifecycle, "source_identity", lambda: "f" * 64)  # a mismatch unrelated to any failure

    with pytest.raises(ResourceError, match="Docker operation lease differs from current execution authority"):
        lifecycle.materialize_docker_authority(handle._lease, fake)


def test_exact_container_removal_retires_its_endpoint(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    project = tmp_path / "project"
    project.mkdir()
    handle = (
        build_test_services(tmp_path / "state")
        .open_run(_spec(project), docker_factory=lambda: fake, admit_docker=True)
        .enter()
    )
    _ = handle.docker_context
    descriptor = json.loads((handle.private_root / "docker-authority.json").read_text())
    fake.compose_ports = {"8080/tcp": [{"HostIp": "127.0.0.1", "HostPort": "28080"}]}
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "up", "--detach"))
    endpoint = handle.publish_endpoint("fixture", "http", 8080)

    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("container", "rm", endpoint.container_id))

    record = json.loads(handle._context.record.read_text())
    assert not record["endpoints"]["fixture"][-1]["active"]
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "down", "--volumes"))
    handle.leave()


def test_exact_container_stop_retires_its_endpoint_for_retention(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    project = tmp_path / "project"
    project.mkdir()
    handle = (
        build_test_services(tmp_path / "state")
        .open_run(_spec(project), docker_factory=lambda: fake, admit_docker=True)
        .enter()
    )
    _ = handle.docker_context
    descriptor = json.loads((handle.private_root / "docker-authority.json").read_text())
    fake.compose_ports = {"8080/tcp": [{"HostIp": "127.0.0.1", "HostPort": "28080"}]}
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "up", "--detach"))
    endpoint = handle.publish_endpoint("fixture", "http", 8080)

    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("container", "stop", endpoint.container_id))

    record = json.loads(handle._context.record.read_text())
    assert not record["endpoints"]["fixture"][-1]["active"]
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "down", "--volumes"))
    handle.leave()


def _entered_run(tmp_path: Path, fake: FakeDockerPort):
    project = tmp_path / "project"
    project.mkdir()
    handle = build_test_services(tmp_path / "state").open_run(_spec(project), docker_factory=lambda: fake).enter()
    _ = handle.docker_context
    descriptor = json.loads((handle.private_root / "docker-authority.json").read_text())
    return handle, descriptor


class _FailedRender:
    """A Compose client whose render fails, printing a resolved value on stdout and its error on stderr."""

    def __init__(self, stderr: str) -> None:
        self.stderr = stderr

    def execute(self, argv: tuple[str, ...], *, timeout_seconds: float) -> DockerResult:
        return DockerResult(1, "SECRET=hunter2\n", self.stderr)


@pytest.mark.parametrize(
    ("stderr", "expected"),
    (
        ("", "Compose configuration could not be rendered before mutation."),
        (
            "yaml: line 3: mapping values are not allowed\n",
            "Compose configuration could not be rendered before mutation; compose said: "
            "yaml: line 3: mapping values are not allowed",
        ),
    ),
)
def test_a_failed_render_names_composes_stderr_never_its_stdout(stderr: str, expected: str) -> None:
    command = ClassifiedDockerCommand(
        "compose", ("compose", "--project-name", "p", "up"), {"subcommand_index": 3, "subcommand": "up"}
    )

    with pytest.raises(InvalidInput) as raised:
        lifecycle._prepare_compose_receipt(_FailedRender(stderr), command, {})

    assert str(raised.value) == expected


class _TimedOutNetworkCreate(FakeDockerPort):
    """A daemon whose network creation outlives the dispatch deadline, as the adapter reports it when relaying."""

    def execute(self, argv: tuple[str, ...], **options) -> DockerResult:
        if argv[:2] == ("network", "create"):
            raise InfrastructureFailure(
                "Docker operation did not complete within its bounded execution; stderr ended: fatal: registry"
            )
        return super().execute(argv, **options)


def test_a_timed_out_mutation_names_the_stderr_tail_and_stays_unresolved(tmp_path: Path) -> None:
    fake = _TimedOutNetworkCreate()
    handle, descriptor = _entered_run(tmp_path, fake)

    with pytest.raises(IncompleteCleanup, match=r"the command did not finish: .*stderr ended: fatal: registry"):
        lifecycle.dispatch_docker(handle._context, descriptor, fake, ("network", "create"))

    receipt = list(json.loads(handle._context.record.read_text())["docker_receipts"].values())[-1]
    assert (receipt["state"], receipt["result"]["exit_code"]) == ("unresolved", 5)


def _prepare_owned_removal(
    handle, descriptor: dict, fake: FakeDockerPort, kind: str
) -> tuple[str, str, tuple[str, ...]]:
    if kind == "container":
        lifecycle.dispatch_docker(handle._context, descriptor, fake, ("run", IMAGE_ID, "true"))
    elif kind == "network":
        lifecycle.dispatch_docker(handle._context, descriptor, fake, ("network", "create"))
    elif kind == "volume":
        lifecycle.dispatch_docker(handle._context, descriptor, fake, ("volume", "create"))
    elif kind == "builder":
        lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "create"))
    else:
        lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "create"))
        lifecycle.dispatch_docker(
            handle._context,
            descriptor,
            fake,
            ("buildx", "build", "--tag", "fixture:latest", "."),
        )
    plural = "builders" if kind == "builder" else f"{kind}s"
    record = json.loads(handle._context.record.read_text())
    target = next(key for key, item in record["owned_resources"][plural].items() if item["state"] != "removed")
    if kind == "builder":
        command = ("buildx", "rm", target)
    elif kind == "container":
        command = ("container", "rm", "-f", "-v", target)
    elif kind == "image":
        command = ("image", "rm", "fixture:latest")
    else:
        command = (kind, "rm", target)
    return plural, target, command


@pytest.mark.parametrize(
    ("container_bytes", "daemon_cpus", "command"),
    [
        (2 * 1024**3 - 1, 2, ("buildx", "create")),
        (2 * 1024**3, 1, ("buildx", "create")),
        (2 * 1024**3 - 1, 2, ("buildx", "build", "--tag", "fixture:latest", ".")),
        (2 * 1024**3, 1, ("buildx", "build", "--tag", "fixture:latest", ".")),
    ],
)
def test_builder_requires_the_calibrated_phase_envelope_before_mutation(
    tmp_path: Path, container_bytes: int, daemon_cpus: int, command: tuple[str, ...]
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    fake = FakeDockerPort()
    handle = (
        build_test_services(tmp_path / "state")
        .open_run(
            _spec(project, container_bytes=container_bytes, daemon_cpus=daemon_cpus),
            docker_factory=lambda: fake,
        )
        .enter()
    )
    _ = handle.docker_context
    descriptor = json.loads((handle.private_root / "docker-authority.json").read_text())

    with pytest.raises(InvalidInput, match="builder envelope"):
        lifecycle.dispatch_docker(handle._context, descriptor, fake, command)

    assert not any(item[:2] == command[:2] for item in fake.commands)
    handle.leave()


def test_builder_config_is_private_and_binds_single_solver_parallelism(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)

    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "create"))

    config = handle.private_root / "buildkitd.toml"
    assert config.read_text() == "[worker.oci]\n  max-parallelism = 1\n"
    assert config.stat().st_mode & 0o777 == 0o600
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "rm", handle.docker_context.builder))
    handle.leave()


def test_builder_config_refuses_a_matching_symlink_before_docker(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)
    outside = tmp_path / "outside.toml"
    outside.write_text("[worker.oci]\n  max-parallelism = 1\n")
    outside.chmod(0o600)
    (handle.private_root / "buildkitd.toml").symlink_to(outside)

    with pytest.raises(InvalidInput, match="configuration identity"):
        lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "create"))

    assert not any(command[:2] == ("buildx", "create") for command in fake.commands)


@pytest.mark.parametrize(
    "override",
    [
        {"Memory": 0},
        {"MemorySwap": 0},
        {"CpuPeriod": 0},
        {"CpuQuota": 0},
        {"RestartPolicy": {"Name": "always"}},
    ],
)
def test_builder_create_records_cleanup_for_each_invalid_effective_limit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, override: dict[str, object]
) -> None:
    fake = FakeDockerPort()
    fake.builder_host_overrides = override
    monkeypatch.setattr(lifecycle, "_BOUND_DOCKER_FACTORY", lambda _identity: fake)
    handle, descriptor = _entered_run(tmp_path, fake)

    with pytest.raises(InfrastructureFailure, match="effective limits"):
        lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "create"))

    record = json.loads(handle._context.record.read_text())
    assert record["owned_resources"]["builders"]
    assert record["owned_resources"]["containers"]
    lifecycle._cleanup_owned_docker_resources(handle._context)
    with pytest.raises(InfrastructureFailure, match="builder-envelope"):
        handle.leave("failed")
    assert json.loads(handle._context.record.read_text())["cleanup_outcome"] == "complete"
    assert fake.builders == {}
    assert fake.containers == {}


def test_builder_create_records_cleanup_when_bootstrap_worker_is_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = FakeDockerPort()
    fake.builder_worker_created = False
    monkeypatch.setattr(lifecycle, "_BOUND_DOCKER_FACTORY", lambda _identity: fake)
    handle, descriptor = _entered_run(tmp_path, fake)

    with pytest.raises(InfrastructureFailure, match="effective limits"):
        lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "create"))

    record = json.loads(handle._context.record.read_text())
    assert record["owned_resources"]["builders"]
    assert record["owned_resources"]["containers"] == {}
    lifecycle._cleanup_owned_docker_resources(handle._context)
    with pytest.raises(InfrastructureFailure, match="builder-envelope"):
        handle.leave("failed")
    assert json.loads(handle._context.record.read_text())["cleanup_outcome"] == "complete"
    assert fake.builders == {}


def test_build_receipt_normalizes_buildx_metadata_permissions(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    fake.build_metadata_mode = 0o644
    handle, descriptor = _entered_run(tmp_path, fake)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "create"))

    lifecycle.dispatch_docker(
        handle._context,
        descriptor,
        fake,
        ("buildx", "build", "--tag", "fixture:latest", "."),
    )

    record = json.loads(handle._context.record.read_text())
    build = next(receipt for receipt in record["docker_receipts"].values() if receipt["kind"] == "build")
    assert build["state"] == "promoted"
    assert Path(build["plan"]["metadata"]).stat().st_mode & 0o777 == 0o600
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("image", "rm", BUILT_IMAGE_ID))
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "rm", handle.docker_context.builder))
    handle.leave()


def test_build_receipt_accepts_missing_optional_created_metadata(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    fake.omit_build_created = True
    handle, descriptor = _entered_run(tmp_path, fake)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "create"))

    result = lifecycle.dispatch_docker(
        handle._context,
        descriptor,
        fake,
        ("buildx", "build", "--tag", "fixture:latest", "."),
    )
    record = json.loads(handle._context.record.read_text())

    assert result.exit_code == 0
    assert record["owned_resources"]["images"][BUILT_IMAGE_ID]["evidence"]["created"] is None
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("image", "rm", BUILT_IMAGE_ID))
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "rm", handle.docker_context.builder))
    handle.leave()


def test_build_promotion_accepts_the_planned_repository_digest(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    repository_digest = f"fixture@{BUILT_IMAGE_ID}"
    fake.build_extra_reference = ("RepoDigests", repository_digest)
    handle, descriptor = _entered_run(tmp_path, fake)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "create"))

    lifecycle.dispatch_docker(
        handle._context,
        descriptor,
        fake,
        ("buildx", "build", "--tag", "fixture:latest", "."),
    )

    evidence = json.loads(handle._context.record.read_text())["owned_resources"]["images"][BUILT_IMAGE_ID]["evidence"]
    assert evidence["repo_digests"] == [repository_digest]
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("image", "rm", BUILT_IMAGE_ID))
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "rm", handle.docker_context.builder))
    handle.leave()


@pytest.mark.parametrize(
    ("field", "foreign_reference"),
    [
        ("RepoTags", "foreign:latest"),
        ("RepoDigests", "foreign@sha256:" + "6" * 64),
    ],
)
def test_build_promotion_refuses_unrequested_references(
    tmp_path: Path,
    field: str,
    foreign_reference: str,
) -> None:
    fake = FakeDockerPort()
    fake.build_extra_reference = (field, foreign_reference)
    handle, descriptor = _entered_run(tmp_path, fake)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "create"))

    with pytest.raises(IncompleteCleanup, match="unbound resource"):
        lifecycle.dispatch_docker(
            handle._context,
            descriptor,
            fake,
            ("buildx", "build", "--tag", "fixture:latest", "."),
        )

    record = json.loads(handle._context.record.read_text())
    assert record["owned_resources"]["images"] == {}
    assert fake.images[foreign_reference]["Id"] == BUILT_IMAGE_ID


def test_multi_tag_build_cleanup_removes_each_exact_owned_reference(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "create"))
    lifecycle.dispatch_docker(
        handle._context,
        descriptor,
        fake,
        ("buildx", "build", "--tag", "fixture:first", "--tag", "fixture:second", "."),
    )

    result = lifecycle.dispatch_docker(handle._context, descriptor, fake, ("image", "rm", BUILT_IMAGE_ID))
    record = json.loads(handle._context.record.read_text())
    receipt = next(item for item in record["docker_receipts"].values() if item["kind"] == "image-rm")

    assert result.exit_code == 0
    assert receipt["plan"] == {
        "image_id": BUILT_IMAGE_ID,
        "references": ["fixture:first", "fixture:second"],
    }
    assert BUILT_IMAGE_ID not in fake.images
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "rm", handle.docker_context.builder))
    handle.leave()


def test_multi_tag_cleanup_preserves_a_foreign_container_using_the_image(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)
    monkeypatch.setattr(lifecycle, "_BOUND_DOCKER_FACTORY", lambda _identity: fake)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "create"))
    lifecycle.dispatch_docker(
        handle._context,
        descriptor,
        fake,
        ("buildx", "build", "--tag", "fixture:first", "--tag", "fixture:second", "."),
    )
    foreign_id = "d" * 64
    fake.containers[foreign_id] = _container(foreign_id, "foreign", {}, image_id=BUILT_IMAGE_ID)

    with pytest.raises(IncompleteCleanup, match=r"unbound resource \(image-rm: its result could not be reconciled\)"):
        lifecycle.dispatch_docker(handle._context, descriptor, fake, ("image", "rm", BUILT_IMAGE_ID))

    assert fake.images["fixture:first"]["Id"] == BUILT_IMAGE_ID
    assert fake.images["fixture:second"]["Id"] == BUILT_IMAGE_ID
    assert fake.containers[foreign_id]["Image"] == BUILT_IMAGE_ID
    assert not any("--force" in command for command in fake.commands if command[:2] == ("image", "rm"))
    fake.containers.pop(foreign_id)
    lifecycle._reconcile_docker_receipt(handle._context)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "rm", handle.docker_context.builder))
    handle.leave()


def test_multi_tag_recovery_accepts_nonzero_after_removing_the_surviving_reference(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)
    monkeypatch.setattr(lifecycle, "_BOUND_DOCKER_FACTORY", lambda _identity: fake)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "create"))
    lifecycle.dispatch_docker(
        handle._context,
        descriptor,
        fake,
        ("buildx", "build", "--tag", "fixture:first", "--tag", "fixture:second", "."),
    )
    fake.fail_before_remove_kind = "image"
    # The message names the step, so a field failure says whether the command or its reconciliation broke.
    with pytest.raises(
        IncompleteCleanup,
        # an operating-system error is named by its type only: its text may carry paths
        match=r"\(image-rm: the command did not finish: OSError\)",
    ):
        lifecycle.dispatch_docker(handle._context, descriptor, fake, ("image", "rm", BUILT_IMAGE_ID))
    image = fake.images.pop("fixture:first")
    image["RepoTags"] = ["fixture:second"]

    lifecycle._reconcile_docker_receipt(handle._context)

    receipt = next(
        item
        for item in json.loads(handle._context.record.read_text())["docker_receipts"].values()
        if item["kind"] == "image-rm"
    )
    assert receipt["state"] == "removed"
    assert receipt["result"] == {"category": "child_failure", "exit_code": 1}
    assert BUILT_IMAGE_ID not in fake.images
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "rm", handle.docker_context.builder))
    handle.leave()


def test_cleanup_accepts_null_repo_tags_after_every_owned_reference_disappears(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "create"))
    lifecycle.dispatch_docker(
        handle._context,
        descriptor,
        fake,
        ("buildx", "build", "--tag", "fixture:latest", "."),
    )
    image = fake.images.pop("fixture:latest")
    image["RepoTags"] = None

    result = lifecycle.dispatch_docker(handle._context, descriptor, fake, ("image", "rm", BUILT_IMAGE_ID))

    assert result.exit_code == 1
    assert (
        json.loads(handle._context.record.read_text())["owned_resources"]["images"][BUILT_IMAGE_ID]["state"]
        == "removed"
    )
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "rm", handle.docker_context.builder))
    handle.leave()


def test_built_image_cleanup_refuses_a_recorded_tag_reassigned_to_a_foreign_image(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "create"))
    lifecycle.dispatch_docker(
        handle._context,
        descriptor,
        fake,
        ("buildx", "build", "--tag", "fixture:latest", "."),
    )
    built = fake.images[BUILT_IMAGE_ID]
    built["RepoTags"] = []
    foreign = {
        "Id": IMAGE_ID,
        "Created": "foreign-created",
        "Config": {"Labels": {}},
        "RepoTags": ["fixture:latest"],
        "RepoDigests": [],
    }
    fake.images["fixture:latest"] = foreign
    fake.images[IMAGE_ID] = foreign

    with pytest.raises(ResourceError, match="image identity changed"):
        lifecycle.dispatch_docker(handle._context, descriptor, fake, ("image", "rm", BUILT_IMAGE_ID))

    assert fake.images["fixture:latest"] is foreign


def test_legacy_build_record_uses_its_recorded_tags_for_exact_cleanup(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "create"))
    lifecycle.dispatch_docker(
        handle._context,
        descriptor,
        fake,
        ("buildx", "build", "--tag", "fixture:first", "--tag", "fixture:second", "."),
    )
    record = json.loads(handle._context.record.read_text())
    evidence = record["owned_resources"]["images"][BUILT_IMAGE_ID]["evidence"]
    evidence.pop("repo_tags")
    evidence.pop("repo_digests")
    lifecycle.atomic_json(handle._context.record, record)  # one atomic replacement: the guardian reads it live

    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("image", "rm", BUILT_IMAGE_ID))

    receipt = next(
        item
        for item in json.loads(handle._context.record.read_text())["docker_receipts"].values()
        if item["kind"] == "image-rm"
    )
    assert receipt["plan"]["references"] == ["fixture:first", "fixture:second"]
    assert ("image", "rm", "fixture:first", "fixture:second") in fake.commands


@pytest.mark.parametrize(
    ("field", "foreign_reference"),
    [
        ("RepoTags", "foreign:latest"),
        ("RepoDigests", "foreign@sha256:" + "7" * 64),
    ],
)
def test_built_image_cleanup_refuses_foreign_references(
    tmp_path: Path,
    field: str,
    foreign_reference: str,
) -> None:
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "create"))
    lifecycle.dispatch_docker(
        handle._context,
        descriptor,
        fake,
        ("buildx", "build", "--tag", "fixture:latest", "."),
    )
    image = fake.images[BUILT_IMAGE_ID]
    image[field] = [foreign_reference]
    fake.images[foreign_reference] = image

    with pytest.raises(ResourceError, match="image identity changed"):
        lifecycle.dispatch_docker(handle._context, descriptor, fake, ("image", "rm", BUILT_IMAGE_ID))

    assert fake.images[foreign_reference] is image
    fake.images.pop(foreign_reference)
    image["RepoTags"] = ["fixture:latest"]
    image["RepoDigests"] = []
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("image", "rm", BUILT_IMAGE_ID))
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "rm", handle.docker_context.builder))
    handle.leave()


def test_preflight_refusal_terminalizes_its_receipt(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)

    with pytest.raises(InvalidInput, match="owned builder"):
        lifecycle.dispatch_docker(
            handle._context,
            descriptor,
            fake,
            ("buildx", "build", "--tag", "fixture:latest", "."),
        )

    record = json.loads(handle._context.record.read_text())
    receipt = next(iter(record["docker_receipts"].values()))
    assert receipt["state"] == "failed"
    assert receipt["result"] == {"category": "child_failure", "exit_code": 2}
    handle.leave("failed")


def test_container_create_revalidates_its_declared_network_before_launch(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)
    fake.networks.clear()
    before = list(fake.commands)

    with pytest.raises(InvalidInput, match="network identity changed"):
        lifecycle.dispatch_docker(
            handle._context,
            descriptor,
            fake,
            ("run", "--network=" + "e" * 64, IMAGE_ID, "true"),
        )

    assert fake.commands == before
    receipt = next(iter(json.loads(handle._context.record.read_text())["docker_receipts"].values()))
    assert receipt["state"] == "failed"
    handle.leave("failed")


def test_blocking_container_run_uses_the_long_streaming_window(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)

    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("run", "--rm", IMAGE_ID, "true"))

    assert fake.timeouts[-1] > lifecycle._BLOCKING_DOCKER_SECONDS - 1
    assert fake.streamed_commands == [fake.commands[-1]]
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("container", "rm", "a" * 64))
    assert fake.timeouts[-1] <= 30
    handle.leave()


def test_a_failed_reconciliation_names_its_step(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "create"))
    fake.fail_image_inspection_after_build = True

    with pytest.raises(
        IncompleteCleanup, match=r"\(build: reconciling its result failed: synthetic image inspection failure\)"
    ):
        lifecycle.dispatch_docker(
            handle._context, descriptor, fake, ("buildx", "build", "--tag", "fixture:latest", ".")
        )

    receipts = json.loads(handle._context.record.read_text())["docker_receipts"].values()
    assert [receipt["state"] for receipt in receipts if receipt["kind"] == "build"] == ["unresolved"]


def test_pre_child_infrastructure_failure_terminalizes_its_receipt(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)
    fake.fail_image_inspection = True
    before = list(fake.commands)

    with pytest.raises(ResourceError):
        lifecycle.dispatch_docker(handle._context, descriptor, fake, ("run", IMAGE_ID, "true"))

    assert fake.commands == before
    receipt = next(iter(json.loads(handle._context.record.read_text())["docker_receipts"].values()))
    assert receipt["state"] == "failed"
    assert receipt["result"] == {"category": "child_failure", "exit_code": 5}
    handle.leave("failed")


def test_compose_down_reconciles_removed_containers_and_keeps_live_volumes(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)

    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "up", "--detach"))
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "down"))
    record = json.loads(handle._context.record.read_text())

    assert {item["state"] for item in record["owned_resources"]["containers"].values()} == {"removed"}
    assert {item["state"] for item in record["owned_resources"]["volumes"].values()} == {"present"}
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "down", "--volumes"))
    assert handle.leave().cleanup_outcome == "complete"


def test_compose_can_recreate_a_verified_removed_named_volume(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)

    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "up", "--detach"))
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "down", "--volumes"))
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "up", "--detach"))
    record = json.loads(handle._context.record.read_text())
    volume = next(iter(record["owned_resources"]["volumes"].values()))

    assert volume["state"] == "present"
    assert volume["evidence"]["created"] == "compose-volume-2"
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "down", "--volumes"))
    assert handle.leave().cleanup_outcome == "complete"


def test_compose_run_is_receipted_and_leaves_no_one_off_container(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "up", "--detach"))
    before = set(fake.containers)

    result = lifecycle.dispatch_docker(
        handle._context,
        descriptor,
        fake,
        (
            "compose",
            "run",
            "--rm",
            "--no-deps",
            "--entrypoint",
            "true",
            "fixture",
            "true",
        ),
    )
    record = json.loads(handle._context.record.read_text())
    receipt = list(record["docker_receipts"].values())[-1]
    command = next(item for item in fake.commands if item[3:4] == ("run",))

    assert result.exit_code == 0
    assert receipt["kind"] == "compose-run"
    assert receipt["state"] == "promoted"
    assert set(fake.containers) == before
    assert command[command.index("--pull") + 1] == "never"
    assert command[command.index("--name") + 1] == receipt["plan"]["name"]
    assert fake.timeouts[fake.commands.index(command)] > lifecycle._BLOCKING_DOCKER_SECONDS - 1
    assert fake.streamed_commands[-1] == command

    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "down", "--volumes"))
    assert handle.leave().cleanup_outcome == "complete"


def test_compose_run_failure_without_a_residual_container_is_terminal(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    fake.compose_run_exit_code = 23
    handle, descriptor = _entered_run(tmp_path, fake)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "up", "--detach"))

    result = lifecycle.dispatch_docker(
        handle._context,
        descriptor,
        fake,
        ("compose", "run", "--rm", "--no-deps", "--entrypoint", "false", "fixture", "false"),
    )
    receipt = list(json.loads(handle._context.record.read_text())["docker_receipts"].values())[-1]

    assert result.exit_code == 23
    assert receipt["state"] == "failed"
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "down", "--volumes"))
    assert handle.leave().cleanup_outcome == "complete"


def test_compose_run_promotes_an_exact_retained_one_off_for_cleanup(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    fake.compose_run_retained = True
    handle, descriptor = _entered_run(tmp_path, fake)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "up", "--detach"))

    lifecycle.dispatch_docker(
        handle._context,
        descriptor,
        fake,
        ("compose", "run", "--rm", "--no-deps", "--entrypoint", "true", "fixture", "true"),
    )
    record = json.loads(handle._context.record.read_text())
    retained = record["owned_resources"]["containers"]["f" * 64]

    assert retained["name"] == list(record["docker_receipts"].values())[-1]["plan"]["name"]
    assert retained["state"] == "stopped"
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "down", "--volumes"))
    assert handle.leave().cleanup_outcome == "complete"


def test_compose_run_refuses_a_generated_name_collision_before_execution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "up", "--detach"))
    operation_id = "33333333-3333-3333-3333-333333333333"
    monkeypatch.setattr(lifecycle.uuid, "uuid4", lambda: operation_id)
    generated_name = f"dtt-{handle.run_id.replace('-', '')[:12]}-333333333333"
    fake.containers["1" * 64] = _container("1" * 64, generated_name, {})

    with pytest.raises(InvalidInput, match="reconciliation name"):
        lifecycle.dispatch_docker(
            handle._context,
            descriptor,
            fake,
            ("compose", "run", "--rm", "--no-deps", "--entrypoint", "true", "fixture", "true"),
        )

    assert not any(command[3:4] == ("run",) for command in fake.commands)
    fake.containers.pop("1" * 64)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "down", "--volumes"))
    assert handle.leave("failed").cleanup_outcome == "complete"


_COMPOSE_RUN = ("compose", "run", "--rm", "--no-deps", "--entrypoint", "true", "fixture", "true")


def _recovering_run(tmp_path: Path, fake: FakeDockerPort, *, retain: bool = False):
    """An entered run with one exposed-port service and a completed ``compose up``."""
    if not fake.compose_ports:
        fake.compose_ports = {"8080/tcp": None}  # exposed, not published: recorded as ``{"8080/tcp": []}``
    project = tmp_path / "project"
    project.mkdir()
    services = build_test_services(tmp_path / "state")
    handle = services.open_run(_spec(project, retain=retain), docker_factory=lambda: fake, admit_docker=True).enter()
    _ = handle.docker_context
    descriptor = json.loads((handle.private_root / "docker-authority.json").read_text())
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "up", "--detach"))
    return services, handle, descriptor


def _record(handle) -> dict:
    return json.loads(handle._context.record.read_text())


def _edit_record(handle, change: Callable[[dict], object]) -> dict:
    """Apply ``change`` to the run record in one atomic replacement (the guardian reads it live); return the record."""
    data = _record(handle)
    change(data)
    lifecycle.atomic_json(handle._context.record, data)
    return data


def _reopen_last_receipt(handle) -> dict:
    """Leave the last receipt as a crash between dispatch and finish leaves it: ``pending``, nothing recorded."""

    def reopen(data: dict) -> None:
        list(data["docker_receipts"].values())[-1].update(state="pending", result=None, resources=[])
        data["revision"] += 1

    return list(_edit_record(handle, reopen)["docker_receipts"].values())[-1]


def test_recovery_finishes_a_pending_compose_run_after_its_siblings_stopped(tmp_path: Path) -> None:
    """The field case: killed during ``compose run``, the engine stopped every container, then ``run recover``."""
    fake = FakeDockerPort()
    services, handle, descriptor = _recovering_run(tmp_path, fake)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, _COMPOSE_RUN)
    receipt = _reopen_last_receipt(handle)
    fake.stop_container("c" * 64)  # an exited container reports ``Ports {}``
    abandon_run(handle._context, services.root / "admission")

    lifecycle.configure_docker_factories(lambda: fake, lambda _identity: fake)
    status = lifecycle.recover_run(handle._context, _test_directory=services.root / "admission")

    record = json.loads(handle._context.record.read_text())
    assert status.state == "complete"
    assert lifecycle.quiescence_blockers(record) == ()
    assert record["docker_receipts"][receipt["id"]]["state"] not in {"pending", "unresolved"}
    assert all(item["state"] == "removed" for items in record["owned_resources"].values() for item in items.values())
    assert "c" * 64 not in fake.containers


def _stop_sibling(fake: FakeDockerPort) -> None:
    fake.stop_container("c" * 64)


def _restart_sibling(fake: FakeDockerPort) -> None:
    fake.containers["c" * 64]["State"]["StartedAt"] = "started-1"
    fake.containers["c" * 64]["RestartCount"] = 1


@pytest.mark.parametrize(
    ("disturb", "expected"),
    [
        (_stop_sibling, {"state": "stopped", "bindings": {}, "started_at": "started-0", "restart_count": 0}),
        (
            _restart_sibling,
            {"state": "running", "bindings": {"8080/tcp": []}, "started_at": "started-1", "restart_count": 1},
        ),
    ],
)
def test_live_compose_run_accepts_a_sibling_whose_state_changed(
    tmp_path: Path, disturb: Callable[[FakeDockerPort], None], expected: dict
) -> None:
    fake = FakeDockerPort()
    _services, handle, descriptor = _recovering_run(tmp_path, fake)
    disturb(fake)

    result = lifecycle.dispatch_docker(handle._context, descriptor, fake, _COMPOSE_RUN)

    record = _record(handle)
    sibling = record["owned_resources"]["containers"]["c" * 64]
    control = sibling["evidence"]["control"]
    assert result.exit_code == 0
    assert list(record["docker_receipts"].values())[-1]["state"] == "promoted"
    assert {
        "state": sibling["state"],
        "bindings": sibling["evidence"]["bindings"],
        "started_at": control["started_at"],
        "restart_count": control["restart_count"],
    } == expected
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "down", "--volumes"))
    assert handle.leave().cleanup_outcome == "complete"


@pytest.mark.parametrize(("field", "value"), [("Created", "other-created"), ("Name", "/renamed")])
def test_compose_run_still_refuses_a_changed_container_identity(tmp_path: Path, field: str, value: str) -> None:
    """A different container under a recorded identifier is not the run's: the refusal stays (AC4)."""
    fake = FakeDockerPort()
    services, handle, descriptor = _recovering_run(tmp_path, fake)
    before = json.loads(handle._context.record.read_text())["owned_resources"]["containers"]["c" * 64]
    original = fake.containers["c" * 64][field]
    fake.containers["c" * 64][field] = value

    with pytest.raises(IncompleteCleanup, match="collides with another exact owned identity") as raised:
        lifecycle.dispatch_docker(handle._context, descriptor, fake, _COMPOSE_RUN)

    record = _record(handle)
    assert record["owned_resources"]["containers"]["c" * 64] == before
    keys = ["evidence.created"] if field == "Created" else ["name"]
    assert raised.value.reason == "owned-identity-conflict"
    assert raised.value.details["conflict"] == {
        "kind": "container",
        "identifier": "c" * 64,
        "name": before["name"],
        "keys": keys,
    }
    assert f"Resource: container {'c' * 64}; differs: {keys[0]}." in str(raised.value)
    receipt = list(record["docker_receipts"].values())[-1]
    assert (receipt["state"], receipt["result"]["exit_code"]) == ("unresolved", 5)
    fake.containers["c" * 64][field] = original
    abandon_run(handle._context, services.root / "admission")
    lifecycle.configure_docker_factories(lambda: fake, lambda _identity: fake)
    assert lifecycle.recover_run(handle._context, _test_directory=services.root / "admission").state == "complete"


def test_compose_run_refuses_an_unexpected_project_container_after_execution(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "up", "--detach"))
    record = json.loads(handle._context.record.read_text())
    up = list(record["docker_receipts"].values())[-1]
    project = up["plan"]["project"]
    extra_id = "1" * 64
    fake.containers[extra_id] = _container(
        extra_id,
        f"{project}-unexpected-1",
        {
            "com.docker.compose.project": project,
            "com.docker.compose.service": "fixture",
        },
        image_id=fake.compose_runtime_image_id,
    )
    receipt = {
        **up,
        "kind": "compose-run",
        "plan": {**up["plan"], "name": "expected-one-off", "service": "fixture", "remove": True},
    }

    assert lifecycle._compose_promotion(fake, receipt, DockerResult(0, "", ""), record) == ("unresolved", [])

    fake.containers.pop(extra_id)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "down", "--volumes"))
    assert handle.leave().cleanup_outcome == "complete"


def test_compose_promotes_and_removes_an_image_declared_anonymous_volume(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    fake.compose_image_volume = True
    handle, descriptor = _entered_run(tmp_path, fake)

    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "up", "--detach"))
    record = json.loads(handle._context.record.read_text())
    anonymous = record["owned_resources"]["volumes"]["7" * 64]

    assert anonymous["state"] == "present"
    assert anonymous["evidence"]["compose_project"] == handle.docker_context.compose_project
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "down"))
    record = json.loads(handle._context.record.read_text())
    assert record["owned_resources"]["volumes"]["7" * 64]["state"] == "present"
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "down", "--volumes"))
    assert handle.leave().cleanup_outcome == "complete"


def test_compose_refuses_an_undeclared_local_image_before_receipt(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    fake = FakeDockerPort()
    spec = _spec(project, include_image=False)
    handle = build_test_services(tmp_path / "state").open_run(spec, docker_factory=lambda: fake).enter()
    _ = handle.docker_context
    descriptor = json.loads((handle.private_root / "docker-authority.json").read_text())

    with pytest.raises(InvalidInput, match="declared image"):
        lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "up", "--detach"))

    assert json.loads(handle._context.record.read_text())["docker_receipts"] == {}
    handle.leave("failed")


def test_observation_requires_one_exact_owned_or_external_target(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)

    observed = lifecycle.dispatch_docker(
        handle._context,
        descriptor,
        fake,
        ("inspect", "--format", "{{.Id}}", IMAGE_ID),
    )
    before = list(fake.commands)
    with pytest.raises(InvalidInput, match="exactly one"):
        lifecycle.dispatch_docker(
            handle._context,
            descriptor,
            fake,
            ("image", "inspect", "sha256:" + "0" * 64, IMAGE_ID),
        )

    assert observed.exit_code == 0
    assert fake.commands == before
    handle.leave()


def test_general_inspect_type_cannot_cross_an_owned_resource_kind(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("run", IMAGE_ID, "true"))
    before = list(fake.commands)

    with pytest.raises(InvalidInput, match="owned nor declared"):
        lifecycle.dispatch_docker(
            handle._context,
            descriptor,
            fake,
            ("inspect", "--type", "volume", "a" * 64),
        )

    assert fake.commands == before
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("container", "rm", "a" * 64))
    handle.leave()


def test_compose_config_drift_and_anonymous_volumes_refuse_before_receipt(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "up", "--detach"))
    before = len(json.loads(handle._context.record.read_text())["docker_receipts"])
    fake.compose_config_variant = True

    with pytest.raises(InvalidInput, match="configuration changed"):
        lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "down", "--volumes"))

    assert len(json.loads(handle._context.record.read_text())["docker_receipts"]) == before
    fake.compose_config_variant = False
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "down", "--volumes"))
    handle.leave()

    other = FakeDockerPort()
    other.compose_anonymous_volume = True
    other_root = tmp_path / "other"
    other_root.mkdir()
    other_handle, other_descriptor = _entered_run(other_root, other)
    with pytest.raises(InvalidInput, match="Anonymous"):
        lifecycle.dispatch_docker(
            other_handle._context,
            other_descriptor,
            other,
            ("compose", "up", "--detach"),
        )
    assert json.loads(other_handle._context.record.read_text())["docker_receipts"] == {}
    other_handle.leave("failed")


def test_compose_receipt_infers_one_shot_dependencies_from_native_compose_semantics() -> None:
    config = {
        "services": {
            "init": {"image": "fixture"},
            "server": {
                "image": "fixture",
                "depends_on": {
                    "init": {"condition": "service_completed_successfully"},
                },
            },
        }
    }

    assert lifecycle._completed_compose_services(config) == ("init",)

    config["services"]["server"]["depends_on"]["init"]["condition"] = "service_healthy"
    assert lifecycle._completed_compose_services(config) == ()


def test_builder_cleanup_accepts_an_already_absent_exact_worker(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "create"))
    lifecycle.dispatch_docker(
        handle._context,
        descriptor,
        fake,
        ("buildx", "build", "--tag", "fixture:latest", "."),
    )
    worker_id = "8" * 64
    fake.containers.pop(worker_id)

    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "rm", handle.docker_context.builder))
    record = json.loads(handle._context.record.read_text())

    assert next(iter(record["owned_resources"]["builders"].values()))["state"] == "removed"
    assert record["owned_resources"]["containers"][worker_id]["state"] == "removed"
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("image", "rm", BUILT_IMAGE_ID))
    handle.leave()


def test_builder_cleanup_independently_removes_worker_left_by_buildx(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)
    monkeypatch.setattr(lifecycle, "_BOUND_DOCKER_FACTORY", lambda _identity: fake)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "create"))
    lifecycle.dispatch_docker(
        handle._context,
        descriptor,
        fake,
        ("buildx", "build", "--tag", "fixture:latest", "."),
    )
    fake.leave_buildkit_after_builder_remove = True

    lifecycle._cleanup_owned_docker_resources(handle._context)
    record = json.loads(handle._context.record.read_text())

    assert [
        item["kind"] for item in record["docker_receipts"].values() if item["kind"] in {"builder-rm", "container-rm"}
    ] == ["builder-rm", "container-rm"]
    assert all(
        resource["state"] == "removed"
        for resources in record["owned_resources"].values()
        for resource in resources.values()
    )
    assert "8" * 64 not in fake.containers
    handle.leave()


def test_interrupted_builder_cleanup_reconciles_worker_independently(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)
    monkeypatch.setattr(lifecycle, "_BOUND_DOCKER_FACTORY", lambda _identity: fake)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "create"))
    lifecycle.dispatch_docker(
        handle._context,
        descriptor,
        fake,
        ("buildx", "build", "--tag", "fixture:latest", "."),
    )
    fake.fail_after_builder_remove = True

    with pytest.raises(ResourceError, match="cleanup result is unresolved"):
        lifecycle._cleanup_owned_docker_resources(handle._context)
    lifecycle._reconcile_docker_receipt(handle._context)
    lifecycle._cleanup_owned_docker_resources(handle._context)
    record = json.loads(handle._context.record.read_text())

    assert [item["kind"] for item in record["docker_receipts"].values()].count("builder-rm") == 1
    assert all(
        resource["state"] == "removed"
        for resources in record["owned_resources"].values()
        for resource in resources.values()
    )
    assert "8" * 64 not in fake.containers
    handle.leave()


def test_recovery_retries_an_exact_builder_removal_that_reported_success_without_effect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)
    monkeypatch.setattr(lifecycle, "_BOUND_DOCKER_FACTORY", lambda _identity: fake)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "create"))
    lifecycle.dispatch_docker(
        handle._context,
        descriptor,
        fake,
        ("buildx", "build", "--tag", "fixture:latest", "."),
    )
    fake.keep_builder_after_remove_once = True

    with pytest.raises(ResourceError, match="cleanup result is unresolved"):
        lifecycle._cleanup_owned_docker_resources(handle._context)
    lifecycle._reconcile_docker_receipt(handle._context)
    lifecycle._cleanup_owned_docker_resources(handle._context)
    record = json.loads(handle._context.record.read_text())

    assert [command[:2] for command in fake.commands].count(("buildx", "rm")) == 2
    assert all(
        resource["state"] == "removed"
        for resources in record["owned_resources"].values()
        for resource in resources.values()
    )
    handle.leave()


@pytest.mark.parametrize("kind", ["container", "network", "volume", "image", "builder"])
def test_recovery_retries_each_still_present_exact_owned_removal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
) -> None:
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)
    monkeypatch.setattr(lifecycle, "_BOUND_DOCKER_FACTORY", lambda _identity: fake)
    plural, target, command = _prepare_owned_removal(handle, descriptor, fake, kind)
    fake.fail_before_remove_kind = kind

    with pytest.raises(ResourceError, match="cleanup result is unresolved"):
        lifecycle._cleanup_owned_docker_resources(handle._context)

    interrupted = json.loads(handle._context.record.read_text())
    receipt = next(item for item in interrupted["docker_receipts"].values() if item["state"] == "unresolved")
    assert receipt["kind"] == f"{kind}-rm"
    assert receipt["result"] == {"category": "child_failure", "exit_code": 5}
    assert fake.commands.count(command) == 1

    lifecycle._reconcile_docker_receipt(handle._context)
    lifecycle._cleanup_owned_docker_resources(handle._context)
    recovered = json.loads(handle._context.record.read_text())

    assert fake.commands.count(command) == 2
    assert recovered["owned_resources"][plural][target]["state"] == "removed"
    assert all(
        resource["state"] == "removed"
        for resources in recovered["owned_resources"].values()
        for resource in resources.values()
    )
    handle.leave()


@pytest.mark.parametrize("kind", ["container", "network", "volume", "image", "builder"])
def test_recovery_promotes_each_already_absent_owned_removal_without_replay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
) -> None:
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)
    monkeypatch.setattr(lifecycle, "_BOUND_DOCKER_FACTORY", lambda _identity: fake)
    plural, target, command = _prepare_owned_removal(handle, descriptor, fake, kind)
    fake.fail_before_remove_kind = kind

    with pytest.raises(ResourceError, match="cleanup result is unresolved"):
        lifecycle._cleanup_owned_docker_resources(handle._context)

    stores = {
        "container": fake.containers,
        "network": fake.networks,
        "volume": fake.volumes,
        "image": fake.images,
        "builder": fake.builders,
    }
    removed = stores[kind].pop(target)
    if kind == "image":
        fake.images = {key: item for key, item in fake.images.items() if item is not removed}

    lifecycle._reconcile_docker_receipt(handle._context)
    recovered = json.loads(handle._context.record.read_text())

    assert fake.commands.count(command) == 1
    assert recovered["owned_resources"][plural][target]["state"] == "removed"
    assert not any(item["state"] == "unresolved" for item in recovered["docker_receipts"].values())
    lifecycle._cleanup_owned_docker_resources(handle._context)
    handle.leave()


def test_recovery_replays_digest_addressed_image_removal(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = tmp_path / "project"
    project.mkdir()
    fake = FakeDockerPort()
    fake.images.clear()
    reference = "example/cache@sha256:" + "2" * 64
    handle = (
        build_test_services(tmp_path / "state")
        .open_run(_spec(project, include_image=False), docker_factory=lambda: fake)
        .enter()
    )
    _ = handle.docker_context
    descriptor = json.loads((handle.private_root / "docker-authority.json").read_text())
    monkeypatch.setattr(lifecycle, "_BOUND_DOCKER_FACTORY", lambda _identity: fake)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("pull", reference))
    fake.fail_before_remove_kind = "image"
    command = ("image", "rm", "--force", reference)

    with pytest.raises(ResourceError, match="cleanup result is unresolved"):
        lifecycle._cleanup_owned_docker_resources(handle._context)

    interrupted = json.loads(handle._context.record.read_text())
    receipt = next(item for item in interrupted["docker_receipts"].values() if item["state"] == "unresolved")
    assert receipt["plan"]["reference"] == reference
    assert fake.commands.count(command) == 1

    lifecycle._reconcile_docker_receipt(handle._context)
    recovered = json.loads(handle._context.record.read_text())

    assert fake.commands.count(command) == 2
    assert recovered["owned_resources"]["images"][IMAGE_ID]["state"] == "removed"
    handle.leave()


def test_recovery_bounds_a_repeated_exact_removal_failure_to_one_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)
    monkeypatch.setattr(lifecycle, "_BOUND_DOCKER_FACTORY", lambda _identity: fake)
    _, _, command = _prepare_owned_removal(handle, descriptor, fake, "network")
    fake.fail_before_remove_kind = "network"

    with pytest.raises(ResourceError, match="cleanup result is unresolved"):
        lifecycle._cleanup_owned_docker_resources(handle._context)

    fake.fail_before_remove_kind = "network"
    with pytest.raises(OSError, match="synthetic interruption before network removal"):
        lifecycle._reconcile_docker_receipt(handle._context)

    still_interrupted = json.loads(handle._context.record.read_text())
    assert fake.commands.count(command) == 2
    assert next(item for item in still_interrupted["docker_receipts"].values() if item["state"] == "unresolved")

    lifecycle._reconcile_docker_receipt(handle._context)
    lifecycle._cleanup_owned_docker_resources(handle._context)
    handle.leave()


def test_recovery_refuses_uninspectable_owned_removal_without_replay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)
    monkeypatch.setattr(lifecycle, "_BOUND_DOCKER_FACTORY", lambda _identity: fake)
    _, _, command = _prepare_owned_removal(handle, descriptor, fake, "image")
    fake.fail_before_remove_kind = "image"

    with pytest.raises(ResourceError, match="cleanup result is unresolved"):
        lifecycle._cleanup_owned_docker_resources(handle._context)

    fake.fail_image_inspection = True
    with pytest.raises(ResourceError, match="synthetic image inspection failure"):
        lifecycle._reconcile_docker_receipt(handle._context)

    refused = json.loads(handle._context.record.read_text())
    assert fake.commands.count(command) == 1
    assert next(item for item in refused["docker_receipts"].values() if item["state"] == "unresolved")

    fake.fail_image_inspection = False
    lifecycle._reconcile_docker_receipt(handle._context)
    lifecycle._cleanup_owned_docker_resources(handle._context)
    handle.leave()


def test_recovery_refuses_an_image_receipt_missing_its_exact_reference_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)
    monkeypatch.setattr(lifecycle, "_BOUND_DOCKER_FACTORY", lambda _identity: fake)
    _plural, _target, command = _prepare_owned_removal(handle, descriptor, fake, "image")
    fake.fail_before_remove_kind = "image"

    with pytest.raises(ResourceError, match="cleanup result is unresolved"):
        lifecycle._cleanup_owned_docker_resources(handle._context)
    record = json.loads(handle._context.record.read_text())
    receipt = next(item for item in record["docker_receipts"].values() if item["kind"] == "image-rm")
    receipt["plan"].pop("references")
    lifecycle.atomic_json(handle._context.record, record)  # one atomic replacement: the guardian reads it live
    attempts = fake.commands.count(command)

    with pytest.raises(ResourceError, match="changed exact authority"):
        lifecycle._reconcile_docker_receipt(handle._context)

    assert fake.commands.count(command) == attempts


@pytest.mark.parametrize(
    ("kind", "field", "value", "keys"),
    [
        ("container", "Created", "foreign", ["evidence.created"]),
        ("container", "Id", "foreign", ["identifier"]),
        ("network", "Created", "foreign", ["evidence.created"]),
        ("volume", "CreatedAt", "foreign", ["evidence.created"]),
        ("volume", "Labels", None, ["observed.labels"]),
        ("image", "RepoTags", "foreign", ["evidence.repo_tags"]),
        ("builder", "Driver", "foreign", ["evidence.driver"]),
    ],
)
def test_recovery_refuses_each_changed_owned_removal_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
    field: str,
    value: object,
    keys: list[str],
) -> None:
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)
    monkeypatch.setattr(lifecycle, "_BOUND_DOCKER_FACTORY", lambda _identity: fake)
    plural, target, command = _prepare_owned_removal(handle, descriptor, fake, kind)
    fake.fail_before_remove_kind = kind

    with pytest.raises(ResourceError, match="cleanup result is unresolved"):
        lifecycle._cleanup_owned_docker_resources(handle._context)

    stores = {
        "container": fake.containers,
        "network": fake.networks,
        "volume": fake.volumes,
        "image": fake.images,
        "builder": fake.builders,
    }
    stores[kind][target][field] = value

    with pytest.raises(ResourceError, match="identity changed") as raised:
        lifecycle._reconcile_docker_receipt(handle._context)

    refused = json.loads(handle._context.record.read_text())
    assert raised.value.reason == "owned-identity-conflict"
    assert raised.value.details["conflict"]["keys"] == keys
    assert (raised.value.details["conflict"]["kind"], raised.value.details["conflict"]["identifier"]) == (kind, target)
    assert fake.commands.count(command) == 1
    assert refused["owned_resources"][plural][target]["state"] != "removed"
    assert next(item for item in refused["docker_receipts"].values() if item["state"] == "unresolved")


def test_read_only_container_and_builder_commands_stay_exactly_scoped(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("run", IMAGE_ID, "true"))
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "create"))
    before = list(fake.commands)

    with pytest.raises(InvalidInput, match="owned nor declared"):
        lifecycle.dispatch_docker(handle._context, descriptor, fake, ("container", "logs", "b" * 64))
    with pytest.raises(InvalidInput, match="owned nor declared"):
        lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "inspect", "foreign"))

    assert fake.commands == before
    result = lifecycle.dispatch_docker(
        handle._context,
        descriptor,
        fake,
        ("container", "logs", "--timestamps", "-f", "a" * 64),
    )
    assert result == DockerResult(0, "logs\n", "")
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("container", "rm", "a" * 64))
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "rm", handle.docker_context.builder))
    handle.leave()


def test_python_consumer_borrows_only_after_explicit_full_closure_grant(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    probe = project / "probe.py"
    probe.write_text(
        "import json\n"
        "import docker_test_toolkit as toolkit\n"
        "context = toolkit.current_consumer_run()\n"
        "print(json.dumps(None if context is None else {\n"
        "    'run_id': context.run_id,\n"
        "    'services': list(context.services),\n"
        "    'executable': str(context.docker_context.executable),\n"
        "    'state': context.status().state,\n"
        "}))\n"
    )
    fake = FakeDockerPort()
    handle = (
        build_test_services(tmp_path / "state")
        .open_run(_spec(project, services=("fixture", "cache")), docker_factory=lambda: fake)
        .enter()
    )

    ordinary, ordinary_id = handle.launch_python_consumer(
        "script",
        str(probe),
        (),
        ("fixture", "cache"),
        cwd=project,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    ordinary_stdout, ordinary_stderr = ordinary.communicate(timeout=20)
    assert ordinary.returncode == 0, ordinary_stderr
    assert json.loads(ordinary_stdout) is None
    handle.complete_consumer(ordinary_id)

    borrowed, borrowed_id = handle.launch_python_consumer(
        "script",
        str(probe),
        (),
        ("fixture", "cache"),
        cwd=project,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        borrow_docker=True,
    )
    borrowed_stdout, borrowed_stderr = borrowed.communicate(timeout=20)
    assert borrowed.returncode == 0, borrowed_stderr
    payload = json.loads(borrowed_stdout)
    assert payload == {
        "run_id": handle.run_id,
        "services": ["fixture", "cache"],
        "executable": str(handle.private_root / f"python-{borrowed_id}-docker" / "docker"),
        "state": "active",
    }
    handle.complete_consumer(borrowed_id)
    handle.leave()


def test_python_consumer_refuses_subset_docker_borrow_before_launch(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    probe = project / "probe.py"
    probe.write_text("raise AssertionError('must not launch')\n")
    fake = FakeDockerPort()
    handle = (
        build_test_services(tmp_path / "state")
        .open_run(_spec(project, services=("fixture", "cache")), docker_factory=lambda: fake)
        .enter()
    )

    with pytest.raises(ResourceError, match="entire declared closure"):
        handle.launch_python_consumer(
            "script",
            str(probe),
            (),
            ("fixture",),
            cwd=project,
            borrow_docker=True,
        )

    handle.leave()


def _builder_world(tmp_path: Path) -> tuple[FakeDockerPort, object, dict]:
    project = tmp_path / "project"
    project.mkdir()
    fake = FakeDockerPort()
    handle = build_test_services(tmp_path / "state").open_run(_spec(project), docker_factory=lambda: fake).enter()
    _ = handle.docker_context
    descriptor = json.loads((handle.private_root / "docker-authority.json").read_text())
    return fake, handle, descriptor


def test_builder_promotion_records_its_state_volume_identity(tmp_path: Path) -> None:
    # Retention may later remove exactly this incarnation, never a volume merely named like it.
    fake, handle, descriptor = _builder_world(tmp_path)

    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "create"))

    record = json.loads(handle._context.record.read_text())
    name = handle.docker_context.builder
    assert record["owned_resources"]["builders"][name]["evidence"]["state_volume"] == {
        "name": f"buildx_buildkit_{name}0_state",
        "created_at": "2026-11-03T18:00:00Z",
    }
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "rm", name))
    handle.leave()


def test_a_pre_existing_state_volume_refuses_the_builder(tmp_path: Path) -> None:
    fake, handle, descriptor = _builder_world(tmp_path)
    name = handle.docker_context.builder
    fake.volumes[f"buildx_buildkit_{name}0_state"] = {"Name": f"buildx_buildkit_{name}0_state", "CreatedAt": "x"}

    with pytest.raises(InvalidInput, match="state volume identity collides"):
        lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "create"))

    assert name not in fake.builders  # refused before `buildx create` ran: the toolkit never adopts a volume
    handle.leave()


def test_a_pull_promotion_records_the_image_size(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    fake = FakeDockerPort()
    fake.images.clear()
    reference = "example/cache@sha256:" + "2" * 64
    handle = (
        build_test_services(tmp_path / "state")
        .open_run(_spec(project, include_image=False), docker_factory=lambda: fake)
        .enter()
    )
    _ = handle.docker_context
    descriptor = json.loads((handle.private_root / "docker-authority.json").read_text())

    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("pull", reference))

    images = json.loads(handle._context.record.read_text())["owned_resources"]["images"]
    assert images[IMAGE_ID]["evidence"]["size_bytes"] == 7_340_032  # the derived disk estimate's input
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("image", "rm", IMAGE_ID))
    assert handle.leave().cleanup_outcome == "complete"


def test_a_build_promotion_records_the_image_size(tmp_path: Path) -> None:
    fake, handle, descriptor = _builder_world(tmp_path)

    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "create"))
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "build", "--tag", "fixture:latest", "."))

    images = json.loads(handle._context.record.read_text())["owned_resources"]["images"]
    built = next(item for item in images.values() if item["state"] != "removed")
    assert built["evidence"]["size_bytes"] == 9_437_184
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("image", "rm", built["identifier"]))
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "rm", handle.docker_context.builder))
    assert handle.leave().cleanup_outcome == "complete"


PODMAN_MANIFEST = "sha256:" + "d" * 64  # Buildx's manifest digest; Podman's repository digest differs again


@pytest.mark.parametrize(("config_digest", "promoted"), [(BUILT_IMAGE_ID, True), ("sha256:" + "4" * 64, False)])
def test_a_podman_build_promotes_through_its_config_digest(tmp_path: Path, config_digest: str, promoted: bool) -> None:
    """Podman's image ID is the metadata's config digest, and its repository digests carry a digest
    of their own (neither the metadata's manifest digest nor the ID)."""
    fake = FakeDockerPort()
    fake.engine = "podman"
    fake.build_metadata = {"containerimage.digest": PODMAN_MANIFEST, "containerimage.config.digest": config_digest}
    fake.build_extra_reference = ("RepoDigests", "fixture@sha256:" + "5" * 64)
    handle, descriptor = _entered_run(tmp_path, fake)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "create"))
    build = ("buildx", "build", "--tag", "fixture:latest", ".")

    if not promoted:
        with pytest.raises(IncompleteCleanup, match="unbound resource"):
            lifecycle.dispatch_docker(handle._context, descriptor, fake, build)
        return
    lifecycle.dispatch_docker(handle._context, descriptor, fake, build)
    evidence = json.loads(handle._context.record.read_text())["owned_resources"]["images"][BUILT_IMAGE_ID]["evidence"]
    assert evidence["repo_digests"] == ["fixture@sha256:" + "5" * 64]
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("image", "rm", BUILT_IMAGE_ID))
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "rm", handle.docker_context.builder))
    handle.leave()


def test_a_podman_build_still_refuses_another_repository(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    fake.engine = "podman"
    fake.build_metadata = {"containerimage.digest": PODMAN_MANIFEST, "containerimage.config.digest": BUILT_IMAGE_ID}
    fake.build_extra_reference = ("RepoDigests", "foreign@sha256:" + "5" * 64)
    handle, descriptor = _entered_run(tmp_path, fake)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "create"))

    with pytest.raises(IncompleteCleanup, match="unbound resource"):
        lifecycle.dispatch_docker(
            handle._context, descriptor, fake, ("buildx", "build", "--tag", "fixture:latest", ".")
        )


def _oom_kill(fake: FakeDockerPort, container_id: str = "c" * 64) -> None:
    fake.containers[container_id]["State"]["OOMKilled"] = True
    fake.stop_container(container_id)


def _end_after_oom(handle, descriptor, fake: FakeDockerPort) -> None:
    """Clear the recorded OOM (it fails any leave) and end the run cleanly."""

    def clear(data: dict) -> None:
        data["infrastructure_failure"] = None
        data.pop("failure_evidence", None)

    _edit_record(handle, clear)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "down", "--volumes"))
    handle.leave("failed")


def test_compose_run_promotion_records_a_sibling_oom_kill(tmp_path: Path) -> None:
    """Nothing observes the services while a compose run is open: its promotion is the first to see the OOM."""
    fake = FakeDockerPort()
    _services, handle, descriptor = _recovering_run(tmp_path, fake)
    _oom_kill(fake)

    result = lifecycle.dispatch_docker(handle._context, descriptor, fake, _COMPOSE_RUN)

    record = json.loads(handle._context.record.read_text())
    assert result.exit_code == 0
    assert record["infrastructure_failure"] == "container-oom"
    assert (record["failure_evidence"]["category"], record["failure_evidence"]["observer"]) == ("breach", "container")
    assert record["failure_evidence"]["first_observed"]["state"] == "active"
    _end_after_oom(handle, descriptor, fake)


def _drop_recorded_control(data: dict) -> None:
    data["owned_resources"]["containers"]["c" * 64]["evidence"].pop("control")


def _earlier_failure(data: dict) -> None:
    data["infrastructure_failure"] = "compose-up-failed"


@pytest.mark.parametrize(
    ("arrange", "failure", "evidence"),
    [(_drop_recorded_control, "container-oom", True), (_earlier_failure, "compose-up-failed", False)],
)
def test_promotion_oom_rule_edges(
    tmp_path: Path, arrange: Callable[[dict], None], failure: str, evidence: bool
) -> None:
    """A container recorded without ``control`` still counts as unseen; an earlier failure keeps its own cause."""
    fake = FakeDockerPort()
    _services, handle, descriptor = _recovering_run(tmp_path, fake)
    _edit_record(handle, arrange)
    _oom_kill(fake)

    lifecycle.dispatch_docker(handle._context, descriptor, fake, _COMPOSE_RUN)

    record = _record(handle)
    assert record["infrastructure_failure"] == failure
    assert ("failure_evidence" in record) is evidence
    _end_after_oom(handle, descriptor, fake)


def test_live_leave_reconciliation_records_a_sibling_oom_kill(tmp_path: Path, request: pytest.FixtureRequest) -> None:
    """An ordinary leave: the guardian reconciles the open compose run in ``stopping``; the OOM is still recorded."""
    fake = FakeDockerPort()
    services, handle, descriptor = _recovering_run(tmp_path, fake)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, _COMPOSE_RUN)
    resume = _pause_for_invalid_records(request, handle._context)
    _reopen_last_receipt(handle)
    prewrite_withdrawal(handle._context, "passed", 30)
    _oom_kill(fake)
    lifecycle.configure_docker_factories(lambda: fake, lambda _identity: fake)

    lifecycle._reconcile_docker_receipt(handle._context)  # the guardian's call in its ``stopping`` branch

    record = json.loads(handle._context.record.read_text())
    assert (record["state"], record["recovery_actor"]) == ("stopping", None)
    assert record["infrastructure_failure"] == "container-oom"
    resume()
    abandon_run(handle._context, services.root / "admission")
    assert lifecycle.recover_run(handle._context, _test_directory=services.root / "admission").state == "complete"


def test_recovery_does_not_record_a_sibling_oom_kill(tmp_path: Path) -> None:
    """Recovery's outcome and failure are already fixed: the refreshed values are stored, nothing more."""
    fake = FakeDockerPort()
    services, handle, descriptor = _recovering_run(tmp_path, fake)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, _COMPOSE_RUN)
    _reopen_last_receipt(handle)
    _oom_kill(fake)
    abandon_run(handle._context, services.root / "admission")
    lifecycle.configure_docker_factories(lambda: fake, lambda _identity: fake)

    assert lifecycle.recover_run(handle._context, _test_directory=services.root / "admission").state == "complete"

    assert json.loads(handle._context.record.read_text())["infrastructure_failure"] != "container-oom"


def test_identity_conflict_never_lists_volatile_container_keys(tmp_path: Path) -> None:
    """A stopped container with a new start generation and another ``Created``: only the identity key is named."""
    fake = FakeDockerPort()
    services, handle, descriptor = _recovering_run(tmp_path, fake)
    fake.stop_container("c" * 64)
    fake.containers["c" * 64]["State"]["StartedAt"] = "started-9"
    fake.containers["c" * 64]["Created"] = "other-created"

    with pytest.raises(ResourceError) as raised:
        lifecycle.dispatch_docker(handle._context, descriptor, fake, _COMPOSE_RUN)

    assert raised.value.details["conflict"]["keys"] == ["evidence.created"]
    fake.containers["c" * 64]["Created"] = "container-created"
    abandon_run(handle._context, services.root / "admission")
    lifecycle.configure_docker_factories(lambda: fake, lambda _identity: fake)
    assert lifecycle.recover_run(handle._context, _test_directory=services.root / "admission").state == "complete"


def test_buildkit_worker_contradiction_names_the_builder(tmp_path: Path) -> None:
    """S2 has no public path (the builder's own comparison fires first); the finish seam shows its naming."""
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "create"))
    record = _record(handle)
    builder_id, builder = next(iter(record["owned_resources"]["builders"].items()))
    worker = record["owned_resources"]["containers"][builder["evidence"]["buildkit_container_id"]]
    receipt = next(item for item in record["docker_receipts"].values() if item["kind"] == "builder-create")

    def reopen(data: dict) -> None:
        data["docker_receipts"][receipt["id"]].update(state="pending", result=None, resources=[])

    pending = _edit_record(handle, reopen)["docker_receipts"][receipt["id"]]
    impostor = {**worker, "identifier": "9" * 64}
    with pytest.raises(ResourceError, match="contradicts its owned builder") as raised:
        lifecycle._finish_docker_receipt(handle._context, pending, "promoted", DockerResult(0, "", ""), [impostor])

    assert raised.value.reason == "owned-identity-conflict"
    assert raised.value.details["conflict"] == {
        "kind": "builder",
        "identifier": builder_id,
        "name": builder["name"],
        "keys": ["evidence.buildkit_container_id"],
    }
    _edit_record(handle, lambda data: data["docker_receipts"][receipt["id"]].update(receipt))
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "rm", handle.docker_context.builder))
    assert handle.leave().cleanup_outcome == "complete"


@pytest.mark.parametrize("recorded_removed", [False, True])
def test_compose_promotion_names_a_recreated_volume(tmp_path: Path, recorded_removed: bool) -> None:
    """S4: a volume the run records, replaced by a foreign one under its name, is named; an unrecorded one is not."""
    fake = FakeDockerPort()
    services, handle, descriptor = _recovering_run(tmp_path, fake)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, _COMPOSE_RUN)
    _reopen_last_receipt(handle)
    volume = f"{handle.docker_context.compose_project}_data"
    if recorded_removed:
        _edit_record(handle, lambda data: data["owned_resources"]["volumes"][volume].update(state="removed"))
    fake.volumes[volume].update(CreatedAt="another", Labels={})
    abandon_run(handle._context, services.root / "admission")
    lifecycle.configure_docker_factories(lambda: fake, lambda _identity: fake)

    with pytest.raises(ResourceError) as raised:
        lifecycle.recover_run(handle._context, _test_directory=services.root / "admission")

    if recorded_removed:
        assert "conflict" not in raised.value.details
    else:
        assert raised.value.reason == "owned-identity-conflict"
        assert raised.value.details["conflict"] == {
            "kind": "volume",
            "identifier": volume,
            "name": volume,
            "keys": ["evidence.compose_project", "evidence.created"],
        }


def test_dispatcher_conflict_before_the_child_fails_the_receipt(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("network", "create"))
    network_id = next(iter(_record(handle)["owned_resources"]["networks"]))
    fake.networks[network_id]["Created"] = "foreign"

    with pytest.raises(ResourceError, match="Owned network identity changed") as raised:
        lifecycle.dispatch_docker(handle._context, descriptor, fake, ("network", "rm", network_id))

    assert not isinstance(raised.value, IncompleteCleanup)
    assert raised.value.details["conflict"]["keys"] == ["evidence.created"]
    assert list(_record(handle)["docker_receipts"].values())[-1]["state"] == "failed"
    assert ("network", "rm", network_id) not in fake.commands
    fake.networks[network_id]["Created"] = "network-created"
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("network", "rm", network_id))
    assert handle.leave().cleanup_outcome == "complete"


def test_cleanup_conflict_fails_its_receipt_and_names_the_resource(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No command ran: the receipt ends ``failed``, so it does not block a later recovery that disowns the resource."""
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)
    monkeypatch.setattr(lifecycle, "_BOUND_DOCKER_FACTORY", lambda _identity: fake)
    _plural, target, command = _prepare_owned_removal(handle, descriptor, fake, "container")
    fake.containers[target]["Name"] = "/renamed"

    with pytest.raises(ResourceError, match="Owned container identity changed") as raised:
        lifecycle._cleanup_owned_docker_resources(handle._context)

    record = _record(handle)
    assert raised.value.details["conflict"]["keys"] == ["name"]
    assert next(item for item in record["docker_receipts"].values() if item["kind"] == "container-rm")["state"] == (
        "failed"
    )
    assert all(item["state"] not in {"pending", "unresolved"} for item in record["docker_receipts"].values())
    assert command not in fake.commands
    fake.containers[target]["Name"] = record["owned_resources"]["containers"][target]["name"]
    lifecycle._cleanup_owned_docker_resources(handle._context)  # the failed receipt does not block the next attempt
    assert handle.leave("failed").cleanup_outcome == "complete"


def test_unresolved_removal_names_its_target(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)
    monkeypatch.setattr(lifecycle, "_BOUND_DOCKER_FACTORY", lambda _identity: fake)
    _plural, target, _command = _prepare_owned_removal(handle, descriptor, fake, "network")
    fake.fail_before_remove_kind = "network"

    with pytest.raises(ResourceError, match="cleanup result is unresolved") as raised:
        lifecycle._cleanup_owned_docker_resources(handle._context)

    assert raised.value.details["resource"] == {"kind": "network", "identifier": target}
    assert str(raised.value).endswith(f"Resource: network {target}.")
    assert (
        next(item for item in _record(handle)["docker_receipts"].values() if item["kind"] == "network-rm")["state"]
        == "unresolved"
    )
    lifecycle._reconcile_docker_receipt(handle._context)
    assert handle.leave("failed").cleanup_outcome == "complete"


def test_reconciliation_that_stays_unresolved_names_its_target(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A removal Docker keeps refusing (an object still in use) names what to disown on the next recovery."""
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)
    monkeypatch.setattr(lifecycle, "_BOUND_DOCKER_FACTORY", lambda _identity: fake)
    _plural, target, _command = _prepare_owned_removal(handle, descriptor, fake, "network")
    fake.in_use.add(target)
    with pytest.raises(ResourceError, match="cleanup result is unresolved"):
        lifecycle._cleanup_owned_docker_resources(handle._context)

    with pytest.raises(ResourceError, match="remains unresolved after exact reconciliation") as raised:
        lifecycle._reconcile_docker_receipt(handle._context)

    assert raised.value.details["resource"] == {"kind": "network", "identifier": target}
    assert str(raised.value).endswith(f"Resource: network {target}.")
    fake.in_use.clear()
    lifecycle._reconcile_docker_receipt(handle._context)
    assert handle.leave("failed").cleanup_outcome == "complete"


class _Crash(Exception):
    """Stops a recovery at a failpoint, as a kill there would."""


def _stuck_by_a_renamed_container(tmp_path: Path, fake: FakeDockerPort, **arrange):
    """A killed run whose pending compose run now finds its service container renamed: plain recovery refuses."""
    services, handle, descriptor = _recovering_run(tmp_path, fake, **arrange)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, _COMPOSE_RUN)
    _reopen_last_receipt(handle)
    fake.containers["c" * 64]["Name"] = "/renamed"
    abandon_run(handle._context, services.root / "admission")
    lifecycle.configure_docker_factories(lambda: fake, lambda _identity: fake)
    return services, handle


def _recover(services, handle, disown: tuple[tuple[str, str], ...] = ()):
    return lifecycle.recover_run(handle._context, disown=disown, _test_directory=services.root / "admission")


def test_disown_is_written_with_the_recovery_actor_before_any_docker_call(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    services, handle = _stuck_by_a_renamed_container(tmp_path, fake)
    commands_before = len(fake.commands)
    seen: dict = {}

    def at_first_section_end(stage: str) -> None:
        if stage == "recovery-first-section-released":
            seen.update(record=_record(handle), commands=len(fake.commands))
            raise _Crash

    install_failpoint(at_first_section_end)
    try:
        with pytest.raises(_Crash):
            _recover(services, handle, (("container", "c" * 64),))
    finally:
        install_failpoint(None)

    container = seen["record"]["owned_resources"]["containers"]["c" * 64]
    assert (container["state"], container["evidence"]["disowned"]) == ("removed", True)
    assert seen["record"]["recovery_actor"] is not None
    assert seen["commands"] == commands_before


@pytest.mark.parametrize(
    "request_",
    [
        (("container", "0" * 64),),
        (("container", "VOLUME"),),  # a recorded volume's name sent as a container
        (("network", "REMOVED"),),
    ],
    ids=["unknown", "other-kind", "already-removed"],
)
def test_disown_refuses_what_the_run_does_not_own(tmp_path: Path, request_: tuple) -> None:
    fake = FakeDockerPort()
    services, handle = _stuck_by_a_renamed_container(tmp_path, fake)
    record = _record(handle)
    volume = next(iter(record["owned_resources"]["volumes"]))
    network = next(iter(record["owned_resources"]["networks"]))
    if request_[0][1] == "REMOVED":
        _edit_record(handle, lambda data: data["owned_resources"]["networks"][network].update(state="removed"))
    request_ = tuple((kind, {"VOLUME": volume, "REMOVED": network}.get(name, name)) for kind, name in request_)
    before = handle._context.record.read_bytes()

    with pytest.raises(InvalidInput, match="does not own"):
        _recover(services, handle, request_)

    assert handle._context.record.read_bytes() == before


def test_disown_is_refused_for_a_retained_run(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    services, handle = _stuck_by_a_renamed_container(tmp_path, fake, retain=True)
    before = handle._context.record.read_bytes()

    with pytest.raises(InvalidInput, match="disown"):
        _recover(services, handle, (("container", "c" * 64),))

    assert handle._context.record.read_bytes() == before


def test_disown_is_refused_for_a_run_that_never_reached_a_grant(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    project = tmp_path / "project"
    project.mkdir()
    services = build_test_services(tmp_path / "state")
    run = services.open_run(_spec(project), docker_factory=lambda: fake, admit_docker=True)
    before = run._context.record.read_bytes()

    with pytest.raises(InvalidInput, match=r"(?i)disown"):
        lifecycle.recover_run(
            run._context, disown=(("container", "c" * 64),), _test_directory=services.root / "admission"
        )

    assert run._context.record.read_bytes() == before
    run.cancel()


def test_disown_retires_the_endpoint_of_a_disowned_container(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    fake.compose_ports = {"8080/tcp": [{"HostIp": "127.0.0.1", "HostPort": "28080"}]}
    services, handle, descriptor = _recovering_run(tmp_path, fake)
    handle.publish_endpoint("fixture", "http", 8080)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, _COMPOSE_RUN)
    _reopen_last_receipt(handle)
    fake.containers["c" * 64]["Name"] = "/renamed"
    abandon_run(handle._context, services.root / "admission")
    lifecycle.configure_docker_factories(lambda: fake, lambda _identity: fake)

    assert _recover(services, handle, (("container", "c" * 64),)).state == "complete"

    assert _record(handle)["endpoints"]["fixture"][-1]["active"] is False


def _released(services, handle, request: tuple, match: str) -> ResourceError:
    """Plain recovery refuses naming the resource; recovery with the request completes."""
    with pytest.raises(ResourceError, match=match) as refused:
        _recover(services, handle)
    assert _recover(services, handle, request).state == "complete"
    record = _record(handle)
    for kind, identifier in request:
        assert lifecycle._owned_bucket(record, kind)[identifier]["evidence"]["disowned"] is True
    return refused.value


def test_renamed_container_is_released_by_disown(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    services, handle = _stuck_by_a_renamed_container(tmp_path, fake)
    kept = copy.deepcopy(fake.containers["c" * 64])

    refused = _released(services, handle, (("container", "c" * 64),), "collides")

    assert refused.details["conflict"]["identifier"] == "c" * 64
    assert fake.containers["c" * 64] == kept
    assert not any(command[:2] == ("container", "rm") and command[-1] == "c" * 64 for command in fake.commands)


def test_cleanup_conflict_is_released_by_disown(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    services, handle, _descriptor = _recovering_run(tmp_path, fake)
    volume = f"{handle.docker_context.compose_project}_data"
    fake.volumes[volume]["CreatedAt"] = "another"
    kept = copy.deepcopy(fake.volumes[volume])
    abandon_run(handle._context, services.root / "admission")
    lifecycle.configure_docker_factories(lambda: fake, lambda _identity: fake)

    refused = _released(services, handle, (("volume", volume),), "Owned volume identity changed")

    assert refused.details["conflict"]["keys"] == ["evidence.created"]
    assert fake.volumes[volume] == kept


def test_recreated_volume_is_released_by_disown(tmp_path: Path) -> None:
    """S4: the foreign volume under the recorded name is skipped by promotion and never re-adopted."""
    fake = FakeDockerPort()
    services, handle, descriptor = _recovering_run(tmp_path, fake)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, _COMPOSE_RUN)
    _reopen_last_receipt(handle)
    volume = f"{handle.docker_context.compose_project}_data"
    fake.volumes[volume].update(CreatedAt="another", Labels={})
    kept = copy.deepcopy(fake.volumes[volume])
    abandon_run(handle._context, services.root / "admission")
    lifecycle.configure_docker_factories(lambda: fake, lambda _identity: fake)

    _released(services, handle, (("volume", volume),), "Owned volume identity changed")

    assert fake.volumes[volume] == kept
    assert _record(handle)["owned_resources"]["volumes"][volume]["evidence"]["created"] != "another"


def test_open_removal_receipt_of_a_disowned_resource_closes_failed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An earlier recovery left a removal unresolved; with its target disowned it closes without a Docker call."""
    fake = FakeDockerPort()
    services, handle, _descriptor = _recovering_run(tmp_path, fake)
    network = next(iter(_record(handle)["owned_resources"]["networks"]))
    abandon_run(handle._context, services.root / "admission")
    lifecycle.configure_docker_factories(lambda: fake, lambda _identity: fake)
    fake.in_use.add(network)
    with pytest.raises(ResourceError, match="cleanup result is unresolved"):
        _recover(services, handle)
    fake.networks[network]["Created"] = "another"

    refused = _released(services, handle, (("network", network),), "Owned network identity changed")

    receipt = next(
        item
        for item in _record(handle)["docker_receipts"].values()
        if item["kind"] == "network-rm" and item["plan"]["network_id"] == network
    )
    assert (receipt["state"], receipt["result"]["exit_code"]) == ("failed", 5)
    assert refused.details["conflict"]["identifier"] == network
    assert network in fake.networks


def test_dependency_in_use_is_named_then_released_by_disown(tmp_path: Path) -> None:
    """A recorded network still used by a disowned container stays in use: it is named, then disowned too."""
    fake = FakeDockerPort()
    services, handle = _stuck_by_a_renamed_container(tmp_path, fake)
    network = next(iter(_record(handle)["owned_resources"]["networks"]))
    fake.in_use.add(network)

    with pytest.raises(ResourceError, match="unresolved") as refused:
        _recover(services, handle, (("container", "c" * 64),))
    assert refused.value.details["resource"] == {"kind": "network", "identifier": network}

    assert _recover(services, handle, (("container", "c" * 64), ("network", network))).state == "complete"
    assert network in fake.networks


def test_second_conflict_not_requested_still_refuses(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    fake.compose_services = ("fixture", "second")
    services, handle = _stuck_by_a_renamed_container(tmp_path, fake)
    fake.containers["d" * 64]["Name"] = "/renamed-too"

    with pytest.raises(ResourceError, match="collides") as refused:
        _recover(services, handle, (("container", "c" * 64),))

    assert refused.value.details["conflict"]["identifier"] == "d" * 64
    assert _record(handle)["owned_resources"]["containers"]["c" * 64]["evidence"]["disowned"] is True
    assert _recover(services, handle, (("container", "d" * 64),)).state == "complete"


def test_crash_after_the_marker_write_resumes_without_touching_it(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    services, handle = _stuck_by_a_renamed_container(tmp_path, fake)
    kept = copy.deepcopy(fake.containers["c" * 64])

    def crash(stage: str) -> None:
        if stage == "recovery-first-section-released":
            raise _Crash

    install_failpoint(crash)
    try:
        with pytest.raises(_Crash):
            _recover(services, handle, (("container", "c" * 64),))
    finally:
        install_failpoint(None)

    assert _recover(services, handle).state == "complete"  # a plain rerun: the marker is durable
    assert fake.containers["c" * 64] == kept
    assert _recover(services, handle).state == "complete"  # and a further one changes nothing


def test_disowned_buildkit_worker_is_not_adopted(tmp_path: Path) -> None:
    """A promotion that observes the worker of a disowned builder leaves the recorded worker entry as it is."""
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "create"))
    record = _record(handle)
    builder_id, builder = next(iter(record["owned_resources"]["builders"].items()))
    worker_id = builder["evidence"]["buildkit_container_id"]
    receipt = next(item for item in record["docker_receipts"].values() if item["kind"] == "builder-create")

    def disown_and_reopen(data: dict) -> None:
        lifecycle._apply_disown(data, (("builder", builder_id),))
        data["docker_receipts"][receipt["id"]].update(state="pending", result=None, resources=[])

    pending = _edit_record(handle, disown_and_reopen)["docker_receipts"][receipt["id"]]
    worker = {**record["owned_resources"]["containers"][worker_id], "state": "stopped"}
    lifecycle._finish_docker_receipt(handle._context, pending, "promoted", DockerResult(0, "", ""), [worker])

    assert (
        _record(handle)["owned_resources"]["containers"][worker_id]
        == record["owned_resources"]["containers"][worker_id]
    )


def test_pending_container_control_drops_a_disowned_target(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A run killed during an exact stop whose target then changed: disowning the target lets recovery finish."""
    fake = FakeDockerPort()
    fake.compose_services = ("caller", "target")
    project = tmp_path / "project"
    project.mkdir()
    services = build_test_services(tmp_path / "state")
    handle = services.open_run(
        _spec(project, services=fake.compose_services), docker_factory=lambda: fake, admit_docker=True
    ).enter()
    _ = handle.docker_context
    descriptor = json.loads((handle.private_root / "docker-authority.json").read_text())
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "up", "--detach"))
    target_id = "d" * 64

    def interrupt_stop(data: dict) -> None:
        data["revision"] += 1
        data["pending_operation"] = {
            "id": "55555555-5555-4555-8555-555555555555",
            "kind": "container-control",
            "generation": data["generation"],
            "revision": data["revision"],
            "actor": data["guardian"],
            "deadline": asdict(lifecycle.Deadline.after(lifecycle.NativeClock(), 30)),
            "plan": {
                "action": "stop",
                "targets": [target_id],
                "expected": {target_id: {"started_at": "started-0", "restart_count": 0}},
                "request_sha256": "e" * 64,
            },
        }

    _edit_record(handle, interrupt_stop)
    fake.containers[target_id]["Name"] = "/renamed"
    abandon_run(handle._context, services.root / "admission")
    lifecycle.configure_docker_factories(lambda: fake, lambda _identity: fake)
    monkeypatch.setattr(lifecycle, "observe_container", _observe_fake_container)  # as the file's other control tests

    with pytest.raises(ResourceError):
        _recover(services, handle)
    assert _recover(services, handle, (("container", target_id),)).state == "complete"

    record = _record(handle)
    assert record["pending_operation"] is None
    assert record["last_operation"]["outcome"] == "failed"
    assert target_id in fake.containers
