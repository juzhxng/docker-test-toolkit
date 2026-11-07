"""Shared fakes and harness of the archive export service tests.

Races run two threads in one process; a failpoint callback pauses a named thread at a named stage. A dispatcher
"crash" is a ``KeyboardInterrupt`` raised from a failpoint: it passes the exception fence and leaves exactly the
on-disk and record state a killed process leaves.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

from docker_test_toolkit.application import lifecycle
from docker_test_toolkit.domain.models import DockerResult
from docker_test_toolkit.factories.testing import build_test_services, install_failpoint, install_lock_observer
from tests.archive_fixtures import Layout, write_archive
from tests.guard.lock_order import LockOrderChecker
from tests.unit.test_docker_receipt_service import FakeDockerPort, _spec

REFERENCE = "example/cache@sha256:" + "2" * 64


class ExportingDockerPort(FakeDockerPort):
    """A containerd-store daemon whose ``image save`` is a configurable function of the output path."""

    def __init__(self, layout: Layout) -> None:
        super().__init__()
        self.layout = layout
        self.image_id = layout.root_digest
        self.descriptor: dict | None = {"mediaType": "application/vnd.oci.image.index.v1+json", "digest": self.image_id}
        self.size = 1024
        self.save: Callable[[Path], int] = self.write_valid
        self.exports: list[tuple[str, ...]] = []

    def write_valid(self, output: Path) -> int:
        """Write the layout the way the Docker client does and report success."""
        write_archive(output, self.layout.members())
        output.chmod(0o600)
        return 0

    def execute(self, argv, *, timeout_seconds, environment=None, stream_output=False, hold_fd=None):
        if argv[:2] == ("image", "pull"):
            item = {
                "Id": self.image_id,
                "Created": "pulled-created",
                "Config": {"Labels": {}},
                "RepoDigests": [lifecycle._repository_digest(argv[2])],
                "Os": "linux",
                "Architecture": "arm64",
                "Size": self.size,
            }
            if self.descriptor is not None:
                item["Descriptor"] = {**self.descriptor, "size": 1}
            self.images[self.image_id] = item
            self.images[argv[2]] = item
            return DockerResult(0, "pulled\n", "")
        if argv[:3] == ("image", "save", "-o"):
            self.commands.append(argv)
            self.timeouts.append(timeout_seconds)
            self.exports.append(argv)
            return DockerResult(self.save(Path(argv[3])), "", "")
        return super().execute(
            argv, timeout_seconds=timeout_seconds, environment=environment, stream_output=stream_output
        )


@dataclass
class ExportRun:
    """One entered run that owns the layout's image."""

    handle: object
    services: object
    descriptor: dict
    fake: ExportingDockerPort
    layout: Layout
    seams: dict = field(default_factory=dict)

    @property
    def image_id(self) -> str:
        return self.layout.root_digest

    @property
    def exports_directory(self) -> Path:
        return self.handle.private_root / "exports"

    def export(self, **seams: object) -> DockerResult:
        return lifecycle.dispatch_docker(
            self.handle._context,
            self.descriptor,
            self.fake,
            ("image", "save", self.image_id),
            **{**self.seams, **seams},
        )

    def record(self) -> dict:
        return json.loads(self.handle._context.record.read_text())

    def receipt(self, kind: str = "image-save") -> dict:
        (receipt,) = [item for item in self.record()["docker_receipts"].values() if item["kind"] == kind]
        return receipt

    def reconcile(self) -> None:
        lifecycle._reconcile_docker_receipt(self.handle._context)

    def staged(self) -> list[Path]:
        return sorted(self.exports_directory.iterdir()) if self.exports_directory.exists() else []


def start_run(
    tmp_path: Path,
    layout: Layout | None = None,
    *,
    configure: Callable[[ExportingDockerPort], None] | None = None,
):
    """Enter a run that owns the layout's pulled image."""
    layout = layout or Layout(docker_manifest=True)
    fake = ExportingDockerPort(layout)
    fake.images.clear()
    if configure is not None:
        configure(fake)
    project = tmp_path / "project"
    project.mkdir()
    services = build_test_services(tmp_path / "state")
    handle = services.open_run(_spec(project, include_image=False), docker_factory=lambda: fake).enter()
    _ = handle.docker_context
    descriptor = json.loads((handle.private_root / "docker-authority.json").read_text())
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("pull", REFERENCE))
    return ExportRun(handle, services, descriptor, fake, layout)


class Stages:
    """A failpoint callback that runs a one-shot action for a named stage, optionally only on a named thread."""

    def __init__(self) -> None:
        self._actions: dict[tuple[str | None, str], Callable[[], None]] = {}
        self.seen: list[tuple[str, str]] = []

    def on(self, stage: str, action: Callable[[], None], *, thread: str | None = None) -> None:
        self._actions[(thread, stage)] = action

    def __call__(self, stage: str) -> None:
        name = threading.current_thread().name
        self.seen.append((name, stage))
        action = self._actions.pop((name, stage), None) or self._actions.pop((None, stage), None)
        if action is not None:
            action()

    def __enter__(self) -> Stages:
        install_failpoint(self)
        return self

    def __exit__(self, *exc: object) -> None:
        install_failpoint(None)


class Pause:
    """Park the calling thread at a stage until the test releases it."""

    def __init__(self) -> None:
        self.reached = threading.Event()
        self.release = threading.Event()

    def __call__(self) -> None:
        self.reached.set()
        assert self.release.wait(30), "the test never released the paused thread"

    def wait_until_reached(self) -> None:
        assert self.reached.wait(30), "the paused thread never reached its stage"


class Dispatcher(threading.Thread):
    """Run an export on its own thread and keep its outcome."""

    def __init__(self, run: ExportRun, name: str = "D", *, method: str = "export", **seams: object) -> None:
        super().__init__(name=name)
        self.run_under_test = run
        self.method = method
        self.seams = seams
        self.result: DockerResult | None = None
        self.error: BaseException | None = None

    def run(self) -> None:
        try:
            self.result = getattr(self.run_under_test, self.method)(**self.seams)
        except BaseException as error:
            self.error = error

    def finish(self) -> BaseException | None:
        self.join(30)
        assert not self.is_alive()
        return self.error


def interrupt() -> None:
    raise KeyboardInterrupt


def single(directory: Path, pattern: str) -> Path:
    (match,) = directory.glob(pattern)
    return match


def archive_of(receipt: dict) -> dict:
    return receipt["plan"]["archive"]


@contextmanager
def observed_locks() -> Iterator[LockOrderChecker]:
    """Record every lock acquisition of the process and expose the order violations it saw."""
    checker = LockOrderChecker()
    install_lock_observer(checker)
    try:
        yield checker
    finally:
        install_lock_observer(None)
