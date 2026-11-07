"""Platform facts on build and Compose receipts: recorded, never enforced."""

from __future__ import annotations

import json
from pathlib import Path
from typing import NamedTuple

from docker_test_toolkit.application import lifecycle
from docker_test_toolkit.application.lifecycle import EngineReply
from docker_test_toolkit.domain.errors import ResourceError
from docker_test_toolkit.domain.models import DockerResult
from docker_test_toolkit.factories.testing import bind_test_observation_reads
from tests.unit.test_docker_receipt_service import FakeDockerPort, _entered_run

ARM_DAEMON = {"os": "linux", "architecture": "arm64"}
AMD_IMAGE = {"os": "linux", "architecture": "amd64"}


def _info(machine: str):
    def engine(_docker: object, path: str, _deadline: object) -> EngineReply:
        assert path == "/info"
        return EngineReply({"OSType": "linux", "Architecture": machine}, b"", 0)

    return engine


def _late(_docker: object, _path: str, _deadline: object) -> EngineReply:
    raise ResourceError("Engine deadline expired.", reason="observation-deadline", observer="engine")


class Built(NamedTuple):
    """One run that built an image: its handle, the docker authority and what the record shows afterwards."""

    handle: object
    descriptor: dict
    fake: FakeDockerPort
    record: dict
    receipt: dict
    result: DockerResult

    def leave(self) -> None:
        """Remove what the build created, exactly, and leave the run."""
        context = self.handle._context  # type: ignore[attr-defined]
        for image_id, item in self.record["owned_resources"]["images"].items():
            if item["state"] != "removed":
                lifecycle.dispatch_docker(context, self.descriptor, self.fake, ("image", "rm", image_id))
        lifecycle.dispatch_docker(
            context,
            self.descriptor,
            self.fake,
            ("buildx", "rm", self.handle.docker_context.builder),  # type: ignore[attr-defined]
        )
        self.handle.leave()  # type: ignore[attr-defined]


def _build(tmp_path: Path, *flags: str, fake: FakeDockerPort | None = None):
    fake = fake or FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "create"))
    command = ("buildx", "build", *flags, "--tag", "fixture:latest", ".")
    result = lifecycle.dispatch_docker(handle._context, descriptor, fake, command)
    record = json.loads(handle._context.record.read_text())
    receipt = next(item for item in record["docker_receipts"].values() if item["kind"] == "build")
    return Built(handle, descriptor, fake, record, receipt, result)


def _image_evidence(record: dict) -> dict:
    return next(item for item in record["owned_resources"]["images"].values() if item["state"] != "removed")["evidence"]


def test_build_platform_records_an_emulated_amd64_build_on_an_arm64_daemon(tmp_path: Path) -> None:
    with bind_test_observation_reads(engine=_info("aarch64")):
        built = _build(tmp_path, "--platform", "linux/amd64")

    assert built.receipt["plan"]["daemon_platform"] == ARM_DAEMON
    assert built.receipt["plan"]["platform"] == {
        "requested": AMD_IMAGE,
        "requested_raw": "linux/amd64",
        "requested_emulated": "emulated",
    }
    assert _image_evidence(built.record)["platform"] == {"image": AMD_IMAGE, "emulated": "emulated"}
    built.leave()


def test_build_platform_without_a_request_compares_the_image_to_the_daemon(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    fake.default_image_platform = "linux/amd64"  # pulled or built for a foreign platform without being asked
    with bind_test_observation_reads(engine=_info("aarch64")):
        built = _build(tmp_path, fake=fake)

    assert built.receipt["plan"]["platform"] == {
        "requested": None,
        "requested_raw": None,
        "requested_emulated": "unknown",
    }
    assert _image_evidence(built.record)["platform"] == {"image": AMD_IMAGE, "emulated": "emulated"}
    built.leave()


def test_build_platform_variants_are_informational(tmp_path: Path) -> None:
    with bind_test_observation_reads(engine=_info("x86_64")):
        built = _build(tmp_path, "--platform=linux/amd64/v2")  # the equals spelling of the option
    assert built.receipt["plan"]["platform"]["requested_emulated"] == "native"
    assert _image_evidence(built.record)["platform"] == {
        "image": {"os": "linux", "architecture": "amd64", "variant": "v2"},
        "emulated": "native",
    }
    built.leave()


def test_build_platform_list_is_recorded_verbatim_and_not_refused(tmp_path: Path) -> None:
    with bind_test_observation_reads(engine=_info("aarch64")):
        built = _build(tmp_path, "--platform", "linux/amd64,linux/arm64")

    assert built.result.exit_code == 0
    assert built.receipt["plan"]["platform"] == {
        "requested": None,
        "requested_raw": "linux/amd64,linux/arm64",
        "requested_emulated": "unknown",
    }
    assert built.record["infrastructure_failure"] is None
    assert _image_evidence(built.record)["platform"] == {"image": None, "emulated": "unknown"}  # the image too
    built.leave()


def test_build_platform_daemon_timeout_fails_open(tmp_path: Path) -> None:
    with bind_test_observation_reads(engine=_late):
        built = _build(tmp_path, "--platform", "linux/amd64")

    assert built.result.exit_code == 0  # the mutation proceeded
    assert built.receipt["plan"]["daemon_platform"] is None
    assert built.receipt["plan"]["platform"]["requested_emulated"] == "unknown"
    assert _image_evidence(built.record)["platform"] == {"image": AMD_IMAGE, "emulated": "unknown"}
    assert built.record["infrastructure_failure"] is None and "failure_evidence" not in built.record
    built.leave()


def test_build_platform_unmapped_daemon_architecture_is_unknown(tmp_path: Path) -> None:
    with bind_test_observation_reads(engine=_info("riscv64")):
        built = _build(tmp_path, "--platform", "linux/amd64")

    assert built.receipt["plan"]["daemon_platform"] is None
    assert _image_evidence(built.record)["platform"]["emulated"] == "unknown"
    built.leave()


def test_build_platform_failed_build_records_no_image_platform(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    fake.fail_build = True
    with bind_test_observation_reads(engine=_info("aarch64")):
        built = _build(tmp_path, "--platform", "linux/amd64", fake=fake)

    assert built.result.exit_code == 1
    assert built.receipt["plan"]["platform"]["requested_emulated"] == "emulated"  # the request is known at preparation
    assert not any(item["state"] != "removed" for item in built.record["owned_resources"]["images"].values())
    assert built.record["infrastructure_failure"] is None
    built.leave()


def _compose_up(tmp_path: Path, *, image: dict | None = None, service: dict | None = None):
    """Bring a one-service Compose project up and return its receipt plan; the caller binds the Engine read."""
    fake = FakeDockerPort()
    for key, value in (image or {}).items():
        fake.images["fixture"][key] = value
    fake.compose_service_extra = service or {}
    handle, descriptor = _entered_run(tmp_path, fake)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "up", "--detach"))
    record = json.loads(handle._context.record.read_text())
    plan = next(item for item in record["docker_receipts"].values() if item["kind"] == "compose")["plan"]
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "down", "--volumes"))
    handle.leave()
    return plan, record


def test_compose_platform_records_an_emulated_image_without_a_requested_platform(tmp_path: Path) -> None:
    with bind_test_observation_reads(engine=_info("aarch64")):
        plan, record = _compose_up(tmp_path, image={"Os": "linux", "Architecture": "amd64"})

    assert plan["daemon_platform"] == ARM_DAEMON
    assert plan["platforms"] == {"fixture": {"requested": None, "image": AMD_IMAGE, "emulated": "emulated"}}
    assert record["infrastructure_failure"] is None


def test_compose_platform_requested_and_native(tmp_path: Path) -> None:
    with bind_test_observation_reads(engine=_info("aarch64")):
        plan, _ = _compose_up(
            tmp_path,
            image={"Os": "linux", "Architecture": "arm64", "Variant": "v8"},
            service={"platform": "linux/arm64"},
        )

    assert plan["platforms"]["fixture"] == {"requested": ARM_DAEMON, "image": ARM_DAEMON, "emulated": "native"}


def test_compose_platform_malformed_image_attributes_fall_back_to_the_request(tmp_path: Path) -> None:
    with bind_test_observation_reads(engine=_info("aarch64")):
        plan, record = _compose_up(tmp_path, image={"Os": 3, "Architecture": None}, service={"platform": "linux/amd64"})
        (tmp_path / "second").mkdir()
        unknown, _ = _compose_up(tmp_path / "second", image={"Os": 3, "Architecture": None})

    assert plan["platforms"]["fixture"] == {"requested": AMD_IMAGE, "image": None, "emulated": "emulated"}
    assert unknown["platforms"]["fixture"] == {"requested": None, "image": None, "emulated": "unknown"}
    assert record["infrastructure_failure"] is None


def test_compose_platform_daemon_timeout_fails_open(tmp_path: Path) -> None:
    with bind_test_observation_reads(engine=_late):
        plan, record = _compose_up(tmp_path, image={"Os": "linux", "Architecture": "amd64"})

    assert plan["daemon_platform"] is None
    assert plan["platforms"]["fixture"] == {"requested": None, "image": AMD_IMAGE, "emulated": "unknown"}
    assert record["infrastructure_failure"] is None


def test_build_platform_the_image_wins_over_the_request(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    fake.build_reports_platform = "linux/amd64"  # asked for arm64, the builder produced amd64
    with bind_test_observation_reads(engine=_info("aarch64")):
        built = _build(tmp_path, "--platform", "linux/arm64", fake=fake)

    assert built.receipt["plan"]["platform"]["requested_emulated"] == "native"  # what was asked for
    assert _image_evidence(built.record)["platform"] == {"image": AMD_IMAGE, "emulated": "emulated"}  # what runs
    built.leave()


def test_build_platform_a_valueless_option_is_not_a_new_refusal(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    handle, descriptor = _entered_run(tmp_path, fake)
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("buildx", "create"))
    with bind_test_observation_reads(engine=_info("aarch64")):
        result = lifecycle.dispatch_docker(
            handle._context, descriptor, fake, ("buildx", "build", "--tag", "fixture:latest", ".", "--platform")
        )

    record = json.loads(handle._context.record.read_text())
    receipt = next(item for item in record["docker_receipts"].values() if item["kind"] == "build")
    assert result.exit_code == 0  # Docker, not the recorder, owns refusing a malformed option
    assert receipt["plan"]["platform"] == {"requested": None, "requested_raw": None, "requested_emulated": "unknown"}
    Built(handle, descriptor, fake, record, receipt, result).leave()


def test_compose_platform_the_image_wins_over_the_request(tmp_path: Path) -> None:
    with bind_test_observation_reads(engine=_info("aarch64")):
        plan, _ = _compose_up(
            tmp_path, image={"Os": "linux", "Architecture": "amd64"}, service={"platform": "linux/arm64"}
        )

    assert plan["platforms"]["fixture"] == {"requested": ARM_DAEMON, "image": AMD_IMAGE, "emulated": "emulated"}


def test_compose_platform_a_non_string_platform_is_no_request(tmp_path: Path) -> None:
    with bind_test_observation_reads(engine=_info("aarch64")):
        plan, _ = _compose_up(tmp_path, image={"Os": "linux", "Architecture": "arm64"}, service={"platform": ["x"]})

    assert plan["platforms"]["fixture"] == {"requested": None, "image": ARM_DAEMON, "emulated": "native"}


def test_compose_run_records_only_the_selected_service(tmp_path: Path) -> None:
    fake = FakeDockerPort()
    fake.images["fixture"].update(Os="linux", Architecture="amd64")
    handle, descriptor = _entered_run(tmp_path, fake)
    with bind_test_observation_reads(engine=_info("aarch64")):
        lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "up", "--detach"))
        lifecycle.dispatch_docker(
            handle._context,
            descriptor,
            fake,
            ("compose", "run", "--rm", "--no-deps", "--entrypoint", "true", "fixture", "true"),
        )
    record = json.loads(handle._context.record.read_text())
    receipt = next(item for item in record["docker_receipts"].values() if item["kind"] == "compose-run")

    assert receipt["state"] == "promoted"  # the mutation succeeded: recording is evidence, not a gate
    assert receipt["plan"]["platforms"] == {"fixture": {"requested": None, "image": AMD_IMAGE, "emulated": "emulated"}}
    lifecycle.dispatch_docker(handle._context, descriptor, fake, ("compose", "down", "--volumes"))
    handle.leave()


def test_a_malformed_info_body_fails_open(tmp_path: Path) -> None:
    def engine(_docker: object, _path: str, _deadline: object) -> EngineReply:
        return EngineReply(["linux"], b"", 0)  # type: ignore[arg-type]

    with bind_test_observation_reads(engine=engine):
        built = _build(tmp_path, "--platform", "linux/amd64")

    assert built.receipt["plan"]["daemon_platform"] is None
    assert built.result.exit_code == 0
    built.leave()
