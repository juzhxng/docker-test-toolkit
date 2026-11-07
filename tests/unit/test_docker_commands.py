from __future__ import annotations

import pytest

from docker_test_toolkit.application.docker_commands import (
    ComposeShareFacts,
    ContainerShareFacts,
    classify_command,
    compose_share_facts,
    compose_trailing_globals,
    container_share_facts,
    requested_platform_values,
)
from docker_test_toolkit.domain.errors import InvalidInput
from docker_test_toolkit.domain.models import ExternalDockerResource
from docker_test_toolkit.domain.platform import Platform, parse_requested_platform

LABELS = {
    "dev.docker-test-toolkit.project": "a" * 64,
    "dev.docker-test-toolkit.session": "11111111-1111-1111-1111-111111111111",
    "dev.docker-test-toolkit.run": "22222222-2222-2222-2222-222222222222",
}
IMAGE_ID = "sha256:" + "f" * 64
NETWORK_ID = "e" * 64
EXTERNAL_IMAGE = (ExternalDockerResource("image", IMAGE_ID, "fixture", "use_image"),)
EXTERNAL_NETWORK = ExternalDockerResource("network", NETWORK_ID, "fixture", "connect_owned_container")


def test_container_run_receipt_has_generated_exact_reconciliation_key() -> None:
    command = classify_command(
        ("run", "--rm", IMAGE_ID, "true"),
        operation_id="33333333-3333-3333-3333-333333333333",
        run_id=LABELS["dev.docker-test-toolkit.run"],
        compose_project="dtt-example",
        builder="dtt-builder",
        labels=LABELS,
        private_root="/private/run",
        owned={},
        external=EXTERNAL_IMAGE,
    )

    assert command.kind == "container-create"
    assert command.blocking
    assert command.plan == {
        "cidfile": "/private/run/cid-33333333-3333-3333-3333-333333333333",
        "image_id": IMAGE_ID,
        "network_id": None,
        "name": "dtt-222222222222-333333333333",
        "remove": True,
    }
    assert command.argv[:2] == ("run", "--pull=never")
    assert "--cidfile" in command.argv
    assert "--name" in command.argv
    assert "true" not in repr(command.plan)


@pytest.mark.parametrize("prefix", [("create",), ("container", "create")])
def test_container_create_remains_a_short_mutation(prefix: tuple[str, ...]) -> None:
    command = classify_command(
        (*prefix, IMAGE_ID),
        operation_id="33333333-3333-3333-3333-333333333333",
        run_id=LABELS["dev.docker-test-toolkit.run"],
        compose_project="dtt-example",
        builder="dtt-builder",
        labels=LABELS,
        private_root="/private/run",
        owned={},
        external=EXTERNAL_IMAGE,
    )

    assert not command.blocking


@pytest.mark.parametrize(
    "argv",
    [
        ("run", "busybox", "echo", IMAGE_ID),
        ("run", "--rm", "busybox", "echo", IMAGE_ID),
        ("container", "run", "busybox", "echo", IMAGE_ID),
    ],
)
def test_container_run_binds_the_positional_image_not_a_command_argument(argv: tuple[str, ...]) -> None:
    with pytest.raises(InvalidInput, match="image"):
        classify_command(
            argv,
            operation_id="33333333-3333-3333-3333-333333333333",
            run_id=LABELS["dev.docker-test-toolkit.run"],
            compose_project="dtt-example",
            builder="dtt-builder",
            labels=LABELS,
            private_root="/private/run",
            owned={},
            external=EXTERNAL_IMAGE,
        )


def test_container_run_preserves_bounded_containment_options() -> None:
    command = classify_command(
        (
            "run",
            "--interactive",
            "--rm",
            "--mount",
            "type=bind,src=/source,dst=/src,readonly",
            "--network=none",
            IMAGE_ID,
            "true",
        ),
        operation_id="33333333-3333-3333-3333-333333333333",
        run_id=LABELS["dev.docker-test-toolkit.run"],
        compose_project="dtt-example",
        builder="dtt-builder",
        labels=LABELS,
        private_root="/private/run",
        owned={},
        external=EXTERNAL_IMAGE,
    )

    assert command.plan["image_id"] == IMAGE_ID
    assert command.plan["network_id"] is None


def test_container_run_preserves_the_current_audit_containment_surface() -> None:
    command = classify_command(
        (
            "run",
            "--interactive",
            "--rm",
            "--init",
            "--user=1000:1000",
            "--read-only",
            "--cap-drop=ALL",
            "--security-opt=no-new-privileges=true",
            "--security-opt=seccomp=builtin",
            "--cpus=1",
            "--memory=536870912",
            "--memory-swap=536870912",
            "--pids-limit=128",
            "--tmpfs=/tmp:rw,noexec,nosuid,nodev,size=67108864,mode=1777",
            "--shm-size=16m",
            "--ulimit=core=0:0",
            "--log-driver=none",
            "--stop-timeout=5",
            f"--network={NETWORK_ID}",
            "--sysctl=net.ipv6.conf.all.disable_ipv6=1",
            "--dns=127.0.0.1",
            "--env=FIXED_IDENTITY=value",
            "--mount",
            "type=bind,src=/source,dst=/src,readonly",
            "--mount",
            "type=bind,src=/controls,dst=/audit-controls,readonly",
            IMAGE_ID,
            "true",
        ),
        operation_id="33333333-3333-3333-3333-333333333333",
        run_id=LABELS["dev.docker-test-toolkit.run"],
        compose_project="dtt-example",
        builder="dtt-builder",
        labels=LABELS,
        private_root="/private/run",
        owned={},
        external=(*EXTERNAL_IMAGE, EXTERNAL_NETWORK),
    )

    assert command.plan["image_id"] == IMAGE_ID
    assert command.plan["network_id"] == NETWORK_ID


def test_container_run_requires_an_exact_declared_explicit_network() -> None:
    common = {
        "operation_id": "33333333-3333-3333-3333-333333333333",
        "run_id": LABELS["dev.docker-test-toolkit.run"],
        "compose_project": "dtt-example",
        "builder": "dtt-builder",
        "labels": LABELS,
        "private_root": "/private/run",
        "owned": {},
    }

    with pytest.raises(InvalidInput, match="network"):
        classify_command(("run", "--network=foreign", IMAGE_ID), external=EXTERNAL_IMAGE, **common)

    command = classify_command(
        ("run", f"--network={NETWORK_ID}", IMAGE_ID),
        external=(*EXTERNAL_IMAGE, EXTERNAL_NETWORK),
        **common,
    )
    assert command.plan["network_id"] == NETWORK_ID

    owned = dict(common, owned={"networks": {NETWORK_ID: {"state": "running"}}})
    command = classify_command(
        ("run", f"--network={NETWORK_ID}", IMAGE_ID),
        external=EXTERNAL_IMAGE,
        **owned,
    )
    assert command.plan["network_id"] == NETWORK_ID


@pytest.mark.parametrize(
    "argv",
    [
        ("system", "prune"),
        ("container", "prune"),
        ("image", "prune"),
        ("network", "prune"),
        ("volume", "prune"),
        ("buildx", "prune"),
        ("builder", "prune"),
        ("run", "--cidfile", "/tmp/foreign", "alpine"),
        ("run", "--name", "foreign", "alpine"),
        ("compose", "--project-name", "foreign", "up"),
        ("compose", "--project-name", "up", "up"),  # a value equal to a subcommand is still the project name
        ("compose", "-p", "up", "up"),
        ("compose", "--project-name=foreign", "up"),
        ("compose", "down", "--rmi", "all"),
        ("compose", "down", "--rmi=local"),
        ("compose", "down", "--remove-orphans"),
        ("compose", "restart", "--project-name=foreign"),
        ("buildx", "create", "--use"),
        ("buildx", "create", "--driver-opt=network=host"),
        ("buildx", "ls"),
        ("buildx", "build", "--tag", "fixture:latest", "--push", "."),
        ("buildx", "build", "--tag", "fixture:latest", "--output", "type=registry", "."),
        ("buildx", "build", "--tag", "fixture:latest", "--metadata-file", "/tmp/foreign", "."),
        ("buildx", "build", "--tag", "fixture:latest", "--resource", "memory=99g", "."),
        ("buildx", "build", "--tag", "fixture:latest", "--cache-from", "type=local,src=/x", "."),
        ("buildx", "build", "--tag", "fixture:latest", "--cache-from=type=registry,ref=r", "."),
        ("buildx", "build", "--tag", "fixture:latest", "-otype=local,dest=/tmp/x", "."),
        ("buildx", "build", "--tag", "fixture:latest", "-qotype=local,dest=/tmp/x", "."),
        ("buildx", "build", "--tag", "fixture:latest", "-o", "type=local,dest=/tmp/x", "."),
        ("buildx", "build", "--tag", "fixture:latest", "-o=type=local,dest=/tmp/x", "."),
        ("buildx", "build", "--tag", "fixture:latest", "--iidfile", "/tmp/victim", "."),
        ("buildx", "build", "--tag", "fixture:latest", "--iidfile=/tmp/victim", "."),
        ("buildx", "build", "--tag", "fixture:latest", "--load=false", "."),
        ("buildx", "build", "--tag", "fixture:latest", "--sbom=true", "."),
        ("buildx", "build", "--tag", "fixture:latest", "--sbom", "."),
        ("buildx", "build", "--tag", "fixture:latest", "--attest", "type=sbom", "."),
        ("buildx", "build", "--tag", "fixture:latest", "--attest=type=provenance", "."),
        ("buildx", "build", "--tag", "fixture:latest", "--provenance=true", "."),
        ("buildx", "build", "--tag", "fixture:latest", "--provenance=mode=max", "."),
        ("buildx", "build", "--tag", "fixture:latest", "--provenance", "."),
        ("compose", "-pforeign", "down"),
        ("compose", "-p=foreign", "down"),
        pytest.param(("compose", "--file", "compose.yml", "-pforeign", "up"), id="argv42"),
        pytest.param(("buildx", "build", "--tag", "fixture:latest", "-qo", "type=local,dest=/tmp/x", "."), id="argv43"),
        pytest.param(("buildx", "build", "--tag", "fixture:latest", "--load=true", "."), id="argv44"),
        pytest.param(("buildx", "build", "--tag", "fixture:latest", "--provenance=0", "."), id="argv45"),
        pytest.param(("buildx", "build", "--tag", "fixture:latest", "--provenance=mode=min", "."), id="argv46"),
        pytest.param(("buildx", "build", "-t", "fixture:latest", "-t=", "."), id="argv47"),
        pytest.param(("buildx", "build", "-t"), id="argv48"),
        pytest.param(
            ("compose", "run", "-pforeign", "--rm", "--no-deps", "--entrypoint", "task", "svc", "go"), id="argv49"
        ),
        pytest.param(("compose", "-p", "foreign", "down"), id="argv50"),
        pytest.param(("compose", "-dpforeign", "up"), id="argv51"),
        pytest.param(("compose", "exec", "-pforeign", "svc", "sh"), id="argv52"),
        pytest.param(("compose", "exec", "--index", "--user", "-pforeign", "svc", "sh"), id="argv53"),
        pytest.param(("compose", "up", "-fcompose.yml"), id="argv54"),
        pytest.param(
            ("compose", "run", "--project-name", "foreign", "--rm", "--no-deps", "--entrypoint", "task", "svc", "go"),
            id="argv55",
        ),
        pytest.param(("compose", "--project-directory", "x", "up", "--remove-orphans"), id="argv56"),
        pytest.param(("compose", "--file", "compose.yml", "down", "--rmi", "all"), id="argv57"),
        pytest.param(("compose", "exec", "-Te", "A=b", "-pforeign", "svc", "sh"), id="argv58"),
        pytest.param(("compose", "exec", "--index", "1", "-pforeign", "svc", "sh"), id="argv59"),
        pytest.param(("compose", "exec", "--index=1", "--project-name=foreign", "svc", "sh"), id="argv60"),
        pytest.param(("compose", "exec", "--unknown-future-flag", "x", "-pforeign", "svc", "sh"), id="argv61"),
        pytest.param(("compose", "exec", "-x", "svc", "sh"), id="argv62"),
        pytest.param(("compose", "exec", "-e", "A=b", "-pforeign", "svc", "sh"), id="argv63"),
        pytest.param(("compose", "exec", "-eA=b", "-p", "foreign", "svc", "sh"), id="argv64"),
        pytest.param(("compose", "exec", "--project-name", "foreign", "svc", "sh"), id="argv65"),
        pytest.param(("compose", "cp", "-pforeign", "svc:/a", "/b"), id="argv68"),
        pytest.param(
            ("compose", "-pforeign", "run", "--rm", "--no-deps", "--entrypoint", "task", "svc", "go"), id="argv69"
        ),
    ],
)
def test_global_or_caller_selected_authority_is_refused(argv: tuple[str, ...]) -> None:
    with pytest.raises(InvalidInput):
        classify_command(
            argv,
            operation_id="33333333-3333-3333-3333-333333333333",
            run_id=LABELS["dev.docker-test-toolkit.run"],
            compose_project="dtt-example",
            builder="dtt-builder",
            labels=LABELS,
            private_root="/private/run",
            owned={},
            external=(),
        )


def test_buildx_builder_and_steps_receive_the_fixed_calibrated_envelope() -> None:
    common = {
        "operation_id": "33333333-3333-3333-3333-333333333333",
        "run_id": LABELS["dev.docker-test-toolkit.run"],
        "compose_project": "dtt-example",
        "builder": "dtt-builder",
        "labels": LABELS,
        "private_root": "/private/run",
        "owned": {},
        "external": (),
    }

    create = classify_command(("buildx", "create"), **common)
    build = classify_command(("buildx", "build", "--tag", "fixture:latest", "."), **common)

    assert create.argv == (
        "buildx",
        "create",
        "--name",
        "dtt-builder",
        "--driver",
        "docker-container",
        "--bootstrap",
        "--driver-opt",
        "memory=2048m",
        "--driver-opt",
        "memory-swap=2048m",
        "--driver-opt",
        "cpu-period=100000",
        "--driver-opt",
        "cpu-quota=200000",
        "--driver-opt",
        "restart-policy=no",
        "--buildkitd-config",
        "/private/run/buildkitd.toml",
    )
    assert create.plan["memory_bytes"] == 2 * 1024**3
    assert create.plan["daemon_cpus"] == 2
    assert create.plan["max_parallelism"] == 1
    assert build.argv[7:15] == (
        "--resource",
        "memory=2048m",
        "--resource",
        "memory-swap=2048m",
        "--resource",
        "cpu-period=100000",
        "--resource",
        "cpu-quota=200000",
    )


@pytest.mark.parametrize(
    "argv",
    [
        ("run", "--pull=always", IMAGE_ID),
        ("run", "--label=dev.docker-test-toolkit.run=foreign", IMAGE_ID),
        ("run", "--label-file=/tmp/foreign", IMAGE_ID),
        ("run", "--volume=data:/data", IMAGE_ID),
        ("run", "--volumes-from=foreign", IMAGE_ID),
        ("run", "--mount=type=volume,src=data,dst=/data", IMAGE_ID),
        ("run", "--mount", "type=volume,src=data,dst=/data", IMAGE_ID),
        ("run", "--mount=src=data,dst=/data", IMAGE_ID),
        ("run", "--mount=type=bind,type=bind,src=data,dst=/data", IMAGE_ID),
        ("run", '--mount=type=bind,"unterminated', IMAGE_ID),
        ("run", "--mount=type=bind,src=data,dst=/data,type=volume", IMAGE_ID),
        ("run", "--mount", "type=bind,src=/source,dst=/data,type=tmpfs", IMAGE_ID),
        ("run", '--mount=type=bind,"type=volume",src=data,dst=/data', IMAGE_ID),
        ("run", "--net=host", IMAGE_ID),
        ("run", "--net=container:" + "a" * 64, IMAGE_ID),
        ("run", "--privileged=true", IMAGE_ID),
        ("run", "--restart=always", IMAGE_ID),
        ("run", "--publish=80:80", IMAGE_ID),
        ("run", "--rm=true", IMAGE_ID),
        ("run", "--security-opt=seccomp=unconfined", IMAGE_ID),
        ("run", "--cap-add=SYS_ADMIN", IMAGE_ID),
    ],
)
def test_container_run_refuses_unplanned_resource_authority(argv: tuple[str, ...]) -> None:
    with pytest.raises(InvalidInput):
        classify_command(
            argv,
            operation_id="33333333-3333-3333-3333-333333333333",
            run_id=LABELS["dev.docker-test-toolkit.run"],
            compose_project="dtt-example",
            builder="dtt-builder",
            labels=LABELS,
            private_root="/private/run",
            owned={},
            external=EXTERNAL_IMAGE,
        )


def test_exact_container_control_requires_a_promoted_owned_id() -> None:
    container_id = "a" * 64
    command = classify_command(
        ("container", "stop", container_id),
        operation_id="33333333-3333-3333-3333-333333333333",
        run_id=LABELS["dev.docker-test-toolkit.run"],
        compose_project="dtt-example",
        builder="dtt-builder",
        labels=LABELS,
        private_root="/private/run",
        owned={"containers": {container_id: {"state": "running"}}},
        external=(),
    )

    assert command.kind == "container-stop"
    assert command.plan == {"container_id": container_id}

    with pytest.raises(InvalidInput, match="owned container"):
        classify_command(
            ("container", "stop", "b" * 64),
            operation_id="33333333-3333-3333-3333-333333333333",
            run_id=LABELS["dev.docker-test-toolkit.run"],
            compose_project="dtt-example",
            builder="dtt-builder",
            labels=LABELS,
            private_root="/private/run",
            owned={"containers": {container_id: {"state": "running"}}},
            external=(),
        )


def test_exact_container_exec_requires_a_promoted_owned_id_and_command() -> None:
    container_id = "a" * 64
    common = {
        "operation_id": "33333333-3333-3333-3333-333333333333",
        "run_id": LABELS["dev.docker-test-toolkit.run"],
        "compose_project": "dtt-example",
        "builder": "dtt-builder",
        "labels": LABELS,
        "private_root": "/private/run",
        "external": (),
    }

    command = classify_command(
        ("container", "exec", container_id, "cat", "/sys/fs/cgroup/memory.peak"),
        owned={"containers": {container_id: {"state": "running"}}},
        **common,
    )

    assert command.kind == "container-io"
    assert command.plan == {"container_id": container_id}
    assert not command.mutating

    with pytest.raises(InvalidInput, match="owned container"):
        classify_command(
            ("container", "exec", "b" * 64, "true"),
            owned={"containers": {container_id: {"state": "running"}}},
            **common,
        )
    with pytest.raises(InvalidInput, match="command"):
        classify_command(
            ("container", "exec", container_id),
            owned={"containers": {container_id: {"state": "running"}}},
            **common,
        )


@pytest.mark.parametrize(
    "argv",
    [
        ("rm", "b" * 64, "a" * 64),
        ("container", "rm", "b" * 64, "a" * 64),
    ],
)
def test_container_removal_refuses_every_extra_target(argv: tuple[str, ...]) -> None:
    with pytest.raises(InvalidInput, match="exactly one"):
        classify_command(
            argv,
            operation_id="33333333-3333-3333-3333-333333333333",
            run_id=LABELS["dev.docker-test-toolkit.run"],
            compose_project="dtt-example",
            builder="dtt-builder",
            labels=LABELS,
            private_root="/private/run",
            owned={"containers": {"a" * 64: {"state": "stopped"}}},
            external=(),
        )


def test_container_removal_preserves_bounded_single_target_flags() -> None:
    container_id = "a" * 64
    command = classify_command(
        ("container", "rm", "--force", "--volumes", container_id),
        operation_id="33333333-3333-3333-3333-333333333333",
        run_id=LABELS["dev.docker-test-toolkit.run"],
        compose_project="dtt-example",
        builder="dtt-builder",
        labels=LABELS,
        private_root="/private/run",
        owned={"containers": {container_id: {"state": "stopped"}}},
        external=(),
    )

    assert command.argv == ("container", "rm", "--force", "--volumes", container_id)
    assert command.plan == {"container_id": container_id}


def test_compose_create_forces_local_images_and_disables_builds() -> None:
    command = classify_command(
        ("compose", "--file", "compose.yml", "up", "--detach"),
        operation_id="33333333-3333-3333-3333-333333333333",
        run_id=LABELS["dev.docker-test-toolkit.run"],
        compose_project="dtt-example",
        builder="dtt-builder",
        labels=LABELS,
        private_root="/private/run",
        owned={},
        external=(),
    )

    assert command.argv == (
        "compose",
        "--project-name",
        "dtt-example",
        "--file",
        "compose.yml",
        "up",
        "--pull",
        "never",
        "--no-build",
        "--detach",
    )


def test_compose_run_gets_one_exact_reconciliation_name() -> None:
    command = classify_command(
        (
            "compose",
            "--file",
            "compose.yml",
            "run",
            "--rm",
            "--no-deps",
            "--entrypoint",
            "neo4j-admin",
            "e2e-neo4j",
            "database",
            "dump",
            "neo4j",
        ),
        operation_id="33333333-3333-3333-3333-333333333333",
        run_id=LABELS["dev.docker-test-toolkit.run"],
        compose_project="dtt-example",
        builder="dtt-builder",
        labels=LABELS,
        private_root="/private/run",
        owned={},
        external=(),
    )

    assert command.kind == "compose-run"
    assert command.blocking
    assert command.argv == (
        "compose",
        "--project-name",
        "dtt-example",
        "--file",
        "compose.yml",
        "run",
        "--pull",
        "never",
        "--name",
        "dtt-222222222222-333333333333",
        "--rm",
        "--no-deps",
        "--entrypoint",
        "neo4j-admin",
        "e2e-neo4j",
        "database",
        "dump",
        "neo4j",
    )
    assert command.plan == {
        "project": "dtt-example",
        "subcommand": "run",
        "subcommand_index": 5,
        "service": "e2e-neo4j",
        "name": "dtt-222222222222-333333333333",
        "remove": True,
    }
    assert "neo4j-admin" not in repr(command.plan)


@pytest.mark.parametrize(
    "arguments",
    [
        ("--no-deps", "--entrypoint", "true", "fixture", "true"),
        ("--rm", "--entrypoint", "true", "fixture", "true"),
        ("--rm", "--no-deps", "fixture", "true"),
        ("--rm", "--no-deps", "--entrypoint", "true", "fixture"),
        ("--rm", "--rm", "--no-deps", "--entrypoint", "true", "fixture", "true"),
        ("--rm", "--no-deps", "--entrypoint", "true", "--name", "foreign", "fixture", "true"),
        ("--rm", "--no-deps", "--entrypoint", "true", "--volume", "data:/data", "fixture", "true"),
        ("--rm", "--no-deps", "--entrypoint", "true", "--env", "A=B", "fixture", "true"),
        ("--rm", "--no-deps", "--entrypoint", "true", "--publish", "80:80", "fixture", "true"),
        ("--rm", "--no-deps", "--entrypoint", "true", "--service-ports", "fixture", "true"),
        ("--rm", "--no-deps", "--entrypoint", "true", "--detach", "fixture", "true"),
        ("--rm", "--no-deps", "--entrypoint", "true", "--build", "fixture", "true"),
        ("--rm", "--no-deps", "--entrypoint", "true", "--pull", "always", "fixture", "true"),
        ("--rm", "--no-deps", "--entrypoint", "true", "invalid/service", "true"),
    ],
)
def test_compose_run_refuses_authority_outside_one_bounded_task(arguments: tuple[str, ...]) -> None:
    with pytest.raises(InvalidInput, match="bounded one-shot"):
        classify_command(
            ("compose", "run", *arguments),
            operation_id="33333333-3333-3333-3333-333333333333",
            run_id=LABELS["dev.docker-test-toolkit.run"],
            compose_project="dtt-example",
            builder="dtt-builder",
            labels=LABELS,
            private_root="/private/run",
            owned={},
            external=(),
        )


@pytest.mark.parametrize("flag", ("--pull=always", "--build"))
def test_compose_run_refuses_image_acquisition_before_the_subcommand(flag: str) -> None:
    with pytest.raises(InvalidInput, match="image acquisition"):
        classify_command(
            ("compose", flag, "run", "--rm", "--no-deps", "--entrypoint", "true", "fixture", "true"),
            operation_id="33333333-3333-3333-3333-333333333333",
            run_id=LABELS["dev.docker-test-toolkit.run"],
            compose_project="dtt-example",
            builder="dtt-builder",
            labels=LABELS,
            private_root="/private/run",
            owned={},
            external=(),
        )


def test_compose_exec_payload_flags_are_not_parsed_as_global_authority() -> None:
    command = classify_command(
        ("compose", "exec", "test-client", "mkdir", "-p", "--remove-orphans"),
        operation_id="33333333-3333-3333-3333-333333333333",
        run_id=LABELS["dev.docker-test-toolkit.run"],
        compose_project="dtt-example",
        builder="dtt-builder",
        labels=LABELS,
        private_root="/private/run",
        owned={},
        external=(),
    )

    assert command.kind == "compose-io"
    assert not command.mutating
    assert command.plan["subcommand"] == "exec"


@pytest.mark.parametrize("flag", ("-p", "--project-name=foreign"))
def test_compose_mutation_rejects_project_override_after_subcommand(flag: str) -> None:
    with pytest.raises(InvalidInput, match="project identity"):
        classify_command(
            ("compose", "up", flag),
            operation_id="33333333-3333-3333-3333-333333333333",
            run_id=LABELS["dev.docker-test-toolkit.run"],
            compose_project="dtt-example",
            builder="dtt-builder",
            labels=LABELS,
            private_root="/private/run",
            owned={},
            external=(),
        )


@pytest.mark.parametrize(
    "tag_argv",
    [
        ("-t", "owned:1"),
        ("-t=owned:1",),
        ("-towned:1",),
        ("-qtowned:1",),
        ("-qt", "owned:1"),
        ("-qtowned:1", "-q=true"),
        ("-q=false", "-towned:1"),
        ("--tag", "owned:1"),
        ("--tag=owned:1",),
    ],
)
def test_every_build_tag_spelling_reaches_the_receipt_plan(tag_argv: tuple[str, ...]) -> None:
    command = classify_command(
        ("buildx", "build", *tag_argv, "."),
        operation_id="33333333-3333-3333-3333-333333333333",
        run_id=LABELS["dev.docker-test-toolkit.run"],
        compose_project="dtt-example",
        builder="dtt-builder",
        labels=LABELS,
        private_root="/private/run",
        owned={},
        external=(),
    )

    assert command.plan["tags"] == ["owned:1"]


@pytest.mark.parametrize(
    "argv",
    [
        ("buildx", "build", "-t", "fixture:latest", "-fDockerfile.opt", "."),
        ("buildx", "build", "-t", "fixture:latest", "-f", "Dockerfile.opt", "."),
        ("buildx", "build", "-t", "fixture:latest", "--provenance=false", "."),
        ("buildx", "build", "-t", "fixture:latest", "--load", "."),
        ("buildx", "build", "-tapp:opt", "-q=true", "."),
        ("compose", "-fcompose.yml", "up", "-d"),
        ("compose", "--file", "compose.yml", "up", "--detach"),
        ("compose", "down", "-t", "5"),
        ("compose", "exec", "-e", "A=p", "-eapp=1", "svc", "mkdir", "-p", "x"),
        ("compose", "exec", "svc", "sh", "-c", "tool -p foreign --project-name=x"),
        ("compose", "cp", "svc:/a", "-"),
        ("compose", "exec", "-T", "-e", "A=1", "svc", "mkdir", "-p", "x"),
        ("compose", "exec", "-Te", "A=p", "--user", "root", "svc", "sh", "-c", "-p"),
        ("compose", "exec", "-dT", "--index=2", "svc", "true"),
        ("compose", "exec", "--detach=false", "--no-tty=true", "svc", "true"),
        ("compose", "up", "-f", "compose.yml"),
        ("compose", "logs", "-t"),
    ],
)
def test_attached_values_that_only_contain_a_forbidden_letter_are_not_options(argv: tuple[str, ...]) -> None:
    command = classify_command(
        argv,
        operation_id="33333333-3333-3333-3333-333333333333",
        run_id=LABELS["dev.docker-test-toolkit.run"],
        compose_project="dtt-example",
        builder="dtt-builder",
        labels=LABELS,
        private_root="/private/run",
        owned={},
        external=(),
    )

    assert command.kind in {"build", "compose", "compose-io", "observe"}


def test_mixed_tag_spellings_are_recorded_once_and_in_order() -> None:
    command = classify_command(
        ("buildx", "build", "-towned:1", "--tag=owned:2", "--tag", "owned:1", "."),
        operation_id="33333333-3333-3333-3333-333333333333",
        run_id=LABELS["dev.docker-test-toolkit.run"],
        compose_project="dtt-example",
        builder="dtt-builder",
        labels=LABELS,
        private_root="/private/run",
        owned={},
        external=(),
    )

    assert command.plan["tags"] == ["owned:1", "owned:2"]


@pytest.mark.parametrize(
    ("build_argv", "tags"),
    [
        (("-f", "-t", "owned:1", "."), ["owned:1"]),
        (("--file", "-tforeign:1", "-towned:1", "."), ["foreign:1", "owned:1"]),
        (("-towned:1", "--file", "-f", "-tforeign:1", "."), ["owned:1", "foreign:1"]),
        (("--build-arg", "-t", "foreign:1", "-towned:1", "."), ["foreign:1", "owned:1"]),
    ],
)
def test_tag_precheck_records_every_tag_any_reading_could_apply(build_argv: tuple[str, ...], tags: list[str]) -> None:
    command = classify_command(
        ("buildx", "build", *build_argv),
        operation_id="33333333-3333-3333-3333-333333333333",
        run_id=LABELS["dev.docker-test-toolkit.run"],
        compose_project="dtt-example",
        builder="dtt-builder",
        labels=LABELS,
        private_root="/private/run",
        owned={},
        external=(),
    )

    assert command.plan["tags"] == tags


@pytest.mark.parametrize(
    "subcommand",
    ("config", "cp", "create", "down", "kill", "logs", "ps", "restart", "rm", "start", "stop", "top", "up"),
)
@pytest.mark.parametrize("override", ("-p", "-pforeign", "-p=foreign", "-tpforeign", "-dpforeign", "-qtpforeign"))
def test_no_compose_subcommand_hides_a_project_override_in_a_short_cluster(subcommand: str, override: str) -> None:
    argv = ("compose", subcommand, override) if override != "-p" else ("compose", subcommand, "-p", "foreign")

    with pytest.raises(InvalidInput):
        classify_command(
            argv,
            operation_id="33333333-3333-3333-3333-333333333333",
            run_id=LABELS["dev.docker-test-toolkit.run"],
            compose_project="dtt-example",
            builder="dtt-builder",
            labels=LABELS,
            private_root="/private/run",
            owned={},
            external=(),
        )


OPERATION_ID = "33333333-3333-3333-3333-333333333333"


def _classify_export(argv: tuple[str, ...], images: dict[str, dict[str, str]]):
    return classify_command(
        argv,
        operation_id=OPERATION_ID,
        run_id=LABELS["dev.docker-test-toolkit.run"],
        compose_project="dtt-example",
        builder="dtt-builder",
        labels=LABELS,
        private_root="/private/run",
        owned={"images": images},
        external=EXTERNAL_IMAGE,
    )


def test_image_save_of_one_owned_present_image_is_a_receipted_export_with_derived_paths() -> None:
    command = _classify_export(("image", "save", IMAGE_ID), {IMAGE_ID: {"state": "present"}})

    assert command.kind == "image-save" and command.mutating and not command.blocking
    assert command.argv == (
        "image",
        "save",
        "-o",
        f"/private/run/exports/{OPERATION_ID}/archive.tar",
        IMAGE_ID,
    )
    assert command.plan == {
        "schema": 1,
        "image_id": IMAGE_ID,
        "max_bytes": 4 * 1024**3,
        "directory": f"/private/run/exports/{OPERATION_ID}",
        "output": f"/private/run/exports/{OPERATION_ID}/archive.tar",
        "final": f"/private/run/exports/{OPERATION_ID}.tar",
        "archive": None,
    }


@pytest.mark.parametrize(
    "argv",
    [
        ("image", "save"),
        ("image", "save", "-o", "/tmp/x.tar", IMAGE_ID),
        ("image", "save", "--output=/tmp/x.tar", IMAGE_ID),
        ("image", "save", "--platform", "linux/arm64", IMAGE_ID),
        ("image", "save", IMAGE_ID, IMAGE_ID),
        ("image", "save", IMAGE_ID, "other"),
        ("image", "save", "example/cache:latest"),
        ("image", "save", "example/cache@sha256:" + "f" * 64),
        ("image", "save", "sha256:" + "f" * 63),
        ("image", "save", "sha256:" + "F" * 64),
        ("image", "save", "f" * 64),
        ("image", "save", IMAGE_ID + "\n"),
        ("save", IMAGE_ID),
        ("save", "-o", "/tmp/x.tar", IMAGE_ID),
        ("image", "save", "sha256:" + "a" * 64),
    ],
)
def test_image_save_refuses_everything_but_one_exact_owned_image_id(argv: tuple[str, ...]) -> None:
    with pytest.raises(InvalidInput):
        _classify_export(argv, {IMAGE_ID: {"state": "present"}})


@pytest.mark.parametrize("images", [{}, {IMAGE_ID: {"state": "removed"}}, {IMAGE_ID: "present"}])
def test_image_save_refuses_an_unowned_or_removed_image(images: dict) -> None:
    with pytest.raises(InvalidInput, match="owned image"):
        _classify_export(("image", "save", IMAGE_ID), images)


def _classify_build(argv: tuple[str, ...]):
    return classify_command(
        argv,
        operation_id=OPERATION_ID,
        run_id=LABELS["dev.docker-test-toolkit.run"],
        compose_project="dtt-example",
        builder="dtt-builder",
        labels=LABELS,
        private_root="/private/run",
        owned={},
        external=(),
    )


@pytest.mark.parametrize(
    "argv",
    [
        ("buildx", "build", "--output", "type=oci", "."),
        ("buildx", "build", "-o", "type=oci", "."),
        ("buildx", "build", "--output=type=oci", "--tag", "x:1", "."),
    ],
)
def test_an_archive_output_build_is_refused(argv: tuple[str, ...]) -> None:
    """The build-archive kind is gone: every ``--output`` spelling is toolkit authority again."""
    with pytest.raises(InvalidInput, match="owned by the toolkit"):
        _classify_build(argv)


def test_a_loaded_build_is_classified_as_a_build() -> None:
    command = _classify_build(("buildx", "build", "--tag", "x:1", "."))

    assert command.kind == "build" and "--load" in command.argv and "--output" not in command.argv


# ---- the classifier surface ------------------------------------------------------------------------------------------

_RUN = "22222222-2222-2222-2222-222222222222"
_OPERATION = "33333333-3333-3333-3333-333333333333"
_OWNED_CONTAINER = "c" * 64


def _classify(argv: tuple[str, ...], *, owned: dict | None = None, external: tuple = EXTERNAL_IMAGE):
    return classify_command(
        argv,
        operation_id=_OPERATION,
        run_id=LABELS["dev.docker-test-toolkit.run"],
        compose_project="dtt-example",
        builder="dtt-builder",
        labels=LABELS,
        private_root="/private/run",
        owned=owned or {},
        external=external,
    )


def test_classifier_surface() -> None:
    """Pin the generated names, the exact-control path and the closed container and compose-run grammars."""
    network = _classify(("network", "create"))
    volume = _classify(("volume", "create"))
    assert (network.kind, network.plan["name"]) == ("network-create", "dtt-222222222222-333333333333")
    assert (volume.kind, volume.plan["name"]) == ("volume-create", "dtt-222222222222-333333333333")
    with pytest.raises(InvalidInput):  # names are generated: a caller-chosen name is refused
        _classify(("network", "create", "shared-name"))

    owned = {"containers": {_OWNED_CONTAINER: {}}}
    for argv, kind in (
        (("container", "start", _OWNED_CONTAINER), "container-start"),
        (("container", "stop", _OWNED_CONTAINER), "container-stop"),
        (("container", "rm", _OWNED_CONTAINER), "container-rm"),
    ):
        assert _classify(argv, owned=owned).kind == kind  # promoted by the exact-control path, not container-create

    for loading_build in (("build", "."), ("docker", "build", ".")):
        with pytest.raises(InvalidInput):  # a plain build falls through to the final refusal
            _classify(loading_build)
    with pytest.raises(InvalidInput):
        _classify(("pull", "redis"))  # arbitrary references are refused; pulls go through the digest form only

    for argv in (  # the closed container grammar refuses publish, privilege, devices and added capabilities
        ("run", "--publish", "5432:5432", IMAGE_ID),
        ("run", "-p", "5432:5432", IMAGE_ID),
        ("run", "--privileged", IMAGE_ID),
        ("run", "--cap-add", "SYS_ADMIN", IMAGE_ID),
        ("run", "--device", "/dev/kvm", IMAGE_ID),
        ("run", "--network", "host", IMAGE_ID),
    ):
        with pytest.raises(InvalidInput):
            _classify(argv)

    for service_ports in (  # compose run accepts only --rm, --no-deps and --entrypoint before the service
        ("compose", "run", "--rm", "--no-deps", "--entrypoint", "true", "--service-ports", "fixture"),
        ("compose", "run", "--rm", "--no-deps", "--entrypoint", "true", "-p", "5432:5432", "fixture"),
    ):
        with pytest.raises(InvalidInput):
            _classify(service_ports)


@pytest.mark.parametrize(
    ("value", "parsed"),
    [
        ("linux/amd64", Platform("linux", "amd64")),
        ("linux/arm64/v8", Platform("linux", "arm64")),
        ("linux/amd64,linux/arm64", None),
        ("linux", None),
        ("linux/amd64/v2/x", None),
        ("Linux/AMD64", None),
        ("linux/amd64 ", None),
    ],
)
def test_requested_platform_grammar(value, parsed) -> None:
    assert parse_requested_platform(value) == parsed


def test_requested_platform_values_reads_both_spellings_in_order() -> None:
    assert requested_platform_values(("--platform", "linux/amd64", "--platform=linux/arm64", ".")) == [
        "linux/amd64",
        "linux/arm64",
    ]
    assert requested_platform_values((".",)) == []
    with pytest.raises(InvalidInput):
        requested_platform_values(("--platform",))


def _compose(*arguments: str):
    return classify_command(
        ("compose", "--file", "compose.yml", *arguments),
        operation_id="33333333-3333-3333-3333-333333333333",
        run_id=LABELS["dev.docker-test-toolkit.run"],
        compose_project="dtt-example",
        builder="dtt-builder",
        labels=LABELS,
        private_root="/private/run",
        owned={},
        external=(),
    )


@pytest.mark.parametrize(
    ("arguments", "wait"),
    [
        (("up", "--detach", "--wait", "--wait-timeout", "120"), 120),
        (("up", "--detach", "--wait", "--wait-timeout=90"), 90),
        (("up", "--detach", "--wait"), None),
        (("up", "--detach"), None),
        # start --wait waits on the same healthchecks (a database after an offline load was cut at 30 s)
        (("start", "--wait", "--wait-timeout", "300", "db"), 300),
        (("start", "--wait", "--wait-timeout=45", "db"), 45),
        (("start", "--wait", "--wait-timeout", "1", "db"), 1),
        (("start", "--wait", "--wait-timeout", "1200", "db"), 1200),
        (("start", "--wait", "db"), None),
        (("stop", "db"), None),
    ],
)
def test_compose_up_and_start_record_a_bounded_wait_timeout(arguments: tuple[str, ...], wait: int | None) -> None:
    assert _compose(*arguments).plan.get("wait_seconds") == wait


@pytest.mark.parametrize("value", ["0", "1201", "abc", "-5", "1.5"])
def test_compose_up_refuses_an_unbounded_or_malformed_wait_timeout(value: str) -> None:
    with pytest.raises(InvalidInput, match="wait timeout"):
        _compose("up", "--detach", "--wait", "--wait-timeout", value)
    with pytest.raises(InvalidInput, match="wait timeout"):
        _compose("up", "--detach", "--wait-timeout")
    with pytest.raises(InvalidInput, match="wait timeout"):
        _compose("start", "--wait", "--wait-timeout", value, "db")
    with pytest.raises(InvalidInput, match="wait timeout"):
        _compose("start", "--wait", "--wait-timeout")


@pytest.mark.parametrize(
    "arguments",
    [("create", "--pull=always"), ("create", "--build=true"), ("up", "--pull=always"), ("up", "--build=true")],
    ids=["create-pull", "create-build", "up-pull", "up-build"],
)
def test_compose_pull_and_build_equals_forms_refused(arguments: tuple[str, ...]) -> None:
    with pytest.raises(InvalidInput, match="image acquisition"):
        _compose(*arguments)


def test_compose_share_facts_read_services_scale_watch_and_files() -> None:
    facts = compose_share_facts(
        (
            "compose", "-f", "a.yml", "--file=b.yml", "--env-file", "e.env", "--profile", "postgres",
            "up", "--detach", "--scale", "web=2", "--wait-timeout", "60", "--no-deps", "--watch", "web", "db",
        )
    )  # fmt: skip

    assert facts == ComposeShareFacts(
        services=("web", "db"),
        no_deps=True,
        scale={"web": 2},
        watch=True,
        files=("a.yml", "b.yml"),
        env_files=("e.env",),
        stdin_file=False,
    )
    run = compose_share_facts(("compose", "--file", "-", "run", "--rm", "--no-deps", "tests", "pytest", "-x"))
    assert (run.services, run.stdin_file) == (("tests",), True)


def test_container_share_facts_read_memory_binds_and_network() -> None:
    facts = container_share_facts(
        (
            "run", "--memory=512m", "--network", "fixture",
            "--mount", "type=bind,source=/w/repo,target=/r,readonly",
            "--mount", "type=bind,src=/w/tmp/run,target=/o",
            "alpine", "true",
        )
    )  # fmt: skip

    assert facts == ContainerShareFacts(memory_bytes=512 << 20, binds=(("/w/repo", True), ("/w/tmp/run", False)))
    assert container_share_facts(("run", "alpine")).memory_bytes is None


# Plans classify_command produced at eaf4f6f, before the share facts existed.
_PINNED_UP = (
    "compose",
    "--project-name",
    "dtt-example",
    "--file",
    "compose.yml",
    "up",
    "--pull",
    "never",
    "--no-build",
)
_PINNED_PLAN = {"project": "dtt-example", "subcommand": "up", "subcommand_index": 5}


@pytest.mark.parametrize(
    "arguments",
    [("--detach",), ("--detach", "--scale", "web=2"), ("--watch",)],
    ids=["up-detached", "up-scale", "up-watch"],
)
def test_exclusive_receipt_plan_unchanged_by_share_facts(arguments: tuple[str, ...]) -> None:
    command = _compose("up", *arguments)

    assert (command.kind, command.argv, command.plan, command.blocking) == (
        "compose",
        (*_PINNED_UP, *arguments),
        _PINNED_PLAN,
        False,
    )


@pytest.mark.parametrize(
    ("trailing", "privileged"),
    [
        (("--privileged", "fixture", "true"), True),
        (("fixture", "run", "--privileged"), False),
        (("fixture", "true"), False),
    ],
    ids=["option", "payload-word", "absent"],
)
def test_compose_exec_privileged_is_read_from_the_option_table_only(
    trailing: tuple[str, ...], privileged: bool
) -> None:
    assert compose_share_facts(("compose", "--file", "c.yml", "exec", *trailing)).privileged is privileged


@pytest.mark.parametrize(
    ("argv", "memory"),
    [
        (("run", IMAGE_ID, "tool", "--memory=512m"), None),
        (("run", "--memory=2g", IMAGE_ID, "tool", "--memory=128m"), 2 << 30),
        (("container", "create", "--memory=1g", IMAGE_ID, "--network", "x"), 1 << 30),
    ],
    ids=["payload-only", "payload-after-real", "container-create"],
)
def test_container_share_facts_read_only_the_option_prefix(argv: tuple[str, ...], memory: int | None) -> None:
    facts = container_share_facts(argv)

    assert facts.memory_bytes == memory


@pytest.mark.parametrize("subcommand", ["start", "restart"])
def test_watch_is_read_only_for_up(subcommand: str) -> None:
    assert compose_share_facts(("compose", "--file", "c.yml", subcommand, "-w", "web")).watch is False


@pytest.mark.parametrize(
    ("argv", "hoisted", "files", "services"),
    [
        (("up", "--file=/b.yml"), ("--file=/b.yml",), ("/a.yml", "/b.yml"), ()),
        (("up", "--file", "/b.yml", "web"), ("--file", "/b.yml"), ("/a.yml", "/b.yml"), ("web",)),
        (("up", "-f", "/b.yml"), ("--file", "/b.yml"), ("/a.yml", "/b.yml"), ()),
        (("up", "--profile=evil", "--env-file=/e.env"), ("--profile=evil", "--env-file=/e.env"), ("/a.yml",), ()),
        (("create", "--project-directory", "/x"), ("--project-directory", "/x"), ("/a.yml",), ()),
        (("logs", "-f", "web"), (), ("/a.yml",), ("web",)),  # logs' own --follow, never a file
        (("down", "--file=/b.yml"), ("--file=/b.yml",), ("/a.yml", "/b.yml"), ()),
        (("stop", "-f", "/b.yml", "web"), ("--file", "/b.yml"), ("/a.yml", "/b.yml"), ("web",)),
        (("rm", "--file", "/b.yml", "web"), ("--file", "/b.yml"), ("/a.yml", "/b.yml"), ("web",)),
        (("rm", "-f", "web"), (), ("/a.yml",), ("web",)),  # rm's own --force, never a file
        (("cp", "--file=/b.yml", "web:/x", "/y"), ("--file=/b.yml",), ("/a.yml", "/b.yml"), ("web:/x", "/y")),
    ],
    ids=[
        "file-attached",
        "file-separate",
        "short-f",
        "profile-env",
        "project-dir",
        "logs-follow",
        "down",
        "stop-short-f",
        "rm-file",
        "rm-force",
        "cp",
    ],
)
def test_persistent_options_after_the_subcommand_are_hoisted(
    argv: tuple[str, ...], hoisted: tuple[str, ...], files: tuple[str, ...], services: tuple[str, ...]
) -> None:
    """Compose applies its persistent options after the subcommand too, so the render and the inputs must see them."""
    command = ("compose", "--file", "/a.yml", *argv)

    assert compose_trailing_globals(command) == (hoisted, True)
    facts = compose_share_facts(command)
    assert (facts.files, facts.services, facts.understood) == (files, services, True)


@pytest.mark.parametrize(
    "arguments",
    [
        ("run", "--file=/b.yml", "--rm", "--no-deps", "--entrypoint", "true", "web", "x"),
        ("exec", "--file=/b.yml", "web", "true"),
        ("exec", "-f", "/b.yml", "web", "true"),
        ("version", "-f", "json"),
    ],
    ids=["run", "exec", "exec-short", "version"],
)
def test_no_unhoisted_subcommand_accepts_a_persistent_option_after_it(arguments: tuple[str, ...]) -> None:
    """``run`` and ``exec`` parse closed option tables and ``version`` is not classified: the hoist's early return for
    them never lets a trailing ``--file`` execute unrendered."""
    with pytest.raises(InvalidInput):
        _compose(*arguments)


@pytest.mark.parametrize(
    ("arguments", "hoisted"),
    [
        (("exec", "-T", "postgres", "psql", "-f", "/run/secrets/x.sql"), ()),  # payload, never an option
        (("up", "--detach", "--file=/b.yml"), ("--file=/b.yml",)),
        (("run", "--rm", "--no-deps", "--entrypoint", "sh", "web", "-f", "/x.yml"), ()),  # run's payload
    ],
    ids=["exec-payload", "up-file", "run-payload"],
)
def test_the_injected_argv_hoists_like_the_callers(arguments: tuple[str, ...], hoisted: tuple[str, ...]) -> None:
    """The receipt render reads the toolkit's injected argv (``--project-name <project>`` first): its subcommand is the
    caller's, so a payload ``-f`` after ``exec`` is never hoisted (``psql -f``)."""
    caller = ("compose", "--file", "compose.yml", "--profile", "postgres", *arguments)
    injected = _compose("--profile", "postgres", *arguments).argv
    assert injected[:3] == ("compose", "--project-name", "dtt-example")

    assert compose_trailing_globals(caller) == (hoisted, True)
    assert compose_trailing_globals(injected) == (hoisted, True)


@pytest.mark.parametrize("tail", [("-df", "/b.yml"), ("-f",), ("-f=",)], ids=["cluster", "dangling", "empty"])
def test_an_unbindable_short_f_is_not_understood(tail: tuple[str, ...]) -> None:
    assert compose_trailing_globals(("compose", "--file", "/a.yml", "up", *tail)) == ((), False)
    assert compose_share_facts(("compose", "--file", "/a.yml", "up", *tail)).understood is False


@pytest.mark.parametrize(
    ("flags", "no_deps"),
    [
        (("--no-deps",), True),
        (("--no-deps", "--no-deps=false"), False),  # the last occurrence wins
        (("--no-deps=f",), False),  # Go's ParseBool spelling
        (("--no-deps=T",), True),
        (("--no-deps=maybe",), False),  # unparsable: count the dependencies
    ],
)
def test_no_deps_reads_like_compose(flags: tuple[str, ...], no_deps: bool) -> None:
    assert compose_share_facts(("compose", "--file", "/a.yml", "up", *flags, "web")).no_deps is no_deps


@pytest.mark.parametrize(
    ("mount", "bind"),
    [
        ("type=bind,source=/project,target=/x,readonly,readonly=false", ("/project", False)),
        ("type=bind,source=/safe,src=/project,target=/x", ("/project", False)),
        ("type=bind,src=/project,target=/x,ro=1", ("/project", True)),
    ],
    ids=["readonly-then-false", "source-then-src", "ro-one"],
)
def test_mount_fields_apply_in_order(mount: str, bind: tuple[str, bool]) -> None:
    facts = container_share_facts(("run", "--memory=256m", "--mount", mount, IMAGE_ID))

    assert facts.binds == (bind,)
