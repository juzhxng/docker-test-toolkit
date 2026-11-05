"""Classify Docker-fronted argv before any mutation receipt is written."""

from __future__ import annotations

import csv
import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

from docker_test_toolkit.application.image_archive import ARCHIVE_MAX_BYTES, archive_paths
from docker_test_toolkit.domain.errors import InvalidInput
from docker_test_toolkit.domain.models import ExternalDockerResource
from docker_test_toolkit.domain.policy import (
    BUILD_VALUE_LETTERS,
    BUILDER_DAEMON_CPUS,
    BUILDER_MEMORY_BYTES,
    NAME,
    short_options,
)
from docker_test_toolkit.domain.share_policy import memory_bytes

_CONTAINER_ID = re.compile(r"[0-9a-f]{64}\Z")
_IMAGE_ID = re.compile(r"sha256:[0-9a-f]{64}\Z")
_IMAGE_REFERENCE = re.compile(r"[^\s@]+@sha256:[0-9a-f]{64}\Z")
_UUID_TOKEN = re.compile(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}\Z")
BUILDKIT_CONFIG_TEXT = "[worker.oci]\n  max-parallelism = 1\n"
_COMPOSE_GLOBAL_VALUE_LETTERS = frozenset("fp")
_COMPOSE_NO_VALUE_LETTERS: frozenset[str] = frozenset()
_COMPOSE_EXEC_VALUE_LETTERS = frozenset("epuw")
_COMPOSE_EXEC_BOOLEAN_LETTERS = frozenset("dT")
_COMPOSE_EXEC_VALUE_OPTIONS = frozenset({"--env", "--index", "--project-name", "--user", "--workdir"})
_COMPOSE_EXEC_BOOLEAN_OPTIONS = frozenset({"--detach", "--no-tty", "--privileged"})
_CONTAINER_FLAGS = {"--init", "--interactive", "--read-only", "--rm"}
_CONTAINER_VALUE_OPTIONS = {
    "--cpus",
    "--dns",
    "--env",
    "--memory",
    "--memory-swap",
    "--pids-limit",
    "--platform",
    "--shm-size",
    "--stop-timeout",
    "--sysctl",
    "--tmpfs",
    "--ulimit",
    "--user",
}
_CONTAINER_EXACT_OPTIONS = {
    ("--cap-drop", "ALL"),
    ("--log-driver", "none"),
    ("--security-opt", "no-new-privileges=true"),
    ("--security-opt", "seccomp=builtin"),
}
_READ_ONLY = {
    ("version",),
    ("info",),
    ("inspect",),
    ("context", "show"),
    ("container", "inspect"),
    ("container", "logs"),
    ("container", "stats"),
    ("container", "wait"),
    ("image", "inspect"),
    ("network", "inspect"),
    ("volume", "inspect"),
    ("buildx", "inspect"),
    ("compose", "config"),
    ("compose", "logs"),
    ("compose", "ps"),
}


@dataclass(frozen=True)
class ClassifiedDockerCommand:
    """Carry sanitized receipt evidence and the exact adapter argv separately."""

    kind: str
    argv: tuple[str, ...]
    plan: dict[str, object]
    mutating: bool = True
    blocking: bool = False


_COMPOSE_WAIT_MAX_SECONDS = 1200


def _compose_wait_timeout(arguments: tuple[str, ...]) -> int | None:
    """Return the ``--wait-timeout`` seconds of ``compose up`` or ``start`` (1 to 1200), ``None`` without one.

    Anything else refuses.

    The dispatcher bounds the mutation by this wait plus its ordinary budget, so a slow service's healthcheck is not cut
    off at the fixed 30 s budget, and no caller can ask for an unbounded wait.
    """
    for position, token in enumerate(arguments):
        option, separator, attached = token.partition("=")
        if option != "--wait-timeout":
            continue
        value = attached if separator else (arguments[position + 1] if position + 1 < len(arguments) else "")
        if not value.isdigit() or not 1 <= int(value) <= _COMPOSE_WAIT_MAX_SECONDS:
            raise InvalidInput("Compose wait timeout must be whole seconds from 1 to 1200.")
        return int(value)
    return None


def _contains_option(argv: tuple[str, ...], name: str) -> bool:
    return any(value == name or value.startswith(f"{name}=") for value in argv)


def _has_short_option(argv: tuple[str, ...], letter: str, value_letters: frozenset[str]) -> bool:
    return any(found == letter for found, _ in short_options(argv, value_letters))


def _labels_argv(labels: Mapping[str, str]) -> tuple[str, ...]:
    result: list[str] = []
    for key in sorted(labels):
        result.extend(("--label", f"{key}={labels[key]}"))
    return tuple(result)


def _owned_container(owned: Mapping[str, object], identity: str) -> None:
    containers = owned.get("containers")
    if not _CONTAINER_ID.fullmatch(identity) or not isinstance(containers, dict) or identity not in containers:
        raise InvalidInput("Docker mutation requires one exact promoted owned container ID.")


def _owned_resource(owned: Mapping[str, object], plural: str, identity: str) -> None:
    items = owned.get(plural)
    if not isinstance(items, dict) or identity not in items:
        raise InvalidInput("Docker removal requires one exact promoted owned resource identity.")


def _validate_bind_mount(value: str) -> None:
    """Require exactly one bind type so duplicate keys cannot change authority."""
    try:
        parts = next(csv.reader([value], skipinitialspace=True, strict=True))
    except csv.Error as error:
        raise InvalidInput("Container mount syntax is invalid.") from error
    if [part for part in parts if part.startswith("type=")] != ["type=bind"]:
        raise InvalidInput("Container mounts may not create unplanned Docker volumes.")


# ``--project-name``/``-p`` take a value too: a caller may not pass them (refused), but the toolkit's own injected argv
# carries ``--project-name <project>``, and the subcommand behind it must be found in that form as well.
_COMPOSE_GLOBAL_VALUE_OPTIONS = frozenset(
    {
        "--ansi",
        "--env-file",
        "--file",
        "--parallel",
        "--profile",
        "--progress",
        "--project-directory",
        "--project-name",
        "-f",
        "-p",
    }
)


def _compose_subcommand(argv: tuple[str, ...]) -> tuple[str, int]:
    options_with_values = _COMPOSE_GLOBAL_VALUE_OPTIONS
    index = 1
    while index < len(argv):
        value = argv[index]
        if value in options_with_values:
            index += 2
        elif value.startswith("-"):
            index += 1
        else:
            return value, index
    raise InvalidInput("Compose command lacks a subcommand.")


def _compose_trailing_options(
    argv: tuple[str, ...], index: int, subcommand: str
) -> tuple[tuple[str, ...], frozenset[str]]:
    """Return the tokens after a Compose subcommand that Compose itself parses as options.

    ``run`` is parsed by the closed bounded-task parser. ``exec`` follows a closed option table
    up to the service name, because everything after it is the executed payload (``mkdir -p`` is
    not a project override) and an option the table does not know could hide where the service
    starts. Every other subcommand takes only options and positionals, so all tokens are scanned
    and no short letter is modelled as value-taking: the same letter is a boolean on one
    subcommand and a value option on another (``logs -t`` against ``down -t``), and reading a
    value as more letters can only over-refuse, never hide a project override.
    """
    rest = argv[index + 1 :]
    if subcommand == "run":
        return (), _COMPOSE_NO_VALUE_LETTERS
    if subcommand != "exec":
        return rest, _COMPOSE_NO_VALUE_LETTERS
    end = 0
    while end < len(rest) and rest[end].startswith("-"):
        token = rest[end]
        if token.startswith("--"):
            name, separator, _value = token.partition("=")
            if name in _COMPOSE_EXEC_VALUE_OPTIONS:
                end += 1 if separator else 2
            elif name in _COMPOSE_EXEC_BOOLEAN_OPTIONS:
                end += 1
            else:
                raise InvalidInput("Compose exec option is outside the bounded toolkit surface.")
            continue
        letters = short_options((token,), _COMPOSE_EXEC_VALUE_LETTERS)
        if not letters or any(
            letter not in _COMPOSE_EXEC_VALUE_LETTERS | _COMPOSE_EXEC_BOOLEAN_LETTERS for letter, _ in letters
        ):
            raise InvalidInput("Compose exec option is outside the bounded toolkit surface.")
        last_letter, attached = letters[-1]
        end += 2 if last_letter in _COMPOSE_EXEC_VALUE_LETTERS and attached is None else 1
    return rest[:end], _COMPOSE_EXEC_VALUE_LETTERS


def _bounded_compose_run_service(argv: tuple[str, ...], index: int) -> str:
    """Return the service from one foreground, auto-removed, dependency-free task."""
    arguments = argv[index + 1 :]
    flags = {"--rm": 0, "--no-deps": 0}
    entrypoint = 0
    cursor = 0
    while cursor < len(arguments) and arguments[cursor].startswith("-"):
        option = arguments[cursor]
        if option in flags:
            flags[option] += 1
            cursor += 1
            continue
        if option == "--entrypoint" and cursor + 1 < len(arguments):
            entrypoint += 1
            cursor += 2
            continue
        if option.startswith("--entrypoint=") and option.partition("=")[2]:
            entrypoint += 1
            cursor += 1
            continue
        raise InvalidInput("Compose run accepts only one bounded one-shot task.")
    service = arguments[cursor] if cursor < len(arguments) else ""
    if (
        flags != {"--rm": 1, "--no-deps": 1}
        or entrypoint != 1
        or not NAME.fullmatch(service)
        or cursor + 1 >= len(arguments)
    ):
        raise InvalidInput("Compose run accepts only one bounded one-shot task.")
    return service


def _build_tags(argv: tuple[str, ...]) -> tuple[str, ...]:
    """Return every tag any reading of the arguments could apply, for the collision precheck.

    Each token is read on its own and a bare ``-t``/``--tag`` takes the next argument, without
    modelling which option consumes a separate value: a tag-looking value of another option
    (``--file -towned:1``) is recorded too. Over-recording can only refuse or leave a build
    unresolved; under-recording would let a foreign tag skip the precheck.
    """
    tags = []
    for position, value in enumerate(argv[2:], start=2):
        if value.startswith("--tag="):
            tags.append(value.split("=", 1)[1])
            continue
        if value == "--tag":
            if position + 1 >= len(argv):
                raise InvalidInput("Build tag option lacks its value.")
            tags.append(argv[position + 1])
            continue
        for letter, attached in short_options((value,), BUILD_VALUE_LETTERS):
            if letter != "t":
                continue
            if attached is not None:
                tags.append(attached)
            elif position + 1 < len(argv):
                tags.append(argv[position + 1])
            else:
                raise InvalidInput("Build tag option lacks its value.")
    if not tags or any(not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9._/:@-]{0,255}", tag) for tag in tags):
        raise InvalidInput("Build requires at least one bounded exact image tag.")
    return tuple(dict.fromkeys(tags))


_BUILD_AUTHORITY_OPTIONS = frozenset(
    {
        "--attest",
        "--builder",
        "--cache-from",
        "--cache-to",
        "--iidfile",
        "--metadata-file",
        "--output",
        "--push",
        "--resource",
        "--sbom",
    }
)


def _refuse_buildx_authority(arguments: tuple[str, ...]) -> None:
    """Refuse the Buildx options that would give the caller authority the toolkit owns, in every spelling."""
    if (
        any(_contains_option(arguments, option) for option in _BUILD_AUTHORITY_OPTIONS)
        or _has_short_option(arguments, "o", BUILD_VALUE_LETTERS)
        or any(value.startswith("--load=") for value in arguments)
        or any(
            value == "--provenance" or (value.startswith("--provenance=") and value != "--provenance=false")
            for value in arguments
        )
    ):
        raise InvalidInput("Buildx authority and nonlocal outputs are owned by the toolkit.")


def requested_platform_values(rest: tuple[str, ...]) -> list[str]:
    """Return every ``--platform`` value of a build command, in order, for both spellings of the option."""
    values = []
    for position, value in enumerate(rest):
        if value == "--platform":
            if position + 1 >= len(rest):
                raise InvalidInput("Build platform option lacks its value.")
            values.append(rest[position + 1])
        elif value.startswith("--platform="):
            values.append(value.split("=", 1)[1])
    return values


def _container_image_and_network(
    argv: tuple[str, ...], owned: Mapping[str, object], external: tuple[ExternalDockerResource, ...]
) -> tuple[str, str | None]:
    index = 0
    network_id = None
    while index < len(argv):
        value = argv[index]
        if value == "--":
            index += 1
            break
        if value in _CONTAINER_FLAGS:
            index += 1
            continue
        if value == "--mount":
            if index + 1 >= len(argv):
                raise InvalidInput("Container mount option lacks its value.")
            _validate_bind_mount(argv[index + 1])
            index += 2
            continue
        if value.startswith("--mount="):
            _validate_bind_mount(value.split("=", 1)[1])
            index += 1
            continue
        if value == "--network" or value.startswith("--network="):
            if network_id is not None:
                raise InvalidInput("Container creation accepts one explicit network.")
            if value == "--network":
                if index + 1 >= len(argv):
                    raise InvalidInput("Container network option lacks its value.")
                network_id = argv[index + 1]
                index += 2
            else:
                network_id = value.split("=", 1)[1]
                index += 1
            continue
        if value.startswith("--") and "=" in value:
            option, argument = value.split("=", 1)
            if not argument or (
                option not in _CONTAINER_VALUE_OPTIONS and (option, argument) not in _CONTAINER_EXACT_OPTIONS
            ):
                raise InvalidInput("Container creation option is outside the bounded toolkit surface.")
            index += 1
            continue
        if value.startswith("-"):
            raise InvalidInput("Container creation option is outside the bounded toolkit surface.")
        break
    image_id = argv[index] if index < len(argv) else ""
    if network_id == "none":
        network_id = None
    elif network_id is not None:
        owned_networks = owned.get("networks")
        external_network = any(
            item.kind == "network" and item.identifier == network_id and item.relationship == "connect_owned_container"
            for item in external
        )
        if not external_network and (not isinstance(owned_networks, dict) or network_id not in owned_networks):
            raise InvalidInput("Container creation requires an exact owned or declared external network.")
    return image_id, network_id


def classify_command(
    argv: tuple[str, ...],
    *,
    operation_id: str,
    run_id: str,
    compose_project: str,
    builder: str,
    labels: Mapping[str, str],
    private_root: str,
    owned: Mapping[str, object],
    external: tuple[ExternalDockerResource, ...],
) -> ClassifiedDockerCommand:
    """Return a closed mutation class or refuse before executing Docker."""
    if (
        not argv
        or any(not isinstance(value, str) or not value or "\0" in value for value in argv)
        or not _UUID_TOKEN.fullmatch(operation_id)
    ):
        raise InvalidInput("Docker command identity or argv is invalid.")
    head = argv[:2]
    if argv[0] in {"system", "builder"} or (len(argv) > 1 and argv[1] == "prune"):
        raise InvalidInput("Global Docker mutation is forbidden.")
    if argv[0] == "compose":
        subcommand, index = _compose_subcommand(argv)
        if _contains_option(argv[:index], "--project-name") or _has_short_option(
            argv[:index], "p", _COMPOSE_GLOBAL_VALUE_LETTERS
        ):
            raise InvalidInput("Compose project identity is owned by the toolkit.")
        if subcommand in {"create", "down", "restart", "rm", "start", "stop", "up"} and (
            _contains_option(argv[index + 1 :], "--rmi") or _contains_option(argv[index + 1 :], "--remove-orphans")
        ):
            raise InvalidInput("Compose may mutate only the exact planned resource set.")
        trailing, trailing_value_letters = _compose_trailing_options(argv, index, subcommand)
        if _contains_option(trailing, "--project-name") or _has_short_option(trailing, "p", trailing_value_letters):
            raise InvalidInput("Compose project identity is owned by the toolkit.")
        compose_argv = argv
        image_acquisition = argv if subcommand in {"create", "up"} else argv[:index]
        if subcommand in {"create", "run", "up"} and (
            _contains_option(image_acquisition, "--pull") or _contains_option(image_acquisition, "--build")
        ):
            raise InvalidInput("Compose image acquisition is owned by the toolkit.")
        if subcommand in {"create", "up"}:
            compose_argv = (
                *argv[: index + 1],
                "--pull",
                "never",
                "--no-build",
                *argv[index + 1 :],
            )
        elif subcommand == "run":
            service = _bounded_compose_run_service(argv, index)
            name = f"dtt-{run_id.replace('-', '')[:12]}-{operation_id.replace('-', '')[:12]}"
            compose_argv = (
                *argv[: index + 1],
                "--pull",
                "never",
                "--name",
                name,
                *argv[index + 1 :],
            )
        injected = ("compose", "--project-name", compose_project, *compose_argv[1:])
        plan = {"project": compose_project, "subcommand": subcommand, "subcommand_index": index + 2}
        if subcommand in {"up", "start"}:
            wait_seconds = _compose_wait_timeout(argv[index + 1 :])
            if wait_seconds is not None:
                plan["wait_seconds"] = wait_seconds
        if subcommand == "run":
            plan.update(service=service, name=name, remove=True)
            return ClassifiedDockerCommand("compose-run", injected, plan, blocking=True)
        if subcommand in {"config", "logs", "ps"}:
            return ClassifiedDockerCommand("observe", injected, plan, False)
        if subcommand in {"cp", "exec"}:
            # exec runs a caller process (a whole test file) and may take as long as ``compose run``
            return ClassifiedDockerCommand("compose-io", injected, plan, False, blocking=subcommand == "exec")
        if subcommand not in {"cp", "create", "down", "exec", "restart", "rm", "start", "stop", "up"}:
            raise InvalidInput("Compose mutation is outside the classified toolkit surface.")
        return ClassifiedDockerCommand("compose", injected, plan)
    if any(argv[: len(prefix)] == prefix for prefix in _READ_ONLY):
        return ClassifiedDockerCommand("observe", argv, {}, False)
    if argv[:1] == ("pull",) or head == ("image", "pull"):
        reference_index = 1 if argv[0] == "pull" else 2
        if len(argv) != reference_index + 1 or not _IMAGE_REFERENCE.fullmatch(argv[reference_index]):
            raise InvalidInput("Image acquisition requires one digest-pinned reference.")
        reference = argv[reference_index]
        return ClassifiedDockerCommand(
            "image-pull",
            ("image", "pull", reference),
            {"reference": reference},
        )
    if argv[0] in {"run", "create"} or head in {("container", "run"), ("container", "create")}:
        forbidden = {"--cidfile", "--label", "--label-file", "--name", "--pull", "--volume", "--volumes-from", "-v"}
        if any(_contains_option(argv, option) for option in forbidden):
            raise InvalidInput("Container identity and resource authority are owned by the toolkit.")
        cidfile = str(Path(private_root) / f"cid-{operation_id}")
        name = f"dtt-{run_id.replace('-', '')[:12]}-{operation_id.replace('-', '')[:12]}"
        offset = 1 if argv[0] in {"run", "create"} else 2
        subcommand = argv[offset - 1]
        prefix = argv[:offset]
        suffix = argv[offset:]
        image_id, network_id = _container_image_and_network(suffix, owned, external)
        owned_images = owned.get("images")
        permitted_images = {
            item.identifier for item in external if item.kind == "image" and item.relationship == "use_image"
        }
        if not _IMAGE_ID.fullmatch(image_id) or (
            image_id not in permitted_images and (not isinstance(owned_images, dict) or image_id not in owned_images)
        ):
            raise InvalidInput("Container creation requires one exact owned or declared-external image ID.")
        injected = (
            "--pull=never",
            *_labels_argv(labels),
            "--cidfile",
            cidfile,
            "--name",
            name,
        )
        return ClassifiedDockerCommand(
            "container-create",
            (*prefix, *injected, *suffix),
            {
                "cidfile": cidfile,
                "image_id": image_id,
                "network_id": network_id,
                "name": name,
                "remove": "--rm" in argv,
            },
            blocking=subcommand == "run",
        )
    if head == ("container", "exec"):
        if len(argv) < 4:
            raise InvalidInput("Container exec requires one exact owned target and command.")
        container_id = argv[2]
        _owned_container(owned, container_id)
        return ClassifiedDockerCommand("container-io", argv, {"container_id": container_id}, False, blocking=True)
    if head in {("container", "start"), ("container", "stop"), ("container", "rm")} or argv[0] in {
        "start",
        "stop",
        "rm",
    }:
        action = argv[1] if argv[0] == "container" else argv[0]
        offset = 2 if argv[0] == "container" else 1
        arguments = argv[offset:]
        allowed = {"-f", "--force", "-v", "--volumes"} if action == "rm" else set()
        targets = [value for value in arguments if value not in allowed]
        if len(targets) != 1 or any(value.startswith("-") for value in targets):
            raise InvalidInput("Container control accepts exactly one owned target.")
        target = targets[0]
        _owned_container(owned, target)
        return ClassifiedDockerCommand(f"container-{action}", argv, {"container_id": target})
    if head == ("buildx", "create"):
        if len(argv) != 2:
            raise InvalidInput("Buildx builder configuration is owned by the toolkit.")
        config_path = str(Path(private_root) / "buildkitd.toml")
        return ClassifiedDockerCommand(
            "builder-create",
            (
                "buildx",
                "create",
                "--name",
                builder,
                "--driver",
                "docker-container",
                "--bootstrap",
                "--driver-opt",
                f"memory={BUILDER_MEMORY_BYTES // 1024**2}m",
                "--driver-opt",
                f"memory-swap={BUILDER_MEMORY_BYTES // 1024**2}m",
                "--driver-opt",
                "cpu-period=100000",
                "--driver-opt",
                "cpu-quota=200000",
                "--driver-opt",
                "restart-policy=no",
                "--buildkitd-config",
                config_path,
            ),
            {
                "name": builder,
                "driver": "docker-container",
                "memory_bytes": BUILDER_MEMORY_BYTES,
                "daemon_cpus": BUILDER_DAEMON_CPUS,
                "max_parallelism": 1,
                "config_path": config_path,
                "config_sha256": hashlib.sha256(BUILDKIT_CONFIG_TEXT.encode()).hexdigest(),
            },
        )
    if head == ("buildx", "build"):
        _refuse_buildx_authority(argv[2:])
        metadata = str(Path(private_root) / f"build-{operation_id}.json")
        tags = _build_tags(argv)
        load = () if "--load" in argv else ("--load",)
        return ClassifiedDockerCommand(
            "build",
            (
                "buildx",
                "build",
                "--builder",
                builder,
                "--metadata-file",
                metadata,
                *load,
                "--resource",
                f"memory={BUILDER_MEMORY_BYTES // 1024**2}m",
                "--resource",
                f"memory-swap={BUILDER_MEMORY_BYTES // 1024**2}m",
                "--resource",
                "cpu-period=100000",
                "--resource",
                "cpu-quota=200000",
                *_labels_argv(labels),
                *argv[2:],
            ),
            {
                "builder": builder,
                "metadata": metadata,
                "tags": list(tags),
                "memory_bytes": BUILDER_MEMORY_BYTES,
                "daemon_cpus": BUILDER_DAEMON_CPUS,
            },
        )
    if head == ("image", "save"):
        if len(argv) != 3 or not _IMAGE_ID.fullmatch(argv[2]):
            raise InvalidInput("Image export requires exactly one full sha256 image ID.")
        image_id = argv[2]
        images = owned.get("images")
        resource = images.get(image_id) if isinstance(images, dict) else None
        if not isinstance(resource, dict) or resource.get("state") != "present":
            raise InvalidInput("Image export requires one exact owned image that is present.")
        paths = archive_paths(private_root, operation_id)
        return ClassifiedDockerCommand(
            "image-save",
            ("image", "save", "-o", paths.output, image_id),
            {
                "schema": 1,
                "image_id": image_id,
                "max_bytes": ARCHIVE_MAX_BYTES,
                "directory": paths.directory,
                "output": paths.output,
                "final": paths.final,
                "archive": None,
            },
        )
    if head == ("network", "connect"):
        if len(argv) != 4:
            raise InvalidInput("Network connect requires exact network and container identities.")
        network_id, container_id = argv[2:]
        _owned_container(owned, container_id)
        relationship = next(
            (
                item
                for item in external
                if item.kind == "network"
                and item.identifier == network_id
                and item.relationship == "connect_owned_container"
            ),
            None,
        )
        if relationship is None:
            raise InvalidInput("Network connect requires one declared external relationship.")
        return ClassifiedDockerCommand(
            "network-connect",
            argv,
            {"network_id": network_id, "container_id": container_id},
        )
    if head in {("network", "create"), ("volume", "create")}:
        if len(argv) != 2:
            raise InvalidInput("Direct resource names are generated by the toolkit.")
        kind = argv[0]
        name = f"dtt-{run_id.replace('-', '')[:12]}-{operation_id.replace('-', '')[:12]}"
        return ClassifiedDockerCommand(
            f"{kind}-create",
            (kind, "create", *_labels_argv(labels), name),
            {"name": name},
        )
    if head in {("network", "rm"), ("volume", "rm"), ("image", "rm"), ("buildx", "rm")}:
        if len(argv) != 3:
            raise InvalidInput("Docker removal accepts exactly one exact owned target.")
        kind = "builder" if argv[0] == "buildx" else argv[0]
        plural = "builders" if kind == "builder" else f"{kind}s"
        _owned_resource(owned, plural, argv[2])
        return ClassifiedDockerCommand(f"{kind}-rm", argv, {f"{kind}_id": argv[2]})
    raise InvalidInput("Docker command is outside the classified toolkit surface.")


# ---- transient share facts (design 2.3, plan P5): read for the share rules, never written into a receipt plan ----

_COMPOSE_UP_VALUE_OPTIONS = frozenset(
    {"--scale", "--wait-timeout", "--timeout", "-t", "--exit-code-from", "--attach", "--no-attach", "--pull"}
)


@dataclass(frozen=True)
class ComposeShareFacts:
    """What the share rules read from one Compose command line besides its rendered document."""

    services: tuple[str, ...]
    no_deps: bool
    scale: dict[str, int]
    watch: bool
    files: tuple[str, ...]
    env_files: tuple[str, ...]
    stdin_file: bool
    project_directory: str | None = None
    privileged: bool = False  # ``exec --privileged``, read only from exec's closed option table
    understood: bool = True  # every persistent option after the subcommand could be bound (R15 otherwise)


@dataclass(frozen=True)
class ContainerShareFacts:
    """What the share rules read from one direct ``run``/``create`` command line."""

    memory_bytes: int | None
    binds: tuple[tuple[str, bool], ...]  # (source, read-only)


def _option_values(argv: Sequence[str], names: frozenset[str], short: str | None = None) -> list[str]:
    values: list[str] = []
    for index, value in enumerate(argv):
        name, separator, attached = value.partition("=")
        if name in names:
            if separator:
                values.append(attached)
            elif index + 1 < len(argv):
                values.append(argv[index + 1])
        elif short is not None and value.startswith(short) and value != short and not value.startswith("--"):
            values.append(value[len(short) :].lstrip("="))
        elif short is not None and value == short and index + 1 < len(argv):
            values.append(argv[index + 1])
    return values


_GO_TRUE = frozenset({"1", "t", "T", "TRUE", "true", "True"})
_GO_FALSE = frozenset({"0", "f", "F", "FALSE", "false", "False"})


def _flag_value(tokens: tuple[str, ...], name: str, letter: str | None = None) -> bool | None:
    """Return a boolean Compose flag's effective value as Compose's flag parser reads it, or None when unparsable.

    The last occurrence wins (``--no-deps --no-deps=false`` is false); a value uses Go's ``strconv.ParseBool``
    spellings; ``letter`` also finds the flag inside a short-option cluster such as ``-dw``. Absent means false.
    """
    value: bool | None = False
    for token in tokens:
        option, separator, given = token.partition("=")
        if option == name:
            value = True if not separator else True if given in _GO_TRUE else False if given in _GO_FALSE else None
        elif letter is not None and token.startswith("-") and not token.startswith("--"):
            if any(found == letter for found, _ in short_options((token,), _COMPOSE_NO_VALUE_LETTERS)):
                value = True
    return value


# Compose's persistent (root) options, which it also accepts after the subcommand; the render must see them too.
_COMPOSE_PERSISTENT_VALUE_OPTIONS = frozenset(
    {"--file", "--env-file", "--profile", "--project-directory", "--ansi", "--parallel", "--progress"}
)
_COMPOSE_PERSISTENT_FLAGS = frozenset({"--compatibility", "--dry-run"})
# ``run`` and ``exec`` parse a closed option table that refuses these options; ``-f`` is local on ``rm`` (``--force``)
# and ``logs`` (``--follow``), so only the long spellings are persistent there.
_CLOSED_COMPOSE_SUBCOMMANDS = frozenset({"run", "exec"})
_LOCAL_SHORT_F_SUBCOMMANDS = frozenset({"rm", "logs"})


def _hoists_short_f(subcommand: str) -> bool:
    return subcommand not in _CLOSED_COMPOSE_SUBCOMMANDS | _LOCAL_SHORT_F_SUBCOMMANDS


def compose_trailing_globals(argv: tuple[str, ...]) -> tuple[tuple[str, ...], bool]:
    """Return the persistent options after a Compose subcommand, in order and ready to precede ``config``, and
    whether every one was understood.

    No subcommand has a local option of these long names, so Compose applies them to the project after every subcommand
    the classifier accepts except ``run`` and ``exec``, whose closed option tables refuse them:
    ``up --file=override.yml`` and ``down --file=other.yml`` read that file. A short cluster that hides ``f``
    (``-df x``) or a dangling ``-f`` is not understood: its value cannot be bound, so a shared run refuses it (R15).
    """
    subcommand, index = _compose_subcommand(argv)
    if subcommand in _CLOSED_COMPOSE_SUBCOMMANDS:
        return (), True
    short_f = _hoists_short_f(subcommand)
    hoisted: list[str] = []
    understood = True
    trailing = argv[index + 1 :]
    position = 0
    while position < len(trailing):
        argument = trailing[position]
        name = argument.partition("=")[0]
        if name in _COMPOSE_PERSISTENT_VALUE_OPTIONS:
            if "=" in argument:
                hoisted.append(argument)
            elif position + 1 < len(trailing):
                hoisted.extend((argument, trailing[position + 1]))
                position += 1
            else:
                understood = False
        elif name in _COMPOSE_PERSISTENT_FLAGS:
            hoisted.append(argument)
        elif not short_f or not argument.startswith("-") or argument.startswith("--"):
            pass
        elif argument == "-f" and position + 1 < len(trailing):
            hoisted.extend(("--file", trailing[position + 1]))
            position += 1
        elif argument.startswith("-f") and argument.removeprefix("-f").removeprefix("="):
            hoisted.append(f"--file={argument[2:].removeprefix('=')}")
        elif "f" in argument[1:]:
            understood = False
        position += 1
    return tuple(hoisted), understood


def compose_share_facts(argv: tuple[str, ...]) -> ComposeShareFacts:
    """Read the share-relevant facts of one Compose command (services, ``--scale``, ``--watch``, inputs).

    Inputs are read from the options before the subcommand and from the persistent ones Compose also accepts after it
    (``compose_trailing_globals``), in Compose's order.
    """
    subcommand, index = _compose_subcommand(argv)
    hoisted, understood = compose_trailing_globals(argv)
    globals_, trailing = (*argv[1:index], *hoisted), argv[index + 1 :]
    files = tuple(_option_values(globals_, frozenset({"--file"}), "-f"))
    services: list[str] = []
    skip = False
    for value in trailing:
        if skip:
            skip = False
            continue
        name = value.partition("=")[0]
        if value in _COMPOSE_UP_VALUE_OPTIONS or (name in _COMPOSE_PERSISTENT_VALUE_OPTIONS and "=" not in value):
            skip = True
            continue
        if _hoists_short_f(subcommand) and value == "-f":
            skip = True
            continue
        if subcommand == "run" and value == "--entrypoint":
            skip = True
            continue
        if not value.startswith("-"):
            services.append(value)
            if subcommand == "run":
                break  # the rest is the one-off's command
    scale: dict[str, int] = {}
    for item in _option_values(trailing, frozenset({"--scale"})):
        service, _, count = item.partition("=")
        scale[service] = int(count) if count.isdecimal() else 0
    no_deps = _flag_value(trailing, "--no-deps")
    watch = _flag_value(trailing, "--watch", "w")
    privileged = _flag_value(_compose_trailing_options(argv, index, subcommand)[0], "--privileged")
    directories = _option_values(globals_, frozenset({"--project-directory"}))
    return ComposeShareFacts(
        services=tuple(services),
        no_deps=no_deps is True,  # unparsable: count the dependencies (R12 over-counts, never under-counts)
        scale=scale,
        watch=subcommand == "up" and watch is not False,  # unparsable: treat as watching (refused)
        files=files,
        env_files=tuple(_option_values(globals_, frozenset({"--env-file"}))),
        stdin_file="-" in files,
        project_directory=directories[-1] if directories else None,
        privileged=subcommand == "exec" and privileged is not False,  # unparsable: treat as privileged (refused)
        understood=understood,
    )


def _container_option_prefix(argv: tuple[str, ...]) -> tuple[str, ...]:
    """Return the Docker options of a direct ``run``/``create``: everything before the image (as the classifier reads
    it, ``_container_image_and_network``); the payload after the image is the container's own command."""
    offset = 2 if argv[:1] == ("container",) else 1
    options = argv[offset:]
    index = 0
    while index < len(options):
        value = options[index]
        if value == "--" or not value.startswith("-"):
            break
        index += 2 if value in ("--mount", "--network") else 1
    return options[:index]


def container_share_facts(argv: tuple[str, ...]) -> ContainerShareFacts:
    """Read the share-relevant facts of one direct ``run``/``create``: ``--memory`` and bind mounts."""
    argv = _container_option_prefix(argv)
    memory = _option_values(argv, frozenset({"--memory"}))
    binds: list[tuple[str, bool]] = []
    for value in _option_values(argv, frozenset({"--mount"})):
        source, read_only = "", False
        for field in next(csv.reader([value], skipinitialspace=True)):  # Docker applies fields in order: last wins
            key, separator, item = field.partition("=")
            if key in ("source", "src"):
                source = item
            elif key in ("readonly", "ro"):
                # an unparsable value counts as read-only only when it is a true spelling; anything else is writable
                read_only = not separator or item in _GO_TRUE
        binds.append((source, read_only))
    return ContainerShareFacts(memory_bytes=memory_bytes(memory[-1]) if memory else None, binds=tuple(binds))
