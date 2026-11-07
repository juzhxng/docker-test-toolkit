"""Share rules: which rendered runs may hold the Docker daemon beside other runs.

Pure predicates over a ``docker compose config --format json`` document, or over the facts of one direct command. A
verdict names the first violated rule and its JSON path; the caller decides what a violation means (a negative verdict
for an exclusive run, a refusal for a shared one). Trimmed from the predicates of the earlier two-holder mode.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import PurePosixPath
from typing import Any

SHARE_RULES_VERSION = 1  # bumped whenever a rule changes; a verdict store with another value predicts nothing
DAEMON_SOCKETS = ("/var/run/docker.sock", "/run/docker.sock")
_CHECKED_KINDS = frozenset({"container-create", "container-start", "compose-run"})
_CHECKED_COMPOSE = frozenset({"create", "up", "start", "restart"})
_SHARED_NAMESPACES = ("network_mode", "pid", "ipc", "uts", "userns_mode", "cgroup")
_MEMORY = re.compile(r"(\d+)([bkmg]?)b?\Z", re.IGNORECASE)
_SHIFT = {"": 0, "b": 0, "k": 10, "m": 20, "g": 30}


@dataclass(frozen=True)
class ShareVerdict:
    """The first violated share rule and its path, or none."""

    rule: str | None = None
    reason: str | None = None

    @property
    def shareable(self) -> bool:
        """True when no rule is violated."""
        return self.rule is None


SHAREABLE = ShareVerdict()


@dataclass(frozen=True)
class BindFact:
    """One bind source after ``realpath``: whether the mount is read-only and whether the source is a socket."""

    source: str
    read_only: bool
    is_socket: bool


@dataclass(frozen=True)
class ShareScope:
    """What a run may reach: its private root, its checkout, the toolkit private-root trees and the daemon sockets."""

    compose_project: str
    private_root: str
    checkout: str
    private_root_trees: tuple[str, ...]
    socket_paths: tuple[str, ...]


def checked_mutation(kind: str, subcommand: object = None) -> bool:
    """Return whether a dispatched receipt is a creating or starting mutation, the rows that evaluate share rules."""
    return kind in _CHECKED_KINDS or (kind == "compose" and subcommand in _CHECKED_COMPOSE)


def memory_bytes(value: object) -> int | None:
    """Return a Compose or CLI memory value in bytes (``1g``, ``512m``, an integer), or None when unreadable."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and (match := _MEMORY.fullmatch(value.strip())):
        return int(match.group(1)) << _SHIFT[match.group(2).lower()]
    return None


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _list(value: object) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _within(path: str, root: str) -> bool:
    return PurePosixPath(path).is_relative_to(root)


def bind_verdict(fact: BindFact, scope: ShareScope, where: str) -> ShareVerdict:
    """R10, an allowlist: inside the run's own private root, or read-only inside its checkout; never near a socket
    or another run's private-root tree."""
    source = fact.source
    own = _within(source, scope.private_root)
    allowed = own or (_within(source, scope.checkout) and fact.read_only)
    near_socket = fact.is_socket or any(_within(socket, source) for socket in scope.socket_paths)
    # A source may not expose a private-root tree (be the tree or one of its ancestors), nor lie inside one unless it
    # lies inside the run's own private root.
    foreign = any(_within(tree, source) or (_within(source, tree) and not own) for tree in scope.private_root_trees)
    if near_socket:
        return ShareVerdict("R10", f"bind source {source} is or contains a Docker daemon socket at {where}")
    if foreign:
        return ShareVerdict("R10", f"bind source {source} exposes another run's private root at {where}")
    if not allowed:
        return ShareVerdict(
            "R10", f"bind source {source} outside the run's own private root and read-only checkout at {where}"
        )
    return SHAREABLE


def effective_memory(service: Mapping[str, Any]) -> int | None:
    """Return a rendered service's hard memory limit (``mem_limit`` or ``deploy.resources.limits.memory``) or None."""
    nested = _mapping(_mapping(_mapping(service.get("deploy")).get("resources")).get("limits")).get("memory")
    for value in (service.get("mem_limit"), nested):
        found = memory_bytes(value) if value is not None else None
        if found is not None and found > 0:
            return found
    return None


def _top_level(document: Mapping[str, Any], scope: ShareScope) -> ShareVerdict:
    for section in ("networks", "volumes"):
        for key, raw in _mapping(document.get(section)).items():
            spec, path = _mapping(raw), f"{section}.{key}"
            expected = f"{scope.compose_project}_{key}"  # the name Compose gives a project-scoped object
            if spec.get("external") or spec.get("name", expected) != expected:
                return ShareVerdict("R3", f"explicit or external name at {path}")
            options = _mapping(spec.get("driver_opts"))
            if section == "volumes" and ("device" in options or "bind" in str(options.get("o", "")).split(",")):
                return ShareVerdict("R3", f"local volume bound to a host path at {path}.driver_opts")
            if section == "networks" and spec.get("ipam", {}) not in ({}, None):
                return ShareVerdict("R4", f"ipam configuration at {path}.ipam")
    return SHAREABLE


def _service(name: str, service: Mapping[str, Any], scope: ShareScope, binds: Mapping[str, BindFact]) -> ShareVerdict:
    path = f"services.{name}"
    for index, port in enumerate(_list(service.get("ports"))):
        if _mapping(port).get("published") not in (None, "", "0", 0):
            return ShareVerdict("R1", f"fixed host port in service {name} at {path}.ports[{index}]")
    if service.get("container_name"):
        return ShareVerdict("R2", f"container_name in service {name}")
    if service.get("external_links"):
        return ShareVerdict("R5", f"external_links in service {name}")
    for key in _SHARED_NAMESPACES:
        value = service.get(key)
        # host, another container's or service's namespace, and Docker's daemon-wide default bridge (which every run
        # could join) are shared; ``none`` and the project's own networks are not
        shared_bridge = key == "network_mode" and value in ("bridge", "default")
        if isinstance(value, str) and (
            value == "host" or value.startswith(("container:", "service:")) or shared_bridge
        ):
            return ShareVerdict("R6", f"shared {key} in service {name}")
    if service.get("privileged") is True:
        return ShareVerdict("R7", f"privileged service {name}")
    if service.get("devices") or service.get("cap_add"):
        return ShareVerdict("R8", f"devices or added capabilities in service {name}")
    if effective_memory(service) is None:
        return ShareVerdict("R9", f"service {name} has no hard memory limit")
    for index, mount in enumerate(_list(service.get("volumes"))):
        mount = _mapping(mount)
        if mount.get("type") == "bind":
            fact = binds.get(str(mount.get("source")))
            if fact is None:
                return ShareVerdict("R10", f"unresolved bind source at {path}.volumes[{index}]")
            own_mode = replace(fact, read_only=mount.get("read_only") is True)  # read-only belongs to each mount
            verdict = bind_verdict(own_mode, scope, f"{path}.volumes[{index}]")
            if not verdict.shareable:
                return verdict
    for entry in _list(service.get("volumes_from")):
        if isinstance(entry, str) and entry.startswith("container:"):
            return ShareVerdict("R13", f"volumes_from a container in service {name}")
    replicas = _mapping(service.get("deploy")).get("replicas")
    if replicas not in (None, 1):
        return ShareVerdict("R14", f"replicas in service {name}")
    return SHAREABLE


def compose_verdict(
    document: Mapping[str, Any],
    scope: ShareScope,
    binds: Mapping[str, BindFact],
    *,
    scale: Mapping[str, int],
    watch: bool,
    services: Sequence[str] = (),
    bound_input: bool = True,
) -> ShareVerdict:
    """Return the first violated rule of R1-R10, R13-R15 for one rendered Compose document and its command facts.

    Every rendered service is judged: the render already reflects the effective profiles (``--profile``, ``"*"``,
    ``COMPOSE_PROFILES``). A service the command names that the render lacks (Compose activates a named service's
    profile, the render does not) cannot be evaluated: R15. ``binds`` maps each bind source to its resolved facts; the
    read-only mode is each mount's own. R12 needs the run's running containers and is decided by ``own_budget_verdict``.
    """
    if not bound_input:
        return ShareVerdict("R15", "Compose input that cannot be bound (stdin or no explicit --file)")
    rendered = _mapping(document.get("services"))
    missing = sorted(set(services) - set(rendered))
    if missing:
        return ShareVerdict("R15", f"service {missing[0]} is absent from the render and cannot be evaluated")
    if watch:
        return ShareVerdict("R14", "compose up --watch")
    for service_name, count in sorted(scale.items()):
        if count != 1:
            return ShareVerdict("R14", f"--scale {service_name}={count}")
    verdict = _top_level(document, scope)
    if not verdict.shareable:
        return verdict
    for name, raw in sorted(rendered.items()):
        verdict = _service(name, _mapping(raw), scope, binds)
        if not verdict.shareable:
            return verdict
    return SHAREABLE


def started_service_limits(
    document: Mapping[str, Any],
    *,
    subcommand: str,
    services: Sequence[str],
    no_deps: bool,
) -> dict[str, int]:
    """Return the rendered memory limit of every service the Compose command creates or starts.

    Named services with their ``depends_on`` closure (the named ones alone under ``--no-deps`` and for ``run``);
    every rendered service when none is named.
    """
    rendered = {name: _mapping(raw) for name, raw in _mapping(document.get("services")).items()}
    if subcommand == "run" or services:
        selected: set[str] = set()
        pending = [name for name in services if name in rendered]
        while pending:
            name = pending.pop()
            if name in selected:
                continue
            selected.add(name)
            if not no_deps and subcommand != "run":
                pending.extend(key for key in _mapping(rendered[name].get("depends_on")) if key in rendered)
                pending.extend(key for key in _list(rendered[name].get("depends_on")) if key in rendered)
    else:
        selected = set(rendered)
    return {name: effective_memory(rendered[name]) or 0 for name in sorted(selected)}


def own_budget_verdict(*, running_bytes: int, added_bytes: int, phase_container_bytes: int) -> ShareVerdict:
    """R12: the run's own running memory limits after the mutation must stay within the current phase's budget."""
    if running_bytes + added_bytes > phase_container_bytes:
        return ShareVerdict(
            "R12",
            f"own running memory limits {running_bytes + added_bytes} exceed the phase budget {phase_container_bytes}",
        )
    return SHAREABLE


def container_verdict(
    *, memory_bytes: int | None, binds: Sequence[BindFact], external_network: str | None, scope: ShareScope
) -> ShareVerdict:
    """Direct ``run``/``create``: R5 (a declared foreign network), R9 (``--memory``) and R10 (bind mounts)."""
    if external_network is not None:
        return ShareVerdict("R5", f"connection to the declared network {external_network}")
    if memory_bytes is None or memory_bytes <= 0:
        return ShareVerdict("R9", "direct container without --memory")
    for index, fact in enumerate(binds):
        verdict = bind_verdict(fact, scope, f"--mount[{index}]")
        if not verdict.shareable:
            return verdict
    return SHAREABLE


def exec_verdict(*, privileged: bool) -> ShareVerdict:
    """R7 for ``compose exec``: a privileged exec reaches beyond the run's containers."""
    return ShareVerdict("R7", "compose exec --privileged") if privileged else SHAREABLE


def builder_verdict(*, removed: bool) -> ShareVerdict:
    """R16: a builder after the run left its first phase; no builder reserve covers it."""
    return ShareVerdict("R16", "a builder after the run left its first phase") if removed else SHAREABLE


def network_connect_verdict() -> ShareVerdict:
    """R5: connecting an owned container to a declared foreign network."""
    return ShareVerdict("R5", "network connect to a declared network")
