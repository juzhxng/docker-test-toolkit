from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from docker_test_toolkit.domain.share_policy import (
    BindFact,
    ShareScope,
    compose_verdict,
    container_verdict,
    exec_verdict,
    network_connect_verdict,
    own_budget_verdict,
    started_service_limits,
)

GIB = 1 << 30
FIXTURES = Path(__file__).parents[1] / "fixtures" / "share_rules"
TEMP = "/w/tmp"  # the temporary parent T that holds every run's private root
SCOPE = ShareScope(
    compose_project="dtt-a",
    private_root=f"{TEMP}/dtt-runs/run-a",
    checkout="/w/repo",
    private_root_trees=(f"{TEMP}/dtt-runs",),
    socket_paths=("/var/run/docker.sock",),
)


def _service(**fields: Any) -> dict[str, Any]:
    return {"image": "x", "mem_limit": "1073741824", **fields}


def _document(**services: dict[str, Any]) -> dict[str, Any]:
    return {"name": "dtt-a", "services": services or {"web": _service()}}


def _verdict(document: dict[str, Any], binds: dict[str, BindFact] | None = None, **facts: Any):
    options = {"scale": {}, "watch": False, **facts}
    return compose_verdict(document, SCOPE, binds or {}, **options)


RULE_CELLS = {
    "R1": (_document(web=_service(ports=[{"target": 80, "published": "8080", "protocol": "tcp"}])), {}),
    "R2": (_document(web=_service(container_name="fixed")), {}),
    "R3": (_document() | {"volumes": {"data": {"name": "dtt-a_data", "driver_opts": {"device": "/x"}}}}, {}),
    "R4": (_document() | {"networks": {"default": {"name": "dtt-a_default", "ipam": {"config": [{}]}}}}, {}),
    "R5": (_document(web=_service(external_links=["other:db"])), {}),
    "R6": (_document(web=_service(network_mode="host")), {}),
    "R7": (_document(web=_service(privileged=True)), {}),
    "R8": (_document(web=_service(cap_add=["NET_ADMIN"])), {}),
    "R9": (_document(web={"image": "x"}), {}),
    "R10": (_document(web=_service(volumes=[{"type": "bind", "source": "/w/repo", "target": "/r"}])), {}),
    "R13": (_document(web=_service(volumes_from=["container:x"])), {}),
    "R14": (_document(web=_service(deploy={"replicas": 2})), {}),
    "R15": (_document(), {"bound_input": False}),
}


@pytest.mark.parametrize("rule", [*RULE_CELLS, "R12"])
def test_rule_cell(rule: str) -> None:
    if rule == "R12":
        verdict = own_budget_verdict(running_bytes=GIB, added_bytes=GIB + 1, phase_container_bytes=2 * GIB)
    else:
        document, facts = RULE_CELLS[rule]
        binds = {"/w/repo": BindFact("/w/repo", read_only=False, is_socket=False)}
        verdict = _verdict(document, binds, **facts)

    assert (verdict.rule, verdict.shareable) == (rule, False)
    assert verdict.reason


@pytest.mark.parametrize(
    ("published", "expected"), [(None, None), ("", None), ("0", None), (0, None), ("8080", "R1"), (8080, "R1")]
)
def test_only_an_ephemeral_published_port_is_shareable(published: object, expected: str | None) -> None:
    port: dict[str, Any] = {"host_ip": "127.0.0.1", "target": 5432, "protocol": "tcp"}
    if published is not None:
        port["published"] = published

    assert _verdict(_document(web=_service(ports=[port]))).rule == expected


@pytest.mark.parametrize(
    ("section", "spec", "expected"),
    [
        ("volumes", {"name": "dtt-a_data"}, None),
        ("volumes", {"name": "shared-data"}, "R3"),
        ("volumes", {"name": "dtt-a_data", "external": True}, "R3"),
        ("volumes", {"name": "dtt-a_data", "driver_opts": {"o": "bind", "type": "none"}}, "R3"),
        ("networks", {"name": "dtt-a_data", "ipam": {}}, None),
        ("networks", {"name": "dtt-a_data", "ipam": {"config": [{"subnet": "11.0.0.0/24"}]}}, "R4"),
    ],
)
def test_top_level_names_and_host_bound_volumes(section: str, spec: dict[str, Any], expected: str | None) -> None:
    assert _verdict(_document() | {section: {"data": spec}}).rule == expected


@pytest.mark.parametrize(
    ("scale", "watch", "expected"), [({"web": 1}, False, None), ({"web": 2}, False, "R14"), ({}, True, "R14")]
)
def test_scale_and_watch_are_r14(scale: dict[str, int], watch: bool, expected: str | None) -> None:
    assert _verdict(_document(), scale=scale, watch=watch).rule == expected


def test_memory_limit_may_come_from_deploy_resources() -> None:
    service = {"image": "x", "deploy": {"resources": {"limits": {"memory": "512m"}}}}

    assert _verdict(_document(web=service)).shareable


def test_every_rendered_service_is_judged_whatever_its_profiles() -> None:
    # The render already reflects the effective profiles (--profile, "*", COMPOSE_PROFILES): a profiled service in it
    # runs, so it is judged.
    document = _document(web=_service(), debug={"image": "x", "privileged": True, "profiles": ["debug"]})

    assert _verdict(document).rule == "R7"


def test_a_named_service_absent_from_the_render_is_r15() -> None:
    # `up debug` auto-activates debug's profile, but `compose config` without the service argument drops it.
    assert _verdict(_document(), services=("debug",)).rule == "R15"
    assert _verdict(_document(), services=("web",)).shareable


def test_bind_read_only_is_judged_per_mount_not_per_source() -> None:
    checkout = "/w/repo/data"
    reader = {"type": "bind", "source": checkout, "target": "/r", "read_only": True}
    writer = {"type": "bind", "source": checkout, "target": "/w"}
    facts = {checkout: BindFact(checkout, read_only=True, is_socket=False)}  # whatever the caller cached
    for first, second in (("a-writer", "z-reader"), ("z-writer", "a-reader")):
        services = {first: _service(volumes=[writer if "writer" in first else reader]),
                    second: _service(volumes=[writer if "writer" in second else reader])}  # fmt: skip
        assert _verdict(_document(**services), facts).rule == "R10", (first, second)


@pytest.mark.parametrize(
    ("source", "read_only", "is_socket", "expected"),
    [
        (f"{TEMP}/dtt-runs/run-a/data", False, False, None),  # own-root-rw
        ("/w/repo/tests", True, False, None),  # checkout-ro
        ("/w/repo/tests", False, False, "R10"),  # checkout-rw
        ("/", True, False, "R10"),  # root-ro
        (TEMP, True, False, "R10"),  # temp-parent-ro: an ancestor of every private root
        ("/var/run", True, False, "R10"),  # var-run-ro: an ancestor of the socket
        (f"{TEMP}/dtt-runs/run-a/docker.sock", False, True, "R10"),  # socket, even inside the own root
    ],
    ids=["own-root-rw", "checkout-ro", "checkout-rw", "root-ro", "temp-parent-ro", "var-run-ro", "socket"],
)
def test_r10_allowlist(source: str, read_only: bool, is_socket: bool, expected: str | None) -> None:
    mount = {"type": "bind", "source": source, "target": "/m", "read_only": read_only}
    binds = {source: BindFact(source, read_only, is_socket)}

    assert _verdict(_document(web=_service(volumes=[mount])), binds).rule == expected


def test_r10_refuses_a_foreign_private_root_and_an_ancestor_of_the_checkouts_socket() -> None:
    foreign = f"{TEMP}/dtt-runs/run-b/data"
    scope = ShareScope("dtt-a", SCOPE.private_root, "/w/repo", SCOPE.private_root_trees, ("/w/repo/run/docker.sock",))

    assert container_verdict(
        memory_bytes=GIB, binds=[BindFact(foreign, True, False)], external_network=None, scope=SCOPE
    ).rule == ("R10")
    assert (
        container_verdict(
            memory_bytes=GIB, binds=[BindFact("/w/repo", True, False)], external_network=None, scope=scope
        )
    ).rule == "R10"


def test_socket_set_without_unix_endpoint_still_refuses_well_known_socket_ancestors() -> None:
    from docker_test_toolkit.domain.share_policy import DAEMON_SOCKETS

    scope = ShareScope("dtt-a", SCOPE.private_root, "/", SCOPE.private_root_trees, DAEMON_SOCKETS)
    for source in ("/var/run", "/run", "/var"):
        fact = BindFact(source, read_only=True, is_socket=False)
        assert container_verdict(memory_bytes=GIB, binds=[fact], external_network=None, scope=scope).rule == "R10"


def test_unresolved_bind_source_is_refused() -> None:
    mount = {"type": "bind", "source": "/w/repo/x", "target": "/m", "read_only": True}

    assert _verdict(_document(web=_service(volumes=[mount]))).rule == "R10"


def test_direct_container_and_exec_and_connect_rules() -> None:
    def verdict(**facts: Any):
        options = {"memory_bytes": GIB, "binds": [], "external_network": None, "scope": SCOPE, **facts}
        return container_verdict(**options).rule

    assert verdict() is None
    assert verdict(memory_bytes=None) == "R9"
    assert verdict(external_network="fixture") == "R5"
    assert exec_verdict(privileged=False).shareable
    assert exec_verdict(privileged=True).rule == "R7"
    assert network_connect_verdict().rule == "R5"


def _stack() -> dict[str, Any]:
    return _document(web=_service(depends_on={"db": {"condition": "service_started"}}), db=_service(), cache=_service())


def _r12(document: dict[str, Any], running_outside: int, budget: int = 2 * GIB, **facts: Any):
    options = {"subcommand": "up", "services": (), "no_deps": False, **facts}
    added = started_service_limits(document, **options)
    return added, own_budget_verdict(
        running_bytes=running_outside, added_bytes=sum(added.values()), phase_container_bytes=budget
    )


def test_r12_whole_document_equal_to_budget_is_shareable() -> None:
    added, verdict = _r12(_stack(), 0, budget=3 * GIB)

    assert sorted(added) == ["cache", "db", "web"]
    assert verdict.shareable
    assert _r12(_stack(), 0, budget=3 * GIB - 1)[1].rule == "R12"


def test_r12_up_one_service_ignores_stopped_siblings() -> None:
    added, verdict = _r12(_stack(), 0, services=("web",))

    assert added == {"db": GIB, "web": GIB}
    assert verdict.shareable
    assert _r12(_stack(), 0)[1].rule == "R12"  # the whole document (3 GiB) would refuse


def test_r12_up_one_service_counts_running_sibling() -> None:
    assert _r12(_stack(), GIB, services=("web",))[1].rule == "R12"  # cache runs outside the set


def test_r12_no_deps_counts_named_services_only() -> None:
    added, verdict = _r12(_stack(), GIB, services=("web",), no_deps=True)

    assert added == {"web": GIB}
    assert verdict.shareable


def test_r12_run_one_off_substitutes_for_stopped_service() -> None:
    added, verdict = _r12(_stack(), GIB, subcommand="run", services=("web",), no_deps=True)

    assert added == {"web": GIB}
    assert verdict.shareable


def test_r12_unnamed_selection_counts_every_rendered_service() -> None:
    document = _load("sample-c-integration-postgres")  # rendered with --profile postgres
    document["services"]["debug"] = {"image": "x", "mem_limit": str(GIB), "profiles": ["debug"]}  # also rendered
    added = started_service_limits(document, subcommand="up", services=(), no_deps=False)

    assert set(added) == {"postgres", "test-client", "debug"}  # profiles never filter a rendered service
    assert added["debug"] == GIB


def _load(stem: str) -> dict[str, Any]:
    text = (FIXTURES / f"{stem}.json").read_text()
    text = text.replace("$PRIVATE_ROOT", SCOPE.private_root).replace("$CHECKOUT", SCOPE.checkout)
    return json.loads(text)


def _bind_facts(document: dict[str, Any]) -> dict[str, BindFact]:
    facts = {}
    for service in document["services"].values():
        for mount in service.get("volumes", []):
            if mount.get("type") == "bind":
                facts[mount["source"]] = BindFact(mount["source"], bool(mount.get("read_only")), is_socket=False)
    return facts


CAPTURES = sorted(path.stem for path in FIXTURES.glob("*.json"))
EXPECTED_R4 = {"sample-b-e2e-extra-postgres", "sample-b-e2e-extra-web"}  # the extra network pins a subnet


@pytest.mark.parametrize("stem", CAPTURES)
def test_captured_adopter_renders_are_shareable_except_a_pinned_subnet(stem: str) -> None:
    document = _load(stem)
    scope = ShareScope(document["name"], SCOPE.private_root, SCOPE.checkout, SCOPE.private_root_trees, ())
    verdict = compose_verdict(document, scope, _bind_facts(document), scale={}, watch=False)

    assert verdict.rule == ("R4" if stem in EXPECTED_R4 else None), verdict.reason


def test_captures_cover_every_adopter() -> None:
    assert {"-".join(stem.split("-")[:2]) for stem in CAPTURES} == {"sample-a", "sample-b", "sample-c"}


@pytest.mark.parametrize(
    ("source", "is_socket", "words"),
    [
        (f"{TEMP}/dtt-runs/run-a/docker.sock", True, "daemon socket"),
        ("/var/run", False, "daemon socket"),
        (f"{TEMP}/dtt-runs/run-b/data", False, "another run's private root"),
        ("/w/elsewhere", False, "outside the run's own private root"),
    ],
    ids=["socket", "socket-ancestor", "foreign-private-root", "outside"],
)
def test_r10_reason_names_the_cause(source: str, is_socket: bool, words: str) -> None:
    """The recorded verdict reason names why the bind was refused, not a generic allowlist phrase."""
    verdict = container_verdict(
        memory_bytes=GIB, binds=[BindFact(source, True, is_socket)], external_network=None, scope=SCOPE
    )

    assert verdict.rule == "R10" and words in verdict.reason


@pytest.mark.parametrize("mode", ["bridge", "default"])
def test_the_daemon_default_bridge_is_a_shared_namespace(mode: str) -> None:
    """Every run on the daemon can join its default bridge: a service there could reach another run's containers."""
    assert _verdict(_document(web=_service(network_mode=mode)), {}).rule == "R6"
    assert _verdict(_document(web=_service(network_mode="none")), {}).shareable
