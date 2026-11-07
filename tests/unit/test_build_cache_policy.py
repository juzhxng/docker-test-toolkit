"""Pure build-cache policy: switch, key, generation names and the closed outcome vocabulary."""

from __future__ import annotations

import ast
import functools
import os
import uuid
from dataclasses import astuple, replace
from pathlib import Path
from typing import Any, cast

import pytest

from docker_test_toolkit.application import lifecycle, supervision
from docker_test_toolkit.application.docker_commands import classify_command
from docker_test_toolkit.application.image_archive import archive_paths
from docker_test_toolkit.application.recovery import atomic_bytes
from docker_test_toolkit.domain import build_cache
from docker_test_toolkit.domain.models import ExternalDockerResource
from docker_test_toolkit.factories.testing import install_failpoint

GIB, MIB = 1024**3, 1024**2
PRIVATE, PROJECT = "/state/private/run-a", "/work/sample"
BASE = ("--file", "Dockerfile", "--build-arg", "UV_SYNC_EXTRAS=", "--tag", "sample:run-a", ".")


def _key(argv: tuple[str, ...], private_root: str = PRIVATE) -> str:
    return build_cache.build_key(argv, private_root=private_root, project_root=PROJECT)


def test_probe_bounds_are_recorded() -> None:
    assert 4 * GIB <= build_cache.C_RET_BYTES <= 16 * GIB
    assert build_cache.CACHE_BLOB_COUNT_MAX >= 4096
    assert build_cache.CACHE_METADATA_MAX >= 16 * MIB
    assert build_cache.CACHE_INDEX_ANNOTATION == ("org.opencontainers.image.ref.name", "latest")


def test_switch_verdict_values() -> None:
    assert build_cache.switch_verdict(None) is None
    assert build_cache.switch_verdict("on") is None
    assert build_cache.switch_verdict("off") == "skipped:disabled"
    for invalid in ("OFF", "0", "", "true"):
        assert build_cache.switch_verdict(invalid) == "skipped:disabled-invalid-value"


def test_build_key_ignores_tags_labels_load_progress_and_quiet() -> None:
    noisy = (
        "--file", "Dockerfile", "--build-arg", "UV_SYNC_EXTRAS=", "--tag", "sample:run-b", "--label", "x=y",
        "--progress", "plain", "--load", "--quiet", "-q", "-tsample:run-c", "--label=z=1", "--progress=tty", ".",
    )  # fmt: skip

    assert _key(noisy) == _key(BASE)


def test_build_key_separates_build_arguments() -> None:
    extras = ("--file", "Dockerfile", "--build-arg", "UV_SYNC_EXTRAS=--extra ml", "--tag", "sample:run-a", ".")

    assert _key(extras) != _key(BASE)
    assert _key(("--file", "Other", *BASE[2:])) != _key(BASE)


def test_build_key_rewrites_private_and_project_roots() -> None:
    first = ("--file", f"{PRIVATE}/Dockerfile", f"{PROJECT}/context")
    second = ("--file", "/state/private/run-b/Dockerfile", f"{PROJECT}/context")

    assert _key(first) == _key(second, private_root="/state/private/run-b")
    assert _key(first) != _key(("--file", f"{PRIVATE}/Dockerfile", "/work/other/context"))


def test_generation_name_round_trip_and_refusals() -> None:
    run, operation = str(uuid.uuid4()), str(uuid.uuid4())
    name = build_cache.generation_name(123, run, operation)

    assert name == f"123-{run}-{operation}"
    assert build_cache.parse_generation_name(name) == (123, run, operation)
    invalid_names = (
        "", "123", f"x-{run}-{operation}", f"123-{run}", f"123-{run}-{operation}-", f"-1-{run}-{operation}",
        f"123-{run.upper()}-{operation}", f"{'9' * 21}-{run}-{operation}", f"../123-{run}-{operation}",
    )  # fmt: skip
    for invalid in invalid_names:
        with pytest.raises(ValueError, match="Cache generation name is invalid"):
            build_cache.parse_generation_name(invalid)


def test_outcome_vocabulary_is_closed() -> None:
    assert build_cache.SKIPPED_REASONS == (
        "disabled", "disabled-invalid-value", "lock-busy", "cache-root-unsafe", "disk-space", "error", "not-promoted",
    )  # fmt: skip
    assert build_cache.NOT_ELIGIBLE_REASONS == (
        "platform-unknown", "foreign-platform", "build-secret", "private-root-context", "stdin-input",
    )  # fmt: skip
    assert build_cache.NOT_PUBLISHED_REASONS == (
        "lock-busy",
        "removed",
        "invalid-export",
        "over-cap",
        "not-native",
        "error",
    )
    vocabulary = {
        "skipped": build_cache.SKIPPED_REASONS,
        "not-eligible": build_cache.NOT_ELIGIBLE_REASONS,
        "not-published": build_cache.NOT_PUBLISHED_REASONS,
    }
    package = Path(build_cache.__file__).parents[1]
    sources = [package / "domain/build_cache.py", package / "application/build_cache.py"]
    seen = 0
    for source in (path for path in sources if path.exists()):
        for node in ast.walk(ast.parse(source.read_text())):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                prefix, _, reason = node.value.partition(":")
                if prefix in vocabulary and reason:
                    seen += 1
                    assert reason in vocabulary[prefix], f"{source.name}: {node.value}"
    assert seen


UUID_A, UUID_B = "33333333-3333-3333-3333-333333333333", "44444444-4444-4444-4444-444444444444"
ELIGIBLE = build_cache.BuildFacts(
    switch=None,
    daemon_platform={"os": "linux", "architecture": "arm64"},
    topology="desktop",
    requested_raw=None,
    requested_emulated="native",
    secret_or_ssh=False,
    private_root_touched=False,
    stdin=False,
)


@pytest.mark.parametrize(
    ("name", "owned"),
    [
        ("docker", True),
        ("docker-test-toolkit-dispatch", True),
        ("docker-authority.json", True),
        ("guardian.stdout.log", True),
        ("guardian.stderr.log", True),
        ("buildkitd.toml", True),
        ("exports", True),
        (f"cid-{UUID_A}", True),
        (f"build-{UUID_A}.json", True),
        ("generation-2", True),
        ("generation-17", True),
        (f"python-{UUID_A}-docker", True),
        (f"python-{UUID_A}.request.json", True),
        (f"python-{UUID_A}.ready.json", True),
        (f"python-{UUID_A}.grant.json", True),
        ("docker-test-toolkit-ab_3defg", True),
        ("docker-test-toolkit-wheels", True),  # the Node shape: the R14 collision, cold only
        ("docker-test-toolkit-recover-ab_3defg", True),
        ("docker-test-toolkit-supervisor-ab_3defg", True),
        (".docker.abcdefgh", True),
        (f".python-{UUID_A}.grant.json.a1b2_c3d", True),
        ("docker-test-toolkit-wheelhouse", False),
        (".unknown.abcdefgh", False),
        (".docker.abcdefgh.abcdefgh", False),  # a temporary of a temporary is not owned
        ("generation-1", False),
        ("generation-02", False),
        ("cid-ABCDEF00-3333-3333-3333-333333333333", False),
        ("agent-archive", False),
        ("Dockerfile", False),
    ],
)
def test_toolkit_owned_exact_names_and_shapes(name: str, owned: bool) -> None:
    assert build_cache.is_toolkit_owned(name) is owned


@pytest.mark.parametrize(
    "argv",
    [
        ("--file", "a/Dockerfile", "."),
        ("--file=a/Dockerfile", "."),
        ("-f", "a/Dockerfile", "."),
        ("-fa/Dockerfile", "."),
        ("-f=a/Dockerfile", "."),
        ("-qf", "a/Dockerfile", "."),
    ],
)
def test_checked_paths_reads_every_dockerfile_spelling(argv: tuple[str, ...]) -> None:
    paths = build_cache.checked_paths(argv)

    assert paths.dockerfiles == ("a/Dockerfile",)
    assert paths.positional == (".",)
    assert not paths.stdin


def test_checked_paths_named_context_values() -> None:
    paths = build_cache.checked_paths(
        (
            "--build-context", "local=../shared",
            "--build-context=layout=oci-layout:///srv/layout@sha256:" + "a" * 64,
            "--build-context", "image=docker-image://alpine:3",
            "--build-context", "remote=https://example.com/context.tar",
            "--build-context", "git=git@github.com:owner/repo",
            "--build-context", "dotgit=https://example.com/repo.git",
            "--build-context", "plain=repo.git",
            ".",
        )
    )  # fmt: skip

    assert paths.named_contexts == ("../shared", "/srv/layout")
    assert paths.positional == (".",)


def test_checked_paths_bare_tokens_are_candidate_contexts() -> None:
    paths = build_cache.checked_paths(("--build-arg", "A=B", "--tag", "x:1", "--pull", "context", "-"))

    assert paths.positional == ("A=B", "x:1", "context", "-")
    assert paths.stdin
    assert build_cache.checked_paths(("--file", "-", ".")).stdin


@pytest.mark.parametrize(
    "argv", [("--file",), (".", "-f"), ("--build-context",), ("--build-context", "missing-equals")]
)
def test_checked_paths_missing_value_raises_value_error(argv: tuple[str, ...]) -> None:
    with pytest.raises(ValueError):
        build_cache.checked_paths(argv)


@pytest.mark.parametrize(
    ("changes", "verdict"),
    [
        ({}, None),
        ({"switch": "off", "daemon_platform": None, "secret_or_ssh": True}, "skipped:disabled"),
        ({"switch": "OFF", "topology": "unknown"}, "skipped:disabled-invalid-value"),
        ({"daemon_platform": None, "secret_or_ssh": True}, "not-eligible:platform-unknown"),
        ({"topology": "remote"}, "not-eligible:platform-unknown"),
        ({"requested_raw": "linux/amd64", "requested_emulated": "emulated"}, "not-eligible:foreign-platform"),
        ({"requested_raw": "linux/arm64", "requested_emulated": "native"}, None),
        ({"secret_or_ssh": True, "private_root_touched": True}, "not-eligible:build-secret"),
        ({"private_root_touched": True, "stdin": True}, "not-eligible:private-root-context"),
        ({"stdin": True}, "not-eligible:stdin-input"),
        ({"topology": "native"}, None),
    ],
)
def test_eligibility_order_and_reasons(changes: dict, verdict: str | None) -> None:
    assert build_cache.eligibility(replace(ELIGIBLE, **changes)) == verdict


def test_toolkit_owned_children_are_ineligible() -> None:
    owned = [f"{PRIVATE}/python-{UUID_A}-docker", f"{PRIVATE}/generation-2/docker", f"{PRIVATE}/exports/x"]

    for path in owned:
        assert build_cache.private_root_verdict([path], PRIVATE), path
    assert build_cache.private_root_verdict([PRIVATE], PRIVATE)  # the private root itself
    assert build_cache.private_root_verdict(["/state"], PRIVATE)  # an ancestor of it
    assert not build_cache.private_root_verdict([f"{PRIVATE}/agent-archive"], PRIVATE)  # X16
    assert not build_cache.private_root_verdict([f"{PRIVATE}-sibling", "/work/sample"], PRIVATE)


def test_build_argument_with_the_private_root_stays_eligible() -> None:
    paths = build_cache.checked_paths(("--build-arg", f"STATE={PRIVATE}", "."))
    resolved = [os.path.realpath(os.path.join(PROJECT, token)) for token in paths.positional]

    assert not build_cache.private_root_verdict(resolved, PRIVATE)


def _private_root_names(tmp_path: Path) -> list[tuple[str, Path]]:
    private = tmp_path / "private"
    private.mkdir()
    context = lifecycle.RunContext(cast(Any, None), UUID_A, UUID_B, tmp_path / "record.json", private)
    command = functools.partial(
        classify_command,
        operation_id=UUID_B,
        run_id=UUID_A,
        compose_project="dtt-example",
        builder="dtt-builder",
        labels={},
        private_root=str(private),
        owned={},
        external=(),
    )
    image = "sha256:" + "f" * 64
    run = classify_command(
        ("run", "--rm", image, "true"),
        operation_id=UUID_B,
        run_id=UUID_A,
        compose_project="dtt-example",
        builder="dtt-builder",
        labels={},
        private_root=str(private),
        owned={},
        external=(ExternalDockerResource("image", image, "fixture", "use_image"),),
    )
    captured: list[Path] = []
    install_failpoint(lambda stage: captured.extend(private.iterdir()) if stage == "probe" else None)
    try:
        atomic_bytes(private / "docker-authority.json", b"{}", before_replace="probe")
    finally:
        install_failpoint(None)
    paths = [
        ("authority-1", *lifecycle._docker_authority_paths(context, 1)),
        ("authority-2", *lifecycle._docker_authority_paths(context, 2)),
        ("consumer-docker", lifecycle._consumer_docker_path(context, UUID_B)),
        ("consumer-launch", *lifecycle._consumer_launch_paths(private, UUID_B)),
        ("guardian-logs", *lifecycle._guardian_log_paths(private)),
        ("cidfile", Path(cast(str, run.plan["cidfile"]))),
        ("builder-config", Path(cast(str, command(("buildx", "create")).plan["config_path"]))),
        ("build-metadata", Path(cast(str, command(("buildx", "build", "--tag", "x:1", ".")).plan["metadata"]))),
        *(("archive", Path(path)) for path in astuple(archive_paths(str(private), UUID_B))),
        ("recovery-worker", lifecycle._recovery_worker_directory(private)),
        ("supervisor", supervision._supervisor_temporary(private)),
        *(("atomic-temporary", path) for path in captured if path.name != "docker-authority.json"),
    ]
    assert any(label == "atomic-temporary" for label, *_ in paths)
    return [(label, path) for label, *group in paths for path in group]


def test_every_private_root_generator_produces_a_toolkit_owned_name(tmp_path: Path) -> None:
    private = tmp_path / "private"
    for label, path in _private_root_names(tmp_path):
        first = path.relative_to(private).parts[0]
        assert build_cache.is_toolkit_owned(first), (label, first)


def _generation(key: str, wall_ns: int, *, run: str = UUID_A, size: int = GIB // 4, valid: bool = True):
    name = build_cache.generation_name(wall_ns, run, str(uuid.UUID(int=wall_ns)))
    return build_cache.GenerationFacts(key, name, wall_ns, run, size, valid)


def _kept(generations, plan) -> list:
    removed = {*plan.invalid, *plan.superseded, *plan.evicted}
    return [item for item in generations if (item.key, item.name) not in removed]


def test_reconcile_plan_steps_in_order() -> None:
    generations = [_generation(f"k{index:02}", 100 + index) for index in range(18)]
    generations += [_generation("k00", 50), _generation("k01", 200, valid=False)]

    plan = build_cache.reconcile_plan(generations, staging=("staging-a",))

    assert plan.staging == ("staging-a",)
    assert plan.invalid == (("k01", generations[19].name),)
    assert plan.superseded == (("k00", generations[18].name),)
    assert plan.evicted == (("k00", generations[0].name), ("k01", generations[1].name))


def test_reconcile_plan_postconditions_hold() -> None:
    for seed in range(200):
        generations = [
            _generation(f"k{(seed * 7 + index * 3) % 23}", (seed * 31 + index * 17) % 97 + index * 1000,
                        size=((seed + index) % 5) * GIB // 2, valid=(seed + index) % 6 != 0)
            for index in range(seed % 30)
        ]  # fmt: skip
        kept = _kept(generations, build_cache.reconcile_plan(generations, staging=()))

        assert len(kept) <= build_cache.N_MAX
        assert sum(item.bytes for item in kept) <= build_cache.C_RET_BYTES
        assert len({item.key for item in kept}) == len(kept)
        assert all(item.valid for item in kept)


def test_repeated_interruption_of_one_key_keeps_one() -> None:
    generations = [_generation("k", 1), _generation("k", 3), _generation("k", 2)]

    plan = build_cache.reconcile_plan(generations, staging=())

    assert _kept(generations, plan) == [generations[1]]
    assert len(plan.superseded) == 2


def test_reconcile_plan_with_a_backwards_clock_keeps_one_valid_generation() -> None:
    newer_invalid = _generation("k", 900, valid=False)
    older_valid = _generation("k", 100)
    clock_went_back = _generation("k", 50)

    plan = build_cache.reconcile_plan([newer_invalid, older_valid, clock_went_back], staging=())

    assert _kept([newer_invalid, older_valid, clock_went_back], plan) == [older_valid]


def test_publication_plan_new_key_at_n_max() -> None:
    other = str(uuid.UUID(int=7))
    kept = [_generation(f"k{index}", 100 + index, run=other, size=1) for index in range(build_cache.N_MAX)]

    plan = build_cache.publication_plan(kept, key="new", run_id=UUID_A, new_bytes=1)

    assert plan == build_cache.PublicationPlan(superseded=(), marked=(("k0", kept[0].name),))


def test_publication_plan_larger_replacement() -> None:
    other = str(uuid.UUID(int=7))
    kept = [
        _generation("old", 1, run=other, size=GIB),
        _generation("k", 2, size=2 * GIB),
        _generation("x", 3, size=GIB),
    ]

    plan = build_cache.publication_plan(kept, key="k", run_id=UUID_A, new_bytes=3 * GIB)

    assert plan == build_cache.PublicationPlan(superseded=(("k", kept[1].name),), marked=(("old", kept[0].name),))


def test_publication_plan_never_evicts_the_current_run() -> None:
    kept = [_generation("mine", 1, size=2 * GIB), _generation("k", 2, size=GIB)]

    assert build_cache.publication_plan(kept, key="new", run_id=UUID_A, new_bytes=2 * GIB) is None


def test_publication_plan_over_cap() -> None:
    assert build_cache.publication_plan([], key="k", run_id=UUID_A, new_bytes=build_cache.C_RET_BYTES + 1) is None
    assert build_cache.publication_plan([], key="k", run_id=UUID_A, new_bytes=build_cache.C_RET_BYTES) == (
        build_cache.PublicationPlan(superseded=(), marked=())
    )


def test_layer_outcome() -> None:
    assert build_cache.layer_outcome([], []) == "miss"
    assert build_cache.layer_outcome(["a"], ["a", "b"]) == "hit"
    assert build_cache.layer_outcome(["base", "rebuilt"], ["base", "old"]) == "miss"
    assert build_cache.layer_outcome(["new"], ["a", "b"]) == "miss"
    assert build_cache.layer_outcome(["a"], []) == "miss"
