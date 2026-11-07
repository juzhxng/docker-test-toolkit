"""Frozen Python contract corpus: the public names used the way adopters use them at baseline.

Run as ``python -I python_baseline.py <installation root> <project root> <redis image id>``. It loads the toolkit
through the reference loader, so it exercises the toolkit build installed under that root. This file only gains cases;
a case never changes, because projects keep calling the toolkit the way it shows. Exit status 0 and a final
``corpus: ok`` line mean the build honours the baseline.
"""

import importlib
import json
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path

root, project, image = Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3]
sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "docs" / "loader"))
loader = importlib.import_module("docker_test_toolkit_loader")  # as a project imports its copy of the loader

toolkit, archive = loader.load_toolkit((root,))

PUBLIC = (
    "ArtifactIdentity",
    "BatchAggregate",
    "BatchCoordinator",
    "BatchExecutionReport",
    "BatchItem",
    "BatchPlan",
    "BatchResult",
    "BatchRunEvidence",
    "BatchSegment",
    "BoundaryResult",
    "ConsumerRunContext",
    "Endpoint",
    "ExternalDockerResource",
    "PhaseBudget",
    "PlannedBatch",
    "RunHandle",
    "RunSpec",
    "SegmentResult",
    "ToolkitError",
    "current_consumer_run",
    "open_batches",
    "open_retained_run",
    "open_run",
    "toolkit_identity",
)
missing = [name for name in PUBLIC if not hasattr(toolkit, name)]
assert not missing, f"public names are gone: {missing}"

# Operator commands run the loader's archive directly (`project retire`, `run recover`).
protocol = subprocess.run(
    [sys.executable, "-I", str(archive), "protocol", "--json"], capture_output=True, text=True, check=False
)
assert protocol.returncode == 0, protocol.stdout + protocol.stderr
envelope = json.loads(protocol.stdout)
assert envelope["ok"] is True and isinstance(envelope["result"]["version"], str)

identity = toolkit.toolkit_identity()
assert isinstance(identity.version, str) and len(identity.sha256) == 64 and isinstance(identity.protocols, Mapping)
pinned = toolkit.ArtifactIdentity(identity.version, identity.sha256, dict(identity.protocols))  # positional

# Batches: positional BatchPlan(root, identity, segments), as adopters build them.
plan = toolkit.BatchPlan(
    project,
    pinned,
    (toolkit.BatchSegment("corpus", (toolkit.BatchItem("item", ("cache",)),)),),
)
coordinator = toolkit.open_batches(plan)
batch = coordinator.next()
assert (batch.segment_id, batch.item_ids, batch.closure) == ("corpus", ("item",), ("cache",))
assert isinstance(batch.batch_id, str) and batch.trailing_boundary_id is None
status = {
    "run_id": "corpus-run",
    "state": "complete",
    "admission": "revoked",
    "generation": 1,
    "revision": 2,
    "ticket": None,
    "test_outcome": "passed",
    "cleanup_outcome": "complete",
    "future": True,  # a field a newer RunStatus adds
}
evidence = toolkit.BatchRunEvidence("corpus-run", status, None, None)  # positional
assert coordinator.report(batch.batch_id, toolkit.BatchExecutionReport("passed", evidence)) is False
assert coordinator.next() is None
aggregate = coordinator.finish()
assert [result.outcome for result in aggregate.batches] == ["passed"]

# One run: open_run -> enter -> docker_context -> leave, reading results only by the attributes projects read.
spec = toolkit.RunSpec(
    project_root=project,
    entrypoint="corpus",
    purpose="python-baseline",
    services=("cache",),
    closure=("cache",),
    profile="corpus",
    phases={"tests": toolkit.PhaseBudget(0, 256 * 1024**2, 1, 1)},  # positional, as an adopter does
    initial_phase="tests",
    artifact=pinned,  # accepted and ignored
    external_resources=(
        toolkit.ExternalDockerResource("image", image, "redis", "use_image"),  # positional, as audit declares it
    ),
    retain=False,
    wait_seconds=120,
    capacity=1,  # accepted and ignored
)
handle = toolkit.open_run(spec)
assert isinstance(handle, toolkit.RunHandle)
assert handle.run_id and handle.project_id and handle.session_id and handle.compose_project
prepared = handle.status()
assert prepared.state == "prepared" and prepared.run_id == handle.run_id
try:
    handle.enter()
    context = handle.docker_context
    assert Path(context.executable).is_file() and "PATH" in context.environment
    entered = handle.status()
    assert isinstance(entered.generation, int) and isinstance(entered.revision, int)
    # A Python consumer borrows the run's Docker lane and reads it through current_consumer_run(), as audit does.
    child, consumer_id = handle.launch_python_consumer(
        "code",
        "import json\n"
        "import docker_test_toolkit as toolkit\n"
        "run = toolkit.current_consumer_run()\n"
        "print(json.dumps({'run_id': run.run_id, 'services': list(run.services),\n"
        "    'version': run.artifact_identity.version, 'docker': str(run.docker_context.executable),\n"
        "    'state': run.status().state}))\n",
        (),
        ("cache",),
        cwd=project,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        borrow_docker=True,
    )
    out, err = child.communicate(timeout=120)
    assert child.returncode == 0, err
    consumer = json.loads(out)
    assert (consumer["run_id"], consumer["services"], consumer["version"]) == (
        handle.run_id,
        ["cache"],
        identity.version,
    )
    assert Path(consumer["docker"]).is_file() and isinstance(consumer["state"], str)
    handle.complete_consumer(consumer_id)
finally:
    left = handle.leave("passed")
assert (left.state, left.test_outcome, left.cleanup_outcome) == ("complete", "passed", "complete")

endpoint = toolkit.Endpoint("cache", "redis", "127.0.0.1", 6379, "c" * 64, 6379, "host", None, 1, 1)  # positional
assert (endpoint.host, endpoint.port, endpoint.namespace, endpoint.generation) == ("127.0.0.1", 6379, None, 1)

try:
    toolkit.open_retained_run(spec, project / "absent-locator.json")
except toolkit.ToolkitError as error:
    assert error.category in {"invalid_input", "incompatible_state", "contention", "infrastructure_failure"}
else:
    raise AssertionError("a missing retained locator was accepted")

print("corpus: ok")
