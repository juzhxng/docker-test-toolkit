# Adopters

## Using the installed toolkit

The toolkit is installed once per machine (see [Operations](operations.md#installation)). A project holds no copy, pin,
digest or bootstrap of it, and no toolkit release ever requires a project change.

**Python projects** copy the reference loader once, verbatim, into their own tree and never update it. It is
`docs/loader/docker_test_toolkit_loader.py` in this repository:

```python
"""Load the machine-installed Docker Test Toolkit. Project-owned; it never needs to change."""

import importlib
import importlib.machinery
import importlib.util
import os
import pwd
import sys
from pathlib import Path
from types import ModuleType

HOME = Path(pwd.getpwuid(os.getuid()).pw_dir)
ROOTS = (HOME / ".local/share/docker-test-toolkit", Path("/opt/docker-test-toolkit"))


def load_toolkit(roots: tuple[Path, ...] = ROOTS) -> tuple[ModuleType, Path]:
    """Return ``(toolkit module, archive path)``; the first load in a process fixes its version."""
    if "docker_test_toolkit" in sys.modules:
        return _imported(roots)
    root = next((root for root in roots if (root / "current").exists()), None)
    if root is None:
        looked_up = " or ".join(str(root / "current") for root in roots)
        raise RuntimeError(f"Docker Test Toolkit is not installed: no {looked_up}")
    version_folder = (root / "current").resolve(strict=True)
    if version_folder.parent != root.resolve():  # `current` names a version folder of this installation, nothing else
        raise RuntimeError(f"Docker Test Toolkit is not installed: {version_folder} is outside {root}")
    archive = version_folder / "docker-test-toolkit.pyz"
    if archive.is_symlink():  # the installed file itself, never a link to another copy
        raise RuntimeError(f"Docker Test Toolkit is not installed: {archive} is a link")
    if not archive.is_file():
        raise RuntimeError(f"Docker Test Toolkit is not installed: {archive} is missing")
    spec = importlib.machinery.PathFinder.find_spec("docker_test_toolkit", [str(archive)])  # this archive only
    package = str(archive / "docker_test_toolkit" / "__init__.py")
    if spec is None or spec.loader is None or spec.origin != package:
        raise RuntimeError(f"Docker Test Toolkit is not installed: {archive} holds no toolkit")
    sys.path.insert(0, str(archive))
    toolkit = importlib.util.module_from_spec(spec)
    sys.modules["docker_test_toolkit"] = toolkit
    try:
        spec.loader.exec_module(toolkit)
    except BaseException:
        _forget(archive)
        raise
    return toolkit, archive


def _imported(roots: tuple[Path, ...]) -> tuple[ModuleType, Path]:
    """Return the toolkit this process already imported, if it came from an installation's version folder."""
    toolkit = importlib.import_module("docker_test_toolkit")
    origin = toolkit.__file__
    archive = Path(origin).parents[1] if origin else Path()
    installed = {root.resolve() for root in roots}
    version_folder = archive.parent
    if (
        archive.name != "docker-test-toolkit.pyz"
        or archive.is_symlink()
        or not archive.is_file()  # the installed archive itself, never an unpacked folder of that name
        or version_folder.is_symlink()  # `current` is a moving alias, never the version this process runs
        or version_folder.parent.resolve() not in installed
    ):
        raise RuntimeError(f"Docker Test Toolkit was not imported from an installation: {origin}")
    return toolkit, archive


def _forget(archive: Path) -> None:
    """Remove every trace of a failed import, so a later load cannot mix it into another version."""
    for name in [name for name in sys.modules if name.partition(".")[0] == "docker_test_toolkit"]:
        del sys.modules[name]
    if str(archive) in sys.path:
        sys.path.remove(str(archive))
```

`load_toolkit()` returns the toolkit module and the archive it runs from. The first load in a process fixes its version:
a later call returns the same module even after an install switched `current`. The package is found in the resolved
archive alone, so a copy elsewhere on the search path never runs, and a module that no installation holds is refused.
Run operator commands with the returned archive: `[sys.executable, "-I", archive, family, action, "--input", file,
"--json"]`. A missing installation raises before any toolkit or Docker state is touched and names both looked-up paths;
the project's harness reports it as a setup failure, never as a skip.

**Other projects** run the command, the first existing of `<passwd home>/.local/bin/docker-test-toolkit` and
`/usr/local/bin/docker-test-toolkit`:

- `docker-test-toolkit bridge lease` and `docker-test-toolkit bridge batch`: NDJSON on stdin and stdout;
- `docker-test-toolkit <family> <action> --input FILE --json [--execute]`: operator commands;
- `docker-test-toolkit protocol --json` and `docker-test-toolkit readiness inspect --json`.

The launcher `exec`s the toolkit, so the spawned process is the bridge itself, with the PID the project spawned.

## The permanent contract

Projects depend only on this contract:

- **Python:** the names in `docker_test_toolkit.__all__`; the constructor fields of the public dataclasses in their
  positions (`BatchPlan(root, identity_or_None, segments)` stays positional); the methods of `RunHandle`,
  `BatchCoordinator` and `ConsumerRunContext`; the `RunStatus` and Docker context attributes; `ToolkitError.category`,
  `reason` and `cleanup`.
- **Wire:** the request envelope `{"command", "input"}`; the response envelope with `protocol`, `command`, `ok`,
  `result` or `error`, and `cleanup`; the lease, batch and operator verbs.
- **Layout:** the two installation roots, `current`, the archive with `docker_test_toolkit` at its top level, and the
  command paths; CPython 3.12 or newer and the standard library only.

Rules for both entry points:

1. **Additive only.** A verb, name, field, value rule or error category is never removed and never changes meaning.
2. **Unknown fields are ignored at every level, on both sides.** The toolkit ignores unknown request fields from the
   envelope down to every nested object; required fields stay required. Projects ignore unknown response fields
   (required-key checks, never exact key sets) and read an unknown value of an outcome-type field (`test_outcome`,
   `cleanup_outcome`, batch outcomes, `state`, `admission`) as "not success", never as a parse error.
3. **Inputs accepted once stay accepted.** A run specification or plan an earlier version accepted is never refused for
   its shape; admission may still make it wait.
4. **Closed categories.** `invalid_input`, `incompatible_state`, `contention`, `infrastructure_failure`, `test_failure`
   and `incomplete_cleanup`, with exit codes 2–7. Projects act on the category and the exit code, and on the listed
   reasons only: `claim-held` means another live recoverer holds the run's takeover claim and a retry may succeed.
   Every other reason, `message` and `next_action` is informational.
5. **Version fields are optional and ignored:** `RunSpec.artifact`, `RunSpec.capacity`, `BatchPlan.artifact_identity`,
   and `artifact`, `capacity` and `artifact_identity` on the wire. `toolkit_identity()` reports the running version.
6. **Value rules.** `run_id` and `session_id` are lowercase UUIDs and `project_id` is 64 lowercase hex characters;
   `generation` is at least 1 after entry and grows with every entry of a run; `private_root` is an absolute directory
   private to the run from preparation until cleanup, where the project may create files; `docker.executable` is the
   absolute path of the run's Docker command and `docker.path` a `PATH` under which `docker` resolves to it;
   `compose_project` and `builder` are opaque names passed to Docker unchanged; an `image save` through the run's Docker
   command prints one JSON object with `path` and `image_id`. Nothing else is contract: not the `dtt-` name pattern,
   not `generation == 1`, not the executable's location, not `builder == compose_project`, not the `protocols` map.

A refused `cleanup project` or `cleanup session` on a project with indexed runs keeps its category and exit code and names
each blocking run in `error.context.runs`: `run_id`, `action` (`run.recover` or `run.release`), `context` (exactly what
that command accepts) and `reason`. A run whose record does not load or belongs to another project has `action` and
`context` null and `reason` `record-unloadable` or `record-foreign`. `cleanup project --execute` and `project retire
--execute` also report the recovery pass they ran as `recovery_trigger` (`spawned`, `spawn_failed`, `skipped`), in `result`
or `error.context`.

The toolkit's frozen Python corpus and wire transcript (`tests/fixtures/contract/`) run against every release, together
with the cross-version scenarios.

## Adapter boundary

The repository computes the deterministic service closure, phase budgets, Compose/readiness/migration policy, endpoints,
assets, and environment mapping before opening a run. It passes only the generic typed run contract to the toolkit.
No repository passes executable callbacks, import paths, arbitrary service URLs, provider settings, or credentials.

Python adapters retain one loaded toolkit for the entire process. Node adapters retain one bridge child
and issue exactly one NDJSON request at a time. Both register all live consumers and preserve separate primary and
cleanup outcomes.

A Python parent that must run one installed child inside the same already-admitted Docker lane may opt in with
`launch_python_consumer(..., borrow_docker=True)`. The child must register the parent's entire declared closure and may
obtain only `current_consumer_run()` with its bounded private Docker shim and read-only status. The parent remains the
sole lifecycle and cleanup authority. Ordinary consumers, including full-closure consumers, do not inherit this access.

A repository that needs an image or build result as a file uses the shim's archive exports (see
[Operations](operations.md#archive-exports)). The archive lives in the run's private root and is removed by `leave`, so
copy it out and verify the copy against the SHA-256 in the printed line or the receipt before `leave`; the repository
owns the copy's retention from then on.

The shim relays a Docker mutation's stdout, stderr and exit code unredacted. Two things can differ from the child's
bytes: bytes that are not UTF-8 are replaced (U+FFFD), and a stream whose relay would exceed 384 KiB keeps up to 192 KiB
of relayed text from its start and from its end around one line
`[docker-test-toolkit: <stream> cut from N bytes to its first and last 192 KiB]` (each cut can also show one replacement
character where it split a character), so both streams fit a 1 MiB capture buffer together. Commands that stream (`run`, `compose run`, `compose exec`, `compose cp`) reach you untouched;
observation and `docker exec` output is exact or refused. A timed-out mutation whose output the toolkit captures, or a
Compose file that does not render, names the end of the child's stderr in the toolkit's message. Adapters that report a failed command's
stderr should keep a redacted tail of it.

Host-only selection does not load the Docker lifecycle, inspect the daemon, or create state. Any newly active
integration/E2E/audit lane must have an explicit repository classification.

Repository adapters own every product command, service, environment name and value, and success meaning; the toolkit
infers none of them from a project's layout.

## Keeping service logs when a run fails

`leave` removes what the run created, the containers and their logs included. To keep the logs of a failed test, read
them before `leave`, on the failure path only, so passing runs are not slowed:

- For each service of the adapter's own closure, run `compose logs --no-color --timestamps --tail N <service>` through
  the run's Docker command and write the output to a file of this run in a directory the adapter owns, outside the
  run's private root (`leave` removes that root).
- Read and write each service on its own. A failed read, or a failed directory creation or file write, is noted in the
  adapter's output and never raised, and `leave` runs in a `finally`, so a capture never changes the test outcome or
  stops cleanup.
- A read through the shim is bounded: 30 seconds, 1 MiB per stream, UTF-8 only; anything else is refused. `--tail`
  selects lines but does not bound bytes, so a noisy service can still be refused; use a smaller N or accept the note.
  `--follow` is not usable.
- `compose run`, `run` and `compose exec` output is already relayed live and needs no capture.
- A crashed run is not covered: nothing is left to read, and recovery removes the containers.
- To keep the whole stopped stack instead, open the run with `retain` and leave it with `retain(outcome)`; the stopped
  containers keep their logs until the run is released, and a retained run blocks the project's next run.
- Logs can carry secrets. The toolkit relays them unredacted; the adapter owns the files, their place, their
  permissions and their deletion.

```python
def keep_service_logs(handle, services, directory, tail=200):
    docker = handle.docker_context
    target = directory / handle.run_id  # outside handle.private_root
    for service in services:
        try:
            target.mkdir(parents=True, exist_ok=True)
            read = subprocess.run(
                [str(docker.executable), "compose", "logs", "--no-color", "--timestamps", "--tail", str(tail), service],
                env=dict(docker.environment),
                capture_output=True,
                text=True,
                timeout=60,
            )
            if read.returncode:
                print(f"logs of {service} not kept: exit {read.returncode}: {read.stderr[-500:]}", file=sys.stderr)
                continue
            (target / f"{service}.log").write_text(read.stdout)
        except (OSError, subprocess.SubprocessError) as error:
            print(f"logs of {service} not kept: {error}", file=sys.stderr)


outcome = "passed"
try:
    run_the_tests()
except BaseException:
    keep_service_logs(handle, closure, Path("test-logs"))
    outcome = "failed"
    raise
finally:
    handle.leave(outcome)
```

## Admission across projects

Adapters submit independent ordinary Docker-backed work once and retain the same queued process or handle until the run
leaves. The toolkit's shared FIFO queues it across projects; another project's active, queued, admitting or
cleanup-pending run is never a reason to wait before submission, to cancel and reopen, or to poll foreign records.
Every adapter repairs only its own project's exact state: an indexed nonterminal, retained or cleanup-pending run of the
project is inspected and repaired (`run status`, `run recover`, resume, release, project cleanup) before another ordinary
run is opened. A dead owner's run in another project is recovered by the toolkit's detached worker; an adapter never
recovers, releases or cleans a foreign run to make room. The adapter's outer timeout must exceed the toolkit queue
deadline plus bounded startup and cleanup; an expired admission takes up to 5 more seconds to report why it waited.

When the machine allows shared runs ([Shared runs](operations.md#shared-runs)), two runs, of the same project too, may
hold the account at once. Adapters change nothing, but three things follow:

- **What keeps a run exclusive.** A request stays exclusive once one of its commands broke a share rule: a fixed host
  port, host networking, another container's namespaces or Docker's default bridge, privileged mode or devices, the Docker socket, a writable
  bind of the checkout, a container without a hard memory limit, `--scale` above 1 or `watch`, or more than the run's
  own phase budget. The shared run's command is refused with `share-rule`, naming the rule and saying that the request
  is now recorded as not shareable; rerun it, and the next run of that request runs alone while the verdict is kept
  (the 256-key store per project evicts positive verdicts before negative ones and an earlier boot's before this boot's; a change of the share rules drops it).
- **Same checkout.** Two runs of one project share the working tree. Outputs written into the checkout (coverage files,
  reports, caches) can race; write them under the run's private root or name them per run.
- **Teardown.** A shared run's leave never deletes an image another run still uses; it hands it off and the last user
  removes it, so an image may outlive the run that built or pulled it by one leave.

## Finite batches

An adapter describes ordered work with `BatchItem`, `BatchSegment`, and `BatchPlan`, then calls `open_batches`. Each
item carries only an opaque adapter ID and its canonical service closure. Per segment, the returned coordinator yields
**one union batch** of the segment's Docker items, then **one host-only batch** of its items without services
(batch protocol 2). Items keep the adapter's order and their own declared closures. The union's closure is the set of
the items' closures, ordered the same way wherever it appears: the first tuple, in whole-plan order, that an item
declares for exactly that set, otherwise each service's first appearance across the plan. Segments never merge, so
**segments are the only separation tool**: items that must not share one stack go to different segments. A segment's
trailing boundary rides its last emitted batch.

For every nonempty union closure, the adapter opens one ordinary run, derives its run budget from that closure, and owns
the native command or subprocess, test selection, coverage arguments and outputs, readiness, and result
classification. Before running a batch it checks that each item's declared closure is a subset of the batch closure. It
reports bounded ordinary lifecycle evidence with `BatchExecutionReport` and `BatchRunEvidence`. A host-only batch has an
empty closure, opens no run, and reports no run evidence. When `report` requests a trailing boundary, the adapter
performs its local reset or safety operation and calls `report_boundary`; after all work, `finish` returns the
aggregate. Batched children never keep or resume a run: retention belongs to a direct, unbatched run.

The service graph, command catalog, item-to-command mapping, segment layout, and boundary implementation stay in the
adopting repository. Adding or removing a service changes adapter data, not toolkit code. There is no central project
runner, product catalog, shared session, cache shared between projects, shared Docker resource, or cross-repository
integration suite.

## Sharing one run safely

One run now serves every test of a segment. These rules keep that safe.

1. **Co-runnable segments.** Items in one segment must run together in one stack. Each of these gets its own segment:
   - a different configuration of a service;
   - a run-wide mode;
   - a test matrix, for example database backends;
   - a different session-wide application configuration derived from the run's endpoints.

   Segments that hold destructive tests come after the segments they could disturb.
2. **Budget from the union.**
   - The run's container budget covers everything resident at once. For long-running services that is the sum of the
     declared limits of the batch closure, not of one item's closure. A lane that only runs one-shot containers one
     after another needs the largest of them plus any resident services.
   - The toolkit refuses at a phase transition when the running owned limits exceed the target phase. That backstop
     works only for transitions after the services start. An adapter that transitions before starting services, or
     never transitions, proves the rule in its own unit tests.
3. **Settings scoped to what a test may see.**
   - **Integration and other per-test suites.** Each test sees only its declared services' settings. The adapter sets
     them for the test and restores them afterwards. Session- or module-scoped fixtures do not write process
     environment.
   - **Session-wide application suites** (E2E with a session app). The settings must exist before the session app is
     built, so per-test scoping cannot apply. The adapter applies the segment union's settings at session scope, before
     any suite session fixture, and restores them at session end. This is safe only because segments are cut by
     application-configuration class (rule 1).
   - Higher-scoped clients and URLs come from the run object, never from the environment. They are allowed only for
     services that no test in the same segment mutates.
4. **Declare every service a test touches.** A test's declared closure is what makes its settings visible.
5. **Own your data.** Each test uses unique data, or resets what it uses in its own fixture. A run is one sequential
   process, so a reset at the start of a test is valid.
6. **Collect only the batch.** Each batch collects only its own test files and executes exactly its node IDs.
7. **Random order** with a printed seed: pytest-randomly in Python; Node's `--test-randomize` only once stable on the
   pinned Node version.
8. **Destructive tests.**
   - A test is destructive when it, or a fixture or helper it uses, invokes a service lifecycle or signal operation on
     the run:
     - a Docker or Compose verb `stop`, `start`, `restart`, `kill`, `pause` or `unpause`, including through the run's
       Docker shim (`run.docker(...)`, `self.run.docker(...)`) and through helpers that forward their arguments to it;
     - an `exec` that sends a signal inside a service;
     - a `subprocess` call on the run's Compose prefix;
     - the run's mutation window;
     - an adapter helper that stops a service.

     Such tests are marked, and a guard finds them by invocation pattern, not by a list of helper names.
   - They restore what they broke and republish the run's endpoints.
   - A readiness check runs after each. A broken service fails that test by name; later tests of the same segment may
     fail as accepted collateral.
   - An end-of-batch check covers every service.
9. **Fit the smallest machine you support.** Certify against it even when you develop on a larger machine, whose
   admission would accept more.
   - Every declared phase budget of each segment's run must pass the toolkit's static admission check on that machine,
     which assesses every phase:
     - guest: `containers + 0.5 GiB ≤ VM MemTotal`;
     - host: `host_process + host_reserve + (containers + 0.5 GiB) + 0.25 GiB ≤ the machine's memory`.
   - `host_reserve` is that machine's declared reserve, 4.5 GiB unless the account declares another value.
   - A guard test per adopter evaluates both bounds for every phase, deriving `containers` from the adapter's own
     closure function and Compose catalog, not from hand-written figures. The smaller bound applies.
   - Limits are first right-sized from measured peaks plus a margin. A segment that still does not fit is split. Where
     a split is impossible, stop and record the exception with the user's agreement.

## Build cache

**Hold-shortening levers live where every adopter already passes:** the toolkit's build shim (the only build path),
admission and the run's receipts. They are on by default, need no adopter declaration and fail open. Adopter guidance is
advisory:

- order Dockerfile steps so that lock files and manifests are copied and installed before sources;
- pass no cache flags; a caller `--cache-from` is refused and the toolkit injects its own;
- pass the run's shim environment (Node: the process environment) so the per-run off switch
  `DOCKER_TEST_TOOLKIT_BUILD_CACHE=off` reaches the dispatcher;
- declare `build` as its own phase if you want the phase split; build durations appear in `project timeline` anyway;
- use `project timeline` to see hits and misses, and the off switch or `cache remove` for cold evidence;
- never bake credentials into layers or build arguments.

See [Project build cache](operations.md#project-build-cache).

## Harness guard

Coding assistants read Docker state freely but change it only through the toolkit. `docker-test-toolkit guard
docker-command` is a pre-tool-use hook that refuses a shell command that would change Docker state directly (`docker
run`, `exec`, `rm`, `build`, `buildx create`, `compose up`, `prune`, ...), also behind `sudo`, environment prefixes,
`$(...)`, `ssh <host> ...` and `sh -c ...`. Reads (`ps`, `inspect`, `logs`, `info`, `buildx ls`, `compose ps`, ...),
here-document bodies and every command that only mentions Docker in its text pass. A command carrying
`# docker-direct-ok` passes; add it only on the user's explicit go. The guard reads command text, so it is a guard
rail, not a sandbox: the toolkit, the adapter and scripts that call Docker themselves are not affected.

Claude Code (`.claude/settings.json`) and Codex (`.codex/hooks.json`) send the same payload and accept the same deny
reply:

```json
{
  "hooks": {
    "PreToolUse": [
      {
        "matcher": "Bash",
        "hooks": [{"type": "command", "command": "docker-test-toolkit guard docker-command", "timeout": 10}]
      }
    ]
  }
}
```

Codex asks once to trust a new hook (`/hooks`). Any other harness or a Git hook uses `--exit-code` (the hook payload on
stdin) or `--command "<text>"`: a refused command exits 2 with the reason on stderr, an allowed one exits 0 silently.

## CI

See the [CI guide](ci.md) for installing the toolkit on hosted and self-hosted runners. Docker-backed integration/E2E jobs use
the ordinary adapter command; they do not call a separate setup/teardown utility.
