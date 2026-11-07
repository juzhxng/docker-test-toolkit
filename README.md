# docker-test-toolkit

[![Release](https://img.shields.io/github/v/release/juzhxng/docker-test-toolkit)](https://github.com/juzhxng/docker-test-toolkit/releases/latest)
[![Python](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![CI](https://github.com/juzhxng/docker-test-toolkit/actions/workflows/ci.yml/badge.svg)](https://github.com/juzhxng/docker-test-toolkit/actions)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)
[![Checked with mypy](https://www.mypy-lang.org/static/mypy_badge.svg)](https://mypy-lang.org/)

Let coding agents run Docker-backed tests from several projects on one machine, without them breaking each other or
leaving anything behind.

When agents start test suites at once on a laptop or CI host, the suites compete for memory, and a killed run leaves
containers, networks and volumes behind. Cleaning up with `docker system prune` then takes other projects' resources with
it. This toolkit sits between your tests and Docker:

- **Admission.** A run states what it needs. It waits until the machine has the memory, CPU and disk for it, then
  runs. Runs that fit together overlap.
- **Exact cleanup.** Every Docker resource a run creates is recorded before it exists, and only those resources are
  removed. It never prunes.
- **Recovery.** A run killed at any point is found and cleaned up by the next queued run or `cleanup project --execute`.
- **Shared runs.** Runs that fit can use the same daemon together, under rules that keep them from disturbing each other.
- **A guard for agents.** A pre-tool-use hook for Claude Code, Codex and others refuses direct Docker changes, so everything
  goes through the toolkit.

Your project keeps its compose files, service graph, readiness checks, migrations and test selection. The toolkit never
reads them for meaning. It is one file with no dependencies beyond Python's standard library, installed once per machine.
Projects hold no copy, and a new toolkit release never requires a project change.

## Set it up with your agent

Paste this into your coding agent, in the project you want to connect:

```text
Set up docker-test-toolkit for this project. Read and follow
https://raw.githubusercontent.com/juzhxng/docker-test-toolkit/main/docs/agent-setup.md
Ask me before anything that changes my machine or my harness configuration.
```

The agent checks your machine, installs the toolkit, connects your tests and proves it with one real run.
[Read the steps](docs/agent-setup.md) first if you like. To do it by hand, read on.

## Requirements

- macOS or Linux, on CPython 3.12 or newer
- Docker CLI with the Compose and Buildx plugins, and one of these runtimes:
  - macOS: [Docker Desktop](https://docs.docker.com/desktop/), [Colima](https://github.com/abiosoft/colima) or a
    [Podman](https://podman.io) machine
  - Linux: Docker Engine

OrbStack and other runtimes are refused. Setup details for each runtime are in the
[operations guide](docs/operations.md#host-telemetry-limits).

## Install

Download `docker-test-toolkit.pyz` from the [latest release](https://github.com/juzhxng/docker-test-toolkit/releases/latest),
compare its hash with the release notes, and install it:

```sh
python3.12 -I docker-test-toolkit.pyz install --json
docker-test-toolkit readiness inspect --json
```

Any CPython 3.12 or newer outside a virtual environment works. On Linux, run `install` as root: the installation lives in
`/opt/docker-test-toolkit` and the command in `/usr/local/bin`.

## Use

**Python projects** copy [`docs/loader/docker_test_toolkit_loader.py`](docs/loader/docker_test_toolkit_loader.py) into
their tree once, and declare a run:

```python
import subprocess
from pathlib import Path

from docker_test_toolkit_loader import load_toolkit

toolkit, _archive = load_toolkit()
GIB = 1024**3

spec = toolkit.RunSpec(
    project_root=Path.cwd(),
    entrypoint="integration",
    purpose="integration-tests",
    services=("redis",),
    closure=("redis",),
    profile="default",
    phases={
        "tests": toolkit.PhaseBudget(container_bytes=2 * GIB, host_process_bytes=1 * GIB, daemon_cpus=2, host_threads=2)
    },
    initial_phase="tests",
)

run = toolkit.open_run(spec).enter()  # waits here until the machine has room
outcome = "failed"
try:
    docker = run.docker_context  # run your compose commands and tests through this

    def run_docker(*arguments: str) -> None:
        subprocess.run([str(docker.executable), *arguments], env=docker.environment, check=True)

    run_docker("pull", "redis@sha256:<digest>")
    run_docker("compose", "up", "--detach", "--wait", "--wait-timeout", "60")
    ...  # your tests
    outcome = "passed"
finally:
    run.leave(outcome)  # removes exactly what the run created
```

The stack's `compose.yaml` uses images the run pulled by digest or built, and gives each service limits within the
budget:

```yaml
services:
  redis:
    image: redis@sha256:<digest>
    mem_limit: 256m
    cpus: 0.5
```

**Other languages** talk to the `docker-test-toolkit bridge lease` command over NDJSON on stdin and stdout.

The [adopter guide](docs/adopters.md) covers both, along with finite batches, sharing a run and CI.

## Documentation

- [Agent setup](docs/agent-setup.md): the steps a coding agent follows to connect a project
- [Adopter guide](docs/adopters.md): integrating a project, and the permanent contract it can rely on
- [Operations guide](docs/operations.md): installation, CI runners, cleanup, recovery and troubleshooting
- [Compatibility policy](docs/compatibility.md): what stays stable between releases

## Development

Developing the toolkit also needs [`uv`](https://docs.astral.sh/uv/) and `gitleaks` 8.30.1.

```sh
uv sync
uv run poe qa
```

Build the archive from committed files. The same commit always gives the same bytes:

```sh
uv run python scripts/build_zipapp.py build --repository . --output-directory dist
```

## License

Copyright 2026 juzhxng. Licensed under [Apache-2.0](LICENSE).
