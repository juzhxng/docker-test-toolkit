# Agent setup

Instructions for a coding agent setting up docker-test-toolkit for its user. People can follow them too. Report what you
found after each step, and stop and ask whenever a step says so.

## Choose where

Ask the user whether to set it up on this machine, in CI, or both, unless they already said. For CI, follow the
[CI guide](ci.md) for the install, then steps 3 to 6 below still apply to the project. Everything below is for this machine.

## 1. Check the machine

Check for CPython 3.12 or newer, the Docker CLI with its Compose and Buildx plugins, and a running daemon.

Supported runtimes are Docker Desktop, Colima and Podman machines on macOS, and Docker Engine on Linux. Other runtimes are
refused. A Podman machine also needs a Docker context at its API socket ([details](operations.md#host-telemetry-limits)). If
something is missing, tell the user what and stop. Do not install or change a runtime yourself.

## 2. Install the toolkit

The toolkit is installed once per machine, for every project of this user. **Ask the user before installing.** If
`docker-test-toolkit protocol --json` already works, tell the user which version is installed and ask whether to upgrade.

```sh
curl -fsSLO https://github.com/juzhxng/docker-test-toolkit/releases/latest/download/docker-test-toolkit.pyz
shasum -a 256 docker-test-toolkit.pyz   # sha256sum on Linux
```

Compare the hash with the table in the [latest release's notes](https://github.com/juzhxng/docker-test-toolkit/releases/latest).
If they differ, delete the file, tell the user and stop. Otherwise install it, using any CPython 3.12 or newer that is not
inside a virtual environment. On macOS:

```sh
python3.12 -I docker-test-toolkit.pyz install --json
docker-test-toolkit readiness inspect --json
```

On Linux the installation lives in `/opt` and belongs to the user who runs the tests. It takes `sudo` for the folder and the
link, but not for the install: a root install leaves that user locked out. Ask the user to run these or to allow `sudo`:

```sh
sudo install -d -o "$(id -un)" -g "$(id -gn)" -m 755 /opt/docker-test-toolkit
python3 -I docker-test-toolkit.pyz install --json
sudo ln -sf /opt/docker-test-toolkit/bin/docker-test-toolkit /usr/local/bin/docker-test-toolkit
docker-test-toolkit readiness inspect --json
```

Delete the downloaded file afterwards. Tell the user anything `readiness inspect` reports as not ready, and if the command is
not found, the install's JSON result names its path.

## 3. Keep the agent from changing Docker directly

Offer the [harness guard](adopters.md#harness-guard), a pre-tool-use hook that refuses shell commands changing Docker state
directly while reads still pass. **It changes the harness configuration, so ask the user first.** Never add
`# docker-direct-ok` to a command unless the user has explicitly agreed to that command.

## 4. Teach the agent the rules

Copy [`.agents/skills/docker-test-toolkit/SKILL.md`](../.agents/skills/docker-test-toolkit/SKILL.md) to where this harness
keeps skills (for example `.claude/skills/docker-test-toolkit/SKILL.md` in Claude Code), or tell the user if the harness has no
such place. **Ask the user whether it belongs in this project or in their user configuration.**

## 5. Connect the project

Read the [adopter guide](adopters.md) first, at least "Using the installed toolkit", "The permanent contract" and "Adapter
boundary". Then:

1. Find the project's Docker-backed tests. Tests that need no Docker stay as they are and never open a run.
2. Show the user what you found: the test groups, services and commands, and the memory, CPU and thread budget you propose
   for each phase. **Wait for their agreement.** Never guess commands, readiness checks or migrations from the framework or
   project name.
3. In a Python project, copy `docs/loader/docker_test_toolkit_loader.py` into the project unchanged. Any other project runs the
   `docker-test-toolkit` command.
4. Add one small adapter that opens a run and sends the project's Docker commands through it. Keep the product's commands,
   services and credentials in the project. In Python, with the loader file on `sys.path`:

   ```python
   spec = toolkit.RunSpec(
       project_root=Path(__file__).resolve().parent,
       entrypoint="integration",  # short names you choose for this kind of run
       purpose="integration-tests",
       profile="default",
       services=("cache",),  # the services these tests need
       closure=("cache",),  # those services plus everything they depend on
       phases={"tests": toolkit.PhaseBudget(container_bytes=gib, host_process_bytes=gib, daemon_cpus=1, host_threads=2)},
       initial_phase="tests",
   )
   run = toolkit.open_run(spec).enter()  # waits until the machine has room
   outcome = "failed"
   try:
       docker = run.docker_context

       def in_run(*arguments):
           return subprocess.run(
               [str(docker.executable), *arguments], env=docker.environment, check=True, text=True, capture_output=True
           )

       in_run("pull", image)
       in_run("compose", "up", "--detach", "--wait", "--wait-timeout", "60")
       service = json.loads(in_run("compose", "ps", "--format", "json").stdout.splitlines()[0])
       port = next(item["PublishedPort"] for item in service["Publishers"] if item["TargetPort"] == 6379)
       # The tests run as an ordinary process on this machine: pass them the address.
       tests = subprocess.run(["pytest", "-q"], env={**os.environ, "CACHE_HOST": "127.0.0.1", "CACHE_PORT": str(port)})
       if tests.returncode == 0:
           outcome = "passed"
   finally:
       run.leave(outcome)
   ```

   The toolkit enforces these rules, so follow them from the start:

   - Pull images by digest (`name@sha256:...`) or build them through the run, and use those same images in Compose. Find a
     digest with `docker buildx imagetools inspect <image>`.
   - Name every volume in Compose: an anonymous one is refused.
   - Give every service `mem_limit` and `cpus` within the phase budget. Without a hard memory limit the run still works, but it
     cannot share the daemon with other runs.
   - Do not pass `-p` to Compose, since the run chooses the project.
   - Read published ports with `compose ps --format json`. `compose port` is refused.

## 6. Prove it

Run one Docker-backed test selection through the adapter, then a cleanup dry run. Its input is one JSON object holding the
project's absolute path:

```sh
input=$(mktemp)
printf '{"project_root": "%s"}' "$PWD" > "$input"
docker-test-toolkit cleanup project --input "$input" --json
rm "$input"
```

Report the test result and anything the dry run lists. Never add `--execute` without the user's agreement.

Never use `docker system prune` or remove resources by name to clean up. If a run is stuck, follow
[Troubleshooting](operations.md#troubleshooting).
