# Agent setup

Instructions for a coding agent setting up docker-test-toolkit for its user. People can follow them too. Report what you
found after each step, and stop and ask whenever a step says so.

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
inside a virtual environment. On Linux the installation lives in `/opt` and needs root, so ask the user to run it or to allow
`sudo`, and link the command afterwards with
`sudo ln -sf /opt/docker-test-toolkit/bin/docker-test-toolkit /usr/local/bin/docker-test-toolkit`:

```sh
python3.12 -I docker-test-toolkit.pyz install --json
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
   run = toolkit.open_run(spec).enter()  # waits until the machine has room
   docker = run.docker_context
   subprocess.run([str(docker.executable), "pull", image], env=docker.environment, check=True)
   subprocess.run(
       [str(docker.executable), "compose", "up", "--detach", "--wait", "--wait-timeout", "60"],
       env=docker.environment,
       check=True,
   )
   ```

   The toolkit enforces these rules, so follow them from the start: pull images by digest (`name@sha256:...`) or build them
   through the run, use those same images in Compose, give every service `mem_limit` and `cpus` within the phase budget, and do
   not pass `-p` to Compose, since the run chooses the project. End with `run.leave("passed")` or
   `run.leave("failed")` in a `finally`. The [README](../README.md#use) has the full example.

## 6. Prove it

Run one Docker-backed test selection through the adapter, then a cleanup dry run. Its input is one JSON object holding the
project's absolute path:

```sh
printf '{"project_root": "%s"}' "$PWD" > "$TMPDIR/project.json"
docker-test-toolkit cleanup project --input "$TMPDIR/project.json" --json
```

Report the test result and anything the dry run lists. Never add `--execute` without the user's agreement.

Never use `docker system prune` or remove resources by name to clean up. If a run is stuck, follow
[Troubleshooting](operations.md#troubleshooting).
