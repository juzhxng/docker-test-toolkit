# CI guide

## How it works on CI

CI uses the same toolkit, the same adapter and the same commands as a developer machine: your job runs its tests, the run
waits until the machine has room, and everything the run created is removed at the end.

What differs is what shares the machine:

- **GitHub-hosted runner:** every job gets a fresh machine, so install the toolkit in the job. Nothing else competes for that
  machine, but capacity checks, exact cleanup and recovery still apply, and your tests run as they do locally.
- **Self-hosted runners sharing one Docker daemon:** one installation serves them all, so their jobs queue for capacity instead
  of colliding, across repositories. A job killed by the runner is recovered by the next run or cleanup.

## GitHub-hosted runners

Add these steps before your tests, in each job that uses Docker. They work on `ubuntu-24.04` and `ubuntu-24.04-arm`. Pin a release
and its SHA-256 from the [release notes](https://github.com/juzhxng/docker-test-toolkit/releases), so that CI changes only when
you change it:

```yaml
- name: Install docker-test-toolkit
  env:
    VERSION: v0.7.5
    SHA256: 64204e16c47373988bbcaa2ef5fa8a84c2791d8a1c70d8d8a097c4c15652893a
  run: |
    curl -fsSLO "https://github.com/juzhxng/docker-test-toolkit/releases/download/$VERSION/docker-test-toolkit.pyz"
    echo "$SHA256  docker-test-toolkit.pyz" | sha256sum -c -
    sudo install -d -o "$(id -un)" -g "$(id -gn)" -m 755 /opt/docker-test-toolkit
    python3 -I docker-test-toolkit.pyz install --json
    sudo ln -sf /opt/docker-test-toolkit/bin/docker-test-toolkit /usr/local/bin/docker-test-toolkit
    sudo chmod go-w /usr/local/bin
    docker-test-toolkit readiness inspect --json
```

`chmod` is needed because the image leaves `/usr/local/bin` writable by everyone, which the toolkit refuses on its safe `PATH`.

If your tests export images to archives, the daemon also needs the containerd image store, which needs a restart:

```yaml
- name: Use the containerd image store
  run: |
    { sudo cat /etc/docker/daemon.json 2>/dev/null || echo '{}'; } | jq '.features["containerd-snapshotter"]=true' > "$RUNNER_TEMP/daemon.json"
    sudo cp "$RUNNER_TEMP/daemon.json" /etc/docker/daemon.json
    sudo systemctl restart docker
```

## Self-hosted runners

Install once for the machine, not per job. Runners that share a Docker daemon and PID namespace must share one installation and
one user. A run's temporary folder must be at the same path in the runners and in the daemon. A single-purpose host can lower its
reserve with `install --host-reserve-bytes`. [Operations](operations.md#ci-runners) lists everything the runner stack provides.

Never use self-hosted runners on a public repository: they would run code from anyone's pull request.

## Checking it worked

The job's log shows the install's JSON result and the `readiness inspect` result. To see what a project's runs left behind, run a
cleanup dry run, as in the [agent setup](agent-setup.md#6-prove-it).
