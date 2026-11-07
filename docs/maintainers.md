# Maintainers

How releases of the toolkit are qualified and published. Adopters do not need this.

## Qualification on a CI host

Before a release is tagged, its candidate runs the whole e2e suite on the CI host, nothing deselected: this is where
the scenarios that do not fit a hosted runner (the 6 GiB host-headroom decline) and the CI host's own daemon topology
are exercised. Copy a kit to the runners' work area: the candidate `docker-test-toolkit.pyz`,
`toolkit.bundle` (`git bundle create toolkit.bundle main --tags`, `main` at the candidate commit) and `SHA256SUMS`.
Inside a runner, with no other CI running on the host:

```sh
scripts/qualify_candidate.sh <kit-dir> <ext4 work dir> <candidate commit>
```

The script checks that `SHA256SUMS` lists both kit files and matches them, that the bundle's `main` is the candidate
commit and that the commit rebuilds to the candidate's bytes, all before it installs anything; then it installs the
candidate, runs `pytest -m e2e tests/e2e` against the installed archive and writes `qualification-<version>.txt`
(summary, hash, readiness) into the kit. Exit codes: 3 kit mismatch, 4 install, 5 checkout, commit, sync or rebuild
(nothing installed), 6 failing suite.

## Releasing

A release never waits for the adopting repositories. It is gated by the toolkit's own suites (`poe qa`, the frozen
contract corpus, the cross-version tests against the previous release, the complete e2e, green in the Linux CI on the
release commit before the tag), built twice to one hash, installed as a candidate on a developer machine and the CI host, and proven
there by one real adopter CI run (the canary); then it is tagged and published with those same bytes. The adopters'
ordinary CI is the field monitor: a bug found there is fixed in the toolkit, and the machines go back to the previous
installed version (`install` of it) until the fix is released. Adopters change nothing for a release.

The runbook, in order:

1. On a developer machine, on the release commit of `main` (version bumped, CI green): build twice and compare,
   `uv run python scripts/build_zipapp.py build --repository . --output-directory dist/a` and `dist/b`; the common
   SHA-256 is the candidate hash.
2. Install the candidate there (`python3.12 -I dist/a/docker-test-toolkit.pyz install --json`).
3. Qualify it on the CI host (["Qualification on a CI host"](#qualification-on-a-ci-host)), which also installs it
   there.
4. The canary: one adopter CI run on the installed candidate; record its run id.
5. Tag the commit (`git tag -a vX.Y.Z -m "docker-test-toolkit X.Y.Z"`) and push the tag.
6. Publish: `gh workflow run publish.yml -f tag=vX.Y.Z -f candidate_sha256=<hash> -f qualification=<reference>
   -f canary=<run id>`. `scripts/publish_release.py` refuses unless the tag is on `main`, the commit's CI run passed
   every required job and the rebuilt archive equals the candidate; it publishes through a draft.
7. `gh release download vX.Y.Z -p docker-test-toolkit.pyz` and compare the hash with the candidate's.

CI runs on GitHub-hosted arm64 runners for every push to `main` and every pull request (`.github/workflows/ci.yml`:
`scan`, `quality`, `e2e`, `shared-runs`); the job logs are the record (`gh run view <id> --log`).
