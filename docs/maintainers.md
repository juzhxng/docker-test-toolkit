# Maintainers

How releases of the toolkit are published. Adopters do not need this.

## Releasing

A release never waits for the adopting repositories. It is gated by the toolkit's own suites (`poe qa`, the frozen
contract corpus, the cross-version tests against the previous release, the complete e2e, green in the Linux CI on the
release commit before the tag), built twice to one hash, installed as a candidate, and proven by one real adopter CI run
on a self-hosted runner stack (the canary); then it is tagged and published with those same bytes. The adopters'
ordinary CI is the field monitor: a bug found there is fixed in the toolkit, and the machines go back to the previous
installed version (`install` of it) until the fix is released. Adopters change nothing for a release.

The runbook, in order:

1. On a developer machine, on the release commit of `main` (version bumped, CI green): build twice and compare,
   `uv run python scripts/build_zipapp.py build --repository . --output-directory dist/a` and `dist/b`; the common
   SHA-256 is the candidate hash.
2. Install the candidate there (`python3.12 -I dist/a/docker-test-toolkit.pyz install --json`).
3. Install it on the host that runs the adopter's CI, and run one adopter CI run on it (the canary); record its run id.
4. Tag the commit (`git tag -a vX.Y.Z -m "docker-test-toolkit X.Y.Z"`) and push the tag.
5. Publish: `gh workflow run publish.yml -f tag=vX.Y.Z -f candidate_sha256=<hash> -f canary=<run id>`. `scripts/publish_release.py` refuses unless the tag is on `main`, the commit's CI run passed
   every required job and the rebuilt archive equals the candidate; it publishes through a draft.
6. `gh release download vX.Y.Z -p docker-test-toolkit.pyz` and compare the hash with the candidate's.

CI runs on GitHub-hosted arm64 and amd64 runners for every push to `main` and every pull request, with nothing deselected
(`.github/workflows/ci.yml`: `scan`, and `quality`, `e2e` and `shared-runs` on both); the job logs are the record (`gh run view <id> --log`).
