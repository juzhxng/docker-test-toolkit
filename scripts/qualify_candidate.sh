#!/usr/bin/env bash
# Qualify a release candidate on the CI host: verify the kit, prove that the bundle's main is the candidate
# commit and rebuilds to the candidate's bytes, then install the candidate and run the whole e2e suite against it,
# nothing deselected, writing qualification-<version>.txt into the kit.
#
# Usage: qualify_candidate.sh <kit-dir> <work-dir> <candidate-commit> [--verify-only]
#   The kit holds docker-test-toolkit.pyz (the candidate), toolkit.bundle (git bundle of main and the tags; main is the
#   candidate commit) and SHA256SUMS listing both. Run it inside a runner container on the CI host, with no other CI
#   running on the host. <work-dir> must be on ext-family storage in the runner account's
#   work area. --verify-only stops after the kit check.
# Exit codes: 2 usage, 3 kit mismatch (nothing installed), 4 install failed, 5 checkout, commit, sync or rebuild check
#   failed (nothing installed), 6 the e2e suite failed (the report is written either way).
set -euo pipefail

usage() { echo "usage: $0 <kit-dir> <work-dir> <candidate-commit> [--verify-only]" >&2; exit 2; }
[ $# -ge 3 ] || usage
kit=$(cd "$1" && pwd)
work=$2
commit=$3
verify_only=${4:-}
[ -z "$verify_only" ] || [ "$verify_only" = "--verify-only" ] || usage

sha256() { if command -v sha256sum >/dev/null 2>&1; then sha256sum "$@"; else shasum -a 256 "$@"; fi; }
for name in docker-test-toolkit.pyz toolkit.bundle; do
  grep -q "  $name\$" "$kit/SHA256SUMS" || { echo "SHA256SUMS: mismatch, $name is not listed; nothing was installed" >&2; exit 3; }
done
if ! (cd "$kit" && sha256 -c SHA256SUMS); then
  echo "SHA256SUMS: mismatch in $kit; nothing was installed" >&2
  exit 3
fi
[ "$verify_only" = "--verify-only" ] && exit 0

fail() { echo "$1; nothing was installed" >&2; exit 5; }
mkdir -p "$work"
clone="$work/qualify"
rm -rf "$clone"
git clone --quiet --branch main "$kit/toolkit.bundle" "$clone" || fail "clone of the bundle's main failed"
git -C "$clone" fetch --quiet "$kit/toolkit.bundle" 'refs/tags/*:refs/tags/*' || fail "tags of the bundle missing"
head=$(git -C "$clone" rev-parse HEAD)
[ "$head" = "$(git -C "$clone" rev-parse "$commit^{commit}" 2>/dev/null)" ] \
  || fail "the bundle's main is $head, not the candidate commit $commit"
(cd "$clone" && uv sync --all-extras --quiet) || fail "uv sync failed"
built=$(cd "$clone" && uv run python scripts/build_zipapp.py build --repository . --output-directory "$work/rebuild") \
  || fail "rebuild of the candidate commit failed"
expected=$(cd "$kit" && sha256 docker-test-toolkit.pyz | cut -d' ' -f1)
printf '%s' "$built" | grep -q "$expected" || fail "the candidate commit does not rebuild to the candidate's bytes"

install_json=$(python3.12 -I "$kit/docker-test-toolkit.pyz" install --json) || { echo "install failed" >&2; exit 4; }
read -r version root < <(printf '%s' "$install_json" \
  | python3.12 -c 'import json,sys; r=json.load(sys.stdin)["result"]; print(r["version"], r["root"])')
archive="$root/$version/docker-test-toolkit.pyz"

report="$kit/qualification-$version.txt"
status=0
(
  cd "$clone"
  DOCKER_TEST_TOOLKIT_E2E_ARTIFACT="$archive" uv run pytest --basetemp="$clone/.pytest-tmp/qualify" -m e2e tests/e2e \
    -v --durations=0 --durations-min=0 -ra --no-fold-skipped
) >"$work/qualification.log" 2>&1 || status=$?
{
  echo "candidate: $archive (commit $commit)"
  sha256 "$archive"
  echo "pytest exit: $status"
  tail -n 40 "$work/qualification.log"
  echo "readiness:"
  docker-test-toolkit readiness inspect --json || true
} >"$report"
echo "qualification report: $report (pytest exit $status)"
[ "$status" -eq 0 ] || exit 6
