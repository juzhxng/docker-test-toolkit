---
name: docker-test-toolkit
description: Use when onboarding, running, queueing, recovering, cleaning, installing, upgrading, or troubleshooting Docker-backed integration, E2E, or audit work managed by Docker Test Toolkit.
---

# Docker Test Toolkit

Paths below are relative to the root of the [docker-test-toolkit repository](https://github.com/juzhxng/docker-test-toolkit).

Use the installed toolkit and the adopter repository's adapter as the authorities. Keep product commands,
service topology, readiness, migrations, credentials, and success semantics repository-owned. Do not infer them from a
framework or project name.

## Coordinator contract

- Submit independent ordinary work once and retain the same queued process or handle.
- Repair only your own project's exact state.
- Foreign active or cleanup-pending work is never a reason to wait before submission.
- A dead owner's run is recovered by the toolkit's detached worker; run recover on a foreign run is an operator action.

## Choose the operation

- **Installing, upgrading, rolling back, or onboarding a project:** the toolkit is installed once per machine and
  projects hold no copy. Read `docs/operations.md` (`#installation`, `#upgrade-and-rollback`, `#ci-runners`) and `docs/adopters.md` (`#using-the-installed-toolkit`, `#the-permanent-contract`). Python projects copy
  the reference loader verbatim; other projects run the `docker-test-toolkit` command. Installing on a machine needs its
  user's go. A `toolkit-upgraded` refusal means: restart the test process.
- **Ordinary tests, E2E, and audits:** read
  `docs/adopters.md` and
  `docs/operations.md`. Classify host-only work before opening Docker state.
  Submit a Docker-backed selection once through the repository adapter; the toolkit's shared FIFO queues it across
  projects. Retain the same process and run handle while queued or admitting. Do not poll another project's records for
  an idle window, cancel and reopen, start a duplicate, or defer submission with a heartbeat or timer. Use an outer
  timeout longer than the toolkit queue deadline plus bounded startup and cleanup.
- **Running independent selections at the same time:** when `readiness inspect` reports `parallel_runs` other than
  `1` (an integer above 1, or `auto`), independent Docker-backed runs may be started concurrently as separate
  processes: a repository's integration and E2E selections, or several repositories' gates. Start each once (for
  example as its own background command) and keep each handle; the toolkit decides per run whether it joins the runs
  already holding or waits its turn, so never pre-check capacity or hold one back yourself. Do not start them together
  when they write the same files in the checkout (coverage data, reports), and keep host-side test servers on port 0.
  At `parallel_runs` 1 concurrent submissions are still safe but run one after another, so each one's outer timeout
  must also cover the full runs queued ahead of it; when in doubt there, start them in sequence. Joining is also
  gated by the install's share scope (`--share-scope repository`: only runs of one Git repository share), a turn for the
  holding repository and CPU pressure (on macOS read inside the Docker VM); share verdicts carry across checkouts of one
  repository. None of this needs anything from you: see `docs/operations.md` (`#shared-runs`).
- **Status, recovery, retention, and cleanup:** read
  `docs/operations.md`. If this project already has indexed nonterminal,
  retained, or cleanup-pending authority, inspect and repair that exact run before opening another. Use exact
  `run status`, `run recover`, `run release`, and repository-owned resume flows. For cleanup, perform project or session
  dry-run, execute, then final dry-run. Never globally prune, infer ownership from names, or mutate a foreign project.
- **Readiness and diagnostics:** use `readiness inspect` (protocol 4: also the daemon's `runtime`, `docker-desktop`, `colima` or `podman` on a Mac, the CPU pressure,
  the daemon probe's latest reading and the free disk and reserve of each place), `run status`, or `resource observe` as described in `docs/operations.md`. Treat structured outcomes and
  recovery actions as authoritative; do not classify prose or raw Docker inventory.
- **Adapter capabilities:** read `docs/adopters.md` for host-only bypass, finite batches and boundaries, consumers, borrowed Docker access, endpoints, and bounded container
  control. Ordinary work uses the repository adapter; do not manually orchestrate the private bridge or lease protocol.
- **Sharing one run:** one run serves every Docker item of a segment (batch protocol 2), and segments are the only
  separation tool. Before laying out segments, read `docs/adopters.md` (`#sharing-one-run-safely`): co-runnable
  segments, budget from the union, settings scoped per test, destructive tests marked and in segments after the ones they could disturb, and the
  fit on the smallest supported machine.
- **Build cache:** the shim caches every loaded `buildx build` per project, on by default and fail-open; adopters
  declare nothing. Order Dockerfile steps lock files first, pass no cache flags, pass the shim environment so the
  per-run off switch `DOCKER_TEST_TOOLKIT_BUILD_CACHE=off` arrives, use `project timeline` for hits and misses and
  `cache remove` for cold evidence, and never bake credentials into layers or build arguments. See
  `docs/operations.md` (`#project-build-cache`).

- **Command bounds:** each shim command runs within a fixed bound: `run`, `compose run`, `compose exec` and
  `container exec` up to 14,400 s; builds, archive exports, image pulls and `compose cp` 1,200 s; `compose up` 30 s plus
  its `--wait-timeout`; everything else 30 s. A long test file belongs in one of the 14,400 s commands. See
  `docs/operations.md` (`#lifecycle-and-cleanup`).

Use `protocol --json` to discover the exact supported protocol map. Do not treat internal APIs or undocumented command
shapes as supported features.
