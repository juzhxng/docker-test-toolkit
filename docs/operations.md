# Operations

## Command boundary

The toolkit is installed once per machine; projects hold no copy of it. Python projects load it through their copy of
the reference loader (see [Adopters](adopters.md#using-the-installed-toolkit)); everything else uses the command
`docker-test-toolkit`. Every command emits one stable JSON document; no caller classifies prose. From a source tree the
toolkit runs only its private test composition: anything that needs the real account refuses with
`toolkit-not-installed`.

The command families are `protocol`, `readiness`, `run`, `cleanup`, `project`, `cache` and `resource`, plus the serial
`bridge lease` and `bridge batch` for Node adapters and `install`. Use the repository adapter for ordinary test runs
rather than managing `run` commands by hand.

## Installation

| Item | macOS | Linux |
|---|---|---|
| Root | `~/.local/share/docker-test-toolkit` (the passwd home) | `/opt/docker-test-toolkit` |
| Version folder | `<root>/<version>/docker-test-toolkit.pyz`, file 0444, folder 0555 | same |
| Active version | `<root>/current`, a symlink to `<version>`, replaced atomically | same |
| Launcher | `<root>/bin/docker-test-toolkit` | same |
| Command path | `~/.local/bin/docker-test-toolkit`, made by `install` | `/usr/local/bin/docker-test-toolkit`, made by `install` when that folder is writable |
| Registry and account | `<root>/state/` and `<root>/state/test-admission/` | same |

Download a release from [GitHub Releases](https://github.com/juzhxng/docker-test-toolkit/releases) and compare its hash with
the table in the release notes:

```sh
curl -fsSLO https://github.com/juzhxng/docker-test-toolkit/releases/latest/download/docker-test-toolkit.pyz
shasum -a 256 docker-test-toolkit.pyz   # sha256sum on Linux
```

On Linux the installation belongs to the user who runs the tests, so create its folder first, and link the command
afterwards if that user cannot write `/usr/local/bin`:

```sh
sudo install -d -o "$(id -un)" -g "$(id -gn)" -m 755 /opt/docker-test-toolkit
python3 -I docker-test-toolkit.pyz install --json
sudo ln -sf /opt/docker-test-toolkit/bin/docker-test-toolkit /usr/local/bin/docker-test-toolkit
```

A root install leaves any other user locked out of the account's state.

The toolkit looks for `docker` only in a fixed list of folders (`~/.local/bin`, `/opt/homebrew/bin`, `/usr/local/bin`
and the system folders), never the caller's `PATH`. Docker Desktop's per-user `~/.docker/bin` is not among them: on a
Mac where Desktop installed its command-line tools for the user, choose System under Settings → Advanced, or the
install reports `probe_image: daemon-unavailable` and every run fails to find Docker.

Install a release archive with a CPython 3.12 or newer that is not inside a virtual environment:

```sh
python3.12 -I docker-test-toolkit.pyz install --json
python3.12 -I docker-test-toolkit.pyz install --host-reserve-bytes 1073741824 --json   # a single-purpose CI host
python3.12 -I docker-test-toolkit.pyz install --parallel-runs 1 --json                   # one run at a time
```

The installer writes only its platform's root. Under `<root>/install.lock` it places the version folder (a folder an
earlier install left without its archive is refused, never repaired), writes the launcher with the interpreter it ran
under (or `--python`), checks the account's state format and switches `current` under the account's metadata lock. It
is idempotent: rerun it after a crash. The JSON result names the previous and the new version, the root, the command
path and `holder_versions`, the versions of runs that still hold the slot. `--host-reserve-bytes` (at least 1 GiB)
replaces the recorded host reserve; without it the reserve stays as recorded, 4.5 GiB by default.
`--disk-reserve-bytes` (at least 1 GiB) replaces the recorded disk reserve of every place (see
[Disk admission](#disk-admission)); without it the reserve stays as recorded, derived per place by default.
`--parallel-runs N|auto` records how many runs may hold the account at once (`parallel-runs.json`); without it the
recorded limit stays, `auto` by default. `auto` sets no count: only the capacity fit below decides. `1` keeps the
one-run-at-a-time behaviour of releases before 0.6.0 exactly. See [Shared runs](#shared-runs). Unknown entries in the
root are left alone. `--root` exists for the toolkit's own tests. After the settings, `install` makes the daemon
probe's image present (`probe_image` in the JSON: `present`, `pulled`, `busy`, `backoff`, `failed` or
`daemon-unavailable`; never an install failure). See [CPU pressure gates joins](#shared-runs). Then it runs one
forced [retention](#retention) pass; `retention` in the JSON is its report, or `null` if the pass failed.

The launcher resolves `current` once and `exec`s the recorded interpreter on that version, so a process runs one version
for its whole life and keeps its PID. If the recorded interpreter is gone it exits 5 with "reinstall the toolkit".

## Upgrade and rollback

- **Upgrade** is installing the newer archive. Runs that hold or wait for the slot keep their version. A process that
  loaded the older version and tries to start a new run afterwards (prepare, enter, a queued pass, publication or
  resume) is refused with `incompatible_state`, reason `toolkit-upgraded` ("restart this test process"); its
  preparation is cleaned up and leaves nothing behind. New runs of the new version wait in the ordinary queue until the
  older holders leave. A retained run may be resumed by the new version, and the older version may still release its
  own retained runs.
- **Rollback** is installing the older archive again; its version folder is reused when the bytes are identical, and
  different bytes for an installed version are refused. Processes of the newer version then refuse the same way.
- **State format.** The account anchor `state/test-admission/metadata.lock` carries `docker-test-toolkit-state:<F>`.
  Code refuses an account written by a newer format (`state-format-newer`) at every entry, including `install`, and
  writes nothing, so there is no rollback across formats.
- **Old version folders** may be removed once no process runs them and no run record of the account names that
  version as holding or waiting for the slot (`run status`). Never remove the folder `current` points to.

## CI runners

The [CI guide](ci.md) explains how the toolkit works on CI. This is the detail of a self-hosted stack.

Runners on a shared CI host that share one Docker daemon and one PID namespace share one account. The runner stack provides:

| Item | Stack setting |
|---|---|
| Installation and account | a host volume mounted read-write at `/opt/docker-test-toolkit` in every runner |
| Interpreter | a standalone CPython 3.12 or newer inside the volume, recorded by `install --python`, and `python3.12` on the runners' `PATH` for helpers that need it |
| Command | `/usr/local/bin/docker-test-toolkit`, a symlink to `/opt/docker-test-toolkit/bin/docker-test-toolkit` |
| Docker configuration | a host-provided `/root/.docker` whose current context reaches the daemon, with a Buildx plugin new enough for the toolkit |
| Run folders | `TMPDIR` set to one shared directory under a host path such as `/srv/ci/`, mounted at the same path in every runner and in the daemon, so Compose bind mounts from a run's private root resolve and a recoverer in another runner can remove it |
| Host reserve | `install --host-reserve-bytes 1073741824`, once |
| PID 1 that reaps orphans | `init: true`, an init such as `dumb-init`, or a shared PID namespace with one: the toolkit never counts a zombie as dead, so under a PID 1 that does not reap, a killed run is never recovered |

For a GitHub-hosted runner, see the [CI guide](ci.md).

Installing a release on a machine and the parallel-runs limit (`install --parallel-runs`) on a shared CI account are
decisions for whoever runs that machine.

## Lifecycle and cleanup

Repository adapters classify host-only versus Docker-backed selection before opening a run. Docker-backed selection
prepares durable state, enters shared FIFO admission, runs product setup through the private shim, registers consumers,
and leaves exactly once. Cleanup uses only recorded identities and receipts. It never globally prunes and never infers
ownership from names or prefixes.

Each shim command runs within a fixed bound. Commands that run a caller process (`run`, `compose run`, `compose exec`,
`container exec`) may take up to 14,400 s; builds, archive exports and image pulls 1,200 s; `compose cp` 1,200 s; `compose up` and
`compose start` 30 s plus their `--wait-timeout`; every other command 30 s. A command that exceeds its bound is stopped and the run fails.

The private shim classifies every Docker option the way the Docker CLI reads it, including attached short forms
(`-ovalue`, `-tvalue`, `-pvalue`) and clusters such as `-qovalue`. `buildx build` refuses `--output`/`-o` (except the
exact archive pair of [Archive exports](#archive-exports)),
`--iidfile`, `--metadata-file`, `--push`, `--cache-from`, `--cache-to`, `--builder`, `--resource`, `--sbom`, `--attest`, `--load=<value>`
and any `--provenance` other than `--provenance=false`, and records every tag spelling for the tag-collision check.
`compose` refuses a caller project name (`--project-name`, `-p`) before and after the subcommand for every subcommand;
for `exec`, options end at the service name and the executed command is never read as Compose options. Every short option
cluster after a Compose subcommand is read letter by letter, so one that contains a project letter is refused, as in
`up -fcompose.yml`; pass such a value as a separate argument (`up -f compose.yml`). Compose's persistent options after
any subcommand other than `run` and `exec` (`--file`, `--env-file`, `--profile`, `--project-directory`; `-f` except
after `rm` and `logs`, where it means `--force` and `--follow`) are rendered and checked exactly as Compose applies
them, as in `down --file=other.yml`; where `-f` names a file, a shared run refuses a short cluster that hides it and
a `-f` without a value.
Earlier versions accepted these spellings, so a caller that relied on them must stop using them.

### Run folders

Each run gets a private folder `$TMPDIR/docker-test-toolkit/<session>/<project>/<run>`. Whenever the toolkit removes a
run's folder (leave, cancel, release, recovery, revocation or takeover), it also removes the run's project folder and
then its session folder while they are empty, never anything else; the shared `docker-test-toolkit` folder stays.

Earlier versions left the two empty folders behind, and older installed versions keep doing so until upgraded. To remove
those leftovers on a machine, run this once for each temporary parent `T` (`$TMPDIR` on a developer machine, each
runner's `TMPDIR` on a CI host):

```bash
/usr/bin/find "$T/docker-test-toolkit" -mindepth 1 -maxdepth 1 -type d -mmin +1440 -exec /usr/bin/find {} -maxdepth 1 -type d -empty -delete \;
```

It selects only session folders untouched for a day and removes, inside them, only empty project folders and then the
session folder if that leaves it empty. It never enters a run folder, so a run that is still preparing or retained
keeps everything it has. Empty folders that are not old enough stay for a later pass.

### Archive exports

One shim command writes a verified archive into the run's private root instead of the Docker daemon. It prints one
canonical JSON line on success (`schema`, `kind`, `operation_id`, `project_id`, `session_id`, `run_id`, `path`, `bytes`,
`sha256`, `layout`, `format`, `root_digest`, `manifest_digest`, `config_digest`, `platform` and `image_id`). The receipt
(`plan["archive"]`) holds the same facts.

- `docker image save <sha256:64 lowercase hex image ID>` exports one image the run owns, has promoted and still holds.
  Refused before any file or receipt: every option, the `save` alias, tags and references, more than one ID, a foreign,
  external, undeclared or removed image. An image is exported once; a repeat is refused with reason `archive-exists`
  and the recorded path, SHA-256 and byte count, and a retry after a failed export is allowed. On Docker only the
  containerd image store is supported: a daemon whose `image inspect` has no `Descriptor`, or an inconsistent one, is
  refused with reason `unsupported-image-store` (the classic image store is a deferred ceiling). On Podman the image
  ID is the config digest and `image save` writes the Docker archive format (uncompressed layers named by their
  SHA-256, `manifest.json`, legacy `<id>/layer.tar` links): it is verified through its own grammar and recorded as
  layout `docker-archive`, its root and config digests both the image ID. Ownership of the image does not change.

The dispatcher exports into a fresh private directory below `<private root>/exports/`, verifies the archive through a
held descriptor without extracting or decompressing it (tar headers, content addresses, descriptor graph, platform and
the image identity), links it under the run lock without replacing anything, and records success last. The published
file is `<operation id>.tar` with mode 0600. An exit code is never evidence that the archive
was published, and an interrupted export is never promoted. One archive is capped at 4 GiB (a `ponytail:` ceiling): free
space and, for an image, its inspected size are checked before the export (`export-storage`) and the size again after
it, so a client can overshoot the cap until it ends and is then rejected.

A failed export finishes the receipt `failed` with a closed class in `plan["archive"].failure`: `not-started`,
`child-failed`, `absent`, `unexpected-entry`, `invalid-output`, `over-cap`, `verification-failed`, `metadata-missing`,
`publish-collision`, `publish-substituted`, `deadline`, `io-error` or `cleanup-incomplete`. `record-changed` reports that
a reconciler finished the receipt first. When the dispatcher's own cleanup also fails, the primary error keeps its exit code and a second stderr
line names the cleanup error.

Recovery, the `stopping` path and operator recovery classify an interrupted export from the disk alone and record it
`failed` with `recovered-absent`, `recovered-partial`, `recovered-complete-unpublished` or `archive-record-invalid`
before they delete anything; deletion is a durable obligation (`cleanup: deferred`) that a later pass completes. They
never promote. While a dispatcher is alive, a reconciler that fails its receipt makes the dispatcher report
`record-changed`. If a dispatcher is killed with SIGKILL, its Docker client or Buildx process may outlive it and write
into a directory the reconciler already removed (residual R1); the guarantee that no descendant keeps running holds only
while the dispatcher is alive. Copy the archive out of the private root before `leave` (see the adopter guide).

Use cleanup dry-run first, then the explicit execute flag:

```sh
docker-test-toolkit cleanup session --input session.json --json
docker-test-toolkit cleanup session --input session.json --execute --json
docker-test-toolkit cleanup project --input project.json --json
docker-test-toolkit cleanup project --input project.json --execute --json
```

`session.json` contains exactly `project_root` and `session_id`; `project.json` contains exactly `project_root`. Session
cleanup refuses while that session still has an indexed run and never selects another session's resources. Project
cleanup is the ordinary completion and crash-recovery command because it reconciles every recorded session.

Cleanup failure remains separate from the primary test result. Retained resources remain recorded and require exact
resume or release. A crash is recovered from durable project/account records, not a current-shell inventory.

### Admission across projects

Independent projects share one account queue, served first in, first out. At the default limit, `auto`, runs that fit
hold together and the fit check alone decides (see [Shared runs](#shared-runs)); `install --parallel-runs 1` restores one
run at a time. A coordinator submits independent ordinary work once and retains the same queued process or handle. Another project's active, queued,
admitting or cleanup-pending run is never a reason to delay the submission, to cancel and reopen the run, or to poll
foreign project records for an idle window; the queued run is authoritative progress. Repair is limited to your own
project's exact state: when the project already has an indexed nonterminal, retained or cleanup-pending run, inspect and
repair that exact run (`run status`, `run recover`, resume, release, project cleanup) before opening another, then submit
the next run immediately. Never recover, release or clean another project's run to make room.

After it reserves the slot, a run grants only once host memory has been watched for a complete 30-second window: the
headroom at a fresh sample must be at least 1.5 GiB and the swap-out rate over the window at most 128 MiB/s. A run that
waited in the queue samples host memory on every queued pass, and those samples count toward the window. A run that
queued for at least 30 seconds is therefore usually granted within one pass of reserving, instead of about 31 seconds
later; a run that never queued still waits the full window. The thresholds, the window length and the fresh check at the
grant are unchanged. What changes is that the window no longer lies entirely after the previous holder's cleanup; the
fresh headroom check at the grant still sees the host after it. Queued samples live only in the guardian's memory: a
restarted guardian, a failed queued read, or a sample whose clock or swap counter does not continue the later samples
falls back to a full window after the reservation. `project timeline` shows the effect: `window_s` (slot reserved to
grant) is then about 1–2 seconds, and the recorded window start precedes the reservation.

### Shared runs

At the default limit, `auto`, or `install --parallel-runs N` above 1, runs that are safe to run beside each other hold
the account together. Nothing changes for adopters: they open runs as before.

- **Mode.** At reservation a run is *shared* when its request (project, entrypoint, profile, closure) has a positive
  verdict in the project's `share-verdicts/` store, written when an earlier run of that request finished without
  breaking a share rule. Otherwise it is *exclusive* and holds the account alone, as today. A new request is therefore
  exclusive once and shared afterwards. A shared run whose own phase budgets cannot fit even alone is admitted
  exclusive.
- **Order.** The queue stays strictly first in, first out: only its head may reserve. The head joins existing holders
  only when it and every holder are shared, the holders are below the limit and every queued run's toolkit version
  supports sharing. Shared runs hold `capacity.lock` shared, exclusive runs hold it exclusively, and a holder never
  converts.
- **Fit.** A joiner is admitted only when the field-wise maximum of its phase budgets (plus a 2 GiB builder reserve for a
  build-capable run) fits beside every holder's reservation, with the holders' own containers attributed to them, not
  counted as external load.
- **Waiting reasons** in `hold_timeline.waiting` and the expiry report: `queue-position`, `exclusive-holder` (a holder
  or the head is exclusive), `parallel-limit`, `older-version-entry`, the capacity reasons, `disk-capacity` (see
  [Disk admission](#disk-admission)), `repository-turn` (see Share scope), and, while holding,
  `image-lock` (a shared run waited for the image lock) and `shared-grants-unreadable`.
- **Share rules.** Before every Docker mutation of a shared run the toolkit checks the rendered command against the
  share rules (no fixed host ports, no host networking, shared namespaces or Docker's default bridge, no privileged or device access, no daemon
  socket, no writable bind of the checkout, hard memory limits, no `--scale` or watch, and the run's own budget). A
  violation records a negative verdict and refuses the command with `share-rule`; the message names the rule, says
  whether the verdict was recorded, asks for a rerun and ends with the reason. The next run of that request is
  exclusive while the verdict is kept. The store holds 256 keys per project and evicts positives before negatives, an
  earlier boot's entries before this boot's, then the oldest; a change of the share rules drops it. An exclusive run never fails because of a share rule; it only
  records the verdict.
- **Image lock.** `<account>/images.lock` serializes shared runs' builds, pulls, builder creation, cached-image binding
  and image removal; every Docker child launched under it inherits it, so it is released only when that child exits.
  Exclusive runs do not take it. Operator `cleanup project` and `cleanup session` take it without waiting, refuse
  with `image-lock-held` while it is busy, and at every limit keep an image a live or retained run still uses.
- **Images in use.** A shared run that leaves while another live or retained run still uses one of its images hands it
  to `<account>/pending-image-deletions.json` instead of deleting it; the last user deletes it when it leaves, after
  checking the image's identity, references and daemon. The file disappears once empty.

### Admission expiry report

A run that never reaches a grant is revoked when its original queue deadline passes, and the deadline is never extended by
the report below. Its `infrastructure_failure` stays empty. The run record carries `failure_evidence` (see
[Telemetry categories and failure evidence](#telemetry-categories-and-failure-evidence)) with `category`
`admission-expired` and `observer` `admission`:

- `waiting` lists every reason the admission waited on, with the first and last tick it held and the time it was waiting for
  it. The reasons are `queue-position` (an earlier ticket or a holder blocks the head), `capacity-lock-busy`,
  `lock-wait` (blocked on a lock until the budget ended), the capacity verdicts (`external-capacity`,
  `job-cgroup-external-capacity`), the pressure waits (`incomplete-pressure-window`, `native-headroom`, `swapout-rate`),
  `disk-capacity`, `repository-turn` and `observation-late`. One pass can hold several reasons, so the durations overlap and
  their sum can exceed the wall-clock wait; each one is at most the span from the first pass to the expiry.
- `reason` is the waiting reason that began first. `pressure` is the last host and daemon sample the admission took
  (headroom, swap used and total, swap-out rate over the window, daemon memory), or `null` when it took none, for
  example while it only waited behind an earlier ticket.
- A run whose guardian found the requester gone while the queue deadline had not passed, or that expired before its
  first pass, records no evidence. Once the guardian finds the deadline passed, the expiry keeps its evidence whether or
  not the requester is still there.

The requester's `Admission ended before grant` message names the longest waiting reasons. The guardian notices the
expiry only after its current pass, so after its own deadline the requester waits up to 5 seconds for the guardian's
write, reading the record every 100 ms, and reports the waits as soon as they are there. It stops at once when the
guardian is gone, and reads the record once more whenever it stops without them. If they are still not written it says
`admission diagnostics not yet persisted by the guardian`; the evidence then appears in the record when the guardian
finishes. A pass slowed by Docker reads under memory pressure can take longer than 5 seconds. The accounting is kept in
the guardian's memory: if the guardian dies, recovery records `guardian-lost` and the report is lost.

An observation that fails while a run waits is classified the same way as for an active run. A late read, or co-holder
records that stay unstable, is only another waiting reason: the next pass decides from fresh inputs. An unreadable
observation or a changed daemon revokes the run at once with `infrastructure_failure` and evidence of that category
(`telemetry-unreadable`, `daemon-identity-changed`), recorded in the state the run had (`queued` or `admitting`).

### Disk admission

A run is admitted only when the disk it will write has room for it beside the runs already holding the account.

- **Estimate.** The owned image sizes of the newest complete run of the same request (repository, entrypoint,
  profile, closure) plus a quarter; without one, 4 GiB. The estimate is the run's lease in `disk-leases.json` while it
  holds the account.
- **Places.** The daemon's data root (read by the daemon probe's `df`), Docker Desktop's VM disk folder on the Mac
  (`~/Library/Containers/com.docker.docker/Data/vms/0/data`; a moved disk image is not looked for) and the run's
  private-root folder (it only needs 1 GiB free). A place that cannot be measured, such as another machine's daemon's data root (a
  `tcp://` endpoint on the loopback, as a CI runner reaching its own host's daemon, is measured),
  is `unknown` and admits: a measurement gap never blocks a run.
- **Reserve.** Each place keeps `min(clamp(total / 32, 2 GiB, 8 GiB), total / 2)` free, or the reserve declared with
  `install --disk-reserve-bytes` (at least 1 GiB, at most half of each place).
- **Decision.** A run waits with `disk-capacity` while `free - reserve - the other holders' leases` is below its
  estimate (a holder without a lease, such as one of an older release, counts 4 GiB), and is refused with
  `disk-capacity` when its estimate exceeds `total - reserve`, which can never fit; the requester's error names the
  place, free, total, reserve and estimate bytes. Before such a refusal the only Docker command is the probe. A run that
  waits on disk with no holder runs one [retention](#retention) pass first.
- **Cleanup is never gated.** Releasing a retained run and recovery's cleanup ignore the disk: freeing space never
  waits for space.

### Hold timeline

Every granted run records in its run record when it reserved the slot, when it was granted, when the slot was released
and why it waited (`hold_timeline`). A loaded build's receipt records when the build finished (`plan.cache.finished_ns`).
The read-only command lists them for one project:

```bash
docker-test-toolkit project timeline --input timeline.json --json
```

`timeline.json` carries `{"project_root": "<path>"}`; other fields are ignored. The command reads run record files only: it takes no
lock, loads no queue and writes nothing, so it never blocks or changes admission. It
reads every record file in the registry's `runs/` that names the project, including completed runs that are no longer
in the project index (`indexed: false`); archived records are not read. Each run lists:

| Segment | From | To |
|---|---|---|
| `startup_s` | preparation | the guardian's first admission pass |
| `queue_s` | preparation | slot reserved |
| `window_s` | slot reserved | grant |
| `active_s` | grant | leave request |
| `stopping_s` | leave request | slot released |
| `hold_s` | slot reserved | slot released |
| `hold_to_leave_s` | slot reserved | leave request |

It also lists each phase with its duration, each build with its duration, and the waiting reasons (the same reasons as
the [Admission expiry report](#admission-expiry-report)). A fact a record does not carry is `null`, never estimated:

- `unrecorded`: the record has no timeline (written by an earlier release); `reserved` is then approximated by the
  start of the admission window (`reserved: "window-start"`) and `waiting` is `null`, never shown as "no waiting".
- `release-on-later-boot`: recovery released the slot after a reboot, so durations across the two boots are `null`.
- `recovered-without-leave`: recovery finished a run that never requested leave.

Run records that name the project but do not load are listed by cell id under `unreadable`; files that name no project
are only counted (`unattributable_count`). `repository` is the checkout's normalized Git `origin` that keys its
share verdicts, and `verdict_scope` is `origin`, or `no-repository-origin` when the checkout has none (its runs stay
exclusive and teach nothing). `run status` is unchanged.

### Project build cache

Every loaded `buildx build` through the shim imports the project's earlier BuildKit layer cache and exports its own, so a
later run rebuilds only what changed. It is on by default, needs no adopter declaration and fails open: any cache
problem runs the build cold and never fails it.

**Location.** `<registry>-build-cache/<project_id>/<os>-<architecture>/`, beside the registry (for the shared account
`~/.local/state/docker-test-toolkit/registry-2-build-cache`). Each project and daemon platform keeps up to 16 published
generations (one per build key, the build's arguments without tags, labels and output options) and at most 4 GiB. The
oldest generations are evicted first. The whole store across projects is capped by [retention](#retention). A permanent lock anchor `<project_id>/<platform>.lock` sits beside the data:
builds hold it shared for the whole build, publication, reconcile and removal hold it exclusively. Caches are never
shared between projects.

**Eligibility.** The first matching row decides; an ineligible build runs exactly as before, without cache flags.

| Condition | Outcome |
|---|---|
| an archive build (`-o type=oci`) | `not-eligible:archive-build` |
| `DOCKER_TEST_TOOLKIT_BUILD_CACHE=off` (any value other than `on` is invalid and also disables it) | `skipped:disabled`, `skipped:disabled-invalid-value` |
| daemon platform unknown, or a remote daemon | `not-eligible:platform-unknown` |
| `--platform` of a foreign architecture | `not-eligible:foreign-platform` |
| `--secret` or `--ssh` | `not-eligible:build-secret` |
| a context, named context or Dockerfile in the run's private root | `not-eligible:private-root-context` |
| a context or Dockerfile read from stdin (`-`) | `not-eligible:stdin-input` |

**Outcomes.** The build receipt's `plan.cache` records `outcome`: `hit` (every new layer was imported), `miss`, or
`skipped:<reason>` (`lock-busy`, `cache-root-unsafe`, `disk-space`, `error`, `not-promoted`), and `published`: the new
generation or `not-published:<reason>` (`lock-busy`, `removed`, `invalid-export`, `over-cap`, `not-native`, `error`).
`project timeline` shows them per build. An export is published only for a promoted build of native images. Before an
export, the toolkit checks for 4 GiB plus 2 GiB of free space; if the disk still fills during the export, the build
fails as any disk-full build fails. Every imported generation's blobs are checked against their digests first; a
corrupt generation is skipped and replaced by its key's next publication.

**Off switch.** Set `DOCKER_TEST_TOOLKIT_BUILD_CACHE=off` in the process that opens the run to build cold for that run
(guardians and borrowed Python consumers inherit it). Node adopters forward their process environment for the switch to
arrive. Assistant gates cannot set it, because they reserve `DOCKER_TEST_TOOLKIT_*`; it is per run, not a gate-wide
setting.

**Removal.**

```bash
docker-test-toolkit cache remove --input cache.json --json
docker-test-toolkit cache remove --input cache.json --execute --json
```

`cache.json` selects `{"project_root": "<path>"}` for one project or `{"orphans": "all"}` for every cached project
whose registry record is gone or whose recorded root no longer exists; exactly one selector is required, and other
fields are ignored. The dry run lists bytes, generations, staging
directories and whether each platform is busy, and creates nothing. Execute deletes each platform's data in place
without waiting; a platform in use by a build reports `"busy"` and is left untouched. Orphans are derived again at
execute time, so a project whose root reappeared is kept. `project retire` removes a retired project's cache after
archiving its records (`build_cache` in its output); `cleanup project` keeps the cache.

A dispatcher killed during a build leaves its staging behind for the next exclusive operation; an orphaned Buildx client
of a killed dispatcher holds no lock and can only fail itself.

### Retention

The toolkit keeps its own state within caps without an operator command. One bounded pass removes only what its
records name; everything it cannot prove is its own is reported, never deleted.

| Kind | Removed | Kept and reported (`skipped` reason) |
|---|---|---|
| `build_cache` | whole generations, oldest first across every project, while the store exceeds `clamp(filesystem / 64, 4 GiB, 32 GiB)`; an orphaned project's generations get no priority | a generation a build is reading (`busy`; the next oldest goes instead, and `over-cap-protected` with bytes when the busy ones keep the store over its cap) |
| `archives` | archived run records whose bytes match the digest in their name, older than 90 days (by the later of mtime and ctime), then the oldest while the archive store exceeds `clamp(filesystem / 1024, 64 MiB, 1 GiB)` | a file with an archive name whose bytes do not verify (`unverified`) |
| `session_roots` | in a session a project record names and no project has an indexed run in: that project's own empty folder, then the session folder once empty and older than a day | a session folder still holding the project's content (`has-content`), one no record names (`unrecorded`); a session of a live or retained run is not entered, and other entries in a session folder are left |
| `builder_volumes` | a completed run's Buildx state volume on its own daemon whose creation time still matches the recorded one | another incarnation (`identity-changed`), a builder killed before promotion (`unpromoted`), a toolkit-named volume no record names (`unrecorded`), a volume in use (`in-use`), the volume of a builder a recovery disowned (`disowned`) |
| `probe_containers` | a probe container left by a killed process, recorded on this daemon, older than 60 s | another daemon's (`other-daemon`), a record that is not a private regular file of a probe's name (`unreadable`) |
| `pending_images` | a handed-off image nobody uses any more, with the leave drain's identity checks | an image that carries a tag its hand-off did not record (`re-tagged`; its entry is dropped) |

The caps are fractions of the filesystem that holds the store, so no machine size is assumed. A pass runs when the last
holding run leaves (`leave`), after `run recover` (`recovery`) and at `install` (forced). It takes no account metadata
or run lock, only each kind's own lock without waiting (the project lock shared, the registry lock, a cache platform's
lock, the image lock for Docker removals), and stops after 60 s or 200 removals, directories included; every Docker
read is bounded by what is left of the 60 s, and each removal too, apart from its own identity check. A pass of the same boot younger than 10 minutes that deferred nothing
is skipped. The pass writes `retention-report.json` in the account: the trigger, the clock tick, per kind
`removed_bytes`, `removed_count`, `skipped` and `deferred`, and under `legacy` the counts of session folders and
builder volumes no record names (leftovers of releases before 0.7.2; remove them by hand once inspected). The
documented `find` for stray session folders still applies to them. `install` and `run recover` include the report in
their JSON (`retention`, `null` when no pass ran).

### Telemetry categories and failure evidence

The guardian observes an active run about every five seconds. A failed observation is classified by the reason its error
carries, not by where it was raised:

| Category | Meaning | Retry |
|---|---|---|
| `observation-late` | a read missed its deadline, or a read-only Docker client call exited non-zero (reason `client-exit`) | three consecutive late attempts, then sticky |
| `telemetry-unreadable` | a native, engine or container read failed or was contradictory, or an error without a classified reason (evidence `detail: "unclassified"`, with the error's own bounded text as `reason`) | none: sticky at the first occurrence |
| `daemon-identity-changed` | the daemon changed during an active observation | none |
| `breach` | a measured limit was exceeded; `reason` is the specific one (`external-capacity`, `native-headroom`, `swapout-rate`, `admission-swap-growth`, `container-oom`, …) | none |
| `admission-expired` | the queue deadline passed before a grant (see above) | not applicable |

The observer names what was being read: `host`, `engine`, `container`, `process` or `admission`.

A daemon inventory read (`ls` followed by `inspect`) tolerates another project removing an object between the two
commands. An object that Docker's exact not-found line proves vanished is left out of the inventory, and the returned
objects must be exactly the other listed ones. Any other diagnostic (including a Docker warning on stderr), exit status,
missing or substituted survivor still fails the read. A container the run owns that is missing from the inventory still
refuses the run.

An unclassified fault's `reason` is the error's own text, whitespace collapsed and cut to 240 characters; so is a
client exit's waiting reason, and the `reason` of an `admission-expired` record whose first wait was one. For a failed
Docker control command that is the command's leading words (never its flags or values) and its exit status; Docker's
own output never enters the message, because it can carry secrets from the command's environment.

A read-only Docker client call that exits non-zero (`context show`, `version`, `info`, an inventory `inspect`) has read
nothing, so it is a late attempt too: on a shared CI daemon such an exit is transient. Its message names the command's
leading words, its exit status and one closed cause (`connect-refused`, `connect-reset`, `daemon-unreachable`,
`context-missing`, `connection-eof`, `daemon-error`, `client-timeout` or `other`), never Docker's own output. A queued or
admitting run keeps waiting and the next pass reads again, and nothing is granted on the missing read. A client exit
that persists for 30 consecutive passes (a runner without a usable Docker context, for example) revokes the run as
`telemetry-unreadable` with the client exit's message as its `reason`; a shorter queue budget ends first as
`admission-expired`, whose `waiting` names that message. The requester's `Admission ended before grant` message carries
the reason whenever it says more than the category.

A late attempt is retried because a loaded machine can miss a deadline once. Three consecutive late attempts without a
complete pass in between are sticky and record `infrastructure_failure` `observation-late`; one complete pass resets the
count. Nominally that is about 100 seconds (three attempts of at most 30 seconds, with five seconds between them). The
count lives in the guardian's memory and is lost if the guardian restarts. A late admission read does not count towards it.
When several things are wrong in one pass, a breach measured in that pass is recorded before a classified fault, so it
is not reported as a late read. A container whose read times out before a later container's OOM was read is not a
measured breach: it counts as a late attempt, and three in a row end the run as `observation-late`.

The run record gains an optional `failure_evidence` key, present only once written. It is written in the same atomic write
that first sets `infrastructure_failure` (and, for an expiry, in the revoke write); the first writer wins and later
overwrites such as `guardian-lost` or a cleanup reason do not change it. It records the category, reason, observer, an
optional `detail`, and `first_observed` (clock origin, tick, the record's state, the grant phase and the record revision).
For `observation-late` the tick is the first of the late attempts. There is no `reported` time: `run status`, `lease` and
`leave` stay read-only, and the leave message says how long before the report the failure was first observed, and the lease message names the
cause and its observer.
A stopped-retained run that carries the key cannot be resumed.

Failures that this does not cover write no evidence: the builder envelope, `guardian-lost`,
`requester-lost`, cleanup failures and recovery revokes. `telemetry-unavailable` is no longer written; older records may
still carry it.

Residual limits: the host figures describe the machine the toolkit runs on. Under a `remote` topology they describe the
client, not the daemon's host, and none of these guarantees applies. Pressure outside a WSL2 virtual machine and the GPU
(VRAM) are not sampled.

### Leaving a run

`leave` writes the requester's withdrawal and then waits for the run's original guardian to acknowledge it. It returns
only after the acknowledgment is durable, the guardian's retirement section has ended, and retirement is verified: with
the project's `registry` lock and the account's `metadata` lock held together, the run's queue entry is gone and its
project index entry is gone (a retained run keeps its index entry). The guardian process itself may still be exiting
for a moment; `run recover` of that run refuses with `actor-live` until it is gone. A run whose record carries an
infrastructure failure (for example `container-oom`) never leaves as passed: every path that sees the
acknowledgment raises that failure.

There is no retry contract. Every error below clears the handle's lease, as an incomplete cleanup always did:

- `incomplete_cleanup`: the guardian was lost, could not be observed or missed the cleanup budget before it
  acknowledged. The record says `cleanup_outcome` `incomplete`; `run recover` finishes the run.
- `incompatible_state` with reason `retirement-incomplete`: the guardian acknowledged and then died inside its
  retirement section, so a queue or index reference is still present. The record is terminal and complete; the leave
  wrote nothing beyond its withdrawal. `run recover` retires the references. A recovery of the same run that runs at
  the same moment can also make the leave report this; the record and references end correct either way.
- `incompatible_state` without that reason, when the acknowledgment found under the locks is malformed: nothing is
  written; inspect the run.
- `contention` with reason `lock-deadline`: a lock taken after the durable withdrawal was not free within the leave's
  deadline (the remaining cleanup budget plus 5 seconds). Nothing beyond the withdrawal was written. If the deadline
  ended before the acknowledgment, the record stays `stopping` with `cleanup_outcome` `pending`: a live guardian still
  acknowledges and retires the run on its own; a dead one needs `run recover`. If it ended after the acknowledgment, the
  record is terminal and a live guardian finishes its retirement when the lock frees.

### Dead owners and the takeover worker

An owner that dies while holding or waiting for the slot is recovered by the toolkit, not by a coordinator. A detection
pass observes the other queue entries read-only. It runs in each queued guardian's admission pass (every pass, whether or
not it can reserve) and after `cleanup project --execute` and `project retire --execute`. Dry runs, `cleanup session`,
`run status`, `run recover`, `run release`, `resource observe`, `project timeline`, `cache remove`, `readiness inspect`,
`protocol` and `install` run no pass. No resident process exists, so on a fully idle account a dead run waits for the next
of these commands.

An entry whose requester, guardian, slot holder, registered consumers and recovery actor are all proven gone (absent or
replaced; an exited process that has not been reaped yet, a zombie, is not proof) receives a claim intent and a detached
`_recover` worker, in ticket order, as long as at most two claims per account hold. The worker waits until the entry has
been dead for 30 seconds on one clock origin and then proves the death again. After an executing command, with a requester
and guardian killed, no consumer and nothing queued, the run is recovered without any further command within 30 s of
metadata wait, 30 s of grace and the run's recovery deadline, suspended time excluded. The command reports the pass in the
additive `recovery_trigger` key (`spawned`, `spawn_failed`, `skipped`), in `result` or in `error.context`. A pass that
cannot take the metadata lock within its budget, or fails, reports `skipped: true`, the command's own outcome stays as it
was, and the next executing command retries.

The worker runs the spawner's installed archive, which never changes in place, from its own private temporary directory
(mode 0700) that holds its logs. The worker removes the directory when it ends; a worker killed by a signal leaves that one directory behind. It recovers only the
entry that names its attempt. When the host slept past its intent it renews the intent, its only write before recovery;
when the entry stops being dead it withdraws the intent and exits. A cleared or replaced attempt is refused with
`claim-held` before any write. A worker never drains a live registered consumer, and an entry with a live consumer gets no
worker at all.

A worker whose recovery fails leaves a back-off: its own claim, bound to itself, for 300 seconds. No automatic worker
starts for that entry until the deadline, a clock-origin change (a reboot), the entry leaving the queue or a recoverer
binding a new claim ends it. Back-offs do not count toward the two claims, and `run recover` ignores them. A worker that
could not start leaves its intent to expire. A worker that fails after its first recovery section leaves `recovery_actor`
and `recovery_outcome: pending`, which `run status` shows and from which `run recover` continues.

A recovered run reads `state complete`, `admission revoked`, `recovery_outcome complete` and `quiescent true`; its owned
resources are `removed`. `cleanup_outcome incomplete` and `infrastructure_failure guardian-lost` stay as the record that the
run did not leave on its own, and `project timeline` notes `recovered-without-leave`.

Killing a requester's whole process group, with SIGKILL or SIGHUP, does not kill its guardian, which runs in a session of
its own: it withdraws the lost requester and ends the run as `requester-lost`.

`run recover` on a run of another project is an operator action, not an ordinary coordinator step. It uses the same
checks as the worker, except that an operator drains registered consumers. While another recoverer is alive or holds an
unexpired claim it refuses with `claim-held` and writes nothing; it takes over a claim whose claimant is proven dead or
whose intent has expired. Two recoverers of one run can never both clean it. A run queued behind a dead holder waits at least the 30 second grace plus the recovery itself before it is
granted, all inside its own queue deadline; choose the deadline, and the outer timeout above it, accordingly.

Recovery has one deadline (the run's cleanup budget plus 60 seconds). When it ends, the error is `lock-deadline` and
names the run and the last completed phase (`claim`, `operation`, `receipts`, `consumers`, `resources`, `private-root`,
`publish`); the claim and the recovery actor stay, and the same command resumes once the claimant is gone.

A Compose command re-observes every container of its project. A container whose identity is unchanged (identifier,
name, creation time, image, Compose project, configuration hash, service and whether it is a completed one-shot
service) is accepted when only its state, start
generation or port map changed since the run recorded it: a restart, a stop, an engine restart. Recovery of a run
killed during `compose run` therefore succeeds after its containers stopped. The promotion of a Compose command that
is the first to see a container killed for memory records `container-oom`, unless a recovery is running or the run
already records a failure;
so does a service that a `compose` command (not `compose run`, whose one-off reports through its own exit code) finds
already killed for memory when it first records it.

When a recorded resource's identity does contradict its record, the refusal has reason `owned-identity-conflict` and
`error.context.conflict` names it: `kind`, `identifier`, `name` and `keys`, the paths that differ (`identifier`, `name`,
`evidence.<key>`, or `observed.labels` for a label map Docker no longer reports; never values). A conflict found before
a command ran ends that command's receipt `failed`; one found after a command ran ends it `unresolved`, and a live
command then fails with `incomplete_cleanup` carrying the same conflict. A removal that stays unresolved names its
target in `error.context.resource`.

#### Disowning a conflicting resource

Fix the cause and rerun `run recover` when you can. When a recorded object can no longer be proven the run's own, for
example a volume replaced by another one under its name, `run recover` can release exactly that run. Copy `kind` and
`identifier` from the refusal into the input:

```json
{"context": {"...the run's six locator fields...": "..."}, "disown": [{"kind": "volume", "identifier": "<name>"}]}
```

- At most 64 entries; each must be a resource this run records as its own. Anything else, or a request on a run that
  is retained, never reached a grant, is a release recovery, or is already complete with an entry not yet disowned, is
  `invalid_input` and writes nothing.
- Recovery marks the entries in its first write (`state removed`, `evidence.disowned`) and then leaves those objects on
  the daemon unchanged: it may still read one, but it never stops, removes or re-adopts it, and a receipt or interrupted
  container control that targets one ends without a Docker call. The result lists every disowned resource under
  `disowned`.
- A BuildKit worker container is disowned only together with its builder (removing the builder would delete the
  worker), unless the builder is already removed; a disowned builder does not take its worker along, so list both.
  While the run has a disowned anonymous volume, its containers are removed without `-v`, so that volume is kept.
- A network or volume still used by a disowned container cannot be removed; that refusal names it in
  `error.context.resource`. Add it to the next request; nothing is disowned implicitly.
- A disowned object is the operator's from then on. `cleanup project` still lists, and on `--execute` removes, an
  object that carries this project's toolkit labels (a container started from an image the run built does), as for any
  object labelled as the project's.
- Finish a recovery with the toolkit version that started it: an older version does not know the marker and could
  adopt a new object under a disowned name or remove a disowned builder's state volume.
- A disown does not withdraw an image hand-off to the account's pending deletions written before it.
- Recover from the machine and process namespace that ran the run, or after its processes are known to be gone: a
  recoverer in another PID namespace observes them as gone.

This is the one supported exception to "never force" and to "do not delete records or resources manually": it changes
only the named entries of the one run's record and no Docker object.

### Unverified history

`cleanup project` lists under `history_unverified` the cell id of every run record that parses to this project's
attribution, is not indexed and is rejected by the lifecycle loader. A project with any such record is never reported
clean, `remove_record` is refused, and retirement refuses it by name. The same holds for the project's own retirement
archive files that are present: one that does not verify by its own bytes or
names another project is listed by its cell id, and an archive directory that cannot be observed or is not private, or
an entry whose name is not an archive name, is listed as `archive-subtree:<project_id>`,
`archive-subtree:<project_id>/runs` or `archive-entry:<project_id>/runs/<name>`. A deleted archive file is not seen by
cleanup. A file that cannot be parsed carries no
attribution and is not listed there, and project cleanup does not judge it; it blocks retirement. Cleanup does not judge records of other projects. Repair the listed
record first by restoring its exact bytes from a backup. There is no force flag.

### Readiness facts

`readiness inspect` reports six checks (`docker-client`, `daemon`, `compose`, `buildx`, `clock`, `capacity`) in protocol
`docker-test-toolkit.readiness/4`. Every fact is a stable field, never prose. Version 4 (0.7.2) adds to the `daemon`
check `runtime` (`docker-desktop`, `colima`, `podman`, `docker-engine`, `unsupported`, or `null` for a remote endpoint), the
daemon's `name` and the client `endpoint` (resolved for the report, never recorded), and to the `capacity`
check `cpu_pressure_percent` with its `cpu_pressure_source` (`host`, `vm-probe` or `none`), `probe_image` (`present`,
`failed` or `unknown`), `probe_reading_age_s` and `disk_data_root_free_bytes`/`disk_data_root_total_bytes`. Readiness
runs no container: the VM figures are the guardians' latest probe reading of this daemon (`probe-reading.json`), with
its age, and are `null` before a probe has run on this boot. It also reports `retention_last_tick_ns` and
`retention_last_removed_bytes` from the last [retention](#retention) report (`null` before the first pass), and per
disk place (`data_root`, `vm_disk_host`, `private_root`) `disk_<place>_free_bytes`, `_total_bytes`, `_reserve_bytes`
and `_state` (`measured` or `unknown`), with `disk_reserve_declared_bytes` (`null` when none is declared). Version 3 adds two machine settings to the `capacity` check:
`host_reserve_bytes` and `parallel_runs` (an integer, or `"auto"`). Version 2 added two facts to version 1:

- `daemon`: `architecture`, the daemon's CPU architecture as `amd64` or `arm64`. An Engine reporting any other
  architecture (for example `riscv64`) makes the check fail as `daemon-unsupported`.
- `capacity`: `host_swap_total_bytes`, the host's total swap. It is zero on a host without swap, which is valid;
  `host_swap_used_bytes` may be zero too. The swap-out rate is not part of readiness.

Readers ignore facts they do not know; a later version only adds facts (see [Compatibility](compatibility.md)).

### Platform facts

Build and Compose receipts record, additively inside their free-form plan and evidence, what platform a mutation
targets. Nothing is refused and no budget changes: this is evidence for reading a slow build or a surprising failure.

- `plan.daemon_platform`: the daemon's `os` and `architecture` (from the Engine `/info`), or `null` when that read is
  late, fails or reports an architecture the toolkit does not map.
- A loaded `buildx build`: `plan.platform` holds `requested` (the one parsed `--platform`, else `null`), `requested_raw`
  (the option verbatim) and `requested_emulated`. A platform list (`linux/amd64,linux/arm64`) is not refused for a loaded
  build: it is recorded verbatim with `requested: null`. On promotion the image's own platform and `emulated` are
  recorded in the image's evidence.
- A Compose `create`, `up` or `run`: `plan.platforms[service]` holds `requested` (the service's `platform` key),
  `image` (from the image inspection preparation already performs) and `emulated`.

`emulated` is `native` when the image (else the request) and the daemon share operating system and architecture; a
variant such as `arm64/v8` is informational and never compared. It is `emulated` when they differ and `unknown` when the
daemon, the image and the request leave it undecided, or when the topology is `remote`. A record without these keys
means "not recorded".

### Host telemetry limits

Capacity is checked against the Docker daemon's memory and the host's memory, never against the GPU. A phase budget is
host memory only; accelerator memory (VRAM, Apple unified memory used by a host-native Metal process) is not budgeted.

- **Runtime first** (0.7.2): before any accounting the toolkit names the runtime from the daemon's `/info`, the client
  platform and the client endpoint (`DOCKER_HOST`, else the current context's). Docker Desktop and Colima are
  recognised from a macOS client only, Colima only through its own profile's socket
  (`~/.colima/<profile>/docker.sock`); Docker Engine from a Linux client. An endpoint that is not a local Unix socket
  (`tcp://`, `ssh://`, `npipe://`) of another host is a remote daemon from any client, without the host-side terms;
  a `tcp://` endpoint on the loopback (`127.0.0.1`, `localhost`, `::1`) is this machine's daemon (a CI runner reaching
  its own host's daemon is native Docker Engine); a loopback port to any other daemon (on a Mac anything but Docker
  Desktop, on Linux Docker Desktop's exposed port) stays remote. Any other runtime
  is refused with `Unsupported container runtime for this client; supported on macOS: Docker Desktop, Colima, Podman.`
- **Docker Desktop on macOS** (`desktop` topology): the daemon is the Docker Desktop VM; its memory is set in Docker
  Desktop. Inside the VM a phase needs its container limit plus 0.5 GiB of guest reserve, beside the other running
  containers' limits; on the host side the Desktop topology adds 0.25 GiB to the phase's host requirement. The VM
  disk's host folder is Desktop's default one; a disk image moved elsewhere is measured `unknown` (Desktop's settings
  are in its Group Container, which macOS 15 and later guard with an access prompt, so they are not read).
- **Colima on macOS** (`desktop` topology): the daemon is the Colima VM; its memory is set with `colima start --memory
  <GiB>`. A run whose container limit plus Colima's guest reserve does not fit the VM is refused naming the size that
  fits, `raise the VM memory: colima stop && colima start --memory <N>`. The VM disk's host folder is
  `~/.colima/_lima/<instance>`. Colima's guest reserve is 512 MiB and its host side 256 MiB (measured with Colima
  0.10.3, 2026-11-04).
- **Podman on macOS** (`desktop` topology; Podman is Apache-2.0): a `podman machine` serves the Docker API on a host
  socket. Prerequisites: `podman` on `PATH`, the Docker CLI with its Buildx and Compose plugins, and a Docker context
  at the machine's API socket (`podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}'`, then
  `docker context create podman --docker host=unix://<socket> && docker context use podman`). Builds use the same
  Buildx builder, rootless. The daemon's identity is a hash of the machine's name, creation time and rootful mode,
  read on the host before every command (`podman machine inspect`, run with the account's own temporary folder, since
  `podman` derives the socket path from it): a re-created or reset machine, or a switch to rootful mode, is another
  daemon, and a project's records stay bound to the one they were made on. Memory and CPUs are set on a stopped machine
  with `podman machine set --memory <MiB> --cpus <C> <name>`; a refused run names that command with the size that
  admits it, but only when a resize would fix the refusal. Overheads: 640 MiB guest reserve, 256 MiB host side
  (measured with Podman 6.1.4). The VM disk's host folder is the machine's image folder. Refusals, each naming its fix:
  `podman-helper-unsupported` (the Docker socket of `podman-mac-helper`, which is not supported),
  `podman-binary-missing` (no `podman` on `PATH`) and `podman-machine-not-found` (the context's endpoint is no running
  machine's API socket).
- **Linux** (`native` topology), x86_64 and aarch64: the daemon sees the host's memory unless a cgroup limits it, and the
  enclosing finite cgroup limits are honoured.
- **Windows through WSL2:** install Docker Engine inside the WSL2 distribution; it runs as the `native` topology.
  `/proc/meminfo` describes the WSL2 virtual machine (by default half of the Windows memory), so pressure outside it on the
  Windows host is not visible. A Docker Desktop daemon reached from a Linux client is refused with `Docker Desktop daemon
  seen from a Linux client; use Docker Engine in the WSL2 distribution.`
- **Host reserve:** every phase keeps 4.5 GiB of host memory free for the machine's own work (IDE, browser, other
  projects). A single-purpose CI host records a smaller reserve with `install --host-reserve-bytes` (decimal bytes, at
  least 1 GiB); it is stored in the account (`host-reserve.json`), also bounds job cgroups, and is replaced only by
  another install. An invalid recorded value fails the guardian closed.
- **A headroom decline is not a breach**: the host's headroom dropping since a run's phase began, through
  any process, does not end a granted run; such pressure makes later runs wait, since every admission keeps the
  1.5 GiB headroom floor and the swap-out rate. A granted run is still ended by its own overuse (host-process envelope,
  OOM, own capacity) and by host-wide pressure, whoever caused it: an unshared run by the 1.5 GiB headroom floor and the
  swap-out rate, every run by swap growth of 3 GiB from its admission (raised by the shared allowance below).
  Undeclared host memory can therefore still end a neighbour through these checks.
- **Share verdicts count per repository, not per checkout** (0.7.2): a request's verdict key is its checkout's
  normalized Git `origin` address (as for the share scope below), entrypoint, profile and service closure, so two
  repositories checked out under the same folder name (a host-mode runner's fixed work folder) never read each other's verdicts. A
  checkout without a verdict reads the other checkouts' stores on the machine (two CI runners' work folders): any
  negative one keeps the run exclusive, else a positive one shares it. A checkout without `origin` neither reads nor
  writes verdicts: its runs stay exclusive. Verdicts written by 0.6.x (keyed by folder name) no longer match; each
  request runs exclusive once and is relearned, and old entries leave the store by its per-project bound.
- **A negative verdict is never replaced by a positive one** (0.7.2): a request whose render varies between its runs
  (a bind source chosen by an environment variable, such as a budget-dependent ledger folder) may break a rule in one
  variant only. A clean run of the other variant no longer teaches the request shared over the refusal; once any run
  of a request broke a share rule, its runs stay exclusive until the share rules change or the entry leaves the store
  by its per-project bound.
- **The builder reserve ends with the first phase**: a run that may build holds 2 GiB for its Buildx
  worker beside its co-holders until it has left its first phase with no live worker. A shared run may not create a
  worker after that (share rule R16: the command is refused and the request runs exclusive from then on). Holders older
  than 0.6.17 do not enforce R16 and keep their reserve.
- **Share scope**: `install --share-scope machine|repository` (default `machine`: any runs that fit
  share). With `repository`, a run joins only holders of its own repository and otherwise waits with reason
  `other-repository`. A repository is the checkout's normalized Git `origin` address (`github.com/owner/name`), so
  every checkout of it in any folder counts as one; a checkout without `origin` is only itself. Use it where two
  repositories' CI rounds together saturate the host (a small CI host: a database timed out at 70 % CPU pressure). Under
  this scope the earliest waiting run of the holders' repository may pass waiting runs of other repositories, so a
  repository's runs keep overlapping; once the oldest waiting run has waited 30 minutes no run passes it and the
  holders drain.
- **Repository turn** (0.7.2): under `--share-scope repository` above `--parallel-runs 1`, a run that leaves gives its
  repository a turn of 120 s (`turn.json` in the account). While no run holds the account, the earliest queued run of
  that repository reserves first, and every other run waits with reason `repository-turn`; with no run of it queued,
  every run waits until the turn ends, which is the time a repository's next batch (a CI job's next test round) has to
  arrive. The 120 s bound the turn, not the wait of a run it pushed back: that run waits for the whole batch, as behind
  any holder. A turn is valid only on the daemon and clock boot it was earned on, never once the oldest waiting run has
  waited 30 minutes, and never while a run holds. Recovery of a crashed run gives no turn and removes only that run's
  own. Under `--share-scope machine` or `--parallel-runs 1` no turn is written or read. Install a new version while no
  run holds the account: a run of an older version reserving after the switch is refused (`toolkit-upgraded`) rather
  than skipping a turn.
- **CPU pressure gates joins**: a run joins runs that already hold the daemon only while the host's CPU
  pressure (Linux PSI `some avg60`, the share of the last minute in which some task waited for a core) is below 50 %;
  otherwise it waits with reason `cpu-pressure`. Memory admission does not see CPU: on a 4-core CI host the CI jobs'
  own steps kept the cores 77 % busy, and a second stack then stalled Neo4j past its 30 s timeout. A run alone is never
  held back. On macOS (0.7.2) the containers compete for the Docker VM's CPUs, so the toolkit reads the VM's PSI
  through the daemon probe: a short-lived, network-less, read-only container (`--memory 32m`, label
  `com.docker-test-toolkit.probe`) of a pinned `alpine` image that prints `/proc/pressure/cpu` and the data root's
  `df`. Every guardian counts it as 32 MiB of external memory. It runs only when a run would join holders, outside every
  account lock, at most once per 10 s per guardian; a probe without a figure counts as full pressure (joins wait). Its
  image is pulled before a run queues and at `install`, never during admission, and the toolkit never deletes it (other
  tools may use the same public image). A Linux kernel without PSI still decides on memory alone; a PSI file that exists
  but cannot be read counts as
  full pressure. The check runs at the reservation and again at the grant. There is no fixed run cap
  (`--parallel-runs auto`).
- **What the host-process envelope counts**: on Linux, each of a run's processes (the requester and its
  active consumers) is charged its anonymous, shared and locked resident memory (`RssAnon + RssShmem + VmLck`), not
  `VmRSS`. Clean file-backed pages (mapped models and libraries) are reclaimable page cache that the host read already
  counts as available. Not charged: dirty file pages awaiting writeback, HugeTLB pages, page tables and unmapped
  tmpfs; memory shared between processes is charged to each of them. Kernels before 4.5 have no `Rss*` rows and are
  charged `VmRSS`. macOS still charges `ps` RSS.
- **Pressure while sharing:** a run that has shared (a co-holder was seen, or a run was granted beside it after its
  admission) is not failed by the 1.5 GiB headroom floor or the swap-out rate; those keep gating joiners. Its swap
  growth threshold grows by the budgets of the runs granted beside it after its admission, which never shrink, even
  after they leave (`<account>/shared-grants.json`), and by the full budget of every run holding beside it during the
  observation, whenever that run was granted. Swap that an earlier holder caused can outlast its departure and count
  against this run. An
  unreadable or invalid grants log treats every shared run as having shared, with no allowance for its growth check,
  and records `shared-grants-unreadable` once per run; nothing rewrites it, so the condition lasts until an operator
  removes `shared-grants.json` while no shared run holds the account. A readable log that cannot be rewritten (a full
  or read-only account folder) keeps a shared run's leave and recovery from completing, so its grant is never lost;
  they finish on the retry after the account folder is writable again.
- **Daemon memory rule:** a run whose container limit plus 0.5 GiB exceeds the daemon's memory is refused
  (`unsupported-base-capacity`); one that fits alone but not beside the other running containers' limits waits
  (`external-capacity`).
- A container using the Apple GPU (Metal, MLX or GGUF with Metal) is not possible in Docker; such work runs on the host,
  outside the toolkit's Docker budget.

## Retiring vanished throwaway projects

A test project whose recorded root no longer exists keeps its project record and run records in the shared registry
until it is retired. `project retire` moves exactly the reviewed projects' records into a sibling archive. It takes only
a project with no live actor, no run in progress or retained, no private root, no resource, image, endpoint, consumer
or queue authority, and an absent root. Every other project is reported with a stable reason and left untouched.

Run it with the installed command; from a source tree it refuses before opening account state. List first, review, then execute with the evidence the listing produced:

```sh
docker-test-toolkit project retire --input retire.json --json
docker-test-toolkit project retire --input retire.json --execute --json
```

`retire.json` takes one of three selections; other fields are ignored. Without `project_ids`, it lists every project
whose recorded root is absent; a project whose
root exists is never listed implicitly, except a partly archived one, which is reported as blocked. `{"project_ids":
[...]}` lists exactly the 1-1024 unique 64-hex ids given. With `--execute`, `{"evidence": [...]}` takes the
`eligible[i].evidence` objects of a listing, unmodified; `evidence` without `--execute` is refused. Execute never touches a project outside that list. It
re-derives every clause under the project's locks and moves its run records, and then its project record, by one atomic
rename each, never by copy. Vanished-root projects that were not in the evidence are reported under
`unreviewed_vanished_roots` and are not touched.

Exit 0 means the command completed, not that projects were archived. Read `results[]` and `stopped`. Every refusal
after the account is admitted (a held lock, drifted evidence, an archive conflict, a reappeared root) is a per-project
result with a category. Only malformed input (exit 2), a refused account (exit 3), and an unreadable registry directory
or invalid archive root (exit 5) fail the command. A filesystem failure while archiving (`rename-failed`: a failed
rename, a failed directory sync after a rename, or any failure once an earlier project was archived) stops the command
after the current project (`stopped: true`); its `archived_runs` and `archived_project` list what had already moved.
Running the listing and execute again completes it, because the archive is the record of progress.

A run's Docker authority binds the client program by its content (SHA-256 and size at its path) and the daemon by its
ID, not by the file's device or inode: any runner container with the same client build that talks to the same daemon can
recover a run another runner started, for example after a CI job was cancelled and its guardian died.

A run that blocks its project carries a `locators` entry. Pass its `context` verbatim to `run recover` (or `run release`
for a retained run), either as the document itself or as `{"context": ...}`. A `run.*` locator carries the six string
fields `project_root`, `project_state`, `run_id`, `session_id`, `record` and `private_root` (other fields are ignored),
and it must name the registry and its own `runs/<run_id>.json`. Anything else is `invalid_input` (exit 2) before any
filesystem access.

The archive is the sibling directory `<registry>-archive`, mode 0700; the first execute creates it and a dry run
never does. It holds one directory per project with
`runs/<run_id>-<sha256>.json` and `project-<sha256>.json`. Never edit it, and never move a file back into `runs/`.

Retirement relies on the registry's and the archive's owner-only modes and on the toolkit's locks. It does not defend
against another process of the same user that ignores them.

## Batch state and stop rules

Batch coordination is finite and process-local. It writes no batch record, schema, compatibility cell, recovery cursor,
status endpoint, or replay log. Ordinary run records and exact project cleanup remain the only crash authority.

| Work or result | Retained authority | What runs next |
|---|---|---|
| Host-only batch | Aggregate memory only; no run | Later eligible work |
| Passed or test-failed Docker batch (a segment's union run) with complete cleanup | Ordinary terminal run evidence | Later batches; a dependent segment requires the previous segment to pass |
| Incompatible state, contention, infrastructure failure, or incomplete cleanup | Ordinary lifecycle evidence when a run opened | Later Docker batches do not run; independent host-only work remains eligible |
| Passed trailing boundary | Aggregate memory only | Next segment |
| Safety-failed or suppressed boundary | Aggregate memory plus underlying ordinary evidence | Later Docker batches do not run |

Test failures continue independent eligible batches so one invocation reports the family. A segment marked
`requires_previous_success` is skipped unless the immediately previous segment passed. A trailing boundary runs only
after its segment's eligible batches and never substitutes for ordinary cleanup. Primary exit categories retain their
stable precedence; cleanup remains separately visible.

If the adapter or coordinator process dies, do not reconstruct or replay its batch position. Dry-run and then execute
exact project cleanup, which reconciles every recorded ordinary run. Start a new finite plan only after that recovery.

## Troubleshooting

- `invalid_input`: check the required fields, their types and absolute paths.
- `incompatible_state`: inspect run status; do not delete records or resources manually.
- `contention`: another live run owns admission or a permanent lock; wait or inspect its bounded identity.
- `infrastructure_failure`: provision the required host, Python, Docker, Compose, or Buildx prerequisite.
- `incomplete_cleanup`: preserve evidence and run exact project recovery; never use a global prune.
- A build failing with `openat2 /sys/fs/cgroup/buildkit/<id>/memory.max: no such file or directory`: the builder's memory
  controller did not come up. BuildKit enables its controllers once at start and only logs `failed to enable cgroup
  controller` when a `docker exec` reaches the starting builder. The toolkit checks this after creating a builder and
  restarts it up to three times, printing `Builder <name> has no memory controller`. If the error still appears, keep the
  builder's `cgroup.subtree_control` and logs before removing it, and report them.
- `incompatible_state` reasons are stable codes; act on the code, not on the prose:
  - `queue-invalid`: the account queue is not readable by this version (malformed, another queue protocol, an invalid
    claim or phase). Do not edit it; inspect the account.
  - `blocked-entry`: one of this project's indexed runs has a record that cannot be trusted. It blocks this project's
    submission and recovery and anything else that needs that run's authority; a slot that run holds stays held, and other
    projects are not affected. Preserve the record and inspect it (`next_action` is `inspect_status`); do not edit it.
  - `record-invalid`, `record-changed`: a run or project record fails validation or changed under
    the command; preserve it and repair that exact record.
  - `claim-held`: another recoverer is alive or holds an unexpired claim; wait for it: at most 60 seconds plus its
    recovery for a worker's intent, 30 seconds for an operator's. Never force.
  - `actor-live`, `actor-unverifiable`: the run's guardian, requester, consumer or previous recoverer may still run (a
    zombie counts) or cannot be observed; nothing was written. Retry after it is reaped or gone. One exception: an
    exact `run recover` of an indexed run that never reached a grant (a queued or preparing run) treats only an executing
    requester or guardian as live, because an exited but unreaped process holds nothing for such a run. Granted runs and
    every takeover claim count a zombie as live.
  - `guardian-start-pending`: a guardian start is still within its bootstrap deadline; retry later.
  - `unsupported-operation-kind`, `unsupported-receipt-kind`: the run's journal names an operation this version cannot
    reconcile; nothing was written; use the version that wrote it.
  - `lock-deadline`: a wait ended with its deadline; the message names the run and the last completed phase; rerun the
    same command. For `leave` see [Leaving a run](#leaving-a-run).
  - `retirement-incomplete`: `leave` saw the guardian's acknowledgment, but the guardian died before it retired the
    run's queue or index reference; run `run recover` on the run (see [Leaving a run](#leaving-a-run)).
  - `owned-identity-conflict`: an object recorded as the run's own no longer matches its record; `context.conflict`
    names its kind, identifier and the differing keys. Fix the cause and rerun, or release the run with `disown` (see
    [Disowning a conflicting resource](#disowning-a-conflicting-resource)).
  - `toolkit-upgraded`: another version was installed after this process loaded the toolkit; restart the process.
  - `state-format-newer`: a newer toolkit wrote the account; install a version with that state format or newer.
  - `toolkit-not-installed`, `toolkit-load-unresolved`: no installation runs this code, or it was imported through
    `current` instead of the resolved version folder; install the toolkit or load it through the reference loader.
  - `observation-deadline`, `telemetry-unreadable`: a bounded Docker, host or engine command or read missed its deadline
    or failed. The run record's `failure_evidence` names the category and observer; see
    [Telemetry categories and failure evidence](#telemetry-categories-and-failure-evidence). An older record may carry the
    historical `telemetry-unavailable`.

Diagnostics intentionally omit credentials, raw environments, unbounded subprocess output, and cross-project resource
inventories.
