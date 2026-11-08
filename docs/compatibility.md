# Compatibility

Projects depend only on [the permanent contract](adopters.md#the-permanent-contract): it changes additively, unknown
fields are ignored at every level on both sides, and inputs accepted once stay accepted. A frozen Python corpus and a
frozen wire transcript (`tests/fixtures/contract/`) run against every release, together with cross-version scenarios
that run two installed builds in one root: the build against a renamed copy of itself and, from the second
installed-era release on, the previous release against the build.

The account's state format is separate from the contract: the anchor `metadata.lock` carries
`docker-test-toolkit-state:<F>`, and code refuses an account a newer format wrote (`state-format-newer`). Rolling back
works within a format, never across one.

## Account state

The account queue is version 2 (`queue: 2` in the protocol map): one entry per run with its project, ticket, phase, slot
holder, dead-since observation, takeover claim and last completed recovery phase. At most the recorded parallel-runs
limit of entries hold the account (one by default); the codec itself no longer rejects a queue with several holders, so
an over-full queue at limit 1 is read rather than refused. Only queue protocol 2 is read.

Shared runs add files the account and registry never needed before; versions that predate them ignore them:
`parallel-runs.json`, `images.lock`, `pending-image-deletions.json` and `shared-grants.json` in the account, and
`share-verdicts/` in the registry. A shared run's record carries the key `sharing` (`{"mode": "shared"}`); an
exclusive run's record has no new key.

- **Rollback with shared state.** An older installer refuses while two or more runs hold the account. With exactly one
  shared holder it does not refuse: the older version then ignores pending image deletions and the grants log, and its
  recoverer cannot read the shared run's record, so a dead shared holder stays unrecovered until a version that reads
  shared state is installed again, which recovers it.
- **Late pull.** A shared run's pull is promoted from the image present under the image lock, whatever its client
  reported; a daemon that finishes a pull after its client has exited and after that check leaves an image nobody
  owns (remove it with `cleanup project`). A reconciled pull is also left unowned when another record, or one the
  toolkit cannot read, may use the same image: provenance cannot be proven, so the image is kept rather than claimed;
  once that record is repaired or gone, `cleanup project` removes it.
- **Operator cleanup keeps images in use.** `cleanup project` and `cleanup session` now keep an image that a live run
  or an indexed retained run uses, at every limit; before shared runs they removed it.
- **Pending entries of another daemon.** A handed-off image is deleted only on the daemon its owner's grant named. If
  that daemon never returns (a removed Docker context), its entries stay in `pending-image-deletions.json`, and every
  leave on the account takes the image lock for them; remove the file by hand once that daemon is gone for good.

A run declares `capacity` for compatibility only: any integer is accepted and ignored, and no run record carries it.

Run records may carry the optional keys `failure_evidence` (written by an observation or admission failure) and
`hold_timeline` (written by every new grant, see [Hold timeline](operations.md#hold-timeline)). Records without them load
unchanged, and no loader adds them to an existing record. Build receipts carry the free-form plan key `cache` (the cache
outcome with `finished_ns`, see [Project build cache](operations.md#project-build-cache)); receipt protocol 1 is
unchanged.

The [project build cache](operations.md#project-build-cache) lives in the account-state sibling
`<registry>-build-cache`; `cache remove` deletes it. A caller `--cache-from` on `buildx build` is refused: the toolkit
injects its own cache flags.

Account files added in 0.7.2, ignored by older versions: `probe-image.json` (per daemon: whether the probe image is
present, whether the toolkit pulled it, a failed pull's backoff stamp), `probe-reading.json` (per daemon: the latest
probe reading, for readiness) and `probes/<container name>` (one file per running probe, holding its daemon ID, removed
when the container is gone), `retention-stamp.json` and `retention-report.json` (the last
[retention](operations.md#retention) pass), `disk-reserve.json` (the declared disk reserve) and `disk-leases.json`
(the disk estimates of the runs holding the account, see [Disk admission](operations.md#disk-admission)). A run that
waited for disk or for another repository's turn records the waiting reason `disk-capacity` or `repository-turn`,
which older versions read as any reason string. `turn.json` holds the repositories' turns (see the share scope in
[operations](operations.md)); older versions ignore it, and the version gate refuses an older guardian's reservation
once a newer version is current. No run, grant or owned-resource record gains a key; inside the free-form
`evidence` of owned items a builder gains `state_volume` (its state volume's name and creation time) and an image
`size_bytes`. Retirement sets an archived record's mtime to the archive time. Share verdicts are keyed by the
checkout's Git origin, so 0.6.x and 0.7.2 processes relearn their verdicts separately during a rollout.

## Protocols

`readiness inspect` reports protocol `readiness/4` (0.7.2: the daemon check adds `runtime`, `name` and `endpoint`;
the capacity check adds CPU pressure, daemon probe, disk and retention facts; all additive, readers ignore unknown
facts). Before that `readiness/3` (the capacity check carries
`host_reserve_bytes` and `parallel_runs`;
version 2 added the daemon's `architecture` and `host_swap_total_bytes`). Receipt plans and evidence carry platform facts as free-form keys only.

On Podman (0.7.2) a run's `daemon_id` is the NAME-shaped `podman-<40 hex>` in the existing fields, and an archive
export records the layout `docker-archive` in `plan.archive`, which earlier releases validate only when they write an
archive themselves; they load such records unchanged.

0.7.4 changes no record, receipt or queue format (0.7.3 was never published). A claim intent it writes may run up to 60 seconds (the rest of the grace
plus 30 seconds) instead of 30; a queued 0.7.2 guardian reads it as held and is granted after the worker recovers the
run. A 0.7.2 guardian treats a 0.7.4 back-off as reclaimable, which can start 0.7.2 workers until the machine switches
version; after the switch a queued old waiter gets `toolkit-upgraded` as before. A clock origin in a time namespace whose offsets are zero now reads as the
initial namespace, the same text as on a host that gives containers no time namespace; a retained run recorded under a
container's own namespace id reads as another clock, as after a reboot.

0.7.5 changes no record, receipt or queue format.

0.7.6 changes no record, receipt or queue format. A finished mutation dispatched through the shim whose output is not
UTF-8 or exceeds 1 MiB now goes through the promotion path like any finished child (receipt `complete` or `child_failure` by its exit code,
shim exit = the child's) where it was always `unresolved` (receipt exit 5, shim exit 7 `incomplete_cleanup`);
reconciliation can still leave it `unresolved`. Its relayed output is cut to 384 KiB per stream plus one marker line, so a valid stream
between 384 KiB and 1 MiB, relayed whole before, now arrives cut around a marker. The toolkit's own cleanup commands
(leave, recovery, retention, archive export) keep the strict reading. Receipt exit codes 2 and 5 can be synthetic: a refusal before the child
gives state `failed` with 2 or 5, and a reconciliation failure after it can replace the real code with 5. The shim's
stderr usually shows which (one toolkit line versus the child's own output), but a child can print anything, so it is a
hint, not proof. The share-rule refusal's text changed (rule and outcome first, then `Reason:`); its reason
`share-rule` and exit 2 did not. Error messages are redacted before they are cut to 512 characters.

Protocol `batch:1` versions only the live Python facade and bridge messages. It adds no durable field and does not change
run, project, account or cleanup record schemas, so a crashed batch has no toolkit batch status or replay operation; its
recovery is decided solely from its ordinary run records.

The receipt kind `image-save` and the `plan["archive"]` record keep receipt protocol 1: `result` is still exactly
`{category, exit_code}`.

Unsupported host floors, changed records, duplicate JSON fields and incomplete generated artifacts fail closed before
Docker mutation. Roots require local APFS or ext-family storage, and Docker evidence requires a current strict run with
certain cleanup.
