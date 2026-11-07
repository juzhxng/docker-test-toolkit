"""Service tests of the owned image export: ``docker image save <id>`` through the receipt dispatcher.

Races run two threads in one process; a failpoint
callback pauses a named thread at a named stage. A dispatcher "crash" is a ``KeyboardInterrupt`` raised from a
failpoint: it passes the exception fence and leaves exactly the on-disk and record state a killed process leaves.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
import threading
import time
from collections.abc import Callable
from pathlib import Path

import pytest

from docker_test_toolkit.adapters.inbound.bridge import dispatch_failure
from docker_test_toolkit.application import image_archive, lifecycle
from docker_test_toolkit.domain.errors import (
    ArchiveRejected,
    IncompleteCleanup,
    InfrastructureFailure,
    InvalidInput,
    ResourceBusy,
    ResourceError,
)
from tests.archive_fixtures import Layout, write_archive
from tests.archive_harness import (
    REFERENCE,
    Dispatcher,
    ExportRun,
    Pause,
    Stages,
    archive_of,
    interrupt,
    observed_locks,
    single,
    start_run,
)
from tests.integration.account_fixtures import current_identity, dead_identity, kill_guardian, retire_actor
from tests.unit.test_docker_receipt_service import IMAGE_ID

# --- success, repeat and refusals before any file -----------------------------------------------------------------


def test_export_publishes_a_verified_archive_and_prints_one_canonical_line(tmp_path: Path) -> None:
    run = start_run(tmp_path)

    result = run.export()

    receipt = run.receipt()
    archive = archive_of(receipt)
    published = Path(archive["path"])
    line = json.loads(result.stdout)
    assert result.stdout.endswith("\n") and result.stdout.count("\n") == 1
    assert result.stdout == json.dumps(line, sort_keys=True, separators=(",", ":")) + "\n"
    assert receipt["state"] == "promoted" and receipt["result"] == {"category": "complete", "exit_code": 0}
    assert published == run.exports_directory / f"{receipt['id']}.tar"
    assert stat.S_IMODE(published.stat().st_mode) == 0o600
    assert line["path"] == str(published) and line["bytes"] == published.stat().st_size == archive["bytes"]
    assert line["sha256"] == archive["sha256"] == hashlib.sha256(published.read_bytes()).hexdigest()
    assert line["kind"] == "image-save" and line["image_id"] == run.image_id and line["root_digest"] == run.image_id
    assert line["format"] == "docker-image-save" and line["layout"] == "oci+docker-manifest"
    assert line["platform"] == {"os": "linux", "architecture": "arm64"}
    assert line["operation_id"] == receipt["id"] and line["run_id"] == run.record()["run_id"]
    assert run.fake.exports == [("image", "save", "-o", receipt["plan"]["output"], run.image_id)]
    assert run.staged() == [published]  # the staging directory is gone
    assert run.record()["owned_resources"]["images"][run.image_id]["state"] == "present"  # ownership is unchanged
    lifecycle.dispatch_docker(run.handle._context, run.descriptor, run.fake, ("image", "rm", run.image_id))
    assert run.handle.leave().cleanup_outcome == "complete"
    assert not published.exists()


def test_a_repeat_export_is_refused_with_the_recorded_facts(tmp_path: Path) -> None:
    run = start_run(tmp_path)
    run.export()
    archive = archive_of(run.receipt())
    commands = len(run.fake.exports)

    with pytest.raises(InvalidInput) as refused:
        run.export()

    assert refused.value.reason == "archive-exists"
    assert archive["path"] in str(refused.value) and archive["sha256"] in str(refused.value)
    assert len(run.fake.exports) == commands
    assert len(run.record()["docker_receipts"]) == 2  # the pull and the first export only


def test_a_tampered_recorded_archive_is_never_quoted(tmp_path: Path) -> None:
    run = start_run(tmp_path)
    run.export()
    record = run.record()
    (receipt,) = [item for item in record["docker_receipts"].values() if item["kind"] == "image-save"]
    receipt["plan"]["archive"]["path"] = "/elsewhere.tar"
    lifecycle.atomic_json(run.handle._context.record, record)

    with pytest.raises(ResourceError) as refused:
        run.export()

    assert refused.value.reason == "archive-record-invalid" and "/elsewhere.tar" not in str(refused.value)


def _assert_nothing_created(run: ExportRun) -> None:
    assert [item for item in run.record()["docker_receipts"].values() if item["kind"] == "image-save"] == []
    assert not run.exports_directory.exists()
    assert run.fake.exports == []


@pytest.mark.parametrize(
    ("argv", "message"),
    [
        (("image", "save", "-o", "/x/archive.tar", "{id}"), "exactly one"),
        (("image", "save", "--output=/x", "{id}"), "exactly one"),
        (("image", "save", "--platform", "linux/arm64", "{id}"), "exactly one"),
        (("image", "save", "{id}", "{id}"), "exactly one"),
        (("image", "save", "example/cache:latest"), "exactly one"),
        (("image", "save", REFERENCE), "exactly one"),
        (("image", "save", "sha256:" + "ab" * 8), "exactly one"),
        (("image", "save", "sha256:" + "AB" * 32), "exactly one"),
        (("image", "save", "sha256:" + "0" * 64), "owned image"),
        (("save", "{id}"), "outside the classified"),
        (("save", "-o", "/x", "{id}"), "outside the classified"),
    ],
)
def test_only_one_exact_owned_image_id_is_accepted(tmp_path: Path, argv: tuple[str, ...], message: str) -> None:
    run = start_run(tmp_path)
    argv = tuple(value.replace("{id}", run.image_id) for value in argv)

    with pytest.raises(InvalidInput, match=message):
        lifecycle.dispatch_docker(run.handle._context, run.descriptor, run.fake, argv)

    _assert_nothing_created(run)


def test_a_removed_image_cannot_be_exported(tmp_path: Path) -> None:
    run = start_run(tmp_path)
    lifecycle.dispatch_docker(run.handle._context, run.descriptor, run.fake, ("image", "rm", run.image_id))

    with pytest.raises(InvalidInput, match="present"):
        run.export()

    _assert_nothing_created(run)


def test_an_external_image_cannot_be_exported(tmp_path: Path) -> None:
    run = start_run(tmp_path)

    with pytest.raises(InvalidInput):
        lifecycle.dispatch_docker(run.handle._context, run.descriptor, run.fake, ("image", "save", IMAGE_ID))

    _assert_nothing_created(run)


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        (lambda item, image_id: item.pop("Descriptor"), "unsupported-image-store"),
        (
            lambda item, image_id: item.__setitem__("Descriptor", {"digest": "sha256:" + "1" * 64}),
            "unsupported-image-store",
        ),
        (lambda item, image_id: item.__setitem__("Descriptor", []), "unsupported-image-store"),
        (lambda item, image_id: item.__setitem__("Os", None), "unsupported-image-store"),
        (lambda item, image_id: item.__setitem__("Size", image_archive.ARCHIVE_MAX_BYTES + 1), "export-storage"),
        (lambda item, image_id: item.__setitem__("Size", "large"), "export-storage"),
    ],
)
def test_an_unsupported_or_oversized_image_is_refused_before_any_file(
    tmp_path: Path, change: Callable[[dict, str], object], reason: str
) -> None:
    run = start_run(tmp_path)
    change(run.fake.images[run.image_id], run.image_id)

    with pytest.raises(ResourceError) as refused:
        run.export()

    assert refused.value.reason == reason
    _assert_nothing_created(run)


def test_free_space_admission_accepts_exactly_the_cap_and_refuses_one_byte_less(tmp_path: Path) -> None:
    run = start_run(tmp_path)
    cap = image_archive.ARCHIVE_MAX_BYTES

    with pytest.raises(ResourceError) as refused:
        run.export(_available_bytes=lambda path: cap - 1)
    assert refused.value.reason == "export-storage"
    _assert_nothing_created(run)

    assert json.loads(run.export(_available_bytes=lambda path: cap).stdout)["bytes"] > 0


def test_export_is_admitted_only_in_an_active_run() -> None:
    """A stopping record cannot be loaded through a run handle, so the two admission checks are called directly."""
    command = lifecycle.ClassifiedDockerCommand("image-save", (), {"image_id": IMAGE_ID})

    with pytest.raises(ResourceBusy):
        lifecycle._admit_archive_append({"state": "stopping"}, command)
    with pytest.raises(ResourceBusy):
        lifecycle._prepare_image_export(None, command, {"state": "stopping"}, lifecycle._ArchiveSeams())


# --- failure classes of a child that ran ---------------------------------------------------------------------------


def _failed(run: ExportRun, failure: str, *, exit_code: int = 0, cleanup: str = "complete") -> None:
    receipt = run.receipt()
    assert receipt["state"] == "failed"
    assert archive_of(receipt) == {"schema": 1, "state": "failed", "cleanup": cleanup, "failure": failure}
    assert receipt["result"]["exit_code"] == exit_code
    assert [path for path in run.staged() if path.suffix == ".tar"] == []  # no final and no staging directory
    assert [path for path in run.staged() if path.is_dir()] == []


def test_exit_zero_without_a_file_fails_as_absent(tmp_path: Path) -> None:
    run = start_run(tmp_path, configure=lambda fake: setattr(fake, "save", lambda output: 0))

    with pytest.raises(ArchiveRejected) as failed:
        run.export()

    assert failed.value.failure_class == "absent" and failed.value.cleanup is None
    _failed(run, "absent")


def test_a_failed_child_keeps_its_exit_code_and_leaves_nothing(tmp_path: Path) -> None:
    def partial(output: Path) -> int:
        output.write_bytes(b"partial")
        return 1

    run = start_run(tmp_path, configure=lambda fake: setattr(fake, "save", partial))

    with pytest.raises(ArchiveRejected) as failed:
        run.export()

    assert failed.value.failure_class == "child-failed"
    _failed(run, "child-failed", exit_code=1)
    assert run.receipt()["result"]["category"] == "child_failure"


def test_an_extra_entry_in_the_staging_directory_is_refused(tmp_path: Path) -> None:
    def extra(output: Path) -> int:
        run_fake.write_valid(output)
        (output.parent / "stray").write_bytes(b"x")
        return 0

    run = start_run(tmp_path)
    run_fake = run.fake
    run.fake.save = extra

    with pytest.raises(ArchiveRejected) as failed:
        run.export()

    assert failed.value.failure_class == "unexpected-entry"
    _failed(run, "unexpected-entry")


def test_an_archive_over_the_cap_is_refused_after_the_write(tmp_path: Path) -> None:
    def huge(output: Path) -> int:
        with output.open("wb") as stream:
            stream.truncate(image_archive.ARCHIVE_MAX_BYTES + 1)
        return 0

    run = start_run(tmp_path, configure=lambda fake: setattr(fake, "save", huge))

    with pytest.raises(ArchiveRejected) as failed:
        run.export()

    assert failed.value.failure_class == "over-cap"
    _failed(run, "over-cap")


def test_a_hard_linked_or_symlinked_output_is_refused_and_removed(tmp_path: Path) -> None:
    def linked(output: Path) -> int:
        run_fake.write_valid(output)
        os.link(output, output.parent / "other")
        return 0

    run = start_run(tmp_path)
    run_fake = run.fake
    run.fake.save = linked

    with pytest.raises(ArchiveRejected) as failed:
        run.export()

    assert failed.value.failure_class == "unexpected-entry"
    _failed(run, "unexpected-entry")


def test_a_symlinked_output_is_an_io_error_and_its_target_is_untouched(tmp_path: Path) -> None:
    victim = tmp_path / "victim"
    victim.write_bytes(b"precious")

    def symlink(output: Path) -> int:
        output.symlink_to(victim)
        return 0

    run = start_run(tmp_path, configure=lambda fake: setattr(fake, "save", symlink))

    with pytest.raises(InfrastructureFailure):
        run.export()

    _failed(run, "io-error")
    assert victim.read_bytes() == b"precious"


@pytest.mark.parametrize("mode", [0o644, 0o666, 0o400])
def test_the_published_archive_is_always_mode_0600(tmp_path: Path, mode: int) -> None:
    def written(output: Path) -> int:
        run_fake.write_valid(output)
        output.chmod(mode)
        return 0

    run = start_run(tmp_path)
    run_fake = run.fake
    run.fake.save = written

    run.export()

    assert stat.S_IMODE(Path(archive_of(run.receipt())["path"]).stat().st_mode) == 0o600


def test_the_output_check_refuses_a_file_the_account_does_not_own(tmp_path: Path) -> None:
    """Root can hand a file to another account; any other account is refused for a root-owned system file."""
    foreign = tmp_path / "foreign"
    foreign.write_bytes(b"x")
    if os.getuid() == 0:
        os.chown(foreign, 65534, -1)
    else:
        foreign = Path("/etc/hosts")
    descriptor = os.open(foreign, os.O_RDONLY)
    try:
        with pytest.raises(ArchiveRejected) as refused:
            image_archive.accept_output(descriptor)
    finally:
        os.close(descriptor)
    assert refused.value.failure_class == "invalid-output"


def test_a_substituted_archive_fails_verification_even_when_it_is_well_formed(tmp_path: Path) -> None:
    def substituted(output: Path) -> int:
        write_archive(output, Layout(docker_manifest=True, attestation=True).members())
        return 0

    run = start_run(tmp_path, configure=lambda fake: setattr(fake, "save", substituted))

    with pytest.raises(ArchiveRejected) as failed:
        run.export()

    assert failed.value.failure_class == "verification-failed"
    _failed(run, "verification-failed")


# --- deadline, seams and rewrites ----------------------------------------------------------------------------------


def test_a_client_that_outlives_the_receipt_deadline_fails_as_deadline(tmp_path: Path) -> None:
    def slow(output: Path) -> int:
        time.sleep(0.3)
        raise InfrastructureFailure("Docker client did not complete within its bounded execution.")

    run = start_run(tmp_path, configure=lambda fake: setattr(fake, "save", slow))

    with pytest.raises(InfrastructureFailure):
        run.export(_receipt_timeout_seconds=0.1)

    _failed(run, "deadline", exit_code=5)


def test_the_deadline_is_checked_before_publication(tmp_path: Path) -> None:
    run = start_run(tmp_path)
    with Stages() as stages:
        stages.on("archive-after-verify", lambda: time.sleep(0.4))
        with pytest.raises(ArchiveRejected) as failed:
            run.export(_receipt_timeout_seconds=0.3)

    assert failed.value.failure_class == "deadline"
    _failed(run, "deadline")


def test_a_staging_removal_failure_after_publication_is_exit_seven_and_nothing_is_published(tmp_path: Path) -> None:
    def refuse(exports: int, name: str, held: int) -> None:
        raise OSError("synthetic removal failure")

    run = start_run(tmp_path)

    with pytest.raises(IncompleteCleanup):
        run.export(_remove_tree=refuse)

    receipt = run.receipt()
    assert receipt["state"] == "failed"
    assert archive_of(receipt) == {
        "schema": 1,
        "state": "failed",
        "cleanup": "incomplete",
        "failure": "cleanup-incomplete",
    }
    assert [path for path in run.staged() if path.suffix == ".tar"] == []  # the link was rolled back


def test_a_removal_failure_on_the_fence_path_keeps_the_primary_error_and_attaches_the_cleanup_error(
    tmp_path: Path,
) -> None:
    def refuse(exports: int, name: str, held: int) -> None:
        raise OSError("synthetic removal failure")

    run = start_run(tmp_path, configure=lambda fake: setattr(fake, "save", lambda output: 3))

    with pytest.raises(ArchiveRejected) as failed:
        run.export(_remove_tree=refuse)

    assert failed.value.failure_class == "child-failed"
    assert isinstance(failed.value.cleanup, IncompleteCleanup)
    assert archive_of(run.receipt())["cleanup"] == "incomplete"


def test_an_archive_extended_after_verification_is_never_published(tmp_path: Path) -> None:
    run = start_run(tmp_path)

    def append() -> None:
        staged = single(run.exports_directory, "*/archive.tar")
        with staged.open("ab") as stream:
            stream.write(b"\0" * 512)

    with Stages() as stages:
        stages.on("archive-after-verify", append)
        with pytest.raises(ArchiveRejected) as failed:
            run.export()

    assert failed.value.failure_class == "publish-substituted"
    assert "archive-after-link" not in [stage for _, stage in stages.seen]  # no final name was ever created
    _failed(run, "publish-substituted")


def test_a_metadata_only_change_after_verification_is_never_linked(tmp_path: Path) -> None:
    """A chmod keeps the size and mtime, so only the ctime comparison under the lock refuses the link."""
    run = start_run(tmp_path)

    def loosen() -> None:
        time.sleep(0.05)  # Linux stamps files with a coarse clock: let one tick pass so the ctime can differ
        single(run.exports_directory, "*/archive.tar").chmod(0o644)

    with Stages() as stages:
        stages.on("archive-after-verify", loosen)
        with pytest.raises(ArchiveRejected) as failed:
            run.export()

    assert failed.value.failure_class == "publish-substituted"
    assert "archive-after-link" not in [stage for _, stage in stages.seen]
    _failed(run, "publish-substituted")


def test_an_archive_rewritten_during_the_walk_with_the_same_size_is_never_published(tmp_path: Path) -> None:
    run = start_run(tmp_path)

    def rewrite() -> None:
        time.sleep(0.05)  # let a coarse Linux timestamp tick pass so the mtime can differ
        staged = single(run.exports_directory, "*/archive.tar")
        data = staged.read_bytes()
        staged.write_bytes(data)

    with Stages() as stages:
        stages.on("archive-after-walk", rewrite)
        with pytest.raises(ArchiveRejected) as failed:
            run.export()

    assert failed.value.failure_class == "publish-substituted"
    _failed(run, "publish-substituted")


def test_a_published_archive_rewritten_after_the_link_is_rolled_back(tmp_path: Path) -> None:
    run = start_run(tmp_path)

    def rewrite() -> None:
        time.sleep(0.05)  # let a coarse Linux timestamp tick pass so the mtime can differ
        published = single(run.exports_directory, "*.tar")
        data = published.read_bytes()
        published.write_bytes(data)

    with Stages() as stages:
        stages.on("archive-after-link", rewrite)
        with pytest.raises(ArchiveRejected) as failed:
            run.export()

    assert failed.value.failure_class == "publish-substituted"
    _failed(run, "publish-substituted")


def test_a_final_name_replaced_before_the_record_is_left_untouched_and_reported(tmp_path: Path) -> None:
    run = start_run(tmp_path)
    foreign = {}

    def replace() -> None:
        published = single(run.exports_directory, "*.tar")
        published.unlink()
        published.write_bytes(b"someone else's file")
        foreign["path"] = published

    with Stages() as stages:
        stages.on("archive-before-finish", replace)
        with pytest.raises(ArchiveRejected) as failed:
            run.export()

    assert failed.value.failure_class == "publish-substituted"
    assert isinstance(failed.value.cleanup, IncompleteCleanup)
    assert foreign["path"].read_bytes() == b"someone else's file"
    assert archive_of(run.receipt())["cleanup"] == "incomplete"


def test_a_final_name_swapped_for_a_symlink_before_the_record_is_left_untouched(tmp_path: Path) -> None:
    run = start_run(tmp_path)
    target = tmp_path / "target"
    target.write_bytes(b"target")

    def swap() -> None:
        published = single(run.exports_directory, "*.tar")
        published.unlink()
        published.symlink_to(target)

    with Stages() as stages:
        stages.on("archive-before-finish", swap)
        with pytest.raises(ArchiveRejected) as failed:
            run.export()

    assert failed.value.failure_class == "publish-substituted"
    assert target.read_bytes() == b"target"
    assert single(run.exports_directory, "*.tar").is_symlink()


def test_a_pre_existing_final_name_is_a_collision_and_stays_byte_identical(tmp_path: Path) -> None:
    run = start_run(tmp_path)
    planted = {}

    def plant() -> None:
        (staging,) = [path for path in run.exports_directory.iterdir() if path.is_dir()]
        final = run.exports_directory / f"{staging.name}.tar"
        final.write_bytes(b"pre-existing")
        planted["final"] = final
        planted["identity"] = (final.stat().st_dev, final.stat().st_ino)

    with Stages() as stages:
        stages.on("archive-after-verify", plant)
        with pytest.raises(ArchiveRejected) as failed:
            run.export()

    final = planted["final"]
    assert failed.value.failure_class == "publish-collision"
    assert final.read_bytes() == b"pre-existing"
    assert (final.stat().st_dev, final.stat().st_ino) == planted["identity"]
    assert failed.value.cleanup is None
    assert archive_of(run.receipt())["cleanup"] == "complete"


def test_a_swapped_export_namespace_fails_the_record_and_rolls_back_through_the_held_descriptor(
    tmp_path: Path,
) -> None:
    run = start_run(tmp_path)
    moved = run.handle.private_root / "moved-exports"  # the same filesystem as `exports`

    def swap() -> None:
        run.exports_directory.rename(moved)
        run.exports_directory.mkdir(mode=0o700)

    with Stages() as stages:
        stages.on("archive-before-finish", swap)
        with pytest.raises(ArchiveRejected) as failed:
            run.export()

    assert failed.value.failure_class == "publish-substituted"
    assert list(moved.glob("*.tar")) == []  # the verified link was removed through the held descriptor
    assert str(moved) not in str(failed.value)
    assert archive_of(run.receipt())["failure"] == "publish-substituted"


@pytest.mark.parametrize("stage", ["finish-record-replaced", "finish-before-replace"])
def test_an_uncertain_commit_is_resolved_from_the_record(tmp_path: Path, stage: str) -> None:
    run = start_run(tmp_path)

    def fail() -> None:
        raise OSError("synthetic commit failure")

    with Stages() as stages:
        stages.on(stage, fail)
        with pytest.raises(InfrastructureFailure) as failed:
            run.export()

    receipt = run.receipt()
    if stage == "finish-record-replaced":
        assert failed.value.reason == "archive-commit-unconfirmed"
        assert receipt["state"] == "promoted"
        assert Path(archive_of(receipt)["path"]).is_file()
        assert archive_of(receipt)["sha256"] in str(failed.value)
    else:
        _failed(run, "io-error")
        assert receipt["state"] == "failed"


def test_not_started_when_the_export_directory_is_not_private(tmp_path: Path) -> None:
    run = start_run(tmp_path)

    with Stages() as stages:
        stages.on("archive-after-append", lambda: run.exports_directory.chmod(0o755))
        with pytest.raises(ResourceError):
            run.export()

    run.exports_directory.chmod(0o700)
    assert run.fake.exports == []
    _failed(run, "not-started", exit_code=5)


# --- races with a reconciler ---------------------------------------------------------------------------------------


def test_the_dispatcher_that_wins_is_not_disturbed_by_a_late_reconciler(tmp_path: Path) -> None:
    run = start_run(tmp_path)
    parked = Pause()
    reconciler = threading.Thread(target=run.reconcile, name="R")
    with Stages() as stages:
        hold = Pause()
        stages.on("archive-after-append", hold, thread="D")
        stages.on("archive-before-classify", parked, thread="R")
        dispatcher = Dispatcher(run)
        dispatcher.start()
        hold.wait_until_reached()
        reconciler.start()
        parked.wait_until_reached()
        hold.release.set()
        assert dispatcher.finish() is None
        parked.release.set()
        reconciler.join(30)

    receipt = run.receipt()
    assert receipt["state"] == "promoted" and Path(archive_of(receipt)["path"]).is_file()
    assert archive_of(receipt)["state"] == "published"


def test_a_reconciler_between_link_and_record_removes_the_final_and_the_dispatcher_reports_record_changed(
    tmp_path: Path,
) -> None:
    run = start_run(tmp_path)
    with Stages() as stages, observed_locks() as locks:
        hold = Pause()
        stages.on("archive-after-link", hold, thread="D")
        dispatcher = Dispatcher(run)
        dispatcher.start()
        hold.wait_until_reached()
        published = single(run.exports_directory, "*.tar")
        identity = (published.stat().st_dev, published.stat().st_ino)
        run.reconcile()
        hold.release.set()
        error = dispatcher.finish()

    assert locks.violations == [] and locks.events
    receipt = run.receipt()
    assert isinstance(error, ResourceError) and error.reason == "record-changed" and error.cleanup is None
    assert archive_of(receipt) == {
        "schema": 1,
        "state": "failed",
        "cleanup": "complete",
        "failure": "recovered-complete-unpublished",
        "final_identity": {"dev": identity[0], "ino": identity[1]},
    }
    assert run.staged() == []


def test_a_reconciler_before_execution_wins_and_the_dispatcher_publishes_nothing(tmp_path: Path) -> None:
    run = start_run(tmp_path)
    with Stages() as stages:
        hold = Pause()
        stages.on("archive-after-append", hold, thread="D")
        dispatcher = Dispatcher(run)
        dispatcher.start()
        hold.wait_until_reached()
        run.reconcile()
        hold.release.set()
        error = dispatcher.finish()

    assert isinstance(error, ResourceError) and error.reason == "record-changed"
    assert archive_of(run.receipt())["failure"] == "recovered-absent"
    assert run.staged() == []


@pytest.mark.parametrize("stage", ["archive-after-verify", "archive-after-walk"])
def test_a_reconciler_during_verification_makes_the_dispatcher_report_record_changed_not_substitution(
    tmp_path: Path, stage: str
) -> None:
    run = start_run(tmp_path)
    with Stages() as stages, observed_locks() as locks:
        hold = Pause()
        stages.on(stage, hold, thread="D")
        dispatcher = Dispatcher(run)
        dispatcher.start()
        hold.wait_until_reached()
        run.reconcile()
        hold.release.set()
        error = dispatcher.finish()

    assert locks.violations == [] and locks.events
    assert isinstance(error, ResourceError) and error.reason == "record-changed"
    assert not isinstance(error, ArchiveRejected)
    assert archive_of(run.receipt())["failure"] == "recovered-partial"
    assert archive_of(run.receipt())["final_identity"] is None
    assert run.staged() == []


def test_a_reconciler_that_removes_a_staging_file_mid_write_fails_the_writer_as_absent(tmp_path: Path) -> None:
    started = threading.Event()
    resume = threading.Event()

    def interrupted(output: Path) -> int:
        temporary = output.parent / ".archive.tar.partial"
        temporary.write_bytes(b"half")
        started.set()
        assert resume.wait(30)
        try:
            os.rename(temporary, output)  # the Docker client's atomicwriter rename
        except OSError:
            return 0  # the client ignores the failed close and exits 0 without a file
        return 0

    run = start_run(tmp_path, configure=lambda fake: setattr(fake, "save", interrupted))
    dispatcher = Dispatcher(run)
    dispatcher.start()
    assert started.wait(30)
    run.reconcile()
    resume.set()
    error = dispatcher.finish()

    assert isinstance(error, ArchiveRejected) and error.failure_class == "absent"
    assert archive_of(run.receipt())["failure"] == "recovered-partial"  # the reconciler's record wins
    assert run.staged() == []


def test_a_second_dispatcher_waiting_at_its_early_check_is_refused_at_the_locked_append(tmp_path: Path) -> None:
    run = start_run(tmp_path)
    with Stages() as stages:
        hold = Pause()
        stages.on("archive-after-early-check", hold, thread="B")
        second = Dispatcher(run, name="B")
        second.start()
        hold.wait_until_reached()
        run.export()
        hold.release.set()
        error = second.finish()

    assert isinstance(error, InvalidInput) and error.reason == "archive-exists"
    assert len(run.fake.exports) == 1
    assert len([item for item in run.record()["docker_receipts"].values() if item["kind"] == "image-save"]) == 1


# --- interrupted dispatchers (a crash leaves what a KeyboardInterrupt at the stage leaves) -------------------------


def _interrupted(run: ExportRun, stage: str) -> None:
    with Stages() as stages:
        stages.on(stage, interrupt)
        with pytest.raises(KeyboardInterrupt):
            run.export()
    assert run.receipt()["state"] == "pending"


@pytest.mark.parametrize(
    ("stage", "failure"),
    [
        ("archive-after-append", "recovered-absent"),
        ("archive-after-verify", "recovered-partial"),
        ("archive-after-link", "recovered-complete-unpublished"),
        ("archive-before-finish", "recovered-complete-unpublished"),
    ],
)
def test_recovery_classifies_an_interrupted_export_from_the_disk_and_deletes_what_it_left(
    tmp_path: Path, stage: str, failure: str
) -> None:
    run = start_run(tmp_path)
    _interrupted(run, stage)
    published = list(run.exports_directory.glob("*.tar"))

    run.reconcile()

    archive = archive_of(run.receipt())
    assert run.receipt()["state"] == "failed"
    assert archive["failure"] == failure and archive["cleanup"] == "complete"
    assert (archive["final_identity"] is not None) == bool(published)
    assert run.staged() == []
    assert run.record()["owned_resources"]["images"][run.image_id]["state"] == "present"  # recovery promotes nothing
    assert list(run.exports_directory.glob("*.tar")) == []


@pytest.mark.parametrize("stage", ["archive-after-record-failed", "archive-after-disposal"])
def test_a_reconciler_killed_after_recording_the_failure_leaves_a_deferred_deletion_that_the_next_pass_completes(
    tmp_path: Path, stage: str
) -> None:
    run = start_run(tmp_path)
    _interrupted(run, "archive-after-link")
    with Stages() as stages:
        stages.on(stage, interrupt)
        with pytest.raises(KeyboardInterrupt):
            run.reconcile()
    deferred = archive_of(run.receipt())["cleanup"]
    assert deferred in {"deferred"}

    run.reconcile()

    assert archive_of(run.receipt())["cleanup"] == "complete"
    assert run.staged() == []


def test_a_final_swapped_after_it_was_recorded_is_left_in_place_and_reported(tmp_path: Path) -> None:
    run = start_run(tmp_path)
    _interrupted(run, "archive-after-link")
    published = single(run.exports_directory, "*.tar")

    def swap() -> None:
        published.unlink()
        published.write_bytes(b"another file")

    with Stages() as stages:
        stages.on("archive-after-record-failed", swap)
        run.reconcile()

    assert published.read_bytes() == b"another file"
    assert archive_of(run.receipt())["cleanup"] == "incomplete"


def test_a_file_planted_at_the_derived_final_name_after_an_interruption_is_removed_by_recovery(tmp_path: Path) -> None:
    run = start_run(tmp_path)
    _interrupted(run, "archive-after-append")
    final = run.exports_directory / f"{run.receipt()['id']}.tar"
    final.write_bytes(b"planted")

    run.reconcile()

    assert archive_of(run.receipt())["failure"] == "recovered-complete-unpublished"
    assert not final.exists()


def test_a_fifo_at_the_final_name_is_removed_without_blocking(tmp_path: Path) -> None:
    run = start_run(tmp_path)
    _interrupted(run, "archive-after-append")
    final = run.exports_directory / f"{run.receipt()['id']}.tar"
    os.mkfifo(final)
    done = threading.Thread(target=run.reconcile, name="R")

    done.start()
    done.join(15)

    assert not done.is_alive(), "recovery blocked on a FIFO"
    assert archive_of(run.receipt())["failure"] == "recovered-complete-unpublished"
    assert not os.path.lexists(final)


def test_a_plan_that_names_other_paths_fails_as_record_invalid_and_touches_only_the_derived_paths(
    tmp_path: Path,
) -> None:
    run = start_run(tmp_path)
    _interrupted(run, "archive-after-append")
    elsewhere = tmp_path / "elsewhere.tar"
    elsewhere.write_bytes(b"keep")
    record = run.record()
    (receipt,) = [item for item in record["docker_receipts"].values() if item["kind"] == "image-save"]
    receipt["plan"]["final"] = str(elsewhere)
    lifecycle.atomic_json(run.handle._context.record, record)

    run.reconcile()

    assert run.receipt()["state"] == "failed"
    assert archive_of(run.receipt())["failure"] == "archive-record-invalid"
    assert elsewhere.read_bytes() == b"keep"
    assert run.staged() == []


def test_recovery_through_the_operator_path_fails_an_interrupted_export(tmp_path: Path) -> None:
    run = start_run(tmp_path)
    _interrupted(run, "archive-after-link")
    kill_guardian(run.handle._context)
    retire_actor(
        run.handle._context.project.state,
        current_identity(),
        dead_identity(),
        account=run.services.root / "admission",
    )
    lifecycle.configure_docker_factories(lambda: run.fake, lambda _identity: run.fake)

    status = lifecycle.recover_run(run.handle._context, _test_directory=run.services.root / "admission")

    assert status.state == "complete"

    assert run.receipt()["state"] == "failed"
    assert archive_of(run.receipt())["failure"] == "recovered-complete-unpublished"


def test_a_dispatch_failure_reports_its_cleanup_error_on_a_second_line_with_the_primary_exit_code() -> None:
    primary = ArchiveRejected("child-failed", "the Docker client exited with 3")
    assert dispatch_failure("docker.dispatch", primary) == (
        "Archive export failed (child-failed): the Docker client exited with 3.\n",
        5,
    )

    primary.cleanup = IncompleteCleanup("Export staging could not be removed completely.")
    text, exit_code = dispatch_failure("docker.dispatch", primary)

    assert text.splitlines() == [
        "Archive export failed (child-failed): the Docker client exited with 3.",
        "Export staging could not be removed completely.",
    ]
    assert exit_code == 5


def test_a_failure_that_cannot_be_recorded_keeps_the_primary_error_reports_it_and_leaves_the_receipt_for_recovery(
    tmp_path: Path,
) -> None:
    def fail() -> None:
        raise OSError("synthetic record failure")

    run = start_run(tmp_path, configure=lambda fake: setattr(fake, "save", lambda output: 0))
    with Stages() as stages:
        stages.on("finish-before-replace", fail)
        with pytest.raises(ArchiveRejected) as failed:
            run.export()

    assert failed.value.failure_class == "absent"
    assert isinstance(failed.value.cleanup, IncompleteCleanup) and "stays pending" in str(failed.value.cleanup)
    assert run.receipt()["state"] == "pending"
    assert run.staged() == []

    run.reconcile()

    assert run.receipt()["state"] == "failed" and archive_of(run.receipt())["failure"] == "recovered-absent"


def test_a_plan_that_names_other_paths_after_the_link_still_removes_the_derived_final(tmp_path: Path) -> None:
    run = start_run(tmp_path)
    _interrupted(run, "archive-after-link")
    derived = single(run.exports_directory, "*.tar")
    elsewhere = tmp_path / "elsewhere.tar"
    elsewhere.write_bytes(b"keep")
    record = run.record()
    (receipt,) = [item for item in record["docker_receipts"].values() if item["kind"] == "image-save"]
    receipt["plan"]["final"] = str(elsewhere)
    lifecycle.atomic_json(run.handle._context.record, record)

    run.reconcile()

    assert archive_of(run.receipt())["failure"] == "archive-record-invalid"
    assert archive_of(run.receipt())["cleanup"] == "complete"
    assert not derived.exists() and elsewhere.read_bytes() == b"keep"


def test_a_promoted_receipt_without_a_published_record_is_never_quoted(tmp_path: Path) -> None:
    run = start_run(tmp_path)
    run.export()
    record = run.record()
    (receipt,) = [item for item in record["docker_receipts"].values() if item["kind"] == "image-save"]
    receipt["plan"]["archive"] = {"schema": 1, "state": "failed", "cleanup": "complete", "failure": "io-error"}
    lifecycle.atomic_json(run.handle._context.record, record)

    with pytest.raises(ResourceError) as refused:
        run.export()

    assert refused.value.reason == "archive-record-invalid"


def test_an_incomplete_cleanup_error_exits_seven() -> None:
    text, exit_code = dispatch_failure(
        "docker.dispatch", IncompleteCleanup("Docker mutation may have created or changed an unbound resource.")
    )

    assert exit_code == 7 and text.count("\n") == 1


def test_a_failure_and_a_staging_removal_that_both_fail_are_reported_together(tmp_path: Path) -> None:
    def refuse(exports: int, name: str, held: int) -> None:
        raise OSError("synthetic removal failure")

    def fail() -> None:
        raise OSError("synthetic record failure")

    run = start_run(tmp_path, configure=lambda fake: setattr(fake, "save", lambda output: 3))
    with Stages() as stages:
        stages.on("finish-before-replace", fail)
        with pytest.raises(ArchiveRejected) as failed:
            run.export(_remove_tree=refuse)

    assert failed.value.failure_class == "child-failed"
    assert isinstance(failed.value.cleanup, IncompleteCleanup)
    assert "staging" in str(failed.value.cleanup) and "stays pending" in str(failed.value.cleanup)
    assert run.receipt()["state"] == "pending"


def _on_podman(fake) -> None:
    fake.engine = "podman"  # a Podman-bound adapter: its image inspect has no Descriptor and no Variant
    fake.descriptor = None


def test_a_podman_export_publishes_the_docker_archive_layout(tmp_path: Path) -> None:
    from tests.archive_fixtures import DockerArchive

    layout = DockerArchive()
    run = start_run(tmp_path, layout, configure=_on_podman)

    line = json.loads(run.export().stdout)

    archive = archive_of(run.receipt())
    assert line["layout"] == archive["layout"] == "docker-archive"
    assert line["root_digest"] == line["config_digest"] == layout.image_id == run.image_id
    assert archive["manifest_digest"] == f"sha256:{hashlib.sha256(layout.manifest).hexdigest()}"


def test_a_docker_daemon_without_descriptors_is_still_refused(tmp_path: Path) -> None:
    from tests.archive_fixtures import DockerArchive

    run = start_run(tmp_path, DockerArchive(), configure=lambda fake: setattr(fake, "descriptor", None))

    with pytest.raises(ResourceError) as refused:
        run.export()

    assert refused.value.reason == "unsupported-image-store"
