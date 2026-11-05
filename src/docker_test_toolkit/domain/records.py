"""Closed record parsing primitives shared by lifecycle services."""

import hashlib
import json
import re
from collections.abc import Iterable
from typing import Any

from docker_test_toolkit.domain.errors import ResourceError, ToolkitError
from docker_test_toolkit.domain.models import ArtifactIdentity, ExternalDockerResource
from docker_test_toolkit.domain.policy import NAME
from docker_test_toolkit.domain.telemetry import CATEGORIES, OBSERVERS

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_IMAGE_ID = re.compile(r"sha256:[0-9a-f]{64}\Z")
_IMAGE_REFERENCE = re.compile(r"[^\s@]+@sha256:[0-9a-f]{64}\Z")
_UUID = re.compile(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}\Z")


def canonical_json_bytes(record: object) -> bytes:
    """Encode one stable JSON record for hashing and durable protocol binding."""
    try:
        return (json.dumps(record, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()
    except (RecursionError, TypeError, ValueError) as error:
        raise ResourceError("Protocol record is not canonical JSON.") from error


def _digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def record_cell_id(kind: str, path: str, content_sha256: str) -> str:
    """Return the stable cell id of one inspected record: its kind and a digest of its path and content."""
    identity = {"content_sha256": content_sha256, "kind": kind, "path": path}
    return f"{kind}:{_digest(canonical_json_bytes(identity))}"


def _artifact_from_record(value: object, label: str) -> ArtifactIdentity:
    if not isinstance(value, dict) or set(value) != {"protocols", "sha256", "version"}:
        raise ResourceError(f"{label} artifact identity is incomplete.")
    try:
        return ArtifactIdentity(**value)
    except (TypeError, ValueError, ToolkitError) as error:
        raise ResourceError(f"{label} artifact identity is invalid.") from error


def unique_object(pairs: Iterable[tuple[str, Any]]) -> dict[str, Any]:
    """Build a JSON object while refusing duplicate fields at every depth."""
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON field")
        result[key] = value
    return result


def validate_public_run_metadata(artifact: object, external_resources: object) -> None:
    """Validate persisted artifact and declared external-resource record fields."""
    if artifact is not None:
        _artifact_from_record(artifact, "Run")
    if not isinstance(external_resources, list):
        raise ResourceError("External Docker declarations are invalid.")
    try:
        resources = tuple(ExternalDockerResource(**item) for item in external_resources)
    except (TypeError, ValueError, ToolkitError) as error:
        raise ResourceError("External Docker declaration is invalid.") from error
    identities = {(item.kind, item.identifier) for item in resources}
    if len(identities) != len(resources):
        raise ResourceError("External Docker declarations contain duplicate identities.")


def _validate_authority(authority: object, data: dict[str, Any]) -> None:
    if authority is None:
        return
    fields = {
        "builder",
        "client_context",
        "client_device",
        "client_inode",
        "client_mtime_ns",
        "client_path",
        "client_sha256",
        "client_size",
        "client_version",
        "compose_project",
        "daemon_id",
        "descriptor_path",
        "descriptor_sha256",
        "dispatcher_path",
        "dispatcher_sha256",
        "generation",
        "shim_path",
        "shim_sha256",
    }
    if not isinstance(authority, dict) or set(authority) != fields:
        raise ResourceError("Docker execution authority is incomplete.")
    path_fields = ("client_path", "descriptor_path", "dispatcher_path", "shim_path")
    if (
        authority["generation"] != data["generation"]
        or authority["daemon_id"] != (data.get("grant") or data.get("retained_grant") or {}).get("daemon_id")
        or any(
            type(authority[key]) is not int or authority[key] < 0
            for key in ("client_device", "client_inode", "client_mtime_ns", "client_size")
        )
        or any(not isinstance(authority[key], str) or not authority[key] for key in path_fields)
        or any(
            not _SHA256.fullmatch(authority[key])
            for key in ("client_sha256", "descriptor_sha256", "dispatcher_sha256", "shim_sha256")
        )
        or not isinstance(authority["client_version"], str)
        or not authority["client_version"]
        or (authority["client_context"] is not None and not isinstance(authority["client_context"], str))
        or not isinstance(authority["compose_project"], str)
        or not isinstance(authority["builder"], str)
    ):
        raise ResourceError("Docker execution authority is contradictory.")


def _validate_owned_resources(resources: object) -> None:
    if not isinstance(resources, dict) or set(resources) != {"builders", "containers", "images", "networks", "volumes"}:
        raise ResourceError("Owned Docker resource record is incomplete.")
    for plural, items in resources.items():
        if not isinstance(items, dict):
            raise ResourceError("Owned Docker resource inventory is invalid.")
        expected_kind = plural[:-1] if plural != "builders" else "builder"
        for key, item in items.items():
            exact_identity = isinstance(key, str) and (
                bool(_SHA256.fullmatch(key))
                if expected_kind in {"container", "network"}
                else bool(_IMAGE_ID.fullmatch(key))
                if expected_kind == "image"
                else bool(NAME.fullmatch(key))
            )
            if (
                not isinstance(key, str)
                or not key
                or not exact_identity
                or not isinstance(item, dict)
                or set(item) != {"evidence", "identifier", "kind", "name", "receipt_id", "state"}
                or item["kind"] != expected_kind
                or item["identifier"] != key
                or not isinstance(item["name"], str)
                or (not item["name"] if expected_kind == "image" else not NAME.fullmatch(item["name"]))
                or not _UUID.fullmatch(item["receipt_id"])
                or item["state"] not in {"present", "running", "stopped", "removed"}
                or not isinstance(item["evidence"], dict)
            ):
                raise ResourceError("Owned Docker resource identity is contradictory.")


def _validate_image_bindings(bindings: object, owned_resources: object) -> None:
    if not isinstance(bindings, dict) or not isinstance(owned_resources, dict):
        raise ResourceError("Docker image bindings are invalid.")
    owned_images = owned_resources.get("images", {})
    for reference, binding in bindings.items():
        if (
            not isinstance(reference, str)
            or not _IMAGE_REFERENCE.fullmatch(reference)
            or not isinstance(binding, dict)
            or set(binding) != {"created", "identifier", "ownership"}
            or (binding["created"] is not None and (not isinstance(binding["created"], str) or not binding["created"]))
            or not isinstance(binding["identifier"], str)
            or not _IMAGE_ID.fullmatch(binding["identifier"])
            or binding["ownership"] not in {"external", "owned"}
            or (binding["ownership"] == "owned" and binding["identifier"] not in owned_images)
        ):
            raise ResourceError("Docker image binding is contradictory.")


def _validate_receipts(receipts: object, data: dict[str, Any]) -> None:
    if not isinstance(receipts, dict):
        raise ResourceError("Docker receipt journal is invalid.")
    pending = 0
    for receipt_id, receipt in receipts.items():
        if (
            not _UUID.fullmatch(receipt_id)
            or not isinstance(receipt, dict)
            or set(receipt)
            != {
                "deadline",
                "descriptor_sha256",
                "generation",
                "id",
                "kind",
                "plan",
                "resources",
                "result",
                "revision",
                "state",
            }
            or receipt["id"] != receipt_id
            or type(receipt["generation"]) is not int
            or not 0 < receipt["generation"] <= data["generation"]
            or type(receipt["revision"]) is not int
            or not 0 < receipt["revision"] <= data["revision"]
            or not isinstance(receipt["deadline"], dict)
            or set(receipt["deadline"]) != {"created_ns", "duration_ns", "expires_ns", "origin"}
            or not isinstance(receipt["deadline"]["origin"], str)
            or any(
                type(receipt["deadline"][key]) is not int or receipt["deadline"][key] < 0
                for key in ("created_ns", "duration_ns", "expires_ns")
            )
            or receipt["deadline"]["duration_ns"] == 0
            or receipt["deadline"]["expires_ns"]
            != receipt["deadline"]["created_ns"] + receipt["deadline"]["duration_ns"]
            or not _SHA256.fullmatch(receipt["descriptor_sha256"])
            or not isinstance(receipt["kind"], str)
            or not receipt["kind"]
            or not isinstance(receipt["plan"], dict)
            or receipt["state"] not in {"failed", "pending", "promoted", "removed", "unresolved"}
            or (receipt["state"] in {"pending", "unresolved"} and receipt["generation"] != data["generation"])
            or not isinstance(receipt["resources"], list)
            or any(not isinstance(item, str) or not item for item in receipt["resources"])
            or (
                receipt["result"] is not None
                and (
                    not isinstance(receipt["result"], dict)
                    or set(receipt["result"]) != {"category", "exit_code"}
                    or not isinstance(receipt["result"]["category"], str)
                    or type(receipt["result"]["exit_code"]) is not int
                )
            )
        ):
            raise ResourceError("Docker mutation receipt is incomplete or contradictory.")
        pending += receipt["state"] in {"pending", "unresolved"}
    if pending > 1:
        raise ResourceError("Multiple unresolved Docker mutation receipts are forbidden.")


def validate_docker_state(data: dict[str, Any]) -> None:
    """Validate private shim authority, exact resources, and mutation receipts."""
    validate_public_run_metadata(data.get("artifact"), data.get("external_resources"))
    _validate_authority(data.get("docker_authority"), data)
    _validate_owned_resources(data.get("owned_resources"))
    _validate_image_bindings(data.get("image_bindings"), data.get("owned_resources"))
    _validate_receipts(data.get("docker_receipts"), data)
    if data.get("docker_authority") is None and (data.get("docker_receipts") or any(data["owned_resources"].values())):
        raise ResourceError("Docker receipts or resources lack bound execution authority.")


_EVIDENCE_FIELDS = {"schema", "category", "reason", "detail", "observer", "first_observed", "waiting", "pressure"}
_EVIDENCE_OBSERVED_FIELDS = {"origin", "tick_ns", "state", "phase", "revision"}
_EVIDENCE_STATES = {"queued", "admitting", "active", "stopping"}
_EVIDENCE_WAITING_FIELDS = {"reason", "first_tick_ns", "last_tick_ns", "waited_ns"}
_EVIDENCE_PRESSURE_FIELDS = {
    "host_headroom_bytes",
    "host_swap_used_bytes",
    "host_swap_total_bytes",
    "swapout_bytes_per_s",
    "daemon_memory_bytes",
}


def _evidence_int(value: object) -> bool:
    return type(value) is int and value >= 0


def _valid_waiting_item(item: object) -> bool:
    """Return whether one waiting span has the closed shape shared by failure evidence and the hold timeline."""
    return (
        isinstance(item, dict)
        and set(item) == _EVIDENCE_WAITING_FIELDS
        and isinstance(item["reason"], str)
        and bool(item["reason"])
        and all(_evidence_int(item[key]) for key in ("first_tick_ns", "last_tick_ns", "waited_ns"))
        and item["last_tick_ns"] >= item["first_tick_ns"]
    )


def validate_failure_evidence(value: object) -> None:
    """Validate the shape of one ``failure_evidence`` record key; refuse anything else (design 4.3).

    The shape is validated and nothing more: the evidence names the first observed cause and may legitimately differ
    from the current ``infrastructure_failure``, which later recovery or cleanup writers overwrite.

    Raises:
        ResourceError: If the value is not exactly the closed evidence shape.
    """
    invalid = ResourceError("Run record failure evidence is invalid.")
    if not isinstance(value, dict) or set(value) != _EVIDENCE_FIELDS:
        raise invalid
    observed = value["first_observed"]
    admission = value["category"] == "admission-expired"
    if (
        value["schema"] != 1
        or type(value["schema"]) is not int
        or value["category"] not in CATEGORIES
        or value["observer"] not in OBSERVERS
        or not isinstance(value["reason"], str)
        or not value["reason"]
        or value["detail"] not in (None, "unclassified")
        or not isinstance(observed, dict)
        or set(observed) != _EVIDENCE_OBSERVED_FIELDS
        or not isinstance(observed["origin"], str)
        or not observed["origin"]
        or not _evidence_int(observed["tick_ns"])
        or not isinstance(observed["state"], str)
        or observed["state"] not in _EVIDENCE_STATES
        or not (observed["phase"] is None or isinstance(observed["phase"], str))
        or not _evidence_int(observed["revision"])
        or (value["waiting"] is not None) != admission
        or (value["pressure"] is not None and not admission)
    ):
        raise invalid
    if admission:
        waiting = value["waiting"]
        if not isinstance(waiting, list) or not waiting or not all(_valid_waiting_item(item) for item in waiting):
            raise invalid
        pressure = value["pressure"]
        if pressure is not None and (
            not isinstance(pressure, dict)
            or set(pressure) != _EVIDENCE_PRESSURE_FIELDS
            or not all(_evidence_int(item) for item in pressure.values())
        ):
            raise invalid


HOLD_TIMELINE_FIELDS = frozenset(
    {
        "schema",
        "origin",
        "accounting_from_ns",
        "reserved_ns",
        "granted_ns",
        "waiting",
        "release",
        "release_origin",
        "released_ns",
    }
)
_HOLD_RELEASES = frozenset({"leave", "recovery"})


def validate_hold_timeline(value: object) -> None:
    """Validate the shape of one ``hold_timeline`` record key (design 3.2); refuse anything else.

    The grant's ticks share ``origin``. The release carries its own origin, so its order against the grant is checked
    only when both origins are equal; a release recorded on a later boot is valid without an order.

    Raises:
        ResourceError: If the value is not exactly the closed hold-timeline shape.
    """
    invalid = ResourceError("Run record hold timeline is invalid.")
    if not isinstance(value, dict) or set(value) != HOLD_TIMELINE_FIELDS:
        raise invalid
    ticks = (value["accounting_from_ns"], value["reserved_ns"], value["granted_ns"])
    release, release_origin, released_ns = value["release"], value["release_origin"], value["released_ns"]
    if (
        type(value["schema"]) is not int
        or value["schema"] != 1
        or not isinstance(value["origin"], str)
        or not value["origin"]
        or not all(_evidence_int(tick) for tick in ticks)
        or not ticks[0] <= ticks[1] <= ticks[2]
        or not isinstance(value["waiting"], list)
        or not all(_valid_waiting_item(item) for item in value["waiting"])
    ):
        raise invalid
    if release is None and release_origin is None and released_ns is None:
        return
    if (
        not isinstance(release, str)  # before set membership: a JSON array or object is unhashable
        or release not in _HOLD_RELEASES
        or not isinstance(release_origin, str)
        or not release_origin
        or not _evidence_int(released_ns)
        or (release_origin == value["origin"] and released_ns < ticks[2])
    ):
        raise invalid
