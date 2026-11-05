"""Image and daemon platforms as pure values (design 4.1)."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class Platform:
    """Name an image platform with ``arm64/v8`` normalized to no variant."""

    os: str
    architecture: str
    variant: str | None = None

    @classmethod
    def from_fields(cls, os_name: object, architecture: object, variant: object = None) -> Platform | None:
        """Return the platform of the given OCI fields, or ``None`` when they are malformed. Never raises."""
        if not isinstance(os_name, str) or not isinstance(architecture, str) or not os_name or not architecture:
            return None
        if variant is not None and not isinstance(variant, str):
            return None
        if not variant or (architecture == "arm64" and variant == "v8"):
            variant = None
        return cls(os_name, architecture, variant)

    def as_dict(self) -> dict[str, str]:
        """Return the closed record form."""
        record = {"os": self.os, "architecture": self.architecture}
        if self.variant is not None:
            record["variant"] = self.variant
        return record


_ENGINE_ARCHITECTURES = {"x86_64": "amd64", "aarch64": "arm64", "amd64": "amd64", "arm64": "arm64"}
_PLATFORM_PART = re.compile(r"[a-z0-9._-]+")

Emulation = Literal["native", "emulated", "unknown"]


def daemon_platform(os_type: object, machine: object) -> Platform | None:
    """Map the Engine ``/info`` ``OSType`` and ``Architecture`` to the daemon's platform; ``None`` when unmapped.

    ``Architecture`` is ``uname -m`` shaped (``x86_64``, ``aarch64``); the already-OCI spellings are accepted too.
    """
    if not isinstance(os_type, str) or not os_type or not isinstance(machine, str):
        return None
    architecture = _ENGINE_ARCHITECTURES.get(machine)
    return None if architecture is None else Platform(os_type, architecture)


def split_requested_platform(value: str) -> tuple[str, ...] | None:
    """Return the ``os``, ``architecture`` and optional ``variant`` of exactly one requested platform, else ``None``.

    This is the one grammar of a requested platform: two or three lowercase ``[a-z0-9._-]+`` parts and no comma.
    """
    parts = tuple(value.split("/"))
    if "," in value or len(parts) not in {2, 3} or not all(_PLATFORM_PART.fullmatch(part) for part in parts):
        return None
    return parts


def parse_requested_platform(value: str) -> Platform | None:
    """Parse one requested ``--platform`` value; ``None`` for a list, a malformed value or an empty one."""
    parts = split_requested_platform(value)
    return None if parts is None else Platform.from_fields(*parts)


def classify_emulation(daemon: Platform | None, target: Platform | None, local: bool) -> Emulation:
    """Say whether running ``target`` on ``daemon`` is native, emulated or unknown.

    Only the operating system and architecture are compared; a variant is informational. A daemon that is not local
    (or any unknown side) is ``unknown``.
    """
    if not local or daemon is None or target is None:
        return "unknown"
    same = (daemon.os, daemon.architecture) == (target.os, target.architecture)
    return "native" if same else "emulated"
