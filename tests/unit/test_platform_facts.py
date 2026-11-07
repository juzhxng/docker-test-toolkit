"""Platform facts: the domain value, the daemon and requested platform parsers and the emulation rule."""

from __future__ import annotations

import pytest

from docker_test_toolkit.application.image_archive import ArchiveRejected, normalized_platform
from docker_test_toolkit.domain.platform import Platform, classify_emulation, daemon_platform, parse_requested_platform


def test_from_fields_normalizes_arm64_v8_to_no_variant() -> None:
    assert Platform.from_fields("linux", "arm64", "v8") == Platform("linux", "arm64")
    assert Platform.from_fields("linux", "amd64", "v2") == Platform("linux", "amd64", "v2")
    assert Platform.from_fields("linux", "amd64", "") == Platform("linux", "amd64")


@pytest.mark.parametrize(
    ("os_name", "architecture", "variant"),
    [(None, "arm64", None), ("linux", None, None), ("", "arm64", None), ("linux", "", None), ("linux", "arm64", 3)],
)
def test_from_fields_returns_none_for_malformed_fields(os_name: object, architecture: object, variant: object) -> None:
    assert Platform.from_fields(os_name, architecture, variant) is None


def test_as_dict_is_the_closed_record_form() -> None:
    assert Platform("linux", "arm64").as_dict() == {"os": "linux", "architecture": "arm64"}
    assert Platform("linux", "amd64", "v2").as_dict() == {"os": "linux", "architecture": "amd64", "variant": "v2"}


def test_the_archive_wrapper_keeps_its_rejection() -> None:
    assert normalized_platform("linux", "arm64", "v8") == Platform("linux", "arm64")
    with pytest.raises(ArchiveRejected) as raised:
        normalized_platform(None, "arm64")
    assert raised.value.failure_class == "verification-failed"


@pytest.mark.parametrize(
    ("os_type", "machine", "expected"),
    [
        ("linux", "x86_64", Platform("linux", "amd64")),
        ("linux", "aarch64", Platform("linux", "arm64")),
        ("linux", "amd64", Platform("linux", "amd64")),
        ("linux", "arm64", Platform("linux", "arm64")),
        ("linux", "riscv64", None),
        ("linux", "", None),
        ("", "x86_64", None),
        (None, "x86_64", None),
        ("linux", 3, None),
    ],
)
def test_daemon_platform_maps_engine_info_spellings(
    os_type: object, machine: object, expected: Platform | None
) -> None:
    assert daemon_platform(os_type, machine) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param("linux/amd64/v2", Platform("linux", "amd64", "v2"), id="linux/amd64/v2-expected2"),
        ("", None),
    ],
)
def test_parse_requested_platform_accepts_exactly_one_platform(value: str, expected: Platform | None) -> None:
    assert parse_requested_platform(value) == expected


def test_classify_emulation_compares_os_and_architecture_only() -> None:
    arm, amd = Platform("linux", "arm64"), Platform("linux", "amd64")

    assert classify_emulation(arm, amd, True) == "emulated"
    assert classify_emulation(arm, Platform("linux", "arm64", "v8"), True) == "native"  # variants are informational
    assert classify_emulation(amd, Platform("linux", "amd64", "v2"), True) == "native"
    assert classify_emulation(arm, amd, False) == "unknown"  # a remote daemon: the guarantee does not apply
    assert classify_emulation(None, amd, True) == "unknown"
    assert classify_emulation(arm, None, True) == "unknown"
    assert classify_emulation(arm, Platform("windows", "arm64"), True) == "emulated"
