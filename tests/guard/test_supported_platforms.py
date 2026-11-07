"""The toolkit's own lock declares the wheel baselines of every supported host (umbrella record, guard G3).

``SUPPORTED_HOSTS`` are the machines the toolkit runs on; ``WHEEL_BASELINES`` are the platforms ``uv lock`` must prove
a wheel for. They are two different lists: the darwin baseline is the host narrowed by ``platform_release`` (the oldest
macOS the wheels target). A guard comparing them for equality would reject the valid shape. The toolkit has no
accelerator variant: it is CPU only, which the extras check below enforces.
"""

from __future__ import annotations

import copy
import tomllib
from pathlib import Path
from typing import Any

import pytest

PYPROJECT = Path(__file__).parents[2] / "pyproject.toml"

SUPPORTED_HOSTS = (
    "sys_platform == 'darwin' and platform_machine == 'arm64'",
    "sys_platform == 'linux' and platform_machine == 'x86_64'",
    "sys_platform == 'linux' and platform_machine == 'aarch64'",
)
WHEEL_BASELINES = (
    "sys_platform == 'darwin' and platform_machine == 'arm64' and platform_release == '23.0.0'",
    "sys_platform == 'linux' and platform_machine == 'x86_64'",
    "sys_platform == 'linux' and platform_machine == 'aarch64'",
)
ACCELERATOR_EXTRAS = frozenset({"cuda", "mps", "mlx", "st", "st-cpu"})


def platform_problems(pyproject: dict[str, Any]) -> list[str]:
    """Return how ``pyproject``'s ``[tool.uv]`` platform declaration departs from the supported shape."""
    uv = pyproject.get("tool", {}).get("uv", {})
    problems = []
    if tuple(uv.get("required-environments", ())) != WHEEL_BASELINES:
        problems.append("required-environments must equal WHEEL_BASELINES")
    if "environments" in uv and tuple(uv["environments"]) != SUPPORTED_HOSTS:
        problems.append("environments must be absent or equal SUPPORTED_HOSTS")
    for host, baseline in zip(SUPPORTED_HOSTS, WHEEL_BASELINES, strict=True):
        narrowed = host + " and platform_release == '23.0.0'"
        if baseline not in (host, narrowed) or (baseline == narrowed) != ("darwin" in host):
            problems.append(f"baseline {baseline!r} is not its host narrowed only by platform_release on darwin")
    extras = set(pyproject.get("project", {}).get("optional-dependencies", {})) | set(
        pyproject.get("dependency-groups", {})
    )
    if extras & ACCELERATOR_EXTRAS:
        problems.append("the toolkit is CPU only: no accelerator extra or group")
    return problems


def _valid() -> dict[str, Any]:
    return {"tool": {"uv": {"required-environments": list(WHEEL_BASELINES)}}}


def test_the_toolkit_lock_declares_its_wheel_baselines() -> None:
    assert platform_problems(tomllib.loads(PYPROJECT.read_text())) == []


def test_the_declaration_helper_accepts_the_supported_shapes() -> None:
    both = _valid()
    both["tool"]["uv"]["environments"] = list(SUPPORTED_HOSTS)

    assert platform_problems(_valid()) == []
    assert platform_problems(both) == []  # environments (hosts) and required-environments (baselines) differ


@pytest.mark.parametrize(
    "mutate",
    (
        lambda uv: uv.update(
            {"environments": list(WHEEL_BASELINES)}
        ),  # the hosts narrowed would drop macOS 13 and older
        lambda uv: uv.pop("required-environments"),
        lambda uv: uv.update({"required-environments": list(SUPPORTED_HOSTS)}),  # darwin without its platform_release
        lambda uv: uv.update({"required-environments": list(WHEEL_BASELINES[:2])}),
        lambda uv: uv.update({"required-environments": [*WHEEL_BASELINES, "sys_platform == 'win32'"]}),
        lambda uv: uv.update({"environments": [*SUPPORTED_HOSTS, "sys_platform == 'win32'"]}),
    ),
    ids=["environments-narrowed", "missing", "no-release", "dropped-linux-arm", "extra-baseline", "extra-host"],
)
def test_the_declaration_helper_rejects_a_departure(mutate: Any) -> None:
    pyproject = copy.deepcopy(_valid())
    mutate(pyproject["tool"]["uv"])

    assert platform_problems(pyproject)


def test_the_declaration_helper_rejects_an_accelerator_extra() -> None:
    pyproject = _valid()
    pyproject["project"] = {"optional-dependencies": {"cuda": ["torch"]}}

    assert platform_problems(pyproject)
