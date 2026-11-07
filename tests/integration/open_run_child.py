"""Open, enter and leave one run in its own process:
``python -P open_run_child.py STATE PROJECT CONTROL_DIR NAME``.

The program is one of the two processes of the test: it calls the public ``open_run`` facade against a private
test state and talks to the parent only through files in ``CONTROL_DIR``, never through sleeps:
``NAME.ready`` once imported, then it waits for ``NAME.go``, records the wall-clock instant it calls ``open_run`` in
``NAME.call`` and writes ``NAME.opened``; it waits for ``NAME.enter-go`` before ``enter`` and writes ``NAME.entered``
once granted; it waits for ``NAME.release`` before ``leave`` and writes ``NAME.result.json`` last. Every wait has a
30 s cap.
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path


def _wait_for(path: Path) -> None:
    deadline = time.monotonic() + 30
    while not path.exists():
        if time.monotonic() > deadline:
            raise TimeoutError(f"{path.name} never appeared")
        time.sleep(0.02)


def main(argv: list[str]) -> int:
    from docker_test_toolkit import PhaseBudget, RunSpec
    from docker_test_toolkit.application.lifecycle import source_identity
    from docker_test_toolkit.domain.models import SUPPORTED_PROTOCOLS, TOOLKIT_VERSION, ArtifactIdentity
    from docker_test_toolkit.factories.api import open_run

    state, project, control, name = Path(argv[0]), Path(argv[1]), Path(argv[2]), argv[3]
    error: BaseException | None = None
    final = None
    try:
        artifact = ArtifactIdentity(TOOLKIT_VERSION, source_identity(), dict(SUPPORTED_PROTOCOLS))
        spec = RunSpec(
            project_root=project,
            entrypoint="integration",
            purpose=name,
            services=(),
            closure=(),
            profile="fixture",
            phases={"tests": PhaseBudget(0, 1, 1, 1)},
            initial_phase="tests",
            artifact=artifact,
            wait_seconds=60,
        )
        (control / f"{name}.ready").write_text("")
        _wait_for(control / f"{name}.go")
        (control / f"{name}.call").write_text(str(time.time_ns()))
        handle = open_run(spec, _test_state=state)
        (control / f"{name}.opened").write_text("")
        _wait_for(control / f"{name}.enter-go")
        handle.enter()
        (control / f"{name}.entered").write_text("")
        _wait_for(control / f"{name}.release")
        final = handle.leave().state
    except BaseException as caught:  # the parent test reads the outcome from the result file
        error = caught
    (control / f"{name}.result.json").write_text(
        json.dumps(
            {
                "final_state": final,
                "error_type": None if error is None else type(error).__name__,
                "message": None if error is None else str(error),
            }
        )
    )
    return 0 if error is None else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
