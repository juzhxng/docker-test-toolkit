# Docker Test Toolkit

When asked to onboard, re-adopt, or operate a repository with this toolkit, use the
`docker-test-toolkit` skill in `.agents/skills/docker-test-toolkit/SKILL.md` .

Agents without skill support should follow `docs/adopters.md` for integrating a repository and `docs/operations.md` for
ordinary operation. Do not load those guides for unrelated toolkit source changes.

This repository lives on GitHub (`juzhxng/docker-test-toolkit`); CI results are read with `gh run list` and
`gh run view <id> --log`; the logs carry each test's duration. Never commit
personal data, private hostnames or host details: the gitleaks pre-commit hook checks secrets with the repository
rules, and the `scan` job checks every commit with them plus the maintainer's private term list.

Develop with `uv sync` and `uv run poe qa`. Releases are described in `docs/maintainers.md`.
