"""Which shell commands change Docker state directly, past the toolkit (through `docker` or `podman`).

Coding assistants read Docker state freely but change it only through the toolkit (the repository adapter or the
`docker-test-toolkit` command). This policy reads a shell command's text and names the first direct Docker change in it.
It looks through command lists, pipes, command substitution, environment and wrapper prefixes, `ssh <host> ...` and
`sh -c ...`. It reads text only, so it is a guard rail, not a sandbox: a script that calls Docker itself passes.
"""

import re
import shlex

OVERRIDE_MARKER = "docker-direct-ok"

_READ_ONLY = frozenset(
    {
        "ps",
        "images",
        "inspect",
        "logs",
        "info",
        "version",
        "stats",
        "top",
        "port",
        "events",
        "diff",
        "history",
        "search",
        "help",
    }
)
_READ_ONLY_GROUPS = {
    "container": frozenset({"ls", "list", "ps", "inspect", "logs", "stats", "top", "port", "diff"}),
    "image": frozenset({"ls", "list", "inspect", "history"}),
    "network": frozenset({"ls", "list", "inspect"}),
    "volume": frozenset({"ls", "list", "inspect"}),
    "system": frozenset({"df", "info", "events"}),
    "context": frozenset({"ls", "list", "show", "inspect"}),
    "buildx": frozenset({"ls", "du", "inspect", "version", "history", "imagetools"}),
    "builder": frozenset({"ls", "du", "inspect"}),
    "compose": frozenset({"ps", "ls", "logs", "config", "version", "images", "top", "port", "events"}),
    "manifest": frozenset({"inspect"}),
    "machine": frozenset({"ls", "list", "inspect", "info"}),  # Podman's machines: a reset or removal is a change
}
_ENGINES = {"docker": "docker", "docker-compose": "docker", "podman": "podman", "podman-compose": "podman"}
# Global options that take a value, so the value is not mistaken for the subcommand.
_VALUE_OPTIONS = frozenset(
    {
        "-H",
        "--host",
        "-c",
        "--context",
        "--config",
        "-l",
        "--log-level",
        "--tlscacert",
        "--tlscert",
        "--tlskey",
        "-f",
        "--file",
        "-p",
        "--project-name",
        "--project-directory",
        "--env-file",
        "--profile",
        "--builder",
        "--ansi",
        "--progress",
        "--parallel",
    }
)
_WRAPPERS = frozenset({"sudo", "command", "exec", "nice", "nohup", "time", "env"})
_SHELLS = frozenset({"sh", "bash", "zsh", "dash"})
# Wrapper options that take a separate value (``--name=value`` forms carry it in the same word).
_WRAPPER_VALUE_OPTIONS = {
    "sudo": frozenset({"-u", "-g", "-C", "-D", "-h", "-p", "-r", "-t", "-U", "--user", "--group", "--chdir"}),
    "env": frozenset({"-u", "-C", "-S", "--unset", "--chdir", "--split-string"}),
    "nice": frozenset({"-n", "--adjustment"}),
    "timeout": frozenset({"-s", "-k", "--signal", "--kill-after"}),
}
_SEPARATORS = re.compile(r"\|\||&&|[;|&\n]|\$\(|`")
_ASSIGNMENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*=.*")
_HEREDOC = re.compile(r"(?<!<)<<(?!<)-?\s*(['\"]?)([A-Za-z_][A-Za-z0-9_]*)\1")


def direct_docker_change(command: str) -> str | None:
    """Return the first direct Docker change in a shell command, such as ``docker run``, or None.

    A command carrying the override marker ``docker-direct-ok`` returns None; only the user's explicit go may add it.
    """
    if ("docker" not in command and "podman" not in command) or OVERRIDE_MARKER in command:
        return None
    return _first_change(_without_heredoc_bodies(command))


def _without_heredoc_bodies(command: str) -> str:
    """Drop here-document bodies: they are data written to a file or a program, not commands of this shell."""
    kept: list[str] = []
    ends: list[str] = []
    for line in command.split("\n"):
        if ends:
            if line.strip() == ends[0]:
                ends.pop(0)
            continue
        kept.append(line)
        ends.extend(match.group(2) for match in _HEREDOC.finditer(line))
    return "\n".join(kept)


def _first_change(command: str) -> str | None:
    for segment in _SEPARATORS.split(command):
        change = _segment_change(_words(segment))
        if change is not None:
            return change
    return None


def _words(segment: str) -> list[str]:
    try:
        return shlex.split(segment, comments=True)
    except ValueError:  # a quote split across segments; plain words still name the program
        return segment.split()


def _segment_change(words: list[str]) -> str | None:
    words = _without_prefix(words)
    if not words:
        return None
    program = words[0].rsplit("/", 1)[-1]
    if program == "ssh":
        remote = _operands(words[1:])[1:]
        return _first_change(" ".join(remote)) if remote else None
    if program in _SHELLS:
        # ``-c`` alone or in an option cluster (``-lc``, ``-ec``): the next word is the script this shell runs
        cluster = next((i for i, w in enumerate(words[1:], 1) if w[:1] == "-" and w[1:2] != "-" and "c" in w), None)
        if cluster is None:
            return None
        script = cluster + 1 + (cluster + 1 < len(words) and words[cluster + 1] == "--")  # ``bash -c -- 'script'``
        return _first_change(words[script]) if script < len(words) else None
    engine = _ENGINES.get(program)
    if engine is None:
        return None
    args = _operands(words[1:])
    if program.endswith("-compose"):
        args = ["compose", *args]
    if not args or args[0] in _READ_ONLY:
        return None
    group = _READ_ONLY_GROUPS.get(args[0])
    if group is None:
        return f"{engine} {args[0]}"
    if len(args) > 1 and args[1] in group:
        return None
    return " ".join([engine, *args[:2]])


def _without_prefix(words: list[str]) -> list[str]:
    """Drop environment assignments and wrapper commands with their options (``sudo -n -u x``, ``env -i``,
    ``nice -n 5``, ``timeout -s KILL 30``), so the wrapped program is the first word."""
    while words:
        name = words[0].rsplit("/", 1)[-1]
        if _ASSIGNMENT.fullmatch(words[0]):
            words = words[1:]
        elif name in _WRAPPERS or name == "timeout":
            words = _after_options(words[1:], _WRAPPER_VALUE_OPTIONS.get(name, frozenset()))
            if name == "timeout":
                words = words[1:]  # its duration
        else:
            break
    return words


def _after_options(words: list[str], value_options: frozenset[str]) -> list[str]:
    """Skip a wrapper's options, and the value of each one that takes a value."""
    while words and words[0].startswith("-") and words[0] != "-":
        option, words = words[0], words[1:]
        if option == "--":
            break
        if option in value_options or _takes_next_word(option, value_options):
            words = words[1:]
    return words


def _takes_next_word(option: str, value_options: frozenset[str]) -> bool:
    """Whether a short-option cluster's value is the next word: read left to right, the first value-taking letter
    takes the rest of the word as its value (``-uFOO``), or the next word when it is the cluster's last (``-nu x``)."""
    if option[:1] != "-" or option[1:2] == "-":
        return False
    for index, letter in enumerate(option[1:], 1):
        if f"-{letter}" in value_options:
            return index == len(option) - 1
    return False


def _operands(words: list[str]) -> list[str]:
    operands, skip_value = [], False
    for word in words:
        if skip_value:
            skip_value = False
        elif word.startswith("-"):
            skip_value = word in _VALUE_OPTIONS
        else:
            operands.append(word)
    return operands
