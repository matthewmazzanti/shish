"""DSL types and builders for shell command construction."""

from __future__ import annotations

from collections.abc import Generator
from dataclasses import dataclass
from pathlib import Path

# Redirect source/sink types


@dataclass(frozen=True)
class FromFile:
    path: Path


@dataclass(frozen=True)
class FromData:
    data: Data


@dataclass(frozen=True)
class Sub:
    """Process substitution: <(cmd) becomes /dev/fd/N argument.

    Use from_proc() and to_proc() to create Sub instances.

    Note on fd numbering:
        Currently we use whatever fd numbers os.pipe() provides. If we later
        add explicit fd redirects (3>, >&3, etc.), we'll need to move Sub fds
        to high numbers (>= 10) using fcntl(F_DUPFD, 10) to avoid conflicts
        with user-specified fd numbers, similar to how bash handles this.
    """

    cmd: Cmd | Pipeline | Redirect
    write: bool


@dataclass(frozen=True)
class ToFile:
    path: Path
    append: bool = False


# Redirect source/sink type aliases
StdinSource = FromFile | FromData
StdoutSink = ToFile


class Cmd:
    """Immutable shell command builder with chainable syntax."""

    def __init__(self, args: list[Arg] | None = None) -> None:
        self.args: list[Arg] = args if args is not None else []

    def __getattr__(self, name: str) -> Cmd:
        """Chain subcommand: cmd.foo -> Cmd([...args, "foo"])."""
        return Cmd([*self.args, name])

    def __call__(
        self, *args: str | int | Path | Sub, **kwargs: str | int | bool
    ) -> Cmd:
        """Add args and flags: cmd("arg", flag=True, Sub(cmd))."""
        new_args: list[Arg] = list(self.args)
        for arg in args:
            if isinstance(arg, Sub):
                new_args.append(arg)
            else:
                new_args.append(str(arg))

        for key, value in kwargs.items():
            if value is False:
                continue
            flag = f"-{key}" if len(key) == 1 else f"--{key.replace('_', '-')}"
            if value is True:
                new_args.append(flag)
            else:
                new_args.extend([flag, str(value)])

        return Cmd(new_args)

    def __or__(self, other: Runnable) -> Pipeline:
        """Pipe to another command: cmd1 | cmd2 -> Pipeline."""
        return pipe(self, other)

    def __gt__(self, path: PathLike) -> Redirect:
        """cmd > "file" - redirect stdout to file."""
        return write(self, path)

    def __rshift__(self, path: PathLike) -> Redirect:
        """cmd >> "file" - append stdout to file."""
        return append(self, path)

    def __lt__(self, path: PathLike) -> Redirect:
        """cmd < "file" - read stdin from file."""
        return read(self, path)

    def __lshift__(self, data: Data) -> Redirect:
        """cmd << "data" - stdin from literal data."""
        return input_(self, data)

    def __await__(self) -> Generator[object, None, int]:
        from shish.runtime import run

        return run(self).__await__()


class Pipeline:
    """Immutable pipeline of commands."""

    def __init__(self, stages: list[Runnable]) -> None:
        # Flatten nested Pipelines
        self.stages: list[Stage] = []
        for stage in stages:
            if isinstance(stage, Pipeline):
                self.stages.extend(stage.stages)
            else:
                self.stages.append(stage)

    def __or__(self, other: Runnable) -> Pipeline:
        """Append to pipeline: pipeline | cmd -> Pipeline."""
        return pipe(self, other)

    def __ror__(self, other: Stage) -> Pipeline:
        """Prepend to pipeline: cmd | pipeline -> Pipeline."""
        return pipe(other, self)

    def __gt__(self, path: PathLike) -> Redirect:
        """pipeline > "file" - redirect stdout to file."""
        return write(self, path)

    def __rshift__(self, path: PathLike) -> Redirect:
        """pipeline >> "file" - append stdout to file."""
        return append(self, path)

    def __lt__(self, path: PathLike) -> Redirect:
        """pipeline < "file" - read stdin from file."""
        return read(self, path)

    def __lshift__(self, data: Data) -> Redirect:
        """pipeline << "data" - stdin from literal data."""
        return input_(self, data)

    def __await__(self) -> Generator[object, None, int]:
        from shish.runtime import run

        return run(self).__await__()


class Redirect:
    """Wraps a Cmd or Pipeline with I/O redirections."""

    def __init__(
        self,
        inner: Runnable,
        stdin: StdinSource | None = None,
        stdout: StdoutSink | None = None,
    ) -> None:
        # Unwrap nested Redirect, merging stdin/stdout
        if isinstance(inner, Redirect):
            self.inner = inner.inner
            self.stdin = stdin if stdin is not None else inner.stdin
            self.stdout = stdout if stdout is not None else inner.stdout
        else:
            self.inner = inner
            self.stdin = stdin
            self.stdout = stdout

    def __gt__(self, path: PathLike) -> Redirect:
        """Chain: (cmd < "in") > "out"."""
        return write(self, path)

    def __rshift__(self, path: PathLike) -> Redirect:
        """Chain: (cmd < "in") >> "out"."""
        return append(self, path)

    def __lt__(self, path: PathLike) -> Redirect:
        """Chain: (cmd > "out") < "in"."""
        return read(self, path)

    def __lshift__(self, data: Data) -> Redirect:
        """Chain: (cmd > "out") << "data"."""
        return input_(self, data)

    def __or__(self, other: Runnable) -> Pipeline:
        """Pipe redirect to another command: (cmd > file) | cmd2 -> Pipeline."""
        return pipe(self, other)

    def __ror__(self, other: Stage) -> Pipeline:
        """Prepend to redirect: cmd | (cmd2 > file) -> Pipeline."""
        return pipe(other, self)

    def __bool__(self) -> bool:
        raise TypeError(
            "Redirect cannot be used as bool. Use parentheses: (cmd < 'in') > 'out'"
        )

    def __await__(self) -> Generator[object, None, int]:
        from shish.runtime import run

        return run(self).__await__()


# Type aliases for common unions
Stage = Cmd | Redirect  # Single pipeline stage (after flattening)
Runnable = Cmd | Pipeline | Redirect  # Anything that can be executed
PathLike = Path | str  # File path argument
Data = str | bytes  # Literal data for stdin
Arg = str | Sub  # Cmd argument (string or process substitution)


# Combinators


def pipe(*cmds: Runnable) -> Pipeline:
    """Pipe commands together: pipe(cmd1, cmd2, ...) -> Pipeline."""
    return Pipeline(list(cmds))


def write(cmd: Runnable, path: PathLike) -> Redirect:
    """Redirect stdout to file: write(cmd, path) -> Redirect."""
    return Redirect(cmd, stdout=ToFile(Path(path)))


def append(cmd: Runnable, path: PathLike) -> Redirect:
    """Append stdout to file: append(cmd, path) -> Redirect."""
    return Redirect(cmd, stdout=ToFile(Path(path), append=True))


def read(cmd: Runnable, path: PathLike) -> Redirect:
    """Read stdin from file: read(cmd, path) -> Redirect."""
    return Redirect(cmd, stdin=FromFile(Path(path)))


def input_(cmd: Runnable, data: Data) -> Redirect:
    """Provide stdin from data: input_(cmd, data) -> Redirect."""
    return Redirect(cmd, stdin=FromData(data))


def from_proc(source: Runnable) -> Sub:
    """Input process substitution: <(source) -> Sub reading from source."""
    return Sub(source, write=False)


def to_proc(sink: Runnable) -> Sub:
    """Output process substitution: >(sink) -> Sub writing to sink."""
    return Sub(sink, write=True)


# Convenience
sh = Cmd()
