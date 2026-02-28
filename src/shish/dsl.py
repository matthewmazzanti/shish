"""DSL types and builders for shell command construction.

Cmd and Pipeline are thin wrappers around ir.Cmd/ir.Pipeline with no public
methods.  All operations (pipe, read, write, feed, run, out, etc.) are
module-level combinator functions that unwrap to IR, delegate, and re-wrap.
"""

from __future__ import annotations

from collections.abc import Generator
from typing import overload

import shish.ir as ir
from shish.fdops import STDIN, STDOUT

Flag = ir.PathLike | bool


class Cmd:
    """Immutable shell command builder with chainable syntax."""

    def __init__(self, _shish_ir: ir.Cmd | None = None) -> None:
        self._shish_ir = ir.Cmd(()) if _shish_ir is None else _shish_ir

    def __getattr__(self, name: str) -> Cmd:
        """Chain subcommand: cmd.foo -> Cmd with "foo" appended."""
        return Cmd(self._shish_ir.arg(name))

    def __call__(self, *args: ir.Arg, **kwargs: Flag) -> Cmd:
        """Add args and flags: cmd("arg", flag=True)."""
        call_args: list[ir.Arg] = list(args)
        for key, value in kwargs.items():
            if value is False:
                continue
            flag = f"-{key}" if len(key) == 1 else f"--{key.replace('_', '-')}"
            if value is True:
                call_args.append(flag)
            else:
                call_args.extend([flag, value])
        if call_args:
            return Cmd(self._shish_ir.arg(*call_args))
        return Cmd(self._shish_ir)

    def __or__(self, other: Runnable) -> Pipeline:
        """cmd1 | cmd2 -> Pipeline."""
        return pipe(self, other)

    def __gt__(self, target: ir.WriteDst | tuple[int, ir.WriteDst]) -> Cmd:
        """cmd > "file", cmd > sub, or cmd > (fd, target)."""
        match target:
            case fd, dst:
                return write(self, dst, fd=fd)
            case dst:
                return write(self, dst)

    def __rshift__(self, target: ir.WriteDst | tuple[int, ir.WriteDst]) -> Cmd:
        """cmd >> "file", cmd >> sub, or cmd >> (fd, target)."""
        match target:
            case fd, dst:
                return write(self, dst, append=True, fd=fd)
            case dst:
                return write(self, dst, append=True)

    def __lt__(self, target: ir.ReadSrc | tuple[int, ir.ReadSrc]) -> Cmd:
        """cmd < "file", cmd < sub, or cmd < (fd, target)."""
        match target:
            case fd, src:
                return read(self, src, fd=fd)
            case src:
                return read(self, src)

    def __lshift__(self, data: ir.Data | tuple[int, ir.Data]) -> Cmd:
        """cmd << "data" or cmd << (fd, "data")."""
        match data:
            case fd, payload:
                return feed(self, payload, fd=fd)
            case _:
                return feed(self, data)

    def __bool__(self) -> bool:
        raise TypeError(
            "Cmd cannot be used as bool. Use parentheses: (cmd < 'in') > 'out'"
        )

    def __await__(self) -> Generator[object, None, int]:
        return self._shish_ir.run().__await__()


class Pipeline:
    """Immutable pipeline of commands."""

    def __init__(self, _shish_ir: ir.Pipeline) -> None:
        self._shish_ir = _shish_ir

    def __or__(self, other: Runnable) -> Pipeline:
        """pipeline | cmd -> Pipeline."""
        return pipe(self, other)

    def __ror__(self, other: Cmd) -> Pipeline:
        """cmd | pipeline -> Pipeline."""
        return pipe(other, self)

    def __gt__(self, target: ir.WriteDst | tuple[int, ir.WriteDst]) -> Pipeline:
        """pipeline > "file", pipeline > sub, or pipeline > (fd, target)."""
        match target:
            case fd, dst:
                return write(self, dst, fd=fd)
            case dst:
                return write(self, dst)

    def __rshift__(self, target: ir.WriteDst | tuple[int, ir.WriteDst]) -> Pipeline:
        """pipeline >> "file", pipeline >> sub, or pipeline >> (fd, target)."""
        match target:
            case fd, dst:
                return write(self, dst, append=True, fd=fd)
            case dst:
                return write(self, dst, append=True)

    def __lt__(self, target: ir.ReadSrc | tuple[int, ir.ReadSrc]) -> Pipeline:
        """pipeline < "file", pipeline < sub, or pipeline < (fd, target)."""
        match target:
            case fd, src:
                return read(self, src, fd=fd)
            case src:
                return read(self, src)

    def __lshift__(self, data: ir.Data | tuple[int, ir.Data]) -> Pipeline:
        """pipeline << "data" or pipeline << (fd, "data")."""
        match data:
            case fd, payload:
                return feed(self, payload, fd=fd)
            case _:
                return feed(self, data)

    def __bool__(self) -> bool:
        raise TypeError(
            "Pipeline cannot be used as bool. Use parentheses: (cmd < 'in') > 'out'"
        )

    def __await__(self) -> Generator[object, None, int]:
        return self._shish_ir.run().__await__()


# Type alias requiring both classes
Runnable = Cmd | Pipeline


# Combinators


@overload
def unwrap(cmd: Cmd) -> ir.Cmd: ...
@overload
def unwrap(cmd: Pipeline) -> ir.Pipeline: ...


def unwrap(cmd: Cmd | Pipeline) -> ir.Cmd | ir.Pipeline:
    """Extract the IR node from a DSL wrapper."""
    return cmd._shish_ir  # pyright: ignore[reportPrivateUsage]


@overload
def wrap(node: ir.Cmd) -> Cmd: ...
@overload
def wrap(node: ir.Pipeline) -> Pipeline: ...


def wrap(node: ir.Cmd | ir.Pipeline) -> Cmd | Pipeline:
    """Wrap an IR node in its DSL counterpart."""
    match node:
        case ir.Cmd():
            return Cmd(node)
        case ir.Pipeline():
            return Pipeline(node)


def pipe(*cmds: Runnable) -> Pipeline:
    """Pipe commands together: pipe(cmd1, cmd2, ...) -> Pipeline."""
    return Pipeline(ir.pipeline(*(unwrap(cmd) for cmd in cmds)))


@overload
def write(cmd: Cmd, dst: ir.WriteDst, *, append: bool = ..., fd: int = ...) -> Cmd: ...
@overload
def write(
    cmd: Pipeline, dst: ir.WriteDst, *, append: bool = ..., fd: int = ...
) -> Pipeline: ...


def write(
    cmd: Cmd | Pipeline,
    dst: ir.WriteDst,
    *,
    append: bool = False,
    fd: int = STDOUT,
) -> Cmd | Pipeline:
    """Redirect fd to file or process substitution. Defaults to STDOUT."""
    return wrap(unwrap(cmd).write(dst, append=append, fd=fd))


@overload
def read(cmd: Cmd, src: ir.ReadSrc, *, fd: int = ...) -> Cmd: ...
@overload
def read(cmd: Pipeline, src: ir.ReadSrc, *, fd: int = ...) -> Pipeline: ...


def read(cmd: Cmd | Pipeline, src: ir.ReadSrc, *, fd: int = STDIN) -> Cmd | Pipeline:
    """Read fd from file or process substitution. Defaults to STDIN."""
    return wrap(unwrap(cmd).read(src, fd=fd))


@overload
def feed(cmd: Cmd, data: ir.Data, *, fd: int = ...) -> Cmd: ...
@overload
def feed(cmd: Pipeline, data: ir.Data, *, fd: int = ...) -> Pipeline: ...


def feed(cmd: Cmd | Pipeline, data: ir.Data, *, fd: int = STDIN) -> Cmd | Pipeline:
    """Feed data into fd. Defaults to STDIN."""
    return wrap(unwrap(cmd).feed(data, fd=fd))


@overload
def close(cmd: Cmd, fd: int) -> Cmd: ...
@overload
def close(cmd: Pipeline, fd: int) -> Pipeline: ...


def close(cmd: Cmd | Pipeline, fd: int) -> Cmd | Pipeline:
    """Close fd."""
    return wrap(unwrap(cmd).close(fd))


def sub_in(source: Runnable) -> ir.SubIn:
    """Input process substitution: <(source)."""
    return ir.SubIn(unwrap(source))


def sub_out(sink: Runnable) -> ir.SubOut:
    """Output process substitution: >(sink)."""
    return ir.SubOut(unwrap(sink))


async def run(cmd: Cmd | Pipeline) -> int:
    """Execute a command or pipeline and return exit code."""
    return await unwrap(cmd).run()


async def out(cmd: Cmd | Pipeline, encoding: str | None = "utf-8") -> str | bytes:
    """Execute command and return stdout."""
    return await unwrap(cmd).out(encoding)


# Convenience


class Sh:
    """Root command builder. Attribute access creates Cmd instances."""

    def __getattr__(self, name: str) -> Cmd:
        """sh.echo -> Cmd with ("echo",)."""
        return Cmd(ir.Cmd((name,)))

    def __call__(self, *args: ir.Arg, **kwargs: Flag) -> Cmd:
        """sh("cmd", "arg", flag=True) -> Cmd."""
        return Cmd()(*args, **kwargs)


sh = Sh()
