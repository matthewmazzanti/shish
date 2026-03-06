"""Syntax types and operators for shell command construction.

Cmd and Pipeline are thin wrappers around builders.Cmd/builders.Pipeline with
no public methods.  All operations (pipe, read, write, feed, run, out, etc.)
are module-level combinator functions that unwrap, delegate, and re-wrap.
"""

from __future__ import annotations

import typing as ty
from collections.abc import Callable, Generator, Mapping

import shish.builders as builders
from shish._defaults import DEFAULT_ENCODING
from shish.fd import STDIN, STDOUT
from shish.fn_stage import ByteFn, TextFn, make_byte_wrapper

if ty.TYPE_CHECKING:
    from shish.runtime import StartCtx

Flag = builders.PathLike | bool


class Cmd:
    """Immutable shell command builder with chainable syntax."""

    def __init__(self, _shish_ir: builders.Cmd) -> None:
        self._shish_ir = _shish_ir

    def __getattr__(self, name: str) -> Cmd:
        """Chain subcommand: cmd.foo -> Cmd with "foo" appended."""
        return Cmd(self._shish_ir.arg(name))

    def __call__(self, *args: builders.Arg, **kwargs: Flag) -> Cmd:
        """Add args and flags: cmd("arg", flag=True)."""
        call_args: list[builders.Arg] = list(args)
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

    def __gt__(self, target: builders.WriteDst | tuple[int, builders.WriteDst]) -> Cmd:
        """cmd > "file", cmd > sub, or cmd > (fd, target)."""
        match target:
            case fd, dst:
                return write(self, dst, fd=fd)
            case dst:
                return write(self, dst)  # type: ignore[arg-type]

    def __rshift__(
        self, target: builders.WriteDst | tuple[int, builders.WriteDst]
    ) -> Cmd:
        """cmd >> "file", cmd >> sub, or cmd >> (fd, target)."""
        match target:
            case fd, dst:
                return write(self, dst, append=True, fd=fd)
            case dst:
                return write(self, dst, append=True)

    def __lt__(self, target: builders.ReadSrc | tuple[int, builders.ReadSrc]) -> Cmd:
        """cmd < "file", cmd < sub, or cmd < (fd, target)."""
        match target:
            case fd, src:
                return read(self, src, fd=fd)
            case src:
                return read(self, src)  # type: ignore[arg-type]

    def __lshift__(self, data: builders.Data | tuple[int, builders.Data]) -> Cmd:
        """cmd << "data" or cmd << (fd, "data")."""
        match data:
            case fd, payload:
                return feed(self, payload, fd=fd)
            case _:
                return feed(self, data)

    def __matmul__(self, path: builders.PathLike) -> Cmd:
        """cmd @ "/tmp" -> set working directory."""
        return cwd(self, path)

    def __rmod__(self, env_vars: Mapping[str, str | None]) -> Cmd:
        """{"FOO": "bar"} % cmd -> set env vars."""
        return env(self, **env_vars)

    def __bool__(self) -> ty.Never:
        raise TypeError(
            "Cmd cannot be used as bool. Use parentheses: (cmd < 'in') > 'out'"
        )

    def __await__(self) -> Generator[object, None, int]:
        return self._shish_ir.run().__await__()


class Pipeline:
    """Immutable pipeline of commands."""

    def __init__(self, _shish_ir: builders.Pipeline) -> None:
        self._shish_ir = _shish_ir

    def __or__(self, other: Runnable) -> Pipeline:
        """pipeline | cmd -> Pipeline."""
        return pipe(self, other)

    def __ror__(self, other: Cmd) -> Pipeline:
        """cmd | pipeline -> Pipeline."""
        return pipe(other, self)

    def __gt__(self, target: object) -> ty.Never:
        """Redirect operators are not supported on Pipeline."""
        raise TypeError(
            "Pipeline does not support >. You probably wanted: cmd1 | (cmd2 > target)"
        )

    def __rshift__(self, target: object) -> ty.Never:
        """Redirect operators are not supported on Pipeline."""
        raise TypeError(
            "Pipeline does not support >>. You probably wanted: cmd1 | (cmd2 >> target)"
        )

    def __lt__(self, target: object) -> ty.Never:
        """Redirect operators are not supported on Pipeline."""
        raise TypeError(
            "Pipeline does not support <. You probably wanted: (cmd1 < source) | cmd2"
        )

    def __lshift__(self, data: object) -> ty.Never:
        """Redirect operators are not supported on Pipeline."""
        raise TypeError(
            "Pipeline does not support <<. You probably wanted: (cmd1 << data) | cmd2"
        )

    def __matmul__(self, path: object) -> ty.Never:
        """@ operator is not supported on Pipeline."""
        raise TypeError("Pipeline does not support @. Apply cwd to individual cmds.")

    def __rmod__(self, env_vars: object) -> ty.Never:
        """% operator is not supported on Pipeline."""
        raise TypeError("Pipeline does not support %. Apply env to individual cmds.")

    def __bool__(self) -> ty.Never:
        raise TypeError(
            "Pipeline cannot be used as bool. Use parentheses: (cmd < 'in') > 'out'"
        )

    def __await__(self) -> Generator[object, None, int]:
        return self._shish_ir.run().__await__()


class Fn:
    """Immutable wrapper for a Python function as a pipeline stage."""

    def __init__(self, _shish_ir: builders.Fn) -> None:
        self._shish_ir = _shish_ir

    def __or__(self, other: Runnable) -> Pipeline:
        """fn | cmd -> Pipeline."""
        return pipe(self, other)

    def __bool__(self) -> ty.Never:
        raise TypeError("Fn cannot be used as bool")

    def __await__(self) -> Generator[object, None, int]:
        return self._shish_ir.run().__await__()


# Type alias requiring all classes
Runnable = Cmd | Pipeline | Fn


# Combinators


@ty.overload
def unwrap(cmd: Cmd) -> builders.Cmd: ...
@ty.overload
def unwrap(cmd: Pipeline) -> builders.Pipeline: ...
@ty.overload
def unwrap(cmd: Fn) -> builders.Fn: ...


def unwrap(cmd: Cmd | Pipeline | Fn) -> builders.Cmd | builders.Pipeline | builders.Fn:
    """Extract the builder from a syntax wrapper."""
    return cmd._shish_ir  # pyright: ignore[reportPrivateUsage]


@ty.overload
def wrap(inner: builders.Cmd) -> Cmd: ...
@ty.overload
def wrap(inner: builders.Pipeline) -> Pipeline: ...
@ty.overload
def wrap(inner: builders.Fn) -> Fn: ...


def wrap(inner: builders.Cmd | builders.Pipeline | builders.Fn) -> Cmd | Pipeline | Fn:
    """Wrap a builder in its syntax counterpart."""
    match inner:
        case builders.Cmd():
            return Cmd(inner)
        case builders.Pipeline():
            return Pipeline(inner)
        case builders.Fn():
            return Fn(inner)


def cmd(*args: builders.Arg, **kwargs: Flag) -> Cmd:
    """Create a command from arguments: cmd("echo", "hello") -> Cmd."""
    return Cmd(builders.cmd())(*args, **kwargs)


@ty.overload
def fn(func: TextFn, /) -> Fn: ...
@ty.overload
def fn(func: TextFn, /, *, encoding: str) -> Fn: ...
@ty.overload
def fn(func: ByteFn, /, *, encoding: None) -> Fn: ...
@ty.overload
def fn(*, encoding: None) -> Callable[[ByteFn], Fn]: ...
@ty.overload
def fn(*, encoding: str = ...) -> Callable[[TextFn], Fn]: ...


def fn(
    func: TextFn | ByteFn | None = None,
    *,
    encoding: str | None = DEFAULT_ENCODING,
) -> Fn | Callable[[ByteFn], Fn] | Callable[[TextFn], Fn]:
    """Create an Fn from an async callable.

    Forms:
        @fn                         — text mode, utf-8
        @fn()                       — text mode, utf-8
        fn(f, encoding="latin-1")   — text mode, custom encoding
        @fn(encoding="latin-1")     — text mode, custom encoding
        fn(f, encoding=None)        — byte mode, no decoding
        @fn(encoding=None)          — byte mode, no decoding
    """
    match func, encoding:
        case None, None:  # @fn(encoding=None)

            def byte_decorator(inner: ByteFn) -> Fn:
                return Fn(builders.Fn(inner))

            return byte_decorator

        case None, enc:  # @fn() or @fn(encoding="...")

            def text_decorator(inner: TextFn) -> Fn:
                return Fn(builders.Fn(make_byte_wrapper(inner, enc)))

            return text_decorator

        case func_, None:  # fn(f, encoding=None)
            # cast: overloads guarantee ByteFn here, but pyright
            # can't narrow the func/encoding correlation
            return Fn(builders.Fn(ty.cast("ByteFn", func_)))

        case func_, enc:  # @fn or fn(f) or fn(f, encoding="...")
            # cast: same — overloads guarantee TextFn here
            return Fn(builders.Fn(make_byte_wrapper(ty.cast("TextFn", func_), enc)))


def pipe(*cmds: Runnable) -> Pipeline:
    """Pipe commands together: pipe(cmd1, cmd2, ...) -> Pipeline."""
    return Pipeline(builders.pipeline(*(unwrap(stage) for stage in cmds)))


def write(
    cmd: Cmd,
    dst: builders.WriteDst,
    *,
    append: bool = False,
    fd: int = STDOUT,
) -> Cmd:
    """Redirect fd to file or process substitution. Defaults to STDOUT."""
    return Cmd(unwrap(cmd).write(dst, append=append, fd=fd))


def read(cmd: Cmd, src: builders.ReadSrc, *, fd: int = STDIN) -> Cmd:
    """Read fd from file or process substitution. Defaults to STDIN."""
    return Cmd(unwrap(cmd).read(src, fd=fd))


def feed(cmd: Cmd, data: builders.Data, *, fd: int = STDIN) -> Cmd:
    """Feed data into fd. Defaults to STDIN."""
    return Cmd(unwrap(cmd).feed(data, fd=fd))


def close(cmd: Cmd, fd: int) -> Cmd:
    """Close fd."""
    return Cmd(unwrap(cmd).close(fd))


def env(cmd: Cmd, **kwargs: str | None) -> Cmd:
    """Set environment variables on a command. None values unset variables."""
    return Cmd(unwrap(cmd).env(**kwargs))


def cwd(cmd: Cmd, path: builders.PathLike) -> Cmd:
    """Set working directory for a command."""
    return Cmd(unwrap(cmd).cwd(path))


def sub_in(source: Runnable) -> builders.SubIn:
    """Input process substitution: <(source)."""
    return builders.SubIn(unwrap(source))


def sub_out(sink: Runnable) -> builders.SubOut:
    """Output process substitution: >(sink)."""
    return builders.SubOut(unwrap(sink))


def start(cmd: Runnable) -> StartCtx[None, None, None]:
    """Spawn a command or pipeline and yield an Execution via async context manager."""
    return unwrap(cmd).start()


async def run(cmd: Runnable) -> int:
    """Execute a command or pipeline and return exit code."""
    return await unwrap(cmd).run()


async def out(cmd: Runnable, encoding: str | None = DEFAULT_ENCODING) -> str | bytes:
    """Execute command and return stdout."""
    return await unwrap(cmd).out(encoding)


# Convenience


class Sh:
    """Root command builder. Attribute access creates Cmd instances."""

    def __getattr__(self, name: str) -> Cmd:
        """sh.echo -> Cmd with ("echo",)."""
        return cmd(name)

    def __call__(self, *args: builders.Arg, **kwargs: Flag) -> Cmd:
        """sh("cmd", "arg", flag=True) -> Cmd."""
        return cmd(*args, **kwargs)


sh = Sh()
