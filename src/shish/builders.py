"""Builders: frozen dataclasses with chainable builder methods."""

from __future__ import annotations

import asyncio
import dataclasses as dc
import typing as ty
from enum import Enum, auto
from pathlib import Path

from shish._defaults import DEFAULT_ENCODING
from shish.fd import PIPE, STDIN, STDOUT, Pipe

if ty.TYPE_CHECKING:
    from shish.fn_stage import ByteFn
    from shish.runtime import JobCtx


class _Unset(Enum):
    """Sentinel for unset fields in _replace."""

    UNSET = auto()


PathLike = Path | str
Data = str | bytes


@dc.dataclass(frozen=True)
class FdToFile:
    fd: int
    path: Path
    append: bool = False


@dc.dataclass(frozen=True)
class FdFromFile:
    fd: int
    path: Path


@dc.dataclass(frozen=True)
class FdFromData:
    fd: int
    data: Data


@dc.dataclass(frozen=True)
class FdToFd:
    src: int
    dst: int


@dc.dataclass(frozen=True)
class FdClose:
    fd: int


@dc.dataclass(frozen=True)
class SubIn:
    """Input process substitution: <(cmd)."""

    cmd: Runnable


@dc.dataclass(frozen=True)
class SubOut:
    """Output process substitution: >(cmd)."""

    cmd: Runnable


Sub = SubIn | SubOut
Arg = PathLike | Sub
ReadSrc = PathLike | SubIn
WriteDst = PathLike | SubOut


@dc.dataclass(frozen=True)
class FdFromSub:
    fd: int
    sub: SubIn


@dc.dataclass(frozen=True)
class FdToSub:
    fd: int
    sub: SubOut


Redirect = FdToFile | FdFromFile | FdFromData | FdToFd | FdClose | FdFromSub | FdToSub


class ShishError(Exception):
    """Non-zero exit from out()."""

    def __init__(
        self,
        returncode: int,
        cmd: BaseRunnable,
        stdout: str | bytes | None,
        stderr: str | bytes | None,
    ) -> None:
        self.returncode = returncode
        self.cmd = cmd
        self.stdout = stdout
        self.stderr = stderr
        super().__init__(f"exit code {returncode}")


class Result[OutT: str | bytes | None, ErrT: str | bytes | None](ty.NamedTuple):
    """Execution result with exit code and optional captured streams."""

    code: int
    out: OutT
    err: ErrT


class BaseRunnable:
    """Shared execution methods for Cmd, Fn, Pipeline."""

    def start(self) -> JobCtx[None, None, None]:
        """Spawn and yield a Job via async context manager."""
        # local: runtime imports builders (circular)
        from shish import runtime  # noqa: PLC0415

        return runtime.start(ty.cast("Runnable", self))

    @ty.overload
    async def result(
        self,
        *,
        check: bool = ...,
        stdout: None = ...,
        stderr: None = ...,
        encoding: str | None = ...,
    ) -> Result[None, None]: ...
    @ty.overload
    async def result(
        self,
        *,
        check: bool = ...,
        stdout: Pipe,
        stderr: None = ...,
        encoding: str = ...,
    ) -> Result[str, None]: ...
    @ty.overload
    async def result(
        self,
        *,
        check: bool = ...,
        stdout: Pipe,
        stderr: None = ...,
        encoding: None = ...,
    ) -> Result[bytes, None]: ...
    @ty.overload
    async def result(
        self,
        *,
        check: bool = ...,
        stdout: None = ...,
        stderr: Pipe,
        encoding: str = ...,
    ) -> Result[None, str]: ...
    @ty.overload
    async def result(
        self,
        *,
        check: bool = ...,
        stdout: None = ...,
        stderr: Pipe,
        encoding: None = ...,
    ) -> Result[None, bytes]: ...
    @ty.overload
    async def result(
        self, *, check: bool = ..., stdout: Pipe, stderr: Pipe, encoding: str = ...
    ) -> Result[str, str]: ...
    @ty.overload
    async def result(
        self, *, check: bool = ..., stdout: Pipe, stderr: Pipe, encoding: None = ...
    ) -> Result[bytes, bytes]: ...

    async def result(
        self,
        *,
        check: bool = True,
        stdout: Pipe | None = None,
        stderr: Pipe | None = None,
        encoding: str | None = DEFAULT_ENCODING,
    ) -> Result[ty.Any, ty.Any]:
        """Execute and return Result with exit code and optional captured streams."""

        ctx = self.start()
        if stdout is PIPE:
            ctx = ctx.stdout(PIPE, encoding=encoding)
        if stderr is PIPE:
            ctx = ctx.stderr(PIPE, encoding=encoding)

        async def noop() -> None:
            """No-op coroutine for unused gather slots."""

        async with ctx as job:
            exit_code, out_data, err_data = await asyncio.gather(
                job.wait(),
                job.stdout.read() if job.stdout else noop(),
                job.stderr.read() if job.stderr else noop(),
            )
        if check and exit_code != 0:
            raise ShishError(exit_code, self, out_data, err_data)
        return Result(exit_code, out_data, err_data)

    async def run(self) -> None:
        """Execute. Raises ShishError on non-zero exit."""
        await self.result(check=True)

    async def code(self) -> int:
        """Execute and return exit code."""
        res = await self.result(check=False)
        return res.code

    @ty.overload
    async def out(self, encoding: None, *, check: ty.Literal[True] = ...) -> bytes: ...
    @ty.overload
    async def out(
        self, encoding: str = ..., *, check: ty.Literal[True] = ...
    ) -> str: ...
    @ty.overload
    async def out(
        self, encoding: None, *, check: ty.Literal[False]
    ) -> tuple[int, bytes]: ...
    @ty.overload
    async def out(
        self, encoding: str = ..., *, check: ty.Literal[False]
    ) -> tuple[int, str]: ...

    async def out(
        self, encoding: str | None = DEFAULT_ENCODING, *, check: bool = True
    ) -> ty.Any:
        """Execute and return stdout. check=False prepends exit code."""
        res = await self.result(check=check, stdout=PIPE, encoding=encoding)
        if check:
            return res.out
        return (res.code, res.out)

    @ty.overload
    async def err(self, encoding: None, *, check: ty.Literal[True] = ...) -> bytes: ...
    @ty.overload
    async def err(
        self, encoding: str = ..., *, check: ty.Literal[True] = ...
    ) -> str: ...
    @ty.overload
    async def err(
        self, encoding: None, *, check: ty.Literal[False]
    ) -> tuple[int, bytes]: ...
    @ty.overload
    async def err(
        self, encoding: str = ..., *, check: ty.Literal[False]
    ) -> tuple[int, str]: ...

    async def err(
        self, encoding: str | None = DEFAULT_ENCODING, *, check: bool = True
    ) -> ty.Any:
        """Execute and return stderr. check=False prepends exit code."""
        res = await self.result(check=check, stderr=PIPE, encoding=encoding)
        if check:
            return res.err
        return (res.code, res.err)

    @ty.overload
    async def out_err(
        self, encoding: None, *, check: ty.Literal[True] = ...
    ) -> tuple[bytes, bytes]: ...
    @ty.overload
    async def out_err(
        self, encoding: str = ..., *, check: ty.Literal[True] = ...
    ) -> tuple[str, str]: ...
    @ty.overload
    async def out_err(
        self, encoding: None, *, check: ty.Literal[False]
    ) -> tuple[int, bytes, bytes]: ...
    @ty.overload
    async def out_err(
        self, encoding: str = ..., *, check: ty.Literal[False]
    ) -> tuple[int, str, str]: ...

    async def out_err(
        self, encoding: str | None = DEFAULT_ENCODING, *, check: bool = True
    ) -> ty.Any:
        """Execute and return stdout + stderr. check=False prepends exit code."""
        res = await self.result(
            check=check, stdout=PIPE, stderr=PIPE, encoding=encoding
        )
        if check:
            return (res.out, res.err)
        return (res.code, res.out, res.err)


@dc.dataclass(frozen=True)
class Cmd(BaseRunnable):
    args: tuple[str | Sub, ...]
    redirects: tuple[Redirect, ...] = ()
    env_vars: tuple[tuple[str, str | None], ...] = ()
    working_dir: Path | None = None

    def _replace(
        self,
        *,
        args: tuple[str | Sub, ...] | _Unset = _Unset.UNSET,
        redirects: tuple[Redirect, ...] | _Unset = _Unset.UNSET,
        env_vars: tuple[tuple[str, str | None], ...] | _Unset = _Unset.UNSET,
        working_dir: Path | None | _Unset = _Unset.UNSET,
    ) -> Cmd:
        """Return a copy with specified fields replaced."""
        return Cmd(
            args=self.args if args is _Unset.UNSET else args,
            redirects=self.redirects if redirects is _Unset.UNSET else redirects,
            env_vars=self.env_vars if env_vars is _Unset.UNSET else env_vars,
            working_dir=(
                self.working_dir if working_dir is _Unset.UNSET else working_dir
            ),
        )

    def arg(self, *args: Arg) -> Cmd:
        """Append positional arguments."""
        resolved: list[str | Sub] = []
        for item in args:
            match item:
                case SubIn() | SubOut():
                    resolved.append(item)
                case _:
                    resolved.append(str(item))

        return self._replace(args=(*self.args, *resolved))

    def pipe(self, other: Cmd | Fn) -> Pipeline:
        """Pipe this command into another."""
        return Pipeline((self, other))

    def read(self, src: ReadSrc, *, fd: int = STDIN) -> Cmd:
        """Read fd from file or process substitution. Defaults to STDIN."""
        match src:
            case SubIn():
                return self._replace(redirects=(*self.redirects, FdFromSub(fd, src)))
            case path:
                return self._replace(
                    redirects=(*self.redirects, FdFromFile(fd, Path(path)))
                )

    def write(self, dst: WriteDst, *, append: bool = False, fd: int = STDOUT) -> Cmd:
        """Write fd to file or process substitution. Defaults to STDOUT."""
        match dst:
            case SubOut():
                return self._replace(redirects=(*self.redirects, FdToSub(fd, dst)))
            case path:
                redirect = FdToFile(fd, Path(path), append)
                return self._replace(redirects=(*self.redirects, redirect))

    def feed(self, data: Data, *, fd: int = STDIN) -> Cmd:
        """Feed literal data into fd. Defaults to STDIN."""
        return self._replace(redirects=(*self.redirects, FdFromData(fd, data)))

    def close(self, fd: int) -> Cmd:
        """Close fd."""
        return self._replace(redirects=(*self.redirects, FdClose(fd)))

    def env(self, **kwargs: str | None) -> Cmd:
        """Set environment variables. None values unset variables."""
        merged = dict(self.env_vars)
        merged.update(kwargs)
        return self._replace(env_vars=tuple(merged.items()))

    def cwd(self, path: PathLike) -> Cmd:
        """Set working directory."""
        return self._replace(working_dir=Path(path))

    def sub_in(self) -> SubIn:
        """Process substitution: <(cmd)."""
        return SubIn(self)

    def sub_out(self) -> SubOut:
        """Process substitution: >(cmd)."""
        return SubOut(self)


@dc.dataclass(frozen=True)
class Fn(BaseRunnable):
    func: ByteFn

    def pipe(self, other: Cmd | Fn) -> Pipeline:
        """Pipe this Fn into another stage."""
        return Pipeline((self, other))

    def sub_in(self) -> SubIn:
        """Process substitution: <(fn)."""
        return SubIn(self)

    def sub_out(self) -> SubOut:
        """Process substitution: >(fn)."""
        return SubOut(self)


@dc.dataclass(frozen=True)
class Pipeline(BaseRunnable):
    stages: tuple[Cmd | Fn, ...]

    def pipe(self, other: Cmd | Fn) -> Pipeline:
        """Append another stage."""
        return Pipeline((*self.stages, other))


Runnable = Cmd | Pipeline | Fn


def cmd(*args: Arg) -> Cmd:
    """Create a command from positional arguments."""
    resolved: list[str | Sub] = []
    for arg in args:
        if isinstance(arg, (SubIn, SubOut)):
            resolved.append(arg)
        else:
            resolved.append(str(arg))
    return Cmd(tuple(resolved))


def pipeline(*stages: Runnable) -> Pipeline:
    """Flatten nested Pipelines into a single stage list."""
    flat: list[Cmd | Fn] = []
    for stage in stages:
        if isinstance(stage, Pipeline):
            flat.extend(stage.stages)
        else:
            flat.append(stage)
    return Pipeline(tuple(flat))
