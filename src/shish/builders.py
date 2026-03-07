"""Builders: frozen dataclasses with chainable builder methods."""

from __future__ import annotations

import asyncio
import dataclasses as dc
import typing as ty
from enum import Enum, auto
from pathlib import Path

from shish._defaults import DEFAULT_ENCODING
from shish.fd import PIPE, STDIN, STDOUT

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
        cmd: Runnable,
        stdout: str | bytes,
        stderr: str | bytes,
    ) -> None:
        self.returncode = returncode
        self.cmd = cmd
        self.stdout = stdout
        self.stderr = stderr
        super().__init__(f"exit code {returncode}")


class BaseRunnable:
    """Shared start/run/out/sub methods for Cmd, Fn, Pipeline."""

    def start(self) -> JobCtx[None, None, None]:
        """Spawn and yield a Job via async context manager."""
        from shish import runtime  # noqa: PLC0415

        return runtime.start(ty.cast("Runnable", self))

    async def run(self) -> int:
        """Execute and return exit code."""
        async with self.start() as job:
            return await job.wait()

    @ty.overload
    async def out(self, encoding: None) -> bytes: ...
    @ty.overload
    async def out(self, encoding: str = ...) -> str: ...

    async def out(self, encoding: str | None = DEFAULT_ENCODING) -> str | bytes:
        """Execute and return stdout."""
        async with (
            self.start()
            .stdout(PIPE, encoding=encoding)
            .stderr(PIPE, encoding=encoding) as job
        ):
            code, stdout, stderr = await asyncio.gather(
                job.wait(), job.stdout.read(), job.stderr.read()
            )
        if code != 0:
            raise ShishError(code, ty.cast("Runnable", self), stdout, stderr)
        return stdout

    def sub_in(self) -> SubIn:
        """Process substitution: <(cmd)."""
        return SubIn(ty.cast("Runnable", self))

    def sub_out(self) -> SubOut:
        """Process substitution: >(cmd)."""
        return SubOut(ty.cast("Runnable", self))


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


@dc.dataclass(frozen=True)
class Fn(BaseRunnable):
    func: ByteFn

    def pipe(self, other: Cmd | Fn) -> Pipeline:
        """Pipe this Fn into another stage."""
        return Pipeline((self, other))


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
