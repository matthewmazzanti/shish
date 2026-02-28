"""IR: frozen dataclasses with chainable builder methods."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from shish.fdops import STDIN, STDOUT

PathLike = Path | str
Data = str | bytes


@dataclass(frozen=True)
class FdToFile:
    fd: int
    path: Path
    append: bool = False


@dataclass(frozen=True)
class FdFromFile:
    fd: int
    path: Path


@dataclass(frozen=True)
class FdFromData:
    fd: int
    data: Data


@dataclass(frozen=True)
class FdToFd:
    src: int
    dst: int


@dataclass(frozen=True)
class FdClose:
    fd: int


@dataclass(frozen=True)
class SubIn:
    """Input process substitution: <(cmd)."""

    cmd: Runnable


@dataclass(frozen=True)
class SubOut:
    """Output process substitution: >(cmd)."""

    cmd: Runnable


Sub = SubIn | SubOut
Arg = PathLike | Sub
ReadSrc = PathLike | SubIn
WriteDst = PathLike | SubOut


@dataclass(frozen=True)
class FdFromSub:
    fd: int
    sub: SubIn


@dataclass(frozen=True)
class FdToSub:
    fd: int
    sub: SubOut


Redirect = FdToFile | FdFromFile | FdFromData | FdToFd | FdClose | FdFromSub | FdToSub


@dataclass(frozen=True)
class Cmd:
    args: tuple[str | Sub, ...]
    redirects: tuple[Redirect, ...] = ()

    def arg(self, *args: Arg) -> Cmd:
        """Append positional arguments."""
        resolved: list[str | Sub] = []
        for item in args:
            if isinstance(item, (SubIn, SubOut)):
                resolved.append(item)
            else:
                resolved.append(str(item))
        return Cmd((*self.args, *resolved), self.redirects)

    def pipe(self, other: Cmd) -> Pipeline:
        """Pipe this command into another."""
        return Pipeline((self, other))

    def read(self, src: ReadSrc, *, fd: int = STDIN) -> Cmd:
        """Read fd from file or process substitution. Defaults to STDIN."""
        match src:
            case SubIn():
                return Cmd(self.args, (*self.redirects, FdFromSub(fd, src)))
            case path:
                return Cmd(self.args, (*self.redirects, FdFromFile(fd, Path(path))))

    def write(self, dst: WriteDst, *, append: bool = False, fd: int = STDOUT) -> Cmd:
        """Write fd to file or process substitution. Defaults to STDOUT."""
        match dst:
            case SubOut():
                return Cmd(self.args, (*self.redirects, FdToSub(fd, dst)))
            case path:
                redirect = FdToFile(fd, Path(path), append)
                return Cmd(self.args, (*self.redirects, redirect))

    def feed(self, data: Data, *, fd: int = STDIN) -> Cmd:
        """Feed literal data into fd. Defaults to STDIN."""
        return Cmd(self.args, (*self.redirects, FdFromData(fd, data)))

    def close(self, fd: int) -> Cmd:
        """Close fd."""
        return Cmd(self.args, (*self.redirects, FdClose(fd)))

    def sub_in(self) -> SubIn:
        """Process substitution: <(cmd)."""
        return SubIn(self)

    def sub_out(self) -> SubOut:
        """Process substitution: >(cmd)."""
        return SubOut(self)

    async def run(self) -> int:
        """Execute and return exit code."""
        from shish import runtime

        return await runtime.run(self)

    async def out(self, encoding: str | None = "utf-8") -> str | bytes:
        """Execute and return stdout."""
        from shish import runtime

        return await runtime.out(self, encoding)


@dataclass(frozen=True)
class Pipeline:
    stages: tuple[Cmd, ...]

    def pipe(self, other: Cmd) -> Pipeline:
        """Append another stage."""
        return Pipeline((*self.stages, other))

    def read(self, src: ReadSrc, *, fd: int = STDIN) -> Pipeline:
        """Read fd from file or sub (first stage). Defaults to STDIN."""
        first = self.stages[0].read(src, fd=fd)
        return Pipeline((first, *self.stages[1:]))

    def write(
        self, dst: WriteDst, *, append: bool = False, fd: int = STDOUT
    ) -> Pipeline:
        """Write fd to file or sub (last stage). Defaults to STDOUT."""
        last = self.stages[-1].write(dst, append=append, fd=fd)
        return Pipeline((*self.stages[:-1], last))

    def feed(self, data: Data, *, fd: int = STDIN) -> Pipeline:
        """Feed literal data into fd (first stage). Defaults to STDIN."""
        first = self.stages[0].feed(data, fd=fd)
        return Pipeline((first, *self.stages[1:]))

    def close(self, fd: int) -> Pipeline:
        """Close fd (last stage)."""
        last = self.stages[-1].close(fd)
        return Pipeline((*self.stages[:-1], last))

    async def run(self) -> int:
        """Execute and return exit code."""
        from shish import runtime

        return await runtime.run(self)

    async def out(self, encoding: str | None = "utf-8") -> str | bytes:
        """Execute and return stdout."""
        from shish import runtime

        return await runtime.out(self, encoding)


Runnable = Cmd | Pipeline


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
    flat: list[Cmd] = []
    for stage in stages:
        if isinstance(stage, Pipeline):
            flat.extend(stage.stages)
        else:
            flat.append(stage)
    return Pipeline(tuple(flat))
