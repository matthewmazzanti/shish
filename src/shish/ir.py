"""IR: frozen dataclasses with chainable builder methods."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from shish.fdops import STDIN, STDOUT


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
    data: str | bytes


@dataclass(frozen=True)
class FdToFd:
    src: int
    dst: int


@dataclass(frozen=True)
class FdClose:
    fd: int


@dataclass(frozen=True)
class Sub:
    cmd: Runnable
    write: bool


@dataclass(frozen=True)
class FdFromSub:
    fd: int
    sub: Sub


@dataclass(frozen=True)
class FdToSub:
    fd: int
    sub: Sub


Redirect = FdToFile | FdFromFile | FdFromData | FdToFd | FdClose | FdFromSub | FdToSub


@dataclass(frozen=True)
class Cmd:
    args: tuple[str | Sub, ...]
    redirects: tuple[Redirect, ...] = ()

    def arg(self, *args: str | Path | int | Sub) -> Cmd:
        """Append positional arguments."""
        resolved: list[str | Sub] = []
        for item in args:
            if isinstance(item, Sub):
                resolved.append(item)
            else:
                resolved.append(str(item))
        return Cmd((*self.args, *resolved), self.redirects)

    def pipe(self, other: Cmd) -> Pipeline:
        """Pipe this command into another."""
        return Pipeline((self, other))

    def read(self, path: Path | str, *, fd: int = STDIN) -> Cmd:
        """Read fd from a file. Defaults to STDIN."""
        return Cmd(self.args, (*self.redirects, FdFromFile(fd, Path(path))))

    def write(self, path: Path | str, *, append: bool = False, fd: int = STDOUT) -> Cmd:
        """Write fd to a file. Defaults to STDOUT."""
        return Cmd(self.args, (*self.redirects, FdToFile(fd, Path(path), append)))

    def feed(self, data: str | bytes, *, fd: int = STDIN) -> Cmd:
        """Feed literal data into fd. Defaults to STDIN."""
        return Cmd(self.args, (*self.redirects, FdFromData(fd, data)))

    def close(self, fd: int) -> Cmd:
        """Close fd."""
        return Cmd(self.args, (*self.redirects, FdClose(fd)))

    def sub_in(self) -> Sub:
        """Process substitution: <(cmd)."""
        return Sub(self, write=False)

    def sub_out(self) -> Sub:
        """Process substitution: >(cmd)."""
        return Sub(self, write=True)

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

    def read(self, path: Path | str, *, fd: int = STDIN) -> Pipeline:
        """Read fd from a file (first stage). Defaults to STDIN."""
        first = self.stages[0].read(path, fd=fd)
        return Pipeline((first, *self.stages[1:]))

    def write(
        self, path: Path | str, *, append: bool = False, fd: int = STDOUT
    ) -> Pipeline:
        """Write fd to a file (last stage). Defaults to STDOUT."""
        last = self.stages[-1].write(path, append=append, fd=fd)
        return Pipeline((*self.stages[:-1], last))

    def feed(self, data: str | bytes, *, fd: int = STDIN) -> Pipeline:
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


def cmd(*args: str | Path | int | Sub) -> Cmd:
    """Create a command from positional arguments."""
    resolved: list[str | Sub] = []
    for arg in args:
        if isinstance(arg, Sub):
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
