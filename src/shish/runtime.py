"""Runtime execution of shell commands and pipelines."""

from __future__ import annotations

import asyncio
import os
import subprocess
from asyncio.subprocess import Process, create_subprocess_exec
from collections.abc import Coroutine
from dataclasses import dataclass
from pathlib import Path
from typing import overload

from shish.aio import async_read, async_write, close_fd
from shish.dsl import (
    Cmd,
    FromData,
    FromFile,
    Pipeline,
    Redirect,
    Runnable,
    Sub,
    ToFile,
)


@dataclass
class PreparedProcess:
    """Process ready to start with resolved fds."""

    args: list[str]
    stdin_fd: int | None = None
    stdout_fd: int | None = None
    pass_fds: tuple[int, ...] = ()  # Fds to keep open (for process substitution)


@dataclass
class FdEntry:
    """Tracked file descriptor with optional data to write before closing."""

    fd: int
    data: str | bytes | None = None


@dataclass
class Result:
    """Execution result with exit code and optional captured stdout."""

    code: int
    stdout: bytes | None = None


class Executor:
    """Prepares and executes a Runnable."""

    def __init__(self) -> None:
        self.fds: list[FdEntry] = []
        self.prepared: list[PreparedProcess] = []
        self.procs: list[Process] = []

    def _alloc_pipe(self) -> tuple[int, int]:
        read_fd, write_fd = os.pipe()
        self.fds.append(FdEntry(read_fd))
        self.fds.append(FdEntry(write_fd))
        return read_fd, write_fd

    def _alloc_data_pipe(self, data: str | bytes) -> int:
        read_fd, write_fd = os.pipe()
        os.set_blocking(write_fd, False)
        self.fds.append(FdEntry(read_fd))
        self.fds.append(FdEntry(write_fd, data))
        return read_fd

    def _open_file(self, path: Path, flags: int) -> int:
        file_fd = os.open(path, flags, 0o644)
        self.fds.append(FdEntry(file_fd))
        return file_fd

    def _prepare(
        self, cmd: Runnable, stdin_fd: int | None, stdout_fd: int | None
    ) -> None:
        match cmd:
            case Cmd():
                self._prepare_cmd(cmd, stdin_fd, stdout_fd)
            case Pipeline():
                self._prepare_pipeline(cmd, stdin_fd, stdout_fd)
            case Redirect():
                self._prepare_redirect(cmd, stdin_fd, stdout_fd)

    def _prepare_cmd(
        self, cmd: Cmd, stdin_fd: int | None, stdout_fd: int | None
    ) -> None:
        """Prepare a Cmd, resolving any Sub arguments to /dev/fd/N paths."""
        resolved_args: list[str] = []
        pass_fds: list[int] = []

        for arg in cmd.args:
            match arg:
                case str() as string:
                    resolved_args.append(string)
                case Sub(cmd=inner, write=write):
                    read_fd, write_fd = self._alloc_pipe()
                    if write:
                        # Output substitution: >(sink)
                        # main process writes via /dev/fd/N, sink reads from pipe
                        self._prepare(inner, read_fd, None)
                        resolved_args.append(f"/dev/fd/{write_fd}")
                        pass_fds.append(write_fd)
                    else:
                        # Input substitution: <(source)
                        # source writes to pipe, main process reads via /dev/fd/N
                        self._prepare(inner, None, write_fd)
                        resolved_args.append(f"/dev/fd/{read_fd}")
                        pass_fds.append(read_fd)

        self.prepared.append(
            PreparedProcess(resolved_args, stdin_fd, stdout_fd, tuple(pass_fds))
        )

    def _prepare_redirect(
        self,
        redirect: Redirect,
        outer_stdin_fd: int | None,
        outer_stdout_fd: int | None,
    ) -> None:
        match redirect.stdin:
            case FromFile(path=path):
                stdin_fd = self._open_file(path, os.O_RDONLY)
            case FromData(data=data):
                stdin_fd = self._alloc_data_pipe(data)
            case _:
                stdin_fd = outer_stdin_fd

        match redirect.stdout:
            case ToFile(path=path, append=do_append):
                flags = os.O_WRONLY | os.O_CREAT
                flags |= os.O_APPEND if do_append else os.O_TRUNC
                stdout_fd = self._open_file(path, flags)
            case _:
                stdout_fd = outer_stdout_fd

        self._prepare(redirect.inner, stdin_fd, stdout_fd)

    def _prepare_pipeline(
        self, pipeline: Pipeline, stdin_fd: int | None, stdout_fd: int | None
    ) -> None:
        stages = pipeline.stages
        if not stages:
            return

        inter_pipes = [self._alloc_pipe() for _ in range(len(stages) - 1)]

        for i, stage in enumerate(stages):
            stage_stdin = stdin_fd if i == 0 else inter_pipes[i - 1][0]
            is_last = i == len(stages) - 1
            stage_stdout = stdout_fd if is_last else inter_pipes[i][1]
            self._prepare(stage, stage_stdin, stage_stdout)

    async def _create_processes(self) -> None:
        """Spawn all prepared processes concurrently."""
        coros = [
            create_subprocess_exec(
                *prep.args,
                stdin=prep.stdin_fd,
                stdout=prep.stdout_fd,
                pass_fds=prep.pass_fds,
            )
            for prep in self.prepared
        ]
        self.procs = list(await asyncio.gather(*coros))

    async def execute(
        self,
        cmd: Runnable,
        stdout_fd: int | None = None,
    ) -> Result:
        """Execute a command and return Result with exit code.

        Args:
            cmd: Command, pipeline, or redirect to execute.
            stdout_fd: File descriptor for process stdout (e.g., write end of pipe).
        """
        try:
            if stdout_fd is not None:
                self.fds.append(FdEntry(stdout_fd))
            self._prepare(cmd, None, stdout_fd)
            await self._create_processes()

            data_writes: list[Coroutine[None, None, None]] = []
            for entry in self.fds:
                if entry.data is None:
                    os.close(entry.fd)
                else:
                    data_writes.append(async_write(entry.fd, entry.data))

            await asyncio.gather(
                *[proc.wait() for proc in self.procs],
                *data_writes,
            )

            for proc in self.procs:
                code = self._normalize_returncode(proc.returncode)
                if code != 0:
                    return Result(code)
            return Result(0)
        finally:
            # TODO: fds closed in loop above get double-closed here; track closed state
            for entry in self.fds:
                close_fd(entry.fd)

    @staticmethod
    def _normalize_returncode(code: int | None) -> int:
        """Convert returncode to bash-style: 128 + signal for killed processes."""
        if code is None:
            return 0
        if code < 0:
            return 128 + (-code)
        return code


async def run(cmd: Runnable) -> int:
    """Execute a command, pipeline, or redirect and return exit code."""
    result = await Executor().execute(cmd)
    return result.code


@overload
async def out(cmd: Runnable, encoding: None) -> bytes: ...
@overload
async def out(cmd: Runnable, encoding: str = "utf-8") -> str: ...


async def out(cmd: Runnable, encoding: str | None = "utf-8") -> str | bytes:
    """Execute command and return stdout.

    Args:
        cmd: Command to execute.
        encoding: Decode stdout with this encoding. None for raw bytes.

    Raises subprocess.CalledProcessError on non-zero exit code.
    """
    read_fd, write_fd = os.pipe()
    executor = Executor()
    try:
        # Read and execute concurrently to avoid deadlock
        result, stdout = await asyncio.gather(
            executor.execute(cmd, stdout_fd=write_fd),
            async_read(read_fd),
        )
        if result.code != 0:
            raise subprocess.CalledProcessError(result.code, [], stdout)
        if encoding is None:
            return stdout
        return stdout.decode(encoding)
    finally:
        close_fd(read_fd)
