"""Runtime execution of shell commands and pipelines."""

from __future__ import annotations

import asyncio
import os
import subprocess
from asyncio.subprocess import Process, create_subprocess_exec
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from pathlib import Path
from typing import overload

from shish.aio import async_read, async_write, close_fd
from shish.fdops import STDERR, STDIN, STDOUT, FdOps, OpClose, OpDup2, OpOpen
from shish.ir import (
    Cmd,
    FdClose,
    FdFromData,
    FdFromFile,
    FdFromSub,
    FdToFd,
    FdToFile,
    FdToSub,
    Pipeline,
    Runnable,
    SubIn,
    SubOut,
)


@dataclass
class PreparedProcess:
    """Process ready to start with resolved fds."""

    args: list[str]
    stdin_fd: int | None = None
    stdout_fd: int | None = None
    pass_fds: tuple[int, ...] = ()
    preexec: Callable[[], None] | None = None
    cwd: Path | None = None
    env: dict[str, str] | None = None


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


def _build_preexec(
    ops: tuple[OpOpen | OpDup2 | OpClose, ...],
) -> Callable[[], None] | None:
    """Build a preexec_fn closure that executes all fd ops in the child.

    All operations (open, dup2, close) run in the child between fork()
    and exec(). Only async-signal-safe syscalls: open, dup2, close.

    Target fds from OpOpen are protected by pass_fds, so close_fds
    (which runs after preexec_fn) won't close them. The intermediate
    fd from os.open() gets dup2'd to the target then closed — close_fds
    cleans it up if we don't.
    """
    if not ops:
        return None

    def _preexec(frozen_ops: tuple[OpOpen | OpDup2 | OpClose, ...] = ops) -> None:
        for op in frozen_ops:
            match op:
                case OpOpen(fd=target_fd, path=path, flags=flags):
                    source_fd = os.open(path, flags, 0o644)
                    if source_fd != target_fd:
                        os.dup2(source_fd, target_fd)
                        os.close(source_fd)
                case OpDup2(src, dst):
                    os.dup2(src, dst)
                case OpClose(fd):
                    os.close(fd)

    return _preexec


class Executor:
    """Prepares and executes a Runnable."""

    fds: list[FdEntry]
    prepared: list[PreparedProcess]
    procs: list[Process]

    def __init__(self) -> None:
        self.fds = []
        self.prepared = []
        self.procs = []

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

    def _prepare(
        self, cmd: Runnable, stdin_fd: int | None, stdout_fd: int | None
    ) -> list[int]:
        """Prepare a Runnable, returning indices of root processes."""
        match cmd:
            case Cmd():
                return self._prepare_cmd(cmd, stdin_fd, stdout_fd)
            case Pipeline():
                return self._prepare_pipeline(cmd, stdin_fd, stdout_fd)

    def _prepare_cmd(
        self, cmd: Cmd, outer_stdin_fd: int | None, outer_stdout_fd: int | None
    ) -> list[int]:
        """Prepare a Cmd, resolving redirects and Sub arguments.

        Two-layer redirect model matching POSIX semantics:

        Layer 1 — pipe wiring (Popen kwargs): stdin=/stdout= from outer
        pipeline. Popen does dup2(pipe_rd, 0) and dup2(pipe_wr, 1).

        Layer 2 — user redirects (preexec_fn via FdOps): ordered ops that
        execute after Popen's pipe dup2, so user redirects naturally
        override pipe wiring.
        """
        # Build FdOps simulation with initial live fds from pipeline
        fdo = FdOps(live={STDIN, STDOUT, STDERR})

        # Feed IR redirects into FdOps
        for redirect in cmd.redirects:
            match redirect:
                case FdToFd(src=src_fd, dst=dst_fd):  # 2>&1
                    fdo.dup2(src_fd, dst_fd)

                case FdToFile(
                    fd=target_fd, path=path, append=do_append
                ):  # > file, >> file, 2> file
                    flags = os.O_WRONLY | os.O_CREAT
                    flags |= os.O_APPEND if do_append else os.O_TRUNC
                    fdo.open(target_fd, path, flags)

                case FdToSub(fd=target_fd, sub=sub):  # 1> >(cmd), 3> >(cmd)
                    read_fd, write_fd = self._alloc_pipe()
                    self._prepare(sub.cmd, read_fd, None)
                    fdo.add_live(write_fd)
                    fdo.move_fd(write_fd, target_fd)

                case FdFromFile(fd=target_fd, path=path):  # < file, 3< file
                    fdo.open(target_fd, path, os.O_RDONLY)

                case FdFromData(fd=target_fd, data=data):  # <<< "string"
                    pipe_read = self._alloc_data_pipe(data)
                    fdo.add_live(pipe_read)
                    fdo.move_fd(pipe_read, target_fd)

                case FdFromSub(fd=target_fd, sub=sub):  # < <(cmd), 3< <(cmd)
                    read_fd, write_fd = self._alloc_pipe()
                    self._prepare(sub.cmd, None, write_fd)
                    fdo.add_live(read_fd)
                    fdo.move_fd(read_fd, target_fd)

                case FdClose(fd=close_fd):  # 3>&-
                    fdo.close(close_fd)

        preexec_fn = _build_preexec(fdo.ops)

        # Resolve Sub arguments to /dev/fd/N paths
        resolved_args: list[str] = []
        # pass_fds: live fds > 2 (subprocess handles 0/1/2 via stdin=/stdout=)
        pass_fds = [fd for fd in fdo.keep_fds() if fd > STDERR]

        for arg in cmd.args:
            match arg:
                case str() as string:
                    resolved_args.append(string)
                case SubOut(cmd=inner):
                    read_fd, write_fd = self._alloc_pipe()
                    self._prepare(inner, read_fd, None)
                    resolved_args.append(f"/dev/fd/{write_fd}")
                    pass_fds.append(write_fd)
                case SubIn(cmd=inner):
                    read_fd, write_fd = self._alloc_pipe()
                    self._prepare(inner, None, write_fd)
                    resolved_args.append(f"/dev/fd/{read_fd}")
                    pass_fds.append(read_fd)

        # Build env overlay and resolve working directory
        proc_env: dict[str, str] | None = None
        if cmd.env_vars or cmd.working_dir is not None:
            proc_env = dict(os.environ)
            for key, value in cmd.env_vars:
                if value is None:
                    proc_env.pop(key, None)
                else:
                    proc_env[key] = value
            if cmd.working_dir is not None:
                proc_env["PWD"] = str(cmd.working_dir)

        root_idx = len(self.prepared)
        self.prepared.append(
            PreparedProcess(
                resolved_args,
                outer_stdin_fd,
                outer_stdout_fd,
                tuple(pass_fds),
                preexec_fn,
                cwd=cmd.working_dir,
                env=proc_env,
            )
        )
        return [root_idx]

    def _prepare_pipeline(
        self, pipeline: Pipeline, stdin_fd: int | None, stdout_fd: int | None
    ) -> list[int]:
        stages = pipeline.stages
        if not stages:
            return []

        root_indices: list[int] = []
        inter_pipes = [self._alloc_pipe() for _ in range(len(stages) - 1)]

        for idx, stage in enumerate(stages):
            stage_stdin = stdin_fd if idx == 0 else inter_pipes[idx - 1][0]
            is_last = idx == len(stages) - 1
            stage_stdout = stdout_fd if is_last else inter_pipes[idx][1]
            root_indices.extend(self._prepare_cmd(stage, stage_stdin, stage_stdout))
        return root_indices

    async def _create_processes(self) -> None:
        """Spawn all prepared processes sequentially.

        Sequential spawning ensures already-started processes are tracked
        in self.procs before a later spawn failure, so the finally block
        in execute() can kill and reap them.
        """
        for prep in self.prepared:
            proc = await create_subprocess_exec(
                *prep.args,
                stdin=prep.stdin_fd,
                stdout=prep.stdout_fd,
                pass_fds=prep.pass_fds,
                preexec_fn=prep.preexec,
                cwd=prep.cwd,
                env=prep.env,
            )
            self.procs.append(proc)

    async def execute(
        self,
        cmd: Runnable,
        stdout_fd: int | None = None,
    ) -> Result:
        """Execute a command and return Result with exit code.

        Args:
            cmd: Command or pipeline to execute.
            stdout_fd: File descriptor for process stdout (e.g., write end of pipe).
        """
        try:
            if stdout_fd is not None:
                self.fds.append(FdEntry(stdout_fd))
            root_indices = self._prepare(cmd, None, stdout_fd)
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

            # Pipefail: rightmost non-zero from root procs only.
            # Sub-process exit codes (process substitutions) are ignored.
            code = 0
            for idx in root_indices:
                proc_code = self._normalize_returncode(self.procs[idx].returncode)
                if proc_code != 0:
                    code = proc_code
            return Result(code)
        finally:
            # Kill live processes and reap. Shield from cancellation
            # to prevent zombies if our caller's task is cancelled.
            reap: list[Coroutine[None, None, int]] = []
            for proc in self.procs:
                if proc.returncode is None:
                    proc.kill()
                    reap.append(proc.wait())
            if reap:
                await asyncio.shield(asyncio.gather(*reap))

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
    """Execute a command or pipeline and return exit code."""
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
