"""Runtime execution of shell commands and pipelines."""

from __future__ import annotations

import asyncio
import os
import subprocess
from asyncio.subprocess import Process, create_subprocess_exec
from collections.abc import Callable, Coroutine, Generator
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
class OwnedFd:
    """Tracked file descriptor with idempotent close and optional data to write."""

    fd: int
    data: str | bytes | None = None
    closed: bool = False

    def close(self) -> None:
        """Close the fd if not already closed."""
        if not self.closed:
            self.closed = True
            close_fd(self.fd)


@dataclass
class CmdNode:
    """A spawned command process (for pipefail tracking)."""

    proc: Process

    def root_procs(self) -> Generator[Process]:
        """Just the main process."""
        yield self.proc


@dataclass
class PipelineNode:
    """Pipeline stages (for pipefail tracking)."""

    stages: list[CmdNode]

    def root_procs(self) -> Generator[Process]:
        """Stage procs only (for pipefail)."""
        for stage in self.stages:
            yield stage.proc


ProcessNode = CmdNode | PipelineNode


@dataclass
class Result:
    """Execution result with exit code and optional captured stdout."""

    code: int
    stdout: bytes | None = None


async def _kill_and_reap(*procs: Process) -> None:
    """Kill and reap processes, shielded from cancellation."""
    reap: list[Coroutine[None, None, int]] = []
    for proc in procs:
        if proc.returncode is None:
            proc.kill()
            reap.append(proc.wait())
    if reap:
        await asyncio.shield(asyncio.gather(*reap))


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
    """Spawns and executes a Runnable as a process tree.

    All allocated fds and spawned processes are tracked in flat lists
    so that execute()'s finally block can clean up everything in one
    place — even if a spawn fails partway through or tasks are cancelled.
    """

    def __init__(self) -> None:
        self.fds: list[OwnedFd] = []
        self.procs: list[Process] = []

    async def _exec(
        self,
        *args: str,
        stdin: int | None = None,
        stdout: int | None = None,
        pass_fds: tuple[int, ...] = (),
        preexec_fn: Callable[[], None] | None = None,
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
    ) -> Process:
        """Spawn a subprocess, tracking it for cleanup."""
        proc = await create_subprocess_exec(
            *args,
            stdin=stdin,
            stdout=stdout,
            pass_fds=pass_fds,
            preexec_fn=preexec_fn,
            cwd=cwd,
            env=env,
        )
        self.procs.append(proc)
        return proc

    def _pipe(self) -> tuple[OwnedFd, OwnedFd]:
        """Allocate a pipe, tracking both ends for cleanup."""
        read_fd, write_fd = os.pipe()
        read_entry = OwnedFd(read_fd)
        write_entry = OwnedFd(write_fd)
        self.fds.append(read_entry)
        self.fds.append(write_entry)
        return read_entry, write_entry

    def _data_pipe(self, data: str | bytes) -> OwnedFd:
        """Allocate a pipe for feeding data, tracking both ends for cleanup.

        Returns the read end; the write end is handled by async_write via
        the flat fd list.
        """
        read_fd, write_fd = os.pipe()
        os.set_blocking(write_fd, False)
        read_entry = OwnedFd(read_fd)
        self.fds.append(read_entry)
        self.fds.append(OwnedFd(write_fd, data))
        return read_entry

    async def _spawn(
        self, cmd: Runnable, stdin_fd: int | None, stdout_fd: int | None
    ) -> ProcessNode:
        """Spawn a Runnable, returning its process tree node."""
        match cmd:
            case Cmd():
                return await self._spawn_cmd(cmd, stdin_fd, stdout_fd)
            case Pipeline():
                return await self._spawn_pipeline(cmd, stdin_fd, stdout_fd)

    async def _spawn_cmd(
        self, cmd: Cmd, outer_stdin_fd: int | None, outer_stdout_fd: int | None
    ) -> CmdNode:
        """Spawn a Cmd, resolving redirects and Sub arguments.

        Two-layer redirect model matching POSIX semantics:

        Layer 1 — pipe wiring (Popen kwargs): stdin=/stdout= from outer
        pipeline. Popen does dup2(pipe_rd, 0) and dup2(pipe_wr, 1).

        Layer 2 — user redirects (preexec_fn via FdOps): ordered ops that
        execute after Popen's pipe dup2, so user redirects naturally
        override pipe wiring.
        """
        # Build FdOps simulation with initial live fds from pipeline
        fdo = FdOps(live={STDIN, STDOUT, STDERR})
        local_fds: list[OwnedFd] = []

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
                    pipe_r, pipe_w = self._pipe()
                    local_fds.extend([pipe_r, pipe_w])
                    await self._spawn(sub.cmd, pipe_r.fd, None)
                    fdo.add_live(pipe_w.fd)
                    fdo.move_fd(pipe_w.fd, target_fd)

                case FdFromFile(fd=target_fd, path=path):  # < file, 3< file
                    fdo.open(target_fd, path, os.O_RDONLY)

                case FdFromData(fd=target_fd, data=data):  # <<< "string"
                    pipe_r = self._data_pipe(data)
                    local_fds.append(pipe_r)
                    fdo.add_live(pipe_r.fd)
                    fdo.move_fd(pipe_r.fd, target_fd)

                case FdFromSub(fd=target_fd, sub=sub):  # < <(cmd), 3< <(cmd)
                    pipe_r, pipe_w = self._pipe()
                    local_fds.extend([pipe_r, pipe_w])
                    await self._spawn(sub.cmd, None, pipe_w.fd)
                    fdo.add_live(pipe_r.fd)
                    fdo.move_fd(pipe_r.fd, target_fd)

                case FdClose(fd=closed_fd):  # 3>&-
                    fdo.close(closed_fd)

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
                    pipe_r, pipe_w = self._pipe()
                    local_fds.extend([pipe_r, pipe_w])
                    await self._spawn(inner, pipe_r.fd, None)
                    resolved_args.append(f"/dev/fd/{pipe_w.fd}")
                    pass_fds.append(pipe_w.fd)
                case SubIn(cmd=inner):
                    pipe_r, pipe_w = self._pipe()
                    local_fds.extend([pipe_r, pipe_w])
                    await self._spawn(inner, None, pipe_w.fd)
                    resolved_args.append(f"/dev/fd/{pipe_r.fd}")
                    pass_fds.append(pipe_r.fd)

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

        # Spawn main process
        proc = await self._exec(
            *resolved_args,
            stdin=outer_stdin_fd,
            stdout=outer_stdout_fd,
            pass_fds=tuple(pass_fds),
            preexec_fn=preexec_fn,
            cwd=cmd.working_dir,
            env=proc_env,
        )

        # Close non-data fds (children have inherited them)
        for fd_entry in local_fds:
            fd_entry.close()

        return CmdNode(proc=proc)

    async def _spawn_pipeline(
        self, pipeline: Pipeline, stdin_fd: int | None, stdout_fd: int | None
    ) -> PipelineNode:
        """Spawn a Pipeline, connecting stages with pipes."""
        stages = pipeline.stages
        if not stages:
            return PipelineNode(stages=[])

        # Allocate inter-stage pipes
        inter_pipes = [self._pipe() for _ in range(len(stages) - 1)]

        # Spawn stages sequentially
        stage_nodes: list[CmdNode] = []
        for idx, stage in enumerate(stages):
            stage_stdin = stdin_fd if idx == 0 else inter_pipes[idx - 1][0].fd
            is_last = idx == len(stages) - 1
            stage_stdout = stdout_fd if is_last else inter_pipes[idx][1].fd
            node = await self._spawn_cmd(stage, stage_stdin, stage_stdout)
            stage_nodes.append(node)

        # Close inter-stage pipe fds (children have inherited them)
        for pipe_r, pipe_w in inter_pipes:
            pipe_r.close()
            pipe_w.close()

        return PipelineNode(stages=stage_nodes)

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
        capture_fd: OwnedFd | None = None
        if stdout_fd is not None:
            capture_fd = OwnedFd(stdout_fd)
            self.fds.append(capture_fd)
        try:
            root = await self._spawn(cmd, None, stdout_fd)

            # Close capture fd — child has inherited it
            if capture_fd is not None:
                capture_fd.close()

            # Hand data fds to async_write
            data_writes: list[Coroutine[None, None, None]] = []
            for fd_entry in self.fds:
                if fd_entry.data is not None and not fd_entry.closed:
                    data_writes.append(async_write(fd_entry.fd, fd_entry.data))
                    fd_entry.closed = True  # async_write takes ownership

            await asyncio.gather(
                *[proc.wait() for proc in self.procs],
                *data_writes,
            )

            # Pipefail: rightmost non-zero from root procs only.
            # Sub-process exit codes (process substitutions) are ignored.
            code = 0
            for proc in root.root_procs():
                proc_code = self._normalize_returncode(proc.returncode)
                if proc_code != 0:
                    code = proc_code
            return Result(code)
        finally:
            await _kill_and_reap(*self.procs)
            for fd_entry in self.fds:
                fd_entry.close()  # idempotent

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
