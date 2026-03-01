"""Runtime execution of shell commands and pipelines."""

from __future__ import annotations

import asyncio
import os
import subprocess
from asyncio.subprocess import Process, create_subprocess_exec
from collections.abc import Callable, Coroutine, Generator
from dataclasses import dataclass, field
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
    """Process tree node for a single spawned command.

    Holds the main process and any substitution sub-processes (from
    FdToSub, FdFromSub, SubOut, SubIn redirects/args). Sub-processes
    are tracked here for tree completeness but are excluded from
    pipefail — only the main proc participates in exit code reporting.

    All processes (main + subs) are also registered in PrepareCtx.procs
    for flat cleanup regardless of tree structure.
    """

    proc: Process
    subs: list[ProcessNode] = field(default_factory=lambda: list[ProcessNode]())

    def root_procs(self) -> Generator[Process]:
        """Yield the main process only (for pipefail exit code reporting).

        Substitution sub-processes are intentionally excluded — their
        exit codes are ignored, matching bash process substitution
        semantics.
        """
        yield self.proc


@dataclass
class PipelineNode:
    """Process tree node for a pipeline (cmd1 | cmd2 | ...).

    Each stage is a CmdNode. Pipefail semantics: root_procs yields
    all stage procs left-to-right, and execute() reports the rightmost
    non-zero exit code. Sub-processes within each stage (process
    substitutions) are excluded from pipefail.
    """

    stages: list[CmdNode]

    def root_procs(self) -> Generator[Process]:
        """Yield stage main procs left-to-right for pipefail reporting.

        Sub-processes within each stage are excluded — only the
        top-level pipeline participants determine the exit code.
        """
        for stage in self.stages:
            yield stage.proc


ProcessNode = CmdNode | PipelineNode


@dataclass
class Execution:
    """Handle for a spawned process tree with a wait() method.

    Returned by prepare(). Callers can inspect the process tree via
    root before calling wait(). wait() gathers all process exits and
    data writes, computes the pipefail exit code, then cleans up.
    """

    root: ProcessNode
    _procs: list[Process]
    _fds: list[OwnedFd]

    async def wait(self) -> int:
        """Wait for all processes and return the pipefail exit code.

        Gathers all process waits and data writes concurrently, then
        computes pipefail (rightmost non-zero from root_procs).
        Finally: SIGKILL+reap + close all fds (idempotent, shielded).
        """
        try:
            data_writes: list[Coroutine[None, None, None]] = []
            for fd_entry in self._fds:
                if fd_entry.data is not None and not fd_entry.closed:
                    data_writes.append(async_write(fd_entry.fd, fd_entry.data))
                    fd_entry.closed = True  # async_write takes ownership

            await asyncio.gather(
                *[proc.wait() for proc in self._procs],
                *data_writes,
            )

            code = 0
            for proc in self.root.root_procs():
                proc_code = _normalize_returncode(proc.returncode)
                if proc_code != 0:
                    code = proc_code
            return code
        finally:
            await _kill_and_reap(*self._procs)
            for fd_entry in self._fds:
                fd_entry.close()


def _normalize_returncode(code: int | None) -> int:
    """Convert returncode to bash-style: 128 + signal for killed processes."""
    if code is None:
        return 0
    if code < 0:
        return 128 + (-code)
    return code


async def _kill_and_reap(*procs: Process) -> None:
    """SIGKILL all still-running processes and wait for them to exit.

    Shielded from cancellation so that cleanup completes even if the
    calling task is cancelled — without this, orphan zombies would
    accumulate. Only processes with returncode=None (not yet reaped)
    are killed; already-exited processes are skipped.
    """
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


@dataclass
class PrepareCtx:
    """Tracks fds and procs during spawn for cleanup and ownership transfer."""

    fds: list[OwnedFd] = field(default_factory=lambda: list[OwnedFd]())
    procs: list[Process] = field(default_factory=lambda: list[Process]())

    async def exec_(
        self,
        *args: str,
        stdin: int | None = None,
        stdout: int | None = None,
        pass_fds: tuple[int, ...] = (),
        preexec_fn: Callable[[], None] | None = None,
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
    ) -> Process:
        """Spawn a subprocess via create_subprocess_exec and register it.

        stdin/stdout are raw fds (or None for inherit); Popen does
        dup2 to wire them to fds 0/1 in the child. pass_fds keeps
        additional fds open across exec (fds > 2 used by redirects
        and process substitutions). preexec_fn runs between fork and
        exec to apply user redirect ops (open, dup2, close).
        """
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

    def pipe(self) -> tuple[OwnedFd, OwnedFd]:
        """Allocate an os.pipe(), tracking both ends for cleanup.

        Returns (read_end, write_end). Both are registered so the
        finally block can close them even if spawn fails before the
        caller gets to close them after fork inheritance.
        """
        read_fd, write_fd = os.pipe()
        read_entry = OwnedFd(read_fd)
        write_entry = OwnedFd(write_fd)
        self.fds.append(read_entry)
        self.fds.append(write_entry)
        return read_entry, write_entry

    def data_pipe(self, data: str | bytes) -> OwnedFd:
        """Allocate a pipe for feeding data (<<< "string" / FdFromData).

        Returns the read end for wiring into the child's stdin (or
        target fd). The write end is stored with its data payload
        attached — wait() later hands it to async_write, which takes
        ownership and closes it after writing.

        The write end is set non-blocking so async_write can use the
        event loop's writer callbacks without stalling.
        """
        read_fd, write_fd = os.pipe()
        os.set_blocking(write_fd, False)
        read_entry = OwnedFd(read_fd)
        self.fds.append(read_entry)
        self.fds.append(OwnedFd(write_fd, data))
        return read_entry


async def _spawn(
    ctx: PrepareCtx, cmd: Runnable, stdin_fd: int | None, stdout_fd: int | None
) -> ProcessNode:
    """Dispatch a Runnable to _spawn_cmd or _spawn_pipeline.

    stdin_fd/stdout_fd are the outer pipe fds from a parent
    pipeline (or None for inherit/capture). Returns a ProcessNode
    for tree-based pipefail reporting.
    """
    match cmd:
        case Cmd():
            return await _spawn_cmd(ctx, cmd, stdin_fd, stdout_fd)
        case Pipeline():
            return await _spawn_pipeline(ctx, cmd, stdin_fd, stdout_fd)


async def _spawn_cmd(
    ctx: PrepareCtx,
    cmd: Cmd,
    outer_stdin_fd: int | None,
    outer_stdout_fd: int | None,
) -> CmdNode:
    """Spawn a single Cmd with all its redirects and sub-processes.

    Redirect resolution follows a two-layer model matching POSIX:

    Layer 1 — pipe wiring (Popen kwargs): outer_stdin_fd/outer_stdout_fd
    from the parent pipeline become stdin=/stdout= on the subprocess
    call. Popen internally does dup2(pipe_rd, 0) / dup2(pipe_wr, 1).

    Layer 2 — user redirects (preexec_fn via FdOps): ordered fd ops
    that execute in the child after Popen's pipe dup2, so user
    redirects (>, <, 2>&1, etc.) naturally override pipe wiring.

    Process substitutions (FdToSub, FdFromSub) and Sub arguments
    (SubOut, SubIn) each allocate a pipe and register a spawn
    coroutine. All sub spawns are deferred and run concurrently
    with the main process spawn via nested asyncio.gather — this
    is safe because pipe fds are allocated eagerly (before any
    spawn), so all processes inherit the correct fds regardless
    of spawn order.

    After all processes have been spawned, local_fds (pipe ends
    used only for child inheritance) are closed in the parent —
    children have inherited them via fork, so the parent must
    close its copies to allow EOF propagation.
    """
    # Build FdOps simulation with initial live fds from pipeline
    fdo = FdOps(live={STDIN, STDOUT, STDERR})
    local_fds: list[OwnedFd] = []
    sub_spawns: list[Coroutine[None, None, ProcessNode]] = []

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
                pipe_r, pipe_w = ctx.pipe()
                local_fds.extend([pipe_r, pipe_w])
                sub_spawns.append(_spawn(ctx, sub.cmd, pipe_r.fd, None))
                fdo.add_live(pipe_w.fd)
                fdo.move_fd(pipe_w.fd, target_fd)

            case FdFromFile(fd=target_fd, path=path):  # < file, 3< file
                fdo.open(target_fd, path, os.O_RDONLY)

            case FdFromData(fd=target_fd, data=data):  # <<< "string"
                pipe_r = ctx.data_pipe(data)
                local_fds.append(pipe_r)
                fdo.add_live(pipe_r.fd)
                fdo.move_fd(pipe_r.fd, target_fd)

            case FdFromSub(fd=target_fd, sub=sub):  # < <(cmd), 3< <(cmd)
                pipe_r, pipe_w = ctx.pipe()
                local_fds.extend([pipe_r, pipe_w])
                sub_spawns.append(_spawn(ctx, sub.cmd, None, pipe_w.fd))
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
                pipe_r, pipe_w = ctx.pipe()
                local_fds.extend([pipe_r, pipe_w])
                sub_spawns.append(_spawn(ctx, inner, pipe_r.fd, None))
                resolved_args.append(f"/dev/fd/{pipe_w.fd}")
                pass_fds.append(pipe_w.fd)
            case SubIn(cmd=inner):
                pipe_r, pipe_w = ctx.pipe()
                local_fds.extend([pipe_r, pipe_w])
                sub_spawns.append(_spawn(ctx, inner, None, pipe_w.fd))
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

    # Spawn main process and sub-processes concurrently
    proc, sub_nodes = await asyncio.gather(
        ctx.exec_(
            *resolved_args,
            stdin=outer_stdin_fd,
            stdout=outer_stdout_fd,
            pass_fds=tuple(pass_fds),
            preexec_fn=preexec_fn,
            cwd=cmd.working_dir,
            env=proc_env,
        ),
        asyncio.gather(*sub_spawns),
    )

    # Close non-data fds (children have inherited them)
    for fd_entry in local_fds:
        fd_entry.close()

    return CmdNode(proc=proc, subs=sub_nodes)


async def _spawn_pipeline(
    ctx: PrepareCtx,
    pipeline: Pipeline,
    stdin_fd: int | None,
    stdout_fd: int | None,
) -> PipelineNode:
    """Spawn all stages of a pipeline, connected by inter-stage pipes.

    Pipes are allocated eagerly before any spawns, then all stages
    are spawned concurrently via asyncio.gather. This is safe
    because each stage only needs its stdin/stdout fds to be valid
    at spawn time, and pipe allocation is synchronous.

    Outer stdin_fd/stdout_fd are wired to the first/last stage
    respectively, with inter-stage pipes connecting the rest.
    After all children have forked, inter-stage pipe fds are
    closed in the parent so EOF propagates when stages exit.
    """
    stages = pipeline.stages
    if not stages:
        return PipelineNode(stages=[])

    # Allocate inter-stage pipes
    inter_pipes = [ctx.pipe() for _ in range(len(stages) - 1)]

    # Per-stage stdin/stdout: outer fds at the edges, pipes in between
    stage_stdins = [stdin_fd] + [pipe_r.fd for pipe_r, _ in inter_pipes]
    stage_stdouts = [pipe_w.fd for _, pipe_w in inter_pipes] + [stdout_fd]

    # Spawn stages concurrently
    spawn_tasks: list[Coroutine[None, None, CmdNode]] = []
    for stage, sin, sout in zip(stages, stage_stdins, stage_stdouts, strict=True):
        spawn_tasks.append(_spawn_cmd(ctx, stage, sin, sout))
    stage_nodes = list(await asyncio.gather(*spawn_tasks))

    # Close inter-stage pipe fds (children have inherited them)
    for pipe_r, pipe_w in inter_pipes:
        pipe_r.close()
        pipe_w.close()

    return PipelineNode(stages=stage_nodes)


async def prepare(cmd: Runnable, stdout_fd: int | None = None) -> Execution:
    """Spawn a process tree and return an Execution handle.

    Creates a PrepareCtx, spawns all processes, closes the capture fd
    (child inherited it), and returns an Execution whose wait() method
    gathers exits and computes pipefail. On spawn failure, cleans up
    before re-raising.

    Args:
        cmd: Command or pipeline to spawn.
        stdout_fd: Write end of a pipe for capturing stdout.
            Caller owns the read end; this function closes the
            write end after fork so readers see EOF.
    """
    ctx = PrepareCtx()
    capture_fd: OwnedFd | None = None
    if stdout_fd is not None:
        capture_fd = OwnedFd(stdout_fd)
        ctx.fds.append(capture_fd)
    try:
        root = await _spawn(ctx, cmd, None, stdout_fd)
    except BaseException:
        await _kill_and_reap(*ctx.procs)
        for fd_entry in ctx.fds:
            fd_entry.close()
        raise

    # Close capture fd — child has inherited it
    if capture_fd is not None:
        capture_fd.close()

    return Execution(root=root, _procs=ctx.procs, _fds=ctx.fds)


async def run(cmd: Runnable) -> int:
    """Execute a command or pipeline and return exit code."""
    return await (await prepare(cmd)).wait()


@overload
async def out(cmd: Runnable, encoding: None) -> bytes: ...
@overload
async def out(cmd: Runnable, encoding: str = "utf-8") -> str: ...


async def out(cmd: Runnable, encoding: str | None = "utf-8") -> str | bytes:
    """Execute a command and return its captured stdout.

    Allocates a pipe, spawns via prepare(), then reads stdout
    concurrently with wait(). Concurrency is required to avoid
    deadlock: if the child fills the pipe buffer, it blocks until
    someone reads, but if we wait() first we never read.

    Args:
        cmd: Command to execute.
        encoding: Decode stdout with this encoding. None for raw bytes.

    Raises:
        subprocess.CalledProcessError: On non-zero exit code, with
            the captured stdout attached for diagnostic use.
    """
    read_fd, write_fd = os.pipe()
    try:
        execution = await prepare(cmd, stdout_fd=write_fd)
        code, stdout = await asyncio.gather(
            execution.wait(),
            async_read(read_fd),
        )
        if code != 0:
            raise subprocess.CalledProcessError(code, [], stdout)
        if encoding is None:
            return stdout
        return stdout.decode(encoding)
    finally:
        close_fd(read_fd)
