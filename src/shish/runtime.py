"""Runtime execution of shell commands and pipelines."""

from __future__ import annotations

import asyncio
import os
import subprocess
from asyncio.subprocess import Process, create_subprocess_exec
from collections.abc import Awaitable, Callable, Coroutine, Generator
from dataclasses import dataclass, field
from pathlib import Path
from typing import overload

from shish.aio import ByteReadStream, ByteStageCtx, ByteWriteStream, OwnedFd
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
    Fn,
    Pipeline,
    Runnable,
    SubIn,
    SubOut,
)


@dataclass
class CmdNode:
    """Process tree node for a single spawned command.

    Holds the main process, owned fds allocated during spawn, and any
    substitution sub-processes (from FdToSub, FdFromSub, SubOut, SubIn
    redirects/args). Sub-processes are excluded from pipefail — only
    the main proc participates in exit code reporting.
    """

    proc: Process
    fds: list[OwnedFd] = field(default_factory=lambda: list[OwnedFd]())
    subs: list[ProcessNode] = field(default_factory=lambda: list[ProcessNode]())

    def root_procs(self) -> Generator[Process]:
        """Yield the main process only (for pipefail exit code reporting)."""
        yield self.proc

    def root_returncodes(self) -> Generator[int]:
        """Yield normalized returncode for pipefail reporting."""
        yield _normalize_returncode(self.proc.returncode)

    def all_procs(self) -> Generator[Process]:
        """Yield all processes in the subtree (main + subs, recursive)."""
        yield self.proc
        for sub in self.subs:
            yield from sub.all_procs()

    def all_fds(self) -> Generator[OwnedFd]:
        """Yield all owned fds in the subtree (own + subs, recursive)."""
        yield from self.fds
        for sub in self.subs:
            yield from sub.all_fds()

    def tasks(self) -> Generator[Coroutine[None, None, object]]:
        """Yield coroutines to gather: proc wait + sub tasks."""
        yield self.proc.wait()
        for sub in self.subs:
            yield from sub.tasks()


@dataclass
class PipelineNode:
    """Process tree node for a pipeline (cmd1 | cmd2 | ...).

    Each stage is a CmdNode or FnNode. Pipefail semantics:
    root_returncodes yields all stage returncodes left-to-right.
    Sub-processes within each stage are excluded from pipefail.
    """

    stages: list[CmdNode | FnNode]

    def root_procs(self) -> Generator[Process]:
        """Yield stage main procs left-to-right for pipefail reporting."""
        for stage in self.stages:
            yield from stage.root_procs()

    def root_returncodes(self) -> Generator[int]:
        """Yield stage returncodes left-to-right for pipefail reporting."""
        for stage in self.stages:
            yield from stage.root_returncodes()

    def all_procs(self) -> Generator[Process]:
        """Yield all processes across all stages (recursive)."""
        for stage in self.stages:
            yield from stage.all_procs()

    def all_fds(self) -> Generator[OwnedFd]:
        """Yield all owned fds across all stages (recursive)."""
        for stage in self.stages:
            yield from stage.all_fds()

    def tasks(self) -> Generator[Coroutine[None, None, object]]:
        """Yield coroutines to gather: all stage tasks."""
        for stage in self.stages:
            yield from stage.tasks()


@dataclass
class WriteNode:
    """Process tree node for an async data write (<<<).

    Leaf node — no process, just an fd and data to write. Participates
    in tree traversal (all_fds for cleanup) but contributes no processes
    to pipefail.
    """

    fd: OwnedFd
    data: str | bytes

    def root_procs(self) -> Generator[Process]:
        """No processes — data writes don't participate in pipefail."""
        yield from ()

    def root_returncodes(self) -> Generator[int]:
        """No returncodes — data writes don't participate in pipefail."""
        yield from ()

    def all_procs(self) -> Generator[Process]:
        """No processes."""
        yield from ()

    def all_fds(self) -> Generator[OwnedFd]:
        """Yield the write fd for cleanup."""
        yield self.fd

    def tasks(self) -> Generator[Coroutine[None, None, object]]:
        """Yield the data write coroutine."""
        yield self._write()

    async def _write(self) -> None:
        """Write data to the fd, then close."""
        writer = ByteWriteStream(self.fd)
        try:
            payload = (
                self.data.encode("utf-8") if isinstance(self.data, str) else self.data
            )
            await writer.write(payload)
        except OSError:
            pass
        finally:
            await writer.close()


@dataclass
class FnNode:
    """Process tree node for an in-process Python function.

    Runs a user-provided async function with ByteStageCtx. No OS
    process is spawned — the function runs as an asyncio task. Owns
    dup'd copies of stdin/stdout fds so pipeline close-after-spawn
    logic doesn't affect it.
    """

    fds: list[OwnedFd]
    _func: Callable[[ByteStageCtx], Awaitable[int]] = field(repr=False)  # type: ignore[type-arg]
    _stdin_fd: OwnedFd = field(repr=False)
    _stdout_fd: OwnedFd = field(repr=False)
    returncode: int = 0

    def root_procs(self) -> Generator[Process]:
        """No OS process."""
        yield from ()

    def root_returncodes(self) -> Generator[int]:
        """Yield the function's return code for pipefail reporting."""
        yield self.returncode

    def all_procs(self) -> Generator[Process]:
        """No OS process to kill."""
        yield from ()

    def all_fds(self) -> Generator[OwnedFd]:
        """Yield owned fds for cleanup."""
        yield from self.fds

    def tasks(self) -> Generator[Coroutine[None, None, object]]:
        """Yield the function execution coroutine."""
        yield self._run()

    async def _run(self) -> None:
        """Execute the user function with ByteStageCtx, then close streams."""
        stdin_stream = ByteReadStream(self._stdin_fd)
        stdout_stream = ByteWriteStream(self._stdout_fd)
        try:
            ctx = ByteStageCtx(stdin=stdin_stream, stdout=stdout_stream)
            self.returncode = await self._func(ctx)
        except Exception:
            import sys
            import traceback

            traceback.print_exc(file=sys.stderr)
            self.returncode = 1
        finally:
            await stdout_stream.close()
            await stdin_stream.close()


ProcessNode = CmdNode | PipelineNode | WriteNode | FnNode


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


@dataclass
class Execution:
    """Handle for a spawned process tree with a wait() method.

    Returned by prepare(). Callers can inspect the process tree via
    root before calling wait(). wait() derives processes and fds by
    recursing the tree, then gathers exits, computes pipefail, and
    cleans up.
    """

    root: ProcessNode

    async def wait(self) -> int:
        """Wait for all processes and return the pipefail exit code.

        Gathers all tasks (process waits + data writes) concurrently,
        then computes pipefail (rightmost non-zero from root_procs).
        Finally: SIGKILL+reap + close all fds (idempotent, shielded).
        """
        procs = list(self.root.all_procs())
        fds = list(self.root.all_fds())
        try:
            await asyncio.gather(*self.root.tasks())
            return self._pipefail_code()
        finally:
            await _kill_and_reap(*procs)
            for fd_entry in fds:
                fd_entry.close()

    def _pipefail_code(self) -> int:
        """Compute pipefail exit code: rightmost non-zero from root returncodes."""
        code = 0
        for returncode in self.root.root_returncodes():
            if returncode != 0:
                code = returncode
        return code


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
        case Fn():
            return _spawn_fn(ctx, cmd, stdin_fd, stdout_fd)
        case Pipeline():
            return await _spawn_pipeline(ctx, cmd, stdin_fd, stdout_fd)


def _spawn_fn(
    ctx: PrepareCtx,
    fn_ir: Fn,
    stdin_fd: int | None,
    stdout_fd: int | None,
) -> FnNode:
    """Create an FnNode for an in-process Python function.

    Dups the received fds so the pipeline's close-after-spawn logic
    (which closes the originals) doesn't affect the Fn. When stdin_fd
    or stdout_fd is None, dups the parent's STDIN/STDOUT to match
    subprocess inherit behavior.
    """
    dup_stdin = OwnedFd(os.dup(stdin_fd if stdin_fd is not None else STDIN))
    dup_stdout = OwnedFd(os.dup(stdout_fd if stdout_fd is not None else STDOUT))
    ctx.fds.append(dup_stdin)
    ctx.fds.append(dup_stdout)
    return FnNode(
        fds=[dup_stdin, dup_stdout],
        _func=fn_ir.func,
        _stdin_fd=dup_stdin,
        _stdout_fd=dup_stdout,
    )


async def _spawn_cmd(
    ctx: PrepareCtx,
    cmd: Cmd,
    stdin_fd: int | None,
    stdout_fd: int | None,
) -> CmdNode:
    """Spawn a single Cmd with all its redirects and sub-processes.

    Redirect resolution follows a two-layer model matching POSIX:

    Layer 1 — pipe wiring (Popen kwargs): outer_stdin_fd/outer_stdout_fd from the parent
    pipeline become stdin=/stdout= on the subprocess call. Popen internally does
    dup2(pipe_rd, 0) / dup2(pipe_wr, 1).

    Layer 2 — user redirects (preexec_fn via FdOps): ordered fd ops that execute in the
    child after Popen's pipe dup2, so user redirects (>, <, 2>&1, etc.) naturally
    override pipe wiring.

    Process substitutions (FdToSub, FdFromSub) and Sub arguments (SubOut, SubIn) each
    allocate a pipe and register a spawn coroutine. All sub spawns are deferred and run
    concurrently with the main process spawn via nested asyncio.gather — this is safe
    because pipe fds are allocated eagerly (before any spawn), so all processes inherit
    the correct fds regardless of spawn order.

    After all processes have been spawned, close_after_spawn fds (pipe ends used only
    for child inheritance) are closed in the parent — children have inherited them via
    fork, so the parent must close its copies to allow EOF propagation.
    """
    # Build FdOps simulation with initial live fds from pipeline
    fdo = FdOps(live={STDIN, STDOUT, STDERR})
    owned_fds: list[OwnedFd] = []
    close_after_spawn: list[OwnedFd] = []
    pending_spawns: list[Coroutine[None, None, ProcessNode]] = []
    children: list[ProcessNode] = []

    def spawn_with_pipe(inner: Runnable, *, stdin: bool) -> tuple[OwnedFd, OwnedFd]:
        """Allocate pipe, track fds, register in fdo, schedule sub spawn.

        stdin=True: sub reads from pipe (stdin=pipe_r), parent keeps pipe_w.
        stdin=False: sub writes to pipe (stdout=pipe_w), parent keeps pipe_r.
        """
        pipe_r, pipe_w = ctx.pipe()
        owned_fds.extend([pipe_r, pipe_w])
        close_after_spawn.extend([pipe_r, pipe_w])
        if stdin:
            fdo.add_live(pipe_w.fd)
            pending_spawns.append(_spawn(ctx, inner, pipe_r.fd, None))
        else:
            fdo.add_live(pipe_r.fd)
            pending_spawns.append(_spawn(ctx, inner, None, pipe_w.fd))
        return pipe_r, pipe_w

    def feed_with_pipe(data: str | bytes) -> OwnedFd:
        """Allocate pipe, schedule async data write, return read end."""
        pipe_r, pipe_w = ctx.pipe()
        owned_fds.extend([pipe_r, pipe_w])
        close_after_spawn.append(pipe_r)
        fdo.add_live(pipe_r.fd)
        children.append(WriteNode(fd=pipe_w, data=data))
        return pipe_r

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
                _, pipe_w = spawn_with_pipe(sub.cmd, stdin=True)
                fdo.move_fd(pipe_w.fd, target_fd)

            case FdFromFile(fd=target_fd, path=path):  # < file, 3< file
                fdo.open(target_fd, path, os.O_RDONLY)

            case FdFromData(fd=target_fd, data=data):  # <<< "string"
                pipe_r = feed_with_pipe(data)
                fdo.move_fd(pipe_r.fd, target_fd)

            case FdFromSub(fd=target_fd, sub=sub):  # < <(cmd), 3< <(cmd)
                pipe_r, _ = spawn_with_pipe(sub.cmd, stdin=False)
                fdo.move_fd(pipe_r.fd, target_fd)

            case FdClose(fd=closed_fd):  # 3>&-
                fdo.close(closed_fd)

    # Resolve Sub arguments to /dev/fd/N paths
    resolved_args: list[str] = []
    for arg in cmd.args:
        match arg:
            case str() as string:
                resolved_args.append(string)
            case SubOut(cmd=inner):
                _, pipe_w = spawn_with_pipe(inner, stdin=True)
                resolved_args.append(f"/dev/fd/{pipe_w.fd}")
            case SubIn(cmd=inner):
                pipe_r, _ = spawn_with_pipe(inner, stdin=False)
                resolved_args.append(f"/dev/fd/{pipe_r.fd}")

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
    proc, spawned = await asyncio.gather(
        ctx.exec_(
            *resolved_args,
            stdin=stdin_fd,
            stdout=stdout_fd,
            # pass_fds: live fds > 2 (subprocess handles 0/1/2 via stdin=/stdout=)
            pass_fds=tuple(fd for fd in fdo.keep_fds() if fd > STDERR),
            preexec_fn=_build_preexec(fdo.ops),
            cwd=cmd.working_dir,
            env=proc_env,
        ),
        asyncio.gather(*pending_spawns),
    )

    # Close inherited fds in parent so EOF propagates
    for fd_entry in close_after_spawn:
        fd_entry.close()

    children.extend(spawned)
    return CmdNode(proc=proc, fds=owned_fds, subs=children)


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

    # Spawn stages concurrently (Cmd stages are async, Fn stages are sync)
    fn_nodes: list[tuple[int, FnNode]] = []
    spawn_tasks: list[Coroutine[None, None, CmdNode]] = []
    spawn_indices: list[int] = []
    for idx, (stage, sin, sout) in enumerate(
        zip(stages, stage_stdins, stage_stdouts, strict=True)
    ):
        if isinstance(stage, Fn):
            fn_nodes.append((idx, _spawn_fn(ctx, stage, sin, sout)))
        else:
            spawn_indices.append(idx)
            spawn_tasks.append(_spawn_cmd(ctx, stage, sin, sout))
    cmd_nodes = list(await asyncio.gather(*spawn_tasks))

    # Merge results in original order
    stage_nodes: list[CmdNode | FnNode] = [None] * len(stages)  # type: ignore[list-item]
    for idx, node in zip(spawn_indices, cmd_nodes, strict=True):
        stage_nodes[idx] = node
    for idx, node in fn_nodes:
        stage_nodes[idx] = node

    # Close inter-stage pipe fds (children have inherited them)
    for pipe_r, pipe_w in inter_pipes:
        pipe_r.close()
        pipe_w.close()

    return PipelineNode(stages=stage_nodes)


async def prepare(
    cmd: Runnable,
    stdin_fd: int | None = None,
    stdout_fd: int | None = None,
) -> Execution:
    """Spawn a process tree and return an Execution handle.

    Creates a PrepareCtx, spawns all processes, closes the capture fd
    (child inherited it), and returns an Execution whose wait() method
    gathers exits and computes pipefail. On spawn failure, cleans up
    before re-raising.

    Args:
        cmd: Command or pipeline to spawn.
        stdin_fd: Read end of a pipe for feeding stdin.
            Caller owns the write end; this function closes the
            read end after fork so writers see SIGPIPE/EPIPE.
        stdout_fd: Write end of a pipe for capturing stdout.
            Caller owns the read end; this function closes the
            write end after fork so readers see EOF.
    """
    caller_fds = [OwnedFd(fd) for fd in (stdin_fd, stdout_fd) if fd is not None]
    ctx = PrepareCtx(fds=list(caller_fds))
    try:
        root = await _spawn(ctx, cmd, stdin_fd, stdout_fd)
    except BaseException:
        await _kill_and_reap(*ctx.procs)
        for fd_entry in ctx.fds:
            fd_entry.close()
        raise

    # Close caller fds — children have inherited them via fork
    for fd_entry in caller_fds:
        fd_entry.close()

    return Execution(root=root)


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
    owned_read = OwnedFd(read_fd)
    try:
        execution = await prepare(cmd, stdout_fd=write_fd)
    except BaseException:
        owned_read.close()
        raise
    reader = ByteReadStream(owned_read)
    try:
        code, stdout = await asyncio.gather(
            execution.wait(),
            reader.read(),
        )
    finally:
        await reader.close()
    if code != 0:
        raise subprocess.CalledProcessError(code, [], stdout)
    if encoding is None:
        return stdout
    return stdout.decode(encoding)
