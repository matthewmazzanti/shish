"""Runtime execution of shell commands and pipelines.

Fd ownership invariant:
- All opened fds are closed after spawn by the opener
  (except FnNode dups, which stay open for in-process execution).
- All fds are tracked by SpawnCtx, allowing cleanup on errors.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal as signal_mod
import subprocess
import sys
import traceback
from asyncio.subprocess import Process, create_subprocess_exec
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import overload

from shish.aio import (
    ByteReadStream,
    ByteStageCtx,
    ByteWriteStream,
    OwnedFd,
    TextStageCtx,
    make_byte_wrapper,
)
from shish.fdops import PIPE, STDERR, STDIN, STDOUT, FdOps, OpClose, OpDup2, OpOpen
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

SUBPROCESS_DEFAULT_FDS = frozenset({STDIN, STDOUT, STDERR})
FD_DIR = Path("/dev/fd")


@dataclass
class StdFds:
    """Owned stdin/stdout fds for a spawn subtree.

    Each field is an OwnedFd — dup'd at start() entry from caller fds
    or parent STDIN/STDOUT, or allocated by pipeline pipe wiring.
    """

    stdin: OwnedFd
    stdout: OwnedFd


@dataclass
class CmdNode:
    """Process tree node for a single spawned command.

    Holds the main process, fds allocated during spawn, and any
    substitution sub-processes (from FdToSub, FdFromSub, SubOut, SubIn
    redirects/args). Fds are closed in the parent immediately after spawn
    (children inherit via fork); Execution.wait() closes again idempotently.
    Sub-processes are excluded from pipefail — only the main proc
    participates in exit code reporting.
    """

    proc: Process
    fds: list[OwnedFd] = field(default_factory=lambda: list[OwnedFd]())
    subs: list[ProcessNode] = field(default_factory=lambda: list[ProcessNode]())

    def root_returncodes(self) -> Iterator[int]:
        """Yield normalized returncode for pipefail reporting."""
        yield _normalize_returncode(self.proc.returncode)

    def all_procs(self) -> Iterator[Process]:
        """Yield all processes in the subtree (main + subs, recursive)."""
        yield self.proc
        for sub in self.subs:
            yield from sub.all_procs()

    def all_fds(self) -> Iterator[OwnedFd]:
        """Yield all owned fds in the subtree (own + subs, recursive)."""
        yield from self.fds
        for sub in self.subs:
            yield from sub.all_fds()

    def tasks(self) -> Iterator[Awaitable[object]]:
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

    def root_returncodes(self) -> Iterator[int]:
        """Yield stage returncodes left-to-right for pipefail reporting."""
        for stage in self.stages:
            yield from stage.root_returncodes()

    def all_procs(self) -> Iterator[Process]:
        """Yield all processes across all stages (recursive)."""
        for stage in self.stages:
            yield from stage.all_procs()

    def all_fds(self) -> Iterator[OwnedFd]:
        """Yield all owned fds across all stages (recursive)."""
        for stage in self.stages:
            yield from stage.all_fds()

    def tasks(self) -> Iterator[Awaitable[object]]:
        """Yield coroutines to gather: all stage tasks."""
        for stage in self.stages:
            yield from stage.tasks()


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
    returncode: int | None = None

    def root_returncodes(self) -> Iterator[int]:
        """Yield the function's return code for pipefail reporting."""
        if self.returncode is None:
            raise RuntimeError("FnNode.returncode is None — task never completed")
        yield self.returncode

    def all_procs(self) -> Iterator[Process]:
        """No OS process to kill."""
        yield from ()

    def all_fds(self) -> Iterator[OwnedFd]:
        """Yield owned fds for cleanup."""
        yield from self.fds

    def tasks(self) -> Iterator[Awaitable[object]]:
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
            traceback.print_exc(file=sys.stderr)
            self.returncode = 1
        finally:
            await stdout_stream.close()
            await stdin_stream.close()


ProcessNode = CmdNode | PipelineNode | FnNode


def _normalize_returncode(code: int | None) -> int:
    """Convert returncode to bash-style: 128 + signal for killed processes."""
    if code is None:
        raise RuntimeError("Process returncode is None — process never reaped")
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
    reap: list[Awaitable[int]] = []
    for proc in procs:
        if proc.returncode is None:
            proc.kill()
            reap.append(proc.wait())
    if reap:
        await asyncio.shield(asyncio.gather(*reap))


@dataclass
class Execution:
    """Handle for a spawned process tree.

    Yielded by start(). Provides signal/terminate/kill for explicit
    control and wait() for exit code retrieval. wait() is idempotent
    — second call returns cached returncode.

    When started with stdin=PIPE or stdout=PIPE, the corresponding
    stream fields are set to ByteWriteStream / ByteReadStream.
    """

    root: ProcessNode
    stdin: ByteWriteStream | None = field(default=None, init=False)
    stdout: ByteReadStream | None = field(default=None, init=False)
    returncode: int | None = field(default=None, init=False)
    cleaned_up: bool = field(default=False, init=False)

    async def wait(self) -> int:
        """Wait for all processes and return the pipefail exit code.

        Gathers all tasks (process waits + data writes) concurrently,
        then computes pipefail (rightmost non-zero from root_procs).
        Finally: SIGKILL+reap + close all fds (idempotent, shielded).
        Idempotent — second call returns cached returncode.
        """
        if self.returncode is not None:
            return self.returncode
        try:
            await asyncio.gather(*self.root.tasks())
            self.returncode = self._pipefail_code()
            return self.returncode
        finally:
            self.cleaned_up = True
            await _kill_and_reap(*self.root.all_procs())
            for fd_entry in self.root.all_fds():
                fd_entry.close()

    def signal(self, sig: int) -> None:
        """Send a signal to all root processes. Skips dead processes."""
        for proc in self.root.all_procs():
            with contextlib.suppress(ProcessLookupError):
                proc.send_signal(sig)

    def terminate(self) -> None:
        """Send SIGTERM to all root processes."""
        self.signal(signal_mod.SIGTERM)

    def kill(self) -> None:
        """Send SIGKILL to all root processes."""
        self.signal(signal_mod.SIGKILL)

    def _pipefail_code(self) -> int:
        """Compute pipefail exit code: rightmost non-zero from root returncodes."""
        code = 0
        for returncode in self.root.root_returncodes():
            if returncode != 0:
                code = returncode
        return code


@dataclass
class SpawnCtx:
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
        self.fds.append(read_entry)
        write_entry = OwnedFd(write_fd)
        self.fds.append(write_entry)
        return read_entry, write_entry

    def dup(self, fd: int) -> OwnedFd:
        duped = OwnedFd(os.dup(fd))
        self.fds.append(duped)
        return duped


def _spawn(ctx: SpawnCtx, cmd: Runnable, std_fds: StdFds) -> Awaitable[ProcessNode]:
    """Dispatch a Runnable to _spawn_cmd or _spawn_pipeline.

    std_fds carries the outer pipe fds from a parent pipeline.
    Returns a coroutine that spawns the process tree.
    Plain function — avoids an extra coroutine frame per dispatch.
    """
    match cmd:
        case Cmd():
            return _spawn_cmd(ctx, cmd, std_fds)
        case Fn():
            return _spawn_fn(ctx, cmd, std_fds)
        case Pipeline():
            return _spawn_pipeline(ctx, cmd, std_fds)


async def _spawn_fn(
    ctx: SpawnCtx,
    fn_ir: Fn,
    std_fds: StdFds,
) -> FnNode:
    """Create an FnNode for an in-process Python function.

    Dups the received fds so the pipeline's close-after-spawn logic (which closes the
    originals) doesn't affect the Fn.
    """
    dup_stdin = ctx.dup(std_fds.stdin.fd)
    dup_stdout = ctx.dup(std_fds.stdout.fd)
    return FnNode(
        fds=[dup_stdin, dup_stdout],
        _func=fn_ir.func,
        _stdin_fd=dup_stdin,
        _stdout_fd=dup_stdout,
    )


class SpawnCmdCtx:
    """Accumulates fds, sub-process spawns, and fd ops while resolving a single Cmd.

    Resolves redirects and args into FdOps entries and deferred spawn coroutines,
    then builds the preexec_fn and pass_fds for the main process spawn.

    All fds are closed in the parent immediately after spawn — children inherit via
    fork, so parent copies must close for EOF propagation. CmdNode receives the (already
    closed) fds for idempotent cleanup in Execution.wait(). Child spawns must dup fds to
    survive this close; subprocess handles this automatically, but in-process Fn stages
    must dup manually (see _spawn_fn).
    """

    ctx: SpawnCtx
    fdo: FdOps
    fds: list[OwnedFd]
    pending: list[Awaitable[ProcessNode]]
    subs: list[ProcessNode]

    def __init__(self, ctx: SpawnCtx, fdo: FdOps) -> None:
        self.ctx = ctx
        self.fdo = fdo
        self.fds = []
        self.pending = []
        self.subs = []

    def spawn_with_pipe(
        self, inner: Runnable, *, to_stdin: bool
    ) -> tuple[OwnedFd, OwnedFd]:
        """Allocate pipe, track fds, register in fdo, schedule sub spawn.

        to_stdin=True: pipe connects to sub's stdin; parent keeps write end.
        to_stdin=False: pipe connects to sub's stdout; parent keeps read end.
        The other side dups from parent STDIN/STDOUT for inherit behavior.
        """
        pipe_r, pipe_w = self.ctx.pipe()
        self.fds.extend([pipe_r, pipe_w])
        if to_stdin:
            self.fdo.add_live(pipe_w.fd)
            inherit_stdout = self.ctx.dup(STDOUT)
            self.fds.append(inherit_stdout)
            self.pending.append(
                _spawn(self.ctx, inner, StdFds(stdin=pipe_r, stdout=inherit_stdout))
            )
        else:
            self.fdo.add_live(pipe_r.fd)
            inherit_stdin = self.ctx.dup(STDIN)
            self.fds.append(inherit_stdin)
            self.pending.append(
                _spawn(self.ctx, inner, StdFds(stdin=inherit_stdin, stdout=pipe_w))
            )
        return pipe_r, pipe_w

    def feed_with_pipe(self, data: str | bytes) -> OwnedFd:
        """Allocate pipe, schedule FnNode data write, return read end."""
        pipe_r, pipe_w = self.ctx.pipe()
        # Both ends closed after spawn: _spawn_fn dups pipe_w,
        # so the FnNode's write end survives parent cleanup.
        self.fds.extend([pipe_r, pipe_w])
        self.fdo.add_live(pipe_r.fd)

        write_data: Callable[[ByteStageCtx], Awaitable[int]]
        if isinstance(data, bytes):

            async def write_byte_data(stage: ByteStageCtx) -> int:
                with contextlib.suppress(OSError):
                    await stage.stdout.write(data)
                return 0

            write_data = write_byte_data
        else:

            async def write_str_data(stage: TextStageCtx) -> int:
                with contextlib.suppress(OSError):
                    await stage.stdout.write(data)
                return 0

            write_data = make_byte_wrapper(write_str_data, "utf-8")

        inherit_stdin = self.ctx.dup(STDIN)
        self.fds.append(inherit_stdin)
        self.pending.append(
            _spawn_fn(
                self.ctx, Fn(write_data), StdFds(stdin=inherit_stdin, stdout=pipe_w)
            )
        )
        return pipe_r

    def resolve_redirects(self, cmd: Cmd) -> None:
        # Feed IR redirects into FdOps
        for redirect in cmd.redirects:
            match redirect:
                case FdToFd(src=src_fd, dst=dst_fd):  # 2>&1
                    self.fdo.dup2(src_fd, dst_fd)

                case FdToFile(
                    fd=target_fd, path=path, append=do_append
                ):  # > file, >> file, 2> file
                    flags = os.O_WRONLY | os.O_CREAT
                    flags |= os.O_APPEND if do_append else os.O_TRUNC
                    self.fdo.open(target_fd, path, flags)

                case FdToSub(fd=target_fd, sub=sub):  # 1> >(cmd), 3> >(cmd)
                    _, pipe_w = self.spawn_with_pipe(sub.cmd, to_stdin=True)
                    self.fdo.move_fd(pipe_w.fd, target_fd)

                case FdFromFile(fd=target_fd, path=path):  # < file, 3< file
                    self.fdo.open(target_fd, path, os.O_RDONLY)

                case FdFromData(fd=target_fd, data=data):  # <<< "string"
                    pipe_r = self.feed_with_pipe(data)
                    self.fdo.move_fd(pipe_r.fd, target_fd)

                case FdFromSub(fd=target_fd, sub=sub):  # < <(cmd), 3< <(cmd)
                    pipe_r, _ = self.spawn_with_pipe(sub.cmd, to_stdin=False)
                    self.fdo.move_fd(pipe_r.fd, target_fd)

                case FdClose(fd=closed_fd):  # 3>&-
                    self.fdo.close(closed_fd)

    def resolve_args(self, cmd: Cmd) -> list[str]:
        # Resolve Sub arguments to /dev/fd/N paths
        resolved_args: list[str] = []
        for arg in cmd.args:
            match arg:
                case str() as string:
                    resolved_args.append(string)
                case SubOut(cmd=inner):
                    _, pipe_w = self.spawn_with_pipe(inner, to_stdin=True)
                    resolved_args.append(self.fd_path_arg(pipe_w.fd))
                case SubIn(cmd=inner):
                    pipe_r, _ = self.spawn_with_pipe(inner, to_stdin=False)
                    resolved_args.append(self.fd_path_arg(pipe_r.fd))

        return resolved_args

    def fd_path_arg(self, fd: int) -> str:
        return str(FD_DIR / str(fd))

    @staticmethod
    def resolve_env(cmd: Cmd) -> dict[str, str] | None:
        # Build env overlay and resolve working directory
        if not cmd.env_vars and cmd.working_dir is None:
            return None

        proc_env = dict(os.environ)
        for key, value in cmd.env_vars:
            if value is None:
                proc_env.pop(key, None)
            else:
                proc_env[key] = value

        if cmd.working_dir is not None:
            proc_env["PWD"] = str(cmd.working_dir)

        return proc_env

    def build_preexec(self) -> Callable[[], None] | None:
        """Build a preexec_fn closure that executes all fd ops in the child.

        All operations (open, dup2, close) run in the child between fork()
        and exec(). Only async-signal-safe syscalls: open, dup2, close.

        Target fds from OpOpen are protected by pass_fds, so close_fds
        (which runs after preexec_fn) won't close them. The intermediate
        fd from os.open() gets dup2'd to the target then closed — close_fds
        cleans it up if we don't.
        """

        # Capture ops in closure to avoid preexec allocation
        ops = self.fdo.ops
        if not ops:
            return None

        def _preexec() -> None:
            for op in ops:
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

    def build_pass_fds(self, ignore: frozenset[int]) -> tuple[int, ...]:
        return tuple(fd for fd in self.fdo.keep_fds() if fd not in ignore)


async def _spawn_cmd(
    ctx: SpawnCtx,
    cmd: Cmd,
    std_fds: StdFds,
) -> CmdNode:
    """Spawn a single Cmd with all its redirects and sub-processes.

    Redirect resolution follows a two-layer model matching POSIX:

    Layer 1 — pipe wiring (Popen kwargs): std_fds from the parent pipeline
    become stdin=/stdout= on the subprocess call. Popen internally does dup2(pipe_rd, 0)
    / dup2(pipe_wr, 1).

    Layer 2 — user redirects (preexec_fn via FdOps): ordered fd ops that execute in the
    child after Popen's pipe dup2, so user redirects (>, <, 2>&1, etc.) naturally
    override pipe wiring.

    Process substitutions (FdToSub, FdFromSub) and Sub arguments (SubOut, SubIn) each
    allocate a pipe and schedule a spawn coroutine via SpawnCmdCtx. All sub spawns run
    concurrently with the main process spawn via asyncio.gather — safe because pipe fds
    are allocated eagerly (before any spawn).

    After all processes have been spawned, pipe fds used only for child inheritance are
    closed in the parent so EOF propagates.
    """
    # Build FdOps simulation with initial live fds from pipeline
    spawn_ctx = SpawnCmdCtx(ctx, FdOps(live=SUBPROCESS_DEFAULT_FDS))

    # Redirects before args: mirrors bash where redirects and <() are independent.
    # Resolving redirects first means they can't see arg-position sub fds.
    spawn_ctx.resolve_redirects(cmd)
    resolved_args = spawn_ctx.resolve_args(cmd)
    proc_env = spawn_ctx.resolve_env(cmd)

    # Spawn main process and resolve/spawn sub-processes concurrently
    proc, spawned = await asyncio.gather(
        ctx.exec_(
            *resolved_args,
            stdin=std_fds.stdin.fd,
            stdout=std_fds.stdout.fd,
            # Exclude 0/1/2 — subprocess handles those via stdin=/stdout=
            pass_fds=spawn_ctx.build_pass_fds(ignore=SUBPROCESS_DEFAULT_FDS),
            preexec_fn=spawn_ctx.build_preexec(),
            cwd=cmd.working_dir,
            env=proc_env,
        ),
        asyncio.gather(*spawn_ctx.pending),
    )
    spawn_ctx.subs.extend(spawned)

    # Close all fds in parent so EOF propagates
    for fd_entry in spawn_ctx.fds:
        fd_entry.close()

    return CmdNode(proc=proc, fds=spawn_ctx.fds, subs=spawn_ctx.subs)


async def _spawn_pipeline(
    ctx: SpawnCtx,
    pipeline: Pipeline,
    std_fds: StdFds,
) -> PipelineNode:
    """Spawn all stages of a pipeline, connected by inter-stage pipes.

    Pipes are allocated eagerly before any spawns, then all stages
    are spawned concurrently via asyncio.gather. This is safe
    because each stage only needs its stdin/stdout fds to be valid
    at spawn time, and pipe allocation is synchronous.

    Outer std_fds are wired to the first/last stage respectively,
    with inter-stage pipes connecting the rest. After all children
    have forked, inter-stage pipe fds are closed in the parent so
    EOF propagates when stages exit.
    """
    stages = pipeline.stages
    if not stages:
        return PipelineNode(stages=[])

    # Allocate inter-stage pipes
    inter_pipes = [ctx.pipe() for _ in range(len(stages) - 1)]

    # Per-stage StdFds: outer fds at the edges, pipes in between
    stage_stdins = [std_fds.stdin] + [pipe_r for pipe_r, _ in inter_pipes]
    stage_stdouts = [pipe_w for _, pipe_w in inter_pipes] + [std_fds.stdout]
    stage_fds = [
        StdFds(stdin=sin, stdout=sout)
        for sin, sout in zip(stage_stdins, stage_stdouts, strict=True)
    ]

    # Spawn all stages concurrently — both Cmd and Fn are async
    spawn_tasks: list[Awaitable[CmdNode | FnNode]] = []
    for stage, fds in zip(stages, stage_fds, strict=True):
        if isinstance(stage, Fn):
            spawn_tasks.append(_spawn_fn(ctx, stage, fds))
        else:
            spawn_tasks.append(_spawn_cmd(ctx, stage, fds))
    stage_nodes = list(await asyncio.gather(*spawn_tasks))

    # Close inter-stage pipe fds (children have inherited them)
    for pipe_r, pipe_w in inter_pipes:
        pipe_r.close()
        pipe_w.close()

    return PipelineNode(stages=stage_nodes)


async def _spawn_tree(
    cmd: Runnable,
    stdin_fd: int | None = None,
    stdout_fd: int | None = None,
) -> Execution:
    """Spawn a process tree and return an Execution handle.

    Dups stdin_fd/stdout_fd (or parent STDIN/STDOUT when None) into
    owned fds, spawns all processes, closes the dups, and returns an
    Execution whose wait() gathers exits and computes pipefail.
    On spawn failure, cleans up before re-raising.

    Callers retain ownership of the raw fds they pass and must close
    them after _spawn_tree() returns (_spawn_tree only closes its dups).

    Args:
        cmd: Command or pipeline to spawn.
        stdin_fd: Fd to dup for child stdin (None = inherit STDIN).
        stdout_fd: Fd to dup for child stdout (None = inherit STDOUT).
    """
    ctx = SpawnCtx()
    std_fds = StdFds(
        stdin=ctx.dup(stdin_fd if stdin_fd is not None else STDIN),
        stdout=ctx.dup(stdout_fd if stdout_fd is not None else STDOUT),
    )

    try:
        root = await _spawn(ctx, cmd, std_fds)
    except BaseException:
        await _kill_and_reap(*ctx.procs)
        for fd_entry in ctx.fds:
            fd_entry.close()
        raise

    # Children inherited via fork; close our dups so EOF propagates.
    # (FnNode dups from _spawn_fn are separate — closed by Execution.wait().)
    std_fds.stdin.close()
    std_fds.stdout.close()

    return Execution(root=root)


@asynccontextmanager
async def start(
    cmd: Runnable,
    stdin: int | None = None,
    stdout: int | None = None,
) -> AsyncIterator[Execution]:
    """Spawn a process tree and yield an Execution handle.

    On context exit without wait(), SIGKILL all processes and reap.

    Args:
        cmd: Command or pipeline to spawn.
        stdin: Fd for child stdin, or PIPE for auto-pipe, or None to inherit.
        stdout: Fd for child stdout, or PIPE for auto-pipe, or None to inherit.
    """
    stdin_pipe_r: OwnedFd | None = None
    stdin_pipe_w: OwnedFd | None = None
    stdout_pipe_r: OwnedFd | None = None
    stdout_pipe_w: OwnedFd | None = None
    spawn_stdin_fd: int | None = None
    spawn_stdout_fd: int | None = None

    try:
        if stdin == PIPE:
            raw_r, raw_w = os.pipe()
            stdin_pipe_r = OwnedFd(raw_r)
            stdin_pipe_w = OwnedFd(raw_w)
            spawn_stdin_fd = stdin_pipe_r.fd
        elif stdin is not None:
            spawn_stdin_fd = stdin

        if stdout == PIPE:
            raw_r, raw_w = os.pipe()
            stdout_pipe_r = OwnedFd(raw_r)
            stdout_pipe_w = OwnedFd(raw_w)
            spawn_stdout_fd = stdout_pipe_w.fd
        elif stdout is not None:
            spawn_stdout_fd = stdout

        execution = await _spawn_tree(
            cmd, stdin_fd=spawn_stdin_fd, stdout_fd=spawn_stdout_fd
        )
    except BaseException:
        if stdin_pipe_r is not None:
            stdin_pipe_r.close()
        if stdin_pipe_w is not None:
            stdin_pipe_w.close()
        if stdout_pipe_r is not None:
            stdout_pipe_r.close()
        if stdout_pipe_w is not None:
            stdout_pipe_w.close()
        raise

    # _spawn_tree dup'd these fds; close our copies so EOF propagates
    if stdin_pipe_r is not None:
        stdin_pipe_r.close()
    if stdout_pipe_w is not None:
        stdout_pipe_w.close()

    # Wrap remaining pipe ends as streams on Execution
    if stdin_pipe_w is not None:
        execution.stdin = ByteWriteStream(stdin_pipe_w)
    if stdout_pipe_r is not None:
        execution.stdout = ByteReadStream(stdout_pipe_r)

    try:
        yield execution
    finally:
        # Close stdin first (sends EOF to child), then stdout
        if execution.stdin is not None:
            await execution.stdin.close()
        if execution.stdout is not None:
            await execution.stdout.close()
        if execution.returncode is None and not execution.cleaned_up:
            execution.kill()
            await execution.wait()


async def run(cmd: Runnable) -> int:
    """Execute a command or pipeline and return exit code."""
    async with start(cmd) as execution:
        return await execution.wait()


@overload
async def out(cmd: Runnable, encoding: None) -> bytes: ...
@overload
async def out(cmd: Runnable, encoding: str = "utf-8") -> str: ...


async def out(cmd: Runnable, encoding: str | None = "utf-8") -> str | bytes:
    """Execute a command and return its captured stdout.

    Spawns with stdout=PIPE, then reads stdout concurrently with
    wait(). Concurrency is required to avoid deadlock: if the child
    fills the pipe buffer, it blocks until someone reads, but if we
    wait() first we never read.

    Args:
        cmd: Command to execute.
        encoding: Decode stdout with this encoding. None for raw bytes.

    Raises:
        subprocess.CalledProcessError: On non-zero exit code, with
            the captured stdout attached for diagnostic use.
    """
    async with start(cmd, stdout=PIPE) as execution:
        assert execution.stdout is not None
        code, captured = await asyncio.gather(
            execution.wait(),
            execution.stdout.read(),
        )
    if code != 0:
        raise subprocess.CalledProcessError(code, [], captured)
    if encoding is None:
        return captured
    return captured.decode(encoding)
