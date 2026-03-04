"""Spawn machinery for building process trees from IR.

Contains SpawnCtx (fd/proc tracking and process tree spawning)
and SpawnCmdCtx (per-command redirect resolution and spawn).

Fd ownership invariant:
- All opened fds are closed after spawn by the opener
  (except FnNode dups, which stay open for in-process execution).
- All fds are tracked by SpawnCtx, allowing cleanup on errors.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import sys
import traceback
from asyncio.subprocess import Process, create_subprocess_exec
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path

from shish.aio import (
    ByteReadStream,
    ByteStageCtx,
    ByteWriteStream,
    OwnedFd,
    TextStageCtx,
    make_byte_wrapper,
)
from shish.fdops import (
    STDIN,
    STDOUT,
    FdOps,
    OpClose,
    OpDup2,
    OpOpen,
)
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
from shish.runtime.tree import (
    CmdNode,
    FnNode,
    PipelineNode,
    ProcessNode,
    StdFds,
)

SUBPROCESS_DEFAULT_FDS = frozenset({STDIN, STDOUT, 2})
FD_DIR = Path("/dev/fd")


@dataclass
class SpawnCtx:
    """Tracks fds and procs during spawn, and builds the process tree.

    Provides low-level primitives (exec_, pipe, dup) for fd/proc tracking,
    plus spawn methods that dispatch IR nodes to the appropriate builder.
    """

    fds: list[OwnedFd] = field(default_factory=lambda: list[OwnedFd]())
    procs: list[Process] = field(default_factory=lambda: list[Process]())
    fn_tasks: list[asyncio.Task[int]] = field(
        default_factory=lambda: list[asyncio.Task[int]]()
    )

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

    def spawn(self, cmd: Runnable, std_fds: StdFds) -> Awaitable[ProcessNode]:
        """Dispatch a Runnable to the appropriate spawn method.

        std_fds carries the outer pipe fds from a parent pipeline.
        Returns a coroutine that spawns the process tree.
        Plain method — avoids an extra coroutine frame per dispatch.
        """
        match cmd:
            case Cmd():
                return self.spawn_cmd(cmd, std_fds)
            case Fn():
                return self.spawn_fn(cmd, std_fds)
            case Pipeline():
                return self.spawn_pipeline(cmd, std_fds)

    async def spawn_cmd(
        self,
        cmd: Cmd,
        std_fds: StdFds,
    ) -> CmdNode:
        """Spawn a single Cmd with redirects and sub-processes."""
        return await SpawnCmdCtx(self, cmd, std_fds).spawn()

    async def spawn_fn(
        self,
        fn_ir: Fn,
        std_fds: StdFds,
    ) -> FnNode:
        """Create an FnNode for an in-process Python function.

        Dups the received fds so the pipeline's close-after-spawn
        logic (which closes the originals) doesn't affect the Fn.
        The task is started eagerly so the function begins executing
        immediately, matching the behavior of spawn_cmd.
        """
        dup_stdin = self.dup(std_fds.stdin.fd)
        dup_stdout = self.dup(std_fds.stdout.fd)
        func = fn_ir.func

        async def execute() -> int:
            stdin_stream = ByteReadStream(dup_stdin)
            stdout_stream = ByteWriteStream(dup_stdout)
            try:
                ctx = ByteStageCtx(stdin=stdin_stream, stdout=stdout_stream)
                return await func(ctx)
            except Exception:
                traceback.print_exc(file=sys.stderr)
                return 1
            finally:
                await stdout_stream.close()
                await stdin_stream.close()

        task = asyncio.create_task(execute())
        self.fn_tasks.append(task)
        return FnNode(_task=task, _stdin_fd=dup_stdin, _stdout_fd=dup_stdout)

    async def spawn_pipeline(
        self,
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
        inter_pipes = [self.pipe() for _ in range(len(stages) - 1)]

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
                spawn_tasks.append(self.spawn_fn(stage, fds))
            else:
                spawn_tasks.append(SpawnCmdCtx(self, stage, fds).spawn())
        stage_nodes = list(await asyncio.gather(*spawn_tasks))

        # Close inter-stage pipe fds (children have inherited them)
        for pipe_r, pipe_w in inter_pipes:
            pipe_r.close()
            pipe_w.close()

        return PipelineNode(stages=stage_nodes)


class SpawnCmdCtx:
    """Resolves redirects/args and spawns a single Cmd.

    Accumulates fds, sub-process spawns, and fd ops while resolving a single Cmd,
    then builds the preexec_fn and pass_fds for the main process spawn.

    All fds are closed in the parent immediately after spawn — children
    inherit via fork, so parent copies must close for EOF propagation.
    CmdNode receives the (already closed) fds for idempotent cleanup in
    StartCtx.__aexit__. Child spawns must dup fds to survive this close;
    subprocess handles this automatically, but in-process Fn stages must
    dup manually (see SpawnCtx.spawn_fn).
    """

    ctx: SpawnCtx
    cmd: Cmd
    std_fds: StdFds
    fdo: FdOps
    fds: list[OwnedFd]
    pending: list[Awaitable[ProcessNode]]
    subs: list[ProcessNode]

    def __init__(self, ctx: SpawnCtx, cmd: Cmd, std_fds: StdFds) -> None:
        self.ctx = ctx
        self.cmd = cmd
        self.std_fds = std_fds
        self.fdo = FdOps(live=SUBPROCESS_DEFAULT_FDS)
        self.fds = []
        self.pending = []
        self.subs = []

    async def spawn(self) -> CmdNode:
        """Spawn a single Cmd with all its redirects and sub-processes.

        Redirect resolution follows a two-layer model matching POSIX:

        Layer 1 — pipe wiring (Popen kwargs): std_fds from the parent
        pipeline become stdin=/stdout= on the subprocess call. Popen
        internally does dup2(pipe_rd, 0) / dup2(pipe_wr, 1).

        Layer 2 — user redirects (preexec_fn via FdOps): ordered fd
        ops that execute in the child after Popen's pipe dup2, so
        user redirects (>, <, 2>&1, etc.) naturally override pipe
        wiring.

        Process substitutions (FdToSub, FdFromSub) and Sub arguments
        (SubOut, SubIn) each allocate a pipe and schedule a spawn
        coroutine. All sub spawns run concurrently with the main
        process spawn via asyncio.gather — safe because pipe fds are
        allocated eagerly (before any spawn).

        After all processes have been spawned, pipe fds used only for
        child inheritance are closed in the parent so EOF propagates.
        """
        # Redirects before args: mirrors bash where redirects and <() are independent.
        # Resolving redirects first means they can't see arg-position sub fds.
        self._resolve_redirects()
        resolved_args = self._resolve_args()
        proc_env = self._resolve_env()

        # Spawn main process and resolve/spawn sub-processes concurrently
        proc, spawned = await asyncio.gather(
            self.ctx.exec_(
                *resolved_args,
                stdin=self.std_fds.stdin.fd,
                stdout=self.std_fds.stdout.fd,
                # Exclude 0/1/2 — subprocess handles those via stdin=/stdout=
                pass_fds=self._build_pass_fds(ignore=SUBPROCESS_DEFAULT_FDS),
                preexec_fn=self._build_preexec(),
                cwd=self.cmd.working_dir,
                env=proc_env,
            ),
            asyncio.gather(*self.pending),
        )
        self.subs.extend(spawned)

        # Close all fds in parent so EOF propagates
        for fd_entry in self.fds:
            fd_entry.close()

        return CmdNode(proc=proc, fds=self.fds, subs=self.subs)

    def _spawn_with_pipe(
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
                self.ctx.spawn(inner, StdFds(stdin=pipe_r, stdout=inherit_stdout))
            )
        else:
            self.fdo.add_live(pipe_r.fd)
            inherit_stdin = self.ctx.dup(STDIN)
            self.fds.append(inherit_stdin)
            self.pending.append(
                self.ctx.spawn(inner, StdFds(stdin=inherit_stdin, stdout=pipe_w))
            )
        return pipe_r, pipe_w

    def _feed_with_pipe(self, data: str | bytes) -> OwnedFd:
        """Allocate pipe, schedule FnNode data write, return read end."""
        pipe_r, pipe_w = self.ctx.pipe()
        # Both ends closed after spawn: SpawnCtx.spawn_fn dups pipe_w,
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
            self.ctx.spawn_fn(
                Fn(write_data), StdFds(stdin=inherit_stdin, stdout=pipe_w)
            )
        )
        return pipe_r

    def _resolve_redirects(self) -> None:
        # Feed IR redirects into FdOps
        for redirect in self.cmd.redirects:
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
                    _, pipe_w = self._spawn_with_pipe(sub.cmd, to_stdin=True)
                    self.fdo.move_fd(pipe_w.fd, target_fd)

                case FdFromFile(fd=target_fd, path=path):  # < file, 3< file
                    self.fdo.open(target_fd, path, os.O_RDONLY)

                case FdFromData(fd=target_fd, data=data):  # <<< "string"
                    pipe_r = self._feed_with_pipe(data)
                    self.fdo.move_fd(pipe_r.fd, target_fd)

                case FdFromSub(fd=target_fd, sub=sub):  # < <(cmd), 3< <(cmd)
                    pipe_r, _ = self._spawn_with_pipe(sub.cmd, to_stdin=False)
                    self.fdo.move_fd(pipe_r.fd, target_fd)

                case FdClose(fd=closed_fd):  # 3>&-
                    self.fdo.close(closed_fd)

    def _resolve_args(self) -> list[str]:
        # Resolve Sub arguments to /dev/fd/N paths
        resolved_args: list[str] = []
        for arg in self.cmd.args:
            match arg:
                case str() as string:
                    resolved_args.append(string)
                case SubOut(cmd=inner):
                    _, pipe_w = self._spawn_with_pipe(inner, to_stdin=True)
                    resolved_args.append(self._fd_path_arg(pipe_w.fd))
                case SubIn(cmd=inner):
                    pipe_r, _ = self._spawn_with_pipe(inner, to_stdin=False)
                    resolved_args.append(self._fd_path_arg(pipe_r.fd))

        return resolved_args

    def _resolve_env(self) -> dict[str, str] | None:
        # Build env overlay and resolve working directory
        cmd = self.cmd
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

    def _build_preexec(self) -> Callable[[], None] | None:
        """Build a preexec_fn closure that executes all fd ops in the child.

        All operations (open, dup2, close) run in the child between fork()
        and exec(). Only async-signal-safe syscalls: open, dup2, close.

        Target fds from OpOpen are protected by pass_fds, so the
        default fd cleanup (which closes fds >2 after preexec_fn)
        won't close them. The intermediate fd from os.open() gets
        dup2'd to the target then closed.
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

    def _build_pass_fds(self, ignore: frozenset[int]) -> tuple[int, ...]:
        return tuple(fd for fd in self.fdo.keep_fds() if fd not in ignore)

    @staticmethod
    def _fd_path_arg(fd: int) -> str:
        return str(FD_DIR / str(fd))
