"""Spawn orchestration: fd/proc tracking and process tree construction.

SpawnScope tracks all allocated resources (fds, procs, tasks) during
spawn for error cleanup, and dispatches IR nodes to the appropriate
spawn method (cmd, fn, pipeline).

Fd ownership invariant — borrow, dup-before-use:
- StdFds are borrowed, not owned. Callers pass fds without duping.
- Use-sites dup only if they need their own copy: spawn_fn dups
  for in-process FnNode execution; fork is an implicit dup for exec_.
- Creators close what they create: __aenter__ closes PIPE fds (non-
  owning inherit/raw-fd Fds are no-op closes), SpawnCmdScope closes
  pipe/redirect fds, spawn_pipeline closes inter-stage pipe fds.
- All created fds are tracked by SpawnScope for error cleanup.
"""

from __future__ import annotations

import asyncio
import os
import sys
import traceback
from asyncio.subprocess import Process, create_subprocess_exec
from collections.abc import Awaitable, Callable
from pathlib import Path

from shish.builders import (
    Cmd,
    Fn,
    Pipeline,
    Runnable,
)
from shish.fd import Fd
from shish.fn_stage import ByteStage
from shish.runtime.spawn_cmd import SpawnCmdScope
from shish.runtime.tree import (
    CmdNode,
    FnNode,
    PipelineNode,
    ProcessNode,
    StdFds,
)
from shish.streams import (
    ByteReadStream,
    ByteWriteStream,
)


class SpawnScope:
    """Tracks fds, procs, and fn_tasks during spawn for error cleanup.

    Spawn can fail partway — some processes already forked, some fds
    already allocated — before a full process tree exists. SpawnScope
    records everything allocated so that cleanup() can tear it all
    down. Also provides low-level primitives (exec_, pipe, dup) and
    spawn methods that dispatch IR nodes to the appropriate builder.
    """

    fds: list[Fd]
    procs: list[Process]
    fn_tasks: list[asyncio.Task[int]]

    def __init__(self) -> None:
        self.fds = []
        self.procs = []
        self.fn_tasks = []

    async def cleanup(self) -> None:
        """Tear down everything allocated so far: kill procs, cancel
        fn_tasks, close fds. Called when spawn fails before a full
        process tree is built — the tree's own cleanup methods can't
        be used yet. Shielded from cancellation.
        """
        reap: list[Awaitable[int]] = []
        for proc in self.procs:
            if proc.returncode is None:
                proc.kill()
                reap.append(proc.wait())
        for task in self.fn_tasks:
            task.cancel()
        pending: list[Awaitable[object]] = list(reap)
        pending.extend(self.fn_tasks)
        if pending:
            await asyncio.shield(asyncio.gather(*pending, return_exceptions=True))
        for fd_entry in self.fds:
            fd_entry.close()

    async def exec_(
        self,
        *args: str,
        stdin: int | None = None,
        stdout: int | None = None,
        stderr: int | None = None,
        pass_fds: tuple[int, ...] = (),
        preexec_fn: Callable[[], None] | None = None,
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
    ) -> Process:
        """Spawn a subprocess via create_subprocess_exec and register it.

        stdin/stdout/stderr are raw fds (or None for inherit); Popen does
        dup2 to wire them to fds 0/1/2 in the child. pass_fds keeps
        additional fds open across exec (fds > 2 used by redirects
        and process substitutions). preexec_fn runs between fork and
        exec to apply user redirect ops (open, dup2, close).
        """
        proc = await create_subprocess_exec(
            *args,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            pass_fds=pass_fds,
            preexec_fn=preexec_fn,
            cwd=cwd,
            env=env,
        )
        self.procs.append(proc)
        return proc

    def pipe(self) -> tuple[Fd, Fd]:
        """Allocate an os.pipe(), tracking both ends for cleanup.

        Returns (read_end, write_end). Both are registered so the
        finally block can close them even if spawn fails before the
        caller gets to close them after fork inheritance.
        """
        read_fd, write_fd = os.pipe()
        read_entry = Fd(read_fd)
        self.fds.append(read_entry)
        write_entry = Fd(write_fd)
        self.fds.append(write_entry)
        return read_entry, write_entry

    def dup(self, fd: int) -> Fd:
        duped = Fd(os.dup(fd))
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
        return await SpawnCmdScope(self, cmd, std_fds).spawn()

    async def spawn_fn(
        self,
        fn_ir: Fn,
        std_fds: StdFds,
    ) -> FnNode:
        """Create an FnNode for an in-process Python function.

        Dups stdin/stdout/stderr from std_fds — the only spawn path
        that needs explicit dups (dup-before-use). FnNode runs in-
        process, so it needs owned copies that survive the caller's
        close-after-spawn. The task is started eagerly so the function
        begins executing immediately, matching the behavior of spawn_cmd.
        """
        dup_stdin = self.dup(std_fds.stdin.fd)
        dup_stdout = self.dup(std_fds.stdout.fd)
        dup_stderr = self.dup(std_fds.stderr.fd)
        func = fn_ir.func

        async def execute() -> int:
            stdin_stream = ByteReadStream(dup_stdin)
            stdout_stream = ByteWriteStream(dup_stdout)
            stderr_stream = ByteWriteStream(dup_stderr)
            try:
                ctx = ByteStage(
                    stdin=stdin_stream, stdout=stdout_stream, stderr=stderr_stream
                )
                return await func(ctx)
            except Exception:
                traceback.print_exc(file=sys.stderr)
                return 1
            finally:
                await stdout_stream.close()
                await stdin_stream.close()
                await stderr_stream.close()

        task = asyncio.create_task(execute())
        self.fn_tasks.append(task)
        return FnNode(
            task=task,
            _stdin_fd=dup_stdin,
            _stdout_fd=dup_stdout,
            _stderr_fd=dup_stderr,
        )

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

        # Per-stage StdFds: outer fds at the edges, pipes in between.
        # All stages share the same stderr fd.
        stage_stdins = [std_fds.stdin] + [pipe_r for pipe_r, _ in inter_pipes]
        stage_stdouts = [pipe_w for _, pipe_w in inter_pipes] + [std_fds.stdout]
        stage_fds = [
            StdFds(stdin=sin, stdout=sout, stderr=std_fds.stderr)
            for sin, sout in zip(stage_stdins, stage_stdouts, strict=True)
        ]

        # Spawn all stages concurrently — both Cmd and Fn are async
        spawn_tasks: list[Awaitable[CmdNode | FnNode]] = []
        for stage, fds in zip(stages, stage_fds, strict=True):
            if isinstance(stage, Fn):
                spawn_tasks.append(self.spawn_fn(stage, fds))
            else:
                spawn_tasks.append(self.spawn_cmd(stage, fds))
        stage_nodes = list(await asyncio.gather(*spawn_tasks))

        # Close inter-stage pipe fds (children have inherited them)
        for pipe_r, pipe_w in inter_pipes:
            pipe_r.close()
            pipe_w.close()

        return PipelineNode(stages=stage_nodes)
