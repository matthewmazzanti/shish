"""Process tree nodes and helpers for runtime execution.

Defines the process tree structure (CmdNode, PipelineNode, FnNode)
and utility functions for pipefail computation, returncode normalization,
and cleanup (kill+reap).
"""

from __future__ import annotations

import asyncio
import signal as signal_mod
import sys
import traceback
from asyncio.subprocess import Process
from collections.abc import Awaitable, Callable, Iterator
from dataclasses import dataclass, field

from shish.aio import (
    ByteReadStream,
    ByteStageCtx,
    ByteWriteStream,
    OwnedFd,
)


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


def pipefail_code(node: ProcessNode) -> int:
    """Compute pipefail exit code: rightmost non-zero from root returncodes."""
    code = 0
    for returncode in node.root_returncodes():
        if returncode != 0:
            code = returncode
    return code


def _normalize_returncode(code: int | None) -> int:
    """Convert returncode to bash-style: 128 + signal for killed processes."""
    if code is None:
        raise RuntimeError("Process returncode is None — process never reaped")
    if code < 0:
        return 128 + (-code)
    return code


async def kill_and_reap(*procs: Process) -> None:
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


def mark_killed_fns(node: ProcessNode) -> None:
    """Assign SIGKILL exit code to FnNodes that never completed.

    After kill_and_reap, OS processes have returncodes but FnNode
    tasks may not (e.g., never started, or cancelled mid-flight).
    This fills in a kill-equivalent code so pipefail_code can
    iterate root_returncodes without hitting None.
    """
    killed = 128 + signal_mod.SIGKILL
    match node:
        case FnNode() if node.returncode is None:
            node.returncode = killed
        case PipelineNode(stages=stages):
            for stage in stages:
                mark_killed_fns(stage)
        case CmdNode(subs=subs):
            for sub in subs:
                mark_killed_fns(sub)
        case _:
            pass
