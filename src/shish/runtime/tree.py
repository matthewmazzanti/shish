"""Process tree nodes and helpers for runtime execution.

Defines the process tree structure (CmdNode, PipelineNode, FnNode)
and utility functions for returncode normalization and cleanup (kill+reap).
"""

from __future__ import annotations

import asyncio
import signal as signal_mod
from asyncio.subprocess import Process
from collections.abc import Awaitable, Iterator
from dataclasses import dataclass, field

from shish.aio import OwnedFd


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

    def returncode(self) -> int | None:
        """Normalized returncode: 128 + signal for killed processes, None if running."""
        if self.proc.returncode is None:
            return None
        return _normalize_returncode(self.proc.returncode)

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

    def all_fn_tasks(self) -> Iterator[asyncio.Task[int]]:
        """Yield FnNode tasks from sub-processes (recursive)."""
        for sub in self.subs:
            yield from sub.all_fn_tasks()

    def tasks(self) -> Iterator[Awaitable[object]]:
        """Yield coroutines to gather: proc wait + sub tasks."""
        yield self.proc.wait()
        for sub in self.subs:
            yield from sub.tasks()


@dataclass
class PipelineNode:
    """Process tree node for a pipeline (cmd1 | cmd2 | ...).

    Each stage is a CmdNode or FnNode. Pipefail semantics:
    returncode() returns the rightmost non-zero stage exit code.
    Sub-processes within each stage are excluded from pipefail.
    """

    stages: list[CmdNode | FnNode]

    def returncode(self) -> int | None:
        """Pipefail exit code: rightmost non-zero, None if running."""
        code = 0
        for stage in self.stages:
            stage_code = stage.returncode()
            if stage_code is None:
                return None
            if stage_code != 0:
                code = stage_code
        return code

    def all_procs(self) -> Iterator[Process]:
        """Yield all processes across all stages (recursive)."""
        for stage in self.stages:
            yield from stage.all_procs()

    def all_fds(self) -> Iterator[OwnedFd]:
        """Yield all owned fds across all stages (recursive)."""
        for stage in self.stages:
            yield from stage.all_fds()

    def all_fn_tasks(self) -> Iterator[asyncio.Task[int]]:
        """Yield FnNode tasks across all stages (recursive)."""
        for stage in self.stages:
            yield from stage.all_fn_tasks()

    def tasks(self) -> Iterator[Awaitable[object]]:
        """Yield coroutines to gather: all stage tasks."""
        for stage in self.stages:
            yield from stage.tasks()


@dataclass
class FnNode:
    """Process tree node for an in-process Python function.

    No OS process is spawned — the function runs as an asyncio task,
    started eagerly by spawn_fn. Owns dup'd copies of stdin/stdout fds
    so pipeline close-after-spawn logic doesn't affect it.
    """

    _task: asyncio.Task[int]
    _stdin_fd: OwnedFd = field(repr=False)
    _stdout_fd: OwnedFd = field(repr=False)

    def returncode(self) -> int | None:
        """Task return code: cancelled→SIGKILL, done→result, None if running."""
        if self._task.cancelled():
            return 128 + signal_mod.SIGKILL
        if not self._task.done():
            return None
        return _normalize_returncode(self._task.result())

    def all_procs(self) -> Iterator[Process]:
        """No OS process to signal."""
        yield from ()

    def all_fds(self) -> Iterator[OwnedFd]:
        """Yield owned fds for cleanup."""
        yield self._stdin_fd
        yield self._stdout_fd

    def all_fn_tasks(self) -> Iterator[asyncio.Task[int]]:
        """Yield this node's task."""
        yield self._task

    def tasks(self) -> Iterator[Awaitable[object]]:
        """Yield the eagerly-started task."""
        yield self._task


ProcessNode = CmdNode | PipelineNode | FnNode


def _normalize_returncode(code: int) -> int:
    """Convert returncode to bash-style: 128 + signal for killed processes."""
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


async def cancel_fn_tasks(node: ProcessNode) -> None:
    """Cancel and await all eagerly-started FnNode tasks in the tree.

    Shielded from cancellation so cleanup completes even if the
    calling task is cancelled — mirrors kill_and_reap for OS processes.
    """
    tasks = list(node.all_fn_tasks())
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.shield(asyncio.gather(*tasks, return_exceptions=True))
