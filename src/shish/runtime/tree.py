"""Process tree — the witness of a successful spawn.

A ProcessNode tree is only constructed once all processes have been
forked and all fds allocated. It tracks every open resource (procs,
tasks, fds) and owns their lifecycle: terminate, kill, close_fds.
SpawnCtx handles the error path when spawn fails partway.
"""

from __future__ import annotations

import asyncio
import contextlib
import signal as signal_mod
from asyncio.subprocess import Process
from collections.abc import Awaitable
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

    Holds the main process and any substitution sub-processes (from
    FdToSub, FdFromSub, SubOut, SubIn redirects/args). Does not own
    fds — spawn closes them in the parent after fork; children inherit
    copies via pass_fds. Sub-processes are excluded from pipefail —
    only the main proc participates in exit code reporting.
    """

    proc: Process
    subs: list[ProcessNode] = field(default_factory=lambda: list[ProcessNode]())

    def returncode(self) -> int | None:
        """Normalized returncode: 128 + signal for killed processes, None if running."""
        if self.proc.returncode is None:
            return None
        return _normalize_returncode(self.proc.returncode)

    def terminate(self) -> None:
        """SIGTERM main proc, recurse subs. Skips dead processes."""
        with contextlib.suppress(ProcessLookupError):
            self.proc.terminate()
        for sub in self.subs:
            sub.terminate()

    def kill(self) -> None:
        """SIGKILL main proc, recurse subs. Skips already-exited processes."""
        if self.proc.returncode is None:
            self.proc.kill()
        for sub in self.subs:
            sub.kill()

    def close_fds(self) -> None:
        """Close owned fds in sub-processes (recursive)."""
        for sub in self.subs:
            sub.close_fds()

    async def wait(self) -> None:
        """Wait for proc and all subs to exit."""
        pending: list[Awaitable[object]] = [self.proc.wait()]
        for sub in self.subs:
            pending.append(sub.wait())
        await asyncio.gather(*pending)


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

    def terminate(self) -> None:
        """Terminate all stages (recursive)."""
        for stage in self.stages:
            stage.terminate()

    def kill(self) -> None:
        """Kill all stages (recursive)."""
        for stage in self.stages:
            stage.kill()

    def close_fds(self) -> None:
        """Close owned fds across all stages (recursive)."""
        for stage in self.stages:
            stage.close_fds()

    async def wait(self) -> None:
        """Wait for all stages to exit."""
        await asyncio.gather(*[stage.wait() for stage in self.stages])


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
            return _normalize_returncode(-signal_mod.SIGKILL)
        if not self._task.done():
            return None
        return _normalize_returncode(self._task.result())

    def terminate(self) -> None:
        """Cancel the task (graceful stop for in-process functions)."""
        self._task.cancel()

    def kill(self) -> None:
        """Cancel the task (same mechanism as terminate for tasks)."""
        self._task.cancel()

    def close_fds(self) -> None:
        """Close owned stdin/stdout fds."""
        self._stdin_fd.close()
        self._stdout_fd.close()

    async def wait(self) -> None:
        """Wait for the task to complete."""
        await asyncio.gather(self._task, return_exceptions=True)


ProcessNode = CmdNode | PipelineNode | FnNode


def _normalize_returncode(code: int) -> int:
    """Convert returncode to bash-style: 128 + signal for killed processes."""
    if code < 0:
        return 128 + (-code)
    return code
