"""Process tree — the witness of a successful spawn.

A ProcessNode tree is only constructed once all processes have been
forked and all fds allocated. It tracks every open resource (procs,
tasks, fds) and owns their lifecycle: wait, terminate, kill, close_fds.
SpawnScope handles the error path when spawn fails partway.
"""

from __future__ import annotations

import abc
import asyncio
import contextlib
import dataclasses as dc
import signal as signal_mod
from asyncio.subprocess import Process
from collections.abc import Awaitable, Iterator

from shish.fd import Fd


@dc.dataclass
class StdFds:
    """Stdin/stdout/stderr fds for a spawn subtree.

    Not owned — the creator is responsible for closing any fds it
    allocated. Use-sites that need their own copy (e.g. spawn_fn
    for in-process execution) dup internally; fork is an implicit
    dup for exec_.
    """

    stdin: Fd
    stdout: Fd
    stderr: Fd


class ProcessNodeBase(abc.ABC):
    """Base class for process tree nodes.

    Provides wait() from awaitables() + returncode().
    Subclasses implement returncode(), awaitables(), terminate(),
    kill(), and close_fds().
    """

    @abc.abstractmethod
    def returncode(self) -> int | None: ...

    @abc.abstractmethod
    def awaitables(self) -> Iterator[Awaitable[int]]: ...

    @abc.abstractmethod
    def terminate(self) -> None: ...

    @abc.abstractmethod
    def kill(self) -> None: ...

    @abc.abstractmethod
    def close_fds(self) -> None: ...

    async def wait(self) -> int:
        """Wait for all awaitables, return exit code."""
        await asyncio.gather(*self.awaitables(), return_exceptions=True)
        code = self.returncode()
        assert code is not None
        return code


@dc.dataclass
class CmdNode(ProcessNodeBase):
    """Process tree node for a single spawned command.

    Holds the main process and any substitution sub-processes (from
    FdToSub, FdFromSub, SubOut, SubIn redirects/args). Does not own
    fds — spawn closes them in the parent after fork; children inherit
    copies via pass_fds. Sub-processes are excluded from pipefail —
    only the main proc participates in exit code reporting.
    """

    proc: Process
    subs: list[ProcessNode] = dc.field(default_factory=lambda: list[ProcessNode]())

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

    def awaitables(self) -> Iterator[Awaitable[int]]:
        """Yield awaitables for this proc and subs."""
        yield self.proc.wait()
        for sub in self.subs:
            yield from sub.awaitables()


@dc.dataclass
class PipelineNode(ProcessNodeBase):
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

    def awaitables(self) -> Iterator[Awaitable[int]]:
        """Yield awaitables across all stages."""
        for stage in self.stages:
            yield from stage.awaitables()


@dc.dataclass
class FnNode(ProcessNodeBase):
    """Process tree node for an in-process Python function.

    No OS process is spawned — the function runs as an asyncio task,
    started eagerly by spawn_fn. Owns dup'd copies of stdin/stdout/stderr
    fds so pipeline close-after-spawn logic doesn't affect it.
    """

    task: asyncio.Task[int]
    _stdin_fd: Fd = dc.field(repr=False)
    _stdout_fd: Fd = dc.field(repr=False)
    _stderr_fd: Fd = dc.field(repr=False)

    def returncode(self) -> int | None:
        """Task return code, None if running."""
        if not self.task.done():
            return None
        if self.task.cancelled():
            return _normalize_returncode(-signal_mod.SIGKILL)
        return _normalize_returncode(self.task.result())

    def terminate(self) -> None:
        """Cancel the task (graceful stop for in-process functions)."""
        self.task.cancel()

    def kill(self) -> None:
        """Cancel the task (same mechanism as terminate for tasks)."""
        self.task.cancel()

    def close_fds(self) -> None:
        """Close owned stdin/stdout/stderr fds."""
        self._stdin_fd.close()
        self._stdout_fd.close()
        self._stderr_fd.close()

    def awaitables(self) -> Iterator[Awaitable[int]]:
        """Yield awaitable for this task."""
        yield self.task


ProcessNode = CmdNode | PipelineNode | FnNode


def _normalize_returncode(code: int) -> int:
    """Convert returncode to bash-style: 128 + signal for killed processes."""
    if code < 0:
        return 128 + (-code)
    return code
