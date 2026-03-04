"""Public lifecycle API: Execution, StartCtx, start(), run(), out().

Provides the user-facing entry points for spawning and managing
process trees built from IR commands.
"""

from __future__ import annotations

import asyncio
import signal as signal_mod
import subprocess
from dataclasses import dataclass, field
from typing import Any, cast, overload

from shish.aio import (
    ByteReadStream,
    ByteWriteStream,
    OwnedFd,
    TextReadStream,
    TextWriteStream,
)
from shish.fdops import PIPE, STDIN, STDOUT, Pipe
from shish.ir import Runnable
from shish.runtime.spawn import SpawnCtx
from shish.runtime.tree import (
    ProcessNode,
    StdFds,
)


@dataclass
class Execution[
    StdinT: (ByteWriteStream, TextWriteStream, None) = None,
    StdoutT: (ByteReadStream, TextReadStream, None) = None,
]:
    """Handle for a spawned process tree.

    Created by StartCtx.__aenter__. Provides signal/terminate/kill for
    explicit control and wait() for exit code retrieval. wait() is
    idempotent — second call returns cached returncode.

    When started with stdin=PIPE or stdout=PIPE, the corresponding
    stream fields are set to text or byte streams depending on the
    encoding parameter. Generic over StdinT/StdoutT so that passing
    PIPE statically narrows the stream type to non-None.
    """

    root: ProcessNode
    stdin: StdinT
    stdout: StdoutT
    returncode: int | None = field(default=None, init=False)

    async def wait(self) -> int:
        """Wait for all processes and return the pipefail exit code.

        Gathers all tasks (process waits + data writes) concurrently,
        then delegates to the root node's returncode() method.
        Idempotent — second call returns cached returncode.
        """
        if self.returncode is not None:
            return self.returncode
        await asyncio.gather(*self.root.tasks())
        code = self.root.returncode()
        assert code is not None
        self.returncode = code
        return self.returncode

    def signal(self, sig: int) -> None:
        """Send a signal to all processes in the tree. Skips dead processes."""
        self.root.signal(sig)

    def terminate(self) -> None:
        """Send SIGTERM to all processes in the tree."""
        self.signal(signal_mod.SIGTERM)

    def kill(self) -> None:
        """Send SIGKILL to all processes in the tree."""
        self.signal(signal_mod.SIGKILL)

    async def cleanup(self) -> None:
        """Close streams, kill+reap if still running, close fds."""
        if self.stdin is not None:
            await self.stdin.close()
        if self.stdout is not None:
            await self.stdout.close()
        if self.returncode is None:
            await asyncio.shield(self.root.kill())
            code = self.root.returncode()
            assert code is not None
            self.returncode = code
        self.root.close_fds()


class StartCtx[
    StdinT: (ByteWriteStream, TextWriteStream, None) = None,
    StdoutT: (ByteReadStream, TextReadStream, None) = None,
]:
    """Async context manager that spawns and owns an Execution.

    Returned by start(). Use chained builder methods to configure streams::

        async with start(cmd).stdin(PIPE).stdout(PIPE) as execution: ...

    __aenter__ spawns the process tree and creates an Execution handle.
    __aexit__ delegates to Execution.cleanup() for full teardown.
    """

    _cmd: Runnable
    _stdin_arg: int | Pipe | None
    _stdout_arg: int | Pipe | None
    _stdin_encoding: str | None
    _stdout_encoding: str | None
    _execution: Execution[Any, Any] | None

    def __init__(
        self,
        cmd: Runnable,
        *,
        _stdin: int | Pipe | None = None,
        _stdout: int | Pipe | None = None,
        _stdin_encoding: str | None = "utf-8",
        _stdout_encoding: str | None = "utf-8",
    ) -> None:
        self._cmd = cmd
        self._stdin_arg = _stdin
        self._stdout_arg = _stdout
        self._stdin_encoding = _stdin_encoding
        self._stdout_encoding = _stdout_encoding
        self._execution = None

    @overload
    def stdin(
        self, arg: Pipe, encoding: None
    ) -> StartCtx[ByteWriteStream, StdoutT]: ...
    @overload
    def stdin(
        self, arg: Pipe, encoding: str = ...
    ) -> StartCtx[TextWriteStream, StdoutT]: ...
    @overload
    def stdin(self, arg: int | None) -> StartCtx[None, StdoutT]: ...

    def stdin(
        self, arg: int | Pipe | None, encoding: str | None = "utf-8"
    ) -> StartCtx[Any, Any]:
        """Set stdin fd: PIPE for auto-pipe, int for raw fd, None to inherit."""
        return StartCtx(
            self._cmd,
            _stdin=arg,
            _stdout=self._stdout_arg,
            _stdin_encoding=encoding,
            _stdout_encoding=self._stdout_encoding,
        )

    @overload
    def stdout(self, arg: Pipe, encoding: None) -> StartCtx[StdinT, ByteReadStream]: ...
    @overload
    def stdout(
        self, arg: Pipe, encoding: str = ...
    ) -> StartCtx[StdinT, TextReadStream]: ...
    @overload
    def stdout(self, arg: int | None) -> StartCtx[StdinT, None]: ...

    def stdout(
        self, arg: int | Pipe | None, encoding: str | None = "utf-8"
    ) -> StartCtx[Any, Any]:
        """Set stdout fd: PIPE for auto-pipe, int for raw fd, None to inherit."""
        return StartCtx(
            self._cmd,
            _stdin=self._stdin_arg,
            _stdout=arg,
            _stdin_encoding=self._stdin_encoding,
            _stdout_encoding=encoding,
        )

    def _alloc_stdin(self, ctx: SpawnCtx) -> tuple[OwnedFd, OwnedFd | None]:
        """Resolve stdin arg into (spawn_fd, stream_fd). PIPE allocates a pipe."""
        if self._stdin_arg is PIPE:
            return ctx.pipe()
        if self._stdin_arg is None:
            return ctx.dup(STDIN), None
        return ctx.dup(self._stdin_arg), None

    def _alloc_stdout(self, ctx: SpawnCtx) -> tuple[OwnedFd | None, OwnedFd]:
        """Resolve stdout arg into (stream_fd, spawn_fd). PIPE allocates a pipe."""
        if self._stdout_arg is PIPE:
            return ctx.pipe()
        if self._stdout_arg is None:
            return None, ctx.dup(STDOUT)
        return None, ctx.dup(self._stdout_arg)

    def _wrap_stdin(
        self, fd: OwnedFd | None
    ) -> ByteWriteStream | TextWriteStream | None:
        """Wrap an owned fd into a stdin stream, optionally text-encoded."""
        if fd is None:
            return None
        stream = ByteWriteStream(fd)
        if self._stdin_encoding is None:
            return stream
        return TextWriteStream(stream, encoding=self._stdin_encoding)

    def _wrap_stdout(
        self, fd: OwnedFd | None
    ) -> ByteReadStream | TextReadStream | None:
        """Wrap an owned fd into a stdout stream, optionally text-decoded."""
        if fd is None:
            return None
        stream = ByteReadStream(fd)
        if self._stdout_encoding is None:
            return stream
        return TextReadStream(stream, encoding=self._stdout_encoding)

    async def __aenter__(self) -> Execution[StdinT, StdoutT]:
        """Spawn the process tree, allocating PIPE fds if requested."""
        ctx = SpawnCtx()
        try:
            spawn_stdin, stream_stdin = self._alloc_stdin(ctx)
            stream_stdout, spawn_stdout = self._alloc_stdout(ctx)

            # Spawn process tree
            std_fds = StdFds(stdin=spawn_stdin, stdout=spawn_stdout)
            root = await ctx.spawn(self._cmd, std_fds)

            # Children inherited via fork; close spawn-side fds so EOF propagates.
            # (FnNode dups from SpawnCtx.spawn_fn are separate — closed by __aexit__.)
            spawn_stdin.close()
            spawn_stdout.close()
        except BaseException:
            await ctx.cleanup()
            raise

        self._execution = Execution(
            root=root,
            stdin=cast("StdinT", self._wrap_stdin(stream_stdin)),
            stdout=cast("StdoutT", self._wrap_stdout(stream_stdout)),
        )
        return self._execution

    async def __aexit__(self, *exc_info: object) -> None:
        """Delegate full cleanup to Execution."""
        assert self._execution is not None
        await self._execution.cleanup()


def start(cmd: Runnable) -> StartCtx[None, None]:
    """Create an async context manager that spawns and manages an Execution.

    Use chained builder methods to configure streams::

        async with start(cmd) as execution:
            code = await execution.wait()

        async with start(cmd).stdin(PIPE).stdout(PIPE) as execution:
            await execution.stdin.write("data")
            captured = await execution.stdout.read()
    """
    return StartCtx(cmd)


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
    async with start(cmd).stdout(PIPE, encoding=encoding) as execution:
        code, captured = await asyncio.gather(
            execution.wait(),
            execution.stdout.read(),
        )

    if code != 0:
        raise subprocess.CalledProcessError(code, [], captured)

    return captured
