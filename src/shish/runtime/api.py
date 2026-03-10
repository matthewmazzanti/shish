"""Public lifecycle API: Job, JobCtx, start().

Provides the user-facing entry points for spawning and managing
process trees built from IR commands.
"""

from __future__ import annotations

import asyncio
import dataclasses as dc
import enum
import typing as ty

from shish._defaults import DEFAULT_ENCODING
from shish.builders import Runnable
from shish.fd import PIPE, STDERR, STDIN, STDOUT, Fd, Pipe
from shish.runtime.spawn import SpawnScope
from shish.runtime.tree import (
    ProcessNode,
    StdFds,
)
from shish.streams import (
    ByteReadStream,
    ByteWriteStream,
    TextReadStream,
    TextWriteStream,
)


class CloseMethod(enum.IntEnum):
    """Shutdown escalation level for Job.close().

    Each level escalates to the next on timeout:
    EOF → TERMINATE → KILL.
    """

    EOF = 0
    TERMINATE = 1
    KILL = 2


@dc.dataclass
class Job[
    StdinT: (ByteWriteStream, TextWriteStream, None) = None,
    StdoutT: (ByteReadStream, TextReadStream, None) = None,
    StderrT: (ByteReadStream, TextReadStream, None) = None,
]:
    """Handle for a spawned process tree.

    Created by JobCtx.__aenter__. Provides signal/terminate/kill for
    explicit control and wait() for exit code retrieval. wait() is
    idempotent — second call returns cached returncode.

    When started with stdin=PIPE, stdout=PIPE, or stderr=PIPE, the
    corresponding stream fields are set to text or byte streams
    depending on the encoding parameter. Generic over StdinT/StdoutT/
    StderrT so that passing PIPE statically narrows the stream type
    to non-None.
    """

    root: ProcessNode
    stdin: StdinT
    stdout: StdoutT
    stderr: StderrT
    returncode: int | None = dc.field(default=None, init=False)

    async def wait(self) -> int:
        """Wait for all processes and return the exit code.

        Idempotent — second call returns cached returncode.
        """
        if self.returncode is not None:
            return self.returncode
        self.returncode = await self.root.wait()
        return self.returncode

    def terminate(self) -> None:
        """Ask nicely, don't wait. SIGTERM for processes, cancel for tasks.

        Call wait() afterwards to collect the exit code.
        """
        self.root.terminate()

    def kill(self) -> None:
        """Tell, don't wait. SIGKILL for processes, cancel for tasks.

        Call wait() afterwards to collect the exit code.
        """
        self.root.kill()

    async def _eof_wait(self, timeout: float) -> bool:
        """Wait for natural exit (processes see EOF). True if exited in time."""
        try:
            await asyncio.wait_for(self.wait(), timeout=timeout)
        except (TimeoutError, asyncio.CancelledError):
            return False
        return True

    async def _terminate_wait(self, timeout: float) -> bool:
        """SIGTERM + wait. True if exited in time."""
        self.terminate()
        try:
            await asyncio.wait_for(self.wait(), timeout=timeout)
        except (TimeoutError, asyncio.CancelledError):
            return False
        return True

    async def _kill_wait(self, timeout: float) -> bool:
        """SIGKILL + wait. Always succeeds — kernel guarantees SIGKILL."""
        self.kill()
        await self.wait()
        return True

    async def close(
        self,
        *,
        method: CloseMethod = CloseMethod.EOF,
        timeout: float = 3,
    ) -> CloseMethod:
        """Close streams and fds, then wait for processes to exit.

        Starts at the given method and escalates on timeout:
        EOF → TERMINATE → KILL.

        Args:
            method: Starting shutdown level.
                EOF — wait for natural exit (processes see EOF).
                TERMINATE — SIGTERM, escalate to KILL on timeout.
                KILL — SIGKILL immediately.
            timeout: Seconds to wait at each escalation step.

        Returns:
            The CloseMethod that successfully stopped the processes.

        Raises:
            RuntimeError: If processes don't exit after SIGKILL + timeout.
        """
        if self.stdin is not None:
            await self.stdin.close()
        if self.stdout is not None:
            await self.stdout.close()
        if self.stderr is not None:
            await self.stderr.close()
        self.root.close_fds()

        if self.returncode is not None:
            return method

        if method == CloseMethod.EOF and self.stdin is None:
            method = CloseMethod.TERMINATE

        steps = [self._eof_wait, self._terminate_wait, self._kill_wait]
        for step in steps[method:]:
            if await step(timeout):
                return method
            method = CloseMethod(method + 1)

        raise AssertionError("unreachable")


class JobCtx[
    StdinT: (ByteWriteStream, TextWriteStream, None) = None,
    StdoutT: (ByteReadStream, TextReadStream, None) = None,
    StderrT: (ByteReadStream, TextReadStream, None) = None,
]:
    """Async context manager that spawns and owns an Job.

    Returned by start(). Use chained builder methods to configure streams::

        async with start(cmd).stdin(PIPE).stdout(PIPE).stderr(PIPE) as execution: ...

    __aenter__ spawns the process tree and creates an Job handle.
    __aexit__ calls close() which escalates EOF → TERMINATE → KILL.
    """

    _cmd: Runnable
    _stdin_arg: int | Pipe | None
    _stdout_arg: int | Pipe | None
    _stderr_arg: int | Pipe | None
    _stdin_encoding: str | None
    _stdout_encoding: str | None
    _stderr_encoding: str | None
    _cleanup_timeout: float
    _execution: Job[ty.Any, ty.Any, ty.Any] | None

    def __init__(
        self,
        cmd: Runnable,
        *,
        _stdin: int | Pipe | None = None,
        _stdout: int | Pipe | None = None,
        _stderr: int | Pipe | None = None,
        _stdin_encoding: str | None = DEFAULT_ENCODING,
        _stdout_encoding: str | None = DEFAULT_ENCODING,
        _stderr_encoding: str | None = DEFAULT_ENCODING,
        _cleanup_timeout: float = 3,
    ) -> None:
        self._cmd = cmd
        self._stdin_arg = _stdin
        self._stdout_arg = _stdout
        self._stderr_arg = _stderr
        self._stdin_encoding = _stdin_encoding
        self._stdout_encoding = _stdout_encoding
        self._stderr_encoding = _stderr_encoding
        self._cleanup_timeout = _cleanup_timeout
        self._execution = None

    @ty.overload
    def stdin(
        self, arg: Pipe, encoding: None
    ) -> JobCtx[ByteWriteStream, StdoutT, StderrT]: ...
    @ty.overload
    def stdin(
        self, arg: Pipe, encoding: str = ...
    ) -> JobCtx[TextWriteStream, StdoutT, StderrT]: ...
    @ty.overload
    def stdin(self, arg: int | None) -> JobCtx[None, StdoutT, StderrT]: ...

    def stdin(
        self, arg: int | Pipe | None, encoding: str | None = DEFAULT_ENCODING
    ) -> JobCtx[ty.Any, ty.Any, ty.Any]:
        """Set stdin fd: PIPE for auto-pipe, int for raw fd, None to inherit."""
        return JobCtx(
            self._cmd,
            _stdin=arg,
            _stdout=self._stdout_arg,
            _stderr=self._stderr_arg,
            _stdin_encoding=encoding,
            _stdout_encoding=self._stdout_encoding,
            _stderr_encoding=self._stderr_encoding,
            _cleanup_timeout=self._cleanup_timeout,
        )

    @ty.overload
    def stdout(
        self, arg: Pipe, encoding: None
    ) -> JobCtx[StdinT, ByteReadStream, StderrT]: ...
    @ty.overload
    def stdout(
        self, arg: Pipe, encoding: str = ...
    ) -> JobCtx[StdinT, TextReadStream, StderrT]: ...
    @ty.overload
    def stdout(self, arg: int | None) -> JobCtx[StdinT, None, StderrT]: ...

    def stdout(
        self, arg: int | Pipe | None, encoding: str | None = DEFAULT_ENCODING
    ) -> JobCtx[ty.Any, ty.Any, ty.Any]:
        """Set stdout fd: PIPE for auto-pipe, int for raw fd, None to inherit."""
        return JobCtx(
            self._cmd,
            _stdin=self._stdin_arg,
            _stdout=arg,
            _stderr=self._stderr_arg,
            _cleanup_timeout=self._cleanup_timeout,
            _stdin_encoding=self._stdin_encoding,
            _stdout_encoding=encoding,
            _stderr_encoding=self._stderr_encoding,
        )

    @ty.overload
    def stderr(
        self, arg: Pipe, encoding: None
    ) -> JobCtx[StdinT, StdoutT, ByteReadStream]: ...
    @ty.overload
    def stderr(
        self, arg: Pipe, encoding: str = ...
    ) -> JobCtx[StdinT, StdoutT, TextReadStream]: ...
    @ty.overload
    def stderr(self, arg: int | None) -> JobCtx[StdinT, StdoutT, None]: ...

    def stderr(
        self, arg: int | Pipe | None, encoding: str | None = DEFAULT_ENCODING
    ) -> JobCtx[ty.Any, ty.Any, ty.Any]:
        """Set stderr fd: PIPE for auto-pipe, int for raw fd, None to inherit."""
        return JobCtx(
            self._cmd,
            _stdin=self._stdin_arg,
            _stdout=self._stdout_arg,
            _stderr=arg,
            _cleanup_timeout=self._cleanup_timeout,
            _stdin_encoding=self._stdin_encoding,
            _stdout_encoding=self._stdout_encoding,
            _stderr_encoding=encoding,
        )

    def _alloc_stdin(self, ctx: SpawnScope) -> tuple[Fd, Fd | None]:
        """Resolve stdin arg into (spawn_fd, stream_fd). PIPE allocates a pipe."""
        if self._stdin_arg is PIPE:
            return ctx.pipe()
        if self._stdin_arg is None:
            return Fd(STDIN, owned=False), None
        return Fd(self._stdin_arg, owned=False), None

    def _alloc_stdout(self, ctx: SpawnScope) -> tuple[Fd | None, Fd]:
        """Resolve stdout arg into (stream_fd, spawn_fd). PIPE allocates a pipe."""
        if self._stdout_arg is PIPE:
            return ctx.pipe()
        if self._stdout_arg is None:
            return None, Fd(STDOUT, owned=False)
        return None, Fd(self._stdout_arg, owned=False)

    def _wrap_stdin(self, fd: Fd | None) -> ByteWriteStream | TextWriteStream | None:
        """Wrap an owned fd into a stdin stream, optionally text-encoded."""
        if fd is None:
            return None
        stream = ByteWriteStream.from_fd(fd)
        if self._stdin_encoding is None:
            return stream
        return TextWriteStream(stream, encoding=self._stdin_encoding)

    def _wrap_stdout(self, fd: Fd | None) -> ByteReadStream | TextReadStream | None:
        """Wrap an owned fd into a stdout stream, optionally text-decoded."""
        if fd is None:
            return None
        stream = ByteReadStream.from_fd(fd)
        if self._stdout_encoding is None:
            return stream
        return TextReadStream(stream, encoding=self._stdout_encoding)

    def _alloc_stderr(self, ctx: SpawnScope) -> tuple[Fd | None, Fd]:
        """Resolve stderr arg into (stream_fd, spawn_fd). PIPE allocates a pipe."""
        if self._stderr_arg is PIPE:
            return ctx.pipe()
        if self._stderr_arg is None:
            return None, Fd(STDERR, owned=False)
        return None, Fd(self._stderr_arg, owned=False)

    def _wrap_stderr(self, fd: Fd | None) -> ByteReadStream | TextReadStream | None:
        """Wrap an owned fd into a stderr stream, optionally text-decoded."""
        if fd is None:
            return None
        stream = ByteReadStream.from_fd(fd)
        if self._stderr_encoding is None:
            return stream
        return TextReadStream(stream, encoding=self._stderr_encoding)

    async def __aenter__(self) -> Job[StdinT, StdoutT, StderrT]:
        """Spawn the process tree, allocating PIPE fds if requested."""
        ctx = SpawnScope()
        try:
            spawn_stdin, stream_stdin = self._alloc_stdin(ctx)
            stream_stdout, spawn_stdout = self._alloc_stdout(ctx)
            stream_stderr, spawn_stderr = self._alloc_stderr(ctx)

            # Spawn process tree
            std_fds = StdFds(
                stdin=spawn_stdin, stdout=spawn_stdout, stderr=spawn_stderr
            )
            root = await ctx.spawn(self._cmd, std_fds)

            # Close spawn-side fds. PIPE fds (owning) are closed so EOF
            # propagates; inherit/raw-fd fds (non-owning) are no-op closes.
            spawn_stdin.close()
            spawn_stdout.close()
            spawn_stderr.close()
        except BaseException:
            await ctx.cleanup()
            raise

        self._execution = Job(
            root=root,
            stdin=ty.cast("StdinT", self._wrap_stdin(stream_stdin)),
            stdout=ty.cast("StdoutT", self._wrap_stdout(stream_stdout)),
            stderr=ty.cast("StderrT", self._wrap_stderr(stream_stderr)),
        )
        return self._execution

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        """Close streams, escalate from EOF → TERMINATE → KILL."""
        assert self._execution is not None
        await self._execution.close(timeout=self._cleanup_timeout)


def start(cmd: Runnable, *, cleanup_timeout: float = 3) -> JobCtx[None, None, None]:
    """Create an async context manager that spawns and manages an Job.

    Use chained builder methods to configure streams::

        async with start(cmd) as execution:
            code = await execution.wait()

        async with start(cmd).stdin(PIPE).stdout(PIPE) as execution:
            await execution.stdin.write("data")
            captured = await execution.stdout.read()

    Args:
        cmd: Command to execute.
        cleanup_timeout: Seconds to wait at each escalation step
            (SIGTERM → SIGKILL) during exception close. Default 3s.
    """
    return JobCtx(cmd, _cleanup_timeout=cleanup_timeout)
