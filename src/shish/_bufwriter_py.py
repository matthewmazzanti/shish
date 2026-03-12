"""Pure-Python reference for C buffered writer.

Single-writer, no lock. write() returns a WriteAwaitable that either:
  - Fast path: memcpy into buffer, StopIteration(len) on first __next__
  - Flush path: drain buffer via os.write, may yield Futures on EAGAIN
  - Write-through: large data bypasses buffer, writes directly to fd

This is the Python blueprint for _cbufwriter.c.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import os
from collections.abc import Buffer, Generator
from typing import Any, Never

from shish.fd import Fd


class WriteAwaitable:
    """Awaitable for a single buffered write.

    Holds a reference to the parent writer and the caller's data.
    All write logic lives in __next__ — the awaitable IS the write.
    """

    __slots__ = ("_writer", "_view", "_length", "_phase", "_future")

    def __init__(self, writer: ByteWriteStream, data: Buffer) -> None:
        self._writer = writer
        self._view: memoryview | None = memoryview(data)
        self._length = len(self._view)
        # 0 = not started, 1 = flushing buffer, 2 = writing through
        self._phase = 0
        self._future: asyncio.Future[None] | None = None

    def __await__(self) -> Generator[Any, None, int]:
        return self  # type: ignore[return-value]

    def __iter__(self) -> WriteAwaitable:
        return self

    def __next__(self) -> asyncio.Future[None]:
        writer = self._writer
        view = self._view
        assert view is not None

        # Phase 0: initial entry — try fast path
        if self._phase == 0:
            length = self._length

            # Fast path: fits in buffer
            if length < writer._buf_cap and writer._buf_len + length <= writer._buf_cap:
                self._buffer_data(writer._buf_len)

            # Need to flush first if buffer has data
            if writer._buf_len > 0:
                self._phase = 1
                return self._flush_step()

            # Buffer empty but data >= buf_cap — write-through
            self._phase = 2
            return self._writethrough_step()

        # Phase 1: flushing buffer
        if self._phase == 1:
            return self._flush_step()

        # Phase 2: write-through
        return self._writethrough_step()

    def _buffer_data(self, pos: int) -> Never:
        """Copy data into buffer at pos and complete."""
        writer = self._writer
        view = self._view
        assert view is not None
        length = self._length
        writer._buffer[pos : pos + length] = view
        writer._buf_len = pos + length
        view.release()
        self._view = None
        raise StopIteration(length)

    def _flush_step(self) -> asyncio.Future[None]:
        """Drain the writer's internal buffer, then buffer or write-through."""
        if not self._writer._drain_buffer():
            return self._wait_writable()

        # Buffer if it fits, otherwise write-through
        if self._length < self._writer._buf_cap:
            self._buffer_data(0)

        self._phase = 2
        return self._writethrough_step()

    def _writethrough_step(self) -> asyncio.Future[None]:
        """Write caller's data directly to fd. Returns Future or raises StopIteration."""
        writer = self._writer
        view = self._view
        assert view is not None

        while writer._wt_pos < self._length:
            try:
                written = os.write(writer._fd_int, view[writer._wt_pos :])
                writer._wt_pos += written
            except BlockingIOError:
                return self._wait_writable()

        # Done
        writer._wt_pos = 0
        view.release()
        self._view = None
        raise StopIteration(self._length)

    def _wait_writable(self) -> asyncio.Future[None]:
        """Create a Future that resolves when the fd is writable."""
        writer = self._writer
        future: asyncio.Future[None] = writer._loop.create_future()
        writer._loop.add_writer(writer._fd_int, future.set_result, None)
        self._future = future
        # asyncio Task checks this flag to distinguish yield vs yield-from
        future._asyncio_future_blocking = True  # type: ignore[attr-defined]
        return future

    def _cancel_writer(self) -> None:
        """Remove event loop writer registration if active."""
        if self._future is not None:
            self._writer._loop.remove_writer(self._writer._fd_int)
            self._future = None

    def send(self, _value: object) -> asyncio.Future[None]:  # noqa: ANN401
        """Resume after yield."""
        self._cancel_writer()
        return self.__next__()

    def throw(  # noqa: ANN401
        self,
        typ: type[BaseException],
        val: BaseException | None = None,
        tb: object = None,
    ) -> asyncio.Future[None]:
        """Inject exception — clean up and re-raise."""
        self._cancel_writer()
        if self._view is not None:
            self._view.release()
            self._view = None
        if val is None:
            raise typ
        if tb is not None:
            raise val.with_traceback(tb)  # type: ignore[arg-type]
        raise val

    def close(self) -> None:
        """Clean up on abandonment."""
        self._cancel_writer()
        if self._view is not None:
            self._view.release()
            self._view = None


class FlushAwaitable:
    """Awaitable that drains the internal buffer."""

    __slots__ = ("_writer", "_future")

    def __init__(self, writer: ByteWriteStream) -> None:
        self._writer = writer
        self._future: asyncio.Future[None] | None = None

    def __await__(self) -> Generator[Any, None, None]:
        return self  # type: ignore[return-value]

    def __iter__(self) -> FlushAwaitable:
        return self

    def __next__(self) -> asyncio.Future[None]:
        if not self._writer._drain_buffer():
            return self._wait_writable()
        raise StopIteration(None)

    def _wait_writable(self) -> asyncio.Future[None]:
        """Create a Future that resolves when the fd is writable."""
        writer = self._writer
        future: asyncio.Future[None] = writer._loop.create_future()
        writer._loop.add_writer(writer._fd_int, future.set_result, None)
        self._future = future
        # asyncio Task checks this flag to distinguish yield vs yield-from
        future._asyncio_future_blocking = True  # type: ignore[attr-defined]
        return future

    def _cancel_writer(self) -> None:
        """Remove event loop writer registration if active."""
        if self._future is not None:
            self._writer._loop.remove_writer(self._writer._fd_int)
            self._future = None

    def send(self, _value: object) -> asyncio.Future[None]:  # noqa: ANN401
        """Resume after yield."""
        self._cancel_writer()
        return self.__next__()

    def throw(  # noqa: ANN401
        self,
        typ: type[BaseException],
        val: BaseException | None = None,
        tb: object = None,
    ) -> asyncio.Future[None]:
        """Inject exception — clean up and re-raise."""
        self._cancel_writer()
        if val is None:
            raise typ
        if tb is not None:
            raise val.with_traceback(tb)  # type: ignore[arg-type]
        raise val

    def close(self) -> None:
        """Clean up."""
        self._cancel_writer()


class ByteWriteStream:
    """Buffered async byte writer — single-writer, no lock.

    write() returns a WriteAwaitable. On the fast path (data fits in
    buffer), the first __next__ does a memcpy and raises StopIteration.
    No coroutine frame, no lock, no event loop interaction.

    This is the pure-Python reference for the C extension.
    """

    __slots__ = (
        "_fd", "_fd_int", "_loop", "_buffer", "_buf_len", "_buf_cap",
        "_flush_pos", "_wt_pos",
    )

    def __init__(self, owned_fd: Fd, buffer_size: int = 8192) -> None:
        self._fd = owned_fd
        self._fd_int = owned_fd.fd
        self._loop = asyncio.get_running_loop()
        self._buffer = bytearray(buffer_size)
        self._buf_len = 0
        self._buf_cap = buffer_size
        self._flush_pos = 0
        self._wt_pos = 0
        os.set_blocking(owned_fd.fd, False)

    @classmethod
    def from_fd(
        cls, owned_fd: Fd, buffer_size: int = 8192,
    ) -> ByteWriteStream:
        """Create from a file descriptor."""
        return cls(owned_fd, buffer_size)

    @property
    def closed(self) -> bool:
        """Whether the fd is closed."""
        return self._fd.closed

    def _drain_buffer(self) -> bool:
        """Try to drain the internal buffer synchronously.

        Returns True if fully drained, False on EAGAIN.
        """
        while self._flush_pos < self._buf_len:
            try:
                written = os.write(self._fd_int, self._buffer[self._flush_pos : self._buf_len])
                self._flush_pos += written
            except BlockingIOError:
                return False
        self._buf_len = 0
        self._flush_pos = 0
        return True

    def write(self, data: Buffer) -> WriteAwaitable:
        """Write data. Returns an awaitable that resolves to len(data)."""
        return WriteAwaitable(self, data)

    def flush(self) -> FlushAwaitable:
        """Flush the internal buffer. Returns an awaitable."""
        return FlushAwaitable(self)

    def close_fd(self) -> None:
        """Close the fd without flushing."""
        self._fd.close()

    async def close(self) -> None:
        """Flush and close."""
        if self.closed:
            return
        try:
            await self.flush()
        finally:
            # Remove any stale writer registration before closing fd
            # to prevent callbacks firing on reused fd numbers
            self._loop.remove_writer(self._fd_int)
            self._fd.close()

    async def __aenter__(self) -> ByteWriteStream:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        *_args: object,
    ) -> None:
        if exc_type is not None and not issubclass(exc_type, Exception):
            self.close_fd()
        else:
            await self.close()
