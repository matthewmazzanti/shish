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
import enum
import os
from collections.abc import Buffer, Generator
from typing import Any

from shish.fd import Fd


class _WritePhase(enum.IntEnum):
    SETUP = 0  # decide: flush or skip
    FLUSH = 1  # draining internal buffer
    CHECK_BUFFER = 2  # buffer small data or start write-through
    WRITE = 3  # write-through for large data


def _make_writable_future(
    loop: asyncio.AbstractEventLoop,
    fd: int,
) -> asyncio.Future[None]:
    """Create a one-shot Future that resolves when fd is writable.

    Removes its own writer registration when the fd becomes writable,
    so callers don't need to track cleanup.

    future = loop.create_future()
    loop.add_writer(fd, lambda: (loop.remove_writer(fd), future.set_result(None)))
    future._asyncio_future_blocking = True
    return future
    """
    future: asyncio.Future[None] = loop.create_future()

    def _on_writable() -> None:
        loop.remove_writer(fd)
        future.set_result(None)

    loop.add_writer(fd, _on_writable)
    # asyncio Task checks this flag to distinguish yield vs yield-from
    future._asyncio_future_blocking = True  # type: ignore[attr-defined]
    return future


class ByteWriteStream:
    """Buffered async byte writer — single-writer, no lock.

    write() returns a WriteAwaitable. On the fast path (data fits in
    buffer), the first __next__ does a memcpy and raises StopIteration.
    No coroutine frame, no lock, no event loop interaction.

    This is the pure-Python reference for the C extension.
    """

    __slots__ = (
        "_buf_cap",
        "_buf_len",
        "_buffer",
        "_fd",
        "_fd_int",
        "_flush_pos",
        "_loop",
    )

    def __init__(self, owned_fd: Fd, buffer_size: int = 8192) -> None:
        self._fd = owned_fd
        self._fd_int = owned_fd.fd
        self._loop = asyncio.get_running_loop()
        self._buffer = bytearray(buffer_size)
        self._buf_len = 0
        self._buf_cap = buffer_size
        self._flush_pos = 0
        os.set_blocking(owned_fd.fd, False)

    @classmethod
    def from_fd(
        cls,
        owned_fd: Fd,
        buffer_size: int = 8192,
    ) -> ByteWriteStream:
        """Create from a file descriptor."""
        return cls(owned_fd, buffer_size)

    @property
    def closed(self) -> bool:
        """Whether the fd is closed."""
        return self._fd.closed

    # ── C internals (static inline in .c) ──

    def _needs_flush(self, length: int) -> bool:
        """Whether a flush is needed before buffering length bytes."""
        return self._buf_len > 0 and self._buf_len + length > self._buf_cap

    def _can_buffer(self, length: int) -> bool:
        """Whether length bytes fit in the internal buffer.

        Strict < so exactly buf_cap writes go through: a full
        buffer would flush immediately anyway (Go/Rust do the same).
        """
        return length < self._buf_cap

    def _copy_to_buf(self, data: memoryview) -> None:
        """Copy data into the internal buffer. Caller must check _can_buffer."""
        length = len(data)
        self._buffer[self._buf_len : self._buf_len + length] = data
        self._buf_len += length

    def _write_fd(
        self,
        view: memoryview,
        pos: int,
    ) -> tuple[int, asyncio.Future[None] | None]:
        """Write loop. Returns (pos, future) — future is set on EAGAIN.

        In C: PyObject *write_fd(..., Py_buffer *view, Py_ssize_t *pos)
        pos is an in/out pointer, return value is future or NULL.
        """
        while pos < len(view):
            try:
                pos += os.write(self._fd_int, view[pos:])
            except BlockingIOError:
                return pos, self._wait_writable()
        return pos, None

    def _flush_step(self) -> asyncio.Future[None] | None:
        """One step of flush. Returns Future on EAGAIN, None when done."""
        # In C: just pointer + length, no view wrapper needed.
        view = memoryview(self._buffer)
        self._flush_pos, future = self._write_fd(view[: self._buf_len], self._flush_pos)
        view.release()
        if future is not None:
            return future
        self._buf_len = 0
        self._flush_pos = 0
        return None

    def _wait_writable(self) -> asyncio.Future[None]:
        """One-shot Future that resolves when the fd is writable."""
        return _make_writable_future(self._loop, self._fd_int)

    def _remove_writer(self) -> None:
        """Remove event loop writer registration."""
        self._loop.remove_writer(self._fd_int)

    # ── Public API ──

    class _WriteAwaitable:
        """Awaitable for a single buffered write.

        Holds a reference to the parent writer and the caller's data.
        All write logic lives in __next__ — the awaitable IS the write.
        """

        __slots__ = ("_data", "_length", "_phase", "_pos", "_view", "_writer")

        def __init__(self, writer: ByteWriteStream, data: Buffer) -> None:
            self._writer = writer
            self._data = data
            self._view: memoryview
            self._length: int
            self._phase = _WritePhase.SETUP
            self._pos = 0

        def __await__(self) -> Generator[Any, None, int]:
            return self  # type: ignore[return-value]

        def __iter__(self) -> ByteWriteStream._WriteAwaitable:
            return self

        def __next__(self) -> asyncio.Future[None]:
            # Scenario                  | Path
            # Small, fits alongside     | SETUP → CHECK_BUFFER
            # Small, doesn't fit        | SETUP → FLUSH → CHECK_BUFFER
            # Large, buffer empty       | SETUP → WRITE
            # Large, buffer has data    | SETUP → FLUSH → WRITE
            writer = self._writer
            while True:
                match self._phase:
                    case _WritePhase.SETUP:
                        self._view = memoryview(self._data)
                        self._length = len(self._view)
                        if writer._needs_flush(self._length):
                            self._phase = _WritePhase.FLUSH
                        else:
                            self._phase = _WritePhase.CHECK_BUFFER
                        continue

                    case _WritePhase.FLUSH:
                        future = writer._flush_step()
                        if future is not None:
                            return future
                        self._phase = _WritePhase.CHECK_BUFFER
                        continue

                    case _WritePhase.CHECK_BUFFER:
                        if writer._can_buffer(self._length):
                            writer._copy_to_buf(self._view)
                            self._view.release()
                            raise StopIteration(self._length)
                        self._phase = _WritePhase.WRITE
                        continue

                    case _WritePhase.WRITE:
                        self._pos, future = writer._write_fd(self._view, self._pos)
                        if future is not None:
                            return future
                        self._view.release()
                        raise StopIteration(self._length)

        def send(self, _value: object) -> asyncio.Future[None]:
            """Resume after yield."""
            return self.__next__()

        def throw(
            self,
            typ: type[BaseException],
            val: BaseException | None = None,
            tb: object = None,
        ) -> asyncio.Future[None]:
            """Inject exception — clean up and re-raise."""
            self._writer._remove_writer()
            self._view.release()
            if val is None:
                raise typ
            if tb is not None:
                raise val.with_traceback(tb)  # type: ignore[arg-type]
            raise val

        def close(self) -> None:
            """Clean up on abandonment."""
            self._writer._remove_writer()
            self._view.release()

    class _FlushAwaitable:
        """Awaitable that drains the internal buffer.

        No state machine — all state lives on the writer (_flush_pos, _buf_len).
        Each __next__ call resumes _flush_step where it left off.
        """

        __slots__ = ("_writer",)

        def __init__(self, writer: ByteWriteStream) -> None:
            self._writer = writer

        def __await__(self) -> Generator[Any]:
            return self  # type: ignore[return-value]

        def __iter__(self) -> ByteWriteStream._FlushAwaitable:
            return self

        def __next__(self) -> asyncio.Future[None]:
            future = self._writer._flush_step()
            if future is not None:
                return future
            raise StopIteration(None)

        def send(self, _value: object) -> asyncio.Future[None]:
            """Resume after yield."""
            return self.__next__()

        def throw(
            self,
            typ: type[BaseException],
            val: BaseException | None = None,
            tb: object = None,
        ) -> asyncio.Future[None]:
            """Inject exception — clean up and re-raise."""
            self._writer._remove_writer()
            if val is None:
                raise typ
            if tb is not None:
                raise val.with_traceback(tb)  # type: ignore[arg-type]
            raise val

        def close(self) -> None:
            """Clean up on abandonment."""
            self._writer._remove_writer()

    # ── Public API ──

    def write(self, data: Buffer) -> _WriteAwaitable:
        """Write data. Returns an awaitable that resolves to len(data)."""
        return self._WriteAwaitable(self, data)

    def flush(self) -> _FlushAwaitable:
        """Flush the internal buffer. Returns an awaitable."""
        return self._FlushAwaitable(self)

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
            self._remove_writer()
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
