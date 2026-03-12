"""v1c: async def, lock bypass, inlined raw writes, sync try-flush."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Buffer, Iterable

from shish.fd import Fd

DEFAULT_BUFFER_SIZE = 8192


class ByteWriteStream:
    """Async writable byte stream with userspace buffering.

    Inlines raw fd writes — no separate RawWriter layer. Three fast
    paths avoid the lock and event loop entirely:
      1. Data fits in buffer → memcpy, return
      2. Buffer full but fd writable → sync flush, memcpy, return
      3. Large data, fd writable → sync write-through, return
    Only acquires the lock and yields when the fd would actually block.
    """

    def __init__(self, owned_fd: Fd, buffer_size: int = DEFAULT_BUFFER_SIZE) -> None:
        self._fd = owned_fd
        self._loop = asyncio.get_running_loop()
        self._buffer = bytearray(buffer_size)
        self._buf_len = 0
        self._buffer_size = buffer_size
        self._lock = asyncio.Lock()
        os.set_blocking(owned_fd.fd, False)

    @classmethod
    def from_fd(
        cls, owned_fd: Fd, buffer_size: int = DEFAULT_BUFFER_SIZE
    ) -> ByteWriteStream:
        """Create a ByteWriteStream from a file descriptor."""
        return cls(owned_fd, buffer_size)

    @property
    def closed(self) -> bool:
        """Whether the stream is closed."""
        return self._fd.closed

    @property
    def buffer_size(self) -> int:
        """Buffer capacity in bytes."""
        return self._buffer_size

    @property
    def buffered(self) -> int:
        """Bytes currently in the write buffer (not yet flushed to raw)."""
        return self._buf_len

    async def write(self, data: Buffer) -> int:
        """Write data, buffering small writes. Returns len(data)."""
        if self.closed:
            raise OSError("write to closed stream")
        if not data:
            return 0

        view = memoryview(data)
        length = len(view)

        if not self._lock.locked() and length < self._buffer_size:
            # Fast path 1: fits without flushing
            if self._buf_len + length <= self._buffer_size:
                self._buffer[self._buf_len : self._buf_len + length] = view
                self._buf_len += length
                view.release()
                return length

            # Fast path 2: sync flush makes room
            if self._try_flush(length):
                self._buffer[self._buf_len : self._buf_len + length] = view
                self._buf_len += length
                view.release()
                return length

        # Slow path: acquire lock for async flush or write-through
        async with self._lock:
            try:
                if self._buf_len > 0 and self._buf_len + length > self._buffer_size:
                    await self._flush()

                if length < self._buffer_size:
                    self._buffer[self._buf_len : self._buf_len + length] = view
                    self._buf_len += length
                    return length

                # Write-through for buffer_size or larger
                pos = 0
                while pos < length:
                    try:
                        pos += os.write(self._fd.fd, view[pos:])
                    except BlockingIOError:
                        await self._writable()
            finally:
                view.release()

            return length

    def _try_flush(self, need: int) -> bool:
        """Try to sync-flush enough buffer to fit `need` bytes.

        Drains via os.write until there's room or a write would block.
        Returns True if enough space was freed.
        """
        pos = 0
        while pos < self._buf_len:
            if self._buf_len - pos + need <= self._buffer_size:
                break
            try:
                pos += os.write(self._fd.fd, self._buffer[pos : self._buf_len])
            except BlockingIOError:
                break
        if pos > 0:
            remaining = self._buf_len - pos
            self._buffer[:remaining] = self._buffer[pos : self._buf_len]
            self._buf_len = remaining
        return self._buf_len + need <= self._buffer_size

    async def writelines(self, data: Iterable[Buffer]) -> None:
        """Write an iterable of byte chunks."""
        for chunk in data:
            await self.write(chunk)

    async def write_eof(self, data: Buffer = b"") -> None:
        """Write final data and close. Signals EOF to the reader."""
        if data:
            await self.write(data)
        await self.close()

    async def flush(self) -> None:
        """Drain the entire internal buffer."""
        async with self._lock:
            await self._flush()

    async def _flush(self) -> None:
        """Drain the buffer via raw writes."""
        pos = 0
        try:
            while pos < self._buf_len:
                try:
                    pos += os.write(self._fd.fd, self._buffer[pos : self._buf_len])
                except BlockingIOError:
                    await self._writable()
        finally:
            if pos > 0:
                remaining = self._buf_len - pos
                self._buffer[:remaining] = self._buffer[pos : self._buf_len]
                self._buf_len = remaining

    async def _writable(self) -> None:
        """Suspend until the fd is writable."""
        future: asyncio.Future[None] = self._loop.create_future()
        self._loop.add_writer(self._fd.fd, future.set_result, None)
        try:
            await future
        finally:
            self._loop.remove_writer(self._fd.fd)

    def close_fd(self) -> None:
        """Close the fd without flushing."""
        self._fd.close()

    async def close(self) -> None:
        """Flush buffer and close the fd."""
        if self.closed:
            return
        try:
            await self.flush()
        finally:
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
