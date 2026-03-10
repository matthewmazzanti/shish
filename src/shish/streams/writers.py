"""Async writable streams — byte and text layers with buffering."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Buffer, Iterable

from shish._defaults import DEFAULT_ENCODING
from shish.fd import Fd

DEFAULT_BUFFER_SIZE = 65536


class _DirectWriter:
    """Unbuffered async fd writer. Owns the fd.

    Uses os.write + loop.add_writer directly. write() performs a
    single os.write call — if the fd would block, it suspends on
    add_writer first. Returns the actual byte count written (may be
    less than len(data) on partial writes). The caller is responsible
    for looping on short writes.
    """

    def __init__(self, owned_fd: Fd) -> None:
        self._fd = owned_fd
        self._loop = asyncio.get_running_loop()
        os.set_blocking(owned_fd.fd, False)

    async def write(self, data: Buffer) -> int:
        """Write once. Returns actual bytes written (may be short)."""
        if not data:
            return 0
        while True:
            try:
                return os.write(self._fd.fd, data)
            except BlockingIOError:
                await self._writable()

    @property
    def closed(self) -> bool:
        """Whether the fd is closed."""
        return self._fd.closed

    def close(self) -> None:
        """Close the fd."""
        self._fd.close()

    async def _writable(self) -> None:
        """Suspend until the fd is writable."""
        future: asyncio.Future[None] = self._loop.create_future()
        self._loop.add_writer(self._fd.fd, future.set_result, None)
        try:
            await future
        finally:
            self._loop.remove_writer(self._fd.fd)


class ByteWriteStream:
    """Async writable byte stream with userspace buffering.

    Mirrors open(mode="wb"). Owns the fd — closing the stream closes it.

    Memory contract:
        The writer allocates exactly buffer_size bytes at construction.
        This buffer never grows. Writes larger than buffer_size bypass
        the buffer entirely (write-through to raw). The writer never
        holds a reference to the caller's data across await boundaries
        beyond the current raw write() call.

    Cancellation contract:
        CancelledError may interrupt any await point. On cancellation:
        - The writer's internal buffer is in a consistent state.
        - An unknown amount of data may have been written to raw.
        - The current write()'s data is partially lost. This is by design.
        - The writer is NOT poisoned — subsequent writes work normally.

    Concurrency:
        An asyncio.Lock serializes write() and flush() so multiple tasks
        can safely call write() without interleaving buffer mutations.
    """

    def __init__(self, owned_fd: Fd, buffer_size: int = DEFAULT_BUFFER_SIZE) -> None:
        self._writer = _DirectWriter(owned_fd)
        self._buffer = bytearray()
        self._buffer_size = buffer_size
        self._lock = asyncio.Lock()

    @property
    def closed(self) -> bool:
        """Whether the stream is closed."""
        return self._writer.closed

    @property
    def buffer_size(self) -> int:
        """Buffer capacity in bytes."""
        return self._buffer_size

    @property
    def buffered(self) -> int:
        """Bytes currently in the write buffer (not yet flushed to raw)."""
        return len(self._buffer)

    async def write(self, data: Buffer) -> int:
        """Write data, buffering small writes. Returns len(data).

        For data <= buffer_size: copies into the internal buffer. Flushes
        the buffer first if needed to make room. No await if data fits in
        available space (fast path).

        For data > buffer_size: flushes the buffer, then writes data
        directly to raw in a loop (write-through). No copy.
        """
        if self.closed:
            raise OSError("write to closed stream")
        if not data:
            return 0

        async with self._lock:
            with memoryview(data) as view:
                length = len(view)
                # Flush if the new data won't fit alongside existing
                if self._buffer and len(self._buffer) + length > self._buffer_size:
                    await self._flush()

                # Buffer if small enough
                if length <= self._buffer_size:
                    self._buffer.extend(view)
                    return length

                # Write-through if oversized
                pos = 0
                while pos < length:
                    pos += await self._writer.write(view[pos:])

                return length

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
        """Drain internal buffer to raw.

        Each memoryview slice is with-blocked so its export lock is
        released on all exit paths (including exception tracebacks).
        This keeps the bytearray resizable for del[:pos] compaction.
        """
        pos = 0
        try:
            while pos < len(self._buffer):
                with memoryview(self._buffer)[pos:] as chunk:
                    pos += await self._writer.write(chunk)
        finally:
            if pos > 0:
                del self._buffer[:pos]

    def close_fd(self) -> None:
        """Close the fd without flushing."""
        self._writer.close()

    async def close(self) -> None:
        """Flush buffer and close the fd."""
        if self.closed:
            return
        try:
            await self.flush()
        finally:
            self._writer.close()

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


class TextWriteStream:
    """Async writable text stream. Encodes strings into a ByteWriteStream.

    Mirrors open(mode="w"). Owns the byte stream — closing this closes it.

    write() encodes and passes through to the underlying ByteWriteStream,
    which handles buffering. Returns the number of characters written
    (always len(data)), not bytes.
    """

    def __init__(
        self,
        writer: ByteWriteStream,
        encoding: str = DEFAULT_ENCODING,
    ) -> None:
        self._writer = writer
        self._encoding = encoding

    @property
    def closed(self) -> bool:
        """Whether the stream is closed."""
        return self._writer.closed

    async def write(self, data: str) -> int:
        """Encode in chunks and write. Returns number of characters written.

        Chunks by the underlying byte stream's buffer size to avoid
        allocating the full encoded string at once.
        """
        if self.closed:
            raise OSError("write to closed stream")
        # Divide by 4: worst-case bytes per char in any Unicode encoding
        # (UTF-8, UTF-16 surrogates, UTF-32). Ensures encoded chunks
        # fit within the byte stream's buffer.
        chunk_size = max(self._writer.buffer_size // 4, 1)
        for offset in range(0, len(data), chunk_size):
            chunk = data[offset : offset + chunk_size]
            await self._writer.write(chunk.encode(self._encoding))
        return len(data)

    async def writelines(self, lines: Iterable[str]) -> None:
        """Write an iterable of strings."""
        for line in lines:
            await self.write(line)

    async def write_eof(self, data: str = "") -> None:
        """Write final data and close. Signals EOF to the reader."""
        if data:
            await self.write(data)
        await self.close()

    async def close(self) -> None:
        """Close the underlying byte stream."""
        await self._writer.close()

    async def __aenter__(self) -> TextWriteStream:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        *_args: object,
    ) -> None:
        if exc_type is not None and not issubclass(exc_type, Exception):
            self._writer.close_fd()
        else:
            await self.close()
