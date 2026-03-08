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

    Wraps a _DirectWriter with a bytearray buffer (matching Python's
    BufferedWriter pattern). Small writes accumulate in the buffer;
    flush() drains them via the underlying writer. close() is async
    to ensure the buffer is flushed before the fd is closed.

    Buffering strategy:
    - write(data) where data fits in remaining buffer space: append.
    - write(data) where data doesn't fit: flush, then write directly
      if data >= buffer_size, else buffer.
    """

    def __init__(self, owned_fd: Fd, buffer_size: int = DEFAULT_BUFFER_SIZE) -> None:
        self._writer = _DirectWriter(owned_fd)
        self._buf = bytearray()
        self._buffer_size = buffer_size

    @property
    def closed(self) -> bool:
        """Whether the stream is closed."""
        return self._writer.closed

    @property
    def buffer_size(self) -> int:
        """Buffer capacity in bytes."""
        return self._buffer_size

    async def write(self, data: Buffer) -> int:
        """Write data, buffering small writes. Returns len(data)."""
        if self.closed:
            raise OSError("write to closed stream")
        if not data:
            return 0
        view = memoryview(data)
        length = len(view)

        if len(self._buf) + length <= self._buffer_size:
            # Fits in buffer — just append
            self._buf.extend(view)
            return length

        # Doesn't fit — flush current buffer first
        await self.flush()

        if length >= self._buffer_size:
            # Large write — bypass buffer, write directly
            await self._drain(view)
        else:
            # Small write — buffer it
            self._buf.extend(view)

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
        """Flush the buffer to the underlying writer."""
        if self._buf:
            await self._drain(self._buf)
            self._buf.clear()

    async def _drain(self, data: Buffer) -> None:
        """Write all of data via the underlying writer, looping on short writes."""
        view = memoryview(data)
        written = 0
        while written < len(view):
            written += await self._writer.write(view[written:])

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

    async def __aexit__(self, *_args: object) -> None:
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

    async def __aexit__(self, *_args: object) -> None:
        await self.close()
