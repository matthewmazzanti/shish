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

    Extracted from the original ByteWriteStream. Uses os.write +
    loop.add_writer directly. write() loops with a memoryview,
    advancing past each partial os.write until all data is delivered.
    If EAGAIN, it suspends on add_writer until the fd is writable.
    """

    def __init__(self, owned_fd: Fd) -> None:
        self._fd = owned_fd
        self._loop = asyncio.get_running_loop()
        os.set_blocking(owned_fd.fd, False)

    async def write(self, data: Buffer) -> int:
        """Write all bytes. Awaits when pipe buffer is full. Returns len(data)."""
        if not data:
            return 0
        view = memoryview(data)
        written = 0
        while written < len(view):
            try:
                written += os.write(self._fd.fd, view[written:])
            except BlockingIOError:
                await self._writable()
        return written

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
        self._fd = owned_fd
        self._buf = bytearray()
        self._buffer_size = buffer_size

    async def write(self, data: Buffer) -> int:
        """Write data, buffering small writes. Returns len(data)."""
        if self._fd.closed:
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
            await self._writer.write(view)
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
            await self._writer.write(self._buf)
            self._buf.clear()

    async def close(self) -> None:
        """Flush buffer and close the fd."""
        if self._fd.closed:
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

    write() encodes in buffer_size character chunks to avoid allocating
    the full encoded bytes for large strings. Returns the number of
    characters written (always len(data)), not bytes.
    """

    def __init__(
        self,
        stream: ByteWriteStream,
        encoding: str = DEFAULT_ENCODING,
        buffer_size: int = DEFAULT_BUFFER_SIZE,
    ) -> None:
        self._stream = stream
        self._encoding = encoding
        self._buffer_size = buffer_size

    async def write(self, data: str) -> int:
        """Encode in chunks and write. Returns number of characters written."""
        for offset in range(0, len(data), self._buffer_size):
            chunk = data[offset : offset + self._buffer_size]
            await self._stream.write(chunk.encode(self._encoding))
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
        await self._stream.close()

    async def __aenter__(self) -> TextWriteStream:
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.close()
