"""Async readable streams — byte and text layers."""

from __future__ import annotations

import asyncio
import codecs
import os
from collections.abc import AsyncIterator

from shish._defaults import DEFAULT_ENCODING
from shish.fd import Fd

DEFAULT_READ_SIZE = 65536


class RawReader:
    """Unbuffered async fd reader. Owns the fd.

    Uses os.read + loop.add_reader directly. read() performs a single
    os.read call — if the fd would block, it suspends on add_reader
    first. Returns the bytes read (may be short), or empty bytes on EOF.
    """

    def __init__(self, owned_fd: Fd) -> None:
        self._fd = owned_fd
        self._loop = asyncio.get_running_loop()
        os.set_blocking(owned_fd.fd, False)

    async def read(self, size: int) -> bytes:
        """Read once. Returns bytes read, empty = EOF."""
        while True:
            try:
                return os.read(self._fd.fd, size)
            except BlockingIOError:
                await self._readable()

    @property
    def closed(self) -> bool:
        """Whether the fd is closed."""
        return self._fd.closed

    def close(self) -> None:
        """Close the fd."""
        self._fd.close()

    async def _readable(self) -> None:
        """Suspend until the fd is readable."""
        future: asyncio.Future[None] = self._loop.create_future()
        self._loop.add_reader(self._fd.fd, future.set_result, None)
        try:
            await future
        finally:
            self._loop.remove_reader(self._fd.fd)


class ByteReadStream:
    """Async readable byte stream with userspace buffering.

    Mirrors open(mode="rb"). Owns the raw reader — closing the stream
    closes the underlying fd.

    Wraps a RawReader with a bytearray buffer (modeled after CPython's
    _pyio.BufferedReader). read(n) loops filling from the underlying reader until n
    bytes are buffered or EOF is reached, matching BufferedReader.read(n) semantics.
    """

    def __init__(self, raw: RawReader, buffer_size: int = DEFAULT_READ_SIZE) -> None:
        self._reader = raw
        self._buf = bytearray()
        self._eof = False
        self._buffer_size = buffer_size
        self._lock = asyncio.Lock()

    @classmethod
    def from_fd(cls, owned_fd: Fd, buffer_size: int = DEFAULT_READ_SIZE) -> ByteReadStream:
        """Create a ByteReadStream from a file descriptor."""
        return cls(RawReader(owned_fd), buffer_size)

    async def read(self, size: int = -1) -> bytes:
        """Read up to size bytes. -1 = read all. Empty bytes = EOF."""
        if size == 0:
            return b""
        async with self._lock:
            if size < 0:
                return await self._read_all()
            while len(self._buf) < size and not self._eof:
                await self._fill(size - len(self._buf))
            result = bytes(self._buf[:size])
            del self._buf[:size]
            return result

    async def readline(self) -> bytes:
        """Read one line including trailing newline. Empty bytes = EOF."""
        async with self._lock:
            while True:
                pos = self._buf.find(b"\n")
                if pos >= 0:
                    result = bytes(self._buf[: pos + 1])
                    del self._buf[: pos + 1]
                    return result
                if self._eof:
                    result = bytes(self._buf)
                    self._buf.clear()
                    return result
                await self._fill()

    async def readlines(self) -> list[bytes]:
        """Read all remaining lines until EOF."""
        return [line async for line in self]

    @property
    def closed(self) -> bool:
        """Whether the stream is closed."""
        return self._reader.closed

    def close_fd(self) -> None:
        """Close the fd without waiting for pending reads."""
        self._reader.close()

    async def close(self) -> None:
        """Close the fd."""
        async with self._lock:
            self._reader.close()

    async def _read_all(self) -> bytes:
        """Read until EOF, return everything."""
        while not self._eof:
            await self._fill()
        result = bytes(self._buf)
        self._buf.clear()
        return result

    async def _fill(self, size: int = -1) -> None:
        """Read once from fd into buffer. Sets _eof on EOF."""
        if self._eof:
            return
        read_size = self._buffer_size if size < 0 else max(size, self._buffer_size)
        chunk = await self._reader.read(read_size)
        if chunk:
            self._buf.extend(chunk)
        else:
            self._eof = True

    async def __aenter__(self) -> ByteReadStream:
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

    async def __aiter__(self) -> AsyncIterator[bytes]:
        """Yield lines with trailing newline. Stops at EOF."""
        while True:
            line = await self.readline()
            if not line:
                return
            yield line


class TextReadStream:
    """Async readable text stream. Decodes bytes from a ByteReadStream.

    Mirrors open(mode="r"). Owns the byte stream — closing this closes it.

    read(n) counts characters, not bytes, matching TextIOWrapper.
    Uses codecs.getincrementaldecoder to handle multi-byte characters
    (e.g. UTF-8 e, U+1F389) that may be split across underlying byte reads.
    The decoder carries state between _fill() calls, so a 4-byte emoji
    arriving as two 2-byte chunks decodes correctly.
    """

    def __init__(
        self,
        stream: ByteReadStream,
        encoding: str = DEFAULT_ENCODING,
        buffer_size: int = DEFAULT_READ_SIZE,
    ) -> None:
        self._stream = stream
        self._decoder = codecs.getincrementaldecoder(encoding)()
        self._buf = ""
        self._buf_start = 0
        self._eof = False
        self._buffer_size = buffer_size
        self._lock = asyncio.Lock()

    @classmethod
    def from_bytes(
        cls,
        stream: ByteReadStream,
        encoding: str = DEFAULT_ENCODING,
        buffer_size: int = DEFAULT_READ_SIZE,
    ) -> TextReadStream:
        """Create a TextReadStream from a ByteReadStream."""
        return cls(stream, encoding, buffer_size)

    @classmethod
    def from_fd(
        cls,
        owned_fd: Fd,
        encoding: str = DEFAULT_ENCODING,
        buffer_size: int = DEFAULT_READ_SIZE,
    ) -> TextReadStream:
        """Create a TextReadStream from a file descriptor."""
        return cls(ByteReadStream.from_fd(owned_fd, buffer_size), encoding, buffer_size)

    @property
    def buffered(self) -> int:
        """Characters available in the buffer (not yet returned to caller)."""
        return len(self._buf) - self._buf_start

    def _consume(self, count: int) -> str:
        """Return count chars from the buffer and advance past them."""
        result = self._buf[self._buf_start : self._buf_start + count]
        self._buf_start += count
        # Compact when consumed prefix exceeds unconsumed suffix
        if self._buf_start > self.buffered:
            self._buf = self._buf[self._buf_start :]
            self._buf_start = 0
        return result

    async def read(self, size: int = -1) -> str:
        """Read up to size characters. -1 = read all. Empty string = EOF."""
        if size == 0:
            return ""
        async with self._lock:
            if size < 0:
                return await self._read_all()
            while self.buffered < size and not self._eof:
                await self._fill()
            return self._consume(min(size, self.buffered))

    async def readline(self) -> str:
        """Read one decoded line including trailing newline. Empty string = EOF."""
        async with self._lock:
            while True:
                pos = self._buf.find("\n", self._buf_start)
                if pos >= 0:
                    return self._consume(pos - self._buf_start + 1)
                if self._eof:
                    return self._consume(self.buffered)
                await self._fill()

    async def readlines(self) -> list[str]:
        """Read all remaining lines until EOF."""
        return [line async for line in self]

    @property
    def closed(self) -> bool:
        """Whether the stream is closed."""
        return self._stream.closed

    async def close(self) -> None:
        """Close the underlying byte stream."""
        async with self._lock:
            await self._stream.close()

    async def _read_all(self) -> str:
        """Read until EOF, return everything decoded."""
        while not self._eof:
            await self._fill()
        return self._consume(self.buffered)

    async def _fill(self) -> None:
        """Read bytes from underlying stream, decode, append to text buffer."""
        if self._eof:
            return
        chunk = await self._stream.read(self._buffer_size)
        if chunk:
            self._buf += self._decoder.decode(chunk)
        else:
            self._eof = True
            self._buf += self._decoder.decode(b"", final=True)

    async def __aenter__(self) -> TextReadStream:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        *_args: object,
    ) -> None:
        if exc_type is not None and not issubclass(exc_type, Exception):
            self._stream.close_fd()
        else:
            await self.close()

    async def __aiter__(self) -> AsyncIterator[str]:
        """Yield decoded lines with trailing newline. Stops at EOF."""
        while True:
            line = await self.readline()
            if not line:
                return
            yield line
