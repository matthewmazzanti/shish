"""Async readable streams — byte and text layers."""

from __future__ import annotations

import asyncio
import codecs
import os
from collections.abc import AsyncIterator

from shish._defaults import DEFAULT_ENCODING
from shish.fd import Fd


class ByteReadStream:
    """Async readable byte stream backed by a non-blocking fd.

    Mirrors open(mode="rb"). Owns the fd — closing the stream closes it.

    Uses os.read + loop.add_reader directly. No asyncio StreamReader —
    just a bytearray buffer filled one syscall at a time. _fill() does
    a single non-blocking read; if EAGAIN, it suspends on add_reader
    until the fd is readable.

    read(n) returns whatever is in the buffer (up to n), or waits for
    one fill if empty. It does NOT loop to accumulate n bytes — that
    matches BufferedReader.read(n) on pipes, where a short read is
    normal when data arrives in chunks.
    """

    def __init__(self, owned_fd: Fd, buffer_size: int = 65536) -> None:
        self._fd = owned_fd
        self._loop = asyncio.get_running_loop()
        self._buf = bytearray()
        self._eof = False
        self._buffer_size = buffer_size
        os.set_blocking(owned_fd.fd, False)

    async def read(self, size: int = -1) -> bytes:
        """Read up to size bytes. -1 = read all. Empty bytes = EOF."""
        if size < 0:
            return await self._read_all()
        if size == 0:
            return b""
        if not self._buf:
            await self._fill()
        result = bytes(self._buf[:size])
        del self._buf[:size]
        return result

    async def readline(self) -> bytes:
        """Read one line including trailing newline. Empty bytes = EOF."""
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

    async def close(self) -> None:
        """Close the fd."""
        self._fd.close()

    async def _read_all(self) -> bytes:
        """Read until EOF, return everything."""
        while not self._eof:
            await self._fill()
        result = bytes(self._buf)
        self._buf.clear()
        return result

    async def _fill(self) -> None:
        """Read once from fd into buffer. Sets _eof on EOF."""
        if self._eof:
            return
        while True:
            try:
                chunk = os.read(self._fd.fd, self._buffer_size)
                if chunk:
                    self._buf.extend(chunk)
                else:
                    self._eof = True
                return
            except BlockingIOError:
                await self._readable()

    async def _readable(self) -> None:
        """Suspend until the fd is readable."""
        future: asyncio.Future[None] = self._loop.create_future()
        self._loop.add_reader(self._fd.fd, future.set_result, None)
        try:
            await future
        finally:
            self._loop.remove_reader(self._fd.fd)

    async def __aenter__(self) -> ByteReadStream:
        return self

    async def __aexit__(self, *_args: object) -> None:
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
        buffer_size: int = 65536,
    ) -> None:
        self._stream = stream
        self._decoder = codecs.getincrementaldecoder(encoding)()
        self._buf = ""
        self._eof = False
        self._buffer_size = buffer_size

    async def read(self, size: int = -1) -> str:
        """Read up to size characters. -1 = read all. Empty string = EOF."""
        if size < 0:
            return await self._read_all()
        if size == 0:
            return ""
        while len(self._buf) < size and not self._eof:
            await self._fill()
        result = self._buf[:size]
        self._buf = self._buf[size:]
        return result

    async def readline(self) -> str:
        """Read one decoded line including trailing newline. Empty string = EOF."""
        while True:
            pos = self._buf.find("\n")
            if pos >= 0:
                result = self._buf[: pos + 1]
                self._buf = self._buf[pos + 1 :]
                return result
            if self._eof:
                result = self._buf
                self._buf = ""
                return result
            await self._fill()

    async def readlines(self) -> list[str]:
        """Read all remaining lines until EOF."""
        return [line async for line in self]

    async def close(self) -> None:
        """Close the underlying byte stream."""
        await self._stream.close()

    async def _read_all(self) -> str:
        """Read until EOF, return everything decoded."""
        while not self._eof:
            await self._fill()
        result = self._buf
        self._buf = ""
        return result

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

    async def __aexit__(self, *_args: object) -> None:
        await self.close()

    async def __aiter__(self) -> AsyncIterator[str]:
        """Yield decoded lines with trailing newline. Stops at EOF."""
        while True:
            line = await self.readline()
            if not line:
                return
            yield line
