"""Async IO streams for subprocess pipes.

Design goal: feel like Python's open(), but async. If open() has a
method, we have the async equivalent. If open() doesn't, we don't.

Two layers, matching the text/binary x read/write matrix:

    Byte streams (ByteReadStream, ByteWriteStream)
        Non-blocking fd + event loop. No asyncio transports or
        protocols — just os.read/os.write with add_reader/add_writer
        for backpressure. These own the fd.

    Text streams (TextReadStream, TextWriteStream)
        Encoding/decoding over a byte stream. TextReadStream uses an
        incremental decoder to handle multi-byte characters split
        across read boundaries. These own their byte stream.

Ownership chain: text stream → byte stream → fd. Closing any layer
closes everything below it. Context managers guarantee cleanup.

Key invariants matching open():
    read()          Read all until EOF (like f.read())
    read(n)         Up to n bytes/chars (like f.read(n) on pipes)
    readline()      One line including \\n, empty = EOF
    readlines()     All remaining lines
    write(data)     All-or-error, returns count
    writelines()    Write iterable, returns None
    close()         Release the fd
    async with      Guarantees close on exit
    async for       Yields lines
"""

from __future__ import annotations

import asyncio
import codecs
import os
from collections import abc

from shish._defaults import DEFAULT_ENCODING
from shish.fd import Fd

# =============================================================================
# Byte streams — non-blocking fd + event loop
# =============================================================================


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

    def close(self) -> None:
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
        self.close()

    async def __aiter__(self) -> abc.AsyncIterator[bytes]:
        """Yield lines with trailing newline. Stops at EOF."""
        while True:
            line = await self.readline()
            if not line:
                return
            yield line


class ByteWriteStream:
    """Async writable byte stream backed by a non-blocking fd.

    Mirrors open(mode="wb"). Owns the fd — closing the stream closes it.

    Uses os.write + loop.add_writer directly. No asyncio StreamWriter —
    write() loops with a memoryview, advancing past each partial
    os.write until all data is delivered. If EAGAIN, it suspends on
    add_writer until the fd is writable. This makes write()
    all-or-error: it returns len(data) or raises, never a short count.
    """

    def __init__(self, owned_fd: Fd) -> None:
        self._fd = owned_fd
        self._loop = asyncio.get_running_loop()
        os.set_blocking(owned_fd.fd, False)

    async def write(self, data: abc.Buffer) -> int:
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

    async def writelines(self, data: abc.Iterable[abc.Buffer]) -> None:
        """Write an iterable of byte chunks."""
        for chunk in data:
            await self.write(chunk)

    async def write_eof(self, data: abc.Buffer = b"") -> None:
        """Write final data and close. Signals EOF to the reader."""
        if data:
            await self.write(data)
        self.close()

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

    async def __aenter__(self) -> ByteWriteStream:
        return self

    async def __aexit__(self, *_args: object) -> None:
        self.close()


# =============================================================================
# Text streams — encoding/decoding over byte streams
# =============================================================================


class TextReadStream:
    """Async readable text stream. Decodes bytes from a ByteReadStream.

    Mirrors open(mode="r"). Owns the byte stream — closing this closes it.

    read(n) counts characters, not bytes, matching TextIOWrapper.
    Uses codecs.getincrementaldecoder to handle multi-byte characters
    (e.g. UTF-8 é, 🎉) that may be split across underlying byte reads.
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

    def close(self) -> None:
        """Close the underlying byte stream."""
        self._stream.close()

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
        self.close()

    async def __aiter__(self) -> abc.AsyncIterator[str]:
        """Yield decoded lines with trailing newline. Stops at EOF."""
        while True:
            line = await self.readline()
            if not line:
                return
            yield line


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
        buffer_size: int = 65536,
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

    async def writelines(self, lines: abc.Iterable[str]) -> None:
        """Write an iterable of strings."""
        for line in lines:
            await self.write(line)

    async def write_eof(self, data: str = "") -> None:
        """Write final data and close. Signals EOF to the reader."""
        if data:
            await self.write(data)
        self.close()

    def close(self) -> None:
        """Close the underlying byte stream."""
        self._stream.close()

    async def __aenter__(self) -> TextWriteStream:
        return self

    async def __aexit__(self, *_args: object) -> None:
        self.close()
