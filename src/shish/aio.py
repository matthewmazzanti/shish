"""Async IO utilities for non-blocking pipe operations."""

from __future__ import annotations

import asyncio
import codecs
import contextlib
import os
from collections.abc import Generator


def close_fd(fd: int) -> None:
    """Close fd, suppressing errors if already closed."""
    with contextlib.suppress(OSError):
        os.close(fd)


async def async_write(fd: int, data: str | bytes) -> None:
    """Write data to fd asynchronously, then close.

    Streams data to a non-blocking fd in fixed-size chunks. When the pipe buffer
    fills (BlockingIOError), yields to the event loop, allowing other coroutines
    to run. This enables multiple concurrent writes to interleave properly.

    Closes the fd when complete, signaling EOF to readers.
    """
    # 64K chunks: matches default pipe buffer (64K) and shutil.COPY_BUFSIZE.
    # Larger chunks amortize Python call overhead; smaller chunks waste syscalls.
    for chunk in iterencode(data, chunk_size=65536):
        written = 0
        # Handle partial writes (os.write may write less than requested)
        while written < len(chunk):
            try:
                written += os.write(fd, chunk[written:])
            except BlockingIOError:
                # Pipe buffer full - yield to event loop until writable
                await wait_writable(fd)
    os.close(fd)


async def wait_writable(fd: int) -> None:
    """Suspend until fd is writable.

    Registers fd with the event loop's writer callback. When the fd becomes
    writable (pipe buffer has space), the callback fires and completes the
    future, resuming this coroutine.
    """
    loop = asyncio.get_running_loop()
    fut: asyncio.Future[None] = loop.create_future()
    loop.add_writer(fd, fut.set_result, None)
    try:
        await fut
    finally:
        loop.remove_writer(fd)


async def wait_readable(fd: int) -> None:
    """Suspend until fd is readable."""
    loop = asyncio.get_running_loop()
    fut: asyncio.Future[None] = loop.create_future()
    loop.add_reader(fd, fut.set_result, None)
    try:
        await fut
    finally:
        loop.remove_reader(fd)


async def async_read(fd: int) -> bytes:
    """Read all data from fd asynchronously, then close.

    Reads in 64K chunks, yielding to the event loop when no data is available.
    Closes the fd when EOF is reached.
    """
    os.set_blocking(fd, False)
    chunks: list[bytes] = []
    while True:
        try:
            chunk = os.read(fd, 65536)
            if not chunk:
                break
            chunks.append(chunk)
        except BlockingIOError:
            await wait_readable(fd)
    os.close(fd)
    return b"".join(chunks)


def iterencode(data: str | bytes, chunk_size: int) -> Generator[bytes]:
    """Yield fixed-size byte chunks from string or bytes.

    For bytes: zero-copy slicing into chunk_size pieces.
    For str: incremental UTF-8 encoding with internal buffering to produce
    exact chunk_size output (except final chunk). Avoids 2x memory allocation
    that str.encode() would require for large strings.

    Buffer overhead: ~4x chunk_size peak (256KB at 64K chunks) due to
    variable-length UTF-8 encoding (1-4 bytes per character).
    """
    # Bytes: simple slicing, no encoding needed
    if isinstance(data, bytes):
        for i in range(0, len(data), chunk_size):
            yield data[i : i + chunk_size]
        return

    # Strings: incremental UTF-8 encoding with output buffering
    encoder = codecs.getincrementalencoder("utf-8")()
    buffer = bytearray()

    # Process chunk_size characters at a time. Each char encodes to 1-4 bytes,
    # so buffer may grow up to 4x chunk_size before we drain it.
    for i in range(0, len(data), chunk_size):
        buffer.extend(encoder.encode(data[i : i + chunk_size]))
        # Drain buffer in chunk_size pieces for consistent output sizing
        while len(buffer) >= chunk_size:
            yield bytes(buffer[:chunk_size])
            del buffer[:chunk_size]  # In-place removal, avoids reallocation

    # Flush encoder state (handles any trailing surrogate pairs)
    buffer.extend(encoder.encode("", final=True))
    if buffer:
        yield bytes(buffer)
