import asyncio
import fcntl
import os

import pytest

from shish.fd import Fd
from shish.streams import RawReader, RawWriter

# =============================================================================
# RawReader
# =============================================================================


async def test_raw_read(read_fd: Fd, write_fd: Fd) -> None:
    """read() returns available bytes (may be short)."""
    os.write(write_fd.fd, b"hello")
    reader = RawReader(read_fd)
    result = await reader.read(1024)
    assert result == b"hello"
    reader.close()


async def test_raw_read_eof(read_fd: Fd, write_fd: Fd) -> None:
    """read() returns empty bytes on EOF."""
    write_fd.close()
    reader = RawReader(read_fd)
    result = await reader.read(1024)
    assert result == b""
    reader.close()


async def test_raw_read_short(read_fd: Fd, write_fd: Fd) -> None:
    """read() may return fewer bytes than requested."""
    os.write(write_fd.fd, b"hi")
    reader = RawReader(read_fd)
    result = await reader.read(1024)
    assert result == b"hi"
    assert len(result) < 1024
    reader.close()


async def test_raw_read_suspends_on_empty_pipe(read_fd: Fd, write_fd: Fd) -> None:
    """read() suspends when pipe is empty, resumes when data arrives."""
    reader = RawReader(read_fd)

    async def delayed_write() -> None:
        await asyncio.sleep(0.01)
        os.write(write_fd.fd, b"delayed")

    asyncio.create_task(delayed_write())
    result = await reader.read(1024)
    assert result == b"delayed"
    reader.close()


async def test_raw_read_close(write_fd: Fd, read_fd: Fd) -> None:
    """close() closes the underlying fd."""
    raw = read_fd.fd
    reader = RawReader(read_fd)
    assert not reader.closed
    reader.close()
    assert reader.closed
    with pytest.raises(OSError):
        os.fstat(raw)


async def test_raw_read_close_idempotent(read_fd: Fd) -> None:
    """close() can be called multiple times."""
    reader = RawReader(read_fd)
    reader.close()
    reader.close()  # no error
    assert reader.closed


# =============================================================================
# RawWriter
# =============================================================================


async def test_raw_write(read_fd: Fd, write_fd: Fd) -> None:
    """write() writes data and returns byte count."""
    writer = RawWriter(write_fd)
    count = await writer.write(b"hello")
    assert count == 5
    result = os.read(read_fd.fd, 1024)
    assert result == b"hello"
    writer.close()


async def test_raw_write_empty(write_fd: Fd) -> None:
    """write() with empty data returns 0 without writing."""
    writer = RawWriter(write_fd)
    count = await writer.write(b"")
    assert count == 0
    writer.close()


async def test_raw_write_suspends_on_full_pipe(read_fd: Fd, write_fd: Fd) -> None:
    """write() suspends when pipe is full, resumes when drained."""
    if hasattr(fcntl, "F_SETPIPE_SZ"):
        fcntl.fcntl(write_fd.fd, fcntl.F_SETPIPE_SZ, 4096)

    writer = RawWriter(write_fd)

    # Fill the pipe with blocking writes until it's full
    os.set_blocking(write_fd.fd, False)
    try:
        while True:
            os.write(write_fd.fd, b"x" * 4096)
    except BlockingIOError:
        pass

    # Next write should suspend — pipe is full
    task = asyncio.create_task(writer.write(b"more"))
    await asyncio.sleep(0.01)
    assert not task.done()

    # Drain some data to unblock
    os.read(read_fd.fd, 4096)
    count = await task
    assert count == 4
    writer.close()


async def test_raw_write_close(write_fd: Fd) -> None:
    """close() closes the underlying fd."""
    raw = write_fd.fd
    writer = RawWriter(write_fd)
    assert not writer.closed
    writer.close()
    assert writer.closed
    with pytest.raises(OSError):
        os.fstat(raw)


async def test_raw_write_close_idempotent(write_fd: Fd) -> None:
    """close() can be called multiple times."""
    writer = RawWriter(write_fd)
    writer.close()
    writer.close()  # no error
    assert writer.closed


async def test_raw_write_broken_pipe(read_fd: Fd, write_fd: Fd) -> None:
    """write() raises BrokenPipeError when read end is closed."""
    read_fd.close()
    writer = RawWriter(write_fd)
    with pytest.raises(BrokenPipeError):
        await writer.write(b"hello")
    writer.close()


async def test_raw_write_memoryview(read_fd: Fd, write_fd: Fd) -> None:
    """write() accepts any Buffer (memoryview, bytearray, etc)."""
    writer = RawWriter(write_fd)
    data = bytearray(b"hello")
    count = await writer.write(memoryview(data))
    assert count == 5
    result = os.read(read_fd.fd, 1024)
    assert result == b"hello"
    writer.close()
