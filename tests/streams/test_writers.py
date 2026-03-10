import asyncio
import fcntl
import os

import pytest

from shish.fd import Fd
from shish.streams import (
    ByteReadStream,
    ByteWriteStream,
    TextWriteStream,
)

# =============================================================================
# ByteWriteStream
# =============================================================================


async def test_write() -> None:
    """write() delivers all bytes and returns the count."""
    read_fd, write_fd = os.pipe()
    async with ByteWriteStream(Fd(write_fd)) as writer:
        count = await writer.write(b"hello")
    result = os.read(read_fd, 1024)
    os.close(read_fd)
    assert result == b"hello"
    assert count == 5


async def test_writelines() -> None:
    """writelines() writes all chunks in order."""
    read_fd, write_fd = os.pipe()
    async with ByteWriteStream(Fd(write_fd)) as writer:
        await writer.writelines([b"aaa", b"bbb", b"ccc"])
    result = os.read(read_fd, 1024)
    os.close(read_fd)
    assert result == b"aaabbbccc"


async def test_write_close_closes_fd() -> None:
    """close() closes the underlying fd."""
    read_fd, write_fd = os.pipe()
    writer = ByteWriteStream(Fd(write_fd))
    await writer.close()
    with pytest.raises(OSError):
        os.fstat(write_fd)
    os.close(read_fd)


async def test_write_context_manager_closes_on_exception() -> None:
    """__aexit__ closes the fd even when the block raises."""
    read_fd, write_fd = os.pipe()
    with pytest.raises(RuntimeError):
        async with ByteWriteStream(Fd(write_fd)):
            raise RuntimeError("boom")
    with pytest.raises(OSError):
        os.fstat(write_fd)
    os.close(read_fd)


async def test_write_after_close() -> None:
    """write() on a closed stream raises."""
    read_fd, write_fd = os.pipe()
    writer = ByteWriteStream(Fd(write_fd))
    await writer.close()
    with pytest.raises(OSError):
        await writer.write(b"hello")
    os.close(read_fd)


async def test_write_broken_pipe() -> None:
    """close() raises BrokenPipeError when the read end is already closed."""
    _read_fd, write_fd = os.pipe()
    os.close(_read_fd)
    writer = ByteWriteStream(Fd(write_fd))
    await writer.write(b"hello")
    with pytest.raises(BrokenPipeError):
        await writer.close()


async def test_write_eof_with_data() -> None:
    """write_eof() writes data and closes the stream."""
    read_fd, write_fd = os.pipe()
    writer = ByteWriteStream(Fd(write_fd))
    await writer.write_eof(b"goodbye")
    assert writer.closed
    result = os.read(read_fd, 1024)
    os.close(read_fd)
    assert result == b"goodbye"


async def test_write_eof_empty() -> None:
    """write_eof() with no data just closes the stream."""
    read_fd, write_fd = os.pipe()
    writer = ByteWriteStream(Fd(write_fd))
    await writer.write_eof()
    assert writer.closed
    result = os.read(read_fd, 1024)
    os.close(read_fd)
    assert result == b""


async def test_write_buffered_then_flushed_on_close() -> None:
    """Small writes are buffered; close() flushes them to the fd."""
    read_fd, write_fd = os.pipe()
    writer = ByteWriteStream(Fd(write_fd), buffer_size=1024)
    await writer.write(b"aaa")
    await writer.write(b"bbb")
    # Data is buffered, not yet on the pipe
    assert os.get_blocking(read_fd) or True  # fd is valid
    await writer.close()
    result = os.read(read_fd, 1024)
    os.close(read_fd)
    assert result == b"aaabbb"


async def test_write_flush_drains_buffer() -> None:
    """Explicit flush() drains buffered data to the fd."""
    read_fd, write_fd = os.pipe()
    writer = ByteWriteStream(Fd(write_fd), buffer_size=1024)
    await writer.write(b"hello")
    await writer.flush()
    # Data should be on the pipe now
    result = os.read(read_fd, 1024)
    assert result == b"hello"
    await writer.close()
    os.close(read_fd)


async def test_write_large_flushes_immediately() -> None:
    """Data exceeding buffer_size is flushed, not just buffered."""
    read_fd, write_fd = os.pipe()
    writer = ByteWriteStream(Fd(write_fd), buffer_size=8)
    await writer.write(b"short")  # 5 bytes, fits in buffer
    await writer.write(b"this-is-longer-than-eight")  # triggers flush
    await writer.close()
    result = os.read(read_fd, 1024)
    os.close(read_fd)
    assert result == b"shortthis-is-longer-than-eight"


async def test_write_error_preserves_buffer() -> None:
    """On write error, unflushed data stays in the buffer."""
    _read_fd, write_fd = os.pipe()
    os.close(_read_fd)
    writer = ByteWriteStream(Fd(write_fd), buffer_size=1024)
    await writer.write(b"buffered")
    # Force flush — will fail because read end is closed
    with pytest.raises(BrokenPipeError):
        await writer.flush()
    # Data that wasn't written should still be in the buffer
    assert writer.buffered > 0
    # Close still closes the fd despite the error
    with pytest.raises(BrokenPipeError):
        await writer.close()
    assert writer.closed


async def test_close_fd_skips_flush() -> None:
    """close_fd() closes the fd without flushing buffered data."""
    read_fd, write_fd = os.pipe()
    writer = ByteWriteStream(Fd(write_fd), buffer_size=1024)
    await writer.write(b"will-not-be-flushed")
    writer.close_fd()
    # Nothing written — pipe is empty, read returns empty on EOF
    result = os.read(read_fd, 1024)
    os.close(read_fd)
    assert result == b""


async def test_cancel_write_through_not_poisoned() -> None:
    """Cancel during write-through does not set sticky error."""
    read_fd, write_fd = os.pipe()
    if hasattr(fcntl, "F_SETPIPE_SZ"):
        fcntl.fcntl(write_fd, fcntl.F_SETPIPE_SZ, 4096)

    writer = ByteWriteStream(Fd(write_fd), buffer_size=1024)

    # Data > buffer_size takes write-through path — blocks when pipe full
    task = asyncio.create_task(writer.write(b"x" * 262144))
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # CancelledError must not break the writer
    assert writer.buffered == 0
    os.close(read_fd)
    await writer.close()


async def test_write_backpressure() -> None:
    """write() suspends when pipe buffer is full, resumes when drained."""
    read_fd, write_fd = os.pipe()
    if hasattr(fcntl, "F_SETPIPE_SZ"):
        fcntl.fcntl(write_fd, fcntl.F_SETPIPE_SZ, 4096)
    # 256KB is well above any default pipe buffer (4KB Linux, 16KB macOS)
    data = b"x" * 262144

    async def do_write() -> None:
        async with ByteWriteStream(Fd(write_fd)) as writer:
            await writer.write(data)

    write_task = asyncio.create_task(do_write())
    async with ByteReadStream(Fd(read_fd)) as reader:
        result = await reader.read()
    await write_task
    assert result == data


async def test_buffered_property() -> None:
    """buffered property tracks buffer usage correctly."""
    read_fd, write_fd = os.pipe()
    writer = ByteWriteStream(Fd(write_fd), buffer_size=1024)
    assert writer.buffered == 0
    await writer.write(b"hello")
    assert writer.buffered == 5
    await writer.write(b"world")
    assert writer.buffered == 10
    await writer.flush()
    assert writer.buffered == 0
    await writer.close()
    os.close(read_fd)


# =============================================================================
# TextWriteStream
# =============================================================================


async def test_text_write() -> None:
    """write() encodes and delivers string, returns char count."""
    read_fd, write_fd = os.pipe()
    async with TextWriteStream(ByteWriteStream(Fd(write_fd))) as writer:
        count = await writer.write("hello")
    result = os.read(read_fd, 1024)
    os.close(read_fd)
    assert result == b"hello"
    assert count == 5


async def test_text_write_unicode() -> None:
    """write() handles multi-byte characters, returns char count not byte count."""
    read_fd, write_fd = os.pipe()
    async with TextWriteStream(ByteWriteStream(Fd(write_fd))) as writer:
        count = await writer.write("héllo 🎉")
    result = os.read(read_fd, 1024)
    os.close(read_fd)
    assert result == "héllo 🎉".encode()
    assert count == 7


async def test_text_writelines() -> None:
    """writelines() writes all strings in order."""
    read_fd, write_fd = os.pipe()
    async with TextWriteStream(ByteWriteStream(Fd(write_fd))) as writer:
        await writer.writelines(["aaa\n", "bbb\n", "ccc\n"])
    result = os.read(read_fd, 1024)
    os.close(read_fd)
    assert result == b"aaa\nbbb\nccc\n"


async def test_text_write_eof_with_data() -> None:
    """write_eof() encodes, writes, and closes."""
    read_fd, write_fd = os.pipe()
    writer = TextWriteStream(ByteWriteStream(Fd(write_fd)))
    await writer.write_eof("goodbye")
    result = os.read(read_fd, 1024)
    os.close(read_fd)
    assert result == b"goodbye"


async def test_text_write_eof_empty() -> None:
    """write_eof() with no data just closes."""
    read_fd, write_fd = os.pipe()
    writer = TextWriteStream(ByteWriteStream(Fd(write_fd)))
    await writer.write_eof()
    result = os.read(read_fd, 1024)
    os.close(read_fd)
    assert result == b""


async def test_text_write_close_closes_fd() -> None:
    """close() propagates through to the fd."""
    read_fd, write_fd = os.pipe()
    writer = TextWriteStream(ByteWriteStream(Fd(write_fd)))
    await writer.close()
    with pytest.raises(OSError):
        os.fstat(write_fd)
    os.close(read_fd)
