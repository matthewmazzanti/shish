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


async def test_write(read_fd: Fd, write_fd: Fd) -> None:
    """write() delivers all bytes and returns the count."""
    async with ByteWriteStream.from_fd(write_fd) as writer:
        count = await writer.write(b"hello")
    result = os.read(read_fd.fd, 1024)
    assert result == b"hello"
    assert count == 5


async def test_writelines(read_fd: Fd, write_fd: Fd) -> None:
    """writelines() writes all chunks in order."""
    async with ByteWriteStream.from_fd(write_fd) as writer:
        await writer.writelines([b"aaa", b"bbb", b"ccc"])
    result = os.read(read_fd.fd, 1024)
    assert result == b"aaabbbccc"


async def test_write_close_closes_fd(write_fd: Fd) -> None:
    """close() closes the underlying fd."""
    writer = ByteWriteStream.from_fd(write_fd)
    await writer.close()
    with pytest.raises(OSError):
        os.fstat(write_fd.fd)


async def test_write_context_manager_closes_on_exception(
    write_fd: Fd,
) -> None:
    """__aexit__ closes the fd even when the block raises."""
    with pytest.raises(RuntimeError):
        async with ByteWriteStream.from_fd(write_fd):
            raise RuntimeError("boom")
    with pytest.raises(OSError):
        os.fstat(write_fd.fd)


async def test_write_aexit_base_exception_skips_flush(
    read_fd: Fd,
    write_fd: Fd,
) -> None:
    """__aexit__ calls close_fd (no flush) on BaseException."""
    with pytest.raises(KeyboardInterrupt):
        async with ByteWriteStream.from_fd(write_fd, buffer_size=1024) as writer:
            await writer.write(b"buffered-data")
            raise KeyboardInterrupt

    # Fd closed but data was not flushed
    result = os.read(read_fd.fd, 1024)
    assert result == b""


async def test_write_after_close(write_fd: Fd) -> None:
    """write() on a closed stream raises."""
    writer = ByteWriteStream.from_fd(write_fd)
    await writer.close()
    with pytest.raises(OSError):
        await writer.write(b"hello")


async def test_write_broken_pipe(read_fd: Fd, write_fd: Fd) -> None:
    """close() raises BrokenPipeError when the read end is already closed."""
    read_fd.close()
    writer = ByteWriteStream.from_fd(write_fd)
    await writer.write(b"hello")
    with pytest.raises(BrokenPipeError):
        await writer.close()


async def test_write_eof_with_data(read_fd: Fd, write_fd: Fd) -> None:
    """write_eof() writes data and closes the stream."""
    writer = ByteWriteStream.from_fd(write_fd)
    await writer.write_eof(b"goodbye")
    assert writer.closed
    result = os.read(read_fd.fd, 1024)
    assert result == b"goodbye"


async def test_write_eof_empty(read_fd: Fd, write_fd: Fd) -> None:
    """write_eof() with no data just closes the stream."""
    writer = ByteWriteStream.from_fd(write_fd)
    await writer.write_eof()
    assert writer.closed
    result = os.read(read_fd.fd, 1024)
    assert result == b""


async def test_write_buffered_then_flushed_on_close(read_fd: Fd, write_fd: Fd) -> None:
    """Small writes are buffered; close() flushes them to the fd."""
    writer = ByteWriteStream.from_fd(write_fd, buffer_size=1024)
    await writer.write(b"aaa")
    await writer.write(b"bbb")
    await writer.close()
    result = os.read(read_fd.fd, 1024)
    assert result == b"aaabbb"


async def test_write_flush_drains_buffer(read_fd: Fd, write_fd: Fd) -> None:
    """Explicit flush() drains buffered data to the fd."""
    writer = ByteWriteStream.from_fd(write_fd, buffer_size=1024)
    await writer.write(b"hello")
    await writer.flush()
    result = os.read(read_fd.fd, 1024)
    assert result == b"hello"
    await writer.close()


async def test_write_large_flushes_immediately(read_fd: Fd, write_fd: Fd) -> None:
    """Data exceeding buffer_size is flushed, not just buffered."""
    writer = ByteWriteStream.from_fd(write_fd, buffer_size=8)
    await writer.write(b"short")  # 5 bytes, fits in buffer
    await writer.write(b"this-is-longer-than-eight")  # triggers flush
    await writer.close()
    result = os.read(read_fd.fd, 1024)
    assert result == b"shortthis-is-longer-than-eight"


async def test_write_error_preserves_buffer(read_fd: Fd, write_fd: Fd) -> None:
    """On write error, unflushed data stays in the buffer."""
    read_fd.close()
    writer = ByteWriteStream.from_fd(write_fd, buffer_size=1024)
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


async def test_close_fd_skips_flush(read_fd: Fd, write_fd: Fd) -> None:
    """close_fd() closes the fd without flushing buffered data."""
    writer = ByteWriteStream.from_fd(write_fd, buffer_size=1024)
    await writer.write(b"will-not-be-flushed")
    writer.close_fd()
    # Nothing written — pipe is empty, read returns empty on EOF
    result = os.read(read_fd.fd, 1024)
    assert result == b""


async def test_cancel_write_through_not_poisoned(
    read_fd: Fd,
    write_fd: Fd,
) -> None:
    """Cancel during write-through does not set sticky error."""
    if hasattr(fcntl, "F_SETPIPE_SZ"):
        fcntl.fcntl(write_fd.fd, fcntl.F_SETPIPE_SZ, 4096)

    writer = ByteWriteStream.from_fd(write_fd, buffer_size=1024)

    # Data > buffer_size takes write-through path — blocks when pipe full
    task = asyncio.create_task(writer.write(b"x" * 262144))
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # CancelledError must not break the writer
    assert writer.buffered == 0
    read_fd.close()
    await writer.close()


async def test_write_backpressure(read_fd: Fd, write_fd: Fd) -> None:
    """write() suspends when pipe buffer is full, resumes when drained."""
    if hasattr(fcntl, "F_SETPIPE_SZ"):
        fcntl.fcntl(write_fd.fd, fcntl.F_SETPIPE_SZ, 4096)
    # 256KB is well above any default pipe buffer (4KB Linux, 16KB macOS)
    data = b"x" * 262144

    async def do_write() -> None:
        async with ByteWriteStream.from_fd(write_fd) as writer:
            await writer.write(data)

    write_task = asyncio.create_task(do_write())
    async with ByteReadStream.from_fd(read_fd) as reader:
        result = await reader.read()
    await write_task
    assert result == data


async def test_buffered_property(write_fd: Fd) -> None:
    """buffered property tracks buffer usage correctly."""
    writer = ByteWriteStream.from_fd(write_fd, buffer_size=1024)
    assert writer.buffered == 0
    await writer.write(b"hello")
    assert writer.buffered == 5
    await writer.write(b"world")
    assert writer.buffered == 10
    await writer.flush()
    assert writer.buffered == 0
    await writer.close()


async def test_write_exactly_buffer_size(read_fd: Fd, write_fd: Fd) -> None:
    """Data exactly equal to buffer_size is buffered, not write-through."""
    writer = ByteWriteStream.from_fd(write_fd, buffer_size=16)
    data = b"x" * 16
    await writer.write(data)
    assert writer.buffered == 16
    await writer.close()
    result = os.read(read_fd.fd, 1024)
    assert result == data


async def test_write_fills_buffer_exactly(read_fd: Fd, write_fd: Fd) -> None:
    """Multiple small writes that exactly fill the buffer stay buffered."""
    writer = ByteWriteStream.from_fd(write_fd, buffer_size=16)
    await writer.write(b"aaaaaaaa")  # 8 bytes
    assert writer.buffered == 8
    await writer.write(b"bbbbbbbb")  # 8 more = exactly 16
    assert writer.buffered == 16
    await writer.close()
    result = os.read(read_fd.fd, 1024)
    assert result == b"aaaaaaaabbbbbbbb"


async def test_write_overflow_flushes_then_buffers(
    read_fd: Fd,
    write_fd: Fd,
) -> None:
    """Write that won't fit flushes existing data, then buffers the new data."""
    writer = ByteWriteStream.from_fd(write_fd, buffer_size=16)
    await writer.write(b"aaaaaaaaaa")  # 10 bytes
    assert writer.buffered == 10
    # 10 bytes won't fit alongside 10 existing — flush first, then buffer
    await writer.write(b"bbbbbbbbbb")  # 10 bytes
    assert writer.buffered == 10  # old data flushed, new data buffered
    await writer.close()
    result = os.read(read_fd.fd, 1024)
    assert result == b"aaaaaaaaaabbbbbbbbbb"


# =============================================================================
# TextWriteStream
# =============================================================================


async def test_text_write(read_fd: Fd, write_fd: Fd) -> None:
    """write() encodes and delivers string, returns char count."""
    async with TextWriteStream.from_fd(write_fd) as writer:
        count = await writer.write("hello")
    result = os.read(read_fd.fd, 1024)
    assert result == b"hello"
    assert count == 5


async def test_text_write_unicode(read_fd: Fd, write_fd: Fd) -> None:
    """write() handles multi-byte characters, returns char count not byte count."""
    async with TextWriteStream.from_fd(write_fd) as writer:
        count = await writer.write("héllo 🎉")
    result = os.read(read_fd.fd, 1024)
    assert result == "héllo 🎉".encode()
    assert count == 7


async def test_text_writelines(read_fd: Fd, write_fd: Fd) -> None:
    """writelines() writes all strings in order."""
    async with TextWriteStream.from_fd(write_fd) as writer:
        await writer.writelines(["aaa\n", "bbb\n", "ccc\n"])
    result = os.read(read_fd.fd, 1024)
    assert result == b"aaa\nbbb\nccc\n"


async def test_text_write_eof_with_data(read_fd: Fd, write_fd: Fd) -> None:
    """write_eof() encodes, writes, and closes."""
    writer = TextWriteStream.from_fd(write_fd)
    await writer.write_eof("goodbye")
    result = os.read(read_fd.fd, 1024)
    assert result == b"goodbye"


async def test_text_write_eof_empty(read_fd: Fd, write_fd: Fd) -> None:
    """write_eof() with no data just closes."""
    writer = TextWriteStream.from_fd(write_fd)
    await writer.write_eof()
    result = os.read(read_fd.fd, 1024)
    assert result == b""


async def test_text_write_large_chunks(read_fd: Fd, write_fd: Fd) -> None:
    """Text larger than buffer_size // 4 is chunked and delivered correctly."""
    byte_writer = ByteWriteStream.from_fd(write_fd, buffer_size=32)
    # chunk_size = 32 // 4 = 8 chars. 26 chars requires 4 chunks.
    data = "abcdefghijklmnopqrstuvwxyz"
    async with TextWriteStream(byte_writer) as writer:
        count = await writer.write(data)
    result = os.read(read_fd.fd, 1024)
    assert result == data.encode()
    assert count == 26


async def test_text_write_aexit_base_exception_skips_flush(
    read_fd: Fd,
    write_fd: Fd,
) -> None:
    """__aexit__ calls close_fd (no flush) on BaseException."""
    with pytest.raises(KeyboardInterrupt):
        async with TextWriteStream(
            ByteWriteStream.from_fd(write_fd, buffer_size=1024)
        ) as writer:
            await writer.write("buffered-data")
            raise KeyboardInterrupt

    result = os.read(read_fd.fd, 1024)
    assert result == b""


async def test_text_write_close_closes_fd(write_fd: Fd) -> None:
    """close() propagates through to the fd."""
    writer = TextWriteStream.from_fd(write_fd)
    await writer.close()
    with pytest.raises(OSError):
        os.fstat(write_fd.fd)
