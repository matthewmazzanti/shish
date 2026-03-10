import asyncio
import fcntl
import os

import pytest

from shish.fd import Fd
from shish.streams import (
    ByteReadStream,
    ByteWriteStream,
    TextReadStream,
)

# =============================================================================
# ByteReadStream
# =============================================================================


async def test_read_all(read_fd: Fd, write_fd: Fd) -> None:
    """read() returns all data until EOF."""
    os.write(write_fd.fd, b"hello world")
    write_fd.close()
    async with ByteReadStream.from_fd(read_fd) as reader:
        result = await reader.read()
    assert result == b"hello world"


async def test_read_partial(read_fd: Fd, write_fd: Fd) -> None:
    """read(n) returns up to n bytes, then EOF."""
    os.write(write_fd.fd, b"hello world")
    write_fd.close()
    async with ByteReadStream.from_fd(read_fd) as reader:
        chunk1 = await reader.read(5)
        chunk2 = await reader.read(10)
        chunk3 = await reader.read(10)
    assert chunk1 == b"hello"
    assert chunk2 == b" world"
    assert chunk3 == b""


async def test_read_eof_empty(read_fd: Fd, write_fd: Fd) -> None:
    """read() on an already-closed pipe returns empty bytes."""
    write_fd.close()
    async with ByteReadStream.from_fd(read_fd) as reader:
        result = await reader.read()
    assert result == b""


async def test_readline(read_fd: Fd, write_fd: Fd) -> None:
    """readline() returns one line at a time including newline."""
    os.write(write_fd.fd, b"line1\nline2\nline3")
    write_fd.close()
    async with ByteReadStream.from_fd(read_fd) as reader:
        assert await reader.readline() == b"line1\n"
        assert await reader.readline() == b"line2\n"
        assert await reader.readline() == b"line3"
        assert await reader.readline() == b""


async def test_readlines(read_fd: Fd, write_fd: Fd) -> None:
    """readlines() returns all lines."""
    os.write(write_fd.fd, b"aaa\nbbb\nccc\n")
    write_fd.close()
    async with ByteReadStream.from_fd(read_fd) as reader:
        lines = await reader.readlines()
    assert lines == [b"aaa\n", b"bbb\n", b"ccc\n"]


async def test_read_iteration(read_fd: Fd, write_fd: Fd) -> None:
    """async for yields lines with trailing newlines."""
    os.write(write_fd.fd, b"aaa\nbbb\nccc\n")
    write_fd.close()
    async with ByteReadStream.from_fd(read_fd) as reader:
        lines = [line async for line in reader]
    assert lines == [b"aaa\n", b"bbb\n", b"ccc\n"]


async def test_read_close_closes_fd(read_fd: Fd, write_fd: Fd) -> None:
    """close() closes the underlying fd."""
    write_fd.close()
    raw = read_fd.fd
    reader = ByteReadStream.from_fd(read_fd)
    await reader.close()
    with pytest.raises(OSError):
        os.fstat(raw)


async def test_read_binary_data(read_fd: Fd, write_fd: Fd) -> None:
    """Handles arbitrary binary data (all 256 byte values)."""
    data = bytes(range(256))
    os.write(write_fd.fd, data)
    write_fd.close()
    async with ByteReadStream.from_fd(read_fd) as reader:
        result = await reader.read()
    assert result == data


async def test_read_large(read_fd: Fd, write_fd: Fd) -> None:
    """Data larger than pipe buffer exercises backpressure on both sides."""
    data = b"x" * (256 * 1024)

    async def do_write() -> None:
        async with ByteWriteStream.from_fd(write_fd) as writer:
            await writer.write(data)

    write_task = asyncio.create_task(do_write())
    async with ByteReadStream.from_fd(read_fd) as reader:
        result = await reader.read()
    await write_task
    assert result == data


async def test_read_close_fd_skips_lock(read_fd: Fd, write_fd: Fd) -> None:
    """close_fd() closes the fd without acquiring the lock."""
    write_fd.close()
    raw = read_fd.fd
    reader = ByteReadStream.from_fd(read_fd)
    reader.close_fd()
    with pytest.raises(OSError):
        os.fstat(raw)


async def test_read_cancel_preserves_buffer(read_fd: Fd, write_fd: Fd) -> None:
    """Cancel mid-read leaves buffer consistent for next read."""
    if hasattr(fcntl, "F_SETPIPE_SZ"):
        fcntl.fcntl(read_fd.fd, fcntl.F_SETPIPE_SZ, 4096)

    # Write some data, keep write end open so read blocks after consuming it
    os.write(write_fd.fd, b"hello")

    reader = ByteReadStream.from_fd(read_fd)
    # read(1024) will get "hello" then block waiting for more
    task = asyncio.create_task(reader.read(1024))
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # Write more data and close — next read should work
    os.write(write_fd.fd, b" world")
    write_fd.close()
    result = await reader.read()
    # Buffer may contain "hello" from the cancelled read plus " world"
    assert b"world" in result
    await reader.close()


async def test_read_aexit_base_exception_calls_close_fd(
    read_fd: Fd,
    write_fd: Fd,
) -> None:
    """__aexit__ calls close_fd on BaseException (not Exception)."""
    write_fd.close()
    raw = read_fd.fd

    with pytest.raises(KeyboardInterrupt):
        async with ByteReadStream.from_fd(read_fd):
            raise KeyboardInterrupt

    with pytest.raises(OSError):
        os.fstat(raw)


# =============================================================================
# TextReadStream
# =============================================================================


async def test_text_read_all(read_fd: Fd, write_fd: Fd) -> None:
    """read() returns all decoded text until EOF."""
    os.write(write_fd.fd, b"hello world")
    write_fd.close()
    async with TextReadStream.from_fd(read_fd) as reader:
        result = await reader.read()
    assert result == "hello world"


async def test_text_read_unicode(read_fd: Fd, write_fd: Fd) -> None:
    """read() decodes multi-byte characters."""
    os.write(write_fd.fd, "héllo 🎉".encode())
    write_fd.close()
    async with TextReadStream.from_fd(read_fd) as reader:
        result = await reader.read()
    assert result == "héllo 🎉"


async def test_text_read_chars(read_fd: Fd, write_fd: Fd) -> None:
    """read(n) counts characters, not bytes."""
    os.write(write_fd.fd, "aé🎉b".encode())
    write_fd.close()
    async with TextReadStream.from_fd(read_fd) as reader:
        chunk1 = await reader.read(2)
        chunk2 = await reader.read(2)
        chunk3 = await reader.read(10)
    assert chunk1 == "aé"
    assert chunk2 == "🎉b"
    assert chunk3 == ""


async def test_text_readline(read_fd: Fd, write_fd: Fd) -> None:
    """readline() returns decoded lines with trailing newline."""
    os.write(write_fd.fd, b"line1\nline2\nline3")
    write_fd.close()
    async with TextReadStream.from_fd(read_fd) as reader:
        assert await reader.readline() == "line1\n"
        assert await reader.readline() == "line2\n"
        assert await reader.readline() == "line3"
        assert await reader.readline() == ""


async def test_text_readlines(read_fd: Fd, write_fd: Fd) -> None:
    """readlines() returns all decoded lines."""
    os.write(write_fd.fd, b"aaa\nbbb\nccc\n")
    write_fd.close()
    async with TextReadStream.from_fd(read_fd) as reader:
        lines = await reader.readlines()
    assert lines == ["aaa\n", "bbb\n", "ccc\n"]


async def test_text_read_iteration(read_fd: Fd, write_fd: Fd) -> None:
    """async for yields decoded lines."""
    os.write(write_fd.fd, b"aaa\nbbb\nccc\n")
    write_fd.close()
    async with TextReadStream.from_fd(read_fd) as reader:
        lines = [line async for line in reader]
    assert lines == ["aaa\n", "bbb\n", "ccc\n"]


async def test_text_read_close_closes_fd(read_fd: Fd, write_fd: Fd) -> None:
    """close() propagates through to the fd."""
    write_fd.close()
    raw = read_fd.fd
    reader = TextReadStream.from_fd(read_fd)
    await reader.close()
    with pytest.raises(OSError):
        os.fstat(raw)


async def test_text_read_then_readline(read_fd: Fd, write_fd: Fd) -> None:
    """read() and readline() share the same buffer."""
    os.write(write_fd.fd, b"abcdef\nghij\n")
    write_fd.close()
    async with TextReadStream.from_fd(read_fd) as reader:
        chunk = await reader.read(3)
        line = await reader.readline()
        rest = await reader.readline()
    assert chunk == "abc"
    assert line == "def\n"
    assert rest == "ghij\n"


async def test_text_read_split_multibyte(read_fd: Fd, write_fd: Fd) -> None:
    """Incremental decoder handles multi-byte chars split across reads."""
    emoji = "🎉".encode()  # 4 bytes: f0 9f 8e 89

    async def do_write() -> None:
        # Write the emoji as two separate chunks so the byte-level
        # reads may see the split.
        os.write(write_fd.fd, emoji[:2])
        await asyncio.sleep(0.01)
        os.write(write_fd.fd, emoji[2:])
        write_fd.close()

    write_task = asyncio.create_task(do_write())
    async with TextReadStream.from_bytes(ByteReadStream.from_fd(read_fd, buffer_size=2)) as reader:
        result = await reader.read()
    await write_task
    assert result == "🎉"


async def test_text_read_eof_empty(read_fd: Fd, write_fd: Fd) -> None:
    """read() on an already-closed pipe returns empty string."""
    write_fd.close()
    async with TextReadStream.from_fd(read_fd) as reader:
        result = await reader.read()
    assert result == ""


async def test_text_read_buffered_property(read_fd: Fd, write_fd: Fd) -> None:
    """buffered property tracks unconsumed characters."""
    os.write(write_fd.fd, b"hello world\n")
    write_fd.close()
    async with TextReadStream.from_fd(read_fd) as reader:
        await reader.read(5)
        assert reader.buffered > 0  # " world\n" still buffered
        await reader.read(6)
        assert reader.buffered > 0  # "\n" still buffered
        await reader.readline()
        remaining = await reader.read()
        assert remaining == ""


async def test_text_read_many_small_reads_compact(
    read_fd: Fd,
    write_fd: Fd,
) -> None:
    """Many small reads exercise the offset/compact path."""
    data = "abcdefghijklmnopqrstuvwxyz"
    os.write(write_fd.fd, data.encode())
    write_fd.close()
    async with TextReadStream.from_fd(read_fd) as reader:
        chars: list[str] = []
        for _ in range(26):
            char = await reader.read(1)
            chars.append(char)
        assert "".join(chars) == data
        assert await reader.read(1) == ""


async def test_text_read_many_small_readlines_compact(
    read_fd: Fd,
    write_fd: Fd,
) -> None:
    """Many readline calls exercise the offset/compact path."""
    lines = [f"line{idx}\n" for idx in range(50)]
    os.write(write_fd.fd, "".join(lines).encode())
    write_fd.close()
    async with TextReadStream.from_fd(read_fd) as reader:
        result: list[str] = []
        for _ in range(50):
            result.append(await reader.readline())
        assert result == lines
        assert await reader.readline() == ""


async def test_text_read_aexit_base_exception_calls_close_fd(
    read_fd: Fd,
    write_fd: Fd,
) -> None:
    """__aexit__ calls close_fd on BaseException (not Exception)."""
    write_fd.close()
    raw = read_fd.fd

    with pytest.raises(KeyboardInterrupt):
        async with TextReadStream.from_fd(read_fd):
            raise KeyboardInterrupt

    with pytest.raises(OSError):
        os.fstat(raw)
