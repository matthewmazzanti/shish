import asyncio
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


async def test_read_all() -> None:
    """read() returns all data until EOF."""
    read_fd, write_fd = os.pipe()
    os.write(write_fd, b"hello world")
    os.close(write_fd)
    async with ByteReadStream(Fd(read_fd)) as reader:
        result = await reader.read()
    assert result == b"hello world"


async def test_read_partial() -> None:
    """read(n) returns up to n bytes, then EOF."""
    read_fd, write_fd = os.pipe()
    os.write(write_fd, b"hello world")
    os.close(write_fd)
    async with ByteReadStream(Fd(read_fd)) as reader:
        chunk1 = await reader.read(5)
        chunk2 = await reader.read(10)
        chunk3 = await reader.read(10)
    assert chunk1 == b"hello"
    assert chunk2 == b" world"
    assert chunk3 == b""


async def test_read_eof_empty() -> None:
    """read() on an already-closed pipe returns empty bytes."""
    read_fd, write_fd = os.pipe()
    os.close(write_fd)
    async with ByteReadStream(Fd(read_fd)) as reader:
        result = await reader.read()
    assert result == b""


async def test_readline() -> None:
    """readline() returns one line at a time including newline."""
    read_fd, write_fd = os.pipe()
    os.write(write_fd, b"line1\nline2\nline3")
    os.close(write_fd)
    async with ByteReadStream(Fd(read_fd)) as reader:
        assert await reader.readline() == b"line1\n"
        assert await reader.readline() == b"line2\n"
        assert await reader.readline() == b"line3"
        assert await reader.readline() == b""


async def test_readlines() -> None:
    """readlines() returns all lines."""
    read_fd, write_fd = os.pipe()
    os.write(write_fd, b"aaa\nbbb\nccc\n")
    os.close(write_fd)
    async with ByteReadStream(Fd(read_fd)) as reader:
        lines = await reader.readlines()
    assert lines == [b"aaa\n", b"bbb\n", b"ccc\n"]


async def test_read_iteration() -> None:
    """async for yields lines with trailing newlines."""
    read_fd, write_fd = os.pipe()
    os.write(write_fd, b"aaa\nbbb\nccc\n")
    os.close(write_fd)
    async with ByteReadStream(Fd(read_fd)) as reader:
        lines = [line async for line in reader]
    assert lines == [b"aaa\n", b"bbb\n", b"ccc\n"]


async def test_read_close_closes_fd() -> None:
    """close() closes the underlying fd."""
    read_fd, write_fd = os.pipe()
    os.close(write_fd)
    reader = ByteReadStream(Fd(read_fd))
    await reader.close()
    with pytest.raises(OSError):
        os.fstat(read_fd)


async def test_read_binary_data() -> None:
    """Handles arbitrary binary data (all 256 byte values)."""
    read_fd, write_fd = os.pipe()
    data = bytes(range(256))
    os.write(write_fd, data)
    os.close(write_fd)
    async with ByteReadStream(Fd(read_fd)) as reader:
        result = await reader.read()
    assert result == data


async def test_read_large() -> None:
    """Data larger than pipe buffer exercises backpressure on both sides."""
    read_fd, write_fd = os.pipe()
    data = b"x" * (256 * 1024)

    async def do_write() -> None:
        async with ByteWriteStream(Fd(write_fd)) as writer:
            await writer.write(data)

    write_task = asyncio.create_task(do_write())
    async with ByteReadStream(Fd(read_fd)) as reader:
        result = await reader.read()
    await write_task
    assert result == data


# =============================================================================
# TextReadStream
# =============================================================================


async def test_text_read_all() -> None:
    """read() returns all decoded text until EOF."""
    read_fd, write_fd = os.pipe()
    os.write(write_fd, b"hello world")
    os.close(write_fd)
    async with TextReadStream(ByteReadStream(Fd(read_fd))) as reader:
        result = await reader.read()
    assert result == "hello world"


async def test_text_read_unicode() -> None:
    """read() decodes multi-byte characters."""
    read_fd, write_fd = os.pipe()
    os.write(write_fd, "héllo 🎉".encode())
    os.close(write_fd)
    async with TextReadStream(ByteReadStream(Fd(read_fd))) as reader:
        result = await reader.read()
    assert result == "héllo 🎉"


async def test_text_read_chars() -> None:
    """read(n) counts characters, not bytes."""
    read_fd, write_fd = os.pipe()
    os.write(write_fd, "aé🎉b".encode())
    os.close(write_fd)
    async with TextReadStream(ByteReadStream(Fd(read_fd))) as reader:
        chunk1 = await reader.read(2)
        chunk2 = await reader.read(2)
        chunk3 = await reader.read(10)
    assert chunk1 == "aé"
    assert chunk2 == "🎉b"
    assert chunk3 == ""


async def test_text_readline() -> None:
    """readline() returns decoded lines with trailing newline."""
    read_fd, write_fd = os.pipe()
    os.write(write_fd, b"line1\nline2\nline3")
    os.close(write_fd)
    async with TextReadStream(ByteReadStream(Fd(read_fd))) as reader:
        assert await reader.readline() == "line1\n"
        assert await reader.readline() == "line2\n"
        assert await reader.readline() == "line3"
        assert await reader.readline() == ""


async def test_text_readlines() -> None:
    """readlines() returns all decoded lines."""
    read_fd, write_fd = os.pipe()
    os.write(write_fd, b"aaa\nbbb\nccc\n")
    os.close(write_fd)
    async with TextReadStream(ByteReadStream(Fd(read_fd))) as reader:
        lines = await reader.readlines()
    assert lines == ["aaa\n", "bbb\n", "ccc\n"]


async def test_text_read_iteration() -> None:
    """async for yields decoded lines."""
    read_fd, write_fd = os.pipe()
    os.write(write_fd, b"aaa\nbbb\nccc\n")
    os.close(write_fd)
    async with TextReadStream(ByteReadStream(Fd(read_fd))) as reader:
        lines = [line async for line in reader]
    assert lines == ["aaa\n", "bbb\n", "ccc\n"]


async def test_text_read_close_closes_fd() -> None:
    """close() propagates through to the fd."""
    read_fd, write_fd = os.pipe()
    os.close(write_fd)
    reader = TextReadStream(ByteReadStream(Fd(read_fd)))
    await reader.close()
    with pytest.raises(OSError):
        os.fstat(read_fd)


async def test_text_read_then_readline() -> None:
    """read() and readline() share the same buffer."""
    read_fd, write_fd = os.pipe()
    os.write(write_fd, b"abcdef\nghij\n")
    os.close(write_fd)
    async with TextReadStream(ByteReadStream(Fd(read_fd))) as reader:
        chunk = await reader.read(3)
        line = await reader.readline()
        rest = await reader.readline()
    assert chunk == "abc"
    assert line == "def\n"
    assert rest == "ghij\n"


async def test_text_read_split_multibyte() -> None:
    """Incremental decoder handles multi-byte chars split across reads."""
    read_fd, write_fd = os.pipe()
    emoji = "🎉".encode()  # 4 bytes: f0 9f 8e 89

    async def do_write() -> None:
        # Write the emoji as two separate chunks so the byte-level
        # reads may see the split.
        os.write(write_fd, emoji[:2])
        await asyncio.sleep(0.01)
        os.write(write_fd, emoji[2:])
        os.close(write_fd)

    write_task = asyncio.create_task(do_write())
    async with TextReadStream(ByteReadStream(Fd(read_fd), buffer_size=2)) as reader:
        result = await reader.read()
    await write_task
    assert result == "🎉"


async def test_text_read_eof_empty() -> None:
    """read() on an already-closed pipe returns empty string."""
    read_fd, write_fd = os.pipe()
    os.close(write_fd)
    async with TextReadStream(ByteReadStream(Fd(read_fd))) as reader:
        result = await reader.read()
    assert result == ""
