import asyncio
import fcntl
import os

import pytest

from shish.aio import (
    ByteReadStream,
    ByteStageCtx,
    ByteWriteStream,
    OwnedFd,
    TextReadStream,
    TextStageCtx,
    TextWriteStream,
    decode,
)

# =============================================================================
# ByteWriteStream
# =============================================================================


async def test_write() -> None:
    """write() delivers all bytes and returns the count."""
    read_fd, write_fd = os.pipe()
    async with ByteWriteStream(OwnedFd(write_fd)) as writer:
        count = await writer.write(b"hello")
    result = os.read(read_fd, 1024)
    os.close(read_fd)
    assert result == b"hello"
    assert count == 5


async def test_writelines() -> None:
    """writelines() writes all chunks in order."""
    read_fd, write_fd = os.pipe()
    async with ByteWriteStream(OwnedFd(write_fd)) as writer:
        await writer.writelines([b"aaa", b"bbb", b"ccc"])
    result = os.read(read_fd, 1024)
    os.close(read_fd)
    assert result == b"aaabbbccc"


async def test_write_close_closes_fd() -> None:
    """close() closes the underlying fd."""
    read_fd, write_fd = os.pipe()
    writer = ByteWriteStream(OwnedFd(write_fd))
    await writer.close()
    with pytest.raises(OSError):
        os.fstat(write_fd)
    os.close(read_fd)


async def test_write_context_manager_closes_on_exception() -> None:
    """__aexit__ closes the fd even when the block raises."""
    read_fd, write_fd = os.pipe()
    with pytest.raises(RuntimeError):
        async with ByteWriteStream(OwnedFd(write_fd)):
            raise RuntimeError("boom")
    with pytest.raises(OSError):
        os.fstat(write_fd)
    os.close(read_fd)


async def test_write_after_close() -> None:
    """write() on a closed stream raises."""
    read_fd, write_fd = os.pipe()
    writer = ByteWriteStream(OwnedFd(write_fd))
    await writer.close()
    with pytest.raises(OSError):
        await writer.write(b"hello")
    os.close(read_fd)


async def test_write_broken_pipe() -> None:
    """write() raises when the read end is already closed."""
    _read_fd, write_fd = os.pipe()
    os.close(_read_fd)
    writer = ByteWriteStream(OwnedFd(write_fd))
    with pytest.raises(BrokenPipeError):
        await writer.write(b"hello")


async def test_write_backpressure() -> None:
    """write() suspends when pipe buffer is full, resumes when drained."""
    read_fd, write_fd = os.pipe()
    if hasattr(fcntl, "F_SETPIPE_SZ"):
        fcntl.fcntl(write_fd, fcntl.F_SETPIPE_SZ, 4096)
    # 256KB is well above any default pipe buffer (4KB Linux, 16KB macOS)
    data = b"x" * 262144

    async def do_write() -> None:
        async with ByteWriteStream(OwnedFd(write_fd)) as writer:
            await writer.write(data)

    write_task = asyncio.create_task(do_write())
    async with ByteReadStream(OwnedFd(read_fd)) as reader:
        result = await reader.read()
    await write_task
    assert result == data


# =============================================================================
# ByteReadStream
# =============================================================================


async def test_read_all() -> None:
    """read() returns all data until EOF."""
    read_fd, write_fd = os.pipe()
    os.write(write_fd, b"hello world")
    os.close(write_fd)
    async with ByteReadStream(OwnedFd(read_fd)) as reader:
        result = await reader.read()
    assert result == b"hello world"


async def test_read_partial() -> None:
    """read(n) returns up to n bytes, then EOF."""
    read_fd, write_fd = os.pipe()
    os.write(write_fd, b"hello world")
    os.close(write_fd)
    async with ByteReadStream(OwnedFd(read_fd)) as reader:
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
    async with ByteReadStream(OwnedFd(read_fd)) as reader:
        result = await reader.read()
    assert result == b""


async def test_readline() -> None:
    """readline() returns one line at a time including newline."""
    read_fd, write_fd = os.pipe()
    os.write(write_fd, b"line1\nline2\nline3")
    os.close(write_fd)
    async with ByteReadStream(OwnedFd(read_fd)) as reader:
        assert await reader.readline() == b"line1\n"
        assert await reader.readline() == b"line2\n"
        assert await reader.readline() == b"line3"
        assert await reader.readline() == b""


async def test_readlines() -> None:
    """readlines() returns all lines."""
    read_fd, write_fd = os.pipe()
    os.write(write_fd, b"aaa\nbbb\nccc\n")
    os.close(write_fd)
    async with ByteReadStream(OwnedFd(read_fd)) as reader:
        lines = await reader.readlines()
    assert lines == [b"aaa\n", b"bbb\n", b"ccc\n"]


async def test_read_iteration() -> None:
    """async for yields lines with trailing newlines."""
    read_fd, write_fd = os.pipe()
    os.write(write_fd, b"aaa\nbbb\nccc\n")
    os.close(write_fd)
    async with ByteReadStream(OwnedFd(read_fd)) as reader:
        lines = [line async for line in reader]
    assert lines == [b"aaa\n", b"bbb\n", b"ccc\n"]


async def test_read_close_closes_fd() -> None:
    """close() closes the underlying fd."""
    read_fd, write_fd = os.pipe()
    os.close(write_fd)
    reader = ByteReadStream(OwnedFd(read_fd))
    await reader.close()
    with pytest.raises(OSError):
        os.fstat(read_fd)


async def test_read_binary_data() -> None:
    """Handles arbitrary binary data (all 256 byte values)."""
    read_fd, write_fd = os.pipe()
    data = bytes(range(256))
    os.write(write_fd, data)
    os.close(write_fd)
    async with ByteReadStream(OwnedFd(read_fd)) as reader:
        result = await reader.read()
    assert result == data


async def test_read_large() -> None:
    """Data larger than pipe buffer exercises backpressure on both sides."""
    read_fd, write_fd = os.pipe()
    data = b"x" * (256 * 1024)

    async def do_write() -> None:
        async with ByteWriteStream(OwnedFd(write_fd)) as writer:
            await writer.write(data)

    write_task = asyncio.create_task(do_write())
    async with ByteReadStream(OwnedFd(read_fd)) as reader:
        result = await reader.read()
    await write_task
    assert result == data


# =============================================================================
# TextWriteStream
# =============================================================================


async def test_text_write() -> None:
    """write() encodes and delivers string, returns char count."""
    read_fd, write_fd = os.pipe()
    async with TextWriteStream(ByteWriteStream(OwnedFd(write_fd))) as writer:
        count = await writer.write("hello")
    result = os.read(read_fd, 1024)
    os.close(read_fd)
    assert result == b"hello"
    assert count == 5


async def test_text_write_unicode() -> None:
    """write() handles multi-byte characters, returns char count not byte count."""
    read_fd, write_fd = os.pipe()
    async with TextWriteStream(ByteWriteStream(OwnedFd(write_fd))) as writer:
        count = await writer.write("héllo 🎉")
    result = os.read(read_fd, 1024)
    os.close(read_fd)
    assert result == "héllo 🎉".encode()
    assert count == 7


async def test_text_writelines() -> None:
    """writelines() writes all strings in order."""
    read_fd, write_fd = os.pipe()
    async with TextWriteStream(ByteWriteStream(OwnedFd(write_fd))) as writer:
        await writer.writelines(["aaa\n", "bbb\n", "ccc\n"])
    result = os.read(read_fd, 1024)
    os.close(read_fd)
    assert result == b"aaa\nbbb\nccc\n"


async def test_text_write_close_closes_fd() -> None:
    """close() propagates through to the fd."""
    read_fd, write_fd = os.pipe()
    writer = TextWriteStream(ByteWriteStream(OwnedFd(write_fd)))
    await writer.close()
    with pytest.raises(OSError):
        os.fstat(write_fd)
    os.close(read_fd)


# =============================================================================
# TextReadStream
# =============================================================================


async def test_text_read_all() -> None:
    """read() returns all decoded text until EOF."""
    read_fd, write_fd = os.pipe()
    os.write(write_fd, b"hello world")
    os.close(write_fd)
    async with TextReadStream(ByteReadStream(OwnedFd(read_fd))) as reader:
        result = await reader.read()
    assert result == "hello world"


async def test_text_read_unicode() -> None:
    """read() decodes multi-byte characters."""
    read_fd, write_fd = os.pipe()
    os.write(write_fd, "héllo 🎉".encode())
    os.close(write_fd)
    async with TextReadStream(ByteReadStream(OwnedFd(read_fd))) as reader:
        result = await reader.read()
    assert result == "héllo 🎉"


async def test_text_read_chars() -> None:
    """read(n) counts characters, not bytes."""
    read_fd, write_fd = os.pipe()
    os.write(write_fd, "aé🎉b".encode())
    os.close(write_fd)
    async with TextReadStream(ByteReadStream(OwnedFd(read_fd))) as reader:
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
    async with TextReadStream(ByteReadStream(OwnedFd(read_fd))) as reader:
        assert await reader.readline() == "line1\n"
        assert await reader.readline() == "line2\n"
        assert await reader.readline() == "line3"
        assert await reader.readline() == ""


async def test_text_readlines() -> None:
    """readlines() returns all decoded lines."""
    read_fd, write_fd = os.pipe()
    os.write(write_fd, b"aaa\nbbb\nccc\n")
    os.close(write_fd)
    async with TextReadStream(ByteReadStream(OwnedFd(read_fd))) as reader:
        lines = await reader.readlines()
    assert lines == ["aaa\n", "bbb\n", "ccc\n"]


async def test_text_read_iteration() -> None:
    """async for yields decoded lines."""
    read_fd, write_fd = os.pipe()
    os.write(write_fd, b"aaa\nbbb\nccc\n")
    os.close(write_fd)
    async with TextReadStream(ByteReadStream(OwnedFd(read_fd))) as reader:
        lines = [line async for line in reader]
    assert lines == ["aaa\n", "bbb\n", "ccc\n"]


async def test_text_read_close_closes_fd() -> None:
    """close() propagates through to the fd."""
    read_fd, write_fd = os.pipe()
    os.close(write_fd)
    reader = TextReadStream(ByteReadStream(OwnedFd(read_fd)))
    await reader.close()
    with pytest.raises(OSError):
        os.fstat(read_fd)


async def test_text_read_then_readline() -> None:
    """read() and readline() share the same buffer."""
    read_fd, write_fd = os.pipe()
    os.write(write_fd, b"abcdef\nghij\n")
    os.close(write_fd)
    async with TextReadStream(ByteReadStream(OwnedFd(read_fd))) as reader:
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
    async with TextReadStream(
        ByteReadStream(OwnedFd(read_fd), buffer_size=2)
    ) as reader:
        result = await reader.read()
    await write_task
    assert result == "🎉"


async def test_text_read_eof_empty() -> None:
    """read() on an already-closed pipe returns empty string."""
    read_fd, write_fd = os.pipe()
    os.close(write_fd)
    async with TextReadStream(ByteReadStream(OwnedFd(read_fd))) as reader:
        result = await reader.read()
    assert result == ""


# =============================================================================
# ByteStageCtx and TextStageCtx
# =============================================================================


async def test_byte_stage_ctx_fields() -> None:
    """ByteStageCtx has stdin (ByteReadStream) and stdout (ByteWriteStream) fields."""
    read_fd, write_fd = os.pipe()
    reader = ByteReadStream(OwnedFd(read_fd))
    writer = ByteWriteStream(OwnedFd(write_fd))
    ctx = ByteStageCtx(stdin=reader, stdout=writer)
    assert ctx.stdin is reader
    assert ctx.stdout is writer
    await reader.close()
    await writer.close()


async def test_text_stage_ctx_fields() -> None:
    """TextStageCtx has stdin (TextReadStream) and stdout (TextWriteStream) fields."""
    read_fd, write_fd = os.pipe()
    byte_reader = ByteReadStream(OwnedFd(read_fd))
    byte_writer = ByteWriteStream(OwnedFd(write_fd))
    reader = TextReadStream(byte_reader)
    writer = TextWriteStream(byte_writer)
    ctx = TextStageCtx(stdin=reader, stdout=writer)
    assert ctx.stdin is reader
    assert ctx.stdout is writer
    await reader.close()
    await writer.close()


async def test_decode_bare_decorator() -> None:
    """@decode (no parens) wraps a text function into a byte function."""

    @decode
    async def upper(ctx: TextStageCtx) -> int:
        data = await ctx.stdin.read()
        await ctx.stdout.write(data.upper())
        return 0

    # stdin pipe: feed "hello" into the read end
    stdin_read_fd, stdin_write_fd = os.pipe()
    os.write(stdin_write_fd, b"hello")
    os.close(stdin_write_fd)

    # stdout pipe: read the result from the read end
    stdout_read_fd, stdout_write_fd = os.pipe()

    byte_ctx = ByteStageCtx(
        stdin=ByteReadStream(OwnedFd(stdin_read_fd)),
        stdout=ByteWriteStream(OwnedFd(stdout_write_fd)),
    )
    result = await upper(byte_ctx)
    await byte_ctx.stdout.close()

    output = os.read(stdout_read_fd, 1024)
    os.close(stdout_read_fd)

    assert result == 0
    assert output == b"HELLO"


async def test_decode_with_parens() -> None:
    """@decode() with no args works same as bare @decode."""

    @decode()
    async def upper(ctx: TextStageCtx) -> int:
        data = await ctx.stdin.read()
        await ctx.stdout.write(data.upper())
        return 0

    stdin_read_fd, stdin_write_fd = os.pipe()
    os.write(stdin_write_fd, b"hello")
    os.close(stdin_write_fd)

    stdout_read_fd, stdout_write_fd = os.pipe()

    byte_ctx = ByteStageCtx(
        stdin=ByteReadStream(OwnedFd(stdin_read_fd)),
        stdout=ByteWriteStream(OwnedFd(stdout_write_fd)),
    )
    result = await upper(byte_ctx)
    await byte_ctx.stdout.close()

    output = os.read(stdout_read_fd, 1024)
    os.close(stdout_read_fd)

    assert result == 0
    assert output == b"HELLO"


async def test_decode_with_encoding() -> None:
    """@decode("latin-1") uses latin-1 encoding for decode/encode."""

    @decode("latin-1")
    async def passthrough(ctx: TextStageCtx) -> int:
        data = await ctx.stdin.read()
        await ctx.stdout.write(data)
        return 0

    # b'\xe9' is 'é' in latin-1 (single byte), unlike utf-8 (two bytes)
    stdin_read_fd, stdin_write_fd = os.pipe()
    os.write(stdin_write_fd, b"caf\xe9")
    os.close(stdin_write_fd)

    stdout_read_fd, stdout_write_fd = os.pipe()

    byte_ctx = ByteStageCtx(
        stdin=ByteReadStream(OwnedFd(stdin_read_fd)),
        stdout=ByteWriteStream(OwnedFd(stdout_write_fd)),
    )
    result = await passthrough(byte_ctx)
    await byte_ctx.stdout.close()

    output = os.read(stdout_read_fd, 1024)
    os.close(stdout_read_fd)

    assert result == 0
    # latin-1 round-trips: single byte \xe9 decodes to 'é', encodes back to \xe9
    assert output == b"caf\xe9"


async def test_decode_does_not_close_byte_streams() -> None:
    """@decode does not close the underlying byte streams.

    The runtime manages fd lifecycle, not the decorator.
    """

    @decode
    async def noop(ctx: TextStageCtx) -> int:
        _ = await ctx.stdin.read()
        await ctx.stdout.write("")
        return 0

    stdin_read_fd, stdin_write_fd = os.pipe()
    os.close(stdin_write_fd)

    stdout_read_fd, stdout_write_fd = os.pipe()

    stdin_stream = ByteReadStream(OwnedFd(stdin_read_fd))
    stdout_stream = ByteWriteStream(OwnedFd(stdout_write_fd))
    byte_ctx = ByteStageCtx(stdin=stdin_stream, stdout=stdout_stream)

    await noop(byte_ctx)

    # Byte streams should NOT be closed by the decorator
    assert not stdin_stream._fd.closed  # pyright: ignore[reportPrivateUsage]
    assert not stdout_stream._fd.closed  # pyright: ignore[reportPrivateUsage]

    # Clean up
    await stdin_stream.close()
    await stdout_stream.close()
    os.close(stdout_read_fd)
