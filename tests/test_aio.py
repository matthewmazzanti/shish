import asyncio
import fcntl
import os

from shish.aio import (
    async_read,
    async_write,
    close_fd,
    iterencode,
    wait_readable,
    wait_writable,
)


def test_bytes_exact_chunk() -> None:
    data = b"abcd"
    chunks = list(iterencode(data, chunk_size=4))
    assert chunks == [b"abcd"]


def test_bytes_multiple_chunks() -> None:
    data = b"abcdefgh"
    chunks = list(iterencode(data, chunk_size=3))
    assert chunks == [b"abc", b"def", b"gh"]


def test_bytes_empty() -> None:
    chunks = list(iterencode(b"", chunk_size=4))
    assert chunks == []


def test_str_ascii() -> None:
    data = "hello world"
    chunks = list(iterencode(data, chunk_size=4))
    assert b"".join(chunks) == b"hello world"
    assert all(len(chunk) == 4 for chunk in chunks[:-1])


def test_str_empty() -> None:
    chunks = list(iterencode("", chunk_size=4))
    assert chunks == []


def test_str_multibyte() -> None:
    # Each emoji is 4 bytes in UTF-8
    data = "🎉🎊🎁"
    chunks = list(iterencode(data, chunk_size=4))
    joined = b"".join(chunks)
    assert joined == data.encode("utf-8")
    assert len(joined) == 12


def test_str_mixed_chars() -> None:
    # Mix of 1-byte (ASCII), 2-byte, 3-byte, and 4-byte chars
    data = "aé中🎉"  # 1 + 2 + 3 + 4 = 10 bytes
    chunks = list(iterencode(data, chunk_size=4))
    joined = b"".join(chunks)
    assert joined == data.encode("utf-8")
    assert len(joined) == 10


def test_chunk_size_alignment() -> None:
    # Verify all chunks except last are exactly chunk_size
    data = "x" * 100
    chunk_size = 7
    chunks = list(iterencode(data, chunk_size=chunk_size))
    for chunk in chunks[:-1]:
        assert len(chunk) == chunk_size
    assert len(chunks[-1]) <= chunk_size


# async_write tests


async def test_async_write_bytes() -> None:
    read_fd, write_fd = os.pipe()
    os.set_blocking(write_fd, False)
    await async_write(write_fd, b"hello")
    result = os.read(read_fd, 1024)
    os.close(read_fd)
    assert result == b"hello"


async def test_async_write_str() -> None:
    read_fd, write_fd = os.pipe()
    os.set_blocking(write_fd, False)
    await async_write(write_fd, "hello world")
    result = os.read(read_fd, 1024)
    os.close(read_fd)
    assert result == b"hello world"


async def test_async_write_str_unicode() -> None:
    read_fd, write_fd = os.pipe()
    os.set_blocking(write_fd, False)
    await async_write(write_fd, "héllo 🎉")
    result = os.read(read_fd, 1024)
    os.close(read_fd)
    assert result == "héllo 🎉".encode()


async def test_async_write_empty() -> None:
    read_fd, write_fd = os.pipe()
    os.set_blocking(write_fd, False)
    await async_write(write_fd, b"")
    result = os.read(read_fd, 1024)
    os.close(read_fd)
    assert result == b""


async def test_async_write_closes_fd() -> None:
    read_fd, write_fd = os.pipe()
    os.set_blocking(write_fd, False)
    await async_write(write_fd, b"test")
    # write_fd should be closed, so this should raise
    try:
        os.fstat(write_fd)
        raise AssertionError("fd should be closed")
    except OSError:
        pass
    os.close(read_fd)


# wait_writable tests


async def test_wait_writable_ready() -> None:
    read_fd, write_fd = os.pipe()
    os.set_blocking(write_fd, False)
    # Pipe is empty, should be immediately writable
    await wait_writable(write_fd)
    # If we get here, it worked
    os.close(read_fd)
    os.close(write_fd)


async def test_async_write_blocking() -> None:
    # Test that async_write handles BlockingIOError by awaiting writability
    read_fd, write_fd = os.pipe()
    os.set_blocking(write_fd, False)
    os.set_blocking(read_fd, False)
    # Set minimum pipe buffer size (usually 4096 = page size)
    fcntl.fcntl(write_fd, fcntl.F_SETPIPE_SZ, 4096)

    # Write more than buffer size to trigger BlockingIOError
    data = b"x" * 16384

    async def reader() -> bytes:
        chunks: list[bytes] = []
        while True:
            await wait_readable(read_fd)
            chunk = os.read(read_fd, 4096)
            if not chunk:
                break
            chunks.append(chunk)
        return b"".join(chunks)

    write_task = asyncio.create_task(async_write(write_fd, data))
    read_task = asyncio.create_task(reader())

    await write_task  # Writer closes fd when done, reader gets EOF
    result = await read_task
    os.close(read_fd)
    assert result == data


async def test_async_write_interleaved() -> None:
    # Test that multiple writers interleave properly (neither blocks the other)
    pipe_a = os.pipe()
    pipe_b = os.pipe()
    read_a, write_a = pipe_a
    read_b, write_b = pipe_b

    for fd in [write_a, write_b, read_a, read_b]:
        os.set_blocking(fd, False)
    fcntl.fcntl(write_a, fcntl.F_SETPIPE_SZ, 4096)
    fcntl.fcntl(write_b, fcntl.F_SETPIPE_SZ, 4096)

    data_a = b"A" * 16384
    data_b = b"B" * 16384

    # Track which pipe we read from to verify interleaving
    read_order: list[str] = []

    async def reader(read_fd: int, label: str) -> bytes:
        chunks: list[bytes] = []
        while True:
            await wait_readable(read_fd)
            chunk = os.read(read_fd, 4096)
            if not chunk:
                break
            chunks.append(chunk)
            read_order.append(label)
        return b"".join(chunks)

    write_a_task = asyncio.create_task(async_write(write_a, data_a))
    write_b_task = asyncio.create_task(async_write(write_b, data_b))
    read_a_task = asyncio.create_task(reader(read_a, "A"))
    read_b_task = asyncio.create_task(reader(read_b, "B"))

    await asyncio.gather(write_a_task, write_b_task)
    result_a, result_b = await asyncio.gather(read_a_task, read_b_task)

    os.close(read_a)
    os.close(read_b)

    assert result_a == data_a
    assert result_b == data_b

    # Verify interleaving: should have reads from both A and B mixed
    assert "A" in read_order
    assert "B" in read_order
    # If properly interleaved, we shouldn't see all A's before all B's
    first_a = read_order.index("A")
    first_b = read_order.index("B")
    last_a = len(read_order) - 1 - read_order[::-1].index("A")
    last_b = len(read_order) - 1 - read_order[::-1].index("B")
    # Ranges should overlap (interleave), not be sequential
    assert not (last_a < first_b or last_b < first_a), f"Not interleaved: {read_order}"


# =============================================================================
# close_fd tests
# =============================================================================


def test_close_fd_valid() -> None:
    read_fd, write_fd = os.pipe()
    close_fd(read_fd)
    close_fd(write_fd)
    # Both should be closed now — fstat raises
    for fd in (read_fd, write_fd):
        try:
            os.fstat(fd)
            raise AssertionError(f"fd {fd} should be closed")
        except OSError:
            pass


def test_close_fd_invalid() -> None:
    # Closing an invalid fd should not raise
    close_fd(9999)


def test_close_fd_double_close() -> None:
    read_fd, write_fd = os.pipe()
    os.close(write_fd)
    close_fd(read_fd)
    # Double close should not raise
    close_fd(read_fd)


# =============================================================================
# wait_readable tests
# =============================================================================


async def test_wait_readable_ready() -> None:
    read_fd, write_fd = os.pipe()
    os.write(write_fd, b"data")
    os.close(write_fd)
    # Data available, should return immediately
    os.set_blocking(read_fd, False)
    await wait_readable(read_fd)
    os.close(read_fd)


async def test_wait_readable_delayed() -> None:
    read_fd, write_fd = os.pipe()
    os.set_blocking(read_fd, False)

    async def delayed_write() -> None:
        await asyncio.sleep(0.01)
        os.write(write_fd, b"hello")
        os.close(write_fd)

    writer = asyncio.create_task(delayed_write())
    await wait_readable(read_fd)
    result = os.read(read_fd, 1024)
    await writer
    os.close(read_fd)
    assert result == b"hello"


# =============================================================================
# async_read tests
# =============================================================================


async def test_async_read_basic() -> None:
    read_fd, write_fd = os.pipe()
    os.write(write_fd, b"hello world")
    os.close(write_fd)
    result = await async_read(read_fd)
    assert result == b"hello world"


async def test_async_read_empty() -> None:
    read_fd, write_fd = os.pipe()
    os.close(write_fd)  # Immediate EOF
    result = await async_read(read_fd)
    assert result == b""


async def test_async_read_large() -> None:
    """Data larger than 64K exercises BlockingIOError path in async_read."""
    read_fd, write_fd = os.pipe()
    os.set_blocking(write_fd, False)
    data = b"x" * (256 * 1024)

    write_task = asyncio.create_task(async_write(write_fd, data))
    result = await async_read(read_fd)
    await write_task
    assert result == data


async def test_async_read_closes_fd() -> None:
    read_fd, write_fd = os.pipe()
    os.close(write_fd)
    await async_read(read_fd)
    # read_fd should be closed after async_read
    try:
        os.fstat(read_fd)
        raise AssertionError("fd should be closed")
    except OSError:
        pass


async def test_async_read_multipart() -> None:
    """Multiple writes before close produce concatenated result."""
    read_fd, write_fd = os.pipe()

    async def writer() -> None:
        os.set_blocking(write_fd, False)
        for chunk in [b"aaa", b"bbb", b"ccc"]:
            os.write(write_fd, chunk)
            await asyncio.sleep(0.01)
        os.close(write_fd)

    write_task = asyncio.create_task(writer())
    result = await async_read(read_fd)
    await write_task
    assert result == b"aaabbbccc"


async def test_async_read_concurrent() -> None:
    """Two concurrent async_reads don't interfere with each other."""
    pipe_a = os.pipe()
    pipe_b = os.pipe()
    read_a, write_a = pipe_a
    read_b, write_b = pipe_b

    os.write(write_a, b"alpha")
    os.close(write_a)
    os.write(write_b, b"beta")
    os.close(write_b)

    result_a, result_b = await asyncio.gather(
        async_read(read_a),
        async_read(read_b),
    )
    assert result_a == b"alpha"
    assert result_b == b"beta"
