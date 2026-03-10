import os

from shish.fd import Fd
from shish.fn_stage import ByteStage, TextStage, decode
from shish.streams import (
    ByteReadStream,
    ByteWriteStream,
    TextReadStream,
    TextWriteStream,
)


async def test_byte_stage_ctx_fields() -> None:
    """ByteStage has stdin (ByteReadStream) and stdout (ByteWriteStream) fields."""
    read_fd, write_fd = os.pipe()
    stderr_fd = os.open(os.devnull, os.O_WRONLY)
    reader = ByteReadStream.from_fd(Fd(read_fd))
    writer = ByteWriteStream.from_fd(Fd(write_fd))
    errwriter = ByteWriteStream.from_fd(Fd(stderr_fd))
    ctx = ByteStage(stdin=reader, stdout=writer, stderr=errwriter)
    assert ctx.stdin is reader
    assert ctx.stdout is writer
    assert ctx.stderr is errwriter
    await reader.close()
    await writer.close()
    await errwriter.close()


async def test_text_stage_ctx_fields() -> None:
    """TextStage has stdin (TextReadStream) and stdout (TextWriteStream) fields."""
    read_fd, write_fd = os.pipe()
    stderr_fd = os.open(os.devnull, os.O_WRONLY)
    byte_reader = ByteReadStream.from_fd(Fd(read_fd))
    byte_writer = ByteWriteStream.from_fd(Fd(write_fd))
    byte_errwriter = ByteWriteStream.from_fd(Fd(stderr_fd))
    reader = TextReadStream(byte_reader)
    writer = TextWriteStream(byte_writer)
    errwriter = TextWriteStream(byte_errwriter)
    ctx = TextStage(stdin=reader, stdout=writer, stderr=errwriter)
    assert ctx.stdin is reader
    assert ctx.stdout is writer
    assert ctx.stderr is errwriter
    await reader.close()
    await writer.close()
    await errwriter.close()


async def test_decode_bare_decorator() -> None:
    """@decode (no parens) wraps a text function into a byte function."""

    @decode
    async def upper(ctx: TextStage) -> int:
        data = await ctx.stdin.read()
        await ctx.stdout.write(data.upper())
        return 0

    # stdin pipe: feed "hello" into the read end
    stdin_read_fd, stdin_write_fd = os.pipe()
    os.write(stdin_write_fd, b"hello")
    os.close(stdin_write_fd)

    # stdout pipe: read the result from the read end
    stdout_read_fd, stdout_write_fd = os.pipe()

    stderr_fd = os.open(os.devnull, os.O_WRONLY)
    byte_ctx = ByteStage(
        stdin=ByteReadStream.from_fd(Fd(stdin_read_fd)),
        stdout=ByteWriteStream.from_fd(Fd(stdout_write_fd)),
        stderr=ByteWriteStream.from_fd(Fd(stderr_fd)),
    )
    result = await upper(byte_ctx)
    await byte_ctx.stdin.close()
    await byte_ctx.stdout.close()
    await byte_ctx.stderr.close()

    output = os.read(stdout_read_fd, 1024)
    os.close(stdout_read_fd)

    assert result == 0
    assert output == b"HELLO"


async def test_decode_with_parens() -> None:
    """@decode() with no args works same as bare @decode."""

    @decode()
    async def upper(ctx: TextStage) -> int:
        data = await ctx.stdin.read()
        await ctx.stdout.write(data.upper())
        return 0

    stdin_read_fd, stdin_write_fd = os.pipe()
    os.write(stdin_write_fd, b"hello")
    os.close(stdin_write_fd)

    stdout_read_fd, stdout_write_fd = os.pipe()

    stderr_fd = os.open(os.devnull, os.O_WRONLY)
    byte_ctx = ByteStage(
        stdin=ByteReadStream.from_fd(Fd(stdin_read_fd)),
        stdout=ByteWriteStream.from_fd(Fd(stdout_write_fd)),
        stderr=ByteWriteStream.from_fd(Fd(stderr_fd)),
    )
    result = await upper(byte_ctx)
    await byte_ctx.stdin.close()
    await byte_ctx.stdout.close()
    await byte_ctx.stderr.close()

    output = os.read(stdout_read_fd, 1024)
    os.close(stdout_read_fd)

    assert result == 0
    assert output == b"HELLO"


async def test_decode_with_encoding() -> None:
    """@decode("latin-1") uses latin-1 encoding for decode/encode."""

    @decode("latin-1")
    async def passthrough(ctx: TextStage) -> int:
        data = await ctx.stdin.read()
        await ctx.stdout.write(data)
        return 0

    # b'\xe9' is 'é' in latin-1 (single byte), unlike utf-8 (two bytes)
    stdin_read_fd, stdin_write_fd = os.pipe()
    os.write(stdin_write_fd, b"caf\xe9")
    os.close(stdin_write_fd)

    stdout_read_fd, stdout_write_fd = os.pipe()

    stderr_fd = os.open(os.devnull, os.O_WRONLY)
    byte_ctx = ByteStage(
        stdin=ByteReadStream.from_fd(Fd(stdin_read_fd)),
        stdout=ByteWriteStream.from_fd(Fd(stdout_write_fd)),
        stderr=ByteWriteStream.from_fd(Fd(stderr_fd)),
    )
    result = await passthrough(byte_ctx)
    await byte_ctx.stdin.close()
    await byte_ctx.stdout.close()
    await byte_ctx.stderr.close()

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
    async def noop(ctx: TextStage) -> int:
        _ = await ctx.stdin.read()
        await ctx.stdout.write("")
        return 0

    stdin_read_fd, stdin_write_fd = os.pipe()
    os.close(stdin_write_fd)

    stdout_read_fd, stdout_write_fd = os.pipe()

    stderr_fd = os.open(os.devnull, os.O_WRONLY)
    stdin_stream = ByteReadStream.from_fd(Fd(stdin_read_fd))
    stdout_stream = ByteWriteStream.from_fd(Fd(stdout_write_fd))
    stderr_stream = ByteWriteStream.from_fd(Fd(stderr_fd))
    byte_ctx = ByteStage(stdin=stdin_stream, stdout=stdout_stream, stderr=stderr_stream)

    await noop(byte_ctx)

    # Byte streams should NOT be closed by the decorator
    assert not stdin_stream.closed
    assert not stdout_stream.closed
    assert not stderr_stream.closed

    # Clean up
    await stdin_stream.close()
    await stdout_stream.close()
    await stderr_stream.close()
    os.close(stdout_read_fd)
