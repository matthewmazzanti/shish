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
    close()         Release the fd (async — flushes write buffer)
    async with      Guarantees close on exit
    async for       Yields lines
"""

from shish.streams.readers import ByteReadStream, TextReadStream
from shish.streams.writers import ByteWriteStream, TextWriteStream

__all__ = [
    "ByteReadStream",
    "ByteWriteStream",
    "TextReadStream",
    "TextWriteStream",
]
