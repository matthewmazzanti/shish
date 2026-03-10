"""Async IO streams for subprocess pipes.

Design goal: feel like Python's open(), but async. If open() has a
method, we have the async equivalent. If open() doesn't, we don't.

Three layers, matching CPython's IO stack (FileIO → BufferedReader/Writer
→ TextIOWrapper):

    Raw IO (RawReader, RawWriter)
        Non-blocking fd + event loop. Single os.read/os.write per call
        with add_reader/add_writer for backpressure. Own the fd.

    Byte streams (ByteReadStream, ByteWriteStream)
        Userspace buffering over the raw layer. ByteWriteStream batches
        small writes and flushes when over capacity. ByteReadStream
        fills on demand. These own their raw reader/writer.

    Text streams (TextReadStream, TextWriteStream)
        Encoding/decoding over a byte stream. TextReadStream uses an
        incremental decoder to handle multi-byte characters split
        across read boundaries. These own their byte stream.

Ownership chain: text stream → byte stream → raw IO → fd. Closing any
layer closes everything below it. Context managers guarantee cleanup.

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

from shish.streams.readers import ByteReadStream, RawReader, TextReadStream
from shish.streams.writers import ByteWriteStream, RawWriter, TextWriteStream

__all__ = [
    "ByteReadStream",
    "ByteWriteStream",
    "RawReader",
    "RawWriter",
    "TextReadStream",
    "TextWriteStream",
]
