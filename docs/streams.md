# Stream API Design

Goal: feel like Python's `open()` but async, for subprocess pipes.

## Python's open() — what makes it good

```python
# Text mode (default)
with open("file.txt") as f:
    for line in f:           # iteration
        print(line)
    f.read(10)               # partial read (chars)
    f.read()                 # read rest
    f.readline()             # one line

with open("file.txt", "w") as f:
    f.write("hello\n")      # write string
    f.writelines(lines)      # write iterable

# Binary mode
with open("file.bin", "rb") as f:
    f.read(1024)             # partial read (bytes)

with open("file.bin", "wb") as f:
    f.write(b"\x00\x01")    # write bytes
```

Key properties: mode selects text/binary, context manager ensures close,
iteration is the common case, read/write never return partial results
silently (write is all-or-error, read returns up to N).

## Why not asyncio StreamReader/StreamWriter

The transport/protocol/reader stack is a Twisted-era adapter pattern
designed for network servers. For pipes it means 5 objects (fd, transport,
protocol, reader, writer) to do what is conceptually just "async read/write
on a pipe." Additionally:

- `connect_read_pipe` requires a file-like object with `.fileno()`, not a
  raw int. `os.fdopen()` works but has `__del__` that GC-closes the fd
  (double-close risk with `OwnedFd`).
- The writer requires a dummy `StreamReader` that never receives data.
- `StreamWriter.write()` is sync (buffers), requiring separate `drain()`
  for backpressure — easy to forget.
- No benefit over raw fd handling for pipes — kernel pipe buffer already
  provides backpressure, and we never swap transports.

## Stream types

Four classes, matching the text/binary x read/write matrix:

| | Read | Write |
|---|---|---|
| **Text** | `TextReadStream` | `TextWriteStream` |
| **Bytes** | `ByteReadStream` | `ByteWriteStream` |

Two layers:

- **Byte streams** (`ByteReadStream`, `ByteWriteStream`) — non-blocking fd
  \+ event loop. No asyncio transports or protocols — just `os.read`/`os.write`
  with `add_reader`/`add_writer` for backpressure. These own the fd.

- **Text streams** (`TextReadStream`, `TextWriteStream`) — encoding/decoding
  over a byte stream. `TextReadStream` uses an incremental decoder to handle
  multi-byte characters split across read boundaries. These own their byte stream.

### Construction

```python
# Byte streams take an OwnedFd directly (no async factory)
reader = ByteReadStream(OwnedFd(fd))
writer = ByteWriteStream(OwnedFd(fd))

# Text wraps bytes
text_reader = TextReadStream(byte_reader)
text_writer = TextWriteStream(byte_writer)
```

### Usage

```python
# Read — bytes
async with ByteReadStream(OwnedFd(fd)) as reader:
    chunk = await reader.read(4096)     # up to 4096 bytes
    all_data = await reader.read()      # rest until EOF
    line = await reader.readline()      # one line
    lines = await reader.readlines()    # all remaining lines
    async for line in reader:           # iterate lines
        ...

# Read — text
async with TextReadStream(ByteReadStream(OwnedFd(fd))) as reader:
    chars = await reader.read(100)      # up to 100 characters
    line = await reader.readline()      # decoded line
    async for line in reader:           # iterate decoded lines
        ...

# Write — bytes
async with ByteWriteStream(OwnedFd(fd)) as writer:
    n = await writer.write(b"hello")    # returns byte count
    await writer.writelines([b"a", b"b"])

# Write — text
async with TextWriteStream(ByteWriteStream(OwnedFd(fd))) as writer:
    n = await writer.write("hello\n")   # returns char count
    await writer.writelines(["a\n", "b\n"])
```

## Read stream invariants

1. **EOF = empty return.** `read()` returns `""` / `b""` at EOF.
   `readline()` returns `""` / `b""` at EOF. Consistent with `open()`.

2. **read() reads all.** `read()` and `read(-1)` await until EOF,
   return everything. Same as `open().read()`.

3. **read(n) returns up to n.** May return fewer (data not yet
   available). Caller must loop if they need exactly n. Matches
   `open().read(n)` on pipes/sockets (not regular files).

4. **readline() includes newline.** Returns `"line\n"`. Last line
   before EOF may lack newline: `"partial"`. Matches `open()`.

5. **Iteration yields lines.** `async for line in stream` yields
   lines with trailing newlines, stops at EOF. Matches `for line in f`.

6. **Text mode counts characters.** `read(5)` returns 5 characters
   even if they're multi-byte in the underlying encoding. Matches
   `open("f", "r").read(5)`.

7. **Incremental decoding.** Multi-byte characters split across OS
   read boundaries are handled correctly. The decoder carries state
   between fills.

8. **No reads after EOF.** Once a read returns empty, all subsequent
   reads return empty immediately.

## Write stream invariants

1. **write() is all-or-error.** `await write(data)` writes everything
   or raises. No partial writes exposed. Returns the count (bytes or
   characters). Matches `open().write()`.

2. **Backpressure is implicit.** `await write()` loops internally with
   `os.write` + `add_writer`. When the pipe buffer is full, the coroutine
   suspends until the fd is writable. No separate `drain()` call needed.

3. **close() signals EOF.** Closing the write stream closes the pipe
   fd. The reading process sees EOF. This is the only way to signal
   "done writing".

4. **Write after close raises.** Calling write() on a closed stream
   raises immediately.

5. **Broken pipe propagates.** If the reading process exits early,
   write() raises `BrokenPipeError`. Caller decides whether to catch.

## Fd ownership

Ownership chain: text stream -> byte stream -> fd. Closing any layer
closes everything below it.

1. **Byte stream owns the fd.** `ByteReadStream(OwnedFd(fd))` takes
   ownership. Closing the stream closes the fd. No double-close —
   `OwnedFd.close()` is idempotent.

2. **Text owns bytes.** `TextReadStream.close()` closes the underlying
   `ByteReadStream`. `TextWriteStream.close()` closes the underlying
   `ByteWriteStream`.

3. **Context managers guarantee cleanup.** `async with` calls `close()`
   on exit, even on exception.

## API surface: matches open()

The rule: if `open()` has it, we have it. If `open()` doesn't, we don't.

`open()` read surface: `read`, `readline`, `readlines`, iteration, `close`.
`open()` write surface: `write`, `writelines`, `close`.

**Excluded (open() doesn't have them):**
- `readexactly()` — asyncio primitive, not in `open()`
- `readuntil()` — asyncio primitive, not in `open()`
- `writeline()` — `open()` has no `writeline()`
- `at_eof()` — `open()` has no `at_eof()`; EOF is signaled by empty return

## Implementation notes

- `ByteWriteStream.write()` accepts `collections.abc.Buffer` (bytes,
  bytearray, memoryview) — uses `memoryview` internally for zero-copy
  partial write advancement.
- `TextWriteStream.write()` encodes in `buffer_size` character chunks
  to avoid allocating the full encoded bytes for large strings.
- Buffer sizes are configurable at each layer via `buffer_size` init arg
  (default 65536).
