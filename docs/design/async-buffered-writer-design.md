# Async Buffered Writer: Design Survey & API Contracts

## 1. Problem Statement

We have an async `RawIOBase`-equivalent (file-descriptor-level writes, returns short counts).
We need a **buffered writer** on top that:

- Presents a clean async write interface
- Bounds memory usage to ~buffer_size
- Has well-defined behavior under cancellation and exceptions
- Works well over pipes (the primary transport)

The motivating scenario: Python programmers love to `data = f.read()` 10 GB into memory
and then push that through a write. The buffered layer must handle this without doubling
memory usage.

---

## 2. Reference Implementation Survey

### 2.1 Python `io.BufferedWriter` (sync)

The canonical reference. Key behaviors:

```python
class BufferedWriter(BufferedIOBase):
    def __init__(self, raw: RawIOBase, buffer_size=DEFAULT_BUFFER_SIZE): ...
    def write(self, b: bytes | bytearray) -> int: ...
    def flush(self) -> None: ...
    def close(self) -> None: ...
```

**write() contract:**
- Returns `len(b)` on success — always consumes the entire input.
- Internally: appends to buffer; if buffer exceeds `buffer_size`, flushes.
- **Critical memory detail**: if `len(b) > buffer_size`, CPython's `_io_BufferedWriter_write_impl`
  does the following:
  1. Flush any existing buffered data.
  2. Write `b` directly via the raw writer in a loop (bypassing the buffer).
  - So the buffer itself stays bounded, but the *input* `b` is held by the caller.
  - No defensive copy of oversized writes — good.
- If `len(b) <= remaining buffer space`, memcpy into the buffer, return immediately.
- If `len(b) <= buffer_size` but doesn't fit: flush existing buffer, then copy `b` into (now-empty) buffer.

**flush() contract:**
- Drains the internal buffer to `raw.write()` in a loop.
- Handles short writes from raw (loops until all buffered bytes written).
- On error mid-flush: **bytes already written to raw are lost from the buffer** — the buffer
  position advances. Remaining unwritten bytes are shifted to buffer start. This means a
  partial flush is not retried from the beginning.

**Error / exception model:**
- `BlockingIOError` — not raised in normal buffered mode but relevant for non-blocking raw.
- `OSError` — propagated from raw. Once `raw.write()` raises, the buffer may be in a
  partially-flushed state (some bytes written, some retained).
- No "poisoned" state — you can retry writes after an error, and the buffer contains
  whatever wasn't flushed.

**Limitations for our purposes:**
- Entirely synchronous — no cancellation concept.
- `write()` may block indefinitely inside `flush()` with no way to interrupt.


### 2.2 Python `asyncio.StreamWriter` (async, pipes/sockets)

```
class StreamWriter:
    def write(self, data: bytes) -> None:        # sync! not a coroutine
    async def drain(self) -> None:
    def close(self) -> None:
    async def wait_closed(self) -> None:
```

**write() contract:**
- **Synchronous**, non-blocking. Appends to the transport's write buffer.
- Returns `None`, not a byte count. Always "succeeds" immediately.
- **No backpressure** — repeated calls without `drain()` grow memory without bound.
- The transport may eventually pause reading on the other side, but the write buffer is
  unbounded in the writer itself.

**drain() contract:**
- Awaits until the transport's write buffer is below the low-water mark.
- This is where backpressure finally happens, but it's opt-in and easily forgotten.
- Can raise `ConnectionResetError` if the peer closed.

**Cancellation:**
- `drain()` is the only cancellation point. If cancelled during drain, buffered data
  remains in the transport buffer and will still be sent (transport handles it).
- `write()` can't be cancelled (it's sync).

**Key takeaway:**
The split write/drain API avoids async overhead per write but creates a
**memory-unbounded footgun**. This is exactly what we want to avoid.


### 2.3 Go `bufio.Writer`

```go
type Writer struct {
    err error    // sticky error
    buf []byte
    n   int      // bytes buffered
    wr  io.Writer
}

func (b *Writer) Write(p []byte) (nn int, err error) { ... }
func (b *Writer) Flush() error { ... }
```

**Write() contract:**
- If `b.err != nil` (sticky error from prior flush failure), returns `0, b.err` immediately.
- If `len(p) > b.Available() && b.Buffered() == 0 && len(p) >= len(b.buf)`:
  **write-through** — writes `p` directly to underlying writer, skipping the buffer entirely.
  This avoids allocating/copying when write > buffer size.
- Otherwise: copy as much of `p` as fits into buffer, flush when full, repeat.
- Returns total bytes buffered/written. Can return `nn < len(p)` only if an error occurs.

**Sticky error model:**
- Once the underlying writer returns an error, `b.err` is set.
- **All subsequent Write() and Flush() calls return the error without doing anything.**
- To recover, you must call `b.Reset(w)` with a new (or same) writer.
- This is extremely conservative but simple: no partial-state reasoning required.

**Memory behavior:**
- Buffer is allocated once at construction (`make([]byte, size)`).
- Never grows. Write-through for oversized writes avoids the copy.
- Memory bound: `buffer_size` + whatever the caller holds.

**Relevance:** The sticky error model is appealing for its simplicity. The write-through
optimization for large writes is elegant and avoids the memory concern directly.


### 2.4 Rust `tokio::io::BufWriter` (async)

```rust
impl<W: AsyncWrite> BufWriter<W> {
    pub fn new(inner: W) -> BufWriter<W>;
    pub fn with_capacity(cap: usize, inner: W) -> BufWriter<W>;
}

// Via AsyncWrite trait:
fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8])
    -> Poll<io::Result<usize>>;
fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>)
    -> Poll<io::Result<()>>;
fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>)
    -> Poll<io::Result<()>>;
```

**poll_write() contract:**
- If `buf.len() >= capacity` (and internal buffer is empty or flushed): delegates directly
  to inner `poll_write`. Write-through, same as Go.
- If data fits: copies into internal buffer, returns `Ready(Ok(n))` immediately.
- If data doesn't fit but is < capacity: initiates flush of existing buffer, then buffers
  new data. If flush returns `Pending`, the whole `poll_write` returns `Pending` — caller
  retries later.
- **Returns short counts** — may buffer fewer bytes than offered. Callers use
  `AsyncWriteExt::write_all()` to loop.

**Cancellation (the key insight for us):**
- Because the interface is poll-based, cancellation == "stop calling poll".
- If you stop mid-flush, the BufWriter retains its state: some bytes flushed, some not.
  Next poll_write/poll_flush resumes from where it left off.
- **No data is lost on cancellation** — the buffer is always in a consistent state.
- The `write_all` future, however, is NOT cancellation-safe: if you cancel a `write_all`,
  you don't know how many bytes from the *caller's* buffer were consumed. The BufWriter's
  internal buffer is fine, but the caller's position is lost.

**Memory behavior:**
- Fixed-size internal `Vec<u8>` (capacity set at construction).
- Write-through for large writes (same pattern).
- `poll_write` never allocates beyond initial capacity.

**Relevance:** The poll-based model gives the strongest cancellation story. The critical
observation is that **cancellation-safety lives at the BufWriter level, but write_all
convenience wrappers are NOT cancellation-safe** by design. This is the exact tension we
need to resolve.

---

## 3. Deep Dive: Memory Bounds

### Goal: Never allocate more than buffer_size (in the writer itself)

All four reference implementations converge on the same strategy:

| Scenario | Strategy |
|---|---|
| `len(data) <= available` | memcpy into buffer, return immediately |
| `len(data) <= buffer_size` but `> available` | flush buffer, then copy data into now-empty buffer |
| `len(data) > buffer_size` | flush buffer, then **write-through** directly to raw |

The write-through path is the key to the memory bound. Without it, you'd need to either:
- Copy the oversized write into the buffer (buffer grows past `buffer_size`), or
- Loop slicing the input into buffer-sized chunks (extra copies, still bounded)

**Decision:** Implement write-through for `len(data) > buffer_size`. The raw writer's
`write()` may return short counts, so this needs a loop, but the caller's `data` buffer
serves as the source — no extra allocation.

### Scatter-gather alternative (considered and rejected)

An alternative design was considered: instead of a flat buffer, maintain a deque of
`_Chunk(memoryview, pos)` descriptors that reference the caller's original data. This
gives zero-copy on all paths — `write()` just appends a memoryview reference, and the
flush loop drains chunks in order.

**Why it was rejected:**

This creates a fundamental ownership problem. When `write()` stores a memoryview into
the caller's data, it pins that data in memory until flush completes. If the caller's
frame unwinds (e.g., from `CancelledError`), the data that would normally be collected
stays alive via the writer's reference. Consider:

1. Caller holds 10 GB `bytes` object, calls `write(data)`.
2. Writer stores `memoryview(data)`, starts draining.
3. `CancelledError` — caller's frame unwinds. Normally `data` would be collected.
4. But the writer's `_chunks` deque holds a memoryview, pinning the 10 GB.
5. Next caller arrives with another 10 GB. Writer must drain old pending first.
6. Two 10 GB allocations live simultaneously. Back to 20 GB.

We're back to 20 GB — not through memcpy, but through reference pinning. This is the
same unbounded growth as `asyncio.StreamWriter`, disguised as a different mechanism.

Rust avoids this via ownership transfer — the type system prevents the caller from
dropping data while the writer holds it. Python has no equivalent.

---

## 4. Deep Dive: Pipe Exception Landscape

Writes to pipes surface a specific set of OS errors. Here's the taxonomy:

### 4.1 POSIX pipe/FIFO write errors

| errno | Python exception | When |
|---|---|---|
| `EPIPE` | `BrokenPipeError` | Reader closed the pipe. Also triggers SIGPIPE (which Python ignores by default via `SIG_IGN` in `Py_InitializeMain`). |
| `EAGAIN` / `EWOULDBLOCK` | `BlockingIOError` | Pipe buffer full, fd is `O_NONBLOCK`. The async event loop uses this internally — your raw layer should convert this to "not ready" / return 0 / re-register for writability. |
| `EFBIG` | `OSError(errno=27)` | Write would exceed file size limit. Not typical for pipes but possible with `ulimit -f`. |
| `ENOSPC` | `OSError(errno=28)` | For named pipes on filesystems? Very rare for anonymous pipes. |
| `EIO` | `OSError(errno=5)` | General I/O error. Can occur with pty pairs. |
| `EINTR` | — | Signal interrupted the write. Python's C-level `write()` wrapper retries automatically on `EINTR` (PEP 475). Shouldn't reach your code. |
| `EINVAL` | `OSError(errno=22)` | fd not open for writing, or unaligned write. Indicates a bug. |
| `EBADF` | `OSError(errno=9)` | Bad file descriptor. Bug or fd closed out from under you. |

### 4.2 Categorization for the buffered writer

**Retryable (transient):**
- `EAGAIN`/`EWOULDBLOCK` — by definition, this means "try again later". The async raw layer
  should handle this transparently (re-register with event loop, await writability).
  Should never reach the buffered layer.

**Fatal-but-expected (peer-driven):**
- `EPIPE` / `BrokenPipeError` — The normal "other end went away" signal. The buffered
  writer should **stop trying** after this. Data in the buffer is undeliverable.
  For pipes, retrying after EPIPE is almost always wrong — the pipe is permanently dead.

**Fatal-unexpected (bugs or system issues):**
- `EBADF`, `EINVAL`, `EIO` — these indicate programming errors or hardware issues.
  Propagate immediately, don't retry.

### 4.3 Practical pipe buffer sizes (context for tuning)

- Linux: pipe capacity defaults to 64 KiB (16 pages), adjustable per-fd via
  `fcntl(F_SETPIPE_SZ)` up to `/proc/sys/fs/pipe-max-size` (default 1 MiB).
- macOS: 16 KiB for most pipes, 64 KiB if first write is > 16 KiB (big-pipe optimization).
- Atomic write guarantee: POSIX guarantees `PIPE_BUF` (typically 4096) bytes written
  atomically. Writes larger than `PIPE_BUF` may be interleaved with other writers.

---

## 5. Deep Dive: Cancellation Semantics

This was the hardest design question. Python's async cancellation model creates a
fundamental four-way constraint that cannot be simultaneously satisfied:

### 5.1 The four-way constraint

1. **Zero-copy** (bounded memory — don't duplicate the caller's data)
2. **Cancellable** (respect `CancelledError`)
3. **Resumable** (no data loss on cancel — all bytes eventually delivered)
4. **Don't pin caller's memory** (let caller's data be GC'd after write returns/raises)

**Pick three.** Every implementation in the survey makes a different trade:

| Implementation | Gives up | Mechanism |
|---|---|---|
| asyncio StreamWriter | 1 (unbounded memory) | Copies into transport buffer, grows without limit |
| Go bufio.Writer | 2 (not cancellable) | Sticky error, no mid-write interruption |
| Rust tokio BufWriter | 4 (pins caller memory) | Ownership transfer enforced by type system |
| CPython BufferedWriter | 2 (not cancellable) | Synchronous, blocks until done |

Rust gets away with pinning because the type system prevents the caller from dropping data
while the writer holds it. Python has no equivalent — the caller's `CancelledError` unwinds
the frame, but if the writer holds a memoryview, the data is silently pinned.

### 5.2 Why short-write-on-cancel doesn't work in Python

An early design attempted Rust-style short counts: `write()` returns the number of bytes
consumed, and on cancellation you get a partial count. The problem is that `CancelledError`
is an exception — it unwinds the stack. You can't simultaneously raise an exception and
return a value.

Options explored:

- **Stash on `self._last_write_pos`**: Side-channel return value. Horrible API, easy to forget.
- **Attach to exception `e.bytes_written = n`**: Fragile, lost if exception is wrapped/replaced.
- **Suppress cancel and return**: Breaks `asyncio.timeout()`, `TaskGroup`, everything.

None of these are acceptable. Short-write-on-cancel is a Rust concept that doesn't
translate to Python's exception-based cancellation model.

### 5.3 Why resumable writes re-create unbounded memory

A resumable design stores a reference to the caller's data on `self._pending` so it can
resume draining after cancellation. But this pins the caller's data:

1. Caller holds 10 GB `bytes`, calls `write(data)`.
2. Writer stores `memoryview(data)` in `self._pending`, starts draining.
3. `CancelledError` — caller's frame unwinds. Data stays alive via `_pending`.
4. Next caller arrives with another 10 GB. Writer must drain old pending first.
5. Two 10 GB allocations live simultaneously. Back to 20 GB.

This is the same unbounded growth as `asyncio.StreamWriter`, disguised as a reference
rather than a copy.

### 5.4 Decision: bounded memory + cancellable, accept data loss on cancel

We choose constraints 1 and 2: **bounded memory and cancellable**. The trade is:

- Cancel may stop a write mid-way. Bytes already written to raw are gone.
  Bytes not yet written are lost. The writer's internal buffer is consistent.
- This is appropriate because cancel typically means "I don't care about this data
  anymore" — tearing down a connection, aborting a request, hitting a timeout.

This dramatically simplifies the design: no `_pending`, no `_Chunk` deque, no phase
tracking, no resume logic, no memoryview pinning. The writer holds no references to
caller data across await points (except transiently within a single `raw.write()` call).

---

## 6. Final Implementation

The design was iterated extensively during implementation. This section documents
the actual API as built, noting where and why it diverges from what the survey
suggested.

### 6.1 Architecture: two layers

```
ByteWriteStream          ← buffered, async, owns the fd
  └── _DirectWriter      ← unbuffered, async, single os.write per call
        └── Fd            ← owned fd, close-once semantics
```

`_DirectWriter` is a concrete class (not a Protocol). It handles non-blocking I/O
via `os.write` + `loop.add_writer`. A single `write()` call performs one `os.write`
and returns the actual byte count (may be short). `BlockingIOError` is handled
internally by suspending on `add_writer` — callers never see it.

`ByteWriteStream` adds buffering on top. `TextWriteStream` adds encoding on top
of that.

### 6.2 Contracts

**Memory contract** (unchanged from survey):
The writer allocates exactly `buffer_size` bytes at construction. This buffer
never grows. Writes larger than `buffer_size` bypass the buffer entirely
(write-through to raw). The writer never holds a reference to the caller's data
across await boundaries beyond the current raw `write()` call.

**Cancellation contract** (unchanged from survey):
`CancelledError` may interrupt any await point. On cancellation the writer's
internal buffer is in a consistent state, an unknown amount of data may have
been written to raw, the current `write()`'s data is partially lost, and the
writer is NOT poisoned — subsequent writes work normally.

**Concurrency contract** (new — diverges from survey):
An `asyncio.Lock` serializes `write()` and `flush()` so multiple tasks can
safely call `write()` without interleaving buffer mutations. See §8 for the
rationale.

**Error contract** (simplified — diverges from survey):
`OSError` propagates directly from the raw writer. There is no sticky error
state. After an `OSError`, the buffer is in a consistent state (partial flush
progress preserved via `_flush()`'s `finally` block) and the writer remains
usable. See §8 for why sticky errors were dropped.

### 6.3 write() algorithm

The `write()` method is a flat three-step sequence, not a loop:

1. **Make room**: If the buffer has data and the new write wouldn't fit alongside
   it, flush first. After flush, the buffer is empty.
2. **Buffer**: If `length <= buffer_size`, copy into the buffer and return. This
   covers both the fast path (data fits in available space, no flush needed) and
   the post-flush path (buffer was just drained). No await on this path.
3. **Write-through**: If `length > buffer_size`, the buffer is guaranteed empty
   (step 1 flushed it if needed). Write directly to raw in a loop. No copy.

The original design used a `while pos < len(view)` loop with `continue` after
flush to re-evaluate. The flat version is equivalent but easier to reason about:
after step 1, exactly one of step 2 or step 3 applies.

### 6.4 _flush() cancel safety

`_flush()` tracks `pos` (bytes successfully written to raw) and uses a `finally`
block to shift unwritten bytes to the front of the buffer on every exit path:

- **Success**: `pos == _buf_len`, `remaining == 0`, buffer is effectively empty.
- **OSError mid-flush**: `pos < _buf_len`, unwritten bytes shifted to `buffer[0:]`.
- **CancelledError mid-flush**: Same as OSError — `finally` runs, buffer compacted.

The `memoryview` on the buffer is scoped with `with` so it's released before the
buffer mutation in `finally`. This prevents `BufferError` from export locks (a
live memoryview on a bytearray prevents the bytearray from being resized — while
we don't resize today, releasing it avoids a confusing failure if a resize path
is ever added).

### 6.5 __aexit__ and teardown

`__aexit__` inspects `exc_type` to choose the teardown path:

- **Normal exit or `Exception`**: `await self.close()` — flush buffer, then close fd.
- **`BaseException` (not `Exception`)**: `self.close_fd()` — close fd immediately,
  skip flush. This covers `CancelledError`, `KeyboardInterrupt`, and
  `SystemExit`.

The check uses `not issubclass(exc_type, Exception)` rather than checking for
`CancelledError` specifically, so all non-recoverable BaseExceptions skip the
flush. Flushing during cancellation would fight the event loop's teardown and
risk hanging.

This eliminates the need for a separate `discard()` method — callers use
`async with` and the context manager handles cleanup correctly for all exit paths.
`spawn.py` was simplified from manual try/except/finally with discard/close to
plain `async with`.

---

## 7. Internal State Machine

All state lives in one variable: `_buf_len`. No phase enum, no error flag.
The `finally` block in `_flush()` guarantees `_buf_len` is correct on every
exit path. The `asyncio.Lock` ensures only one task mutates the buffer at a time.

On `write(data)`:

- **len ≤ buffer_size** (bufferable):
  - Fits alongside existing data → memcpy into buffer, return. No await.
  - Doesn't fit → flush buffer, then memcpy into now-empty buffer, return.
- **len > buffer_size** (write-through):
  - Flush buffer if non-empty, then write directly to raw in a loop.
  - Cancel here: partial data written to raw, rest lost. Writer state clean.

On `flush()`:
- Drain buffer to raw in a loop. Partial progress tracked by `pos`.
- On any exit (success, OSError, CancelledError): shift unwritten bytes to front.

---

## 8. Key Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Memory bound | Write-through for oversized data | All four references converge on this (§3) |
| Cancellation | Allow, accept data loss | Cancel means "don't care about this data" (§5.4) |
| Return type of write() | `int` (always `len(data)`) | Compatible with `io.BufferedIOBase.write()` |
| Buffer strategy | Fixed `bytearray` + `_buf_len` + memmove | See "Why not growable bytearray" below |
| Scatter-gather | Rejected | Pins caller memory, re-creates unbounded growth (§3) |
| Short-write-on-cancel | Rejected | Can't return a value and raise an exception (§5.2) |
| Resume-after-cancel | Rejected | Requires holding reference to caller data (§5.3) |
| memoryview in _flush | `with` block, released before mutation | Zero-copy slicing; prevents export lock |
| __aexit__ | BaseException-aware | Skip flush on cancel/interrupt, flush on normal exit |

### Changes from the original proposal

**Sticky errors: dropped.** The survey (§2.3) highlighted Go's sticky error as
appealing for simplicity. During implementation we found it adds complexity
without saving meaningful work. The OS enforces the same behavior: writing to a
dead pipe raises `BrokenPipeError` every time. A sticky flag just caches what
the OS already tells you. Dropping it removes `_error`, `error` property, and
`reset()` — three fewer things to test, document, and get wrong. The buffer
stays consistent on OSError via `_flush()`'s `finally` block regardless.

**asyncio.Lock: added.** The original proposal specified single-task use. During
implementation we decided the API surface resembles Python's `logging` module —
multiple tasks writing to the same stream is a natural usage pattern. The Lock
serializes `write()` and `flush()` to prevent interleaved buffer mutations. Cost
is one uncontended lock acquisition per write (fast path). The Lock is not held
across the entire `_flush()` drain — it's acquired in the public `write()` and
`flush()` methods, which call `_flush()` internally.

**close_fd() instead of discard().** The original design had `discard()` to drop
buffered data. In practice, `discard()` was always followed by `close()`. The
combined operation — "close the fd without flushing" — is `close_fd()`. This
simplified `spawn.py` from manual try/except/finally with discard/close to
plain `async with`, since `__aexit__` calls `close_fd()` on BaseException.

**write() algorithm: flat three-step instead of while loop.** The original
proposal used a `while pos < len(view)` loop with `continue` after flush to
re-evaluate whether data fits. The flat version (flush → buffer → write-through)
is equivalent but each step has a clear single responsibility. After flushing,
the buffer is empty, so exactly one of buffer-or-write-through applies —
no need to re-evaluate in a loop.

**Why not a growable bytearray.** We experimented with removing `_buf_len` in
favor of a growable `bytearray` (using `len()` as the implicit size tracker,
`extend()` to append, `del[:n]` to compact). This failed: exception tracebacks
pin `memoryview` slices passed to `_DirectWriter.write()`. When `OSError` is
raised, the traceback keeps the frame alive, which holds a memoryview slice of
the buffer. This export lock prevents `clear()`/`del` on the bytearray, raising
`BufferError: Existing exports of data: object cannot be re-sized`. The fixed-
size buffer with external `_buf_len` tracker never resizes, so export locks are
irrelevant — slicing a fixed buffer is always safe.

