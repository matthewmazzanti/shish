# Write Path Performance Analysis

Investigation into per-call overhead of the async write stack, comparing
optimization strategies and identifying where time is spent.

## Cost breakdown

Measured on Linux (CPython 3.13), writing to `/dev/null` or pipes with
`cat > /dev/null` as the reader.

### Baseline costs (per call)

| Operation | Cost | Notes |
|---|---|---|
| sync function call | ~50ns | Pure Python call + return |
| async function call | ~95ns | Coroutine frame alloc + send + dealloc |
| memoryview alloc + release | ~90ns | On top of any call |
| async + memoryview | ~190ns | Floor for any async write accepting Buffer |
| `os.write()` to /dev/null | ~450ns | Kernel syscall, data size doesn't matter |
| `asyncio.Lock` `acquire()` | ~163ns | 1 coroutine frame (uncontended) |
| `async with lock` | ~395ns | 3 coroutine frames (__aenter__ + acquire + __aexit__) |
| Per coroutine frame | ~116ns | Measured via lock overhead breakdown |

### Writer variant costs (per call, /dev/null)

| Variant | 32B | 128B | 1KiB | Notes |
|---|---|---|---|---|
| `os.write()` | ~450ns | ~450ns | ~450ns | Flat — syscall dominates |
| v1 (async def, always locked) | ~2μs | ~1.1μs | ~1.2μs | Lock + RawWriter frames |
| v1c (lock bypass, inlined, try-flush) | ~1μs | ~500ns | ~700ns | Fast path avoids lock |
| C buffered writer | ~228ns | ~235ns | ~314ns | tp_iternext + memcpy + StopIteration |
| unbuffered | ~1.2μs | ~880ns | ~930ns | No buffer, one async frame + syscall |
| asyncio StreamWriter | ~2.1μs | ~1.1μs | ~1.1μs | Transport buffering, flat |

### Pure fast-path cost (huge buffer, no flush ever triggered)

| Variant | 32B | 128B | 1KiB |
|---|---|---|---|
| v1 (always locked) | 1526ns | 1094ns | 1401ns |
| v1c (lock bypass) | 619ns | 621ns | 829ns |
| raw `buf[pos:pos+n] = chunk` | 181ns | 222ns | 663ns |

v1c fast-path overhead at small writes: **~400-440ns** of pure Python
(async frame, memoryview, lock check, property access, bounds check,
slice assign bookkeeping) on top of the raw memcpy.

## Where time goes

For v1c at 32B (fast path, no flush):
- ~95ns: async coroutine frame alloc/send/dealloc
- ~90ns: memoryview alloc + release
- ~50ns: `self.closed` property access
- ~50ns: `self._lock.locked()` method call
- ~50ns: bounds check, `len(view)`
- ~180ns: slice assignment bookkeeping (`self._buffer[a:b] = view`)
- **Total: ~515ns** (measured ~619ns — rest is attribute lookups, etc.)

For unbuffered at 32B:
- ~190ns: async frame + memoryview (unavoidable)
- ~450ns: `os.write()` syscall
- ~260ns: attribute lookups, try/except, `self._fd.fd`, while loop
- **Total: ~900ns**

## Optimization strategies explored

### 1. Lock bypass (v1b, v1c)

Check `not self._lock.locked()` before the fast path. Since asyncio is
single-threaded, code with no await/yield points is atomic — if the lock
isn't held, no other coroutine can be mid-flush. Saves ~395ns (3 coroutine
frames of `async with lock`) on every uncontended write.

**Result:** v1 ~2μs → v1b/v1c ~500-620ns at small writes. The lock bypass
is the single largest optimization.

### 2. Sync try-flush (v1c)

When the buffer is full but the fd is writable, drain synchronously via
`os.write()` without yielding. Avoids lock acquisition and event loop
interaction when writes would succeed immediately.

**Result:** Improves pipe throughput at medium sizes. v1c beats `os.write()`
at 32B pipe throughput (58 vs 36 MiB/s) due to syscall batching.

### 3. Inlined raw writes (v1c)

Eliminate the separate `RawWriter` layer — `ByteWriteStream` owns the fd
directly and calls `os.write()`. Saves one method dispatch and attribute
lookup per flush.

### 4. Generator coroutines (v5)

`@types.coroutine` generators: `return` resolves immediately (~100ns),
`yield from` suspends to event loop. Slightly less overhead than async def
(~100ns vs ~120ns per frame).

**Result:** v5 ≈ v1b in benchmarks. The generator doesn't enable the
optimization — the lock bypass does. ~20ns savings is noise.

### 5. `int | Coroutine` return (v2)

`RawWriter.write()` returns `int` directly on fast path, callers branch
with isinstance. Lowest overhead (~485ns, 45ns above `os.write()`).

**Result:** Fastest raw writer, but leaks branching to all callers.
Not composable — can't just `await` it.

### 6. Custom awaitable objects (v3, v4)

`_Resolved` class with `__await__` that raises `StopIteration(value)`
immediately. Fresh per call (v3, ~990ns) or reused/mutated (v4, ~855ns).

**Result:** Both slower than async def (~575ns). The `__await__` →
`__next__` → `StopIteration` protocol through Python method dispatch
is more expensive than CPython's optimized coroutine send path.

### 7. C extension awaitable (_cbufwriter.c)

Custom C type with `tp_iternext` for the complete buffered writer. On the
fast path (data fits in buffer): tp_iternext → memcpy → StopIteration(len).
No Python frames. Three-phase state machine: Initial (buffer), Flushing
(drain internal buffer), WriteThrough (large data bypasses buffer).

**Result:** ~228-260ns at small writes (/dev/null). **~3x faster than v1c**,
within ~2x of C stdio. Pipe throughput reaches 5.5 GiB/s at 8KiB, beating
C stdio.

### 8. Rust/PyO3 port investigation

Ported `_cbufwriter.c` to Rust using PyO3 bindings. Fully working
implementation with the same awaitable protocol and buffer strategy.

**Result:** ~260-320ns at small writes — ~30-40% slower than C on the fast
path. The gap is in PyO3's awaitable protocol overhead, not the write
logic itself. **Decision: stay with C.**

## C vs Rust/PyO3 awaitable overhead

Built noop awaitables (immediately raise StopIteration) in both C and Rust
to isolate protocol overhead. Pre-allocated objects, /dev/null, 500K
iterations.

### Protocol floor comparison

| Layer | C | Rust/PyO3 | Delta |
|---|---|---|---|
| Awaitable floor (noop) | ~96 ns | ~135 ns | +39 ns |
| + clone_ref + borrow_mut | — | +3 ns | negligible |
| + memcpy (256B) | — | +45 ns | actual work |
| + full write logic (256B) | — | +73 ns | state machine + buffer protocol |

### Incremental overhead benchmark

```
  Size    c noop    rust noop       borrow          copy       full write
------  --------  -----------  -----------  -----------  ----------------
  64 B    199 ns  265 ns (+67)  355 ns (+90)  443 ns (+87)   384 ns (+29)
 256 B    100 ns  140 ns (+40)  190 ns (+50)  235 ns (+45)   263 ns (+73)
1024 B     96 ns  137 ns (+41)  179 ns (+42)  241 ns (+62)  319 ns (+140)
4096 B     95 ns  132 ns (+37)  182 ns (+49)  267 ns (+85)  509 ns (+327)
```

### Why PyO3 is slower

PyO3's tp_iternext slot goes through a generated wrapper:

1. Wrapper checks borrow flag, sets it, creates `PyRefMut`
2. Wrapper calls Rust `__next__`
3. Rust returns `Err(PyStopIteration::new_err(0))` — Rust `PyErr` struct
4. Wrapper catches `Err`, calls `pyerr.restore(py)` → `PyErr_Restore`
5. Wrapper releases borrow flag, returns NULL

C calls `tp_iternext` directly — no intermediate layer, no borrow tracking.
The 39 ns is structural, not transient. It doesn't close with more iterations.

### Rust alternatives considered

- **Raw pointer instead of `clone_ref`**: Replaced `Py<ByteWriteStream>` with
  `*mut ByteWriteStream`. Required `unsafe impl Send + Sync`. Measured no
  improvement (~3 ns was already noise). Rolled back — not worth the unsafe.

- **pyo3-ffi for hot path**: Write raw `tp_iternext` in Rust using C API
  directly, keep PyO3 for cold paths (constructors, send/throw/close). Would
  close the 39 ns gap but loses the safety benefits that motivated the port.

### Rust implementation notes

Key patterns discovered during the port (reference for future PyO3 work):

- **`clone_ref` + `borrow_mut`**: Access parent pyclass from child's
  `__next__`. `Py::clone_ref(py)` creates an independent handle (Py_INCREF),
  then `borrow_mut(py)` does runtime borrow checking. Cost: ~3 ns.

- **Raw pointer for buffer data**: `PyBuffer::buf_ptr()` → `from_raw_parts()`
  breaks the borrow chain so the data slice lifetime is independent of `&self`,
  allowing mutable access to other fields.

- **Caching fields on awaitables**: Copy `loop_obj`/`fd` from parent to child
  to avoid borrowing the parent for event loop operations in send/throw/close.

- **Two `impl` blocks per pyclass**: Standard PyO3 — plain `impl` for Rust
  internals, `#[pymethods]` for Python API.

- **`_asyncio_future_blocking`**: Private CPython attribute on Future objects,
  must be set to `True` before yielding from custom awaitables. No public API;
  stable since Python 3.4.

## C extension analysis

### What's in C vs Python in asyncio

| Component | Implementation |
|---|---|
| `Future` (set_result, __await__, callbacks) | **C** (`_asyncio.Future`) |
| `Task` | **C** (`_asyncio.Task`) |
| `get_running_loop()` | **C** |
| Event loop (add_writer, remove_writer, create_future) | **Python** (`selector_events.py`) |
| Selector (epoll/kqueue registration) | **Python** |

The event loop itself is pure Python. A C extension can construct
`_asyncio.Future` directly (C type constructor), but `add_writer` /
`remove_writer` always cross into Python. The C awaitable's win is
entirely on the fast path — when the fd is writable and no event loop
interaction is needed.

### Slow path cost (fd blocks)

When `write()` returns EAGAIN, the slow path requires:
1. `loop.create_future()` — Python method → C Future init
2. `loop.add_writer(fd, future.set_result, None)` — pure Python + selector
3. yield Future → event loop polls → callback fires
4. `loop.remove_writer(fd)` — pure Python + selector

Steps 2-4 are ~1-2μs of Python calls. C can't shortcut this without
reimplementing the selector loop. The optimization strategy is to minimize
how often you hit this path (bigger buffer, sync try-flush).

### Level-triggered wakeups

`add_writer` uses level-triggered selectors (epoll/kqueue default). The
selector reports "fd is writable" on every poll iteration while the fd
has buffer space. This is why current code does `remove_writer` immediately
after resume — otherwise the callback fires spuriously every loop iteration.

A persistent registration (register once, swap futures on each EAGAIN)
would save add/remove churn during sustained back-pressure, but causes
spurious callbacks when not waiting. Edge-triggered (`EPOLLET`) would fix
this but asyncio's selector loop doesn't support it.

## Throughput: pipe to `cat > /dev/null`

10,000 writes per scenario.

### Python writers

| Size | os.write() | print() | file.write() | asyncio | v1 locked | v1c bypass | unbuf |
|---|---|---|---|---|---|---|---|
| 32B | 36 MiB/s | 56 MiB/s | 146 MiB/s | 21 MiB/s | 15 MiB/s | 58 MiB/s | 24 MiB/s |
| 128B | 153 MiB/s | 186 MiB/s | 461 MiB/s | 78 MiB/s | 56 MiB/s | 190 MiB/s | 76 MiB/s |
| 512B | 617 MiB/s | 583 MiB/s | 849 MiB/s | 303 MiB/s | 179 MiB/s | 629 MiB/s | 268 MiB/s |
| 1KiB | 1.3 GiB/s | 876 MiB/s | 1.1 GiB/s | 580 MiB/s | 336 MiB/s | 875 MiB/s | 618 MiB/s |
| 8KiB | 5.0 GiB/s | 2.7 GiB/s | 3.6 GiB/s | 2.4 GiB/s | 1.3 GiB/s | 2.2 GiB/s | 4.9 GiB/s |
| 64KiB | 3.5 GiB/s | 4.0 GiB/s | 3.8 GiB/s | 3.3 GiB/s | 3.9 GiB/s | 4.1 GiB/s | 3.8 GiB/s |

### C buffered writer (pipe throughput)

| Size | bytes.write() | v1c | C awaitable | Rust/PyO3 awaitable |
|---|---|---|---|---|
| 64B | 482 MiB/s | 96 MiB/s | — | 195 MiB/s |
| 256B | 1.0 GiB/s | 353 MiB/s | — | 403 MiB/s |
| 1KiB | 1.9 GiB/s | 1.0 GiB/s | — | 1.1 GiB/s |
| 4KiB | 3.3 GiB/s | 1.8 GiB/s | — | 2.5 GiB/s |
| 8KiB | 5.2 GiB/s | 2.4 GiB/s | — | 5.5 GiB/s |

Key observations:
- **v1c beats `os.write()` at 32B** (58 vs 36 MiB/s) — syscall batching wins
- **`file.write()` is 3-5x faster than v1c at small writes** — C stdio buffering
  with zero Python per-write overhead is the real competition
- **All converge at 64KiB** — kernel pipe buffer (64KiB) becomes the bottleneck
- **unbuf wins at 8KiB+** — no buffer copy overhead, straight to fd
- **Pipe throughput peaks at 8KiB** then drops — larger writes exceed the pipe
  buffer, requiring cross-process scheduling

## Throughput model

With buffer_size B, write_size W, fast-path cost F, and syscall cost S (~450ns):

- Buffered: `writes_per_flush = B / W`, amortized cost = `F + S / (B/W)` per write
- Unbuffered: cost = `F + S` per write (syscall every call)

At small writes (W << B), the syscall amortizes to ~0. Per-call overhead F
dominates entirely.

### Calibrating C fast-path cost

Initial estimate of 50ns (bare `tp_iternext` → memcpy → StopIteration)
was too optimistic. Measured `bytes.write()` (C stdio, raw bytes, no
encoding) gives the real floor for a C buffered writer:

| | Per-call cost | Source |
|---|---|---|
| v1c Python | ~620ns | measured fast-path bench |
| C stdio (`bytes.write()`) | ~242ns | measured pipe throughput |
| C awaitable (estimated) | ~200ns | slightly faster than stdio (no Python method dispatch) |
| Bare tp_iternext (theoretical) | ~50ns | ignores buffer protocol overhead |
| C noop awaitable (measured) | ~96ns | bare StopIteration, no work |

C stdio's ~242ns includes: Python method dispatch (~50ns), `PyObject_GetBuffer`,
internal bookkeeping, memcpy. A C awaitable doing `tp_iternext` → buffer protocol
→ bounds check → memcpy → StopIteration would skip the method dispatch but still
pay buffer protocol + memcpy. Estimated ~200ns.

## Conclusions

1. **Per-call Python overhead dominates at small writes.** The ~620ns
   fast-path cost (async frame + memoryview + checks) is the bottleneck,
   not the syscall or memcpy.

2. **Lock bypass is the biggest single win.** Avoiding `async with lock`
   saves ~395ns (3 coroutine frames). Other Python-level optimizations
   (generators, custom awaitables) save 20-50ns — noise by comparison.

3. **C fast path gives ~3x at small writes.** Reducing per-call
   overhead from ~620ns to ~200ns. Calibrated against measured C stdio
   performance (`bytes.write()` at ~242ns/call). Real-world gains capped
   by pipe throughput at ~4-6 GiB/s.

4. **Rust/PyO3 adds ~39 ns over C.** The PyO3 wrapper layer (borrow
   checking, error conversion, function indirection) is structural and
   irreducible without dropping to pyo3-ffi. Not worth the unsafe surface
   area for a ~20% improvement on the protocol floor.

5. **C only helps on the fast path.** The slow path (fd blocks) requires
   `add_writer`/`remove_writer` which are pure Python in asyncio's event
   loop. No way to shortcut from C without replacing the loop.

6. **Buffering vs unbuffered is a tradeoff.** Buffering wins at <512B
   (batches syscalls), unbuffered wins at >2KiB (no copy). Crossover at
   ~1-2 KiB. For shell output (lines of text), buffering is usually better.

7. **C stdio is the real ceiling.** `bytes.write()` at 126 MiB/s (32B)
   represents the practical limit for a C buffered writer going through
   Python's object protocol. A C awaitable would be slightly faster
   (~152 MiB/s projected) by avoiding Python method dispatch.

## Files

Benchmark scripts in `benchmarks/`:
- `bench_call_overhead.py` — function call + memoryview baseline costs
- `bench_lock.py` — asyncio.Lock overhead breakdown
- `bench_rawwriter.py` — C vs Python RawWriter (async def, Py awaitable, C awaitable)
- `bench_syscall.py` — os.write vs C write() syscall overhead
- `bench_fastpath.py` — isolated buffer fast-path cost (no flushes)
- `bench_writer.py` — ByteWriteStream variants (v1, v1b, v1c, v5)
- `bench_vs_asyncio.py` — shish vs asyncio StreamWriter (/dev/null, pipe, slow pipe)
- `bench_vs_print.py` — shish vs print() / file.write() / os.write()
- `bench_bufwriter.py` — C buffered writer vs Python variants (pipe + /dev/null)
- `bench_awaitable_overhead.py` — C vs Rust noop/borrow/copy/write overhead breakdown
- `model_throughput.py` — theoretical throughput model (v1c vs C buffered)

Writer variants in `src/shish/streams/`:
- `writers.py` — v1: async def, always locked (baseline)
- `writers_v1b.py` — async def + lock bypass
- `writers_v1c.py` — async def + lock bypass + inlined writes + sync try-flush
- `writers_v5.py` — @types.coroutine generators + lock bypass + try-flush
- `writers_unbuf.py` — unbuffered, direct os.write per call

C extensions:
- `_cbufwriter.c` — buffered writer with C awaitable fast path (production)
- `_rawwriter.c` — RawWriter with C awaitable fast path
- `_cnoop.c` — minimal noop awaitable for overhead benchmarking

Python equivalents:
- `_bufwriter_py.py` — Python awaitable buffered writer
- `_rawwriter_py.py` — Python awaitable RawWriter

Rust/PyO3 port: `src/lib.rs` (experimental, on `feat/c-bufwriter` branch)
