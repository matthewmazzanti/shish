# Process Interaction — Projected Design

Future features that build on the stream API (`ByteReadStream`,
`ByteWriteStream`, `TextReadStream`, `TextWriteStream`).

## `Fn` — Python functions as commands

A `Fn` is a Python async callable that can replace any `Cmd` slot:
pipeline stages, process substitutions, redirects. It runs in-process
rather than spawning a subprocess.

```python
Runnable = Cmd | Pipeline | Fn  # Fn joins the union
```

### Construction

```python
# IR: Fn wraps an async callable
@dataclass(frozen=True)
class Fn:
    func: Callable[[ByteStage], Awaitable[int]]

# DSL: fn() constructor, mirrors cmd()
fn(upper)
```

### ByteStage

The runtime always passes byte streams via `ByteStage`:

```python
@dataclass
class ByteStage:
    stdin: ByteReadStream
    stdout: ByteWriteStream
```

Extensible — future fields (stderr, env, signals) without breaking
the callable signature. The runtime sees a consistent interface:
bytes in, bytes out.

### Everywhere a Cmd can go

`Fn` slots in anywhere a `Cmd` does. `Runnable = Cmd | Pipeline | Fn`,
so `.pipe()`, `SubIn`/`SubOut`, and all combinators accept it.

Builder API (methods on IR):
```python
# Pipeline
cmd("cat", "file").pipe(fn(upper)).pipe(cmd("sort"))

# Process substitution
cmd("diff").arg(fn(sort_lines).sub_in(), fn(sort_lines).sub_in())

# Input redirect via sub
cmd("wc").read(fn(generate_data).sub_in())

# Output redirect via sub
cmd("echo", "hello").write(fn(collect).sub_out())
```

DSL API (operators and combinators):
```python
# Pipeline
sh.cat("file") | upper | sh.sort

# Process substitution
sh.diff(sub_in(sort_lines), sub_in(sort_lines))

# Input redirect via sub
sh.wc < generate_data

# Output redirect via sub
sh.echo("hello") > collect
```

Builder API requires explicit `.sub_in()`/`.sub_out()` wrapping for
redirects. DSL auto-wraps: `|` wraps callables in `Fn`, `<` wraps
in `SubIn(Fn(...))`, `>` wraps in `SubOut(Fn(...))`. `sub_in()`/
`sub_out()` combinators also accept bare callables.

`Fn` participates in all the same fd wiring as `Cmd` — the runtime
allocates pipes, wraps fds in byte streams, constructs `ByteStage`,
calls the function, closes streams after return.

### Text mode via `@decode`

`@decode` wraps byte streams in text streams before calling the
function. Default encoding is UTF-8. Pure user-side adapter — the
runtime always sees the byte-level callable.

```python
@dataclass
class TextStage:
    stdin: TextReadStream
    stdout: TextWriteStream

@decode
async def upper(ctx: TextStage) -> int:
    async for line in ctx.stdin:        # str
        await ctx.stdout.write(line.upper())
    return 0

@decode("latin-1")
async def upper_latin(ctx: TextStage) -> int: ...
```

Under the hood, `@decode` produces a wrapper that:
1. Receives `ByteStage` (byte streams) from the runtime
2. Wraps in `TextReadStream`/`TextWriteStream`
3. Constructs `TextStage` and calls the inner function

### `<<` desugars to `Fn`

`<<` creates a `Fn` that writes the data to stdout, piped into stdin
via process substitution. No special `FdFromData` IR node — `<<` is
pure sugar over `Fn`:

```python
# These are equivalent:
sh.cat << "hello world"

sh.cat < (lambda ctx: ctx.stdout.write(b"hello world"))
```

This replaces `FdFromData` / `WriteNode` in the IR and runtime.
One fewer IR node, one fewer process tree node type.

### Return codes

Return value is the exit code (0 = success). Participates in pipefail
like subprocess exit codes.

### Requirements

- IR: `Fn` dataclass wrapping an async callable. `Runnable = Cmd | Pipeline | Fn`.
  `Pipeline.stages` becomes `tuple[Cmd | Fn, ...]`. Remove `FdFromData` —
  `feed()`/`<<` desugar to `FdFromSub` + `Fn`.
- DSL: `fn()` constructor. `|`, `<`, `>` auto-wrap bare callables.
  `sub_in()`/`sub_out()` combinators accept bare callables.
- Runtime: `_spawn` dispatches `Fn` → run in-process with pipe-backed
  `ByteStage`. `_spawn_cmd` stays subprocess-only. New `_spawn_fn`
  handles the in-process case. Remove `WriteNode` — `Fn` subsumes it.
- Fd wiring: same pipe allocation as subprocess — inter-stage pipes
  wrapped in `ByteReadStream`/`ByteWriteStream` via `ByteStage`.
- Process tree: new `FnNode` replaces `WriteNode`. Has a return code.

## Process handle: `start()`

Interactive use — write to stdin, read stdout as streams,
control process lifecycle via context manager. Accepts commands,
pipelines, and `Fn` (stdin wires to first stage, stdout reads
from last).

`encoding` param controls stream types. Default `"utf-8"` returns
text streams; `None` returns byte streams. Same convention as `out()`.

```python
# Text mode (default) — stdin is TextWriteStream, stdout is TextReadStream
async with cmd("grep", "pattern").start() as proc:
    await proc.stdin.write("hello world\n")
    await proc.stdin.close()
    async for line in proc.stdout:
        print(line)  # "hello world\n"

# Binary mode — stdin is ByteWriteStream, stdout is ByteReadStream
async with cmd("gzip").start(encoding=None) as proc:
    await proc.stdin.write(raw_bytes)
    await proc.stdin.close()
    compressed = await proc.stdout.read()

# Combinator API (module-level, for DSL):
async with start(sh.grep("pattern")) as proc: ...
async with start(sh.gzip(), encoding=None) as proc: ...
```

Overloaded return type based on `encoding`:

```python
# encoding: str (default "utf-8") -> TextProcess
@dataclass
class TextProcess:
    stdin: TextWriteStream
    stdout: TextReadStream
    returncode: int | None

    async def wait(self) -> int: ...

# encoding: None -> ByteProcess
@dataclass
class ByteProcess:
    stdin: ByteWriteStream
    stdout: ByteReadStream
    returncode: int | None

    async def wait(self) -> int: ...

@overload
async def start(cmd, encoding: None) -> ByteProcess: ...
@overload
async def start(cmd, encoding: str = "utf-8") -> TextProcess: ...
```

Internally, `start()` always creates byte streams from pipe fds,
then wraps in text streams when `encoding` is not None. Same layering
as `out()` which always captures bytes then decodes.

- Context manager waits for exit, kills on exception, closes all streams.
- `returncode` available after `wait()` or `__aexit__`.
- Follows same pattern as `run()`, `out()`, `prepare()`:
  `ir.Cmd.start()` -> `runtime.start()`, `dsl.start()` unwraps and delegates.

### Stderr

Stderr inherits to the parent process (matches subprocess default).
Capture/redirect deferred — can be added later via a `stderr=` param
without breaking existing callers.
