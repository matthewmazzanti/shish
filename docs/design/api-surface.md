# API Surface Redesign: run / start / out

## Overview

| function | returns | description |
|---|---|---|
| `run` / `await cmd` | `None` | execute, raise on non-zero |
| `code` | `int` | exit code |
| `ok` | `bool` | True if exit code == 0 |
| `err` | `bool` | True if exit code != 0 |
| `out` | `str` | captured stdout (default) |
| `out(stderr=True)` | `(str, str)` | stdout + stderr |
| `out(stdout=False, stderr=True)` | `str` | stderr only |
| `out(check=False)` | `(int, str)` | exit code + stdout |
| `out(encoding=None)` | `bytes` | raw bytes |
| `result(stdout=PIPE)` | `Result[str, None]` | full control escape hatch |
| `start` | `JobCtx` | streaming/interactive escape hatch |

3 execution functions (`run`, `out`, `result`), 3 exit-code accessors (`code`,
`ok`, `err`), 1 streaming escape hatch (`start`).

`out()` is the single capture entrypoint with `stdout`/`stderr` bool selectors,
`encoding` for str/bytes, and `check` to prepend exit code. Single-stream
capture returns a scalar; multi-stream returns a tuple.

`result()` is the parametric escape hatch returning `Result` — covers any
combination via explicit `stdout=PIPE`/`stderr=PIPE` flags.

`start()` is the escape hatch for streaming, interactive, or long-running processes.

## Motivation

The current API has asymmetric error handling: `await cmd` silently ignores
non-zero exit codes, while `out()` raises. `run()`/`out()` are implemented in
`runtime/api.py` but are trivially expressible via `start()`, creating
unnecessary coupling. There's no explicit opt-in for exit code retrieval.

## Design

### `await cmd` → `run()`

`await cmd` delegates to `run()` — returns `None`, raises on non-zero.
Safe default: `await sh.cp(...)` just works without accidentally buffering stdout.

### Result: generic NamedTuple

`Result` is a generic `NamedTuple` parameterized by stdout/stderr types.
Field names match the alias names: `code`, `out`, `err`. Lives in `builders.py`.

```python
class Result[OutT, ErrT](NamedTuple):
    code: int
    out: OutT
    err: ErrT
```

- Named attribute access: `result.code`, `result.out`, `result.err`
- Typed tuple unpacking: `code, out, err = result` with per-position types
- Generic parameterization: `Result[str, None]`, `Result[bytes, str]`, etc.

### `result()`: unified parametric function

```python
async def result(
    cmd: Runnable,
    *,
    check: bool = True,
    stdout: Pipe | None = None,   # PIPE to capture, None to inherit
    stderr: Pipe | None = None,
    encoding: str | None = DEFAULT_ENCODING,
) -> Result[OutT, ErrT]:         # parameterized by overloads
    ...
```

Overloads narrow the `Result` generic:

```python
result(cmd)                                          -> Result[None, None]
result(cmd, stdout=PIPE)                             -> Result[str, None]
result(cmd, stderr=PIPE)                             -> Result[None, str]
result(cmd, stdout=PIPE, stderr=PIPE)                -> Result[str, str]
result(cmd, stdout=PIPE, encoding=None)              -> Result[bytes, None]
result(cmd, stdout=PIPE, stderr=PIPE, encoding=None) -> Result[bytes, bytes]
```

```python
# Named access
r = await result(sh.ls(), stdout=PIPE)
r.code  # int
r.out   # str
r.err   # None

# Typed tuple unpacking
code, out, err = await result(sh.ls(), stdout=PIPE, stderr=PIPE)

# Rare combos
code, out, err = await result(sh.grep("pat"), check=False, stdout=PIPE)
```

### `out()`: flexible capture

`out()` is the single capture entrypoint. Bool selectors for `stdout`/`stderr`
control which streams to capture. `encoding` controls str vs bytes. `check=False`
prepends exit code to the return.

```python
async def out(
    cmd: Runnable,
    encoding: str | None = DEFAULT_ENCODING,
    *,
    stdout: bool = True,
    stderr: bool = False,
    check: bool = True,
) -> ...:  # 12 overloads
```

Single-stream capture returns a scalar, multi-stream returns a tuple:

```python
await out(sh.ls())                                     # -> str
await out(sh.ls(), stderr=True)                        # -> (str, str)
await out(sh.ls(), stdout=False, stderr=True)          # -> str (stderr)
await out(sh.ls(), check=False)                        # -> (int, str)
await out(sh.ls(), stderr=True, check=False)           # -> (int, str, str)
await out(sh.ls(), encoding=None)                      # -> bytes
await out(sh.ls(), encoding=None, stderr=True)         # -> (bytes, bytes)
```

Raises `ValueError` if both `stdout=False` and `stderr=False` (use `run()` instead).

### Exit code accessors

```python
async def run(cmd) -> None:      # raises ShishError on non-zero
async def code(cmd) -> int:      # exit code
async def ok(cmd) -> bool:       # True if exit code == 0
async def err(cmd) -> bool:      # True if exit code != 0
```

### `start()` for streaming / interactive

Unchanged — the escape hatch for streaming stdin/stdout, long-running
processes, signal control, etc.

```python
async with start(cmd).stdin(PIPE).stdout(PIPE) as job:
    await job.stdin.write("data")
    output = await job.stdout.read()
```

## Layer responsibilities

### Runtime (`runtime/api.py`)
- Exports: `start()`, `Job`, `JobCtx`, `CloseMethod`
- No `run()`, `out()`, `code()`, `result()` — those live in builders/syntax

### Builders (`builders.py`)
- `ShishError` exception class
- `Result` NamedTuple
- `BaseRunnable` base class:
  - `.start()` → delegates to `runtime.start()`
  - `.result()` → implemented via `self.start()`, returns `Result` (7 overloads)
  - `.out()` → sugar over `.result()` with stream selectors (12 overloads)
  - `.run()`, `.code()`, `.ok()`, `.err()` → sugar over `.result()`/`.code()`
- `Cmd`, `Pipeline`, `Fn` are frozen dataclasses inheriting from `BaseRunnable`
- `Runnable = Cmd | Pipeline | Fn` union type

### Syntax (`syntax.py`)
- `Cmd`, `Pipeline`, `Fn` syntax wrappers with `__await__` → `.run()`
- Thin combinators delegate to builder methods:
  - `result(cmd)` → `unwrap(cmd).result()` (7 overloads)
  - `out(cmd)` → `unwrap(cmd).out()` (12 overloads)
  - `run(cmd)`, `code(cmd)`, `ok(cmd)`, `err(cmd)` → delegate
  - `start(cmd)` → `unwrap(cmd).start()`

## Deferred

### Encoding defaults

`await cmd` maps to `run()` (no capture), so encoding isn't relevant there.
For `out()`, encoding defaults to `DEFAULT_ENCODING`. Configurability of
encoding defaults deferred to configuration API design (which will also
handle defaults for env, cwd, etc.).
