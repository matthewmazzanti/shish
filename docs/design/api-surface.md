# API Surface Redesign: run / start / out

## Overview

| check | stdout | stderr | alias | returns |
|---|---|---|---|---|
| True | None | None | `run` / `await cmd` | `None` |
| True | PIPE | None | `out` | `str` |
| True | None | PIPE | `err` | `str` |
| True | PIPE | PIPE | `out_err` | `(str, str)` |
| False | None | None | `code` | `int` |
| False | PIPE | None | `out(check=False)` | `(int, str)` |
| False | None | PIPE | `err(check=False)` | `(int, str)` |
| False | PIPE | PIPE | `out_err(check=False)` | `(int, str, str)` |

5 functions: `run`, `code`, `out`, `err`, `out_err`. The capture functions
(`out`, `err`, `out_err`) accept `check=False` to prepend exit code to the return.

`result()` is the unified parametric function returning `Result` — covers any
combination via explicit flags. Aliases are sugar over `result()`.

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
result(cmd)                                        -> Result[None, None]
result(cmd, stdout=PIPE)                           -> Result[str, None]
result(cmd, stderr=PIPE)                           -> Result[None, str]
result(cmd, stdout=PIPE, stderr=PIPE)              -> Result[str, str]
result(cmd, stdout=PIPE, encoding=None)            -> Result[bytes, None]
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

### Convenience aliases

5 functions. `check=False` on capture functions prepends exit code to the return.
Naming convention: `out` → `stdout=PIPE`, `err` → `stderr=PIPE`.

```python
# run: no capture, raise on non-zero
async def run(cmd) -> None:

# code: no capture, return exit code
async def code(cmd) -> int:

# out: capture stdout (check= overload)
async def out(cmd, encoding=...) -> str:                  # raises
async def out(cmd, check=False, encoding=...) -> tuple[int, str]:  # (code, stdout)

# err: capture stderr (check= overload)
async def err(cmd, encoding=...) -> str:                  # raises
async def err(cmd, check=False, encoding=...) -> tuple[int, str]:  # (code, stderr)

# out_err: capture both (check= overload)
async def out_err(cmd, encoding=...) -> tuple[str, str]:                    # raises
async def out_err(cmd, check=False, encoding=...) -> tuple[int, str, str]:  # (code, stdout, stderr)
```

Stderr is only captured when explicitly requested — no implicit capture for
exceptions. If a checked function raises `ShishError` without stderr captured,
the exception has no stderr context (the user sees it in the terminal instead).

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
- Exports: `start()`, `Job`, `JobCtx`, `ShishError`, `CloseMethod`
- No `run()`, `out()`, `code()`, `result()` — those live in builders/syntax

### Builders (`builders.py`)
- `BaseRunnable` base class:
  - `.start()` → delegates to `runtime.start()`
  - `.result()` → implemented via `self.start()`, returns `Result`
  - `.run()`, `.code()`, `.out()`, `.err()`, `.out_err()` → sugar over `.result()`
- `Cmd`, `Pipeline`, `Fn` are frozen dataclasses inheriting from `BaseRunnable`
  - Pure spec/data + builder methods (`.arg()`, `.env()`, `.cwd()`, etc.)
- `Result` NamedTuple defined here
- `Runnable = Cmd | Pipeline | Fn` union type unchanged
- Only one import from runtime needed (`start`)

### Syntax (`syntax.py`)
- `Cmd`, `Pipeline`, `Fn` syntax wrappers with `__await__` → `.run()`
- Thin combinators delegate to builder methods:
  - `result(cmd)` → `unwrap(cmd).result()`
  - `run(cmd)` → `unwrap(cmd).run()`
  - `out(cmd)` → `unwrap(cmd).out()`
  - `code(cmd)` → `unwrap(cmd).code()`
  - `err(cmd)` → `unwrap(cmd).err()`
  - `out_err(cmd)` → `unwrap(cmd).out_err()`
- `start(cmd)` → `unwrap(cmd).start()`

## Deferred

### Encoding defaults

`await cmd` maps to `run()` (no capture), so encoding isn't relevant there.
For `out()`/`err()`/`out_err()`, encoding defaults to `DEFAULT_ENCODING`.
Configurability of encoding defaults deferred to configuration API design
(which will also handle defaults for env, cwd, etc.).

### `out_err` naming

No satisfying alternative found. Candidates considered and rejected:
`output`, `streams`, `outs`, `capture`, `communicate`. Keeping `out_err`
for now — consistent with the naming convention even if not beautiful.
