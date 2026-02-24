# shish

Async shell command library for Python with operator-based DSL.

## Build & Dev

direnv auto-activates venv via `.envrc` (runs `uv sync --frozen` + `source .venv/bin/activate`).

```bash
just test       # run pytest
just check      # ruff + pyright
just fmt        # format + autofix
uv build        # build package
```

## Project Structure

```
src/shish/      # main package
  __init__.py   # re-exports from dsl + runtime
  dsl.py        # Cmd, Pipeline, Redirect, Sub, sh, combinators
  runtime.py    # Executor, run, out, Result
  aio.py        # async_read, async_write, fd utilities
TODO.md         # planned features and known issues
```

## Key Concepts

- `sh.cmd()` returns `Cmd` - immutable command builder
- `cmd1 | cmd2` returns `Pipeline` - concurrent execution
- `await cmd` or `await run(cmd)` - returns exit code
- `await out(cmd)` - returns stdout as string (or bytes with `encoding=None`)
- `from_proc(cmd)` / `to_proc(cmd)` - process substitution via `/dev/fd/N`
- Operators: `|` pipe, `>` write, `>>` append, `<` read, `<<` input
- Pipefail by default (128 + signal for killed processes)

## Style

- All function signatures must be typed (args + return), including tests
- pyright strict mode enforced
- ruff ANN rules enforce annotation coverage
- No one or two letter variable names except loop indexes

## Implementation Notes

- `Cmd` is immutable - chaining returns new instances
- `Pipeline` holds `list[Stage]` (Stage = Cmd | Redirect)
- `Redirect` wraps Cmd/Pipeline with stdin/stdout redirections
- `Sub` holds process substitution commands (resolved to `/dev/fd/N` at runtime)
- `Executor` manages fd lifecycle and process spawning
- Uses `asyncio.subprocess.create_subprocess_exec` with `pass_fds`
- Pipeline stages run concurrently via `os.pipe()` fds
- Per-stage redirects override pipe connections
- Async IO via event loop reader/writer callbacks (aio.py)
- SIGPIPE propagates naturally for early termination
