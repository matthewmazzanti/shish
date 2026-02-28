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
  ir.py         # frozen dataclass IR: Cmd, Pipeline, per-fd redirects
  dsl.py        # thin wrappers (Cmd, Pipeline), operators, combinators
  fdops.py      # fd-table simulator for computing pass_fds
  runtime.py    # executes IR nodes, run, out, Result
  aio.py        # async_read, async_write, fd utilities
TODO.md         # planned features and known issues
```

## Key Concepts

- `sh.cmd()` returns `Cmd` - immutable command builder
- `cmd1 | cmd2` returns `Pipeline` - concurrent execution
- `await cmd` or `await run(cmd)` - returns exit code
- `await out(cmd)` - returns stdout as string (or bytes with `encoding=None`)
- `sub_in(cmd)` / `sub_out(cmd)` - process substitution via `/dev/fd/N`
- Operators: `|` pipe, `>` write, `>>` append, `<` read, `<<` feed
- Tuple fd syntax: `cmd > (STDERR, "err.log")` targets specific fds
- Combinators: `write`, `read`, `feed`, `close`, `pipe` — accept files or subs, with `fd=` kwarg
- Pipefail by default (128 + signal for killed processes)

## Style

- All function signatures must be typed (args + return), including tests
- pyright strict mode enforced
- ruff ANN rules enforce annotation coverage
- No one or two letter variable names except loop indexes

## Implementation Notes

- IR layer (`ir.py`): frozen dataclasses with builder methods, type aliases (PathLike, Data, Arg, ReadSrc, WriteDst)
- DSL layer (`dsl.py`): thin wrappers with no public methods, operators delegate to combinators
- `unwrap()`/`wrap()` bridge DSL and IR layers
- Per-fd redirects: FdToFile, FdFromFile, FdFromData, FdToFd, FdClose, FdFromSub, FdToSub
- `SubIn`/`SubOut` hold process substitution commands (resolved to `/dev/fd/N` at runtime)
- `fdops.py` simulates fd table to compute `pass_fds` for subprocess
- Uses `asyncio.subprocess.create_subprocess_exec` with `pass_fds`
- Pipeline stages run concurrently via `os.pipe()` fds
- Per-stage redirects override pipe connections
- SIGKILL orphan processes on error, shield reap from cancellation
- Async IO via event loop reader/writer callbacks (aio.py)
- SIGPIPE propagates naturally for early termination
