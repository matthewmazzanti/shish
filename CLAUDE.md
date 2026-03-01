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
  runtime.py    # executes IR: prepare, Execution, run, out
  aio.py        # async_read, async_write, fd utilities
TODO.md         # planned features and known issues
```

## Key Concepts

- `cmd("echo", "hello")` or `sh.echo("hello")` returns `Cmd` - immutable command builder
- `cmd1 | cmd2` returns `Pipeline` - concurrent execution
- `await cmd` or `await run(cmd)` - returns exit code
- `await out(cmd)` - returns stdout as string (or bytes with `encoding=None`)
- `sub_in(cmd)` / `sub_out(cmd)` - process substitution via `/dev/fd/N`
- Operators: `|` pipe, `>` write, `>>` append, `<` read, `<<` feed, `@` cwd, `%` env (Cmd only, not Pipeline; enforced order: `env % cmd @ cwd`)
- Redirect operators are Cmd-only to avoid precedence confusion: in Python `|` binds tighter
  than `<`/`>`, so `cmd1 | cmd2 < "file"` would parse as `(cmd1 | cmd2) < "file"` (unlike bash).
  Apply redirects on individual cmds: `cmd1 | (cmd2 < "file")`, or use combinators.
- Tuple fd syntax: `cmd > (STDERR, "err.log")` targets specific fds
- Combinators: `write`, `read`, `feed`, `close`, `pipe` — accept files or subs, with `fd=` kwarg
- Pipefail by default (128 + signal for killed processes)

## Style

- All function signatures must be typed (args + return), including tests
- pyright strict mode enforced
- ruff ANN rules enforce annotation coverage
- No one or two letter variable names except loop indexes
- Conventional Commits: messages use `feat:`, `fix:`, `chore:`, `docs:`, etc. prefix
- Branch names match: `feat/`, `fix/`, `chore/`, `docs/`, etc.

## Implementation Notes

- IR layer (`ir.py`): frozen dataclasses with builder methods, type aliases (PathLike, Data, Arg, ReadSrc, WriteDst)
- DSL layer (`dsl.py`): thin wrappers with no public methods, operators delegate to combinators
- `unwrap()`/`wrap()` bridge DSL and IR layers
- Per-fd redirects: FdToFile, FdFromFile, FdFromData, FdToFd, FdClose, FdFromSub, FdToSub
- `SubIn`/`SubOut` hold process substitution commands (resolved to `/dev/fd/N` at runtime)
- `fdops.py` simulates fd table to compute `pass_fds` for subprocess
- Runtime (`runtime.py`): `prepare()` spawns a process tree, returns `Execution` handle
  - `PrepareCtx` tracks fds/procs during spawn for error cleanup
  - Module-level `_spawn`/`_spawn_cmd`/`_spawn_pipeline` build the process tree
  - Process tree: `CmdNode` (single cmd + subs) / `PipelineNode` (stages)
  - `Execution.wait()` derives procs/fds by recursing the tree via `all_procs()`/`all_fds()`
  - Pipefail: rightmost non-zero from `root_procs()` (subs excluded, matching bash)
- Uses `asyncio.subprocess.create_subprocess_exec` with `pass_fds`
- Pipeline stages run concurrently via `os.pipe()` fds
- Per-stage redirects override pipe connections
- SIGKILL orphan processes on error, shield reap from cancellation
- Async IO via event loop reader/writer callbacks (aio.py)
- SIGPIPE propagates naturally for early termination

## Test Organization

- `test_ir.py` — IR layer: `cmd()` builder methods, `ir.pipeline()` flattening. Sync only, no execution.
- `test_dsl.py` — `sh` magic + operators produce correct IR. Sync only, no execution.
- `test_fdops.py` — FdOps fd-table simulation. Sync only, no execution.
- `test_aio.py` — async_read, async_write, fd utilities.
- `test_runtime.py` — Raw IR → run. No builders or DSL. Tests runtime behavior.
- `test_e2e.py` — Full integration from DSL or builder → run.
- `test_fd_hygiene.py` — Verifies child processes see exactly the expected fd set.

Rules for `test_runtime.py` and `test_e2e.py`:
- Only use commands available in typical macOS/Linux environments (coreutils, util-linux, BSD)
- Clean runs: use `tmp_path` for any file writes, no system side effects
