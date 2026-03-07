# shish

Async shell command library for Python with operator-based DSL.

## Build & Dev

direnv auto-activates venv via `.envrc` (runs `uv sync --frozen` + `source .venv/bin/activate`).

```bash
just check      # format + typecheck + test
just fmt        # format + autofix
just lint       # ruff format --check + ruff check
just typecheck  # pyright
just test       # pytest
just build      # uv build
```

## Project Structure

```
src/shish/          # main package
  __init__.py       # re-exports from syntax + runtime
  builders.py       # frozen dataclass builders: Cmd, Pipeline, per-fd redirects
  syntax.py         # thin wrappers (Cmd, Pipeline), operators, combinators
  fd.py             # fd constants (STDIN/STDOUT/STDERR/PIPE), Fd
  fn_stage.py       # Fn stage contexts, ByteFn/TextFn aliases, decode wrapper
  streams.py        # async byte/text streams for subprocess pipes
  runtime/
    __init__.py     # re-exports from api + tree
    api.py          # Job, JobCtx, start(), run(), out()
    spawn.py        # SpawnScope: fd/proc tracking, pipeline/fn spawn
    spawn_cmd.py    # SpawnCmdScope, FdOps: per-cmd redirect resolution
    tree.py         # process tree nodes: CmdNode, PipelineNode, FnNode
TODO.md             # planned features and known issues
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
- Local imports are allowed but must include a descriptive comment explaining why and `# noqa: PLC0415`
- Put the descriptive comment on its own line above the import, keep `# noqa: PLC0415` inline (ruff format can reflow long lines, breaking inline comments off the import)
- Conventional Commits: messages use `feat:`, `fix:`, `chore:`, `docs:`, etc. prefix
- Branch names match: `feat/`, `fix/`, `chore/`, `docs/`, etc.
- Always ask before committing to main — use a feature branch instead

## Implementation Notes

- Builder layer (`builders.py`): frozen dataclasses with builder methods, type aliases (PathLike, Data, Arg, ReadSrc, WriteDst)
- Syntax layer (`syntax.py`): thin wrappers with no public methods, operators delegate to combinators
- `unwrap()`/`wrap()` bridge syntax and builder layers
- Per-fd redirects: FdToFile, FdFromFile, FdFromData, FdToFd, FdClose, FdFromSub, FdToSub
- `SubIn`/`SubOut` hold process substitution commands (resolved to `/dev/fd/N` at runtime)
- `FdOps` (`runtime/spawn_cmd.py`) simulates fd table to compute `pass_fds` for subprocess
- Runtime (`runtime/`): spawns process trees, returns `Job` handle
  - `SpawnScope` (`spawn.py`) tracks fds/procs during spawn for error cleanup
  - `SpawnCmdScope` (`spawn_cmd.py`) resolves per-cmd redirects and spawns
  - Process tree (`tree.py`): `CmdNode` (single cmd + subs) / `PipelineNode` (stages) / `FnNode` (in-process)
  - Pipefail: rightmost non-zero (subs excluded, matching bash)
- Uses `asyncio.subprocess.create_subprocess_exec` with `pass_fds`
- Pipeline stages run concurrently via `os.pipe()` fds
- Per-stage redirects override pipe connections
- SIGKILL orphan processes on error, shield reap from cancellation
- Async IO via event loop reader/writer callbacks (`streams.py`)
- SIGPIPE propagates naturally for early termination

## Test Organization

- `test_builders.py` — Builder layer: `cmd()` builder methods, `builders.pipeline()` flattening. Sync only, no execution.
- `test_syntax.py` — `sh` magic + operators produce correct builders. Sync only, no execution.
- `test_fd_ops.py` — FdOps fd-table simulation. Sync only, no execution.
- `test_streams.py` — async byte/text streams.
- `test_runtime.py` — Raw builders → run. No syntax layer. Tests runtime behavior.
- `test_e2e.py` — Full integration from syntax or builder → run.
- `test_fd_hygiene.py` — Verifies child processes see exactly the expected fd set.

Rules for `test_runtime.py` and `test_e2e.py`:
- Only use commands available in typical macOS/Linux environments (coreutils, util-linux, BSD)
- Clean runs: use `tmp_path` for any file writes, no system side effects
