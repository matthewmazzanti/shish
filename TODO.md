# TODO

## Fixes

- CalledProcessError in `out` passes empty list for cmd arg
- Exhaustive FD checking — verify no leaked fds after each spawn (e.g. walk `/dev/fd` in child)
- Sporadic "cat: write error: Resource temporarily unavailable" in tests (not yet reproducible)
- KeyboardInterrupt testing — verify fn() tasks that raise KeyboardInterrupt
  propagate it through wait() and kill the event loop (not swallowed by
  `return_exceptions=True`). Needs subprocess-based test since
  KeyboardInterrupt kills the event loop before in-process assertions can run.

## Features
- Stderr capture — currently stderr is completely untouched (inherited from parent).
  Need capture API: `err(cmd)`? `out(cmd, out=True, err=True)`? Design TBD.
  Also: `prepare()` should dup stdin/stdout in `caller_fds` so they go through
  the close() machinery instead of being bare fds.
- Streaming output - async iteration over lines as they arrive
- Shell renderer - serialize Cmd/Pipeline back to shell string (complex: quoting, escaping)
- Process handle - `async with start(cmd) as handle:` for lifecycle control (signal, wait, timeout, auto-kill on exit)
- Python stages - functions as pipeline stages with stdin/stdout/stderr/exit code interface
- Fn redirects - read/feed/write/close on Fn stages (fd 0/1 only). May want
  to leverage the emulated fd table to provide a `dict[int, WriteStream]` interface
  rather than just stdin/stdout. Needs more design thought.

## Design

- Default encoding: library hardcodes `"utf-8"` everywhere (`out`, `fn`, `feed`/`<<`, text streams).
  Bash uses `LC_CTYPE` for heredocs and output — should default to `locale.getpreferredencoding(False)`?

- Should `run` return Result by default instead of int?
- Bad return codes: map to something? raise exceptions? configurable?
- Defaults vs explicit configuration - where's the line?
- Configurable command builder: `sh = CommandBuilder(env={...}, cwd="...", raise_on_error=True)`

## Open questions

- **`out()` too involved.** Capture mode on execute, or method on process handle?
- **Prepare walk mirrors IR.** Visitor pattern or composable FdOps transforms?
  Risk: coupling IR to runtime.
- **`_spawn_cmd` is heavy.** Refactored into `SpawnCtx` class. Evaluate whether
  further decomposition is worthwhile.

## Architecture notes

- Process tree: CmdNode (proc + fds + subs) / PipelineNode (stages)
  - `root_procs()` for pipefail, `all_procs()`/`all_fds()` for cleanup
  - Subs excluded from pipefail — matches bash process substitution semantics
- Prepare/spawn split: parent must close pipe fds after all spawns
- Arg-position subs use `/dev/fd/N` path via pass_fds; redirect-position
  subs go through FdOps move_fd — distinct mechanisms matching bash
- Cleanup: SIGKILL live procs on error, shield reap from cancellation
- Graceful shutdown: SIGTERM → timeout → SIGKILL (like Docker/systemd)
