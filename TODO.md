# TODO

## Fixes

- CalledProcessError in `out` passes empty list for cmd arg

## Features
- Streaming output - async iteration over lines as they arrive
- Shell renderer - serialize Cmd/Pipeline back to shell string (complex: quoting, escaping)
- Process handle - `async with start(cmd) as handle:` for lifecycle control (signal, wait, timeout, auto-kill on exit)
- Python stages - functions as pipeline stages with stdin/stdout/stderr/exit code interface

## Design

- Should `run` return Result by default instead of int?
- Bad return codes: map to something? raise exceptions? configurable?
- Defaults vs explicit configuration - where's the line?
- Configurable command builder: `sh = CommandBuilder(env={...}, cwd="...", raise_on_error=True)`

## Open questions

- **`out()` too involved.** Capture mode on execute, or method on process handle?
- **Prepare walk mirrors IR.** Visitor pattern or composable FdOps transforms?
  Risk: coupling IR to runtime.

## Architecture notes

- Process tree: CmdNode (proc + fds + subs) / PipelineNode (stages)
  - `root_procs()` for pipefail, `all_procs()`/`all_fds()` for cleanup
  - Subs excluded from pipefail — matches bash process substitution semantics
- Prepare/spawn split: parent must close pipe fds after all spawns
- Arg-position subs use `/dev/fd/N` path via pass_fds; redirect-position
  subs go through FdOps move_fd — distinct mechanisms matching bash
- Cleanup: SIGKILL live procs on error, shield reap from cancellation
- Graceful shutdown: SIGTERM → timeout → SIGKILL (like Docker/systemd)
