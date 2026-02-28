# TODO

## Fixes

- CalledProcessError in `out` passes empty list for cmd arg

## Features
- env vars - per-command environment (e.g., `cmd.env(VAR="value")`)
- cwd - working directory option for commands
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

- **Process indices fragile.** Merge prepare+spawn into async `_spawn` that
  returns Process objects directly. Fd closes accumulate during walk.
- **Result tree.** Tree of per-stage codes mirroring command structure.
  Encodes root-vs-ancillary naturally. Pipefail resolved at root.
- **`out()` too involved.** Capture mode on execute, or method on process handle?
- **Prepare walk mirrors IR.** Visitor pattern or composable FdOps transforms?
  Risk: coupling IR to runtime.

## Architecture notes

- Pipefail (rightmost non-zero), sub exit codes ignored — matches bash
- Prepare/spawn split: parent must close pipe fds after all spawns
- Arg-position subs use `/dev/fd/N` path via pass_fds; redirect-position
  subs go through FdOps move_fd — distinct mechanisms matching bash
- Cleanup: SIGKILL live procs on error, shield reap from cancellation
