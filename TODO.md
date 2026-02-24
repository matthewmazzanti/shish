# TODO

## Fixes

- Double close fd in execute (fds closed in loop then again in finally)
- CalledProcessError in `out` passes empty list for cmd arg, stdout is bytes not decoded

## Features

- stderr handling (capture, redirect, combine with stdout)
- Allow fd redirects (e.g., `4>2`, `2>&1`)
- Emulate bash's fd handling for from_proc/to_proc (move to high fds >= 10 via F_DUPFD)
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
