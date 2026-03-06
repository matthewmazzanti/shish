# TODO

## Fixes

- CalledProcessError in `out` passes empty list for cmd arg
- Sporadic "cat: write error: Resource temporarily unavailable" in tests (not yet reproducible)
- KeyboardInterrupt testing — verify fn() tasks that raise KeyboardInterrupt
  propagate it through wait() and kill the event loop (not swallowed by
  `return_exceptions=True`). Needs subprocess-based test since
  KeyboardInterrupt kills the event loop before in-process assertions can run.

## Features

- Fn redirects — read/feed/write/close on Fn stages (fd 0/1 only). May want
  to leverage the emulated fd table to provide a `dict[int, WriteStream]` interface
  rather than just stdin/stdout. Needs more design thought.

## Design

- Bad return codes: map to something? raise exceptions? configurable?
- Defaults vs explicit configuration — where's the line?
- Configurable command builder: `sh = CommandBuilder(env={...}, cwd="...", raise_on_error=True)`
