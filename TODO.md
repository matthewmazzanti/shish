# TODO

## Fixes

- CalledProcessError in `out` passes empty list for cmd arg
- README shows `await proc.stdin.close()` but `close()` is synchronous
- `builders.Cmd.start()` returns `StartCtx[None, None]` (2 type args) — should be `StartCtx[None, None, None]`
- Sporadic "cat: write error: Resource temporarily unavailable" in tests (not yet reproducible)
- KeyboardInterrupt testing — verify fn() tasks that raise KeyboardInterrupt
  propagate it through wait() and kill the event loop (not swallowed by
  `return_exceptions=True`). Needs subprocess-based test since
  KeyboardInterrupt kills the event loop before in-process assertions can run.

## Features

- Fn redirects — read/feed/write/close on Fn stages (fd 0/1 only). May want
  to leverage the emulated fd table to provide a `dict[int, WriteStream]` interface
  rather than just stdin/stdout. Needs more design thought.
- Immediately close unused stdin in `write_byte_data`/`write_str_data` feed Fns.
  Alternative: custom tree node for feed that only gets stdout, avoids stdin/stderr entirely.
- Add stderr to `ByteStageCtx` (and `TextStageCtx`) — currently duped but never exposed

## Refactor

- Rename `ByteStageCtx`/`TextStageCtx`/`SpawnCtx`/`SpawnCmdCtx`/`StartCtx` to
  something more descriptive
- Lint rules: enforce `import typing as ty`, `dataclass as dc`, `collections.abc as ...` style

## Docs

- Examples (cookbook-style usage patterns)
- User docs — GitHub Pages? Read the Docs? Loose markdown?

## Testing

- KeyboardInterrupt propagation through Fn (see Fixes)
- General test cleanup pass
- Load testing — many concurrent processes
- Performance analysis

## Security

- Security analysis pass

## Design

- Buffered writes on `ByteWriteStream` — needs investigation into Python stdlib `BufferedWriter` behavior
- Signal forwarding — propagate SIGINT/SIGTERM to children instead of just SIGKILL.
  Challenge: adapter needed for Fn stages (asyncio tasks have no signal equivalent).

- Bad return codes: map to something? raise exceptions? configurable?
- Defaults vs explicit configuration — where's the line?
- Configurable command builder: `sh = CommandBuilder(env={...}, cwd="...", raise_on_error=True)`
