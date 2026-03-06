# TODO

## Bugs

- CalledProcessError in `out` passes empty list for cmd arg
- Sporadic "cat: write error: Resource temporarily unavailable" in tests (not yet reproducible)

## Features

- Custom tree node for feed — avoids duping stdin/stderr entirely, saves 2 dups per feed.
- `<<` from async generators — stream data into stdin incrementally.
- Buffered writes on `ByteWriteStream` — needs investigation into Python stdlib `BufferedWriter` behavior
- Fn redirects — read/feed/write/close on Fn stages (fd 0/1 only). May want
  to leverage the emulated fd table to provide a `dict[int, WriteStream]` interface
  rather than just stdin/stdout. Needs more design thought.
- Signal forwarding — propagate SIGINT/SIGTERM to children instead of just SIGKILL.
  Challenge: adapter needed for Fn stages (asyncio tasks have no signal equivalent).
- Configurable flag handling — current `Sh.__call__` hardcodes `-k`/`--key` and `_` → `-`
  mapping. Consider making flag style pluggable (e.g. `+flag`, `/flag`, `--no-flag` negation).
- Configurable command builder: `sh = CommandBuilder(env={...}, cwd="...", raise_on_error=True)`
- Bad return codes: map to something? raise exceptions? configurable?

## Quality

- Rename `ByteStageCtx`/`TextStageCtx`/`SpawnCtx`/`SpawnCmdCtx`/`StartCtx` to
  something more descriptive
- Lint rules: enforce `import typing as ty`, `dataclass as dc`, `collections.abc as ...` style
- Examples (cookbook-style usage patterns)
- User docs — GitHub Pages? Read the Docs? Loose markdown?
- Defaults vs explicit configuration — where's the line?
- KeyboardInterrupt propagation through Fn — needs subprocess-based test
- General test cleanup pass
- Load testing — many concurrent processes
- Performance analysis
- Security analysis pass
