# TODO

## Bugs

- Sporadic "cat: write error: Resource temporarily unavailable" in tests (not yet reproducible)

## Features

- Buffered writes on `ByteWriteStream` — needs investigation into Python stdlib `BufferedWriter` behavior
- Custom tree node for feed — avoids duping stdin/stderr entirely, saves 2 dups per feed.
- Configurable command builder: `sh = CommandBuilder(env={...}, cwd="...", raise_on_error=True)`
- Configurable flag handling — current `Sh.__call__` hardcodes `-k`/`--key` and `_` → `-` mapping. Consider making flag style pluggable (e.g. `+flag`, `/flag`, `--no-flag` negation).
- `<<` from async generators — stream data into stdin incrementally.
- Signal forwarding — propagate SIGINT/SIGTERM to children instead of just SIGKILL. Challenge: adapter needed for Fn stages (asyncio tasks have no signal equivalent).
- Fn redirects — read/feed/write/close on Fn stages (fd 0/1 only). May want to leverage the emulated fd table to provide a `dict[int, WriteStream]` interface rather than just stdin/stdout. Needs more design thought.

## Quality

- User docs — GitHub Pages? Read the Docs? Loose markdown?
- Examples (cookbook-style usage patterns)
- KeyboardInterrupt propagation through Fn — needs subprocess-based test
- General test cleanup pass
- Load testing — many concurrent processes
- Performance analysis
- Security analysis pass
