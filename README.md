# shish

Async shell commands for Python with operator-based piping.

```python
from shish import sh

await (sh.cat("input.txt") | sh.grep("error") | sh.wc("-l"))
```

Python operators map directly to shell:

```python
# Piping
await (sh.cat("file") | sh.grep("pattern") | sh.wc("-l"))

# Redirections
await (sh.curl("http://example.com") > "page.html")  # stdout to file
await (sh.echo("line") >> "log.txt")                 # append
await (sh.grep("error") < "input.txt")               # stdin from file
await (sh.grep("error") << "line1\nline2\n")         # stdin from string

# Environment variables and working directory
await ({"FOO": "bar"} % sh.echo("$FOO") @ "/tmp")

# Process substitution (as args)
await sh.diff(sub_in(sh.sort("a.txt")), sub_in(sh.sort("b.txt")))

# Process substitution (as redirect targets)
await (sh.cat() < sub_in(sh.sort("a.txt")))    # <(sort a.txt)
await (sh.echo("hi") > sub_out(sh.gzip() > "out.gz"))  # >(gzip > out.gz)

# Kwargs to flags
await sh.git.commit(message="fix bug", amend=True)
# -> git commit --message 'fix bug' --amend

# Capture output (returns str, decoded as utf-8)
stdout = await out(sh.ls("-la"))
stdout = await out(sh.cat(), encoding=None)  # raw bytes
```

## Why Not stdlib?

Subprocess calls are verbose and error-prone:

```python
import subprocess

# Quoting? Escaping? Shell injection?
subprocess.run("cat input.txt | grep error | wc -l", shell=True)

# Safe, but unwieldy, no way to pipe
subprocess.run(["git", "commit", "--message", "fix bug", "--amend"])
```

The async version is worse - correct concurrent piping requires manual fd wiring:

```python
import asyncio

async def pipeline():
    cat = await asyncio.create_subprocess_exec(
        "cat", "input.txt",
        stdout=asyncio.subprocess.PIPE
    )
    grep = await asyncio.create_subprocess_exec(
        "grep", "error",
        stdin=cat.stdout,
        stdout=asyncio.subprocess.PIPE
    )
    wc = await asyncio.create_subprocess_exec(
        "wc", "-l",
        stdin=grep.stdout
    )
    await asyncio.gather(cat.wait(), grep.wait(), wc.wait())
```

## Features

**Concurrent pipelines** - All stages run in parallel via `os.pipe()`, just like a real shell. No buffering entire outputs in memory.

**Async-native** - Commands are lazy until awaited. Build pipelines, pass them around, execute when ready.

**Pipefail by default** - Returns first non-zero exit code from any pipeline stage.

**SIGPIPE handling** - Early termination works naturally (128 + signal for killed processes).

## Control Flow

Use Python:

```python
# Sequential (&&)
if await sh.mkdir("dir") == 0:
    await sh.touch("dir/file")

# Fallback (||)
if await sh.test("-f", "config.json") != 0:
    await sh.cp("config.default.json", "config.json")

# Timeout
await asyncio.wait_for(sh.long_running(), timeout=30)

# Background
task = asyncio.create_task(sh.server())
```

## Per-fd Redirects

Tuple syntax targets specific file descriptors:

```python
await (sh.make() > (STDERR, "err.log"))        # stderr to file
await (sh.make() >> (STDERR, "err.log"))       # stderr append
await (sh.cat() < (3, "data.txt"))             # fd 3 from file
await (sh.cat() << (3, "input"))               # fd 3 from string
```

## Combinators

Operators delegate to functions for when you need them:

```python
from shish import out, run, pipe, write, read, feed, close, sub_in, sub_out, env, cwd

pipe(sh.a(), sh.b(), sh.c())              # varargs pipeline
write(read(sh.cat(), "in"), "out")        # functional composition
write(sh.make(), "err.log", fd=STDERR)    # stderr to file
read(sh.cat(), sub_in(sh.sort("a.txt")))  # read from process sub
write(sh.tee(), sub_out(sh.gzip() > "a.gz"))  # write to process sub
close(sh.make(), STDERR)                  # close stderr
env(sh.echo(), FOO="bar")                # set env vars
cwd(sh.pwd(), "/tmp")                    # set working directory
stdout = await out(sh.ls())               # capture stdout as str
```

## See Also

- **[sh](https://github.com/amoffat/sh)** - Popular shell wrapper. Uses `_in=` for piping but processes run sequentially due to Python's eager evaluation. Sync-only.

- **[plumbum](https://plumbum.readthedocs.io/)** - Supports `|` operator but requires `cmd["arg1", "arg2"]()` syntax. Has async support but more complex API.
