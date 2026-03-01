# shish
> sh-ish

Async shell commands for Python with operator-based piping.

```python
from shish import sh, out, STDERR

# Pipelines: cat input.txt | grep error | wc -l
await (sh.cat("input.txt") | sh.grep("error") | sh.wc("-l"))

# Redirect: >  >>  <  <<  with optional (fd, target) for specific fds
await (sh.curl("http://example.com") > "page.html")    # curl ... > page.html
await (sh.grep("error") < "input.txt")                 # grep error < input.txt
await (sh.grep("error") << "line1\nline2\n")           # feed string to stdin
await (sh.make() > (STDERR, "err.log"))                # make 2>err.log

# Capture output
stdout = await out(sh.ls("-la"))                       # stdout=$(ls -la)

# Environment and working directory: %  @
await ({"FOO": "bar"} % sh.echo("$FOO") @ "/tmp")      # FOO=bar echo $FOO  (in /tmp)

# Kwargs to flags, subcommands via attribute access
await sh.git.commit(message="fix bug", amend=True)     # git commit --message 'fix bug' --amend
```

## Features

**Async-native** - Commands are lazy until awaited. Build pipelines, pass them around, execute when ready.

**Concurrent pipelines** - All stages run in parallel via `os.pipe()`, just like a real shell. No buffering entire outputs in memory.

**No shell injection** - Always uses `exec`, never `shell=True`. No quoting or escaping bugs.

**Per-fd control** - Redirect, close, or feed any file descriptor, not just stdin/stdout/stderr. Tuple syntax targets specific fds: `cmd > (STDERR, "file")`.

**Process substitution** - `sub_in()` / `sub_out()` resolve to `/dev/fd/N` at runtime, matching bash `<(cmd)` / `>(cmd)`.

**Pipefail by default** - Returns the rightmost non-zero exit code from any pipeline stage, matching `set -o pipefail`.

**Orphan cleanup** - On error, all spawned processes are SIGKILL'd and reaped, shielded from cancellation. No zombie processes.

**SIGPIPE handling** - Early termination works naturally; killed processes report 128 + signal number.

## Control flow

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

## Combinators

Operators delegate to combinator functions. Use them directly for programmatic composition:

```python
from shish import pipe, write, read, feed, close, sub_in, sub_out, env, cwd

pipe(sh.a(), sh.b(), sh.c())                        # varargs pipeline
write(sh.make(), "err.log", fd=STDERR)              # stderr to file
read(sh.cat(), "input.txt")                         # stdin from file
feed(sh.grep("error"), "line1\nline2\n")            # stdin from string
close(sh.cmd(), STDERR)                             # close stderr
env(sh.echo(), FOO="bar")                           # set env vars
cwd(sh.pwd(), "/tmp")                               # set working directory
```

## Process substitution

`sub_in` / `sub_out` mirror bash's `<(cmd)` / `>(cmd)`. They work as arguments or as redirect sources/targets:

```python
# As arguments - diff <(sort a.txt) <(sort b.txt)
await sh.diff(sub_in(sh.sort("a.txt")), sub_in(sh.sort("b.txt")))

# As redirect sources/targets
await read(sh.cat(), sub_in(sh.sort("a.txt")))              # cat < <(sort a.txt)
await write(sh.echo("hi"), sub_out(sh.gzip() > "out.gz"))   # echo hi > >(gzip > out.gz)
```

## Builder Pattern

`sh` and operators are convenient but rely on `__getattr__` and operator overloading. The IR layer (`shish.ir`) exposes the same functionality as frozen dataclasses with chainable builder methods - no magic, fully typed:

```python
from shish.ir import cmd
from shish.fdops import STDERR

# Chainable builders on frozen dataclasses
grep = cmd("grep", "error").read("input.txt")
make = cmd("make").write("err.log", fd=STDERR)
pipeline = cmd("cat", "input.txt").pipe(cmd("grep", "error")).pipe(cmd("wc", "-l"))

await grep.run()
await pipeline.run()
stdout = await cmd("ls", "-la").out()
```

## Comparison with subprocess, sh, and plumbum

### subprocess

`subprocess.run` is fine for one-off calls, but it doesn't scale well to larger scripts. Shuffling args through lists gets old, capturing output needs extra wiring (`.stdout.read().decode()`), and piping means wiring up fds and concurrent waits yourself. `shell=True` is tempting but then you're responsible for escaping every argument. I've ended up building abstractions on top in various projects to handle this, which is why I started looking elsewhere.

### sh

shish borrows the magic `sh.foo` attribute access from [sh](https://github.com/amoffat/sh). sh calls commands eagerly - `sh.ls()` executes immediately and returns the output. Piping via `_in=` runs the inner command to completion before starting the outer one, so large streams buffer entirely in memory. shish keeps commands lazy until awaited and pipes them concurrently. sh is also synchronous-only and dynamically typed.

### plumbum

shish borrows the `|` operator piping from [plumbum](https://plumbum.readthedocs.io/). plumbum uses bracket indexing (`cmd["arg"]`) rather than function calls, and doesn't support per-fd redirects or process substitution. plumbum is a larger toolkit (SSH remoting, CLI framework, ANSI colors) while shish stays focused on local async command execution.
