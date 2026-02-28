# Redirect Implementation Notes

## How Bash Handles Redirects

Bash uses `fork()` (not `vfork()` or `posix_spawn()`) with redirects set up in the child:

```
Parent: fork()
           │
Child:     fd = open("filename", O_WRONLY|O_CREAT|O_TRUNC)
           dup2(fd, 1)      # stdout now points to file
           close(fd)
           execve("ls", ...)
```

All redirect setup happens in the child, after fork, before exec. This is the only place it can happen - the parent's fds are unaffected, and after exec the new program inherits the fd table as-is.

Key syscalls (all async-signal-safe):
- `open()` - open file, get fd
- `dup2(oldfd, newfd)` - make newfd point to same file as oldfd
- `close()` - close fd

### Why Bash Uses fork(), Not vfork()

`vfork()` shares memory between parent and child until exec - the child can't modify
anything without corrupting the parent. But bash needs to do setup work in the child:
- Open files for redirects
- Call dup2() to wire up fds
- Reset signal handlers
- Set up process groups

All of this would be illegal with `vfork()`. With modern Copy-on-Write, `fork()` is
cheap anyway for small processes like shells - no actual memory copy until write.

## Current shish Approach

shish opens files in the parent, then passes fds to the child:

```
Parent:  open("filename") → fd 3
         fork()
         │
Child:   inherits fd 3
         subprocess uses stdin=fd3 or stdout=fd3
```

### What Works

| Feature | Implementation | Notes |
|---------|---------------|-------|
| `> file` | open() in parent | Pass fd via stdout= |
| `< file` | open() in parent | Pass fd via stdin= |
| `>> file` | open(O_APPEND) in parent | Pass fd via stdout= |
| `<< data` | pipe() in parent + async write | Pass read end via stdin= |
| `<(cmd)` | pipe() in parent | Run cmd with write end |
| `>(cmd)` | pipe() in parent | Run cmd with read end |

### What Requires preexec_fn

| Feature | Implementation | Notes |
|---------|---------------|-------|
| `2>&1` | dup2(1,2) in child | stderr → stdout |
| `2>file` | open() in parent, dup2(fd,2) in child | stderr → file |
| `1>&2` | dup2(2,1) in child | stdout → stderr |
| `3>&1` | dup2(1,3) in child | arbitrary fd redirect |
| `n>&-` | close(n) in child | close fd |

### Tradeoffs: Parent Opens vs Child Opens

| Scenario | Parent opens | Child opens (bash-style) |
|----------|-------------|-------------------------|
| Error handling | Easy - exception in parent | Hard - need error pipe from child |
| `cwd` + relative path | Relative to parent's cwd | Relative to child's cwd |
| Dropping privileges | Opens as parent's user | Opens as child's user |
| fd-to-fd redirects | Cannot do | Works naturally |

## preexec_fn Safety Analysis

### The Risk

Python's `preexec_fn` runs in the child after fork but before exec. The concern is deadlock from locks held by threads that no longer exist in the child.

From Python docs:
> "The preexec_fn parameter is NOT SAFE to use in the presence of threads in your application. The child process could deadlock before exec is called."

### Why It's Mostly Safe for dup2()

1. **Python 3.11+ uses stack-allocated frames** - function calls don't malloc
2. **dup2() is a thin syscall wrapper** - no complex Python operations
3. **PyOS_AfterFork_Child() resets the GIL** - Python's main lock is reinitialized
4. **No failure mode** - dup2() on valid fds doesn't raise exceptions

Trace through `os.dup2(src, dst)` in Python 3.11+:
```
preexec():                    # _PyInterpreterFrame on C stack (no malloc)
    os.dup2(src, dst)         # PyCFunction - no Python frame
    ↓
    posix_dup2(args)          # C code
      PyArg_ParseTuple()      # extracts ints - small ints are cached
      dup2(fd, fd2)           # syscall - async-signal-safe
      return Py_None          # singleton - no allocation
```

### What's Actually Dangerous

```python
def preexec():
    logging.info("starting")     # stdlib locks, I/O locks, string formatting
    os.environ["FOO"] = "bar"    # might resize dict → malloc
    some_c_extension.setup()     # unknown locks
    open("file", "w")            # exception on failure → malloc
```

### Recommended Hybrid Approach

1. **File redirects**: Open in parent (current approach)
   - Safe, easy error handling
   - No preexec_fn needed

2. **fd-to-fd redirects**: Use preexec_fn with dup2 only
   - Minimal code in fork-exec gap
   - Pure syscalls, no failure modes

```python
# For: cmd 2>&1 > file

# Parent (safe):
file_fd = os.open("file", os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)

# preexec_fn (minimal):
def preexec():
    os.dup2(file_fd, 1)  # stdout → file
    os.dup2(1, 2)        # stderr → stdout
```

## Alternative: C Extension

For maximum safety, a C extension could handle the entire fork-exec:

```c
// Between fork() and execve() - pure C, no Python, no locks
for (int i = 0; i < redirect_count; i += 2) {
    dup2(redirects[i], redirects[i+1]);
}
```

Tradeoffs:
- **Pro**: Completely safe, matches bash exactly
- **Con**: Need to compile and ship binary wheels (~30 per release)
- **Con**: Reimplement subprocess functionality

For shish's scope, preexec_fn with minimal dup2() calls is likely sufficient.

## Alternative: posix_spawn

Python 3.8+ provides `os.posix_spawn()` with built-in file_actions support:

```python
import os

file_actions = os.posix_spawn_file_actions()
file_actions.addopen(1, "out.txt", os.O_WRONLY | os.O_CREAT, 0o644)
file_actions.adddup2(1, 2)  # 2>&1

pid = os.posix_spawnp("ls", ["ls"], os.environ, file_actions=file_actions)
```

Supported file actions:
- `addopen(fd, path, flags, mode)` - open file at fd
- `addclose(fd)` - close fd
- `adddup2(fd, newfd)` - duplicate fd to newfd

### How posix_spawn Works

Under the hood (glibc 2.24+), `posix_spawn()` uses `clone(CLONE_VM | CLONE_VFORK)`:
- Child shares memory with parent (like vfork)
- Parent blocks until child execs
- File actions are applied by libc in the child, using async-signal-safe code

This is completely safe - no Python runs between fork and exec.

### Why Bash Doesn't Use posix_spawn

Bash needs flexibility that posix_spawn doesn't provide:
- Complex conditional logic between fork and exec
- Custom signal handler setup
- Job control setup (process groups, terminal control)
- The file_actions API is limited to open/close/dup2

For a shell, `fork()` + custom setup code is more appropriate.

### posix_spawn vs subprocess

Python's subprocess module *can* use posix_spawn, but rarely does:
- `close_fds=True` (the default) disables it
- Some edge case bugs with environment handling
- On Linux, vfork is often faster anyway

### Tradeoffs for shish

| Approach | fd redirects | Safety | Integration |
|----------|--------------|--------|-------------|
| subprocess + preexec_fn | dup2 in Python | ~safe in 3.11+ | asyncio native |
| os.posix_spawn | file_actions | Completely safe | Manual process mgmt |
| C extension | Pure C dup2 | Completely safe | Complex build |

Using `os.posix_spawn` directly would require reimplementing process management
(pipes, waiting, signals) that asyncio's subprocess already handles.

### Hybrid: posix_spawn + asyncio Child Watcher

It may be possible to use posix_spawn for safe process creation, then hand the PID
to asyncio for lifecycle management:

```python
import os
import asyncio

async def spawn_with_redirects(argv: list[str], redirects: list[tuple[int, int]]) -> int:
    loop = asyncio.get_running_loop()

    # Create pipes in parent
    read_fd, write_fd = os.pipe()

    # Set up file actions (safe - applied by libc, not Python)
    file_actions = os.posix_spawn_file_actions()
    file_actions.adddup2(write_fd, 1)  # stdout → write end
    file_actions.addclose(read_fd)     # close read end in child
    for src, dst in redirects:
        file_actions.adddup2(src, dst)

    # Spawn process
    pid = os.posix_spawnp(argv[0], argv, os.environ, file_actions=file_actions)
    os.close(write_fd)  # close write end in parent

    # Register with asyncio's child watcher for exit notification
    exit_future: asyncio.Future[int] = loop.create_future()
    watcher = asyncio.get_child_watcher()
    watcher.add_child_handler(pid, lambda p, rc: exit_future.set_result(rc))

    # Read stdout asynchronously
    # ... use loop.add_reader() or run_in_executor() ...

    returncode = await exit_future
    return returncode
```

Key pieces:
- `os.posix_spawn()` - safe process creation with file_actions
- `watcher.add_child_handler(pid, callback)` - async exit notification
- `loop.add_reader(fd, callback)` - async pipe reading

Caveats:
- Child watcher API deprecated in Python 3.12+
- `PidfdChildWatcher` (Linux 5.3+) is the modern replacement
- Need to handle pipe buffering, partial reads, etc.
- More investigation needed before implementing

## References

- [Python subprocess docs on preexec_fn](https://docs.python.org/3/library/subprocess.html)
- [CPython _posixsubprocess.c](https://github.com/python/cpython/blob/main/Modules/_posixsubprocess.c)
- [Python 3.11 frame changes](https://discuss.python.org/t/python-3-11-frame-structure-and-various-changes/17895)
- [Fork safety discussion](https://pythondev.readthedocs.io/fork.html)
- [preexec_fn deprecation issue](https://github.com/python/cpython/issues/82616)
- [posix_spawn man page](https://man7.org/linux/man-pages/man3/posix_spawn.3.html)
- [Fork is evil, vfork is goodness](https://gist.github.com/nicowilliams/a8a07b0fc75df05f684c23c18d7db234)
