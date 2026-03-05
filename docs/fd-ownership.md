# Fd Ownership in Spawn

## Invariant

Parent provides fds, child dups if needed. Specifically:

- **exec_ (CmdNode)**: fork is an implicit dup. No explicit dup needed.
- **spawn_fn (FnNode)**: runs in-process, dups stdin/stdout/stderr
  internally so its copies survive the parent's close-after-spawn.
- **_spawn_with_pipe / _feed_with_pipe**: pass parent's `std_fds`
  directly to sub StdFds. No dup — the sub handles it recursively.
- **spawn_pipeline**: creates inter-stage pipes, passes Fd objects
  into per-stage StdFds. Closes inter-stage pipes after spawn.

## Fd Ownership

`Fd(fd, owning=True)` (default) — `close()` calls `os.close()`.
Created by `ctx.pipe()`, `ctx.dup()`, and PIPE allocation.

`Fd(fd, owning=False)` — `close()` is a no-op. Used for borrowed
fds: wrapping STDIN/STDOUT/STDERR or caller-owned raw fds.

## Dup Sites

### `StartCtx.__aenter__`
- **PIPE**: `ctx.pipe()` → two owning Fds. Spawn-side closed after
  fork (essential for stdout/stderr EOF propagation).
- **Inherit (None)**: `Fd(STDIN, owning=False)` — no dup, close is
  a no-op.
- **Raw fd (int)**: `Fd(user_fd, owning=False)` — no dup, borrowed.

Close-after-spawn stays uniform: `spawn_{stdin,stdout,stderr}.close()`
works for all cases — owning Fds close the os fd, non-owning are no-ops.

### `SpawnCtx.spawn_fn`
- `self.dup(std_fds.stdin.fd)` → owning Fd for FnNode
- `self.dup(std_fds.stdout.fd)` → owning Fd for FnNode
- `self.dup(std_fds.stderr.fd)` → owning Fd for FnNode

Essential — FnNode runs in-process and needs copies that survive
the parent's close-after-spawn.

### `SpawnCmdCtx` (exec_ path)
No dups. Fork is the implicit dup.

### `SpawnCmdCtx._spawn_with_pipe` / `_feed_with_pipe`
No dups. Passes parent's `std_fds` members directly to sub StdFds.
