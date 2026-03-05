"""Per-command redirect resolution, fd-table simulation, and spawn.

SpawnCmdCtx resolves a single Cmd's redirects and arguments into
fd operations and sub-process spawns, then builds the preexec_fn
and pass_fds for the main process spawn.

FdOps simulates the child fd table, emitting ordered operations
(OpOpen, OpDup2, OpClose) that the preexec_fn executes between
fork() and exec().
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import typing as ty
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path

from shish.builders import (
    Cmd,
    FdClose,
    FdFromData,
    FdFromFile,
    FdFromSub,
    FdToFd,
    FdToFile,
    FdToSub,
    Fn,
    Runnable,
    SubIn,
    SubOut,
)
from shish.fd import STDIN, STDOUT, Fd
from shish.runtime.tree import (
    CmdNode,
    ProcessNode,
    StdFds,
)
from shish.streams import (
    ByteStageCtx,
    TextStageCtx,
    make_byte_wrapper,
)

if ty.TYPE_CHECKING:
    from shish.runtime.spawn import SpawnCtx

SUBPROCESS_DEFAULT_FDS = frozenset({STDIN, STDOUT, 2})
FD_DIR = Path("/dev/fd")


# ── Operations (pure data, interpreted by preexec_fn) ────────────────


@dataclass(frozen=True)
class OpOpen:
    fd: int
    path: bytes
    flags: int


@dataclass(frozen=True)
class OpDup2:
    src: int
    dst: int


@dataclass(frozen=True)
class OpClose:
    fd: int


Op = OpOpen | OpDup2 | OpClose


# ── FdOps simulator ─────────────────────────────────────────────────


class FdOps:
    """Simulate child fd table, emit ops and pass_fds.

    Pure data structure — records operations and tracks which fds are
    alive in the child after all ops execute. The backend interprets
    ops into real syscalls (preexec_fn, posix_spawn file_actions, etc.).

    Constructor takes initial live fds: pipeline pipe fds that the
    spawn mechanism wires to 0/1 before our ops run. FdToFd needs to
    know these exist as dup2 sources.
    """

    def __init__(self, live: ty.Iterable[int] | None = None) -> None:
        self._ops: list[Op] = []
        self._live: set[int] = set(live) if live is not None else set()

    def add_live(self, fd: int) -> None:
        """Register an externally-provided fd as live (e.g. parent-allocated pipe)."""
        self._live.add(fd)

    def open(self, fd: int, path: Path, flags: int) -> None:
        """Open path to fd. fd becomes live. Converts path to bytes for child."""
        self._ops.append(OpOpen(fd, bytes(path), flags))
        self._live.add(fd)

    def dup2(self, src: int, dst: int) -> None:
        """dup2(src, dst). dst becomes live, src stays live."""
        if src not in self._live:
            raise ValueError(f"dup2 source fd {src} is not live")
        self._ops.append(OpDup2(src, dst))
        self._live.add(dst)

    def move_fd(self, src: int, dst: int) -> None:
        """dup2(src, dst) then close(src). Use for pipe wiring."""
        self.dup2(src, dst)
        self.close(src)

    def close(self, fd: int) -> None:
        """close(fd). fd leaves live set."""
        self._ops.append(OpClose(fd))
        self._live.discard(fd)

    @property
    def ops(self) -> tuple[Op, ...]:
        """Ordered operations for the child."""
        return tuple(self._ops)

    @property
    def live(self) -> frozenset[int]:
        """Fds alive in child after all ops."""
        return frozenset(self._live)

    def keep_fds(self) -> tuple[int, ...]:
        """All live fds, sorted. Backend decides which need pass_fds."""
        return tuple(sorted(self._live))


# ── SpawnCmdCtx ──────────────────────────────────────────────────────


class SpawnCmdCtx:
    """Resolves redirects/args and spawns a single Cmd.

    Accumulates fds, sub-process spawns, and fd ops while resolving a single Cmd,
    then builds the preexec_fn and pass_fds for the main process spawn.

    All fds are closed in the parent immediately after spawn — children
    inherit via fork, so parent copies must close for EOF propagation.
    Child spawns must dup fds to survive this close; subprocess handles
    this automatically, but in-process Fn stages must dup manually
    (see SpawnCtx.spawn_fn).
    """

    ctx: SpawnCtx
    cmd: Cmd
    std_fds: StdFds
    fdo: FdOps
    fds: list[Fd]
    pending: list[Awaitable[ProcessNode]]
    subs: list[ProcessNode]

    def __init__(self, ctx: SpawnCtx, cmd: Cmd, std_fds: StdFds) -> None:
        self.ctx = ctx
        self.cmd = cmd
        self.std_fds = std_fds
        self.fdo = FdOps(live=SUBPROCESS_DEFAULT_FDS)
        self.fds = []
        self.pending = []
        self.subs = []

    async def spawn(self) -> CmdNode:
        """Spawn a single Cmd with all its redirects and sub-processes.

        Redirect resolution follows a two-layer model matching POSIX:

        Layer 1 — pipe wiring (Popen kwargs): std_fds from the parent
        pipeline become stdin=/stdout= on the subprocess call. Popen
        internally does dup2(pipe_rd, 0) / dup2(pipe_wr, 1).

        Layer 2 — user redirects (preexec_fn via FdOps): ordered fd
        ops that execute in the child after Popen's pipe dup2, so
        user redirects (>, <, 2>&1, etc.) naturally override pipe
        wiring.

        Process substitutions (FdToSub, FdFromSub) and Sub arguments
        (SubOut, SubIn) each allocate a pipe and schedule a spawn
        coroutine. All sub spawns run concurrently with the main
        process spawn via asyncio.gather — safe because pipe fds are
        allocated eagerly (before any spawn).

        After all processes have been spawned, pipe fds used only for
        child inheritance are closed in the parent so EOF propagates.
        """
        # Redirects before args: mirrors bash where redirects and <() are independent.
        # Resolving redirects first means they can't see arg-position sub fds.
        self._resolve_redirects()
        resolved_args = self._resolve_args()
        proc_env = self._resolve_env()

        # Spawn main process and resolve/spawn sub-processes concurrently
        proc, spawned = await asyncio.gather(
            self.ctx.exec_(
                *resolved_args,
                stdin=self.std_fds.stdin.fd,
                stdout=self.std_fds.stdout.fd,
                # Exclude 0/1/2 — subprocess handles those via stdin=/stdout=
                pass_fds=self._build_pass_fds(ignore=SUBPROCESS_DEFAULT_FDS),
                preexec_fn=self._build_preexec(),
                cwd=self.cmd.working_dir,
                env=proc_env,
            ),
            asyncio.gather(*self.pending),
        )
        self.subs.extend(spawned)

        # Close all fds in parent so EOF propagates
        for fd_entry in self.fds:
            fd_entry.close()

        return CmdNode(proc=proc, subs=self.subs)

    def _spawn_with_pipe(self, inner: Runnable, *, to_stdin: bool) -> tuple[Fd, Fd]:
        """Allocate pipe, track fds, register in fdo, schedule sub spawn.

        to_stdin=True: pipe connects to sub's stdin; parent keeps write end.
        to_stdin=False: pipe connects to sub's stdout; parent keeps read end.
        The other side dups from parent STDIN/STDOUT for inherit behavior.
        """
        pipe_r, pipe_w = self.ctx.pipe()
        self.fds.extend([pipe_r, pipe_w])
        if to_stdin:
            self.fdo.add_live(pipe_w.fd)
            inherit_stdout = self.ctx.dup(STDOUT)
            self.fds.append(inherit_stdout)
            self.pending.append(
                self.ctx.spawn(inner, StdFds(stdin=pipe_r, stdout=inherit_stdout))
            )
        else:
            self.fdo.add_live(pipe_r.fd)
            inherit_stdin = self.ctx.dup(STDIN)
            self.fds.append(inherit_stdin)
            self.pending.append(
                self.ctx.spawn(inner, StdFds(stdin=inherit_stdin, stdout=pipe_w))
            )
        return pipe_r, pipe_w

    def _feed_with_pipe(self, data: str | bytes) -> Fd:
        """Allocate pipe, schedule FnNode data write, return read end."""
        pipe_r, pipe_w = self.ctx.pipe()
        # Both ends closed after spawn: SpawnCtx.spawn_fn dups pipe_w,
        # so the FnNode's write end survives parent cleanup.
        self.fds.extend([pipe_r, pipe_w])
        self.fdo.add_live(pipe_r.fd)

        write_data: Callable[[ByteStageCtx], Awaitable[int]]
        if isinstance(data, bytes):

            async def write_byte_data(stage: ByteStageCtx) -> int:
                with contextlib.suppress(OSError):
                    await stage.stdout.write(data)
                return 0

            write_data = write_byte_data
        else:

            async def write_str_data(stage: TextStageCtx) -> int:
                with contextlib.suppress(OSError):
                    await stage.stdout.write(data)
                return 0

            write_data = make_byte_wrapper(write_str_data, "utf-8")

        inherit_stdin = self.ctx.dup(STDIN)
        self.fds.append(inherit_stdin)
        self.pending.append(
            self.ctx.spawn_fn(
                Fn(write_data), StdFds(stdin=inherit_stdin, stdout=pipe_w)
            )
        )
        return pipe_r

    def _resolve_redirects(self) -> None:
        # Feed redirects into FdOps
        for redirect in self.cmd.redirects:
            match redirect:
                case FdToFd(src=src_fd, dst=dst_fd):  # 2>&1
                    self.fdo.dup2(src_fd, dst_fd)

                case FdToFile(
                    fd=target_fd, path=path, append=do_append
                ):  # > file, >> file, 2> file
                    flags = os.O_WRONLY | os.O_CREAT
                    flags |= os.O_APPEND if do_append else os.O_TRUNC
                    self.fdo.open(target_fd, path, flags)

                case FdToSub(fd=target_fd, sub=sub):  # 1> >(cmd), 3> >(cmd)
                    _, pipe_w = self._spawn_with_pipe(sub.cmd, to_stdin=True)
                    self.fdo.move_fd(pipe_w.fd, target_fd)

                case FdFromFile(fd=target_fd, path=path):  # < file, 3< file
                    self.fdo.open(target_fd, path, os.O_RDONLY)

                case FdFromData(fd=target_fd, data=data):  # <<< "string"
                    pipe_r = self._feed_with_pipe(data)
                    self.fdo.move_fd(pipe_r.fd, target_fd)

                case FdFromSub(fd=target_fd, sub=sub):  # < <(cmd), 3< <(cmd)
                    pipe_r, _ = self._spawn_with_pipe(sub.cmd, to_stdin=False)
                    self.fdo.move_fd(pipe_r.fd, target_fd)

                case FdClose(fd=closed_fd):  # 3>&-
                    self.fdo.close(closed_fd)

    def _resolve_args(self) -> list[str]:
        # Resolve Sub arguments to /dev/fd/N paths
        resolved_args: list[str] = []
        for arg in self.cmd.args:
            match arg:
                case str() as string:
                    resolved_args.append(string)
                case SubOut(cmd=inner):
                    _, pipe_w = self._spawn_with_pipe(inner, to_stdin=True)
                    resolved_args.append(self._fd_path_arg(pipe_w.fd))
                case SubIn(cmd=inner):
                    pipe_r, _ = self._spawn_with_pipe(inner, to_stdin=False)
                    resolved_args.append(self._fd_path_arg(pipe_r.fd))

        return resolved_args

    def _resolve_env(self) -> dict[str, str] | None:
        # Build env overlay and resolve working directory
        cmd = self.cmd
        if not cmd.env_vars and cmd.working_dir is None:
            return None

        proc_env = dict(os.environ)
        for key, value in cmd.env_vars:
            if value is None:
                proc_env.pop(key, None)
            else:
                proc_env[key] = value

        if cmd.working_dir is not None:
            proc_env["PWD"] = str(cmd.working_dir)

        return proc_env

    def _build_preexec(self) -> Callable[[], None] | None:
        """Build a preexec_fn closure that executes all fd ops in the child.

        All operations (open, dup2, close) run in the child between fork()
        and exec(). Only async-signal-safe syscalls: open, dup2, close.

        Target fds from OpOpen are protected by pass_fds, so the
        default fd cleanup (which closes fds >2 after preexec_fn)
        won't close them. The intermediate fd from os.open() gets
        dup2'd to the target then closed.
        """

        # Capture ops in closure to avoid preexec allocation
        ops = self.fdo.ops
        if not ops:
            return None

        def _preexec() -> None:
            for op in ops:
                match op:
                    case OpOpen(fd=target_fd, path=path, flags=flags):
                        source_fd = os.open(path, flags, 0o644)
                        if source_fd != target_fd:
                            os.dup2(source_fd, target_fd)
                            os.close(source_fd)
                    case OpDup2(src, dst):
                        os.dup2(src, dst)
                    case OpClose(fd):
                        os.close(fd)

        return _preexec

    def _build_pass_fds(self, ignore: frozenset[int]) -> tuple[int, ...]:
        return tuple(fd for fd in self.fdo.keep_fds() if fd not in ignore)

    @staticmethod
    def _fd_path_arg(fd: int) -> str:
        return str(FD_DIR / str(fd))
