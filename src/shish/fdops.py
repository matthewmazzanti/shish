"""Fd-table simulation: build an ordered op list and compute pass_fds.

Pure simulation — no OS calls. Methods record abstract operations and
track the child's live fd set. The backend (Executor, posix_spawn, etc.)
interprets the ops into real syscalls.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

STDIN: int = 0
STDOUT: int = 1
STDERR: int = 2


# ── Operations (pure data, interpreted by backend) ───────────────────


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

    def __init__(self, live: set[int] | None = None) -> None:
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
