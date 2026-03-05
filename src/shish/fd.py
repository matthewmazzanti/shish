"""File descriptor constants and ownership tracking."""

from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum, auto


class Pipe(Enum):
    PIPE = auto()


PIPE = Pipe.PIPE
STDIN: int = 0
STDOUT: int = 1
STDERR: int = 2


@dataclass
class Fd:
    """Tracked file descriptor with idempotent close.

    owning=True (default): close() calls os.close().
    owning=False: close() is a no-op. Use for borrowed fds
    (e.g. wrapping STDIN/STDOUT/STDERR or caller-owned fds).
    """

    fd: int
    owning: bool = True
    closed: bool = False

    def fileno(self) -> int:
        """Return the raw fd number."""
        return self.fd

    def close(self) -> None:
        """Close the fd if owning and not already closed."""
        if not self.closed:
            self.closed = True
            if self.owning:
                os.close(self.fd)
