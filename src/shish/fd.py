"""File descriptor constants and ownership tracking."""

from __future__ import annotations

import dataclasses as dc
import os
from enum import Enum, auto


class Pipe(Enum):
    PIPE = auto()


PIPE = Pipe.PIPE
STDIN: int = 0
STDOUT: int = 1
STDERR: int = 2


@dc.dataclass
class Fd:
    """Tracked file descriptor with idempotent close.

    owned=True (default): close() calls os.close().
    owned=False: close() is a no-op. Use for borrowed fds
    (e.g. wrapping STDIN/STDOUT/STDERR or caller-owned fds).
    """

    fd: int
    owned: bool = True
    closed: bool = False

    def fileno(self) -> int:
        """Return the raw fd number."""
        return self.fd

    def close(self) -> None:
        """Close the fd if owned and not already closed."""
        if not self.closed:
            self.closed = True
            if self.owned:
                os.close(self.fd)

    def __enter__(self) -> Fd:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()
