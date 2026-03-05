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
class OwnedFd:
    """Tracked file descriptor with idempotent close."""

    fd: int
    closed: bool = False

    def fileno(self) -> int:
        """Return the raw fd number."""
        return self.fd

    def close(self) -> None:
        """Close the fd if not already closed."""
        if not self.closed:
            self.closed = True
            os.close(self.fd)
