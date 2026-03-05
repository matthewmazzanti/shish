"""Shared test utilities."""

import os


def process_fds() -> set[int]:
    """Return the set of open fds in the current process.

    os.listdir("/dev/fd") opens a directory handle that appears in
    its own listing but is closed by the time listdir returns. fstat
    filters it out.
    """
    return {fd for entry in os.listdir("/dev/fd") if _is_open(fd := int(entry))}


def _is_open(fd: int) -> bool:
    try:
        os.fstat(fd)
        return True
    except OSError:
        return False
