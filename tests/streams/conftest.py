"""Shared fixtures for stream tests."""

import os
from collections.abc import Generator

import pytest

from shish.fd import Fd


@pytest.fixture
def pipe_fds() -> Generator[tuple[Fd, Fd]]:
    """Create an os.pipe(), yielding (read_fd, write_fd) as Fd objects."""
    read_fd, write_fd = os.pipe()
    read_entry = Fd(read_fd)
    write_entry = Fd(write_fd)
    yield read_entry, write_entry
    read_entry.close()
    write_entry.close()


@pytest.fixture
def read_fd(pipe_fds: tuple[Fd, Fd]) -> Fd:
    """The read end of a pipe."""
    return pipe_fds[0]


@pytest.fixture
def write_fd(pipe_fds: tuple[Fd, Fd]) -> Fd:
    """The write end of a pipe."""
    return pipe_fds[1]
