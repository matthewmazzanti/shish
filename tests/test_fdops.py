"""Tests for FdOps: pure fd-table simulation."""

import os
from pathlib import Path

import pytest

from shish.fdops import FdOps, OpClose, OpDup2, OpOpen

# =============================================================================
# Empty state
# =============================================================================


def test_empty() -> None:
    fdo = FdOps()
    assert fdo.ops == ()
    assert fdo.live == frozenset()
    assert fdo.keep_fds() == ()


def test_initial_live_set() -> None:
    fdo = FdOps(live={0, 1, 2})
    assert fdo.live == frozenset({0, 1, 2})
    assert fdo.keep_fds() == (0, 1, 2)


# =============================================================================
# open()
# =============================================================================


def test_open_records_op() -> None:
    fdo = FdOps()
    fdo.open(3, Path("/tmp/f"), os.O_RDONLY)
    assert fdo.ops == (OpOpen(3, b"/tmp/f", os.O_RDONLY),)
    assert 3 in fdo.live
    assert fdo.keep_fds() == (3,)


def test_open_stdout() -> None:
    fdo = FdOps()
    fdo.open(1, Path("/tmp/f"), os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
    assert 1 in fdo.live
    assert fdo.keep_fds() == (1,)


def test_open_preserves_order() -> None:
    fdo = FdOps()
    fdo.open(3, Path("a"), os.O_RDONLY)
    fdo.open(4, Path("b"), os.O_RDONLY)
    assert fdo.ops == (
        OpOpen(3, b"a", os.O_RDONLY),
        OpOpen(4, b"b", os.O_RDONLY),
    )
    assert fdo.keep_fds() == (3, 4)


# =============================================================================
# dup2()
# =============================================================================


def test_dup2_records_op() -> None:
    fdo = FdOps(live={0, 1, 2})
    fdo.dup2(1, 2)
    assert fdo.ops == (OpDup2(1, 2),)
    assert 2 in fdo.live
    assert fdo.keep_fds() == (0, 1, 2)


def test_dup2_arbitrary_target() -> None:
    fdo = FdOps(live={0, 1, 2})
    fdo.dup2(1, 5)
    assert 5 in fdo.live
    assert fdo.keep_fds() == (0, 1, 2, 5)


def test_dup2_does_not_remove_src() -> None:
    """dup2 doesn't close src — src stays live."""
    fdo = FdOps(live={3})
    fdo.dup2(3, 4)
    assert 3 in fdo.live
    assert 4 in fdo.live


def test_dup2_rejects_non_live_src() -> None:
    fdo = FdOps(live={0, 1, 2})
    with pytest.raises(ValueError, match="fd 7 is not live"):
        fdo.dup2(7, 0)


# =============================================================================
# close()
# =============================================================================


def test_close_records_op() -> None:
    fdo = FdOps()
    fdo.close(3)
    assert fdo.ops == (OpClose(3),)
    assert 3 not in fdo.live


def test_close_removes_from_live() -> None:
    fdo = FdOps(live={3})
    fdo.close(3)
    assert 3 not in fdo.live
    assert fdo.keep_fds() == ()


def test_close_after_open() -> None:
    """open then close same fd — not live."""
    fdo = FdOps()
    fdo.open(3, Path("/tmp/f"), os.O_RDONLY)
    fdo.close(3)
    assert 3 not in fdo.live
    assert fdo.keep_fds() == ()


# =============================================================================
# Combined scenarios
# =============================================================================


def test_stdin_stdout_redirects() -> None:
    """cat < in.txt > out.txt"""
    fdo = FdOps(live={0, 1, 2})
    fdo.open(0, Path("in.txt"), os.O_RDONLY)
    fdo.open(1, Path("out.txt"), os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
    assert fdo.keep_fds() == (0, 1, 2)


def test_stderr_to_file() -> None:
    """cmd 2> err.txt"""
    fdo = FdOps(live={0, 1, 2})
    fdo.open(2, Path("err.txt"), os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
    assert fdo.keep_fds() == (0, 1, 2)


def test_arbitrary_fd_redirect() -> None:
    """cmd 3> file"""
    fdo = FdOps(live={0, 1, 2})
    fdo.open(3, Path("file"), os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
    assert fdo.keep_fds() == (0, 1, 2, 3)


def test_multiple_targets() -> None:
    """cmd 3> a 4> b"""
    fdo = FdOps(live={0, 1, 2})
    fdo.open(3, Path("a"), os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
    fdo.open(4, Path("b"), os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
    assert fdo.keep_fds() == (0, 1, 2, 3, 4)


# =============================================================================
# Order-dependent redirect patterns
# =============================================================================


def test_file_then_dup() -> None:
    """> file 2>&1 — both stdout and stderr go to file."""
    fdo = FdOps(live={0, 1, 2})
    fdo.open(1, Path("out.txt"), os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
    fdo.dup2(1, 2)

    ops = fdo.ops
    assert ops[0] == OpOpen(1, b"out.txt", os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
    assert ops[1] == OpDup2(1, 2)


def test_dup_then_file() -> None:
    """2>&1 > file — stderr to original stdout, stdout to file."""
    fdo = FdOps(live={0, 1, 2})
    fdo.dup2(1, 2)
    fdo.open(1, Path("out.txt"), os.O_WRONLY | os.O_CREAT | os.O_TRUNC)

    ops = fdo.ops
    assert ops[0] == OpDup2(1, 2)
    assert ops[1] == OpOpen(1, b"out.txt", os.O_WRONLY | os.O_CREAT | os.O_TRUNC)


def test_open_dup_close() -> None:
    """3> file 4>&3 3>&- — open 3, dup to 4, close 3."""
    fdo = FdOps(live={0, 1, 2})
    fdo.open(3, Path("file"), os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
    fdo.dup2(3, 4)
    fdo.close(3)

    assert 3 not in fdo.live
    assert 4 in fdo.live
    assert fdo.keep_fds() == (0, 1, 2, 4)


# =============================================================================
# Data pipe dup2 (caller allocates pipe, tells FdOps about the wiring)
# =============================================================================


def test_add_live() -> None:
    """add_live registers an external fd without emitting ops."""
    fdo = FdOps()
    fdo.add_live(7)
    assert 7 in fdo.live
    assert fdo.ops == ()
    assert fdo.keep_fds() == (7,)


def test_pipe_move_fd() -> None:
    """Executor allocates pipe read_fd=7, adds it live, moves to fd 0."""
    fdo = FdOps(live={0, 1, 2})
    fdo.add_live(7)
    fdo.move_fd(7, 0)
    assert 0 in fdo.live
    assert 7 not in fdo.live  # move_fd closes src
    assert fdo.keep_fds() == (0, 1, 2)
    assert fdo.ops == (OpDup2(7, 0), OpClose(7))
