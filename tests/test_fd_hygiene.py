"""FD hygiene tests — verify child processes see exactly the expected fd set."""

import json
import os
from pathlib import Path

from shish import STDERR, STDIN, STDOUT, ir
from shish.runtime import out, run


def _child_fds(output: str) -> set[int]:
    """Parse _list_fds JSON output into a set of fd numbers."""
    return set(json.loads(output))


# =============================================================================
# Child fd visibility
# =============================================================================


async def test_child_default_fds(list_fds_bin: str) -> None:
    """Plain command sees only stdin, stdout, stderr."""
    fds = _child_fds(await out(ir.Cmd((list_fds_bin,))))
    assert fds == {STDIN, STDOUT, STDERR}


async def test_child_fds_with_redirect(list_fds_bin: str, tmp_path: Path) -> None:
    """Command with fd 3 redirect sees exactly 0, 1, 2, 3."""
    outfile = tmp_path / "out.txt"
    command = ir.Cmd(
        (list_fds_bin,),
        redirects=(ir.FdToFile(3, outfile),),
    )
    fds = _child_fds(await out(command))
    assert fds == {STDIN, STDOUT, STDERR, 3}


async def test_child_fds_with_sub_redirect(list_fds_bin: str) -> None:
    """FdFromSub makes the sub fd visible to the child."""
    sub = ir.SubIn(ir.Cmd(("echo", "hello")))
    command = ir.Cmd(
        (list_fds_bin,),
        redirects=(ir.FdFromSub(3, sub),),
    )
    fds = _child_fds(await out(command))
    assert fds == {STDIN, STDOUT, STDERR, 3}


async def test_child_fds_close_removes_fd(list_fds_bin: str) -> None:
    """FdClose actually removes the fd from the child's table."""
    command = ir.Cmd(
        (list_fds_bin,),
        redirects=(ir.FdClose(STDIN),),
    )
    fds = _child_fds(await out(command))
    assert fds == {STDOUT, STDERR}


async def test_child_fds_multiple_arbitrary(list_fds_bin: str, tmp_path: Path) -> None:
    """Multiple arbitrary fd redirects all visible, no extras."""
    command = ir.Cmd(
        (list_fds_bin,),
        redirects=(
            ir.FdToFile(3, tmp_path / "a.txt"),
            ir.FdToFile(5, tmp_path / "b.txt"),
        ),
    )
    fds = _child_fds(await out(command))
    assert fds == {STDIN, STDOUT, STDERR, 3, 5}


async def test_child_fds_dup_no_extra(list_fds_bin: str) -> None:
    """FdToFd (2>&1) doesn't create extra fds."""
    command = ir.Cmd(
        (list_fds_bin,),
        redirects=(ir.FdToFd(STDOUT, STDERR),),
    )
    fds = _child_fds(await out(command))
    assert fds == {STDIN, STDOUT, STDERR}


async def test_child_fds_sub_in_arg(list_fds_bin: str) -> None:
    """SubIn as arg exposes exactly one extra fd."""
    sub = ir.SubIn(ir.Cmd(("echo", "hello")))
    command = ir.Cmd((list_fds_bin, sub))
    fds = _child_fds(await out(command))
    # Sub fd replaces the SubIn arg with /dev/fd/N — only that fd is extra
    assert STDIN in fds
    assert STDOUT in fds
    assert STDERR in fds
    extra = fds - {STDIN, STDOUT, STDERR}
    assert len(extra) == 1


async def test_child_fds_multiple_sub_in_args(list_fds_bin: str) -> None:
    """Two SubIn args expose exactly two extra fds."""
    sub1 = ir.SubIn(ir.Cmd(("echo", "a")))
    sub2 = ir.SubIn(ir.Cmd(("echo", "b")))
    command = ir.Cmd((list_fds_bin, sub1, sub2))
    fds = _child_fds(await out(command))
    assert STDIN in fds
    assert STDOUT in fds
    assert STDERR in fds
    extra = fds - {STDIN, STDOUT, STDERR}
    assert len(extra) == 2


# =============================================================================
# Pipeline fd isolation
# =============================================================================


async def test_child_fds_pipeline_no_leak(list_fds_bin: str) -> None:
    """Pipeline stage doesn't leak inter-stage pipe fds to child."""
    pipeline = ir.Pipeline(
        (
            ir.Cmd(("true",)),
            ir.Cmd((list_fds_bin,)),
            ir.Cmd(("cat",)),
        )
    )
    fds = _child_fds(await out(pipeline))
    assert fds == {STDIN, STDOUT, STDERR}


async def test_child_fds_pipeline_first_stage_redirect(
    list_fds_bin: str, tmp_path: Path
) -> None:
    """First pipeline stage with stdin redirect still sees only {0,1,2}."""
    infile = tmp_path / "in.txt"
    infile.write_text("hello")
    pipeline = ir.Pipeline(
        (
            ir.Cmd((list_fds_bin,), redirects=(ir.FdFromFile(STDIN, infile),)),
            ir.Cmd(("cat",)),
        )
    )
    fds = _child_fds(await out(pipeline))
    assert fds == {STDIN, STDOUT, STDERR}


# =============================================================================
# Parent fd leak detection
# =============================================================================


async def test_no_fd_leak_after_execution() -> None:
    """Parent process doesn't leak fds after complex execution."""
    before = set(os.listdir("/proc/self/fd"))
    sub = ir.SubIn(ir.Cmd(("echo", "from sub")))
    pipeline = ir.Pipeline(
        (
            ir.Cmd(("cat", sub)),
            ir.Cmd(("cat",)),
        )
    )
    await run(pipeline)
    after = set(os.listdir("/proc/self/fd"))
    assert before == after


async def test_parent_fds_not_leaked_to_child(list_fds_bin: str) -> None:
    """Pipe fds open in parent process are not visible to child."""
    read_fd, write_fd = os.pipe()
    try:
        fds = _child_fds(await out(ir.Cmd((list_fds_bin,))))
        assert read_fd not in fds
        assert write_fd not in fds
    finally:
        os.close(read_fd)
        os.close(write_fd)


async def test_parent_open_file_not_leaked_to_child(
    list_fds_bin: str, tmp_path: Path
) -> None:
    """A file opened in the parent is not visible to child."""
    with open(tmp_path / "leak.txt", "w") as fobj:
        fds = _child_fds(await out(ir.Cmd((list_fds_bin,))))
        assert fobj.fileno() not in fds
