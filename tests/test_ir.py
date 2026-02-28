"""Tests for the IR layer: frozen dataclasses and factory functions."""

from pathlib import Path

from shish import ir

# =============================================================================
# Basic IR construction
# =============================================================================


def test_cmd_frozen() -> None:
    node = ir.Cmd(("echo", "hello"))
    assert node.args == ("echo", "hello")
    assert node.redirects == ()


def test_cmd_with_redirects() -> None:
    node = ir.Cmd(
        ("cat",),
        redirects=(ir.FdFromFile(0, Path("in.txt")),),
    )
    assert node.args == ("cat",)
    assert node.redirects == (ir.FdFromFile(0, Path("in.txt")),)


def test_cmd_with_multiple_redirects() -> None:
    node = ir.Cmd(
        ("cat",),
        redirects=(
            ir.FdFromData(0, "hello"),
            ir.FdToFile(1, Path("out.txt")),
        ),
    )
    assert node.redirects == (
        ir.FdFromData(0, "hello"),
        ir.FdToFile(1, Path("out.txt")),
    )


def test_pipeline_frozen() -> None:
    cmd_a = ir.Cmd(("a",))
    cmd_b = ir.Cmd(("b",))
    node = ir.Pipeline((cmd_a, cmd_b))
    assert node.stages == (cmd_a, cmd_b)


def test_sub_in_frozen() -> None:
    cmd = ir.Cmd(("sort",))
    sub = ir.SubIn(cmd)
    assert sub.cmd == cmd


def test_sub_out_frozen() -> None:
    cmd = ir.Cmd(("sort",))
    sub = ir.SubOut(cmd)
    assert sub.cmd == cmd


# =============================================================================
# Redirect types
# =============================================================================


def test_fd_to_file_default() -> None:
    node = ir.FdToFile(1, Path("out.txt"))
    assert node.fd == 1
    assert node.path == Path("out.txt")
    assert node.append is False


def test_fd_to_file_append() -> None:
    node = ir.FdToFile(1, Path("out.txt"), append=True)
    assert node.append is True


def test_fd_to_file_arbitrary_fd() -> None:
    node = ir.FdToFile(3, Path("log.txt"))
    assert node.fd == 3


def test_fd_from_file() -> None:
    node = ir.FdFromFile(0, Path("/tmp/f"))
    assert node.fd == 0
    assert node.path == Path("/tmp/f")


def test_fd_from_data_str() -> None:
    node = ir.FdFromData(0, "hello")
    assert node.fd == 0
    assert node.data == "hello"


def test_fd_from_data_bytes() -> None:
    node = ir.FdFromData(0, b"hello")
    assert node.data == b"hello"


def test_fd_to_fd() -> None:
    node = ir.FdToFd(1, 2)
    assert node.src == 1
    assert node.dst == 2


def test_fd_close() -> None:
    node = ir.FdClose(3)
    assert node.fd == 3


def test_fd_from_sub() -> None:
    inner = ir.Cmd(("sort", "file.txt"))
    sub = ir.SubIn(inner)
    node = ir.FdFromSub(0, sub)
    assert node.fd == 0
    assert node.sub.cmd == inner


def test_fd_to_sub() -> None:
    inner = ir.Cmd(("cat",))
    sub = ir.SubOut(inner)
    node = ir.FdToSub(1, sub)
    assert node.fd == 1
    assert node.sub.cmd == inner


# =============================================================================
# Factory: pipeline() — flattening
# =============================================================================


def test_pipeline_factory_flat() -> None:
    cmd_a = ir.Cmd(("a",))
    cmd_b = ir.Cmd(("b",))
    node = ir.pipeline(cmd_a, cmd_b)
    assert node.stages == (cmd_a, cmd_b)


def test_pipeline_factory_flattens_nested() -> None:
    cmd_a = ir.Cmd(("a",))
    cmd_b = ir.Cmd(("b",))
    cmd_c = ir.Cmd(("c",))
    inner = ir.Pipeline((cmd_a, cmd_b))
    node = ir.pipeline(inner, cmd_c)
    assert node.stages == (cmd_a, cmd_b, cmd_c)


def test_pipeline_factory_flattens_both_sides() -> None:
    cmd_a = ir.Cmd(("a",))
    cmd_b = ir.Cmd(("b",))
    cmd_c = ir.Cmd(("c",))
    cmd_d = ir.Cmd(("d",))
    left = ir.Pipeline((cmd_a, cmd_b))
    right = ir.Pipeline((cmd_c, cmd_d))
    node = ir.pipeline(left, right)
    assert node.stages == (cmd_a, cmd_b, cmd_c, cmd_d)


def test_pipeline_factory_preserves_cmd_redirects() -> None:
    cmd_a = ir.Cmd(("a",))
    cmd_b = ir.Cmd(("b",), redirects=(ir.FdToFile(1, Path("out.txt")),))
    node = ir.pipeline(cmd_a, cmd_b)
    assert node.stages == (cmd_a, cmd_b)
    assert node.stages[1].redirects == (ir.FdToFile(1, Path("out.txt")),)
