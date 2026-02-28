"""Tests for the IR layer: frozen dataclasses and factory functions."""

from pathlib import Path

from shish import ir

# =============================================================================
# Basic IR construction
# =============================================================================


def test_cmd_frozen() -> None:
    command = ir.Cmd(("echo", "hello"))
    assert command.args == ("echo", "hello")
    assert command.redirects == ()


def test_cmd_with_redirects() -> None:
    command = ir.Cmd(
        ("cat",),
        redirects=(ir.FdFromFile(0, Path("in.txt")),),
    )
    assert command.args == ("cat",)
    assert command.redirects == (ir.FdFromFile(0, Path("in.txt")),)


def test_cmd_with_multiple_redirects() -> None:
    command = ir.Cmd(
        ("cat",),
        redirects=(
            ir.FdFromData(0, "hello"),
            ir.FdToFile(1, Path("out.txt")),
        ),
    )
    assert command.redirects == (
        ir.FdFromData(0, "hello"),
        ir.FdToFile(1, Path("out.txt")),
    )


def test_pipeline_frozen() -> None:
    cmd_a = ir.Cmd(("a",))
    cmd_b = ir.Cmd(("b",))
    pipeline = ir.Pipeline((cmd_a, cmd_b))
    assert pipeline.stages == (cmd_a, cmd_b)


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
    redirect = ir.FdToFile(1, Path("out.txt"))
    assert redirect.fd == 1
    assert redirect.path == Path("out.txt")
    assert redirect.append is False


def test_fd_to_file_append() -> None:
    redirect = ir.FdToFile(1, Path("out.txt"), append=True)
    assert redirect.append is True


def test_fd_to_file_arbitrary_fd() -> None:
    redirect = ir.FdToFile(3, Path("log.txt"))
    assert redirect.fd == 3


def test_fd_from_file() -> None:
    redirect = ir.FdFromFile(0, Path("/tmp/f"))
    assert redirect.fd == 0
    assert redirect.path == Path("/tmp/f")


def test_fd_from_data_str() -> None:
    redirect = ir.FdFromData(0, "hello")
    assert redirect.fd == 0
    assert redirect.data == "hello"


def test_fd_from_data_bytes() -> None:
    redirect = ir.FdFromData(0, b"hello")
    assert redirect.data == b"hello"


def test_fd_to_fd() -> None:
    redirect = ir.FdToFd(1, 2)
    assert redirect.src == 1
    assert redirect.dst == 2


def test_fd_close() -> None:
    redirect = ir.FdClose(3)
    assert redirect.fd == 3


def test_fd_from_sub() -> None:
    inner = ir.Cmd(("sort", "file.txt"))
    sub = ir.SubIn(inner)
    redirect = ir.FdFromSub(0, sub)
    assert redirect.fd == 0
    assert redirect.sub.cmd == inner


def test_fd_to_sub() -> None:
    inner = ir.Cmd(("cat",))
    sub = ir.SubOut(inner)
    redirect = ir.FdToSub(1, sub)
    assert redirect.fd == 1
    assert redirect.sub.cmd == inner


# =============================================================================
# Factory: pipeline() — flattening
# =============================================================================


def test_pipeline_factory_flat() -> None:
    cmd_a = ir.Cmd(("a",))
    cmd_b = ir.Cmd(("b",))
    pipeline = ir.pipeline(cmd_a, cmd_b)
    assert pipeline.stages == (cmd_a, cmd_b)


def test_pipeline_factory_flattens_nested() -> None:
    cmd_a = ir.Cmd(("a",))
    cmd_b = ir.Cmd(("b",))
    cmd_c = ir.Cmd(("c",))
    inner = ir.Pipeline((cmd_a, cmd_b))
    pipeline = ir.pipeline(inner, cmd_c)
    assert pipeline.stages == (cmd_a, cmd_b, cmd_c)


def test_pipeline_factory_flattens_both_sides() -> None:
    cmd_a = ir.Cmd(("a",))
    cmd_b = ir.Cmd(("b",))
    cmd_c = ir.Cmd(("c",))
    cmd_d = ir.Cmd(("d",))
    left = ir.Pipeline((cmd_a, cmd_b))
    right = ir.Pipeline((cmd_c, cmd_d))
    pipeline = ir.pipeline(left, right)
    assert pipeline.stages == (cmd_a, cmd_b, cmd_c, cmd_d)


def test_pipeline_factory_preserves_cmd_redirects() -> None:
    cmd_a = ir.Cmd(("a",))
    cmd_b = ir.Cmd(("b",), redirects=(ir.FdToFile(1, Path("out.txt")),))
    pipeline = ir.pipeline(cmd_a, cmd_b)
    assert pipeline.stages == (cmd_a, cmd_b)
    assert pipeline.stages[1].redirects == (ir.FdToFile(1, Path("out.txt")),)
