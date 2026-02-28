"""Tests for the IR layer: cmd() builder methods, pipeline factory, construction."""

from pathlib import Path

from shish import STDERR, STDIN, STDOUT, ir
from shish.ir import cmd

# =============================================================================
# Cmd construction
# =============================================================================


def test_cmd_basic() -> None:
    assert cmd("echo", "hello") == ir.Cmd(("echo", "hello"))


def test_cmd_arg() -> None:
    assert cmd("echo").arg("hello", "world") == ir.Cmd(("echo", "hello", "world"))


def test_cmd_path_arg() -> None:
    assert cmd("cat", Path("/tmp/file.txt")) == ir.Cmd(("cat", "/tmp/file.txt"))


def test_cmd_numeric_str_arg() -> None:
    assert cmd("head", "-n", "5") == ir.Cmd(("head", "-n", "5"))


def test_cmd_returns_new_instance() -> None:
    base = cmd("echo")
    extended = base.arg("hello")
    assert base == ir.Cmd(("echo",))
    assert extended == ir.Cmd(("echo", "hello"))


# =============================================================================
# Pipeline construction
# =============================================================================


def test_pipe_two() -> None:
    pipeline = cmd("echo", "hello").pipe(cmd("cat"))
    assert isinstance(pipeline, ir.Pipeline)
    assert len(pipeline.stages) == 2
    assert pipeline.stages[0] == ir.Cmd(("echo", "hello"))
    assert pipeline.stages[1] == ir.Cmd(("cat",))


def test_pipe_chain() -> None:
    pipeline = cmd("echo", "hello").pipe(cmd("grep", "h")).pipe(cmd("wc", "-l"))
    assert isinstance(pipeline, ir.Pipeline)
    assert len(pipeline.stages) == 3


def test_pipe_returns_pipeline() -> None:
    assert isinstance(cmd("echo", "hello").pipe(cmd("cat")), ir.Pipeline)


# =============================================================================
# Redirect construction
# =============================================================================


def test_read() -> None:
    assert cmd("cat").read("in.txt") == ir.Cmd(
        ("cat",), redirects=(ir.FdFromFile(STDIN, Path("in.txt")),)
    )


def test_write() -> None:
    assert cmd("echo", "hello").write("out.txt") == ir.Cmd(
        ("echo", "hello"), redirects=(ir.FdToFile(STDOUT, Path("out.txt")),)
    )


def test_write_append() -> None:
    assert cmd("echo", "hello").write("out.txt", append=True) == ir.Cmd(
        ("echo", "hello"),
        redirects=(ir.FdToFile(STDOUT, Path("out.txt"), append=True),),
    )


def test_feed() -> None:
    assert cmd("cat").feed("hello") == ir.Cmd(
        ("cat",), redirects=(ir.FdFromData(STDIN,"hello"),)
    )


def test_feed_bytes() -> None:
    assert cmd("cat").feed(b"binary") == ir.Cmd(
        ("cat",), redirects=(ir.FdFromData(STDIN,b"binary"),)
    )


def test_redirect_chain_read_write() -> None:
    assert cmd("cat").read("in.txt").write("out.txt") == ir.Cmd(
        ("cat",),
        redirects=(
            ir.FdFromFile(STDIN, Path("in.txt")),
            ir.FdToFile(STDOUT, Path("out.txt")),
        ),
    )


def test_redirect_chain_write_read() -> None:
    assert cmd("cat").write("out.txt").read("in.txt") == ir.Cmd(
        ("cat",),
        redirects=(
            ir.FdToFile(STDOUT, Path("out.txt")),
            ir.FdFromFile(STDIN, Path("in.txt")),
        ),
    )


def test_redirect_returns_cmd() -> None:
    assert isinstance(cmd("cat").read("in.txt"), ir.Cmd)


def test_read_explicit_fd() -> None:
    assert cmd("foo").read("in.txt", fd=3) == ir.Cmd(
        ("foo",), redirects=(ir.FdFromFile(3, Path("in.txt")),)
    )


def test_write_explicit_fd() -> None:
    assert cmd("foo").write("err.log", fd=2) == ir.Cmd(
        ("foo",), redirects=(ir.FdToFile(STDERR, Path("err.log")),)
    )


def test_feed_explicit_fd() -> None:
    assert cmd("foo").feed("data", fd=3) == ir.Cmd(
        ("foo",), redirects=(ir.FdFromData(3, "data"),)
    )


def test_pipeline_write() -> None:
    pipeline = cmd("echo", "hello").pipe(cmd("tr", "a-z", "A-Z")).write("out.txt")
    assert isinstance(pipeline, ir.Pipeline)
    assert len(pipeline.stages) == 2
    assert pipeline.stages[0] == ir.Cmd(("echo", "hello"))
    assert pipeline.stages[1] == ir.Cmd(
        ("tr", "a-z", "A-Z"), redirects=(ir.FdToFile(STDOUT, Path("out.txt")),)
    )


def test_pipeline_read() -> None:
    pipeline = cmd("cat").pipe(cmd("grep", "x")).read("in.txt")
    assert isinstance(pipeline, ir.Pipeline)
    assert len(pipeline.stages) == 2
    assert pipeline.stages[0] == ir.Cmd(
        ("cat",), redirects=(ir.FdFromFile(STDIN, Path("in.txt")),)
    )
    assert pipeline.stages[1] == ir.Cmd(("grep", "x"))


# =============================================================================
# Process substitution construction
# =============================================================================


def test_sub_in() -> None:
    sub = cmd("sort", "a.txt").sub_in()
    assert isinstance(sub, ir.SubIn)
    assert sub.cmd == ir.Cmd(("sort", "a.txt"))


def test_sub_out() -> None:
    sub = cmd("gzip").sub_out()
    assert isinstance(sub, ir.SubOut)
    assert sub.cmd == ir.Cmd(("gzip",))


def test_cmd_with_sub_arg() -> None:
    sub = cmd("sort", "a.txt").sub_in()
    command = cmd("cat", sub)
    assert isinstance(command, ir.Cmd)
    assert command.args == ("cat", ir.SubIn(ir.Cmd(("sort", "a.txt"))))


# =============================================================================
# Redirect piping
# =============================================================================


def test_redirect_pipe() -> None:
    pipeline = cmd("cat").read("in.txt").pipe(cmd("grep", "x"))
    assert isinstance(pipeline, ir.Pipeline)
    assert len(pipeline.stages) == 2
    assert pipeline.stages[0] == ir.Cmd(
        ("cat",), redirects=(ir.FdFromFile(STDIN, Path("in.txt")),)
    )
    assert pipeline.stages[1] == ir.Cmd(("grep", "x"))


# =============================================================================
# Pipeline factory — flattening
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
    cmd_b = ir.Cmd(("b",), redirects=(ir.FdToFile(STDOUT, Path("out.txt")),))
    pipeline = ir.pipeline(cmd_a, cmd_b)
    assert pipeline.stages == (cmd_a, cmd_b)
    assert pipeline.stages[1].redirects == (ir.FdToFile(STDOUT, Path("out.txt")),)
