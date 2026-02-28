"""Tests for the IR layer: cmd() builder methods, pipeline construction, redirects."""

from pathlib import Path

from shish import STDERR, STDIN, STDOUT, ir
from shish.ir import cmd

# =============================================================================
# Cmd construction
# =============================================================================


def test_cmd_basic() -> None:
    assert cmd("echo", "hello") == ir.Cmd(("echo", "hello"))


def test_cmd_multiple_args() -> None:
    assert cmd("echo", "hello", "world") == ir.Cmd(("echo", "hello", "world"))


def test_cmd_path_arg() -> None:
    assert cmd("cat", Path("/tmp/file.txt")) == ir.Cmd(("cat", "/tmp/file.txt"))


def test_cmd_numeric_str_arg() -> None:
    assert cmd("head", "-n", "5") == ir.Cmd(("head", "-n", "5"))


# =============================================================================
# Pipeline construction
# =============================================================================


def test_pipe_two() -> None:
    result = cmd("echo", "hello").pipe(cmd("cat"))
    assert isinstance(result, ir.Pipeline)
    assert result == ir.Pipeline((
        ir.Cmd(("echo", "hello")),
        ir.Cmd(("cat",)),
    ))


def test_pipe_chain() -> None:
    result = cmd("echo", "hello").pipe(cmd("grep", "h")).pipe(cmd("wc", "-l"))
    assert result == ir.Pipeline((
        ir.Cmd(("echo", "hello")),
        ir.Cmd(("grep", "h")),
        ir.Cmd(("wc", "-l")),
    ))


# =============================================================================
# Redirect construction
# =============================================================================


def test_write_stdout() -> None:
    assert cmd("echo", "hello").write("out.txt") == ir.Cmd(
        ("echo", "hello"), redirects=(ir.FdToFile(STDOUT, Path("out.txt")),)
    )


def test_write_stdout_append() -> None:
    assert cmd("echo", "hello").write("out.txt", append=True) == ir.Cmd(
        ("echo", "hello"),
        redirects=(ir.FdToFile(STDOUT, Path("out.txt"), append=True),),
    )


def test_read_stdin() -> None:
    assert cmd("cat").read("in.txt") == ir.Cmd(
        ("cat",), redirects=(ir.FdFromFile(STDIN, Path("in.txt")),)
    )


def test_feed_stdin_string() -> None:
    assert cmd("cat").feed("hello") == ir.Cmd(
        ("cat",), redirects=(ir.FdFromData(STDIN, "hello"),)
    )


def test_feed_stdin_bytes() -> None:
    assert cmd("cat").feed(b"binary") == ir.Cmd(
        ("cat",), redirects=(ir.FdFromData(STDIN, b"binary"),)
    )


def test_close_fd() -> None:
    assert cmd("cat").close(STDIN) == ir.Cmd(
        ("cat",), redirects=(ir.FdClose(STDIN),)
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


# =============================================================================
# Explicit fd redirects
# =============================================================================


def test_write_explicit_fd() -> None:
    assert cmd("foo").write("err.log", fd=STDERR) == ir.Cmd(
        ("foo",), redirects=(ir.FdToFile(STDERR, Path("err.log")),)
    )


def test_write_append_explicit_fd() -> None:
    assert cmd("foo").write("err.log", append=True, fd=STDERR) == ir.Cmd(
        ("foo",),
        redirects=(ir.FdToFile(STDERR, Path("err.log"), append=True),),
    )


def test_read_explicit_fd() -> None:
    assert cmd("foo").read("in.txt", fd=3) == ir.Cmd(
        ("foo",), redirects=(ir.FdFromFile(3, Path("in.txt")),)
    )


def test_feed_explicit_fd() -> None:
    assert cmd("foo").feed("data", fd=3) == ir.Cmd(
        ("foo",), redirects=(ir.FdFromData(3, "data"),)
    )


# =============================================================================
# Redirects as pipeline stages
# =============================================================================


def test_redirect_in_pipeline_first() -> None:
    result = cmd("cat").read("in.txt").pipe(cmd("grep", "x"))
    assert result == ir.Pipeline((
        ir.Cmd(("cat",), redirects=(ir.FdFromFile(STDIN, Path("in.txt")),)),
        ir.Cmd(("grep", "x")),
    ))


def test_redirect_in_pipeline_last() -> None:
    result = cmd("echo", "hello").pipe(cmd("cat").write("out.txt"))
    assert result == ir.Pipeline((
        ir.Cmd(("echo", "hello")),
        ir.Cmd(
            ("cat",),
            redirects=(ir.FdToFile(STDOUT, Path("out.txt")),),
        ),
    ))


def test_redirect_in_pipeline_middle() -> None:
    result = cmd("a").pipe(cmd("b").write("log.txt")).pipe(cmd("c"))
    assert result == ir.Pipeline((
        ir.Cmd(("a",)),
        ir.Cmd(
            ("b",),
            redirects=(ir.FdToFile(STDOUT, Path("log.txt")),),
        ),
        ir.Cmd(("c",)),
    ))


# =============================================================================
# Process substitution
# =============================================================================


def test_sub_in() -> None:
    result = cmd("sort", "a.txt").sub_in()
    assert isinstance(result, ir.SubIn)
    assert result == ir.SubIn(ir.Cmd(("sort", "a.txt")))


def test_sub_out() -> None:
    result = cmd("gzip").sub_out()
    assert isinstance(result, ir.SubOut)
    assert result == ir.SubOut(ir.Cmd(("gzip",)))


def test_sub_in_as_arg() -> None:
    result = cmd("cat", cmd("echo", "hello").sub_in())
    assert result == ir.Cmd((
        "cat",
        ir.SubIn(ir.Cmd(("echo", "hello"))),
    ))


def test_sub_in_multiple_args() -> None:
    result = cmd(
        "diff",
        cmd("sort", "a.txt").sub_in(),
        cmd("sort", "b.txt").sub_in(),
    )
    assert result == ir.Cmd((
        "diff",
        ir.SubIn(ir.Cmd(("sort", "a.txt"))),
        ir.SubIn(ir.Cmd(("sort", "b.txt"))),
    ))


def test_sub_out_as_arg() -> None:
    result = cmd("tee", cmd("cat").write("out.txt").sub_out())
    assert result == ir.Cmd((
        "tee",
        ir.SubOut(
            ir.Cmd(
                ("cat",),
                redirects=(ir.FdToFile(STDOUT, Path("out.txt")),),
            )
        ),
    ))


def test_sub_out_with_redirect() -> None:
    result = cmd("gzip").write("out.gz").sub_out()
    assert result == ir.SubOut(
        ir.Cmd(
            ("gzip",),
            redirects=(ir.FdToFile(STDOUT, Path("out.gz")),),
        )
    )


# =============================================================================
# Builder-only: .arg() and immutability
# =============================================================================


def test_cmd_arg() -> None:
    assert cmd("echo").arg("hello", "world") == ir.Cmd(
        ("echo", "hello", "world")
    )


def test_cmd_arg_returns_new_instance() -> None:
    base = cmd("echo")
    extended = base.arg("hello")
    assert base == ir.Cmd(("echo",))
    assert extended == ir.Cmd(("echo", "hello"))


# =============================================================================
# Builder-only: pipeline methods
# =============================================================================


def test_pipeline_write_last_stage() -> None:
    result = (
        cmd("echo", "hello")
        .pipe(cmd("tr", "a-z", "A-Z"))
        .write("out.txt")
    )
    assert isinstance(result, ir.Pipeline)
    assert result == ir.Pipeline((
        ir.Cmd(("echo", "hello")),
        ir.Cmd(
            ("tr", "a-z", "A-Z"),
            redirects=(ir.FdToFile(STDOUT, Path("out.txt")),),
        ),
    ))


def test_pipeline_read_first_stage() -> None:
    result = cmd("cat").pipe(cmd("grep", "x")).read("in.txt")
    assert isinstance(result, ir.Pipeline)
    assert result == ir.Pipeline((
        ir.Cmd(
            ("cat",),
            redirects=(ir.FdFromFile(STDIN, Path("in.txt")),),
        ),
        ir.Cmd(("grep", "x")),
    ))


def test_pipeline_feed_first_stage() -> None:
    result = cmd("cat").pipe(cmd("grep", "x")).feed("hello")
    assert result == ir.Pipeline((
        ir.Cmd(("cat",), redirects=(ir.FdFromData(STDIN, "hello"),)),
        ir.Cmd(("grep", "x")),
    ))


def test_pipeline_close_last_stage() -> None:
    result = cmd("cat").pipe(cmd("grep", "x")).close(STDOUT)
    assert result == ir.Pipeline((
        ir.Cmd(("cat",)),
        ir.Cmd(("grep", "x"), redirects=(ir.FdClose(STDOUT),)),
    ))


# =============================================================================
# Builder-only: pipeline factory
# =============================================================================


def test_pipeline_factory_flat() -> None:
    result = ir.pipeline(ir.Cmd(("a",)), ir.Cmd(("b",)))
    assert result == ir.Pipeline((ir.Cmd(("a",)), ir.Cmd(("b",))))


def test_pipeline_factory_flattens_nested() -> None:
    inner = ir.Pipeline((ir.Cmd(("a",)), ir.Cmd(("b",))))
    result = ir.pipeline(inner, ir.Cmd(("c",)))
    assert result == ir.Pipeline((
        ir.Cmd(("a",)),
        ir.Cmd(("b",)),
        ir.Cmd(("c",)),
    ))


def test_pipeline_factory_flattens_both_sides() -> None:
    left = ir.Pipeline((ir.Cmd(("a",)), ir.Cmd(("b",))))
    right = ir.Pipeline((ir.Cmd(("c",)), ir.Cmd(("d",))))
    result = ir.pipeline(left, right)
    assert result == ir.Pipeline((
        ir.Cmd(("a",)),
        ir.Cmd(("b",)),
        ir.Cmd(("c",)),
        ir.Cmd(("d",)),
    ))


def test_pipeline_factory_preserves_redirects() -> None:
    cmd_b = ir.Cmd(
        ("b",), redirects=(ir.FdToFile(STDOUT, Path("out.txt")),)
    )
    result = ir.pipeline(ir.Cmd(("a",)), cmd_b)
    assert result == ir.Pipeline((ir.Cmd(("a",)), cmd_b))
