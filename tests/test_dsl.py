from pathlib import Path

import pytest

from shish import (
    STDIN,
    STDOUT,
    Cmd,
    Pipeline,
    cmd,
    feed,
    ir,
    pipe,
    read,
    sh,
    sub_in,
    sub_out,
    unwrap,
    write,
)

# =============================================================================
# Magic cmd() builder
# =============================================================================


def test_cmd_getattr() -> None:
    assert unwrap(cmd().echo) == ir.Cmd(("echo",))


def test_cmd_chain() -> None:
    assert unwrap(cmd().git.status) == ir.Cmd(("git", "status"))


def test_cmd_call_args() -> None:
    assert unwrap(cmd().echo("hello", "world")) == ir.Cmd(("echo", "hello", "world"))


def test_cmd_call_short_flag() -> None:
    assert unwrap(cmd().ls(l=True, a=True)) == ir.Cmd(("ls", "-l", "-a"))


def test_cmd_call_long_flag() -> None:
    assert unwrap(cmd().git.commit(message="fix")) == ir.Cmd(
        ("git", "commit", "--message", "fix")
    )


def test_cmd_call_flag_false() -> None:
    assert unwrap(cmd().ls(l=True, a=False)) == ir.Cmd(("ls", "-l"))


def test_cmd_underscore_to_dash() -> None:
    assert unwrap(cmd().foo(some_flag="value")) == ir.Cmd(
        ("foo", "--some-flag", "value")
    )


# =============================================================================
# sh shorthand
# =============================================================================


def test_sh_basic() -> None:
    assert unwrap(sh.echo("hello")) == ir.Cmd(("echo", "hello"))


def test_sh_subcommand() -> None:
    assert unwrap(sh.git.status()) == ir.Cmd(("git", "status"))


def test_sh_deep_chain() -> None:
    assert unwrap(sh.docker.compose.up(d=True)) == ir.Cmd(
        ("docker", "compose", "up", "-d")
    )


def test_sh_mixed() -> None:
    assert unwrap(sh.git.commit("file.txt", m="fix", amend=True)) == ir.Cmd(
        ("git", "commit", "file.txt", "-m", "fix", "--amend")
    )


def test_cmd_path_arg() -> None:
    assert unwrap(sh.cat(Path("/tmp/file.txt"))) == ir.Cmd(("cat", "/tmp/file.txt"))


# =============================================================================
# Pipe operator
# =============================================================================


def test_pipe_creates_pipeline() -> None:
    result = sh.echo("hello") | sh.cat()
    assert isinstance(result, Pipeline)
    assert unwrap(result) == ir.Pipeline(
        (
            ir.Cmd(("echo", "hello")),
            ir.Cmd(("cat",)),
        )
    )


def test_pipe_chain() -> None:
    result = sh.echo("hello") | sh.grep("h") | sh.wc(l=True)
    assert unwrap(result) == ir.Pipeline(
        (
            ir.Cmd(("echo", "hello")),
            ir.Cmd(("grep", "h")),
            ir.Cmd(("wc", "-l")),
        )
    )


def test_pipe_flattens_left() -> None:
    p1 = sh.a() | sh.b()
    result = p1 | sh.c()
    assert unwrap(result) == ir.Pipeline(
        (
            ir.Cmd(("a",)),
            ir.Cmd(("b",)),
            ir.Cmd(("c",)),
        )
    )


def test_pipe_flattens_right() -> None:
    p1 = sh.b() | sh.c()
    result = sh.a() | p1
    assert unwrap(result) == ir.Pipeline(
        (
            ir.Cmd(("a",)),
            ir.Cmd(("b",)),
            ir.Cmd(("c",)),
        )
    )


def test_pipe_flattens_both() -> None:
    p1 = sh.a() | sh.b()
    p2 = sh.c() | sh.d()
    result = p1 | p2
    assert unwrap(result) == ir.Pipeline(
        (
            ir.Cmd(("a",)),
            ir.Cmd(("b",)),
            ir.Cmd(("c",)),
            ir.Cmd(("d",)),
        )
    )


# =============================================================================
# Redirect operators
# =============================================================================


def test_redirect_stdout() -> None:
    result = sh.echo("hello") > "out.txt"
    assert isinstance(result, Cmd)
    assert unwrap(result) == ir.Cmd(
        ("echo", "hello"),
        redirects=(ir.FdToFile(STDOUT, Path("out.txt")),),
    )


def test_redirect_stdout_append() -> None:
    result = sh.echo("hello") >> "out.txt"
    assert isinstance(result, Cmd)
    assert unwrap(result) == ir.Cmd(
        ("echo", "hello"),
        redirects=(ir.FdToFile(STDOUT, Path("out.txt"), append=True),),
    )


def test_redirect_stdin_file() -> None:
    result = sh.cat() < "in.txt"
    assert isinstance(result, Cmd)
    assert unwrap(result) == ir.Cmd(
        ("cat",),
        redirects=(ir.FdFromFile(STDIN, Path("in.txt")),),
    )


def test_redirect_stdin_data() -> None:
    result = sh.cat() << "hello"
    assert isinstance(result, Cmd)
    assert unwrap(result) == ir.Cmd(
        ("cat",),
        redirects=(ir.FdFromData(STDIN,"hello"),),
    )


def test_redirect_chain_stdin_stdout() -> None:
    result = (sh.cat() < "in.txt") > "out.txt"
    assert unwrap(result) == ir.Cmd(
        ("cat",),
        redirects=(
            ir.FdFromFile(STDIN, Path("in.txt")),
            ir.FdToFile(STDOUT, Path("out.txt")),
        ),
    )


def test_redirect_chain_stdout_stdin() -> None:
    result = (sh.cat() > "out.txt") < "in.txt"
    assert unwrap(result) == ir.Cmd(
        ("cat",),
        redirects=(
            ir.FdToFile(STDOUT, Path("out.txt")),
            ir.FdFromFile(STDIN, Path("in.txt")),
        ),
    )


def test_redirect_pipeline() -> None:
    result = (sh.cat() | sh.grep("x")) > "out.txt"
    assert isinstance(result, Pipeline)
    assert unwrap(result) == ir.Pipeline(
        (
            ir.Cmd(("cat",)),
            ir.Cmd(("grep", "x"), redirects=(ir.FdToFile(STDOUT, Path("out.txt")),)),
        )
    )


def test_redirect_bool_raises() -> None:
    with pytest.raises(TypeError, match="parentheses"):
        bool(sh.cat() < "in.txt")


# =============================================================================
# Redirects as pipeline stages
# =============================================================================


def test_redirect_in_pipeline_first() -> None:
    result = (sh.cat() < "in.txt") | sh.grep("x")
    assert unwrap(result) == ir.Pipeline(
        (
            ir.Cmd(("cat",), redirects=(ir.FdFromFile(STDIN, Path("in.txt")),)),
            ir.Cmd(("grep", "x")),
        )
    )


def test_redirect_in_pipeline_last() -> None:
    result = sh.echo("hello") | (sh.cat() > "out.txt")
    assert unwrap(result) == ir.Pipeline(
        (
            ir.Cmd(("echo", "hello")),
            ir.Cmd(("cat",), redirects=(ir.FdToFile(STDOUT, Path("out.txt")),)),
        )
    )


def test_redirect_in_pipeline_middle() -> None:
    result = sh.a() | (sh.b() > "log.txt") | sh.c()
    assert unwrap(result) == ir.Pipeline(
        (
            ir.Cmd(("a",)),
            ir.Cmd(("b",), redirects=(ir.FdToFile(STDOUT, Path("log.txt")),)),
            ir.Cmd(("c",)),
        )
    )


# =============================================================================
# Combinator functions
# =============================================================================


def test_pipe_two() -> None:
    result = pipe(sh.echo("hello"), sh.cat())
    assert isinstance(result, Pipeline)
    assert unwrap(result) == ir.Pipeline(
        (
            ir.Cmd(("echo", "hello")),
            ir.Cmd(("cat",)),
        )
    )


def test_pipe_varargs() -> None:
    result = pipe(sh.a(), sh.b(), sh.c(), sh.d())
    assert unwrap(result) == ir.Pipeline(
        (
            ir.Cmd(("a",)),
            ir.Cmd(("b",)),
            ir.Cmd(("c",)),
            ir.Cmd(("d",)),
        )
    )


def test_pipe_flattens() -> None:
    p1 = pipe(sh.a(), sh.b())
    result = pipe(p1, sh.c())
    assert unwrap(result) == ir.Pipeline(
        (
            ir.Cmd(("a",)),
            ir.Cmd(("b",)),
            ir.Cmd(("c",)),
        )
    )


def test_write_fn() -> None:
    result = write(sh.echo("hello"), "out.txt")
    assert isinstance(result, Cmd)
    assert unwrap(result) == ir.Cmd(
        ("echo", "hello"),
        redirects=(ir.FdToFile(STDOUT, Path("out.txt")),),
    )


def test_write_append_fn() -> None:
    result = write(sh.echo("hello"), "out.txt", append=True)
    assert unwrap(result) == ir.Cmd(
        ("echo", "hello"),
        redirects=(ir.FdToFile(STDOUT, Path("out.txt"), append=True),),
    )


def test_read_fn() -> None:
    result = read(sh.cat(), "in.txt")
    assert isinstance(result, Cmd)
    assert unwrap(result) == ir.Cmd(
        ("cat",),
        redirects=(ir.FdFromFile(STDIN, Path("in.txt")),),
    )


def test_feed_fn() -> None:
    result = feed(sh.cat(), "hello")
    assert unwrap(result) == ir.Cmd(
        ("cat",),
        redirects=(ir.FdFromData(STDIN,"hello"),),
    )


def test_chain_read_write() -> None:
    result = write(read(sh.cat(), "in.txt"), "out.txt")
    assert unwrap(result) == ir.Cmd(
        ("cat",),
        redirects=(
            ir.FdFromFile(STDIN, Path("in.txt")),
            ir.FdToFile(STDOUT, Path("out.txt")),
        ),
    )


# =============================================================================
# Process substitution
# =============================================================================


def test_sub_in() -> None:
    assert sub_in(sh.sort("a.txt")) == ir.SubIn(ir.Cmd(("sort", "a.txt")))


def test_sub_out() -> None:
    assert sub_out(sh.gzip()) == ir.SubOut(ir.Cmd(("gzip",)))


def test_sub_in_with_pipeline() -> None:
    result = sub_in(sh.cat("a") | sh.sort())
    assert result == ir.SubIn(
        ir.Pipeline(
            (
                ir.Cmd(("cat", "a")),
                ir.Cmd(("sort",)),
            )
        )
    )


def test_sub_out_with_redirect() -> None:
    result = sub_out(sh.gzip() > "out.gz")
    assert result == ir.SubOut(
        ir.Cmd(("gzip",), redirects=(ir.FdToFile(STDOUT, Path("out.gz")),))
    )
