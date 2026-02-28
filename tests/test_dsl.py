from pathlib import Path

import pytest

from shish import (
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


def test_cmd_getattr() -> None:
    assert unwrap(cmd().echo).args == ("echo",)


def test_cmd_chain() -> None:
    assert unwrap(cmd().git.status).args == ("git", "status")


def test_cmd_call_args() -> None:
    assert unwrap(cmd().echo("hello", "world")).args == ("echo", "hello", "world")


def test_cmd_call_short_flag() -> None:
    assert unwrap(cmd().ls(l=True, a=True)).args == ("ls", "-l", "-a")


def test_cmd_call_long_flag() -> None:
    assert unwrap(cmd().git.commit(message="fix")).args == (
        "git",
        "commit",
        "--message",
        "fix",
    )


def test_cmd_call_flag_false() -> None:
    assert unwrap(cmd().ls(l=True, a=False)).args == ("ls", "-l")


def test_cmd_underscore_to_dash() -> None:
    assert unwrap(cmd().foo(some_flag="value")).args == (
        "foo",
        "--some-flag",
        "value",
    )


def test_sh_basic() -> None:
    assert unwrap(sh.echo("hello")).args == ("echo", "hello")


def test_sh_subcommand() -> None:
    assert unwrap(sh.git.status()).args == ("git", "status")


def test_sh_deep_chain() -> None:
    assert unwrap(sh.docker.compose.up(d=True)).args == (
        "docker",
        "compose",
        "up",
        "-d",
    )


def test_sh_mixed() -> None:
    assert unwrap(sh.git.commit("file.txt", m="fix", amend=True)).args == (
        "git",
        "commit",
        "file.txt",
        "-m",
        "fix",
        "--amend",
    )


def test_cmd_path_arg() -> None:
    assert unwrap(sh.cat(Path("/tmp/file.txt"))).args == ("cat", "/tmp/file.txt")


# Pipeline spec tests


def test_pipe_creates_pipeline() -> None:
    p = sh.echo("hello") | sh.cat()
    assert isinstance(p, Pipeline)
    stages = unwrap(p).stages
    assert len(stages) == 2
    assert stages[0].args == ("echo", "hello")
    assert stages[1].args == ("cat",)


def test_pipe_chain() -> None:
    p = sh.echo("hello") | sh.grep("h") | sh.wc(l=True)
    assert isinstance(p, Pipeline)
    assert len(unwrap(p).stages) == 3


def test_pipe_flattens_left() -> None:
    p1 = sh.a() | sh.b()
    p2 = p1 | sh.c()
    stages = unwrap(p2).stages
    assert len(stages) == 3
    assert stages[0].args == ("a",)
    assert stages[1].args == ("b",)
    assert stages[2].args == ("c",)


def test_pipe_flattens_right() -> None:
    p1 = sh.b() | sh.c()
    p2 = sh.a() | p1
    stages = unwrap(p2).stages
    assert len(stages) == 3
    assert stages[0].args == ("a",)
    assert stages[1].args == ("b",)
    assert stages[2].args == ("c",)


def test_pipe_flattens_both() -> None:
    p1 = sh.a() | sh.b()
    p2 = sh.c() | sh.d()
    p3 = p1 | p2
    assert len(unwrap(p3).stages) == 4


# Redirect spec tests


def test_redirect_stdout() -> None:
    result = sh.echo("hello") > "out.txt"
    assert isinstance(result, Cmd)
    node = unwrap(result)
    assert len(node.redirects) == 1
    assert isinstance(node.redirects[0], ir.FdToFile)
    assert node.redirects[0].path == Path("out.txt")
    assert node.redirects[0].append is False


def test_redirect_stdout_append() -> None:
    result = sh.echo("hello") >> "out.txt"
    assert isinstance(result, Cmd)
    node = unwrap(result)
    assert len(node.redirects) == 1
    assert isinstance(node.redirects[0], ir.FdToFile)
    assert node.redirects[0].append is True


def test_redirect_stdin_file() -> None:
    result = sh.cat() < "in.txt"
    assert isinstance(result, Cmd)
    node = unwrap(result)
    assert len(node.redirects) == 1
    assert isinstance(node.redirects[0], ir.FdFromFile)
    assert node.redirects[0].path == Path("in.txt")


def test_redirect_stdin_data() -> None:
    result = sh.cat() << "hello"
    assert isinstance(result, Cmd)
    node = unwrap(result)
    assert len(node.redirects) == 1
    assert isinstance(node.redirects[0], ir.FdFromData)
    assert node.redirects[0].data == "hello"


def test_redirect_chain_stdin_stdout() -> None:
    result = (sh.cat() < "in.txt") > "out.txt"
    assert isinstance(result, Cmd)
    node = unwrap(result)
    assert len(node.redirects) == 2
    assert isinstance(node.redirects[0], ir.FdFromFile)
    assert isinstance(node.redirects[1], ir.FdToFile)


def test_redirect_chain_stdout_stdin() -> None:
    result = (sh.cat() > "out.txt") < "in.txt"
    assert isinstance(result, Cmd)
    node = unwrap(result)
    assert len(node.redirects) == 2
    assert isinstance(node.redirects[0], ir.FdToFile)
    assert isinstance(node.redirects[1], ir.FdFromFile)


def test_redirect_pipeline() -> None:
    p = sh.cat() | sh.grep("x")
    result = p > "out.txt"
    assert isinstance(result, Pipeline)
    # Pipeline write applies to last stage
    last = unwrap(result).stages[-1]
    assert len(last.redirects) == 1
    assert isinstance(last.redirects[0], ir.FdToFile)


def test_redirect_bool_raises() -> None:
    with pytest.raises(TypeError, match="parentheses"):
        bool(sh.cat() < "in.txt")


# Redirect as pipeline stage tests


def test_redirect_in_pipeline_first() -> None:
    p = (sh.cat() < "in.txt") | sh.grep("x")
    assert isinstance(p, Pipeline)
    stages = unwrap(p).stages
    assert len(stages) == 2
    assert len(stages[0].redirects) == 1
    assert isinstance(stages[0].redirects[0], ir.FdFromFile)
    assert len(stages[1].redirects) == 0


def test_redirect_in_pipeline_last() -> None:
    p = sh.echo("hello") | (sh.cat() > "out.txt")
    assert isinstance(p, Pipeline)
    stages = unwrap(p).stages
    assert len(stages) == 2
    assert len(stages[0].redirects) == 0
    assert len(stages[1].redirects) == 1
    assert isinstance(stages[1].redirects[0], ir.FdToFile)


def test_redirect_in_pipeline_middle() -> None:
    p = sh.a() | (sh.b() > "log.txt") | sh.c()
    assert isinstance(p, Pipeline)
    stages = unwrap(p).stages
    assert len(stages) == 3
    assert len(stages[0].redirects) == 0
    assert len(stages[1].redirects) == 1
    assert isinstance(stages[1].redirects[0], ir.FdToFile)
    assert len(stages[2].redirects) == 0


# Combinator function tests


def test_pipe_two() -> None:
    p = pipe(sh.echo("hello"), sh.cat())
    assert isinstance(p, Pipeline)
    assert len(unwrap(p).stages) == 2


def test_pipe_varargs() -> None:
    p = pipe(sh.a(), sh.b(), sh.c(), sh.d())
    assert isinstance(p, Pipeline)
    assert len(unwrap(p).stages) == 4


def test_pipe_flattens() -> None:
    p1 = pipe(sh.a(), sh.b())
    p2 = pipe(p1, sh.c())
    assert len(unwrap(p2).stages) == 3


def test_write_fn() -> None:
    result = write(sh.echo("hello"), "out.txt")
    assert isinstance(result, Cmd)
    node = unwrap(result)
    assert len(node.redirects) == 1
    assert isinstance(node.redirects[0], ir.FdToFile)
    assert node.redirects[0].path == Path("out.txt")
    assert node.redirects[0].append is False


def test_write_append_fn() -> None:
    result = write(sh.echo("hello"), "out.txt", append=True)
    assert isinstance(result, Cmd)
    node = unwrap(result)
    assert len(node.redirects) == 1
    assert isinstance(node.redirects[0], ir.FdToFile)
    assert node.redirects[0].append is True


def test_read_fn() -> None:
    result = read(sh.cat(), "in.txt")
    assert isinstance(result, Cmd)
    node = unwrap(result)
    assert len(node.redirects) == 1
    assert isinstance(node.redirects[0], ir.FdFromFile)
    assert node.redirects[0].path == Path("in.txt")


def test_feedfn() -> None:
    result = feed(sh.cat(), "hello")
    assert isinstance(result, Cmd)
    node = unwrap(result)
    assert len(node.redirects) == 1
    assert isinstance(node.redirects[0], ir.FdFromData)
    assert node.redirects[0].data == "hello"


def test_chain_read_write() -> None:
    result = write(read(sh.cat(), "in.txt"), "out.txt")
    assert isinstance(result, Cmd)
    node = unwrap(result)
    assert len(node.redirects) == 2
    assert isinstance(node.redirects[0], ir.FdFromFile)
    assert isinstance(node.redirects[1], ir.FdToFile)


def test_sub_in() -> None:
    sub = sub_in(sh.sort("a.txt"))
    assert isinstance(sub, ir.SubIn)
    assert isinstance(sub.cmd, ir.Cmd)
    assert sub.cmd.args == ("sort", "a.txt")


def test_sub_out() -> None:
    sub = sub_out(sh.gzip())
    assert isinstance(sub, ir.SubOut)
    assert isinstance(sub.cmd, ir.Cmd)
    assert sub.cmd.args == ("gzip",)


def test_sub_in_with_pipeline() -> None:
    sub = sub_in(sh.cat("a") | sh.sort())
    assert isinstance(sub, ir.SubIn)
    assert isinstance(sub.cmd, ir.Pipeline)


def test_sub_out_with_redirect() -> None:
    sub = sub_out(sh.gzip() > "out.gz")
    assert isinstance(sub, ir.SubOut)
    assert isinstance(sub.cmd, ir.Cmd)
    assert sub.cmd.args == ("gzip",)
    assert len(sub.cmd.redirects) == 1
    assert isinstance(sub.cmd.redirects[0], ir.FdToFile)
