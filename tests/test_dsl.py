from pathlib import Path

import pytest

from shish import (
    Cmd,
    FromData,
    FromFile,
    Pipeline,
    Redirect,
    Sub,
    ToFile,
    append,
    from_proc,
    input_,
    pipe,
    read,
    sh,
    to_proc,
    write,
)


def test_cmd_init() -> None:
    cmd = Cmd(["echo", "hello"])
    assert cmd.args == ["echo", "hello"]


def test_cmd_init_empty() -> None:
    cmd = Cmd()
    assert cmd.args == []


def test_cmd_getattr() -> None:
    cmd = Cmd().echo
    assert cmd.args == ["echo"]


def test_cmd_chain() -> None:
    cmd = Cmd().git.status
    assert cmd.args == ["git", "status"]


def test_cmd_call_args() -> None:
    cmd = Cmd().echo("hello", "world")
    assert cmd.args == ["echo", "hello", "world"]


def test_cmd_call_short_flag() -> None:
    cmd = Cmd().ls(l=True, a=True)
    assert cmd.args == ["ls", "-l", "-a"]


def test_cmd_call_long_flag() -> None:
    cmd = Cmd().git.commit(message="fix")
    assert cmd.args == ["git", "commit", "--message", "fix"]


def test_cmd_call_flag_false() -> None:
    cmd = Cmd().ls(l=True, a=False)
    assert cmd.args == ["ls", "-l"]


def test_cmd_underscore_to_dash() -> None:
    cmd = Cmd().foo(some_flag="value")
    assert cmd.args == ["foo", "--some-flag", "value"]


def test_sh_basic() -> None:
    cmd = sh.echo("hello")
    assert cmd.args == ["echo", "hello"]


def test_sh_subcommand() -> None:
    cmd = sh.git.status()
    assert cmd.args == ["git", "status"]


def test_sh_deep_chain() -> None:
    cmd = sh.docker.compose.up(d=True)
    assert cmd.args == ["docker", "compose", "up", "-d"]


def test_sh_mixed() -> None:
    cmd = sh.git.commit("file.txt", m="fix", amend=True)
    assert cmd.args == ["git", "commit", "file.txt", "-m", "fix", "--amend"]


def test_cmd_path_arg() -> None:
    cmd = sh.cat(Path("/tmp/file.txt"))
    assert cmd.args == ["cat", "/tmp/file.txt"]


# Pipeline spec tests


def test_pipe_creates_pipeline() -> None:
    p = sh.echo("hello") | sh.cat()
    assert isinstance(p, Pipeline)
    assert len(p.stages) == 2
    s0, s1 = p.stages[0], p.stages[1]
    assert isinstance(s0, Cmd) and s0.args == ["echo", "hello"]
    assert isinstance(s1, Cmd) and s1.args == ["cat"]


def test_pipe_chain() -> None:
    p = sh.echo("hello") | sh.grep("h") | sh.wc(l=True)
    assert isinstance(p, Pipeline)
    assert len(p.stages) == 3


def test_pipe_flattens_left() -> None:
    p1 = sh.a() | sh.b()
    p2 = p1 | sh.c()
    assert len(p2.stages) == 3
    s0, s1, s2 = p2.stages
    assert isinstance(s0, Cmd) and s0.args == ["a"]
    assert isinstance(s1, Cmd) and s1.args == ["b"]
    assert isinstance(s2, Cmd) and s2.args == ["c"]


def test_pipe_flattens_right() -> None:
    p1 = sh.b() | sh.c()
    p2 = sh.a() | p1
    assert len(p2.stages) == 3
    s0, s1, s2 = p2.stages
    assert isinstance(s0, Cmd) and s0.args == ["a"]
    assert isinstance(s1, Cmd) and s1.args == ["b"]
    assert isinstance(s2, Cmd) and s2.args == ["c"]


def test_pipe_flattens_both() -> None:
    p1 = sh.a() | sh.b()
    p2 = sh.c() | sh.d()
    p3 = p1 | p2
    assert len(p3.stages) == 4


# Redirect spec tests


def test_redirect_stdout() -> None:
    r = sh.echo("hello") > "out.txt"
    assert isinstance(r, Redirect)
    assert isinstance(r.stdout, ToFile)
    assert r.stdout.path == Path("out.txt")
    assert r.stdout.append is False


def test_redirect_stdout_append() -> None:
    r = sh.echo("hello") >> "out.txt"
    assert isinstance(r.stdout, ToFile)
    assert r.stdout.append is True


def test_redirect_stdin_file() -> None:
    r = sh.cat() < "in.txt"
    assert isinstance(r, Redirect)
    assert isinstance(r.stdin, FromFile)
    assert r.stdin.path == Path("in.txt")


def test_redirect_stdin_data() -> None:
    r = sh.cat() << "hello"
    assert isinstance(r, Redirect)
    assert isinstance(r.stdin, FromData)
    assert r.stdin.data == "hello"


def test_redirect_chain_stdin_stdout() -> None:
    r = (sh.cat() < "in.txt") > "out.txt"
    assert isinstance(r.stdin, FromFile)
    assert isinstance(r.stdout, ToFile)


def test_redirect_chain_stdout_stdin() -> None:
    r = (sh.cat() > "out.txt") < "in.txt"
    assert isinstance(r.stdin, FromFile)
    assert isinstance(r.stdout, ToFile)


def test_redirect_pipeline() -> None:
    p = sh.cat() | sh.grep("x")
    r = p > "out.txt"
    assert isinstance(r, Redirect)
    assert isinstance(r.inner, Pipeline)


def test_redirect_bool_raises() -> None:
    with pytest.raises(TypeError, match="parentheses"):
        bool(sh.cat() < "in.txt")


# Redirect as pipeline stage tests


def test_redirect_in_pipeline_first() -> None:
    p = (sh.cat() < "in.txt") | sh.grep("x")
    assert isinstance(p, Pipeline)
    assert len(p.stages) == 2
    assert isinstance(p.stages[0], Redirect)
    assert isinstance(p.stages[1], Cmd)


def test_redirect_in_pipeline_last() -> None:
    p = sh.echo("hello") | (sh.cat() > "out.txt")
    assert isinstance(p, Pipeline)
    assert len(p.stages) == 2
    assert isinstance(p.stages[0], Cmd)
    assert isinstance(p.stages[1], Redirect)


def test_redirect_in_pipeline_middle() -> None:
    p = sh.a() | (sh.b() > "log.txt") | sh.c()
    assert isinstance(p, Pipeline)
    assert len(p.stages) == 3
    assert isinstance(p.stages[0], Cmd)
    assert isinstance(p.stages[1], Redirect)
    assert isinstance(p.stages[2], Cmd)


# Combinator function tests


def test_pipe_two() -> None:
    p = pipe(sh.echo("hello"), sh.cat())
    assert isinstance(p, Pipeline)
    assert len(p.stages) == 2


def test_pipe_varargs() -> None:
    p = pipe(sh.a(), sh.b(), sh.c(), sh.d())
    assert isinstance(p, Pipeline)
    assert len(p.stages) == 4


def test_pipe_flattens() -> None:
    p1 = pipe(sh.a(), sh.b())
    p2 = pipe(p1, sh.c())
    assert len(p2.stages) == 3


def test_write_fn() -> None:
    r = write(sh.echo("hello"), "out.txt")
    assert isinstance(r, Redirect)
    assert isinstance(r.stdout, ToFile)
    assert r.stdout.path == Path("out.txt")
    assert r.stdout.append is False


def test_append_fn() -> None:
    r = append(sh.echo("hello"), "out.txt")
    assert isinstance(r, Redirect)
    assert isinstance(r.stdout, ToFile)
    assert r.stdout.append is True


def test_read_fn() -> None:
    r = read(sh.cat(), "in.txt")
    assert isinstance(r, Redirect)
    assert isinstance(r.stdin, FromFile)
    assert r.stdin.path == Path("in.txt")


def test_input_fn() -> None:
    r = input_(sh.cat(), "hello")
    assert isinstance(r, Redirect)
    assert isinstance(r.stdin, FromData)
    assert r.stdin.data == "hello"


def test_chain_read_write() -> None:
    r = write(read(sh.cat(), "in.txt"), "out.txt")
    assert isinstance(r.stdin, FromFile)
    assert isinstance(r.stdout, ToFile)


def test_from_proc() -> None:
    # from_proc(source) = Sub(source, write=False)
    sub = from_proc(sh.sort("a.txt"))
    assert isinstance(sub, Sub)
    assert sub.write is False
    assert isinstance(sub.cmd, Cmd)
    assert sub.cmd.args == ["sort", "a.txt"]


def test_to_proc() -> None:
    # to_proc(sink) = Sub(sink, write=True)
    sub = to_proc(sh.gzip())
    assert isinstance(sub, Sub)
    assert sub.write is True
    assert isinstance(sub.cmd, Cmd)
    assert sub.cmd.args == ["gzip"]


def test_from_proc_with_pipeline() -> None:
    # from_proc with pipeline source
    sub = from_proc(sh.cat("a") | sh.sort())
    assert isinstance(sub, Sub)
    assert isinstance(sub.cmd, Pipeline)


def test_to_proc_with_redirect() -> None:
    # to_proc with redirect sink
    sub = to_proc(sh.gzip() > "out.gz")
    assert isinstance(sub, Sub)
    assert isinstance(sub.cmd, Redirect)
