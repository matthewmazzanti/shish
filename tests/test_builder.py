"""Tests for the builder DSL."""

import asyncio
import subprocess
from pathlib import Path

import pytest

from shish import ir
from shish.ir import cmd
from shish.runtime import out

# =============================================================================
# Cmd construction
# =============================================================================


def test_cmd_basic() -> None:
    assert cmd("echo", "hello") == ir.Cmd(("echo", "hello"))


def test_cmd_arg() -> None:
    assert cmd("echo").arg("hello", "world") == ir.Cmd(("echo", "hello", "world"))


def test_cmd_path_arg() -> None:
    assert cmd("cat", Path("/tmp/file.txt")) == ir.Cmd(("cat", "/tmp/file.txt"))


def test_cmd_int_arg() -> None:
    assert cmd("head", "-n", 5) == ir.Cmd(("head", "-n", "5"))


def test_cmd_returns_new_instance() -> None:
    base = cmd("echo")
    extended = base.arg("hello")
    assert base == ir.Cmd(("echo",))
    assert extended == ir.Cmd(("echo", "hello"))


# =============================================================================
# Pipeline construction
# =============================================================================


def test_pipe_two() -> None:
    node = cmd("echo", "hello").pipe(cmd("cat"))
    assert isinstance(node, ir.Pipeline)
    assert len(node.stages) == 2
    assert node.stages[0] == ir.Cmd(("echo", "hello"))
    assert node.stages[1] == ir.Cmd(("cat",))


def test_pipe_chain() -> None:
    node = cmd("echo", "hello").pipe(cmd("grep", "h")).pipe(cmd("wc", "-l"))
    assert isinstance(node, ir.Pipeline)
    assert len(node.stages) == 3


def test_pipe_returns_pipeline() -> None:
    assert isinstance(cmd("echo", "hello").pipe(cmd("cat")), ir.Pipeline)


# =============================================================================
# Redirect construction
# =============================================================================


def test_read() -> None:
    assert cmd("cat").read("in.txt") == ir.Cmd(
        ("cat",), redirects=(ir.FdFromFile(0, Path("in.txt")),)
    )


def test_write() -> None:
    assert cmd("echo", "hello").write("out.txt") == ir.Cmd(
        ("echo", "hello"), redirects=(ir.FdToFile(1, Path("out.txt")),)
    )


def test_write_append() -> None:
    assert cmd("echo", "hello").write("out.txt", append=True) == ir.Cmd(
        ("echo", "hello"), redirects=(ir.FdToFile(1, Path("out.txt"), append=True),)
    )


def test_feed() -> None:
    assert cmd("cat").feed("hello") == ir.Cmd(
        ("cat",), redirects=(ir.FdFromData(0, "hello"),)
    )


def test_feed_bytes() -> None:
    assert cmd("cat").feed(b"binary") == ir.Cmd(
        ("cat",), redirects=(ir.FdFromData(0, b"binary"),)
    )


def test_redirect_chain_read_write() -> None:
    assert cmd("cat").read("in.txt").write("out.txt") == ir.Cmd(
        ("cat",),
        redirects=(ir.FdFromFile(0, Path("in.txt")), ir.FdToFile(1, Path("out.txt"))),
    )


def test_redirect_chain_write_read() -> None:
    assert cmd("cat").write("out.txt").read("in.txt") == ir.Cmd(
        ("cat",),
        redirects=(ir.FdToFile(1, Path("out.txt")), ir.FdFromFile(0, Path("in.txt"))),
    )


def test_redirect_returns_cmd() -> None:
    assert isinstance(cmd("cat").read("in.txt"), ir.Cmd)


def test_read_explicit_fd() -> None:
    assert cmd("foo").read("in.txt", fd=3) == ir.Cmd(
        ("foo",), redirects=(ir.FdFromFile(3, Path("in.txt")),)
    )


def test_write_explicit_fd() -> None:
    assert cmd("foo").write("err.log", fd=2) == ir.Cmd(
        ("foo",), redirects=(ir.FdToFile(2, Path("err.log")),)
    )


def test_feed_explicit_fd() -> None:
    assert cmd("foo").feed("data", fd=3) == ir.Cmd(
        ("foo",), redirects=(ir.FdFromData(3, "data"),)
    )


def test_pipeline_write() -> None:
    node = cmd("echo", "hello").pipe(cmd("tr", "a-z", "A-Z")).write("out.txt")
    assert isinstance(node, ir.Pipeline)
    assert len(node.stages) == 2
    assert node.stages[0] == ir.Cmd(("echo", "hello"))
    assert node.stages[1] == ir.Cmd(
        ("tr", "a-z", "A-Z"), redirects=(ir.FdToFile(1, Path("out.txt")),)
    )


def test_pipeline_read() -> None:
    node = cmd("cat").pipe(cmd("grep", "x")).read("in.txt")
    assert isinstance(node, ir.Pipeline)
    assert len(node.stages) == 2
    assert node.stages[0] == ir.Cmd(
        ("cat",), redirects=(ir.FdFromFile(0, Path("in.txt")),)
    )
    assert node.stages[1] == ir.Cmd(("grep", "x"))


# =============================================================================
# Process substitution
# =============================================================================


def test_sub_in() -> None:
    sub = cmd("sort", "a.txt").sub_in()
    assert isinstance(sub, ir.Sub)
    assert sub.write is False
    assert sub.cmd == ir.Cmd(("sort", "a.txt"))


def test_sub_out() -> None:
    sub = cmd("gzip").sub_out()
    assert isinstance(sub, ir.Sub)
    assert sub.write is True
    assert sub.cmd == ir.Cmd(("gzip",))


def test_cmd_with_sub_arg() -> None:
    sub = cmd("sort", "a.txt").sub_in()
    node = cmd("cat", sub)
    assert isinstance(node, ir.Cmd)
    assert node.args == ("cat", ir.Sub(ir.Cmd(("sort", "a.txt")), write=False))


# =============================================================================
# Redirect piping
# =============================================================================


def test_redirect_pipe() -> None:
    node = cmd("cat").read("in.txt").pipe(cmd("grep", "x"))
    assert isinstance(node, ir.Pipeline)
    assert len(node.stages) == 2
    assert node.stages[0] == ir.Cmd(
        ("cat",), redirects=(ir.FdFromFile(0, Path("in.txt")),)
    )
    assert node.stages[1] == ir.Cmd(("grep", "x"))


# =============================================================================
# E2E execution tests
# =============================================================================


async def test_run_simple() -> None:
    assert await cmd("true").run() == 0
    assert await cmd("false").run() == 1


async def test_await_cmd() -> None:
    assert await cmd("true").run() == 0
    assert await cmd("false").run() == 1


async def test_pipeline_execution() -> None:
    assert await cmd("echo", "hello").pipe(cmd("cat")).run() == 0


async def test_write_to_file(tmp_path: Path) -> None:
    outfile = tmp_path / "out.txt"
    assert await cmd("echo", "hello").write(outfile).run() == 0
    assert outfile.read_text() == "hello\n"


async def test_write_append_e2e(tmp_path: Path) -> None:
    outfile = tmp_path / "out.txt"
    await cmd("echo", "first").write(outfile).run()
    await cmd("echo", "second").write(outfile, append=True).run()
    assert outfile.read_text() == "first\nsecond\n"


async def test_read_from_file(tmp_path: Path) -> None:
    inp = tmp_path / "in.txt"
    outfile = tmp_path / "out.txt"
    inp.write_text("hello from file")
    await cmd("cat").read(inp).write(outfile).run()
    assert outfile.read_text() == "hello from file"


async def test_pipeline_write_e2e(tmp_path: Path) -> None:
    outfile = tmp_path / "out.txt"
    await cmd("echo", "hello world").pipe(cmd("tr", "a-z", "A-Z")).write(outfile).run()
    assert outfile.read_text() == "HELLO WORLD\n"


async def test_feed_e2e(tmp_path: Path) -> None:
    outfile = tmp_path / "out.txt"
    await cmd("cat").feed("hello world").write(outfile).run()
    assert outfile.read_text() == "hello world"


async def test_feed_bytes_e2e(tmp_path: Path) -> None:
    outfile = tmp_path / "out.bin"
    data = bytes(range(256))
    await cmd("cat").feed(data).write(outfile).run()
    assert outfile.read_bytes() == data


async def test_sub_from_single(tmp_path: Path) -> None:
    outfile = tmp_path / "out.txt"
    await cmd("cat", cmd("echo", "hello").sub_in()).write(outfile).run()
    assert outfile.read_text() == "hello\n"


async def test_sub_from_diff(tmp_path: Path) -> None:
    file1 = tmp_path / "file1.txt"
    file2 = tmp_path / "file2.txt"
    outfile = tmp_path / "out.txt"
    file1.write_text("b\na\nc\n")
    file2.write_text("b\na\nc\n")
    result = (
        await cmd(
            "diff",
            cmd("sort", str(file1)).sub_in(),
            cmd("sort", str(file2)).sub_in(),
        )
        .write(outfile)
        .run()
    )
    assert result == 0
    assert outfile.read_text() == ""


async def test_sub_to_with_ir_sub(tmp_path: Path) -> None:
    outfile = tmp_path / "out.txt"
    main_out = tmp_path / "main.txt"
    sink = ir.Sub(
        ir.Cmd(("cat",), redirects=(ir.FdToFile(1, outfile),)),
        write=True,
    )
    await cmd("echo", "hello").pipe(cmd("tee", sink)).write(main_out).run()
    assert outfile.read_text() == "hello\n"
    assert main_out.read_text() == "hello\n"


async def test_out_simple() -> None:
    result = await out(cmd("echo", "hello"))
    assert result == "hello\n"


async def test_out_pipeline() -> None:
    result = await out(cmd("echo", "hello world").pipe(cmd("tr", "a-z", "A-Z")))
    assert result == "HELLO WORLD\n"


async def test_out_with_feed() -> None:
    result = await out(cmd("cat").feed("input data"))
    assert result == "input data"


async def test_out_raises_on_failure() -> None:
    with pytest.raises(subprocess.CalledProcessError) as exc_info:
        await out(cmd("false"))
    assert exc_info.value.returncode == 1


async def test_out_binary() -> None:
    data = bytes(range(256))
    result = await out(cmd("cat").feed(data), encoding=None)
    assert result == data


async def test_concurrent_runs() -> None:
    results = await asyncio.gather(
        cmd("echo", "a").pipe(cmd("cat")).run(),
        cmd("echo", "b").pipe(cmd("cat")).run(),
        cmd("echo", "c").pipe(cmd("cat")).run(),
    )
    assert results == [0, 0, 0]


async def test_concurrent_with_files(tmp_path: Path) -> None:
    files = [tmp_path / f"out{idx}.txt" for idx in range(5)]
    await asyncio.gather(
        *[
            cmd("echo", f"content{idx}").write(fpath).run()
            for idx, fpath in enumerate(files)
        ],
    )
    for idx, fpath in enumerate(files):
        assert fpath.read_text() == f"content{idx}\n"
