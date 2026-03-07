"""Runtime tests using IR nodes directly — verifies runtime consumes IR correctly."""

import asyncio
import os
import signal
from pathlib import Path

import pytest

from shish import PIPE, STDERR, STDIN, STDOUT, builders
from shish.builders import ShishError
from shish.fd import Fd
from shish.fn_stage import ByteStage
from shish.runtime import CloseMethod, Job, start
from shish.streams import ByteReadStream, ByteWriteStream

# =============================================================================
# Basic Job
# =============================================================================


async def test_run_cmd() -> None:
    assert await builders.Cmd(("true",)).run() == 0
    assert await builders.Cmd(("false",)).run() == 1


async def test_run_cmd_with_args() -> None:
    result = await builders.Cmd(("echo", "hello")).out()
    assert result == "hello\n"


# =============================================================================
# Pipelines
# =============================================================================


async def test_pipeline() -> None:
    pipeline = builders.Pipeline(
        (
            builders.Cmd(("echo", "hello")),
            builders.Cmd(("cat",)),
        )
    )
    assert await pipeline.run() == 0


async def test_pipeline_output() -> None:
    pipeline = builders.Pipeline(
        (
            builders.Cmd(("echo", "hello world")),
            builders.Cmd(("tr", "a-z", "A-Z")),
        )
    )
    result = await pipeline.out()
    assert result == "HELLO WORLD\n"


async def test_pipeline_pipefail() -> None:
    pipeline = builders.Pipeline(
        (
            builders.Cmd(("false",)),
            builders.Cmd(("cat",)),
        )
    )
    assert await pipeline.run() != 0


async def test_pipeline_pipefail_rightmost() -> None:
    """Pipefail returns rightmost non-zero exit code."""
    pipeline = builders.Pipeline(
        (
            builders.Cmd(("sh", "-c", "exit 1")),
            builders.Cmd(("sh", "-c", "exit 0")),
            builders.Cmd(("sh", "-c", "exit 2")),
        )
    )
    assert await pipeline.run() == 2


async def test_pipeline_three_stages() -> None:
    pipeline = builders.Pipeline(
        (
            builders.Cmd(("echo", "hello")),
            builders.Cmd(("tr", "a-z", "A-Z")),
            builders.Cmd(("tr", "H", "J")),
        )
    )
    result = await pipeline.out()
    assert result == "JELLO\n"


async def test_pipeline_sigpipe() -> None:
    pipeline = builders.Pipeline(
        (
            builders.Cmd(("yes",)),
            builders.Cmd(("head", "-n", "1")),
        )
    )
    result = await asyncio.wait_for(pipeline.run(), timeout=2.0)
    assert result in (0, 141)


async def test_empty_pipeline() -> None:
    """Empty pipeline returns success."""
    assert await builders.Pipeline(()).run() == 0


async def test_single_stage_pipeline() -> None:
    """Pipeline with one stage behaves like a plain command."""
    result = await builders.Pipeline((builders.Cmd(("echo", "hello")),)).out()
    assert result == "hello\n"


# =============================================================================
# File Redirects (on Cmd)
# =============================================================================


async def test_cmd_stdout_redirect(tmp_path: Path) -> None:
    outfile = tmp_path / "out.txt"
    command = builders.Cmd(
        ("echo", "hello"), redirects=(builders.FdToFile(STDOUT, outfile),)
    )
    assert await command.run() == 0
    assert outfile.read_text() == "hello\n"


async def test_cmd_stdout_append(tmp_path: Path) -> None:
    outfile = tmp_path / "out.txt"
    await builders.Cmd(
        ("echo", "first"), redirects=(builders.FdToFile(STDOUT, outfile),)
    ).run()
    await builders.Cmd(
        ("echo", "second"),
        redirects=(builders.FdToFile(STDOUT, outfile, append=True),),
    ).run()
    assert outfile.read_text() == "first\nsecond\n"


async def test_cmd_stdin_file(tmp_path: Path) -> None:
    inp = tmp_path / "in.txt"
    outfile = tmp_path / "out.txt"
    inp.write_text("hello from file")
    command = builders.Cmd(
        ("cat",),
        redirects=(
            builders.FdFromFile(STDIN, inp),
            builders.FdToFile(STDOUT, outfile),
        ),
    )
    assert await command.run() == 0
    assert outfile.read_text() == "hello from file"


async def test_cmd_stdin_data(tmp_path: Path) -> None:
    outfile = tmp_path / "out.txt"
    command = builders.Cmd(
        ("cat",),
        redirects=(
            builders.FdFromData(STDIN, "hello world"),
            builders.FdToFile(STDOUT, outfile),
        ),
    )
    assert await command.run() == 0
    assert outfile.read_text() == "hello world"


async def test_cmd_stdin_data_bytes(tmp_path: Path) -> None:
    outfile = tmp_path / "out.bin"
    data = bytes(range(256))
    command = builders.Cmd(
        ("cat",),
        redirects=(
            builders.FdFromData(STDIN, data),
            builders.FdToFile(STDOUT, outfile),
        ),
    )
    assert await command.run() == 0
    assert outfile.read_bytes() == data


# =============================================================================
# Pipeline with per-stage redirects
# =============================================================================


async def test_pipeline_first_stage_stdin(tmp_path: Path) -> None:
    inp = tmp_path / "in.txt"
    inp.write_text("hello\nworld\n")
    pipeline = builders.Pipeline(
        (
            builders.Cmd(("cat",), redirects=(builders.FdFromFile(STDIN, inp),)),
            builders.Cmd(("grep", "world")),
        )
    )
    result = await pipeline.out()
    assert result == "world\n"


async def test_pipeline_stage_stdout_override(tmp_path: Path) -> None:
    # (echo "hello" > file) | cat — stdout goes to file, cat gets nothing
    outfile = tmp_path / "out.txt"
    result_file = tmp_path / "result.txt"
    pipeline = builders.Pipeline(
        (
            builders.Cmd(
                ("echo", "hello"), redirects=(builders.FdToFile(STDOUT, outfile),)
            ),
            builders.Cmd(("cat",), redirects=(builders.FdToFile(STDOUT, result_file),)),
        )
    )
    assert await pipeline.run() == 0
    assert outfile.read_text() == "hello\n"
    assert result_file.read_text() == ""


async def test_pipeline_first_stage_data(tmp_path: Path) -> None:
    """(cat << "data") | grep — data redirect on first pipeline stage."""
    pipeline = builders.Pipeline(
        (
            builders.Cmd(
                ("cat",),
                redirects=(builders.FdFromData(STDIN, "hello\nworld\n"),),
            ),
            builders.Cmd(("grep", "world")),
        )
    )
    result = await pipeline.out()
    assert result == "world\n"


async def test_pipeline_last_stage_file(tmp_path: Path) -> None:
    """echo | cat > file — last stage redirects stdout to file."""
    outfile = tmp_path / "out.txt"
    pipeline = builders.Pipeline(
        (
            builders.Cmd(("echo", "hello")),
            builders.Cmd(("cat",), redirects=(builders.FdToFile(STDOUT, outfile),)),
        )
    )
    assert await pipeline.run() == 0
    assert outfile.read_text() == "hello\n"


async def test_pipeline_middle_stage_redirect(tmp_path: Path) -> None:
    """a | (b > log) | c — middle stage redirect diverts to file."""
    logfile = tmp_path / "log.txt"
    pipeline = builders.Pipeline(
        (
            builders.Cmd(("echo", "hello")),
            builders.Cmd(("cat",), redirects=(builders.FdToFile(STDOUT, logfile),)),
            builders.Cmd(("cat",)),
        )
    )
    result = await pipeline.out()
    assert logfile.read_text() == "hello\n"
    assert result == ""


# =============================================================================
# Fd-to-fd redirects (2>&1, etc.)
# =============================================================================


async def test_fd_to_fd_stderr_to_stdout() -> None:
    """cmd 2>&1 — stderr merges into stdout."""
    command = builders.Cmd(
        ("sh", "-c", "echo out; echo err >&2"),
        redirects=(builders.FdToFd(STDOUT, STDERR),),
    )
    result = await command.out()
    assert "out\n" in result
    assert "err\n" in result


async def test_fd_to_fd_file_then_dup(tmp_path: Path) -> None:
    """cmd > file 2>&1 — both stdout and stderr go to file."""
    outfile = tmp_path / "out.txt"
    command = builders.Cmd(
        ("sh", "-c", "echo out; echo err >&2"),
        redirects=(
            builders.FdToFile(STDOUT, outfile),
            builders.FdToFd(STDOUT, STDERR),
        ),
    )
    assert await command.run() == 0
    content = outfile.read_text()
    assert "out\n" in content
    assert "err\n" in content


async def test_fd_to_fd_order_matters(tmp_path: Path) -> None:
    """cmd 2>&1 > file — stderr goes to original stdout, only stdout goes to file.

    Order matters: 2>&1 copies current fd 1 (pipe/terminal) to fd 2,
    then > file redirects fd 1 to file. So stderr goes to pipe, stdout to file.
    """
    outfile = tmp_path / "out.txt"
    command = builders.Cmd(
        ("sh", "-c", "echo out; echo err >&2"),
        redirects=(
            builders.FdToFd(STDOUT, STDERR),
            builders.FdToFile(STDOUT, outfile),
        ),
    )
    # Capture what comes through the pipeline stdout (which is stderr after dup)
    result = await command.out()
    assert result == "err\n"
    assert outfile.read_text() == "out\n"


async def test_fd_to_file_stderr(tmp_path: Path) -> None:
    """cmd 2> file — stderr to file, stdout to pipeline."""
    errfile = tmp_path / "err.txt"
    command = builders.Cmd(
        ("sh", "-c", "echo out; echo err >&2"),
        redirects=(builders.FdToFile(STDERR, errfile),),
    )
    result = await command.out()
    assert result == "out\n"
    assert errfile.read_text() == "err\n"


async def test_fd_to_file_arbitrary_fd(tmp_path: Path) -> None:
    """cmd 3> file — redirect fd 3 to a file."""
    outfile = tmp_path / "out.txt"
    command = builders.Cmd(
        ("sh", "-c", "echo hello >&3"),
        redirects=(builders.FdToFile(3, outfile),),
    )
    assert await command.run() == 0
    assert outfile.read_text() == "hello\n"


async def test_fd_from_file_arbitrary_fd(tmp_path: Path) -> None:
    """cmd 3< file — redirect fd 3 from a file, then read it."""
    infile = tmp_path / "in.txt"
    infile.write_text("hello from fd3")
    command = builders.Cmd(
        ("sh", "-c", "cat <&3"),
        redirects=(builders.FdFromFile(3, infile),),
    )
    result = await command.out()
    assert result == "hello from fd3"


async def test_fd_from_data_arbitrary_fd(tmp_path: Path) -> None:
    """cmd 3<<< "data" — feed data into fd 3, then read it."""
    command = builders.Cmd(
        ("sh", "-c", "cat <&3"),
        redirects=(builders.FdFromData(3, "hello from fd3"),),
    )
    result = await command.out()
    assert result == "hello from fd3"


async def test_fd_to_file_append_arbitrary_fd(tmp_path: Path) -> None:
    """cmd 3>> file — append mode on arbitrary fd."""
    outfile = tmp_path / "out.txt"
    await builders.Cmd(
        ("sh", "-c", "echo first >&3"), redirects=(builders.FdToFile(3, outfile),)
    ).run()
    await builders.Cmd(
        ("sh", "-c", "echo second >&3"),
        redirects=(builders.FdToFile(3, outfile, append=True),),
    ).run()
    assert outfile.read_text() == "first\nsecond\n"


async def test_multiple_redirects_same_fd(tmp_path: Path) -> None:
    """Last redirect on same fd wins, matching bash semantics."""
    file1 = tmp_path / "file1.txt"
    file2 = tmp_path / "file2.txt"
    command = builders.Cmd(
        ("echo", "hello"),
        redirects=(
            builders.FdToFile(STDOUT, file1),
            builders.FdToFile(STDOUT, file2),
        ),
    )
    assert await command.run() == 0
    # Second redirect wins — output goes to file2
    assert file2.read_text() == "hello\n"
    # file1 gets created but nothing written to it
    assert file1.read_text() == ""


# =============================================================================
# FdClose
# =============================================================================


async def test_fd_close() -> None:
    """Closing stdin causes cat to fail with EBADF."""
    command = builders.Cmd(
        ("cat",),
        redirects=(builders.FdClose(STDIN),),
    )
    assert await command.run() == 1


# =============================================================================
# Process Substitution
# =============================================================================


async def test_sub_in(tmp_path: Path) -> None:
    outfile = tmp_path / "out.txt"
    sub = builders.SubIn(builders.Cmd(("echo", "hello")))
    command = builders.Cmd(
        ("cat", sub), redirects=(builders.FdToFile(STDOUT, outfile),)
    )
    assert await command.run() == 0
    assert outfile.read_text() == "hello\n"


async def test_sub_in_two_sources(tmp_path: Path) -> None:
    file1 = tmp_path / "file1.txt"
    file2 = tmp_path / "file2.txt"
    outfile = tmp_path / "out.txt"
    file1.write_text("b\na\nc\n")
    file2.write_text("b\na\nc\n")
    sub1 = builders.SubIn(builders.Cmd(("sort", str(file1))))
    sub2 = builders.SubIn(builders.Cmd(("sort", str(file2))))
    command = builders.Cmd(
        ("diff", sub1, sub2), redirects=(builders.FdToFile(STDOUT, outfile),)
    )
    result = await command.run()
    assert result == 0
    assert outfile.read_text() == ""


async def test_sub_out(tmp_path: Path) -> None:
    outfile = tmp_path / "out.txt"
    main_out = tmp_path / "main.txt"
    sink = builders.SubOut(
        builders.Cmd(("cat",), redirects=(builders.FdToFile(STDOUT, outfile),)),
    )
    pipeline = builders.Pipeline(
        (
            builders.Cmd(("echo", "hello")),
            builders.Cmd(
                ("tee", sink), redirects=(builders.FdToFile(STDOUT, main_out),)
            ),
        )
    )
    assert await pipeline.run() == 0
    assert outfile.read_text() == "hello\n"
    assert main_out.read_text() == "hello\n"


async def test_sub_in_with_pipeline(tmp_path: Path) -> None:
    outfile = tmp_path / "out.txt"
    sub = builders.SubIn(
        builders.Pipeline(
            (
                builders.Cmd(("echo", "hello")),
                builders.Cmd(("tr", "a-z", "A-Z")),
            )
        ),
    )
    command = builders.Cmd(
        ("cat", sub), redirects=(builders.FdToFile(STDOUT, outfile),)
    )
    assert await command.run() == 0
    assert outfile.read_text() == "HELLO\n"


async def test_fd_from_sub_stdin() -> None:
    """cat < <(echo hello) — redirect stdin from process substitution."""
    sub = builders.SubIn(builders.Cmd(("echo", "hello")))
    command = builders.Cmd(("cat",), redirects=(builders.FdFromSub(STDIN, sub),))
    result = await command.out()
    assert result == "hello\n"


async def test_fd_from_sub_arbitrary_fd(tmp_path: Path) -> None:
    """cmd 3< <(echo hello) — redirect fd 3 from process substitution."""
    outfile = tmp_path / "out.txt"
    sub = builders.SubIn(builders.Cmd(("echo", "hello")))
    command = builders.Cmd(
        ("sh", "-c", "cat <&3"),
        redirects=(
            builders.FdFromSub(3, sub),
            builders.FdToFile(STDOUT, outfile),
        ),
    )
    assert await command.run() == 0
    assert outfile.read_text() == "hello\n"


async def test_fd_to_sub_stdout(tmp_path: Path) -> None:
    """cmd 1> >(cat > file) — redirect stdout to process substitution."""
    outfile = tmp_path / "out.txt"
    sub = builders.SubOut(
        builders.Cmd(("cat",), redirects=(builders.FdToFile(STDOUT, outfile),)),
    )
    command = builders.Cmd(
        ("echo", "hello"), redirects=(builders.FdToSub(STDOUT, sub),)
    )
    assert await command.run() == 0
    assert outfile.read_text() == "hello\n"


async def test_fd_to_sub_arbitrary_fd(tmp_path: Path) -> None:
    """cmd 3> >(cat > file) — redirect fd 3 to process substitution."""
    outfile = tmp_path / "out.txt"
    sub = builders.SubOut(
        builders.Cmd(("cat",), redirects=(builders.FdToFile(STDOUT, outfile),)),
    )
    command = builders.Cmd(
        ("sh", "-c", "echo hello >&3"),
        redirects=(builders.FdToSub(3, sub),),
    )
    assert await command.run() == 0
    assert outfile.read_text() == "hello\n"


async def test_fd_from_sub_with_pipeline() -> None:
    """cat < <(echo hello | tr a-z A-Z) — sub contains a pipeline."""
    sub = builders.SubIn(
        builders.Pipeline(
            (
                builders.Cmd(("echo", "hello")),
                builders.Cmd(("tr", "a-z", "A-Z")),
            )
        ),
    )
    command = builders.Cmd(("cat",), redirects=(builders.FdFromSub(STDIN, sub),))
    result = await command.out()
    assert result == "HELLO\n"


async def test_fd_to_sub_with_pipeline(tmp_path: Path) -> None:
    """cmd > >(pipeline > file) — output sub contains a pipeline."""
    outfile = tmp_path / "out.txt"
    sub = builders.SubOut(
        builders.Pipeline(
            (
                builders.Cmd(("tr", "a-z", "A-Z")),
                builders.Cmd(("cat",), redirects=(builders.FdToFile(STDOUT, outfile),)),
            )
        ),
    )
    command = builders.Cmd(
        ("echo", "hello"), redirects=(builders.FdToSub(STDOUT, sub),)
    )
    assert await command.run() == 0
    assert outfile.read_text() == "HELLO\n"


async def test_data_write_to_early_exit() -> None:
    """Large data feed to process that exits early doesn't hang or crash."""
    data = b"x" * (256 * 1024)
    # head -c 1 reads 1 byte then exits; rest of data write hits broken pipe
    command = builders.Cmd(
        ("head", "-c", "1"),
        redirects=(builders.FdFromData(STDIN, data),),
    )
    result = await command.out()
    assert result == "x"


async def test_sub_exit_code_ignored_arg() -> None:
    """cat <(false) — sub failure doesn't affect main exit code."""
    sub = builders.SubIn(builders.Cmd(("false",)))
    command = builders.Cmd(("cat", sub))
    assert await command.run() == 0


async def test_sub_exit_code_ignored_redirect() -> None:
    """true 1> >(false) — redirect sub failure doesn't affect exit code."""
    sub = builders.SubOut(builders.Cmd(("false",)))
    command = builders.Cmd(("true",), redirects=(builders.FdToSub(STDOUT, sub),))
    assert await command.run() == 0


async def test_sub_exit_code_ignored_from_redirect() -> None:
    """cat < <(false) — input sub failure doesn't affect exit code."""
    sub = builders.SubIn(builders.Cmd(("false",)))
    command = builders.Cmd(("cat",), redirects=(builders.FdFromSub(STDIN, sub),))
    assert await command.run() == 0


async def test_sub_nested(tmp_path: Path) -> None:
    """Nested process substitution: cat <(cat <(echo hello))."""
    outfile = tmp_path / "out.txt"
    inner = builders.SubIn(builders.Cmd(("echo", "hello")))
    outer = builders.SubIn(builders.Cmd(("cat", inner)))
    command = builders.Cmd(
        ("cat", outer), redirects=(builders.FdToFile(STDOUT, outfile),)
    )
    assert await command.run() == 0
    assert outfile.read_text() == "hello\n"


async def test_sub_in_with_data_stdin(tmp_path: Path) -> None:
    outfile = tmp_path / "out.txt"
    sub = builders.SubIn(
        builders.Cmd(
            ("cat",), redirects=(builders.FdFromData(STDIN, "injected data"),)
        ),
    )
    command = builders.Cmd(
        ("cat", sub), redirects=(builders.FdToFile(STDOUT, outfile),)
    )
    assert await command.run() == 0
    assert outfile.read_text() == "injected data"


# =============================================================================
# Output Capture
# =============================================================================


async def test_out_simple() -> None:
    result = await builders.Cmd(("echo", "hello")).out()
    assert result == "hello\n"


async def test_out_pipeline() -> None:
    pipeline = builders.Pipeline(
        (
            builders.Cmd(("echo", "hello world")),
            builders.Cmd(("tr", "a-z", "A-Z")),
        )
    )
    result = await pipeline.out()
    assert result == "HELLO WORLD\n"


async def test_out_empty() -> None:
    command = builders.Cmd(("cat",), redirects=(builders.FdFromData(STDIN, ""),))
    result = await command.out()
    assert result == ""


async def test_out_with_data() -> None:
    command = builders.Cmd(
        ("cat",), redirects=(builders.FdFromData(STDIN, "input data"),)
    )
    result = await command.out()
    assert result == "input data"


async def test_out_binary() -> None:
    data = bytes(range(256))
    command = builders.Cmd(("cat",), redirects=(builders.FdFromData(STDIN, data),))
    result = await command.out(encoding=None)
    assert result == data


async def test_out_large_data() -> None:
    """Data larger than pipe buffer (64K) exercises backpressure path."""
    data = b"x" * (256 * 1024)
    command = builders.Cmd(("cat",), redirects=(builders.FdFromData(STDIN, data),))
    result = await command.out(encoding=None)
    assert result == data


async def test_out_large_data_pipeline() -> None:
    """Multiple large data feeds in concurrent pipelines exercise interleaved writes."""
    data = b"x" * (256 * 1024)
    result_a, result_b = await asyncio.gather(
        builders.Cmd(("cat",), redirects=(builders.FdFromData(STDIN, data),)).out(
            encoding=None
        ),
        builders.Cmd(("cat",), redirects=(builders.FdFromData(STDIN, data),)).out(
            encoding=None
        ),
    )
    assert result_a == data
    assert result_b == data


async def test_out_large_data_multi_fd(tmp_path: Path) -> None:
    """Concurrent reads from multiple fds force interleaved async writes."""
    data = b"x" * (256 * 1024)
    out3 = tmp_path / "fd3.bin"
    out4 = tmp_path / "fd4.bin"
    command = builders.Cmd(
        ("sh", "-c", f"cat <&3 > {out3} & cat <&4 > {out4} & wait"),
        redirects=(
            builders.FdFromData(3, data),
            builders.FdFromData(4, data),
        ),
    )
    assert await command.run() == 0
    assert out3.read_bytes() == data
    assert out4.read_bytes() == data


# =============================================================================
# start() with stdin_fd
# =============================================================================


async def test_start_stdin_fd() -> None:
    """start(stdin=...) feeds stdin via caller-owned pipe."""
    read_fd, write_fd = os.pipe()
    async with start(builders.Cmd(("cat",))).stdin(read_fd) as execution:
        os.close(read_fd)  # start() dup'd it; close our copy
        async with ByteWriteStream(Fd(write_fd)) as writer:
            await writer.write(b"hello from pipe")
        assert await execution.wait() == 0


async def test_start_stdin_fd_with_stdout_fd() -> None:
    """start() with both stdin and stdout wires through correctly."""
    stdin_r, stdin_w = os.pipe()
    stdout_r, stdout_w = os.pipe()
    async with (
        start(builders.Cmd(("cat",))).stdin(stdin_r).stdout(stdout_w) as execution
    ):
        os.close(stdin_r)  # start() dup'd these; close our copies
        os.close(stdout_w)

        async def do_write() -> None:
            async with ByteWriteStream(Fd(stdin_w)) as writer:
                await writer.write(b"round trip")

        async with ByteReadStream(Fd(stdout_r)) as reader:
            write_task = asyncio.create_task(do_write())
            code, captured = await asyncio.gather(
                execution.wait(),
                reader.read(),
            )
            await write_task
        assert code == 0
        assert captured == b"round trip"


async def test_out_raises_on_failure() -> None:
    with pytest.raises(ShishError) as exc_info:
        await builders.Cmd(("false",)).out()
    assert exc_info.value.returncode == 1


async def test_out_raises_preserves_output() -> None:
    with pytest.raises(ShishError) as exc_info:
        await builders.Cmd(("sh", "-c", "echo partial; exit 1")).out()
    assert exc_info.value.stdout == "partial\n"


# =============================================================================
# start() with PIPE
# =============================================================================


async def test_start_stdin_pipe() -> None:
    """start(stdin=PIPE) defaults to text mode."""
    async with start(builders.Cmd(("cat",))).stdin(PIPE) as execution:
        await execution.stdin.write("hello from pipe")
        execution.stdin.close()
        assert await execution.wait() == 0


async def test_start_stdout_pipe() -> None:
    """start(stdout=PIPE) defaults to text mode."""
    async with start(builders.Cmd(("echo", "hello"))).stdout(PIPE) as execution:
        code, captured = await asyncio.gather(
            execution.wait(),
            execution.stdout.read(),
        )
    assert code == 0
    assert captured == "hello\n"


async def test_start_stdin_stdout_pipe() -> None:
    """start(stdin=PIPE, stdout=PIPE) defaults to text mode."""
    async with start(builders.Cmd(("cat",))).stdin(PIPE).stdout(PIPE) as execution:
        await execution.stdin.write("round trip")
        execution.stdin.close()
        code, captured = await asyncio.gather(
            execution.wait(),
            execution.stdout.read(),
        )
    assert code == 0
    assert captured == "round trip"


async def test_start_stdout_pipe_large_data() -> None:
    """PIPE handles data larger than pipe buffer (64K)."""
    data = b"x" * (256 * 1024)
    async with start(
        builders.Cmd(("cat",), redirects=(builders.FdFromData(STDIN, data),)),
    ).stdout(PIPE, encoding=None) as execution:
        code, captured = await asyncio.gather(
            execution.wait(),
            execution.stdout.read(),
        )
    assert code == 0
    assert captured == data


async def test_start_stdin_pipe_bytes() -> None:
    """start(stdin=PIPE, encoding=None) gives ByteWriteStream."""
    async with start(builders.Cmd(("cat",))).stdin(PIPE, encoding=None) as execution:
        await execution.stdin.write(b"hello bytes")
        execution.stdin.close()
        assert await execution.wait() == 0


async def test_start_stdout_pipe_bytes() -> None:
    """start(stdout=PIPE, encoding=None) gives ByteReadStream."""
    async with start(builders.Cmd(("echo", "hello"))).stdout(
        PIPE, encoding=None
    ) as execution:
        code, captured = await asyncio.gather(
            execution.wait(),
            execution.stdout.read(),
        )
    assert code == 0
    assert captured == b"hello\n"


async def test_start_stdin_stdout_pipe_bytes() -> None:
    """Both stdin and stdout in explicit bytes mode."""
    async with (
        start(builders.Cmd(("cat",)))
        .stdin(PIPE, encoding=None)
        .stdout(PIPE, encoding=None) as execution
    ):
        await execution.stdin.write(b"bytes round trip")
        execution.stdin.close()
        code, captured = await asyncio.gather(
            execution.wait(),
            execution.stdout.read(),
        )
    assert code == 0
    assert captured == b"bytes round trip"


async def test_start_stdout_pipe_readline() -> None:
    """Text-mode stdout supports readline."""
    async with start(
        builders.Cmd(("printf", "line1\\nline2\\nline3\\n")),
    ).stdout(PIPE) as execution:
        first = await execution.stdout.readline()
        second = await execution.stdout.readline()
        third = await execution.stdout.readline()
        await execution.wait()
    assert first == "line1\n"
    assert second == "line2\n"
    assert third == "line3\n"


async def test_start_no_pipe_streams_none() -> None:
    """Without PIPE, stdin/stdout streams are None."""
    async with start(builders.Cmd(("true",))) as execution:
        assert execution.stdin is None
        assert execution.stdout is None
        await execution.wait()


# =============================================================================
# Concurrent Job
# =============================================================================


async def test_concurrent_runs() -> None:
    results = await asyncio.gather(
        builders.Pipeline((builders.Cmd(("echo", "a")), builders.Cmd(("cat",)))).run(),
        builders.Pipeline((builders.Cmd(("echo", "b")), builders.Cmd(("cat",)))).run(),
        builders.Pipeline((builders.Cmd(("echo", "c")), builders.Cmd(("cat",)))).run(),
    )
    assert results == [0, 0, 0]


async def test_concurrent_file_writes(tmp_path: Path) -> None:
    files = [tmp_path / f"out{idx}.txt" for idx in range(5)]
    await asyncio.gather(
        *[
            builders.Cmd(
                ("echo", f"content{idx}"),
                redirects=(builders.FdToFile(STDOUT, fpath),),
            ).run()
            for idx, fpath in enumerate(files)
        ]
    )
    for idx, fpath in enumerate(files):
        assert fpath.read_text() == f"content{idx}\n"


# =============================================================================
# Cancellation and cleanup
# =============================================================================


async def test_cancel_kills_child() -> None:
    """Cancelling a running task kills the child process."""
    task = asyncio.create_task(builders.Cmd(("sleep", "60")).run())
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_cancel_pipeline_kills_all_stages() -> None:
    """Cancelling a pipeline kills all stages."""
    task = asyncio.create_task(
        builders.Pipeline(
            (builders.Cmd(("sleep", "60")), builders.Cmd(("sleep", "60")))
        ).run()
    )
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


# =============================================================================
# Factory functions used by runtime
# =============================================================================


async def test_signal_killed_exit_code() -> None:
    """Process killed by signal returns 128 + signal number."""
    # SIGKILL = 9, so exit code should be 137
    result = await builders.Cmd(("sh", "-c", "kill -9 $$")).run()
    assert result == 137


async def test_pipeline_factory_execution() -> None:
    pipeline = builders.pipeline(
        builders.Cmd(("echo", "hello")),
        builders.Cmd(("tr", "a-z", "A-Z")),
    )
    result = await pipeline.out()
    assert result == "HELLO\n"


# =============================================================================
# Environment Variables
# =============================================================================


async def test_env_vars() -> None:
    """Child process sees env vars set via env_vars."""
    command = builders.Cmd(("printenv", "FOO"), env_vars=(("FOO", "bar"),))
    result = await command.out()
    assert result == "bar\n"


async def test_env_vars_multiple() -> None:
    """Child process sees multiple env vars."""
    command = builders.Cmd(
        ("sh", "-c", "echo $FOO $BAZ"),
        env_vars=(("FOO", "hello"), ("BAZ", "world")),
    )
    result = await command.out()
    assert result == "hello world\n"


async def test_env_vars_unset() -> None:
    """None value unsets an env var."""
    command = builders.Cmd(
        ("sh", "-c", "echo ${HOME-unset}"),
        env_vars=(("HOME", None),),
    )
    result = await command.out()
    assert result == "unset\n"


async def test_env_vars_override() -> None:
    """Env vars override inherited values."""

    original = os.environ.get("HOME", "")
    command = builders.Cmd(("printenv", "HOME"), env_vars=(("HOME", "/fake/home"),))
    result = await command.out()
    assert result == "/fake/home\n"
    # Verify parent env is not affected
    assert os.environ.get("HOME", "") == original


async def test_env_vars_pipeline_per_cmd() -> None:
    """Env vars are per-cmd in a pipeline, not shared."""
    pipeline = builders.Pipeline(
        (
            builders.Cmd(("printenv", "FOO"), env_vars=(("FOO", "first"),)),
            builders.Cmd(("cat",)),
        )
    )
    result = await pipeline.out()
    assert result == "first\n"


# =============================================================================
# Working Directory
# =============================================================================


async def test_working_dir(tmp_path: Path) -> None:
    """Child process runs in the specified working directory."""
    command = builders.Cmd(("pwd",), working_dir=tmp_path)
    result = await command.out()
    assert result.strip() == str(tmp_path.resolve())


async def test_working_dir_pwd_synced(tmp_path: Path) -> None:
    """PWD env var is synced when working_dir is set."""
    command = builders.Cmd(("printenv", "PWD"), working_dir=tmp_path)
    result = await command.out()
    assert result.strip() == str(tmp_path)


# =============================================================================
# start() + wait() lifecycle
# =============================================================================


async def test_start_returns_execution() -> None:
    """start() yields an Job with a root ProcessNode."""
    async with start(builders.Cmd(("true",))) as execution:
        assert isinstance(execution, Job)
        assert execution.root is not None
        code = await execution.wait()
    assert code == 0


async def test_start_exit_code() -> None:
    """wait() returns the correct exit code."""
    async with start(builders.Cmd(("false",))) as execution:
        assert await execution.wait() == 1


async def test_start_pipeline_wait() -> None:
    """start()+wait() works with pipelines."""
    pipeline = builders.Pipeline(
        (
            builders.Cmd(("echo", "hello")),
            builders.Cmd(("tr", "a-z", "A-Z")),
        )
    )
    async with start(pipeline) as execution:
        assert await execution.wait() == 0


async def test_start_pipefail() -> None:
    """Pipefail semantics work through start()+wait()."""
    pipeline = builders.Pipeline(
        (
            builders.Cmd(("sh", "-c", "exit 1")),
            builders.Cmd(("sh", "-c", "exit 0")),
            builders.Cmd(("sh", "-c", "exit 2")),
        )
    )
    async with start(pipeline) as execution:
        assert await execution.wait() == 2


# =============================================================================
# Fn (Python function as pipeline stage)
# =============================================================================


async def _upper(ctx: ByteStage) -> int:
    """Read stdin, uppercase, write to stdout."""
    data = await ctx.stdin.read()
    await ctx.stdout.write(data.upper())
    return 0


async def _generate(ctx: ByteStage) -> int:
    """Write fixed data to stdout (ignores stdin)."""
    await ctx.stdout.write(b"generated\n")
    return 0


async def _exit_42(ctx: ByteStage) -> int:
    """Return non-zero exit code."""
    return 42


async def _raises(ctx: ByteStage) -> int:
    """Raise an exception."""
    raise ValueError("test error")


async def test_fn_standalone() -> None:
    """Standalone Fn execution returns 0."""
    assert await builders.Fn(_generate).run() == 0


async def test_fn_standalone_out() -> None:
    """Standalone Fn captures stdout correctly."""
    result = await builders.Fn(_generate).out()
    assert result == "generated\n"


async def test_fn_in_pipeline_last() -> None:
    """Fn as the last stage of a pipeline processes piped input."""
    pipeline = builders.Pipeline((builders.Cmd(("echo", "hello")), builders.Fn(_upper)))
    result = await pipeline.out()
    assert result == "HELLO\n"


async def test_fn_in_pipeline_first() -> None:
    """Fn as the first stage of a pipeline produces output for next stage."""
    pipeline = builders.Pipeline((builders.Fn(_generate), builders.Cmd(("cat",))))
    result = await pipeline.out()
    assert result == "generated\n"


async def test_fn_in_pipeline_middle() -> None:
    """Fn as a middle stage transforms data between two commands."""
    pipeline = builders.Pipeline(
        (builders.Cmd(("echo", "hello")), builders.Fn(_upper), builders.Cmd(("cat",)))
    )
    result = await pipeline.out()
    assert result == "HELLO\n"


async def test_fn_only_pipeline() -> None:
    """Pipeline with only Fn stages works end to end."""
    pipeline = builders.Pipeline((builders.Fn(_generate), builders.Fn(_upper)))
    result = await pipeline.out()
    assert result == "GENERATED\n"


async def test_fn_exit_code() -> None:
    """Fn exit code is propagated through .run()."""
    assert await builders.Fn(_exit_42).run() == 42


async def test_fn_pipefail() -> None:
    """Pipefail returns rightmost non-zero — Fn participates in reporting."""
    # Fn fails (42), Cmd succeeds (0) → rightmost non-zero is 42
    pipeline_fn_first = builders.Pipeline(
        (builders.Fn(_exit_42), builders.Cmd(("cat",)))
    )
    assert await pipeline_fn_first.run() == 42

    # Cmd succeeds (0), Fn fails (42) → rightmost non-zero is 42
    pipeline_fn_last = builders.Pipeline(
        (builders.Cmd(("true",)), builders.Fn(_exit_42))
    )
    assert await pipeline_fn_last.run() == 42

    # Both succeed → 0
    pipeline_both_ok = builders.Pipeline(
        (builders.Fn(_generate), builders.Cmd(("cat",)))
    )
    assert await pipeline_both_ok.run() == 0


async def test_fn_exception_returns_1() -> None:
    """Exception in Fn is caught and reported as returncode 1."""
    assert await builders.Fn(_raises).run() == 1


async def test_fn_as_sub_in() -> None:
    """Fn used as input process substitution via FdFromSub."""
    sub = builders.SubIn(builders.Fn(_generate))
    command = builders.Cmd(("cat",), redirects=(builders.FdFromSub(STDIN, sub),))
    result = await command.out()
    assert result == "generated\n"


async def test_fn_as_sub_out() -> None:
    """Fn used as output process substitution via FdToSub."""

    async def _collector(ctx: ByteStage) -> int:
        """Read all stdin and discard."""
        await ctx.stdin.read()
        return 0

    sub = builders.SubOut(builders.Fn(_collector))
    command = builders.Cmd(
        ("echo", "hello"), redirects=(builders.FdToSub(STDOUT, sub),)
    )
    assert await command.run() == 0


async def test_fn_cancel() -> None:
    """Cancelling a running Fn task raises CancelledError."""

    async def _slow(ctx: ByteStage) -> int:
        await asyncio.sleep(60)
        return 0

    task = asyncio.create_task(builders.Fn(_slow).run())
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_fn_cancel_mixed_pipeline() -> None:
    """Cancelling a cmd | fn | cmd pipeline cancels all stages."""

    async def _slow(ctx: ByteStage) -> int:
        await asyncio.sleep(60)
        return 0

    task = asyncio.create_task(
        builders.Pipeline(
            (
                builders.Cmd(("sleep", "60")),
                builders.Fn(_slow),
                builders.Cmd(("sleep", "60")),
            )
        ).run()
    )
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_fn_cancel_mid_read() -> None:
    """Cancelling while Fn is blocked on stdin.read() cleans up."""

    async def _reader(ctx: ByteStage) -> int:
        await ctx.stdin.read()  # blocks until EOF or cancel
        return 0

    task = asyncio.create_task(
        builders.Pipeline((builders.Cmd(("sleep", "60")), builders.Fn(_reader))).run()
    )
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_fn_cancel_mid_write() -> None:
    """Cancelling while Fn is blocked on stdout.write() cleans up."""

    async def _writer(ctx: ByteStage) -> int:
        # Write enough to fill the pipe buffer and block
        chunk = b"x" * 65536
        while True:
            await ctx.stdout.write(chunk)
        return 0  # type: ignore[unreachable]

    task = asyncio.create_task(
        builders.Pipeline((builders.Fn(_writer), builders.Cmd(("sleep", "60")))).run()
    )
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


# =============================================================================
# start() — async context manager lifecycle
# =============================================================================


async def test_start_wait_returns_exit_code() -> None:
    """start() + wait() returns exit code."""
    async with start(builders.Cmd(("true",))) as execution:
        assert isinstance(execution, Job)
        code = await execution.wait()
    assert code == 0
    assert execution.returncode == 0


async def test_start_wait_failure() -> None:
    """start() + wait() returns non-zero exit code."""
    async with start(builders.Cmd(("false",))) as execution:
        code = await execution.wait()
    assert code == 1
    assert execution.returncode == 1


async def test_start_auto_kill_on_exit() -> None:
    """Exception exit SIGTERM→SIGKILL long-running process."""

    execution: Job[None, None] | None = None
    with pytest.raises(RuntimeError, match="bail"):
        async with start(builders.Cmd(("sleep", "60"))) as execution:
            raise RuntimeError("bail")
    assert execution is not None
    assert execution.returncode == 128 + signal.SIGTERM


async def test_start_signal_term() -> None:
    """send SIGTERM via execution.terminate(), check 128+SIGTERM exit."""

    async with start(builders.Cmd(("sleep", "60"))) as execution:
        await asyncio.sleep(0.01)
        execution.terminate()
        code = await execution.wait()
    assert code == 128 + signal.SIGTERM


async def test_start_kill() -> None:
    """execution.kill() sends SIGKILL, wait() collects exit code."""

    async with start(builders.Cmd(("sleep", "60"))) as execution:
        await asyncio.sleep(0.01)
        execution.kill()
        code = await execution.wait()
    assert code == 128 + signal.SIGKILL


async def test_start_idempotent_wait() -> None:
    """Calling wait() twice returns the same code."""
    async with start(builders.Cmd(("true",))) as execution:
        first = await execution.wait()
        second = await execution.wait()
    assert first == 0
    assert second == 0
    assert first == second


async def test_start_pipeline() -> None:
    """start() works with pipelines and pipefail semantics."""
    pipeline = builders.Pipeline(
        (
            builders.Cmd(("sh", "-c", "exit 1")),
            builders.Cmd(("sh", "-c", "exit 0")),
            builders.Cmd(("sh", "-c", "exit 2")),
        )
    )
    async with start(pipeline) as execution:
        code = await execution.wait()
    assert code == 2


async def test_start_pipeline_auto_kill() -> None:
    """Exception exit kills all pipeline stages."""

    pipeline = builders.Pipeline(
        (
            builders.Cmd(("sleep", "60")),
            builders.Cmd(("sleep", "60")),
        )
    )
    execution: Job[None, None] | None = None
    with pytest.raises(RuntimeError, match="bail"):
        async with start(pipeline) as execution:
            raise RuntimeError("bail")
    assert execution is not None
    assert execution.returncode == 128 + signal.SIGTERM


# =============================================================================
# spawn_fn deferred start deadlocks
# =============================================================================


async def test_start_fn_stdout_read_before_wait() -> None:
    """Reading stdout before wait() on a standalone Fn should not deadlock."""
    async with start(builders.Fn(_generate)).stdout(PIPE) as execution:
        captured = await asyncio.wait_for(execution.stdout.read(), timeout=2.0)
        code = await execution.wait()
    assert code == 0
    assert captured == "generated\n"


async def test_start_fn_pipeline_stdout_read_before_wait() -> None:
    """Reading stdout before wait() on an Fn|cmd pipeline should not deadlock."""
    pipeline = builders.Pipeline((builders.Fn(_generate), builders.Cmd(("cat",))))
    async with start(pipeline).stdout(PIPE) as execution:
        captured = await asyncio.wait_for(execution.stdout.read(), timeout=2.0)
        code = await execution.wait()
    assert code == 0
    assert captured == "generated\n"


async def test_start_feed_stdout_read_before_wait() -> None:
    """Reading stdout before wait() with FdFromData should not deadlock."""
    command = builders.Cmd(("cat",), redirects=(builders.FdFromData(STDIN, "hello"),))
    async with start(command).stdout(PIPE) as execution:
        captured = await asyncio.wait_for(execution.stdout.read(), timeout=2.0)
        code = await execution.wait()
    assert code == 0
    assert captured == "hello"


# =============================================================================
# Job.close()
# =============================================================================


async def test_close_eof_with_stdin_pipe() -> None:
    """EOF close: cat sees EOF from closed stdin pipe and exits naturally."""
    async with start(builders.Cmd(("cat",))).stdin(PIPE) as execution:
        used = await execution.close(method=CloseMethod.EOF, timeout=2.0)
    assert used == CloseMethod.EOF
    assert execution.returncode == 0


async def test_close_eof_without_stdin_escalates() -> None:
    """EOF close without stdin pipe auto-escalates to TERMINATE."""

    async with start(builders.Cmd(("sleep", "60"))) as execution:
        used = await execution.close(method=CloseMethod.EOF, timeout=0.5)
    assert used == CloseMethod.TERMINATE
    assert execution.returncode == 128 + signal.SIGTERM


async def test_close_terminate() -> None:
    """TERMINATE close sends SIGTERM, process exits."""

    async with start(builders.Cmd(("sleep", "60"))) as execution:
        used = await execution.close(method=CloseMethod.TERMINATE, timeout=2.0)
    assert used == CloseMethod.TERMINATE
    assert execution.returncode == 128 + signal.SIGTERM


async def test_close_kill() -> None:
    """KILL close sends SIGKILL immediately."""

    async with start(builders.Cmd(("sleep", "60"))) as execution:
        used = await execution.close(method=CloseMethod.KILL, timeout=2.0)
    assert used == CloseMethod.KILL
    assert execution.returncode == 128 + signal.SIGKILL


async def test_close_terminate_escalates_to_kill() -> None:
    """TERMINATE escalates to KILL when process ignores SIGTERM."""

    # Python with SIG_IGN reliably ignores SIGTERM at kernel level
    ignore_term = builders.Cmd(
        (
            "python3",
            "-c",
            "import signal,time;"
            "signal.signal(signal.SIGTERM,signal.SIG_IGN);"
            "time.sleep(60)",
        )
    )
    async with start(ignore_term) as execution:
        await asyncio.sleep(0.05)  # let Python start and install SIG_IGN
        used = await execution.close(method=CloseMethod.TERMINATE, timeout=0.05)
    assert used == CloseMethod.KILL
    assert execution.returncode == 128 + signal.SIGKILL


async def test_close_eof_escalates_through_terminate_to_kill() -> None:
    """EOF escalates through TERMINATE to KILL when process ignores both."""

    # Ignores SIGTERM, no stdin pipe → EOF step skipped, TERM ignored, escalates to KILL
    ignore_term = builders.Cmd(
        (
            "python3",
            "-c",
            "import signal,time;"
            "signal.signal(signal.SIGTERM,signal.SIG_IGN);"
            "time.sleep(60)",
        )
    )
    async with start(ignore_term) as execution:
        await asyncio.sleep(0.05)  # let Python start and install SIG_IGN
        used = await execution.close(method=CloseMethod.EOF, timeout=0.05)
    assert used == CloseMethod.KILL
    assert execution.returncode == 128 + signal.SIGKILL


async def test_close_pipeline_terminate() -> None:
    """TERMINATE close kills all pipeline stages."""

    pipeline = builders.Pipeline(
        (builders.Cmd(("sleep", "60")), builders.Cmd(("sleep", "60")))
    )
    async with start(pipeline) as execution:
        used = await execution.close(method=CloseMethod.TERMINATE, timeout=2.0)
    assert used == CloseMethod.TERMINATE
    assert execution.returncode == 128 + signal.SIGTERM


async def test_close_default_is_eof() -> None:
    """Default close method is EOF — cat exits on EOF from closed stdin pipe."""
    async with start(builders.Cmd(("cat",))).stdin(PIPE) as execution:
        used = await execution.close(timeout=2.0)
    assert used == CloseMethod.EOF
    assert execution.returncode == 0


async def test_close_idempotent() -> None:
    """Calling close() twice does not error."""
    async with start(builders.Cmd(("cat",))).stdin(PIPE) as execution:
        await execution.close(timeout=2.0)
        await execution.close(timeout=2.0)
    assert execution.returncode == 0


async def test_close_fn_terminate() -> None:
    """TERMINATE cancels a long-running fn() task."""

    async def _slow(ctx: ByteStage) -> int:
        await asyncio.sleep(60)
        return 0

    async with start(builders.Fn(_slow)) as execution:
        used = await execution.close(method=CloseMethod.TERMINATE, timeout=2.0)
    assert used == CloseMethod.TERMINATE
    assert execution.returncode == 128 + signal.SIGKILL


async def test_close_fn_kill() -> None:
    """KILL cancels a long-running fn() task."""

    async def _slow(ctx: ByteStage) -> int:
        await asyncio.sleep(60)
        return 0

    async with start(builders.Fn(_slow)) as execution:
        used = await execution.close(method=CloseMethod.KILL, timeout=2.0)
    assert used == CloseMethod.KILL
    assert execution.returncode == 128 + signal.SIGKILL


# =============================================================================
# start() with stderr PIPE
# =============================================================================


async def test_start_stderr_pipe() -> None:
    """start().stderr(PIPE) captures stderr from a single command."""
    async with start(
        builders.Cmd(("sh", "-c", "echo err >&2")),
    ).stderr(PIPE) as execution:
        code, captured = await asyncio.gather(
            execution.wait(),
            execution.stderr.read(),
        )
    assert code == 0
    assert captured == "err\n"


async def test_start_stdout_stderr_pipe() -> None:
    """start().stdout(PIPE).stderr(PIPE) captures both simultaneously."""
    async with (
        start(
            builders.Cmd(("sh", "-c", "echo out; echo err >&2")),
        )
        .stdout(PIPE)
        .stderr(PIPE) as execution
    ):
        code, captured_out, captured_err = await asyncio.gather(
            execution.wait(),
            execution.stdout.read(),
            execution.stderr.read(),
        )
    assert code == 0
    assert captured_out == "out\n"
    assert captured_err == "err\n"


async def test_start_stderr_pipe_bytes() -> None:
    """start().stderr(PIPE, encoding=None) gives ByteReadStream."""
    async with start(
        builders.Cmd(("sh", "-c", "echo err >&2")),
    ).stderr(PIPE, encoding=None) as execution:
        code, captured = await asyncio.gather(
            execution.wait(),
            execution.stderr.read(),
        )
    assert code == 0
    assert captured == b"err\n"


async def test_start_stderr_pipe_pipeline() -> None:
    """Pipeline stderr: all stages' stderr captured in one stream."""
    pipeline = builders.Pipeline(
        (
            builders.Cmd(("sh", "-c", "echo e1 >&2; echo out")),
            builders.Cmd(("sh", "-c", "echo e2 >&2; cat")),
        )
    )
    async with start(pipeline).stdout(PIPE).stderr(PIPE) as execution:
        code, captured_out, captured_err = await asyncio.gather(
            execution.wait(),
            execution.stdout.read(),
            execution.stderr.read(),
        )
    assert code == 0
    assert captured_out == "out\n"
    assert "e1\n" in captured_err
    assert "e2\n" in captured_err


async def test_start_stderr_fd() -> None:
    """start().stderr(fd) for raw fd passthrough."""
    read_fd, write_fd = os.pipe()
    async with start(
        builders.Cmd(("sh", "-c", "echo err >&2")),
    ).stderr(write_fd) as execution:
        os.close(write_fd)  # start() dup'd it; close our copy
        code = await execution.wait()
    data = os.read(read_fd, 4096)
    os.close(read_fd)
    assert code == 0
    assert data == b"err\n"


async def test_start_no_stderr_pipe_stream_none() -> None:
    """Without stderr PIPE, stderr stream is None."""
    async with start(builders.Cmd(("true",))) as execution:
        assert execution.stderr is None
        await execution.wait()


async def test_close_fn_pipeline() -> None:
    """TERMINATE close kills cmd and fn stages in a mixed pipeline."""

    async def _slow(ctx: ByteStage) -> int:
        await asyncio.sleep(60)
        return 0

    pipeline = builders.Pipeline(
        (
            builders.Cmd(("sleep", "60")),
            builders.Fn(_slow),
            builders.Cmd(("sleep", "60")),
        )
    )
    async with start(pipeline) as execution:
        used = await execution.close(method=CloseMethod.TERMINATE, timeout=2.0)
    assert used == CloseMethod.TERMINATE
    assert execution.returncode != 0
