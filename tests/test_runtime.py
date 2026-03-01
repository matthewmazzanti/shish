"""Runtime tests using IR nodes directly — verifies executor consumes IR correctly."""

import asyncio
import subprocess
from pathlib import Path

import pytest

from shish import STDERR, STDIN, STDOUT, ir
from shish.runtime import out, run

# =============================================================================
# Basic Execution
# =============================================================================


async def test_run_cmd() -> None:
    assert await run(ir.Cmd(("true",))) == 0
    assert await run(ir.Cmd(("false",))) == 1


async def test_run_cmd_with_args() -> None:
    result = await out(ir.Cmd(("echo", "hello")))
    assert result == "hello\n"


# =============================================================================
# Pipelines
# =============================================================================


async def test_pipeline() -> None:
    pipeline = ir.Pipeline(
        (
            ir.Cmd(("echo", "hello")),
            ir.Cmd(("cat",)),
        )
    )
    assert await run(pipeline) == 0


async def test_pipeline_output() -> None:
    pipeline = ir.Pipeline(
        (
            ir.Cmd(("echo", "hello world")),
            ir.Cmd(("tr", "a-z", "A-Z")),
        )
    )
    result = await out(pipeline)
    assert result == "HELLO WORLD\n"


async def test_pipeline_pipefail() -> None:
    pipeline = ir.Pipeline(
        (
            ir.Cmd(("false",)),
            ir.Cmd(("cat",)),
        )
    )
    assert await run(pipeline) != 0


async def test_pipeline_pipefail_rightmost() -> None:
    """Pipefail returns rightmost non-zero exit code."""
    pipeline = ir.Pipeline(
        (
            ir.Cmd(("sh", "-c", "exit 1")),
            ir.Cmd(("sh", "-c", "exit 0")),
            ir.Cmd(("sh", "-c", "exit 2")),
        )
    )
    assert await run(pipeline) == 2


async def test_pipeline_three_stages() -> None:
    pipeline = ir.Pipeline(
        (
            ir.Cmd(("echo", "hello")),
            ir.Cmd(("tr", "a-z", "A-Z")),
            ir.Cmd(("tr", "H", "J")),
        )
    )
    result = await out(pipeline)
    assert result == "JELLO\n"


async def test_pipeline_sigpipe() -> None:
    pipeline = ir.Pipeline(
        (
            ir.Cmd(("yes",)),
            ir.Cmd(("head", "-n", "1")),
        )
    )
    result = await asyncio.wait_for(run(pipeline), timeout=2.0)
    assert result in (0, 141)


async def test_empty_pipeline() -> None:
    """Empty pipeline returns success."""
    assert await run(ir.Pipeline(())) == 0


async def test_single_stage_pipeline() -> None:
    """Pipeline with one stage behaves like a plain command."""
    result = await out(ir.Pipeline((ir.Cmd(("echo", "hello")),)))
    assert result == "hello\n"


# =============================================================================
# File Redirects (on Cmd)
# =============================================================================


async def test_cmd_stdout_redirect(tmp_path: Path) -> None:
    outfile = tmp_path / "out.txt"
    command = ir.Cmd(("echo", "hello"), redirects=(ir.FdToFile(STDOUT, outfile),))
    assert await run(command) == 0
    assert outfile.read_text() == "hello\n"


async def test_cmd_stdout_append(tmp_path: Path) -> None:
    outfile = tmp_path / "out.txt"
    await run(ir.Cmd(("echo", "first"), redirects=(ir.FdToFile(STDOUT, outfile),)))
    await run(
        ir.Cmd(
            ("echo", "second"),
            redirects=(ir.FdToFile(STDOUT, outfile, append=True),),
        )
    )
    assert outfile.read_text() == "first\nsecond\n"


async def test_cmd_stdin_file(tmp_path: Path) -> None:
    inp = tmp_path / "in.txt"
    outfile = tmp_path / "out.txt"
    inp.write_text("hello from file")
    command = ir.Cmd(
        ("cat",),
        redirects=(
            ir.FdFromFile(STDIN, inp),
            ir.FdToFile(STDOUT, outfile),
        ),
    )
    assert await run(command) == 0
    assert outfile.read_text() == "hello from file"


async def test_cmd_stdin_data(tmp_path: Path) -> None:
    outfile = tmp_path / "out.txt"
    command = ir.Cmd(
        ("cat",),
        redirects=(
            ir.FdFromData(STDIN, "hello world"),
            ir.FdToFile(STDOUT, outfile),
        ),
    )
    assert await run(command) == 0
    assert outfile.read_text() == "hello world"


async def test_cmd_stdin_data_bytes(tmp_path: Path) -> None:
    outfile = tmp_path / "out.bin"
    data = bytes(range(256))
    command = ir.Cmd(
        ("cat",),
        redirects=(
            ir.FdFromData(STDIN, data),
            ir.FdToFile(STDOUT, outfile),
        ),
    )
    assert await run(command) == 0
    assert outfile.read_bytes() == data


# =============================================================================
# Pipeline with per-stage redirects
# =============================================================================


async def test_pipeline_first_stage_stdin(tmp_path: Path) -> None:
    inp = tmp_path / "in.txt"
    inp.write_text("hello\nworld\n")
    pipeline = ir.Pipeline(
        (
            ir.Cmd(("cat",), redirects=(ir.FdFromFile(STDIN, inp),)),
            ir.Cmd(("grep", "world")),
        )
    )
    result = await out(pipeline)
    assert result == "world\n"


async def test_pipeline_stage_stdout_override(tmp_path: Path) -> None:
    # (echo "hello" > file) | cat — stdout goes to file, cat gets nothing
    outfile = tmp_path / "out.txt"
    result_file = tmp_path / "result.txt"
    pipeline = ir.Pipeline(
        (
            ir.Cmd(("echo", "hello"), redirects=(ir.FdToFile(STDOUT, outfile),)),
            ir.Cmd(("cat",), redirects=(ir.FdToFile(STDOUT, result_file),)),
        )
    )
    assert await run(pipeline) == 0
    assert outfile.read_text() == "hello\n"
    assert result_file.read_text() == ""


async def test_pipeline_first_stage_data(tmp_path: Path) -> None:
    """(cat << "data") | grep — data redirect on first pipeline stage."""
    pipeline = ir.Pipeline(
        (
            ir.Cmd(
                ("cat",),
                redirects=(ir.FdFromData(STDIN, "hello\nworld\n"),),
            ),
            ir.Cmd(("grep", "world")),
        )
    )
    result = await out(pipeline)
    assert result == "world\n"


async def test_pipeline_last_stage_file(tmp_path: Path) -> None:
    """echo | cat > file — last stage redirects stdout to file."""
    outfile = tmp_path / "out.txt"
    pipeline = ir.Pipeline(
        (
            ir.Cmd(("echo", "hello")),
            ir.Cmd(("cat",), redirects=(ir.FdToFile(STDOUT, outfile),)),
        )
    )
    assert await run(pipeline) == 0
    assert outfile.read_text() == "hello\n"


async def test_pipeline_middle_stage_redirect(tmp_path: Path) -> None:
    """a | (b > log) | c — middle stage redirect diverts to file."""
    logfile = tmp_path / "log.txt"
    pipeline = ir.Pipeline(
        (
            ir.Cmd(("echo", "hello")),
            ir.Cmd(("cat",), redirects=(ir.FdToFile(STDOUT, logfile),)),
            ir.Cmd(("cat",)),
        )
    )
    result = await out(pipeline)
    assert logfile.read_text() == "hello\n"
    assert result == ""


# =============================================================================
# Fd-to-fd redirects (2>&1, etc.)
# =============================================================================


async def test_fd_to_fd_stderr_to_stdout() -> None:
    """cmd 2>&1 — stderr merges into stdout."""
    command = ir.Cmd(
        ("sh", "-c", "echo out; echo err >&2"),
        redirects=(ir.FdToFd(STDOUT, STDERR),),
    )
    result = await out(command)
    assert "out\n" in result
    assert "err\n" in result


async def test_fd_to_fd_file_then_dup(tmp_path: Path) -> None:
    """cmd > file 2>&1 — both stdout and stderr go to file."""
    outfile = tmp_path / "out.txt"
    command = ir.Cmd(
        ("sh", "-c", "echo out; echo err >&2"),
        redirects=(
            ir.FdToFile(STDOUT, outfile),
            ir.FdToFd(STDOUT, STDERR),
        ),
    )
    assert await run(command) == 0
    content = outfile.read_text()
    assert "out\n" in content
    assert "err\n" in content


async def test_fd_to_fd_order_matters(tmp_path: Path) -> None:
    """cmd 2>&1 > file — stderr goes to original stdout, only stdout goes to file.

    Order matters: 2>&1 copies current fd 1 (pipe/terminal) to fd 2,
    then > file redirects fd 1 to file. So stderr goes to pipe, stdout to file.
    """
    outfile = tmp_path / "out.txt"
    command = ir.Cmd(
        ("sh", "-c", "echo out; echo err >&2"),
        redirects=(
            ir.FdToFd(STDOUT, STDERR),
            ir.FdToFile(STDOUT, outfile),
        ),
    )
    # Capture what comes through the pipeline stdout (which is stderr after dup)
    result = await out(command)
    assert result == "err\n"
    assert outfile.read_text() == "out\n"


async def test_fd_to_file_stderr(tmp_path: Path) -> None:
    """cmd 2> file — stderr to file, stdout to pipeline."""
    errfile = tmp_path / "err.txt"
    command = ir.Cmd(
        ("sh", "-c", "echo out; echo err >&2"),
        redirects=(ir.FdToFile(STDERR, errfile),),
    )
    result = await out(command)
    assert result == "out\n"
    assert errfile.read_text() == "err\n"


async def test_fd_to_file_arbitrary_fd(tmp_path: Path) -> None:
    """cmd 3> file — redirect fd 3 to a file."""
    outfile = tmp_path / "out.txt"
    command = ir.Cmd(
        ("sh", "-c", "echo hello >&3"),
        redirects=(ir.FdToFile(3, outfile),),
    )
    assert await run(command) == 0
    assert outfile.read_text() == "hello\n"


async def test_fd_from_file_arbitrary_fd(tmp_path: Path) -> None:
    """cmd 3< file — redirect fd 3 from a file, then read it."""
    infile = tmp_path / "in.txt"
    infile.write_text("hello from fd3")
    command = ir.Cmd(
        ("sh", "-c", "cat <&3"),
        redirects=(ir.FdFromFile(3, infile),),
    )
    result = await out(command)
    assert result == "hello from fd3"


async def test_fd_from_data_arbitrary_fd(tmp_path: Path) -> None:
    """cmd 3<<< "data" — feed data into fd 3, then read it."""
    command = ir.Cmd(
        ("sh", "-c", "cat <&3"),
        redirects=(ir.FdFromData(3, "hello from fd3"),),
    )
    result = await out(command)
    assert result == "hello from fd3"


async def test_fd_to_file_append_arbitrary_fd(tmp_path: Path) -> None:
    """cmd 3>> file — append mode on arbitrary fd."""
    outfile = tmp_path / "out.txt"
    await run(
        ir.Cmd(("sh", "-c", "echo first >&3"), redirects=(ir.FdToFile(3, outfile),))
    )
    await run(
        ir.Cmd(
            ("sh", "-c", "echo second >&3"),
            redirects=(ir.FdToFile(3, outfile, append=True),),
        )
    )
    assert outfile.read_text() == "first\nsecond\n"


async def test_multiple_redirects_same_fd(tmp_path: Path) -> None:
    """Last redirect on same fd wins, matching bash semantics."""
    file1 = tmp_path / "file1.txt"
    file2 = tmp_path / "file2.txt"
    command = ir.Cmd(
        ("echo", "hello"),
        redirects=(
            ir.FdToFile(STDOUT, file1),
            ir.FdToFile(STDOUT, file2),
        ),
    )
    assert await run(command) == 0
    # Second redirect wins — output goes to file2
    assert file2.read_text() == "hello\n"
    # file1 gets created but nothing written to it
    assert file1.read_text() == ""


# =============================================================================
# FdClose
# =============================================================================


async def test_fd_close() -> None:
    """Closing stdin causes cat to fail with EBADF."""
    command = ir.Cmd(
        ("cat",),
        redirects=(ir.FdClose(STDIN),),
    )
    assert await run(command) == 1


# =============================================================================
# Process Substitution
# =============================================================================


async def test_sub_in(tmp_path: Path) -> None:
    outfile = tmp_path / "out.txt"
    sub = ir.SubIn(ir.Cmd(("echo", "hello")))
    command = ir.Cmd(("cat", sub), redirects=(ir.FdToFile(STDOUT, outfile),))
    assert await run(command) == 0
    assert outfile.read_text() == "hello\n"


async def test_sub_in_two_sources(tmp_path: Path) -> None:
    file1 = tmp_path / "file1.txt"
    file2 = tmp_path / "file2.txt"
    outfile = tmp_path / "out.txt"
    file1.write_text("b\na\nc\n")
    file2.write_text("b\na\nc\n")
    sub1 = ir.SubIn(ir.Cmd(("sort", str(file1))))
    sub2 = ir.SubIn(ir.Cmd(("sort", str(file2))))
    command = ir.Cmd(("diff", sub1, sub2), redirects=(ir.FdToFile(STDOUT, outfile),))
    result = await run(command)
    assert result == 0
    assert outfile.read_text() == ""


async def test_sub_out(tmp_path: Path) -> None:
    outfile = tmp_path / "out.txt"
    main_out = tmp_path / "main.txt"
    sink = ir.SubOut(
        ir.Cmd(("cat",), redirects=(ir.FdToFile(STDOUT, outfile),)),
    )
    pipeline = ir.Pipeline(
        (
            ir.Cmd(("echo", "hello")),
            ir.Cmd(("tee", sink), redirects=(ir.FdToFile(STDOUT, main_out),)),
        )
    )
    assert await run(pipeline) == 0
    assert outfile.read_text() == "hello\n"
    assert main_out.read_text() == "hello\n"


async def test_sub_in_with_pipeline(tmp_path: Path) -> None:
    outfile = tmp_path / "out.txt"
    sub = ir.SubIn(
        ir.Pipeline(
            (
                ir.Cmd(("echo", "hello")),
                ir.Cmd(("tr", "a-z", "A-Z")),
            )
        ),
    )
    command = ir.Cmd(("cat", sub), redirects=(ir.FdToFile(STDOUT, outfile),))
    assert await run(command) == 0
    assert outfile.read_text() == "HELLO\n"


async def test_fd_from_sub_stdin() -> None:
    """cat < <(echo hello) — redirect stdin from process substitution."""
    sub = ir.SubIn(ir.Cmd(("echo", "hello")))
    command = ir.Cmd(("cat",), redirects=(ir.FdFromSub(STDIN, sub),))
    result = await out(command)
    assert result == "hello\n"


async def test_fd_from_sub_arbitrary_fd(tmp_path: Path) -> None:
    """cmd 3< <(echo hello) — redirect fd 3 from process substitution."""
    outfile = tmp_path / "out.txt"
    sub = ir.SubIn(ir.Cmd(("echo", "hello")))
    command = ir.Cmd(
        ("sh", "-c", "cat <&3"),
        redirects=(
            ir.FdFromSub(3, sub),
            ir.FdToFile(STDOUT, outfile),
        ),
    )
    assert await run(command) == 0
    assert outfile.read_text() == "hello\n"


async def test_fd_to_sub_stdout(tmp_path: Path) -> None:
    """cmd 1> >(cat > file) — redirect stdout to process substitution."""
    outfile = tmp_path / "out.txt"
    sub = ir.SubOut(
        ir.Cmd(("cat",), redirects=(ir.FdToFile(STDOUT, outfile),)),
    )
    command = ir.Cmd(("echo", "hello"), redirects=(ir.FdToSub(STDOUT, sub),))
    assert await run(command) == 0
    assert outfile.read_text() == "hello\n"


async def test_fd_to_sub_arbitrary_fd(tmp_path: Path) -> None:
    """cmd 3> >(cat > file) — redirect fd 3 to process substitution."""
    outfile = tmp_path / "out.txt"
    sub = ir.SubOut(
        ir.Cmd(("cat",), redirects=(ir.FdToFile(STDOUT, outfile),)),
    )
    command = ir.Cmd(
        ("sh", "-c", "echo hello >&3"),
        redirects=(ir.FdToSub(3, sub),),
    )
    assert await run(command) == 0
    assert outfile.read_text() == "hello\n"


async def test_fd_from_sub_with_pipeline() -> None:
    """cat < <(echo hello | tr a-z A-Z) — sub contains a pipeline."""
    sub = ir.SubIn(
        ir.Pipeline(
            (
                ir.Cmd(("echo", "hello")),
                ir.Cmd(("tr", "a-z", "A-Z")),
            )
        ),
    )
    command = ir.Cmd(("cat",), redirects=(ir.FdFromSub(STDIN, sub),))
    result = await out(command)
    assert result == "HELLO\n"


async def test_fd_to_sub_with_pipeline(tmp_path: Path) -> None:
    """cmd > >(pipeline > file) — output sub contains a pipeline."""
    outfile = tmp_path / "out.txt"
    sub = ir.SubOut(
        ir.Pipeline(
            (
                ir.Cmd(("tr", "a-z", "A-Z")),
                ir.Cmd(("cat",), redirects=(ir.FdToFile(STDOUT, outfile),)),
            )
        ),
    )
    command = ir.Cmd(("echo", "hello"), redirects=(ir.FdToSub(STDOUT, sub),))
    assert await run(command) == 0
    assert outfile.read_text() == "HELLO\n"


async def test_data_write_to_early_exit() -> None:
    """Large data feed to process that exits early doesn't hang or crash."""
    data = b"x" * (256 * 1024)
    # head -c 1 reads 1 byte then exits; rest of data write hits broken pipe
    command = ir.Cmd(
        ("head", "-c", "1"),
        redirects=(ir.FdFromData(STDIN, data),),
    )
    result = await out(command)
    assert result == "x"


async def test_sub_exit_code_ignored_arg() -> None:
    """cat <(false) — sub failure doesn't affect main exit code."""
    sub = ir.SubIn(ir.Cmd(("false",)))
    command = ir.Cmd(("cat", sub))
    assert await run(command) == 0


async def test_sub_exit_code_ignored_redirect() -> None:
    """true 1> >(false) — redirect sub failure doesn't affect exit code."""
    sub = ir.SubOut(ir.Cmd(("false",)))
    command = ir.Cmd(("true",), redirects=(ir.FdToSub(STDOUT, sub),))
    assert await run(command) == 0


async def test_sub_exit_code_ignored_from_redirect() -> None:
    """cat < <(false) — input sub failure doesn't affect exit code."""
    sub = ir.SubIn(ir.Cmd(("false",)))
    command = ir.Cmd(("cat",), redirects=(ir.FdFromSub(STDIN, sub),))
    assert await run(command) == 0


async def test_sub_nested(tmp_path: Path) -> None:
    """Nested process substitution: cat <(cat <(echo hello))."""
    outfile = tmp_path / "out.txt"
    inner = ir.SubIn(ir.Cmd(("echo", "hello")))
    outer = ir.SubIn(ir.Cmd(("cat", inner)))
    command = ir.Cmd(("cat", outer), redirects=(ir.FdToFile(STDOUT, outfile),))
    assert await run(command) == 0
    assert outfile.read_text() == "hello\n"


async def test_sub_in_with_data_stdin(tmp_path: Path) -> None:
    outfile = tmp_path / "out.txt"
    sub = ir.SubIn(
        ir.Cmd(("cat",), redirects=(ir.FdFromData(STDIN, "injected data"),)),
    )
    command = ir.Cmd(("cat", sub), redirects=(ir.FdToFile(STDOUT, outfile),))
    assert await run(command) == 0
    assert outfile.read_text() == "injected data"


# =============================================================================
# Output Capture
# =============================================================================


async def test_out_simple() -> None:
    result = await out(ir.Cmd(("echo", "hello")))
    assert result == "hello\n"


async def test_out_pipeline() -> None:
    pipeline = ir.Pipeline(
        (
            ir.Cmd(("echo", "hello world")),
            ir.Cmd(("tr", "a-z", "A-Z")),
        )
    )
    result = await out(pipeline)
    assert result == "HELLO WORLD\n"


async def test_out_empty() -> None:
    command = ir.Cmd(("cat",), redirects=(ir.FdFromData(STDIN, ""),))
    result = await out(command)
    assert result == ""


async def test_out_with_data() -> None:
    command = ir.Cmd(("cat",), redirects=(ir.FdFromData(STDIN, "input data"),))
    result = await out(command)
    assert result == "input data"


async def test_out_binary() -> None:
    data = bytes(range(256))
    command = ir.Cmd(("cat",), redirects=(ir.FdFromData(STDIN, data),))
    result = await out(command, encoding=None)
    assert result == data


async def test_out_large_data() -> None:
    """Data larger than pipe buffer (64K) exercises backpressure path."""
    data = b"x" * (256 * 1024)
    command = ir.Cmd(("cat",), redirects=(ir.FdFromData(STDIN, data),))
    result = await out(command, encoding=None)
    assert result == data


async def test_out_large_data_pipeline() -> None:
    """Multiple large data feeds in concurrent pipelines exercise interleaved writes."""
    data = b"x" * (256 * 1024)
    result_a, result_b = await asyncio.gather(
        out(
            ir.Cmd(("cat",), redirects=(ir.FdFromData(STDIN, data),)),
            encoding=None,
        ),
        out(
            ir.Cmd(("cat",), redirects=(ir.FdFromData(STDIN, data),)),
            encoding=None,
        ),
    )
    assert result_a == data
    assert result_b == data


async def test_out_large_data_multi_fd(tmp_path: Path) -> None:
    """Concurrent reads from multiple fds force interleaved async writes."""
    data = b"x" * (256 * 1024)
    out3 = tmp_path / "fd3.bin"
    out4 = tmp_path / "fd4.bin"
    command = ir.Cmd(
        ("sh", "-c", f"cat <&3 > {out3} & cat <&4 > {out4} & wait"),
        redirects=(
            ir.FdFromData(3, data),
            ir.FdFromData(4, data),
        ),
    )
    assert await run(command) == 0
    assert out3.read_bytes() == data
    assert out4.read_bytes() == data


async def test_out_raises_on_failure() -> None:
    with pytest.raises(subprocess.CalledProcessError) as exc_info:
        await out(ir.Cmd(("false",)))
    assert exc_info.value.returncode == 1


async def test_out_raises_preserves_output() -> None:
    with pytest.raises(subprocess.CalledProcessError) as exc_info:
        await out(ir.Cmd(("sh", "-c", "echo partial; exit 1")))
    assert exc_info.value.output == b"partial\n"


async def test_out_no_fd_leak_on_failure() -> None:
    """out() doesn't leak its capture pipe when command fails."""
    import os

    before = set(os.listdir("/proc/self/fd"))
    with pytest.raises(subprocess.CalledProcessError):
        await out(ir.Cmd(("false",)))
    after = set(os.listdir("/proc/self/fd"))
    assert before == after


# =============================================================================
# Concurrent Execution
# =============================================================================


async def test_concurrent_runs() -> None:
    results = await asyncio.gather(
        run(ir.Pipeline((ir.Cmd(("echo", "a")), ir.Cmd(("cat",))))),
        run(ir.Pipeline((ir.Cmd(("echo", "b")), ir.Cmd(("cat",))))),
        run(ir.Pipeline((ir.Cmd(("echo", "c")), ir.Cmd(("cat",))))),
    )
    assert results == [0, 0, 0]


async def test_concurrent_file_writes(tmp_path: Path) -> None:
    files = [tmp_path / f"out{idx}.txt" for idx in range(5)]
    await asyncio.gather(
        *[
            run(
                ir.Cmd(
                    ("echo", f"content{idx}"),
                    redirects=(ir.FdToFile(STDOUT, fpath),),
                )
            )
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
    task = asyncio.create_task(run(ir.Cmd(("sleep", "60"))))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_cancel_pipeline_kills_all_stages() -> None:
    """Cancelling a pipeline kills all stages."""
    task = asyncio.create_task(
        run(ir.Pipeline((ir.Cmd(("sleep", "60")), ir.Cmd(("sleep", "60")))))
    )
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_cancel_no_fd_leak() -> None:
    """No fds leak in the parent after cancellation."""
    import os

    before = set(os.listdir("/proc/self/fd"))
    task = asyncio.create_task(
        run(
            ir.Pipeline(
                (
                    ir.Cmd(("sleep", "60")),
                    ir.Cmd(("sleep", "60")),
                )
            )
        )
    )
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    after = set(os.listdir("/proc/self/fd"))
    assert before == after


# =============================================================================
# Factory functions used by runtime
# =============================================================================


async def test_signal_killed_exit_code() -> None:
    """Process killed by signal returns 128 + signal number."""
    # SIGKILL = 9, so exit code should be 137
    result = await run(ir.Cmd(("sh", "-c", "kill -9 $$")))
    assert result == 137


async def test_no_fd_leak_after_error() -> None:
    """No fds leak in the parent when command-not-found raises."""
    import os

    before = set(os.listdir("/proc/self/fd"))
    with pytest.raises(FileNotFoundError):
        await run(ir.Cmd(("nonexistent_command_xyz_12345",)))
    after = set(os.listdir("/proc/self/fd"))
    assert before == after


async def test_no_fd_leak_after_pipeline_error() -> None:
    """No fds leak when one pipeline stage fails to spawn."""
    import os

    before = set(os.listdir("/proc/self/fd"))
    with pytest.raises(FileNotFoundError):
        await run(
            ir.Pipeline(
                (
                    ir.Cmd(("echo", "hello")),
                    ir.Cmd(("nonexistent_command_xyz_12345",)),
                )
            )
        )
    after = set(os.listdir("/proc/self/fd"))
    assert before == after


async def test_pipeline_factory_execution() -> None:
    pipeline = ir.pipeline(
        ir.Cmd(("echo", "hello")),
        ir.Cmd(("tr", "a-z", "A-Z")),
    )
    result = await out(pipeline)
    assert result == "HELLO\n"


# =============================================================================
# Environment Variables
# =============================================================================


async def test_env_vars() -> None:
    """Child process sees env vars set via env_vars."""
    command = ir.Cmd(("printenv", "FOO"), env_vars=(("FOO", "bar"),))
    result = await out(command)
    assert result == "bar\n"


async def test_env_vars_multiple() -> None:
    """Child process sees multiple env vars."""
    command = ir.Cmd(
        ("sh", "-c", "echo $FOO $BAZ"),
        env_vars=(("FOO", "hello"), ("BAZ", "world")),
    )
    result = await out(command)
    assert result == "hello world\n"


async def test_env_vars_unset() -> None:
    """None value unsets an env var."""
    command = ir.Cmd(
        ("sh", "-c", "echo ${HOME-unset}"),
        env_vars=(("HOME", None),),
    )
    result = await out(command)
    assert result == "unset\n"


async def test_env_vars_override() -> None:
    """Env vars override inherited values."""
    import os

    original = os.environ.get("HOME", "")
    command = ir.Cmd(("printenv", "HOME"), env_vars=(("HOME", "/fake/home"),))
    result = await out(command)
    assert result == "/fake/home\n"
    # Verify parent env is not affected
    assert os.environ.get("HOME", "") == original


async def test_env_vars_pipeline_per_cmd() -> None:
    """Env vars are per-cmd in a pipeline, not shared."""
    pipeline = ir.Pipeline(
        (
            ir.Cmd(("printenv", "FOO"), env_vars=(("FOO", "first"),)),
            ir.Cmd(("cat",)),
        )
    )
    result = await out(pipeline)
    assert result == "first\n"


# =============================================================================
# Working Directory
# =============================================================================


async def test_working_dir(tmp_path: Path) -> None:
    """Child process runs in the specified working directory."""
    command = ir.Cmd(("pwd",), working_dir=tmp_path)
    result = await out(command)
    assert result.strip() == str(tmp_path.resolve())


async def test_working_dir_pwd_synced(tmp_path: Path) -> None:
    """PWD env var is synced when working_dir is set."""
    command = ir.Cmd(("printenv", "PWD"), working_dir=tmp_path)
    result = await out(command)
    assert result.strip() == str(tmp_path)
