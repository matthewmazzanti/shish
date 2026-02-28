import asyncio
import subprocess
from pathlib import Path

import pytest

from shish import STDERR, STDIN, STDOUT, close, out, run, sh, sub_in, sub_out, write
from shish.ir import cmd

# =============================================================================
# Basic Execution
# =============================================================================


async def test_run_exit_code() -> None:
    assert await run(sh.true()) == 0
    assert await run(sh.false()) == 1


async def test_await_cmd() -> None:
    assert await sh.true() == 0
    assert await sh.false() == 1


async def test_single_command_no_args() -> None:
    assert await run(sh.true()) == 0


async def test_command_with_empty_string_arg(tmp_path: Path) -> None:
    out = tmp_path / "out.txt"
    await (sh.echo("") > out)
    assert out.read_text() == "\n"


# =============================================================================
# Pipelines
# =============================================================================


async def test_pipeline_execution() -> None:
    assert await (sh.echo("hello") | sh.cat()) == 0


async def test_pipeline_pipefail() -> None:
    assert await (sh.false() | sh.cat()) != 0


async def test_pipeline_sigpipe() -> None:
    # yes | head -1: yes gets SIGPIPE when head exits
    result = await asyncio.wait_for(sh.yes() | sh.head(n="1"), timeout=2.0)
    assert result in (0, 141)  # head exits 0, yes gets SIGPIPE (128+13)


async def test_pipeline_with_failing_middle_stage() -> None:
    result = await (sh.echo("test") | sh.false() | sh.cat())
    assert result != 0


async def test_pipeline_five_stages() -> None:
    result = await out(
        sh.echo("hello") | sh.cat() | sh.cat() | sh.cat() | sh.cat() | sh.cat()
    )
    assert result == "hello\n"


async def test_pipeline_many_transforms() -> None:
    result = await out(
        sh.echo("hello") | sh.tr("a-z", "A-Z") | sh.tr("H", "J") | sh.rev() | sh.rev()
    )
    assert result == "JELLO\n"


# =============================================================================
# File Redirects (< and >)
# =============================================================================


async def test_redirect_stdout_to_file(tmp_path: Path) -> None:
    out = tmp_path / "out.txt"
    assert await (sh.echo("hello") > out) == 0
    assert out.read_text() == "hello\n"


async def test_redirect_stdout_overwrite(tmp_path: Path) -> None:
    out = tmp_path / "out.txt"
    out.write_text("old content")
    await (sh.echo("new") > out)
    assert out.read_text() == "new\n"


async def test_redirect_stdout_append(tmp_path: Path) -> None:
    out = tmp_path / "out.txt"
    await (sh.echo("first") > out)
    await (sh.echo("second") >> out)
    assert out.read_text() == "first\nsecond\n"


async def test_redirect_stdin_from_file(tmp_path: Path) -> None:
    inp = tmp_path / "in.txt"
    out = tmp_path / "out.txt"
    inp.write_text("hello from file")
    await ((sh.cat() < inp) > out)
    assert out.read_text() == "hello from file"


async def test_redirect_pipeline_stdout() -> None:
    result = await out(sh.echo("hello world") | sh.tr("a-z", "A-Z"))
    assert result == "HELLO WORLD\n"


async def test_redirect_pipeline_stdin(tmp_path: Path) -> None:
    inp = tmp_path / "in.txt"
    inp.write_text("hello\nworld\n")
    result = await out((sh.cat() < inp) | sh.grep("world"))
    assert result == "world\n"


async def test_redirect_chain_in_out(tmp_path: Path) -> None:
    inp = tmp_path / "in.txt"
    out = tmp_path / "out.txt"
    inp.write_text("chained data")
    await ((sh.cat() < inp) > out)
    assert out.read_text() == "chained data"


async def test_redirect_chain_reverse_order(tmp_path: Path) -> None:
    inp = tmp_path / "in.txt"
    out = tmp_path / "out.txt"
    inp.write_text("reversed order")
    await ((sh.cat() > out) < inp)
    assert out.read_text() == "reversed order"


# =============================================================================
# Redirects as Pipeline Stages
# =============================================================================


async def test_redirect_stage_stdout(tmp_path: Path) -> None:
    # (echo "hello" > file) | cat - stdout goes to file, cat gets nothing
    outfile = tmp_path / "out.txt"
    result = await out((sh.echo("hello") > outfile) | sh.cat())
    assert outfile.read_text() == "hello\n"
    assert result == ""


async def test_redirect_stage_stdin(tmp_path: Path) -> None:
    # echo "ignored" | (cat < file) - cat reads from file, not pipe
    inp = tmp_path / "in.txt"
    inp.write_text("from file\n")
    result = await out(sh.echo("ignored") | (sh.cat() < inp))
    assert result == "from file\n"


async def test_redirect_stage_tee_like(tmp_path: Path) -> None:
    # echo "hello" | (cat > log.txt) | cat - middle stage logs to file
    log = tmp_path / "log.txt"
    result = await out(sh.echo("hello") | (sh.cat() > log) | sh.cat())
    assert log.read_text() == "hello\n"
    assert result == ""


# =============================================================================
# Data Redirects (<<)
# =============================================================================


async def test_from_data_string(tmp_path: Path) -> None:
    out = tmp_path / "out.txt"
    await ((sh.cat() << "hello world") > out)
    assert out.read_text() == "hello world"


async def test_from_data_bytes(tmp_path: Path) -> None:
    out = tmp_path / "out.txt"
    await ((sh.cat() << b"binary data") > out)
    assert out.read_bytes() == b"binary data"


async def test_from_data_multiline(tmp_path: Path) -> None:
    out = tmp_path / "out.txt"
    await ((sh.cat() << "line1\nline2\nline3\n") > out)
    assert out.read_text() == "line1\nline2\nline3\n"


async def test_from_data_pipeline() -> None:
    result = await out((sh.cat() << "hello\nworld\n") | sh.grep("world"))
    assert result == "world\n"


async def test_from_data_in_stage() -> None:
    result = await out(sh.echo("ignored") | (sh.cat() << "injected"))
    assert result == "injected"


async def test_data_redirect_then_file_output(tmp_path: Path) -> None:
    out = tmp_path / "out.txt"
    await ((sh.cat() << "heredoc style") > out)
    assert out.read_text() == "heredoc style"


async def test_empty_data_redirect(tmp_path: Path) -> None:
    out = tmp_path / "out.txt"
    await ((sh.cat() << "") > out)
    assert out.read_text() == ""


async def test_binary_data_through_pipeline(tmp_path: Path) -> None:
    out = tmp_path / "out.bin"
    data = bytes(range(256))  # All byte values 0-255
    await ((sh.cat() << data) > out)
    assert out.read_bytes() == data


# =============================================================================
# Process Substitution (sub_in / sub_out)
# =============================================================================


async def test_sub_in_single(tmp_path: Path) -> None:
    out = tmp_path / "out.txt"
    await (sh.cat(sub_in(sh.echo("hello"))) > out)
    assert out.read_text() == "hello\n"


async def test_sub_in_two_sources(tmp_path: Path) -> None:
    file1 = tmp_path / "file1.txt"
    file2 = tmp_path / "file2.txt"
    out = tmp_path / "out.txt"
    file1.write_text("b\na\nc\n")
    file2.write_text("b\na\nc\n")
    result = await (sh.diff(sub_in(sh.sort(file1)), sub_in(sh.sort(file2))) > out)
    assert result == 0
    assert out.read_text() == ""


async def test_sub_in_diff_sources(tmp_path: Path) -> None:
    file1 = tmp_path / "file1.txt"
    file2 = tmp_path / "file2.txt"
    file1.write_text("a\nb\n")
    file2.write_text("a\nc\n")
    result = await sh.diff(sub_in(sh.sort(file1)), sub_in(sh.sort(file2)))
    assert result == 1


async def test_sub_in_with_pipeline(tmp_path: Path) -> None:
    out = tmp_path / "out.txt"
    await (sh.cat(sub_in(sh.echo("hello") | sh.tr("a-z", "A-Z"))) > out)
    assert out.read_text() == "HELLO\n"


async def test_sub_in_with_redirect(tmp_path: Path) -> None:
    inp = tmp_path / "in.txt"
    out = tmp_path / "out.txt"
    inp.write_text("from file\n")
    await (sh.cat(sub_in(sh.cat() < inp)) > out)
    assert out.read_text() == "from file\n"


async def test_sub_in_with_data_redirect(tmp_path: Path) -> None:
    out = tmp_path / "out.txt"
    await (sh.cat(sub_in(sh.cat() << "injected data")) > out)
    assert out.read_text() == "injected data"


async def test_sub_out_single(tmp_path: Path) -> None:
    outfile = tmp_path / "out.txt"
    result = await out(sh.echo("hello") | sh.tee(sub_out(sh.cat() > outfile)))
    assert outfile.read_text() == "hello\n"
    assert result == "hello\n"


async def test_sub_out_multiple(tmp_path: Path) -> None:
    out_a = tmp_path / "a.txt"
    out_b = tmp_path / "b.txt"
    result = await out(
        sh.echo("hello") | sh.tee(sub_out(sh.cat() > out_a), sub_out(sh.cat() > out_b))
    )
    assert out_a.read_text() == "hello\n"
    assert out_b.read_text() == "hello\n"
    assert result == "hello\n"


async def test_sub_out_with_data_redirect(tmp_path: Path) -> None:
    outfile = tmp_path / "out.txt"
    await out(sh.echo("from tee") | sh.tee(sub_out(sh.cat() > outfile)))
    assert outfile.read_text() == "from tee\n"


async def test_mixed_sub_in_and_file_redirect(tmp_path: Path) -> None:
    file1 = tmp_path / "file1.txt"
    file2 = tmp_path / "file2.txt"
    file1.write_text("c\na\nb\n")
    file2.write_text("a\nb\nc\n")
    result = await sh.diff(sub_in(sh.sort(file1)), file2)
    assert result == 0  # sorted file1 == file2


# =============================================================================
# Concurrent Execution
# =============================================================================


async def test_concurrent_runs() -> None:
    results = await asyncio.gather(
        run(sh.echo("a") | sh.cat()),
        run(sh.echo("b") | sh.cat()),
        run(sh.echo("c") | sh.cat()),
    )
    assert results == [0, 0, 0]


async def test_concurrent_runs_with_files(tmp_path: Path) -> None:
    files = [tmp_path / f"out{i}.txt" for i in range(5)]
    await asyncio.gather(
        *[run(sh.echo(f"content{i}") > f) for i, f in enumerate(files)]
    )
    for i, f in enumerate(files):
        assert f.read_text() == f"content{i}\n"


async def test_concurrent_data_writes(tmp_path: Path) -> None:
    files = [tmp_path / f"data{i}.txt" for i in range(3)]
    data = [f"data block {i}" * 1000 for i in range(3)]
    await asyncio.gather(
        *[run((sh.cat() << d) > f) for d, f in zip(data, files, strict=True)]
    )
    for i, f in enumerate(files):
        assert f.read_text() == data[i]


# =============================================================================
# Error Handling
# Note: These raise exceptions rather than shell-style exit codes (127/126).
# =============================================================================


async def test_command_not_found() -> None:
    with pytest.raises(FileNotFoundError):
        await run(sh.nonexistent_command_that_does_not_exist_12345())


async def test_permission_denied(tmp_path: Path) -> None:
    script = tmp_path / "not_executable.sh"
    script.write_text("#!/bin/sh\necho hello")
    script.chmod(0o644)
    with pytest.raises(PermissionError):
        await run(sh(str(script)))


# =============================================================================
# Output Capture (out)
# =============================================================================


async def test_out_simple() -> None:
    result = await out(sh.echo("hello"))
    assert result == "hello\n"


async def test_out_pipeline() -> None:
    result = await out(sh.echo("hello world") | sh.tr("a-z", "A-Z"))
    assert result == "HELLO WORLD\n"


async def test_out_with_data_redirect() -> None:
    result = await out(sh.cat() << "input data")
    assert result == "input data"


async def test_out_with_sub_in() -> None:
    result = await out(sh.cat(sub_in(sh.echo("nested"))))
    assert result == "nested\n"


async def test_out_raises_on_failure() -> None:
    with pytest.raises(subprocess.CalledProcessError) as exc_info:
        await out(sh.false())
    assert exc_info.value.returncode == 1


async def test_out_raises_preserves_output() -> None:
    with pytest.raises(subprocess.CalledProcessError) as exc_info:
        await out(sh.sh("-c", "echo partial; exit 1"))
    assert exc_info.value.output == b"partial\n"


async def test_out_empty() -> None:
    result = await out(sh.cat() << "")
    assert result == ""


async def test_out_binary() -> None:
    data = bytes(range(256))
    result = await out(sh.cat() << data, encoding=None)
    assert result == data


# =============================================================================
# Builder (cmd()) E2E
# =============================================================================


async def test_builder_run() -> None:
    assert await cmd("true").run() == 0
    assert await cmd("false").run() == 1


async def test_builder_pipeline(tmp_path: Path) -> None:
    outfile = tmp_path / "out.txt"
    await cmd("echo", "hello world").pipe(cmd("tr", "a-z", "A-Z")).write(outfile).run()
    assert outfile.read_text() == "HELLO WORLD\n"


async def test_builder_write_and_append(tmp_path: Path) -> None:
    outfile = tmp_path / "out.txt"
    await cmd("echo", "first").write(outfile).run()
    await cmd("echo", "second").write(outfile, append=True).run()
    assert outfile.read_text() == "first\nsecond\n"


async def test_builder_read(tmp_path: Path) -> None:
    inp = tmp_path / "in.txt"
    outfile = tmp_path / "out.txt"
    inp.write_text("hello from file")
    await cmd("cat").read(inp).write(outfile).run()
    assert outfile.read_text() == "hello from file"


async def test_builder_feed(tmp_path: Path) -> None:
    outfile = tmp_path / "out.txt"
    await cmd("cat").feed("hello world").write(outfile).run()
    assert outfile.read_text() == "hello world"


async def test_builder_feed_bytes(tmp_path: Path) -> None:
    outfile = tmp_path / "out.bin"
    data = bytes(range(256))
    await cmd("cat").feed(data).write(outfile).run()
    assert outfile.read_bytes() == data


async def test_builder_sub_in(tmp_path: Path) -> None:
    outfile = tmp_path / "out.txt"
    await cmd("cat", cmd("echo", "hello").sub_in()).write(outfile).run()
    assert outfile.read_text() == "hello\n"


async def test_builder_sub_out(tmp_path: Path) -> None:
    outfile = tmp_path / "out.txt"
    main_out = tmp_path / "main.txt"
    sink = cmd("cat").write(outfile).sub_out()
    await cmd("echo", "hello").pipe(cmd("tee", sink)).write(main_out).run()
    assert outfile.read_text() == "hello\n"
    assert main_out.read_text() == "hello\n"


async def test_builder_out() -> None:
    result = await cmd("echo", "hello").out()
    assert result == "hello\n"


async def test_builder_out_raises_on_failure() -> None:
    with pytest.raises(subprocess.CalledProcessError) as exc_info:
        await cmd("false").out()
    assert exc_info.value.returncode == 1


# =============================================================================
# Tuple fd syntax (DSL-only)
# =============================================================================


async def test_tuple_fd_stderr_to_file(tmp_path: Path) -> None:
    """cmd > (STDERR, "file") — redirect stderr via tuple syntax."""
    errfile = tmp_path / "err.txt"
    result = await out(sh.sh("-c", "echo out; echo err >&2") > (STDERR, errfile))
    assert result == "out\n"
    assert errfile.read_text() == "err\n"


async def test_tuple_fd_append(tmp_path: Path) -> None:
    """cmd >> (STDERR, "file") — append stderr via tuple syntax."""
    errfile = tmp_path / "err.txt"
    await run(sh.sh("-c", "echo first >&2") > (STDERR, errfile))
    await run(sh.sh("-c", "echo second >&2") >> (STDERR, errfile))
    assert errfile.read_text() == "first\nsecond\n"


# =============================================================================
# close() combinator
# =============================================================================


async def test_close_stdin() -> None:
    """close(cmd, STDIN) — cat with stdin closed exits non-zero."""
    result = await run(close(sh.cat(), STDIN))
    assert result == 1


async def test_close_stdout(tmp_path: Path) -> None:
    """close(cmd, STDOUT) — echo with stdout closed, stderr still works."""
    errfile = tmp_path / "err.txt"
    # echo fails when stdout is closed, redirect stderr to verify it ran
    await run(close(sh.sh("-c", "echo err >&2"), STDOUT) > (STDERR, errfile))
    assert errfile.read_text() == "err\n"


# =============================================================================
# write() combinator with subs
# =============================================================================


async def test_write_to_sub_out(tmp_path: Path) -> None:
    """write(cmd, sub_out(...)) — combinator wiring to process substitution."""
    outfile = tmp_path / "out.txt"
    result = await out(write(sh.echo("hello"), sub_out(sh.cat() > outfile)))
    assert outfile.read_text() == "hello\n"
    assert result == ""


# =============================================================================
# Nested process substitution
# =============================================================================


async def test_nested_sub_in() -> None:
    """cat <(cat <(echo hello)) — two levels of sub nesting."""
    result = await out(sh.cat(sub_in(sh.cat(sub_in(sh.echo("hello"))))))
    assert result == "hello\n"


async def test_nested_sub_in_with_transform() -> None:
    """cat <(tr a-z A-Z <(echo hello)) — nested sub with pipeline."""
    inner = sub_in(sh.echo("hello"))
    result = await out(sh.cat(sub_in(sh.cat(inner) | sh.tr("a-z", "A-Z"))))
    assert result == "HELLO\n"


# =============================================================================
# Cancellation via DSL
# =============================================================================


async def test_cancel_await_cmd() -> None:
    """Cancelling await sh.cmd() kills the child."""
    task = asyncio.create_task(run(sh.sleep("60")))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_cancel_pipeline() -> None:
    """Cancelling a DSL pipeline kills all stages."""
    task = asyncio.create_task(run(sh.sleep("60") | sh.sleep("60")))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
