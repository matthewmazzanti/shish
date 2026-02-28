import asyncio
import subprocess
from pathlib import Path

import pytest

from shish import out, run, sh, sub_in, sub_out

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


async def test_pipeline_five_stages(tmp_path: Path) -> None:
    out = tmp_path / "out.txt"
    result = await (
        (sh.echo("hello") | sh.cat() | sh.cat() | sh.cat() | sh.cat() | sh.cat()) > out
    )
    assert result == 0
    assert out.read_text() == "hello\n"


async def test_pipeline_many_transforms(tmp_path: Path) -> None:
    out = tmp_path / "out.txt"
    result = await (
        (sh.echo("hello") | sh.tr("a-z", "A-Z") | sh.tr("H", "J") | sh.rev() | sh.rev())
        > out
    )
    assert result == 0
    assert out.read_text() == "JELLO\n"


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


async def test_redirect_pipeline_stdout(tmp_path: Path) -> None:
    out = tmp_path / "out.txt"
    await ((sh.echo("hello world") | sh.tr("a-z", "A-Z")) > out)
    assert out.read_text() == "HELLO WORLD\n"


async def test_redirect_pipeline_stdin(tmp_path: Path) -> None:
    inp = tmp_path / "in.txt"
    out = tmp_path / "out.txt"
    inp.write_text("hello\nworld\n")
    await (((sh.cat() | sh.grep("world")) < inp) > out)
    assert out.read_text() == "world\n"


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
    out = tmp_path / "out.txt"
    result = tmp_path / "result.txt"
    await (((sh.echo("hello") > out) | sh.cat()) > result)
    assert out.read_text() == "hello\n"
    assert result.read_text() == ""


async def test_redirect_stage_stdin(tmp_path: Path) -> None:
    # echo "ignored" | (cat < file) - cat reads from file, not pipe
    inp = tmp_path / "in.txt"
    out = tmp_path / "out.txt"
    inp.write_text("from file\n")
    await ((sh.echo("ignored") | (sh.cat() < inp)) > out)
    assert out.read_text() == "from file\n"


async def test_redirect_stage_tee_like(tmp_path: Path) -> None:
    # echo "hello" | (cat > log.txt) | cat - middle stage logs to file
    log = tmp_path / "log.txt"
    out = tmp_path / "out.txt"
    await (((sh.echo("hello") | (sh.cat() > log)) | sh.cat()) > out)
    assert log.read_text() == "hello\n"
    assert out.read_text() == ""


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


async def test_from_data_pipeline(tmp_path: Path) -> None:
    out = tmp_path / "out.txt"
    await (((sh.cat() << "hello\nworld\n") | sh.grep("world")) > out)
    assert out.read_text() == "world\n"


async def test_from_data_in_stage(tmp_path: Path) -> None:
    out = tmp_path / "out.txt"
    await ((sh.echo("ignored") | (sh.cat() << "injected")) > out)
    assert out.read_text() == "injected"


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
    out = tmp_path / "out.txt"
    main_out = tmp_path / "main.txt"
    await ((sh.echo("hello") | sh.tee(sub_out(sh.cat() > out))) > main_out)
    assert out.read_text() == "hello\n"
    assert main_out.read_text() == "hello\n"


async def test_sub_out_multiple(tmp_path: Path) -> None:
    out_a = tmp_path / "a.txt"
    out_b = tmp_path / "b.txt"
    main_out = tmp_path / "main.txt"
    await (
        (
            sh.echo("hello")
            | sh.tee(sub_out(sh.cat() > out_a), sub_out(sh.cat() > out_b))
        )
        > main_out
    )
    assert out_a.read_text() == "hello\n"
    assert out_b.read_text() == "hello\n"
    assert main_out.read_text() == "hello\n"


async def test_sub_out_with_data_redirect(tmp_path: Path) -> None:
    out = tmp_path / "out.txt"
    main_out = tmp_path / "main.txt"
    await ((sh.echo("from tee") | sh.tee(sub_out(sh.cat() > out))) > main_out)
    assert out.read_text() == "from tee\n"


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
