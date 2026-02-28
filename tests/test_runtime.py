"""Runtime tests using IR nodes directly — verifies executor consumes IR correctly."""

import asyncio
import subprocess
from pathlib import Path

import pytest

from shish import ir
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
    node = ir.Pipeline(
        (
            ir.Cmd(("echo", "hello")),
            ir.Cmd(("cat",)),
        )
    )
    assert await run(node) == 0


async def test_pipeline_output() -> None:
    node = ir.Pipeline(
        (
            ir.Cmd(("echo", "hello world")),
            ir.Cmd(("tr", "a-z", "A-Z")),
        )
    )
    result = await out(node)
    assert result == "HELLO WORLD\n"


async def test_pipeline_pipefail() -> None:
    node = ir.Pipeline(
        (
            ir.Cmd(("false",)),
            ir.Cmd(("cat",)),
        )
    )
    assert await run(node) != 0


async def test_pipeline_pipefail_rightmost() -> None:
    """Pipefail returns rightmost non-zero exit code."""
    node = ir.Pipeline(
        (
            ir.Cmd(("sh", "-c", "exit 1")),
            ir.Cmd(("sh", "-c", "exit 0")),
            ir.Cmd(("sh", "-c", "exit 2")),
        )
    )
    assert await run(node) == 2


async def test_pipeline_three_stages() -> None:
    node = ir.Pipeline(
        (
            ir.Cmd(("echo", "hello")),
            ir.Cmd(("tr", "a-z", "A-Z")),
            ir.Cmd(("tr", "H", "J")),
        )
    )
    result = await out(node)
    assert result == "JELLO\n"


async def test_pipeline_sigpipe() -> None:
    node = ir.Pipeline(
        (
            ir.Cmd(("yes",)),
            ir.Cmd(("head", "-n", "1")),
        )
    )
    result = await asyncio.wait_for(run(node), timeout=2.0)
    assert result in (0, 141)


# =============================================================================
# File Redirects (on Cmd)
# =============================================================================


async def test_cmd_stdout_redirect(tmp_path: Path) -> None:
    outfile = tmp_path / "out.txt"
    node = ir.Cmd(("echo", "hello"), redirects=(ir.FdToFile(1, outfile),))
    assert await run(node) == 0
    assert outfile.read_text() == "hello\n"


async def test_cmd_stdout_append(tmp_path: Path) -> None:
    outfile = tmp_path / "out.txt"
    await run(ir.Cmd(("echo", "first"), redirects=(ir.FdToFile(1, outfile),)))
    await run(
        ir.Cmd(
            ("echo", "second"),
            redirects=(ir.FdToFile(1, outfile, append=True),),
        )
    )
    assert outfile.read_text() == "first\nsecond\n"


async def test_cmd_stdin_file(tmp_path: Path) -> None:
    inp = tmp_path / "in.txt"
    outfile = tmp_path / "out.txt"
    inp.write_text("hello from file")
    node = ir.Cmd(
        ("cat",),
        redirects=(
            ir.FdFromFile(0, inp),
            ir.FdToFile(1, outfile),
        ),
    )
    assert await run(node) == 0
    assert outfile.read_text() == "hello from file"


async def test_cmd_stdin_data(tmp_path: Path) -> None:
    outfile = tmp_path / "out.txt"
    node = ir.Cmd(
        ("cat",),
        redirects=(
            ir.FdFromData(0, "hello world"),
            ir.FdToFile(1, outfile),
        ),
    )
    assert await run(node) == 0
    assert outfile.read_text() == "hello world"


async def test_cmd_stdin_data_bytes(tmp_path: Path) -> None:
    outfile = tmp_path / "out.bin"
    data = bytes(range(256))
    node = ir.Cmd(
        ("cat",),
        redirects=(
            ir.FdFromData(0, data),
            ir.FdToFile(1, outfile),
        ),
    )
    assert await run(node) == 0
    assert outfile.read_bytes() == data


# =============================================================================
# Pipeline with per-stage redirects
# =============================================================================


async def test_pipeline_first_stage_stdin(tmp_path: Path) -> None:
    inp = tmp_path / "in.txt"
    inp.write_text("hello\nworld\n")
    node = ir.Pipeline(
        (
            ir.Cmd(("cat",), redirects=(ir.FdFromFile(0, inp),)),
            ir.Cmd(("grep", "world")),
        )
    )
    result = await out(node)
    assert result == "world\n"


async def test_pipeline_stage_stdout_override(tmp_path: Path) -> None:
    # (echo "hello" > file) | cat — stdout goes to file, cat gets nothing
    outfile = tmp_path / "out.txt"
    result_file = tmp_path / "result.txt"
    node = ir.Pipeline(
        (
            ir.Cmd(("echo", "hello"), redirects=(ir.FdToFile(1, outfile),)),
            ir.Cmd(("cat",), redirects=(ir.FdToFile(1, result_file),)),
        )
    )
    assert await run(node) == 0
    assert outfile.read_text() == "hello\n"
    assert result_file.read_text() == ""


# =============================================================================
# Fd-to-fd redirects (2>&1, etc.)
# =============================================================================


async def test_fd_to_fd_stderr_to_stdout() -> None:
    """cmd 2>&1 — stderr merges into stdout."""
    node = ir.Cmd(
        ("sh", "-c", "echo out; echo err >&2"),
        redirects=(ir.FdToFd(1, 2),),
    )
    result = await out(node)
    assert "out\n" in result
    assert "err\n" in result


async def test_fd_to_fd_file_then_dup(tmp_path: Path) -> None:
    """cmd > file 2>&1 — both stdout and stderr go to file."""
    outfile = tmp_path / "out.txt"
    node = ir.Cmd(
        ("sh", "-c", "echo out; echo err >&2"),
        redirects=(
            ir.FdToFile(1, outfile),
            ir.FdToFd(1, 2),
        ),
    )
    assert await run(node) == 0
    content = outfile.read_text()
    assert "out\n" in content
    assert "err\n" in content


async def test_fd_to_fd_order_matters(tmp_path: Path) -> None:
    """cmd 2>&1 > file — stderr goes to original stdout, only stdout goes to file.

    Order matters: 2>&1 copies current fd 1 (pipe/terminal) to fd 2,
    then > file redirects fd 1 to file. So stderr goes to pipe, stdout to file.
    """
    outfile = tmp_path / "out.txt"
    node = ir.Cmd(
        ("sh", "-c", "echo out; echo err >&2"),
        redirects=(
            ir.FdToFd(1, 2),
            ir.FdToFile(1, outfile),
        ),
    )
    # Capture what comes through the pipeline stdout (which is stderr after dup)
    result = await out(node)
    assert result == "err\n"
    assert outfile.read_text() == "out\n"


async def test_fd_to_file_stderr(tmp_path: Path) -> None:
    """cmd 2> file — stderr to file, stdout to pipeline."""
    errfile = tmp_path / "err.txt"
    node = ir.Cmd(
        ("sh", "-c", "echo out; echo err >&2"),
        redirects=(ir.FdToFile(2, errfile),),
    )
    result = await out(node)
    assert result == "out\n"
    assert errfile.read_text() == "err\n"


async def test_fd_to_file_arbitrary_fd(tmp_path: Path) -> None:
    """cmd 3> file — redirect fd 3 to a file."""
    outfile = tmp_path / "out.txt"
    node = ir.Cmd(
        ("sh", "-c", "echo hello >&3"),
        redirects=(ir.FdToFile(3, outfile),),
    )
    assert await run(node) == 0
    assert outfile.read_text() == "hello\n"


# =============================================================================
# FdClose
# =============================================================================


async def test_fd_close() -> None:
    """Closing stdin causes cat to fail with EBADF."""
    node = ir.Cmd(
        ("cat",),
        redirects=(ir.FdClose(0),),
    )
    assert await run(node) == 1


# =============================================================================
# Process Substitution
# =============================================================================


async def test_sub_from(tmp_path: Path) -> None:
    outfile = tmp_path / "out.txt"
    sub = ir.Sub(ir.Cmd(("echo", "hello")), write=False)
    node = ir.Cmd(("cat", sub), redirects=(ir.FdToFile(1, outfile),))
    assert await run(node) == 0
    assert outfile.read_text() == "hello\n"


async def test_sub_from_two_sources(tmp_path: Path) -> None:
    file1 = tmp_path / "file1.txt"
    file2 = tmp_path / "file2.txt"
    outfile = tmp_path / "out.txt"
    file1.write_text("b\na\nc\n")
    file2.write_text("b\na\nc\n")
    sub1 = ir.Sub(ir.Cmd(("sort", str(file1))), write=False)
    sub2 = ir.Sub(ir.Cmd(("sort", str(file2))), write=False)
    node = ir.Cmd(("diff", sub1, sub2), redirects=(ir.FdToFile(1, outfile),))
    result = await run(node)
    assert result == 0
    assert outfile.read_text() == ""


async def test_sub_to(tmp_path: Path) -> None:
    outfile = tmp_path / "out.txt"
    main_out = tmp_path / "main.txt"
    sink = ir.Sub(
        ir.Cmd(("cat",), redirects=(ir.FdToFile(1, outfile),)),
        write=True,
    )
    node = ir.Pipeline(
        (
            ir.Cmd(("echo", "hello")),
            ir.Cmd(("tee", sink), redirects=(ir.FdToFile(1, main_out),)),
        )
    )
    assert await run(node) == 0
    assert outfile.read_text() == "hello\n"
    assert main_out.read_text() == "hello\n"


async def test_sub_from_with_pipeline(tmp_path: Path) -> None:
    outfile = tmp_path / "out.txt"
    sub = ir.Sub(
        ir.Pipeline(
            (
                ir.Cmd(("echo", "hello")),
                ir.Cmd(("tr", "a-z", "A-Z")),
            )
        ),
        write=False,
    )
    node = ir.Cmd(("cat", sub), redirects=(ir.FdToFile(1, outfile),))
    assert await run(node) == 0
    assert outfile.read_text() == "HELLO\n"


async def test_fd_from_sub_stdin() -> None:
    """cat < <(echo hello) — redirect stdin from process substitution."""
    sub = ir.Sub(ir.Cmd(("echo", "hello")), write=False)
    node = ir.Cmd(("cat",), redirects=(ir.FdFromSub(0, sub),))
    result = await out(node)
    assert result == "hello\n"


async def test_fd_from_sub_arbitrary_fd(tmp_path: Path) -> None:
    """cmd 3< <(echo hello) — redirect fd 3 from process substitution."""
    outfile = tmp_path / "out.txt"
    sub = ir.Sub(ir.Cmd(("echo", "hello")), write=False)
    node = ir.Cmd(
        ("sh", "-c", "cat <&3"),
        redirects=(
            ir.FdFromSub(3, sub),
            ir.FdToFile(1, outfile),
        ),
    )
    assert await run(node) == 0
    assert outfile.read_text() == "hello\n"


async def test_fd_to_sub_stdout(tmp_path: Path) -> None:
    """cmd 1> >(cat > file) — redirect stdout to process substitution."""
    outfile = tmp_path / "out.txt"
    sub = ir.Sub(
        ir.Cmd(("cat",), redirects=(ir.FdToFile(1, outfile),)),
        write=True,
    )
    node = ir.Cmd(("echo", "hello"), redirects=(ir.FdToSub(1, sub),))
    assert await run(node) == 0
    assert outfile.read_text() == "hello\n"


async def test_fd_to_sub_arbitrary_fd(tmp_path: Path) -> None:
    """cmd 3> >(cat > file) — redirect fd 3 to process substitution."""
    outfile = tmp_path / "out.txt"
    sub = ir.Sub(
        ir.Cmd(("cat",), redirects=(ir.FdToFile(1, outfile),)),
        write=True,
    )
    node = ir.Cmd(
        ("sh", "-c", "echo hello >&3"),
        redirects=(ir.FdToSub(3, sub),),
    )
    assert await run(node) == 0
    assert outfile.read_text() == "hello\n"


async def test_fd_from_sub_with_pipeline() -> None:
    """cat < <(echo hello | tr a-z A-Z) — sub contains a pipeline."""
    sub = ir.Sub(
        ir.Pipeline(
            (
                ir.Cmd(("echo", "hello")),
                ir.Cmd(("tr", "a-z", "A-Z")),
            )
        ),
        write=False,
    )
    node = ir.Cmd(("cat",), redirects=(ir.FdFromSub(0, sub),))
    result = await out(node)
    assert result == "HELLO\n"


async def test_sub_exit_code_ignored_arg() -> None:
    """cat <(false) — sub failure doesn't affect main exit code."""
    sub = ir.Sub(ir.Cmd(("false",)), write=False)
    node = ir.Cmd(("cat", sub))
    assert await run(node) == 0


async def test_sub_exit_code_ignored_redirect() -> None:
    """true 1> >(false) — redirect sub failure doesn't affect exit code."""
    sub = ir.Sub(ir.Cmd(("false",)), write=True)
    node = ir.Cmd(("true",), redirects=(ir.FdToSub(1, sub),))
    assert await run(node) == 0


async def test_sub_exit_code_ignored_from_redirect() -> None:
    """cat < <(false) — input sub failure doesn't affect exit code."""
    sub = ir.Sub(ir.Cmd(("false",)), write=False)
    node = ir.Cmd(("cat",), redirects=(ir.FdFromSub(0, sub),))
    assert await run(node) == 0


async def test_sub_from_with_data_stdin(tmp_path: Path) -> None:
    outfile = tmp_path / "out.txt"
    sub = ir.Sub(
        ir.Cmd(("cat",), redirects=(ir.FdFromData(0, "injected data"),)),
        write=False,
    )
    node = ir.Cmd(("cat", sub), redirects=(ir.FdToFile(1, outfile),))
    assert await run(node) == 0
    assert outfile.read_text() == "injected data"


# =============================================================================
# Output Capture
# =============================================================================


async def test_out_simple() -> None:
    result = await out(ir.Cmd(("echo", "hello")))
    assert result == "hello\n"


async def test_out_pipeline() -> None:
    node = ir.Pipeline(
        (
            ir.Cmd(("echo", "hello world")),
            ir.Cmd(("tr", "a-z", "A-Z")),
        )
    )
    result = await out(node)
    assert result == "HELLO WORLD\n"


async def test_out_with_data() -> None:
    node = ir.Cmd(("cat",), redirects=(ir.FdFromData(0, "input data"),))
    result = await out(node)
    assert result == "input data"


async def test_out_binary() -> None:
    data = bytes(range(256))
    node = ir.Cmd(("cat",), redirects=(ir.FdFromData(0, data),))
    result = await out(node, encoding=None)
    assert result == data


async def test_out_raises_on_failure() -> None:
    with pytest.raises(subprocess.CalledProcessError) as exc_info:
        await out(ir.Cmd(("false",)))
    assert exc_info.value.returncode == 1


async def test_out_raises_preserves_output() -> None:
    with pytest.raises(subprocess.CalledProcessError) as exc_info:
        await out(ir.Cmd(("sh", "-c", "echo partial; exit 1")))
    assert exc_info.value.output == b"partial\n"


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
                    redirects=(ir.FdToFile(1, fpath),),
                )
            )
            for idx, fpath in enumerate(files)
        ]
    )
    for idx, fpath in enumerate(files):
        assert fpath.read_text() == f"content{idx}\n"


# =============================================================================
# Factory functions used by runtime
# =============================================================================


async def test_pipeline_factory_execution() -> None:
    node = ir.pipeline(
        ir.Cmd(("echo", "hello")),
        ir.Cmd(("tr", "a-z", "A-Z")),
    )
    result = await out(node)
    assert result == "HELLO\n"
