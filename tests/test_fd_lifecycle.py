"""Parent-process fd lifecycle tests.

Verify that fds are correctly allocated and cleaned up during
execution — no leaks, no unexpected fds.
"""

import asyncio
import sys

from shish.builders import Fn, cmd, pipeline
from shish.fn_stage import ByteStageCtx
from shish.runtime import start
from tests.core import process_fds

# asyncio uses pidfd on Linux to monitor children (1 fd per child) when
# available; falls back to threaded watcher (0 fds) otherwise.
# macOS uses kqueue + SIGCHLD (0 fds per child).


def _pidfd_per_child() -> int:
    if sys.platform != "linux":
        return 0
    try:
        # Guarded: only exists on Linux, may be removed in future Python versions.
        from asyncio.unix_events import (  # noqa: PLC0415
            can_use_pidfd,  # type: ignore[attr-defined]
        )

        return 1 if can_use_pidfd() else 0
    except (ImportError, AttributeError):
        return 0


PIDFD_PER_CHILD = _pidfd_per_child()


async def test_cmd_no_pipe() -> None:
    """Cmd with inherited stdio has no shish-allocated fds."""
    baseline = process_fds()

    async with start(cmd("sleep", "10")) as execution:
        during = process_fds()
        execution.kill()
        await execution.wait()

    assert len(during - baseline) == 1 * PIDFD_PER_CHILD
    # no leaks after exit
    assert process_fds() == baseline


async def test_two_cmds_no_pipe() -> None:
    """Two concurrent cmds: 2 pidfds during, none after."""
    baseline = process_fds()

    async with (
        start(cmd("sleep", "10")) as ex1,
        start(cmd("sleep", "10")) as ex2,
    ):
        during = process_fds()
        ex1.kill()
        ex2.kill()
        await ex1.wait()
        await ex2.wait()

    assert len(during - baseline) == 2 * PIDFD_PER_CHILD
    assert process_fds() == baseline


async def test_pipeline_no_pipe() -> None:
    """Pipeline with inherited stdio: 1 pidfd per stage, none after."""
    baseline = process_fds()

    async with start(
        pipeline(
            cmd("sleep", "10"),
            cmd("sleep", "10"),
            cmd("sleep", "10"),
        )
    ) as execution:
        during = process_fds()
        execution.kill()
        await execution.wait()

    assert len(during - baseline) == 3 * PIDFD_PER_CHILD
    assert process_fds() == baseline


async def test_fn_no_pipe() -> None:
    """Fn with inherited stdio: 3 dup'd fds during, none after."""
    baseline = process_fds()
    done = asyncio.Event()

    async def wait_fn(ctx: ByteStageCtx) -> int:
        await done.wait()
        return 0

    async with start(Fn(wait_fn)) as execution:
        during = process_fds()
        done.set()
        await execution.wait()

    # 3 dup'd fds (stdin, stdout, stderr)
    assert len(during - baseline) == 3
    assert process_fds() == baseline


async def test_two_fns_no_pipe() -> None:
    """Two concurrent fns: 6 dup'd fds during, none after."""
    baseline = process_fds()
    done = asyncio.Event()

    async def wait_fn(ctx: ByteStageCtx) -> int:
        await done.wait()
        return 0

    async with (
        start(Fn(wait_fn)) as ex1,
        start(Fn(wait_fn)) as ex2,
    ):
        during = process_fds()
        done.set()
        await ex1.wait()
        await ex2.wait()

    assert len(during - baseline) == 6
    assert process_fds() == baseline


async def test_fn_pipeline_no_pipe() -> None:
    """Pipeline of fns: 3 dup'd fds per stage, none after."""
    baseline = process_fds()
    done = asyncio.Event()

    async def wait_fn(ctx: ByteStageCtx) -> int:
        await done.wait()
        return 0

    async with start(
        pipeline(
            Fn(wait_fn),
            Fn(wait_fn),
            Fn(wait_fn),
        )
    ) as execution:
        during = process_fds()
        done.set()
        await execution.wait()

    # 3 fns x 3 dup'd fds each
    assert len(during - baseline) == 9
    assert process_fds() == baseline


async def test_cmd_with_sub_in_redirect() -> None:
    """FdFromSub: 2 procs (main + sub), pipe fds cleaned up."""
    baseline = process_fds()

    sub = cmd("sleep", "10").sub_in()
    command = cmd("sleep", "10").read(sub, fd=3)
    async with start(command) as execution:
        during = process_fds()
        execution.kill()
        await execution.wait()

    assert len(during - baseline) == 2 * PIDFD_PER_CHILD
    assert process_fds() == baseline


async def test_cmd_with_sub_out_redirect() -> None:
    """FdToSub: 2 procs (main + sub), pipe fds cleaned up."""
    baseline = process_fds()

    sub = cmd("sleep", "10").sub_out()
    command = cmd("sleep", "10").write(sub, fd=3)
    async with start(command) as execution:
        during = process_fds()
        execution.kill()
        await execution.wait()

    assert len(during - baseline) == 2 * PIDFD_PER_CHILD
    assert process_fds() == baseline


async def test_cmd_with_sub_in_arg() -> None:
    """SubIn arg: 2 procs (main + sub), pipe fds cleaned up."""
    baseline = process_fds()

    sub = cmd("sleep", "10").sub_in()
    command = cmd("sleep", "10", sub)
    async with start(command) as execution:
        during = process_fds()
        execution.kill()
        await execution.wait()

    assert len(during - baseline) == 2 * PIDFD_PER_CHILD
    assert process_fds() == baseline


async def test_cmd_with_sub_out_arg() -> None:
    """SubOut arg: 2 procs (main + sub), pipe fds cleaned up."""
    baseline = process_fds()

    sub = cmd("sleep", "10").sub_out()
    command = cmd("sleep", "10", sub)
    async with start(command) as execution:
        during = process_fds()
        execution.kill()
        await execution.wait()

    assert len(during - baseline) == 2 * PIDFD_PER_CHILD
    assert process_fds() == baseline
