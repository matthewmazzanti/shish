"""Benchmark: buffered writer variants vs C stdio.

Compares:
  bytes.write()  — C stdio buffered (the ceiling)
  py awaitable   — Python WriteAwaitable, no lock, no async frames
  c awaitable    — C WriteAwaitable via _cbufwriter

Usage:
    python benchmarks/bench_bufwriter.py
"""

from __future__ import annotations

import asyncio
import os
import random
import subprocess
import sys
import time

from shish._cbufwriter import ByteWriteStream as CBufWriter
from tabulate import tabulate

from shish._bufwriter_py import ByteWriteStream as PyBufWriter
from shish.fd import Fd

WRITE_SIZES: list[int] = [
    32,
    64,
    128,
    256,
    512,
    1_024,
    2_048,
    4_096,
    8_192,
    16_384,
    32_768,
    65_536,
]

DEVNULL_WRITES = 100_000
PIPE_WRITES = 10_000


def format_size(nbytes: int) -> str:
    """Format byte count with binary units."""
    if nbytes >= 1 << 10:
        value = nbytes / (1 << 10)
        return f"{int(value)} KiB" if value == int(value) else f"{value:.1f} KiB"
    return f"{nbytes} B"


def format_rate(nbytes: int, elapsed: float) -> str:
    """Format throughput."""
    bps = nbytes / elapsed if elapsed > 0 else float("inf")
    if bps >= 1 << 30:
        return f"{bps / (1 << 30):.1f} GiB/s"
    if bps >= 1 << 20:
        return f"{bps / (1 << 20):.0f} MiB/s"
    return f"{bps / (1 << 10):.0f} KiB/s"


def format_duration(seconds: float) -> str:
    """Format duration for display."""
    if seconds >= 1e-3:
        return f"{seconds * 1e3:.1f} ms"
    if seconds >= 1e-6:
        return f"{seconds * 1e6:.1f} \u03bcs"
    return f"{seconds * 1e9:.0f} ns"


def open_devnull() -> Fd:
    """Open /dev/null for writing."""
    return Fd(os.open("/dev/null", os.O_WRONLY))


def make_pipe() -> tuple[Fd, Fd]:
    """Create a pipe."""
    read_raw, write_raw = os.pipe()
    return Fd(read_raw), Fd(write_raw)


def spawn_sink(read_fd: Fd) -> subprocess.Popen[bytes]:
    """Spawn cat > /dev/null."""
    proc = subprocess.Popen(["cat"], stdin=read_fd.fd, stdout=subprocess.DEVNULL)
    read_fd.close()
    return proc


# ── Timed loops ──


def timed_bytes_write(write_fd: Fd, chunk: bytes, writes: int) -> tuple[float, int]:
    """Time file.buffer.write() — C stdio."""
    pipe_file = os.fdopen(write_fd.fd, "wb", closefd=False)
    chunk_len = len(chunk)
    start = time.perf_counter()
    for _ in range(writes):
        pipe_file.write(chunk)
    pipe_file.flush()
    elapsed = time.perf_counter() - start
    pipe_file.close()
    write_fd.close()
    return elapsed, writes * chunk_len


async def timed_py_buf(write_fd: Fd, chunk: bytes, writes: int) -> tuple[float, int]:
    """Time Python awaitable buffered writer."""
    stream = PyBufWriter.from_fd(write_fd)
    chunk_len = len(chunk)
    start = time.perf_counter()
    for _ in range(writes):
        await stream.write(chunk)
    await stream.close()
    elapsed = time.perf_counter() - start
    return elapsed, writes * chunk_len


async def timed_c_buf(write_fd: Fd, chunk: bytes, writes: int) -> tuple[float, int]:
    """Time C awaitable buffered writer."""
    loop = asyncio.get_running_loop()
    stream = CBufWriter(write_fd)
    chunk_len = len(chunk)
    start = time.perf_counter()
    for _ in range(writes):
        await stream.write(chunk)
    await stream.flush()
    elapsed = time.perf_counter() - start
    loop.remove_writer(write_fd.fd)
    write_fd.close()
    return elapsed, writes * chunk_len


# ── Part 1: /dev/null overhead ──


async def run_devnull() -> None:
    """Benchmark per-call overhead."""
    rows: list[list[str]] = []

    for write_size in WRITE_SIZES:
        chunk = random.randbytes(write_size)

        stdio_elapsed, _ = timed_bytes_write(open_devnull(), chunk, DEVNULL_WRITES)
        py_elapsed, _ = await timed_py_buf(open_devnull(), chunk, DEVNULL_WRITES)
        c_elapsed, _ = await timed_c_buf(open_devnull(), chunk, DEVNULL_WRITES)

        rows.append(
            [
                format_size(write_size),
                format_duration(stdio_elapsed / DEVNULL_WRITES),
                format_duration(py_elapsed / DEVNULL_WRITES),
                format_duration(c_elapsed / DEVNULL_WRITES),
            ]
        )
        print(f"  {format_size(write_size):>10} done", file=sys.stderr, flush=True)

    print(
        f"\nPart 1: Per-call overhead — /dev/null ({DEVNULL_WRITES:,} writes)",
        file=sys.stderr,
    )
    print(file=sys.stderr)
    print(
        tabulate(
            rows,
            headers=[
                "Write Size",
                "bytes.write()",
                "py awaitable",
                "c awaitable",
            ],
            colalign=("right", "right", "right", "right"),
        ),
        file=sys.stderr,
    )
    print(file=sys.stderr)


# ── Part 2: Pipe throughput ──


async def run_pipe() -> None:
    """Benchmark pipe throughput."""
    rows: list[list[str]] = []

    for write_size in WRITE_SIZES:
        chunk = random.randbytes(write_size)

        read_fd, write_fd = make_pipe()
        proc = spawn_sink(read_fd)
        stdio_elapsed, stdio_total = timed_bytes_write(write_fd, chunk, PIPE_WRITES)
        proc.wait()

        read_fd, write_fd = make_pipe()
        proc = spawn_sink(read_fd)
        py_elapsed, py_total = await timed_py_buf(write_fd, chunk, PIPE_WRITES)
        proc.wait()

        read_fd, write_fd = make_pipe()
        proc = spawn_sink(read_fd)
        c_elapsed, c_total = await timed_c_buf(write_fd, chunk, PIPE_WRITES)
        proc.wait()

        rows.append(
            [
                format_size(write_size),
                format_rate(stdio_total, stdio_elapsed),
                format_rate(py_total, py_elapsed),
                format_rate(c_total, c_elapsed),
            ]
        )
        print(f"  {format_size(write_size):>10} done", file=sys.stderr, flush=True)

    print(
        f"\nPart 2: Pipe throughput — cat > /dev/null ({PIPE_WRITES:,} writes)",
        file=sys.stderr,
    )
    print(file=sys.stderr)
    print(
        tabulate(
            rows,
            headers=[
                "Write Size",
                "bytes.write()",
                "py awaitable",
                "c awaitable",
            ],
            colalign=("right", "right", "right", "right"),
        ),
        file=sys.stderr,
    )
    print(file=sys.stderr)


async def main() -> None:
    """Run benchmarks."""
    await run_devnull()
    await run_pipe()


if __name__ == "__main__":
    asyncio.run(main())
