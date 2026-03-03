"""Shared fixtures for shish tests."""

import os
import subprocess
from collections.abc import Generator
from pathlib import Path

import pluggy
import pytest


@pytest.fixture(scope="session")
def list_fds_bin(tmp_path_factory: pytest.TempPathFactory) -> str:
    """Compile _list_fds.c to a temporary binary (once per test session)."""
    src = Path(__file__).parent / "_list_fds.c"
    binary = tmp_path_factory.mktemp("bin") / "list_fds"
    subprocess.check_call(["cc", "-Wall", "-Wextra", "-o", str(binary), str(src)])
    return str(binary)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_call(
    item: pytest.Item,
) -> Generator[None, pluggy.Result[None]]:
    """Detect fd leaks in every test.

    Snapshots /dev/fd before and after the call phase. Any new fds that
    appear are reported as a test FAILURE (not a teardown error) via
    pluggy's force_exception, so the leak is attributed to the test itself.
    """
    before = set(os.listdir("/dev/fd"))
    outcome = yield
    after = set(os.listdir("/dev/fd"))
    leaked = after - before
    if leaked and outcome.exception is None:
        outcome.force_exception(
            AssertionError(f"Leaked fds: {sorted(int(fd) for fd in leaked)}")
        )
