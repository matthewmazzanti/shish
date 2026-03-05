"""Shared fixtures for shish tests."""

from collections.abc import Generator
from pathlib import Path

import pluggy
import pytest

from tests.core import process_fds


@pytest.fixture(scope="session")
def list_fds_bin() -> str:
    """Return path to the _list_fds.py executable."""
    return str(Path(__file__).parent / "_list_fds.py")


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_call(
    item: pytest.Item,
) -> Generator[None, pluggy.Result[None]]:
    """Detect fd leaks in every test.

    Snapshots /dev/fd before and after the call phase. Any new fds that
    appear are reported as a test FAILURE (not a teardown error) via
    pluggy's force_exception, so the leak is attributed to the test itself.
    """
    before = process_fds()
    outcome = yield
    after = process_fds()
    leaked = after - before
    if leaked and outcome.exception is None:
        outcome.force_exception(AssertionError(f"Leaked fds: {sorted(leaked)}"))
