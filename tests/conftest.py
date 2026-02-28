"""Shared fixtures for shish tests."""

import subprocess
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def list_fds_bin(tmp_path_factory: pytest.TempPathFactory) -> str:
    """Compile _list_fds.c to a temporary binary (once per test session)."""
    src = Path(__file__).parent / "_list_fds.c"
    binary = tmp_path_factory.mktemp("bin") / "list_fds"
    subprocess.check_call(["cc", "-Wall", "-Wextra", "-o", str(binary), str(src)])
    return str(binary)
