"""Shared fixtures. The pipeline scripts use relative paths ("data", "puma_dtc.db"), so the
fixture builds a complete Bronze/Silver/Gold database from synthetic data inside a temp
directory and hands the tests a path to it.

The layout is discovered, not assumed: it works with a flat project (ingest.py next to app/ and
agent/) and with the src/ layout (src/bronze, src/silver, src/gold, src/agent, src/dashboard).
"""
import contextlib
import os
import shutil
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CODE_DIRS = [os.path.join(ROOT, *parts) for parts in (
    (), ("src",), ("src", "bronze"), ("src", "silver"), ("src", "gold"),
    ("bronze",), ("silver",), ("gold",))]
for path in [*CODE_DIRS, os.path.join(ROOT, "tests")]:
    if os.path.isdir(path) and path not in sys.path:
        sys.path.insert(0, path)

APP_PATH = next((p for p in (os.path.join(ROOT, "src", "dashboard", "app.py"),
                             os.path.join(ROOT, "app", "app.py")) if os.path.exists(p)), None)


@contextlib.contextmanager
def working_dir(path):
    previous = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous)


@pytest.fixture(scope="session")
def built_db(tmp_path_factory):
    """Run Bronze -> Silver -> Gold once on synthetic data and return the database path."""
    import make_synthetic_data
    from analytics import build_gold
    from ingest import ingest_bronze
    from transform import transform_silver

    work = tmp_path_factory.mktemp("pipeline")
    make_synthetic_data.make(os.path.join(work, "data"), n_transactions=150_000)
    with working_dir(work):
        ingest_bronze()
        transform_silver()
        build_gold()
    return os.path.join(work, "puma_dtc.db")


@pytest.fixture()
def db_copy(built_db, tmp_path):
    """A throw-away copy of the built database that a test may damage."""
    target = tmp_path / "copy.db"
    shutil.copy(built_db, target)
    return str(target)
