"""The deployment path: the snapshot database, startup.py, and the Railway config.

A deployed container has no database and no source data. These tests prove that the snapshot holds what the
dashboard and the agent need, that it is only built from a run whose checks passed, that startup.py picks it up
without downloading anything, and that Railway has exactly one start command.
"""
import importlib.util
import json
import os
import shutil
import sqlite3
import sys
import types

import pandas as pd
import pytest

from checks import run_checks
from conftest import APP_PATH, ROOT, working_dir

import make_demo_db
import startup


@pytest.fixture(scope="module")
def published_db(built_db, tmp_path_factory):
    """The synthetic database after a full local run of the checks, as the real one is after run_pipeline.py."""
    path = str(tmp_path_factory.mktemp("published") / "puma_dtc.db")
    shutil.copy(built_db, path)
    run_checks(db_path=path)
    return path


@pytest.fixture(scope="module")
def snapshot(published_db, tmp_path_factory):
    path = str(tmp_path_factory.mktemp("snapshot") / "deploy" / "puma_dtc_demo.sqlite")
    make_demo_db.build(published_db, path)
    return path


def tables(path):
    conn = sqlite3.connect(path)
    try:
        return {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    finally:
        conn.close()


def scalar(path, sql):
    conn = sqlite3.connect(path)
    try:
        return conn.execute(sql).fetchone()[0]
    finally:
        conn.close()


# ----------------------------------------------------------------------------------- the snapshot
def test_snapshot_holds_only_what_the_app_needs(published_db, snapshot):
    assert tables(snapshot) == {*make_demo_db.KEEP, "data_period"}
    for table in make_demo_db.KEEP:
        assert scalar(snapshot, f"SELECT COUNT(*) FROM {table}") == scalar(published_db, f"SELECT COUNT(*) FROM {table}")
    assert os.path.getsize(snapshot) < 300_000  # kilobytes, so it can live in git


def test_snapshot_records_the_period_of_the_clean_sales(published_db, snapshot):
    newest = scalar(published_db, "SELECT MAX(t_dat) FROM silver_transactions WHERE quality_flag = 'clean'")
    first = scalar(published_db, "SELECT MIN(t_dat) FROM silver_transactions WHERE quality_flag = 'clean'")
    assert scalar(snapshot, "SELECT last_sale FROM data_period") == newest
    assert scalar(snapshot, "SELECT first_sale FROM data_period") == first


def damaged_copy(published_db, tmp_path, sql):
    path = str(tmp_path / "damaged.db")
    shutil.copy(published_db, path)
    conn = sqlite3.connect(path)
    conn.execute(sql)
    conn.commit()
    conn.close()
    return path


def test_snapshot_is_refused_when_a_check_failed(published_db, tmp_path):
    path = damaged_copy(published_db, tmp_path, "UPDATE data_quality_report SET status = 'FAIL' WHERE rowid = 1")
    with pytest.raises(SystemExit, match="checks failed"):
        make_demo_db.build(path, str(tmp_path / "out.sqlite"))
    assert not os.path.exists(tmp_path / "out.sqlite")


def test_snapshot_is_refused_when_a_layer_was_never_checked(published_db, tmp_path):
    path = damaged_copy(published_db, tmp_path, "DELETE FROM data_quality_report WHERE layer = 'silver'")
    with pytest.raises(SystemExit, match="no results for silver"):
        make_demo_db.build(path, str(tmp_path / "out.sqlite"))


def test_snapshot_is_refused_without_a_report(published_db, tmp_path):
    path = damaged_copy(published_db, tmp_path, "DROP TABLE data_quality_report")
    with pytest.raises(SystemExit, match="data_quality_report"):
        make_demo_db.build(path, str(tmp_path / "out.sqlite"))


def test_snapshot_needs_a_source(tmp_path):
    with pytest.raises(SystemExit, match="not found"):
        make_demo_db.build(str(tmp_path / "nothing.db"), str(tmp_path / "out.sqlite"))


def test_a_new_snapshot_replaces_the_old_one(published_db, tmp_path):
    target = str(tmp_path / "deploy" / "demo.sqlite")
    make_demo_db.build(published_db, target)
    make_demo_db.build(published_db, target)  # the second run must not fail on existing tables
    assert tables(target) == {*make_demo_db.KEEP, "data_period"}


# ----------------------------------------------------------------------------------- what runs on the snapshot
@pytest.fixture()
def container(snapshot, tmp_path):
    """A folder like the Railway container: the snapshot as puma_dtc.db and nothing else."""
    folder = tmp_path / "container"
    folder.mkdir()
    shutil.copy(snapshot, folder / "puma_dtc.db")
    return folder


def test_agent_reads_only_gold_from_the_snapshot(container, monkeypatch):
    from agent import query_agent as qa
    monkeypatch.setattr(qa, "DB_PATH", str(container / "puma_dtc.db"))
    schema = qa.get_schema()
    assert "gold_inventory_health" in schema and "data_period" not in schema
    frame, truncated = qa.run_sql("SELECT puma_division, inventory_health_score FROM gold_inventory_health")
    assert len(frame) > 0 and not truncated


def test_dashboard_runs_on_the_snapshot(container, published_db, monkeypatch):
    pytest.importorskip("fastapi")
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient
    assert APP_PATH, "app.py not found under src/dashboard or app/"
    spec = importlib.util.spec_from_file_location("dashboard_on_snapshot", APP_PATH)
    module = importlib.util.module_from_spec(spec)
    with working_dir(container):
        spec.loader.exec_module(module)
        page = TestClient(module.app).get("/")
    assert page.status_code == 200
    assert "Data checks:" in page.text and "0 failed" in page.text
    newest = scalar(published_db, "SELECT MAX(t_dat) FROM silver_transactions WHERE quality_flag = 'clean'")
    assert f"Sales data to {pd.Timestamp(newest).strftime('%d %b %Y')}" in page.text
    assert "Pipeline run" in page.text and "unknown" not in page.text


# ----------------------------------------------------------------------------------- startup.py
def with_snapshot(folder, snapshot):
    os.makedirs(folder / "deploy", exist_ok=True)
    shutil.copy(snapshot, folder / startup.DEMO_DB)


def test_startup_copies_the_snapshot_and_downloads_nothing(snapshot, tmp_path, monkeypatch):
    with_snapshot(tmp_path, snapshot)
    monkeypatch.setattr(startup, "download_data", lambda: pytest.fail("must not download"))
    with working_dir(tmp_path):
        startup.prepare_database()
        assert tables(startup.DB) == {*make_demo_db.KEEP, "data_period"}


def test_startup_keeps_an_existing_database(snapshot, tmp_path, capsys):
    with_snapshot(tmp_path, snapshot)
    conn = sqlite3.connect(tmp_path / "puma_dtc.db")
    conn.execute("CREATE TABLE marker (x)")
    conn.commit()
    conn.close()
    with working_dir(tmp_path):
        startup.prepare_database()
        assert tables(startup.DB) == {"marker"}  # the snapshot did not replace it
    assert "WARNING" in capsys.readouterr().out  # no check results in it, so it says so


def test_startup_builds_when_the_source_data_is_there(snapshot, tmp_path, monkeypatch):
    with_snapshot(tmp_path, snapshot)
    os.makedirs(tmp_path / "data")
    (tmp_path / startup.SOURCE_FILE).write_text("t_dat\n")
    built = []
    package, module = types.ModuleType("src"), types.ModuleType("src.run_pipeline")
    module.run = lambda: built.append("run")
    monkeypatch.setitem(sys.modules, "src", package)
    monkeypatch.setitem(sys.modules, "src.run_pipeline", module)
    monkeypatch.setattr(startup, "download_data", lambda: pytest.fail("data is already there"))
    with working_dir(tmp_path):
        startup.prepare_database()
        assert built == ["run"] and not os.path.exists(startup.DB)


def test_serve_starts_the_dashboard_on_the_port_railway_sets(snapshot, tmp_path, monkeypatch):
    uvicorn = pytest.importorskip("uvicorn")
    calls = []
    monkeypatch.setattr(uvicorn, "run", lambda *a, **k: calls.append((a, k)))
    monkeypatch.setenv("PORT", "4321")
    with_snapshot(tmp_path, snapshot)
    with working_dir(tmp_path):
        startup.main([])
        assert calls == []  # without --serve it only prepares the database
        startup.main(["--serve"])
    assert calls == [(("src.dashboard.app:app",), {"host": "0.0.0.0", "port": 4321})]


# ----------------------------------------------------------------------------------- the config
def test_railway_has_exactly_one_start_command():
    config = os.path.join(ROOT, "railway.json")
    if not os.path.exists(config):
        pytest.skip("no railway.json in this project")
    with open(config, encoding="utf-8") as handle:
        assert json.load(handle)["deploy"]["startCommand"] == "python startup.py --serve"
    for stale in ("Procfile", "nixpacks.toml"):
        assert not os.path.exists(os.path.join(ROOT, stale)), \
            f"{stale} is a second start command. Delete it, railway.json is the only one."


def test_the_start_command_points_at_a_real_app():
    if not os.path.exists(os.path.join(ROOT, "src")):
        pytest.skip("flat layout")
    assert os.path.exists(os.path.join(ROOT, "src", "dashboard", "app.py"))  # "src.dashboard.app:app"
