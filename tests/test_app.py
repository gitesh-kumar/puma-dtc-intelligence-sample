"""Smoke tests for the dashboard. The app needs Python 3.12 or newer (nested quotes inside its
f-strings) and fastapi + httpx; the tests skip themselves when those are missing."""
import importlib.util
import os
import shutil
import sys

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")
if sys.version_info < (3, 12):
    pytest.skip("app.py needs Python 3.12 or newer", allow_module_level=True)

from fastapi.testclient import TestClient

from checks import run_checks
from conftest import APP_PATH, working_dir

FAKE_ANSWER = {
    "question": "q", "answer": "Swim is the problem.", "sql": "SELECT 1", "columns": ["puma_division"],
    "rows": [["Swim"]], "truncated": False, "verified": True, "ungrounded": [], "error": None,
}


@pytest.fixture()
def client(built_db, tmp_path, monkeypatch):
    shutil.copy(built_db, tmp_path / "puma_dtc.db")
    run_checks(db_path=str(tmp_path / "puma_dtc.db"))
    assert APP_PATH, "app.py not found under src/dashboard or app/"
    spec = importlib.util.spec_from_file_location("dashboard_app", APP_PATH)
    module = importlib.util.module_from_spec(spec)
    with working_dir(tmp_path):
        spec.loader.exec_module(module)
        monkeypatch.setattr(module, "ask", lambda question: {**FAKE_ANSWER, "question": question})
        yield TestClient(module.app)


def test_dashboard_renders_with_the_data_check_panel(client):
    page = client.get("/")
    assert page.status_code == 200
    assert "Data checks:" in page.text and "0 failed" in page.text
    assert "On simulated stock" in page.text
    assert "innerHTML" not in page.text  # model text must never be injected as HTML


def test_ask_endpoint_returns_answer_sql_and_verification(client):
    body = client.post("/ask", json={"question": "Which division?"}).json()
    assert body["answer"] and body["sql"] and body["verified"] is True
    assert body["question"] == "Which division?"


def test_dashboard_without_a_report_table_says_the_checks_have_not_run(built_db, tmp_path, monkeypatch):
    """A database built by the old pipeline has no data_quality_report table."""
    import sqlite3
    shutil.copy(built_db, tmp_path / "puma_dtc.db")
    conn = sqlite3.connect(tmp_path / "puma_dtc.db")
    conn.execute("DROP TABLE IF EXISTS data_quality_report")
    conn.commit()
    conn.close()
    spec = importlib.util.spec_from_file_location("dashboard_app_old_db", APP_PATH)
    module = importlib.util.module_from_spec(spec)
    with working_dir(tmp_path):
        spec.loader.exec_module(module)
        page = TestClient(module.app).get("/")
    assert page.status_code == 200
    assert "Data checks have not been run yet" in page.text
    assert "src\\run_pipeline.py" in page.text
