"""Smoke tests for the dashboard. They need fastapi + httpx and skip themselves when those are missing."""
import importlib.util
import os
import shutil
import sqlite3

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")

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


def test_headline_names_the_division_with_the_lowest_health_score(client, built_db):
    conn = sqlite3.connect(built_db)
    worst = conn.execute("SELECT puma_division FROM gold_inventory_health ORDER BY inventory_health_score LIMIT 1").fetchone()[0]
    conn.close()
    page = client.get("/").text
    assert f"{worst} needs attention first" in page
    assert "on simulated stock" in page


def test_page_states_that_it_is_a_demo_and_not_affiliated(client):
    assert "Not affiliated with, endorsed by or connected to PUMA SE" in client.get("/").text


def test_every_value_is_in_the_page_without_javascript(client):
    """Charts need Chart.js from a CDN. The lists and the table views carry every number on their own."""
    page = client.get("/").text
    assert page.count('class="row row-health"') == 6 and page.count('class="row row-markdown"') == 6
    assert page.count('class="table-view"') == 2
    assert 'id="dash-data"' in page


def test_passing_checks_are_plain_text_and_only_problems_get_chips(client):
    page = client.get("/").text
    assert page.count('class="pass"') == 32
    assert "chip-critical" not in page.split('class="dq')[1].split("</details>")[0]


def test_headline_when_nothing_needs_attention(built_db, tmp_path):
    shutil.copy(built_db, tmp_path / "puma_dtc.db")
    conn = sqlite3.connect(tmp_path / "puma_dtc.db")
    conn.execute("UPDATE gold_inventory_health SET inventory_health_score = 90, health_label = 'HEALTHY'")
    conn.commit()
    conn.close()
    spec = importlib.util.spec_from_file_location("dashboard_app_healthy", APP_PATH)
    module = importlib.util.module_from_spec(spec)
    with working_dir(tmp_path):
        spec.loader.exec_module(module)
        page = TestClient(module.app).get("/").text
    assert "No division needs attention right now" in page


def test_division_names_are_escaped_in_the_page(built_db, tmp_path):
    shutil.copy(built_db, tmp_path / "puma_dtc.db")
    conn = sqlite3.connect(tmp_path / "puma_dtc.db")
    for table in ("gold_inventory_health", "gold_markdown_risk", "gold_sell_through", "gold_reorder_signals"):
        conn.execute(f"UPDATE {table} SET puma_division = '<script>alert(1)</script>' WHERE puma_division = 'Swim'")
    conn.commit()
    conn.close()
    spec = importlib.util.spec_from_file_location("dashboard_app_escape", APP_PATH)
    module = importlib.util.module_from_spec(spec)
    with working_dir(tmp_path):
        spec.loader.exec_module(module)
        page = TestClient(module.app).get("/").text
    assert "<script>alert(1)</script>" not in page
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in page
    assert r"\u003cscript>alert(1)\u003c/script>" in page  # and escaped inside the JSON block for the charts
