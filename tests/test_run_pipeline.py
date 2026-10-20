"""run_pipeline.run() builds all three layers, records the check results, and stops on a failing layer."""
import os
import sqlite3

import pytest

import make_synthetic_data
from conftest import working_dir


@pytest.fixture()
def project(tmp_path):
    make_synthetic_data.make(os.path.join(tmp_path, "data"), n_transactions=60_000)
    return tmp_path


def layers_reported(db):
    conn = sqlite3.connect(db)
    try:
        return {r[0]: r[1] for r in conn.execute(
            "SELECT layer, SUM(status = 'FAIL') FROM data_quality_report GROUP BY layer")}
    finally:
        conn.close()


def test_run_builds_every_layer_and_records_every_check(project):
    import run_pipeline
    with working_dir(project):
        run_pipeline.run()
    assert layers_reported(project / "puma_dtc.db") == {"bronze": 0, "silver": 0, "gold": 0}


def test_run_stops_when_a_layer_fails_and_does_not_build_the_next_one(project, monkeypatch):
    import run_pipeline

    def broken_silver():
        conn = sqlite3.connect("puma_dtc.db")
        conn.execute("CREATE TABLE silver_transactions (article_id TEXT)")  # empty: row counts cannot reconcile
        conn.commit()
        conn.close()

    monkeypatch.setattr(run_pipeline, "transform_silver", broken_silver)
    monkeypatch.setattr(run_pipeline, "build_gold", lambda: pytest.fail("Gold must not be built after a failed Silver"))
    with working_dir(project):
        with pytest.raises(SystemExit) as stop:
            run_pipeline.run()
    assert stop.value.code == 1
    reported = layers_reported(project / "puma_dtc.db")
    assert reported["silver"] > 0 and "gold" not in reported
