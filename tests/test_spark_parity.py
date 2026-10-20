"""The PySpark port against the pandas pipeline, on the same data.

Needs pyspark and Java (pip install pyspark pyarrow). Skipped when either is missing, which is the normal case
on a laptop that only runs the dashboard. Run it after every change to transform.py, analytics.py or checks.py:
the Databricks port is a second copy of that logic and does not change by itself.

What runs:
  * the two notebooks (01_pipeline.py, 02_parity.py), cell by cell, as Databricks would, except the cells
    marked "databricks-only" (they create the schema and the volume)
  * a data set with the awkward rows on purpose: missing and tiny prices, unknown articles, an unknown channel,
    boundary ages, a division that only sold in the last month, a December month end
  * the rounding helper against numpy on values that sit exactly on a rounding tie
"""
import os
import shutil
import sys
from datetime import datetime

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("pyspark")
pytest.importorskip("pyarrow")
from conftest import ROOT, working_dir  # noqa: E402

DATABRICKS = os.path.join(ROOT, "databricks")
if not os.path.isdir(DATABRICKS):
    pytest.skip("no databricks/ folder next to the tests", allow_module_level=True)
if DATABRICKS not in sys.path:
    sys.path.insert(0, DATABRICKS)

import dtc_reference as R  # noqa: E402
import dtc_spark as D  # noqa: E402
from export_bronze import export  # noqa: E402

DB_COUNTER = iter(range(10_000))


@pytest.fixture(scope="session")
def spark_session(tmp_path_factory):
    """A local Spark. DTC_SPARK_MODE=connect runs it through Spark Connect, which is how Databricks serverless
    talks to Spark (needs: pip install grpcio grpcio-status googleapis-common-protos zstandard)."""
    if shutil.which("java") is None:
        pytest.skip("Java is needed for a local Spark")
    from pyspark.sql import SparkSession
    os.environ["PYSPARK_PYTHON"] = os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable  # workers = this Python
    folder = tmp_path_factory.mktemp("spark")
    builder = SparkSession.builder.master("local[2]").appName("dtc-parity")
    if os.environ.get("DTC_SPARK_MODE") == "connect":
        os.chdir(folder)  # the local Connect server keeps its warehouse in ./spark-warehouse
        builder = builder.config("spark.api.mode", "connect")
    else:
        builder = (builder.config("spark.ui.enabled", "false").config("spark.driver.host", "127.0.0.1")
                   .config("spark.sql.shuffle.partitions", "4").config("spark.sql.warehouse.dir", str(folder)))
    session = builder.getOrCreate()
    yield session
    session.stop()


@pytest.fixture()
def spark(spark_session):
    """The shared session with a fresh, empty database selected."""
    name = f"dtc_test_{next(DB_COUNTER)}"
    spark_session.sql(f"CREATE DATABASE {name}")
    spark_session.sql(f"USE {name}")
    spark_session.conf.set("spark.sql.ansi.enabled", "true")
    spark_session.conf.set("spark.sql.session.timeZone", "UTC")
    yield spark_session
    spark_session.sql(f"DROP DATABASE {name} CASCADE")


@pytest.fixture(scope="session")
def local_run(built_db, tmp_path_factory):
    """The synthetic database after a local run of the checks, so that it holds data_quality_report."""
    from checks import run_checks
    path = str(tmp_path_factory.mktemp("local") / "puma_dtc.db")
    shutil.copy(built_db, path)
    run_checks(db_path=path)
    return path


def run_notebook(name, spark, landing, printed=None):
    """Execute a notebook file cell by cell. Cells marked databricks-only are skipped."""
    namespace = {"spark": spark, "display": lambda df: None, "LANDING": landing}
    if printed is not None:
        namespace["print"] = lambda *a, **k: printed.append(" ".join(str(x) for x in a))
    with open(os.path.join(DATABRICKS, name), encoding="utf-8") as handle:
        cells = handle.read().split("# COMMAND ----------")
    for i, cell in enumerate(cells):
        code = "\n".join(line for line in cell.strip().splitlines() if not line.startswith("# MAGIC")).strip()
        if not code or code.splitlines()[0].startswith("# databricks-only"):
            continue
        exec(compile(code, f"{name} cell {i}", "exec"), namespace)  # noqa: S102
        namespace["LANDING"] = landing  # the notebook points at /Volumes, the test at a temp folder
    return namespace


# ----------------------------------------------------------------------------------- the notebooks, end to end
@pytest.mark.parametrize("ansi", ["true", "false"])
def test_notebooks_prove_parity(spark, local_run, tmp_path, ansi):
    landing = str(tmp_path / "landing")
    export(local_run, landing)
    assert sorted(os.listdir(landing)) == ["bronze_articles.parquet", "bronze_customers.parquet",
                                          "bronze_transactions.parquet", "local_reference.json"]

    spark.conf.set("spark.sql.ansi.enabled", ansi)  # the port must hold in both modes, serverless uses true
    printed = []
    run_notebook("01_pipeline.py", spark, landing, printed)
    ns = run_notebook("02_parity.py", spark, landing, printed)
    summary = R.summarise(ns["report"])
    assert summary["failed"] == 0, ns["report"][ns["report"]["status"] == "FAIL"].to_string()
    assert any(line.startswith("PARITY PASS") for line in printed), printed
    # 4 Gold tables, 4 Silver groupings, the Bronze counts and the 32 checks were all compared
    tables = set(ns["report"]["table"])
    assert {"bronze_counts", *R.GOLD_TABLES, "silver:tx_by_group", "silver:tx_by_week", "silver:customers",
            "silver:articles", "checks"} <= tables
    assert (ns["report"]["table"] == "checks").sum() == 32


def test_notebooks_stop_on_a_failed_check(spark, local_run, tmp_path):
    """A broken Bronze copy must stop notebook 01 at the Bronze gate, before Silver is built."""
    landing = str(tmp_path / "landing")
    export(local_run, landing)
    path = os.path.join(landing, "bronze_transactions.parquet")
    pd.read_parquet(path).iloc[:-10].to_parquet(path, index=False)  # lose ten rows in the "upload"
    with pytest.raises(D.PipelineFailed, match="bronze"):
        run_notebook("01_pipeline.py", spark, landing)
    assert not spark.catalog.tableExists("silver_transactions")


def test_parity_notebook_catches_a_changed_rule(spark, local_run, tmp_path, monkeypatch):
    """If the port and the local pipeline disagree, notebook 02 must fail, not shrug."""
    landing = str(tmp_path / "landing")
    export(local_run, landing)
    monkeypatch.setitem(D.INVENTORY_SCENARIOS, "Swim", 3.9)  # the local run uses 4.0
    run_notebook("01_pipeline.py", spark, landing)
    with pytest.raises(AssertionError, match="PARITY FAIL"):
        run_notebook("02_parity.py", spark, landing)


# ----------------------------------------------------------------------------------- awkward data
def make_edge_db(folder):
    """A small Bronze database with awkward rows, run through the pandas pipeline."""
    import sqlite3

    import make_synthetic_data
    from analytics import build_gold
    from checks import run_checks
    from transform import transform_silver

    os.makedirs(folder, exist_ok=True)
    data = os.path.join(folder, "data")
    make_synthetic_data.make(data, n_transactions=40_000, n_articles=300, n_customers=300, seed=11)
    tx = pd.read_csv(os.path.join(data, "transactions_train.csv"))
    art = pd.read_csv(os.path.join(data, "articles.csv"))
    cust = pd.read_csv(os.path.join(data, "customers.csv"))
    for df in (tx, art, cust):
        df.columns = [c.lower() for c in df.columns]

    # Dates: June to December 2020. The newest date is the 30th, so the last month is incomplete and the
    # next month of the reorder forecast wraps around to January.
    tx["t_dat"] = pd.to_datetime(tx["t_dat"]) + pd.DateOffset(months=8)
    # A division that only sold in the last month: no prior sales, so the velocity change is undefined.
    swim = art.loc[art["product_group_name"] == "Swimwear", "article_id"]
    tx = tx[~(tx["article_id"].isin(swim) & (tx["t_dat"] < "2020-12-01"))].reset_index(drop=True)
    last = tx.index[-12:]                                # the newest rows, they reach the Gold windows
    tx.loc[0:4, "price"] = np.nan                        # missing price
    tx.loc[last[:4], "price"] = 0.000004                 # positive price that rounds to 0.00 cents
    tx.loc[last[4:8], "article_id"] = 999                # article that does not exist: no category, no division
    tx.loc[last[8:], "price"] = np.nan                   # missing price in the newest month
    tx.loc[15:19, "sales_channel_id"] = 3                # channel that is neither store nor online
    cust.loc[0:5, "age"] = [0, 25, 25.5, 100, 101, np.nan]  # the edges of the age bands
    cust.loc[6, "club_member_status"] = np.nan

    conn = sqlite3.connect(os.path.join(folder, "puma_dtc.db"))
    stamp = datetime.now()
    for table, df in (("bronze_transactions", tx), ("bronze_articles", art), ("bronze_customers", cust)):
        df = df.copy()
        df["ingested_at"], df["source"] = stamp, "h_and_m"
        df.to_sql(table, conn, index=False)
    audit = pd.DataFrame([("bronze", "source_transaction_rows", round(len(tx) / 0.06)),
                          ("bronze", "sampled_transaction_rows", len(tx)),
                          ("bronze", "sample_fraction_target", 0.06),
                          ("bronze", "source_article_rows", len(art)),
                          ("bronze", "source_customer_rows", len(cust))], columns=["stage", "metric", "value"])
    audit["value"] = audit["value"].astype(float)
    audit["recorded_at"] = stamp
    audit.to_sql("pipeline_audit", conn, index=False)
    conn.close()

    with working_dir(folder):
        transform_silver()
        build_gold()
        report = run_checks()
    return os.path.join(folder, "puma_dtc.db"), report


@pytest.mark.parametrize("ansi,zone", [("true", "UTC"), ("false", "America/Los_Angeles")])
def test_awkward_rows_give_the_same_numbers(spark, tmp_path, ansi, zone):
    db, local_report = make_edge_db(str(tmp_path / "edge"))
    # the data must really be awkward, otherwise this test proves nothing
    assert (local_report["status"] == "FAIL").sum() >= 5
    assert (local_report["status"] == "WARN").any()

    landing = str(tmp_path / "landing")
    export(db, landing, require_clean=False)
    reference = R.load_reference(os.path.join(landing, "local_reference.json"))
    assert any(row["velocity_change_pct"] is None for row in reference["gold"]["gold_sell_through"])

    spark.conf.set("spark.sql.ansi.enabled", ansi)
    spark.conf.set("spark.sql.session.timeZone", zone)
    D.load_bronze(spark, landing, reference["pipeline_audit"])
    D.build_silver(spark)
    D.build_gold(spark)
    D.save_report(spark, [D.run_checks(spark, ("bronze", "silver", "gold"))])
    report = R.compare_reference(reference, D.collect_platform(spark))
    assert R.summarise(report)["failed"] == 0, report[report["status"] == "FAIL"].to_string()
    # the checks that fail locally fail here, for the same reason
    assert (report["table"] == "checks").sum() == 32



# ----------------------------------------------------------------------------------- helpers
def test_round_matches_numpy_on_ties(spark):
    from pyspark.sql import functions as F
    values = [0.5, 1.5, 2.5, -0.5, -1.5, 0.125, 0.375, 2.675, 1.005, 0.25, 0.35, 0.45, 10.5, 123.456, 1e-7, 0.0]
    for decimals in (0, 1, 2):
        df = spark.createDataFrame([(float(v),) for v in values], ["x"])
        got = [r[0] for r in df.select(D._round(F.col("x"), decimals)).collect()]
        assert got == [float(np.round(v, decimals)) for v in values], decimals
    # Spark's own round() is not the same function, which is the reason for the helper
    assert [r[0] for r in spark.createDataFrame([(2.5,), (0.125,)], ["x"]).select(F.round("x", 0)).collect()][0] == 3.0


def test_clip_keeps_missing_values(spark):
    from pyspark.sql import functions as F
    df = spark.createDataFrame([(-80.0,), (20.0,), (None,), (120.0,)], "x double")
    got = [r[0] for r in df.select(D._clip(F.col("x"), -50, 50)).collect()]
    assert got[0] == -50.0 and got[1] == 20.0 and got[2] is None and got[3] == 50.0


def test_lookup_returns_null_for_a_missing_key_also_in_ansi_mode(spark):
    df = spark.createDataFrame([("Shoes",), ("Nothing",), (None,)], "g string")
    got = [r[0] for r in df.select(D._lookup("g", D.PUMA_CATEGORY_MAP)).collect()]
    assert got == ["Footwear", None, None]


def test_sql_translation():
    assert D._to_spark_sql('SELECT COUNT(*) FROM t WHERE "age group" IS NULL', []) == \
        "SELECT COUNT(*) FROM t WHERE `age group` IS NULL"
    assert D._to_spark_sql("SELECT 1 WHERE a = ? AND b IN (?,?)", ["x'y", 1, 2.5]) == \
        "SELECT 1 WHERE a = 'x\\'y' AND b IN (1,2.5)"
    with pytest.raises(ValueError):
        D._to_spark_sql("SELECT ?", [])


def test_null_check_is_not_vacuous(spark):
    """The Gold null check quotes column names with "double quotes". In Spark that would be a string and the
    check would pass for any data. SparkConn must turn it into a column reference."""
    for name in R.GOLD_TABLES:
        spark.createDataFrame([("Running", 1.0)], "puma_division string, x double").write.saveAsTable(name)
    spark.sql("DROP TABLE gold_sell_through")
    spark.createDataFrame([("Running", None)], "puma_division string, x double").write.saveAsTable("gold_sell_through")
    checks = D._checks_module()
    checker = checks.Checker(D.SparkConn(spark))
    checks.gold_checks(checker)
    nulls = [r for r in checker.results if r["check_name"] == "no empty values in Gold tables"][0]
    assert nulls["status"] == "FAIL" and "gold_sell_through" in nulls["observed"]
