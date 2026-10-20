"""Write the small snapshot database that the deployed dashboard runs on.

    python src\\make_demo_db.py            # reads puma_dtc.db, writes deploy\\puma_dtc_demo.sqlite

Why: the full database is not in git (it is built from 3.5 GB of Kaggle data) and a free host cannot build
it on every start. The dashboard and the agent only read the four Gold tables, the data check report and the
period of the sales data, a few kilobytes in total. The snapshot holds exactly that and no raw data.

The script refuses to write a snapshot from a run whose checks failed or are missing, so the demo can never
show numbers that the pipeline itself rejected. Run it after every pipeline run you want to publish, commit
the file, push.
"""
import os
import sqlite3
import sys

import pandas as pd

SOURCE = "puma_dtc.db"
TARGET = os.path.join("deploy", "puma_dtc_demo.sqlite")  # not *.db, which .gitignore excludes
GOLD = ("gold_sell_through", "gold_markdown_risk", "gold_reorder_signals", "gold_inventory_health")
KEEP = (*GOLD, "data_quality_report")
LAYERS = {"bronze", "silver", "gold"}


def build(source=SOURCE, target=TARGET):
    """Write the snapshot. Returns {table: rows}. Exits with a message when the source is not publishable."""
    if not os.path.exists(source):
        raise SystemExit(f"{source} not found. Run this from the project folder after the pipeline has run.")
    conn = sqlite3.connect(source)
    try:
        existing = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
        missing = [t for t in (*KEEP, "silver_transactions") if t not in existing]
        if missing:
            raise SystemExit("These tables are missing: " + ", ".join(missing) +
                             "\nRun the pipeline first: python src\\run_pipeline.py")
        report = pd.read_sql("SELECT * FROM data_quality_report", conn)
        absent = LAYERS - set(report["layer"])
        if absent:
            raise SystemExit("The data check report has no results for " + ", ".join(sorted(absent)) +
                             ". Run the whole pipeline: python src\\run_pipeline.py")
        failed = report[report["status"] == "FAIL"]
        if len(failed):
            raise SystemExit("Not publishing, these checks failed:\n  " + "\n  ".join(failed["check_name"]))
        frames = {t: pd.read_sql(f"SELECT * FROM {t}", conn) for t in KEEP}
        frames["data_period"] = pd.read_sql(
            """SELECT MIN(t_dat) AS first_sale, MAX(t_dat) AS last_sale, COUNT(*) AS clean_rows
               FROM silver_transactions WHERE quality_flag = 'clean'""", conn)
    finally:
        conn.close()

    os.makedirs(os.path.dirname(target) or ".", exist_ok=True)
    if os.path.exists(target):
        os.remove(target)
    out = sqlite3.connect(target)
    try:
        for name, df in frames.items():
            df.to_sql(name, out, index=False)
    finally:
        out.close()
    return {name: len(df) for name, df in frames.items()}


def main():
    counts = build()
    for name, rows in counts.items():
        print(f"  {name}: {rows} rows")
    print(f"Wrote {TARGET} ({os.path.getsize(TARGET) / 1024:.0f} KB).")
    print("Commit it and push to publish: git add deploy\\puma_dtc_demo.sqlite")


if __name__ == "__main__":
    sys.exit(main())
