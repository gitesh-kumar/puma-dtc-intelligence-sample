"""Prepare the files you upload to Databricks. Run this on your own machine, from the project root:

    python databricks\\export_bronze.py                      # reads puma_dtc.db, writes databricks_export\\
    python databricks\\export_bronze.py --db other.db --out some_folder

It writes four files:
    bronze_transactions.parquet, bronze_articles.parquet, bronze_customers.parquet
        a copy of the Bronze tables, exactly as your local run saw them
    local_reference.json
        the numbers of your local run: the four Gold tables, group totals of the Silver tables, the row counts
        of the Bronze tables, the data quality report. The parity notebook compares Databricks against it.

Run the local pipeline first (python src\\run_pipeline.py --skip-ingest) so that Silver, Gold and the report
belong to the Bronze data you are exporting. The script stops if one of them is missing.

Needs pandas and pyarrow (pip install pyarrow).
"""
import argparse
import os
import sqlite3
import sys

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from dtc_reference import BRONZE_TABLES, GOLD_TABLES, build_reference, save_reference  # noqa: E402

NEEDED = ("pipeline_audit", "silver_transactions", "silver_articles", "silver_customers", *GOLD_TABLES)


def export(db_path, out_dir, require_clean=True):
    """Write the four files into out_dir and return their sizes in MB.

    With require_clean the export stops when the local run has FAILed checks (the tests turn this off)."""
    try:
        import pyarrow  # noqa: F401
    except ImportError:
        raise SystemExit("pyarrow is missing. Run: pip install pyarrow") from None
    if not os.path.exists(db_path):
        raise SystemExit(f"{db_path} not found. Run this from the project folder, or pass --db.")

    conn = sqlite3.connect(db_path)
    try:
        existing = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
        needed = (*BRONZE_TABLES, *NEEDED, *(("data_quality_report",) if require_clean else ()))
        missing = [t for t in needed if t not in existing]
        if missing:
            raise SystemExit("These tables are missing from the database: " + ", ".join(sorted(set(missing))) +
                             "\nRun the local pipeline first: python src\\run_pipeline.py")
        failed = []
        if "data_quality_report" in existing:
            failed = [r[0] for r in conn.execute("SELECT check_name FROM data_quality_report WHERE status = 'FAIL'")]
        if failed and require_clean:
            raise SystemExit("The local run has failed checks, so there is nothing worth comparing yet:\n  "
                             + "\n  ".join(failed))

        os.makedirs(out_dir, exist_ok=True)
        sizes = {}
        for table in BRONZE_TABLES:
            path = os.path.join(out_dir, f"{table}.parquet")
            pd.read_sql(f"SELECT * FROM {table}", conn).to_parquet(path, index=False, compression="zstd")
            sizes[os.path.basename(path)] = os.path.getsize(path) / 1e6
            print(f"  {os.path.basename(path)}: {sizes[os.path.basename(path)]:.1f} MB")
    finally:
        conn.close()

    reference = build_reference(db_path)
    path = os.path.join(out_dir, "local_reference.json")
    save_reference(reference, path)
    sizes["local_reference.json"] = os.path.getsize(path) / 1e6
    print(f"  local_reference.json: {sizes['local_reference.json']:.2f} MB "
          f"({len(reference['checks'])} checks from the local run)")
    return sizes


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", default="puma_dtc.db")
    parser.add_argument("--out", default="databricks_export")
    args = parser.parse_args()
    print(f"Reading {args.db}")
    export(args.db, args.out)
    print(f"\nDone. Upload the four files in {args.out} to your Databricks volume.")


if __name__ == "__main__":
    main()
