"""Run the whole pipeline with the quality checks between the stages.

    python run_pipeline.py                 # Bronze -> checks -> Silver -> checks -> Gold -> checks
    python run_pipeline.py --skip-ingest   # reuse the existing Bronze tables

The run stops at the first stage whose checks FAIL, so a broken layer never feeds the next one.
"""
import argparse
import os
import sys

# The stage scripts may sit next to this file or in bronze/, silver/ and gold/ sub-folders.
HERE = os.path.dirname(os.path.abspath(__file__))
for sub in ("", "bronze", "silver", "gold"):
    folder = os.path.join(HERE, sub)
    if os.path.isdir(folder) and folder not in sys.path:
        sys.path.insert(0, folder)

from analytics import build_gold
from checks import FAIL, run_checks, summarize
from ingest import ingest_bronze
from transform import transform_silver


def gate(stage):
    df = run_checks((stage,))
    s = summarize(df)
    print(f"[{stage}] checks: {s['PASS']} passed, {s['WARN']} warnings, {s['FAIL']} failed")
    for row in df[df["status"] != "PASS"].itertuples():
        print(f"    {row.status}  {row.check_name}: observed {row.observed}, expected {row.expected}")
    if s[FAIL]:
        if df["observed"].astype(str).str.contains("pipeline_audit").any():
            print("    Hint: this database was built before the audit table existed. Run once without --skip-ingest.")
        print(f"Stopping: the {stage} layer failed its checks.")
        sys.exit(1)


def run(skip_ingest=False):
    """Bronze, checks, Silver, checks, Gold, checks. Exits with code 1 when a layer fails its checks."""
    if not skip_ingest:
        ingest_bronze()
    gate("bronze")
    transform_silver()
    gate("silver")
    build_gold()
    gate("gold")
    print("Pipeline finished, all checks passed.")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--skip-ingest", action="store_true", help="reuse the Bronze tables already in the database")
    run(parser.parse_args().skip_ingest)


if __name__ == "__main__":
    main()
