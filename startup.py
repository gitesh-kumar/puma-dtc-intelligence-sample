"""Get a database in place, and with --serve start the dashboard.

    python startup.py            # prepare the database only
    python startup.py --serve    # prepare it, then run the dashboard on $PORT (8000 when unset). Railway uses this.

Database, in this order:
  1. puma_dtc.db exists: use it, and warn if its data checks are missing or failed.
  2. no database, no source data, but deploy/puma_dtc_demo.sqlite exists: copy the snapshot (a deployed
     container, or a fresh clone). No download.
  3. otherwise: download the H&M data if needed and build the whole pipeline with its checks.
"""
import argparse
import os
import shutil
import sqlite3
import subprocess
import zipfile

DB = "puma_dtc.db"
DEMO_DB = os.path.join("deploy", "puma_dtc_demo.sqlite")
SOURCE_FILE = os.path.join("data", "transactions_train.csv")


def download_data():
    print("Downloading H&M dataset from Kaggle...")
    os.makedirs("data", exist_ok=True)

    files = ["transactions_train.csv", "articles.csv", "customers.csv"]

    for f in files:
        print(f"Downloading {f}...")
        subprocess.run([
            "kaggle", "competitions", "download",
            "-c", "h-and-m-personalized-fashion-recommendations",
            "-f", f,
            "--path", "data"
        ])

    for f in os.listdir("data"):
        if f.endswith(".zip"):
            print(f"Unzipping {f}...")
            with zipfile.ZipFile(f"data/{f}", "r") as z:
                z.extractall("data")
            os.remove(f"data/{f}")

    print("Download complete.")


def check_problem():
    """Say what is wrong with the existing database's check results, or return None when all is well."""
    try:
        conn = sqlite3.connect(DB)
        layers = dict(conn.execute("SELECT layer, SUM(status = 'FAIL') FROM data_quality_report GROUP BY layer"))
        conn.close()
    except sqlite3.Error:
        return "the data checks have never been run on it"
    missing = {"bronze", "silver", "gold"} - set(layers)
    if missing:
        return f"it has no check results for {', '.join(sorted(missing))}, so a build was probably cut short"
    failed = sorted(layer for layer, fails in layers.items() if fails)
    if failed:
        return f"the last checks failed for {', '.join(failed)}"
    return None


def prepare_database():
    if os.path.exists(DB):
        problem = check_problem()
        if problem:
            print(f"WARNING: database found, but {problem}.")
            print("Delete puma_dtc.db and run this again, or run: python src\\run_pipeline.py")
        print("Database found. Starting app...")
    elif os.path.exists(DEMO_DB) and not os.path.exists(SOURCE_FILE):
        shutil.copy(DEMO_DB, DB)
        print(f"No database and no source data here. Using the demo snapshot {DEMO_DB}.")
    else:
        print("Database not found. Building pipeline...")
        if not os.path.exists(SOURCE_FILE):
            download_data()
        from src.run_pipeline import run  # only needed for a build, and pulls in pandas
        run()  # Bronze, checks, Silver, checks, Gold, checks. Exits with code 1 if a layer fails.
        print("Pipeline complete.")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--serve", action="store_true", help="start the dashboard after preparing the database")
    args = parser.parse_args(argv)
    prepare_database()
    if args.serve:
        import uvicorn
        uvicorn.run("src.dashboard.app:app", host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))


if __name__ == "__main__":
    main()
