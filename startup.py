import os
import sqlite3
import subprocess
import zipfile
from src.run_pipeline import run

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
        conn = sqlite3.connect("puma_dtc.db")
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

if __name__ == "__main__":
    if not os.path.exists("puma_dtc.db"):
        print("Database not found. Building pipeline...")
        if not os.path.exists("data/transactions_train.csv"):
            download_data()
        run()  # Bronze, checks, Silver, checks, Gold, checks. Exits with code 1 if a layer fails.
        print("Pipeline complete.")
    else:
        problem = check_problem()
        if problem:
            print(f"WARNING: database found, but {problem}.")
            print("Delete puma_dtc.db and run this again, or run: python src\\run_pipeline.py")
        print("Database found. Starting app...")
