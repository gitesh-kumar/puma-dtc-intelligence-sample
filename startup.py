import os
import subprocess
import zipfile
from src.bronze.ingest import ingest_bronze
from src.silver.transform import transform_silver
from src.gold.analytics import build_gold

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

if __name__ == "__main__":
    if not os.path.exists("puma_dtc.db"):
        print("Database not found. Building pipeline...")
        if not os.path.exists("data/transactions_train.csv"):
            download_data()
        ingest_bronze()
        transform_silver()
        build_gold()
        print("Pipeline complete.")
    else:
        print("Database found. Starting app...")