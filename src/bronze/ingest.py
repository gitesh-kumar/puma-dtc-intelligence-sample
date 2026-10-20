import pandas as pd
import sqlite3
import os
from datetime import datetime

# Paths
DATA_DIR = "data"
DB_PATH = "puma_dtc.db"
SAMPLE_FRACTION = 0.06

def ingest_bronze():
    print(f"[{datetime.now()}] Starting Bronze layer ingestion...")
    
    conn = sqlite3.connect(DB_PATH)

    # Audit trail: what was read from the source files and what was written to Bronze.
    # checks.py reconciles the Bronze tables against these numbers.
    audit = []

    # --- TRANSACTIONS ---
    print("Loading transactions (chunked random sample)...")
    
    chunk_size = 100000
    sample_chunks = []
    source_rows = 0

    for i, chunk in enumerate(pd.read_csv(
        os.path.join(DATA_DIR, "transactions_train.csv"),
        chunksize=chunk_size,
        parse_dates=["t_dat"]
    )):
        source_rows += len(chunk)
        # Keep 6% of each chunk randomly
        sampled = chunk.sample(frac=SAMPLE_FRACTION, random_state=42)
        sample_chunks.append(sampled)
        if i % 10 == 0:
            print(f"  Processed {i * chunk_size:,} rows...")
    
    transactions = pd.concat(sample_chunks, ignore_index=True)
    print(f"Transactions loaded: {len(transactions)} rows across all months")
    transactions.columns = [c.lower() for c in transactions.columns]
    transactions["ingested_at"] = datetime.now()
    transactions["source"] = "h_and_m"
    transactions.to_sql("bronze_transactions", conn, if_exists="replace", index=False)
    audit += [
        ("bronze", "source_transaction_rows", source_rows),
        ("bronze", "sampled_transaction_rows", len(transactions)),
        ("bronze", "sample_fraction_target", SAMPLE_FRACTION),
    ]

    # --- ARTICLES ---
    print("Loading articles...")
    articles = pd.read_csv(os.path.join(DATA_DIR, "articles.csv"))
    articles.columns = [c.lower() for c in articles.columns]
    articles["ingested_at"] = datetime.now()
    articles["source"] = "h_and_m"
    articles.to_sql("bronze_articles", conn, if_exists="replace", index=False)
    print(f"Articles loaded: {len(articles)} rows")
    audit.append(("bronze", "source_article_rows", len(articles)))

    # --- CUSTOMERS ---
    print("Loading customers...")
    customers = pd.read_csv(os.path.join(DATA_DIR, "customers.csv"))
    customers.columns = [c.lower() for c in customers.columns]
    customers["ingested_at"] = datetime.now()
    customers["source"] = "h_and_m"
    customers.to_sql("bronze_customers", conn, if_exists="replace", index=False)
    print(f"Customers loaded: {len(customers)} rows")
    audit.append(("bronze", "source_customer_rows", len(customers)))

    audit_df = pd.DataFrame(audit, columns=["stage", "metric", "value"])
    audit_df["value"] = audit_df["value"].astype(float)
    audit_df["recorded_at"] = datetime.now()
    audit_df.to_sql("pipeline_audit", conn, if_exists="replace", index=False)

    conn.close()
    print(f"[{datetime.now()}] Bronze layer complete. Database saved to {DB_PATH}")

if __name__ == "__main__":
    ingest_bronze()