"""Synthetic stand-in for the H&M CSV files, used only by the tests.

It reproduces the columns the pipeline reads (transactions_train.csv, articles.csv,
customers.csv) and a few deliberately awkward rows (zero prices, unmapped product groups),
so the quality checks have something to catch. It is NOT the real dataset.
"""
import os

import numpy as np
import pandas as pd

PRODUCT_GROUPS = [
    "Garment Upper body", "Garment Lower body", "Garment Full body", "Accessories",
    "Underwear", "Shoes", "Swimwear", "Socks & Tights", "Nightwear", "Bags",
    "Cosmetic", "Unknown", "Items", "Stationery",
]
# Mostly mapped groups, a thin tail of groups that fall into "Other"
GROUP_WEIGHTS = np.array([14, 10, 4, 12, 8, 9, 5, 6, 5, 6, 1, 1, 0.5, 0.5], dtype=float)
GROUP_WEIGHTS = GROUP_WEIGHTS / GROUP_WEIGHTS.sum()


def make(data_dir, n_transactions=220_000, n_articles=600, n_customers=400, seed=7):
    rng = np.random.default_rng(seed)
    os.makedirs(data_dir, exist_ok=True)

    article_ids = np.arange(100_000_000, 100_000_000 + n_articles)
    groups = rng.choice(PRODUCT_GROUPS, size=n_articles, p=GROUP_WEIGHTS)
    articles = pd.DataFrame({
        "article_id": article_ids,
        "product_code": article_ids // 1000,
        "prod_name": [f"item {i}" for i in range(n_articles)],
        "product_type_name": [f"type {g[:4]}" for g in groups],
        "product_group_name": groups,
        "colour_group_name": rng.choice(["Black", "White", "Blue", "Red"], size=n_articles),
        "department_name": rng.choice(["Jersey", "Denim", "Trouser", "Knitwear"], size=n_articles),
        "section_name": rng.choice(["Ladieswear", "Menswear", "Divided"], size=n_articles),
        "garment_group_name": rng.choice(["Jersey Basic", "Outdoor", "Swimwear"], size=n_articles),
    })
    articles.to_csv(os.path.join(data_dir, "articles.csv"), index=False)

    customer_ids = [f"cust{i:06d}" for i in range(n_customers)]
    customers = pd.DataFrame({
        "customer_id": customer_ids,
        "FN": rng.choice([1.0, np.nan], size=n_customers),
        "Active": rng.choice([1.0, np.nan], size=n_customers),
        "club_member_status": rng.choice(["ACTIVE", "PRE-CREATE", "LEFT CLUB"], size=n_customers, p=[0.9, 0.08, 0.02]),
        "fashion_news_frequency": rng.choice(["NONE", "Regularly"], size=n_customers),
        "age": rng.integers(18, 80, size=n_customers).astype(float),
        "postal_code": ["x"] * n_customers,
    })
    customers.to_csv(os.path.join(data_dir, "customers.csv"), index=False)

    # Six and a half months of sales, with a gentle seasonal wave
    days = pd.date_range("2019-10-01", "2020-04-30", freq="D")
    day_idx = rng.integers(0, len(days), size=n_transactions)
    t_dat = days[np.sort(day_idx)]
    price = rng.uniform(0.005, 0.12, size=n_transactions).round(6)
    zero_rows = rng.choice(n_transactions, size=40, replace=False)
    price[zero_rows] = 0.0  # invalid prices the Silver flag should catch
    transactions = pd.DataFrame({
        "t_dat": t_dat.strftime("%Y-%m-%d"),
        "customer_id": rng.choice(customer_ids, size=n_transactions),
        "article_id": rng.choice(article_ids, size=n_transactions),
        "price": price,
        "sales_channel_id": rng.choice([1, 2], size=n_transactions, p=[0.35, 0.65]),
    })
    transactions.to_csv(os.path.join(data_dir, "transactions_train.csv"), index=False)
    return data_dir


if __name__ == "__main__":
    import sys
    make(sys.argv[1] if len(sys.argv) > 1 else "data")
