import pandas as pd
import sqlite3
from datetime import datetime

DB_PATH = "puma_dtc.db"

# Map H&M product groups to PUMA-equivalent categories
PUMA_CATEGORY_MAP = {
    "Garment Upper body": "Apparel - Tops",
    "Garment Lower body": "Apparel - Bottoms",
    "Garment Full body": "Apparel - Full Body",
    "Shoes": "Footwear",
    "Accessories": "Accessories",
    "Underwear": "Underwear",
    "Socks & Tights": "Socks",
    "Swimwear": "Swimwear",
    "Nightwear": "Lifestyle",
    "Underwear/nightwear": "Lifestyle",
    "Bags": "Accessories",
    "Cosmetic": "Accessories",
    "Unknown": "Other",
    "Items": "Other",
    "Furniture": "Other",
    "Garment and Shoe care": "Other",
    "Stationery": "Other",
    "Interior textile": "Other",
    "Fun": "Other"
}

# Map to PUMA sport divisions
PUMA_DIVISION_MAP = {
    "Apparel - Tops": "Training",
    "Apparel - Bottoms": "Training",
    "Apparel - Full Body": "Training",
    "Footwear": "Running",
    "Accessories": "Accessories",
    "Underwear": "Basics",
    "Socks": "Basics",
    "Swimwear": "Swim",
    "Lifestyle": "Lifestyle",
    "Other": "Other"
}

def get_season(month):
    if month in [12, 1, 2]:
        return "Winter"
    elif month in [3, 4, 5]:
        return "Spring"
    elif month in [6, 7, 8]:
        return "Summer"
    else:
        return "Autumn"

def get_demand_index(month):
    # Seasonal demand patterns for sportswear
    demand_map = {
        1: 0.7, 2: 0.75, 3: 0.9, 4: 1.0, 5: 1.1,
        6: 1.2, 7: 1.15, 8: 1.3, 9: 1.1, 10: 1.0,
        11: 1.2, 12: 1.4  # Holiday peak
    }
    return demand_map.get(month, 1.0)

def transform_silver():
    print(f"[{datetime.now()}] Starting Silver layer transformation...")
    
    conn = sqlite3.connect(DB_PATH)

    # Load bronze tables
    print("Loading bronze data...")
    transactions = pd.read_sql("SELECT * FROM bronze_transactions", conn)
    articles = pd.read_sql("SELECT * FROM bronze_articles", conn)
    customers = pd.read_sql("SELECT * FROM bronze_customers", conn)

    # --- SILVER ARTICLES ---
    print("Transforming articles...")
    silver_articles = articles[[
        "article_id", "product_code", "product_type_name",
        "product_group_name", "colour_group_name",
        "department_name", "section_name", "garment_group_name"
    ]].copy()

    silver_articles["puma_category"] = silver_articles["product_group_name"].map(PUMA_CATEGORY_MAP).fillna("Other")
    silver_articles["puma_division"] = silver_articles["puma_category"].map(PUMA_DIVISION_MAP).fillna("Other")
    silver_articles["is_footwear"] = silver_articles["puma_category"] == "Footwear"
    silver_articles["is_apparel"] = silver_articles["puma_category"].str.startswith("Apparel")
    silver_articles["transformed_at"] = datetime.now()

    silver_articles.to_sql("silver_articles", conn, if_exists="replace", index=False)
    print(f"Silver articles: {len(silver_articles)} rows")

    # --- SILVER TRANSACTIONS ---
    print("Transforming transactions...")
    silver_transactions = transactions[[
        "t_dat", "customer_id", "article_id", "price", "sales_channel_id"
    ]].copy()

    silver_transactions["t_dat"] = pd.to_datetime(silver_transactions["t_dat"])
    silver_transactions["year"] = silver_transactions["t_dat"].dt.year
    silver_transactions["month"] = silver_transactions["t_dat"].dt.month
    silver_transactions["week"] = silver_transactions["t_dat"].dt.isocalendar().week.astype(int)
    silver_transactions["season"] = silver_transactions["month"].apply(get_season)
    silver_transactions["demand_index"] = silver_transactions["month"].apply(get_demand_index)
    silver_transactions["channel"] = silver_transactions["sales_channel_id"].map({1: "store", 2: "online"})
    silver_transactions["price_eur"] = (silver_transactions["price"] * 100).round(2)

    # Join with silver articles
    silver_transactions = silver_transactions.merge(
        silver_articles[["article_id", "puma_category", "puma_division"]],
        on="article_id", how="left"
    )

    silver_transactions["quality_flag"] = "clean"
    silver_transactions.loc[silver_transactions["price"] <= 0, "quality_flag"] = "invalid_price"
    silver_transactions.loc[silver_transactions["puma_category"] == "Other", "quality_flag"] = "unmapped_category"
    silver_transactions["transformed_at"] = datetime.now()

    silver_transactions.to_sql("silver_transactions", conn, if_exists="replace", index=False)
    print(f"Silver transactions: {len(silver_transactions)} rows")

    # --- SILVER CUSTOMERS ---
    print("Transforming customers...")
    silver_customers = customers[[
        "customer_id", "age", "club_member_status", "fashion_news_frequency"
    ]].copy()

    silver_customers["age_group"] = pd.cut(
        silver_customers["age"],
        bins=[0, 25, 35, 45, 55, 100],
        labels=["18-25", "26-35", "36-45", "46-55", "55+"]
    )
    silver_customers["is_club_member"] = silver_customers["club_member_status"] == "ACTIVE"
    silver_customers["transformed_at"] = datetime.now()

    silver_customers.to_sql("silver_customers", conn, if_exists="replace", index=False)
    print(f"Silver customers: {len(silver_customers)} rows")

    conn.close()
    print(f"[{datetime.now()}] Silver layer complete.")

if __name__ == "__main__":
    transform_silver()