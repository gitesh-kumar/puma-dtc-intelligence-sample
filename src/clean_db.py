import sqlite3

conn = sqlite3.connect("puma_dtc.db")
cursor = conn.cursor()

# Drop old gold tables that are no longer part of our pipeline
old_tables = [
    "gold_stock_risk",
    "gold_financial_risk", 
    "gold_category_performance",
    "gold_demand_forecast"
]

for table in old_tables:
    cursor.execute(f"DROP TABLE IF EXISTS {table}")
    print(f"Dropped: {table}")

conn.commit()
conn.close()
print("Done. Database cleaned.")