import sqlite3
import pandas as pd

conn = sqlite3.connect("puma_dtc.db")

# Check row counts
print("Transactions:", pd.read_sql("SELECT COUNT(*) as count FROM bronze_transactions", conn).iloc[0,0])
print("Articles:", pd.read_sql("SELECT COUNT(*) as count FROM bronze_articles", conn).iloc[0,0])
print("Customers:", pd.read_sql("SELECT COUNT(*) as count FROM bronze_customers", conn).iloc[0,0])

# Preview transactions
print("\nTransactions sample:")
print(pd.read_sql("SELECT * FROM bronze_transactions LIMIT 3", conn))

# Check article categories
print("\nProduct groups:")
print(pd.read_sql("SELECT product_group_name, COUNT(*) as count FROM bronze_articles GROUP BY product_group_name ORDER BY count DESC", conn))

conn.close()