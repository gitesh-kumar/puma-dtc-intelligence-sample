import pandas as pd
import sqlite3

conn = sqlite3.connect("puma_dtc.db")

# Check what divisions exist
print("Divisions in silver transactions:")
print(pd.read_sql("""
    SELECT puma_division, COUNT(*) as count 
    FROM silver_transactions 
    GROUP BY puma_division
""", conn))

# Check monthly data
print("\nMonthly data sample:")
print(pd.read_sql("""
    SELECT puma_division, year, month, COUNT(*) as units
    FROM silver_transactions 
    WHERE puma_division IS NOT NULL
    GROUP BY puma_division, year, month
    LIMIT 20
""", conn))

conn.close()