import sqlite3
import pandas as pd
conn = sqlite3.connect("puma_dtc.db")
print(pd.read_sql("SELECT * FROM gold_reorder_signals", conn))
conn.close()