# Databricks notebook source
# MAGIC %md
# MAGIC # PUMA DTC pipeline on Databricks: Bronze, Silver, Gold
# MAGIC
# MAGIC Runs the same Medallion pipeline as the local project, with Spark and Delta tables.
# MAGIC Bronze is a copy of the local Bronze tables (uploaded as Parquet), Silver and Gold are rebuilt here.
# MAGIC The 32 data checks of `checks.py` run unchanged between the layers. A FAIL stops the notebook.
# MAGIC
# MAGIC Before you run it, upload the four files from `databricks_export` to the volume named below.
# MAGIC The next notebook (`02_parity`) compares the result with your local numbers.

# COMMAND ----------

# Settings. If your workspace calls its default catalog something else, change CATALOG
# (the Catalog icon in the sidebar shows the name).
CATALOG = "workspace"
SCHEMA = "puma_dtc"
VOLUME = "landing"
LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

# COMMAND ----------

# databricks-only: creates the schema and the volume for the uploads, then makes them the default.
# Run this cell first, upload the four files (see the README), then run the rest.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
spark.sql(f"USE SCHEMA {CATALOG}.{SCHEMA}")
print(f"Upload the files to {LANDING}")

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.getcwd())  # the folder of this notebook, where dtc_spark.py sits

import dtc_reference as R
import dtc_spark as D

needed = [f"{t}.parquet" for t in R.BRONZE_TABLES] + ["local_reference.json"]
missing = [f for f in needed if not os.path.exists(f"{LANDING}/{f}")]
assert not missing, f"Upload these files to {LANDING} first: {missing}"

try:
    spark.conf.set("spark.sql.session.timeZone", "UTC")  # one clock for the dates, whatever the workspace default
except Exception as exc:  # noqa: BLE001
    print("Could not set the time zone:", exc)
print("Spark", spark.version, "| ANSI mode:", spark.conf.get("spark.sql.ansi.enabled"),
      "| time zone:", spark.conf.get("spark.sql.session.timeZone"))

reference = R.load_reference(f"{LANDING}/local_reference.json")
print("Local reference written", reference["created_at"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze
# MAGIC The uploaded Parquet files become Delta tables, and `pipeline_audit` is rebuilt from the local audit numbers.
# MAGIC Checks: row counts equal the audit, keys unique, every transaction has an article.

# COMMAND ----------

report = []  # the results of all checks, written to data_quality_report at the end
counts = D.load_bronze(spark, LANDING, reference["pipeline_audit"])
print(counts)
D.gate(spark, "bronze", report)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver
# MAGIC Categories, divisions, season, demand index, channel, price in cents, quality flag.
# MAGIC Checks: no rows lost, price total equals Bronze, clean rows have a positive price, newest month complete.

# COMMAND ----------

D.build_silver(spark)
D.gate(spark, "silver", report)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold
# MAGIC Sell-through, markdown risk, reorder signals, inventory health. Built from the clean Silver rows only.
# MAGIC The stock is simulated, as in the local project.

# COMMAND ----------

D.build_gold(spark)
D.gate(spark, "gold", report)
D.save_report(spark, report)
display(spark.table("gold_inventory_health"))

# COMMAND ----------

display(spark.table("data_quality_report"))
