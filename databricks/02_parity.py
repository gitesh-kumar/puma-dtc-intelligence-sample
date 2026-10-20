# Databricks notebook source
# MAGIC %md
# MAGIC # Parity: Databricks against the local run
# MAGIC
# MAGIC Compares what `01_pipeline` built with the numbers of your local pipeline (`local_reference.json`).
# MAGIC
# MAGIC * Gold: all four tables, every column. Counts, rates, labels and scores must be identical. Money columns
# MAGIC   may differ by one cent, because pandas and Spark add floating point numbers in a different order.
# MAGIC * Silver: row counts and price totals per group, by quality flag, division, channel, month, ISO week,
# MAGIC   season, age group and category.
# MAGIC * Bronze: row counts after the upload.
# MAGIC * Data checks: status and observed value of all 32 checks.
# MAGIC
# MAGIC The last cell fails when anything differs. Only a run that ends in PARITY PASS counts as proof.

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "puma_dtc"
VOLUME = "landing"
LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

# COMMAND ----------

# databricks-only: makes the schema of 01_pipeline the default.
spark.sql(f"USE SCHEMA {CATALOG}.{SCHEMA}")

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.getcwd())

import dtc_reference as R
import dtc_spark as D

reference = R.load_reference(f"{LANDING}/local_reference.json")
platform = D.collect_platform(spark)
report = R.compare_reference(reference, platform)
summary = R.summarise(report)
print(f"{summary['compared']} comparisons: {summary['passed']} passed, {summary['failed']} failed")

# COMMAND ----------

# Every comparison. A column is one row, "worst_difference" is the largest gap that was found.
display(report)

# COMMAND ----------

# Keep the result next to the tables.
D.save(spark.createDataFrame(report.assign(worst_difference=report["worst_difference"].astype(float))), "parity_report")

# COMMAND ----------

failed = report[report["status"] == "FAIL"]
if len(failed):
    display(failed)
    raise AssertionError(f"PARITY FAIL: {len(failed)} of {len(report)} comparisons differ. See the table above.")
print(f"PARITY PASS: all {len(report)} comparisons agree with the local run "
      f"(Spark {spark.version}, ANSI mode {spark.conf.get('spark.sql.ansi.enabled')}, "
      f"local reference from {reference['created_at']}).")
