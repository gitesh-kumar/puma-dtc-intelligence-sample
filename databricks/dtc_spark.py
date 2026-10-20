"""Silver and Gold of the DTC pipeline in PySpark: a port of transform.py and analytics.py.

The logic is the same, the engine is Spark. Where pandas and Spark behave differently by default, the port
follows pandas, because the point is to get the same numbers:

* pandas rounds half to even after scaling (numpy.round), Spark's round() rounds half up.  _round() uses rint.
* pandas drops rows whose group key is missing, Spark keeps them as a group of their own.  The group-bys filter.
* pandas clip() keeps a missing value, Spark's greatest()/least() skip it.  _clip() guards.
* Spark can run in ANSI mode, which Databricks serverless does by default.  Division by zero raises an error,
  a missing map key raises an error, and a bad cast raises an error.  Every division is guarded and lookups
  use CASE WHEN, never map[key].

The data checks are not ported. checks.py runs unchanged against Spark through SparkConn, so the 32 checks
cannot drift from the local ones.

Two copies of the same logic can drift apart. tests/test_spark_parity.py runs both on the same data and fails
when a number differs. Run it after every change to transform.py or analytics.py.
"""
import importlib
import os
import re
import sys
from datetime import datetime
from functools import reduce

import pandas as pd
from pyspark.sql import functions as F

from dtc_reference import BRONZE_TABLES, FINGERPRINT_SQL, GOLD_TABLES

HERE = os.path.dirname(os.path.abspath(__file__))

# --------------------------------------------------------------------------- mappings (copied from transform.py)
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
    "Fun": "Other",
}
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
    "Other": "Other",
}
DEMAND_INDEX = {1: 0.7, 2: 0.75, 3: 0.9, 4: 1.0, 5: 1.1, 6: 1.2, 7: 1.15, 8: 1.3, 9: 1.1, 10: 1.0, 11: 1.2, 12: 1.4}

# --------------------------------------------------------------------------- mappings (copied from analytics.py)
# Simulated stock: hand-picked multipliers, not data. Everything downstream inherits them.
INVENTORY_SCENARIOS = {"Running": 1.5, "Training": 3.5, "Swim": 4.0, "Basics": 0.8,
                       "Accessories": 0.4, "Lifestyle": 0.3, "Other": 1.2}
SEASONAL_FACTORS = DEMAND_INDEX  # analytics.py repeats the same twelve numbers
MARKDOWN_DISCOUNT = {"CRITICAL": 0.35, "HIGH": 0.25, "MEDIUM": 0.15, "LOW": 0.0}
REORDER_PRIORITY = {"URGENT - Order immediately": 4, "SOON - Order within 2 weeks": 3,
                    "PLAN - Review in 30 days": 2, "OK - Sufficient stock": 1}
MARKDOWN_SCORE = {"LOW": 100, "MEDIUM": 70, "HIGH": 40, "CRITICAL": 10}


# --------------------------------------------------------------------------- helpers
def _lookup(column, mapping):
    """dict.get() as a CASE WHEN: the mapped value, or null when the key is missing (also in ANSI mode)."""
    col = F.col(column) if isinstance(column, str) else column
    branches = [(col == F.lit(key), F.lit(value)) for key, value in mapping.items()]
    expr = F.when(*branches[0])
    return reduce(lambda acc, branch: acc.when(*branch), branches[1:], expr)


def _round(column, decimals):
    """numpy.round / pandas .round(): scale, round half to even, scale back."""
    scale = F.lit(float(10 ** decimals))
    return F.rint(column * scale) / scale


def _clip(column, low, high):
    """pandas .clip(): a missing value stays missing."""
    clipped = F.least(F.greatest(column, F.lit(float(low))), F.lit(float(high)))
    return F.when(column.isNull(), F.lit(None).cast("double")).otherwise(clipped)


# --------------------------------------------------------------------------- Silver
def silver_articles(bronze_articles):
    df = bronze_articles.select("article_id", "product_code", "product_type_name", "product_group_name",
                                "colour_group_name", "department_name", "section_name", "garment_group_name")
    df = df.withColumn("puma_category", F.coalesce(_lookup("product_group_name", PUMA_CATEGORY_MAP), F.lit("Other")))
    df = df.withColumn("puma_division", F.coalesce(_lookup("puma_category", PUMA_DIVISION_MAP), F.lit("Other")))
    return (df.withColumn("is_footwear", F.col("puma_category") == "Footwear")
              .withColumn("is_apparel", F.col("puma_category").startswith("Apparel"))
              .withColumn("transformed_at", F.current_timestamp()))


def silver_transactions(bronze_transactions, articles):
    t = bronze_transactions.select("t_dat", "customer_id", "article_id", "price", "sales_channel_id")
    t = (t.withColumn("t_dat", F.to_timestamp("t_dat"))
          .withColumn("year", F.year("t_dat"))
          .withColumn("month", F.month("t_dat"))
          .withColumn("week", F.weekofyear("t_dat")))  # ISO week, like pandas isocalendar()
    month = F.col("month")
    t = (t.withColumn("season", F.when(month.isin(12, 1, 2), "Winter").when(month.isin(3, 4, 5), "Spring")
                                 .when(month.isin(6, 7, 8), "Summer").otherwise("Autumn"))
          .withColumn("demand_index", F.coalesce(_lookup(month, DEMAND_INDEX), F.lit(1.0)))
          .withColumn("channel", _lookup(F.col("sales_channel_id"), {1: "store", 2: "online"}))
          .withColumn("price_eur", _round(F.col("price") * F.lit(100.0), 2)))
    t = t.join(F.broadcast(articles.select("article_id", "puma_category", "puma_division")), on="article_id", how="left")
    # An unmapped category wins over an invalid price, as in transform.py. A missing price is invalid.
    t = t.withColumn("quality_flag",
                     F.when(F.col("puma_category") == "Other", "unmapped_category")
                      .when(~F.coalesce(F.col("price_eur") > 0, F.lit(False)), "invalid_price")
                      .otherwise("clean"))
    return t.select("t_dat", "customer_id", "article_id", "price", "sales_channel_id", "year", "month", "week",
                    "season", "demand_index", "channel", "price_eur", "puma_category", "puma_division",
                    "quality_flag").withColumn("transformed_at", F.current_timestamp())


def silver_customers(bronze_customers):
    df = bronze_customers.select("customer_id", "age", "club_member_status", "fashion_news_frequency")
    age = F.col("age")
    return (df.withColumn("age_group",
                          F.when((age > 0) & (age <= 25), "18-25").when((age > 25) & (age <= 35), "26-35")
                           .when((age > 35) & (age <= 45), "36-45").when((age > 45) & (age <= 55), "46-55")
                           .when((age > 55) & (age <= 100), "55+"))
              .withColumn("is_club_member", F.coalesce(F.col("club_member_status") == "ACTIVE", F.lit(False)))
              .withColumn("transformed_at", F.current_timestamp()))


# --------------------------------------------------------------------------- Gold
def gold_tables(silver_tx):
    """The four Gold tables as a dict of DataFrames, built from the clean Silver rows (as analytics.py)."""
    clean = (silver_tx.filter(F.col("quality_flag") == "clean")
                      .withColumn("date_key", F.col("year") * 100 + F.col("month")))
    keys = sorted(r[0] for r in clean.select("date_key").distinct().collect())
    if not keys:
        raise ValueError("No clean rows in silver_transactions. Check the Silver layer first.")
    last_3, last_6 = keys[-3:], keys[-6:]
    by_division = clean.filter(F.col("puma_division").isNotNull())  # pandas groupby drops missing keys

    # ---- GOLD 1: sell-through
    recent = (by_division.filter(F.col("date_key").isin(last_3)).groupBy("puma_division")
              .agg(F.count("article_id").alias("units_last_3m"), F.sum("price_eur").alias("revenue_last_3m"),
                   F.countDistinct("article_id").alias("unique_products_recent")))
    history = (by_division.filter(F.col("date_key").isin(last_6)).groupBy("puma_division")
               .agg(F.count("article_id").alias("units_last_6m"), F.sum("price_eur").alias("revenue_last_6m")))
    st = recent.join(history, on="puma_division", how="left")
    st = st.withColumn("avg_monthly_recent", F.col("units_last_3m") / F.lit(3.0))
    st = st.withColumn("avg_monthly_prior", (F.col("units_last_6m") - F.col("units_last_3m")) / F.lit(3.0))
    prior = F.col("avg_monthly_prior")
    st = st.withColumn("velocity_change_pct", _round(
        F.when(prior == 0, F.lit(None).cast("double"))
         .otherwise((F.col("avg_monthly_recent") - prior) / prior * F.lit(100.0)), 2))
    st = st.withColumn("stock_multiplier", F.coalesce(_lookup("puma_division", INVENTORY_SCENARIOS), F.lit(1.0)))
    st = st.withColumn("simulated_stock_units",
                       (F.col("avg_monthly_recent") * F.col("stock_multiplier") * F.lit(4.0)).cast("long"))
    st = st.withColumn("total_available", F.col("units_last_3m") + F.col("simulated_stock_units"))
    st = st.withColumn("sell_through_rate", _round(
        F.when(F.col("total_available") == 0, F.lit(None).cast("double"))
         .otherwise(F.col("units_last_3m") / F.col("total_available") * F.lit(100.0)), 2))
    rate = F.col("sell_through_rate")
    st = st.withColumn("sell_through_status", F.when(rate >= 75, "EXCELLENT").when(rate >= 50, "GOOD")
                                               .when(rate >= 30, "SLOW").otherwise("POOR"))
    st = st.select("puma_division", "units_last_3m", "revenue_last_3m", "unique_products_recent", "units_last_6m",
                   "revenue_last_6m", "avg_monthly_recent", "avg_monthly_prior", "velocity_change_pct",
                   "stock_multiplier", "simulated_stock_units", "total_available", "sell_through_rate",
                   "sell_through_status").withColumn("generated_at", F.current_timestamp())

    # ---- GOLD 2: markdown risk
    md = st.select("puma_division", "sell_through_rate", "velocity_change_pct", "simulated_stock_units",
                   "avg_monthly_recent")
    avg_recent = F.col("avg_monthly_recent")
    md = md.withColumn("weeks_of_stock", _round(
        F.when(avg_recent > 0, F.col("simulated_stock_units") / (avg_recent / F.lit(4.3))), 1))
    vel = F.col("velocity_change_pct")
    md = md.withColumn("markdown_risk",
                       F.when((rate < 30) & (vel < -10), "CRITICAL").when((rate < 40) & (vel < 0), "HIGH")
                        .when(rate < 50, "MEDIUM").otherwise("LOW"))
    avg_price = by_division.groupBy("puma_division").agg(F.mean("price_eur").alias("avg_price_eur"))
    md = md.join(avg_price, on="puma_division", how="left")
    md = md.withColumn("inventory_value_eur", _round(F.col("simulated_stock_units") * F.col("avg_price_eur"), 2))
    md = md.withColumn("estimated_markdown_cost_eur", _round(
        F.col("inventory_value_eur") * _lookup("markdown_risk", MARKDOWN_DISCOUNT), 2))
    md = md.select("puma_division", "sell_through_rate", "velocity_change_pct", "simulated_stock_units",
                   "avg_monthly_recent", "weeks_of_stock", "markdown_risk", "avg_price_eur", "inventory_value_eur",
                   "estimated_markdown_cost_eur").withColumn("generated_at", F.current_timestamp())

    # ---- GOLD 3: reorder signals
    current_month = keys[-1] % 100
    next_month = current_month + 1 if current_month < 12 else 1
    next_factor = SEASONAL_FACTORS.get(next_month, 1.0)
    ro = md.select("puma_division", "simulated_stock_units", "avg_monthly_recent", "weeks_of_stock")
    ro = ro.withColumn("forecast_next_month_units",
                       _round(F.col("avg_monthly_recent") * F.lit(next_factor), 0).cast("long"))
    ro = ro.withColumn("stock_after_next_month", F.col("simulated_stock_units") - F.col("forecast_next_month_units"))
    weeks = F.col("weeks_of_stock")
    ro = ro.withColumn("reorder_signal",
                       F.when(weeks < 4, "URGENT - Order immediately").when(weeks < 8, "SOON - Order within 2 weeks")
                        .when(F.col("stock_after_next_month") < F.col("forecast_next_month_units"),
                              "PLAN - Review in 30 days")
                        .otherwise("OK - Sufficient stock"))
    ro = ro.withColumn("reorder_priority", _lookup("reorder_signal", REORDER_PRIORITY))
    ro = ro.withColumn("generated_at", F.current_timestamp())

    # ---- GOLD 4: inventory health
    h = st.select("puma_division", "sell_through_rate", "velocity_change_pct")
    h = h.join(md.select("puma_division", "markdown_risk", "weeks_of_stock"), on="puma_division")
    h = h.join(ro.select("puma_division", "reorder_priority"), on="puma_division")
    h = h.withColumn("sell_through_score", _clip(F.col("sell_through_rate"), 0, 100))
    h = h.withColumn("velocity_score", _clip(F.lit(50.0) + _clip(F.col("velocity_change_pct"), -50, 50), 0, 100))
    w = F.col("weeks_of_stock")
    h = h.withColumn("weeks_score",
                     F.when(w.between(8, 16), 100)
                      .when(((w >= 4) & (w < 8)) | ((w > 16) & (w <= 24)), 70)
                      .when(((w >= 2) & (w < 4)) | ((w > 24) & (w <= 32)), 40).otherwise(10))
    h = h.withColumn("markdown_score", _lookup("markdown_risk", MARKDOWN_SCORE))
    h = h.withColumn("inventory_health_score", _round(
        F.col("sell_through_score") * F.lit(0.30) + F.col("velocity_score") * F.lit(0.25)
        + F.col("weeks_score") * F.lit(0.25) + F.col("markdown_score") * F.lit(0.20), 1))
    score = F.col("inventory_health_score")
    h = h.withColumn("health_label", F.when(score >= 80, "HEALTHY").when(score >= 60, "WATCH")
                                      .when(score >= 40, "AT RISK").otherwise("CRITICAL"))
    h = h.select("puma_division", "sell_through_rate", "velocity_change_pct", "markdown_risk", "weeks_of_stock",
                 "reorder_priority", "sell_through_score", "velocity_score", "weeks_score", "markdown_score",
                 "inventory_health_score", "health_label").withColumn("generated_at", F.current_timestamp())

    return {"gold_sell_through": st, "gold_markdown_risk": md, "gold_reorder_signals": ro, "gold_inventory_health": h}


# --------------------------------------------------------------------------- tables
def save(df, name):
    """Write a table, replacing the old one. Delta on Databricks, Parquet in a plain local Spark."""
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(name)


def load_bronze(spark, folder, audit_rows):
    """Bronze is a copy of the local Bronze tables, uploaded as Parquet. Returns the row counts."""
    for table in BRONZE_TABLES:
        save(spark.read.parquet(os.path.join(folder, f"{table}.parquet")), table)
    audit = pd.DataFrame(audit_rows, columns=["stage", "metric", "value"])
    audit["value"] = audit["value"].astype(float)
    audit["recorded_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    save(spark.createDataFrame(audit), "pipeline_audit")
    return {t: spark.table(t).count() for t in BRONZE_TABLES}


def build_silver(spark):
    articles = silver_articles(spark.table("bronze_articles"))
    save(articles, "silver_articles")
    articles = spark.table("silver_articles")
    save(silver_transactions(spark.table("bronze_transactions"), articles), "silver_transactions")
    save(silver_customers(spark.table("bronze_customers")), "silver_customers")


def build_gold(spark):
    for name, df in gold_tables(spark.table("silver_transactions")).items():
        save(df, name)


# --------------------------------------------------------------------------- data checks, unchanged from checks.py
class PipelineFailed(Exception):
    """A layer failed its data checks, so the next layer must not be built from it."""


def _checks_module():
    """checks.py of the project. It sits in ../src next to this folder, or next to this file."""
    for folder in (HERE, os.path.join(HERE, "..", "src"), os.path.join(HERE, "..")):
        if os.path.exists(os.path.join(folder, "checks.py")):
            folder = os.path.abspath(folder)
            if folder not in sys.path:
                sys.path.insert(0, folder)
            return importlib.import_module("checks")
    raise ImportError("checks.py not found next to dtc_spark.py or in ../src")


class _Rows(list):
    def fetchone(self):
        return self[0] if self else None

    def fetchall(self):
        return list(self)


def _literal(value):
    if isinstance(value, str):
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"
    return repr(value.item() if hasattr(value, "item") else value)


def _to_spark_sql(sql, params):
    # SQLite quotes names with "double quotes", in Spark that is a string. Names get backticks.
    sql = re.sub(r'"([^"]*)"', lambda m: f"`{m.group(1)}`", sql)
    parts = sql.split("?")
    if len(parts) - 1 != len(params):
        raise ValueError(f"{len(parts) - 1} placeholders but {len(params)} values")
    return parts[0] + "".join(_literal(p) + part for p, part in zip(params, parts[1:]))


class SparkConn:
    """Just enough of a sqlite3 connection for checks.Checker, so the checks run unchanged on Spark."""

    def __init__(self, spark):
        self.spark = spark

    def execute(self, sql, params=()):
        pragma = re.match(r"\s*PRAGMA\s+table_info\((\w+)\)\s*;?\s*$", sql, re.IGNORECASE)
        if pragma:
            columns = self.spark.table(pragma.group(1)).columns
            return _Rows((i, name, "", 0, None, 0) for i, name in enumerate(columns))
        return _Rows(tuple(row) for row in self.spark.sql(_to_spark_sql(sql, list(params))).collect())


def run_checks(spark, stages):
    """Run the checks of the given layers and return the results as a DataFrame (as checks.run_checks)."""
    checks = _checks_module()
    checker = checks.Checker(SparkConn(spark))
    for stage in stages:
        checks.STAGES[stage](checker)
    df = pd.DataFrame(checker.results, columns=["layer", "check_name", "status", "observed", "expected"])
    df.insert(0, "run_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    return df


def gate(spark, stage, report):
    """Check one layer, add the results to `report` (a list of DataFrames), stop on a FAIL like run_pipeline.py."""
    df = run_checks(spark, (stage,))
    report.append(df)
    counts = df["status"].value_counts().to_dict()
    print(f"[{stage}] checks: {counts.get('PASS', 0)} passed, {counts.get('WARN', 0)} warnings, "
          f"{counts.get('FAIL', 0)} failed")
    for row in df[df["status"] != "PASS"].itertuples():
        print(f"    {row.status}  {row.check_name}: observed {row.observed}, expected {row.expected}")
    if counts.get("FAIL", 0):
        save_report(spark, report)
        raise PipelineFailed(f"The {stage} layer failed its checks.")


def save_report(spark, report):
    if report:
        save(spark.createDataFrame(pd.concat(report, ignore_index=True)), "data_quality_report")


# --------------------------------------------------------------------------- what the parity notebook compares
def collect_platform(spark):
    """The same figures as dtc_reference.build_reference(), taken from the tables on this platform."""
    def frame(name):
        df = spark.table(name).toPandas()
        return df.drop(columns=[c for c in ("generated_at", "transformed_at", "ingested_at") if c in df.columns])

    return {
        "bronze_counts": pd.DataFrame([(t, int(spark.table(t).count())) for t in BRONZE_TABLES],
                                      columns=["table", "n_rows"]),
        "gold": {name: frame(name) for name in GOLD_TABLES},
        "silver": {name: spark.sql(sql).toPandas() for name, sql in FINGERPRINT_SQL.items()},
        "checks": spark.table("data_quality_report").toPandas()[["layer", "check_name", "status", "observed", "expected"]],
    }
