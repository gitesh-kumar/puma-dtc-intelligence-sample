"""The numbers from the local run, and the comparison against another platform's numbers.

Plain pandas and sqlite3, no Spark. The local machine builds a reference file from puma_dtc.db
(export_bronze.py does this), the platform run recomputes the same figures, and compare_reference()
says column by column whether they agree.

What is compared:
  gold      the four Gold tables, every column except the timestamp
  silver    row counts and price sums per group, from the SQL in FINGERPRINT_SQL (the same text runs on
            SQLite and on Spark), which covers the flag, division, channel, month, ISO week, season,
            age group and category columns without moving 1.9 million rows around
  bronze    row counts after the upload
  checks    status of all 32 data checks, and the observed values
"""
import json
import math
import sqlite3
from datetime import datetime

import numpy as np
import pandas as pd

GOLD_TABLES = ("gold_sell_through", "gold_markdown_risk", "gold_reorder_signals", "gold_inventory_health")
BRONZE_TABLES = ("bronze_transactions", "bronze_articles", "bronze_customers")
GOLD_KEY = "puma_division"
SKIP_COLUMNS = ("generated_at", "transformed_at", "ingested_at", "run_at")

# Columns that add up many floating point numbers. pandas sums with compensation and Spark does not, so the
# last digits can differ. They may differ by one cent, everything else has to match exactly.
MONEY_COLUMNS = ("revenue_last_3m", "revenue_last_6m", "avg_price_eur", "inventory_value_eur",
                 "estimated_markdown_cost_eur", "price_eur_sum")
MONEY_TOLERANCE = 0.011

# The same SQL runs on SQLite (local) and on Spark (platform).
FINGERPRINT_SQL = {
    "tx_by_group": """
        SELECT quality_flag, puma_division, channel, year, month,
               COUNT(*) AS n_rows, SUM(price_eur) AS price_eur_sum
        FROM silver_transactions
        GROUP BY quality_flag, puma_division, channel, year, month""",
    "tx_by_week": """
        SELECT year, week, season, demand_index, COUNT(*) AS n_rows
        FROM silver_transactions
        GROUP BY year, week, season, demand_index""",
    "customers": """
        SELECT age_group, is_club_member, COUNT(*) AS n_rows
        FROM silver_customers
        GROUP BY age_group, is_club_member""",
    "articles": """
        SELECT puma_category, puma_division, is_footwear, is_apparel, COUNT(*) AS n_rows
        FROM silver_articles
        GROUP BY puma_category, puma_division, is_footwear, is_apparel""",
}
FINGERPRINT_KEYS = {
    "tx_by_group": ["quality_flag", "puma_division", "channel", "year", "month"],
    "tx_by_week": ["year", "week", "season", "demand_index"],
    "customers": ["age_group", "is_club_member"],
    "articles": ["puma_category", "puma_division", "is_footwear", "is_apparel"],
}


# --------------------------------------------------------------------------- local side
def _records(df):
    df = df.drop(columns=[c for c in SKIP_COLUMNS if c in df.columns])
    return json.loads(df.to_json(orient="records"))  # NaN becomes null


def build_reference(db_path):
    """Read the local run's figures out of puma_dtc.db."""
    conn = sqlite3.connect(db_path)
    try:
        def read(sql):
            return pd.read_sql(sql, conn)

        ref = {
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "bronze_counts": _records(pd.DataFrame(
                [(t, int(conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0])) for t in BRONZE_TABLES],
                columns=["table", "n_rows"])),
            "pipeline_audit": json.loads(read("SELECT stage, metric, value FROM pipeline_audit").to_json(orient="records")),
            "gold": {t: _records(read(f"SELECT * FROM {t}")) for t in GOLD_TABLES},
            "silver": {k: _records(read(sql)) for k, sql in FINGERPRINT_SQL.items()},
            "checks": [],
        }
        try:
            ref["checks"] = _records(read("SELECT layer, check_name, status, observed, expected FROM data_quality_report"))
        except Exception:  # noqa: BLE001 - the report table may not exist yet
            pass
        return ref
    finally:
        conn.close()


def save_reference(ref, path):
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(ref, handle, indent=1, default=str)


def load_reference(path):
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


# --------------------------------------------------------------------------- comparison
def _norm(value):
    """One text form for a key value, whichever engine it came from."""
    if value is None or (isinstance(value, float) and math.isnan(value)) or value is pd.NA:
        return "<null>"
    if isinstance(value, (bool, np.bool_)):
        return str(int(value))
    if isinstance(value, (float, np.floating)) and float(value).is_integer():
        return str(int(value))
    return str(value)


def _keyed(df, keys):
    df = df.copy()
    df["_key"] = ["|".join(_norm(v) for v in row) for row in df[keys].itertuples(index=False, name=None)]
    if df["_key"].duplicated().any():
        raise ValueError(f"keys {keys} are not unique")
    return df.set_index("_key").sort_index()


def _as_numbers(series):
    try:
        return pd.to_numeric(series.astype(object).where(series.notna(), np.nan), errors="raise").astype(float)
    except (ValueError, TypeError):
        return None


def compare_frames(table, platform, local, keys):
    """One row per column: PASS when the two frames agree, FAIL with the keys that differ otherwise."""
    cols = ["table", "column", "status", "worst_difference", "detail"]
    rows = []
    a, b = _keyed(pd.DataFrame(platform), keys), _keyed(pd.DataFrame(local), keys)
    if set(a.index) != set(b.index):
        only_platform, only_local = sorted(set(a.index) - set(b.index)), sorted(set(b.index) - set(a.index))
        rows.append((table, "(rows)", "FAIL", None, f"only on the platform: {only_platform}; only local: {only_local}"))
        return pd.DataFrame(rows, columns=cols)
    a = a.reindex(b.index)
    rows.append((table, "(rows)", "PASS", 0.0, f"{len(b)} rows, same keys"))
    for column in b.columns:
        if column in SKIP_COLUMNS or column in keys:
            continue
        if column not in a.columns:
            rows.append((table, column, "FAIL", None, "column missing on the platform"))
            continue
        x, y = _as_numbers(a[column]), _as_numbers(b[column])
        if x is not None and y is not None:
            tolerance = MONEY_TOLERANCE if column in MONEY_COLUMNS else 1e-9
            diff = (x - y).abs().where(~(x.isna() & y.isna()), 0.0)
            bad = diff.isna() | (diff > tolerance + 1e-12 * y.abs().fillna(0))
            rows.append((table, column, "FAIL" if bad.any() else "PASS", None if bad.any() else float(diff.max()),
                         f"differs for {list(b.index[bad])}" if bad.any() else f"within {tolerance}"))
        else:
            same = pd.Series([_norm(p) == _norm(q) for p, q in zip(a[column], b[column])], index=b.index)
            rows.append((table, column, "PASS" if same.all() else "FAIL", None,
                         "identical" if same.all() else f"differs for {list(b.index[~same])}"))
    return pd.DataFrame(rows, columns=cols)


def _close_text(p, q):
    """Observed values are text. Numbers must agree within a cent, anything else must be identical."""
    try:
        return abs(float(p) - float(q)) <= MONEY_TOLERANCE
    except (TypeError, ValueError):
        return str(p) == str(q)


def compare_checks(platform, local):
    cols = ["table", "column", "status", "worst_difference", "detail"]
    a = pd.DataFrame(platform, columns=["layer", "check_name", "status", "observed", "expected"])
    b = pd.DataFrame(local, columns=["layer", "check_name", "status", "observed", "expected"])
    rows = []
    for row in b.itertuples():
        match = a[(a["layer"] == row.layer) & (a["check_name"] == row.check_name)]
        name = f"{row.layer}: {row.check_name}"
        if match.empty:
            rows.append(("checks", name, "FAIL", None, "check missing on the platform"))
            continue
        got = match.iloc[0]
        if got["status"] != row.status:
            rows.append(("checks", name, "FAIL", None, f"status {got['status']} on the platform, {row.status} locally"))
        elif not (_close_text(got["observed"], row.observed) and _close_text(got["expected"], row.expected)):
            rows.append(("checks", name, "FAIL", None, f"observed '{got['observed']}' vs '{row.observed}'"))
        else:
            rows.append(("checks", name, "PASS", None, f"{row.status}, same observed value"))
    for row in a.itertuples():
        if b[(b["layer"] == row.layer) & (b["check_name"] == row.check_name)].empty:
            rows.append(("checks", f"{row.layer}: {row.check_name}", "FAIL", None, "check not in the local run"))
    return pd.DataFrame(rows, columns=cols)


def compare_reference(reference, platform):
    """reference is load_reference(); platform has the same shape with pandas frames:
    {"bronze_counts": df, "gold": {name: df}, "silver": {name: df}, "checks": df}.
    Returns a DataFrame with one row per column compared."""
    parts = [compare_frames("bronze_counts", platform["bronze_counts"], reference["bronze_counts"], ["table"])]
    for name in GOLD_TABLES:
        parts.append(compare_frames(name, platform["gold"][name], reference["gold"][name], [GOLD_KEY]))
    for name, keys in FINGERPRINT_KEYS.items():
        parts.append(compare_frames(f"silver:{name}", platform["silver"][name], reference["silver"][name], keys))
    if reference["checks"]:
        parts.append(compare_checks(platform["checks"], reference["checks"]))
    else:
        parts.append(pd.DataFrame([("checks", "(local report)", "FAIL", None,
                                    "the local run has no data_quality_report, so the checks cannot be compared. "
                                    "Run python src\\run_pipeline.py --skip-ingest and export again")],
                                  columns=["table", "column", "status", "worst_difference", "detail"]))
    return pd.concat(parts, ignore_index=True)


def summarise(report):
    n_fail = int((report["status"] == "FAIL").sum())
    return {"compared": len(report), "failed": n_fail, "passed": len(report) - n_fail}

