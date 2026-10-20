"""Data quality and reconciliation checks for the Bronze / Silver / Gold pipeline.

Run:   python checks.py [bronze|silver|gold|all]
Exit:  1 if any check FAILs, 0 otherwise. WARNs never fail the run.

Every run writes its results to the data_quality_report table, which the dashboard shows.

FAIL checks are invariants: a row count that must match, a key that must be unique, a value
range that must hold. WARN checks are thresholds on data quality. The thresholds below were
set before the checks ever saw the full dataset, so read the first real run and adjust them.
"""
import calendar
import sqlite3
import sys
from datetime import datetime

import pandas as pd

DB_PATH = "puma_dtc.db"

PASS, WARN, FAIL = "PASS", "WARN", "FAIL"
LAYERS = ("bronze", "silver", "gold")

# Thresholds ---------------------------------------------------------------
SAMPLE_FRACTION = 0.06          # must match ingest.py
SAMPLE_TOLERANCE = 0.005        # allowed drift of the realised sample fraction
MAX_UNMAPPED_SHARE = 0.05       # WARN above 5% of Silver rows in the "Other" category
MAX_INVALID_PRICE_SHARE = 0.005  # WARN above 0.5% of Silver rows with price <= 0
VALID_FLAGS = ("clean", "invalid_price", "unmapped_category")
VALID_MARKDOWN = ("CRITICAL", "HIGH", "MEDIUM", "LOW")
VALID_HEALTH = ("HEALTHY", "WATCH", "AT RISK", "CRITICAL")
GOLD_TABLES = ("gold_sell_through", "gold_markdown_risk", "gold_reorder_signals", "gold_inventory_health")


class Checker:
    def __init__(self, conn):
        self.conn = conn
        self.results = []

    def scalar(self, sql, params=()):
        return self.conn.execute(sql, params).fetchone()[0]

    def values(self, sql):
        return {row[0] for row in self.conn.execute(sql)}

    def audit(self, metric):
        row = self.conn.execute("SELECT value FROM pipeline_audit WHERE metric = ?", (metric,)).fetchone()
        if row is None:
            raise ValueError(f"pipeline_audit has no '{metric}' (re-run ingest.py)")
        return float(row[0])

    def check(self, layer, name, fn, severity=FAIL):
        """fn returns (ok, observed, expected). Any SQL error (missing table or column) is a FAIL."""
        try:
            ok, observed, expected = fn()
        except Exception as exc:  # noqa: BLE001 - a broken check must show up as a failure
            self.results.append(dict(layer=layer, check_name=name, status=FAIL,
                                     observed=f"error: {exc}", expected="check can run"))
            return
        self.results.append(dict(layer=layer, check_name=name, status=PASS if ok else severity,
                                 observed=str(observed), expected=str(expected)))


# --------------------------------------------------------------------------- Bronze
def bronze_checks(c):
    for table in ("bronze_transactions", "bronze_articles", "bronze_customers"):
        c.check("bronze", f"{table} is not empty",
                lambda t=table: ((n := c.scalar(f"SELECT COUNT(*) FROM {t}")) > 0, n, "> 0"))

    def sample_fraction():
        source, sampled = c.audit("source_transaction_rows"), c.audit("sampled_transaction_rows")
        ratio = sampled / source
        return abs(ratio - SAMPLE_FRACTION) <= SAMPLE_TOLERANCE, f"{ratio:.4f}", f"{SAMPLE_FRACTION} +/- {SAMPLE_TOLERANCE}"
    c.check("bronze", "sample fraction matches the 6% target", sample_fraction)

    def table_matches_audit(table, metric):
        def run():
            n, expected = c.scalar(f"SELECT COUNT(*) FROM {table}"), int(c.audit(metric))
            return n == expected, n, expected
        return run
    c.check("bronze", "bronze_transactions rows equal rows ingested",
            table_matches_audit("bronze_transactions", "sampled_transaction_rows"))
    c.check("bronze", "bronze_articles rows equal source rows",
            table_matches_audit("bronze_articles", "source_article_rows"))
    c.check("bronze", "bronze_customers rows equal source rows",
            table_matches_audit("bronze_customers", "source_customer_rows"))

    for table, key in (("bronze_articles", "article_id"), ("bronze_customers", "customer_id")):
        c.check("bronze", f"{table}.{key} is unique",
                lambda t=table, k=key: ((d := c.scalar(f"SELECT COUNT(*) - COUNT(DISTINCT {k}) FROM {t}")) == 0, d, 0))

    c.check("bronze", "every transaction has a matching article", lambda: (
        (n := c.scalar("""SELECT COUNT(*) FROM bronze_transactions t
                          LEFT JOIN bronze_articles a ON a.article_id = t.article_id
                          WHERE a.article_id IS NULL""")) == 0, n, 0))


# --------------------------------------------------------------------------- Silver
def silver_checks(c):
    def row_count():
        b, s = c.scalar("SELECT COUNT(*) FROM bronze_transactions"), c.scalar("SELECT COUNT(*) FROM silver_transactions")
        return b == s, s, f"{b} (Bronze)"
    c.check("silver", "no rows lost or duplicated Bronze to Silver", row_count)

    def price_sum():
        b, s = c.scalar("SELECT SUM(price) FROM bronze_transactions"), c.scalar("SELECT SUM(price) FROM silver_transactions")
        return abs(b - s) <= 1e-6 * max(abs(b), 1.0), round(s, 4), f"{round(b, 4)} (Bronze)"
    c.check("silver", "total price equals Bronze total", price_sum)

    c.check("silver", "silver_articles.article_id is unique", lambda: (
        (d := c.scalar("SELECT COUNT(*) - COUNT(DISTINCT article_id) FROM silver_articles")) == 0, d, 0))
    c.check("silver", "every transaction has a division", lambda: (
        (n := c.scalar("SELECT COUNT(*) FROM silver_transactions WHERE puma_division IS NULL")) == 0, n, 0))
    c.check("silver", "every transaction has a channel", lambda: (
        (n := c.scalar("SELECT COUNT(*) FROM silver_transactions WHERE channel IS NULL")) == 0, n, 0))

    def flag_values():
        found = c.values("SELECT DISTINCT quality_flag FROM silver_transactions")
        unexpected = sorted(found - set(VALID_FLAGS))
        return not unexpected, unexpected or "only known flags", list(VALID_FLAGS)
    c.check("silver", "quality_flag holds only known values", flag_values)

    c.check("silver", "clean rows have a positive price", lambda: (
        (n := c.scalar("""SELECT COUNT(*) FROM silver_transactions
                          WHERE quality_flag = 'clean' AND (price_eur IS NULL OR price_eur <= 0)""")) == 0, n, 0))

    def dates():
        nulls = c.scalar("SELECT COUNT(*) FROM silver_transactions WHERE t_dat IS NULL")
        newest = c.scalar("SELECT MAX(t_dat) FROM silver_transactions")
        in_past = newest is not None and str(newest)[:10] <= datetime.now().strftime("%Y-%m-%d")
        return nulls == 0 and in_past, f"{nulls} null dates, newest {str(newest)[:10]}", "no null dates, newest not in the future"
    c.check("silver", "dates are present and not in the future", dates)

    def newest_month_complete():
        newest = str(c.scalar("SELECT MAX(t_dat) FROM silver_transactions"))[:10]
        day = datetime.strptime(newest, "%Y-%m-%d")
        days_in_month = calendar.monthrange(day.year, day.month)[1]
        return (day.day == days_in_month, f"newest date {newest} ({day.day} of {days_in_month} days)",
                "a complete month, otherwise the last 3-month window is understated")
    c.check("silver", "newest month is complete", newest_month_complete, WARN)

    def share(flag, limit):
        def run():
            total = c.scalar("SELECT COUNT(*) FROM silver_transactions")
            n = c.scalar("SELECT COUNT(*) FROM silver_transactions WHERE quality_flag = ?", (flag,))
            ratio = n / total if total else 0.0
            return ratio <= limit, f"{ratio:.2%} ({n} rows)", f"<= {limit:.1%}"
        return run
    c.check("silver", "share of unmapped categories", share("unmapped_category", MAX_UNMAPPED_SHARE), WARN)
    c.check("silver", "share of invalid prices", share("invalid_price", MAX_INVALID_PRICE_SHARE), WARN)


# --------------------------------------------------------------------------- Gold
def gold_checks(c):
    for table in GOLD_TABLES:
        c.check("gold", f"{table} is not empty",
                lambda t=table: ((n := c.scalar(f"SELECT COUNT(*) FROM {t}")) > 0, n, "> 0"))

    def divisions():
        gold = c.values("SELECT puma_division FROM gold_sell_through")
        silver = c.values("SELECT DISTINCT puma_division FROM silver_transactions WHERE quality_flag = 'clean'")
        others = {t: c.values(f"SELECT puma_division FROM {t}") for t in GOLD_TABLES[1:]}
        mismatched = sorted(t for t, v in others.items() if v != gold)
        ok = gold == silver and not mismatched
        return ok, f"Gold {sorted(gold)}; tables differing from sell-through: {mismatched or 'none'}", f"Silver clean {sorted(silver)}"
    c.check("gold", "divisions match Silver and agree across Gold tables", divisions)

    def units():
        keys = [r[0] for r in c.conn.execute(
            """SELECT DISTINCT year * 100 + month AS k FROM silver_transactions
               WHERE quality_flag = 'clean' ORDER BY k DESC LIMIT 3""")]
        placeholders = ",".join("?" * len(keys))
        silver = c.scalar(f"""SELECT COUNT(*) FROM silver_transactions
                              WHERE quality_flag = 'clean' AND year * 100 + month IN ({placeholders})""", keys)
        gold = c.scalar("SELECT SUM(units_last_3m) FROM gold_sell_through")
        return gold == silver, gold, f"{silver} (clean Silver rows, last 3 months)"
    c.check("gold", "units sold in the last 3 months reconcile to Silver", units)

    c.check("gold", "sell-through rate is between 0 and 100", lambda: (
        (n := c.scalar("""SELECT COUNT(*) FROM gold_sell_through
                          WHERE sell_through_rate IS NULL OR sell_through_rate < 0 OR sell_through_rate > 100""")) == 0, n, 0))
    c.check("gold", "inventory health score is between 0 and 100", lambda: (
        (n := c.scalar("""SELECT COUNT(*) FROM gold_inventory_health
                          WHERE inventory_health_score IS NULL
                             OR inventory_health_score < 0 OR inventory_health_score > 100""")) == 0, n, 0))
    c.check("gold", "markdown cost is between 0 and inventory value", lambda: (
        (n := c.scalar("""SELECT COUNT(*) FROM gold_markdown_risk
                          WHERE estimated_markdown_cost_eur IS NULL OR estimated_markdown_cost_eur < 0
                             OR estimated_markdown_cost_eur > inventory_value_eur""")) == 0, n, 0))

    def labels():
        bad_risk = c.values("SELECT DISTINCT markdown_risk FROM gold_markdown_risk") - set(VALID_MARKDOWN)
        bad_health = c.values("SELECT DISTINCT health_label FROM gold_inventory_health") - set(VALID_HEALTH)
        return not bad_risk and not bad_health, sorted(bad_risk | bad_health) or "only known labels", "known labels"
    c.check("gold", "risk and health labels are known values", labels)

    def nulls():
        total, per_table = 0, {}
        for table in GOLD_TABLES:
            columns = [r[1] for r in c.conn.execute(f"PRAGMA table_info({table})") if r[1] != "generated_at"]
            n = sum(c.scalar(f'SELECT COUNT(*) FROM {table} WHERE "{col}" IS NULL') for col in columns)
            if n:
                per_table[table] = n
            total += n
        return total == 0, per_table or 0, 0
    c.check("gold", "no empty values in Gold tables", nulls)


STAGES = {"bronze": bronze_checks, "silver": silver_checks, "gold": gold_checks}


def run_checks(stages=LAYERS, db_path=None, write=True):
    """Run the checks for the given layers and return the results as a DataFrame."""
    conn = sqlite3.connect(db_path or DB_PATH)
    try:
        c = Checker(conn)
        for stage in stages:
            STAGES[stage](c)
        df = pd.DataFrame(c.results, columns=["layer", "check_name", "status", "observed", "expected"])
        df.insert(0, "run_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        if write:
            conn.execute("""CREATE TABLE IF NOT EXISTS data_quality_report (
                                run_at TEXT, layer TEXT, check_name TEXT, status TEXT, observed TEXT, expected TEXT)""")
            conn.executemany("DELETE FROM data_quality_report WHERE layer = ?", [(s,) for s in stages])
            conn.commit()
            df.to_sql("data_quality_report", conn, if_exists="append", index=False)
        return df
    finally:
        conn.close()


def summarize(df):
    counts = df["status"].value_counts().to_dict()
    return {PASS: counts.get(PASS, 0), WARN: counts.get(WARN, 0), FAIL: counts.get(FAIL, 0)}


def main(argv):
    stage = argv[1] if len(argv) > 1 else "all"
    if stage != "all" and stage not in STAGES:
        print(f"Unknown stage '{stage}'. Use bronze, silver, gold or all.")
        return 2
    df = run_checks(LAYERS if stage == "all" else (stage,))
    with pd.option_context("display.width", 200, "display.max_colwidth", 80):
        print(df[["layer", "status", "check_name", "observed", "expected"]].to_string(index=False))
    s = summarize(df)
    print(f"\n{s[PASS]} passed, {s[WARN]} warnings, {s[FAIL]} failed")
    return 1 if s[FAIL] else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
