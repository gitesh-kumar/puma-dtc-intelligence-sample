"""Tests for checks.py: the checks must pass on a healthy database and each one must
actually fail when the thing it guards is broken (a check nobody has seen fail proves nothing)."""
import sqlite3

import pytest

from checks import FAIL, WARN, run_checks, summarize


def statuses(db, stages=("bronze", "silver", "gold")):
    df = run_checks(stages, db_path=db, write=False)
    return dict(zip(df["check_name"], df["status"]))


def damage(db, *statements):
    conn = sqlite3.connect(db)
    for sql in statements:
        conn.execute(sql)
    conn.commit()
    conn.close()


def test_healthy_database_has_no_failures_or_warnings(db_copy):
    df = run_checks(db_path=db_copy, write=False)
    assert summarize(df)[FAIL] == 0, df[df["status"] == FAIL]
    assert summarize(df)[WARN] == 0, df[df["status"] == WARN]
    assert len(df) >= 30


def test_incomplete_newest_month_raises_a_warning(db_copy):
    damage(db_copy, "DELETE FROM silver_transactions WHERE t_dat > '2020-04-15'")
    result = statuses(db_copy, ("silver",))
    assert result["newest month is complete"] == WARN


def test_results_are_written_to_the_report_table(db_copy):
    run_checks(db_path=db_copy)
    conn = sqlite3.connect(db_copy)
    n, layers = conn.execute("SELECT COUNT(*), COUNT(DISTINCT layer) FROM data_quality_report").fetchone()
    conn.close()
    assert layers == 3 and n >= 30


def test_rerunning_one_layer_replaces_only_that_layer(db_copy):
    run_checks(db_path=db_copy)
    run_checks(("gold",), db_path=db_copy)
    conn = sqlite3.connect(db_copy)
    counts = dict(conn.execute("SELECT layer, COUNT(*) FROM data_quality_report GROUP BY layer"))
    conn.close()
    assert set(counts) == {"bronze", "silver", "gold"}
    assert counts["gold"] == len([1 for _ in run_checks(("gold",), db_path=db_copy, write=False).itertuples()])


# (name of the damage, SQL that causes it, the check that must turn red)
BREAKAGES = [
    ("bronze rows deleted",
     ["DELETE FROM bronze_transactions WHERE rowid % 50 = 0"],
     "bronze_transactions rows equal rows ingested"),
    ("duplicate article in bronze",
     ["INSERT INTO bronze_articles SELECT * FROM bronze_articles LIMIT 1"],
     "bronze_articles.article_id is unique"),
    ("transaction points to unknown article",
     ["UPDATE bronze_transactions SET article_id = -1 WHERE rowid = 1"],
     "every transaction has a matching article"),
    ("silver rows lost",
     ["DELETE FROM silver_transactions WHERE rowid % 100 = 0"],
     "no rows lost or duplicated Bronze to Silver"),
    ("silver price altered",
     ["UPDATE silver_transactions SET price = price * 2 WHERE rowid = 1"],
     "total price equals Bronze total"),
    ("silver division missing",
     ["UPDATE silver_transactions SET puma_division = NULL WHERE rowid = 1"],
     "every transaction has a division"),
    ("silver channel missing",
     ["UPDATE silver_transactions SET channel = NULL WHERE rowid = 1"],
     "every transaction has a channel"),
    ("unknown quality flag",
     ["UPDATE silver_transactions SET quality_flag = 'weird' WHERE rowid = 1"],
     "quality_flag holds only known values"),
    ("clean row with zero price",
     ["UPDATE silver_transactions SET price_eur = 0 WHERE quality_flag = 'clean' AND rowid = 1"],
     "clean rows have a positive price"),
    ("date in the future",
     ["UPDATE silver_transactions SET t_dat = '2999-01-01 00:00:00' WHERE rowid = 1"],
     "dates are present and not in the future"),
    ("duplicate silver article",
     ["INSERT INTO silver_articles SELECT * FROM silver_articles LIMIT 1"],
     "silver_articles.article_id is unique"),
    ("gold sell-through out of range",
     ["UPDATE gold_sell_through SET sell_through_rate = 120 WHERE rowid = 1"],
     "sell-through rate is between 0 and 100"),
    ("gold units no longer reconcile",
     ["UPDATE gold_sell_through SET units_last_3m = units_last_3m + 1 WHERE rowid = 1"],
     "units sold in the last 3 months reconcile to Silver"),
    ("gold division missing from one table",
     ["DELETE FROM gold_markdown_risk WHERE rowid = 1"],
     "divisions match Silver and agree across Gold tables"),
    ("gold health score out of range",
     ["UPDATE gold_inventory_health SET inventory_health_score = -5 WHERE rowid = 1"],
     "inventory health score is between 0 and 100"),
    ("markdown cost above inventory value",
     ["UPDATE gold_markdown_risk SET estimated_markdown_cost_eur = inventory_value_eur * 2 + 1 WHERE rowid = 1"],
     "markdown cost is between 0 and inventory value"),
    ("unknown risk label",
     ["UPDATE gold_markdown_risk SET markdown_risk = 'SEVERE' WHERE rowid = 1"],
     "risk and health labels are known values"),
    ("empty value in gold",
     ["UPDATE gold_reorder_signals SET forecast_next_month_units = NULL WHERE rowid = 1"],
     "no empty values in Gold tables"),
]


@pytest.mark.parametrize("label, statements, expected_check", BREAKAGES, ids=[b[0] for b in BREAKAGES])
def test_check_fails_when_its_layer_is_damaged(db_copy, label, statements, expected_check):
    damage(db_copy, *statements)
    result = statuses(db_copy)
    assert result[expected_check] == FAIL, f"{label}: '{expected_check}' stayed {result[expected_check]}"


def test_many_unmapped_categories_raise_a_warning_not_a_failure(db_copy):
    damage(db_copy, "UPDATE silver_transactions SET quality_flag = 'unmapped_category' WHERE rowid % 4 = 0")
    result = statuses(db_copy, ("silver",))
    assert result["share of unmapped categories"] == WARN


def test_missing_audit_table_is_reported_as_failure_not_a_crash(db_copy):
    damage(db_copy, "DROP TABLE pipeline_audit")
    result = statuses(db_copy, ("bronze",))
    assert result["sample fraction matches the 6% target"] == FAIL
    assert result["bronze_articles rows equal source rows"] == FAIL


def test_missing_gold_table_is_reported_as_failure_not_a_crash(db_copy):
    damage(db_copy, "DROP TABLE gold_reorder_signals")
    result = statuses(db_copy, ("gold",))
    assert result["gold_reorder_signals is not empty"] == FAIL
