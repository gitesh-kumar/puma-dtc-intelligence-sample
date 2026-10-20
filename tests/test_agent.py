"""Tests for the agent's guard rails. The language model is replaced by a scripted stub, so
these tests prove the safety and grounding logic, not the quality of the model's SQL."""
import json
import sqlite3

import pandas as pd
import pytest

from agent import query_agent as qa


@pytest.fixture()
def agent_db(db_copy, monkeypatch):
    monkeypatch.setattr(qa, "DB_PATH", db_copy)
    return db_copy


def script_model(monkeypatch, replies):
    """Make _chat return the given replies in order."""
    queue = iter(replies)
    calls = []

    def fake_chat(system, user, temperature, max_tokens=300):
        calls.append(user)
        return next(queue)

    monkeypatch.setattr(qa, "_chat", fake_chat)
    return calls


# --------------------------------------------------------------------------- validate_sql
@pytest.mark.parametrize("sql", [
    "SELECT * FROM gold_markdown_risk",
    "select puma_division from gold_sell_through where sell_through_rate < 30;",
    "WITH w AS (SELECT * FROM gold_reorder_signals) SELECT puma_division FROM w",
    "SELECT a.puma_division FROM gold_sell_through a JOIN gold_markdown_risk b ON a.puma_division = b.puma_division",
    "SELECT * FROM gold_markdown_risk -- trailing comment",
    "SELECT * FROM gold_markdown_risk WHERE puma_division = 'UPDATE'",
])
def test_valid_queries_are_accepted(sql):
    assert qa.validate_sql(sql)


@pytest.mark.parametrize("sql, reason", [
    ("", "empty"),
    ("DROP TABLE gold_markdown_risk", "only SELECT"),
    ("DELETE FROM gold_markdown_risk", "only SELECT"),
    ("UPDATE gold_markdown_risk SET markdown_risk = 'LOW'", "only SELECT"),
    ("INSERT INTO gold_markdown_risk SELECT * FROM gold_markdown_risk", "only SELECT"),
    ("SELECT 1; DROP TABLE gold_markdown_risk", "one statement"),
    ("SELECT * FROM gold_markdown_risk; SELECT * FROM gold_sell_through", "one statement"),
    ("ATTACH DATABASE 'x.db' AS x", "only SELECT"),
    ("PRAGMA table_info(gold_markdown_risk)", "only SELECT"),
    ("SELECT * FROM silver_transactions", "not a Gold table"),
    ("SELECT * FROM sqlite_master", "not a Gold table"),
    ("SELECT * FROM gold_sell_through JOIN bronze_customers ON 1=1", "not a Gold table"),
    ("WITH x AS (SELECT 1) DELETE FROM gold_markdown_risk", "DELETE"),
])
def test_unsafe_queries_are_rejected(sql, reason):
    with pytest.raises(qa.SQLRejected, match=reason):
        qa.validate_sql(sql)


# --------------------------------------------------------------------------- the database itself refuses
def test_connection_refuses_writes_even_if_validation_is_skipped(agent_db):
    conn = qa.connect_readonly()
    for sql in ("DROP TABLE gold_markdown_risk",
                "DELETE FROM gold_markdown_risk",
                "UPDATE gold_markdown_risk SET markdown_risk = 'LOW'",
                "CREATE TABLE evil (x INTEGER)"):
        with pytest.raises(sqlite3.DatabaseError):
            conn.execute(sql)
    conn.close()
    check = sqlite3.connect(agent_db)
    assert check.execute("SELECT COUNT(*) FROM gold_markdown_risk").fetchone()[0] > 0
    check.close()


def test_connection_refuses_to_read_non_gold_tables(agent_db):
    conn = qa.connect_readonly()
    for table in ("silver_transactions", "bronze_customers", "sqlite_master", "data_quality_report"):
        with pytest.raises(sqlite3.DatabaseError):
            conn.execute(f"SELECT * FROM {table}").fetchall()
    conn.close()


def test_run_sql_caps_rows_and_reports_truncation(agent_db):
    df, truncated = qa.run_sql("SELECT a.puma_division FROM gold_sell_through a, gold_sell_through b, gold_sell_through c")
    assert len(df) == qa.MAX_ROWS and truncated
    df, truncated = qa.run_sql("SELECT puma_division FROM gold_sell_through")
    assert len(df) == 6 and not truncated


def test_slow_query_is_interrupted(agent_db, monkeypatch):
    monkeypatch.setattr(qa, "QUERY_TIMEOUT_S", 0.2)
    tables = ", ".join(f"gold_sell_through t{i}" for i in range(12))
    with pytest.raises(sqlite3.OperationalError, match="interrupted"):
        qa.run_sql(f"SELECT COUNT(*) FROM {tables}")


# --------------------------------------------------------------------------- number grounding
RESULT = pd.DataFrame({
    "puma_division": ["Swim", "Training"],
    "estimated_markdown_cost_eur": [3016.39, 16907.05],
    "weeks_of_stock": [68.8, 60.2],
    "share": [0.43, 0.12],
})


@pytest.mark.parametrize("answer", [
    "Training carries EUR 16,907 of markdown risk.",
    "Training carries about 16.9k of markdown risk.",
    "Swim holds 68.8 weeks of simulated stock.",
    "Swim holds 69 weeks of simulated stock.",
    "Both of the 2 divisions are above 60 weeks.",
    "Swim is 43% of the total.",
    "In 2026 Swim stays flagged.",
    "Markdown cost is 3,016.39 for Swim.",
])
def test_numbers_that_trace_to_the_result_pass(answer):
    assert qa.ungrounded_numbers(answer, RESULT) == []


@pytest.mark.parametrize("answer, bad", [
    ("Training carries 17,500 of markdown risk.", ["17,500"]),
    ("Swim holds 70 weeks of stock.", ["70"]),
    ("Training could lose 8% more margin.", ["8"]),
    ("Markdown cost is 1.2m in total.", ["1.2m"]),
])
def test_invented_numbers_are_flagged(answer, bad):
    assert qa.ungrounded_numbers(answer, RESULT) == bad


def test_numbers_from_the_question_and_sql_are_allowed():
    df = pd.DataFrame({"puma_division": ["Swim"], "weeks_of_stock": [68.8]})
    answer = "Swim has more than 8 weeks of stock."
    assert qa.ungrounded_numbers(answer, df) == ["8"]
    assert qa.ungrounded_numbers(answer, df, question="Which divisions have more than 8 weeks of stock?") == []


# --------------------------------------------------------------------------- ask() end to end, model scripted
GOOD_SQL = "```sql\nSELECT puma_division, estimated_markdown_cost_eur FROM gold_markdown_risk ORDER BY estimated_markdown_cost_eur DESC LIMIT 1\n```"


def top_cost(agent_db):
    conn = sqlite3.connect(agent_db)
    division, cost = conn.execute(
        "SELECT puma_division, estimated_markdown_cost_eur FROM gold_markdown_risk "
        "ORDER BY estimated_markdown_cost_eur DESC LIMIT 1").fetchone()
    conn.close()
    return division, cost


def test_ask_returns_answer_sql_rows_and_verification(agent_db, monkeypatch):
    division, cost = top_cost(agent_db)
    script_model(monkeypatch, [GOOD_SQL, f"{division} has the highest simulated markdown cost at {cost:,.2f}. Review its stock plan."])
    out = qa.ask("Which division has the highest markdown cost?")
    assert out["verified"] and out["ungrounded"] == [] and out["error"] is None
    assert out["rows"][0][0] == division
    assert "SELECT" in out["sql"]
    json.dumps(out)  # the API response must be JSON-safe


def test_ask_repairs_a_rejected_query_once(agent_db, monkeypatch):
    division, cost = top_cost(agent_db)
    calls = script_model(monkeypatch, [
        "```sql\nDROP TABLE gold_markdown_risk\n```",
        GOOD_SQL,
        f"{division} leads at {cost:,.2f}.",
    ])
    out = qa.ask("Which division has the highest markdown cost?")
    assert out["error"] is None and out["verified"]
    assert "only SELECT" in calls[1]  # the model was shown why its first query was rejected
    conn = sqlite3.connect(agent_db)
    assert conn.execute("SELECT COUNT(*) FROM gold_markdown_risk").fetchone()[0] > 0
    conn.close()


def test_ask_gives_up_safely_when_every_query_is_rejected(agent_db, monkeypatch):
    script_model(monkeypatch, ["```sql\nDELETE FROM gold_markdown_risk\n```"] * qa.MAX_SQL_ATTEMPTS)
    out = qa.ask("Delete everything")
    assert out["error"] and not out["verified"] and out["rows"] == []
    assert "could not" in out["answer"]
    conn = sqlite3.connect(agent_db)
    assert conn.execute("SELECT COUNT(*) FROM gold_markdown_risk").fetchone()[0] > 0
    conn.close()


def test_ask_rewrites_an_answer_with_invented_numbers(agent_db, monkeypatch):
    division, cost = top_cost(agent_db)
    calls = script_model(monkeypatch, [
        GOOD_SQL,
        f"{division} is at {cost:,.2f}, which is 777% above target.",
        f"{division} is at {cost:,.2f}.",
    ])
    out = qa.ask("Which division has the highest markdown cost?")
    assert out["verified"] and "777" not in out["answer"]
    assert "777" in calls[-1]  # the rewrite request named the offending number


def test_ask_marks_an_answer_unverified_when_the_rewrite_still_invents(agent_db, monkeypatch):
    division, cost = top_cost(agent_db)
    script_model(monkeypatch, [GOOD_SQL, "It is 777 percent above target.", "It is 555 percent above target."])
    out = qa.ask("Which division has the highest markdown cost?")
    assert not out["verified"] and out["ungrounded"] == ["555"]


def test_empty_result_is_reported_without_asking_the_model_to_explain(agent_db, monkeypatch):
    calls = script_model(monkeypatch, ["```sql\nSELECT * FROM gold_markdown_risk WHERE puma_division = 'Nope'\n```"])
    out = qa.ask("Show Nope")
    assert out["answer"] == "The query returned no rows." and out["verified"]
    assert len(calls) == 1


# --------------------------------------------------------------------------- the authorizer works on its own
def test_authorizer_alone_blocks_writes_and_foreign_tables(agent_db):
    """Open the file read-write so only the authorizer stands in the way."""
    conn = sqlite3.connect(agent_db)
    conn.set_authorizer(qa._authorizer)
    for sql in ("DELETE FROM gold_markdown_risk",
                "DROP TABLE gold_markdown_risk",
                "UPDATE gold_markdown_risk SET markdown_risk = 'LOW'",
                "SELECT * FROM silver_transactions",
                "SELECT * FROM sqlite_master"):
        with pytest.raises(sqlite3.DatabaseError, match="not authorized|prohibited"):
            conn.execute(sql).fetchall()
    assert conn.execute("SELECT COUNT(*) FROM gold_markdown_risk").fetchone()[0] > 0
    conn.close()


@pytest.mark.parametrize("sql", [
    "WITH w AS (SELECT * FROM gold_reorder_signals) SELECT COUNT(*) FROM w",
    "SELECT a.puma_division, ROUND(b.estimated_markdown_cost_eur, 1) AS cost FROM gold_sell_through a "
    "JOIN gold_markdown_risk b ON a.puma_division = b.puma_division ORDER BY cost DESC LIMIT 3",
    "SELECT puma_division FROM gold_inventory_health WHERE inventory_health_score < (SELECT AVG(inventory_health_score) FROM gold_inventory_health)",
    "SELECT COALESCE(MAX(weeks_of_stock), 0), MIN(weeks_of_stock), AVG(weeks_of_stock) FROM gold_markdown_risk",
    "SELECT markdown_risk, COUNT(*) FROM gold_markdown_risk GROUP BY markdown_risk HAVING COUNT(*) >= 1",
])
def test_legitimate_gold_queries_still_run(agent_db, sql):
    df, truncated = qa.run_sql(sql)
    assert len(df) >= 1 and not truncated


# --------------------------------------------------------------------------- model unavailable
def test_missing_model_returns_an_error_instead_of_crashing(agent_db, monkeypatch):
    def broken_client():
        raise ModuleNotFoundError("No module named 'groq'")

    monkeypatch.setattr(qa, "get_client", broken_client)
    out = qa.ask("Which division has the highest markdown cost?")
    assert out["error"] and "could not be reached" in out["error"]
    assert out["verified"] is False and out["rows"] == []


def test_model_failure_while_explaining_keeps_the_query_result(agent_db, monkeypatch):
    replies = iter(["```sql\nSELECT puma_division FROM gold_markdown_risk LIMIT 1\n```"])

    def flaky_chat(system, user, temperature, max_tokens=300):
        try:
            return next(replies)
        except StopIteration:
            raise qa.ModelError("RateLimitError: slow down")

    monkeypatch.setattr(qa, "_chat", flaky_chat)
    out = qa.ask("Which division is first?")
    assert out["error"] and "RateLimitError" in out["error"]
    assert out["sql"] and out["rows"], "the query result stays visible when only the explanation fails"


# --------------------------------------------------------------------------- leftovers from older pipelines
def test_leftover_gold_tables_are_invisible_and_unreadable(agent_db):
    """clean_db.py exists because older versions wrote other gold_ tables. They must not reach the model."""
    conn = sqlite3.connect(agent_db)
    conn.execute("CREATE TABLE gold_stock_risk (puma_division TEXT, note TEXT)")
    conn.execute("INSERT INTO gold_stock_risk VALUES ('Swim', 'stale')")
    conn.commit()
    conn.close()

    assert "gold_stock_risk" not in qa.get_schema()
    with pytest.raises(qa.SQLRejected, match="not a Gold table"):
        qa.validate_sql("SELECT * FROM gold_stock_risk")

    conn = sqlite3.connect(agent_db)  # authorizer alone, without the validator in front of it
    conn.set_authorizer(qa._authorizer)
    with pytest.raises(sqlite3.DatabaseError):
        conn.execute("SELECT * FROM gold_stock_risk").fetchall()
    conn.close()
