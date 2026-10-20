"""The evaluation set must itself be sound: every reference query runs on the Gold schema,
and the result comparison accepts reasonable variations while rejecting wrong answers."""
import pandas as pd
import pytest

from agent import query_agent as qa
from eval_agent import GOLDEN, results_match


def test_there_are_twenty_questions():
    assert len(GOLDEN) == 20


@pytest.mark.parametrize("question, sql", GOLDEN, ids=[q[:50] for q in (g[0] for g in GOLDEN)])
def test_reference_query_runs_on_the_gold_schema(db_copy, monkeypatch, question, sql):
    monkeypatch.setattr(qa, "DB_PATH", db_copy)
    df, _ = qa.run_sql(sql)
    assert len(df) >= 1, "a reference query that returns nothing cannot catch a wrong answer"


def test_matching_ignores_column_order_row_order_and_float_noise():
    golden = pd.DataFrame({"d": ["Swim", "Training"], "v": [1.001, 2.0]})
    agent = pd.DataFrame({"v": [2.0, 1.0], "d": ["Training", "Swim"]})
    assert results_match(golden, agent)


def test_matching_accepts_extra_columns():
    golden = pd.DataFrame({"d": ["Swim"]})
    agent = pd.DataFrame({"d": ["Swim"], "cost": [3016.39]})
    assert results_match(golden, agent)


def test_matching_rejects_wrong_values_and_wrong_row_counts():
    golden = pd.DataFrame({"d": ["Swim"]})
    assert not results_match(golden, pd.DataFrame({"d": ["Training"]}))
    assert not results_match(golden, pd.DataFrame({"d": ["Swim", "Training"]}))
    assert not results_match(golden, pd.DataFrame({"d": []}))
