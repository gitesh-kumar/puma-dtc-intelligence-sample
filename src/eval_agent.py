"""Execution-accuracy test set for the text-to-SQL agent.

    python eval_agent.py                 # needs GROQ_API_KEY; asks the agent all 20 questions
    python eval_agent.py --sleep 2       # pause between questions if the API rate-limits you
    python eval_agent.py --golden-only   # no API call, only checks that the reference queries run

For each question the agent's result must match the result of a hand-written reference query
(the "golden" SQL). Matching is on values, not on wording: the agent may return extra columns or
a different column order, but every column of the reference result must appear in its result.
Results go to eval_results.csv. Quote the pass rate only after you have run this yourself.
"""
import argparse
import sys
import time

import pandas as pd

GOLDEN = [
    ("Which division has the highest estimated markdown cost?",
     "SELECT puma_division FROM gold_markdown_risk ORDER BY estimated_markdown_cost_eur DESC LIMIT 1"),
    ("What is the total estimated markdown cost across all divisions?",
     "SELECT SUM(estimated_markdown_cost_eur) FROM gold_markdown_risk"),
    ("How many divisions have a CRITICAL inventory health label?",
     "SELECT COUNT(*) FROM gold_inventory_health WHERE health_label = 'CRITICAL'"),
    ("Which division has the lowest inventory health score?",
     "SELECT puma_division FROM gold_inventory_health ORDER BY inventory_health_score ASC LIMIT 1"),
    ("Which divisions should be ordered within 2 weeks?",
     "SELECT puma_division FROM gold_reorder_signals WHERE reorder_signal LIKE 'SOON%'"),
    ("What is the average inventory health score?",
     "SELECT AVG(inventory_health_score) FROM gold_inventory_health"),
    ("Which division has the most weeks of stock?",
     "SELECT puma_division FROM gold_markdown_risk ORDER BY weeks_of_stock DESC LIMIT 1"),
    ("What is the sell-through rate of Swim?",
     "SELECT sell_through_rate FROM gold_sell_through WHERE puma_division = 'Swim'"),
    ("List the divisions with their inventory health score, worst first.",
     "SELECT puma_division, inventory_health_score FROM gold_inventory_health ORDER BY inventory_health_score ASC"),
    ("How many units did Running sell in the last 3 months?",
     "SELECT units_last_3m FROM gold_sell_through WHERE puma_division = 'Running'"),
    ("Which division had the highest revenue in the last 3 months?",
     "SELECT puma_division FROM gold_sell_through ORDER BY revenue_last_3m DESC LIMIT 1"),
    ("What is the forecast of next month's units for Training?",
     "SELECT forecast_next_month_units FROM gold_reorder_signals WHERE puma_division = 'Training'"),
    ("Show the markdown risk label of every division.",
     "SELECT puma_division, markdown_risk FROM gold_markdown_risk"),
    ("Which divisions have less than 8 weeks of stock?",
     "SELECT puma_division FROM gold_markdown_risk WHERE weeks_of_stock < 8"),
    ("What is the total simulated inventory value?",
     "SELECT SUM(inventory_value_eur) FROM gold_markdown_risk"),
    ("Which division has the highest sell-through rate?",
     "SELECT puma_division FROM gold_sell_through ORDER BY sell_through_rate DESC LIMIT 1"),
    ("How many divisions have HIGH or CRITICAL markdown risk?",
     "SELECT COUNT(*) FROM gold_markdown_risk WHERE markdown_risk IN ('HIGH', 'CRITICAL')"),
    ("What is the average item price in Accessories?",
     "SELECT avg_price_eur FROM gold_markdown_risk WHERE puma_division = 'Accessories'"),
    ("How many divisions have a WATCH or better inventory health label?",
     "SELECT COUNT(*) FROM gold_inventory_health WHERE health_label IN ('HEALTHY', 'WATCH')"),
    ("What is the lowest number of weeks of stock across all divisions?",
     "SELECT MIN(weeks_of_stock) FROM gold_markdown_risk"),
]


def _norm(value):
    if value is None or (isinstance(value, float) and value != value):
        return None
    if isinstance(value, (int, float)):
        return round(float(value), 2)
    return str(value)


def _column_values(series):
    return sorted((_norm(v) for v in series), key=lambda v: (v is None, str(v)))


def results_match(golden_df, agent_df):
    """True when both results have the same number of rows and every golden column appears in the agent result."""
    if len(golden_df) != len(agent_df):
        return False
    agent_columns = [_column_values(agent_df[c]) for c in agent_df.columns]
    return all(_column_values(golden_df[c]) in agent_columns for c in golden_df.columns)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--sleep", type=float, default=0.0, help="seconds to wait between questions")
    parser.add_argument("--golden-only", action="store_true", help="only run the reference queries")
    args = parser.parse_args()

    from agent.query_agent import ask, run_sql

    rows = []
    for question, golden_sql in GOLDEN:
        golden_df, _ = run_sql(golden_sql)
        if args.golden_only:
            print(f"ok   {question}")
            continue
        out = ask(question)
        agent_df = pd.DataFrame(out["rows"], columns=out["columns"]) if out["columns"] else pd.DataFrame()
        rows.append({
            "question": question,
            "ran": out["error"] is None,
            "matches_reference": out["error"] is None and results_match(golden_df, agent_df),
            "numbers_verified": out["verified"],
            "agent_sql": out["sql"],
            "reference_sql": golden_sql,
            "error": out["error"],
        })
        print(f"{'PASS' if rows[-1]['matches_reference'] else 'FAIL'}  {question}")
        time.sleep(args.sleep)

    if args.golden_only:
        print(f"\n{len(GOLDEN)} reference queries run.")
        return 0
    report = pd.DataFrame(rows)
    report.to_csv("eval_results.csv", index=False)
    n = len(report)
    print(f"\nQuery ran:                {report['ran'].sum()}/{n}")
    print(f"Matches reference result: {report['matches_reference'].sum()}/{n}")
    print(f"Numbers verified:         {report['numbers_verified'].sum()}/{n}")
    for row in report[~report["matches_reference"]].itertuples():
        print(f"\nMISS: {row.question}\n  agent:     {row.agent_sql}\n  reference: {row.reference_sql}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
