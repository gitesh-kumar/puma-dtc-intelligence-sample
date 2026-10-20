"""Text-to-SQL analyst for the Gold layer.

Safety:     the database is opened read-only, a SQLite authorizer allows SELECT on the four
            current Gold tables only, the SQL is validated first, and a query that runs too long
            is interrupted.
Grounding:  every number in the answer has to appear in the query result. If one does not,
            the model gets one chance to rewrite, then the answer is returned marked unverified.
"""
import numbers
import os
import re
import sqlite3
import time
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

DB_PATH = "puma_dtc.db"
MODEL = "qwen/qwen3.8-27b"
MAX_ROWS = 20
QUERY_TIMEOUT_S = 5
MAX_SQL_ATTEMPTS = 2

# The agent may read these tables and no others. An allowlist, not a "gold_" prefix, so a leftover
# table from an older version of the pipeline (see clean_db.py) is neither shown to the model nor readable.
# Add the name here when you add a new Gold table.
GOLD_TABLES = ("gold_sell_through", "gold_markdown_risk", "gold_reorder_signals", "gold_inventory_health")


class SQLRejected(ValueError):
    """The generated SQL is not a single read-only query on the Gold tables."""


class ModelError(RuntimeError):
    """The language model could not be called (missing key, network, rate limit)."""


_client = None


def get_client():
    """Create the Groq client on first use so the module imports without an API key."""
    global _client
    if _client is None:
        from groq import Groq
        _client = Groq(api_key=os.getenv("GROQ_API_KEY"))
    return _client


# --------------------------------------------------------------------------- database access
def _db_uri():
    return Path(DB_PATH).resolve().as_uri() + "?mode=ro"


def _authorizer(action, arg1, arg2, arg3, arg4):
    """Allow SELECT statements, SQL functions and reading columns of the Gold tables. Deny the rest."""
    if action in (sqlite3.SQLITE_SELECT, sqlite3.SQLITE_FUNCTION):
        return sqlite3.SQLITE_OK
    if action == sqlite3.SQLITE_READ and str(arg1) in GOLD_TABLES:
        return sqlite3.SQLITE_OK
    return sqlite3.SQLITE_DENY


def connect_readonly(restricted=True):
    conn = sqlite3.connect(_db_uri(), uri=True)
    if restricted:
        conn.set_authorizer(_authorizer)
        deadline = time.monotonic() + QUERY_TIMEOUT_S
        conn.set_progress_handler(lambda: 1 if time.monotonic() > deadline else 0, 10_000)
    return conn


def get_schema():
    """Give the agent only the Gold schema, with sample values for text columns."""
    conn = connect_readonly(restricted=False)
    try:
        tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
                  if r[0] in GOLD_TABLES]
        schema = "DATABASE SCHEMA:\n"
        for table in tables:
            columns = conn.execute(f'PRAGMA table_info("{table}")').fetchall()
            schema += f"\nTable: {table}\nColumns: {', '.join(c[1] for c in columns)}\n"
            for _, name, col_type, *_ in columns:
                if "TEXT" in (col_type or "").upper() or col_type == "":
                    try:
                        values = [str(r[0]) for r in conn.execute(
                            f'SELECT DISTINCT "{name}" FROM "{table}" LIMIT 5') if r[0] is not None]
                    except sqlite3.Error:
                        values = []
                    if values:
                        schema += f"  {name} values: {', '.join(values)}\n"
        return schema
    finally:
        conn.close()


# --------------------------------------------------------------------------- SQL validation
_FORBIDDEN = re.compile(
    r"\b(insert|update|delete|drop|alter|create|attach|detach|pragma|replace|vacuum|reindex|analyze|truncate)\b",
    re.IGNORECASE,
)


def _for_inspection(sql):
    """Strip string literals and comments so keyword checks do not trip over text values."""
    sql = re.sub(r"'(?:[^']|'')*'", "''", sql)
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.S)
    return re.sub(r"--[^\n]*", " ", sql)


def validate_sql(sql):
    """Return the SQL ready to run, or raise SQLRejected with the reason.

    This gives the model a readable error to repair. The read-only connection and the
    authorizer enforce the same rules even if something slips through here."""
    if not sql or not sql.strip():
        raise SQLRejected("empty query")
    inspected = _for_inspection(sql).strip().rstrip(";").strip()
    if ";" in inspected:
        raise SQLRejected("only one statement is allowed")
    if not re.match(r"(select|with)\b", inspected, re.IGNORECASE):
        raise SQLRejected("only SELECT queries are allowed")
    forbidden = _FORBIDDEN.search(inspected)
    if forbidden:
        raise SQLRejected(f"'{forbidden.group(1).upper()}' is not allowed")
    ctes = set(re.findall(r"\b([A-Za-z_]\w*)\s+as\s*\(", inspected, re.IGNORECASE))
    for table in re.findall(r"\b(?:from|join)\s+([A-Za-z_]\w*)", inspected, re.IGNORECASE):
        if table not in ctes and table not in GOLD_TABLES:
            raise SQLRejected(f"table '{table}' is not a Gold table")
    return sql.strip().rstrip(";").strip()


def extract_sql(text):
    """Pull the query out of the model's reply."""
    match = re.search(r"```(?:sql)?\s*(.*?)\s*```", text, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    match = re.search(r"\b((?:select|with)\b.*?)(?:;|$)", text, re.DOTALL | re.IGNORECASE)
    return match.group(1).strip() if match else None


def run_sql(sql):
    """Validate and run a query. Returns (DataFrame of at most MAX_ROWS rows, truncated flag)."""
    checked = validate_sql(sql)
    conn = connect_readonly()
    try:
        cursor = conn.execute(checked)
        rows = cursor.fetchmany(MAX_ROWS + 1)
        columns = [d[0] for d in cursor.description]
    finally:
        conn.close()
    return pd.DataFrame(rows[:MAX_ROWS], columns=columns), len(rows) > MAX_ROWS


# --------------------------------------------------------------------------- number grounding
_MULTIPLIERS = {"k": 1e3, "thousand": 1e3, "m": 1e6, "million": 1e6, "b": 1e9, "bn": 1e9, "billion": 1e9}
_NUMBER = re.compile(r"(?<![\w.])(\d[\d,]*(?:\.\d+)?)\s?(billion|million|thousand|bn|[kKmMbB])?(?!\w)")


def _numbers_in(text):
    """Yield (value, decimals shown, multiplier, token) for every number in the text."""
    for match in _NUMBER.finditer(text):
        raw = match.group(1).replace(",", "")
        multiplier = _MULTIPLIERS.get((match.group(2) or "").lower(), 1)
        decimals = len(raw.split(".")[1]) if "." in raw else 0
        yield float(raw) * multiplier, decimals, multiplier, match.group(0).strip()


def _allowed_numbers(df, question, sql):
    allowed = set()
    for column in df.columns:
        for value in df[column].astype(object):
            if isinstance(value, numbers.Real) and not isinstance(value, bool) and value == value:
                allowed.update({float(value), abs(float(value))})
                if abs(value) <= 1:
                    allowed.add(abs(float(value)) * 100)  # a fraction quoted as a percentage
            elif isinstance(value, str):
                allowed.update(n for n, *_ in _numbers_in(value))
    allowed.update(n for n, *_ in _numbers_in(question))
    allowed.update(n for n, *_ in _numbers_in(sql))
    allowed.update(float(i) for i in range(len(df) + 1))  # row counts and ranks
    return allowed


def ungrounded_numbers(answer, df, question="", sql=""):
    """Numbers in the answer that cannot be traced to the query result (or the question or SQL)."""
    allowed = _allowed_numbers(df, question, sql)
    bad = []
    for value, decimals, multiplier, token in _numbers_in(answer):
        if decimals == 0 and multiplier == 1 and 1900 <= value <= 2100:
            continue  # a year
        tolerance = 0.5 * 10 ** (-decimals) * multiplier * 1.0001 + 1e-9  # rounding to the digits shown
        if not any(abs(value - a) <= tolerance for a in allowed) and token not in bad:
            bad.append(token)
    return bad


# --------------------------------------------------------------------------- the agent
SQL_SYSTEM = """You are a SQL expert for PUMA's DTC inventory intelligence demo.

{schema}

Your job is to write a single SQLite query to answer the user's question.
Return ONLY the SQL query wrapped in ```sql ``` tags.
Do not explain. Do not add anything else. Just the SQL query.
Use only the gold_ tables listed in the schema above. Write read-only SELECT queries."""

ANSWER_SYSTEM = """You are a senior PUMA DTC inventory analyst briefing a merchandising director.
You have just run a SQL query and received the result.
Give a clear, concise, actionable answer in under 100 words.
Use only numbers that appear in the query result. Do not calculate new numbers.
Stock levels, inventory value, weeks of stock and markdown cost come from simulated stock: say "simulated" when you quote them.
Always end with one concrete recommendation."""


def _chat(system, user, temperature, max_tokens=300):
    try:
        response = get_client().chat.completions.create(
            model=MODEL,
            messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
            temperature=temperature,
            max_tokens=max_tokens,
            extra_body= {"reasoning_effort": "none"},
        )
        return response.choices[0].message.content
    except Exception as exc:  # missing package or key, network, rate limit: all the same to the caller
        raise ModelError(f"{type(exc).__name__}: {str(exc)[:200]}") from exc


def _answer_prompt(question, sql, df, truncated, correction=""):
    note = f"\n(Only the first {MAX_ROWS} rows are shown.)" if truncated else ""
    return (f"Question: {question}\n\nSQL Query run: {sql}\n\nQuery Result:\n"
            f"{df.to_string(index=False)}{note}\n\nAnswer the question based on this data.{correction}")


def _json_rows(df):
    return df.astype(object).where(df.notna(), None).values.tolist()


def _model_unavailable(out, exc):
    print(f"  [Model error] {exc}")
    out["error"] = f"The language model could not be reached ({exc}). Check GROQ_API_KEY and the connection."
    out["answer"] = "I could not reach the language model, so there is no answer to show."
    return out


def ask(question: str) -> dict:
    """Answer a question. Returns the answer plus the SQL, the result rows and the number check."""
    out = {"question": question, "answer": None, "sql": None, "columns": [], "rows": [],
           "truncated": False, "verified": False, "ungrounded": [], "error": None}
    schema = get_schema()

    # Step 1: the agent writes SQL. A rejected or failing query is shown to the model once to repair.
    prompt, df, sql, truncated, error = question, None, None, False, None
    for _ in range(MAX_SQL_ATTEMPTS):
        try:
            raw = _chat(SQL_SYSTEM.format(schema=schema), prompt, temperature=0.1)
        except ModelError as exc:
            return _model_unavailable(out, exc)
        sql = extract_sql(raw)
        if not sql:
            error = f"no SQL found in the reply: {raw[:200]}"
            continue
        print(f"  [SQL] {sql}")
        try:
            df, truncated = run_sql(sql)
            error = None
            break
        except (SQLRejected, sqlite3.Error) as exc:
            error = str(exc)
            print(f"  [SQL error] {error}")
            prompt = (f"{question}\n\nYour previous query was:\n{sql}\nIt failed with: {error}\n"
                      "Write a corrected single SELECT query on the gold_ tables.")
    out["sql"] = sql
    if df is None:
        out["error"] = error
        out["answer"] = "I could not turn that question into a safe query on the Gold tables."
        return out

    out.update(columns=list(df.columns), rows=_json_rows(df), truncated=truncated)
    print(f"  [Result] {df.head(3).to_string(index=False)[:200]}...")
    if df.empty:
        out.update(answer="The query returned no rows.", verified=True)
        return out

    # Step 2: the agent explains the result. Every number must trace back to the result.
    try:
        answer = _chat(ANSWER_SYSTEM, _answer_prompt(question, sql, df, truncated), temperature=0.3)
        bad = ungrounded_numbers(answer, df, question, sql)
        if bad:
            print(f"  [Unverified numbers] {bad}, asking for a rewrite")
            correction = (f"\n\nYour previous answer used numbers that are not in the result: {', '.join(bad)}. "
                          "Rewrite it using only numbers from the result.")
            answer = _chat(ANSWER_SYSTEM, _answer_prompt(question, sql, df, truncated, correction), temperature=0.1)
            bad = ungrounded_numbers(answer, df, question, sql)
    except ModelError as exc:
        return _model_unavailable(out, exc)
    out.update(answer=answer, verified=not bad, ungrounded=bad)
    return out


if __name__ == "__main__":
    print("PUMA DTC Inventory Intelligence Agent")
    print("=" * 50)
    print("Type your question or 'quit' to exit\n")

    while True:
        question = input("You: ").strip()
        if question.lower() in ["quit", "exit", "q"]:
            break
        if not question:
            continue
        result = ask(question)
        flag = "numbers verified" if result["verified"] else f"UNVERIFIED numbers: {result['ungrounded']}"
        print(f"\nAgent: {result['answer']}\n  ({flag})\n")
