import sqlite3
import pandas as pd
from groq import Groq
from dotenv import load_dotenv
import os
import re

load_dotenv()

DB_PATH = "puma_dtc.db"
client = Groq(api_key=os.getenv("GROQ_API_KEY"))

def get_schema():
    """Give the agent only the schema - no data, but with sample values"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]
    
    schema = "DATABASE SCHEMA:\n"
    for table in tables:
        if table.startswith("gold_"):
            cursor.execute(f"PRAGMA table_info({table})")
            columns = cursor.fetchall()
            col_names = [col[1] for col in columns]
            schema += f"\nTable: {table}\nColumns: {', '.join(col_names)}\n"
            
            # Add sample distinct values for text columns
            for col in columns:
                col_name = col[1]
                col_type = col[2]
                if "TEXT" in col_type.upper() or col_type == "":
                    try:
                        cursor.execute(f"SELECT DISTINCT {col_name} FROM {table} LIMIT 5")
                        values = [str(r[0]) for r in cursor.fetchall() if r[0] is not None]
                        if values:
                            schema += f"  {col_name} values: {', '.join(values)}\n"
                    except:
                        pass
    
    conn.close()
    return schema

def run_sql(sql: str) -> str:
    """Execute SQL and return result as string"""
    try:
        conn = sqlite3.connect(DB_PATH)
        result = pd.read_sql(sql, conn)
        conn.close()
        
        if result.empty:
            return "No results found."
        
        # Return max 20 rows to keep tokens low
        return result.head(20).to_string(index=False)
    except Exception as e:
        return f"SQL Error: {str(e)}"

def extract_sql(text: str) -> str:
    """Extract SQL query from agent response"""
    # Look for SQL between ```sql and ``` or just SELECT statements
    sql_pattern = r"```sql\s*(.*?)\s*```"
    match = re.search(sql_pattern, text, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    
    # Fallback - look for SELECT statement
    select_pattern = r"(SELECT.*?;)"
    match = re.search(select_pattern, text, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    
    return None

def ask(question: str) -> str:
    schema = get_schema()
    
    # Step 1 - Agent writes SQL query
    sql_response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[
            {
                "role": "system",
                "content": f"""You are a SQL expert for PUMA's DTC inventory intelligence system.

{schema}

Your job is to write a single SQL query to answer the user's question.
Return ONLY the SQL query wrapped in ```sql ``` tags.
Do not explain. Do not add anything else. Just the SQL query.
Use only the gold_ tables listed in the schema above."""
            },
            {
                "role": "user",
                "content": question
            }
        ],
        temperature=0.1,
        max_tokens=300
    )
    
    sql_text = sql_response.choices[0].message.content
    sql_query = extract_sql(sql_text)
    
    if not sql_query:
        return f"Could not generate SQL query. Raw response: {sql_text}"
    
    print(f"  [SQL] {sql_query}")
    
    # Step 2 - Run the SQL, get only the result
    query_result = run_sql(sql_query)
    print(f"  [Result] {query_result[:200]}...")
    
    # Step 3 - Agent interprets the result and answers in plain English
    answer_response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[
            {
                "role": "system",
                "content": """You are a senior PUMA DTC inventory analyst briefing a merchandising director.
You have just run a SQL query and received the result.
Give a clear, concise, actionable answer in under 100 words.
Always reference specific numbers. Always end with one concrete recommendation."""
            },
            {
                "role": "user",
                "content": f"""Question: {question}

SQL Query run: {sql_query}

Query Result:
{query_result}

Answer the question based on this data."""
            }
        ],
        temperature=0.3,
        max_tokens=300
    )
    
    return answer_response.choices[0].message.content

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
        print(f"\nAgent: {ask(question)}\n")