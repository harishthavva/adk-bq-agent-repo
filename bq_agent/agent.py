import os
import re
from dotenv import load_dotenv

from google.adk.agents.llm_agent import Agent
from google.cloud import bigquery

# ----------------------------
# Load env vars
# ----------------------------

load_dotenv()

PROJECT_ID = os.getenv("BQ_PROJECT")
DATASET = os.getenv("BQ_DATASET")

# ----------------------------
# BigQuery Client
# ----------------------------

bq_client = bigquery.Client(project=PROJECT_ID)

# ----------------------------
# Schema Context
# ----------------------------

SCHEMA = f"""
Project: {PROJECT_ID}
Dataset: {DATASET}
Table: transactions

Columns:
- order_id STRING
- order_date DATE
- region STRING
- product STRING
- amount FLOAT64
"""

# ----------------------------
# SQL Validator
# ----------------------------

FORBIDDEN = ["delete", "update", "insert", "drop", "alter", "merge"]

def validate_sql(sql: str):
    lower = sql.lower().strip()

    if not lower.startswith("select"):
        raise ValueError("Only SELECT queries allowed")

    for word in FORBIDDEN:
        if word in lower:
            raise ValueError("Unsafe SQL detected")

    return sql

# ----------------------------
# BigQuery Tool Function
# ----------------------------

def run_bigquery(sql: str):
    """
    Executes a safe SELECT query on BigQuery and returns rows.
    """

    sql = validate_sql(sql)

    job_config = bigquery.QueryJobConfig(
        maximum_bytes_billed=5 * 1024 * 1024 * 1024  # 5GB safety limit
    )

    query_job = bq_client.query(sql, job_config=job_config)
    rows = query_job.result()

    return [dict(row) for row in rows]

# ----------------------------
# ADK Agent
# ----------------------------

root_agent = Agent(
    name="bigquery_agent",
    model="gemini-2.5-flash",
    instruction=f"""
You are a BigQuery data analyst assistant.

Users ask questions in English.

Steps:
1) Translate the question into BigQuery SQL.
2) Call the run_bigquery tool.
3) Explain the results in simple language.

Rules:
- Only query the schema below.
- Always use fully-qualified names: `{PROJECT_ID}.{DATASET}.transactions`
- Never modify data.

Schema:
{SCHEMA}
""",
    tools=[run_bigquery],
)
