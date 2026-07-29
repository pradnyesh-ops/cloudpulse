"""
CloudPulse — Serving Layer: Athena Queries
Creates the Athena database + tables over S3 data and provides helper
functions to run analytics queries against the full history.

Run once to set up:
    python serving_layer/athena_queries.py --setup

Then query interactively:
    python serving_layer/athena_queries.py --query top_affected
    python serving_layer/athena_queries.py --query latency_trend --country US
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import (
    ATHENA_DATABASE, ATHENA_OUTPUT_LOCATION, ATHENA_WORKGROUP,
    AWS_REGION, S3_BUCKET, S3_RAW_PREFIX,
)

athena = boto3.client("athena", region_name=AWS_REGION)


# ─── DDL ──────────────────────────────────────────────────────────────────────
CREATE_DATABASE_SQL = f"CREATE DATABASE IF NOT EXISTS {ATHENA_DATABASE}"

CREATE_RAW_TABLE_SQL = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {ATHENA_DATABASE}.probe_events (
    event_id        STRING,
    `timestamp`     STRING,
    unix_ts         BIGINT,
    probe_id        STRING,
    region_id       STRING,
    region_name     STRING,
    country_code    STRING,
    country_name    STRING,
    continent       STRING,
    lat             DOUBLE,
    lon             DOUBLE,
    avg_rtt_ms      DOUBLE,
    min_rtt_ms      DOUBLE,
    max_rtt_ms      DOUBLE,
    packet_loss_pct DOUBLE,
    hop_count       INT,
    is_reachable    BOOLEAN,
    outage_detected BOOLEAN,
    health_score    DOUBLE,
    state           STRING,
    provider        STRING,
    flag            STRING,
    target_ip       STRING,
    target_name     STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('ignore.malformed.json' = 'true')
LOCATION 's3://{S3_BUCKET}/{S3_RAW_PREFIX}'
TBLPROPERTIES ('has_encrypted_data'='false')
"""

CREATE_BATCH_VIEW_SQL = f"""
CREATE OR REPLACE VIEW {ATHENA_DATABASE}.v_batch_summary AS
SELECT
    country_code,
    country_name,
    region_id,
    region_name,
    continent,
    COUNT(*)                    AS total_probes,
    AVG(avg_rtt_ms)             AS avg_latency_ms,
    MIN(avg_rtt_ms)             AS min_latency_ms,
    MAX(avg_rtt_ms)             AS max_latency_ms,
    AVG(packet_loss_pct)        AS avg_packet_loss_pct,
    AVG(health_score)           AS avg_health_score,
    SUM(CAST(outage_detected AS INT)) AS total_outages
FROM {ATHENA_DATABASE}.probe_events
GROUP BY country_code, country_name, region_id, region_name, continent
ORDER BY avg_health_score ASC
"""

# ─── Analytic queries ─────────────────────────────────────────────────────────
QUERIES: dict[str, str] = {

    "top_affected": f"""
        SELECT
            country_code,
            country_name,
            AVG(health_score)           AS avg_health,
            AVG(avg_rtt_ms)             AS avg_latency,
            AVG(packet_loss_pct)        AS avg_loss,
            SUM(CAST(outage_detected AS INT)) AS outage_events,
            COUNT(*)                    AS probe_count
        FROM {ATHENA_DATABASE}.probe_events
        WHERE unix_ts >= TO_UNIXTIME(NOW()) - 86400   -- last 24 hours
        GROUP BY country_code, country_name
        ORDER BY avg_health ASC
        LIMIT 10
    """,

    "latency_trend": f"""
        SELECT
            DATE_TRUNC('hour', FROM_UNIXTIME(unix_ts)) AS hour_bucket,
            country_code,
            AVG(avg_rtt_ms)          AS avg_latency_ms,
            AVG(packet_loss_pct)     AS avg_loss_pct,
            AVG(health_score)        AS avg_health
        FROM {ATHENA_DATABASE}.probe_events
        WHERE unix_ts >= TO_UNIXTIME(NOW()) - 86400
          AND country_code = :country
        GROUP BY 1, 2
        ORDER BY 1
    """,

    "global_trend": f"""
        SELECT
            DATE_TRUNC('hour', FROM_UNIXTIME(unix_ts)) AS hour_bucket,
            AVG(health_score)        AS global_health,
            AVG(avg_rtt_ms)          AS global_latency,
            AVG(packet_loss_pct)     AS global_loss,
            SUM(CAST(outage_detected AS INT)) AS outage_events
        FROM {ATHENA_DATABASE}.probe_events
        WHERE unix_ts >= TO_UNIXTIME(NOW()) - 604800  -- last 7 days
        GROUP BY 1
        ORDER BY 1
    """,

    "outage_hotspots": f"""
        SELECT
            region_id,
            region_name,
            country_code,
            country_name,
            SUM(CAST(outage_detected AS INT)) AS outage_count,
            AVG(avg_rtt_ms)                   AS avg_latency_during_outage,
            AVG(packet_loss_pct)              AS avg_loss
        FROM {ATHENA_DATABASE}.probe_events
        WHERE outage_detected = TRUE
          AND unix_ts >= TO_UNIXTIME(NOW()) - 86400
        GROUP BY 1, 2, 3, 4
        ORDER BY outage_count DESC
        LIMIT 10
    """,

    "speedup_benchmark": f"""
        -- Compare sequential vs parallel processing count (for benchmarks)
        SELECT
            COUNT(*) AS total_records,
            COUNT(DISTINCT region_id) AS regions,
            MIN(unix_ts) AS first_ts,
            MAX(unix_ts) AS last_ts
        FROM {ATHENA_DATABASE}.probe_events
    """,
}


# ─── Athena execution helpers ─────────────────────────────────────────────────
def run_query(sql: str, params: dict[str, str] | None = None, timeout_s: int = 120) -> list[dict]:
    """Execute an Athena query and return results as a list of dicts."""
    if params:
        for k, v in params.items():
            sql = sql.replace(f":{k}", f"'{v}'")

    resp = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT_LOCATION},
        WorkGroup=ATHENA_WORKGROUP,
    )
    qid = resp["QueryExecutionId"]
    print(f"[Athena] Query submitted: {qid}")

    # Poll
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        status = athena.get_query_execution(QueryExecutionId=qid)
        state  = status["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            reason = status["QueryExecution"]["Status"].get("StateChangeReason", "unknown")
            raise RuntimeError(f"Athena query {state}: {reason}")
        time.sleep(2)
    else:
        raise TimeoutError(f"Athena query timed out after {timeout_s}s")

    # Fetch results
    rows     = []
    paginator = athena.get_paginator("get_query_results")
    headers: list[str] = []

    for page in paginator.paginate(QueryExecutionId=qid):
        result_rows = page["ResultSet"]["Rows"]
        if not headers and result_rows:
            headers = [c["VarCharValue"] for c in result_rows[0]["Data"]]
            result_rows = result_rows[1:]
        for row in result_rows:
            values = [c.get("VarCharValue", "") for c in row["Data"]]
            rows.append(dict(zip(headers, values)))

    print(f"[Athena] Query returned {len(rows)} rows.")
    return rows


def setup_athena(demo: bool = False) -> None:
    """Create database and table in Athena (idempotent)."""
    if demo:
        print("[Athena] Skipping setup in demo mode.")
        return
    print("[Athena] Setting up database and tables …")
    for sql in [CREATE_DATABASE_SQL, CREATE_RAW_TABLE_SQL, CREATE_BATCH_VIEW_SQL]:
        run_query(sql)
    print("[Athena] Setup complete.")


# ─── CLI ──────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="CloudPulse Athena query runner")
    parser.add_argument("--setup",   action="store_true",    help="Create DB + tables")
    parser.add_argument("--query",   choices=list(QUERIES),  help="Named query to run")
    parser.add_argument("--country", default="US",           help="Country code filter (for latency_trend)")
    parser.add_argument("--out",     default=None,           help="Save results to this JSON file")
    args = parser.parse_args()

    if args.setup:
        setup_athena()
        return

    if args.query:
        sql    = QUERIES[args.query]
        params = {"country": args.country} if args.query == "latency_trend" else None
        rows   = run_query(sql, params)
        print(json.dumps(rows, indent=2))
        if args.out:
            Path(args.out).write_text(json.dumps(rows, indent=2))
        return

    parser.print_help()


if __name__ == "__main__":
    main()
