"""
CloudPulse — AWS Lambda Function (Speed Layer)
Triggered by Kinesis Data Streams.  Each invocation receives a batch of
Kinesis records, decodes them, computes per-record metrics, and:
  1. Updates a DynamoDB item (one per region) with the latest state + TTL.
  2. Puts a CloudWatch metric for the health score and packet loss.
  3. Writes windowed aggregates to S3 every SLIDE_INTERVAL invocations.

Deploy this file as a Lambda function:
    Handler:  lambda_function.lambda_handler
    Runtime:  python3.12
    Timeout:  60 s
    Memory:   256 MB
    Trigger:  Kinesis Data Stream (cloudpulse-stream), batch size 100

Required environment variables (set in Lambda console or via Terraform):
    S3_BUCKET           cloudpulse-data-bucket
    DYNAMODB_TABLE      cloudpulse-speed-layer
    AWS_REGION          us-east-1
    SPEED_WINDOW_SECONDS 300
"""

from __future__ import annotations

import base64
import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

log = logging.getLogger()
log.setLevel(logging.INFO)

# ─── Environment ──────────────────────────────────────────────────────────────
S3_BUCKET          = os.environ.get("S3_BUCKET",           "cloudpulse-data-bucket")
DYNAMODB_TABLE     = os.environ.get("DYNAMODB_TABLE",      "cloudpulse-speed-layer")
AWS_REGION         = os.environ.get("AWS_REGION",          "us-east-1")
S3_SPEED_PREFIX    = os.environ.get("S3_SPEED_PREFIX",     "speed-results/")
CW_NAMESPACE       = os.environ.get("CW_NAMESPACE",        "CloudPulse")
SPEED_WINDOW_SECS  = int(os.environ.get("SPEED_WINDOW_SECONDS", "300"))

# Lazy boto3 clients (reused across warm Lambda invocations)
_s3_client  = None
_ddb_client = None
_cw_client  = None


def _s3():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3", region_name=AWS_REGION)
    return _s3_client


def _ddb():
    global _ddb_client
    if _ddb_client is None:
        _ddb_client = boto3.client("dynamodb", region_name=AWS_REGION)
    return _ddb_client


def _cw():
    global _cw_client
    if _cw_client is None:
        _cw_client = boto3.client("cloudwatch", region_name=AWS_REGION)
    return _cw_client


# ─── Record decoding ──────────────────────────────────────────────────────────
def _decode_kinesis_record(kinesis_record: dict) -> dict | None:
    try:
        raw  = base64.b64decode(kinesis_record["kinesis"]["data"])
        return json.loads(raw)
    except Exception as exc:
        log.warning("Failed to decode Kinesis record: %s", exc)
        return None


# ─── DynamoDB upsert ──────────────────────────────────────────────────────────
def _upsert_region_state(record: dict) -> None:
    """
    Upsert one item per region into DynamoDB with a TTL so stale entries
    are automatically removed after SPEED_WINDOW_SECS.
    """
    region_id = record.get("region_id", "unknown")
    ttl       = int(time.time()) + SPEED_WINDOW_SECS * 2  # retain 2x window

    item = {
        "region_id":        {"S": region_id},
        "timestamp":        {"S": record.get("timestamp", "")},
        "unix_ts":          {"N": str(record.get("unix_ts", int(time.time())))},
        "country_code":     {"S": record.get("country_code", "XX")},
        "country_name":     {"S": record.get("country_name", "Unknown")},
        "region_name":      {"S": record.get("region_name", region_id)},
        "avg_rtt_ms":       {"N": str(record.get("avg_rtt_ms", 0))},
        "packet_loss_pct":  {"N": str(record.get("packet_loss_pct", 0))},
        "health_score":     {"N": str(record.get("health_score", 100))},
        "is_reachable":     {"BOOL": record.get("is_reachable", True)},
        "outage_detected":  {"BOOL": record.get("outage_detected", False)},
        "state":            {"S": record.get("state", "healthy")},
        "lat":              {"N": str(record.get("lat", 0))},
        "lon":              {"N": str(record.get("lon", 0))},
        "flag":             {"S": record.get("flag", "")},
        "ttl":              {"N": str(ttl)},
    }
    try:
        _ddb().put_item(TableName=DYNAMODB_TABLE, Item=item)
    except ClientError as exc:
        log.error("DynamoDB put_item failed for %s: %s", region_id, exc)


# ─── CloudWatch metrics ────────────────────────────────────────────────────────
def _put_cloudwatch_metrics(records: list[dict]) -> None:
    """
    Emit custom CloudWatch metrics for the processed batch.
    Dimensions: Region (region_id), Country (country_code)
    """
    metric_data = []
    for r in records:
        dim = [
            {"Name": "Region",  "Value": r.get("region_id",    "unknown")},
            {"Name": "Country", "Value": r.get("country_code", "XX")},
        ]
        metric_data.extend([
            {
                "MetricName": "HealthScore",
                "Dimensions": dim,
                "Value":      r.get("health_score", 100),
                "Unit":       "None",
                "Timestamp":  datetime.now(timezone.utc),
            },
            {
                "MetricName": "AvgLatencyMs",
                "Dimensions": dim,
                "Value":      r.get("avg_rtt_ms", 0),
                "Unit":       "Milliseconds",
                "Timestamp":  datetime.now(timezone.utc),
            },
            {
                "MetricName": "PacketLossPct",
                "Dimensions": dim,
                "Value":      r.get("packet_loss_pct", 0),
                "Unit":       "Percent",
                "Timestamp":  datetime.now(timezone.utc),
            },
        ])
        if len(metric_data) >= 20:   # CloudWatch limit: 20 per call
            _cw().put_metric_data(Namespace=CW_NAMESPACE, MetricData=metric_data)
            metric_data = []

    if metric_data:
        try:
            _cw().put_metric_data(Namespace=CW_NAMESPACE, MetricData=metric_data)
        except ClientError as exc:
            log.warning("CloudWatch put_metric_data failed: %s", exc)


# ─── Window aggregation (per invocation) ──────────────────────────────────────
def _aggregate_batch(records: list[dict]) -> dict:
    """Compute lightweight aggregates over the current Lambda batch."""
    by_region: dict[str, list] = defaultdict(list)
    for r in records:
        by_region[r.get("region_id", "unknown")].append(r)

    aggs = {}
    for region_id, recs in by_region.items():
        n     = len(recs)
        lats  = [r.get("avg_rtt_ms",     0.0) for r in recs]
        loss  = [r.get("packet_loss_pct", 0.0) for r in recs]
        hlth  = [r.get("health_score",    0.0) for r in recs]
        ot    = sum(1 for r in recs if r.get("outage_detected"))
        latest = recs[-1]
        aggs[region_id] = {
            "region_id":       region_id,
            "region_name":     latest.get("region_name"),
            "country_code":    latest.get("country_code"),
            "country_name":    latest.get("country_name"),
            "lat":             latest.get("lat"),
            "lon":             latest.get("lon"),
            "flag":            latest.get("flag"),
            "avg_latency_ms":  round(sum(lats) / n, 2),
            "avg_packet_loss": round(sum(loss) / n, 2),
            "avg_health":      round(sum(hlth) / n, 1),
            "outage_count":    ot,
            "sample_count":    n,
            "is_outage":       ot > n * 0.3,
            "current_state":   latest.get("state", "healthy"),
            "updated_at":      datetime.now(timezone.utc).isoformat(),
        }
    return aggs


def _write_aggs_to_s3(aggs: dict) -> None:
    key  = f"{S3_SPEED_PREFIX}lambda_aggs/{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
    body = json.dumps(list(aggs.values()), default=str).encode()
    try:
        _s3().put_object(Bucket=S3_BUCKET, Key=key, Body=body, ContentType="application/json")
        # Also overwrite the "latest" file for dashboard consumption
        _s3().put_object(
            Bucket=S3_BUCKET,
            Key=f"{S3_SPEED_PREFIX}current_state.json",
            Body=body,
            ContentType="application/json",
        )
    except ClientError as exc:
        log.error("S3 write failed: %s", exc)


# ─── Lambda handler ────────────────────────────────────────────────────────────
def lambda_handler(event: dict, context) -> dict:
    """
    Entry point invoked by Kinesis trigger.
    `event['Records']` contains up to <batch_size> Kinesis records.
    """
    t_start = time.perf_counter()
    raw_records = event.get("Records", [])

    # Decode
    probe_records = []
    for kr in raw_records:
        decoded = _decode_kinesis_record(kr)
        if decoded:
            probe_records.append(decoded)

    log.info("Received %d records (%d decoded)", len(raw_records), len(probe_records))

    if not probe_records:
        return {"statusCode": 200, "processed": 0}

    # 1. Update DynamoDB state
    for r in probe_records:
        _upsert_region_state(r)

    # 2. CloudWatch metrics
    _put_cloudwatch_metrics(probe_records)

    # 3. Aggregate + write to S3
    aggs = _aggregate_batch(probe_records)
    _write_aggs_to_s3(aggs)

    duration_ms = round((time.perf_counter() - t_start) * 1000, 1)
    log.info("Processed %d records in %s ms", len(probe_records), duration_ms)

    return {
        "statusCode":     200,
        "processed":      len(probe_records),
        "regions_updated": len(aggs),
        "duration_ms":    duration_ms,
    }
