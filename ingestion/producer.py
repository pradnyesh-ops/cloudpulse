"""
CloudPulse — Kinesis Producer
Reads probe records from data_sources and puts them onto the Kinesis stream.
Supports both demo mode (writes to local JSON files) and production mode (Kinesis).

Usage:
    python producer.py                   # demo mode (reads DEMO_MODE from .env)
    DEMO_MODE=false python producer.py   # production mode — requires AWS creds
"""

from __future__ import annotations

import base64
import json
import logging
import os
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Callable

import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import (
    AWS_REGION, DEMO_MODE, INGESTION_RATE,
    KINESIS_STREAM_NAME, LOCAL_DATA_DIR, PROBE_COUNT,
    SQS_QUEUE_NAME, STREAM_BACKEND,
    USE_RIPE_ATLAS,
)
from ingestion.data_sources import REGIONS, simulate_stream, fetch_ripe_atlas_measurements

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Producer] %(levelname)s — %(message)s",
)
log = logging.getLogger(__name__)

# ─── Metrics ──────────────────────────────────────────────────────────────────
_metrics: dict = defaultdict(int)


def _record_metric(name: str, value: int = 1) -> None:
    _metrics[name] += value


# ─── Demo-mode local storage ───────────────────────────────────────────────────
def _ensure_local_dirs() -> Path:
    base = Path(LOCAL_DATA_DIR)
    (base / "raw").mkdir(parents=True, exist_ok=True)
    return base


def write_local(record: dict, base: Path) -> None:
    """Append record to a rotating hourly NDJSON file."""
    from datetime import datetime, timezone
    hour = datetime.now(timezone.utc).strftime("%Y%m%d_%H")
    path = base / "raw" / f"{hour}.ndjson"
    with path.open("a") as f:
        f.write(json.dumps(record) + "\n")


# ─── Production: Kinesis ───────────────────────────────────────────────────────
def _get_kinesis_client():
    return boto3.client("kinesis", region_name=AWS_REGION)


def _get_sqs_client():
    return boto3.client("sqs", region_name=AWS_REGION)


def _create_stream_if_missing(client, stream_name: str, shard_count: int = 2) -> None:
    from config import KINESIS_SHARD_COUNT
    try:
        client.describe_stream_summary(StreamName=stream_name)
        log.info("Kinesis stream '%s' already exists.", stream_name)
    except client.exceptions.ResourceNotFoundException:
        log.info("Creating Kinesis stream '%s' with %d shards …", stream_name, KINESIS_SHARD_COUNT)
        client.create_stream(StreamName=stream_name, ShardCount=KINESIS_SHARD_COUNT)
        # Wait until ACTIVE
        waiter = client.get_waiter("stream_exists")
        waiter.wait(StreamName=stream_name)
        log.info("Kinesis stream '%s' is now ACTIVE.", stream_name)


def put_record_kinesis(client, record: dict, stream_name: str) -> bool:
    """Put a single record to Kinesis. Returns True on success."""
    try:
        payload = json.dumps(record).encode()
        client.put_record(
            StreamName=stream_name,
            Data=payload,
            PartitionKey=record.get("region_id", "default"),
        )
        _record_metric("kinesis_puts_ok")
        return True
    except ClientError as exc:
        log.warning("Kinesis put_record failed: %s", exc)
        _record_metric("kinesis_puts_err")
        return False


def put_records_batch_kinesis(client, records: list[dict], stream_name: str) -> int:
    """
    Put up to 500 records in one Kinesis PutRecords call.
    Returns count of successfully published records.
    """
    entries = [
        {"Data": json.dumps(r).encode(), "PartitionKey": r.get("region_id", "default")}
        for r in records
    ]
    try:
        resp     = client.put_records(StreamName=stream_name, Records=entries)
        failures = resp.get("FailedRecordCount", 0)
        success  = len(records) - failures
        _record_metric("kinesis_puts_ok", success)
        _record_metric("kinesis_puts_err", failures)
        return success
    except ClientError as exc:
        log.error("Kinesis put_records failed: %s", exc)
        _record_metric("kinesis_puts_err", len(records))
        return 0


def _get_or_create_sqs_queue_url(client, queue_name: str) -> str:
    attrs = {
        "VisibilityTimeout": "120",
        "MessageRetentionPeriod": "345600",
        "ReceiveMessageWaitTimeSeconds": "20",
    }
    client.create_queue(QueueName=queue_name, Attributes=attrs)
    return client.get_queue_url(QueueName=queue_name)["QueueUrl"]


def put_records_batch_sqs(client, records: list[dict], queue_url: str) -> int:
    """Put up to 10 records in one SQS SendMessageBatch call."""
    sent = 0
    for i in range(0, len(records), 10):
        chunk = records[i:i + 10]
        entries = [
            {
                "Id": str(idx),
                "MessageBody": json.dumps(r),
            }
            for idx, r in enumerate(chunk)
        ]
        try:
            resp = client.send_message_batch(QueueUrl=queue_url, Entries=entries)
            ok = len(resp.get("Successful", []))
            err = len(resp.get("Failed", []))
            sent += ok
            _record_metric("sqs_puts_ok", ok)
            _record_metric("sqs_puts_err", err)
        except ClientError as exc:
            log.error("SQS send_message_batch failed: %s", exc)
            _record_metric("sqs_puts_err", len(chunk))
    return sent


def _resolve_stream_backend() -> str:
    backend = STREAM_BACKEND
    if backend == "sqs":
        return "sqs"
    if backend == "kinesis":
        return "kinesis"
    return "auto"


# ─── Main producer loop ────────────────────────────────────────────────────────
def run_producer(
    rate: float = INGESTION_RATE,
    demo: bool = DEMO_MODE,
    on_record: Callable[[dict], None] | None = None,
) -> None:
    """
    Continuously generate probe records and push them to Kinesis (or local files).
    `on_record` is an optional callback used by the dashboard to receive records
    in-process without writing to disk.
    """
    log.info("Starting producer — mode=%s  rate=%.1f rec/s", "DEMO" if demo else "PROD", rate)

    kinesis_client = None
    sqs_client = None
    sqs_queue_url: str | None = None
    selected_backend = "demo"
    local_base: Path | None = None

    if demo:
        local_base = _ensure_local_dirs()
        log.info("Demo mode: records → %s/raw/", LOCAL_DATA_DIR)
    else:
        backend = _resolve_stream_backend()
        if backend in ("auto", "kinesis"):
            try:
                kinesis_client = _get_kinesis_client()
                _create_stream_if_missing(kinesis_client, KINESIS_STREAM_NAME)
                selected_backend = "kinesis"
                log.info("Production mode: records → Kinesis stream '%s'", KINESIS_STREAM_NAME)
            except ClientError as exc:
                code = exc.response.get("Error", {}).get("Code")
                if code == "SubscriptionRequiredException" and backend == "auto":
                    log.warning("Kinesis unavailable for this account, switching to SQS fallback.")
                else:
                    raise

        if selected_backend != "kinesis":
            sqs_client = _get_sqs_client()
            sqs_queue_url = _get_or_create_sqs_queue_url(sqs_client, SQS_QUEUE_NAME)
            selected_backend = "sqs"
            log.info("Production mode: records → SQS queue '%s'", SQS_QUEUE_NAME)

    interval    = 1.0 / max(rate, 0.1)
    batch_buf   = []
    batch_size  = 25   # flush every N records in production
    log_every   = 100  # log a summary every N records
    total_sent  = 0

    stream_gen = simulate_stream(REGIONS, rate=rate)

    while True:
        try:
            record = next(stream_gen)

            if on_record:
                on_record(record)

            if demo:
                write_local(record, local_base)
            else:
                batch_buf.append(record)
                if len(batch_buf) >= batch_size:
                    if selected_backend == "kinesis":
                        put_records_batch_kinesis(kinesis_client, batch_buf, KINESIS_STREAM_NAME)
                    else:
                        put_records_batch_sqs(sqs_client, batch_buf, sqs_queue_url)
                    batch_buf.clear()

            total_sent += 1
            if total_sent % log_every == 0:
                log.info(
                    "Sent %d records | metrics=%s",
                    total_sent,
                    dict(_metrics),
                )

            # Optionally augment with real RIPE Atlas data
            if USE_RIPE_ATLAS and total_sent % 500 == 0:
                atlas_records = fetch_ripe_atlas_measurements(limit=10)
                for ar in atlas_records:
                    if on_record:
                        on_record(ar)
                    if demo and local_base:
                        write_local(ar, local_base)

            time.sleep(interval)

        except KeyboardInterrupt:
            log.info("Producer stopped by user. Total records sent: %d", total_sent)
            break
        except Exception as exc:
            log.error("Producer error: %s", exc, exc_info=True)
            time.sleep(2)


if __name__ == "__main__":
    run_producer()
