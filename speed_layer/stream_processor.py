"""
CloudPulse — Speed Layer: Kinesis Stream Processor
Consumes records from the Kinesis stream, maintains a sliding-window state in
memory, and continuously writes fresh real-time views to S3 (or local JSON
files in demo mode).

Speed-layer outputs:
  • current_state.json       — latest health per region (for map markers)
  • window_aggregates.json   — 5-min sliding window stats per region
  • live_events.json         — rolling ring buffer of last 50 outage events
  • speed_global.json        — global real-time KPIs

Sliding window:
  • Width  : 5 minutes (SPEED_WINDOW_SECONDS)
  • Slide  : every 60 s  (SPEED_SLIDE_SECONDS)

Run locally (demo mode):
    python speed_layer/stream_processor.py

Run in production on EC2 (DEMO_MODE=false):
    python speed_layer/stream_processor.py
"""

from __future__ import annotations

import collections
import json
import logging
import sys
import time
import threading
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import (
    AWS_REGION, DEMO_MODE, KINESIS_STREAM_NAME,
    LOCAL_DATA_DIR, S3_BUCKET, S3_SPEED_PREFIX,
    SPEED_SLIDE_SECONDS, SPEED_WINDOW_SECONDS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SpeedLayer] %(levelname)s — %(message)s",
)
log = logging.getLogger(__name__)

# ─── Shared in-process state (used when dashboard imports this module) ─────────
_shared_state: dict = {
    "current_state":    {},   # region_id → latest record
    "window_aggs":      {},   # region_id → windowed aggregates
    "live_events":      deque(maxlen=50),
    "speed_global":     {},
    "history":          deque(maxlen=120),  # last 120 global snapshots (2 hrs @ 1/min)
}

# Sliding window: per-region deque of (unix_ts, record)
_window: dict[str, deque] = defaultdict(lambda: deque())

_lock = threading.Lock()


# ─── Window management ────────────────────────────────────────────────────────
def _evict_old(now_ts: float) -> None:
    """Remove records older than SPEED_WINDOW_SECONDS from all windows."""
    cutoff = now_ts - SPEED_WINDOW_SECONDS
    for region_id, dq in _window.items():
        while dq and dq[0][0] < cutoff:
            dq.popleft()


def _ingest_record(record: dict) -> None:
    """Add a new record to the sliding window and update current state."""
    region_id = record.get("region_id", "unknown")
    unix_ts   = record.get("unix_ts", int(time.time()))

    with _lock:
        _window[region_id].append((unix_ts, record))
        _shared_state["current_state"][region_id] = record

        event = {
            "ts":          record.get("timestamp"),
            "region_id":   region_id,
            "region_name": record.get("region_name"),
            "country":     record.get("country_name"),
            "flag":        record.get("flag", ""),
            "severity":    record.get("state", "unknown"),
            "latency_ms":  record.get("avg_rtt_ms"),
            "packet_loss": record.get("packet_loss_pct"),
            "health":      record.get("health_score"),
            "event_type":  "Wikimedia activity" if record.get("measurement_type") == "wikimedia-edit-proxy" else "Network telemetry",
        }
        _shared_state["live_events"].appendleft(event)


# ─── Aggregate computation ────────────────────────────────────────────────────
def _compute_window_aggregates(now_ts: float) -> dict:
    """
    Compute windowed aggregates for every region with data in the window.
    Returns {region_id: {agg_fields…}}
    """
    _evict_old(now_ts)
    aggs = {}
    with _lock:
        for region_id, dq in _window.items():
            if not dq:
                continue
            records = [r for _, r in dq]
            n       = len(records)

            latencies   = [r.get("avg_rtt_ms",     0.0) for r in records]
            losses      = [r.get("packet_loss_pct", 0.0) for r in records]
            health_vals = [r.get("health_score",    0.0) for r in records]
            outage_cnt  = sum(1 for r in records if r.get("outage_detected"))

            latest = records[-1]
            _avg_health  = round(sum(health_vals) / n, 1)
            _avg_latency = round(sum(latencies) / n, 2)
            _avg_loss    = round(sum(losses) / n, 2)
            _is_outage   = outage_cnt > n * 0.3

            def _status(s: float) -> str:
                return "healthy" if s >= 80 else "degraded" if s >= 60 else "warning" if s >= 40 else "critical"

            aggs[region_id] = {
                "region_id":           region_id,
                "region_name":         latest.get("region_name"),
                "country_code":        latest.get("country_code"),
                "country_name":        latest.get("country_name"),
                "continent":           latest.get("continent"),
                "lat":                 latest.get("lat"),
                "lon":                 latest.get("lon"),
                "flag":                latest.get("flag", ""),
                "provider":            latest.get("provider", ""),
                "window_start":        datetime.fromtimestamp(now_ts - SPEED_WINDOW_SECONDS, tz=timezone.utc).isoformat(),
                "window_end":          datetime.fromtimestamp(now_ts, tz=timezone.utc).isoformat(),
                "window_size_s":       SPEED_WINDOW_SECONDS,
                "sample_count":        n,
                # canonical names (used by serving-layer merge + dashboard JS)
                "avg_health_score":    _avg_health,
                "avg_latency_ms":      _avg_latency,
                "avg_packet_loss":     _avg_loss,
                # aliased names expected by dashboard JS (merged-region format)
                "current_health_score": _avg_health,
                "current_latency_ms":   _avg_latency,
                "current_packet_loss":  _avg_loss,
                "hist_health_score":    _avg_health,
                "hist_latency_ms":      _avg_latency,
                "status":               _status(_avg_health),
                "impact_score":         round(100.0 - _avg_health, 2),
                "min_latency_ms":       round(min(latencies), 2),
                "max_latency_ms":       round(max(latencies), 2),
                "outage_count":         outage_cnt,
                "is_outage":            _is_outage,
                "current_state":        latest.get("state", "unknown"),
            }
    return aggs


def _compute_global_kpis(aggs: dict, now_ts: float) -> dict:
    vals = list(aggs.values())
    if not vals:
        return {}
    n = len(vals)
    global_health  = round(sum(v["avg_health_score"] for v in vals) / n, 1)
    global_latency = round(sum(v["avg_latency_ms"]   for v in vals) / n, 1)
    global_loss    = round(sum(v["avg_packet_loss"]   for v in vals) / n, 2)
    active_outages = sum(1 for v in vals if v["is_outage"])

    kpis = {
        "computed_at":         datetime.fromtimestamp(now_ts, tz=timezone.utc).isoformat(),
        "global_health_score": global_health,
        "global_avg_latency":  global_latency,
        "global_avg_loss":     global_loss,
        "active_outages":      active_outages,
        "regions_monitored":   n,
        "layer":               "speed",
    }

    with _lock:
        _shared_state["speed_global"] = kpis
        _shared_state["history"].appendleft({
            "ts":           kpis["computed_at"],
            "health_score": global_health,
            "latency_ms":   global_latency,
            "loss_pct":     global_loss,
        })

    return kpis


# ─── Storage writers ──────────────────────────────────────────────────────────
def _write_local(data: object, filename: str) -> None:
    path = Path(LOCAL_DATA_DIR) / "speed-results" / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str))


def _write_s3(data: object, key: str) -> None:
    try:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        body = json.dumps(data, default=str).encode()
        s3.put_object(Bucket=S3_BUCKET, Key=f"{S3_SPEED_PREFIX}{key}", Body=body,
                      ContentType="application/json")
    except ClientError as exc:
        log.error("S3 write failed for %s: %s", key, exc)


def _flush_results(aggs: dict, global_kpis: dict, demo: bool) -> None:
    now = datetime.now(timezone.utc).isoformat()
    write = _write_local if demo else _write_s3

    regions_list = sorted(aggs.values(), key=lambda v: v["avg_health_score"])
    write(regions_list,                                  "window_aggregates.json")
    write(global_kpis,                                   "speed_global.json")
    write(list(_shared_state["live_events"]),            "live_events.json")
    write(list(_shared_state["history"]),                "history.json")

    # current_state indexed by region_id
    with _lock:
        write(list(_shared_state["current_state"].values()), "current_state.json")


# ─── Kinesis consumer (production mode) ───────────────────────────────────────
def _get_shard_iterators(client, stream_name: str) -> list[str]:
    resp   = client.describe_stream(StreamName=stream_name)
    shards = resp["StreamDescription"]["Shards"]
    iters  = []
    for shard in shards:
        si = client.get_shard_iterator(
            StreamName=stream_name,
            ShardId=shard["ShardId"],
            ShardIteratorType="LATEST",
        )
        iters.append(si["ShardIterator"])
    return iters


def _poll_kinesis(shard_iters: list[str], client) -> tuple[list[dict], list[str]]:
    """Poll all shards, return (records, updated_iterators)."""
    all_records = []
    new_iters   = []
    for it in shard_iters:
        if not it:
            continue
        try:
            resp = client.get_records(ShardIterator=it, Limit=200)
            for r in resp["Records"]:
                try:
                    payload = json.loads(r["Data"])
                    all_records.append(payload)
                except json.JSONDecodeError:
                    pass
            new_iters.append(resp.get("NextShardIterator", ""))
        except ClientError as exc:
            log.warning("Kinesis get_records error: %s", exc)
            new_iters.append("")
    return all_records, new_iters


# ─── Main processing loop ─────────────────────────────────────────────────────
def run_stream_processor(
    demo: bool = DEMO_MODE,
    on_flush: Callable[[dict, dict], None] | None = None,
) -> None:
    """
    Main loop:
      1. Receive records (from Kinesis or shared in-process queue)
      2. Ingest into sliding window
      3. Every SPEED_SLIDE_SECONDS → compute aggregates + flush results
    """
    log.info("Speed-layer stream processor starting — mode=%s  window=%ds  slide=%ds",
             "DEMO" if demo else "PROD", SPEED_WINDOW_SECONDS, SPEED_SLIDE_SECONDS)

    kinesis_client = None
    shard_iters: list[str] = []

    if not demo:
        kinesis_client = boto3.client("kinesis", region_name=AWS_REGION)
        shard_iters    = _get_shard_iterators(kinesis_client, KINESIS_STREAM_NAME)
        log.info("Connected to Kinesis '%s' — %d shards", KINESIS_STREAM_NAME, len(shard_iters))
    else:
        log.info("Demo mode — waiting for records via _ingest_record()")

    last_flush = time.time()

    while True:
        now_ts = time.time()

        # In production: poll Kinesis
        if not demo and kinesis_client:
            records, shard_iters = _poll_kinesis(shard_iters, kinesis_client)
            for r in records:
                _ingest_record(r)
            time.sleep(1)

        # Slide every SPEED_SLIDE_SECONDS
        if now_ts - last_flush >= SPEED_SLIDE_SECONDS:
            aggs        = _compute_window_aggregates(now_ts)
            global_kpis = _compute_global_kpis(aggs, now_ts)
            _flush_results(aggs, global_kpis, demo)

            if on_flush:
                on_flush(aggs, global_kpis)

            log.info(
                "Window flush: %d regions | global_health=%.1f | active_outages=%d",
                len(aggs),
                global_kpis.get("global_health_score", 0),
                global_kpis.get("active_outages", 0),
            )
            last_flush = now_ts

        if demo:
            time.sleep(1)


# ─── Public API for dashboard (in-process usage) ──────────────────────────────
def get_current_state() -> dict:
    return dict(_shared_state)


def ingest_record(record: dict) -> None:
    """Called by the producer/dashboard to push a record into the speed layer."""
    _ingest_record(record)


if __name__ == "__main__":
    run_stream_processor()
