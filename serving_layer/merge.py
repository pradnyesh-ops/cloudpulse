"""
CloudPulse — Serving Layer: Merge
Combines batch-layer historical results with speed-layer real-time windows
to produce a unified "serving view" — the Lambda Architecture merge.

Merge logic (per region):
  • If speed layer has data for a region  → use speed-layer health / latency
    for current values; use batch layer for historical baseline.
  • If speed layer has no data (cold start) → use batch layer values only.
  • Impact score is computed to rank regions for the "top affected" list.

Outputs:
  • merged_regions.json    — one entry per region (map markers + table)
  • merged_global.json     — combined global KPIs
  • top_affected.json      — top 10 worst regions (for dashboard table)

In demo mode, reads from LOCAL_DATA_DIR/{speed,batch}-results/.
In production,  reads from s3://<bucket>/{speed,batch}-results/.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import (
    AWS_REGION, DEMO_MODE,
    LOCAL_DATA_DIR, S3_BUCKET, S3_BATCH_PREFIX, S3_MERGED_PREFIX, S3_SPEED_PREFIX,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [Serving] %(message)s")


# ─── Storage helpers ──────────────────────────────────────────────────────────
def _read_local(rel_path: str) -> Any:
    path = Path(LOCAL_DATA_DIR) / rel_path
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except Exception as exc:
        log.warning("Failed to read %s: %s", path, exc)
        return None


def _write_local(data: Any, rel_path: str) -> None:
    path = Path(LOCAL_DATA_DIR) / rel_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str))


def _read_s3(key: str) -> Any:
    try:
        s3  = boto3.client("s3", region_name=AWS_REGION)
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "NoSuchKey":
            return None
        log.warning("S3 read failed for %s: %s", key, exc)
        return None


def _write_s3(data: Any, key: str) -> None:
    try:
        s3   = boto3.client("s3", region_name=AWS_REGION)
        body = json.dumps(data, default=str).encode()
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body, ContentType="application/json")
    except ClientError as exc:
        log.error("S3 write failed for %s: %s", key, exc)


def read_json(path_or_key: str, demo: bool = DEMO_MODE) -> Any:
    return _read_local(path_or_key) if demo else _read_s3(path_or_key)


def write_json(data: Any, path_or_key: str, demo: bool = DEMO_MODE) -> None:
    if demo:
        _write_local(data, path_or_key)
    else:
        _write_s3(data, path_or_key)


# ─── Load layer results ───────────────────────────────────────────────────────
def load_speed_results(demo: bool = DEMO_MODE) -> dict:
    """Load speed-layer window aggregates. Returns {region_id: agg_dict}."""
    if demo:
        raw = read_json("speed-results/window_aggregates.json", demo=True)
    else:
        raw = read_json(f"{S3_SPEED_PREFIX}window_aggregates.json", demo=False)

    if not raw or not isinstance(raw, list):
        return {}
    return {item["region_id"]: item for item in raw if "region_id" in item}


def load_batch_results(demo: bool = DEMO_MODE) -> dict:
    """Load batch-layer latency stats. Returns {region_id: stat_dict}."""
    if demo:
        raw = read_json("batch-results/latency_by_region.json", demo=True)
    else:
        raw = read_json(f"{S3_BATCH_PREFIX}latency_by_region.json", demo=False)

    if not raw or not isinstance(raw, list):
        return {}
    return {item["region_id"]: item for item in raw if "region_id" in item}


def load_batch_global(demo: bool = DEMO_MODE) -> dict:
    if demo:
        return read_json("batch-results/global_summary.json", demo=True) or {}
    return read_json(f"{S3_BATCH_PREFIX}global_summary.json", demo=False) or {}


def load_speed_global(demo: bool = DEMO_MODE) -> dict:
    if demo:
        return read_json("speed-results/speed_global.json", demo=True) or {}
    return read_json(f"{S3_SPEED_PREFIX}speed_global.json", demo=False) or {}


# ─── Merge logic ──────────────────────────────────────────────────────────────
def _status_from_health(score: float) -> str:
    if score >= 80:
        return "healthy"
    if score >= 60:
        return "degraded"
    if score >= 40:
        return "warning"
    return "critical"


def merge_regions(speed: dict, batch: dict) -> list[dict]:
    """
    Produce merged view for every known region.
    Speed-layer values are used as the "current" snapshot;
    batch-layer values provide the 24 h historical baseline.
    """
    all_region_ids = set(speed) | set(batch)
    merged = []

    for rid in all_region_ids:
        sp = speed.get(rid, {})
        ba = batch.get(rid, {})

        # Current (real-time) values from speed layer
        current_health  = sp.get("avg_health_score",  ba.get("avg_health_score",  100.0))
        current_latency = sp.get("avg_latency_ms",    ba.get("avg_latency_ms",    50.0))
        current_loss    = sp.get("avg_packet_loss",   ba.get("avg_packet_loss_pct", 0.0))
        is_outage       = sp.get("is_outage",         False)
        state           = sp.get("current_state",     "healthy")

        # Historical baseline from batch layer
        hist_health  = ba.get("avg_health_score",   current_health)
        hist_latency = ba.get("avg_latency_ms",     current_latency)
        hist_loss    = ba.get("avg_packet_loss_pct", current_loss)

        # Geographic / metadata
        lat  = sp.get("lat",          ba.get("lat",  0))
        lon  = sp.get("lon",          ba.get("lon",  0))
        flag = sp.get("flag",         ba.get("flag", ""))
        name = sp.get("region_name",  ba.get("region_name", rid))
        cc   = sp.get("country_code", ba.get("country_code", "XX"))
        cn   = sp.get("country_name", ba.get("country_name", "Unknown"))
        cont = sp.get("continent",    ba.get("continent", ""))
        prov = sp.get("provider",     ba.get("provider", ""))

        # Impact score for ranking (higher = worse)
        impact = round((100 - current_health) * 0.6 + current_loss * 1.0 + (20 if is_outage else 0), 2)

        merged.append({
            "region_id":           rid,
            "region_name":         name,
            "country_code":        cc,
            "country_name":        cn,
            "continent":           cont,
            "lat":                 lat,
            "lon":                 lon,
            "flag":                flag,
            "provider":            prov,
            # Real-time (speed layer)
            "current_health_score": round(current_health, 1),
            "current_latency_ms":   round(current_latency, 1),
            "current_packet_loss":  round(current_loss, 2),
            "is_outage":            is_outage,
            "state":                state,
            "status":               _status_from_health(current_health),
            # Historical baseline (batch layer)
            "hist_health_score":    round(hist_health, 1),
            "hist_latency_ms":      round(hist_latency, 1),
            "hist_packet_loss":     round(hist_loss, 2),
            # Combined
            "impact_score":        impact,
            "merged_at":           datetime.now(timezone.utc).isoformat(),
            "data_sources": {
                "speed_layer": bool(sp),
                "batch_layer": bool(ba),
            },
        })

    # Sort by impact score descending (worst first)
    return sorted(merged, key=lambda x: x["impact_score"], reverse=True)


def merge_global(speed_g: dict, batch_g: dict, regions: list[dict]) -> dict:
    """Combine global KPIs from both layers."""
    n = len(regions)
    if n == 0:
        return {}

    active_outages   = sum(1 for r in regions if r["is_outage"])
    global_health    = round(sum(r["current_health_score"] for r in regions) / n, 1)
    global_latency   = round(sum(r["current_latency_ms"]   for r in regions) / n, 1)
    global_loss      = round(sum(r["current_packet_loss"]  for r in regions) / n, 2)

    return {
        "merged_at":            datetime.now(timezone.utc).isoformat(),
        "global_health_score":  global_health,
        "global_avg_latency":   global_latency,
        "global_avg_loss":      global_loss,
        "active_outages":       active_outages,
        "regions_monitored":    n,
        "total_records_batch":  batch_g.get("total_records",       0),
        "total_outage_events":  batch_g.get("total_outage_events", 0),
        "batch_health":         batch_g.get("global_health_score", global_health),
        "speed_health":         speed_g.get("global_health_score", global_health),
        "layer":                "merged",
    }


# ─── Public merge runner ──────────────────────────────────────────────────────
def run_merge(demo: bool = DEMO_MODE) -> dict:
    """Load both layers, merge, write outputs, return merged global KPIs."""
    speed_r  = load_speed_results(demo)
    batch_r  = load_batch_results(demo)
    speed_g  = load_speed_global(demo)
    batch_g  = load_batch_global(demo)

    regions      = merge_regions(speed_r, batch_r)
    global_kpis  = merge_global(speed_g, batch_g, regions)
    top_affected = regions[:10]

    # Write outputs
    write_json(regions,      "merged-results/merged_regions.json",  demo)
    write_json(global_kpis,  "merged-results/merged_global.json",   demo)
    write_json(top_affected, "merged-results/top_affected.json",    demo)

    log.info(
        "Merge complete: %d regions | health=%.1f | outages=%d",
        len(regions),
        global_kpis.get("global_health_score", 0),
        global_kpis.get("active_outages", 0),
    )
    return global_kpis


# ─── Continuous merge loop ────────────────────────────────────────────────────
def run_merge_loop(interval_s: int = 15, demo: bool = DEMO_MODE) -> None:
    """Re-merge every `interval_s` seconds."""
    log.info("Serving merge loop starting (interval=%ds) …", interval_s)
    while True:
        try:
            run_merge(demo)
        except Exception as exc:
            log.error("Merge error: %s", exc, exc_info=True)
        time.sleep(interval_s)


if __name__ == "__main__":
    run_merge_loop()
