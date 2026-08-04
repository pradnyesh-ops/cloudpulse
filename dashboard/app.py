"""
CloudPulse — Flask Dashboard Server
Serves the live monitoring dashboard and exposes REST + SSE endpoints.

In demo mode the server:
  1. Spawns a background producer thread (ingestion/producer.py).
  2. Spawns a background speed-layer thread (speed_layer/stream_processor.py).
  3. Spawns a background merge thread (serving_layer/merge.py).
All three run in-process and share in-memory state, so the demo requires
zero AWS credentials.

Endpoints:
  GET  /                      → main dashboard HTML
  GET  /api/map-data          → merged region list (for Leaflet markers)
  GET  /api/global            → merged global KPIs
  GET  /api/top-affected      → top 10 worst regions
  GET  /api/history           → speed-layer global health history (last 2 h)
  GET  /api/live-events       → last 50 outage events
  GET  /api/batch-summary     → batch-layer global summary
  GET  /stream                → Server-Sent Events push channel (real-time)

Run:
    python dashboard/app.py
    # or
    DEMO_MODE=false python dashboard/app.py   (production; reads from S3)
"""

from __future__ import annotations

import json
import logging
import os
import queue
import sys
import threading
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Any

from flask import Flask, Response, jsonify, render_template, stream_with_context
from flask_cors import CORS

try:
    from .helpers import build_alerts, build_architecture_nodes, build_aws_status, build_incidents, build_observability_summary
except ImportError:  # pragma: no cover - allows running app.py directly
    from helpers import build_alerts, build_architecture_nodes, build_aws_status, build_incidents, build_observability_summary

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import (
    AWS_REGION, DASHBOARD_DEBUG, DASHBOARD_HOST, DASHBOARD_PORT,
    DEMO_MODE, INGESTION_RATE, LOCAL_DATA_DIR, SECRET_KEY,
    SPEED_SLIDE_SECONDS, USE_REAL_STREAM,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [Dashboard] %(levelname)s — %(message)s")

app = Flask(__name__, template_folder="templates", static_folder="static")
app.secret_key = SECRET_KEY
CORS(app)

# ─── SSE subscriber registry ──────────────────────────────────────────────────
_sse_queues: list[queue.Queue] = []
_sse_lock = threading.Lock()


def _broadcast_sse(event_type: str, data: object) -> None:
    """Push an SSE event to all connected clients."""
    msg = f"event: {event_type}\ndata: {json.dumps(data, default=str)}\n\n"
    dead: list[queue.Queue] = []
    with _sse_lock:
        for q in _sse_queues:
            try:
                q.put_nowait(msg)
            except queue.Full:
                dead.append(q)
        for q in dead:
            _sse_queues.remove(q)


# ─── In-process state (demo mode) ────────────────────────────────────────────
# Imported lazily to avoid circular imports before config is ready.
_speed_state: dict = {}
_merge_state:  dict = {}
_sqs_client = None
_sqs_queue_url: str | None = None


def _get_sqs_queue_url() -> str:
    global _sqs_client, _sqs_queue_url
    if _sqs_queue_url:
        return _sqs_queue_url
    import boto3
    from config import SQS_QUEUE_NAME
    _sqs_client = boto3.client("sqs", region_name=AWS_REGION)
    _sqs_queue_url = _sqs_client.get_queue_url(QueueName=SQS_QUEUE_NAME)["QueueUrl"]
    return _sqs_queue_url


def _publish_to_sqs(record: dict) -> None:
    if _sqs_client is None:
        _get_sqs_queue_url()
    _sqs_client.send_message(QueueUrl=_sqs_queue_url, MessageBody=json.dumps(record))


def _load_live_state_from_dynamodb() -> list[dict]:
    try:
        import boto3
        from config import DYNAMODB_TABLE
        table = boto3.resource("dynamodb", region_name=AWS_REGION).Table(DYNAMODB_TABLE)
        response = table.scan()
        items = response.get("Items", [])
        while response.get("LastEvaluatedKey"):
            response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
            items.extend(response.get("Items", []))
        return items
    except Exception as exc:
        log.warning("DynamoDB live-state read failed: %s", exc)
        return []


def _load_json_file(rel: str) -> object:
    path = Path(LOCAL_DATA_DIR) / rel
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            pass
    return None


def _load_from_s3(key: str) -> object:
    try:
        import boto3
        s3  = boto3.client("s3", region_name=AWS_REGION)
        obj = s3.get_object(Bucket=os.environ.get("S3_BUCKET", "cloudpulse-data-bucket"), Key=key)
        return json.loads(obj["Body"].read())
    except Exception:
        return None


def _fetch(local_rel: str, s3_key: str) -> object:
    if DEMO_MODE:
        return _load_json_file(local_rel)
    return _load_from_s3(s3_key)


# ── Field-name normaliser (raw probe → merged region format) ─────────────────
def _status_from_score(score: float) -> str:
    if score >= 80: return "healthy"
    if score >= 60: return "degraded"
    if score >= 40: return "warning"
    return "critical"


def _normalize_probe_records(current_state: dict) -> list[dict]:
    """Convert raw probe records (from speed-layer current_state) to the
    merged-region format that the dashboard JS expects."""
    result = []
    for r in current_state.values():
        score = r.get("health_score", 100.0)
        result.append({
            "region_id":            r.get("region_id", ""),
            "region_name":          r.get("region_name", ""),
            "country_code":         r.get("country_code", "XX"),
            "country_name":         r.get("country_name", "Unknown"),
            "continent":            r.get("continent", ""),
            "lat":                  r.get("lat", 0),
            "lon":                  r.get("lon", 0),
            "flag":                 r.get("flag", ""),
            "provider":             r.get("provider", ""),
            "current_health_score": round(score, 1),
            "current_latency_ms":   round(r.get("avg_rtt_ms",     50.0), 1),
            "current_packet_loss":  round(r.get("packet_loss_pct", 0.0), 2),
            "is_outage":            r.get("outage_detected", False),
            "state":                r.get("state", "healthy"),
            "status":               _status_from_score(score),
            "hist_health_score":    round(score, 1),
            "hist_latency_ms":      round(r.get("avg_rtt_ms", 50.0), 1),
            "impact_score":         round(100.0 - score, 2),
        })
    return sorted(result, key=lambda x: x["impact_score"], reverse=True)


def _global_from_state(current_state: dict) -> dict:
    """Compute live global KPIs directly from in-memory probe records."""
    records = list(current_state.values())
    if not records:
        return {}
    n       = len(records)
    health  = round(sum(r.get("health_score",    100.0) for r in records) / n, 1)
    latency = round(sum(r.get("avg_rtt_ms",      50.0)  for r in records) / n, 1)
    loss    = round(sum(r.get("packet_loss_pct",  0.0)  for r in records) / n, 2)
    outages = sum(1 for r in records if r.get("outage_detected", False))
    return {
        "global_health_score": health,
        "global_avg_latency":  latency,
        "global_avg_loss":     loss,
        "active_outages":      outages,
        "regions_monitored":   n,
        "merged_at":           datetime.now(timezone.utc).isoformat(),
        "layer":               "live",
    }


# ─── Background threads (demo mode) ──────────────────────────────────────────
def _start_demo_threads() -> None:
    from speed_layer.stream_processor import ingest_record, get_current_state
    from serving_layer.merge import run_merge

    if USE_REAL_STREAM:
        from ingestion.data_sources import stream_wikimedia
        def _producer_thread() -> None:
            log.info("Wikimedia real-stream producer started")
            for record in stream_wikimedia():
                try:
                    _publish_to_sqs(record)
                    _broadcast_sse("outage_event", {
                        "region":  record["region_name"],
                        "country": record["country_name"],
                        "flag":    record.get("flag", ""),
                        "severity":record.get("state", "unknown"),
                        "latency": record.get("avg_rtt_ms"),
                        "loss":    record.get("packet_loss_pct"),
                        "health":  record.get("health_score"),
                        "event_type": "Wikimedia activity",
                        "ts":      record.get("timestamp"),
                    })
                except Exception as exc:
                    log.error("Wikimedia producer error: %s", exc)
    else:
        from ingestion.data_sources import REGIONS, generate_probe_record
        def _producer_thread() -> None:
            log.info("Demo producer started (simulated data)")
            interval = 1.0 / max(INGESTION_RATE, 0.1)
            idx = 0
            while True:
                try:
                    region = REGIONS[idx % len(REGIONS)]
                    record = generate_probe_record(region)
                    ingest_record(record)
                    if record.get("outage_detected") or record.get("state") in ("warning", "critical"):
                        _broadcast_sse("outage_event", {
                            "region":  record["region_name"],
                            "country": record["country_name"],
                            "flag":    record.get("flag", ""),
                            "severity":record.get("state", "unknown"),
                            "latency": record.get("avg_rtt_ms"),
                            "loss":    record.get("packet_loss_pct"),
                            "health":  record.get("health_score"),
                            "ts":      record.get("timestamp"),
                        })
                    idx += 1
                    time.sleep(interval)
                except Exception as exc:
                    log.error("Producer thread error: %s", exc)
                    time.sleep(2)

    def _speed_layer_thread() -> None:
        from speed_layer.stream_processor import (
            _compute_window_aggregates, _compute_global_kpis, _flush_results
        )
        log.info("Demo speed-layer thread started")
        first_run = True
        while True:
            # First flush after 5 s so dashboard has data immediately;
            # subsequent flushes on the full SPEED_SLIDE_SECONDS cadence.
            time.sleep(5 if first_run else SPEED_SLIDE_SECONDS)
            first_run = False
            try:
                now  = time.time()
                aggs = _compute_window_aggregates(now)
                kpis = _compute_global_kpis(aggs, now)
                _flush_results(aggs, kpis, demo=True)
                _broadcast_sse("window_update", {
                    "global_health":  kpis.get("global_health_score"),
                    "active_outages": kpis.get("active_outages"),
                    "global_latency": kpis.get("global_avg_latency"),
                    "regions":        len(aggs),
                    "ts":             datetime.now(timezone.utc).isoformat(),
                })
            except Exception as exc:
                log.error("Speed-layer thread error: %s", exc)

    def _merge_thread() -> None:
        log.info("Demo merge thread started")
        first_run = True
        while True:
            time.sleep(8 if first_run else 15)
            first_run = False
            try:
                run_merge(demo=True)
            except Exception as exc:
                log.error("Merge thread error: %s", exc)

    for target, name in [
        (_producer_thread,     "demo-producer"),
        (_speed_layer_thread,  "demo-speed-layer"),
        (_merge_thread,        "demo-merge"),
    ]:
        t = threading.Thread(target=target, name=name, daemon=True)
        t.start()
        log.info("Started background thread: %s", name)


# ─── Routes ───────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/map-data")
def api_map_data():
    if USE_REAL_STREAM and not DEMO_MODE:
        records = _load_live_state_from_dynamodb()
        if records:
            return jsonify(_normalize_probe_records({r.get("region_id", ""): r for r in records}))
    # 1. Try fully merged file (speed + batch combined)
    data = _fetch("merged-results/merged_regions.json", "merged-results/merged_regions.json")
    if data:
        return jsonify(data)
    # 2. Try speed-layer aggregated file
    data = _fetch("speed-results/window_aggregates.json", "speed-results/window_aggregates.json")
    if data:
        return jsonify(data)
    # 3. Fallback: in-memory current_state (always available after ~1 s)
    from speed_layer.stream_processor import get_current_state
    cs = get_current_state().get("current_state", {})
    return jsonify(_normalize_probe_records(cs))


@app.route("/api/global")
def api_global():
    if USE_REAL_STREAM and not DEMO_MODE:
        records = _load_live_state_from_dynamodb()
        if records:
            return jsonify(_global_from_state({r.get("region_id", ""): r for r in records}))
    data = _fetch("merged-results/merged_global.json", "merged-results/merged_global.json")
    if data:
        return jsonify(data)
    # Try speed-layer global
    data = _fetch("speed-results/speed_global.json", "speed-results/speed_global.json")
    if data:
        return jsonify(data)
    # Fallback: compute live from in-memory probe records
    from speed_layer.stream_processor import get_current_state
    cs = get_current_state().get("current_state", {})
    return jsonify(_global_from_state(cs))


@app.route("/api/top-affected")
def api_top_affected():
    if USE_REAL_STREAM and not DEMO_MODE:
        records = _load_live_state_from_dynamodb()
        if records:
            return jsonify(_normalize_probe_records({r.get("region_id", ""): r for r in records})[:10])
    data = _fetch("merged-results/top_affected.json", "merged-results/top_affected.json")
    if data:
        return jsonify(data[:10])
    # Fallback: derive from in-memory state
    from speed_layer.stream_processor import get_current_state
    cs = get_current_state().get("current_state", {})
    normalized = _normalize_probe_records(cs)
    return jsonify(normalized[:10])


@app.route("/api/history")
def api_history():
    data = _fetch("speed-results/history.json", "speed-results/history.json")
    if data:
        return jsonify(data)
    from speed_layer.stream_processor import get_current_state
    return jsonify(list(get_current_state().get("history", [])))


@app.route("/api/live-events")
def api_live_events():
    data = _fetch("speed-results/live_events.json", "speed-results/live_events.json")
    if data:
        return jsonify(data[:50])
    from speed_layer.stream_processor import get_current_state
    return jsonify(list(get_current_state().get("live_events", [])))


@app.route("/api/batch-summary")
def api_batch_summary():
    data = _fetch("batch-results/global_summary.json", "batch-results/global_summary.json")
    return jsonify(data or {"message": "Batch layer not yet run. Execute: python batch_layer/batch_job.py"})


@app.route("/api/observability")
def api_observability():
    from speed_layer.stream_processor import get_current_state
    cs = get_current_state().get("current_state", {})
    regions = _normalize_probe_records(cs)
    return jsonify(build_observability_summary(regions, _global_from_state(cs)))


@app.route("/api/incidents")
def api_incidents():
    from speed_layer.stream_processor import get_current_state
    cs = get_current_state().get("current_state", {})
    regions = _normalize_probe_records(cs)
    return jsonify(build_incidents(regions))


@app.route("/api/alerts")
def api_alerts():
    from speed_layer.stream_processor import get_current_state
    cs = get_current_state().get("current_state", {})
    regions = _normalize_probe_records(cs)
    return jsonify(build_alerts(regions))


@app.route("/api/aws-status")
def api_aws_status():
    from speed_layer.stream_processor import get_current_state
    cs = get_current_state().get("current_state", {})
    global_data = _global_from_state(cs)
    return jsonify(build_aws_status(global_data.get("global_health_score", 100)))


@app.route("/api/architecture")
def api_architecture():
    from speed_layer.stream_processor import get_current_state
    cs = get_current_state().get("current_state", {})
    global_data = _global_from_state(cs)
    return jsonify(build_architecture_nodes(global_data.get("global_health_score", 100)))


@app.route("/api/window-aggregates")
def api_window_aggregates():
    data = _fetch("speed-results/window_aggregates.json", "speed-results/window_aggregates.json")
    return jsonify(data or [])


@app.route("/api/health")
def api_health():
    return jsonify({"status": "ok", "mode": "demo" if DEMO_MODE else "production",
                    "ts": datetime.now(timezone.utc).isoformat()})


@app.route("/stream")
def stream():
    """Server-Sent Events endpoint. Clients connect once and receive push updates."""
    def generate():
        q: queue.Queue = queue.Queue(maxsize=50)
        with _sse_lock:
            _sse_queues.append(q)
        try:
            # Send a hello event immediately
            yield f"event: connected\ndata: {json.dumps({'mode': 'demo' if DEMO_MODE else 'prod'})}\n\n"
            while True:
                try:
                    msg = q.get(timeout=30)
                    yield msg
                except queue.Empty:
                    yield ": heartbeat\n\n"  # keep connection alive
        except GeneratorExit:
            pass
        finally:
            with _sse_lock:
                if q in _sse_queues:
                    _sse_queues.remove(q)

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control":  "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


# ─── Entry point ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    Path(LOCAL_DATA_DIR).mkdir(parents=True, exist_ok=True)
    if DEMO_MODE or USE_REAL_STREAM:
        if USE_REAL_STREAM and not DEMO_MODE:
            log.info("Starting in LIVE Wikimedia mode (no demo data)")
        else:
            log.info("Starting in DEMO mode (no AWS credentials required)")
        _start_demo_threads()
    else:
        log.info("Starting in PRODUCTION mode (reads from AWS)")

    app.run(
        host=DASHBOARD_HOST,
        port=DASHBOARD_PORT,
        debug=DASHBOARD_DEBUG,
        use_reloader=False,   # disable reloader so background threads survive
        threaded=True,
    )
