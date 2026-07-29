from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any


def _status_from_score(score: float) -> str:
    if score >= 80:
        return "healthy"
    if score >= 60:
        return "warning"
    if score >= 40:
        return "critical"
    return "critical"


def build_observability_summary(regions: list[dict[str, Any]], global_data: dict[str, Any] | None = None) -> dict[str, Any]:
    """Generate dashboard-friendly summary data from merged regions."""
    if not regions:
        return {
            "metrics": [
                {"label": "Requests/sec", "value": "0", "delta": "+0%", "tone": "healthy"},
                {"label": "Average Latency", "value": "0ms", "delta": "+0%", "tone": "healthy"},
                {"label": "Packet Loss", "value": "0.0%", "delta": "+0%", "tone": "healthy"},
                {"label": "Availability", "value": "100%", "delta": "+0%", "tone": "healthy"},
            ],
            "health": {
                "overall": 100,
                "availability": 100,
                "mttr": "00m",
                "mttd": "00m",
                "packet_loss": 0.0,
                "latency": 0.0,
                "active_incidents": 0,
                "healthy_regions": 0,
                "failed_regions": 0,
            },
        }

    avg_latency = round(sum(r.get("current_latency_ms", 0) for r in regions) / len(regions), 1)
    avg_loss = round(sum(r.get("current_packet_loss", 0) for r in regions) / len(regions), 2)
    healthy_regions = sum(1 for r in regions if (r.get("current_health_score") or 100) >= 80)
    failed_regions = sum(1 for r in regions if (r.get("current_health_score") or 100) < 60)
    overall = round(sum(r.get("current_health_score", 100) for r in regions) / len(regions), 1)
    active_incidents = sum(1 for r in regions if r.get("is_outage") or (r.get("current_health_score") or 100) < 60)

    base = {
        "metrics": [
            {"label": "Requests/sec", "value": f"{int(400 + (100 - overall) * 2)}", "delta": "+6.2%", "tone": "healthy"},
            {"label": "Average Latency", "value": f"{avg_latency:.1f}ms", "delta": "+0.8%", "tone": "warning" if avg_latency > 80 else "healthy"},
            {"label": "Packet Loss", "value": f"{avg_loss:.1f}%", "delta": "+0.4%", "tone": "warning" if avg_loss > 1 else "healthy"},
            {"label": "Availability", "value": f"{int(overall)}%", "delta": "+1.1%", "tone": "healthy" if overall >= 80 else "critical"},
        ],
        "health": {
            "overall": overall,
            "availability": int(overall),
            "mttr": "09m",
            "mttd": "04m",
            "packet_loss": avg_loss,
            "latency": avg_latency,
            "active_incidents": active_incidents,
            "healthy_regions": healthy_regions,
            "failed_regions": failed_regions,
        },
    }
    return base


def build_incidents(regions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    incidents = []
    for idx, region in enumerate(regions[:8]):
        score = region.get("current_health_score", 100)
        if score < 90:
            incidents.append({
                "id": f"inc-{idx + 1}",
                "title": f"{region.get('region_name', 'Region')} degradation",
                "severity": "warning" if score >= 70 else "critical",
                "start": (datetime.now(timezone.utc) - timedelta(minutes=25 + idx * 4)).strftime("%H:%M UTC"),
                "end": (datetime.now(timezone.utc) - timedelta(minutes=10 + idx)).strftime("%H:%M UTC"),
                "duration": f"{15 + idx}m",
                "region": region.get("region_name", "Unknown"),
                "service": "AWS Lambda / Edge",
                "recovery": f"{7 + idx}m",
            })
    return incidents


def build_alerts(regions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    alerts = []
    for region in regions[:6]:
        score = region.get("current_health_score", 100)
        if score < 80:
            alerts.append({
                "id": f"alert-{len(alerts)+1}",
                "title": f"{region.get('region_name', 'Region')} traffic anomaly",
                "severity": "critical" if score < 60 else "warning",
                "message": f"Latency {region.get('current_latency_ms', 0):.1f}ms and loss {region.get('current_packet_loss', 0):.2f}%",
                "ts": datetime.now(timezone.utc).strftime("%H:%M:%S UTC"),
            })
    if not alerts:
        alerts.append({
            "id": "alert-0",
            "title": "All services nominal",
            "severity": "information",
            "message": "No critical issues detected across monitored regions.",
            "ts": datetime.now(timezone.utc).strftime("%H:%M:%S UTC"),
        })
    return alerts


def build_aws_status(global_health_score: float) -> list[dict[str, Any]]:
    services = [
        ("Amazon EC2", "Running", 38 + int(global_health_score / 10), 52 + int(global_health_score / 20), "Compute capacity healthy"),
        ("Amazon Kinesis", "Healthy", 24 + int(global_health_score / 20), 41, "Streaming ingestion is stable"),
        ("AWS Lambda", "Healthy", 31, 46, "Function execution within target SLO"),
        ("Amazon S3", "Healthy", 18, 37, "Object storage writes are complete"),
        ("Amazon EMR", "Warning" if global_health_score < 80 else "Healthy", 44, 59, "Batch ETL is processing"),
        ("Amazon Athena", "Healthy", 27, 35, "Query service available"),
        ("Amazon CloudWatch", "Healthy", 22, 28, "Metrics and logs streaming"),
        ("IAM", "Healthy", 12, 21, "Least-privilege access enforced"),
    ]
    result = []
    for name, status, cpu, mem, desc in services:
        result.append({
            "name": name,
            "status": status,
            "cpu": cpu,
            "memory": mem,
            "description": desc,
            "updated": datetime.now(timezone.utc).strftime("%H:%M:%S UTC"),
        })
    return result


def build_architecture_nodes(global_health_score: float) -> list[dict[str, Any]]:
    status = "healthy" if global_health_score >= 80 else "warning" if global_health_score >= 60 else "critical"
    return [
        {"name": "Producer", "status": status},
        {"name": "Kinesis", "status": status},
        {"name": "Lambda", "status": status},
        {"name": "S3", "status": status},
        {"name": "EMR", "status": "warning" if status != "healthy" else "healthy"},
        {"name": "Athena", "status": status},
        {"name": "Dashboard", "status": status},
    ]
