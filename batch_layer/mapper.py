#!/usr/bin/env python3
"""
CloudPulse — Hadoop Streaming Mapper
Reads NDJSON probe records from stdin (one per line) and emits:
    <country_code>\t<latency_ms>,<packet_loss>,<outage_flag>

Used with Hadoop Streaming on EMR:
    hadoop jar hadoop-streaming.jar \
        -input  s3://<bucket>/raw/ \
        -output s3://<bucket>/hadoop-out/ \
        -mapper mapper.py \
        -reducer reducer.py

Must be executable: chmod +x mapper.py
"""

import json
import sys


def emit(key: str, value: str) -> None:
    sys.stdout.write(f"{key}\t{value}\n")


def main() -> None:
    for raw_line in sys.stdin:
        line = raw_line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue

        country = record.get("country_code", "XX")
        latency = record.get("avg_rtt_ms", 0.0)
        loss    = record.get("packet_loss_pct", 0.0)
        outage  = 1 if record.get("outage_detected", False) else 0
        health  = record.get("health_score", 100.0)
        region  = record.get("region_id", "unknown")

        # Emit: key=country_code  value=<csv fields>
        emit(country, f"{latency:.3f},{loss:.3f},{outage},{health:.2f},{region}")


if __name__ == "__main__":
    main()
