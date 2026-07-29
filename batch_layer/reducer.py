#!/usr/bin/env python3
"""
CloudPulse — Hadoop Streaming Reducer
Reads sorted mapper output from stdin and aggregates per country_code:
  • count, avg_latency, avg_packet_loss, total_outages, avg_health_score

Output (one JSON line per country):
    {"country_code": "US", "count": 1234, "avg_latency_ms": 14.2, ...}

Must be executable: chmod +x reducer.py
"""

import json
import sys
from collections import defaultdict


def main() -> None:
    # Aggregation accumulators
    counts:      dict[str, int]   = defaultdict(int)
    lat_sum:     dict[str, float] = defaultdict(float)
    loss_sum:    dict[str, float] = defaultdict(float)
    outages:     dict[str, int]   = defaultdict(int)
    health_sum:  dict[str, float] = defaultdict(float)
    regions:     dict[str, set]   = defaultdict(set)

    for raw_line in sys.stdin:
        line = raw_line.strip()
        if not line or "\t" not in line:
            continue

        country, value = line.split("\t", 1)
        parts = value.split(",")
        if len(parts) < 5:
            continue

        try:
            latency = float(parts[0])
            loss    = float(parts[1])
            outage  = int(parts[2])
            health  = float(parts[3])
            region  = parts[4]
        except ValueError:
            continue

        counts[country]     += 1
        lat_sum[country]    += latency
        loss_sum[country]   += loss
        outages[country]    += outage
        health_sum[country] += health
        regions[country].add(region)

    # Emit one JSON line per country
    for country in sorted(counts):
        n = counts[country]
        result = {
            "country_code":          country,
            "probe_count":           n,
            "avg_latency_ms":        round(lat_sum[country] / n, 2),
            "avg_packet_loss_pct":   round(loss_sum[country] / n, 2),
            "total_outage_events":   outages[country],
            "avg_health_score":      round(health_sum[country] / n, 1),
            "affected_regions":      list(regions[country]),
        }
        sys.stdout.write(json.dumps(result) + "\n")


if __name__ == "__main__":
    main()
