"""
CloudPulse — Phase 3 Performance Benchmarking
Measures throughput, latency, and speedup across:
  1. Sequential vs parallel batch job execution (vary worker count)
  2. Speed-layer latency vs ingestion rate
  3. Kinesis throughput over time
  4. End-to-end pipeline latency (ingest → serve)

Results are saved to data/benchmarks/ as JSON + matplotlib PNGs.

Usage:
    python benchmarks/benchmark.py --all              # run everything
    python benchmarks/benchmark.py --batch-speedup    # sequential vs parallel
    python benchmarks/benchmark.py --ingestion-rate   # vary ingestion rate
    python benchmarks/benchmark.py --e2e              # end-to-end latency
    python benchmarks/benchmark.py --plot             # plot saved results
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
import time
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

RESULTS_DIR = Path("data/benchmarks")
RESULTS_DIR.mkdir(parents=True, exist_ok=True)


# ─── Helpers ──────────────────────────────────────────────────────────────────
def _save(name: str, data: dict) -> Path:
    ts   = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = RESULTS_DIR / f"{name}_{ts}.json"
    path.write_text(json.dumps(data, indent=2))
    print(f"  Saved → {path}")
    return path


def _print_table(headers: list, rows: list) -> None:
    widths = [max(len(str(h)), max(len(str(r[i])) for r in rows))
              for i, h in enumerate(headers)]
    sep    = "─" * (sum(widths) + 3 * len(widths) - 1)
    print(f"\n  {sep}")
    print("  " + " │ ".join(str(h).ljust(widths[i]) for i, h in enumerate(headers)))
    print(f"  {sep}")
    for row in rows:
        print("  " + " │ ".join(str(v).ljust(widths[i]) for i, v in enumerate(row)))
    print(f"  {sep}\n")


# ─── Benchmark 1: Sequential vs Parallel Batch ────────────────────────────────
def bench_batch_speedup(max_workers: int = 8) -> dict:
    """
    Simulate the batch job with increasing parallelism.
    In demo mode: uses pandas with varying chunk sizes.
    In production: submit real Spark jobs with varying executor counts.
    """
    print("\n[Benchmark 1] Sequential vs Parallel Batch Processing")
    print(f"  Testing 1 … {max_workers} workers")

    import pandas as pd
    import numpy as np

    # Generate synthetic dataset (~50k records)
    rng = np.random.default_rng(42)
    n   = 50_000
    df  = pd.DataFrame({
        "region_id":        rng.choice(["us-east","de","jp","sg","br","za"], n),
        "avg_rtt_ms":       rng.uniform(10, 500, n),
        "packet_loss_pct":  rng.uniform(0, 50, n),
        "health_score":     rng.uniform(0, 100, n),
        "outage_detected":  rng.random(n) < 0.1,
        "country_code":     rng.choice(["US","DE","JP","SG","BR","ZA"], n),
    })

    def _compute(chunk: pd.DataFrame) -> pd.DataFrame:
        return chunk.groupby("region_id").agg(
            avg_latency=("avg_rtt_ms","mean"),
            avg_loss=("packet_loss_pct","mean"),
            avg_health=("health_score","mean"),
            outages=("outage_detected","sum"),
        )

    worker_counts = [1, 2, 4, max_workers] if max_workers >= 4 else list(range(1, max_workers + 1))
    results = []
    base_time = None

    for workers in worker_counts:
        # Simulate parallelism by splitting df into 'workers' chunks
        chunks     = [df.iloc[i::workers] for i in range(workers)]
        t0         = time.perf_counter()

        if workers == 1:
            _compute(df)                     # sequential: process all at once
        else:
            from concurrent.futures import ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=workers) as pool:
                list(pool.map(_compute, chunks))  # parallel chunks

        elapsed = round(time.perf_counter() - t0, 3)
        if base_time is None:
            base_time = elapsed

        speedup = round(base_time / elapsed, 2)
        efficiency = round(speedup / workers * 100, 1)
        results.append({
            "workers":    workers,
            "time_s":     elapsed,
            "speedup":    speedup,
            "efficiency": efficiency,
        })
        print(f"  workers={workers:2d}  time={elapsed:.3f}s  speedup={speedup:.2f}x  efficiency={efficiency}%")

    _print_table(
        ["Workers", "Time (s)", "Speedup", "Efficiency %"],
        [[r["workers"], r["time_s"], r["speedup"], r["efficiency"]] for r in results]
    )

    data = {"benchmark": "batch_speedup", "records": n, "results": results,
            "ts": datetime.now(timezone.utc).isoformat()}
    _save("batch_speedup", data)
    return data


# ─── Benchmark 2: Speed-Layer Latency vs Ingestion Rate ──────────────────────
def bench_ingestion_rate() -> dict:
    """
    Measure speed-layer processing latency at different ingestion rates.
    Uses the in-process stream_processor.
    """
    print("\n[Benchmark 2] Speed-Layer Latency vs Ingestion Rate")

    from ingestion.data_sources import REGIONS, generate_probe_record
    from speed_layer.stream_processor import _ingest_record, _compute_window_aggregates

    rates      = [1, 5, 10, 25, 50, 100, 250]   # records per second
    duration_s = 5                                # test each rate for 5 s
    results    = []

    for rate in rates:
        interval = 1.0 / rate
        sent     = 0
        latencies = []

        deadline = time.time() + duration_s
        idx      = 0
        while time.time() < deadline:
            region = REGIONS[idx % len(REGIONS)]
            record = generate_probe_record(region)

            t0 = time.perf_counter()
            _ingest_record(record)
            ingest_lat = (time.perf_counter() - t0) * 1000   # ms

            latencies.append(ingest_lat)
            sent += 1
            idx  += 1

            sleep = interval - (time.perf_counter() - t0)
            if sleep > 0:
                time.sleep(sleep)

        # Compute aggregation latency
        t0  = time.perf_counter()
        _compute_window_aggregates(time.time())
        agg_lat = (time.perf_counter() - t0) * 1000

        actual_rate = round(sent / duration_s, 1)
        p50 = round(statistics.median(latencies), 3)
        p95 = round(sorted(latencies)[int(0.95 * len(latencies))], 3)
        avg = round(statistics.mean(latencies), 3)

        results.append({
            "target_rate":   rate,
            "actual_rate":   actual_rate,
            "records_sent":  sent,
            "avg_ingest_ms": avg,
            "p50_ingest_ms": p50,
            "p95_ingest_ms": p95,
            "agg_latency_ms": round(agg_lat, 2),
        })
        print(f"  rate={rate:4d} r/s  actual={actual_rate:6.1f}  "
              f"p50={p50:.3f}ms  p95={p95:.3f}ms  agg={agg_lat:.2f}ms")

    _print_table(
        ["Target r/s", "Actual r/s", "p50 (ms)", "p95 (ms)", "Agg (ms)"],
        [[r["target_rate"], r["actual_rate"], r["p50_ingest_ms"],
          r["p95_ingest_ms"], r["agg_latency_ms"]] for r in results]
    )

    data = {"benchmark": "ingestion_rate", "duration_s": duration_s, "results": results,
            "ts": datetime.now(timezone.utc).isoformat()}
    _save("ingestion_rate", data)
    return data


# ─── Benchmark 3: End-to-End Pipeline Latency ────────────────────────────────
def bench_e2e_latency(n_records: int = 200) -> dict:
    """
    Measure total time from record generation → window aggregation → merge output.
    Simulates the full pipeline in-process.
    """
    print("\n[Benchmark 3] End-to-End Pipeline Latency")
    from ingestion.data_sources import REGIONS, generate_probe_record
    from speed_layer.stream_processor import _ingest_record, _compute_window_aggregates
    from serving_layer.merge import merge_regions, load_speed_results, load_batch_results

    e2e_latencies = []

    for i in range(n_records):
        region = REGIONS[i % len(REGIONS)]
        t0     = time.perf_counter()

        # Stage 1: generate + ingest
        record = generate_probe_record(region)
        _ingest_record(record)

        # Stage 2: window aggregation (every 10th record)
        if i % 10 == 0:
            _compute_window_aggregates(time.time())

        # Stage 3: merge (every 50th record)
        if i % 50 == 0:
            speed_r = {}
            batch_r = {}
            merge_regions(speed_r, batch_r)

        e2e_ms = (time.perf_counter() - t0) * 1000
        e2e_latencies.append(e2e_ms)

    p50  = round(statistics.median(e2e_latencies), 3)
    p95  = round(sorted(e2e_latencies)[int(0.95 * len(e2e_latencies))], 3)
    p99  = round(sorted(e2e_latencies)[int(0.99 * len(e2e_latencies))], 3)
    avg  = round(statistics.mean(e2e_latencies), 3)
    total = round(sum(e2e_latencies), 1)

    print(f"  Records: {n_records}  Total: {total}ms  Avg: {avg}ms  p50: {p50}ms  p95: {p95}ms  p99: {p99}ms")

    data = {
        "benchmark": "e2e_latency",
        "n_records":   n_records,
        "total_ms":    total,
        "avg_ms":      avg,
        "p50_ms":      p50,
        "p95_ms":      p95,
        "p99_ms":      p99,
        "samples":     e2e_latencies[:100],  # first 100 for plotting
        "ts":          datetime.now(timezone.utc).isoformat(),
    }
    _save("e2e_latency", data)
    return data


# ─── Plotting ─────────────────────────────────────────────────────────────────
def plot_results() -> None:
    """Generate matplotlib charts from saved benchmark JSON files."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.style as mstyle
        mstyle.use("dark_background")
    except ImportError:
        print("matplotlib not installed. Run: pip install matplotlib")
        return

    for json_file in sorted(RESULTS_DIR.glob("*.json")):
        data = json.loads(json_file.read_text())
        bench = data.get("benchmark")

        fig, ax = plt.subplots(figsize=(9, 5))
        fig.patch.set_facecolor("#0d1117")
        ax.set_facecolor("#0d1117")
        ax.tick_params(colors="#8b949e")
        ax.spines[:].set_color("#30363d")

        if bench == "batch_speedup":
            workers = [r["workers"] for r in data["results"]]
            speedup = [r["speedup"] for r in data["results"]]
            ideal   = [float(w) for w in workers]
            ax.plot(workers, speedup, "o-", color="#00d4aa", linewidth=2, label="Actual speedup")
            ax.plot(workers, ideal,   "--", color="#388bfd", linewidth=1, label="Ideal (linear)", alpha=0.6)
            ax.set_xlabel("Number of Workers", color="#8b949e")
            ax.set_ylabel("Speedup (×)",       color="#8b949e")
            ax.set_title("Batch Layer: Speedup vs Worker Count", color="#e6edf3")
            ax.legend()

        elif bench == "ingestion_rate":
            rates  = [r["target_rate"]   for r in data["results"]]
            p95    = [r["p95_ingest_ms"] for r in data["results"]]
            p50    = [r["p50_ingest_ms"] for r in data["results"]]
            ax.fill_between(rates, p50, p95, alpha=0.2, color="#00d4aa", label="p50–p95 band")
            ax.plot(rates, p50, "o-", color="#00d4aa", linewidth=2, label="p50 latency")
            ax.plot(rates, p95, "s--",color="#f78166", linewidth=1.5, label="p95 latency")
            ax.set_xlabel("Ingestion Rate (records/s)", color="#8b949e")
            ax.set_ylabel("Ingest Latency (ms)",        color="#8b949e")
            ax.set_title("Speed Layer: Latency vs Ingestion Rate", color="#e6edf3")
            ax.legend()

        elif bench == "e2e_latency":
            samples = data.get("samples", [])
            ax.plot(samples, color="#00d4aa", linewidth=0.8, label="E2E latency (ms)")
            ax.axhline(data["p50_ms"], color="#3fb950", linestyle="--", linewidth=1, label=f"p50={data['p50_ms']}ms")
            ax.axhline(data["p95_ms"], color="#f78166", linestyle="--", linewidth=1, label=f"p95={data['p95_ms']}ms")
            ax.set_xlabel("Record #",       color="#8b949e")
            ax.set_ylabel("Latency (ms)",   color="#8b949e")
            ax.set_title("End-to-End Pipeline Latency", color="#e6edf3")
            ax.legend()

        else:
            plt.close(fig)
            continue

        out = json_file.with_suffix(".png")
        fig.tight_layout()
        fig.savefig(out, dpi=150)
        plt.close(fig)
        print(f"  Chart saved → {out}")


# ─── CLI ──────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="CloudPulse Performance Benchmarks")
    parser.add_argument("--all",            action="store_true", help="Run all benchmarks")
    parser.add_argument("--batch-speedup",  action="store_true", help="Batch speedup test")
    parser.add_argument("--ingestion-rate", action="store_true", help="Ingestion rate vs latency")
    parser.add_argument("--e2e",            action="store_true", help="End-to-end latency")
    parser.add_argument("--plot",           action="store_true", help="Plot saved results")
    parser.add_argument("--workers",        type=int, default=8,   help="Max workers for speedup test")
    parser.add_argument("--records",        type=int, default=200,  help="Records for e2e test")
    args = parser.parse_args()

    if args.plot:
        plot_results()
        return

    if args.all or args.batch_speedup:
        bench_batch_speedup(max_workers=args.workers)

    if args.all or args.ingestion_rate:
        bench_ingestion_rate()

    if args.all or args.e2e:
        bench_e2e_latency(n_records=args.records)

    if args.all:
        plot_results()

    if not any([args.all, args.batch_speedup, args.ingestion_rate, args.e2e, args.plot]):
        parser.print_help()


if __name__ == "__main__":
    main()
