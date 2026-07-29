"""
CloudPulse — Batch Layer (PySpark on EMR)
Reads raw NDJSON probe records from S3 (or local files in demo mode) and
computes full-history aggregate views that form the Batch Layer of the
Lambda Architecture.

Outputs written to S3 (or LOCAL_DATA_DIR/batch-results/):
  • latency_by_region.json   — avg / p50 / p95 latency per region
  • outage_frequency.json    — outage count, total duration per country
  • hourly_health_trend.json — avg health score per region per hour
  • top_affected.json        — top 10 most impacted countries
  • global_summary.json      — global aggregate KPIs

Run locally:
    spark-submit batch_layer/batch_job.py

Run on EMR (via submit_batch.py):
    spark-submit s3://<bucket>/scripts/batch_job.py \
        --input  s3://<bucket>/raw/ \
        --output s3://<bucket>/batch-results/
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# PySpark imports — only available when running under spark-submit
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType, StructField,
        StringType, DoubleType, LongType, BooleanType, IntegerType,
    )
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    print("[WARNING] PySpark not found — batch_job will run in pandas-fallback mode.")

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# ─── Schema ───────────────────────────────────────────────────────────────────
RAW_SCHEMA = StructType([
    StructField("event_id",         StringType(),  True),
    StructField("timestamp",        StringType(),  True),
    StructField("unix_ts",          LongType(),    True),
    StructField("probe_id",         StringType(),  True),
    StructField("region_id",        StringType(),  True),
    StructField("region_name",      StringType(),  True),
    StructField("country_code",     StringType(),  True),
    StructField("country_name",     StringType(),  True),
    StructField("continent",        StringType(),  True),
    StructField("lat",              DoubleType(),  True),
    StructField("lon",              DoubleType(),  True),
    StructField("avg_rtt_ms",       DoubleType(),  True),
    StructField("min_rtt_ms",       DoubleType(),  True),
    StructField("max_rtt_ms",       DoubleType(),  True),
    StructField("packet_loss_pct",  DoubleType(),  True),
    StructField("hop_count",        IntegerType(), True),
    StructField("is_reachable",     BooleanType(), True),
    StructField("outage_detected",  BooleanType(), True),
    StructField("health_score",     DoubleType(),  True),
    StructField("state",            StringType(),  True),
    StructField("provider",         StringType(),  True),
    StructField("flag",             StringType(),  True),
])

# ─── Spark session ─────────────────────────────────────────────────────────────
def create_spark_session(app_name: str = "CloudPulse-BatchProcessor") -> "SparkSession":
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    )
    # S3A configuration (set on EMR via instance profile; override locally if needed)
    s3_endpoint = os.environ.get("S3_ENDPOINT_URL")
    if s3_endpoint:
        builder = builder.config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
    return builder.getOrCreate()


# ─── Batch view computations ───────────────────────────────────────────────────
def compute_latency_by_region(df: "DataFrame") -> "DataFrame":
    """Per-region latency statistics over the full history."""
    return (
        df.groupBy("region_id", "region_name", "country_code", "country_name",
                   "continent", "lat", "lon", "flag", "provider")
          .agg(
              F.count("*").alias("probe_count"),
              F.avg("avg_rtt_ms").alias("avg_latency_ms"),
              F.min("avg_rtt_ms").alias("min_latency_ms"),
              F.max("avg_rtt_ms").alias("max_latency_ms"),
              F.percentile_approx("avg_rtt_ms", 0.50).alias("p50_latency_ms"),
              F.percentile_approx("avg_rtt_ms", 0.95).alias("p95_latency_ms"),
              F.avg("packet_loss_pct").alias("avg_packet_loss_pct"),
              F.avg("health_score").alias("avg_health_score"),
          )
          .orderBy(F.col("avg_latency_ms").desc())
    )


def compute_outage_frequency(df: "DataFrame") -> "DataFrame":
    """Outage count and duration per country over full history."""
    outages = df.filter(F.col("outage_detected") == True)
    return (
        outages.groupBy("country_code", "country_name")
               .agg(
                   F.count("*").alias("outage_events"),
                   F.countDistinct("region_id").alias("affected_regions"),
                   F.avg("packet_loss_pct").alias("avg_packet_loss_during_outage"),
                   F.avg("avg_rtt_ms").alias("avg_latency_during_outage"),
               )
               .orderBy(F.col("outage_events").desc())
    )


def compute_hourly_health_trend(df: "DataFrame") -> "DataFrame":
    """Hourly average health score per region (time-series for historical chart)."""
    return (
        df.withColumn("hour", F.date_trunc("hour", F.to_timestamp("timestamp")))
          .groupBy("hour", "region_id", "region_name", "country_code")
          .agg(
              F.avg("health_score").alias("avg_health_score"),
              F.avg("avg_rtt_ms").alias("avg_latency_ms"),
              F.avg("packet_loss_pct").alias("avg_packet_loss_pct"),
              F.count("*").alias("sample_count"),
          )
          .orderBy("region_id", "hour")
    )


def compute_top_affected(latency_df: "DataFrame", outage_df: "DataFrame") -> "DataFrame":
    """Top 10 most impacted countries combining latency + outage metrics."""
    joined = (
        latency_df
        .select("country_code", "country_name", "avg_latency_ms",
                "avg_packet_loss_pct", "avg_health_score")
        .join(
            outage_df.select("country_code", "outage_events", "affected_regions"),
            "country_code",
            "left",
        )
        .fillna(0, subset=["outage_events", "affected_regions"])
        # Composite impact score: lower health + more outages = worse
        .withColumn(
            "impact_score",
            (100 - F.col("avg_health_score")) * 0.6 + F.col("outage_events") * 0.4,
        )
        .orderBy(F.col("impact_score").desc())
        .limit(10)
    )
    return joined


def compute_global_summary(df: "DataFrame") -> dict:
    """Single-row global KPIs."""
    row = df.agg(
        F.avg("health_score").alias("global_health"),
        F.avg("avg_rtt_ms").alias("global_latency"),
        F.avg("packet_loss_pct").alias("global_loss"),
        F.count("*").alias("total_records"),
        F.sum(F.col("outage_detected").cast("int")).alias("total_outage_events"),
        F.countDistinct("country_code").alias("countries_monitored"),
        F.countDistinct("region_id").alias("regions_monitored"),
    ).first()

    return {
        "computed_at":        datetime.now(timezone.utc).isoformat(),
        "global_health_score": round(row["global_health"] or 0, 1),
        "global_avg_latency":  round(row["global_latency"] or 0, 1),
        "global_avg_loss":     round(row["global_loss"] or 0, 2),
        "total_records":       int(row["total_records"] or 0),
        "total_outage_events": int(row["total_outage_events"] or 0),
        "countries_monitored": int(row["countries_monitored"] or 0),
        "regions_monitored":   int(row["regions_monitored"] or 0),
    }


# ─── Sequential vs parallel benchmark helper ──────────────────────────────────
def benchmark_sequential_vs_parallel(spark: "SparkSession", df: "DataFrame") -> dict:
    """
    Compare sequential (1 partition) vs default-parallel execution of the
    latency aggregation job.  Results are used in Phase 3 benchmarking.
    """
    results = {}

    # Sequential (force single partition)
    t0 = time.perf_counter()
    compute_latency_by_region(df.coalesce(1)).count()
    results["sequential_s"] = round(time.perf_counter() - t0, 3)

    # Parallel (default parallelism from Spark config)
    t0 = time.perf_counter()
    compute_latency_by_region(df).count()
    results["parallel_s"] = round(time.perf_counter() - t0, 3)

    results["speedup"] = round(results["sequential_s"] / max(results["parallel_s"], 0.001), 2)
    results["worker_count"] = spark.sparkContext.defaultParallelism
    return results


# ─── Write helpers ─────────────────────────────────────────────────────────────
def _write_df(df: "DataFrame", path: str, fmt: str = "json") -> None:
    df.coalesce(1).write.mode("overwrite").json(path)
    print(f"[Batch] Written {df.count()} rows → {path}")


def _write_dict(data: dict, path: str) -> None:
    import json
    from pathlib import Path as P
    p = P(path)
    if str(path).startswith("s3"):
        # On EMR, use boto3
        import boto3
        bucket, key = path.replace("s3://", "").split("/", 1)
        boto3.client("s3").put_object(
            Bucket=bucket, Key=key, Body=json.dumps(data, indent=2).encode()
        )
    else:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(data, indent=2))
    print(f"[Batch] Written dict → {path}")


# ─── Entry point ───────────────────────────────────────────────────────────────
def main(input_path: str, output_path: str, run_benchmark: bool = True) -> None:
    if not SPARK_AVAILABLE:
        _pandas_fallback(input_path, output_path)
        return

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[Batch] Reading raw data from: {input_path}")
    t_start = time.perf_counter()

    df = spark.read.schema(RAW_SCHEMA).json(input_path)
    record_count = df.cache().count()
    print(f"[Batch] Loaded {record_count:,} records in {time.perf_counter() - t_start:.1f}s")

    # Compute views
    latency_df = compute_latency_by_region(df)
    outage_df  = compute_outage_frequency(df)
    trend_df   = compute_hourly_health_trend(df)
    top_df     = compute_top_affected(latency_df, outage_df)
    summary    = compute_global_summary(df)
    summary["batch_duration_s"] = round(time.perf_counter() - t_start, 2)

    # Benchmark
    if run_benchmark:
        bench = benchmark_sequential_vs_parallel(spark, df)
        summary["benchmark"] = bench
        print(f"[Batch] Benchmark: sequential={bench['sequential_s']}s  "
              f"parallel={bench['parallel_s']}s  speedup={bench['speedup']}x")

    # Write outputs
    _write_df(latency_df, f"{output_path}/latency_by_region")
    _write_df(outage_df,  f"{output_path}/outage_frequency")
    _write_df(trend_df,   f"{output_path}/hourly_health_trend")
    _write_df(top_df,     f"{output_path}/top_affected")
    _write_dict(summary,  f"{output_path}/global_summary.json")

    total_time = round(time.perf_counter() - t_start, 2)
    print(f"[Batch] Completed in {total_time}s. Records processed: {record_count:,}")
    spark.stop()


# ─── Pandas fallback (demo mode without Spark installed) ──────────────────────
def _pandas_fallback(input_path: str, output_path: str) -> None:
    """Light pandas fallback used in demo mode when PySpark is not installed."""
    import json, pandas as pd
    from pathlib import Path as P

    print("[Batch] PySpark not available — running pandas fallback …")
    raw_dir = P(input_path)
    files   = list(raw_dir.glob("*.ndjson"))
    if not files:
        print("[Batch] No raw data files found.")
        return

    records = []
    for f in files:
        for line in f.read_text().splitlines():
            if line.strip():
                records.append(json.loads(line))
    df = pd.DataFrame(records)

    out = P(output_path)
    out.mkdir(parents=True, exist_ok=True)

    # Latency by region
    lat = (df.groupby(["region_id", "region_name", "country_code", "country_name",
                        "continent", "lat", "lon", "flag", "provider"])
             .agg(probe_count=("event_id","count"),
                  avg_latency_ms=("avg_rtt_ms","mean"),
                  min_latency_ms=("avg_rtt_ms","min"),
                  max_latency_ms=("avg_rtt_ms","max"),
                  avg_packet_loss_pct=("packet_loss_pct","mean"),
                  avg_health_score=("health_score","mean"))
             .reset_index()
             .sort_values("avg_latency_ms", ascending=False))
    lat.to_json(out / "latency_by_region.json", orient="records", indent=2)

    # Outage frequency
    out_df = (df[df["outage_detected"] == True]
              .groupby(["country_code", "country_name"])
              .agg(outage_events=("event_id","count"))
              .reset_index()
              .sort_values("outage_events", ascending=False))
    out_df.to_json(out / "outage_frequency.json", orient="records", indent=2)

    # Global summary
    summary = {
        "computed_at":         datetime.now(timezone.utc).isoformat(),
        "global_health_score": round(float(df["health_score"].mean()), 1),
        "global_avg_latency":  round(float(df["avg_rtt_ms"].mean()), 1),
        "global_avg_loss":     round(float(df["packet_loss_pct"].mean()), 2),
        "total_records":       len(df),
        "total_outage_events": int(df["outage_detected"].sum()),
        "countries_monitored": int(df["country_code"].nunique()),
        "regions_monitored":   int(df["region_id"].nunique()),
        "engine":              "pandas-fallback",
    }
    (out / "global_summary.json").write_text(json.dumps(summary, indent=2))

    print(f"[Batch] Pandas fallback complete. {len(df):,} records → {output_path}")


# ─── CLI ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CloudPulse Batch Processor")
    parser.add_argument("--input",     default="data/raw",          help="Input path (S3 or local)")
    parser.add_argument("--output",    default="data/batch-results", help="Output path (S3 or local)")
    parser.add_argument("--no-bench",  action="store_true",          help="Skip seq/parallel benchmark")
    args = parser.parse_args()
    main(args.input, args.output, run_benchmark=not args.no_bench)
