# CloudPulse — Global Internet Outage Intelligence Platform

**Architecture: Lambda (Batch + Speed + Serving)**  
**Stack: Python · AWS Kinesis · EMR · PySpark · S3 · Athena · Lambda · Flask · Leaflet.js**

---

## Real-Time Question

> **"Which regions or cloud services are experiencing internet issues right now?"**

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│              AUTO-SCALING INFRASTRUCTURE (EC2 ASG / EMR)        │
│                                                                  │
│  Data Source          Ingestion            Speed Layer           │
│  ──────────    ──────────────────────    ─────────────────────   │
│  RIPE Atlas  ─►  Kinesis Data Streams  ─►  Lambda Function       │
│  Simulated       (2–N shards, SSE)         Sliding Windows       │
│  probes                                    (5-min / 1-min slide) │
│                         │                       │                │
│                         ▼                       ▼                │
│                       S3 raw/          S3 speed-results/         │
│                         │                       │                │
│                         ▼               Serving Layer            │
│                   Batch Layer          ──────────────────        │
│                   ──────────────       merge.py                  │
│                   PySpark on EMR ─────► merged_regions.json      │
│                   Hadoop Streaming     merged_global.json        │
│                         │                       │                │
│                         ▼                       ▼                │
│                   S3 batch-results/    S3 merged-results/        │
│                         │                       │                │
│                         └───────────┬───────────┘                │
│                                     ▼                            │
│                             Flask Dashboard                       │
│                             (EC2 Auto-Scaling Group)             │
│                          World Map · Charts · Live Feed           │
└─────────────────────────────────────────────────────────────────┘
```

### Lambda Architecture Justification

| Layer | Purpose | Technology |
|-------|---------|-----------|
| **Batch** | Correctness over all history | PySpark on EMR · Hadoop Streaming |
| **Speed** | Low-latency recent views | Kinesis · AWS Lambda · sliding windows |
| **Serving** | Merge + query | S3 + Athena · Flask REST API |

---

## Project Structure

```
cloudpulse/
├── config.py                   # Central configuration (env-var driven)
├── requirements.txt
├── .env.example                # Copy → .env and fill values
│
├── infrastructure/
│   ├── setup_aws.py            # One-command AWS resource creation
│   ├── auto_scaling.py         # EC2 ASG + CloudWatch alarms
│   └── teardown_aws.py         # Clean teardown
│
├── ingestion/
│   ├── data_sources.py         # Probe simulator + RIPE Atlas connector
│   └── producer.py             # Kinesis / local file producer
│
├── batch_layer/
│   ├── batch_job.py            # PySpark batch job (main)
│   ├── mapper.py               # Hadoop Streaming mapper
│   ├── reducer.py              # Hadoop Streaming reducer
│   └── submit_batch.py         # EMR job submitter
│
├── speed_layer/
│   ├── stream_processor.py     # Kinesis consumer + 5-min sliding window
│   └── lambda_function.py      # AWS Lambda handler (Kinesis trigger)
│
├── serving_layer/
│   ├── merge.py                # Batch + speed merge (Lambda "serve")
│   └── athena_queries.py       # Athena DDL + named queries
│
├── dashboard/
│   ├── app.py                  # Flask server + SSE push channel
│   ├── templates/index.html    # World-map dashboard UI
│   └── static/
│       ├── css/style.css
│       └── js/dashboard.js
│
└── benchmarks/
    └── benchmark.py            # Phase 3 performance measurement
```

---

## Quick Start (Demo Mode — No AWS Required)

```bash
# 1. Clone and install
git clone https://github.com/<your-org>/cloudpulse.git
cd cloudpulse
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 2. Configure (demo mode by default)
cp .env.example .env

# 3. Run the dashboard (starts all background threads automatically)
python dashboard/app.py

# 4. Open http://localhost:5000
```

The demo mode:
- Generates realistic probe data across 39 global regions
- Runs the speed layer (5-min sliding window) in-process
- Runs the merge layer every 15 s
- Shows live map, charts, and outage feed — no AWS credentials needed

---

## Production Deployment (AWS)

```bash
# Prerequisites: AWS credentials configured (aws configure or IAM role)
# Region used in this project: eu-west-1

# 0. Configure environment for production
cp .env.example .env
# Edit .env and ensure:
#   DEMO_MODE=false
#   AWS_REGION=eu-west-1
#   S3_BUCKET=<your-globally-unique-bucket>

# 1. Create all AWS resources
python infrastructure/setup_aws.py

# 2. Create/update ASG bootstrap from your GitHub repo
# Replace URL with your repository
python infrastructure/auto_scaling.py --create --repo-url https://github.com/<your-user>/<your-repo>.git --repo-branch main

# 3. Start data ingestion (local or on EC2)
DEMO_MODE=false python ingestion/producer.py

# 4. Submit the batch job to EMR (creates cluster if needed)
python batch_layer/submit_batch.py --create --spark --wait

# 5. Run the dashboard (reads from S3)
DEMO_MODE=false python dashboard/app.py

# 6. Set up Athena for ad-hoc SQL queries
python serving_layer/athena_queries.py --setup
```

### One-command Europe bootstrap

```bash
# Runs setup_aws + auto_scaling in AWS eu-west-1
chmod +x infrastructure/deploy_eu.sh
./infrastructure/deploy_eu.sh https://github.com/<your-user>/<your-repo>.git main
```

### Temporary fallback when Kinesis is unavailable

If your AWS account returns `SubscriptionRequiredException` for Kinesis,
CloudPulse now falls back to SQS automatically when `STREAM_BACKEND=auto`
(default).

```bash
# Optional: force SQS fallback explicitly
export STREAM_BACKEND=sqs

# Run setup (creates SQS queue + Lambda SQS trigger)
python infrastructure/setup_aws.py --stream-backend sqs

# Start ingestion (producer publishes to SQS in production mode)
DEMO_MODE=false STREAM_BACKEND=sqs python ingestion/producer.py
```

## Push To GitHub

```bash
# 1. Initialize local commits
git add .
git commit -m "Initial CloudPulse AWS deployment setup"

# 2. Create GitHub repository (via UI), then set remote
git remote add origin https://github.com/<your-user>/<your-repo>.git

# 3. Push main branch
git push -u origin main
```

---

## AWS Services Used

| Service | Role |
|---------|------|
| **Kinesis Data Streams** | Real-time ingestion (2 shards, ~10 Mbps) |
| **AWS Lambda** | Speed-layer processor (triggered per Kinesis batch) |
| **EC2 + Auto-Scaling Group** | Dashboard server; scales 1–5 instances on CPU |
| **EMR (Spark + Hadoop)** | Batch layer; managed scaling 2–10 workers |
| **S3** | Raw data, batch results, speed results, merged serving view |
| **Athena** | Ad-hoc SQL over the full history |
| **DynamoDB** | Real-time per-region state store (TTL 10 min) |
| **CloudWatch** | Metrics, alarms, ASG scaling triggers |

**Auto-Scaling Policy:**
- Scale **out**: CPU > 70 % for 2 min → add instance (cooldown 300 s)
- Scale **in**: CPU < 30 % for 10 min → remove instance (cooldown 300 s)
- Min 1 · Desired 2 · Max 5 instances

---

## Running Benchmarks (Phase 3)

```bash
# Run all benchmarks and generate plots
python benchmarks/benchmark.py --all

# Individual tests
python benchmarks/benchmark.py --batch-speedup --workers 8
python benchmarks/benchmark.py --ingestion-rate
python benchmarks/benchmark.py --e2e --records 500

# Plot saved results
python benchmarks/benchmark.py --plot
```

Outputs saved to `data/benchmarks/` as JSON + PNG charts.

---

## Batch Layer — Hadoop Streaming (MapReduce)

```bash
# Local test
cat data/raw/*.ndjson | python batch_layer/mapper.py | sort | python batch_layer/reducer.py

# Submit to EMR
python batch_layer/submit_batch.py --hadoop
```

---

## Speed Layer Windows

| Parameter | Value |
|-----------|-------|
| Window width | 5 minutes |
| Slide interval | 60 seconds |
| Output: top-N | "Top 5 trending issues in last 5 minutes" |
| Output: rolling avg | Per-region latency average |
| Output: outage flag | region marked as outage if >30% of window records show issues |

---

## Dashboard Features

| Panel | Data Source |
|-------|-------------|
| World map (Leaflet) | Speed layer → merged regions |
| Global health score | Serving layer merge |
| Top affected regions | Serving layer (impact score) |
| Live outage feed | Speed layer SSE push |
| Health trend (60 min) | Speed layer history |
| Regional latency bar | Speed layer window aggregates |
| Packet loss chart | Speed layer window aggregates |
| Historical analysis | Batch layer global summary |
