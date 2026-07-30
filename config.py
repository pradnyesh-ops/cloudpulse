"""
CloudPulse — Global Internet Outage Intelligence Platform
Central configuration. All values can be overridden via environment variables or .env file.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ─── AWS ──────────────────────────────────────────────────────────────────────
AWS_REGION          = os.environ.get("AWS_REGION", "eu-west-1")
AWS_PROFILE         = os.environ.get("AWS_PROFILE", "default")

# ─── Kinesis ──────────────────────────────────────────────────────────────────
KINESIS_STREAM_NAME = os.environ.get("KINESIS_STREAM_NAME", "cloudpulse-stream")
KINESIS_SHARD_COUNT = int(os.environ.get("KINESIS_SHARD_COUNT", "2"))

# ─── Streaming backend fallback ───────────────────────────────────────────────
# STREAM_BACKEND values:
#   auto    -> use Kinesis if available, otherwise SQS fallback
#   kinesis -> force Kinesis only
#   sqs     -> force SQS only
STREAM_BACKEND      = os.environ.get("STREAM_BACKEND", "auto").lower()
SQS_QUEUE_NAME      = os.environ.get("SQS_QUEUE_NAME", "cloudpulse-stream-queue")

# ─── S3 ───────────────────────────────────────────────────────────────────────
S3_BUCKET           = os.environ.get("S3_BUCKET", "cloudpulse-data-bucket")
S3_RAW_PREFIX       = "raw/"
S3_BATCH_PREFIX     = "batch-results/"
S3_SPEED_PREFIX     = "speed-results/"
S3_MERGED_PREFIX    = "merged-results/"
S3_SCRIPTS_PREFIX   = "scripts/"
S3_EMR_LOGS_PREFIX  = "emr-logs/"

# ─── EMR ──────────────────────────────────────────────────────────────────────
EMR_CLUSTER_NAME         = "cloudpulse-emr-cluster"
EMR_RELEASE_LABEL        = "emr-6.15.0"
EMR_MASTER_INSTANCE_TYPE = "m5.xlarge"
EMR_WORKER_INSTANCE_TYPE = "m5.xlarge"
EMR_MIN_WORKERS          = 2
EMR_MAX_WORKERS          = 10
EMR_LOG_URI              = f"s3://{S3_BUCKET}/{S3_EMR_LOGS_PREFIX}"

# ─── Athena ───────────────────────────────────────────────────────────────────
ATHENA_DATABASE         = "cloudpulse_db"
ATHENA_WORKGROUP        = "primary"
ATHENA_OUTPUT_LOCATION  = f"s3://{S3_BUCKET}/athena-results/"

# ─── DynamoDB (real-time state store) ─────────────────────────────────────────
DYNAMODB_TABLE = "cloudpulse-speed-layer"
DYNAMODB_TTL   = 600   # seconds — keep 10 minutes of fine-grained records

# ─── CloudWatch ───────────────────────────────────────────────────────────────
CLOUDWATCH_NAMESPACE    = "CloudPulse"
CLOUDWATCH_LOG_GROUP    = "/cloudpulse/stream-processor"

# ─── Auto-Scaling ─────────────────────────────────────────────────────────────
ASG_NAME            = "cloudpulse-asg"
ASG_MIN_SIZE        = 1
ASG_MAX_SIZE        = 5
ASG_DESIRED_CAPACITY = 2
ASG_SCALE_OUT_CPU   = 70   # percent — add instance when CPU > 70 %
ASG_SCALE_IN_CPU    = 30   # percent — remove instance when CPU < 30 % (10 min)
ASG_COOLDOWN        = 300  # seconds

# ─── Lambda Architecture Windows ──────────────────────────────────────────────
SPEED_WINDOW_SECONDS = 300   # 5-minute sliding window
SPEED_SLIDE_SECONDS  = 60    # 1-minute slide interval
BATCH_RUN_INTERVAL_H = 1     # Run full batch every N hours

# ─── Dashboard ────────────────────────────────────────────────────────────────
DASHBOARD_HOST  = os.environ.get("DASHBOARD_HOST", "0.0.0.0")
DASHBOARD_PORT  = int(os.environ.get("DASHBOARD_PORT", "5000"))
DASHBOARD_DEBUG = os.environ.get("DASHBOARD_DEBUG", "true").lower() == "true"
SECRET_KEY      = os.environ.get("SECRET_KEY", os.urandom(24).hex())

# ─── Demo / Simulation Mode ───────────────────────────────────────────────────
# DEMO_MODE=true  → no AWS needed; data is generated and stored locally
# DEMO_MODE=false → full AWS pipeline (Kinesis, EMR, S3, Lambda)
DEMO_MODE      = os.environ.get("DEMO_MODE", "true").lower() == "true"
PROBE_COUNT    = int(os.environ.get("PROBE_COUNT", "50"))
INGESTION_RATE = float(os.environ.get("INGESTION_RATE", "5.0"))   # records/sec
LOCAL_DATA_DIR = os.environ.get("LOCAL_DATA_DIR", "data")

# ─── RIPE Atlas (optional, public data, no key required) ─────────────────────
RIPE_ATLAS_API_BASE = "https://atlas.ripe.net/api/v2"
USE_RIPE_ATLAS      = os.environ.get("USE_RIPE_ATLAS", "false").lower() == "true"

# ─── Wikimedia Event Streams (real-time public SSE, no key required) ──────────
# Set USE_REAL_STREAM=true to consume live Wikipedia edit events instead of
# simulated probe data.  URL: https://stream.wikimedia.org/v2/stream/recentchange
USE_REAL_STREAM = os.environ.get("USE_REAL_STREAM", "false").lower() == "true"
WIKIMEDIA_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
