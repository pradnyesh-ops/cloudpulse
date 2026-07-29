"""
CloudPulse — Teardown Script
Deletes all AWS resources created by setup_aws.py and auto_scaling.py.
Requires explicit confirmation before any destructive action.

Usage:
    python infrastructure/teardown_aws.py
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import (
    ASG_NAME, AWS_REGION, DYNAMODB_TABLE,
    KINESIS_STREAM_NAME, S3_BUCKET,
)

print("\nCloudPulse Teardown — this will DELETE all AWS resources.")
confirm = input("Type 'DELETE' to confirm: ")
if confirm != "DELETE":
    print("Aborted.")
    sys.exit(0)


def _section(name: str) -> None:
    print(f"\n── {name} {'─' * (48 - len(name))}")


# ─── ASG ──────────────────────────────────────────────────────────────────────
_section("Auto-Scaling Group")
try:
    asg = boto3.client("autoscaling", region_name=AWS_REGION)
    asg.delete_auto_scaling_group(AutoScalingGroupName=ASG_NAME, ForceDelete=True)
    print(f"  Deleted ASG: {ASG_NAME}")
except ClientError as e:
    print(f"  ASG skip: {e.response['Error']['Code']}")

# ─── Lambda ───────────────────────────────────────────────────────────────────
_section("Lambda")
try:
    lmb = boto3.client("lambda", region_name=AWS_REGION)
    lmb.delete_function(FunctionName="cloudpulse-speed-processor")
    print("  Deleted Lambda: cloudpulse-speed-processor")
except ClientError as e:
    print(f"  Lambda skip: {e.response['Error']['Code']}")

# ─── Kinesis ──────────────────────────────────────────────────────────────────
_section("Kinesis")
try:
    kinesis = boto3.client("kinesis", region_name=AWS_REGION)
    kinesis.delete_stream(StreamName=KINESIS_STREAM_NAME, EnforceConsumerDeletion=True)
    print(f"  Deleted Kinesis stream: {KINESIS_STREAM_NAME}")
except ClientError as e:
    print(f"  Kinesis skip: {e.response['Error']['Code']}")

# ─── DynamoDB ─────────────────────────────────────────────────────────────────
_section("DynamoDB")
try:
    ddb = boto3.client("dynamodb", region_name=AWS_REGION)
    ddb.delete_table(TableName=DYNAMODB_TABLE)
    print(f"  Deleted DynamoDB table: {DYNAMODB_TABLE}")
except ClientError as e:
    print(f"  DynamoDB skip: {e.response['Error']['Code']}")

# ─── S3 ───────────────────────────────────────────────────────────────────────
_section("S3 (empty + delete)")
try:
    s3_res = boto3.resource("s3", region_name=AWS_REGION)
    bucket = s3_res.Bucket(S3_BUCKET)
    # Delete all object versions
    bucket.object_versions.delete()
    bucket.objects.delete()
    bucket.delete()
    print(f"  Deleted S3 bucket: {S3_BUCKET}")
except ClientError as e:
    print(f"  S3 skip: {e.response['Error']['Code']}")

# ─── CloudWatch alarms ────────────────────────────────────────────────────────
_section("CloudWatch Alarms")
cw = boto3.client("cloudwatch", region_name=AWS_REGION)
for alarm in ("cloudpulse-cpu-high", "cloudpulse-cpu-low"):
    try:
        cw.delete_alarms(AlarmNames=[alarm])
        print(f"  Deleted alarm: {alarm}")
    except Exception:
        pass

print("\nTeardown complete.")
