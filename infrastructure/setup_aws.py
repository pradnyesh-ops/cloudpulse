"""
CloudPulse — AWS Infrastructure Setup
Creates all required AWS resources in the correct order.
Idempotent: re-running is safe and skips already-created resources.

Usage:
    python infrastructure/setup_aws.py           # create everything
    python infrastructure/setup_aws.py --dry-run # print what would be created
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import (
    AWS_REGION, ATHENA_DATABASE, ATHENA_OUTPUT_LOCATION, ATHENA_WORKGROUP,
    DYNAMODB_TABLE, DYNAMODB_TTL,
    KINESIS_SHARD_COUNT, KINESIS_STREAM_NAME,
    SQS_QUEUE_NAME, STREAM_BACKEND,
    S3_BUCKET,
    EMR_CLUSTER_NAME,
)

DRY = False


def _warn(msg: str) -> None:
    print(f"  !  {msg}")


def _info(msg: str) -> None:
    print(f"  ✓  {msg}")


def _skip(msg: str) -> None:
    print(f"  –  {msg} (already exists, skipping)")


def _dry(msg: str) -> None:
    print(f"  [DRY] Would create: {msg}")


# ─── S3 ───────────────────────────────────────────────────────────────────────
def setup_s3() -> None:
    print("\n── S3 ────────────────────────────────────────────")
    s3 = boto3.client("s3", region_name=AWS_REGION)

    if DRY:
        _dry(f"S3 bucket: {S3_BUCKET}")
        return

    try:
        if AWS_REGION == "us-east-1":
            s3.create_bucket(Bucket=S3_BUCKET)
        else:
            s3.create_bucket(
                Bucket=S3_BUCKET,
                CreateBucketConfiguration={"LocationConstraint": AWS_REGION},
            )
        # Block public access
        s3.put_public_access_block(
            Bucket=S3_BUCKET,
            PublicAccessBlockConfiguration={
                "BlockPublicAcls":       True,
                "IgnorePublicAcls":      True,
                "BlockPublicPolicy":     True,
                "RestrictPublicBuckets": True,
            },
        )
        # Lifecycle: expire raw data after 30 days
        s3.put_bucket_lifecycle_configuration(
            Bucket=S3_BUCKET,
            LifecycleConfiguration={
                "Rules": [{
                    "ID": "expire-raw-30d",
                    "Status": "Enabled",
                    "Filter": {"Prefix": "raw/"},
                    "Expiration": {"Days": 30},
                }]
            },
        )
        # Server-side encryption (SSE-S3)
        s3.put_bucket_encryption(
            Bucket=S3_BUCKET,
            ServerSideEncryptionConfiguration={
                "Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]
            },
        )
        _info(f"Created S3 bucket: {S3_BUCKET}")
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            _skip(f"S3 bucket {S3_BUCKET}")
        else:
            raise

    # Create prefix placeholders
    for prefix in ("raw/", "batch-results/", "speed-results/", "merged-results/",
                    "scripts/", "emr-logs/", "athena-results/"):
        s3.put_object(Bucket=S3_BUCKET, Key=prefix)
    _info("Created S3 prefixes")


# ─── Kinesis ──────────────────────────────────────────────────────────────────
def setup_kinesis() -> bool:
    print("\n── Kinesis ───────────────────────────────────────")
    kinesis = boto3.client("kinesis", region_name=AWS_REGION)

    if DRY:
        _dry(f"Kinesis stream: {KINESIS_STREAM_NAME}  shards={KINESIS_SHARD_COUNT}")
        return True

    def _is_subscription_error(exc: ClientError) -> bool:
        return exc.response.get("Error", {}).get("Code") == "SubscriptionRequiredException"

    try:
        kinesis.describe_stream_summary(StreamName=KINESIS_STREAM_NAME)
        _skip(f"Kinesis stream {KINESIS_STREAM_NAME}")
        return True
    except kinesis.exceptions.ResourceNotFoundException:
        try:
            kinesis.create_stream(StreamName=KINESIS_STREAM_NAME, ShardCount=KINESIS_SHARD_COUNT)
            # Enable server-side encryption
            kinesis.start_stream_encryption(
                StreamName=KINESIS_STREAM_NAME,
                EncryptionType="KMS",
                KeyId="alias/aws/kinesis",
            )
            # Wait for ACTIVE
            waiter = kinesis.get_waiter("stream_exists")
            waiter.wait(StreamName=KINESIS_STREAM_NAME)
            _info(f"Created Kinesis stream: {KINESIS_STREAM_NAME}  shards={KINESIS_SHARD_COUNT}")

            # Enhanced fan-out (1 per consumer)
            kinesis.register_stream_consumer(
                StreamARN=kinesis.describe_stream_summary(StreamName=KINESIS_STREAM_NAME)
                          ["StreamDescriptionSummary"]["StreamARN"],
                ConsumerName="cloudpulse-speed-layer",
            )
            _info("Registered enhanced fan-out consumer")
            return True
        except ClientError as exc:
            if _is_subscription_error(exc):
                _warn("Kinesis is not enabled for this account (SubscriptionRequiredException).")
                _warn("Proceeding without Kinesis. Lambda trigger will be skipped.")
                return False
            raise
    except ClientError as exc:
        if _is_subscription_error(exc):
            _warn("Kinesis is not enabled for this account (SubscriptionRequiredException).")
            _warn("Proceeding without Kinesis. Lambda trigger will be skipped.")
            return False
        raise


# ─── SQS (fallback stream) ───────────────────────────────────────────────────
def setup_sqs() -> tuple[str | None, str | None]:
    print("\n── SQS (Fallback) ───────────────────────────────")
    sqs = boto3.client("sqs", region_name=AWS_REGION)

    if DRY:
        _dry(f"SQS queue: {SQS_QUEUE_NAME}")
        return None, None

    attrs = {
        "VisibilityTimeout": "120",
        "MessageRetentionPeriod": "345600",
        "ReceiveMessageWaitTimeSeconds": "20",
    }

    sqs.create_queue(QueueName=SQS_QUEUE_NAME, Attributes=attrs)
    queue_url = sqs.get_queue_url(QueueName=SQS_QUEUE_NAME)["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]
    _info(f"SQS queue ready: {SQS_QUEUE_NAME}")
    return queue_url, queue_arn


# ─── DynamoDB ─────────────────────────────────────────────────────────────────
def setup_dynamodb() -> None:
    print("\n── DynamoDB ──────────────────────────────────────")
    ddb = boto3.client("dynamodb", region_name=AWS_REGION)

    if DRY:
        _dry(f"DynamoDB table: {DYNAMODB_TABLE}")
        return

    try:
        ddb.describe_table(TableName=DYNAMODB_TABLE)
        _skip(f"DynamoDB table {DYNAMODB_TABLE}")
    except ddb.exceptions.ResourceNotFoundException:
        ddb.create_table(
            TableName=DYNAMODB_TABLE,
            KeySchema=[{"AttributeName": "region_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "region_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
            SSESpecification={"Enabled": True},
        )
        waiter = ddb.get_waiter("table_exists")
        waiter.wait(TableName=DYNAMODB_TABLE)
        # Enable TTL
        ddb.update_time_to_live(
            TableName=DYNAMODB_TABLE,
            TimeToLiveSpecification={"Enabled": True, "AttributeName": "ttl"},
        )
        _info(f"Created DynamoDB table: {DYNAMODB_TABLE}  TTL=ttl")


# ─── Lambda ───────────────────────────────────────────────────────────────────
def setup_lambda(
    stream_backend: str = "kinesis",
    kinesis_enabled: bool = True,
    sqs_queue_arn: str | None = None,
) -> None:
    print("\n── Lambda ────────────────────────────────────────")
    lmb = boto3.client("lambda", region_name=AWS_REGION)
    iam = boto3.client("iam",    region_name=AWS_REGION)

    if DRY:
        _dry("Lambda function: cloudpulse-speed-processor")
        return

    # Zip the lambda function
    import io, zipfile
    buf = io.BytesIO()
    lambda_src = Path(__file__).parent.parent / "speed_layer" / "lambda_function.py"
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(lambda_src, "lambda_function.py")
    buf.seek(0)

    # Get execution role ARN
    try:
        role_arn = iam.get_role(RoleName="cloudpulse-lambda-role")["Role"]["Arn"]
    except iam.exceptions.NoSuchEntityException:
        _info("Lambda role not found — create 'cloudpulse-lambda-role' with Kinesis+DynamoDB+S3+CloudWatch permissions")
        return

    env_vars = {
        "S3_BUCKET":              S3_BUCKET,
        "DYNAMODB_TABLE":         DYNAMODB_TABLE,
        "APP_AWS_REGION":         AWS_REGION,
        "SPEED_WINDOW_SECONDS":   "300",
        "STREAM_BACKEND":         stream_backend,
    }

    fn_name = "cloudpulse-speed-processor"
    created = False

    def _is_conflict(exc: ClientError) -> bool:
        return exc.response.get("Error", {}).get("Code") == "ResourceConflictException"

    def _retry_update() -> None:
        attempts = 8
        for i in range(attempts):
            try:
                lmb.update_function_code(FunctionName=fn_name, ZipFile=buf.getvalue())
                lmb.get_waiter("function_updated").wait(FunctionName=fn_name)
                lmb.update_function_configuration(FunctionName=fn_name, Environment={"Variables": env_vars})
                lmb.get_waiter("function_updated").wait(FunctionName=fn_name)
                return
            except ClientError as exc:
                if not _is_conflict(exc) or i == attempts - 1:
                    raise
                time.sleep(5)

    try:
        _retry_update()
        _info(f"Updated Lambda function: {fn_name}")
    except lmb.exceptions.ResourceNotFoundException:
        lmb.create_function(
            FunctionName=fn_name,
            Runtime="python3.12",
            Role=role_arn,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": buf.getvalue()},
            Timeout=60,
            MemorySize=256,
            Environment={"Variables": env_vars},
            Description="CloudPulse speed-layer processor",
        )
        _info(f"Created Lambda function: {fn_name}")
        created = True

    # Wait briefly for configuration propagation before mapping changes.
    if created:
        time.sleep(5)

    if stream_backend == "kinesis" and kinesis_enabled:
        stream_arn = boto3.client("kinesis", region_name=AWS_REGION) \
            .describe_stream_summary(StreamName=KINESIS_STREAM_NAME) \
            ["StreamDescriptionSummary"]["StreamARN"]
        mappings = lmb.list_event_source_mappings(FunctionName=fn_name)["EventSourceMappings"]
        has_mapping = any(m.get("EventSourceArn") == stream_arn for m in mappings)
        if has_mapping:
            _skip("Lambda Kinesis trigger")
        else:
            lmb.create_event_source_mapping(
                EventSourceArn=stream_arn,
                FunctionName=fn_name,
                StartingPosition="LATEST",
                BatchSize=100,
                BisectBatchOnFunctionError=True,
            )
            _info("Added Kinesis trigger to Lambda (batch=100)")
    elif stream_backend == "sqs" and sqs_queue_arn:
        mappings = lmb.list_event_source_mappings(FunctionName=fn_name)["EventSourceMappings"]
        has_mapping = any(m.get("EventSourceArn") == sqs_queue_arn for m in mappings)
        if has_mapping:
            _skip("Lambda SQS trigger")
        else:
            lmb.create_event_source_mapping(
                EventSourceArn=sqs_queue_arn,
                FunctionName=fn_name,
                BatchSize=10,
                MaximumBatchingWindowInSeconds=5,
                Enabled=True,
            )
            _info("Added SQS trigger to Lambda (batch=10)")
    else:
        _warn("Skipped Lambda trigger setup (no supported stream backend available).")


# ─── Athena ───────────────────────────────────────────────────────────────────
def setup_athena() -> None:
    print("\n── Athena ────────────────────────────────────────")
    athena = boto3.client("athena", region_name=AWS_REGION)

    if DRY:
        _dry(f"Athena database: {ATHENA_DATABASE}")
        return

    def _run(sql: str) -> None:
        resp = athena.start_query_execution(
            QueryString=sql,
            ResultConfiguration={"OutputLocation": ATHENA_OUTPUT_LOCATION},
            WorkGroup=ATHENA_WORKGROUP,
        )
        qid = resp["QueryExecutionId"]
        for _ in range(30):
            state = athena.get_query_execution(QueryExecutionId=qid) \
                          ["QueryExecution"]["Status"]["State"]
            if state == "SUCCEEDED":
                return
            if state in ("FAILED", "CANCELLED"):
                raise RuntimeError(f"Athena setup query failed: {qid}")
            time.sleep(2)

    _run(f"CREATE DATABASE IF NOT EXISTS {ATHENA_DATABASE}")
    _info(f"Athena database ready: {ATHENA_DATABASE}")

    from serving_layer.athena_queries import CREATE_RAW_TABLE_SQL, CREATE_BATCH_VIEW_SQL
    _run(CREATE_RAW_TABLE_SQL)
    _run(CREATE_BATCH_VIEW_SQL)
    _info("Athena table + view created")


# ─── Auto-Scaling (EC2 ASG) ───────────────────────────────────────────────────
def setup_auto_scaling() -> None:
    """
    Defines the ASG and scaling policies — see auto_scaling.py for full details.
    This function prints instructions because ASG requires an AMI ID and launch template
    that are account-specific.
    """
    print("\n── Auto-Scaling ──────────────────────────────────")
    if DRY:
        _dry("EC2 Auto Scaling Group: cloudpulse-asg")
        return
    print("  ℹ  Run `python infrastructure/auto_scaling.py` to create the ASG")
    print("     (requires a valid EC2 Launch Template with your AMI ID).")


# ─── Entry point ──────────────────────────────────────────────────────────────
def main(dry: bool = False, skip_kinesis: bool = False, stream_backend: str = STREAM_BACKEND) -> None:
    global DRY
    DRY = dry
    print(f"\n{'[DRY RUN] ' if dry else ''}CloudPulse AWS Setup — region={AWS_REGION}")
    print("=" * 54)

    setup_s3()

    selected_backend = stream_backend
    kinesis_enabled = False
    sqs_queue_url: str | None = None
    sqs_queue_arn: str | None = None

    if selected_backend not in ("auto", "kinesis", "sqs"):
        raise ValueError("STREAM_BACKEND must be one of: auto, kinesis, sqs")

    if selected_backend in ("auto", "kinesis"):
        kinesis_enabled = False if skip_kinesis else setup_kinesis()
        if selected_backend == "kinesis" and not kinesis_enabled:
            raise RuntimeError("Kinesis backend requested but Kinesis is unavailable for this account.")

    if selected_backend == "auto":
        if kinesis_enabled:
            selected_backend = "kinesis"
        else:
            selected_backend = "sqs"
            sqs_queue_url, sqs_queue_arn = setup_sqs()
            if sqs_queue_url:
                _info(f"Using streaming backend: {selected_backend}")
    elif selected_backend == "sqs":
        sqs_queue_url, sqs_queue_arn = setup_sqs()

    setup_dynamodb()
    setup_lambda(
        stream_backend=selected_backend,
        kinesis_enabled=kinesis_enabled,
        sqs_queue_arn=sqs_queue_arn,
    )
    setup_athena()
    setup_auto_scaling()

    print("\n" + "=" * 54)
    print("Setup complete. Next steps:")
    print("  1. python infrastructure/auto_scaling.py   — create EC2 ASG")
    print("  2. python ingestion/producer.py            — start data ingestion")
    print("  3. python dashboard/app.py                 — open dashboard")
    print("  4. python batch_layer/submit_batch.py --create --spark --wait")
    if selected_backend == "sqs" and sqs_queue_url:
        print(f"  5. SQS fallback queue URL: {sqs_queue_url}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-kinesis", action="store_true")
    parser.add_argument("--stream-backend", choices=["auto", "kinesis", "sqs"], default=STREAM_BACKEND)
    args = parser.parse_args()
    main(dry=args.dry_run, skip_kinesis=args.skip_kinesis, stream_backend=args.stream_backend)
