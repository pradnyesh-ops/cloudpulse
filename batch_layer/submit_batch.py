"""
CloudPulse — EMR Job Submitter
Uploads batch_job.py to S3 then submits a Spark step to the CloudPulse EMR cluster.
Also supports submitting the Hadoop Streaming MapReduce step.

Usage:
    python submit_batch.py --spark      # submit PySpark batch job
    python submit_batch.py --hadoop     # submit Hadoop Streaming MapReduce
    python submit_batch.py --create     # create a new EMR cluster first
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import (
    AWS_REGION, EMR_CLUSTER_NAME, EMR_LOG_URI,
    EMR_MASTER_INSTANCE_TYPE, EMR_MIN_WORKERS, EMR_MAX_WORKERS,
    EMR_RELEASE_LABEL, EMR_WORKER_INSTANCE_TYPE,
    S3_BUCKET, S3_BATCH_PREFIX, S3_RAW_PREFIX, S3_SCRIPTS_PREFIX,
)

emr    = boto3.client("emr",    region_name=AWS_REGION)
s3     = boto3.client("s3",     region_name=AWS_REGION)
iam    = boto3.client("iam",    region_name=AWS_REGION)


# ─── Helpers ──────────────────────────────────────────────────────────────────
def upload_script(local_path: str, s3_key: str) -> str:
    """Upload a local script to S3 and return the s3:// URI."""
    s3.upload_file(local_path, S3_BUCKET, s3_key)
    uri = f"s3://{S3_BUCKET}/{s3_key}"
    print(f"[Submit] Uploaded {local_path} → {uri}")
    return uri


def find_cluster_id(name: str) -> str | None:
    """Return the RUNNING/WAITING cluster id for the given name."""
    paginator = emr.get_paginator("list_clusters")
    for page in paginator.paginate(ClusterStates=["RUNNING", "WAITING"]):
        for c in page["Clusters"]:
            if c["Name"] == name:
                return c["Id"]
    return None


def wait_step(cluster_id: str, step_id: str, poll_s: int = 15) -> str:
    """Block until the EMR step reaches a terminal state. Returns final state."""
    while True:
        resp  = emr.describe_step(ClusterId=cluster_id, StepId=step_id)
        state = resp["Step"]["Status"]["State"]
        print(f"[Submit] Step {step_id} — {state}")
        if state in ("COMPLETED", "FAILED", "CANCELLED", "INTERRUPTED"):
            return state
        time.sleep(poll_s)


# ─── Cluster creation ─────────────────────────────────────────────────────────
def create_emr_cluster() -> str:
    """Create a new EMR cluster with managed scaling and return its id."""
    print(f"[Submit] Creating EMR cluster '{EMR_CLUSTER_NAME}' …")
    resp = emr.run_job_flow(
        Name=EMR_CLUSTER_NAME,
        ReleaseLabel=EMR_RELEASE_LABEL,
        Applications=[{"Name": "Spark"}, {"Name": "Hadoop"}, {"Name": "Hive"}],
        Instances={
            "InstanceGroups": [
                {
                    "Name": "Master",
                    "Market": "ON_DEMAND",
                    "InstanceRole": "MASTER",
                    "InstanceType": EMR_MASTER_INSTANCE_TYPE,
                    "InstanceCount": 1,
                },
                {
                    "Name": "Core",
                    "Market": "ON_DEMAND",
                    "InstanceRole": "CORE",
                    "InstanceType": EMR_WORKER_INSTANCE_TYPE,
                    "InstanceCount": EMR_MIN_WORKERS,
                },
            ],
            "KeepJobFlowAliveWhenNoSteps": True,
            "TerminationProtected": False,
        },
        ManagedScalingPolicy={
            "ComputeLimits": {
                "UnitType": "Instances",
                "MinimumCapacityUnits": EMR_MIN_WORKERS,
                "MaximumCapacityUnits": EMR_MAX_WORKERS,
                "MaximumOnDemandCapacityUnits": EMR_MAX_WORKERS,
            }
        },
        LogUri=EMR_LOG_URI,
        ServiceRole="EMR_DefaultRole",
        JobFlowRole="EMR_EC2_DefaultRole",
        VisibleToAllUsers=True,
        Tags=[
            {"Key": "Project", "Value": "CloudPulse"},
            {"Key": "Environment", "Value": "dev"},
        ],
    )
    cluster_id = resp["JobFlowId"]
    print(f"[Submit] Cluster created: {cluster_id}")
    return cluster_id


# ─── Spark step ───────────────────────────────────────────────────────────────
def submit_spark_step(cluster_id: str) -> str:
    script_local = str(Path(__file__).parent / "batch_job.py")
    script_s3    = upload_script(script_local, f"{S3_SCRIPTS_PREFIX}batch_job.py")

    input_s3  = f"s3://{S3_BUCKET}/{S3_RAW_PREFIX}"
    output_s3 = f"s3://{S3_BUCKET}/{S3_BATCH_PREFIX}"

    resp = emr.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[{
            "Name": "CloudPulse-SparkBatch",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode", "cluster",
                    "--conf", "spark.yarn.submit.waitAppCompletion=true",
                    script_s3,
                    "--input",  input_s3,
                    "--output", output_s3,
                ],
            },
        }],
    )
    step_id = resp["StepIds"][0]
    print(f"[Submit] Spark step submitted: {step_id}")
    return step_id


# ─── Hadoop Streaming step ────────────────────────────────────────────────────
def submit_hadoop_step(cluster_id: str) -> str:
    mapper_local  = str(Path(__file__).parent / "mapper.py")
    reducer_local = str(Path(__file__).parent / "reducer.py")
    mapper_s3  = upload_script(mapper_local,  f"{S3_SCRIPTS_PREFIX}mapper.py")
    reducer_s3 = upload_script(reducer_local, f"{S3_SCRIPTS_PREFIX}reducer.py")

    input_s3  = f"s3://{S3_BUCKET}/{S3_RAW_PREFIX}"
    output_s3 = f"s3://{S3_BUCKET}/hadoop-results/"

    resp = emr.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[{
            "Name": "CloudPulse-HadoopStreaming",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "/usr/lib/hadoop/hadoop-streaming.jar",
                "Args": [
                    "-files",   f"{mapper_s3},{reducer_s3}",
                    "-mapper",  "mapper.py",
                    "-reducer", "reducer.py",
                    "-input",   input_s3,
                    "-output",  output_s3,
                ],
            },
        }],
    )
    step_id = resp["StepIds"][0]
    print(f"[Submit] Hadoop Streaming step submitted: {step_id}")
    return step_id


# ─── CLI ──────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="CloudPulse EMR job submitter")
    parser.add_argument("--spark",  action="store_true", help="Submit PySpark batch job")
    parser.add_argument("--hadoop", action="store_true", help="Submit Hadoop Streaming MapReduce")
    parser.add_argument("--create", action="store_true", help="Create new EMR cluster")
    parser.add_argument("--wait",   action="store_true", help="Wait for step to finish")
    args = parser.parse_args()

    if args.create:
        cluster_id = create_emr_cluster()
    else:
        cluster_id = find_cluster_id(EMR_CLUSTER_NAME)
        if not cluster_id:
            print(f"[Submit] No running cluster named '{EMR_CLUSTER_NAME}'. Use --create first.")
            sys.exit(1)
        print(f"[Submit] Using existing cluster: {cluster_id}")

    step_id = None
    if args.spark:
        step_id = submit_spark_step(cluster_id)
    if args.hadoop:
        step_id = submit_hadoop_step(cluster_id)

    if not step_id:
        parser.print_help()
        return

    if args.wait and step_id:
        final = wait_step(cluster_id, step_id)
        print(f"[Submit] Step final state: {final}")
        sys.exit(0 if final == "COMPLETED" else 1)


if __name__ == "__main__":
    main()
