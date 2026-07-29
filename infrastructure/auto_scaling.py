"""
CloudPulse — EC2 Auto-Scaling Group Configuration
Creates an ASG for the dashboard + speed-layer EC2 instances with
target-tracking and step scaling policies.

Scaling triggers:
  • Scale OUT : CPU > 70 % for 2 minutes → add 1 instance  (cooldown 300 s)
  • Scale IN  : CPU < 30 % for 10 minutes → remove 1 instance (cooldown 300 s)
  • Min: 1, Desired: 2, Max: 5 instances

Usage:
    python infrastructure/auto_scaling.py --create    # create ASG + policies
    python infrastructure/auto_scaling.py --status    # show current ASG state
    python infrastructure/auto_scaling.py --delete    # delete ASG (confirm first)
"""

from __future__ import annotations

import argparse
import json
import sys
import os
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import (
    ASG_COOLDOWN, ASG_DESIRED_CAPACITY, ASG_MAX_SIZE,
    ASG_MIN_SIZE, ASG_NAME, ASG_SCALE_IN_CPU, ASG_SCALE_OUT_CPU,
    AWS_REGION, CLOUDWATCH_NAMESPACE,
)

asg = boto3.client("autoscaling",  region_name=AWS_REGION)
ec2 = boto3.client("ec2",          region_name=AWS_REGION)
cw  = boto3.client("cloudwatch",   region_name=AWS_REGION)
# ─── Launch template ──────────────────────────────────────────────────────────
def _build_userdata_b64(repo_url: str, repo_branch: str) -> str:
    import base64

    script = f"""#!/bin/bash
set -e
yum update -y
yum install -y python3 python3-pip git

# Clone and set up CloudPulse
git clone --branch {repo_branch} {repo_url} /opt/cloudpulse
cd /opt/cloudpulse
pip3 install -r requirements.txt

# Write environment
cat > /opt/cloudpulse/.env << 'ENV'
DEMO_MODE=false
AWS_REGION={AWS_REGION}
KINESIS_STREAM_NAME=cloudpulse-stream
S3_BUCKET=cloudpulse-data-bucket
ENV

# Start speed-layer processor as a service
cat > /etc/systemd/system/cloudpulse-speed.service << 'SVC'
[Unit]
Description=CloudPulse Speed Layer
After=network.target
[Service]
WorkingDirectory=/opt/cloudpulse
ExecStart=/usr/bin/python3 speed_layer/stream_processor.py
Restart=always
RestartSec=10
[Install]
WantedBy=multi-user.target
SVC

systemctl daemon-reload
systemctl enable --now cloudpulse-speed
"""
    return base64.b64encode(script.encode()).decode()


def get_or_create_launch_template(repo_url: str, repo_branch: str) -> str:
    """Return existing launch template ID or create a new one."""
    lt_name = "cloudpulse-launch-template"

    userdata_b64 = _build_userdata_b64(repo_url=repo_url, repo_branch=repo_branch)

    try:
        resp = ec2.describe_launch_templates(
            Filters=[{"Name": "launch-template-name", "Values": [lt_name]}]
        )
        if resp["LaunchTemplates"]:
            lt_id = resp["LaunchTemplates"][0]["LaunchTemplateId"]
            ec2.create_launch_template_version(
                LaunchTemplateId=lt_id,
                SourceVersion="$Latest",
                VersionDescription=f"CloudPulse bootstrap update ({repo_branch})",
                LaunchTemplateData={"UserData": userdata_b64},
            )
            print(f"  Using existing launch template: {lt_id}")
            print("  Created new launch template version with current repo bootstrap settings")
            return lt_id
    except Exception:
        pass

    # Get latest Amazon Linux 2023 AMI
    ami_resp = ec2.describe_images(
        Owners=["amazon"],
        Filters=[
            {"Name": "name",               "Values": ["al2023-ami-*-x86_64"]},
            {"Name": "state",              "Values": ["available"]},
            {"Name": "root-device-type",   "Values": ["ebs"]},
            {"Name": "virtualization-type","Values": ["hvm"]},
        ],
    )
    amis   = sorted(ami_resp["Images"], key=lambda x: x["CreationDate"], reverse=True)
    ami_id = amis[0]["ImageId"] if amis else "ami-0c02fb55956c7d316"  # fallback us-east-1
    print(f"  Latest AL2023 AMI: {ami_id}")

    resp = ec2.create_launch_template(
        LaunchTemplateName=lt_name,
        VersionDescription="CloudPulse initial",
        LaunchTemplateData={
            "ImageId":      ami_id,
            "InstanceType": "t3.medium",
            "UserData":     userdata_b64,
            "IamInstanceProfile": {"Name": "cloudpulse-ec2-profile"},
            "Monitoring":   {"Enabled": True},
            "TagSpecifications": [{
                "ResourceType": "instance",
                "Tags": [
                    {"Key": "Project",     "Value": "CloudPulse"},
                    {"Key": "Role",        "Value": "speed-layer"},
                ],
            }],
        },
    )
    lt_id = resp["LaunchTemplate"]["LaunchTemplateId"]
    print(f"  Created launch template: {lt_id}")
    return lt_id


# ─── ASG ──────────────────────────────────────────────────────────────────────
def create_asg(lt_id: str) -> None:
    # Get first two availability zones
    azs = [az["ZoneName"] for az in ec2.describe_availability_zones()["AvailabilityZones"][:2]]
    print(f"  Availability zones: {azs}")

    try:
        asg.create_auto_scaling_group(
            AutoScalingGroupName=ASG_NAME,
            LaunchTemplate={"LaunchTemplateId": lt_id, "Version": "$Latest"},
            MinSize=ASG_MIN_SIZE,
            MaxSize=ASG_MAX_SIZE,
            DesiredCapacity=ASG_DESIRED_CAPACITY,
            AvailabilityZones=azs,
            DefaultCooldown=ASG_COOLDOWN,
            HealthCheckType="EC2",
            HealthCheckGracePeriod=120,
            Tags=[
                {"Key": "Project", "Value": "CloudPulse",    "PropagateAtLaunch": True},
                {"Key": "Name",    "Value": "cloudpulse-ec2", "PropagateAtLaunch": True},
            ],
        )
        print(f"  Created ASG: {ASG_NAME}  min={ASG_MIN_SIZE} desired={ASG_DESIRED_CAPACITY} max={ASG_MAX_SIZE}")
    except asg.exceptions.AlreadyExistsFault:
        print(f"  ASG {ASG_NAME} already exists — skipping.")


# ─── Target-tracking scale-out policy ─────────────────────────────────────────
def create_scaling_policies() -> None:
    # Target-tracking: maintain CPU at 60 %
    asg.put_scaling_policy(
        AutoScalingGroupName=ASG_NAME,
        PolicyName="cloudpulse-target-tracking-cpu",
        PolicyType="TargetTrackingScaling",
        TargetTrackingConfiguration={
            "PredefinedMetricSpecification": {"PredefinedMetricType": "ASGAverageCPUUtilization"},
            "TargetValue":        60.0,
            "DisableScaleIn":     False,
            "ScaleOutCooldown":   ASG_COOLDOWN,
            "ScaleInCooldown":    ASG_COOLDOWN * 2,
        },
    )
    print(f"  Created target-tracking policy (CPU target=60 %)")

    # Step scaling scale-OUT (CPU > 70 % → +1 instance)
    cw.put_metric_alarm(
        AlarmName="cloudpulse-cpu-high",
        ComparisonOperator="GreaterThanThreshold",
        EvaluationPeriods=2,
        MetricName="CPUUtilization",
        Namespace="AWS/EC2",
        Period=60,
        Statistic="Average",
        Threshold=ASG_SCALE_OUT_CPU,
        ActionsEnabled=True,
        Dimensions=[{"Name": "AutoScalingGroupName", "Value": ASG_NAME}],
        AlarmDescription="CloudPulse: CPU > 70 % — scale out",
        TreatMissingData="notBreaching",
    )

    # Step scaling scale-IN  (CPU < 30 % → -1 instance)
    cw.put_metric_alarm(
        AlarmName="cloudpulse-cpu-low",
        ComparisonOperator="LessThanThreshold",
        EvaluationPeriods=10,   # 10 minutes of low CPU
        MetricName="CPUUtilization",
        Namespace="AWS/EC2",
        Period=60,
        Statistic="Average",
        Threshold=ASG_SCALE_IN_CPU,
        ActionsEnabled=True,
        Dimensions=[{"Name": "AutoScalingGroupName", "Value": ASG_NAME}],
        AlarmDescription="CloudPulse: CPU < 30 % — scale in",
        TreatMissingData="notBreaching",
    )
    print(f"  Created CloudWatch alarms: cpu-high ({ASG_SCALE_OUT_CPU}%) / cpu-low ({ASG_SCALE_IN_CPU}%)")


# ─── Status ───────────────────────────────────────────────────────────────────
def show_status() -> None:
    try:
        resp  = asg.describe_auto_scaling_groups(AutoScalingGroupNames=[ASG_NAME])
        group = resp["AutoScalingGroups"][0]
        instances = group.get("Instances", [])
        print(f"\nASG: {ASG_NAME}")
        print(f"  Min={group['MinSize']}  Desired={group['DesiredCapacity']}  Max={group['MaxSize']}")
        print(f"  Instances ({len(instances)}):")
        for i in instances:
            print(f"    {i['InstanceId']}  {i['AvailabilityZone']}  {i['LifecycleState']}  {i['HealthStatus']}")
    except (IndexError, KeyError):
        print(f"ASG '{ASG_NAME}' not found.")


# ─── CLI ──────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="CloudPulse ASG manager")
    parser.add_argument("--create", action="store_true", help="Create ASG + policies")
    parser.add_argument("--status", action="store_true", help="Show ASG status")
    parser.add_argument("--delete", action="store_true", help="Delete ASG (confirms first)")
    parser.add_argument(
        "--repo-url",
        default=os.environ.get("CLOUDPULSE_REPO_URL", ""),
        help="GitHub URL to clone on EC2 bootstrap (or set CLOUDPULSE_REPO_URL)",
    )
    parser.add_argument(
        "--repo-branch",
        default=os.environ.get("CLOUDPULSE_REPO_BRANCH", "main"),
        help="Git branch to clone on EC2 bootstrap",
    )
    args = parser.parse_args()

    if args.create:
        if not args.repo_url:
            print("Error: --repo-url is required for --create (or set CLOUDPULSE_REPO_URL).")
            sys.exit(2)
        print("\nCreating Auto-Scaling Group …")
        lt_id = get_or_create_launch_template(repo_url=args.repo_url, repo_branch=args.repo_branch)
        create_asg(lt_id)
        create_scaling_policies()
        print("\nAuto-Scaling setup complete.")
        print(f"  Trigger scale-out  : CPU > {ASG_SCALE_OUT_CPU}%  for 2 min")
        print(f"  Trigger scale-in   : CPU < {ASG_SCALE_IN_CPU}%  for 10 min")
        print(f"  Cooldown           : {ASG_COOLDOWN}s")

    elif args.status:
        show_status()

    elif args.delete:
        confirm = input(f"Delete ASG '{ASG_NAME}'? This will terminate all instances. [yes/no]: ")
        if confirm.lower() == "yes":
            asg.delete_auto_scaling_group(AutoScalingGroupName=ASG_NAME, ForceDelete=True)
            print(f"  Deleted ASG: {ASG_NAME}")
        else:
            print("Cancelled.")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
