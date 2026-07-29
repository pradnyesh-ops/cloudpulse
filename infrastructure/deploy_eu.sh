#!/usr/bin/env bash
set -euo pipefail

# CloudPulse Europe deployment helper.
# Usage:
#   ./infrastructure/deploy_eu.sh <github_repo_url> [branch]
# Example:
#   ./infrastructure/deploy_eu.sh https://github.com/your-user/cloudpulse.git main

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <github_repo_url> [branch]"
  exit 1
fi

REPO_URL="$1"
BRANCH="${2:-main}"

export AWS_REGION="${AWS_REGION:-eu-west-1}"
export DEMO_MODE=false

echo "[Deploy] Using AWS region: ${AWS_REGION}"
echo "[Deploy] Verifying AWS caller identity..."
aws sts get-caller-identity >/dev/null

echo "[Deploy] Creating core AWS resources (S3/Kinesis/DynamoDB/Lambda/Athena)..."
python infrastructure/setup_aws.py

echo "[Deploy] Creating/Updating Auto Scaling bootstrap from GitHub repo..."
python infrastructure/auto_scaling.py --create --repo-url "${REPO_URL}" --repo-branch "${BRANCH}"

echo "[Deploy] Deployment bootstrap complete."
echo "[Deploy] Next: start producer + dashboard"
echo "  DEMO_MODE=false python ingestion/producer.py"
echo "  DEMO_MODE=false python dashboard/app.py"
