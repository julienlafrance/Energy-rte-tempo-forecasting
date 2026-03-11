#!/usr/bin/env bash
set -euo pipefail

KESTRA_URL=${KESTRA_URL:-http://localhost:8082}

for flow in 150-flows/dev/*.yaml 150-flows/dev/*.yml; do
  echo "Deploying $flow"

  curl -sS -X POST "$KESTRA_URL/api/v1/main/flows" \
    -u "${KESTRA_ADMIN_USER}:${KESTRA_ADMIN_PASS}" \
    -H "Content-Type: application/x-yaml" \
    --data-binary @"$flow"
done

echo "Flows deployed successfully."