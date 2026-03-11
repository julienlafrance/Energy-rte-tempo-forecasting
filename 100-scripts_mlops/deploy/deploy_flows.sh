#!/usr/bin/env bash
set -euo pipefail

KESTRA_URL="${KESTRA_URL:-http://localhost:8082}"
FLOW_DIR="${FLOW_DIR:-150-flows/dev}"

echo "Deploying flows from $FLOW_DIR"
echo "Kestra URL: $KESTRA_URL"
echo

shopt -s nullglob

for flow in "$FLOW_DIR"/*.yaml "$FLOW_DIR"/*.yml; do
  echo "Validating $flow"

  curl -sSf -X POST "$KESTRA_URL/api/v1/main/flows/validate" \
    -u "${KESTRA_ADMIN_USER}:${KESTRA_ADMIN_PASS}" \
    -H "Content-Type: application/x-yaml" \
    --data-binary @"$flow" >/dev/null

  namespace=$(grep '^namespace:' "$flow" | awk '{print $2}')
  id=$(grep '^id:' "$flow" | awk '{print $2}')

  echo "Deploying flow $namespace/$id"

  curl -sSf -X PUT "$KESTRA_URL/api/v1/main/flows/$namespace/$id" \
    -u "${KESTRA_ADMIN_USER}:${KESTRA_ADMIN_PASS}" \
    -H "Content-Type: application/x-yaml" \
    --data-binary @"$flow" >/dev/null

  echo "✓ $namespace/$id deployed"
  echo
done

echo "All flows deployed successfully."