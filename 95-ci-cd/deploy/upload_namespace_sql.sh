#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────
# upload_namespace_sql.sh — Upload SQL namespace files to Kestra
# via the REST API (multipart form-data).
#
# kestractl nsfiles upload is broken ("no value given for required
# property deleted"), so we use curl directly against the Kestra
# API — the same approach used in rollback_prod.sh.
#
# Required env vars:
#   KESTRA_SERVER    — e.g. http://localhost:8082
#   KESTRA_NAMESPACE — e.g. projet713
#   KESTRA_USER      — Kestra admin username
#   KESTRA_PASS      — Kestra admin password
#   SQL_DIR          — repo-relative path to SQL root (e.g. 140-sql)
# ─────────────────────────────────────────────────────────────────
set -euo pipefail

# ── Validate prerequisites ──────────────────────────────────────
for var in KESTRA_SERVER KESTRA_NAMESPACE KESTRA_USER KESTRA_PASS SQL_DIR; do
  if [ -z "${!var:-}" ]; then
    echo "❌ Missing required env var: $var"
    exit 1
  fi
done

command -v curl >/dev/null || { echo "❌ curl not found"; exit 1; }

# ── Upload each SQL file ────────────────────────────────────────
SQL_COUNT=0
FAILED=0

for filepath in "$SQL_DIR"/queries/*.sql; do
  [ -f "$filepath" ] || continue

  # Strip the SQL_DIR prefix to get the Kestra namespace-relative path
  # e.g. 140-sql/queries/linky_gold.sql → queries/linky_gold.sql
  REL_PATH="${filepath#"$SQL_DIR"/}"

  HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
    -X POST "${KESTRA_SERVER}/api/v1/main/namespaces/${KESTRA_NAMESPACE}/files?path=${REL_PATH}" \
    -u "${KESTRA_USER}:${KESTRA_PASS}" \
    -H "Content-Type: multipart/form-data" \
    -F "fileContent=@${filepath}")

  if [ "$HTTP_CODE" -ge 200 ] && [ "$HTTP_CODE" -lt 300 ]; then
    echo "  ✓ Uploaded $REL_PATH (HTTP $HTTP_CODE)"
    SQL_COUNT=$((SQL_COUNT + 1))
  else
    echo "  ❌ Failed to upload $REL_PATH (HTTP $HTTP_CODE)"
    FAILED=$((FAILED + 1))
  fi
done

echo ""
if [ "$FAILED" -gt 0 ]; then
  echo "❌ $FAILED SQL file(s) failed to upload."
  exit 1
fi

if [ "$SQL_COUNT" -eq 0 ]; then
  echo "⚠️  No SQL files found in $SQL_DIR/queries/"
else
  echo "✅ $SQL_COUNT SQL namespace file(s) uploaded successfully."
fi
