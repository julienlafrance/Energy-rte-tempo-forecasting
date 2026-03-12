#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────
# rollback_prod.sh — Robust rollback to HEAD~1 for all pipeline
# artifacts (flows, SQL namespace files, flow scripts).
#
# Uses `git ls-tree` + `git show` to enumerate and restore ALL
# files that existed at HEAD~1, including files that were DELETED
# between HEAD~1 and HEAD (which `git checkout HEAD~1 -- <dir>`
# would miss).
#
# Called by deploy.yml on failure. Non-destructive: only restores
# Git-tracked resources, never deletes remote resources.
#
# Required env vars:
#   KESTRA_SERVER, KESTRA_NAMESPACE, KESTRA_USER, KESTRA_PASS
#   FLOWS_DIR, SQL_DIR, SCRIPTS_DIR
# ─────────────────────────────────────────────────────────────────
set -euo pipefail

PREV="HEAD~1"

# ── Validate prerequisites ──────────────────────────────────────
for var in KESTRA_SERVER KESTRA_NAMESPACE KESTRA_USER KESTRA_PASS \
           FLOWS_DIR SQL_DIR SCRIPTS_DIR; do
  if [ -z "${!var:-}" ]; then
    echo "❌ Missing required env var: $var"
    exit 1
  fi
done

git rev-parse "$PREV" >/dev/null 2>&1 || {
  echo "⚠️  No previous commit available (first commit?). Skipping rollback."
  exit 0
}

echo "⚠️  Rolling back to $(git rev-parse --short "$PREV")"
echo ""

# ── 1. Restore and redeploy SQL namespace files ────────────────
echo "── Restoring SQL namespace files ──"
SQL_COUNT=0
for filepath in $(git ls-tree -r --name-only "$PREV" -- "$SQL_DIR/queries" 2>/dev/null | grep '\.sql$' || true); do
  mkdir -p "$(dirname "$filepath")"
  git show "${PREV}:${filepath}" > "$filepath"

  REL_PATH="${filepath#"$SQL_DIR"/}"  # e.g. queries/linky_gold.sql
  curl -s -X POST "${KESTRA_SERVER}/api/v1/main/namespaces/${KESTRA_NAMESPACE}/files?path=${REL_PATH}" \
    -u "${KESTRA_USER}:${KESTRA_PASS}" \
    -H "Content-Type: multipart/form-data" \
    -F "fileContent=@${filepath}" && echo "  ⏪ Rolled back $REL_PATH"
  SQL_COUNT=$((SQL_COUNT + 1))
done
echo "$SQL_COUNT SQL file(s) rolled back."
echo ""

# ── 2. Restore and redeploy Kestra flows ───────────────────────
echo "── Restoring Kestra flows ──"
FLOW_COUNT=0
for filepath in $(git ls-tree -r --name-only "$PREV" -- "$FLOWS_DIR" 2>/dev/null | grep -E '\.(yaml|yml)$' || true); do
  mkdir -p "$(dirname "$filepath")"
  git show "${PREV}:${filepath}" > "$filepath"

  NS=$(grep '^namespace:' "$filepath" | head -1 | awk '{print $2}')
  ID=$(grep '^id:' "$filepath" | head -1 | awk '{print $2}')
  if [ -z "$NS" ] || [ -z "$ID" ]; then
    echo "  ⚠️  Skipping $filepath (missing namespace or id)"
    continue
  fi
  curl -s -X PUT "${KESTRA_SERVER}/api/v1/main/flows/${NS}/${ID}" \
    -u "${KESTRA_USER}:${KESTRA_PASS}" \
    -H "Content-Type: application/x-yaml" \
    --data-binary @"$filepath" && echo "  ⏪ Rolled back ${NS}/${ID}"
  FLOW_COUNT=$((FLOW_COUNT + 1))
done
echo "$FLOW_COUNT flow(s) rolled back."
echo ""

# ── 3. Restore flow scripts (local disk only) ──────────────────
echo "── Restoring flow scripts ──"
SCRIPT_COUNT=0
for filepath in $(git ls-tree -r --name-only "$PREV" -- "$SCRIPTS_DIR" 2>/dev/null \
                  | grep '\.py$' \
                  | grep -v -E "^${SCRIPTS_DIR}/(ci|deploy)/" \
                  | grep -v '__pycache__' || true); do
  mkdir -p "$(dirname "$filepath")"
  git show "${PREV}:${filepath}" > "$filepath"
  echo "  ⏪ Restored $filepath"
  SCRIPT_COUNT=$((SCRIPT_COUNT + 1))
done
echo "$SCRIPT_COUNT flow script(s) rolled back."
echo ""

echo "✅ Rollback complete: $SQL_COUNT SQL, $FLOW_COUNT flows, $SCRIPT_COUNT scripts."
