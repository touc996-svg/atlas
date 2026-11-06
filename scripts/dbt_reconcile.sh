#!/usr/bin/env bash
# Run an intentional full rebuild and all dbt data tests.
# Intended for a scheduled maintenance window (for example, weekly).
set -euo pipefail

PROJECT_DIR="${DBT_PROJECT_DIR:-atlas_analytics}"
LOOKBACK_HOURS="${ATLAS_DBT_LOOKBACK_HOURS:-2}"

if ! [[ "$LOOKBACK_HOURS" =~ ^[1-9][0-9]*$ ]]; then
  echo "ATLAS_DBT_LOOKBACK_HOURS must be a positive integer." >&2
  exit 2
fi

echo "ATLAS dbt reconciliation starting"
echo "Project: ${PROJECT_DIR}"
echo "Incremental overlap after reconciliation: ${LOOKBACK_HOURS} hour(s)"

dbt run \
  --project-dir "$PROJECT_DIR" \
  --full-refresh \
  --vars "{\"atlas_incremental_lookback_hours\": ${LOOKBACK_HOURS}}"

# Report model execution results before dbt test replaces run_results.json.
python scripts/dbt_run_report.py \
  --results "${PROJECT_DIR}/target/run_results.json"

dbt test --project-dir "$PROJECT_DIR"

echo "ATLAS dbt reconciliation completed successfully."
