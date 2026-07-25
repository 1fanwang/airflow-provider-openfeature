#!/usr/bin/env bash
# Create the airflow.task.pool toggle in Unleash for the multi-backend e2e.
# Enables the flag for the canary cohort (mig_dag_000..009) via a dag_id constraint,
# so UnleashProvider(enabled_values={"airflow.task.pool": "canary_pool"}) returns canary_pool
# for that cohort and default_pool for the rest. Idempotent.
#
#   docker compose -f system_tests/docker-compose.unleash.yml up -d
#   bash system_tests/setup_unleash.sh
set -euo pipefail
BASE=${UNLEASH_URL:-http://localhost:4242}
ADMIN="Authorization: *:*.unleash-insecure-admin-api-token"
FLAG=airflow.task.pool
PROJ=default
ENV=development

# canary cohort as a JSON array
VALUES=$(python3 -c 'import json;print(json.dumps([f"mig_dag_{i:03d}" for i in range(10)]))')

echo "context field dag_id"
curl -s -X POST "$BASE/api/admin/context" -H "$ADMIN" -H 'content-type: application/json' \
  -d '{"name":"dag_id","description":"Airflow DAG id","stickiness":false}' -o /dev/null -w "  -> %{http_code}\n" || true

echo "feature flag $FLAG"
curl -s -X POST "$BASE/api/admin/projects/$PROJ/features" -H "$ADMIN" -H 'content-type: application/json' \
  -d "{\"name\":\"$FLAG\",\"type\":\"release\"}" -o /dev/null -w "  -> %{http_code}\n" || true

echo "strategy (dag_id IN canary cohort) in $ENV"
curl -s -X POST "$BASE/api/admin/projects/$PROJ/features/$FLAG/environments/$ENV/strategies" \
  -H "$ADMIN" -H 'content-type: application/json' \
  -d "{\"name\":\"default\",\"constraints\":[{\"contextName\":\"dag_id\",\"operator\":\"IN\",\"values\":$VALUES}]}" \
  -o /dev/null -w "  -> %{http_code}\n" || true

echo "enable $FLAG in $ENV"
curl -s -X POST "$BASE/api/admin/projects/$PROJ/features/$FLAG/environments/$ENV/on" \
  -H "$ADMIN" -o /dev/null -w "  -> %{http_code}\n" || true

echo "done"
