#!/usr/bin/env bash
# One-time provisioning for the FPE demo.
#
#   ./fpe/scripts/provision.sh
#
# Idempotent: every step is a create-if-absent. Safe to re-run.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# shellcheck disable=SC1091
source "${REPO_ROOT}/config/prelude.sh"

echo "==> Project: ${PROJECT_ID} (region ${REGION})"

echo "==> Enabling APIs"
gcloud services enable \
  run.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com \
  bigquery.googleapis.com \
  bigqueryconnection.googleapis.com \
  monitoring.googleapis.com \
  logging.googleapis.com \
  --project="${PROJECT_ID}"

echo "==> Artifact Registry repo: ${AR_REPOSITORY}"
if ! gcloud artifacts repositories describe "${AR_REPOSITORY}" \
      --location="${REGION}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
  gcloud artifacts repositories create "${AR_REPOSITORY}" \
    --repository-format=docker \
    --location="${REGION}" \
    --description="Images for the BigQuery remote function demos" \
    --project="${PROJECT_ID}"
else
  echo "    already exists"
fi

echo "==> BigQuery dataset: ${FPE_DATASET}"
if ! bq --project_id="${PROJECT_ID}" ls -d "${FPE_DATASET}" >/dev/null 2>&1; then
  bq --project_id="${PROJECT_ID}" mk \
    --dataset \
    --location="${BQ_LOCATION}" \
    --description="Vendor-free FPE remote function demo + perf sweep" \
    "${PROJECT_ID}:${FPE_DATASET}"
else
  echo "    already exists"
fi

echo "==> BigQuery connection: ${BQ_CONNECTION} (${BQ_LOCATION})"
if ! bq --project_id="${PROJECT_ID}" show --connection \
      --location="${BQ_LOCATION}" "${BQ_CONNECTION}" >/dev/null 2>&1; then
  bq --project_id="${PROJECT_ID}" mk --connection \
    --location="${BQ_LOCATION}" \
    --connection_type=CLOUD_RESOURCE \
    "${BQ_CONNECTION}"
else
  echo "    already exists"
fi

CONN_SA="$(bq --project_id="${PROJECT_ID}" show --format=prettyjson --connection \
  --location="${BQ_LOCATION}" "${BQ_CONNECTION}" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["cloudResource"]["serviceAccountId"])')"
echo "==> Connection service account: ${CONN_SA}"

echo "==> Granting run.invoker to the connection SA on ${FPE_SERVICE}"
# The service may not exist yet on first run; grant at project level so the
# binding survives service recreation across the sweep.
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${CONN_SA}" \
  --role="roles/run.invoker" \
  --condition=None \
  --quiet >/dev/null

echo "==> Clear-text table: ${FPE_DATASET}.${FPE_TABLE_CLEAR}"
if bq --project_id="${PROJECT_ID}" show "${FPE_DATASET}.${FPE_TABLE_CLEAR}" >/dev/null 2>&1; then
  echo "    already exists"
elif [[ -n "${FPE_SOURCE_TABLE:-}" ]]; then
  echo "    copying ${FPE_ROWS} rows from ${FPE_SOURCE_TABLE}"
  bq --project_id="${PROJECT_ID}" query --use_legacy_sql=false --format=none \
    "CREATE OR REPLACE TABLE \`${PROJECT_ID}.${FPE_DATASET}.${FPE_TABLE_CLEAR}\` AS
     SELECT id, name, email, ssn, dob
     FROM \`${PROJECT_ID}.${FPE_SOURCE_TABLE}\`
     LIMIT ${FPE_ROWS}"
else
  echo "    ! FPE_SOURCE_TABLE is empty and the table does not exist."
  echo "      Generate and load one first:"
  echo "        python shared/generate_mock_data.py ${FPE_ROWS}"
  echo "        bq load --source_format=CSV --skip_leading_rows=1 \\"
  echo "          ${FPE_DATASET}.${FPE_TABLE_CLEAR} pii_data.csv \\"
  echo "          id:INTEGER,name:STRING,email:STRING,ssn:STRING,dob:STRING"
  exit 1
fi

echo
echo "Provisioning complete."
echo "  Dataset:     ${PROJECT_ID}:${FPE_DATASET}"
echo "  Connection:  ${PROJECT_ID}.${BQ_LOCATION}.${BQ_CONNECTION}"
echo "  Conn SA:     ${CONN_SA}"
echo "  AR repo:     ${REGION}-docker.pkg.dev/${PROJECT_ID}/${AR_REPOSITORY}"
