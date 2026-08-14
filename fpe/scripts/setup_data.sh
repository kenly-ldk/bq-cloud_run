#!/usr/bin/env bash
# Build the tokenized table the sweep reads from.
#
# Run AFTER the service is deployed and the remote functions exist, since it
# calls fpe_encrypt to produce the ciphertext column.
#
#   ./fpe/scripts/setup_data.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# shellcheck disable=SC1091
source "${REPO_ROOT}/config/prelude.sh"

echo "==> Building ${FPE_DATASET}.${FPE_TABLE_TOKENIZED} from ${FPE_TABLE_CLEAR}"
echo "    (this runs the remote function over every row — expect a few minutes)"

bq --project_id="${PROJECT_ID}" query --use_legacy_sql=false --format=none \
  "CREATE OR REPLACE TABLE \`${PROJECT_ID}.${FPE_DATASET}.${FPE_TABLE_TOKENIZED}\` AS
   SELECT
     id,
     -- Deliberately mixed cardinality, so the dedup optimisation has both a
     -- case where it wins big (name: ~100 distinct) and one where it cannot
     -- help (ssn/email: near-unique).
     \`${PROJECT_ID}.${FPE_DATASET}\`.fpe_encrypt(name,  'name')  AS name,
     \`${PROJECT_ID}.${FPE_DATASET}\`.fpe_encrypt(email, 'email') AS email,
     \`${PROJECT_ID}.${FPE_DATASET}\`.fpe_encrypt(ssn,   'ssn')   AS ssn,
     \`${PROJECT_ID}.${FPE_DATASET}\`.fpe_encrypt(dob,   'digits') AS dob
   FROM \`${PROJECT_ID}.${FPE_DATASET}.${FPE_TABLE_CLEAR}\`"

echo "==> Verifying roundtrip on a sample"
bq --project_id="${PROJECT_ID}" query --use_legacy_sql=false --format=prettyjson \
  "SELECT
     c.ssn   AS ssn_clear,
     t.ssn   AS ssn_tokenized,
     \`${PROJECT_ID}.${FPE_DATASET}\`.fpe_decrypt(t.ssn, 'ssn') AS ssn_roundtrip,
     c.email AS email_clear,
     t.email AS email_tokenized,
     \`${PROJECT_ID}.${FPE_DATASET}\`.fpe_decrypt(t.email, 'email') AS email_roundtrip
   FROM \`${PROJECT_ID}.${FPE_DATASET}.${FPE_TABLE_CLEAR}\` c
   JOIN \`${PROJECT_ID}.${FPE_DATASET}.${FPE_TABLE_TOKENIZED}\` t USING (id)
   LIMIT 3"

echo
echo "Checking that every row roundtrips ..."
bq --project_id="${PROJECT_ID}" query --use_legacy_sql=false --format=csv \
  "SELECT COUNTIF(\`${PROJECT_ID}.${FPE_DATASET}\`.fpe_decrypt(t.ssn, 'ssn') != c.ssn)
            AS ssn_mismatches,
          COUNT(*) AS checked
   FROM \`${PROJECT_ID}.${FPE_DATASET}.${FPE_TABLE_CLEAR}\` c
   JOIN \`${PROJECT_ID}.${FPE_DATASET}.${FPE_TABLE_TOKENIZED}\` t USING (id)
   WHERE MOD(c.id, 1000) = 0"

echo "Done."
