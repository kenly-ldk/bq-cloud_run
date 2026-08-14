#!/usr/bin/env bash
# Create the entitlement table and the three authorized-view patterns.
#
#   ./fpe/scripts/setup_access_control.sh
#
# Renders fpe/sql/access_control_patterns.sql with the real project/dataset and
# applies it. Run after the remote functions exist.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# shellcheck disable=SC1091
source "${REPO_ROOT}/config/prelude.sh"

export PROJECT="${PROJECT_ID}"
export DATASET="${FPE_DATASET}"

RENDERED="$(mktemp -t access-control-XXXXXX.sql)"
trap 'rm -f "${RENDERED}"' EXIT

python3 - "${REPO_ROOT}/fpe/sql/access_control_patterns.sql" "${RENDERED}" <<'PY'
import os, re, sys
text = open(sys.argv[1]).read()
missing = []
def sub(m):
    if m.group(1) not in os.environ:
        missing.append(m.group(1)); return m.group(0)
    return os.environ[m.group(1)]
out = re.sub(r"\$\{(\w+)\}", sub, text)
if missing:
    sys.exit(f"ERROR: unset variables: {sorted(set(missing))}")
open(sys.argv[2], "w").write(out)
PY

echo "==> Applying access-control patterns to ${PROJECT_ID}:${FPE_DATASET}"
bq --project_id="${PROJECT_ID}" query --use_legacy_sql=false --format=none \
  < "${RENDERED}"

echo "==> Entitlements granted:"
bq --project_id="${PROJECT_ID}" query --use_legacy_sql=false --format=prettyjson \
  "SELECT * FROM \`${PROJECT_ID}.${FPE_DATASET}.entitlements\`"

echo "Done. Benchmark with:"
echo "  python fpe/scripts/sweep.py --phase access_control --skip-deploy"
