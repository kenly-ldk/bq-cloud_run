#!/usr/bin/env bash
# Render the Knative manifest and deploy one revision.
#
# Every tuning knob is an env var, so a sweep step is just:
#   FPE_CPU=4 FPE_WORKERS=4 FPE_CONCURRENCY=8 ./fpe/scripts/deploy.sh
#
# Defaults come from config/shared.env; anything exported in the environment
# wins, which is how fpe/scripts/sweep.py drives the matrix.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Capture caller overrides before prelude.sh sources the defaults over them.
_OVERRIDES="$(env | grep -E '^(FPE_|REVISION_SUFFIX=)' || true)"
# shellcheck disable=SC1091
source "${REPO_ROOT}/config/prelude.sh"
# Re-apply caller overrides on top of the file defaults.
if [[ -n "${_OVERRIDES}" ]]; then
  while IFS= read -r line; do
    [[ -n "${line}" ]] && export "${line?}"
  done <<< "${_OVERRIDES}"
fi

TAG="${IMAGE_TAG:-latest}"
export FPE_IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${AR_REPOSITORY}/${FPE_IMAGE_NAME}:${TAG}"

# Revision names must be unique, lowercase alnum + dashes, <= 63 chars.
export REVISION_SUFFIX="${REVISION_SUFFIX:-c${FPE_CONCURRENCY}-w${FPE_WORKERS}-t${FPE_THREADS}-cpu${FPE_CPU}}"
REVISION_SUFFIX="$(echo "${REVISION_SUFFIX}" | tr '[:upper:]_.' '[:lower:]--' | cut -c1-40)"
export REVISION_SUFFIX

# Defaults for knobs that only exist at deploy time.
export FPE_EXECUTION_ENV="${FPE_EXECUTION_ENV:-gen2}"
export FPE_CPU_THROTTLING="${FPE_CPU_THROTTLING:-false}"
export FPE_REQUEST_TIMEOUT="${FPE_REQUEST_TIMEOUT:-900}"
export FPE_LOG_REQUESTS="${FPE_LOG_REQUESTS:-true}"

RENDERED="$(mktemp -t fpe-service-XXXXXX.yaml)"
trap 'rm -f "${RENDERED}"' EXIT

# Render ${VAR} placeholders. Python rather than envsubst so this works on a
# bare container/CI image where gettext isn't installed.
python3 - "${REPO_ROOT}/fpe/service/service.yaml.template" "${RENDERED}" <<'PY'
import os, re, sys

src, dst = sys.argv[1], sys.argv[2]
text = open(src).read()
missing = []


def sub(match):
    name = match.group(1)
    if name not in os.environ:
        missing.append(name)
        return match.group(0)
    return os.environ[name]


out = re.sub(r"\$\{(\w+)\}", sub, text)
if missing:
    sys.exit(f"ERROR: unset variables in service.yaml.template: {sorted(set(missing))}")
open(dst, "w").write(out)
PY

echo "==> Deploying ${FPE_SERVICE} revision ${FPE_SERVICE}-${REVISION_SUFFIX}"
echo "    cpu=${FPE_CPU} mem=${FPE_MEMORY} concurrency=${FPE_CONCURRENCY}"
echo "    workers=${FPE_WORKERS} threads=${FPE_THREADS} class=${FPE_WORKER_CLASS}"
echo "    minScale=${FPE_MIN_INSTANCES} maxScale=${FPE_MAX_INSTANCES} throttling=${FPE_CPU_THROTTLING}"

gcloud run services replace "${RENDERED}" \
  --region="${REGION}" \
  --project="${PROJECT_ID}" \
  --quiet

# Remote functions are invoked by the BigQuery connection SA, never anonymously.
gcloud run services remove-iam-policy-binding "${FPE_SERVICE}" \
  --region="${REGION}" --project="${PROJECT_ID}" \
  --member="allUsers" --role="roles/run.invoker" --quiet >/dev/null 2>&1 || true

URL="$(gcloud run services describe "${FPE_SERVICE}" \
  --region="${REGION}" --project="${PROJECT_ID}" --format='value(status.url)')"
echo
echo "Deployed: ${URL}"
