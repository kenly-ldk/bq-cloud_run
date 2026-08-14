#!/usr/bin/env bash
# Shell counterpart to config/_loader.py. Source this at the top of any script
# in this repo:
#
#   source "$(dirname "${BASH_SOURCE[0]}")/../../config/prelude.sh"
#
# Loads shared.env, layers shared.env.local on top, and exports the three
# standard GCP env vars that every downstream layer (gcloud subprocesses,
# google.cloud.* clients) reads.

set -euo pipefail

_PRELUDE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

set -a
# shellcheck disable=SC1091
source "${_PRELUDE_DIR}/shared.env"
if [[ -f "${_PRELUDE_DIR}/shared.env.local" ]]; then
  # shellcheck disable=SC1091
  source "${_PRELUDE_DIR}/shared.env.local"
fi
set +a

# Empty GCP_CREDENTIALS_FILE means "use ambient ADC" — don't export an empty
# GOOGLE_APPLICATION_CREDENTIALS, which google-auth treats as a hard error.
if [[ -n "${GCP_CREDENTIALS_FILE:-}" ]]; then
  export GOOGLE_APPLICATION_CREDENTIALS="${GCP_CREDENTIALS_FILE}"
fi
if [[ -n "${GCLOUD_CONFIG_NAME:-}" ]]; then
  export CLOUDSDK_ACTIVE_CONFIG_NAME="${GCLOUD_CONFIG_NAME}"
fi
export GOOGLE_CLOUD_PROJECT="${PROJECT_ID}"

if [[ "${PROJECT_ID}" == "your-gcp-project-id" ]]; then
  echo "ERROR: PROJECT_ID is still the placeholder." >&2
  echo "       Create ${_PRELUDE_DIR}/shared.env.local with your real values." >&2
  exit 1
fi
