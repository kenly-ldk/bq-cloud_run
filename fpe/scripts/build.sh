#!/usr/bin/env bash
# Build and push the FPE service image with Cloud Build.
#
#   ./fpe/scripts/build.sh [TAG]        # TAG defaults to "latest"
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
# shellcheck disable=SC1091
source "${REPO_ROOT}/config/prelude.sh"

TAG="${1:-latest}"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${AR_REPOSITORY}/${FPE_IMAGE_NAME}:${TAG}"

echo "==> Building ${IMAGE}"
gcloud builds submit "${REPO_ROOT}/fpe/service" \
  --tag "${IMAGE}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}"

echo
echo "Built: ${IMAGE}"
