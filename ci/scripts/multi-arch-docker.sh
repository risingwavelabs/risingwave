#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

date="$(date +%Y%m%d)"
ghcraddr="ghcr.io/risingwavelabs"

components=(
  "risingwave"
  "compute-node"
  "meta-node"
  "frontend-node"
  "compactor-node"
)

echo "--- ghcr login"
echo "$GHCR_TOKEN" | docker login ghcr.io -u "$GHCR_USERNAME" --password-stdin

for component in "${components[@]}"
do
  echo "--- multi arch image create : ${component}"
  if [[ "${#BUILDKITE_COMMIT}" = 40 ]]; then
    # If the commit is 40 characters long, it's probably a SHA.
    TAG="${ghcraddr}/${component}:git-${BUILDKITE_COMMIT}"
    docker manifest create --insecure "$TAG" \
      --amend "${ghcraddr}/${component}:latest-x86_64" \
      --amend "${ghcraddr}/${component}:latest-aarch64"
    docker manifest push --insecure "$TAG"
  fi

  if [ "${BUILDKITE_SOURCE}" == "schedule" ] || [ "${BUILDKITE_SOURCE}" == "ui" ]; then
    # If this is a schedule/ui build, tag the image with the date.
    TAG="${ghcraddr}/${component}:nightly-${date}"
    docker manifest create --insecure "$TAG" \
      --amend "${ghcraddr}/${component}:latest-x86_64" \
      --amend "${ghcraddr}/${component}:latest-aarch64"
    docker manifest push --insecure "$TAG"
  fi

  if [[ -n "${BUILDKITE_TAG}" ]]; then
    # If there's a tag, we tag the image.
    TAG="${ghcraddr}/${component}:${BUILDKITE_TAG}"
    docker manifest create --insecure "$TAG" \
      --amend "${ghcraddr}/${component}:latest-x86_64" \
      --amend "${ghcraddr}/${component}:latest-aarch64"
    docker manifest push --insecure "$TAG"
  fi

  TAG="${ghcraddr}/${component}:latest"
  docker manifest create --insecure "$TAG" \
    --amend "${ghcraddr}/${component}:latest-x86_64" \
    --amend "${ghcraddr}/${component}:latest-aarch64"
  docker manifest push --insecure "$TAG"
done