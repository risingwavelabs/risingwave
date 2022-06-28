#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

date="$(date +%Y%m%d)"
ghcraddr="ghcr.io/singularity-data"

# If this is a schedule/ui build, tag the image with the date.
if [ "${BUILDKITE_SOURCE}" == "schedule" ] || [ "${BUILDKITE_SOURCE}" == "ui" ]; then
  TAG="nightly-${date}"
  echo "$TAG"
fi

# If there's a tag, we tag the image.
if [[ -n "${BUILDKITE_TAG}" ]]; then
  TAG="${BUILDKITE_TAG}"
  echo "$TAG"
fi

# If the commit is 40 characters long, it's probably a SHA.
if [[ "${#BUILDKITE_COMMIT}" = 40 ]]; then
  TAG="git-${BUILDKITE_COMMIT}"
  echo "$TAG"
fi

components=(
  "risingwave"
  "compute-node"
  "meta-node"
  "frontend-node"
  "compactor-node"
)

for component in "${components[@]}"
do
  echo "--- docker build and tag : ${component}"
  docker build -f docker/Dockerfile -t "${ghcraddr}/${component}:latest" --target "${component}" .
  docker tag "${ghcraddr}/${component}:latest" "${ghcraddr}/${component}:${TAG}"
done

echo "--- docker images"
docker images

if [ "$PUSH_GHCR" = true ]; then
  echo "--- ghcr login"
  echo "$GHCR_TOKEN" | docker login ghcr.io -u "$GHCR_USERNAME" --password-stdin

  echo "--- docker push to ghcr"
  for component in "${components[@]}"
  do
    docker push "${ghcraddr}/${component}:latest"
    docker push "${ghcraddr}/${component}:${TAG}"
  done
fi
