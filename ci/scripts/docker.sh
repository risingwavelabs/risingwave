#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

ghcraddr="ghcr.io/singularity-data"
imagetag="$(date +%Y%m%d)"
if [ "$BUILDKITE_TAG" != "" ]; then
  imagetag="${BUILDKITE_TAG}"
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
  docker build -f docker/Dockerfile -t "${ghcraddr}"/"${component}":latest --target "${component}" .
  docker tag "${ghcraddr}"/"${component}":latest "${ghcraddr}"/"${component}":nightly-"${imagetag}"
done

echo "--- docker images"
docker images

if [ "$PUSH_GHCR" = true ]; then
  echo "--- ghcr login"
  echo "$GHCR_TOKEN" | docker login ghcr.io -u "$GHCR_USERNAME" --password-stdin

  echo "--- docker push to ghcr"
  for component in "${components[@]}"
  do
    docker push "${ghcraddr}"/"${component}":latest
    docker push "${ghcraddr}"/"${component}":nightly-"${imagetag}"
  done
fi
