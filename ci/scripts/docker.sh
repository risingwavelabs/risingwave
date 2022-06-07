#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

date="$(date +%Y%m%d)"
ghcraddr="ghcr.io/singularity-data"

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
  docker tag "${ghcraddr}"/"${component}":latest "${ghcraddr}"/"${component}":nightly-"${date}"
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
    docker push "${ghcraddr}"/"${component}":nightly-"${date}"
  done
fi
