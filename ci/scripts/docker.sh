#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

date="$(date +%Y%m%d)"
ghcraddr="ghcr.io/risingwavelabs"
dockerhubaddr="risingwavelabs/risingwave"
arch="$(uname -m)"

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
  docker build -f docker/Dockerfile -t "${ghcraddr}/${component}:latest-${arch}" --target "${component}" .
done

echo "--- docker images"
docker images

if [ "$PUSH" = true ]; then
  echo "--- ghcr login"
  echo "$GHCR_TOKEN" | docker login ghcr.io -u "$GHCR_USERNAME" --password-stdin

  echo "--- dockerhub login"
  echo "$DOCKER_TOKEN" | docker login -u "risingwavelabs" --password-stdin

  for component in "${components[@]}"
  do
    echo "--- ${component}: docker push to ghcr"
    docker push "${ghcraddr}/${component}:latest-${arch}"

    if [ "${component}" == "risingwave" ]; then
      echo "--- ${component}: docker push to dockerhub"
      docker tag "${ghcraddr}/${component}:latest-${arch}" "${dockerhubaddr}:latest-${arch}"
      docker push "${dockerhubaddr}:latest-${arch}"
      docker push "${dockerhubaddr}:latest-${arch}"
    fi
  done
fi
