#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

date="$(date +%Y%m%d)"
ghcraddr="ghcr.io/risingwavelabs"
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
  if [ "${BUILDKITE_SOURCE}" == "schedule" ] || [ "${BUILDKITE_SOURCE}" == "ui" ]; then
    # If this is a schedule/ui build, tag the image with the date.
    TAG="${ghcraddr}/${component}:nightly-${date}-${arch}"
    docker tag "${ghcraddr}/${component}:latest-${arch}" "$TAG"
    echo "$TAG"
  fi
  if [[ -n "${BUILDKITE_TAG}" ]]; then
    # If there's a tag, we tag the image.
    TAG="${ghcraddr}/${component}:${BUILDKITE_TAG}-${arch}"
    docker tag "${ghcraddr}/${component}:latest-${arch}" "$TAG"
    echo "$TAG"
  fi
  if [[ "${#BUILDKITE_COMMIT}" = 40 ]]; then
    # If the commit is 40 characters long, it's probably a SHA.
    TAG="${ghcraddr}/${component}:git-${BUILDKITE_COMMIT}-${arch}"
    docker tag "${ghcraddr}/${component}:latest-${arch}" "$TAG"
    echo "$TAG"
  fi
done

echo "--- docker images"
docker images

if [ "$PUSH_GHCR" = true ]; then
  echo "--- ghcr login"
  echo "$GHCR_TOKEN" | docker login ghcr.io -u "$GHCR_USERNAME" --password-stdin

  echo "--- docker push to ghcr"
  for component in "${components[@]}"
  do
    if [[ "${#BUILDKITE_COMMIT}" = 40 ]]; then
      # If the commit is 40 characters long, it's probably a SHA.
      TAG="${ghcraddr}/${component}:git-${BUILDKITE_COMMIT}-${arch}"
      docker push "$TAG"
    fi
    if [ "${BUILDKITE_SOURCE}" == "schedule" ] || [ "${BUILDKITE_SOURCE}" == "ui" ]; then
      # If this is a schedule/ui build, tag the image with the date.
      TAG="${ghcraddr}/${component}:nightly-${date}-${arch}"
      docker push "$TAG"
    fi
    if [[ -n "${BUILDKITE_TAG}" ]]; then
      # If there's a tag, we tag the image.
      TAG="${ghcraddr}/${component}:${BUILDKITE_TAG}-${arch}"
      docker push "$TAG"
    fi
    docker push "${ghcraddr}/${component}:latest-${arch}"
  done
fi