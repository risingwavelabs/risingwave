#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

ghcraddr="ghcr.io/risingwavelabs/risingwave"
dockerhubaddr="risingwavelabs/risingwave"
arch="$(uname -m)"

echo "--- ghcr login"
echo "$GHCR_TOKEN" | docker login ghcr.io -u "$GHCR_USERNAME" --password-stdin

echo "--- dockerhub login"
echo "$DOCKER_TOKEN" | docker login -u "risingwavelabs" --password-stdin

# Build RisingWave docker image ${BUILDKITE_COMMIT}-${arch}
echo "--- docker build and tag"
docker buildx create \
  --name container \
  --driver=docker-container

docker buildx build -f docker/Dockerfile \
  --build-arg "GIT_SHA=${BUILDKITE_COMMIT}" -t "${ghcraddr}:${BUILDKITE_COMMIT}-${arch}" \
  --progress plain \
  --builder=container \
  --load \
  --cache-to "type=registry,ref=ghcr.io/risingwavelabs/risingwave-build-cache:${arch}" \
  --cache-from "type=registry,ref=ghcr.io/risingwavelabs/risingwave-build-cache:${arch}" \
  .

echo "--- check the image can start correctly"
container_id=$(docker run -d "${ghcraddr}:${BUILDKITE_COMMIT}-${arch}" playground)
sleep 10
container_status=$(docker inspect --format='{{.State.Status}}' "$container_id")
if [ "$container_status" != "running" ]; then
  echo "docker run failed with status $container_status"
  docker inspect "$container_id"
  docker logs "$container_id"
  exit 1
fi

echo "--- docker images"
docker images

echo "--- docker push to ghcr"
docker push "${ghcraddr}:${BUILDKITE_COMMIT}-${arch}"

echo "--- docker push to dockerhub"
docker tag "${ghcraddr}:${BUILDKITE_COMMIT}-${arch}" "${dockerhubaddr}:${BUILDKITE_COMMIT}-${arch}"
docker push "${dockerhubaddr}:${BUILDKITE_COMMIT}-${arch}"
