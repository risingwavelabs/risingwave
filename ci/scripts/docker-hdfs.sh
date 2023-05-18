#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

ghcraddr="ghcr.io/risingwavelabs/risingwave"
dockerhubaddr="risingwavelabs/risingwave"
arch="$(uname -m)"

java_home_path=$(uname -m)
if [ "$arch" = "arm64" ] || [ "$arch" = "aarch64" ]; then
    java_home_path="/usr/lib/jvm/java-11-openjdk-arm64"
else 
# x86_64
    java_home_path="/usr/lib/jvm/java-11-openjdk-amd64"
fi
echo $java_home_path

# Build RisingWave docker image ${BUILDKITE_COMMIT}-${arch}
echo "--- docker build and tag"
docker build -f docker/Dockerfile --build-arg "GIT_SHA=${BUILDKITE_COMMIT}" --build-arg "JAVA_HOME_PATH=${java_home_path}" -t "${ghcraddr}:${BUILDKITE_COMMIT}-${arch}" --target risingwave .

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

echo "--- ghcr login"
echo "$GHCR_TOKEN" | docker login ghcr.io -u "$GHCR_USERNAME" --password-stdin

echo "--- dockerhub login"
echo "$DOCKER_TOKEN" | docker login -u "risingwavelabs" --password-stdin

echo "--- docker push to ghcr"
docker push "${ghcraddr}:${BUILDKITE_COMMIT}-${arch}"

echo "--- docker push to dockerhub"
docker tag "${ghcraddr}:${BUILDKITE_COMMIT}-${arch}" "${dockerhubaddr}:${BUILDKITE_COMMIT}-${arch}"
docker push "${dockerhubaddr}:${BUILDKITE_COMMIT}-${arch}"