#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

ghcraddr="ghcr.io/risingwavelabs/risingwave"
dockerhubaddr="risingwavelabs/risingwave"
arch="$(uname -m)"
CARGO_PROFILE=${CARGO_PROFILE:-production}
DOCKER_BUILD_TIMING=${DOCKER_BUILD_TIMING:-false}

echo "--- ghcr login"
echo "$GHCR_TOKEN" | docker login ghcr.io -u "$GHCR_USERNAME" --password-stdin

echo "--- dockerhub login"
echo "$DOCKER_TOKEN" | docker login -u "risingwavelabs" --password-stdin

if [[ "$DOCKER_BUILD_TIMING" != "true" && -n "${ORIGINAL_IMAGE_TAG+x}" && -n "${NEW_IMAGE_TAG+x}" ]]; then
  echo "--- retag docker image"
  docker pull ${ghcraddr}:${ORIGINAL_IMAGE_TAG}
  docker tag ${ghcraddr}:${ORIGINAL_IMAGE_TAG} ${ghcraddr}:${NEW_IMAGE_TAG}-${arch}
  docker push ${ghcraddr}:${NEW_IMAGE_TAG}-${arch}
  exit 0
fi

builder=container
image="${ghcraddr}:${BUILDKITE_COMMIT}-${arch}"
cache_args=(
  --cache-to "type=registry,ref=ghcr.io/risingwavelabs/risingwave-build-cache:${arch}"
  --cache-from "type=registry,ref=ghcr.io/risingwavelabs/risingwave-build-cache:${arch}"
)
if [[ "$DOCKER_BUILD_TIMING" == "true" ]]; then
  if [[ "$arch" != "x86_64" ]]; then
    echo "Expected x86_64 timing runner, got $arch"
    exit 1
  fi
  builder="rw-timing-${BUILDKITE_JOB_ID}"
  image="rw-build-timing:${BUILDKITE_JOB_ID}"
  cache_args=(--no-cache)
fi

echo "--- init Debezium submodule"
git submodule update --init java/debezium

# Build RisingWave docker image ${BUILDKITE_COMMIT}-${arch}
echo "--- docker build and tag"
echo "CARGO_PROFILE is set to ${CARGO_PROFILE}"
docker buildx create \
  --name "$builder" \
  --driver=docker-container

if [[ "$DOCKER_BUILD_TIMING" == "true" ]]; then
  container_id=""
  cleanup() {
    if [[ -n "$container_id" ]]; then
      docker rm "$container_id" >/dev/null || true
    fi
    docker image rm "$image" >/dev/null 2>&1 || true
    docker buildx rm "$builder" >/dev/null || true
  }
  trap cleanup EXIT
  lscpu | grep -E '^(Architecture|CPU\(s\)|Model name):'
  grep '^MemTotal:' /proc/meminfo
fi

PULL_PARAM=""
if [[ "${ALWAYS_PULL:-false}" = "true" ]]; then
  PULL_PARAM="--pull"
fi

docker buildx build -f docker/Dockerfile \
  --build-arg "GIT_SHA=${BUILDKITE_COMMIT}" \
  --build-arg "CARGO_PROFILE=${CARGO_PROFILE}" \
  -t "$image" \
  --progress plain \
  --builder="$builder" \
  --load \
  ${PULL_PARAM} \
  "${cache_args[@]}" \
  .

if [[ "$DOCKER_BUILD_TIMING" == "true" ]]; then
  container_id=$(docker create "$image")
  docker cp "${container_id}:/risingwave/bin/cargo-timing.html" ./cargo-timing.html
  buildkite-agent artifact upload cargo-timing.html
  exit 0
fi

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
