#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

ghcraddr="ghcr.io/risingwavelabs/risingwave"
dockerhubaddr="risingwavelabs/risingwave"
arch="$(uname -m)"
CARGO_PROFILE=${CARGO_PROFILE:-production}

echo "--- ghcr login"
echo "$GHCR_TOKEN" | docker login ghcr.io -u "$GHCR_USERNAME" --password-stdin

echo "--- dockerhub login"
echo "$DOCKER_TOKEN" | docker login -u "risingwavelabs" --password-stdin

if [[ -n "${ORIGINAL_IMAGE_TAG+x}" ]] && [[ -n "${NEW_IMAGE_TAG+x}" ]]; then
  echo "--- retag docker image"
  docker pull ${ghcraddr}:${ORIGINAL_IMAGE_TAG}
  docker tag ${ghcraddr}:${ORIGINAL_IMAGE_TAG} ${ghcraddr}:${NEW_IMAGE_TAG}-${arch}
  docker push ${ghcraddr}:${NEW_IMAGE_TAG}-${arch}
  exit 0
fi

echo "--- init Debezium submodule"
git submodule update --init java/debezium

# Build RisingWave docker image ${BUILDKITE_COMMIT}-${arch}
echo "--- docker build and tag"
echo "CARGO_PROFILE is set to ${CARGO_PROFILE}"
docker buildx create \
  --name container \
  --driver=docker-container

cache_ref="ghcr.io/risingwavelabs/risingwave-build-cache:${arch}"
build_args=(
  -f docker/Dockerfile
  --build-arg "GIT_SHA=${BUILDKITE_COMMIT}"
  --build-arg "CARGO_PROFILE=${CARGO_PROFILE}"
  --progress plain
  --builder=container
  --cache-from "type=registry,ref=${cache_ref}"
)
pull_args=()
if [[ "${ALWAYS_PULL:-false}" = "true" ]]; then
  pull_args=(--pull)
fi
tag="${BUILDKITE_COMMIT}-${arch}"
test_image="risingwave-image-test:${tag}"
container_id=
cleanup() {
  if [[ -n "$container_id" ]]; then
    docker rm -f "$container_id" >/dev/null || true
  fi
  docker image rm "$test_image" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker buildx build "${build_args[@]}" "${pull_args[@]}" \
  --target risingwave-nodwp --load -t "$test_image" \
  .

echo "--- check the image can start correctly"
container_id=$(docker run -d "$test_image" playground)
sleep 10
container_status=$(docker inspect --format='{{.State.Status}}' "$container_id")
if [ "$container_status" != "running" ]; then
  echo "docker run failed with status $container_status"
  docker inspect "$container_id"
  docker logs "$container_id"
  exit 1
fi

echo "--- export and push the image with debug symbols"
# Publish single-architecture manifests for multi-arch-docker.sh.
docker buildx build "${build_args[@]}" \
  --target risingwave --provenance=false --push \
  -t "${ghcraddr}:${tag}" -t "${dockerhubaddr}:${tag}" \
  --cache-to "type=registry,ref=${cache_ref}" \
  .
