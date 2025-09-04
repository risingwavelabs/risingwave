#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

export DOCKER_BUILDKIT=1
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
cd "$DIR"

: "${ACR_LOGIN_SERVER:?Set ACR_LOGIN_SERVER in environment}"
: "${ACR_USERNAME:?Set ACR_USERNAME in environment}"
: "${ACR_PASSWORD:?Set ACR_PASSWORD in environment}"

acraddr="${ACR_LOGIN_SERVER}/risingwave"
arch="$(uname -m)"
CARGO_PROFILE=${CARGO_PROFILE:-production}

echo "--- Docker login"
echo "$ACR_PASSWORD" | docker login "$ACR_LOGIN_SERVER" -u "$ACR_USERNAME" --password-stdin

if [[ -n "${ORIGINAL_IMAGE_TAG+x}" ]] && [[ -n "${NEW_IMAGE_TAG+x}" ]]; then
  echo "--- retag docker image"
  docker pull ${acraddr}:${ORIGINAL_IMAGE_TAG}
  docker tag ${acraddr}:${ORIGINAL_IMAGE_TAG} ${acraddr}:${NEW_IMAGE_TAG}-${arch}
  docker push ${acraddr}:${NEW_IMAGE_TAG}-${arch}
  exit 0
fi

# Check image existence
set +e
docker image rm "${acraddr}:${BUILDKITE_COMMIT}-${arch}" 2>/dev/null
if docker manifest inspect "${acraddr}:${BUILDKITE_COMMIT}-${arch}" 2>/dev/null; then
  echo "+++ Image already exists"
  echo "${acraddr}:${BUILDKITE_COMMIT}-${arch} already exists -- skipping build"
  exit 0
fi
set -e

# Build RisingWave docker image ${BUILDKITE_COMMIT}-${arch}
echo "--- docker build and tag"
echo "CARGO_PROFILE is set to ${CARGO_PROFILE}"

docker buildx create \
  --name container \
  --driver=docker-container

PULL_PARAM=""
if [[ "${ALWAYS_PULL:-false}" = "true" ]]; then
  PULL_PARAM="--pull"
fi

if [[ -z ${BUILDKITE} ]]; then
  export DOCKER_BUILD_PROGRESS="--progress=auto"
else
  export DOCKER_BUILD_PROGRESS="--progress=plain"
fi

docker buildx build -f docker/Dockerfile \
  --build-arg "GIT_SHA=${BUILDKITE_COMMIT}" \
  --build-arg "CARGO_PROFILE=${CARGO_PROFILE}" \
  -t "${acraddr}:${BUILDKITE_COMMIT}-${arch}" \
  $DOCKER_BUILD_PROGRESS \
  --builder=container \
  --load \
  ${PULL_PARAM} \
  --cache-to "type=registry,ref=${ACR_LOGIN_SERVER}/risingwave-build-cache:${arch}" \
  --cache-from "type=registry,ref=${ACR_LOGIN_SERVER}/risingwave-build-cache:${arch}" \
  .

echo "--- check the image can start correctly"
container_id=$(docker run -d "${acraddr}:${BUILDKITE_COMMIT}-${arch}" playground)
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

echo "--- docker push"
docker push "${acraddr}:${BUILDKITE_COMMIT}-${arch}"