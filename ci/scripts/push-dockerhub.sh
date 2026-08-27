#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

: "${IMAGE_TAG:?Set IMAGE_TAG to the tag to copy from GHCR}"

TARGET_IMAGE_TAG="${TARGET_IMAGE_TAG:-${IMAGE_TAG}}"
ghcraddr="ghcr.io/risingwavelabs/risingwave"
dockerhubaddr="docker.io/risingwavelabs/risingwave"

function validateImageTag() {
  local tag="$1"
  local variable_name="$2"

  if [[ ! "$tag" =~ ^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$ ]]; then
    echo "Invalid ${variable_name}: ${tag}"
    exit 1
  fi
}

validateImageTag "${IMAGE_TAG}" "IMAGE_TAG"
validateImageTag "${TARGET_IMAGE_TAG}" "TARGET_IMAGE_TAG"

source_image="${ghcraddr}:${IMAGE_TAG}"
target_image="${dockerhubaddr}:${TARGET_IMAGE_TAG}"

echo "--- ghcr login"
echo "${GHCR_TOKEN}" | docker login ghcr.io -u "${GHCR_USERNAME}" --password-stdin

echo "--- dockerhub login"
echo "${DOCKER_TOKEN}" | docker login docker.io -u "risingwavelabs" --password-stdin

echo "--- inspect source image"
docker buildx imagetools inspect "${source_image}"

echo "--- copy image to dockerhub"
docker buildx imagetools create \
  --tag "${target_image}" \
  "${source_image}"

echo "--- inspect dockerhub image"
docker buildx imagetools inspect "${target_image}"
