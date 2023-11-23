#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

# Create multi-arch docker images from ${BUILDKITE_COMMIT}-x86_64 and ${BUILDKITE_COMMIT}-aarch64
# They are created by ci/scripts/docker.sh
#
# Also add addtional tags to the images:
# nightly-yyyyMMdd: nightly build in main-cron
# latest: only push to ghcr. dockerhub latest is latest release

date="$(date +%Y%m%d)"
ghcraddr="ghcr.io/risingwavelabs/risingwave"
dockerhubaddr="risingwavelabs/risingwave"


arches=()

if [ "${SKIP_TARGET_AMD64:-false}" != "true" ]; then
  arches+=("x86_64")
fi

if [ "${SKIP_TARGET_AARCH64:-false}" != "true" ]; then
  arches+=("aarch64")
fi

# doPush <registry> <image_tag> <debug_suffix>
function doPush() {
  debug_suffix=$3
  tag="$1:$2${debug_suffix}"
  echo "image tag: ${tag}"
  args=()
  for arch in "${arches[@]}"
  do
    args+=( --amend "$1:${BUILDKITE_COMMIT}-${arch}${debug_suffix}" )
  done
  docker manifest create --insecure "$tag" "$2"
  docker manifest push --insecure "$tag"
}

# push images to gchr
function pushGchr() {
  doPush "${ghcraddr}" "$1" ""
  doPush "${ghcraddr}" "$1" "-debug"
}

function pushDockerhub() {
  doPush "${dockerhubaddr}" "$1" ""
  doPush "${dockerhubaddr}" "$1" "-debug"
}

echo "--- ghcr login"
echo "$GHCR_TOKEN" | docker login ghcr.io -u "$GHCR_USERNAME" --password-stdin

echo "--- dockerhub login"
echo "$DOCKER_TOKEN" | docker login -u "risingwavelabs" --password-stdin


echo "--- multi arch image create "
if [[ "${#BUILDKITE_COMMIT}" = 40 ]]; then
  # If the commit is 40 characters long, it's probably a SHA.
  TAG="git-${BUILDKITE_COMMIT}"
  pushGchr ${TAG}
fi

if [ "${BUILDKITE_SOURCE}" == "schedule" ]; then
  # If this is a schedule build, tag the image with the date.
  TAG="nightly-${date}"
  pushGchr ${TAG}
  pushDockerhub ${TAG}
  TAG="latest"
  pushGchr ${TAG}
fi

if [ "${BUILDKITE_SOURCE}" == "ui" ] && [[ -n "${IMAGE_TAG+x}" ]]; then
  # If this is a ui build, tag the image with the $imagetag.
  TAG="${IMAGE_TAG}"
  pushGchr ${TAG}
fi

if [[ -n "${BUILDKITE_TAG}" ]]; then
  # If there's a tag, we tag the image.
  TAG="${BUILDKITE_TAG}"
  pushGchr ${TAG}
  pushDockerhub ${TAG}

  TAG="latest"
  pushDockerhub ${TAG}
fi

echo "--- delete the manifest images from dockerhub"
args=()
for arch in "${arches[@]}"
do
  args+=( "${dockerhubaddr}:${BUILDKITE_COMMIT}-${arch}" )
  args+=( "${dockerhubaddr}:${BUILDKITE_COMMIT}-${arch}-debug" )
done
docker run --rm lumir/remove-dockerhub-tag \
  --user "risingwavelabs" --password "$DOCKER_TOKEN" "${args[@]}"
