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

# push images to gchr
function pushGchr() {
  GHCRTAG="${ghcraddr}:$1"
  echo "push to gchr, image tag: ${GHCRTAG}"
  docker manifest create --insecure "$GHCRTAG" \
    --amend "${ghcraddr}:${BUILDKITE_COMMIT}-x86_64" \
    --amend "${ghcraddr}:${BUILDKITE_COMMIT}-aarch64"
  docker manifest push --insecure "$GHCRTAG"
}

# push images to dockerhub
function pushDockerhub() {
  DOCKERTAG="${dockerhubaddr}:$1"
  echo "push to dockerhub, image tag: ${DOCKERTAG}"
  docker manifest create --insecure "$DOCKERTAG" \
    --amend "${dockerhubaddr}:${BUILDKITE_COMMIT}-x86_64" \
    --amend "${dockerhubaddr}:${BUILDKITE_COMMIT}-aarch64"
  docker manifest push --insecure "$DOCKERTAG"
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
docker run --rm lumir/remove-dockerhub-tag \
  --user "risingwavelabs" --password "$DOCKER_TOKEN" \
  "${dockerhubaddr}:${BUILDKITE_COMMIT}-x86_64" \
  "${dockerhubaddr}:${BUILDKITE_COMMIT}-aarch64"