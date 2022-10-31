#!/bin/bash

# Exits as soon as any line fails.
set -euo pipefail

date="$(date +%Y%m%d)"
ghcraddr="ghcr.io/risingwavelabs"
dockerhubaddr="risingwavelabs/risingwave"

components=(
  "risingwave"
  "compute-node"
  "meta-node"
  "frontend-node"
  "compactor-node"
)

# push images to gchr
function pushGchr() {
  GHCRTAG="${ghcraddr}/$1:$2}"
  echo "GHCRTAG: ${GHCRTAG}"
  docker manifest create --insecure "$GHCRTAG" \
    --amend "${ghcraddr}/$1:latest-x86_64" \
    --amend "${ghcraddr}/$1:latest-aarch64"
  docker manifest push --insecure "$GHCRTAG"
}

# push images to dockerhub
function pushDockerhub() {
  if [ "$1" == "risingwave" ]; then
    DOCKERTAG="${dockerhubaddr}:$2"
    echo "DOCKERTAG: ${DOCKERTAG}"
    docker manifest create --insecure "$DOCKERTAG" \
      --amend "${ghcraddr}/$1:latest-x86_64" \
      --amend "${ghcraddr}/$1:latest-aarch64"
    docker manifest push --insecure "$DOCKERTAG"
  fi
}

echo "--- ghcr login"
echo "$GHCR_TOKEN" | docker login ghcr.io -u "$GHCR_USERNAME" --password-stdin

echo "--- dockerhub login"
echo "$DOCKER_TOKEN" | docker login -u "risingwavelabs" --password-stdin

for component in "${components[@]}"
do
  echo "--- multi arch image create : ${component}"
  if [[ "${#BUILDKITE_COMMIT}" = 40 ]]; then
    # If the commit is 40 characters long, it's probably a SHA.
    TAG="git-${BUILDKITE_COMMIT}"
    pushGchr ${component} ${TAG}
    pushDockerhub ${component} ${TAG}
  fi

  if [ "${BUILDKITE_SOURCE}" == "schedule" ] || [ "${BUILDKITE_SOURCE}" == "ui" ]; then
    # If this is a schedule/ui build, tag the image with the date.
    TAG="nightly-${date}"
    pushGchr ${component} ${TAG}
    pushDockerhub ${component} ${TAG}
  fi

  if [[ -n "${BUILDKITE_TAG}" ]]; then
    # If there's a tag, we tag the image.
    TAG="${BUILDKITE_TAG}"
    pushGchr ${component} ${TAG}
    pushDockerhub ${component} ${TAG}
  fi

  TAG="latest"
  pushGchr ${component} ${TAG}
  pushDockerhub ${component} ${TAG}
done
