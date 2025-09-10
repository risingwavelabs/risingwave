#!/usr/bin/env bash

set -euo pipefail

export DOCKER_BUILDKIT=1
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
cd "$DIR"


: "${ACR_LOGIN_SERVER:?Set ACR_LOGIN_SERVER in environment}"
: "${ACR_USERNAME:?Set ACR_USERNAME in environment}"
: "${ACR_PASSWORD:?Set ACR_PASSWORD in environment}"


cat ../rust-toolchain
# shellcheck disable=SC2155

# Import `BUILD_ENV_VERSION` from `.env`
source .env

export BUILD_TAG="${ACR_LOGIN_SERVER}/${ACR_REPOSITORY}:${BUILD_ENV_VERSION}"

echo "+++ Arch"
arch

echo "--- Docker login"
echo "${ACR_PASSWORD}" | docker login "${ACR_LOGIN_SERVER}" -u "${ACR_USERNAME}" --password-stdin

echo "--- Check image existence"
set +e
# remove all local images to ensure we fetch remote images
docker image rm "$BUILD_TAG"
# check manifest
if docker manifest inspect "$BUILD_TAG"; then
	echo "+++ Image already exists"
	echo "${BUILD_TAG} already exists -- skipping build image"
	exit 0
fi
set -ex

echo "--- Docker build"
if [[ -z ${BUILDKITE} ]]; then
	export DOCKER_BUILD_PROGRESS="--progress=auto"
else
	export DOCKER_BUILD_PROGRESS="--progress=plain"
fi

docker build -t "$BUILD_TAG" "$DOCKER_BUILD_PROGRESS" --no-cache .

echo "--- Docker push"
docker push "$BUILD_TAG"
