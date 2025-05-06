#!/usr/bin/env bash

set -euo pipefail

# Default behavior: check and build if necessary
CHECK_ONLY=false

# Parse command line arguments
while getopts "c" opt; do
	case ${opt} in
		c)
			CHECK_ONLY=true
			;;
		\?)
			echo "Usage: $0 [-c]"
			echo "-c: Only check image existence, don't build"
			exit 1
			;;
	esac
done

export DOCKER_BUILDKIT=1
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
cd "$DIR"

cat ../rust-toolchain
# shellcheck disable=SC2155

# Generate version based on docker-compose.yml file hash and rust-toolchain.
MOD_DATE=$(date -r docker-compose.yml +"%Y%m%d")
FILE_HASH=$(sha256sum docker-compose.yml | cut -c1-8)
RUST_VERSION=$(grep -o 'nightly-[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}' ../rust-toolchain)
export RW_BUILD_ENV_VERSION="v${MOD_DATE}-${FILE_HASH}-rust-${RUST_VERSION}"

export BUILD_TAG="public.ecr.aws/w1p7b4n3/rw-build-env:${RW_BUILD_ENV_VERSION}"

echo "+++ Setting image version to following Buildkite steps"
buildkite-agent env set "RW_BUILD_ENV_VERSION=${RW_BUILD_ENV_VERSION}"

echo "+++ Arch"
arch

echo "--- Docker login"
aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws/w1p7b4n3

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
set -e

# If we get here, the image doesn't exist
if [ "$CHECK_ONLY" = true ]; then
	echo "Image doesn't exist, but -c flag provided. Not building."
	exit 0
fi

echo "--- Docker build"
if [[ -z ${BUILDKITE} ]]; then
	export DOCKER_BUILD_PROGRESS="--progress=auto"
else
	export DOCKER_BUILD_PROGRESS="--progress=plain"
fi

docker build -t "$BUILD_TAG" "$DOCKER_BUILD_PROGRESS" --no-cache .

echo "--- Docker push"
docker push "$BUILD_TAG"
