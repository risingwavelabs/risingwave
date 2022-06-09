#!/bin/bash

set -e

export DOCKER_BUILDKIT=1
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR"/..

cat ../rust-toolchain
export DEFAULT_RUST_TOOLCHAIN=$(cat ../rust-toolchain)
export BUILD_ENV_VERSION=v20220609
export BUILD_TAG="public.ecr.aws/x5u3w5h6/rw-build-env:${BUILD_ENV_VERSION}"

docker build -t ${BUILD_TAG} .
docker push ${BUILD_TAG}
