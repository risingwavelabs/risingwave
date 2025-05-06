#!/usr/bin/env bash

set -exuo pipefail

MOD_DATE=$(date -r ci/docker-compose.yml +"%Y%m%d")
FILE_HASH=$(sha256sum ci/docker-compose.yml | cut -c1-8)
RUST_VERSION=$(grep -o 'nightly-[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}' ci/rust-toolchain)

RW_BUILD_ENV_VERSION="v${MOD_DATE}-${FILE_HASH}-rust-${RUST_VERSION}"

buildkite-agent meta-data set "RW_BUILD_ENV_VERSION" "${RW_BUILD_ENV_VERSION}"
