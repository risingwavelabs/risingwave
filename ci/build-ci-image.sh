#!/bin/bash

set -e

export DOCKER_BUILDKIT=1
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR"/..

cat ../rust-toolchain
export DEFAULT_RUST_TOOLCHAIN=$(cat ../rust-toolchain)

