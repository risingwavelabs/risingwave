#!/bin/bash

set -e

export DOCKER_BUILDKIT=1
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

cd "$DIR/../.."
cargo build -p risingwave_cmd_all --release
objcopy --compress-debug-sections=zlib-gnu target/release/risingwave "$DIR/risingwave"

cd "$DIR"
docker build -t "${RW_REGISTRY}:latest" .
docker push "${RW_REGISTRY}:latest"
