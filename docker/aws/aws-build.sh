#!/usr/bin/env bash

set -e

export DOCKER_BUILDKIT=1
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

if ! [[ "$DIR" =~ ^/ebs.* ]] ; then
    echo "$(tput setaf 3)Warning: You're running build script in a non-persistent volume. Please refer to the guide and copy ~/risingwave to /ebs directory, and compile it in /ebs/risingwave, so that data won't be lost during EC2 re-create.$(tput sgr0)"
fi

cd "$DIR/../.."
cargo build -p risingwave_cmd_all --release --features "rw-static-link"
cp target/release/risingwave "$DIR/risingwave"

cd "$DIR"
docker build -t "${RW_REGISTRY}:latest" .
docker push "${RW_REGISTRY}:latest"
