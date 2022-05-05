#!/bin/bash

set -e

export DOCKER_BUILDKIT=1
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR"/..

docker build -f docker/Dockerfile -t frontend-node:latest --target frontend-node .
docker build -f docker/Dockerfile -t compute-node:latest --target compute-node .
docker build -f docker/Dockerfile -t meta-node:latest --target meta-node .
docker build -f docker/Dockerfile -t compactor-node:latest --target compactor-node .
docker build -f docker/Dockerfile -t risingwave:latest --target risingwave .
