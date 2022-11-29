#!/bin/bash

set -e

usage() {
    {
        echo "This script builds the docker images"
        echo ""
        echo "Usage:"
        echo "$0 [-h] [-d] [-r] [-k <aws_access_key>] [-s <aws_secret_key>] [-n <namespace>]"
        echo ""
        echo "-c    Build one specific component. Valid values are: 'risingwave', 'compute-node', 'meta-node', 'frontend-node', 'compactor-node'"
        echo "-h    Show this help message"
    } 1>&2

    exit 1
}

c=false

while getopts ":c:h" o; do
    case "${o}" in
        c)
            c=${OPTARG}
            ;;
        h)
            usage
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))


export DOCKER_BUILDKIT=1
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR"/..

# By default build all or one if user specified that
components=(
  "frontend-node"
  "compute-node"
  "meta-node"
  "compactor-node"
  "risingwave"
)
if [[ $c != false ]]; then 
    components=(
        "$c"
    )
fi 

for component in "${components[@]}"
do
  echo "--- docker build and tag : ${component}:latest"
  docker build -f docker/Dockerfile -t "${component}:latest" --target "${component}" .
done
