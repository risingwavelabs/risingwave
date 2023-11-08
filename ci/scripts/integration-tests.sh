#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

while getopts 'c:f:' opt; do
    case ${opt} in
        c )
            case=$OPTARG
            ;;
        f )
            format=$OPTARG
            ;;
        \? )
            echo "Invalid Option: -$OPTARG" 1>&2
            exit 1
            ;;
        : )
            echo "Invalid option: $OPTARG requires an argument" 1>&2
            ;;
    esac
done
shift $((OPTIND -1))

echo "export INTEGRATION_TEST_CASE=${case}" > env_vars.sh

echo "--- clean up docker"
if [ $(docker ps -aq |wc -l) -gt 0 ]; then
  docker rm -f $(docker ps -aq)
fi
docker network prune -f
docker volume prune -f

echo "--- ghcr login"
echo "$GHCR_TOKEN" | docker login ghcr.io -u "$GHCR_USERNAME" --password-stdin

echo "--- install postgresql"
sudo yum install -y postgresql15

cd integration_tests/scripts

echo "--- case: ${case}, format: ${format}"

if [ -n "${RW_IMAGE_TAG}" ]; then
  export RW_IMAGE="ghcr.io/risingwavelabs/risingwave:${RW_IMAGE_TAG}"
  echo Docker image: $RW_IMAGE
fi

if [ "${BUILDKITE_SOURCE}" == "schedule" ]; then
  # Use ghcr nightly image for scheduled build. If not specified, we use dockerhub's 'risingwavelabs/risingwave'.
  export RW_IMAGE="ghcr.io/risingwavelabs/risingwave:nightly-$(date '+%Y%m%d')"
  echo Docker image: $RW_IMAGE
fi

echo "--- rewrite docker compose for protobuf"
if [ "${format}" == "protobuf" ]; then
  python3 gen_pb_compose.py ${case} ${format}
fi

echo "--- run Demos"
python3 run_demos.py --case ${case} --format ${format}

echo "--- run docker ps"
docker ps

echo "--- check if the ingestion is successful"
# extract the type of upstream source,e.g. mysql,postgres,etc
upstream=$(echo ${case} | cut -d'-' -f 1)
if [ "${upstream}" == "mysql" ]; then
  echo "install mysql"
  sudo rpm -Uvh https://dev.mysql.com/get/mysql80-community-release-el9-1.noarch.rpm
  sudo dnf -y install mysql-community-server
fi

export PGPASSWORD=123456
python3 check_data.py ${case} ${upstream}

echo "--- clean Demos"
python3 clean_demos.py --case ${case}
