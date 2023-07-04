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
sudo yum install -y postgresql

rw_image_tag="latest"
# Check if the variable is set and not empty
if [[ -n "${RW_IMAGE_VERSION+x}" ]]; then
  rw_image_tag=$RW_IMAGE_VERSION
fi
sed -i "s|risingwave:latest|risingwave:$rw_image_tag|g" docker/docker-compose.yml

grep "risingwave:" docker/docker-compose.yml

cd integration_tests/scripts

echo "--- case: ${case}, format: ${format}"

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
  sudo yum install -y mysql
fi

export PGPASSWORD=123456
python3 check_data.py ${case} ${upstream}

echo "--- clean Demos"
python3 clean_demos.py --case ${case}
