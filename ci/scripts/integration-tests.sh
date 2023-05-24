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

echo "--- clean up docker containers"
if [ $(docker ps -aq |wc -l) -gt 0 ]; then
  docker rm -f $(docker ps -aq)
fi

echo "--- ghcr login"
echo "$GHCR_TOKEN" | docker login ghcr.io -u "$GHCR_USERNAME" --password-stdin

echo "--- install postgresql"
sudo yum install -y postgresql

cd integration_tests/scripts

echo "--- case: ${case}, format: ${format}"

echo "--- rewrite docker compose for protobuf"
if [ "${format}" == "protobuf" ]; then
  python3 gen_pb_compose.py ${case} ${format}
fi

echo "--- run Demos"
python3 run_demos.py --case ${case} --format ${format} || {

  echo "-- run demos failed"
  cd ../${case}
  docker compose logs

  exit 1
}

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