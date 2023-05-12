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

echo "--- install postgresql"
sudo yum install -y postgresql

cd integration_tests/scripts

echo "--- case: ${case}, format: ${format}"

echo "--- Rewrite docker compose for protobuf"
if [ "${format}" == "protobuf" ]; then
  python3 gen_pb_compose.py ${case} ${format}
fi

echo "--- Run Demos"
python3 run_demos.py --case ${case} --format ${format}

echo "--- Check if the ingestion is successful"
python3 check_data.py ${case}

echo "--- Stop the docker-compose cluster"
docker compose down -v
