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

echo "~~~ clean up docker"
# shellcheck disable=SC2046
if [ $(docker ps -aq |wc -l) -gt 0 ]; then
  docker rm -f $(docker ps -aq)
fi
docker network prune -f
docker volume prune -f -a

echo "~~~ ghcr login"
echo "$GHCR_TOKEN" | docker login ghcr.io -u "$GHCR_USERNAME" --password-stdin

echo "+++ set RW_IMAGE"

if [[ -n "${RW_IMAGE_TAG+x}" ]]; then
  export RW_IMAGE="ghcr.io/risingwavelabs/risingwave:${RW_IMAGE_TAG}"
  echo Docker image: "$RW_IMAGE"
fi

if [ "${BUILDKITE_SOURCE}" == "schedule" ]; then
  # Use ghcr nightly image for scheduled build. If not specified, we use dockerhub's 'risingwavelabs/risingwave'.
  export RW_IMAGE="ghcr.io/risingwavelabs/risingwave:nightly-$(date '+%Y%m%d')"
  echo Docker image: "$RW_IMAGE"
fi

if [ "${BUILDKITE_SOURCE}" == "webhook" ]; then
  # Use ghcr nightly image for webhook build (PR). If not specified, we use dockerhub's 'risingwavelabs/risingwave'.
  # use yesterday's date
  export RW_IMAGE="ghcr.io/risingwavelabs/risingwave:nightly-$(date -d '-1 day' '+%Y%m%d')"
  echo Docker image: "$RW_IMAGE"
fi

if [ -z "${RW_IMAGE+x}" ]; then
  echo "RW_IMAGE is not set. The image defined in docker-compose.yml will be used."
fi

echo "--- case: ${case}, format: ${format}"

if [ "${case}" == "client-library" ]; then
  python3 integration_tests/client-library/client_test.py
  exit 0
fi

echo "~~~ install postgresql client"
pg_client_pkg="postgresql15"

# For postgres-cdc tests, align local psql major version with the upstream image when possible.
if [[ "${case}" == "postgres-cdc" && -n "${POSTGRES_CDC_IMAGE:-}" ]]; then
  if [[ "${POSTGRES_CDC_IMAGE}" =~ ^postgres:([0-9]+)([.-].*)?$ ]]; then
    pg_candidate="postgresql${BASH_REMATCH[1]}"
    echo "Detected POSTGRES_CDC_IMAGE=${POSTGRES_CDC_IMAGE}, probing ${pg_candidate}"
    if yum info "${pg_candidate}" >/dev/null 2>&1; then
      pg_client_pkg="${pg_candidate}"
    else
      echo "WARN: ${pg_candidate} is unavailable on this runner; fallback to ${pg_client_pkg}"
    fi
  else
    echo "WARN: cannot parse postgres major version from POSTGRES_CDC_IMAGE=${POSTGRES_CDC_IMAGE}; fallback to ${pg_client_pkg}"
  fi
fi

sudo yum install -y "${pg_client_pkg}"

echo "~~~ install poetry"
curl -sSL https://install.python-poetry.org | POETRY_VERSION=1.8.0 python3 -
export PATH=$PATH:$HOME/.local/bin


echo "--- download rwctest-key"
aws secretsmanager get-secret-value --secret-id "gcp-buildkite-rwctest-key" --region us-east-2 --query "SecretString" --output text >gcp-rwctest.json

echo "--- rewrite docker compose for protobuf"
if [ "${format}" == "protobuf" ]; then
  python3 integration_tests/scripts/gen_pb_compose.py "${case}" "${format}"
fi

echo "--- set vm.max_map_count=2000000 for doris"
max_map_count_original_value=$(sysctl -n vm.max_map_count)
sudo sysctl -w vm.max_map_count=2000000

echo "--- run Demos"
python3 integration_tests/scripts/run_demos.py --case "${case}" --format "${format}"

echo "--- run docker ps"
docker ps

echo "--- check if the ingestion is successful"
# extract the type of upstream source,e.g. mysql,postgres,etc
upstream=$(echo "${case}" | cut -d'-' -f 1)
python3 integration_tests/scripts/check_data.py "${case}" "${upstream}"

echo "--- reset vm.max_map_count={$max_map_count_original_value}"
sudo sysctl -w vm.max_map_count="$max_map_count_original_value"
