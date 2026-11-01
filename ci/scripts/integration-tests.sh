#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

usage_error() {
    echo "Error: $1" >&2
    echo "Usage: $0 -c <test-case> -f <format>" >&2
    exit 1
}

case=''
format=''

while getopts ':c:f:' opt; do
    case "$opt" in
        c) case=$OPTARG ;;
        f) format=$OPTARG ;;
        \?) usage_error "Unknown option: -$OPTARG" ;;
        :) usage_error "Option -$OPTARG requires an argument" ;;
    esac
done
shift "$((OPTIND - 1))"

if [[ -z "$case" || "$case" == -* ]]; then
    usage_error "Option -c requires a non-empty test case that does not start with '-'"
fi
if [[ -z "$format" || "$format" == -* ]]; then
    usage_error "Option -f requires a non-empty format that does not start with '-'"
fi
if (( $# > 0 )); then
    usage_error "Unexpected argument: $1"
fi

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

# explicit tag -> explicit image -> nightly image -> the image defined in docker-compose.yml
if [[ -n "${RW_IMAGE_TAG:-}" ]]; then
  RW_IMAGE="ghcr.io/risingwavelabs/risingwave:${RW_IMAGE_TAG}"
elif [[ -z "${RW_IMAGE:-}" ]]; then
  case "${BUILDKITE_SOURCE:-}" in
    schedule)
      RW_IMAGE="ghcr.io/risingwavelabs/risingwave:nightly-$(date '+%Y%m%d')"
      ;;
    webhook)
      # Use yesterday's nightly image for webhook builds (PRs).
      RW_IMAGE="ghcr.io/risingwavelabs/risingwave:nightly-$(date -d '-1 day' '+%Y%m%d')"
      ;;
  esac
fi

if [[ -n "${RW_IMAGE:-}" ]]; then
  export RW_IMAGE
  echo Docker image: "$RW_IMAGE"
else
  echo "RW_IMAGE is unset or empty. The image defined in docker-compose.yml will be used."
fi

echo "--- case: ${case}, format: ${format}"

if [ "${case}" == "client-library" ]; then
  python3 integration_tests/client-library/client_test.py
  exit 0
fi

echo "~~~ install postgresql"
sudo yum install -y postgresql15

echo "~~~ install poetry"
curl -sSL https://install.python-poetry.org | POETRY_VERSION=1.8.0 python3 -
export PATH=$PATH:$HOME/.local/bin


echo "--- download rwctest-key"
aws secretsmanager get-secret-value --secret-id "gcp-buildkite-rwctest-key" --region us-east-2 --query "SecretString" --output text >gcp-rwctest.json

echo "--- rewrite docker compose for protobuf"
if [ "${format}" == "protobuf" ]; then
  python3 integration_tests/scripts/gen_pb_compose.py "${case}" "${format}"
fi

restore_max_map_count() {
  local exit_status=$?
  echo "--- reset vm.max_map_count=${max_map_count_original_value} ---"
  if ! sudo sysctl -w vm.max_map_count="$max_map_count_original_value"; then
    echo "Error: failed to restore vm.max_map_count=${max_map_count_original_value}" >&2
    if (( exit_status == 0 )); then
      exit_status=1
    fi
  fi
  exit "$exit_status"
}

if [ "${case}" == "doris-sink" ]; then
  echo "--- set vm.max_map_count=2000000 for doris ---"
  max_map_count_original_value=$(sysctl -n vm.max_map_count)
  trap restore_max_map_count EXIT
  sudo sysctl -w vm.max_map_count=2000000
fi

echo "--- run Demos"
python3 integration_tests/scripts/run_demos.py --case "${case}" --format "${format}"

echo "--- run docker ps"
docker ps

echo "--- check if the ingestion is successful"
# extract the type of upstream source,e.g. mysql,postgres,etc
upstream=$(echo "${case}" | cut -d'-' -f 1)
python3 integration_tests/scripts/check_data.py "${case}" "${upstream}"
