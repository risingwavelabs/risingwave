#!/usr/bin/env bash

# EXAMPLE USAGE:
: '
AWS_PROFILE=rwctest \
BUILDKITE_BUILD_NUMBER=511 \
./ci/scripts/upload-micro-bench-results.sh
'

# Exits as soon as any line fails.
set -uo pipefail

setup() {
  echo "--- Installing utils"
  echo ">>> Installing jq"
  wget https://github.com/stedolan/jq/releases/download/jq-1.6/jq-linux64
  chmod +x jq-linux64
  mv jq-linux64 /usr/local/bin/jq
}

get_branch() {
   curl -H "Authorization: Bearer $BUILDKITE_TOKEN" \
   "https://api.buildkite.com/v2/organizations/risingwavelabs/pipelines/main-cron/builds/$BUILDKITE_BUILD_NUMBER" \
  | jq '.branch'
}

get_date() {
  curl -H "Authorization: Bearer $BUILDKITE_TOKEN" \
   "https://api.buildkite.com/v2/organizations/risingwavelabs/pipelines/main-cron/builds/$BUILDKITE_BUILD_NUMBER" \
  | jq '.jobs | .[] | select ( .name | contains("micro benchmark")) | .finished_at' \
  | sed 's/\.[0-9]*Z/Z/' \
  | sed 's/\"//g'
}

get_commit() {
  curl -H "Authorization: Bearer $BUILDKITE_TOKEN" \
   "https://api.buildkite.com/v2/organizations/risingwavelabs/pipelines/main-cron/builds/$BUILDKITE_BUILD_NUMBER" \
  | jq '.commit'
}

setup

BUILDKITE_BUILD_URL="https://buildkite.com/risingwavelabs/main-cron/builds/$BUILDKITE_BUILD_NUMBER"
END_DATE=$(get_date)
COMMIT=$(get_commit)
BRANCH=$(get_branch)

wget https://download.docker.com/linux/static/stable/x86_64/docker-24.0.2.tgz
tar -xvf docker-24.0.2.tgz --no-same-owner
cp docker/* /usr/bin
dockerd &

docker run -it -rm ghcr.io/risingwavelabs/qa-infra ctl -I 52.207.243.214:8081 execution create-micro-benchmark-executions \
  --exec-url ${BUILDKITE_BUILD_URL} \
  --branch "$BRANCH" \
  --tag latest \
  --commit "$COMMIT" \
  --end-time "$END_DATE" \
  --status SUCCESS