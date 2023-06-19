#!/usr/bin/env bash

# EXAMPLE USAGE:
# BUILDKITE_BUILD_NUMBER=511 ./ci/scripts/upload-micro-bench-results.sh

# Exits as soon as any line fails.
set -euo pipefail

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
  | jq '.branch' \
  | sed 's/\"//g'
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
  | jq '.commit' \
  | sed 's/\"//g'
}

setup

BUILDKITE_BUILD_URL="https://buildkite.com/risingwavelabs/main-cron/builds/$BUILDKITE_BUILD_NUMBER"
END_DATE=$(get_date)
COMMIT=$(get_commit)
BRANCH=$(get_branch)

curl -L https://rw-qa-infra-public.s3.us-west-2.amazonaws.com/scripts/download-qa.sh | bash

git clone --depth 1 https://"$GITHUB_TOKEN"@github.com/risingwavelabs/qa-infra.git
cp -r qa-infra/certs ./certs
rm -rf qa-infra

echo "--- Uploading results for $BUILDKITE_BUILD_URL"
echo "Commit: $COMMIT"
echo "Branch: $BRANCH"
echo "Date: $END_DATE"

./qa ctl -I 52.207.243.214:8081 execution create-micro-benchmark-executions \
  --exec-url "${BUILDKITE_BUILD_URL}" \
  --branch "$BRANCH" \
  --tag latest \
  --commit "$COMMIT" \
  --end-time "$END_DATE" \
  --status SUCCESS