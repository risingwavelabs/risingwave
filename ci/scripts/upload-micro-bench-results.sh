#!/usr/bin/env bash

# EXAMPLE USAGE:
: '
AWS_PROFILE=rwctest \
BUILDKITE_BUILD_NUMBER=511 \
./ci/scripts/upload-micro-bench-results.sh
'

# Exits as soon as any line fails.
set -euo pipefail

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

BUILDKITE_BUILD_URL="https://buildkite.com/risingwavelabs/main-cron/builds/$BUILDKITE_BUILD_NUMBER"
END_DATE=$(get_date)
COMMIT=$(get_commit)
BRANCH=$(get_branch)

#echo "--- Install Necessary Tools"
## pip3 install toml-cli
## download qa files for use in nexmark-bench.
#if [[ ! -f "./qa" ]]; then
#  curl -L https://rw-qa-infra-public.s3.us-west-2.amazonaws.com/scripts/download-qa.sh | bash && ./qa --help
#fi
#
#echo "--- AWS and Kube Config"
#set +u
#if [[ -z "$AWS_PROFILE" ]]; then
#  # FIXME(kwannoel): Ask @huangjw to help with configuring it.
#  aws configure set aws_access_key_id $RWCTEST_ACCESS_KEY
#  aws configure set aws_secret_access_key $RWCTEST_SECRET_KEY
#fi
#set -u

# Get certs for command client from s3.
# Use by qa command.
#echo "--- Download S3 Crt Keys"
#if [[ ! -d "./certs" ]]; then
#  aws s3 cp s3://rw-qa-infra/client-certs ./certs --recursive
#fi

# Use this command to create the execution.
# For microbench mark it need not be some complex.
# There will be another command.
# https://github.com/risingwavelabs/qa-infra/pull/35
# Can add --tag and --commit.
# 2 most important are the url and the time.
# Can see "BUILDKITE_BUILD_URL" below.
# We will fetch the artifacts from the URL above.
# Get the url from buildkite pipeline environment variable.
# End time can set to NOW. Obtain from buildkite? Or just see in nexmark-bench script how JW sets the time.
# See the PR docs.
# Add this script to RW repository buildkite ci.
# For pipeline settings:
# https://buildkite.com/risingwave-test/nexmark-benchmark
./bin/qa ctl -I 52.207.243.214:8081 execution create-micro-benchmark-executions \
  --exec-url ${BUILDKITE_BUILD_URL} \
  --branch "$BRANCH" \
  --tag latest \
  --commit "$COMMIT" \
  --end-time "$END_DATE" \
  --status SUCCESS