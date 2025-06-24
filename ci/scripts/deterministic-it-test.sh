#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

export RUST_LOG="risingwave_stream::executor::sync_kv_log_store=trace,integration_tests::log_store::scale=info,risingwave_stream::common::log_store_impl::kv_log_store=trace"
export LOGDIR=.risingwave/log
mkdir -p $LOGDIR

echo "--- Download artifacts"
buildkite-agent artifact download simulation-it-test.tar.zst .

echo "--- Extract artifacts"
tar -xvf simulation-it-test.tar.zst
mkdir target/sim
mv target/ci-sim target/sim

if [[ -z "${1:-}" ]]; then
  TEST_PATTERN=""
elif [[ "$1" == "pull-request" ]]; then
  # exclude logstore tests
  TEST_PATTERN="-E 'not test(/^log_store/)'"
else
  TEST_PATTERN="-E 'test(/^$1/)'"
fi

# NOTE(kwannoel): for TEST_PATTERN, please consult: https://nexte.st/docs/filtersets/reference/.
# Using substring matching may run unexpected tests.
# For example, `scale::` will run all tests with a `scale` module prefix.
# This includes sink::scale::*, scale::*.
# We just want to run `scale::*`.

echo "--- Run integration tests in deterministic simulation mode"
MADSIM_TEST_SEED=13 NEXTEST_PROFILE=ci-sim \
 cargo nextest run \
 $NEXTEST_PARTITION_ARG \
 --no-fail-fast \
 --cargo-metadata target/nextest/cargo-metadata.json \
 --binaries-metadata target/nextest/binaries-metadata.json \
 -E 'test(/^log_store/)' \
 2> $LOGDIR/it-test-13.log && rm $LOGDIR/it-test-13.log