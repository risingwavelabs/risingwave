#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

export LOGDIR=.risingwave/log
mkdir -p $LOGDIR

echo "--- Download artifacts"
buildkite-agent artifact download simulation-it-test.tar.zst .

echo "--- Extract artifacts"
tar -xvf simulation-it-test.tar.zst
mkdir target/sim
mv target/ci-sim target/sim

TEST_PATTERN="$@"

echo "--- Run integration tests in deterministic simulation mode"
seq "$TEST_NUM" | parallel -j 8 --line-buffer "MADSIM_TEST_SEED={} NEXTEST_PROFILE=ci-sim RUST_LOG='risingwave_stream::executor::sync_kv_log_store=trace,integration_tests::log_store::scale=info,risingwave_stream::common::log_store_impl::kv_log_store=trace' \
 cargo nextest run \
 $NEXTEST_PARTITION_ARG \
 --no-fail-fast \
 --cargo-metadata target/nextest/cargo-metadata.json \
 --binaries-metadata target/nextest/binaries-metadata.json \
 test_scale_in_synced_log_store > $LOGDIR/test_scale_in_synced_log_store_{}.log 2>&1"