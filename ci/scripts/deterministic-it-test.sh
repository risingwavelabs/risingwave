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
seq $TEST_NUM | parallel "MADSIM_TEST_SEED={} NEXTEST_PROFILE=ci-sim \
 cargo nextest run \
 --no-fail-fast \
 --cargo-metadata target/nextest/cargo-metadata.json \
 --binaries-metadata target/nextest/binaries-metadata.json \
 $TEST_PATTERN 1>$LOGDIR/deterministic-it-test-{}.log 2>&1 && rm $LOGDIR/deterministic-it-test-{}.log"
