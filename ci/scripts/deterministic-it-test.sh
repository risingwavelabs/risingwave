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

# NOTE(kwannoel): for TEST_PATTERN, please consult: https://nexte.st/docs/filtersets/reference/.
# Using substring matching may run unexpected tests.
# For example, `scale::` will run all tests with a `scale` module prefix.
# This includes sink::scale::*, scale::*.
# We just want to run `scale::*`.

echo "--- Run integration tests in deterministic simulation mode"
seq "$TEST_NUM" | parallel -j 8 --line-buffer "MADSIM_TEST_SEED={} NEXTEST_PROFILE=ci-sim \
 cargo nextest run \
 $NEXTEST_PARTITION_ARG \
 --no-fail-fast \
 --cargo-metadata target/nextest/cargo-metadata.json \
 --binaries-metadata target/nextest/binaries-metadata.json \
 $TEST_PATTERN"
