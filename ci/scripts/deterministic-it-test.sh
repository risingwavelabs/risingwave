#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

echo "--- Download artifacts"
buildkite-agent artifact download simulation-it-test.tar.zst .

echo "--- Extract artifacts"
tar -xvf simulation-it-test.tar.zst
mkdir target/sim
mv target/ci-sim target/sim

echo "--- Run integration tests in deterministic simulation mode"
MADSIM_TEST_SEED=50 NEXTEST_PROFILE=ci-sim \
 cargo nextest run \
 --no-fail-fast \
 --cargo-metadata target/nextest/cargo-metadata.json \
 --binaries-metadata target/nextest/binaries-metadata.json \
 "test_background_mv_barrier_recovery"
