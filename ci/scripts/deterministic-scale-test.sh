#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

echo "--- Download artifacts"
buildkite-agent artifact download scale-test.tar.zst .

echo "--- Extract artifacts"
apt install zstd
tar -xvf scale-test.tar.zst
mkdir target/sim
mv target/ci-sim target/sim

echo "--- Run scaling tests in deterministic simulation mode"
seq $TEST_NUM | parallel MADSIM_TEST_SEED={} NEXTEST_PROFILE=ci-scaling cargo nextest run --no-fail-fast --cargo-metadata target/nextest/cargo-metadata.json --binaries-metadata target/nextest/binaries-metadata.json
