#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

echo "--- Download artifacts"
buildkite-agent artifact download risingwave_simulation .
chmod +x ./risingwave_simulation

echo "--- Extract data for Kafka"
cd ./scripts/source/
mkdir -p ./test_data
unzip -o test_data.zip -d .
cd ../../

export RUST_LOG=info
export LOGDIR=.risingwave/log

mkdir -p $LOGDIR

echo "--- deterministic simulation e2e, ci-3cn-2fe, fuzzing"
MADSIM_TEST_SEED=6 ./risingwave_simulation --sqlsmith 100 ./src/tests/sqlsmith/tests/testdata
