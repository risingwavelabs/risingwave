#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

echo "--- Download artifacts"
download-and-decompress-artifact risingwave_simulation .
chmod +x ./risingwave_simulation

echo "--- Extract data for Kafka"
pushd ./e2e_test/source_legacy/basic/scripts/
mkdir -p ./test_data
unzip -o test_data.zip -d .
popd

echo "--- Extract data for SqlSmith"
pushd ./src/tests/sqlsmith/tests
git clone https://"$GITHUB_TOKEN"@github.com/risingwavelabs/sqlsmith-query-snapshots.git
# FIXME(kwannoel): Uncomment this to stage changes. Should have a better approach.
# pushd sqlsmith-query-snapshots
# git checkout stage
# popd
popd

export LOGDIR=.risingwave/log

mkdir -p $LOGDIR

echo "--- deterministic simulation e2e, ci-3cn-2fe, batch"
MADSIM_TEST_SEED=1 RUST_LOG='risingwave_frontend::handler::explain_analyze_stream_job=debug' ./risingwave_simulation ./e2e_test/batch/\*\*/\*.slt 2> $LOGDIR/batch-1.log && rm $LOGDIR/batch-1.log