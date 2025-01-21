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

echo "--- deterministic simulation e2e, ci-3cn-2fe, parallel, streaming"
seq 9 9 | parallel 'MADSIM_TEST_SEED={} ./risingwave_simulation -j 16 ./e2e_test/streaming/\*\*/\*.slt 2> $LOGDIR/parallel-streaming-{}.log && rm $LOGDIR/parallel-streaming-{}.log'

