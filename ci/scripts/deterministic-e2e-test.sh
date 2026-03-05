#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

TARGET_RELEASE_BRANCH="release-2.6"
IS_TARGET_RELEASE_BRANCH=0
if [[ "${BUILDKITE_BRANCH:-}" == "${TARGET_RELEASE_BRANCH}" || "${BUILDKITE_PULL_REQUEST_BASE_BRANCH:-}" == "${TARGET_RELEASE_BRANCH}" ]]; then
  IS_TARGET_RELEASE_BRANCH=1
fi

if [[ "${IS_TARGET_RELEASE_BRANCH}" == "1" && "${RW_CI_ENABLE_RELEASE_2_6_DETERMINISTIC_E2E:-0}" != "1" ]]; then
  echo "--- skip deterministic simulation e2e, ci-3cn-2fe on ${TARGET_RELEASE_BRANCH}"
  echo "Reason: temporarily disabled on ${TARGET_RELEASE_BRANCH} due to test stability concerns."
  echo "Set RW_CI_ENABLE_RELEASE_2_6_DETERMINISTIC_E2E=1 to re-enable."
  exit 0
fi

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

echo "--- deterministic simulation e2e, ci-3cn-2fe, ddl"
seq "$TEST_NUM" | parallel 'MADSIM_TEST_SEED={} ./risingwave_simulation ./e2e_test/ddl/\*\*/\*.slt 2> $LOGDIR/ddl-{}.log && rm $LOGDIR/ddl-{}.log'

echo "--- deterministic simulation e2e, ci-3cn-2fe, streaming"
seq "$TEST_NUM" | parallel 'MADSIM_TEST_SEED={} ./risingwave_simulation ./e2e_test/streaming/\*\*/\*.slt 2> $LOGDIR/streaming-{}.log && rm $LOGDIR/streaming-{}.log'

echo "--- deterministic simulation e2e, ci-3cn-2fe, batch"
seq "$TEST_NUM" | parallel 'MADSIM_TEST_SEED={} ./risingwave_simulation ./e2e_test/batch/\*\*/\*.slt 2> $LOGDIR/batch-{}.log && rm $LOGDIR/batch-{}.log'

echo "--- deterministic simulation e2e, ci-3cn-2fe, kafka source"
seq "$TEST_NUM" | parallel 'MADSIM_TEST_SEED={} ./risingwave_simulation --kafka-datadir=./e2e_test/source_legacy/basic/scripts/test_data ./e2e_test/source_legacy/basic/kafka\*.slt 2> $LOGDIR/source-{}.log && rm $LOGDIR/source-{}.log'

echo "--- deterministic simulation e2e, ci-3cn-2fe, parallel, streaming"
seq "$TEST_NUM" | parallel 'MADSIM_TEST_SEED={} ./risingwave_simulation -j 16 ./e2e_test/streaming/\*\*/\*.slt 2> $LOGDIR/parallel-streaming-{}.log && rm $LOGDIR/parallel-streaming-{}.log'

echo "--- deterministic simulation e2e, ci-3cn-2fe, parallel, batch"
seq "$TEST_NUM" | parallel 'MADSIM_TEST_SEED={} ./risingwave_simulation -j 16 ./e2e_test/batch/\*\*/\*.slt 2> $LOGDIR/parallel-batch-{}.log && rm $LOGDIR/parallel-batch-{}.log'

if [[ "${IS_TARGET_RELEASE_BRANCH}" != "1" || "${RW_CI_ENABLE_PREGENERATED_SQLSMITH_FUZZING:-0}" == "1" ]]; then
  echo "--- deterministic simulation e2e, ci-3cn-2fe, fuzzing (pre-generated-queries)"
  timeout 10m seq 64 | parallel RUST_MIN_STACK=4194304 'MADSIM_TEST_SEED={} ./risingwave_simulation --run-sqlsmith-queries ./src/tests/sqlsmith/tests/sqlsmith-query-snapshots/{} 2> $LOGDIR/fuzzing-{}.log && rm $LOGDIR/fuzzing-{}.log'
else
  echo "--- skip deterministic simulation e2e, ci-3cn-2fe, fuzzing (pre-generated-queries)"
  echo "Reason: temporarily disabled on ${TARGET_RELEASE_BRANCH} due to test stability concerns."
  echo "Set RW_CI_ENABLE_PREGENERATED_SQLSMITH_FUZZING=1 to re-enable."
fi

echo "--- deterministic simulation e2e, ci-3cn-2fe, e2e extended mode test"
seq "$TEST_NUM" | parallel 'MADSIM_TEST_SEED={} RUST_LOG=info ./risingwave_simulation -e 2> $LOGDIR/extended-{}.log && rm $LOGDIR/extended-{}.log'
