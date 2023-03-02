#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh
source ci/scripts/pr.env.sh

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

function run_parallel_jobs {
  # Limit the number of parallel jobs to avoid OOM.
  # See https://github.com/risingwavelabs/risingwave/issues/7561
  MAX_JOBS=32
  BATCHES=$(( (TEST_NUM + MAX_JOBS - 1) / MAX_JOBS ))

  # Run the jobs in batches
  for (( i=0; i<$BATCHES; i++ )); do
    start=$((i * MAX_JOBS + 1))
    end=$(( (i + 1) * MAX_JOBS ))
    if [[ $end -gt $TEST_NUM ]]; then
      end=$TEST_NUM
    fi
    seq $start $end | parallel MADSIM_TEST_SEED={} $1
  done
}

echo "--- deterministic simulation e2e, ci-3cn-2fe, ddl"
run_parallel_jobs './risingwave_simulation ./e2e_test/ddl/\*\*/\*.slt 2> $LOGDIR/ddl-{}.log && rm $LOGDIR/ddl-{}.log'

echo "--- deterministic simulation e2e, ci-3cn-2fe, streaming"
run_parallel_jobs './risingwave_simulation ./e2e_test/streaming/\*\*/\*.slt 2> $LOGDIR/streaming-{}.log && rm $LOGDIR/streaming-{}.log'

echo "--- deterministic simulation e2e, ci-3cn-2fe, batch"
run_parallel_jobs './risingwave_simulation ./e2e_test/batch/\*\*/\*.slt 2> $LOGDIR/batch-{}.log && rm $LOGDIR/batch-{}.log'

echo "--- deterministic simulation e2e, ci-3cn-2fe, kafka source"
run_parallel_jobs './risingwave_simulation --kafka-datadir=./scripts/source/test_data ./e2e_test/source/basic/kafka\*.slt 2> $LOGDIR/source-{}.log && rm $LOGDIR/source-{}.log'

echo "--- deterministic simulation e2e, ci-3cn-2fe, parallel, streaming"
run_parallel_jobs './risingwave_simulation -j 16 ./e2e_test/streaming/\*\*/\*.slt 2> $LOGDIR/parallel-streaming-{}.log && rm $LOGDIR/parallel-streaming-{}.log'

echo "--- deterministic simulation e2e, ci-3cn-2fe, parallel, batch"
run_parallel_jobs './risingwave_simulation -j 16 ./e2e_test/batch/\*\*/\*.slt 2> $LOGDIR/parallel-batch-{}.log && rm $LOGDIR/parallel-batch-{}.log'

echo "--- deterministic simulation e2e, ci-3cn-2fe, fuzzing (pre-generated-queries)"
run_parallel_jobs './risingwave_simulation  --run-sqlsmith-queries ./src/tests/sqlsmith/tests/freeze/{} 2> $LOGDIR/fuzzing-{}.log && rm $LOGDIR/fuzzing-{}.log'
