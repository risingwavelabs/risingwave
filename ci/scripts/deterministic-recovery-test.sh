#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.env.sh

echo "--- Download artifacts"
buildkite-agent artifact download risingwave_simulation .
chmod +x ./risingwave_simulation

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

echo "--- deterministic simulation e2e, ci-3cn-2fe-3meta, recovery, streaming"
run_parallel_jobs './risingwave_simulation --kill --kill-rate=${KILL_RATE} ./e2e_test/streaming/\*\*/\*.slt 2> $LOGDIR/recovery-streaming-{}.log && rm $LOGDIR/recovery-streaming-{}.log'

echo "--- deterministic simulation e2e, ci-3cn-2fe-3meta, recovery, batch"
run_parallel_jobs './risingwave_simulation --kill --kill-rate=${KILL_RATE} ./e2e_test/batch/\*\*/\*.slt 2> $LOGDIR/recovery-batch-{}.log && rm $LOGDIR/recovery-batch-{}.log'
