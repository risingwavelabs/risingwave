#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

echo "--- Download artifacts"
download-and-decompress-artifact risingwave_simulation .
chmod +x ./risingwave_simulation

export RUST_LOG="info,risingwave_meta::barrier::recovery=debug,\
risingwave_meta::manager::catalog=debug,\
risingwave_meta::rpc::ddl_controller=debug,\
risingwave_meta::barrier::mod=debug,\
risingwave_simulation=debug,\
risingwave_meta::stream::stream_manager=debug,\
risingwave_meta::barrier::progress=debug,\
sqlx=error,\
risingwave_storage::hummock::compactor=error"

# Extra logs you can enable if the existing trace does not give enough info.
#risingwave_stream::executor::backfill=trace,
#risingwave_meta::barrier::progress=debug
#risingwave_meta::controller::streaming_job=trace

# ========= Some tips for debugging recovery tests =========
# 1. If materialized view failed to create after multiple retries
#    - Check logs to see where the materialized view creation was stuck.
#      1. Is it stuck at waiting for backfill executor response?
#         In that case perhaps some backfill logic is flawed, add more trace in backfill to debug.
#      2. Is it stuck at waiting for backfill executor to finish?
#         In that case perhaps some part of the backfill loop is slow / stuck.
#      3. Is it stuck at waiting for some executor to report progress?
#         In that case perhaps the tracking of backfill's progress in meta is flawed.

export LOGDIR=.risingwave/log

mkdir -p $LOGDIR

filter_stack_trace_for_all_logs() {
  # Defined in `common.sh`
  for log in "${LOGDIR}"/*.log; do
    filter_stack_trace "$log"
  done
}

# trap filter_stack_trace_for_all_logs ERR

export EXTRA_ARGS="${EXTRA_ARGS:-}"
if [[ -n "${USE_ARRANGEMENT_BACKFILL:-}" ]]; then
  export EXTRA_ARGS="$EXTRA_ARGS --use-arrangement-backfill"
fi

echo "--- EXTRA_ARGS: ${EXTRA_ARGS}"

echo "--- deterministic simulation e2e, ci-3cn-2fe-1meta, recovery, background_ddl"
seq "$TEST_NUM" | parallel --ungroup 'set -o pipefail && ((MADSIM_TEST_SEED={} ./risingwave_simulation \
--kill \
--kill-rate=${KILL_RATE} \
${EXTRA_ARGS:-} \
./e2e_test/background_ddl/sim/basic.slt \
2> $LOGDIR/recovery-background-ddl-{}.log && rm $LOGDIR/recovery-background-ddl-{}.log) \
| awk -W interactive "{print \"(seed = {}): \" \$0; fflush()}")'

echo "--- deterministic simulation e2e, ci-3cn-2fe-1meta, recovery, ddl"
seq "$TEST_NUM" | parallel --tmpdir .risingwave --ungroup 'set -o pipefail && ((MADSIM_TEST_SEED={} ./risingwave_simulation \
--kill \
--kill-rate=${KILL_RATE} \
--background-ddl-rate=${BACKGROUND_DDL_RATE} \
${EXTRA_ARGS:-} \
./e2e_test/ddl/\*\*/\*.slt 2> $LOGDIR/recovery-ddl-{}.log && rm $LOGDIR/recovery-ddl-{}.log) \
| awk -W interactive "{print \"(seed = {}): \" \$0; fflush()}")'

echo "--- deterministic simulation e2e, ci-3cn-2fe-1meta, recovery, streaming"
seq "$TEST_NUM" | parallel --ungroup 'set -o pipefail && ((MADSIM_TEST_SEED={} ./risingwave_simulation \
--kill \
--kill-rate=${KILL_RATE} \
--background-ddl-rate=${BACKGROUND_DDL_RATE} \
${EXTRA_ARGS:-} \
./e2e_test/streaming/\*\*/\*.slt 2> $LOGDIR/recovery-streaming-{}.log && rm $LOGDIR/recovery-streaming-{}.log) \
| awk -W interactive "{print \"(seed = {}): \" \$0; fflush()}")'

echo "--- deterministic simulation e2e, ci-3cn-2fe-1meta, recovery, batch"
seq "$TEST_NUM" | parallel --ungroup 'set -o pipefail && ((MADSIM_TEST_SEED={} ./risingwave_simulation \
--kill \
--kill-rate=${KILL_RATE} \
--background-ddl-rate=${BACKGROUND_DDL_RATE} \
${EXTRA_ARGS:-} \
./e2e_test/batch/\*\*/\*.slt 2> $LOGDIR/recovery-batch-{}.log && rm $LOGDIR/recovery-batch-{}.log) \
| awk -W interactive "{print \"(seed = {}): \" \$0; fflush()}")'

echo "--- deterministic simulation e2e, ci-3cn-2fe-1meta, recovery, kafka source,sink"
seq "$TEST_NUM" | parallel --ungroup 'set -o pipefail && ((MADSIM_TEST_SEED={} ./risingwave_simulation \
--kill \
--kill-rate=${KILL_RATE} \
--kafka-datadir=./e2e_test/source_legacy/basic/scripts/test_data \
${EXTRA_ARGS:-} \
./e2e_test/source_legacy/basic/kafka\*.slt 2> $LOGDIR/recovery-source-{}.log && rm $LOGDIR/recovery-source-{}.log) \
| awk -W interactive "{print \"(seed = {}): \" \$0; fflush()}")'
