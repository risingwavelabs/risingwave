#!/usr/bin/env bash

set -euo pipefail

source ci/scripts/common.sh

while getopts 'p:' opt; do
    case ${opt} in
        p )
            profile=$OPTARG
            ;;
        \? )
            echo "Invalid Option: -$OPTARG" 1>&2
            exit 1
            ;;
        : )
            echo "Invalid option: $OPTARG requires an argument" 1>&2
            ;;
    esac
done
shift $((OPTIND -1))

download_and_prepare_rw "$profile" source

echo "--- e2e, generic sink test"
RUST_LOG="await_tree::future=error" risedev ci-start ci-sink-test

risedev slt './e2e_test/sink/append_only_sink.slt'
risedev slt './e2e_test/sink/blackhole_sink.slt'
risedev slt './e2e_test/sink/file_sink.slt'
risedev slt './e2e_test/sink/license.slt'
risedev slt './e2e_test/sink/rate_limit.slt'
risedev slt './e2e_test/sink/auto_schema_change.slt'
risedev slt './e2e_test/sink/sink_into_table/*.slt'
risedev slt './e2e_test/sink/sink_vector_columns.slt'
risedev slt './e2e_test/sink/force_compaction_sink.slt'

echo "--- Kill cluster"
risedev ci-kill

echo "--- e2e, ci-1cn-1fe, nexmark endless"
RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info,await_tree::future=error" \
risedev ci-start ci-1cn-1fe
risedev slt './e2e_test/sink/nexmark_endless_mvs/*.slt'
risedev slt './e2e_test/sink/nexmark_endless_sinks/*.slt'

echo "--- Kill cluster"
risedev ci-kill
