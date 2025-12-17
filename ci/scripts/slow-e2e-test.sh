#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

while getopts 'p:m:' opt; do
    case ${opt} in
        p )
            profile=$OPTARG
            ;;
        m )
            mode=$OPTARG
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

source ci/scripts/common.sh

download_and_prepare_rw "$profile" common


echo "--- e2e, $mode, slow tests"
python3 -m pip install --break-system-packages -r e2e_test/udf/remote_python/requirements.txt
RUST_LOG="info" \
risedev ci-start "$mode"
risedev slt -p 4566 -d dev './e2e_test/slow_tests/**/*.slt'

echo "--- Kill cluster"
risedev ci-kill

echo "--- e2e, $mode, backfill"
RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info,risingwave_stream::common::table::state_table=warn" \
risedev ci-start "$mode"

risedev slt -p 4566 -d dev './e2e_test/backfill/sink/different_pk_and_dist_key.slt'

if [[ "$profile" == "ci-release" ]]; then
  # only run in release-mode. It's too slow for dev-mode.
  risedev slt -p 4566 -d dev './e2e_test/backfill/backfill_order_control.slt'
  risedev slt -p 4566 -d dev './e2e_test/backfill/backfill_order_control_recovery.slt'
  risedev slt -p 4566 -d dev './e2e_test/backfill/backfill_progress/test.slt'
fi

echo "--- Kill cluster"
risedev ci-kill
