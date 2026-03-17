#!/usr/bin/env bash

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
            exit 1
            ;;
    esac
done
shift $((OPTIND -1))

source ci/scripts/common.sh

download_and_prepare_rw "$profile" common

echo "--- e2e, $mode, refill"
RUST_LOG="info,risingwave_storage=info" \
risedev ci-start "$mode"

risedev slt -p 4566 -d dev './e2e_test/refill/table_refill.slt'

echo "--- Kill cluster"
risedev ci-kill
