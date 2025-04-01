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

download_and_prepare_rw "$profile" common

echo "--- starting risingwave cluster with connector node"
risedev ci-start ci-1cn-1fe

echo "--- Run test"
python3 -m pip install --break-system-packages psycopg2-binary
python3 e2e_test/source_legacy/pulsar/astra-streaming.py
# python3 e2e_test/source_legacy/pulsar/streamnative-cloud.py

echo "--- Kill cluster"
risedev ci-kill
