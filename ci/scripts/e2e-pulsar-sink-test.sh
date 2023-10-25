#!/usr/bin/env bash

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

echo "--- starting risingwave cluster"
cargo make ci-start ci-pulsar-test
sleep 1

echo "--- waiting until pulsar is healthy"
HTTP_CODE=404
MAX_RETRY=20
while [[ $HTTP_CODE -ne 200 && MAX_RETRY -gt 0 ]]
do
    HTTP_CODE=$(curl --connect-timeout 2 -s -o /dev/null -w ''%{http_code}'' http://pulsar:8080/admin/v2/clusters)
    ((MAX_RETRY--))
    sleep 5
done

# Exits as soon as any line fails.
set -euo pipefail

echo "--- testing pulsar sink"
sqllogictest -p 4566 -d dev './e2e_test/sink/pulsar_sink.slt'

sleep 1

echo "--- Kill cluster"
cargo make ci-kill