#!/usr/bin/env bash

# Exits as soon as any line fails.
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

echo "--- starting risingwave cluster"
risedev ci-start ci-sink-test

export DORIS_HOST="doris-server"
export DORIS_HTTP_PORT="8030"
export DORIS_QUERY_PORT="9030"
export DORIS_CONTAINER="doris-server"
export DORIS_USER="users"
export DORIS_PASSWORD="123456"
export DORIS_DATABASE="demo"
export RISEDEV_DORIS_WITH_OPTIONS_COMMON="connector='doris',doris.url='http://doris-server:8030',doris.user='users',doris.password='123456',doris.database='demo'"

echo "--- testing sinks"
sqllogictest -p 4566 -d dev './e2e_test/sink/doris_sink.slt'

echo "--- Kill cluster"
risedev ci-kill
