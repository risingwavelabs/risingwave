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
sleep 1

export STARROCKS_HOST="starrocks-fe-server"
export STARROCKS_HTTP_PORT="8030"
export STARROCKS_QUERY_PORT="9030"
export STARROCKS_CONTAINER="starrocks-fe-server"
export STARROCKS_USER="users"
export STARROCKS_PASSWORD="123456"
export STARROCKS_DATABASE="demo"
export RISEDEV_STARROCKS_WITH_OPTIONS_COMMON="connector='starrocks',starrocks.host='starrocks-fe-server',starrocks.mysqlport='9030',starrocks.httpport='8030',starrocks.user='users',starrocks.password='123456',starrocks.database='demo'"

echo "--- testing sinks"
sqllogictest -p 4566 -d dev './e2e_test/sink/starrocks_sink.slt'

echo "--- Kill cluster"
risedev ci-kill
