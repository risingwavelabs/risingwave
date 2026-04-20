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

export CLICKHOUSE_HOST="clickhouse-server"
export CLICKHOUSE_PORT="9000"
export CLICKHOUSE_HTTP_PORT="8123"
export CLICKHOUSE_USER="default"
export CLICKHOUSE_PASSWORD="default"
export CLICKHOUSE_DATABASE="default"
export RISEDEV_CLICKHOUSE_HTTP_URL="http://clickhouse-server:8123"
export RISEDEV_CLICKHOUSE_WITH_OPTIONS_COMMON="connector='clickhouse',clickhouse.url='http://clickhouse-server:8123',clickhouse.user='default',clickhouse.password='default',clickhouse.database='default'"

echo "--- testing sinks"
sqllogictest -p 4566 -d dev './e2e_test/sink/clickhouse_sink.slt'

echo "--- Kill cluster"
risedev ci-kill
