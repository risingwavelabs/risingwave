#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

# prepare environment
export CONNECTOR_LIBS_PATH="./connector-node/libs"

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

echo "--- Download connector node package"
buildkite-agent artifact download risingwave-connector.tar.gz ./
mkdir ./connector-node
tar xf ./risingwave-connector.tar.gz -C ./connector-node

PGPASSWORD='post\tgres' psql -h db -p 5432 -U postgres -c "DROP DATABASE IF EXISTS metadata;" -c "CREATE DATABASE metadata;"

echo "--- starting risingwave cluster"
mkdir -p .risingwave/log
risedev ci-start ci-iceberg-engine
sleep 1


echo "--- testing iceberg engine"
sqllogictest -p 4566 -d dev './e2e_test/iceberg/test_case/iceberg_engine.slt'
sleep 1


echo "--- Kill cluster"
risedev ci-kill
