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

echo "--- Download connector node package"
buildkite-agent artifact download risingwave-connector.tar.gz ./
mkdir ./connector-node
tar xf ./risingwave-connector.tar.gz -C ./connector-node
export CONNECTOR_LIBS_PATH="./connector-node/libs"

echo "--- starting risingwave cluster"
PGPASSWORD=postgres psql -h db -p 5432 -U postgres -c "DROP DATABASE IF EXISTS metadata;" -c "CREATE DATABASE metadata;"
risedev ci-start ci-iceberg-test


unset BUILDKITE_PARALLEL_JOB
unset BUILDKITE_PARALLEL_JOB_COUNT


echo "--- Running tests"
cd e2e_test/iceberg
# Don't remove the `--quiet` option since poetry has a bug when printing output, see
# https://github.com/python-poetry/poetry/issues/3412
poetry update --quiet
poetry run python main.py

# echo "--- Running pure slt tests"
# risedev slt './e2e_test/iceberg/test_case/pure_slt/*.slt'

# Run benchmarks separately (not parallelized)
# echo "--- Running benchmarks"
# poetry run python main.py -t ./benches/predicate_pushdown.toml
