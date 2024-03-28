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

download_and_prepare_rw "$profile" common

echo "--- starting risingwave cluster, ci-1cn-1fe-with-recovery"
RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info" \
cargo make ci-start ci-1cn-1fe-with-recovery

echo "--- init cluster with some data & DDL"
sqllogictest -h localhost -p 4565 -d dev './e2e_test/sql_migration/prepare.slt'

echo "--- kill cluster"
cargo make ci-kill

echo "--- restart etcd"
cargo make dev ci-meta-etcd-for-migration

echo "--- run migration"
mkdir -p "${PREFIX_DATA}/sqlite/"
./target/debug/risectl \
meta \
migration \
--etcd-endpoints localhost:2388 \
--sql-endpoint sqlite://"${PREFIX_DATA}/sqlite/metadata.db"\?mode=rwc \
-f

echo "--- kill etcd"
cargo make ci-kill

echo "--- starting risingwave cluster, meta-1cn-1fe-sqlite"
cargo make dev meta-1cn-1fe-sqlite

echo "--- run check"
sqllogictest -h localhost -p 4565 -d dev './e2e_test/sql_migration/check.slt'

echo "--- kill cluster"
cargo make kill

echo "--- clean data"
cargo make clean-data

