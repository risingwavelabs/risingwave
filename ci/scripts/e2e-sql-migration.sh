#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

export RW_PREFIX=$PWD/.risingwave
export RW_PREFIX_DATA=$RW_PREFIX/data

source ci/scripts/common.sh

wait_for_recovery() {
  set +e
  timeout 20s bash -c '
    while true; do
      echo "Polling every 1s to check if the recovery is complete for 20s"
      if psql -h localhost -p 4566 -d dev -U root -c "FLUSH;" </dev/null
      then exit 0;
      else sleep 1;
      fi
    done
  '
  STATUS=$?
  set -e
  if [[ $STATUS -ne 0 ]]; then
    echo "Cluster failed to get recovered: $STATUS"
    exit 1
  else
    echo "Cluster is recovered"
  fi
}

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
sqllogictest -d dev -h localhost -p 4566 './e2e_test/sql_migration/prepare.slt'

echo "--- kill cluster"
cargo make ci-kill

echo "--- restart etcd"
cargo make dev ci-meta-etcd-for-migration

echo "--- run migration"
mkdir -p "${RW_PREFIX_DATA}/sqlite/"
./target/debug/risingwave risectl \
meta \
migration \
--etcd-endpoints localhost:2388 \
--sql-endpoint sqlite://"${RW_PREFIX_DATA}/sqlite/metadata.db"\?mode=rwc \
-f

echo "--- kill etcd"
cargo make ci-kill

echo "--- starting risingwave cluster, meta-1cn-1fe-sqlite"
cargo make dev meta-1cn-1fe-sqlite

echo "--- wait for recovery"
wait_for_recovery

echo "--- run check"
sqllogictest -d dev -h localhost -p 4566 './e2e_test/sql_migration/check.slt'

echo "--- kill cluster"
cargo make kill

echo "--- clean data"
cargo make clean-data

