#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

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
            ;;
    esac
done
shift $((OPTIND -1))

if [[ $mode == "standalone" ]]; then
  source ci/scripts/standalone-utils.sh
fi

if [[ $mode == "single-node" ]]; then
  source ci/scripts/single-node-utils.sh
fi

cluster_start() {
  if [[ $mode == "standalone" ]]; then
    mkdir -p "$PREFIX_LOG"
    risedev clean-data
    risedev pre-start-dev
    start_standalone "$PREFIX_LOG"/standalone.log &
    risedev dev standalone-minio-sqlite
  elif [[ $mode == "single-node" ]]; then
    mkdir -p "$PREFIX_LOG"
    risedev clean-data
    risedev pre-start-dev
    start_single_node "$PREFIX_LOG"/single-node.log &
    # Give it a while to make sure the single-node is ready.
    sleep 3
  else
    risedev ci-start "$mode"
  fi
}

cluster_stop() {
  if [[ $mode == "standalone" ]]
  then
    stop_standalone
    # Don't check standalone logs, they will exceed the limit.
    risedev kill
  elif [[ $mode == "single-node" ]]
  then
    stop_single_node
  else
    risedev ci-kill
  fi
}

download_and_prepare_rw "$profile" common

echo "--- e2e, ci-meta-backup-test"
test_root="src/storage/backup/integration_tests"
BACKUP_TEST_MCLI=".risingwave/bin/mcli" \
BACKUP_TEST_MCLI_CONFIG=".risingwave/config/mcli" \
BACKUP_TEST_RW_ALL_IN_ONE="target/debug/risingwave" \
RW_HUMMOCK_URL="hummock+minio://hummockadmin:hummockadmin@127.0.0.1:9301/hummock001" \
RW_META_ADDR="http://127.0.0.1:5690" \
RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info" \
META_STORE_TYPE="sql" \
RW_SQLITE_DB=".risingwave/data/sqlite/metadata.db" \
bash "${test_root}/run_all.sh"
echo "--- Kill cluster"
risedev kill
