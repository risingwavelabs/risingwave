#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

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

source ci/scripts/common.sh

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
    risedev dev standalone-minio-sqlite &
    PID=$!
    sleep 1
    start_standalone "$PREFIX_LOG"/standalone.log &
    wait $PID
  elif [[ $mode == "single-node" ]]; then
    mkdir -p "$PREFIX_LOG"
    risedev clean-data
    risedev pre-start-dev
    start_single_node "$PREFIX_LOG"/single-node.log &
    # Give it a while to make sure the single-node is ready.
    sleep 30
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

echo "--- Download artifacts"
# preparing for external java udf tests
mkdir -p e2e_test/udf/java/target/
buildkite-agent artifact download udf.jar e2e_test/udf/java/target/
# preparing for extended mode tests
download-and-decompress-artifact risingwave_e2e_extended_mode_test-"$profile" target/debug/
mv target/debug/risingwave_e2e_extended_mode_test-"$profile" target/debug/risingwave_e2e_extended_mode_test
chmod +x ./target/debug/risingwave_e2e_extended_mode_test

echo "--- e2e, $mode, streaming"
RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info,risingwave_stream::common::table::state_table=warn" \
cluster_start
for i in {1..100}; do
  risedev slt -p 4566 -d dev './e2e_test/backfill/sink/different_pk_and_dist_key.slt'
done

echo "--- Kill cluster"
cluster_stop

echo "--- Upload JUnit test results"
buildkite-agent artifact upload "*-junit.xml"