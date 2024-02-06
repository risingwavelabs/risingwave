#!/usr/bin/env bash

set -euo pipefail

export RW_PREFIX=$PWD/.risingwave
export PREFIX_BIN=./target/debug
export PREFIX_LOG=$RW_PREFIX/log

# You can fill up this section by consulting
# .risingwave/log/risedev.log, after calling ./risedev d full.
# It is expected that minio, etcd will be started after this is called.
start_single_node() {
  mkdir -p "$HOME/.risingwave/state_store"
  mkdir -p "$HOME/.risingwave/meta_store"
  RUST_BACKTRACE=1 "$PREFIX_BIN"/risingwave >"$1" 2>&1
}

stop_single_node() {
  pkill risingwave
}

wait_single_node() {
  set +e
  timeout 20s bash -c '
    while true; do
      echo "Polling every 1s for single_node to be ready for 20s"
      if psql -h localhost -p 4566 -d dev -U root -c "SELECT 1;" </dev/null
      then exit 0;
      else sleep 1;
      fi
    done
  '
  STATUS=$?
  set -e
  if [[ $STATUS -ne 0 ]]; then
    echo "Single node failed to start with status: $STATUS"
    exit 1
  else
    echo "Single node is ready"
  fi
}

restart_single_node() {
  stop_single_node
  sleep 5
  start_single_node "$PREFIX_LOG"/single-node-restarted.log &
  wait_single_node
}
