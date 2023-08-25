#!/usr/bin/env bash

set -euo pipefail

RW_PREFIX=$PWD/.risingwave
PREFIX_BIN=$RW_PREFIX/bin
PREFIX_LOG=$RW_PREFIX/log

start_standalone() {
  RUST_BACKTRACE=1 \
  "$PREFIX_BIN"/risingwave/standalone \
     --meta-opts=" \
        --listen-addr 127.0.0.1:5690 \
        --advertise-addr 127.0.0.1:5690 \
        --dashboard-host 127.0.0.1:5691 \
        --prometheus-host 127.0.0.1:1250 \
        --connector-rpc-endpoint 127.0.0.1:50051 \
        --backend etcd \
        --etcd-endpoints 127.0.0.1:2388 \
        --state-store hummock+minio://hummockadmin:hummockadmin@127.0.0.1:9301/hummock001 \
        --data-directory hummock_001 \
        --dashboard-ui-path $RW_PREFIX/ui" \
     --compute-opts=" \
        --listen-addr 127.0.0.1:5688 \
        --prometheus-listener-addr 127.0.0.1:1222 \
        --advertise-addr 127.0.0.1:5688 \
        --metrics-level 1 \
        --async-stack-trace verbose \
        --connector-rpc-endpoint 127.0.0.1:50051 \
        --parallelism 4 \
        --total-memory-bytes 8589934592 \
        --role both \
        --meta-address http://127.0.0.1:5690" \
     --frontend-opts=" \
       --listen-addr 127.0.0.1:4566 \
       --advertise-addr 127.0.0.1:4566 \
       --prometheus-listener-addr 127.0.0.1:2222 \
       --health-check-listener-addr 127.0.0.1:6786 \
       --metrics-level 1 \
       --meta-addr http://127.0.0.1:5690"
}

stop_standalone() {
  pkill standalone
}

restart_standalone() {
  stop_standalone
  sleep 5
  start_standalone >"$PREFIX_LOG"/standalone-restarted.log 2>&1 &
}