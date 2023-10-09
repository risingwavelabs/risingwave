#!/usr/bin/env bash

set -euo pipefail

export RW_PREFIX=$PWD/.risingwave
export PREFIX_BIN=$RW_PREFIX/bin
export PREFIX_LOG=$RW_PREFIX/log

# NOTE(kwannoel): Compared to start_standalone below, we omitted the compactor-opts,
# so it should not start.
start_standalone_without_compactor() {
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
       --meta-addr http://127.0.0.1:5690" >"$1" 2>&1
}

# You can fill up this section by consulting
# .risingwave/log/risedev.log, after calling ./risedev d full.
# It is expected that minio, etcd will be started after this is called.
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
       --meta-addr http://127.0.0.1:5690" \
     --compactor-opts=" \
         --listen-addr 127.0.0.1:6660 \
         --prometheus-listener-addr 127.0.0.1:1260 \
         --advertise-addr 127.0.0.1:6660 \
         --meta-address http://127.0.0.1:5690" >"$1" 2>&1
}

stop_standalone() {
  pkill standalone
}

wait_standalone() {
  set +e
  timeout 20s bash -c '
    while true; do
      echo "Polling every 1s for standalone to be ready for 20s"
      if psql -h localhost -p 4566 -d dev -U root -c "SELECT 1;" </dev/null
      then exit 0;
      else sleep 1;
      fi
    done
  '
  STATUS=$?
  set -e
  if [[ $STATUS -ne 0 ]]; then
    echo "Standalone failed to start with status: $STATUS"
    exit 1
  else
    echo "Standalone is ready"
  fi
}

restart_standalone() {
  stop_standalone
  sleep 5
  start_standalone "$PREFIX_LOG"/standalone-restarted.log &
  wait_standalone
}
