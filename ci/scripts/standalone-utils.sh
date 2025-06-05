#!/usr/bin/env bash

set -euo pipefail

export RW_PREFIX=$PWD/.risingwave
export PREFIX_BIN=$RW_PREFIX/bin
export PREFIX_LOG=$RW_PREFIX/log
export PREFIX_DATA=$RW_PREFIX/data
export RW_SQLITE_DB=$PREFIX_DATA/metadata.db

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
        --backend sqlite \
        --sql-endpoint ${RW_SQLITE_DB} \
        --state-store hummock+minio://hummockadmin:hummockadmin@127.0.0.1:9301/hummock001 \
        --data-directory hummock_001 \
        --config-path src/config/ci-single-node-standalone.toml" \
     --compute-opts=" \
        --listen-addr 127.0.0.1:5688 \
        --prometheus-listener-addr 127.0.0.1:1222 \
        --advertise-addr 127.0.0.1:5688 \
        --async-stack-trace verbose \
        --parallelism 4 \
        --total-memory-bytes 8589934592 \
        --role both \
        --meta-address http://127.0.0.1:5690 \
        --config-path src/config/ci-single-node-standalone.toml" \
     --frontend-opts=" \
       --listen-addr 127.0.0.1:4566 \
       --advertise-addr 127.0.0.1:4566 \
       --prometheus-listener-addr 127.0.0.1:2222 \
       --health-check-listener-addr 0.0.0.0:6786 \
       --meta-addr http://127.0.0.1:5690 \
       --config-path src/config/ci-single-node-standalone.toml" >"$1" 2>&1
}

# You can fill up this section by consulting
# .risingwave/log/risedev.log, after calling `risedev d full`.
start_standalone() {
  RUST_BACKTRACE=1 \
  "$PREFIX_BIN"/risingwave/standalone \
     --meta-opts=" \
        --listen-addr 127.0.0.1:5690 \
        --advertise-addr 127.0.0.1:5690 \
        --dashboard-host 127.0.0.1:5691 \
        --prometheus-host 127.0.0.1:1250 \
        --backend sqlite \
        --sql-endpoint ${RW_SQLITE_DB} \
        --state-store hummock+minio://hummockadmin:hummockadmin@127.0.0.1:9301/hummock001 \
        --data-directory hummock_001 \
        --config-path src/config/ci-single-node-standalone.toml" \
     --compute-opts=" \
        --listen-addr 127.0.0.1:5688 \
        --prometheus-listener-addr 127.0.0.1:1222 \
        --advertise-addr 127.0.0.1:5688 \
        --async-stack-trace verbose \
        --parallelism 4 \
        --total-memory-bytes 8589934592 \
        --role both \
        --meta-address http://127.0.0.1:5690 \
        --config-path src/config/ci-single-node-standalone.toml" \
     --frontend-opts=" \
       --listen-addr 127.0.0.1:4566 \
       --advertise-addr 127.0.0.1:4566 \
       --prometheus-listener-addr 127.0.0.1:2222 \
       --health-check-listener-addr 0.0.0.0:6786 \
       --meta-addr http://127.0.0.1:5690 \
       --config-path src/config/ci-single-node-standalone.toml" \
     --compactor-opts=" \
         --listen-addr 127.0.0.1:6660 \
         --prometheus-listener-addr 127.0.0.1:1260 \
         --advertise-addr 127.0.0.1:6660 \
         --meta-address http://127.0.0.1:5690 \
         --config-path src/config/ci-single-node-standalone.toml" >"$1" 2>&1
}

stop_standalone() {
  killall --wait standalone
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
  start_standalone "$PREFIX_LOG"/standalone-restarted.log &
  wait_standalone
}
