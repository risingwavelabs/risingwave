#!/usr/bin/env bash

set -euo pipefail

RW_PREFIX=$PWD/.risingwave
PREFIX_BIN=$RW_PREFIX/bin
PREFIX_LOG=$RW_PREFIX/log
PREFIX_DATA=$RW_PREFIX/data
RW_SQLITE_DB=$PREFIX_DATA/metadata.db

start_standalone() {
  RUST_BACKTRACE=1 \
  cargo run -p risingwave_cmd_all \
            --profile "${RISINGWAVE_BUILD_PROFILE}" \
            ${RISINGWAVE_FEATURE_FLAGS} \
            -- standalone \
                 --meta-opts=" \
                    --listen-addr 127.0.0.1:5690 \
                    --advertise-addr 127.0.0.1:5690 \
                    --dashboard-host 127.0.0.1:5691 \
                    --prometheus-host 127.0.0.1:1250 \
                    --backend sqlite \
                    --sql-endpoint ${RW_SQLITE_DB} \
                    --state-store hummock+minio://hummockadmin:hummockadmin@127.0.0.1:9301/hummock001 \
                    --data-directory hummock_001 \
                    --config-path src/config/standalone-example.toml" \
                 --compute-opts=" \
                    --config-path src/config/standalone-example.toml \
                    --listen-addr 127.0.0.1:5688 \
                    --prometheus-listener-addr 127.0.0.1:1222 \
                    --advertise-addr 127.0.0.1:5688 \
                    --async-stack-trace verbose \
                    --parallelism 4 \
                    --total-memory-bytes 8589934592 \
                    --role both \
                    --meta-address http://127.0.0.1:5690" \
                 --frontend-opts=" \
                   --config-path src/config/standalone-example.toml \
                   --listen-addr 127.0.0.1:4566 \
                   --advertise-addr 127.0.0.1:4566 \
                   --prometheus-listener-addr 127.0.0.1:2222 \
                   --health-check-listener-addr 0.0.0.0:6786 \
                   --meta-addr http://127.0.0.1:5690" \
                 --compactor-opts=" \
                   --listen-addr 127.0.0.1:6660 \
                   --prometheus-listener-addr 127.0.0.1:1260 \
                   --advertise-addr 127.0.0.1:6660 \
                   --meta-address http://127.0.0.1:5690"
}

start_standalone
