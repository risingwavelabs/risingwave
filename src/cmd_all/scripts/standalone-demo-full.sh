#!/usr/bin/env bash

# set -euo pipefail

RW_PREFIX=$PWD/.risingwave
PREFIX_BIN=$RW_PREFIX/bin
PREFIX_LOG=$RW_PREFIX/log

start_minio() {
  MINIO_CI_CD=1 \
  MINIO_PROMETHEUS_AUTH_TYPE=public \
  MINIO_PROMETHEUS_URL=http://127.0.0.1:9500 \
  MINIO_ROOT_PASSWORD=hummockadmin \
  MINIO_ROOT_USER=hummockadmin \
  "$PREFIX_BIN"/minio \
  server \
  --address \
  127.0.0.1:9301 \
  --console-address \
  127.0.0.1:9400 \
  --config-dir \
  "$RW_PREFIX"/config/minio \
  "$RW_PREFIX"/data/minio
}

configure_minio() {
  sleep 5 # wait for minio to start
  "$PREFIX_BIN"/mcli -C "$RW_PREFIX"/config/mcli \
    alias set hummock-minio http://127.0.0.1:9301 \
    hummockadmin \
    hummockadmin

  "$PREFIX_BIN"/mcli -C "$RW_PREFIX"/config/mcli mb hummock-minio/hummock001
}

start_etcd() {
    "$PREFIX_BIN"/etcd/etcd \
    --listen-client-urls \
    http://127.0.0.1:2388 \
    --advertise-client-urls \
    http://127.0.0.1:2388 \
    --listen-peer-urls \
    http://127.0.0.1:2389 \
    --initial-advertise-peer-urls \
    http://127.0.0.1:2389 \
    --listen-metrics-urls \
    http://127.0.0.1:2379 \
    --max-txn-ops \
    999999 \
    --max-request-bytes \
    10485760 \
    --auto-compaction-mode \
    periodic \
    --auto-compaction-retention \
    1m \
    --snapshot-count \
    10000 \
    --name \
    etcd-2388 \
    --initial-cluster-token \
    risingwave-etcd \
    --initial-cluster-state \
    new \
    --initial-cluster \
    etcd-2388=http://127.0.0.1:2389 \
    --data-dir \
    "$RW_PREFIX"/data/etcd-2388
}

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
                    --connector-rpc-endpoint 127.0.0.1:50051 \
                    --backend etcd \
                    --etcd-endpoints 127.0.0.1:2388 \
                    --state-store hummock+minio://hummockadmin:hummockadmin@127.0.0.1:9301/hummock001 \
                    --data-directory hummock_001 \
                    --config-path src/config/standalone-example.toml \
                    --dashboard-ui-path $RW_PREFIX/ui" \
                 --compute-opts=" \
                    --config-path src/config/standalone-example.toml \
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
                   --config-path src/config/standalone-example.toml \
                   --listen-addr 127.0.0.1:4566 \
                   --advertise-addr 127.0.0.1:4566 \
                   --prometheus-listener-addr 127.0.0.1:2222 \
                   --health-check-listener-addr 127.0.0.1:6786 \
                   --metrics-level 1 \
                   --meta-addr http://127.0.0.1:5690"
}

start_compactor() {
    "$PREFIX_BIN"/risingwave/compactor \
  --config-path \
  "$RW_PREFIX"/config/risingwave.toml \
  --listen-addr \
  127.0.0.1:6660 \
  --prometheus-listener-addr \
  127.0.0.1:1260 \
  --advertise-addr \
  127.0.0.1:6660 \
  --metrics-level \
  1 \
  --meta-address \
  http://127.0.0.1:5690
}

start_prometheus() {
  "$PREFIX_BIN"/prometheus/prometheus \
  --web.listen-address=127.0.0.1:9500 \
  --storage.tsdb.retention.time=30d \
  --config.file="$RW_PREFIX"/config/prometheus.yml \
  --storage.tsdb.path="$RW_PREFIX"/data/prometheus
}

start_grafana() {
    "$PREFIX_BIN"/grafana/bin/grafana-server
}

start_zookeeper() {
    "$PREFIX_BIN"/kafka/bin/zookeeper-server-start.sh \
  "$RW_PREFIX"/config/zookeeper-2181.properties
}

start_kafka() {
    "$PREFIX_BIN"/kafka/bin/kafka-server-start.sh \
  "$RW_PREFIX"/config/kafka-29092.properties
}

start_connector() {
  "$PREFIX_BIN"/connector-node/start-service.sh \
  -p \
  50051
}

main() {
  mkdir -p "$RW_PREFIX"
  mkdir -p "$PREFIX_LOG"
  mkdir -p "$PREFIX_BIN"
  echo "--- made tmp folders ---"

  start_minio >$PREFIX_LOG/minio.log 2>&1 &
  MINIO_PID=$!
  echo "--- minio started ---"
  configure_minio
  echo "--- minio configured ---"
  start_etcd >$PREFIX_LOG/etcd.log 2>&1 &
  ETCD_PID=$!
  echo "--- etcd started ---"
  start_standalone >$PREFIX_LOG/standalone.log 2>&1 &
  STANDALONE_PID=$!
  echo "--- standalone started ---"
  start_compactor >$PREFIX_LOG/compactor.log 2>&1 &
  COMPACTOR_PID=$!
  echo "--- compactor started ---"
  start_prometheus >$PREFIX_LOG/prometheus.log 2>&1 &
  PROMETHEUS_PID=$!
  echo "--- prometheus started ---"
  start_grafana >$PREFIX_LOG/grafana.log 2>&1 &
  GRAFANA_PID=$!
  echo "--- grafana started ---"
  start_zookeeper >$PREFIX_LOG/zookeeper.log 2>&1 &
  ZOOKEEPER_PID=$!
  echo "--- zookeeper started ---"
  start_kafka >$PREFIX_LOG/kafka.log 2>&1 &
  KAFKA_PID=$!
  echo "--- kafka started ---"
  start_connector >$PREFIX_LOG/connector.log 2>&1 &
  CONNECTOR_PID=$!
  echo "--- connector started ---"
  echo "--- Started all services"
}

start_standalone