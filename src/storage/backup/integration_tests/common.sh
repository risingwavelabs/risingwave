#!/bin/bash
set -eo pipefail
[ -n "${BACKUP_TEST_PREFIX_BIN}" ]
[ -n "${BACKUP_TEST_PREFIX_DATA}" ]

function stop_cluster() {
  cargo make k || true
}

function clean_all_data {
  cargo make clean-data
}

function clean_etcd_data() {
  cargo make clean-etcd-data
}

function start_cluster() {
  cargo make d ci-meta-backup-test
}

function wait_cluster_ready() {
  # TODO #6482: wait cluster to finish actor migration and other recovery stuff deterministically.
  sleep 5
}

function full_gc_sst() {
  cargo make ctl hummock trigger-full-gc -s 0
  # TODO #6482: wait full gc finish deterministically.
  # Currently have to wait long enough.
  sleep 30
}

function start_etcd_minio() {
  cargo make d ci-meta-backup-test-restore
}

function create_mvs() {
  cargo make slt -p 4566 -d dev "e2e_test/backup_restore/tpch_snapshot_create.slt"
}

function query_mvs() {
  cargo make slt -p 4566 -d dev "e2e_test/backup_restore/tpch_snapshot_query.slt"
}

function drop_mvs() {
  cargo make slt -p 4566 -d dev "e2e_test/backup_restore/tpch_snapshot_drop.slt"
}

function backup() {
  local job_id
  job_id=$(cargo make ctl meta backup-meta | grep "backup job succeeded" | awk '{print $(NF)}')
  [ -n "${job_id}" ]
  echo "${job_id}"
}

function delete_snapshot() {
  local snapshot_id
  snapshot_id=$1
  cargo make ctl meta delete-meta-snapshots "${snapshot_id}"
}

function restore() {
  local job_id
  job_id=$1
  echo "try to restore snapshot ${job_id}"
  stop_cluster
  clean_etcd_data
  start_etcd_minio
  "${BACKUP_TEST_PREFIX_BIN}"/backup-restore \
  --backend etcd \
  --meta-snapshot-id "${job_id}" \
  --etcd-endpoints 127.0.0.1:2388 \
  --storage-directory backup \
  --storage-url minio://hummockadmin:hummockadmin@127.0.0.1:9301/hummock001
}

function get_total_sst_count() {
  find "${BACKUP_TEST_PREFIX_DATA}/minio/hummock001" -type f -name "*.data" |wc -l
}
