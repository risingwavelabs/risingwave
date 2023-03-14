#!/usr/bin/env bash
set -eo pipefail
[ -n "${BACKUP_TEST_BACKUP_RESTORE}" ]
[ -n "${BACKUP_TEST_MCLI}" ]
[ -n "${BACKUP_TEST_MCLI_CONFIG}" ]
[ -n "${BACKUP_TEST_RW_ALL_IN_ONE}" ]

function stop_cluster() {
  cargo make k 1>/dev/null 2>&1 || true
}

function clean_all_data {
  cargo make clean-data 1>/dev/null 2>&1
}

function clean_etcd_data() {
  cargo make clean-etcd-data 1>/dev/null 2>&1
}

function start_cluster() {
  cargo make d ci-meta-backup-test 1>/dev/null 2>&1
}

function wait_cluster_ready() {
  # TODO #6482: wait cluster to finish actor migration and other recovery stuff deterministically.
  sleep 5
}

function full_gc_sst() {
  ${BACKUP_TEST_RW_ALL_IN_ONE} risectl hummock trigger-full-gc -s 0 1>/dev/null 2>&1
  # TODO #6482: wait full gc finish deterministically.
  # Currently have to wait long enough.
  sleep 30
}

function manual_compaction() {
  ${BACKUP_TEST_RW_ALL_IN_ONE} risectl hummock trigger-manual-compaction "$@" 1>/dev/null 2>&1
}

function start_etcd_minio() {
  cargo make d ci-meta-backup-test-restore 1>/dev/null 2>&1
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
  job_id=$(${BACKUP_TEST_RW_ALL_IN_ONE} risectl meta backup-meta | grep "backup job succeeded" | awk '{print $(NF)}')
  [ -n "${job_id}" ]
  echo "${job_id}"
}

function delete_snapshot() {
  local snapshot_id
  snapshot_id=$1
  ${BACKUP_TEST_RW_ALL_IN_ONE} risectl meta delete-meta-snapshots "${snapshot_id}"
}

function restore() {
  local job_id
  job_id=$1
  echo "try to restore snapshot ${job_id}"
  stop_cluster
  clean_etcd_data
  start_etcd_minio
  ${BACKUP_TEST_BACKUP_RESTORE} \
  --meta-store-type etcd \
  --meta-snapshot-id "${job_id}" \
  --etcd-endpoints 127.0.0.1:2388 \
  --storage-directory backup \
  --storage-url minio://hummockadmin:hummockadmin@127.0.0.1:9301/hummock001 \
  1>/dev/null
}

function execute_sql() {
  local sql
  sql=$1
  echo "${sql}" | psql -h localhost -p 4566 -d dev -U root 2>&1
}

function execute_sql_and_expect() {
  local sql
  sql=$1
  local expected
  expected=$2

  echo "execute SQL ${sql}"
  echo "expected string in result: ${expected}"
  query_result=$(execute_sql "${sql}")
  printf "actual result:\n%s\n" "${query_result}"
  result=$(echo "${query_result}" | grep "${expected}")
  [ -n "${result}" ]
}

function get_max_committed_epoch() {
  mce=$(${BACKUP_TEST_RW_ALL_IN_ONE} risectl hummock list-version | grep max_committed_epoch | sed -n 's/^.*max_committed_epoch: \(.*\),/\1/p')
  echo "${mce}"
}

function get_safe_epoch() {
  safe_epoch=$(${BACKUP_TEST_RW_ALL_IN_ONE} risectl hummock list-version | grep safe_epoch | sed -n 's/^.*safe_epoch: \(.*\),/\1/p')
  echo "${safe_epoch}"
}

function get_total_sst_count() {
  ${BACKUP_TEST_MCLI} -C "${BACKUP_TEST_MCLI_CONFIG}" \
  find "hummock-minio/hummock001" -name "*.data" |wc -l
}

function get_max_committed_epoch_in_backup() {
  local id
  id=$1
  sed_str="s/.*{\"id\":${id},\"hummock_version_id\":.*,\"ssts\":\[.*\],\"max_committed_epoch\":\([[:digit:]]*\),\"safe_epoch\":.*}.*/\1/p"
  ${BACKUP_TEST_MCLI} -C "${BACKUP_TEST_MCLI_CONFIG}" \
  cat "hummock-minio/hummock001/backup/manifest.json" | sed -n "${sed_str}"
}

function get_safe_epoch_in_backup() {
  local id
  id=$1
  sed_str="s/.*{\"id\":${id},\"hummock_version_id\":.*,\"ssts\":\[.*\],\"max_committed_epoch\":.*,\"safe_epoch\":\([[:digit:]]*\)}.*/\1/p"
  ${BACKUP_TEST_MCLI} -C "${BACKUP_TEST_MCLI_CONFIG}" \
  cat "hummock-minio/hummock001/backup/manifest.json" | sed -n "${sed_str}"
}

function get_min_pinned_snapshot() {
  s=$(${BACKUP_TEST_RW_ALL_IN_ONE} risectl hummock list-pinned-snapshots | grep "min_pinned_snapshot" | sed -n 's/.*min_pinned_snapshot \(.*\)/\1/p' | sort -n | head -1)
  echo "${s}"
}
