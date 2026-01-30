#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "${DIR}/common.sh"

stop_cluster
clean_all_data
start_cluster

echo "try to backup meta for empty cluster"
job_id_1=$(backup)
echo "create snapshot ${job_id_1} succeeded"
get_all_sst_paths

create_mvs
query_mvs

echo "try to backup meta after creating mvs"
ssts_snapshot_2_file=$(mktemp)
get_all_sst_paths > "${ssts_snapshot_2_file}"
job_id_2=$(backup)
echo "create snapshot ${job_id_2} succeeded"
cat "${ssts_snapshot_2_file}"

sleep 5

restore "${job_id_1}"
start_cluster
get_all_sst_paths
sst_count_before_gc=$(get_total_sst_count)
# SSTs are pinned by snapshot 2
full_gc_sst
sst_count_after_gc=$(get_total_sst_count)
ssts_after_gc_file=$(mktemp)
get_all_sst_paths > "${ssts_after_gc_file}"
echo "after gc"
cat "${ssts_after_gc_file}"
echo "sst count before gc: ${sst_count_before_gc}, after gc: ${sst_count_after_gc}"
missing_ssts=$(comm -23 "${ssts_snapshot_2_file}" "${ssts_after_gc_file}")
rm -f "${ssts_after_gc_file}"
if [ -n "${missing_ssts}" ]; then
  echo "Missing pinned SSTs after GC:"
  echo "${missing_ssts}"
  exit 1
fi
[ "${sst_count_before_gc}" -gt 0 ]

delete_snapshot "${job_id_2}"
restore "${job_id_1}"
start_cluster
sst_count_before_gc=$(get_total_sst_count)
ssts_before_final_gc_file=$(mktemp)
get_all_sst_paths > "${ssts_before_final_gc_file}"
missing_ssts=$(comm -23 "${ssts_snapshot_2_file}" "${ssts_before_final_gc_file}")
rm -f "${ssts_before_final_gc_file}" "${ssts_snapshot_2_file}"
if [ -n "${missing_ssts}" ]; then
  echo "Missing SSTs after deleting snapshot 2:"
  echo "${missing_ssts}"
  exit 1
fi
# SSTs are no longer pinned
full_gc_sst
sst_count_after_gc=$(get_total_sst_count)
[ 0 -eq "${sst_count_after_gc}" ]

echo "test succeeded"
