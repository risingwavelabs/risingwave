#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "${DIR}/common.sh"

stop_cluster
clean_all_data
start_cluster

execute_sql_and_expect \
"SHOW parameters;" \
"backup_storage_directory | backup"

execute_sql_and_expect \
"SHOW parameters;" \
"backup_storage_url       | minio://hummockadmin:hummockadmin@127.0.0.1:9301/hummock001"

backup

execute_sql_and_expect \
"SELECT meta_snapshot_id FROM rw_catalog.rw_meta_snapshot;" \
"1 row"

execute_sql_and_expect \
"alter system set  backup_storage_directory to backup_1;" \
"ALTER_SYSTEM"
# system params application is async.
sleep 5

execute_sql_and_expect \
"SELECT meta_snapshot_id FROM rw_catalog.rw_meta_snapshot;" \
"0 row"

backup
backup
backup
execute_sql_and_expect \
"SELECT meta_snapshot_id FROM rw_catalog.rw_meta_snapshot;" \
"3 row"

execute_sql_and_expect \
"alter system set  backup_storage_directory to backup;" \
"ALTER_SYSTEM"
sleep 5

execute_sql_and_expect \
"SELECT meta_snapshot_id FROM rw_catalog.rw_meta_snapshot;" \
"1 row"

execute_sql_and_expect \
"alter system set backup_storage_url to memory;" \
"ALTER_SYSTEM"
sleep 5

execute_sql_and_expect \
"SELECT meta_snapshot_id FROM rw_catalog.rw_meta_snapshot;" \
"0 row"

backup
backup
execute_sql_and_expect \
"SELECT meta_snapshot_id FROM rw_catalog.rw_meta_snapshot;" \
"2 row"

execute_sql_and_expect \
"alter system set backup_storage_url to \"minio://hummockadmin:hummockadmin@127.0.0.1:9301/hummock001\"" \
"ALTER_SYSTEM"
sleep 5

execute_sql_and_expect \
"SELECT meta_snapshot_id FROM rw_catalog.rw_meta_snapshot;" \
"1 row"

backup
execute_sql_and_expect \
"SELECT meta_snapshot_id FROM rw_catalog.rw_meta_snapshot;" \
"2 row"

echo "test succeeded"