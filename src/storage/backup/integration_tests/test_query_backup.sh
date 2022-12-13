#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "${DIR}/common.sh"

stop_cluster
clean_all_data
start_cluster

execute_sql "
SET RW_IMPLICIT_FLUSH TO true;
create table t1(v1 int, v2 int);
insert into t1 values (2,1),(1,2),(1,1);
"

result=$(
execute_sql "
select * from t1;
" | grep "3 row"
)
[ -n "${result}" ]

# backup before delete rows

job_id=$(backup)
echo "${job_id}"
backup_mce=$(get_max_committed_epoch_in_backup "${job_id}")
backup_safe_epoch=$(get_safe_epoch_in_backup "${job_id}")
echo "backup MCE: ${backup_mce}"
echo "backup safe_epoch: ${backup_safe_epoch}"

execute_sql "
SET RW_IMPLICIT_FLUSH TO true;
delete from t1 where v1=1;
"

result=$(
execute_sql "
select * from t1;
" | grep "1 row"
)
[ -n "${result}" ]

result=$(
execute_sql "
SET QUERY_EPOCH TO 0;
select * from t1;
" | grep "1 row"
)
[ -n "${result}" ]

min_pinned_snapshot=$(get_min_pinned_snapshot)
while [ "${min_pinned_snapshot}" -le "${backup_mce}" ] ;
do
  echo "wait frontend to unpin snapshot. current: ${min_pinned_snapshot}, expect: ${backup_mce}"
  sleep 5
  min_pinned_snapshot=$(get_min_pinned_snapshot)
done
# safe epoch equals to 0 because no compaction has been done
safe_epoch=$(get_safe_epoch)
[ "${safe_epoch}" -eq 0 ]
# trigger a compaction to increase safe_epoch
manual_compaction -c 3 -l 0
# wait until compaction is done
while [ "${safe_epoch}" -le "${backup_mce}" ] ;
do
  safe_epoch=$(get_safe_epoch)
  sleep 5
done
echo "safe epoch after compaction: ${safe_epoch}"

echo "query with safe_epoch"
result=$(
execute_sql "
SET QUERY_EPOCH TO ${safe_epoch};
select * from t1;
" | grep "1 row\|3 row"
)
[ -n "${result}" ]

echo "query with safe_epoch - 1"
result=$(
execute_sql "
SET QUERY_EPOCH TO $(( safe_epoch - 1 ));
select * from t1;
" | grep "Expired Epoch"
)
[ -n "${result}" ]

echo "query with QUERY_EPOCH=0 aka use latest epoch"
result=$(
execute_sql "
SET QUERY_EPOCH TO 0;
select * from t1;
" | grep "1 row"
)
[ -n "${result}" ]

echo "query with backup_safe_epoch + 1 < safe_epoch but covered by backup"
[ $((backup_safe_epoch + 1)) -eq 1 ]
result=$(
execute_sql "
SET QUERY_EPOCH TO $((backup_safe_epoch + 1));
select * from t1;
" | grep "0 row"
)
[ -n "${result}" ]

echo "query with backup_mce < safe_epoch but covered by backup"
result=$(
execute_sql "
SET QUERY_EPOCH TO $((backup_mce));
select * from t1;
" | grep "3 row"
)
[ -n "${result}" ]

echo "query with future epoch"
result=$(
execute_sql "
SET QUERY_EPOCH TO 18446744073709551615;
select * from t1;
" | grep "cannot query with future epoch"
)
[ -n "${result}" ]