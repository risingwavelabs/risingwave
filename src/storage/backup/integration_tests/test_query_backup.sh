#!/usr/bin/env bash

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
select * from t1;
" | grep "1 row"
)
[ -n "${result}" ]

echo "QUERY_EPOCH=0 aka disabling query backup"
execute_sql_and_expect \
"SET QUERY_EPOCH TO 0;
select * from t1;" \
"1 row"

table_committed_epoch=$(get_table_committed_epoch_in_meta_snapshot)
echo "QUERY_EPOCH=table committed epoch in meta snapshot, it's covered by backup"
execute_sql_and_expect \
"SET QUERY_EPOCH TO ${table_committed_epoch};
select * from t1;" \
"3 row"

echo "QUERY_EPOCH=future epoch. It should fail because it's not covered by any backup"
future_epoch=18446744073709486080
execute_sql_and_expect \
"SET QUERY_EPOCH TO ${future_epoch};
select * from t1;" \
"backup include epoch ${future_epoch} not found"

echo "test succeeded"