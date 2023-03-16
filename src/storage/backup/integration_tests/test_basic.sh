#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. "${DIR}/common.sh"

stop_cluster
clean_all_data
start_cluster

echo "try to backup meta for empty cluster"
job_id_1=$(backup)
echo "create snapshot ${job_id_1} succeeded"

echo "try to backup meta after creating mvs"
create_mvs
query_mvs
job_id_2=$(backup)
echo "create snapshot ${job_id_2} succeeded"

echo "try to backup meta after dropping mvs"
drop_mvs
job_id_3=$(backup)
echo "create snapshot ${job_id_3} succeeded"

invalid_job_id=$((job_id_3 + 1))
if restore "${invalid_job_id}"; then
  echo "restore invalid snapshot ${invalid_job_id} should fail"
  exit 1
fi

restore "${job_id_1}"
start_cluster
if ! psql -h localhost -p 4566 -d dev -U root -c "show materialized views;" | grep -q "0 row"; then
  echo "expect 0 MV"
  exit 1
fi
echo "restore snapshot ${job_id_1} succeeded"

restore "${job_id_2}"
start_cluster
if ! psql -h localhost -p 4566 -d dev -U root -c "show materialized views;" | grep -q "1 row"; then
  echo "expect 1 MVs"
  exit 1
fi
echo "restore snapshot ${job_id_2} succeeded"
query_mvs
# any other ops in the restored cluster
drop_mvs
create_mvs
query_mvs

restore "${job_id_3}"
start_cluster
if ! psql -h localhost -p 4566 -d dev -U root -c "show materialized views;" | grep -q "0 row"; then
  echo "expect 0 MV"
  exit 1
fi
echo "restore snapshot ${job_id_3} succeeded"

echo "test succeeded"
