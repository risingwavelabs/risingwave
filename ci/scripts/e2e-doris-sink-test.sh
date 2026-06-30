#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

while getopts 'p:' opt; do
    case ${opt} in
        p )
            profile=$OPTARG
            ;;
        \? )
            echo "Invalid Option: -$OPTARG" 1>&2
            exit 1
            ;;
        : )
            echo "Invalid option: $OPTARG requires an argument" 1>&2
            ;;
    esac
done
shift $((OPTIND -1))

sink_test_env_setup "$profile" --sleep-duration 0

create_doris_table() {
  local ddl

  ddl="CREATE DATABASE IF NOT EXISTS demo;
USE demo;
DROP TABLE IF EXISTS demo_bhv_table;
CREATE TABLE demo_bhv_table(v1 int,v2 smallint,v3 bigint,v4 float,v5 double,v6 string,v7 datev2,v8 datetime,v9 boolean,v10 json) UNIQUE KEY(\`v1\`)
DISTRIBUTED BY HASH(\`v1\`) BUCKETS 1
PROPERTIES (
    \"replication_allocation\" = \"tag.location.default: 1\"
);"

  echo "--- create doris table"
  for _ in $(seq 1 60); do
    if mysql -uroot -P 9030 -h doris-server -e "$ddl"; then
      return
    fi
    mysql -uroot -P 9030 -h doris-server -e "SHOW BACKENDS;" || true
    sleep 2
  done

  echo "Doris backend did not become ready for table creation in time"
  mysql -uroot -P 9030 -h doris-server -e "SHOW BACKENDS;" || true
  exit 1
}

create_doris_table
mysql -uroot -P 9030 -h doris-server -e "CREATE USER 'users'@'%' IDENTIFIED BY '123456';
GRANT ALL ON *.* TO 'users'@'%';"
sleep 2

echo "--- testing sinks"
sqllogictest -p 4566 -d dev './e2e_test/sink/doris_sink.slt'
sleep 1
mysql -uroot -P 9030 -h doris-server -e "select * from demo.demo_bhv_table" > ./query_result.csv


if cat ./query_result.csv | sed '1d; s/\t/,/g' | awk -F "," '{
    exit !($1 == 1 && $2 == 1 && $3 == 1 && $4 == 1.1 && $5 == 1.2 && $6 == "test" && $7 == "2013-01-01" && $8 == "2013-01-01 01:01:01" && $9 == 0 && $10 == "{\"a\":1}"); }'; then
  echo "Doris sink check passed"
else
  cat ./query_result.csv
  echo "The output is not as expected."
  exit 1
fi

echo "--- Kill cluster"
risedev ci-kill
