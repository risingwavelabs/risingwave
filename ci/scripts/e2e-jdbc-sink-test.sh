#!/usr/bin/env bash

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

prepare_pg() {
  export PGPASSWORD='post\tgres'
  psql -h db -U postgres -c "CREATE ROLE test LOGIN SUPERUSER PASSWORD 'connector';" || true
  dropdb -h db -U postgres test || true
  createdb -h db -U postgres test
  psql -h db -U postgres -d test -c "CREATE TABLE t4 (v1 int PRIMARY KEY, v2 int);"
  psql -h db -U postgres -d test -c "create table t5 (v1 smallint primary key, v2 int, v3 bigint, v4 float4, v5 float8, v6 decimal, v7 varchar, v8 timestamp, v9 boolean);"
  psql -h db -U postgres -d test < ./e2e_test/sink/remote/pg_create_table.sql
}

download_and_prepare_rw "$profile" source
download_connector_node_artifact

echo "--- prepare mysql"
mysql --host=mysql --port=3306 -u root -p123456 -e "CREATE DATABASE IF NOT EXISTS test;"
mysql --host=mysql --port=3306 -u root -p123456 -e "GRANT ALL PRIVILEGES ON test.* TO 'mysqluser'@'%';"
mysql --host=mysql --port=3306 -u root -p123456 test < ./e2e_test/sink/remote/mysql_create_table.sql

echo "--- prepare postgresql"
prepare_pg

echo "--- starting risingwave cluster: ci-1cn-1fe-jdbc-to-native"
RUST_LOG="await_tree::future=error" risedev ci-start ci-1cn-1fe-jdbc-to-native
risedev slt './e2e_test/sink/remote/jdbc.load.slt'
sleep 1
SLT_PASSWORD=$PGPASSWORD sqllogictest -h db -p 5432 -d test './e2e_test/sink/remote/jdbc.check.pg.slt' --label 'pg-native'
sleep 1
risedev ci-kill

echo "--- starting risingwave cluster: ci-sink-test"
RUST_LOG="await_tree::future=error" risedev ci-start ci-sink-test
risedev slt './e2e_test/sink/create_sink_as.slt'
risedev slt './e2e_test/sink/remote/types.slt'
risedev slt './e2e_test/sink/remote/jdbc.alter_connector_props.slt'
risedev slt './e2e_test/sink/remote/jdbc.load.slt'
sleep 1
SLT_PASSWORD=$PGPASSWORD sqllogictest -h db -p 5432 -d test './e2e_test/sink/remote/jdbc.check.pg.slt' --label 'jdbc'
sleep 1

diff -u ./e2e_test/sink/remote/mysql_expected_result_0.tsv \
<(mysql --host=mysql --port=3306 -u root -p123456 -s -N -r test -e "SELECT * FROM test.t_remote_0 ORDER BY id")

diff -u ./e2e_test/sink/remote/mysql_expected_result_1.tsv \
<(mysql --host=mysql --port=3306 -u root -p123456 -s -N -r test -e "SELECT id, v_varchar, v_text, v_integer, v_smallint, v_bigint, v_decimal, v_real, v_double, v_boolean, v_date, v_time, v_timestamp, v_timestamptz, v_interval, v_jsonb, TO_BASE64(v_bytea) FROM test.t_remote_1 ORDER BY id")

diff -u ./e2e_test/sink/remote/mysql_expected_result_2.tsv \
<(mysql --host=mysql --port=3306 -u root -p123456 -s -N -r test -e "SELECT * FROM test.t_types ORDER BY id")

echo "--- Kill cluster"
risedev ci-kill
