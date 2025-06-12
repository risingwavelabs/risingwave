#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

# prepare environment
export CONNECTOR_LIBS_PATH="./connector-node/libs"
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

download_and_prepare_rw "$profile" source

prepare_pg() {
  # set up PG sink destination
  export PGPASSWORD='post\tgres'
  psql -h db -U postgres -c "CREATE ROLE test LOGIN SUPERUSER PASSWORD 'connector';" || true
  dropdb -h db -U postgres test || true
  createdb -h db -U postgres test
  psql -h db -U postgres -d test -c "CREATE TABLE t4 (v1 int PRIMARY KEY, v2 int);"
  psql -h db -U postgres -d test -c "create table t5 (v1 smallint primary key, v2 int, v3 bigint, v4 float4, v5 float8, v6 decimal, v7 varchar, v8 timestamp, v9 boolean);"
  psql -h db -U postgres -d test < ./e2e_test/sink/remote/pg_create_table.sql
}

# Change process number limit
echo "--- os limits"
ulimit -a

echo "--- download connector node package"
buildkite-agent artifact download risingwave-connector.tar.gz ./
mkdir ./connector-node
tar xf ./risingwave-connector.tar.gz -C ./connector-node

echo "--- prepare mysql"
# prepare environment mysql sink
mysql --host=mysql --port=3306 -u root -p123456 -e "CREATE DATABASE IF NOT EXISTS test;"
# grant access to `test` for ci test user
mysql --host=mysql --port=3306 -u root -p123456 -e "GRANT ALL PRIVILEGES ON test.* TO 'mysqluser'@'%';"
# creates two table named t_remote_0, t_remote_1
mysql --host=mysql --port=3306 -u root -p123456 test < ./e2e_test/sink/remote/mysql_create_table.sql

echo "--- preparing postgresql"
prepare_pg

echo "--- starting risingwave cluster: ci-1cn-1fe-switch-to-pg-native"
risedev ci-start ci-1cn-1fe-jdbc-to-native

echo "--- test sink: jdbc:postgres switch to postgres native"
# check sink destination postgres
risedev slt './e2e_test/sink/remote/jdbc.load.slt'
sleep 1
SLT_PASSWORD=$PGPASSWORD sqllogictest -h db -p 5432 -d test './e2e_test/sink/remote/jdbc.check.pg.slt' --label 'pg-native'
sleep 1

echo "--- killing risingwave cluster: ci-1cn-1fe-switch-to-pg-native"
risedev ci-kill

echo "--- starting risingwave cluster"
# Use ci-inline-source-test since it will configure ports, db, host etc... env vars via risedev-env.
# These are required for cli tools like psql have env vars correctly configured.
risedev ci-start ci-inline-source-test

echo "--- check connectivity for postgres"
PGPASSWORD='post\tgres' psql -h db -U postgres -d postgres -p 5432 -c "SELECT 1;"

echo "--- dumping risedev-env"
echo "risedev-env:"
risedev show-risedev-env

# MUST use risedev slt, not sqllogictest, else env var not loaded and test fails.
echo "--- testing postgres_sink"
risedev slt './e2e_test/sink/postgres_sink.slt'

echo "--- testing common sinks"
risedev slt './e2e_test/sink/append_only_sink.slt'
risedev slt './e2e_test/sink/create_sink_as.slt'
risedev slt './e2e_test/sink/blackhole_sink.slt'
risedev slt './e2e_test/sink/remote/types.slt'
risedev slt './e2e_test/sink/sink_into_table/*.slt'
risedev slt './e2e_test/sink/file_sink.slt'
sleep 1

echo "--- preparing postgresql"
prepare_pg

echo "--- testing remote sinks"

# check sink destination postgres
risedev slt './e2e_test/sink/remote/jdbc.load.slt'
sleep 1
SLT_PASSWORD=$PGPASSWORD sqllogictest -h db -p 5432 -d test './e2e_test/sink/remote/jdbc.check.pg.slt' --label 'jdbc'
sleep 1

# check sink destination mysql using shell
diff -u ./e2e_test/sink/remote/mysql_expected_result_0.tsv \
<(mysql --host=mysql --port=3306 -u root -p123456 -s -N -r test -e "SELECT * FROM test.t_remote_0 ORDER BY id")
if [ $? -eq 0 ]; then
  echo "mysql sink check 0 passed"
else
  echo "The output is not as expected."
  exit 1
fi

diff -u ./e2e_test/sink/remote/mysql_expected_result_1.tsv \
<(mysql --host=mysql --port=3306 -u root -p123456 -s -N -r test -e "SELECT id, v_varchar, v_text, v_integer, v_smallint, v_bigint, v_decimal, v_real, v_double, v_boolean, v_date, v_time, v_timestamp, v_timestamptz, v_interval, v_jsonb, TO_BASE64(v_bytea) FROM test.t_remote_1 ORDER BY id")
if [ $? -eq 0 ]; then
  echo "mysql sink check 1 passed"
else
  echo "The output is not as expected."
  exit 1
fi

diff -u ./e2e_test/sink/remote/mysql_expected_result_2.tsv \
<(mysql --host=mysql --port=3306 -u root -p123456 -s -N -r test -e "SELECT * FROM test.t_types ORDER BY id")
if [ $? -eq 0 ]; then
  echo "mysql sink check 0 passed"
else
  echo "The output is not as expected."
  exit 1
fi

echo "--- testing kafka sink"
./ci/scripts/e2e-kafka-sink-test.sh
if [ $? -eq 0 ]; then
  echo "kafka sink check passed"
else
  echo "kafka sink test failed"
  exit 1
fi

echo "--- testing elasticsearch sink"
./ci/scripts/e2e-elasticsearch-sink-test.sh
if [ $? -eq 0 ]; then
  echo "elasticsearch sink check passed"
else
  echo "elasticsearch sink test failed"
  exit 1
fi

echo "--- Kill cluster"
risedev ci-kill

echo "--- e2e, ci-1cn-1fe, nexmark endless"
RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info" \
risedev ci-start ci-1cn-1fe
risedev slt './e2e_test/sink/nexmark_endless_mvs/*.slt'
risedev slt './e2e_test/sink/nexmark_endless_sinks/*.slt'

echo "--- Kill cluster"
risedev ci-kill
