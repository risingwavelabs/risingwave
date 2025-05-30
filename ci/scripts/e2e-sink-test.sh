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
  export PGPASSWORD=postgres
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

echo "--- download pg dependencies"
apt-get -y install postgresql-client jq

echo "--- prepare mysql"
# prepare environment mysql sink
mysql --host=mysql --port=3306 -u root -p123456 -e "CREATE DATABASE IF NOT EXISTS test;"
# grant access to `test` for ci test user
mysql --host=mysql --port=3306 -u root -p123456 -e "GRANT ALL PRIVILEGES ON test.* TO 'mysqluser'@'%';"
# creates two table named t_remote_0, t_remote_1
mysql --host=mysql --port=3306 -u root -p123456 test < ./e2e_test/sink/remote/mysql_create_table.sql

echo "--- preparing postgresql"
prepare_pg

RUST_LOG='risingwave_connector::sink::postgres=trace'
echo "--- starting risingwave cluster: ci-1cn-1fe-switch-to-pg-native"
risedev ci-start ci-1cn-1fe-jdbc-to-native

echo "--- test sink: jdbc:postgres switch to postgres native"
# check sink destination postgres
sqllogictest -p 4566 -d dev './e2e_test/sink/remote/jdbc.load.slt'
sleep 1
sqllogictest -h db -p 5432 -d test './e2e_test/sink/remote/jdbc.check.pg.slt' --label 'pg-native'
sleep 1
