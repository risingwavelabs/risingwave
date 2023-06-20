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

download_and_prepare_rw "$profile" source

# Change process number limit
echo "--- os limits"
ulimit -a

echo "--- Download connector node package"
buildkite-agent artifact download risingwave-connector.tar.gz ./
mkdir ./connector-node
tar xf ./risingwave-connector.tar.gz -C ./connector-node

# prepare environment mysql sink
mysql --host=mysql --port=3306 -u root -p123456 -e "CREATE DATABASE IF NOT EXISTS test;"
# grant access to `test` for ci test user
mysql --host=mysql --port=3306 -u root -p123456 -e "GRANT ALL PRIVILEGES ON test.* TO 'mysqluser'@'%';"
# creates two table named t_remote_0, t_remote_1
mysql --host=mysql --port=3306 -u root -p123456 test < ./e2e_test/sink/remote/mysql_create_table.sql

echo "--- preparing postgresql"

# set up PG sink destination
apt-get -y install postgresql-client
export PGPASSWORD=postgres
psql -h db -U postgres -c "CREATE ROLE test LOGIN SUPERUSER PASSWORD 'connector';"
createdb -h db -U postgres test
psql -h db -U postgres -d test -c "CREATE TABLE t4 (v1 int PRIMARY KEY, v2 int);"
psql -h db -U postgres -d test -c "create table t5 (v1 smallint primary key, v2 int, v3 bigint, v4 float4, v5 float8, v6 decimal, v7 varchar, v8 timestamp, v9 boolean);"
psql -h db -U postgres -d test < ./e2e_test/sink/remote/pg_create_table.sql

node_port=50051
node_timeout=10

echo "--- starting risingwave cluster with connector node"
cargo make ci-start ci-kafka
./connector-node/start-service.sh -p $node_port > .risingwave/log/connector-node.log 2>&1 &

echo "waiting for connector node to start"
start_time=$(date +%s)
while :
do
    if nc -z localhost $node_port; then
        echo "Port $node_port is listened! Connector Node is up!"
        break
    fi

    current_time=$(date +%s)
    elapsed_time=$((current_time - start_time))
    if [ $elapsed_time -ge $node_timeout ]; then
        echo "Timeout waiting for port $node_port to be listened!"
        exit 1
    fi
    sleep 0.1
done

echo "--- testing common sinks"
sqllogictest -p 4566 -d dev './e2e_test/sink/append_only_sink.slt'
sqllogictest -p 4566 -d dev './e2e_test/sink/create_sink_as.slt'
sqllogictest -p 4566 -d dev './e2e_test/sink/blackhole_sink.slt'
sqllogictest -p 4566 -d dev './e2e_test/sink/remote/types.slt'
sleep 1

echo "--- testing remote sinks"
# check sink destination postgres
sqllogictest -p 4566 -d dev './e2e_test/sink/remote/jdbc.load.slt'
sleep 1
sqllogictest -h db -p 5432 -d test './e2e_test/sink/remote/jdbc.check.pg.slt'
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
chmod +x ./ci/scripts/e2e-elasticsearch-sink-test.sh
./ci/scripts/e2e-elasticsearch-sink-test.sh
if [ $? -eq 0 ]; then
  echo "elasticsearch sink check passed"
else
  echo "elasticsearch sink test failed"
  exit 1
fi

echo "--- Kill cluster"
cargo make ci-kill
pkill -f connector-node

echo "--- e2e, ci-1cn-1fe, nexmark endless"
RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info" \
cargo make ci-start ci-1cn-1fe
sqllogictest -p 4566 -d dev './e2e_test/source/nexmark_endless_mvs/*.slt'
sqllogictest -p 4566 -d dev './e2e_test/source/nexmark_endless_sinks/*.slt'

echo "--- Kill cluster"
cargo make ci-kill
