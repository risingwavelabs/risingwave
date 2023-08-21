#!/usr/bin/env bash

set -euo pipefail

insert_json_kafka() {
  echo $1 | \
    $KAFKA_PATH/bin/kafka-console-producer.sh \
      --topic source_kafka \
      --bootstrap-server localhost:29092
}

create_topic_kafka() {
  "$KAFKA_PATH"/bin/kafka-topics.sh \
    --create \
    --topic source_kafka \
    --bootstrap-server localhost:29092
}

# Make sure we start on clean cluster
set +e
./risedev clean-data
./risedev k
pkill risingwave
set -e

RW_PREFIX=$PWD/.risingwave
LOG_PREFIX=$RW_PREFIX/log
BIN_PREFIX=$RW_PREFIX/bin
KAFKA_PATH=$BIN_PREFIX/kafka

mkdir -p "$RW_PREFIX"
mkdir -p "$LOG_PREFIX"
mkdir -p "$BIN_PREFIX"

./risedev build

./risedev d standalone-full-peripherals >"$LOG_PREFIX"/peripherals.log 2>&1 &

# Wait for peripherals to finish startup
sleep 5

./risedev standalone-demo-full >"$LOG_PREFIX"/standalone.log 2>&1 &
STANDALONE_PID=$!

# Wait for rw cluster to finish startup
sleep 20

echo "--- Setting up table"
./risedev psql -c "
CREATE TABLE t (v1 int);
INSERT INTO t VALUES (1);
flush;
"

echo "--- Querying table"
./risedev psql -c "SELECT * from t;"

echo "--- Seeding kafka topic"
create_topic_kafka
insert_json_kafka '{"v1": 1}'

echo "--- Setting up kafka source"
./risedev psql -c "
CREATE TABLE kafka_source (v1 int) WITH (
  connector = 'kafka',
  topic = 'source_kafka',
  properties.bootstrap.server = 'localhost:29092',
  scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON;
"

echo "--- Querying source"
./risedev psql -c "SELECT * FROM kafka_source;"

echo "--- Tearing down rw components, check if data still persists"
kill $STANDALONE_PID

echo "--- Restarting rw"
./risedev standalone-demo-full >$LOG_PREFIX/standalone-restarted.log 2>&1 &
STANDALONE_PID=$!

sleep 10

echo "--- Querying table"
./risedev psql -c "SELECT * from t;"

echo "--- Querying source"
./risedev psql -c "SELECT * FROM kafka_source;"