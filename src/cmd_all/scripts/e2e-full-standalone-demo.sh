#!/usr/bin/env bash

# This script is used to run a full standalone demo of RisingWave.
# It includes the following components:
# - RisingWave cluster
# - Minio
# - Etcd
# - Kafka
# - Connector
# - Compactor
# - Prometheus
# - Grafana
#
# We test the full cluster by:
# 1. Creating source and rw table.
# 2. Inserting data into the tables.
# 3. Querying the data from the tables.
# 4. Restart the cluster, repeat step 3.

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

echo "--- Compiling Risingwave"
./risedev build

echo "--- Starting peripherals"
./risedev d standalone-full-peripherals >"$LOG_PREFIX"/peripherals.log 2>&1 &

# Wait for peripherals to finish startup
sleep 5

echo "--- Starting standalone cluster"
./risedev standalone-demo-full >"$LOG_PREFIX"/standalone.log 2>&1 &
STANDALONE_PID=$!

# Wait for rw cluster to finish startup
sleep 10

# Make sure the env file is present
set +e
while [ ! -f "$RW_PREFIX"/config/risedev-env ]; do
  echo "Waiting for risedev-env to be configured."
  sleep 1
done
set -e

echo "--- Setting up table"
./risedev psql -c "
CREATE TABLE t (v1 int);
INSERT INTO t VALUES (1);
INSERT INTO t VALUES (2);
INSERT INTO t VALUES (3);
INSERT INTO t VALUES (4);
INSERT INTO t VALUES (5);
flush;
"

echo "--- Querying table"
./risedev psql -c "SELECT * from t;"

echo "--- Seeding kafka topic"
create_topic_kafka
insert_json_kafka '{"v1": 1}'
insert_json_kafka '{"v1": 2}'
insert_json_kafka '{"v1": 3}'
insert_json_kafka '{"v1": 4}'
insert_json_kafka '{"v1": 5}'

echo "--- Setting up kafka source"
./risedev psql -c "
CREATE TABLE kafka_source (v1 int) WITH (
  connector = 'kafka',
  topic = 'source_kafka',
  properties.bootstrap.server = 'localhost:29092',
  scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON;
"

sleep 5

echo "--- Querying source"
./risedev psql -c "SELECT * FROM kafka_source;"

echo "--- Kill standalone cluster"
pkill risingwave

echo "--- Restarting standalone cluster"
./risedev standalone-demo-full >"$LOG_PREFIX"/standalone-restarted.log 2>&1 &
STANDALONE_PID=$!

# Wait for rw cluster to finish startup & recovery to finish
echo "--- Waiting 60s for recovery to finish"
sleep 60

echo "--- Querying table"
./risedev psql -c "SELECT * FROM t;"

echo "--- Querying source"
./risedev psql -c "SELECT * FROM kafka_source;"

echo "--- Running cleanup"
./risedev k
pkill risingwave
./risedev clean-data
