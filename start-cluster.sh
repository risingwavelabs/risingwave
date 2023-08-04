#!/usr/bin/env bash

set -euo pipefail

KAFKA_PATH=$PWD/.risingwave/bin/kafka

###################### UTILS

make_pg() {
    mkdir -p ~/pg_data
    docker run -d \
      --name perm-postgres \
      -e POSTGRES_USER=postgres \
      -e POSTGRES_PASSWORD=abc \
      -e POSTGRES_HOST_AUTH_METHOD=trust \
      -v ~/pg_data:/var/lib/postgresql/data \
      -p 5432:5432 \
      -it postgres:14.1-alpine
}

start_pg() {
    docker start perm-postgres
    sleep 60
}
stop_pg() {
    docker stop perm-postgres
}

clean_pg() {
    docker rm perm-postgres
    rm -rf ~/pg_data
}

connect_pg() {
    psql -h localhost -p 5432 -U postgres
}

run_sql_pg() {
  psql -h localhost -p 5432 -U postgres -c "$@"
}

# https://www.risingwave.dev/docs/current/ingest-from-postgres-cdc/
configure_postgres() {
  run_sql_pg "SHOW wal_level;"
  run_sql_pg "ALTER SYSTEM SET wal_level = logical;"
  # restart the pg service
  # Make sure changes take effect.
  docker restart perm-postgres
  sleep 60
  run_sql_pg "SHOW wal_level;"

#  run_sql_pg "GRANT CONNECT ON DATABASE <database_name> TO postgres;"
#  run_sql_pg "GRANT USAGE ON SCHEMA <schema_name> TO postgres"
#  run_sql_pg "GRANT SELECT ON ALL TABLES IN SCHEMA <schema_name> TO postgres"
#  run_sql_pg "GRANT CREATE ON DATABASE postgres TO postgres"
}

run_sql_rw() {
  psql -h localhost -p 4566 -d dev -U root -c "$@"
}

insert_json_kafka() {
  echo $1 | $KAFKA_PATH/bin/kafka-console-producer.sh --topic source_kafka --bootstrap-server localhost:29092
}

################### CDC

create_cdc_source() {
run_sql_pg "
DROP TABLE IF EXISTS test1;
CREATE TABLE test1 (
 obj_id integer,
 name varchar,
 age integer,
 PRIMARY KEY ( obj_id )
);
"
run_sql_rw "
CREATE TABLE source_postgres (
 obj_id integer,
 name varchar,
 age integer,
 PRIMARY KEY ( obj_id )
)
WITH (
 connector='postgres-cdc',
 hostname='localhost',
 port='5432',
 username='postgres',
 password='abc',
 database.name='postgres',
 table.name='test1',
 slot.name='source_postgres'
);
"
}

insert_cdc_source() {
run_sql_pg "
INSERT INTO "public"."test1" ("obj_id", "name", "age") VALUES (1, '张三', 11);
INSERT INTO "public"."test1" ("obj_id", "name", "age") VALUES (2, '李四', 22);
INSERT INTO "public"."test1" ("obj_id", "name", "age") VALUES (3, '王五', 33);
INSERT INTO "public"."test1" ("obj_id", "name", "age") VALUES (4, '赵六', 12);
INSERT INTO "public"."test1" ("obj_id", "name", "age") VALUES (5, '哈哈', 34);
"
}

query_cdc_source() {
run_sql_rw "SELECT * FROM source_postgres;"
}

################## KAFKA

create_kafka_source() {
run_sql_rw "
CREATE SOURCE IF NOT EXISTS source_kafka (
 timestamp timestamp,
 user_id integer,
 page_id integer,
 action varchar
)
WITH (
 connector='kafka',
 topic='source_kafka',
 properties.bootstrap.server='localhost:29092',
 scan.startup.mode='earliest',
) FORMAT PLAIN ENCODE JSON;
"
}

insert_kafka_source() {
  insert_json_kafka '{"timestamp": "2023-07-28 07:11:00", "user_id": 1, "page_id": 1, "action": "gtrgretrg"}'
  insert_json_kafka '{"timestamp": "2023-07-28 07:11:00", "user_id": 2, "page_id": 1, "action": "fsdfgerrg"}'
  insert_json_kafka '{"timestamp": "2023-07-28 07:11:00", "user_id": 3, "page_id": 1, "action": "sdfergtth"}'

  insert_json_kafka '{"timestamp": "2023-07-28 06:54:00", "user_id": 4, "page_id": 2, "action": "erwerhghj"}'
  insert_json_kafka '{"timestamp": "2023-07-28 06:54:00", "user_id": 5, "page_id": 2, "action": "kiku7ikkk"}'

  insert_json_kafka '{"timestamp": "2023-07-28 06:54:00", "user_id": 6, "page_id": 3, "action": "6786745ge"}'
  insert_json_kafka '{"timestamp": "2023-07-28 06:54:00", "user_id": 7, "page_id": 3, "action": "fgbgfnyyy"}'

  insert_json_kafka '{"timestamp": "2023-07-28 06:54:00", "user_id": 8, "page_id": 4, "action": "werwerwwe"}'
  insert_json_kafka '{"timestamp": "2023-07-28 06:54:00", "user_id": 9, "page_id": 4, "action": "yjtyjtyyy"}'
}

query_kafka_source() {
run_sql_rw "SELECT * FROM source_kafka;"
}

create_mv() {
run_sql_rw "
CREATE MATERIALIZED VIEW view_kafka_join_postgres AS
SELECT page_id,
timestamp as timestamp,
action as action,
user_id as user_id,
obj_id AS obj_id,
name AS name,
age as age
FROM source_kafka join source_postgres on source_kafka.page_id=source_postgres.obj_id
where (timestamp - INTERVAL '1 minute') > now();
"
}

query_mv() {
run_sql_rw "SELECT * FROM view_kafka_join_postgres;"
}

create_kafka_sink_from_mv() {
run_sql_rw "CREATE SINK sink_kafka_from_view_kafka_join_postgres FROM view_kafka_join_postgres
WITH (
 connector='kafka',
 type='append-only',
 force_append_only='true',
 properties.bootstrap.server='localhost:29092',
 topic='sink_kafka'
);"
}

create_source_from_kafka_sink() {
run_sql_rw "CREATE SOURCE IF NOT EXISTS source_sink_kafka
(
 action varchar,
 user_id integer,
 obj_id integer,
 name varchar,
 page_id integer,
 age integer
)
WITH (
 connector='kafka',
 topic='sink_kafka',
 properties.bootstrap.server='localhost:29092',
 scan.startup.mode='earliest',
) FORMAT PLAIN ENCODE JSON;"
}

create_pg_sink_from_mv() {
run_sql_pg "DROP TABLE IF EXISTS test2;"
run_sql_pg "CREATE TABLE test2(
  page_id integer,
  timestamp timestamp,
  action varchar,
  user_id integer,
  obj_id integer,
  name varchar,
  age integer
);"

run_sql_rw "CREATE SINK sink_mysql_view_kafka_join_postgres from view_kafka_join_postgres WITH (
 connector='jdbc',
 jdbc.url='jdbc:postgresql://localhost:5432/postgres?user=postgres&password=abc',
 table.name='test2',
 force_append_only='true',
 type='append-only'
);"
run_sql_pg "select * from test2;"
}

adhoc_sink_test() {
run_sql_pg "DROP TABLE IF EXISTS test3;"
run_sql_pg "CREATE TABLE test3(obj_id integer primary key, name varchar, age integer);"
run_sql_rw "
CREATE TABLE test(obj_id integer primary key, name varchar, age integer);

CREATE SINK test_sink from test WITH (
    connector='jdbc',
    jdbc.url='jdbc:postgresql://localhost:5432/postgres?user=postgres&password=abc',
    table.name='test3',
    type='upsert'
);

insert into test values (1, '123', 1);
"
}

################## TEST

# Start postgres first for CDC.
# Start it via a docker service.
set +e
make_pg
set -e
start_pg

# Configure it for CDC
configure_postgres

# Make sure clean slate.
set +e
./risedev k
./risedev clean-data
$KAFKA_PATH/bin/zookeeper-server-stop.sh
$KAFKA_PATH/bin/kafka-server-stop.sh
set -e

# Start the full cluster.
RISEDEV_ENABLE_HEAP_PROFILE=1 ./risedev d full-with-connector

# Create tables on rw side for postgres CDC.
create_cdc_source
insert_cdc_source
sleep 5
query_cdc_source

# Create tables on rw side for Kafka.
# NOTE: You should have installed Kafka via RW.
#"$KAFKA_PATH"/bin/zookeeper-server-start.sh \
# "$KAFKA_PATH"/config/zookeeper.properties &
#sleep 5
#"$KAFKA_PATH"/bin/kafka-server-start.sh \
# "$KAFKA_PATH"/config/server.properties &
#sleep 5
"$KAFKA_PATH"/bin/kafka-topics.sh \
 --create \
 --topic source_kafka --bootstrap-server localhost:29092

"$KAFKA_PATH"/bin/kafka-topics.sh \
 --create \
 --topic sink_kafka --bootstrap-server localhost:29092

sleep 5

create_kafka_source
insert_kafka_source
query_kafka_source

create_mv
query_mv

create_kafka_sink_from_mv

create_source_from_kafka_sink

create_pg_sink_from_mv

adhoc_sink_test