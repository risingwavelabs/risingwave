#!/bin/bash

set -euo pipefail

# setup postgres
docker compose exec postgres bash -c "psql postgresql://postgresuser:postgrespw@postgres:5432/mydb < postgres_prepare.sql"

echo "Deploying Debezium Postgres connector"

# default handling mode
curl -S -X PUT -H "Content-Type: application/json" http://localhost:8083/connectors/pg-default/config \
  -d '{
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "topic.prefix": "pg",
    "database.hostname": "postgres",
    "database.port": 5432,
    "database.user": "postgresuser",
    "database.password": "postgrespw",
    "database.dbname": "mydb",
    "database.server.name": "postgres",
    "database.schema": "public",
    "database.history.kafka.bootstrap.servers": "message_queue:29092",
    "database.history.kafka.topic": "postgres-history",
    "time.precision.mode": "adaptive",
    "decimal.handling.mode": "precise",
    "interval.handling.mode": "numeric",
    "include.schema.changes": false,
    "slot.name": "debezium_1",
    "table.include.list": "public.orders,public.pg_all_data_types"
}'

echo "Deploying Debezium Postgres connector"
# time: adaptive_time_microseconds
# interval: string
# decimal: double
curl -S -X PUT -H "Content-Type: application/json" http://localhost:8083/connectors/pg-double-microseconds/config \
  -d '{
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "topic.prefix": "pg",
    "database.hostname": "postgres",
    "database.port": 5432,
    "database.user": "postgresuser",
    "database.password": "postgrespw",
    "database.dbname": "mydb",
    "database.server.name": "postgres",
    "database.schema": "public",
    "database.history.kafka.bootstrap.servers": "message_queue:29092",
    "database.history.kafka.topic": "postgres-history",
    "time.precision.mode": "adaptive_time_microseconds",
    "interval.handling.mode": "string",
    "decimal.handling.mode": "double",
    "include.schema.changes": false,
    "slot.name": "debezium_2",
    "table.include.list": "public.pg_types2"
}'

echo "Deploying Debezium Postgres connector"
# time: connnect
# decimal: string
curl -S -X PUT -H "Content-Type: application/json" http://localhost:8083/connectors/pg-connect-string/config \
  -d '{
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "topic.prefix": "pg",
    "database.hostname": "postgres",
    "database.port": 5432,
    "database.user": "postgresuser",
    "database.password": "postgrespw",
    "database.dbname": "mydb",
    "database.server.name": "postgres",
    "database.schema": "public",
    "database.history.kafka.bootstrap.servers": "message_queue:29092",
    "database.history.kafka.topic": "postgres-history",
    "time.precision.mode": "connect",
    "decimal.handling.mode": "string",
    "include.schema.changes": false,
    "slot.name": "debezium_3",
    "table.include.list": "public.pg_types3"
}'

echo 'sleep one minute wait for debezium create all topics.'
sleep 60

echo 'Done'
