#!/bin/bash

set -euo pipefail

# setup mysql
docker compose exec mysql bash -c "mysql -p123456 -h localhost -P 3306 mydb < mysql_prepare.sql"

echo "Deploying Debezium MySQL connector"

curl -S -X PUT -H  "Content-Type:application/json" http://localhost:8083/connectors/mysql-default/config \
    -d '{
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "topic.prefix": "mysql",
    "database.hostname": "mysql",
    "database.port": 3306,
    "database.user": "debezium",
    "database.password": "mysqlpw",
    "database.server.id": "2304321",
    "database.allowPublicKeyRetrieval": true,
    "schema.history.internal.kafka.bootstrap.servers": "message_queue:29092",
    "schema.history.internal.kafka.topic": "schema-changes.inventory",
    "time.precision.mode": "adaptive_time_microseconds",
    "decimal.handling.mode": "precise",
    "binary.handling.mode": "bytes",
    "include.schema.changes": false,
    "table.include.list": "mydb.orders,mydb.mysql_all_data_types"
 }'

echo "Deploying Debezium MySQL connector"
# time: connect
# decimal: double
curl -S -X PUT -H  "Content-Type:application/json" http://localhost:8083/connectors/mysql-connect-double/config \
    -d '{
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "topic.prefix": "mysql",
    "database.hostname": "mysql",
    "database.port": 3306,
    "database.user": "debezium",
    "database.password": "mysqlpw",
    "database.server.id": "412741",
    "database.allowPublicKeyRetrieval": true,
    "schema.history.internal.kafka.bootstrap.servers": "message_queue:29092",
    "schema.history.internal.kafka.topic": "schema-changes.inventory",
    "time.precision.mode": "connect",
    "decimal.handling.mode": "double",
    "include.schema.changes": false,
    "table.include.list": "mydb.mysql_types2"
 }'

echo "Deploying Debezium MySQL connector"
# decimal: string
curl -S -X PUT -H  "Content-Type:application/json" http://localhost:8083/connectors/mysql-string/config \
    -d '{
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "topic.prefix": "mysql",
    "database.hostname": "mysql",
    "database.port": 3306,
    "database.user": "debezium",
    "database.password": "mysqlpw",
    "database.server.id": "85092",
    "database.allowPublicKeyRetrieval": true,
    "schema.history.internal.kafka.bootstrap.servers": "message_queue:29092",
    "schema.history.internal.kafka.topic": "schema-changes.inventory",
    "decimal.handling.mode": "string",
    "include.schema.changes": false,
    "table.include.list": "mydb.mysql_types3"
 }'

echo 'sleep two minutes wait for debezium create all topics.'
sleep 120

echo 'Done'
