#!/usr/bin/env bash

echo "Deploying Debezium MySQL connector"

curl -s -X PUT -H  "Content-Type:application/json" http://debezium:8083/connectors/register-mysql/config \
    -d '{
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "database.hostname": "mysql",
    "database.port": 3306,
    "database.user": "debezium",
    "database.password": "mysqlpw",
    "database.server.name": "mysql",
    "database.server.id": "1",
    "database.allowPublicKeyRetrieval": true,
    "database.history.kafka.bootstrap.servers":"message_queue:29092",
    "database.history.kafka.topic": "mysql-history",
    "database.include.list": "mydb",
    "time.precision.mode": "connect",
    "include.schema.changes": false
 }'
