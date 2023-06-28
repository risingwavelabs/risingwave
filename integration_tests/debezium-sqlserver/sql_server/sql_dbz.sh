#!/usr/bin/env bash

echo "Deploying Debezium SQL Server connector"

curl -s -X PUT -H "Content-Type: application/json" http://debezium:8083/connectors/register-sqlserver/config \
    -d '{
    "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
    "database.hostname": "sqlserver",
    "database.port": 1433,
    "database.user": "sa",
    "database.password": "YourPassword123",
    "database.dbname": "mydb",
    "database.server.name": "sqlserver",
    "database.history.kafka.bootstrap.servers": "message_queue:29092",
    "database.history.kafka.topic": "sqlserver-history",
    "database.history.store.only.captured.table.ddl": true
 }'
