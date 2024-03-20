#!/bin/bash

set -euo pipefail

# setup sqlserver
docker compose exec sqlserver bash -c "/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P YourPassword123 -d master -Q 'create database mydb;'"
docker compose exec sqlserver bash -c "/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P YourPassword123 -d mydb -i /sqlserver_prepare.sql"

echo "Deploying Debezium Sqlserver connector"

# default handling mode
curl -S -X PUT -H "Content-Type: application/json" http://localhost:8083/connectors/sqlserver-default/config \
  -d '{
    "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
    "topic.prefix": "sqlserver",
    "database.hostname": "sqlserver",
    "database.port": 1433,
    "database.user": "sa",
    "database.password": "YourPassword123",
    "database.names": "mydb",
    "schema.history.internal.kafka.bootstrap.servers": "message_queue:29092",
    "schema.history.internal.kafka.topic": "sqlserver-history",
    "schema.history.internal.store.only.captured.table.ddl": true,
    "database.encrypt": false,
    "decimal.handling.mode": "precise",
    "binary.handling.mode": "bytes",
    "time.precision.mode": "adaptive",
    "table.include.list": "dbo.orders,dbo.sqlserver_all_data_types"
}'

echo 'Done'
