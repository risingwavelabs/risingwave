curl -H 'Content-Type: application/json' debezium:8083/connectors --data '
{
  "name": "postgres-connector",  
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "plugin.name": "pgoutput",
    "database.hostname": "postgres", 
    "database.port": "5432", 
    "database.user": "user", 
    "database.password": "pw", 
    "database.dbname" : "ch_benchmark_db", 
    "database.server.name": "postgres",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter"
  }
}'
