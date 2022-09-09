curl -H 'Content-Type: application/json' debezium:8083/connectors --data '
{
  "name": "simple-connector",  
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector", 
    "plugin.name": "pgoutput",
    "database.hostname": "postgres", 
    "database.port": "5432", 
    "database.user": "user", 
    "database.password": "pw", 
    "database.dbname" : "simple_db", 
    "database.server.name": "postgres", 
    "table.include.list": "public.t",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter"
  }
}'
