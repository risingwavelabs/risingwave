CREATE DATABASE example_data
WITH ENGINE = "postgres",
PARAMETERS = {
  "user": "root",
  "host": "frontend-node-0",
  "port": "4566",
  "database": "dev"
};
