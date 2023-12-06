CREATE DATABASE example_data
WITH ENGINE = "postgres",
PARAMETERS = {
  "user": "root",
  "host": "risingwave-standalone",
  "port": "4566",
  "database": "dev"
};
