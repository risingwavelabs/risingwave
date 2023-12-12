CREATE DATABASE example_data
WITH ENGINE = "postgres",
PARAMETERS = {
  "user": "root",
  "host": "risingwave-standalone",
  "port": "4566",
  "database": "dev"
};

CREATE MODEL mindsdb.home_rentals_model
FROM example_data
  (SELECT * FROM home_rentals)
PREDICT rental_price;
