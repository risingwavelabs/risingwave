# Requirements

1. Install cargo make.
2. Install docker.


# Commands

1. Setup pipeline: `cargo make setup`
2. Check results:
    1. Flink Posgresql: `docker exec postgres bash -c 'psql -U $POSTGRES_USER $POSTGRES_DB -c "select * from counts"'`
    2. Postgresql: `docker exec postgres2 bash -c 'psql -U $POSTGRES_USER $POSTGRES_DB -c "select * from counts"'`
    3. MySQL: `docker exec mysql bash -c 'mysql -u $MYSQL_USER  -p$MYSQL_PASSWORD mydb -e "select * from counts"'`
    4. Or use `cargo make check-db`

# Pipeline
There are two pipelines in this test:

Risingwave -(Debezium Json)-> Kafka -(Debezisum Json)-> Flink SQL --> Postgresql

Risingwave -(Debezium Json)-> Kafka -(Debezium Json)-> JDBC connector -> MySQL and Postgresql