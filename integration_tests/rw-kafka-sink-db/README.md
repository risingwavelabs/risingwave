# Requirements

1. Install cargo make.
2. Install docker.
3. Install psql.


# Commands

1. Clean all states: `cargo make clean-all`
2. Setup pipeline: `cargo make setup`
3. Check results in postgresql: `PGPASSWORD=123456 psql -h localhost -p 5432 -U myuser mydb`
4. Check results in mysql: 

# Pipeline

Risingwave -(Debezisum Json)-> Kafka -> JDBC connector --> PostgreSQL/MySQL
