# Requirements

1. Install cargo make.
2. Install docker.

# Commands

1. Setup pipeline: `cargo make setup`
2. Check result: `cargo make check-db` 

If you want to test on local RisingWave:
1. Start RisingWave
2. `cargo make setup-local`
3. `cargo make check-db`

# Pipeline

Risingwave -(Debezisum Json)-> Kafka -> JDBC connector --> PostgreSQL/MySQL
