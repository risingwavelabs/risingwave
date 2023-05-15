# Requirements

1. Install cargo make.
2. Install docker.

# Commands

To test, in `../scripts` folder, run `python3 run_demos.py --case rw-kafka-sink-db --format json`

To test on local newly built RisingWave:
1. Start RisingWave
2. `cargo make setup-local`
3. `cargo make check-db`

# Pipeline

Risingwave -(Debezisum Json)-> Kafka -> JDBC connector --> PostgreSQL/MySQL
