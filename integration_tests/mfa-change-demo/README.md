# Description

Feature store demo.

We use 'simulators' to simulate data input.

Then all messages will be sent to the 'server' and written in Kafka -> RisingWave. RisingWave will process the data based on pre-defined operations.

We also utilize the 'simulator' to simulate user queries to our 'feature'. The 'server' will receive requests -> query data -> and return results.

If we intend to modify our business logic, we simply need to update the materialized view within our RisingWave by using SQL statements.

## Installation

Run it in local.

1. Start kafka and risingwave. Then create source and mv in risingwave. can run this script

```./run_local.sh```

We have the option to modify the mfa-start.sql file and the server/src/serving directory to accommodate new requirements or make modifications.

2. Start feature store serve demo. It can accept data writing to Kafka and provide feature query services

```python3 server/model ```
```cd server ```
```cargo run -- --brokers localhost:9092 --output-topics taxi```

We can adjust the configuration to utilize Python for executing SQL queries (which might be more convenient, especially if deep learning needs to be incorporated).

3. Run data simulator.

```cd simulator```
```cargo run -- --types taxi```

4. We also can run mfa demo.

```cd server ```
```cargo run -- --brokers localhost:9092 --output-topics mfa```
```cd simulator```
```cargo run -- --types mfa```

* Build Dependencies: librdkafka, pkg-config , openssl