# Description

Feature store demo.

We use 'simulators' to simulate data input.

Then all messages will be sent to the 'server' and written in Kafka -> RisingWave. RisingWave will process the data based on pre-defined operations.

We also utilize the 'simulator' to simulate user queries to our 'feature'. The 'server' will receive requests -> query data -> and return results.

If we intend to modify our business logic, we simply need to update the materialized view within our RisingWave by using SQL statements.

## Installation

Run it in local.

1. Build docker. Kafka RisingWave and Feature Store.

```docker compose up --build```

2. Then we can get the simulation results for Feature store in `.log`.

```cat .log/simulator_log```

3. We can modify the configuration in the `Dockerfile` to use MFA example.

* Build Dependencies: librdkafka, pkg-config , openssl