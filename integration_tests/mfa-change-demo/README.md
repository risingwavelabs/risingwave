# Descriptive

Feature store dome.

We simulate functionality of feature store demo by using a simple business scenario. We use 'simulator' to simulate a constant stream of user modifying mfa. 

Then all message will be send to 'server', and write in kafka -> risingwave. Risingwave will compute the data based on pre-defined operations. (In this demo, we will calculate the 30-minute mfa change count and the sum of accumulated user changes in 30-minute).

We also use 'simulator' to simulate user query our 'feature'. The 'server' will accept requests -> query data -> return results.

If we want to modify our business logic, we only need to modify the materialized view in our risingwave (use sql statement).

## Installation

Run it in local.

1. Start kafka and risingwave. Then create source and mv in risingwave. can run this script

```./run.sh```

we can change recwave-start.sql and server/src/serving to add new demands or modift it.

2. Start feature store serve demo. It can accept data writing to Kafka and provide feature query services

```cd server```
```cargo run ```

We can change the configuration to use py to execute sql (It might be easier if we need to use deep learning) 

3. Run data simulator. It can generate simulated data and simulate user queries.

```cd simulator```
```cargo run```

* Recommender Pipeline
    * Build Dependencies: librdkafka, pkg-config , openssl




