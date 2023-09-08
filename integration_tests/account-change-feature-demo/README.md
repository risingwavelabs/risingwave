# Description

#### Feature store demo.

We use `simulators` to simulate data input.

Then all messages will be sent to the `server` and written in `Kafka` -> `RisingWave`. `RisingWave` will process the data based on pre-defined operations.

We also utilize the `simulator` to simulate user queries to our `feature`. The `server` will receive requests -> query data -> and return results.

If we intend to modify our business logic, we simply need to update the materialized view within our `RisingWave` by using SQL statements.

#### Specific case:

This chapter is a simple demo of feature extraction in `RisingWave`, primarily showcasing the real-time feature aggregation and updating capabilities of `RisingWave`.

In this case, we need to calculate the frequency and count of user account changes over a period of time. We mainly use SQL aggregation functions and UDFs (User-Defined Functions) to achieve this.

Due to the similarity between the code in this demo and another code, the implementation code is located in the `nyc-taxi-feature-store-demo` folder.

## Installation

Run it in local.

1. Build docker. Kafka RisingWave and Feature Store.

```docker compose up --build```

2. Then we can get the simulation results for Feature store in `.log`.

```cat .log/simulator_log```