# Description

Feature store demo.

We use `simulators` to simulate data input.

Then all messages will be sent to the `server` and written in `Kafka` -> `RisingWave`. `RisingWave` will process the data based on pre-defined operations.

We also utilize the `simulator` to simulate user queries to our `feature`. The `server` will receive requests -> query data -> and return results.

If we intend to modify our business logic, we simply need to update the materialized view within our `RisingWave` by using SQL statements.

# Nyc taxi feature store
#### Case Description

The case in this chapter is a New York taxi fare prediction. We need to predict the taxi fare based on the starting and ending points of the trip.

We use the starting and ending points as primary keys, extract and transform corresponding features, and save them in `RisingWave`. These features are updated based on user behavior.

When a user needs to make a prediction using these features, they can retrieve all the features for training.

When a user needs to make a prediction using these features, they can provide their starting and ending points, query the corresponding features in `RisingWave`, and inject them into a machine learning model for real-time fare prediction.

#### Installation

1. Build docker. Kafka RisingWave and Feature Store.

```docker compose up --build```


The Feature Store system performs several tasks in sequence:

- Writes simulated offline data into `Kafka`→`RisingWave`, extracting behavior and feature tables.

- Joins the behavior and feature tables to obtain corresponding offline training data and conducts model training.

- Writes simulated online feature data into `Kafka`→`RisingWave`.

- Uses `do_location_id` (destination location) and `pu_location_id` (pickup location) to query the latest online features in RisingWave and utilizes these online features along with the trained model for predictions.

2. Then we can get the simulation results for Feature store in `.log`.

```cat .log/simulator_log```

# Account change feature store
#### Case Description

This chapter is a simple demo of feature extraction in `RisingWave`, primarily showcasing the real-time feature aggregation and updating capabilities of `RisingWave`.

In this case, we need to calculate the frequency and count of user account changes over a period of time. We mainly use SQL aggregation functions and UDFs (User-Defined Functions) to achieve this.

#### Installation

1. Build docker. Kafka RisingWave and Feature Store.

```docker compose build --build-arg BUILD_ARG=mfa```

2. Then we can get the simulation results for Feature store in `.log`.

```cat .log/simulator_log```