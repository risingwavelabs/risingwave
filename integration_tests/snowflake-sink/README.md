# Example Use Case: Sinking to Snowflake

this tutorial (and the corresponding examples) aims at showcasing how to sink data to Snowflake in RisingWave.

## 1. Preparation

due to the SaaS nature of snowflake, sinking data to snowflake via risingWave typically has some prerequisites.

for detailed data-pipelining and sinking logic, please refer to the official documentation(s), e.g., [data load with snowpipe overview](https://docs.snowflake.com/user-guide/data-load-snowpipe-rest-overview).

### 1.1 S3 setup

users will need to setup an **external** S3 bucket first, and please make sure you have the corresponding credentials, which will be required **both** in your snowflake stage and risingwave sink creation time.

note: the required credentials including the following, i.e.,
- `snowflake.s3_bucket` (a.k.a. the `URL` in snowflake stage)
- `snowflake.aws_access_key_id` (a.k.a., the `AWS_KEY_ID` in snowflake stage)
- `snowflake.aws_secret_access_key` (a.k.a. the `AWS_SECRET_KEY` in snowflake stage)

### 1.2 Snowflake setup
You need to have a table ,a stage and a pipe. In the meantime you need to open s3's SQS queue
You can complete the above setup by https://docs.snowflake.com/en/user-guide/data-load-snowpipe-auto-s3

an example for snowflake setup commands could be checked at `snowflake_prep.sql`, this also corresponds to the following example sinking use case.


## 2. Begin to sink data

launch your risingwave cluster, and execute the following sql commands respectively.

- `create_source.sql`
- `create_mv.sql`
- `create_sink.sql`

note: the column name(s) in your materialized view should be exactly the same as the ones in your pre-defined snowflake table, due to what we specified for snowflake pipe previously in `snowflake_prep.sql`.

## 3. Sink data into snowflake with UPSERT

1. To begin the process of sink data into Snowflake with upsert, we need to set up snowflake and s3 as we did for step 1

2. Execute the following sql commands respectively.
    - `upsert/create_source.sql`
    - `upsert/create_mv.sql`
    - `upsert/create_sink.sql`

    After execution, we will import RisingWave's data change log into the snowflake's table.

3. We then use the following sql statement to create the dynamic table. We can select it to get the result of the upsert
    ```
    CREATE OR REPLACE DYNAMIC TABLE user_behaviors
    TARGET_LAG = '1 minute'
    WAREHOUSE = test_warehouse
    AS SELECT *
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY {primary_key} ORDER BY __row_id DESC) AS dedupe_id
            FROM t3
        ) AS subquery
    WHERE dedupe_id = 1 AND (__op = 1 or __op = 3)
    ```