# Connectors

RisingWave's SQL programs can be connected to external systems for reading and writing data chunks. The `Source` operator can access data from external systems (such as a database, key-value store, message queue, or file system) as input with the help of various `connector`s. Depending on the type of operators, they support different formats such as CSV and Avro.

## Supported Connectors

RisingWave natively supports various connectors as below.

|Name|Version|Source|
|---|---|---|
|Local File System|Na|Bounded and Unbounded Scan, Lookup|
|Apache Kafka|1.17+|Unbounded Scan|
|Apache Pulsar|`TBD`|Unbounded Scan|
|Amazon Kinesis Data Streams|Na|Unbounded Scan|
|Amazon S3|Na|Bounded and Unbounded Scan, Lookup|
|Debezium|`TBD`|Unbounded Scan|
|RedPanda|`TBD`|Unbounded Scan|

## How to use connectors

RisingWave supports using SQL `` to register source. One can define the source name, the source schema, and the options for connecting to an external system.

The following code shows a full example of how to connect to Kafka for reading and writing JSON records.

```SQL
create source part (
    p_partkey INTEGER NOT NULL,
    p_name VARCHAR(55) NOT NULL,
    p_mfgr CHAR(25) NOT NULL,
) with (
    'upstream.source' = 'kafka',
    'kafka.topic' = 'topic_name',
    'kafka.bootstrap.servers' = 'localhost:29092' )
row format 'json' -- declare a format
```
