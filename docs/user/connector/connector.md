# Connectors

RisingWave's SQL programs can be connected to external systems for reading and writing data chunks. The `Source` operator, which defines the data structure, can access data from external systems (such as a database, key-value store, message queue, or file system) as input with the help of various `connector`s. Depending on the type of operators, they support different formats such as CSV and Avro.

Also the `Source` operator maintains the schema of the data source, and RisingWave supports queries on sources just like tables.

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

Connectors' properties are configured by string-based key-value pairs and factories will build corresponding sources and connectors. Only one factory can be configured for one source operator, otherwise, an exception will be thrown with additional information about considered factories and related properties.

## Implementation (beta)

> The current design and implementation of the connector is still in progress and may result in breaking change in the future.

A connector is mainly made up of at least one Split Readers and an Enumerator. All components are listed below.

* **SplitReader**: pulls data from one or more slices (like partition in Kafka) of the upstream
* **Enumerator**: watches the upstream splits and passes them to the `Source` and the `Source` specifies for each `SplitReader` which split to consume
* **SourceSplit**: the data format used when assigning tasks to the `SplitReader` and contains the identifier and start/stop position of the slice to be consumed
* **SourceMessage**: represents the format of one upstream message, allowing user-defined methods for conversion to bytes
* **State Store**: the checkpoint for one `SplitReader` and if there are any errors downstream the system will roll back from the latest checkpoint

User should implement all traits defined in `risingwave/rust/connector/src/base.rs` for a new connector.

### State Store

Each `SplitReader` maintains its state and takes a snapshot when a barrier is received and tags it with the epoch brought by the barrier. Each snapshot should contain the identifier of the currently consumed slice and the offset of the most recent successful consumption. The snapshot is persisted by `SourceStateHandler` and tagged with the epoch brought by the barrier.

If there is an error downstream, the system finds the last successful epoch and rolls back to the state at that time.
