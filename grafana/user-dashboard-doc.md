## Actor/Table Id Info

- **Actor Info**:

	Type: table

	Information about actors

- **State Table Info**:

	Type: table

	Information about state tables. Column `materialized_view_id` is the id of the materialized view that this state table belongs to.

## Overview

- **Source Throughput(rows/s)**:

	Type: timeseries

	The figure shows the number of rows read by each source per second.

- **Source Throughput(MB/s)**:

	Type: timeseries

	The figure shows the number of bytes read by each source per second.

- **Sink Throughput(rows/s)**:

	Type: timeseries

	The number of rows streamed into each sink per second.

- **Materialized View Throughput(rows/s)**:

	Type: timeseries

	The figure shows the number of rows written into each materialized view per second.

- **Barrier Latency**:

	Type: timeseries

	The time that the data between two consecutive barriers gets fully processed, i.e. the computation results are made durable into materialized views or sink to external systems. This metric shows to users the freshness of materialized views.

- **Alerts**:

	Type: timeseries

	Alerts in the system group by type:
    - Too Many Barriers: there are too many uncommitted barriers generated. This means the streaming graph is stuck or under heavy load. Check 'Barrier Latency' panel.
    - Recovery Triggered: cluster recovery is triggered. Check 'Errors by Type' / 'Node Count' panels.
    - Lagging Version: the checkpointed or pinned version id is lagging behind the current version id. Check 'Hummock Manager' section in dev dashboard.
    - Lagging Epoch: the pinned or safe epoch is lagging behind the current max committed epoch. Check 'Hummock Manager' section in dev dashboard.
    - Lagging Compaction: there are too many files in L0. This can be caused by compactor failure or lag of compactor resource. Check 'Compaction' section in dev dashboard.
    - Lagging Vacuum: there are too many stale files waiting to be cleaned. This can be caused by compactor failure or lag of compactor resource. Check 'Compaction' section in dev dashboard.
    - Abnormal Meta Cache Memory: the meta cache memory usage is too large, exceeding the expected 10 percent.
    - Abnormal Block Cache Memory: the block cache memory usage is too large, exceeding the expected 10 percent.
    - Abnormal Uploading Memory Usage: uploading memory is more than 70 percent of the expected, and is about to spill.
    - Write Stall: Compaction cannot keep up. Stall foreground write.

- **Errors**:

	Type: timeseries

	Errors in the system group by type

- **Batch Query QPS**:

	Type: timeseries

- **Node Count**:

	Type: timeseries

	The number of each type of RisingWave components alive.

- **Active Sessions**:

	Type: timeseries

	Number of active sessions in frontend nodes

## CPU

- **Node CPU Usage**:

	Type: timeseries

	The CPU usage of each RisingWave component.

- **Node CPU Core Number**:

	Type: timeseries

	Number of CPU cores per RisingWave component.

## Memory

- **Node Memory**:

	Type: timeseries

	The memory usage of each RisingWave component.

- **Memory Usage (Total)**:

	Type: timeseries

- **Memory Usage (Detailed)**:

	Type: timeseries

- **Executor Cache**:

	Type: timeseries

	Executor cache statistics

- **Executor Cache Miss Ratio**:

	Type: timeseries

- **Storage Cache**:

	Type: timeseries

	Storage cache statistics

- **Storage Bloom Filer**:

	Type: timeseries

	Storage bloom filter statistics

- **Storage File Cache**:

	Type: timeseries

	Storage file cache statistics

## Network

- **Streming Remote Exchange (Bytes/s)**:

	Type: timeseries

	Send/Recv throughput per node for streaming exchange

- **Storage Remote I/O (Bytes/s)**:

	Type: timeseries

	The remote storage read/write throughput per node

- **Batch Exchange Recv (Rows/s)**:

	Type: timeseries

## Storage

- **Object Size**:

	Type: timeseries

	Objects are classified into 3 groups:
    - not referenced by versions: these object are being deleted from object store.
    - referenced by non-current versions: these objects are stale (not in the latest version), but those old versions may still be in use (e.g. long-running pinning). Thus those objects cannot be deleted at the moment.
    - referenced by current version: these objects are in the latest version.

- **Materialized View Size**:

	Type: timeseries

	The storage size of each materialized view

- **Object Total Number**:

	Type: timeseries

	Objects are classified into 3 groups:
    - not referenced by versions: these object are being deleted from object store.
    - referenced by non-current versions: these objects are stale (not in the latest version), but those old versions may still be in use (e.g. long-running pinning). Thus those objects cannot be deleted at the moment.
    - referenced by current version: these objects are in the latest version.

- **Write Bytes**:

	Type: timeseries

	The number of bytes that have been written by compaction.Flush refers to the process of compacting Memtables to SSTables at Level 0.Compaction refers to the process of compacting SSTables at one level to another level.

- **Storage Remote I/O (Bytes/s)**:

	Type: timeseries

	The remote storage read/write throughput

- **Checkpoint Size**:

	Type: timeseries

	Size statistics for checkpoint

## Streaming

- **Source Throughput(rows/s)**:

	Type: timeseries

	The figure shows the number of rows read by each source per second.

- **Source Throughput(MB/s)**:

	Type: timeseries

	The figure shows the number of bytes read by each source per second.

- **Source Backfill Throughput(rows/s)**:

	Type: timeseries

	The figure shows the number of rows read by each source per second.

- **Materialized View Throughput(rows/s)**:

	Type: timeseries

	The figure shows the number of rows written into each materialized executor actor per second.

- **Backfill Throughput(rows)**:

	Type: timeseries

	Total number of rows that have been read from the backfill operator used by MV on MV

- **Actor Output Blocking Time Ratio (Backpressure)**:

	Type: timeseries

	We first record the total blocking duration(ns) of output buffer of each actor. It shows how much time it takes an actor to process a message, i.e. a barrier, a watermark or rows of data, on average. Then we divide this duration by 1 second and show it as a percentage.

## Batch

- **Running query in distributed execution mode**:

	Type: timeseries

- **Rejected query in distributed execution mode**:

	Type: timeseries

- **Completed query in distributed execution mode**:

	Type: timeseries

- **Query Latency in Distributed Execution Mode**:

	Type: timeseries

- **Query Latency in Local Execution Mode**:

	Type: timeseries

## Connector Node

- **Connector Source Throughput(rows)**:

	Type: timeseries

- **Connector Sink Throughput(rows)**:

	Type: timeseries

