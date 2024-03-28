## Actor/Table Id Info

- **Actor Info**:

	Type: table

	Information about actors

- **State Table Info**:

	Type: table

	Information about state tables. Column `materialized_view_id` is the id of the materialized view that this state table belongs to.

## Cluster Node

- **Node Count**:

	Type: timeseries

	The number of each type of RisingWave components alive.

- **Node Memory**:

	Type: timeseries

	The memory usage of each RisingWave component.

- **Node CPU**:

	Type: timeseries

	The CPU usage of each RisingWave component.

- **Meta Cluster**:

	Type: timeseries

	RW cluster can configure multiple meta nodes to achieve high availability. One is the leader and the rest are the followers.

## Recovery

- **Recovery Successful Rate**:

	Type: timeseries

	The rate of successful recovery attempts

- **Failed recovery attempts**:

	Type: timeseries

	Total number of failed reocovery attempts

- **Recovery latency**:

	Type: timeseries

	Time spent in a successful recovery attempt

## Streaming

- **Source Throughput(rows/s)**:

	Type: timeseries

	The figure shows the number of rows read by each source per second.

- **Source Throughput(rows/s) Per Partition**:

	Type: timeseries

	Each query is executed in parallel with a user-defined parallelism. This figure shows the throughput of each parallelism. The throughput of all the parallelism added up is equal to Source Throughput(rows).

- **Source Throughput(MB/s)**:

	Type: timeseries

	The figure shows the number of bytes read by each source per second.

- **Source Throughput(MB/s) Per Partition**:

	Type: timeseries

	Each query is executed in parallel with a user-defined parallelism. This figure shows the throughput of each parallelism. The throughput of all the parallelism added up is equal to Source Throughput(MB/s).

- **Source Backfill Throughput(rows/s)**:

	Type: timeseries

	The figure shows the number of rows read by each source per second.

- **Source Upstream Status**:

	Type: timeseries

	Monitor each source upstream, 0 means the upstream is not normal, 1 means the source is ready.

- **Source Split Change Events frequency(events/s)**:

	Type: timeseries

	Source Split Change Events frequency by source_id and actor_id

- **Kafka Consumer Lag Size**:

	Type: timeseries

	Kafka Consumer Lag Size by source_id, partition and actor_id

- **Sink Throughput(rows/s)**:

	Type: timeseries

	The number of rows streamed into each sink per second.

- **Sink Throughput(rows/s) per Partition**:

	Type: timeseries

	The number of rows streamed into each sink per second.

- **Materialized View Throughput(rows/s)**:

	Type: timeseries

	The figure shows the number of rows written into each materialized view per second.

- **Materialized View Throughput(rows/s) per Partition**:

	Type: timeseries

	The figure shows the number of rows written into each materialized view per second.

- **Backfill Snapshot Read Throughput(rows)**:

	Type: timeseries

	Total number of rows that have been read from the backfill snapshot

- **Backfill Upstream Throughput(rows)**:

	Type: timeseries

	Total number of rows that have been output from the backfill upstream

- **Barrier Number**:

	Type: timeseries

	The number of barriers that have been ingested but not completely processed. This metric reflects the current level of congestion within the system.

- **Barrier Send Latency**:

	Type: timeseries

	The duration between the time point when the scheduled barrier needs to be sent and the time point when the barrier gets actually sent to all the compute nodes. Developers can thus detect any internal congestion.

- **Barrier Latency**:

	Type: timeseries

	The time that the data between two consecutive barriers gets fully processed, i.e. the computation results are made durable into materialized views or sink to external systems. This metric shows to users the freshness of materialized views.

- **Barrier In-Flight Latency**:

	Type: timeseries

- **Barrier Sync Latency**:

	Type: timeseries

- **Barrier Wait Commit Latency**:

	Type: timeseries

- **Earliest In-Flight Barrier Progress**:

	Type: timeseries

	The number of actors that have processed the earliest in-flight barriers per second. This metric helps users to detect potential congestion or stuck in the system.

## Streaming CDC

- **CDC Backfill Snapshot Read Throughput(rows)**:

	Type: timeseries

	Total number of rows that have been read from the cdc backfill snapshot

- **CDC Backfill Upstream Throughput(rows)**:

	Type: timeseries

	Total number of rows that have been output from the cdc backfill upstream

- **CDC Consume Lag Latency**:

	Type: timeseries

- **CDC Source Errors**:

	Type: timeseries

## Streaming Actors

- **Actor Output Blocking Time Ratio (Backpressure)**:

	Type: timeseries

	We first record the total blocking duration(ns) of output buffer of each actor. It shows how much time it takes an actor to process a message, i.e. a barrier, a watermark or rows of data, on average. Then we divide this duration by 1 second and show it as a percentage.

- **Actor Input Blocking Time Ratio**:

	Type: timeseries

- **Actor Input Throughput (rows/s)**:

	Type: timeseries

- **Actor Output Throughput (rows/s)**:

	Type: timeseries

- **Executor Cache Memory Usage**:

	Type: timeseries

	The operator-level memory usage statistics collected by each LRU cache

- **Executor Cache Memory Usage of Materialized Views**:

	Type: timeseries

	Memory usage aggregated by materialized views

- **Temporal Join Executor Cache**:

	Type: timeseries

- **Materialize Executor Cache**:

	Type: timeseries

- **Over Window Executor Cache**:

	Type: timeseries

- **Executor Cache Miss Ratio**:

	Type: timeseries

- **Join Executor Barrier Align**:

	Type: timeseries

- **Join Actor Input Blocking Time Ratio**:

	Type: timeseries

- **Join Actor Match Duration Per Second**:

	Type: timeseries

- **Join Cached Keys**:

	Type: timeseries

	Multiple rows with distinct primary keys may have the same join key. This metric counts the number of join keys in the executor cache.

- **Join Executor Matched Rows**:

	Type: timeseries

	The number of matched rows on the opposite side

- **Aggregation Executor Cache Statistics For Each StreamChunk**:

	Type: timeseries

- **Aggregation Cached Keys**:

	Type: timeseries

	The number of keys cached in each hash aggregation executor's executor cache.

- **Aggregation Dirty Groups Count**:

	Type: timeseries

	The number of dirty (unflushed) groups in each hash aggregation executor's executor cache.

- **Aggregation Dirty Groups Heap Size**:

	Type: timeseries

	The total heap size of dirty (unflushed) groups in each hash aggregation executor's executor cache.

- **TopN Cached Keys**:

	Type: timeseries

	The number of keys cached in each top_n executor's executor cache.

- **Temporal Join Cache Keys**:

	Type: timeseries

	The number of keys cached in temporal join executor's executor cache.

- **Lookup Cached Keys**:

	Type: timeseries

	The number of keys cached in lookup executor's executor cache.

- **Over Window Cached Keys**:

	Type: timeseries

	The number of keys cached in over window executor's executor cache.

- **Executor Throughput**:

	Type: timeseries

	When enabled, this metric shows the input throughput of each executor.

- **Actor Memory Usage (TaskLocalAlloc)**:

	Type: timeseries

	The actor-level memory usage statistics reported by TaskLocalAlloc. (Disabled by default)

## Streaming Actors (Tokio)

- **Actor Execution Time**:

	Type: timeseries

- **Tokio: Actor Fast Poll Time**:

	Type: timeseries

- **Tokio: Actor Fast Poll Count**:

	Type: timeseries

- **Tokio: Actor Fast Poll Avg Time**:

	Type: timeseries

- **Tokio: Actor Slow Poll Total Time**:

	Type: timeseries

- **Tokio: Actor Slow Poll Count**:

	Type: timeseries

- **Tokio: Actor Slow Poll Avg Time**:

	Type: timeseries

- **Tokio: Actor Poll Total Time**:

	Type: timeseries

- **Tokio: Actor Poll Count**:

	Type: timeseries

- **Tokio: Actor Poll Avg Time**:

	Type: timeseries

- **Tokio: Actor Idle Total Time**:

	Type: timeseries

- **Tokio: Actor Idle Count**:

	Type: timeseries

- **Tokio: Actor Idle Avg Time**:

	Type: timeseries

- **Tokio: Actor Scheduled Total Time**:

	Type: timeseries

- **Tokio: Actor Scheduled Count**:

	Type: timeseries

- **Tokio: Actor Scheduled Avg Time**:

	Type: timeseries

## Streaming Exchange

- **Fragment-level Remote Exchange Send Throughput**:

	Type: timeseries

- **Fragment-level Remote Exchange Recv Throughput**:

	Type: timeseries

## User Streaming Errors

- **Compute Errors by Type**:

	Type: timeseries

	Errors that happened during computation. Check the logs for detailed error message.

- **Source Errors by Type**:

	Type: timeseries

	Errors that happened during source data ingestion. Check the logs for detailed error message.

- **Sink Errors by Type**:

	Type: timeseries

	Errors that happened during data sink out. Check the logs for detailed error message.

## Batch Metrics

- **Exchange Recv Row Number**:

	Type: timeseries

- **Batch Mpp Task Number**:

	Type: timeseries

- **Batch Mem Usage**:

	Type: timeseries

	All memory usage of batch executors in bytes

- **Batch Heartbeat Worker Number**:

	Type: timeseries

- **Row SeqScan Next Duration**:

	Type: timeseries

## Hummock (Read)

- **Cache Ops**:

	Type: timeseries

- **Cache Size**:

	Type: timeseries

	Hummock has three parts of memory usage: 1. Meta Cache 2. Block CacheThis metric shows the real memory usage of each of these three caches.

- **Cache Miss Rate**:

	Type: timeseries

- **Iter keys flow**:

	Type: timeseries

- **Read Merged SSTs**:

	Type: timeseries

- **Read Duration - Get**:

	Type: timeseries

	Histogram of the latency of Get operations that have been issued to the state store.

- **Read Duration - Iter**:

	Type: timeseries

	Histogram of the time spent on iterator initialization.Histogram of the time spent on iterator scanning.

- **Bloom Filter Ops**:

	Type: timeseries

- **Bloom Filter Positive Rate**:

	Type: timeseries

	Positive / Total

- **Bloom Filter False-Positive Rate**:

	Type: timeseries

	False-Positive / Total

- **Slow Fetch Meta Unhits**:

	Type: timeseries

- **Read Ops**:

	Type: timeseries

- **Read Item Size - Get**:

	Type: timeseries

- **Read Item Size - Iter**:

	Type: timeseries

- **Materialized View Read Size**:

	Type: timeseries

- **Read Item Count - Iter**:

	Type: timeseries

- **Read Throughput - Get**:

	Type: timeseries

	The size of a single key-value pair when reading by operation Get.Operation Get gets a single key-value pair with respect to a caller-specified key. If the key does not exist in the storage, the size of key is counted into this metric and the size of value is 0.

- **Read Throughput - Iter**:

	Type: timeseries

	The size of all the key-value paris when reading by operation Iter.Operation Iter scans a range of key-value pairs.

- **Fetch Meta Duration**:

	Type: timeseries

- **Fetch Meta Unhits**:

	Type: timeseries

## Hummock (Write)

- **Uploader Memory Size**:

	Type: timeseries

	This metric shows the real memory usage of uploader.

- **Build and Sync Sstable Duration**:

	Type: timeseries

	Histogram of time spent on compacting shared buffer to remote storage.

- **Materialized View Write Size**:

	Type: timeseries

- **Uploader - Tasks Count**:

	Type: timeseries

- **Uploader - Task Size**:

	Type: timeseries

- **Write Ops**:

	Type: timeseries

- **Write Duration**:

	Type: timeseries

- **Write Item Count**:

	Type: timeseries

- **Write Throughput**:

	Type: timeseries

- **Write Batch Size**:

	Type: timeseries

	This metric shows the statistics of mem_table size on flush. By default only max (p100) is shown.

- **Mem Table Spill Count**:

	Type: timeseries

- **Checkpoint Sync Size**:

	Type: timeseries

- **Event handler pending event number**:

	Type: timeseries

- **Event handle latency**:

	Type: timeseries

## Compaction

- **SSTable Count**:

	Type: timeseries

	The number of SSTables at each level

- **SSTable Size(KB)**:

	Type: timeseries

	The size(KB) of SSTables at each level

- **Commit Flush Bytes by Table**:

	Type: timeseries

	The  of bytes that have been written by commit epoch per second.

- **Compaction Failure Count**:

	Type: timeseries

	The number of compactions from one level to another level that have completed or failed

- **Compaction Success Count**:

	Type: timeseries

	The number of compactions from one level to another level that have completed or failed

- **Compaction Skip Count**:

	Type: timeseries

	The number of compactions from one level to another level that have been skipped.

- **Compaction Task L0 Select Level Count**:

	Type: timeseries

	Avg l0 select_level_count of the compact task, and categorize it according to different cg, levels and task types

- **Compaction Task File Count**:

	Type: timeseries

	Avg file count of the compact task, and categorize it according to different cg, levels and task types

- **Compaction Task Size Distribution**:

	Type: timeseries

	The distribution of the compact task size triggered, including p90 and max. and categorize it according to different cg, levels and task types.

- **Compactor Running Task Count**:

	Type: timeseries

	The number of compactions from one level to another level that are running.

- **Compaction Duration**:

	Type: timeseries

	compact-task: The total time have been spent on compaction.

- **Compaction Throughput**:

	Type: timeseries

	KBs read from next level during history compactions to next level

- **Compaction Write Bytes(GiB)**:

	Type: timeseries

	The number of bytes that have been written by compaction.Flush refers to the process of compacting Memtables to SSTables at Level 0.Write refers to the process of compacting SSTables at one level to another level.

- **Compaction Write Amplification**:

	Type: timeseries

	Write amplification is the amount of bytes written to the remote storage by compaction for each one byte of flushed SSTable data. Write amplification is by definition higher than 1.0 because we write each piece of data to L0, and then write it again to an SSTable, and then compaction may read this piece of data and write it to a new SSTable, that's another write.

- **Compacting SSTable Count**:

	Type: timeseries

	The number of SSTables that is being compacted at each level

- **Compacting Task Count**:

	Type: timeseries

	num of compact_task

- **KBs Read/Write by Level**:

	Type: timeseries

- **Count of SSTs Read/Write by level**:

	Type: timeseries

- **Hummock Sstable Size**:

	Type: timeseries

	Total bytes gotten from sstable_bloom_filter, for observing bloom_filter size

- **Hummock Sstable Item Size**:

	Type: timeseries

	Total bytes gotten from sstable_avg_key_size, for observing sstable_avg_key_size

- **Hummock Sstable Stat**:

	Type: timeseries

	Avg count gotten from sstable_distinct_epoch_count, for observing sstable_distinct_epoch_count

- **Hummock Remote Read Duration**:

	Type: timeseries

	Total time of operations which read from remote storage when enable prefetch

- **Compactor Iter keys**:

	Type: timeseries

- **Lsm Compact Pending Bytes**:

	Type: timeseries

	bytes of Lsm tree needed to reach balance

- **Lsm Level Compression Ratio**:

	Type: timeseries

	compression ratio of each level of the lsm tree

## Object Storage

- **Operation Throughput**:

	Type: timeseries

- **Operation Duration**:

	Type: timeseries

- **Operation Rate**:

	Type: timeseries

- **Operation Size**:

	Type: timeseries

- **Operation Failure Rate**:

	Type: timeseries

- **Operation Retry Rate**:

	Type: timeseries

- **Estimated S3 Cost (Realtime)**:

	Type: timeseries

	There are two types of operations: 1. GET, SELECT, and DELETE, they cost 0.0004 USD per 1000 requests. 2. PUT, COPY, POST, LIST, they cost 0.005 USD per 1000 requests.Reading from S3 across different regions impose extra cost. This metric assumes 0.01 USD per 1GB data transfer. Please checkout AWS's pricing model for more accurate calculation.

- **Estimated S3 Cost (Monthly)**:

	Type: timeseries

	This metric uses the total size of data in S3 at this second to derive the cost of storing data for a whole month. The price is 0.023 USD per GB. Please checkout AWS's pricing model for more accurate calculation.

## Hummock Tiered Cache

- **Ops**:

	Type: timeseries

- **Duration**:

	Type: timeseries

- **Throughput**:

	Type: timeseries

- **Hit Ratio**:

	Type: timeseries

- **Refill Ops**:

	Type: timeseries

- **Data Refill Throughput**:

	Type: timeseries

- **Refill Duration**:

	Type: timeseries

- **Refill Queue Length**:

	Type: timeseries

- **Size**:

	Type: timeseries

- **Inner Op Duration**:

	Type: timeseries

- **Slow Ops**:

	Type: timeseries

- **Slow Op Duration**:

	Type: timeseries

- **Inheritance - Parent Meta Lookup Ops**:

	Type: timeseries

- **Inheritance - Parent Meta Lookup Ratio**:

	Type: timeseries

- **Inheritance - Unit inheritance Ops**:

	Type: timeseries

- **Inheritance - Unit inheritance Ratio**:

	Type: timeseries

- **Block Refill Ops**:

	Type: timeseries

- **Block Refill Ratio**:

	Type: timeseries

## Hummock Manager

- **Lock Time**:

	Type: timeseries

- **Real Process Time**:

	Type: timeseries

- **Version Size**:

	Type: timeseries

- **Version Id**:

	Type: timeseries

- **Epoch**:

	Type: timeseries

- **Table Size**:

	Type: timeseries

- **Materialized View Size**:

	Type: timeseries

- **Table KV Count**:

	Type: timeseries

- **Object Total Number**:

	Type: timeseries

	Objects are classified into 3 groups:
- not referenced by versions: these object are being deleted from object store.
- referenced by non-current versions: these objects are stale (not in the latest version), but those old versions may still be in use (e.g. long-running pinning). Thus those objects cannot be deleted at the moment.
- referenced by current version: these objects are in the latest version.

Additionally, a metric on all objects (including dangling ones) is updated with low-frequency. The metric is updated right before full GC. So subsequent full GC may reduce the actual value significantly, without updating the metric.

- **Object Total Size**:

	Type: timeseries

	Refer to `Object Total Number` panel for classification of objects.

- **Delta Log Total Number**:

	Type: timeseries

	total number of hummock version delta log

- **Version Checkpoint Latency**:

	Type: timeseries

	hummock version checkpoint latency

- **Write Stop Compaction Groups**:

	Type: timeseries

	When certain per compaction group threshold is exceeded (e.g. number of level 0 sub-level in LSMtree), write op to that compaction group is stopped temporarily. Check log for detail reason of write stop.

- **Full GC Trigger Count**:

	Type: timeseries

	total number of attempts to trigger full GC

- **Full GC Last Watermark**:

	Type: timeseries

	the object id watermark used in last full GC

- **Compaction Event Loop Time**:

	Type: timeseries

- **Move State Table Count**:

	Type: timeseries

	The times of move_state_table occurs

- **State Table Count**:

	Type: timeseries

	The number of state_tables in each CG

- **Branched SST Count**:

	Type: timeseries

	The number of branched_sst in each CG

## Backup Manager

- **Job Count**:

	Type: timeseries

	Total backup job count since the Meta node starts

- **Job Process Time**:

	Type: timeseries

	Latency of backup jobs since the Meta node starts

## gRPC Meta: Catalog Service

- **Create latency**:

	Type: timeseries

- **Drop latency**:

	Type: timeseries

- **GetCatalog latency**:

	Type: timeseries

## gRPC Meta: Cluster Service

- **AddWorkerNode latency**:

	Type: timeseries

- **ListAllNodes latency**:

	Type: timeseries

## gRPC Meta: Stream Manager

- **CreateMaterializedView latency**:

	Type: timeseries

- **DropMaterializedView latency**:

	Type: timeseries

- **Flush latency**:

	Type: timeseries

## gRPC Meta: Hummock Manager

- **UnpinVersionBefore latency**:

	Type: timeseries

- **UnpinSnapshotBefore latency**:

	Type: timeseries

- **ReportCompactionTasks latency**:

	Type: timeseries

- **GetNewSstIds latency**:

	Type: timeseries

## gRPC: Hummock Meta Client

- **compaction_count**:

	Type: timeseries

- **version_latency**:

	Type: timeseries

- **snapshot_latency**:

	Type: timeseries

- **snapshot_count**:

	Type: timeseries

- **table_latency**:

	Type: timeseries

- **table_count**:

	Type: timeseries

- **compaction_latency**:

	Type: timeseries

## Frontend

- **Active Sessions**:

	Type: timeseries

	Number of active sessions

- **Query Per Second (Local Query Mode)**:

	Type: timeseries

- **Query Per Second (Distributed Query Mode)**:

	Type: timeseries

- **The Number of Running Queries (Distributed Query Mode)**:

	Type: timeseries

- **The Number of Rejected queries (Distributed Query Mode)**:

	Type: timeseries

- **The Number of Completed Queries (Distributed Query Mode)**:

	Type: timeseries

- **Query Latency (Distributed Query Mode)**:

	Type: timeseries

- **Query Latency (Local Query Mode)**:

	Type: timeseries

## Memory manager

- **LRU manager loop count per sec**:

	Type: timeseries

- **LRU manager watermark steps**:

	Type: timeseries

- **LRU manager diff between watermark_time and now (ms)**:

	Type: timeseries

	watermark_time is the current lower watermark of cached data. physical_now is the current time of the machine. The diff (physical_now - watermark_time) shows how much data is cached.

- **The allocated memory of jemalloc**:

	Type: timeseries

- **The active memory of jemalloc**:

	Type: timeseries

- **The resident memory of jemalloc**:

	Type: timeseries

- **The metadata memory of jemalloc**:

	Type: timeseries

- **The allocated memory of jvm**:

	Type: timeseries

- **The active memory of jvm**:

	Type: timeseries

- **LRU manager diff between current watermark and evicted watermark time (ms) for actors**:

	Type: timeseries

## Connector Node

- **Connector Source Throughput(rows)**:

	Type: timeseries

- **Connector Sink Throughput(rows)**:

	Type: timeseries

## Sink Metrics

- **Commit Duration**:

	Type: timeseries

- **Log Store Read/Write Epoch**:

	Type: timeseries

- **Log Store Lag**:

	Type: timeseries

- **Log Store Consume Persistent Log Lag**:

	Type: timeseries

- **Log Store Consume Throughput(rows)**:

	Type: timeseries

- **Executor Log Store Consume Throughput(rows)**:

	Type: timeseries

- **Log Store Write Throughput(rows)**:

	Type: timeseries

- **Executor Log Store Write Throughput(rows)**:

	Type: timeseries

- **Kv Log Store Read Storage Row Ops**:

	Type: timeseries

- **Kv Log Store Read Storage Size**:

	Type: timeseries

- **Kv Log Store Write Storage Row Ops**:

	Type: timeseries

- **Kv Log Store Write Storage Size**:

	Type: timeseries

- **Kv Log Store Rewind Rate**:

	Type: timeseries

- **Rewind delay (second)**:

	Type: timeseries

## Kafka Metrics

- **Kafka high watermark and source latest message**:

	Type: timeseries

	Kafka high watermark by source and partition and source latest message by partition, source and actor

- **Message Count in Producer Queue**:

	Type: timeseries

	Current number of messages in producer queues

- **Message Size in Producer Queue**:

	Type: timeseries

	Current total size of messages in producer queues

- **Message Produced Count**:

	Type: timeseries

	Total number of messages transmitted (produced) to Kafka brokers

- **Message Received Count**:

	Type: timeseries

	Total number of messages consumed, not including ignored messages (due to offset, etc), from Kafka brokers.

- **Message Count Pending to Transmit (per broker)**:

	Type: timeseries

	Number of messages awaiting transmission to broker

- **Inflight Message Count (per broker)**:

	Type: timeseries

	Number of messages in-flight to broker awaiting response

- **Error Count When Transmitting (per broker)**:

	Type: timeseries

	Total number of transmission errors

- **Error Count When Receiving (per broker)**:

	Type: timeseries

	Total number of receive errors

- **Timeout Request Count (per broker)**:

	Type: timeseries

	Total number of requests timed out

- **RTT (per broker)**:

	Type: timeseries

	Broker latency / round-trip time in milli seconds

- **Throttle Time (per broker)**:

	Type: timeseries

	Broker throttling time in milliseconds

- **Topic Metadata_age Age**:

	Type: timeseries

	Age of metadata from broker for this topic (milliseconds)

- **Topic Batch Size**:

	Type: timeseries

	Batch sizes in bytes

- **Message to be Transmitted**:

	Type: timeseries

	Number of messages ready to be produced in transmit queue

- **Message in pre fetch queue**:

	Type: timeseries

	Number of pre-fetched messages in fetch queue

- **Next offset to fetch**:

	Type: timeseries

	Next offset to fetch

- **Committed Offset**:

	Type: timeseries

	Last committed offset

## Network connection

- **Network throughput**:

	Type: timeseries

- **S3 throughput**:

	Type: timeseries

- **gRPC throughput**:

	Type: timeseries

- **IO error rate**:

	Type: timeseries

- **Existing connection count**:

	Type: timeseries

- **Create new connection rate**:

	Type: timeseries

- **Create new connection err rate**:

	Type: timeseries

## Iceberg Sink Metrics

- **Write Qps Of Iceberg Writer**:

	Type: timeseries

	iceberg write qps

- **Write Latency Of Iceberg Writer**:

	Type: timeseries

- **Iceberg rolling unfushed data file**:

	Type: timeseries

- **Iceberg position delete cache num**:

	Type: timeseries

- **Iceberg partition num**:

	Type: timeseries

## User Defined Function

- **UDF Calls Count**:

	Type: timeseries

- **UDF Input Chunk Rows**:

	Type: timeseries

- **UDF Latency**:

	Type: timeseries

- **UDF Throughput (rows)**:

	Type: timeseries

- **UDF Throughput (bytes)**:

	Type: timeseries

