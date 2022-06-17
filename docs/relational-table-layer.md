# An Overview of Relational Table Layer
- [An Overview of RisingWave State Store](#an-overview-of-risingwave-state-store)
  - [Overview](#overview)
  - [Architecture](#architecture)
  - [The Hummock User API](#the-hummock-user-api)
  - [Hummock Internals](#hummock-internals)
    - [Storage Format](#storage-format)
    - [Write Path](#write-path)
    - [Read Path](#read-path)
    - [Compaction](#compaction)
    - [Transaction Management with Hummock Manager](#transaction-management-with-hummock-manager)
    - [Checkpointing in Streaming](#checkpointing-in-streaming)

<!-- Created by https://github.com/ekalinin/github-markdown-toc -->

## Overview

In RisingWave, all streaming executors store their data into a state store. As the state's key encoding is very similar to a cell-based table, each kind of state is stored as a cell-based relational table first. And the cell-based relational table provides the interface accessing relational data in KV. Therefore, relational table can be regared as the bridge between state-ful executors and state store.
## Architecture


![Overview of Architecture](images/relational-table-layer/relational-table-01.svg)

Relational table consists of State Table, Mem Table and Cell-based Table. The State Table provides the table operations by these APIs: `get_row`, `insert_row`, `delete_row` and `update_row`, the Mem Table
is a buffer for modify operations without encoding, and the Cell-based Table is responsible for performing serialization and deserialization between cell-based encoding and KV encoding.


### Write Path
Operations on relational table will first be cached in Mem Table, which is a BTreeMap data structure. Once executor wants to write these operations to state store, cell-based table will covert these operations into kv pairs and write to state store with specific epoch. 

### Read Path



### Write Path

Hummock client will batch writes and generate SSTs to sync to the underlying S3-compatible service. An SST consists of two files:
- <id>`.data`: Data file composed of ~64KB blocks, each of which contains actual key-value pairs.
- <id>`.meta`: Meta file containing large metadata including min-max index, Bloom filter as well as data block metadata.

After the SST is uploaded to an S3-compatible service, Hummock client will let the Hummock manager know there’s a new table.
The list of all SSTs along with some metadata forms a ***version***. When Hummock client adds new SSTs to the Hummock manager, a new version will be generated with the new set of SST files.

![Write Path](images/state-store-overview/state-store-overview-02.svg)

### Read Path

To read from Hummock, we need a ***version*** (a consistent state of list of SSTs we can read) and ***epoch*** to generate a consistent read snapshot. To avoid RPC with Hummock manager in every user read, the Hummock client will cache a most recent ***version*** locally. Local version will be updated when 1) client initiates a write batch and 2) background refresher triggers.

For every read operation (`scan`, `get`), we will first select SSTs that might contain the required keys.

For `scan`, we simply select by overlapping key range. For point get, we will filter SSTs further by Bloom filter. After that, we will compose a single `MergeIterator` over all SSTs. The `MergeIterator` will return all keys in range along with their epochs. Then, we will create `UserIterator` over `MergeIterator`, and for all user keys, the user iterator will pick the first full key whose epoch <= read epoch. Therefore, users can perform snapshot read from Hummock based on the given epoch. The snapshot should be acquired beforehand and released afterwards.

![Read Path](images/state-store-overview/state-store-overview-03.svg)

Hummock implements the following iterators:
- `BlockIterator`: iterates a block of an SSTable.
- `SSTableIterator`: iterates an SSTable.
- `ConcatIterator`: iterates SSTables with non-overlapping keyspaces.
- `MergeIterator`: iterates SSTables with overlapping keyspaces.
- `UserIterator`: wraps internal iterators and outputs user key-value with epoch <= read epoch.

[iterators source code](https://github.com/singularity-data/risingwave/tree/main/src/storage/src/hummock/iterator)


### Compaction

Currently, Hummock is using a compaction strategy similar to leveled-compaction in RocksDB. It will compact data using consistent hash (docs and implementation TBD), so that data on shared storage distribute in the same way as how stream executors use them.

Compaction is done on a special worker node called compactor node. The standalone compactor listens for compaction jobs from meta node, compacts one or more SSTs into new ones, and reports completion to the meta node. (In Hummock in-memory mode, compactor will be running as a thread inside compute node.)

To support MVCC read without affecting compaction, we track the epoch low watermark in Hummock snapshots. A user key-value pair will be retained if (1) it is the latest, or (2) it belongs to an epoch above the low watermark.

### Transaction Management with Hummock Manager

[source code of Hummock manager on meta service](https://github.com/singularity-data/risingwave/tree/main/src/meta/src/hummock)

In this part, we discuss how Hummock coordinates between multiple compute nodes. We will introduce key concepts like “snapshot”, “version”, and give examples on how Hummock manages them.

Every operation on the LSM-tree yields a new ***version*** on Hummock manager, e.g., adding new L0 SSTs and compactions. In streaming, each stream barrier is associated with an ***epoch***. When the barrier flows across the system and collected by the stream manager, we can start doing ***checkpoint*** on this epoch. SSTs produced in a single checkpoint are associated with an ***uncommitted epoch***. After all compute nodes flush shared buffers to shared storage, Hummock manager considers the epoch committed. Therefore, apart from the list of files in LSM, a version also contains committed epoch number `max_committed_epoch` and SSTs in uncommitted epochs. As a result, both an ***operation on LSM*** and a ***streaming checkpoint*** will yield a new ***version*** in Hummock manager.

Currently, there is only one checkpoint happening in the system at the same time. In the future, we might support more checkpoint optimizations including concurrent checkpointing.

As mentioned in [Read Path](#read-path), reads are performed on a ***version*** based on a given ***epoch***. During the whole read process, data from the specified read epoch cannot be removed by compaction, which is guaranteed by ***pinning a snapshot***; SSTs within a ***version*** cannot be vacuumed by compaction, which is guaranteed by ***pinning a version***.

The SQL frontend will get the latest epoch from meta service. Then, it will embed the epoch number into SQL plans, so that all compute nodes will read from that epoch. In theory, both SQL frontend and compute nodes will ***pin the snapshot***, to handle the case that frontend goes down and the compute nodes are still reading from Hummock (#622). However, to simplify the process, currently we *only pin on the frontend side**.*

![Hummock Service](images/state-store-overview/state-store-overview-04.svg)

Hummock only guarantees that writes on one node can be immediately read from the same node. However, the worker nodes running batch queries might have a slightly outdated version with a batch query plan is received (due to the local version caching). Therefore, we have a `wait_epoch` interface to wait until the local cached version contains full data of one epoch.

When there is no reference to a version, all file deletions in this version can be actually applied. There is a background vacuum task dealing with the actual deletion.

### Checkpointing in Streaming

[related PR](https://github.com/singularity-data/risingwave/pull/602)

Now we discuss how streaming executors and streaming manager use Hummock as a state store.

From the perspective of the streaming executors, when they receive a barrier, they will be “in the new epoch”. For data received in epoch 1, they will be persisted (write batch) with epoch 1. Receiving the barrier also causes the read and write epoch being set to the new one.

Here we have two cases: Agg executors always persist and produce new write batches when receiving a barrier; Join executors (in the future when async flush gets implemented) will produce write batches within an epoch.

![Checkpoint in Streaming](images/state-store-overview/state-store-overview-05.svg)

Streaming executors cannot control when data will be persisted — they can only write to Hummock `shared buffer`. When a barrier flows across the system and is collected by meta service, we can ensure that all executors have written their states of ***the previous epoch*** to the shared buffer, so we can initiate checkpoint process on all worker nodes, and upload SSTs to persistent remote storage.

For example, the barrier manager sends barrier epoch = 2. When the epoch 2 barrier is collected on meta service, we can ensure that data prior to epoch 2 have been fully flushed to Hummock shared buffer. (Note that epoch number in streaming is generated by machine time + serial number, so ***we cannot simply use +1 -1 to determine the epoch of the previous / next barrier***.) Assuming the previous barrier is of epoch 1, we can start checkpointing data from epoch 1 after barrier of epoch 2 has been collected.
