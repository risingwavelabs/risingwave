# An Overview of RisingWave State Store

In RisingWave, all streaming executors store their data into a state store. This state store is backed by a service called Hummock. Hummock can be considered as a cloud-native RocksDB, which provides KV API, and stores all data on S3-compatible service. However, it is not a key-value store for general purpose, and is strongly coupled with the design of streaming engine.

## Architecture

Reading this document requires prior knowledge of LSM-based KV storage engine, like RocksDB, LevelDB, etc.

![Overview of Architecture](images/state-store-overview-01.svg)

Hummock consists of manager service on meta node, clients on compute nodes, and a shared storage to store files. Every time a new write batch is produced, HummockClient will upload those files to shared storage, and notify the Hummock manager of the new data. With compaction going on, new files will be added and old files won’t be used. The Hummock manager will take care of the lifecycle of a file — is a file is being used? can we delete a file? etc.

Streaming state store has distinguish workload characteristics.

* Every streaming executor will only read *and write its own portion of data*, which are multiple consecutive non-overlapping ranges of keys (we call it *key space*).
* Data (generally) *won’t be shared across nodes*, so every worker node will only read and write its own data. Therefore, all Hummock API like get, scan only guarantees writes on one node can be immediately read from the same node. In some cases, if we want to read data written from other nodes, we will need to *wait for the epoch*.
* Streaming data are *committed in serial*. Based on the *barrier-based checkpoint* algorithm, the states are persisted epoch by epoch. We can tailor the write path specifically for the epoch-based checkpoint workload.

This leads to the design of Hummock, the cloud-native KV-based streaming state store. We’ll explain concepts like “epoch”, “key space” and “barrier” in the following chapters.

## The Hummock User API

[source code](https://github.com/singularity-data/risingwave/blob/main/rust/storage/src/hummock/mod.rs)

In this part, we will introduce how users can use Hummock as a KV store. This is helpful for designing benchmarks solely relying on the storage layer.

The Hummock itself provides 3 simple APIs: write batch, get, and scan. Hummock provides MVCC write and read on KV pairs. Every key stored in Hummock has an *epoch* (aka. timestamp). Developers should specify an epoch (e.g., epoch belongs to a write batch, read epoch, etc.) when calling Hummock APIs.

Hummock doesn’t support writing a single key. To write data into Hummock, users should provide a *sorted, unique* list of *keys* and the corresponding *operations* (put value, delete), with an *epoch*, and call the write_batch API. Therefore, within one epoch, users can only have one operation for a key. For example,

```
[a => put 1, b => put 2] epoch = 1 is a valid write batch
[a => put 1, a => delete, b => put 2] epoch = 1 is an invalid write batch
[b => put 1, a => put 2] epoch = 1 is an invalid write batch
```

For reads, we can call scan and get API on Hummock client. Developers need to specify a read epoch for read APIs. Hummock only guarantees writes on one node can be immediately read from the same node. Let’s take a look at the following example:

```
Node 1: write a => 1, b => 2 at epoch 1
Node 1: write a => 3, b => 4 at epoch 2
Node 2: write c => 5, d => 6 at epoch 2
```

After all operations have been done,

```
Read at epoch 2 on Node 1: a => 3, b => 4, (c => 5, d => 6 may be read)
Read at epoch 1 on Node 1: a => 1, b => 2
Read at epoch 2 on Node 2 with `wait_epoch 2`: a => 3, b => 4, c => 5, d => 6
```

To synchronize data between manager and clients, there are also a lot of internal APIs. We will discuss them later along with the checkpoint process.

## Hummock Internals

In this part, we will discuss how data are stored and organized in Hummock internally. If you will develop Hummock, you should learn some basic concepts, like SST, key encoding, read / write path, consistency, from the following sections.

### SSTable and Key Encoding

[SST encoding source code](https://github.com/singularity-data/risingwave/tree/main/rust/storage/src/hummock/sstable)

All key-value pairs are stored in SSTables. For each user key, it has an epoch associated with it. In SSTs, key-value pairs are sorted first by user key (lexicographical order), and then by epoch (largest to smallest).

For example, if users write two batches in consequence:

```
write a => 1, b => 2 at epoch 1
write a => delete, b => 3 at epoch 2
```

After compaction (w/ min watermark = 0), there will eventually be an SST with the following content:

```
(a, 2) => delete
(a, 1) => 1
(b, 2) => 3
(b, 1) => 2
```

The final written key (aka. full key) is encoded by appending the 8-byte epoch after the user key. When doing full key comparison in Hummock, we should always compare using the VersionedComparator so as to get the correct result.

### The Write Path

Every write batch will produce an SST (Hummock might batch multiple mini write batches into a single SST in the future after async checkpoint has been implemented). The SST consists of two files: <id>.data and <id>.meta. The data file is composed of ~64KB blocks, where each block contains actual key value pairs; while the meta file contains large metadata like bloom filter. After the SST is uploaded to S3, Hummock client will let the Hummock manager know there’s a new table.
The list of all SSTs is called a *Version*. When Hummock client adds new SSTs to the Hummock manager, a new version will be generated with the new set of SST files.

![Write Path](images/state-store-overview-02.svg)

### The Read Path

[iterators source code](https://github.com/singularity-data/risingwave/tree/main/rust/storage/src/hummock/iterator)

To read from the Hummock shared storage, we will first need a *Version* (a consistent state of list of SSTs we can read). To avoid contacting Hummock manager in every user read, the Hummock client will cache a most recent *Version* locally. Local version will be updated when 1. client initiated a write batch 2. refreshed in a fixed interval in the background. (maybe replaced with a push service in the future)

For every read operation (scan, get), we will first select SSTs that might contain a key.

For scan, we simply select by overlapping key range. For point get, we will filter SSTs further by bloom filter. After that, we will compose a single MergeIterator over all SSTs. The MergeIterator will return all keys in range along with their epoch. Then, we will create UserIterator over MergeIterator, and for all user keys, the user iterator will pick the first full key that have epoch <= read epoch. Therefore, users can read the Hummock storage at any epoch given.

![Read Path](images/state-store-overview-03.svg)

If we are scanning on Hummock, we must ensure that the files we need exist throughout the scan process, and prevent Hummock manager from vacuuming the SST file being read. This is done by doing reference counting on *Version*.

When we create a user iterator, we will “pin” a version, so that all files in that version won’t be deleted. The pinning process is managed by meta client, so that “pin” a version won’t require extra RPC to Hummock manager. See “Resource Governance on Meta Client” for more information.


### Compaction

Currently, Hummock is using a compaction strategy similar to leveled-compaction in RocksDB. In the future, we will migrate to a compaction scheme that can “understand” how data are distributed in streaming executors. Therefore, SSTs will be split not by key range, but by hash of the keys. See “compaction based on consistent hash” for more information.

Compaction in Hummock needs a low watermark as parameter, therefore to support MVCC read while removing unused data. For the same user key, we will retain them only when (1) they are of the latest epoch, or (2) they are above low watermark.

### Hummock Service

[source code of Hummock manager on meta service](https://github.com/singularity-data/risingwave/tree/main/rust/meta/src/hummock)

In this part, we will discuss how Hummock coordinates between multiple compute nodes. We will introduce concepts like “snapshot”, “version”, and give examples on how Hummock manages them.

Every operation on the LSM tree yields a new *version* on Hummock manager, e.g., add new L0 table, compaction. In streaming, we have *epoch*. Each stream barrier is associated with an epoch. When the barrier flows across the system and collected by the stream manager, we can start doing *checkpoint* on an epoch. SSTs produced within checkpoint are associated with an *uncommitted epoch*. After all worker nodes complete adding L0 table to Hummock, the epoch will be considered committed. Therefore, apart from the list of files in LSM, a version also contains committed epoch number max_committed_epoch and SSTs in uncommitted epochs. Note that even if an epoch doesn’t contain any new changes (e.g., config change barrier, or no-data epoch), max_committed_epoch will still increase. Therefore, *operations on LSM* and *streaming checkpoint* will both yield new version in Hummock manager. Related source code can be found in VersionManager (or LocalVersionManager) on both Hummock manager and Hummock client.

Currently (as of writing this document), there will be only one checkpoint happening in the system at the same time. But after we implemented “New BarrierManager Design” for MV on MV, it is possible that:

* Multiple checkpoints are happening in our system.
* Data in one streaming epoch might be added multiple and unknown times. (Previously, one checkpoint initiates SST upload on all worker nodes, so we can always determine the end of an epoch and get the epoch “sealed”.)

Therefore, we will use “transaction” or “atomic write” to describe this process. There will be new document covering this case.

When a batch query needs to read data from Hummock, it will specify an *epoch*. We can always specify the latest *version* to read, and specify that read epoch, so that keys from later epochs will be ignored (so-called MVCC read). During the whole read process, data from the specified read epoch should not be removed during the compaction. This is done by *pinning an epoch* (aka. *pinning a snapshot*).

The SQL frontend will get the latest epoch from meta service. Then, it will embed the epoch number into SQL plans, so that all compute nodes will read from that epoch. In theory, both SQL frontend and compute nodes will *pin the snapshot*, to handle the case that frontend goes down and the compute nodes are still reading from Hummock (#622). However, to simplify the process, currently we *only pin on the frontend side**.*

![Hummock Service](images/state-store-overview-04.svg)

Hummock only guarantees that writes on one node can be immediately read from the same node (See Hummock Shared Buffer docs). However, the worker nodes running batch queries might be having a slightly outdated version with a batch query plan is received (due to the local version caching). Therefore, we have an wait_epoch interface to wait until the local cached version contains full data of one epoch.

When there is no reference to a version, all file deletions in this version can be actually applied. The Hummock Vacuum docs will cover this.

### Checkpointing in Streaming

[related PR](https://github.com/singularity-data/risingwave/pull/602)

Now we will discuss how streaming executors and streaming manager will use Hummock as a state store. The design docs for this part is already detailed enough, so we won’t go over details in this overview docs.

From the perspective of the streaming executors, when they receive a barrier, they will be “in the new epoch”. For data received in epoch 1, they will be persisted (write batch) with epoch 1. Receiving the barrier also causes the read and write epoch being set to the new one.

Here we have two cases: Agg executors always persist and produce new write batches when receiving a barrier; Join executors (in the future when async flush gets implemented) will produce write batches within an epoch.

![Checkpoint in Streaming](images/state-store-overview-05.svg)

Streaming executors cannot control when data will be persisted — they can only write to Hummock shared buffer (aka. memtable). When a barrier flows across the system and is collected by meta service, we can ensure that all executors have written their states of *the previous epoch* to the shared buffer, so we can initiate checkpoint process on all worker nodes, and upload SSTs to S3.

For example, the barrier manager sends barrier epoch = 2. When the epoch 2 barrier is collected on meta service, we can ensure that data prior to epoch 2 have been fully flushed to Hummock shared buffer. (Note that epoch number in streaming is generated by machine time + serial number, so *we cannot simply use +1 -1 to determine the epoch of the previous / next barrier*.) Assuming the previous barrier is of epoch 1, we can start checkpointing data from epoch 1 after barrier of epoch 2 has been collected.
