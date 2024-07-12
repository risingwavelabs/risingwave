# The Hummock Shared Buffer

<!-- toc -->

## Introduction

> Note: L0 and Shared buffer are totally different. They both exist independently.

The Hummock Shared Buffer serves 3 purposes:

- Batch writes on worker node level, so as to reduce SST number.

  - Currently, a single epoch might produce hundreds of SST, which makes meta service hard to handle.

- Support async checkpoint.

  - The shared buffer will generate SST based on epoch, and provide a consistent view of a epoch, by merging the snapshot of the storage SSTs and the immutable in-memory buffers.

- Support read-after-write (so-called async flush), so as to make executor logic simpler.

  Currently, if executors need to compute the “maximum” value, there are only two ways:
  1. Produce a write batch (i.e. write directly to object store), and read from the storage (like ExtremeState i.e. the state of `MAX()/MIN()`).
  2. Write to in-memory flush buffer, and merge data from flush buffer and storage (like TopN, HashJoin).

## Part 1: Async Checkpoint

Previously, when an executor is processing a barrier,
it will flush its content to Hummock using the write_batch interface.
But it turns out that we have so many executors,
that a single epoch might produce 200∼300 write batches,
which yields 200∼300 SSTs. The SST number is tremendous for an LSM engine.

The first idea is to batch writes.
For example, we can collect all write batches from a single epoch,
and flush them as one SST to the storage engine.
However, it is not as simple as said — the downstream executor might rely on the upstream barrier to process data.

See the following example:

```text
┌─────────────┐      ┌─────────────┐
│             │      │             │
│   HashAgg   ├──────► XXXExecutor │
│             │      │             │
└─────────────┘      └─────────────┘
```

When HashAgg is processing ExtremeState,
it will need to “Produce a write batch, and read from the storage”.
Therefore, it cannot produce data for the current epoch until data get fully checkpoint.
However, we want to batch writes and checkpoint at once.
This is a contradiction.
Therefore, the only way to solve this is to add a memtable and async checkpoint.

The async checkpoint logic will be implemented with StreamManager (on meta) and HummockLocalManager (on CN), and we will only have immutable memtables in this part.

### Write Path

Assume there is a new barrier flowing across our system.

When each executor consumes a barrier, it will produce a write batch (which should complete immediately) and forward the barrier.

After all executors have completed processing barrier,
a checkpoint will be initiated,
and new SST of the previous epoch will be produced.
As our system only have one barrier flowing,
we can simply checkpoint epoch 3 after stream manager collects barrier 4.

### Read Path

As executors all own their own keyspace, the read path doesn’t need to do snapshot isolation. We can simply view all committed SSTs and memtables as a “HummockVersion”.

When the barrier is being processed (but not checkpoint), the read set is simply all memtables on current worker node and all SSTs on shared storage.

When a checkpoint is being processed, things would become a little bit complex. The HummockVersion might contain both SSTs committed, and SSTs being checkpoint. In this case, the read set is:

- All SSTs (including those uncommitted), but set the read epoch to the previous checkpoint (in this case, < epoch 3, or simply <= epoch 2). Therefore, all epochs being checkpoint will be ignored from shared storage.
- All memtable of the checkpoint epoch (in this case, epoch 3).

These two parts combined provide a consistent view of the KV storage.

After the checkpoint has been finished (SSTs have been committed), we can either:

- Evict epoch 3 from Hummock memtable, and serve all data from shared storage.
- Retain latest N epochs in Hummock memtable, and serve all data where memtable epoch >= E, and shared storage epoch < E.

Having implemented async checkpoint,
we can now batch writes and significantly reduce SST number.

## Part 2: Write Anytime / Async Flush

As said in [introduction](#introduction), previously, all RisingWave streaming executors will do either of the following to maintain operator states:

- merge-on-write: Produce a write batch, and read from the storage (like ExtremeState).
- merge-on-read: Write to in-memory flush buffer, and merge data from flush buffer and storage (like TopN, HashJoin).

We have [established earlier](#part-1-async-checkpoint) that the merge-on-write way is not scalable, as it will produce too many SSTs. So we will explore the second way.

When executors are using the second way,
it will always need to “merge data from flush buffer and storage”.
This “merge iterator” has been implemented in various ways in different executors,
and make the ManagedState very hard to read.

Therefore, to standardize this, we support “async flush” in shared buffer,
which means that streaming executors can write to the state store at any time,
and the state store will provide “read after write” semantics within epoch.

Currently, all streaming executors will only read their own keys, since they are partitioned by state_table_id and vnode.

Therefore, we can leverage this property to provide a mutable memtable to each executor,
and unify the “merge” logic across all “merge-on-read” executors.

### A New Merge Iterator

Apart from the MergeIterator in Hummock, which merges SSTs from various levels in the LSMTree,
we now need a “unified” merge iterator above the state store:

The MutableMemTable will store data in its in-memory-representation (e.g., i32, i32).
The special MergeIterator will merge encoded data from Hummock and memory-representation data from MutableMemTable.

Therefore, we can unify all logic across TopN executor, HashJoin executor, etc.

Every executor only has one active MutableMemTable. When one epoch ends, the MutableMemTable should be converted to an immutable memtable in Hummock, and everything stays the same as The Hummock Shared Buffer — Part 1 (Async Checkpoint).

#### Considerations

For all data a, b of the same type, we must ensure that:

```
in-memory representation of a < in-memory representation of b,
iff memcomparable(a) < memcomparable(b)
```
