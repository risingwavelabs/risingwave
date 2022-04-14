# Checkpoint

## Revisit: Consistency Model

Similiar to other relational databases, RisingWave provide consistent snapshot reads on both tables and materialized views. Specifically,

1. **Consistency.** Given a query Q and latest event timestamp t, the system returns a result Q(t′​) such that Q(t′​) is consistent on a timestamp t′≤t , i.e. evaluating Q over a snapshot S(t′)​ from a previous timestamp t′≤t, where S(t′) is the set of all tuples presented before timestamp t′. That is, the systems delivers a query result that is consistent with a previous timestamp t′. 
2. **Monotonicity.** For any two timestamps t1​ and t2​ such that t1​<t2​, assume the result for Q on t1​ and t2​ are consistent with snapshots on t1′​ and t2′​ respectively, then t1′​<t2′​. That is, later query should not return results that correspond to earlier snapshots. 

Note that RisingWave does **not** gurantee a write must be visiable to subsequence reads aka. the read-after-write consistency. Users may use the `FLUSH` command to make sure the changes have taken effect before reads.

Internally, the up-coming changes may take a while to propogate from source to materialized views, and at least one barrier event is required to flush the changes. Such two kinds of latency determines the latency between write and read.

## Streaming Checkpoint

The consistent checkpoints play 2 roles in our system.

1. Fault-tolerance. To recover the cluster from an unexpected failure, every stateful streaming operator need to recover their states from a consistent checkpoint.
2. Consistent snapshot. The snapshot to be read is actually the lastest completed checkpoint. As the previous section discussed, it's required to gurantee the data of tables & materialized views consistent in one snapshot.

RisingWave makes checkpointing via [Chandy–Lamport algorithm](https://en.wikipedia.org/wiki/Chandy%E2%80%93Lamport_algorithm). A speical kind of message, checkpoint barriers, are generated on streaming source and propogates across the streaming graph to the materialize views (or sink).

![](./images/checkpoint/checkpoint.svg)

1. The barrier manager on meta node starts a checkpoint by sending requests to all compute nodes. A checkpint epoch is specified.
2. The barrier messages go through every streaming operators (actors) in the streaming graph. 
   - For fan-out operators like `Dispatch`, the barrier messages is copied to every downstream.
   - For fan-in operators like `Merge` or `Join`, the barrier messages are collected and emit out once collected from all upstreams. 
   - Otherwise, the barrier just pass the operators and trigger them to do checkpointing, i.e. write changes into storage 
3. If the barrier messages have passed all actors, the barrier manager can tell the storage manager to commit the checkpoint and marks this round of checkpoint as completed.

## Transaction on Storage

As mentioned before, during the checkpointing, every operator write their changes of this epoch into storage. For storage, these data is still uncommited i.e. not persisted to the shared storage, but needs to be visible to that operator locally.

A local shared buffer is introduced to stage these uncommitted write batches. Once the barriers have pass through all actors, the storage manager can notify all compute nodes to 'commit' their buffered write batches into the shared storage.

![](./images/checkpoint/shared-buffer.svg)

Another benefit of shared buffer is that the write batches in a compute node can be compacted into a single SSTable file before uploading, which significantly reduced the number of SSTables in Layer 0.