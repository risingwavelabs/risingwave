# Rate Limit and Backpressure

Here we compare 2 concepts in RisingWave used to keep barrier latency low.

## Why keep Barrier Latency Low?

1. Commands such as `Drop, Cancel, Alter` depend on barriers.
   When barrier latency is high, it also means these commands take a long time to run.* [1]
2. High Barrier Latency means Low Data Freshness. User gets stale results.

[1] This could potentially be resolved by decoupling `Drop, Cancel`: https://github.com/risingwavelabs/risingwave/issues/13396

## Why can Barrier Latency be High?

The data between barriers takes a long time to process.
This can be due to high amplification, UDF latency, or other reasons.

By default, Barriers have an interval of 256ms.
If records between those barrier take longer than 256ms to process, the barrier latency will increase by that difference.

## How does Backpressure affect Barrier Latency?

### A simple scenario without channels

In our system, we have Actors which run various parts of a stream job.

Let's consider a cluster with a single stream job, where this job has just 2 actors:
```text
Backfill (A) -> Stream Materialize (B)
```

If B is busy, it just won't request new Stream Messages (Barriers and StreamChunks) from A.
This is called Backpressure. You can see that A's stream will be backpressured by B, i.e. no longer progress, until B is ready again.

Now consider if B had the following sequence of events:
```text
1. Scheduled Barrier X (<1ms processing time)
2. StreamChunk (250ms processing time)
3. Scheduled Barrier Y (<1ms processing time)
```

The barrier latency for Y will be 0ms, since our barrier interval is 256ms,
and the time between X and Y is less than 256ms.

Consider another case where B has the following sequence of events:
```text
1. Scheduled Barrier X (<1ms processing time)
2. StreamChunk (256ms processing time)
3. StreamChunk (250ms processing time)
4. Scheduled Barrier Y (<1ms processing time)
```

Now barrier latency will be around 250ms.

In the case above, where we have Backfill and Stream Materialize, this is unlikely to occur however.
Backfill should always let upstream side (barriers) bypass, so we should see stream chunk (3) only after (4).

### Adding Channels to the equation

Now consider a case where we have `join`.

```
StreamScan A -- HashJoin X
              /
             /
StreamScan B
```

Now both A and B will have a channel between themselves and X.
The channel is there to provide a buffer between actors, since A and B are not directly connected to X, they could be on different machines.
Hence providing a buffer isolates X from network latency of getting Stream Messages from A and B.

If X is busy, it will first backpressure the channel.
However, the channel will not immediately backpressure A and B.
It will only do so once it is full.

This means that the stream chunks between barriers are equivalent to channel capacity.
And then the barrier latency is the duration taken to process these barriers.

_example.toml_
```toml
[streaming.developer]
stream_exchange_initial_permits = 2048
```

### Considering Amplification and UDF latency

When there's high amplification or UDF latency in some downstream actor, upstream will naturally be backpressured.
Overtime, a stream graph will hit its max capacity (all actor channels are full).

The barrier latency is then proportional to the time it takes to process stream chunks between barriers.

This is basically the number of chunks in a channel, multiplied by the time taken to process the join for it, or the UDF call latency for it.

## What is Rate Limit

Rate Limit simply limits the rate at which records are processed.
It will only rate limit snapshot read / source read side of an actor.
This means that barriers are not rate limited.

## Rate Limit to keep barrier latency low

### Backfill

In a newly created MV, it will undergo Backfilling, to fill the MV with historical data and fresh data concurrently.
When the MV has joins or UDF calls with high latency, the barrier latency can spike.

In such a case, we can rate limit the Backfill actor's snapshot read side to keep barrier latency low.

Caveat: This will slow down the backfill process, but it will keep barrier latency low.

### Source

Another case is to rate limit the source executor, if it's causing high barrier latency due to similar reasons as above (some downstream is the bottleneck).

Caveat: It could potentially lead to loss in data freshness.

Example case:
Source has 1-100 row/s throughput, average throughput of 10 row/s, downstream has max processing rate of 10 row/s.
We rate limit source to 10 row/s. Otherwise when we hit 100 row/s,
upstream source will read that into the channel,
barrier latency will spike as downstream can't keep up.