# Metrics

The contents of this document may be subject to frequent change.
It covers what each metric measures, and what information we may derive from it.

## Barrier Latency

Prerequisite: [Checkpoint](./checkpoint.md)

This metric measures the duration from which a barrier is injected into **all** sources in the stream graph,
to the barrier flown through all executors in the graph.

### What can we understand from it?

Usually when examining barrier latency, we look at **high barrier latency**.

There are two contributing factors to it:
1. Time taken to actually process the streaming messages.
2. Bandwidth for streaming messages.

First, we keep total order for streaming messages.
This means that all streaming messages currently in the stream graph have to be processed
before the barrier can pass through.
If barrier latency is high, it could mean a long time is taken to process these streaming messages.
Concretely, here are some costs of processing:
1. CPU cost of evaluating expressions.
2. I/O remote exchange between fragments.
3. hash-join / hash-agg cache-miss (results in extra costs to access state on s3).

Next, high barrier latency could also be caused by buffers in the graph,
for instance the channel in the exchanges. These buffers determine the bandwidth for the streaming messages.
If some downstream buffer is congested, we will be unable to queue and continue processing upstream messages.