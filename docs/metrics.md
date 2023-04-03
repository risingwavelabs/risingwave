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
2. Buffer capacity for streaming messages.

#### Processing costs

When injecting a new barrier,
there will usually be streaming messages in the stream graph (unless it's the initial barrier).
Since we keep total order for streaming messages,
this means that all streaming messages currently in the stream graph have to be processed
before the barrier can pass through.
If barrier latency is high, it could mean a long time is taken to process these streaming messages.
Concretely, here are some costs of processing streaming messages:
1. CPU cost of evaluating expressions.
2. I/O remote exchange between fragments.
3. Stateful Executor cache-miss (for instance hash-join and hash-agg). This results in extra costs to access state on s3.

#### Buffer capacities

Next, high barrier latency could also be caused by buffers in the graph.
If some downstream buffer is congested, we will be unable to queue and continue processing upstream messages.

For instance, if the channel in the exchange executor is full,
upstream messages cannot be sent through this channel.
This means the upstream executor will be unable to continue processing new stream messages, until some space on the buffer is freed.

The various buffer sizes can currently be adjusted via options in the developer configuration file.
For instance, options to configure buffer size of the exchange executor can be found [here](https://github.com/risingwavelabs/risingwave/blob/a36e01307d60491b91870ac5a37049a378fe986f/src/config/example.toml#L49-L50).

Another subtle cause is that large buffer size can also worsen barrier latency.
Suppose stream message processing is at its limit, and there's high latency as a result.
Typically, backpressure kicks in, the source is throttled.
If buffer sizes are too large, or if there are many buffers, there will not be backpressure applied to source immediately.
During this delay, we will continue to see high barrier latency.
A heuristic algorithm is on the way to deal with this: https://github.com/risingwavelabs/risingwave/issues/8654. 
