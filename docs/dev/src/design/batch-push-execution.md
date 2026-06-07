# Design: Batch Push Execution Pipeline Parallelism

## Goal

Batch push execution should follow the same execution ownership model as DuckDB:

1. The scheduler owns the pipeline DAG, pipeline dependencies, and worker loop.
2. Sources expose morsels. Workers pull morsels from scheduler-owned queues.
3. Operators are passive. They expose worker-local execution state instead of owning parallel loops.
4. Breaker sinks expose local state, combine, and finalize. Workers write local sink state, then the scheduler combines into global state before dependent pipelines run.

`PushPipelineSchedule` is only a transition helper for explicit dependency ordering inside executors. It must not grow into a parallel runner because it shares one `&mut dyn PushSink`.

## Current Blockers

- B1: `ParallelMorselPipelineDriver` still serializes final sink calls. This is correct for order-preserving output sinks, but breaker sinks need worker-local sink state plus a combine/finalize step.
- B2: `Executor::execute_push(self, &mut dyn PushSink)` still lets operators own child-driving loops. The long-term model should move source loops into scheduler-owned drivers.
- B3: `PushPipelineSchedule` is deterministic and single-sink by design. It is a dependency-ordering helper, not a parallel event executor.
- B4: right/full hash join probe needs parallel-safe build-row match state. Parallel probe workers cannot mutate one shared `ChunkedData<bool>`.

## Phases

1. Hash join probe parallelism: build/finalize still produces one read-only hash table, then probe runs through a morsel driver with worker-local probe operators. Right/full/semi/anti use shared atomic match flags before one global unmatched-build finalization.
2. Local/global breaker sink state: add reusable scheduler helpers that drive push children into worker-local sinks and return the locals for executor-specific combine/finalize. Hash join build is the first user; hash agg and sort should follow the same shape.
3. Scheduler-owned pipeline DAG: lift pipeline dependency descriptions out of operator bodies into the batch scheduler layer. Spill partitions should become independent dependent pipelines instead of an executor-internal sequential loop.

## Implementation State

- `ExecutorMorselSource` turns a push child into scheduler-owned morsels without using the pull stream bridge.
- `drive_push_executor_into_parallel_sinks` drives a push child into worker-local sink states and returns them for combine/finalize.
- Hash join build now combines worker-local build sinks before finalize.
- Hash join probe now uses worker-local probe operators for supported parallel paths. Simple right/full/semi/anti paths use shared atomic match flags and one global unmatched-build finalization.
- Local and distributed root execution now enter through `PushQueryScheduler`, which is the root-level owner for the future scheduler DAG instead of calling `Executor::execute_push` directly from task runners.
