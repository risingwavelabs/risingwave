# Pin cache refill and version publication

## Responsibilities

The event handler is the only writer of shared `TableCacheMetadata`: table cache
policies, the effective Pin table set, role and streaming/serving vnode ownership.
Foyer borrows it during submission. The Pin worker receives the existing
`ReadOnlyRwLockRef`; each asynchronous attempt retains immutable task inputs.

The handler separately owns optional `PinCacheMembership` in
`event_handler::pin_cache_membership`. It contains the derived object reference index and the published /
received version ledger, without a shared lock or runtime. The index borrows the
current Pin table set; it does not keep another policy copy. Neither refiller reads
or writes this lifecycle state.

`CacheRefiller` constructs the independent backends and combines their conditions.
`FoyerRefiller` owns candidate admission, task projections, spawning, timeout and
result handling. `PinCacheRefiller` owns candidate construction, batch selection,
metadata preparation, downloads and cancellation. Shared metadata/data permits
bound I/O across both backends.

Received versions register Pin membership before backend submission, even when a
request waits behind an active batch or no download is admitted. Publication retires
removals while preserving references in received unpublished versions. Pin policy
changes rebuild membership across published and received snapshots; they do not
create download intent. The reference index counts logical SST references in the
latest received version; backend registration protects the version union.

The metadata lock never covers I/O or an await. Policy/vnode replacement, membership
policy rebuild and cancellation share its writer guard so admission cannot observe
an intermediate policy transition. Version receipt and publication mutate the
handler-exclusive lifecycle without a metadata write lock. Retirement unregisters
objects before cancelling their executor waiters; backend tokens prevent stale
publication. Lock order remains metadata, Pin batch state, executor/cache locks.
Workers read metadata before locking their batch state.

The handler constructs the refiller with read-only metadata and passes both to its
inner constructor. Foyer directly creates tasks at submission; controlled task
injection is test-only. Backend tests live in `refiller/foyer/tests.rs` and
`refiller/pin/tests.rs`; composition/publication tests remain in `refiller/tests.rs`.
Shared fixtures remain in `refiller/test_utils.rs`. `version_update` stays independent
of metadata policy and backend implementations.

The version update framework applies received deltas once to construct unpublished
version snapshots. Refill planning can advance beyond the version visible to readers.

Foyer and Pin own their admission, execution, timeout and cancellation. Foyer starts
an independent attempt for each received event. Pin may merge already queued adjacent
events, up to `pin_cache_max_batch_size`. It starts immediately when idle, without
waiting for a future compaction. An active batch is not retroactively rebatched.

Pin starts its next batch when its current attempt ends, regardless of Foyer or
publication progress. Publication notifies the CN membership owner; it never schedules
refill work. Existing shared I/O limits may still cause resource contention.

## Publication conditions

A condition waits for an event's independent Foyer attempt and its Pin batch attempt
to settle, then returns whether that event's target may be a publication endpoint:

| Condition state | Meaning |
| --- | --- |
| Pending | A prerequisite attempt is still outstanding. |
| Ready(true) | Attempts settled and this target is an allowed endpoint. |
| Ready(false) | Attempts settled, but a selected batch subsumed this target. |

`false` does not mean refill failed. Backend errors and backend-owned timeouts allow
remote fallback. They preserve the selected batch's endpoint decisions.

For example, a commit at v11 adds A and a compaction at v12 replaces A with B. If
both events queue behind an earlier Pin attempt, Pin can merge them and discard A
against the selected v12 snapshot before metadata loading and download allocation.
While the batch runs, both Pin conditions wait. Once it settles, Pin's v11 predicate
is false and v12's is true: v11 must not become an independent published head after
its refill was skipped. A later unselected version must not influence this pruning.

Each selected finite Pin batch has exactly one true predicate, at its final event,
including on error, timeout, cancellation, panic or task drop. All selected nonfinal
events return false, even if no SST was ultimately pruned. Unselected requests have
not been jointly pruned and retain their own endpoint on teardown. No-Pin events
return true after their Foyer attempt ends, preserving legacy task-error behavior.

## Queue and publication

The queue caches each condition result and examines the settled FIFO prefix. It
publishes through the last true result in that prefix. It stops at the first Pending
condition: a later Foyer task cannot substitute for an unfinished earlier attempt.
A false suffix stays queued until a later true endpoint settles. Canceling a queue
wait neither removes events nor causes completed futures to be polled again.

The queue receives no Pin batch IDs or required version ranges. It has no deadline,
backlog force-release or retry policy. It retains main's 16-event publication bound
when each event is an endpoint. If the boundary falls inside a subsumed batch, it
continues to the next allowed endpoint so that a selected Pin batch is never split.

Publication retires the prefix together and advances the current version to its final
target. Existing history and per-event local read-version processing are retained;
each local read version applies the prefix under its own write lock. Watch readers
and the uploader receive the final target. Historical snapshots can still read A
through remote fallback; neither historical cache coverage nor global atomicity
across different read holders is promised.

This contract covers batched Pin and per-event Foyer. Adding another independently
batched backend requires reviewing endpoint compatibility and progress; simply
ANDing arbitrary batching predicates is not established by this design.
