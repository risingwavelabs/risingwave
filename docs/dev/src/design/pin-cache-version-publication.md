# Per-event Pin refill and version publication

## Responsibilities

The version update queue applies incoming deltas once to construct unpublished
versions. Raw deltas remain available only until refill planning and membership
updates finish. Every event has a completion condition; the existing FIFO queue
publishes at most 16 completed events without bypassing an unfinished predecessor.

The event handler owns `TableCacheMetadata` as a plain value: table policies,
Pin table IDs, role and streaming/serving vnode mappings. Both backends borrow it
synchronously during submission. They keep no shared mutable metadata or policy
copy. Foyer captures immutable inputs for its existing asynchronous tasks.

The handler separately owns `PinCacheMembership`. Received versions register
objects before any download admission. Publication retires deletions while
protecting references in received unpublished versions. Policy changes rebuild
this protection across published and received snapshots, without creating refill
intent. Refiller success or failure does not decide logical membership.

`CacheRefiller` starts independent Foyer and Pin attempts and combines their
completion futures. Foyer retains per-event spawning and existing block admission.
Pin performs table admission and submits downloads directly to the existing
executor. There is no Pin event queue, batch worker, metadata-preparation stage,
shared metadata lock, or publication-endpoint predicate in this layer.

## Admission, execution and cancellation

Pin plans contain only observed insertions and surviving logical table projections
in this event's target. A full version snapshot restores membership without
creating refill intent. Each admitted Pin table must have at least one local vnode
under the worker role. This coarse admission can download objects with no actual
locally owned rows; block-level precision is deferred.

Admission and executor submission happen synchronously before the handler can
process a metadata replacement. Download jobs execute independently of polling
publication conditions. The timeout is constructed at submission, so a delayed
first poll does not grant the event a fresh timeout window. Ordinary errors,
executor-caught panics and timeouts allow remote fallback; neither backend's
failure bypasses the other backend's unfinished condition.

Policy withdrawal or losing all local vnodes for an admitted table revokes its
executor work. A nonempty-to-nonempty vnode change does not invalidate coarse
admission. Copies may outlive the event's waiter timeout; cancellation still finds
them in the executor. There is no automatic retry or ownership-gain backfill.

Dropping a completion waiter does not stop physical copies. Dropping the refiller
owner drops the executor and stops its driver. The handler owns the publication
queue and refillers together. Without a Pin backend, Foyer retains its original
JoinError and detach-on-drop behavior.

## Publication and scope

The queue sees only completion, not refiller-specific success, batch IDs or
required version ranges. Every event may become a published endpoint once both
attempts settle. This layer does not skip a commit's objects because of a later
compaction event: cross-event plan merging and its publication contract are a
separate capability.

Existing per-event local read-version and history updates remain intact; watch
readers and the uploader receive the completed prefix's final version. The CN
membership owner then retires obsolete registrations.

Ownership-driven reclamation and periodic physical GC scheduling are deferred.
Version deletion and Pin withdrawal still unregister routes through the existing
backend, but this PR adds no physical cleanup driver or disk-reclamation guarantee.
Final production activation must address GC separately. No read-through refill,
LRU, retry debt, batch configuration or pressure-forced publication is introduced.
