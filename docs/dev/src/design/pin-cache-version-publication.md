# Pin cache refill and version publication

## Review contract

These acceptance conditions govern the implementation; tests must not redefine them.

| Boundary | Required behavior |
| --- | --- |
| Version input | Apply complete deltas once; membership sees the complete version independently of refill filtering. |
| Common admission | Both backends consume the same logical-table and block vnode-range selection result. Sharing table metadata alone is insufficient. |
| Backend policy | One table policy selects ordinary Foyer refill or Pin. Preserve main's ordinary Enabled and scoped-policy semantics. Pin uses local ownership; Foyer's L0, level, recent and inheritance rules do not apply to Pin. Without a Pin backend, Pin tables skip refill. |
| Pin preparation | Select a queued prefix and prune its merged candidates against the fixed final target before loading metadata. After I/O, intersect surviving receipt-time intent with current ownership before submitting downloads. RESET then SET cannot revive withdrawn intent. |
| Ownership lifetime | Foyer owns inputs retained across asynchronous work. Pin borrows current bitmap inputs while holding the metadata read guard through synchronous admission/submission; no bitmap snapshot is needed inside that scope. |
| Foyer metadata-only work | Continue warming admitted SST metadata when disk data refill is disabled or the level is excluded, but skip block locality computation and data jobs. |
| Independence and publication | Each backend executes and times out independently. After both attempts settle, each event reports whether it may be a publication endpoint. The queue publishes settled FIFO prefixes without splitting a selected Pin batch. Shared I/O limits do not create shared completion scheduling. |
| Scope | Cross-event merging belongs to Pin. No retries, GC scans, queue deadlines, backlog force-release. Production activation is wired by the separate layer below. Locality bounds remain conservative; downloads retain unit/whole-object granularity. |

## Preparation boundary

Cheap table/role/ownership checks run before backend planning. Full block locality
needs metadata. Pin selects up to `pin_cache_max_batch_size` queued events, coalesces
owned uncached candidates and prunes them against the selected final version before
loading metadata. Later events cannot change this endpoint. After I/O it rechecks
current authority before download submission.

Common algorithms and backend adapters have separate source boundaries:
`admission.rs` loads SST metadata and selects local blocks from resolved inputs;
`admission/foyer.rs` adapts ordinary policies, snapshots and L0 serving-only rules;
`admission/pin.rs` adapts selected plans and surviving intent. The common algorithms
import no Pin plan/object, version delta or CN metadata owner. Both runtimes call
the same metadata loader, and both adapters use the common locality selector;
neither runtime implements vnode overlap.

The CN fields and Foyer task snapshot follow the ownership boundary in #27614.
Role-projected ownership is queried directly on `TableCacheMetadata`; no separate
ownership or per-table locality view is maintained. Foyer's existing snapshot
interprets ordinary policy and the L0 serving rule. Pin checks its surviving table
intent and the current owned bitmaps under the metadata guard. Both callers reuse the store metadata cache keyed by physical object ID; they do
not share an event-wide preparation barrier or retain one prepared result for all
future calls. Concurrent fetches coalesce in that cache; eviction can require a later
fetch. The common selector
computes physical block ranges and applies these admission predicates without
interpreting `CacheRefillPolicy`.

| Entry | Output and consumer |
| --- | --- |
| `admission::load_sst_metadata` | Loads a batch through the shared store metadata cache, retaining each input alongside its metadata. Both callers preserve their own preparation/error and timeout boundaries. No additional result cache or scheduler is introduced. |
| `foyer::select_blocks` | Adapts logical projections and L0 serving rules to the common locality selector after metadata preparation. Metadata-only warming completes without generating Foyer data work. |
| `pin::select_owned_candidates` | Cached objects are rejected first; only owned candidates are copied under the selection guard. The common loader prepares metadata afterward. No download jobs are constructed. |
| `pin::iter_admitted_objects` | Rechecks current authority and surviving request intent, then yields only eligible objects with narrowed cancellation dependencies. Pin submits them synchronously under the same metadata guard. |

The `iter_` name denotes lazy admission: the iterator is consumed inside the same
metadata guard as submission, not collected or used after releasing it. Pin retains
the two phases because no metadata guard may cross I/O, and admission
must remain serialized with submission and invalidation. The worker owns their
lifecycle and timeout, but does not inspect block metadata or compute vnode overlap.
There is no new preparation worker, queue, mutable policy snapshot or retry loop.

## Responsibilities and admission

The event handler is the only writer of shared `TableCacheMetadata`: table cache
policies, role and streaming/serving vnode ownership. Pin membership and admission
query the same policy map; no separate Pin table set is stored or notified.
The default table policy initializes this metadata directly from storage options;
`CacheRefillConfig` contains only execution settings. Shared read access is needed
by Pin's asynchronous worker; the handler remains the sole writer.
It separately owns `PinCacheMembership`, which registers received versions before
refill and retires removals after publication while protecting later received versions.
Neither refiller maintains membership. Snapshots and policy changes update membership
without creating download intent; only observed delta insertions create intent.

`CacheRefiller` applies shared table admission before dispatch. Ordinary Disabled
and scoped tables without local vnodes are rejected before job/snapshot allocation.
The Pin policy uses the union of role-projected ownership lanes.
Ordinary Enabled retains main's unscoped normal-refill behavior.

`admission::foyer::select_table_inputs` excludes Pin projections and applies Foyer's L0
serving rule before capturing immutable task contexts. These rules do not narrow
Pin input. A mixed physical SST can supply logical projections to both backends.
Foyer receives task inputs rather than CN metadata. It warms metadata through the
shared loader, then checks disk-cache and target-level eligibility before computing
locality. Metadata warming still happens when data refill is disabled.

Both backends consume `admission::select_local_blocks` and its `SstLocality` result:
a metadata holder and eligible block bitmap intersected with logical projections.
Foyer groups eligible blocks into table-bounded units, applies recent/inheritance
filters and downloads blocks. Pin downloads a whole physical object only if at least
one admitted logical projection has a local block. Main's conservative vnode range
bounds are preserved; this is not an exact per-row residency guarantee.

## Pin batch lifecycle

Pin submission captures event intent, enqueues it and wakes a single consumer on the
existing Tokio runtime. This consumer selects an already queued prefix, independently
of Foyer and publication. The default batch bound is 16 events; a value of 1 keeps
per-event attempts. There is no batching timer or wait for a future compaction.
The executor owns physical downloads, deduplication and cancellation.

A queued request retains its target snapshot, refill plan and completion sender.
The target is needed to prune a future selected batch against that exact version;
latest received metadata cannot substitute for it. The selected prefix stays queued
during metadata preparation so Pin withdrawal can permanently narrow its intent.
A later SET cannot recreate it. The worker releases
all guards for I/O, then borrows current ownership and intersects surviving intent
under the same read guard as synchronous executor submission. Metadata replacement
and invalidation share the writer guard; updates cannot miss a newly admitted job.
Lock order is metadata, pending requests, executor/cache locks. No guard crosses await.

Loss of any local vnode conservatively cancels dependent copies. Added ownership or
transfers between local streaming and serving lanes do not revoke a still-valid union.
No per-job vnode bitmap or new GC metadata read is introduced. Preparation may finish
after withdrawal but cannot submit withdrawn intent. Published files are not swept.

The batch timeout covers metadata preparation and waiting for submitted downloads.
Failure and timeout allow remote fallback. Dropping a submitted waiter leaves its
executor-owned copy running; subsequent cancellation still finds it in the executor.
When an attempt settles, the worker drains exactly the selected prefix, reports
false for its intermediate targets and true for its final target, then starts the
next batch without waiting for publication. Failure and timeout preserve these
endpoint decisions. Later queued requests cannot alter an already selected batch.

Dropping the refiller aborts the preparation worker and drops the executor owner.
Only the worker removes requests; owner shutdown must not concurrently clear its
selected prefix.
The handler drops refillers and publication queue together. Without Pin, Foyer retains
its original timeout, JoinError and detach-on-drop behavior.

## Publication conditions and deferred work

Every event keeps its independent Foyer completion. Its composite condition waits
for Foyer and its Pin batch attempt, then returns the Pin endpoint decision:

| Result | Meaning |
| --- | --- |
| Pending | An attempt is unfinished; a later event cannot bypass it. |
| Ready(false) | Attempts settled, but this target was subsumed by a selected batch. |
| Ready(true) | Attempts settled and this target is an allowed publication endpoint. |

For a queued commit v11 adding A followed by compaction v12 replacing A with B,
Pin can select both and skip A against v12 before metadata preparation. While Pin
runs both conditions wait. After it settles, v11 reports false and v12 true;
publication still waits for both events' Foyer attempts. An ordinary failure or
backend timeout permits remote fallback without changing those endpoint decisions.

The version queue only sees these results, not Pin plans, batch IDs or required
version ranges. It caches completed results so a canceled wait cannot repoll a
completed future. It publishes the settled FIFO prefix through the last true result;
a false suffix stays queued until its endpoint settles. Main's 16-event publication
bound is preserved when every event is an endpoint, and extends only to avoid
splitting a selected batch. Existing history and per-event local read-version
updates remain intact; watch readers and the uploader receive the prefix's final version.

Without Pin, each settled Foyer event is an endpoint and retains the original
JoinError and detach behavior. Handler shutdown drops refillers and the publication
queue together. Ownership-driven published-file reclamation remains separate work.
No read-through refill or automatic retry is introduced.

## Activation layer

The activation layer enables the backend only with a configured local directory.
Startup consumes the version and table policies from the same Meta snapshot before
constructing the cache and starting event processing. Recovery restores files by
version membership without remote data or metadata reads, and does not backfill
missing objects. Subsequent delta insertions use the shared admission path above.

`cache_refill_policy = "pin"` selects whole-SST Pin refill. The experimental
`pin_cache_table_id` selector sets this same policy for one physical table while
other tables retain the job policy. Pin has no fallback policy: a worker without
a Pin backend rejects its refill work, rather than scheduling Foyer refill.
Query reads remain independent: a missing or failed Pin read uses the ordinary
Foyer/remote path without changing the table policy or refilling Pin on reads.

After recovery, a storage-owned GC worker runs one full pass for startup garbage
and periodic minor passes for withdrawn files whose readers have exited. Storage
shutdown aborts this worker. This does not add ownership-driven scans of published
files: unpin and version retirement still drive their withdrawal.
