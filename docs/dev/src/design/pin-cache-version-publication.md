# Pin cache refill and version publication

## Review contract

These acceptance conditions govern the implementation; tests must not redefine them.

| Boundary | Required behavior |
| --- | --- |
| Version input | Apply complete deltas once; membership sees the complete version independently of refill filtering. |
| Common admission | Both backends consume the same logical-table and block vnode-range selection result. Sharing table metadata alone is insufficient. |
| Backend policy | Preserve main's ordinary Enabled and scoped-policy semantics. Pin enablement is independent; Foyer's L0, level, recent and inheritance rules cannot veto Pin. |
| Pin preparation | Process each event against its own target before loading metadata. After I/O, intersect surviving receipt-time intent with current ownership before submitting downloads. RESET then SET cannot revive withdrawn intent. |
| Ownership lifetime | Foyer owns inputs retained across asynchronous work. Pin borrows current bitmap inputs while holding the metadata read guard through synchronous admission/submission; no bitmap snapshot is needed inside that scope. |
| Foyer metadata-only work | Continue warming admitted SST metadata when disk data refill is disabled or the level is excluded, but skip block locality computation and data jobs. |
| Independence and publication | Each backend executes and times out independently. Every event becomes publishable after both attempts settle; the queue publishes settled FIFO prefixes. Shared I/O limits do not create shared completion scheduling. |
| Scope | No cross-event merging, endpoint predicates, retries, GC scans or production activation. Locality bounds remain conservative; downloads retain unit/whole-object granularity. |

## Preparation boundary

Cheap table/role/ownership checks run before backend planning. Full block locality
needs metadata. Pin selects the next event and discards unowned or cached candidates
before loading metadata, then rechecks current authority before download submission.

Common algorithms and backend adapters have separate source boundaries:
`admission.rs` loads SST metadata and selects local blocks from resolved inputs;
`admission/foyer.rs` adapts ordinary policies, snapshots and L0 serving-only rules;
`admission/pin.rs` adapts selected plans and surviving intent. The common algorithms
import no Pin plan/object, version delta or CN metadata owner. Both adapters call
the same loader and selector; neither runtime implements vnode overlap.

| Entry | Output and consumer |
| --- | --- |
| `foyer::load_metadata_and_select_blocks` | Loads metadata, preserves metadata-only warming, and returns `SstLocality` for Foyer unit generation. Foyer supplies only its data-refill eligibility and immutable contexts. |
| `pin::select_owned_candidates` / `pin::load_metadata` | Cached objects are rejected first; only owned candidates are copied under the selection guard. Metadata is loaded afterward. No download jobs are constructed. |
| `pin::iter_admitted_objects` | Rechecks current authority and surviving request intent, then yields only eligible objects with narrowed cancellation dependencies. Pin submits them synchronously under the same metadata guard. |

The `iter_` name denotes lazy admission: the iterator is consumed inside the same
metadata guard as submission, not collected or used after releasing it. Pin retains
the two phases because no metadata guard may cross I/O, and admission
must remain serialized with submission and invalidation. The worker owns their
lifecycle and timeout, but does not inspect block metadata or compute vnode overlap.
There is no new preparation worker, queue, mutable policy snapshot or retry loop.

## Responsibilities and admission

The event handler is the only writer of shared `TableCacheMetadata`: table cache
policies, the effective Pin table set, role and streaming/serving vnode ownership.
It separately owns `PinCacheMembership`, which registers received versions before
refill and retires removals after publication while protecting later received versions.
Neither refiller maintains membership. Snapshots and policy changes update membership
without creating download intent; only observed delta insertions create intent.

`CacheRefiller` applies shared table admission before dispatch. Ordinary Disabled
and scoped tables without local vnodes are rejected before job/snapshot allocation.
Pin has independent enablement and uses the union of role-projected ownership lanes.
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

## Pin event lifecycle

Pin submission captures event intent, enqueues it and wakes a single consumer on the
existing Tokio runtime. This consumer prepares one event at a time, independently
of Foyer and publication. There is no cross-event merging or batching configuration.
The executor owns physical downloads, deduplication and cancellation.

A queued request retains only its version ID and refill plan; the publication queue
and membership ledger retain the target version. The selected request stays queued
during metadata preparation so Pin withdrawal can
permanently narrow its intent. A later SET cannot recreate it. The worker releases
all guards for I/O, then borrows current ownership and intersects surviving intent
under the same read guard as synchronous executor submission. Metadata replacement
and invalidation share the writer guard; updates cannot miss a newly admitted job.
Lock order is metadata, pending requests, executor/cache locks. No guard crosses await.

Loss of any local vnode conservatively cancels dependent copies. Added ownership or
transfers between local streaming and serving lanes do not revoke a still-valid union.
No per-job vnode bitmap or new GC metadata read is introduced. Preparation may finish
after withdrawal but cannot submit withdrawn intent. Published files are not swept.

The event timeout covers metadata preparation and waiting for submitted downloads.
Failure and timeout allow remote fallback. Dropping a submitted waiter leaves its
executor-owned copy running; subsequent cancellation still finds it in the executor.
When an attempt settles, the worker removes its request, completes it and starts the
next event without waiting for publication. Later compaction cannot suppress an
intermediate event's downloads in this layer.

Dropping the refiller aborts the preparation worker and drops the executor owner.
Only the worker removes requests; owner shutdown must not concurrently clear its queue.
The handler drops refillers and publication queue together. Without Pin, Foyer retains
its original timeout, JoinError and detach-on-drop behavior.

## Publication and deferred work

The queue sees completion only, not backend success, batch IDs or endpoint predicates.
Every event can be published once both attempts settle, up to main's existing
16-event ready-prefix limit. Existing history and per-event local read-version updates
remain intact; watch readers and the uploader receive the prefix's final version.

Cross-event Pin merging and the corresponding publication endpoints belong to the
next layer. Ownership-driven published-file reclamation, physical GC scheduling and
production activation remain separate work. No read-through refill or automatic retry
is introduced.
