# Fixed-state HashJoin I/O replay

This opt-in benchmark replays a bounded probe epoch through the real HashJoin,
StateTable, Hummock, Foyer and object-store scheduler. It requires only MinIO,
not a running RisingWave cluster. It is a read-path experiment, not a production
throughput benchmark or a general streaming trace recorder.

The right side is committed once. Subsequent runs reopen the exact same SST
objects and Hummock version. Each sample starts a fresh four-thread runtime,
storage instance, Join cache and byte-weighted 64 MiB Foyer block/meta caches.
The cache weights and eviction policies follow the production construction.
At the explicit64MiB budget both caches use one shard, matching CN sizing;
the common default helper returns shard bits and is not used for auto shard
selection. Disk caching is disabled explicitly. Output chunk size comes from
ActorContext configuration (default 256); fixture SST/block targets come from
StorageOpts defaults (256 MiB/64 KiB). Writer resources remain bounded test
budgets, and effective options are saved beside each run. Only left input
chunks are ready after the initial barrier, so the production random two-input
poll order cannot change the probe sequence. Production HashJoin is unmodified.

## Build and run

Use the local shared test guard when other experiments may be running. Start a
dedicated MinIO server and create **a separate empty bucket for each fixture**.
Keep the bucket data: the saved version refers to those immutable objects.
Do not point this tool at a development/production bucket with existing SSTs.

```sh
mkdir -p target/my-join-replay
cp src/stream/benches/hash_join_replay/multi-block.json target/my-join-replay/config.json
export RW_HASH_JOIN_REPLAY_URL='minio://USER:PASSWORD@127.0.0.1:9301/DEDICATED-BUCKET'
# Set "rw_config": "src/stream/benches/hash_join_replay/read-optimization.toml"
# in target/my-join-replay/config.json to enable the candidate controls.
cargo bench --profile dev -p risingwave_stream --bench stream_hash_join_replay -- target/my-join-replay/config.json
python3 src/stream/benches/hash_join_replay/summarize.py target/my-join-replay/results-TIMESTAMP
```

The `--profile release` build option is useful for CPU/latency comparisons. Keep
build profiles, toolchains and runtime threads identical between compared
binaries. Initial local validation uses dev builds and does not predict S3
latency. Credentials are read from the environment and not saved in the results.

The first invocation creates `fixture.pb`, `fixture-spec.json` and
`fixture-manifest.json`. The manifest records object content fingerprints and
block offsets/compressed/uncompressed sizes. Later invocations verify the
manifest and **do not regenerate or upload the fixture**. A change in data shape, fixture format, SST/block targets
or table definitions requires a new directory and bucket. The noncryptographic
fingerprint is for accidental fixture drift, not a security boundary or a
cross-toolchain format guarantee.

Each invocation creates a new `results-TIMESTAMP/`, preserving earlier results.
It copies the config and alternates scheduler-off/on sample order. Each sample
contains raw `before.prom`, `after.prom` and `result.json`.

## Cases and interpretation

| Config | Fixture / question |
|---|---|
| `single-block.json` | Small prefixes sharing one SST block; expose overhead with no adjacent cold blocks to combine. |
| `multi-block.json` | Wide prefixes in a fixed SST set; expose bounded lookahead and range coalescing. |
| `cache-holes.json` | Same wide-prefix shape with every third SST block loaded through ordinary Foyer before timing. |
| `128mib-short.json` | 8192 independent keys × 16 rows, 128 MiB payload; input chunks of 256. |
| `128mib-wide.json` | 512 independent keys × 256 rows, 128 MiB payload; input chunks of 256. |
| `512mib-wide.json` | 2048 independent keys × 256 rows, 512 MiB payload; input chunks of 256. |

Optional `probe_rounds` (default 1, maximum 4) repeats the fixed key sequence
with unique left primary keys. `probe_pattern` defaults to `insert_only`;
`insert_delete` inserts all probe keys and then deletes those identical rows
in each round. The output oracle checks both operations and their order.
`probe_order` selects `stride` (the original
default), `sequential`, or a fixed seeded `shuffled` permutation. Sequential
means logical key order, not a guarantee about global vnode/SST order. Repeated
rounds retain the same ordering and model subsequent probes with a warm Join
cache when each entry fits the production row limit; they do not emulate actor
memory-manager eviction. Large fixtures retain the output for validation
outside timing, so allow memory for state, Join cache and full joined output.
Payload size is not physical SST/read bytes. Optional `schedulers` selects one or both arms
(default `[false, true]`). `trace: true` enables separate JSON read-path
diagnostics; never mix traced timing with untraced performance samples.
`warm_every_nth_block: 1` warms every data block
and the summary requires zero remote reads. Join lookup counters flush only
on a commit barrier, absent here, and are not collected. Earlier result files
containing zero deltas cannot establish zero lookups or misses.

Set the replay JSON's `rw_config` field to a production TOML path. The bundled
`read-optimization.toml` enables blockwise prefetch and sets scheduler bounds. The replay `schedulers` array
still selects scheduler OFF/ON arms, with bounds loaded from that same TOML.
Omitting `rw_config` uses disabled production defaults. Restart the process after
changing it. Neither in-image mode is clean main: other candidate source changes
remain. Both arms use identical fixture
objects and probe chunks. The first scope is inner equijoin, insert probes,
fixed right state, one actor and one measured epoch; it does not cover degree
updates, two-sided changelog interleaving, watermarks, barriers/commits during
measurement, distributed scheduling or backpressure. These are deliberate
limits, not general operator replay support.

The fixture is a constructed locality case, not a recording of Q20. Its cache
and data sizes make the geometric mechanism visible; a successful synthetic
case does not establish that Nexmark has the same opportunities. Probe order
is a fixed permutation, chunk boundaries are explicit, and validation checks
every joined value plus uniqueness/completeness, not just output row counts.
Output collection drains the executor to EOF without a synthetic commit barrier and is timed; expensive row-content validation is outside timing.

The replay storage installs the frozen version using the existing test version
injection API. Its fresh mock meta manager is used for catalog setup, not for
committing changes against that version. The probe epoch must not upload SSTs
or change the version. Dropping its dedicated runtime terminates its tasks;
probe writes remain uncommitted and are discarded. The immutable fixture is
retained for the next run. Successful reads must all finish before the end
snapshot; failures/retries invalidate a sample.

The summary checks equal output counts, equal fetched bytes, real physical I/O
exposure, scheduler activation, and logical/physical/group-count reconciliation.
A change in fetched bytes is an investigation signal, not something to normalize
away. Tokio execution and MinIO completion timing remain real: **do not assert
an exact merge count or deterministic elapsed time**. Repeatability means fixed
work/initial state plus measured variance. Scheduler algorithm interleavings
still need their own deterministic unit tests.

## Recorded breadth round (2026-11-09)

Correction from the subsequent scale preflight: this old round used the generic
mock store constructor, whose default cache weight is **one per entry**, not
bytes; the old 64 MiB cache description was incorrect. It also used output
chunks of 1024 and test SST targets of 4 MiB. Its observations remain valid
for that small fixture, but do not establish production cache-pressure behavior.
The revised harness uses snapshot format 2 and requires new fixtures.

A separate harness-only build of historical clean base `88b9811` and the current
candidate replayed identical immutable fixtures: 11 cases, 7 repetitions for
main/current-off/current-on (231 samples). All exact output, equal per-case read
byte, immutable fixture and candidate scheduler accounting checks passed.

| Case | Median GETs main/off/on | Median probe ms main/off/on |
|---|---:|---:|
| Short prefix, 16 rows/key | 70/70/68 | 83.10/78.30/80.18 |
| Wide prefix, 256 rows/key | 70/70/62 | 79.16/53.06/54.47 |
| Long prefix, 1024 rows/key | 70/70/59 | 78.46/46.47/48.40 |
| Alternating blocks hot | 35/35/35 | 47.33/40.25/40.71 |
| All blocks hot | 0/0/0 | 19.07/19.66/18.54 |

Read-ahead concurrency accounts for most of the measured latency benefit;
coalescing further reduces GETs without consistent incremental latency benefit
on local MinIO. Increasing chunk size 1→8→16 did not show a reliable merge gain.
Four probe rounds produced four times the output with unchanged remote bytes.
These dev-build samples do not establish production S3 performance or absence
of overfetch for partial/early-terminated scans. Even fixed SSTs do not make
Tokio merge grouping deterministic: wide-prefix on used 55–65 GETs.

Full matrix, binary/source identities, limits and evidence:
`target/hashjoin-replay-cases.jd4lgifr/round-summary.md` and `execution.md`.
The owned MinIO server is stopped; its immutable fixture data is retained.


## Recorded scale/N round (2026-11-09)

The revised harness used byte-weighted64MiB memory caches, CN-equivalent one
shard at that budget, output chunks256, input chunks256, and default256MiB/64KiB
SST/block targets. Three fixtures and seven arms (main, N1/N3/N8 off/on) ran
three times each:63 performance samples. An initial128MiB real-load preflight
and three128-wide trace samples are excluded from timing statistics.

| Case | Data blocks | Median GETs main/N3-on/N8-on | Median seconds main/N3-on/N8-on | Read MiB main/N3/N8 |
|---|---:|---:|---:|---:|
|128MiB,256 rows/key|2151|2663/2306/1736|2.563/1.690/1.437|164.47/151.97/137.63|
|128MiB,16 rows/key|2151|10343/10159/10138|8.273/7.318/7.368|639.61/639.61/639.61|
|512MiB,256 rows/key|8602|10650/10412/8600|9.351/6.211/5.871|657.99/657.99/657.99|

N8 actually reaches at most4 blocks under the256KiB byte cap. The128-wide trace
reads the same2151 unique blocks in all arms, with509/307/75 repeat remote block
reads for N1/N3/N8 and no overlapping same-block remote reads. N3 traces directly
show same-window adjacent reads split across scheduler batches; independent
Foyer producers do not form an atomic submission batch. N8's improved128-wide
cache residency does not persist at512MiB. Scheduler reduces GET count but does
not consistently improve latency beyond read-ahead in local MinIO/dev runs.
Production window constant remains3; no universal optimum is claimed.

Full ranges, all seven arms, source/binary identities, preflight failures,
trace limits and cleanup: `target/hashjoin-replay-scale.zyly5tft/round-summary.md`.


## Byte-cap / applicability round (2026-11-09)

Evidence: `target/hashjoin-replay-caps.1033b773/round-summary.md`, with all 30
case/arm rows, 3 repeats each, 5 separate diagnostic traces and `execution.md`.
Base remains unoptimized `88b98114336ae4511dcb9685afa95678e948e65a`, not a newly
fetched main. All arms use the same revised harness and immutable SSTs. New
input controls are sequential and fixed shuffled order; no product default
was changed. Meta batch reuse is disabled to isolate prefetch/scheduler.

The 256 KiB control uses N8, effectively at most 4 blocks. The 1 MiB candidate uses N16
and raises both the leaf and scheduler caps; intermediate N8 variants isolate
the two ceilings. Per-cell values below are medians of 3 samples.

|Case|GET main /256KiB /N16-1MiB|Seconds main /256KiB /N16-1MiB|Read MiB256KiB /N16-1MiB|
|---|---:|---:|---:|
|short-shuffled|6308 /6168 /6148|6.206 /6.022 /5.956|393.50 /393.50|
|wide-shuffled|2318 /1688 /1518|2.519 /1.538 /1.296|133.92 /138.92|
|wide-sequential|2154 /1625 /1191|2.396 /1.608 /1.179|132.99 /132.99|
|long-shuffled|2190 /1683 /547|2.343 /1.366 /0.929|134.28 /134.04|
|narrow-shuffled|3165 /1839 /1901|7.546 /5.617 /5.591|185.33 /185.33|
|long-repeat4|2190 /1663 /579|3.116 /2.066 /1.613|134.28 /134.04|

Long physical prefixes benefit; a large row count with narrow projection does
not imply benefit from a larger cap. Trace confirms actual windows 4→8→16,
maximum merged GET ~823 KiB, and no overlapping same-block remote producers.
The two wide-prefix traces attribute all 5.01 MiB of additional bytes to repeat
loads 15→96 on the same 2,151 blocks, not new block coverage or gap reads. Larger
windows change how many producers enter Foyer with Hint::Low; normal demand
does not automatically promote that entry to the high-priority LRU pool.
Hint and completion-order contributions remain unisolated.

These are cold single-actor fixed-state experiments with 64 MiB caches and
high-entropy payload, not claims about typical join fanout or production S3
latency. A useful production case must retain enough projected bytes, miss
higher-level caches and expose adjacent cold blocks within the same SST.
Current defaults are restored to N3/256 KiB and scheduler 256 KiB; multi-actor
admission/memory behavior and cache-treatment choices remain future work.
All 90 samples passed output/accounting gates; Clippy and scheduler tests at
both caps passed. Owned processes stopped, fixtures preserved.


## Existing high-fanout workload shapes (2026-11-09)

`fanout-*.json` reuses cardinalities and dimension insert/delete patterns from
unaligned-join recovery/scale/SLT tests, running ordinary inner HashJoin without
SyncLogStore or downstream aggregation. The test input remains a simplified
key/id/payload schema and fixed key-major-ingested SSTs; it is not an exact SQL
or multi-actor topology reproduction. Empty payload represents narrow state;
128 B payload is a separately labeled cache-pressure extension.

Evidence: `target/hashjoin-replay-fanout.ukqb4xov/round-summary.md`,
`index.jsonl`, `warm-followup-index.jsonl`, `warm-summary.json`, and
`execution.md`. Compare frozen clean base `88b9811` against N16/1MiB prefetch
and scheduler. These input configs alone use current-code scheduler off/on;
that is not a clean-main comparison. Full main-relative measured results:

|Case|Pairs|GET main / candidate|Read MiB main / candidate|Seconds main / candidate|Elapsed change vs main|
|---|---:|---:|---:|---:|---:|
|20k-cold|3|116 / 51|7.15 / 7.15|0.947 / 0.790|-16.52%|
|20k-warm|9|0 / 0|0.00 / 0.00|0.769 / 0.776|+0.94%|
|20k-changelog|3|116 / 47|7.15 / 7.15|1.811 / 1.671|-7.74%|
|60k-changelog|3|688 / 255|42.97 / 42.97|16.653 / 16.010|-3.86%|
|200k-changelog|3|230 / 80|14.32 / 14.32|2.562 / 2.334|-8.92%|
|60k-payload128|3|12206 / 4123|760.69 / 721.41|28.705 / 17.061|-40.56%|

Warm results include nine pairs; other rows are three-pair medians. All nine
warm pairs were slower on the candidate, despite zero GET/bytes: a small
observed main-relative regression remains. Other cases improve elapsed. Narrow
60k/200k entries exceed the default 30000-row Join cache limit but their entire
fixtures fit 64 MiB Foyer, so repeated scans do not imply repeated remote I/O.
The 128 B extension exceeds block-cache capacity and has the largest benefit.
No product default was changed. Clippy, exact signed-output oracle, immutable
fixtures and read accounting passed for all 48 measured samples; runtime
cleanup was verified and fixtures retained. See the report for limits and
regression details rather than interpreting GET savings alone as performance.

## Retained response window (2026-11-09)

`target/hashjoin-retained-window/round-summary.md` records a focused follow-up:
the SST iterator retains bounded Foyer responses and consumes them directly,
instead of dropping speculative responses and querying the cache again. There
is no `contains` fast path. Failed speculative reads fall back to ordinary
demand get; seek/end/drop release retained neighbors.

The same 10-key x 20000-row fixture and frozen main/previous binaries were used
with a new N16/1MiB candidate. Source defaults remain N3/256KiB. One actor,
chunk256, real Foyer/MinIO,64MiB cache, meta reuse disabled, dev build.

|Case|Repeats per arm|Main seconds|Previous seconds|Retained seconds|Retained vs main|
|---|---:|---:|---:|---:|---:|
|All warm|6|0.781230|0.782247|0.778500|-0.35%|
|Cold|3|0.933338|0.800221|0.792614|-15.08%|

These are medians. All warm runs had zero GET/bytes. Cold GET medians were
116/51/53, with identical 7,499,765 bytes in every sample. Coalescing did not
consistently improve. New was faster than previous in only3/6 warm pairs;
previous's main-relative median slowdown was0.13% this time. Thus this does
not attribute all of the earlier0.945% regression to duplicate cache lookups
or establish a robust warm speedup. The removed lookup is proven separately
by consuming a retained hot block after removing it from cache and damaging
its backing data. Fourteen prefetch tests and bench Clippy passed.

Retaining handles keeps the shared block allocations alive; it does not prevent
every form of cache eviction. The existing count/byte limits are not a CN-wide
memory limit. This single-SST follow-up does not cover memory
pressure with many active SST iterators. See the round report for all paired
timings, hashes, validation and verified cleanup; fixtures remain available.

## Actual unaligned join on a full cluster (2026-11-09)

`target/unaligned-full.s6fa0i_6/round-summary.md` records the real workload from
`e2e_test/streaming/unaligned-join.slt`: two keys with 200000 fact rows each,
dimension DELETE, actual unaligned HashJoin/SyncLogStore and downstream
GROUP BY/Materialize. This is separate from the ordinary-Join replay above.
Both binaries were freshly built from base `88b9811` with matching dev flags;
the candidate patch and binary hashes are frozen beside the evidence.

Runs used guarded `./risedev d full`, default parallelism4 and chunk256, real
MinIO, CN8GiB, block cache688MiB, meta cache917MiB. Candidate retained prefetch
uses source defaults N3/256KiB with scheduler enabled. Experimental native
MultiGet/meta batch reuse is disabled. Order: main-a, candidate-a, candidate-b,
main-b. Cold phases restart the cluster with persisted data before DELETE.

|Phase/pair|GET main / candidate|Read MiB main / candidate|Drain seconds main / candidate|Candidate coalesced calls saved|
|---|---:|---:|---:|---:|
|Normal A|768 / 825|50.75 / 57.95|29.87 / 33.70|59|
|Normal B|882 / 719|57.97 / 51.96|27.27 / 31.32|65|
|Cold A|1070 / 1269|71.83 / 87.65|26.83 / 42.10|72|
|Cold B|915 / 864|59.72 / 60.81|30.33 / 28.45|80|

All eight measured output oracles passed, with no object failures/retries or
recovery during measurement. Normal latency regressed in both pairs; cold
latency changed in opposite directions. This does not establish a stable
end-to-end benefit. Two-sample normal mean GET fell6.42% but drain rose13.80%.

Existing high-amplification logs place the two Join match-loop endpoints near
2.0–2.2s on main and1.75–1.96s on candidate, well before full output drain.
They are approximate operation-relative endpoints, not isolated IO timings.
The long tail follows bulk Join matching in the log-store/aggregation/
materialization pipeline. Scheduler queue means were52–238us per request.
The additional downstream work/CPU has not been causally isolated.

SST layouts differ across runs. CN read totals include the whole pipeline;
increased bytes cannot be attributed to speculative overread from these totals.
Per-table storage metrics flush only after more than32 reports per thread/table,
so zeros for two large Join scans do not establish absence of reads. See the
report for raw inventories, metrics, logs, diagnostic scripts, excluded harness
initialization failures and verified cleanup. Owned services/data were stopped/
removed, scratch restored clean, normal workspace runtime/data left untouched.

Offline follow-up: `target/unaligned-full.s6fa0i_6/downstream-audit.md` combines
the existing metrics with base/candidate source and frozen-binary disassembly;
no workloads were rerun. It identifies new scalar-read costs even with native
MultiGet disabled: an async helper transition before Bloom rejection and an
enlarged common seek future despite disabled point-read prefetch. Dev sanity
checks amplify scalar-read volume during state flush. In normal pair A the
candidate made fewer Agg reads and fewer combined Agg/MV cache accesses yet
used more CN CPU, so differing SST layouts alone do not explain the regression.
Worst cold A also has measurable SST-access amplification. Per-function timing
contributions remain unquantified. Actor poll counters publish on 15s intervals
and sparse storage counters can be delayed; neither is an exact endpoint-based
breakdown of this short run. The audit contains the evidence and proposed scope
of correction; no production fix or post-fix result is claimed.


### Scalar future correction after offline diagnosis (2026-11-09)

`target/scalar-read-future-fix/round-summary.md` records the incremental source
fix, Astra review, validation and matching dev-binary inspection. No workload
was rerun. The point metadata helper is now one async body accepting optional
prepared metadata. Range refill and admitted Blockwise registration construct
their boxed futures outside the common seek poll; cached consumption and cheap
rejection remain inline. N3/256KiB, retained responses, Foyer single flight,
metadata reuse and scheduler behavior are preserved. A sync_point feature build
failure in the earlier synchronous candidate coroutine was also corrected by
moving right-bound rejection to both async consumers before statistics and IO.

|Dev static observation|Main|Previous candidate|Corrected candidate|
|---|---:|---:|---:|
|seek_idx poll frame bytes|9952|17520|8752|
|Pre-Bloom nested helper future copy bytes|absent|2752|absent|
|Demand get future copy bytes at seek site|1256|1616|1616|

22 targeted tests passed during the correction (prefetch, point, batch delete,
success/cancellation and vnode pruning). The final Range factory extraction
repassed14 affected prefetch tests, affected Clippy and full dev binary build.
Final binary SHA256 is
`829f390e19a2828bf8c2c8fb82e864e1fd84decc99b3adb942cc0e8d9a571267`.
These are stack-layout/correctness findings, not a new throughput or latency
result. Remaining lower-level future growth and SST visitation differences are
not resolved by this evidence; recovery of the prior normal13.8% drain regression
has not been measured. All validation used the shared local-test guard and left
no owned runtime/test/compiler process.


### Remaining get state and same-protocol retest (2026-11-09)

`target/get-future-followup/round-summary.md` records a further local correction:
private object read_direct now boxes its existing async body at actual IO
execution, keeping the large retry state out of cache-facing read futures. No
Box is placed in Foyer's eagerly called fetch factory; cache hits/followers do
not allocate this box. Trace, logical/physical metrics and cancellation ownership
remain unchanged. Demand get future copy1616→904B (main1256B), common seek poll
frame8752→7328B (main9952B). Astra review,19targeted tests, affected Clippy and dev
build passed. Candidate binary SHA256:
`b495efbc09aca8957247acc38372c520bb062cc563207939138c049a21530918`.

`target/unaligned-get-fix.3dxbcc_7/round-summary.md` records the actual retest,
using the same SQL/configuration/measurement protocol as the original full
unaligned-join round. Saved main88b9811 binary was reused, not rebuilt. Order
main-a/candidate-a/candidate-b/main-b, normal and restart-cold DELETE each,
8valid samples under the shared guard and risedev d full. Native metadata batch
reuse remained OFF; schedulerON and retained prefetchN3/256KiB on candidate.

|Two-sample means|main|corrected candidate|change|
|---|---:|---:|---:|
|Normal drain seconds|29.268|33.628|+14.90%|
|Normal CN CPU seconds|114|131.5|+15.35%|
|Normal GET|819|744.5|-9.10%|
|Normal read MiB|54.080|53.610|-0.87%|
|Cold drain seconds|30.113|36.272|+20.45%|
|Cold CN CPU seconds|119|145|+21.85%|
|Cold GET|911|1149.5|+26.18%|
|Cold read MiB|60.851|80.035|+31.53%|

No end-to-end benefit is established. Normal weighted Agg application get mean
improves220.9→200.1us, but Agg/MV logical data/meta accesses rise23.6%. Counters
include debug flush sanity reads while monitored get duration/count cover the
application path: do not divide them to infer SST accesses per application get.
Initial Agg/MV SST counts are3on all arms, so different initial file counts do
not explain normal regression. Restart counts main1/3vs candidate5/6establish
cold-layout differences without proving they explain the full slowdown. Queue
sums<0.21s and early Join match endpoints do not explain the downstream CPU tail.
Next diagnostic should isolate flush sanity-check read counts/work/duration;
no new matrix or speculative code fix was launched after these results. All
output/error/recovery checks passed, owned runtime cleaned and scratch clean.
Local dev builds, MinIO and two samples limit production/S3 generalization.

## Fixed-version scalar get diagnostic (2026-11-10)

The opt-in `point_reads: true` mode replays raw scalar gets with no prefix hint,
matching the read options of debug flush sanity checks. `fixture_epochs` defaults
to1; values2–8 interleave unique keys across committed epochs and are supported
only in point mode. Point order is a fixed shuffle; `probe_pattern` is not used.
Snapshot format3 requires new fixture directories; previous evidence is retained.
Both variants install the same info subscriber when trace diagnostics are off,
flush local metric buffers outside timing and recheck the object manifest after
replay. Storage debug metrics are enabled for per-category work reconciliation.

One400k-row/four-epoch fixture was read by historical cleanmain88b9811 and current
candidate (scheduler on, native MultiGet/meta reuse off), three pairs per cache
state.4worker threads, real MinIO/Foyer,64MiB byte-weighted memory caches; devbuild.
Every sample had999,994 overlapping SST visits and1,000,564 block requests, with
exactly equal metadata and per-source work counters across both binaries.

|Case|GET main / candidate|Read bytes main / candidate|Median seconds main / candidate|Change|
|---|---:|---:|---:|---:|
|Fresh Foyer|236 /236|15,003,380 /15,003,380|9.744 /9.765|+0.21%|
|All blocks hot|0 /0|0 /0|9.431 /9.569|+1.45%|

All12samples passed value, snapshot immutability, work equality and I/O gates.
The hot slowdown had the same sign in all3pairs (0.84–1.72%); cold paired changes
were -0.90–+1.35%. This does not reproduce the full unaligned pipeline15–20%
regression at equal read work, but does not establish zero hot-path overhead.
This is neither the full streaming pipeline nor a test of coalescing benefits:
serial point reads provide no concurrent merge opportunity. See
`target/fixed-point-snapshot/round-summary.md`, `comparison.json`,
`results-table.md` and `execution.md`. Owned MinIO stopped; fixture retained.

## Current large-fixture HashJoin replay (2026-11-10)

Following the small scalar diagnostic, return to actual HashJoin prefix scans on
fixed data larger than the64MiB block cache. Reuse the old scale SST objects and
version unchanged (copy-on-write MinIO clone), with recorded fixture-spec2->3
migration adding fixture_epochs1. Both binaries verify the original manifest
before and after every run. No regeneration or production code change.

|Case / actual SST size|Data blocks|Median GET main/current|Median seconds main/current|GET / elapsed change|
|---|---:|---:|---:|---:|
|16rows/key,133MiB|2,151|10,343 /10,034|11.010 /9.992|-2.99% /-9.25%|
|256rows/key,532MiB|8,602|10,650 /7,955|19.081 /14.730|-25.31% /-22.80%|

Three alternating pairs per case;12/12samples passed. Payload1KiB, one actor,
four runtimeworkers, chunk256, real MinIO and byte-weighted Foyer. Main savedbase
88b9811 vs currentN3/256KiB retained prefetch+scheduler256KiB. Native metadata
batch reuse off. Same devprofile/info subscriber/Debug storage metrics in both.
Read bytes match exactly:670,676,417 for short and689,948,889 for wide. Candidate
scheduler saved275–318 and2,626–2,725GETs respectively. In this round those counts
exactly explain physical GET reductions; latency is package-level, not an isolated
scheduler effect. The point-read hot-cost issue remains outside this scan coverage.
No Q20/fullstream production generalization. Evidence:
`target/hashjoin-replay-current-large/{round-summary.md,execution.md,comparison.json,index.jsonl}`.
Owned services stopped, immutable imported data retained.

### Downstream state replay

Set `downstream: true`, `probe_pattern: "delete_only"`, `probe_rounds: 1`, and
`fixture_epochs: 1` in a **new** fixture directory/bucket to measure the real
HashJoin → HashAgg → Materialize chain. The fixture seeds one left dimension
row per key, right fact rows, count(*) intermediate state (table 4, value column
2 only, full group-key filter prefix of length 2), and MV rows (table 5) in the same committed version. HashAgg groups by
the right `(key, seq)`. Deleting each left key must emit exactly one
`Delete(key, seq, 1)` per fact row.

Unlike join-only replay, this mode drains a final barrier, including local state
flush and dev-build old-value checks. The shared buffer is 688 MiB; block/meta
caches remain 64 MiB each. There is no meta commit or compaction in the measured
window, and any remote write fails the run. Exact per-table flush counts,
downstream get calls, and an untimed read of empty staged tables 0/4/5 verify
that downstream work actually happened. Metrics and cache usage are captured
before these final oracle reads. `point_gets` counts get calls, not the number
of SSTs visited; use the raw per-type SST histogram sums for that distinction.

This is a single-actor real-executor replay, not a full-cluster or unaligned
log-store benchmark. It supplements the join-only IO benefit cases and scalar
CPU diagnostic; it does not replace either.

### Corrected downstream comparison (2026-11-10)

Evidence: `target/hashjoin-agg-mv-production/` (`round-summary.md`,
`results-table.md`, `index.jsonl`, raw Prometheus files, and independent
`parent-audit.json`). The earlier `target/hashjoin-agg-mv-replay/` partial round
used missing Agg/MV filter hints and remains a separate no-hint diagnostic;
none of those samples are included below.

The corrected registered catalogs use Join hint1, Agg hint2/value_indices=[2],
and MV hint2/full-row values. The functional preflight and eight large samples
(two alternating pairs per case) passed. Baseline is unoptimized main
`88b98114336ae4511dcb9685afa95678e948e65a` with the same harness. Candidate uses
N=3 retained blockwise prefetch and IO scheduler; native MultiGet/meta reuse
is disabled in both arms. Both use dev builds, one real executor chain, four
runtime workers, chunk256, 64 MiB block/meta caches, 688 MiB shared buffer,
real Foyer and local MinIO. This is a constructed fixed-state replay.

All numbers below are means of the two runs per arm:

| Case | Keys × rows per key | Fixture bytes / blocks | Physical GET main → candidate | Read bytes change | Whole-chain seconds main → candidate | Time change |
| --- | --- | --- | --- | --- | --- | --- |
| Short prefix | 8,192 × 16 | 149,437,915 / 2,289 | 10,703 → 10,187.5 | −0.038% | 57.855 → 56.066 | −3.09% |
| Long prefix | 2,048 × 256 | 595,289,440 / 9,121 | 12,466.5 → 7,884.5 | −1.169% | 98.119 → 91.888 | −6.35% |

All paired samples improved elapsed time. Exact delete output, per-table
flush counts, empty final staged tables, unchanged fixture hashes/version,
and zero measured remote writes/errors/retries passed. Tables 0/2/4/5 had
identical committed-SST visits and logical block/meta request counts across
all arms; downstream point-get counts also matched. Agg Bloom checks were
131,072 per short run and 1,032,192 per long run; the latter rejected 507,897
SST checks, identical between arms, proving the production hint was exercised.

Scheduler accounting proves 507–516 fewer physical reads per short run and
4,430–4,443 per long run. Total main-to-candidate GET differences also include
4 fewer logical remote requests in the short case and 145–146 in the long
case; those extra differences are not attributed entirely to coalescing.

GET savings are not proportional to end-to-end latency savings: read bytes
changed little, and unchanged row/state work remains substantial. Coarse
process CPU deltas were main52/53 vs candidate52/52 seconds (short), and
main90/91 vs candidate88/88 seconds (long). These are process totals, not a
per-operator time breakdown. The earlier all-hot scalar ~1.45% overhead remains
separately unattributed; this combined replay shows no whole-chain regression.
Owned runtime/MinIO processes were stopped; immutable fixtures are retained.

### Candidate-only MultiGet/meta reuse increment (2026-11-10)

Evidence: `target/hashjoin-agg-mv-multiget-on/`, including `parent-audit.json`
and raw sample metrics. Reused the exact corrected fixtures and candidate
binary SHA `350b0b5d95449048935b7952322c67fbed0d4d2f66777b5be81674ceb20ed4f2`.
Only `RW_EXPERIMENTAL_SST_META_BATCH_REUSE` changed from 0 to 1; retained N3
prefetch, scheduler, dev sanity checks, cache budgets, and workload stayed the
same. No build or main rerun; one enabled sample per case, compared with the
previous two disabled candidate samples. This is an exploratory increment,
not a repeated significance test.

| Case | OFF seconds (prior two-run mean) | ON seconds (one run) | Time change | Agg meta lookups OFF → ON | Physical GET change |
| --- | --- | --- | --- | --- | --- |
| Short prefix | 56.066 | 56.127 | +0.11% | 262,144 → 144,384 (−44.92%) | +0.48% |
| Long prefix | 91.888 | 90.589 | −1.41% | 2,064,384 → 1,085,824 (−47.40%) | −2.96% |

Both enabled samples passed exact output/state/flush/Bloom/immutable-fixture
and zero measured remote write/error/retry gates. Per-key calls, committed SST
visits and logical data-block requests stayed equal to disabled samples.
Metadata lookups are `state_store_sst_store_block_request_counts{type="meta_total"}`,
not remote GETs. MV meta lookups stayed unchanged: this NoCheck MV's measured
gets are scalar flush sanity checks, outside native MultiGet. CPU deltas were
52s/87s (short/long), versus disabled52s/88s. Read bytes were unchanged for
short and +10,634 bytes for long. Scheduler accounting reconciled 463/4,671
saved reads. No additional runs were added; owned services were stopped and
fixture clones retained.

### Rolling native MultiGet with sanity disabled (2026-11-10)

Candidate-only, same-binary native OFF/ON comparison; no main rebuild/run. Binary SHA256 `2b17f6e97c57b098c5c7ffa16f6a35ab9af3c495b3f2f9271be2ec77f8d6ecee`. The candidate includes rolling key admission (C=10), batch metadata sharing with ordinary16-entry/1MiB retention plus one oversized SST, and the replay-only sanity-disable switch. Scalar get source is unchanged from the preceding candidate.

Two small preflights and four large samples reused the exact production-catalog SST/version fixtures. Each sample started with fresh real Foyer caches and used real MinIO, one actor/four runtime workers, output chunks256, block/meta caches64MiB, N=3 and scheduler ON. Dev code generation remains in use; disabling sanity does not make this a release benchmark.

| Case | Native OFF → ON time | Time change | GET OFF → ON | Read MiB (both) | Agg metadata queries OFF → ON |
|---|---:|---:|---:|---:|---:|
| Short:8192 prefixes ×16 rows | 23.640 → 23.348s | -1.23% | 10487 → 10489 | 655.596 | 131072 → 512 |
| Long:2048 prefixes ×256 rows | 36.087 → 33.925s | -5.99% | 11271 → 11354 | 720.163 | 1032192 → 4032 |

All samples passed exact output/flush counts, final staged-empty state, frozen manifest and zero measured remote write/error/retry gates. With sanity disabled, point gets for tables0/4/5 were exactly0/rows/0 in both arms. Per-key committed SST visits and logical data-block requests stayed equal; metadata query counts fell99.609375%. CPU deltas were21→21s(short) and35→33s(long).

GETs did not improve. Logical object reads were10585/11586 in both arms; scheduler saved98→96(short) and315→232(long), exactly accounting for the additional2/83 physical GETs. Read bytes were equal. No trace was collected to identify the detailed arrival-order cause. These are single samples per arm, not a statistically established improvement; old sanity-ON timings cannot isolate the rolling change.

Targeted validation also passed4 tests: pending metadata sharing/cancellation, bounded ordinary+oversized metadata reuse, blocked-first-key rolling admission, and native duplicate/missing/tombstone semantics. Astra reviewed the final source. Multiple oversized SSTs may still replace each other in the one oversized retention slot; retention excludes additional handles held by active reads.

Evidence: `target/hashjoin-agg-mv-no-sanity/` (`comparison.json`, `results-table.md`, `index.jsonl`, raw logs/results and source identities) and `target/multiget-rolling-review/` (review notes and targeted tests). Owned MinIO/runner stopped and fixture clones retained. This README entry was appended after the final source-identity gate, with no further executable-source changes.


### CPU profile and replay-context caveat (2026-11-10)

Two guarded long-prefix native OFF/ON captures reused candidate SHA256 `2b17f6e97c57b098c5c7ffa16f6a35ab9af3c495b3f2f9271be2ec77f8d6ecee` and the frozen 2048×256 fixture with the preceding sanity-OFF configuration. No build, main run, or executable-source edit. Xcode Running-only1ms samples were trimmed to the measured replay interval; startup and oracle work were excluded. Sampled CPU was39.602s/37.117s. Both captures passed the existing correctness, fixture and IO accounting gates; owned services exited.

| Inclusive CPU path | Native OFF | Native ON |
|---|---:|---:|
| HashAgg state restore | 19.565s | 17.329s |
| SST iterator construction | 7.660s | 7.633s |
| Missing streaming CONFIG path | 5.538s | 5.522s |
| SST metadata lookup | 1.739s | 0.087s |

Rows overlap; do not sum them. Metadata reuse demonstrably reduces lookup CPU, but other costs remain. Iterator construction linearly scans block table IDs; this also exists in baseline main88b98114336ae4511dcb9685afa95678e948e65a.

**Timing caveat for preceding replay rounds:** the harness creates ActorContext but does not install production Actor::run's task-local streaming CONFIG. Each long capture emitted1048576 warnings (~186MB), also observed in the preceding unprofiled log. The fallback flag equals the default strict-consistency setting, but logging is non-production overhead and perturbs scheduling. Storage sanity OFF does not remove it. Correctness gates remain valid; historical timings and scheduling-dependent GET savings need this qualification. The next harness correction should install the context, not silence the warning.

Profiler overhead also changed scheduler saved GETs from315 in the prior unprofiled OFF sample to4285 under profiling; these diagnostic captures are not new throughput or IO-effectiveness measurements. Dev precondition-check work remains significant. No source fixes were made in this round.

Full standalone report, effective configuration, identity/cleanup evidence and raw stacks: `target/multiget-cpu-profile/round-summary.md`, `analysis.json`, `execution.md`, `index.jsonl`, and `cases/512-wide/`.


### Corrected replay context and binary-search bounds (2026-11-10)

Installed the actual actor's streaming CONFIG scope around replay construction/polling, preserving strict consistency. Changed SST table-boundary clipping to binary search while retaining O(1) endpoint matches for full/single-table SST reads. Main/Astra cross-review of source, interacting paths and frozen execution data passed before runs; nine existing forward iterator tests and candidate build passed. This is a scoped correction review, not a whole-stack signoff.

Candidate SHA256 `61385aa9bd44d07f400e6e19f090f25f517ba8c40d63f1f8162bdfa21eed8013`; source identity unchanged during build/runs. No main run, profiler or fixture generation. Reused exact prior512-wide2048×256 production-catalog SST/version, real Foyer/MinIO, one actor/four workers, chunks256,N3,C10,schedulerON,64MiB block/meta caches, storage sanityOFF/dev code generation. Two small preflights plus one long sample per mode under the shared guard.

| Native MultiGet | Prior unprofiled seconds | Corrected seconds | Change | GET prior → corrected | CPU seconds prior → corrected |
|---|---:|---:|---:|---:|---:|
| OFF | 36.087 | 27.096 | −24.91% | 11271 → 7759 | 35 → 25 |
| ON | 33.925 | 25.273 | −25.50% | 11354 → 7716 | 33 → 23 |

Corrected ON is6.73% faster than corrected OFF. These compare the combined context+iterator correction against an already-rolling MultiGet candidate; they do not isolate each fix or establish statistical/release speedup. Missing-CONFIG warnings1048576→0; long logs~185.8MB→182KB.

All output/state/flush/version/manifest and zero remote-write/error/retry gates passed. Config and logical-work comparison with prior samples passed:524288rows, table4data_total524482, SST-child visits1032192, meta queries1032192 OFF/4032 ON. Object logical reads11586 and755145564bytes (720.163MiB) stay identical. Scheduler savedGETs315→3827 OFF and232→3870 ON exactly reconcile physical GET decreases. Arrival timing changes, but no trace isolates which correction changed each group.

Table4data_miss increased8983→9202 OFF and9032→9285 ON. It counts returned Source::Outer handles, including Foyer single-flight waiters, not unique IO; it is not a logical-work invariant. The analysis preserves this distinction and does not treat increased miss counters as increased physical reads.

Full configuration, review, identity, raw results and standalone record: `target/replay-context-binary-search/round-summary.md`, `source-review.md`, `execution.md`, `comparison.json`, `index.jsonl`, `cases/`. Owned services stopped, guards released, original fixtures retained. This entry was appended after source-identity validation with no additional executable changes.


### Fair main baseline and corrected candidate profile (2026-11-10)

Main reference88b98114336ae4511dcb9685afa95678e948e65a rebuilt with the same corrected CONFIG/sanityOFF harness, preserving unoptimized production reads/Foyer/linear bounds. Main SHA256c534fb4339d98c63abb7e5efb8cbcd1e9a928fc5b7f2dc00823272c0fee451c0; unchanged candidateSHA61385aa9bd44d07f400e6e19f090f25f517ba8c40d63f1f8162bdfa21eed8013. Main/Astra source/harness/data review passed before execution. Same frozen512-wide2048×256, HashJoin→HashAgg→MV, localMinIO/realFoyer, oneactor4workers/chunks256,64MiBcaches, storage sanityOFF/dev. One main smallpreflight + one unprofiled main/candidateON pair + separate candidateON profile; no fixture generation.

| Arm | Normal seconds | CPU seconds | GETs | Read MiB | Agg meta queries |
|---|---:|---:|---:|---:|---:|
| main | 40.088 | 34 | 11590 | 720.413 | 1032192 |
| corrected candidate ON | 24.893 | 23 | 7901 | 720.163 | 4032 |

Whole-stack observed time−37.91%, GET−31.83%; single pair, not statistical/release/fullcluster evidence. Both524288rows and all output/state/flush/immutable/Bloom/SST-visit gates passed; missingCONFIGwarnings0. Main logicalread metrics are unavailable, not0IO. Scheduler accounts3685 of3689 fewerGETs; remaining4 and261988fewerbytes are not attributed to coalescing. Prior correctedcandidate25.273s is also valid (~36.96% faster vs thismain); new candidate run was an extra contemporaneous check with no code/data/config change.

Separate correctedON profile:23.393sampledCPU seconds,0.043%unresolved, trimmed to replay interval. Disjoint ownerCPU: HashAgg14.960s/63.95%, backgroundstorageIOcache3.270s/13.98%, HashJoin2.534s/10.83%, Materialize1.187s/5.07%. HashAgg state restore9.840s and flush_data3.713s remain substantial; per-keySST/block lookup/seek and state/output processing remain after metadata sharing. Inclusive paths overlap. Old→new profileiteratorconstruction7.633→0.396s and strict-consistency/missingCONFIGpath5.522→0.026s corroborate fixes. Memorycopy and devprecondition-check overhead remain; no single oversizedfuture defect established. Profiletiming/GETs excluded from normal comparison.

Evidence, exact configuration/identities, review and standalone report: `target/replay-main-and-profile/round-summary.md`, `review.md`, `execution.md`, `comparison.json`, `analysis.json`, `cases/512-wide/`. Sourcefreeze gate passed; scratch restoredclean and ownedservices/profiler stopped. README appended after gate, no further executable changes.

## Distributed state-key controls

Set `checked_materialize: true` on a distributed downstream fixture to
exercise Materialize's `Overwrite` conflict checks. The actor has a downstream
dispatcher, and the replay requires old-row cache misses and SST meta lookups
on table 5. The fixture identity, final outputs and empty staged tables are
verified by the same gates as the existing delete replay. With
`workload: "hash_agg"`, `fixture_epochs` can spread the groups across separately
committed vnode ranges. Confirm the resulting SST count before using this
as a low-locality comparison.

`agg-distributed.json` restores and retracts2,097,152 existing COUNT groups in
seeded shuffled order through HashAgg -> Materialize. `join-regroup.json` reads
2048 join prefixes x256 rows with1024-byte payload, then aggregates by an
independent permuted key:262,144 existing groups start at count2. Each case uses
explicit hash distribution and a256-vnode bitmap in every StateTable, including
Materialize's production constructor. One actor owns all vnodes; the latter
case tests the state layout after redistribution, without network Exchange or
multi-actor backpressure. Neither case is a production cluster throughput test.

Set `RW_REPLAY_PREPARE_ONLY=1` in a dedicated process to create and validate the
fixture without timing replay. Subsequent normal processes reuse its SSTs and
version. `fixture-layout.json` distinguishes block-start vnode samples from
actual aggregation-key vnode coverage and reports both compressed block bytes
and decoded cache weight. Data layout inspection uses a separate store before
timing: Foyer starts fresh, but MinIO/OS caches are not guaranteed cold.

Point reads are scalar by default. To exercise the optional HashAgg and
Materialize MultiGet path, set

```toml
[storage.read_batch]
enabled = true
meta_reuse = true
```

Keep `enabled` and `concurrency` fixed and toggle `meta_reuse` to isolate
metadata reuse. To reproduce a historical measurement, use its recorded
binary and configuration.
A clean main comparison needs this same harness with test-only support and
scheduler unavailable, not an old singleton-fixture result. Keep build profiles,
configuration, fixture fingerprints and final-state checks identical across arms.

Distributed output validation accepts valid intermediate aggregate updates but
requires counts to decrease, paired UpdateDelete/UpdateInsert records, no
resurrection, and final deletion of every pre-existing group. Every vnode is
checked after the measured flush. Runtime read/CPU measurements exclude fixture
construction, layout inspection, input generation and final verification.

### Distributed-key comparison (2026-11-10)

Two new frozen fixtures exercised actual 256-vnode state distribution, with
identical catalogs, SST/version, inputs and output/state checks across arms.
Six tiny preflights and six formal samples passed. Main is the unoptimized
`88b98114336ae4511dcb9685afa95678e948e65a` reference with harness-only support;
candidate OFF/ON differ only in ready-block batch reuse. Both candidate arms
keep native MultiGet, metadata reuse, blockwise prefetch and scheduler enabled.
One sample per arm, no profiler, dev build, storage sanity OFF, four runtime
threads, one actor owning all vnodes, chunk size 256 and 64 MiB caches.

| Workload | Arm | Seconds | CPU seconds | GETs | Read MiB | Agg meta queries | Agg Foyer queries | Ready-block hits |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Random HashAgg -> MV | main | 182.260 | 179 | 742 | 56.295 | 2097152 | 2097892 | 0 |
| Random HashAgg -> MV | OFF | 75.426 | 75 | 740 | 56.295 | 8192 | 2097892 | 0 |
| Random HashAgg -> MV | ON | 77.226 | 76 | 742 | 56.295 | 8192 | 2056858 | 41034 |
| HashJoin -> independent-group HashAgg -> MV | main | 22.766 | 18 | 10420 | 646.896 | 262144 | 262236 | 0 |
| HashJoin -> independent-group HashAgg -> MV | OFF | 15.949 | 15 | 7622 | 642.130 | 1024 | 262236 | 0 |
| HashJoin -> independent-group HashAgg -> MV | ON | 15.833 | 15 | 7731 | 642.130 | 1024 | 220534 | 41702 |

The random Agg fixture has 2,097,152 groups in 741 blocks, with 47.330 MiB
decoded Foyer weight. Its state fits the 64 MiB block cache: this is a random
point-read CPU control, not sustained remote-IO pressure. The join fixture
has 8,597 right-side blocks / 531.895 MiB compressed block payload and 262,144
aggregation groups in 93 blocks / 5.917 MiB decoded weight. All 256 aggregation
vnodes contain actual keys. This replay omits network Exchange and multi-actor
backpressure; it is not a production cluster throughput measurement.

Observed whole-stack elapsed reductions against main are 58.62%/57.63%
(Agg OFF/ON) and 29.94%/30.45% (join OFF/ON). Agg GETs and bytes essentially
stay fixed: its speedup is not IO coalescing. Metadata acquisition counts fall
by 256x in both workloads, but the main comparison also contains earlier
iterator/future and other read-path changes, so it does not isolate MultiGet.

For join OFF/ON, 10,343 logical object reads produce 7,622/7,731 GETs. Scheduler
group counters account for exactly 2,721/2,612 saved GETs. The remaining 77
fewer GETs and 4.766 MiB versus main are not attributed to coalescing. Both
candidate arms have identical logical reads and bytes; their GET difference
is accounted for by different scheduler grouping, not fewer reads from ready
block retention.

Ready-block reuse alone reduces Agg Foyer queries by 1.96% in random Agg and
15.90% after regrouping. Its observed elapsed changes versus OFF are +2.39%
and -0.73%, respectively. These single samples establish no consistent timing
benefit or statistically significant regression. The bounded 16-entry batch
retention has much less locality in these workloads than in the earlier
singleton clustered fixture. Do not treat vnode count alone as the cause:
group-key/input distribution and block sharing are the relevant variables.

Source hashes matched before/after build and replay; main scratch was restored
clean and owned MinIO/runner exited. Exact binaries, fixture fingerprints,
configs, raw metrics, review and standalone report:
`target/distributed-read-replay/{summary.md,review.md,index.jsonl,cases/}`.
Main binary SHA256 `f4445bb72dbd05838b5137b8b71b9a3c86b9063d05faf55553aa4a662947146c`;
candidate `9e82636cbc343c5551c9ed86d00a7f174d34fa0f4e9c199974ec190f918fdfe4`.
This results section was added after the source-identity gate; no executable
source changed after measurement.

### Ready-block reuse removed (2026-11-10)

Removed the batch block map, its count/byte retention budget, cache-policy
matching, point-iterator batch association, runtime switch and data_batch_hit
metric. The batch owner is again SstMetaBatch, borrowed by rolling MultiGet
reads; only metadata is retained there. Iterator prefetch responses, Foyer
single-flight, native MultiGet concurrency and scheduler are preserved.
The historical comparison above remains evidence for the archived binaries,
not a timing measurement of the cleanup. No workload was rerun for this deletion.
Source-equivalence review and targeted validation are recorded under
`target/remove-block-reuse/`.

### Production startup controls (2026-11-10)

The earlier environment switches have been removed. Use `rw_config` in replay
JSON to load the same production TOML controls as CNs, including both direct
SstableStore and HummockTestEnv construction. See
[read optimization controls and metrics](../../../../docs/dev/src/design/read-optimization-testing.md)
for defaults, counter semantics, tuning guidance and Cloud preflight. Historical
samples above describe archived code, not measurements of this configuration
and observability update.

## Hummock change log read diagnostic

`change_log_reads: true` selects a separate storage diagnostic. It calls real
`HummockStorage::iter_log`; the older `fanout-*-changelog.json` cases instead
feed insert/delete rows into HashJoin and do not exercise this API.

Use a fresh output directory and empty dedicated MinIO bucket for each invocation.
`keys` is the number of scanned vnodes, `rows_per_key` is the number of keys per
vnode, and `value_bytes` controls deterministic value entropy/size. The fixture
commits three epochs, then scans only epoch 2 to check insert/update/delete values
without including epoch 3. Each invocation creates this fixture once and shares
its unchanged SSTs, version and metadata manager across all arms; it does not
reuse the older `fixture.pb` workflow.

The diagnostic runs three arms (all OFF, scheduler only, changelog prefetch plus
scheduler), reversing order every other repeat. Each arm has fresh actual Foyer
caches and the same configured storage budgets. Window budgets come from `rw_config`;
only `changelog_enabled` and scheduler activation vary between arms. Cases are a
full cold-cache scan, every-third-block cache warming, a two-key hard range, and
one-row early termination of a wide range. These are storage scenarios, not a
full snapshot-backfill actor or Cloud throughput benchmark. MinIO's OS cache is
not reset between arms. The in-image OFF arm is not a separate main build.

Treat the artificial cache holes, two-key bound, and one-row early termination as
mechanism/correctness diagnostics, not representative production performance
workloads. In particular, early termination declares a wide range and then drops
the iterator; it does not model ordinary chunk delivery or backpressure.
Production evaluation should follow snapshot backfill's `batch_iter_vnode_log`
callers: scan committed change logs from the saved PK through the vnode end,
consume through `VnodeStream` with the real chunk size and assigned vnodes, and
let normal reads and writes determine cache residency. Validate ongoing
insert/update/delete ingestion and log catch-up using the existing snapshot
backfill workload before drawing rollout conclusions from this diagnostic.

Results are saved under `changelog-results/`, including fixture object IDs/block
counts, effective storage options, per-arm metrics and `results.json`. Elapsed time
includes result validation and excludes draining outstanding prefetch. Physical
counts include that tail work. Cache-only gets join already registered Foyer
producers without supplying an outer-fetch closure; the drain's Foyer lookup
statistics must not be interpreted as scan lookup counts. No per-arm CPU metric
is collected. A long prefetch arm must consume at least one prefetched handle,
and every arm checks row content/order, stable version, no writes/retries/errors,
and zero pending/in-flight object reads after draining.


## Latest scheduler on the distributed join-regroup fixture (2026-11-10)

One current candidate replay reused the accepted `distributed-read-replay`
join-regroup SST/version/spec/manifest/layout and its historical main results.
This is the 256-vnode fixture with 262,144 groups and two input rows/group;
group key is decoupled from join prefix. One actor owns all vnodes, with four
runtime threads, shuffled probes, chunk256, weighted64MiB caches, dev build,
storage sanity OFF and strict consistency ON. It models post-distribution state
access without network exchange. The earlier legacy512-wide plan was stopped
in guard idle before build or execution after the fixture-selection correction.

Current settings: prefix lookaheadN3, MultiGet/meta reuseON at concurrency10,
Q1024/P32, batch16 and maxread256KiB; ready-block batch reuse is removed. The new
production TOML controls were checked in saved effective storage options. Only
one small current preflight and one long current sample ran, with no profiler.

| Version | Seconds | CPU seconds | GETs | Read bytes |
| --- | ---: | ---: | ---: | ---: |
| Saved main88b98114 | 22.766 | 18 | 10,420 | 678,319,392 |
| Saved candidate, ready-block reuse OFF | 15.949 | 15 | 7,622 | 673,322,318 |
| Latest candidate Q1024/P32 | 15.919 | 15 | 6,739 | 673,322,318 |

Latest logical reads10,343 minus saved3,604 requests equals6,739 physical GETs.
Compared with the saved candidate, physical reads fell11.58% at identical bytes
and logical counts; elapsed/CPU did not materially change. Compared with main,
elapsed fell30.08% and GETs35.33%, but that includes all candidate changes and
some changed read bytes; it is not isolated scheduler attribution. Aggregation
still makes262,144 point gets and262,236 block queries, with1,024 metadata queries.
All256 aggregation vnodes, frozen fixture hashes, output/finalstate invariants
and no measured writes/errors/retries were checked by the existing harness.
One-sample dev/MinIO evidence, not production rollout qualification.

Evidence: `target/replay-latest-regroup/`, with `index.jsonl`, per-case raw
metrics, effective options, logs, and identical source snapshots across build/run.
Latest binary SHA256
`b2864550af5a0f6e344c6749bb27ae1dbe7c72973fbfccb3f6d2a6b01a1515f5`.


## Latest distributed join-regroup CPU diagnosis (2026-11-10)

One separate Time Profiler run reused the exact latest binary and accepted
256-vnode fixture/configuration, with storage sanity OFF and strict consistency
ON. No rebuild or extra normal sample ran. PID-filtered Running samples within
the measured interval (10ms trimmed at each boundary) captured14.490CPU seconds;
whole-second process metric reported14s, unresolved stacks0.069%. Normal timing
remains15.919s; the diagnostic15.502s and7,200GETs are not performance comparisons.

| Nearest executor/background owner | Sampled CPU seconds | Share |
| --- | ---: | ---: |
| HashAgg | 7.934 | 54.8% |
| Background storage/IO/cache | 2.470 | 17.0% |
| HashJoin | 2.435 | 16.8% |
| Runtime/other | 1.011 | 7.0% |
| Materialize | 0.519 | 3.6% |
| Harness and unresolved | 0.121 | 0.8% |

Within HashAgg, state restoration includes5.019s and get_rows3.820s; nested
per-key get_with_metadata includes3.208s. These are inclusive, not additive.
Batch coordination outside per-key futures samples0.263s. Actual SST metadata
lookup0.037s and scheduler planning0.022s are small. Aggregate state fits in the
64MiBcache but262,144 pointgets/262,236 blockqueries still execute, explaining
why fewer remoteGETs leave substantial hot-read and aggregation CPU.

Copy leaves total1.950s (13.5%), spread among existing aggregate outputs, futures,
containers and read paths. This is not proof of one oversized future or a new
MultiGet regression. Dev Rust precondition paths include1.327s and overlap the
owner totals. No missingCONFIG warning storm or storage sanity path recurred.
Details, source attribution and limitations: `target/profile-latest-regroup/REPORT.txt`.
Output/fixture/config/source gates passed and ownedMinIO was stopped; raw trace,
XML, symbols, folded stacks and targeted stackaudit are retained beside the report.


## Bounded HashAgg allocation cleanup (2026-11-10)

The native batch restore now borrows GroupKey PK projections instead of copying
OwnedRows. Restore futures are created as buffer_unordered(10) admits them,
rather than collected for every missing group in advance. The missing-key list
remains to finish cache mutation before result insertion. Root and Astra reviewed
key/result alignment, borrowed-key lifetime, scalar fallback, errors and cancellation.
The single-state get_outputs fast path was deferred because directly awaiting its
large enum future needs separate frame-size evidence.

One guarded replay reused the exact accepted 256-vnode join-regroup fixture:
524,288 inputs, 262,144 groups, chunk256, four runtime workers, 64MiB caches,
N3, read batch concurrency10/meta reuseON, Q1024/P32, sanityOFF/strictON.
No main rerun, profiler, fixture generation or parameter sweep was performed.

| Version | Seconds | CPU seconds | GETs | Read bytes |
| --- | ---: | ---: | ---: | ---: |
| Saved main88b98114 | 22.766 | 18 | 10,420 | 678,319,392 |
| Previous latest | 15.919 | 15 | 6,739 | 673,322,318 |
| HashAgg cleanup | 15.431 | 14 | 7,087 | 673,322,318 |

Elapsed decreased3.07% versus the preceding sample; GETs increased5.16% with
unchanged10,343 logical reads and bytes. Async scheduling affects merge grouping;
these single dev/MinIO samples cannot attribute elapsed or CPU changes solely to
the cleanup. Agg pointgets262,144, blockqueries262,236 and metaqueries1,024 are
unchanged. No new block reuse/cache strategy was added.

Existing HashAgg integration tests4/4 and full workspace all-target Clippy passed.
Frozen source hashes match before checks/build and after replay; fixtures, output
and final state invariants passed; measured object writes/errors/retries werezero.
All scheduler gauges drained and owned MinIO was stopped. Full evidence and
limitations: `target/agg-hotpath-cleanup/REPORT.txt`; binary SHA256
`d66a32257abb5ef8aaa3eaa3731b1684e4df4e90e634148381eb01d9c256f0e7`.

### Changelog analysis while replay ran

No CPU stack profile exists for the saved full-cluster changelog run. A re-read
of its107 metric scrapes found8 with nonzero queued requests, including queues
of572/579/556 with observed physical in-flight1/3/6. Gauges are non-atomic and
coarse; this neither proves underutilization nor supports attributing all24.082ms
queue time to P32 saturation. Queue time includes wakeup, planning and time until
the group future is polled. Current source has no mandatory full-batch wait.
New/old change-log iterator initial positioning is serial despite independent
inputs; concurrent initialization is only a candidate, not a measured win, and
later epoch pairing still requires ordered advancement. No changelog source was
changed. Detailed source/metric limits and next diagnostic scope are recorded in
`target/cross-db-changelog-dispatch/PROFILE-INTERPRETATION.txt`.


## Aggregate output future size (2026-11-10)

Following the get_outputs copy samples, an actual HummockStorage monomorphization
was compiled with rustc -Zprint-type-sizes. The large per-aggregate future was
caused by the materialized-input cache-miss branch: iter construction4176B and
live scan3592B. Even count/sum shared that static future layout. TryJoinAll's
outer future was only88B, but each TryMaybeDone slot occupied4352B. Profile
copy stacks include collection, Pin::set and take_output/mem::replace.

The unchanged cache-fill body now lives in private sync_cache, boxed only when
input cache is unsynced. Count/sum and synced materialized states incur no added
allocation; misses incur one. Parallel output collection, reset, stats, limits,
prefix/prefetch, errors and cancellation remain unchanged. This keeps the entire
scan, not merely iterator construction, outside hot output futures.

| Actual compiled future/layout | Before bytes | After bytes |
| --- | ---: | ---: |
| AggState::get_output | 4,344 | 312 |
| MaterializedInputState::get_output | 4,304 | 272 |
| TryMaybeDone element | 4,352 | 320 |
| AggGroup::get_outputs | 152 | 152 |
| TryJoinAll | 88 | 88 |

The extracted cold sync_cache future is4248B on the heap when needed. These are
current dev-build layouts, not a measurement of main's sizes or release CPU.
Main88b98114 already contains the same inline-scan structure.

Astra reviewed the scoped extraction;8 existing materialized-input unit tests,
4 HashAgg integration tests and workspace all-target Clippy passed. The ordinary
benchmark was built separately without diagnostic rustc flags. One guarded
preflight and one formal replay used the same frozen256-vnode join-regroup
fixture/configuration as the preceding round; no profile/mainrerun/newdata.

| Version | Seconds | CPU seconds | GETs | Read bytes |
| --- | ---: | ---: | ---: | ---: |
| Saved main88b98114 | 22.766 | 18 | 10,420 | 678,319,392 |
| Previous HashAgg allocation cleanup | 15.431 | 14 | 7,087 | 673,322,318 |
| Cold input-cache future boxed | 14.863 | 14 | 7,157 | 673,322,318 |

Elapsed decreased3.68% in this sample. Logicalreads10,343, Aggpointgets262,144,
Foyerqueries262,236 and metaqueries1,024 remained unchanged. GETgrouping changed
by~1%; the optimization targets CPU/allocation, not IOvolume. Future-size savings
are confirmed; a single dev/MinIO sample does not prove stable/release speedup.
Cold-miss allocation cost was not performance-benchmarked for every aggregate.

Fixture/output/final-state checks and unchanged source snapshots passed, zero
measured object writes/errors/retries, all scheduler gauges drained; ownedMinIO
stopped. Evidence: `target/agg-output-audit/` and
`target/agg-output-cold-box/REPORT.txt`. Ordinary binary SHA256:
`6259a3412f76f3519d1664da278dd30b11c727dcc54e723c46a91dc6bbd2b95e`.

## Release validation of aggregate cold-path boxing (2026-11-10)

This comparison isolates only the preceding `sync_cache` boxing change on the
current optimization branch, not main. Both binaries use ordinary Cargo release
(opt-level 3, debug assertions and overflow checks disabled), without profiler
or profile overrides. HEAD was `817b276bcf69917c61b093f4251d080035d2e618`;
dirty-source snapshots and binary hashes are retained with the evidence.

The immutable join-regroup fixture and configuration were reused: 524,288 inputs,
262,144 groups, 256 aggregation vnodes owned by one actor, four runtime workers,
chunk 256, 64 MiB Foyer block cache, N3/256 KiB, MultiGet C10 and meta reuse ON,
scheduler Q1024/P32/batch quantum16, sanity OFF and strict consistency ON. Each
formal sample starts a fresh replay process and cache against real MinIO. The
measurement covers the replay phase, excluding build and fixture preparation.
Order: unboxed, boxed, boxed, unboxed, unboxed, boxed.

| Release variant | Three elapsed samples (s) | Median (s) | Physical GETs |
| --- | --- | ---: | --- |
| Unboxed | 4.724, 4.676, 4.698 | 4.698 | 10,190 / 10,197 / 10,208 |
| Boxed | 4.622, 4.628, 4.747 | 4.628 | 10,224 / 10,199 / 10,178 |

The boxed median is 1.50% lower, but ranges overlap and the boxed slowest sample
is slower than every unboxed sample. Three samples per arm do not establish a
stable release benefit. The coarse CPU counter reports 3 seconds for every run;
it cannot resolve sub-second CPU differences. All runs have 10,343 logical reads,
673,322,318 read bytes, 262,144 aggregation point gets, 262,236 Foyer queries and
1,024 meta queries. Output and final-state oracles pass.

Separately, release saves only 119–165 physical requests (1.15–1.60%) versus
about 30.8% in the preceding boxed dev sample. Build profiles differ; this does
not identify the cause. Dev IO-coalescing results must not be extrapolated to
release. No follow-up scheduler experiment was performed in this round.

The initial attempt failed before measurement because the Meta test checkpoint
store uses `memory-isolated-for-test`, previously accepted only with debug
assertions. An explicit object-store test feature, enabled by the existing Meta
test feature, now supports this isolated store in release tests. The SST fixture
still uses real MinIO. Default production builds do not enable the test feature.
A source-inventory false positive when unboxed `minput.rs` exactly matched HEAD
was also corrected. The replacement round passed runtime preflight immediately
after each build, then the formal matrix. Workspace Clippy and scoped review
passed. Earlier failed-attempt evidence remains in `target/agg-output-release/`.

Evidence: `target/agg-output-release-ready/REPORT.txt`, `comparison.json`,
`index.jsonl`, per-run logs/metrics/effective options and source snapshots.
Unboxed binary SHA256:
`a86a680b36da0a827719496ba8ce68e666124bdd64571e77a44cd24a2756a446`;
boxed binary SHA256:
`fb233af2d6b16fa2c9dd2d1851dbdb2eee380ecf494b9191b4223f355b2f5e96`.
Source snapshots match after restoring the boxed source. There were no measured
object writes, errors or retries; scheduler gauges drained. Owned MinIO/replay
processes stopped, ports 9301/9400 closed, and the immutable fixture was retained.

### HashAgg miss launch order

Compared original miss order with sorting misses by the intermediate StateTable's
encoded PK, with the same 10 rolling get-to-restore futures. Two alternating
pairs per case used fixed SSTs on local MinIO with fresh real Foyer caches.
Both regenerated fixtures matched the previous byte/layout manifests.

| Case | Median GETs, original → sorted | Median elapsed |
| --- | ---: | ---: |
| Five SST COUNT, 22 MiB cache | 70,427 → 68,379 | 35.87 → 34.89 s |
| Same COUNT fixture, 64 MiB cache | 377 → 355 | 31.62 → 33.42 s |
| 512-wide pipeline, 64 MiB cache | 11,590 → 11,590 | 31.27 → 31.92 s |

In the cold COUNT case both pairs saved GETs. About 96% of the reduction came
from fewer Foyer outer fetches. The remaining GET savings came from scheduler
merges. HashAgg meta query counts were identical. Elapsed changes were -6.5%
and +1.4% in the cold case, and both other cases ran slower after sorting.
The sorted arm includes eager key preparation and a second key encoding for
the comparator. The temporary dev replay patch was removed from product source.
See `target/hashagg-launch-order/round-summary.md` for exact identity,
configuration, per-pair metrics, and cleanup evidence.

### HashAgg meta reuse at C10, C32 and C64

The archived fixed ordered MultiGet binary ran two pairs per concurrency and
fixture. Within each pair, only the meta reuse switch changed. Each fixture
reused its pinned SST snapshot and the historical byte/layout manifest.

| COUNT case | Foyer meta queries saved at C10 / C32 / C64 | Median elapsed change at C10 / C32 / C64 |
| --- | ---: | ---: |
| 5 SST, 64 MiB cache | 94.1% / 94.0% / 94.0% | −4.7% / −4.4% / −5.6% |
| 128 SST, 5 MiB cache | 12.1% / 12.1% / 12.1% | +0.5% / +0.7% / +0.8% |

The 5 SST case benefits at all three limits. In the 128 SST case C64
raises chunk P99 in both pairs, and the retention budget rejects about
214,000 lookup attempts per sample at each limit. Physical GET counts
show no consistent reduction. See `target/hashagg-meta-concurrency/round-summary.md`
for the effective settings, exact pairs and raw evidence.

### Longer HashAgg meta reuse replay

Replayed 1,048,576 COUNT retractions against one fixed five-SST snapshot
at C64, using fresh Foyer caches and local MinIO. Both cache sizes had an
OFF/ON pair and an ON/OFF pair, with the scheduler unchanged. A diagnostic
32 MiB meta retention budget avoided evicting these five handles.

| Block cache | Median elapsed, OFF → ON | Process CPU seconds | Physical GETs |
| --- | ---: | ---: | ---: |
| 64 MiB | 31.70 → 30.14 s (−4.9%) | 35.36 → 33.82 | 359 → 359 |
| 22 MiB | 36.22 → 34.69 s (−4.2%) | 63.18 → 61.22 | 68,826 → 68,343 |

Both cache sizes saved about 1.57 seconds during input processing and 98%
of Foyer meta lookups. At 64 MiB, the warm input windows had no GETs and
the same two roughly 2.8-second HashAgg output flushes. At 22 MiB, GETs
continued throughout the input and averaged 0.707 ms with either setting.
The evidence points to CPU savings from avoiding Foyer meta lookups. It
does not establish a gain under the default retention budget or on S3.
See `target/hashagg-meta-long/round-summary.md` for settings and raw evidence.
